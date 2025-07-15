import pandas as pd
import json
from flask import Flask, render_template, request, jsonify
from flask.json.provider import DefaultJSONProvider
from core.analysis import run_multi_year_analysis
import os
import numpy as np
from sqlalchemy import create_engine, text, exc, inspect
import logging
from dotenv import load_dotenv

load_dotenv()
app = Flask(__name__)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.StreamHandler()])
logger = logging.getLogger(__name__)

class CustomJSONProvider(DefaultJSONProvider):
    def default(self, obj):
        if isinstance(obj, np.integer): return int(obj)
        if isinstance(obj, np.floating): return float(obj)
        if isinstance(obj, np.ndarray): return obj.tolist()
        if isinstance(obj, pd.Timestamp): return obj.isoformat()
        if pd.isna(obj): return None
        return super().default(obj)

app.json = CustomJSONProvider(app)

def create_db_engine():
    database_url = os.environ.get('DATABASE_URL')
    if not database_url:
        logger.error("DATABASE_URL não definida.")
        raise ValueError("DATABASE_URL não definida.")
    
    if database_url.startswith("postgresql://"):
        conn_str = database_url.replace("postgresql://", "postgresql+psycopg2://", 1)
    else:
        conn_str = database_url

    try:
        engine = create_engine(conn_str, pool_pre_ping=True)
        with engine.connect():
            logger.info("Conexão com o banco de dados estabelecida com sucesso.")
        return engine
    except Exception as e:
        logger.error(f"Falha ao criar a engine do banco de dados: {e}", exc_info=True)
        return None

def load_ticker_mapping(file_path='data/mapeamento_tickers.csv'):
    logger.info(f"Carregando mapeamento de tickers de {file_path}...")
    try:
        df_tickers = pd.read_csv(file_path, sep=',')
        df_tickers.columns = [col.strip().upper() for col in df_tickers.columns]
        df_tickers['CD_CVM'] = pd.to_numeric(df_tickers['CD_CVM'], errors='coerce').dropna().astype(int)
        df_tickers = df_tickers[['CD_CVM', 'TICKER']].drop_duplicates(subset=['CD_CVM'])
        logger.info(f"{len(df_tickers)} mapeamentos carregados.")
        return df_tickers
    except Exception as e:
        logger.error(f"Erro ao carregar mapeamento: {e}", exc_info=True)
        return pd.DataFrame()

def get_companies_list(engine, ticker_mapping_df):
    if engine is None:
        logger.error("Engine do banco de dados não está disponível para get_companies_list.")
        return []
    
    logger.info("Buscando lista de empresas do banco (versão otimizada)...")
    try:
        query = text('SELECT DISTINCT "denom_cia", "cd_cvm" FROM financial_data ORDER BY "denom_cia";')
        connection = engine.connect().execution_options(stream_results=True)
        
        all_chunks = []
        for chunk_df in pd.read_sql(query, connection, chunksize=5000):
            all_chunks.append(chunk_df)
        
        connection.close()

        if not all_chunks:
            logger.warning("Nenhuma empresa encontrada no banco de dados.")
            return []

        df_companies_db = pd.concat(all_chunks, ignore_index=True)
        df_companies_db.rename(columns={'denom_cia': 'NOME_EMPRESA', 'cd_cvm': 'CD_CVM'}, inplace=True)
        
        final_df = pd.merge(df_companies_db, ticker_mapping_df, on='CD_CVM', how='left')
        
        final_df['TICKER'] = final_df['TICKER'].fillna('S/TICKER')
        
        logger.info(f"{len(final_df)} empresas encontradas e mapeadas com sucesso.")
        return final_df.to_dict(orient='records')
        
    except Exception as e:
        logger.error(f"Erro ao buscar lista de empresas: {e}", exc_info=True)
        return []

def ensure_valuation_table_exists(engine):
    if engine is None: return False
    try:
        inspector = inspect(engine)
        if inspector.has_table('valuation_results'):
            logger.info("Tabela 'valuation_results' já existe.")
            return True
        logger.info("Criando tabela 'valuation_results'...")
        create_query = text("""
        CREATE TABLE valuation_results (
            "Ticker" VARCHAR(20) PRIMARY KEY, "Nome" VARCHAR(255), "Upside" NUMERIC, "ROIC" NUMERIC, 
            "WACC" NUMERIC, "Spread" NUMERIC, "Preco_Atual" NUMERIC, "Preco_Justo" NUMERIC,
            "Market_Cap" BIGINT, "EVA" NUMERIC, "Capital_Empregado" NUMERIC, "NOPAT" NUMERIC
        );""")
        with engine.connect() as connection:
            with connection.begin():
                connection.execute(create_query)
        logger.info("Tabela 'valuation_results' criada com sucesso.")
        return True
    except Exception as e:
        logger.error(f"Erro ao criar tabela valuation_results: {e}", exc_info=True)
        return False

def run_valuation_worker_if_needed(engine):
    if engine is None: return
    try:
        with engine.connect() as connection:
            count = connection.execute(text("SELECT COUNT(*) FROM valuation_results")).scalar_one_or_none()
        
        if count == 0:
            logger.info("Tabela valuation_results está vazia. Iniciando worker de valuation empresa por empresa...")
            from core.valuation_analysis import run_full_valuation_analysis
            
            df_tickers = load_ticker_mapping()
            if df_tickers.empty:
                logger.error("Mapeamento de tickers não pôde ser carregado. Abortando worker.")
                return

            with engine.connect() as connection:
                company_codes_df = pd.read_sql('SELECT DISTINCT "cd_cvm" FROM financial_data', connection)
            
            all_valuation_results = []
            total_companies = len(company_codes_df)
            logger.info(f"Encontradas {total_companies} empresas para analisar.")

            for index, row in company_codes_df.iterrows():
                # ## CORREÇÃO 1: ERRO `numpy.int64` ##
                # Converte explicitamente o código CVM para um int nativo do Python.
                # O driver do banco de dados (psycopg2) não sabe como lidar com o tipo 'numpy.int64' do Pandas.
                cvm_code = int(row['cd_cvm'])
                
                logger.info(f"Processando empresa {index + 1}/{total_companies} (CVM: {cvm_code})...")
                
                with engine.connect() as connection:
                    query = text('SELECT * FROM financial_data WHERE "cd_cvm" = :cvm_code')
                    df_company_data = pd.read_sql(query, connection, params={'cvm_code': cvm_code})

                if not df_company_data.empty:
                    valuation_result = run_full_valuation_analysis(df_company_data, df_tickers)
                    if valuation_result:
                        all_valuation_results.extend(valuation_result)

            if all_valuation_results:
                df_results = pd.DataFrame(all_valuation_results)
                df_results.to_sql('valuation_results', engine, if_exists='replace', index=False, chunksize=100)
                logger.info(f"Worker concluído. {len(df_results)} resultados salvos no banco.")
            else:
                logger.warning("Worker de valuation não gerou resultados.")
                
    except Exception as e:
        logger.error(f"Erro ao executar worker de valuation: {e}", exc_info=True)

# --- Inicialização da Aplicação ---
db_engine = create_db_engine()
if db_engine:
    df_tickers = load_ticker_mapping()
    companies_list = get_companies_list(db_engine, df_tickers)
    if ensure_valuation_table_exists(db_engine):
        run_valuation_worker_if_needed(db_engine)

# --- Rotas Flask ---
@app.route('/', methods=['GET', 'POST'])
def index():
    try:
        if request.method == 'GET':
            return render_template('index.html', companies=companies_list)

        # ## CORREÇÃO 2: ERRO `int(None)` ##
        # Verifica se o 'cvm_code' foi enviado pelo formulário antes de tentar convertê-lo.
        cvm_code_str = request.form.get('cvm_code')
        if not cvm_code_str:
            return render_template('index.html', companies=companies_list, error="Por favor, selecione uma empresa.")
        
        cvm_code = int(cvm_code_str)
        start_year = int(request.form.get('start_year'))
        end_year = int(request.form.get('end_year'))
        years_to_analyze = list(range(start_year, end_year + 1))

        with db_engine.connect() as connection:
            query = text('SELECT * FROM financial_data WHERE "cd_cvm" = :cvm_code')
            df_company = pd.read_sql(query, connection, params={'cvm_code': cvm_code})

        if df_company.empty:
            return render_template('index.html', companies=companies_list, error=f"Nenhum dado encontrado para a empresa CVM {cvm_code}.")

        logger.info(f"Iniciando análise Fleuriet para CVM {cvm_code}")
        fleuriet_results, fleuriet_error = run_multi_year_analysis(df_company, cvm_code, years_to_analyze)
        
        if fleuriet_error:
            return render_template('index.html', companies=companies_list, error=fleuriet_error)

        return render_template(
            'index.html', 
            companies=companies_list, 
            fleuriet_results=fleuriet_results,
            selected_company=str(cvm_code),
            start_year=start_year,
            end_year=end_year
        )
    except Exception as e:
        logger.error(f"Erro crítico na rota principal: {e}", exc_info=True)
        return render_template('500.html', error="Ocorreu um erro inesperado."), 500

@app.route('/get_valuation_data', methods=['GET'])
def get_valuation_data():
    logger.info("Buscando dados de valuation pré-calculados...")
    try:
        if not db_engine or not inspect(db_engine).has_table('valuation_results'):
            return jsonify({"error": "A análise de valuation ainda não foi executada. Por favor, aguarde alguns minutos e tente novamente."}), 404
        
        with db_engine.connect() as connection:
            df_valuation = pd.read_sql_table('valuation_results', connection)
            
        if df_valuation.empty:
            return jsonify({"error": "A análise de valuation ainda não foi concluída. Por favor, aguarde alguns minutos e tente novamente."}), 404
            
        return jsonify(df_valuation.to_dict(orient='records'))
        
    except Exception as e:
        logger.error(f"Erro ao buscar dados de valuation: {e}", exc_info=True)
        return jsonify({"error": "Falha ao acessar os resultados da análise de valuation."}), 500

@app.route('/run_valuation_worker', methods=['POST'])
def run_valuation_worker_manual():
    try:
        logger.info("Executando worker de valuation manualmente...")
        run_valuation_worker_if_needed(db_engine)
        return jsonify({"success": True, "message": "Worker de valuation executado com sucesso."})
    except Exception as e:
        logger.error(f"Erro ao executar worker manualmente: {e}", exc_info=True)
        return jsonify({"success": False, "error": "Falha ao executar o worker de valuation."}), 500

@app.errorhandler(404)
def page_not_found(e): return render_template('404.html'), 404
@app.errorhandler(500)
def internal_server_error(e): return render_template('500.html'), 500

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)
