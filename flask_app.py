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
    
    # Garante que o dialeto psycopg2 seja usado
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
    """
    Busca a lista de empresas do banco de dados de forma otimizada para não estourar a memória.
    """
    if engine is None:
        logger.error("Engine do banco de dados não está disponível para get_companies_list.")
        return []
    
    logger.info("Buscando lista de empresas do banco (versão otimizada)...")
    try:
        # CORREÇÃO 1: Nomes das colunas em minúsculo ("DENOM_CIA" -> "denom_cia") para corresponder ao schema do PostgreSQL.
        query = text('SELECT DISTINCT "denom_cia", "cd_cvm" FROM financial_data ORDER BY "denom_cia";')

        # CORREÇÃO 2: Otimização de memória usando streaming e lotes (chunks) para evitar o erro 'OutOfMemory'.
        connection = engine.connect().execution_options(stream_results=True)
        
        all_chunks = []
        # O 'chunksize' divide a leitura do banco de dados em pedaços para não carregar tudo na RAM de uma vez.
        for chunk_df in pd.read_sql(query, connection, chunksize=5000):
            all_chunks.append(chunk_df)
        
        connection.close()

        if not all_chunks:
            logger.warning("Nenhuma empresa encontrada no banco de dados.")
            return []

        df_companies_db = pd.concat(all_chunks, ignore_index=True)

        # Renomeia as colunas para um padrão consistente para o merge e a exibição.
        df_companies_db.rename(columns={'denom_cia': 'NOME_EMPRESA', 'cd_cvm': 'CD_CVM'}, inplace=True)
        
        # Junta os dados do banco com o mapeamento de tickers.
        final_df = pd.merge(df_companies_db, ticker_mapping_df, on='CD_CVM', how='left')
        final_df['TICKER'].fillna('S/TICKER', inplace=True)
        
        logger.info(f"{len(final_df)} empresas encontradas e mapeadas com sucesso.")
        return final_df.to_dict(orient='records')
        
    except Exception as e:
        logger.error(f"Erro ao buscar lista de empresas: {e}", exc_info=True)
        return []

def ensure_valuation_table_exists(engine):
    """Garante que a tabela valuation_results existe, criando-a se necessário."""
    if engine is None:
        return False
    
    try:
        inspector = inspect(engine)
        if inspector.has_table('valuation_results'):
            logger.info("Tabela 'valuation_results' já existe.")
            return True
        
        logger.info("Criando tabela 'valuation_results'...")
        create_query = text("""
        CREATE TABLE valuation_results (
            "Ticker" VARCHAR(20) PRIMARY KEY,
            "Nome" VARCHAR(255),
            "Upside" NUMERIC, "ROIC" NUMERIC, "WACC" NUMERIC, "Spread" NUMERIC,
            "Preco_Atual" NUMERIC, "Preco_Justo" NUMERIC,
            "Market_Cap" BIGINT, "EVA" NUMERIC,
            "Capital_Empregado" NUMERIC, "NOPAT" NUMERIC
        );
        """)
        
        with engine.connect() as connection:
            with connection.begin(): # Usar transação
                connection.execute(create_query)
        
        logger.info("Tabela 'valuation_results' criada com sucesso.")
        return True
        
    except Exception as e:
        logger.error(f"Erro ao criar tabela valuation_results: {e}", exc_info=True)
        return False

def run_valuation_worker_if_needed(engine):
    """Executa o worker de valuation se a tabela estiver vazia."""
    if engine is None:
        return
    
    try:
        with engine.connect() as connection:
            result = connection.execute(text("SELECT COUNT(*) FROM valuation_results")).fetchone()
            count = result[0] if result else 0
            
        if count == 0:
            logger.info("Tabela valuation_results está vazia. Executando worker...")
            from core.valuation_analysis import run_full_valuation_analysis
            
            # Carrega dados necessários
            df_tickers = load_ticker_mapping()
            with engine.connect() as connection:
                df_full_data = pd.read_sql('SELECT * FROM financial_data', connection)
            
            if not df_full_data.empty and not df_tickers.empty:
                valuation_results = run_full_valuation_analysis(df_full_data, df_tickers)
                
                if valuation_results:
                    df_results = pd.DataFrame(valuation_results)
                    df_results.to_sql(
                        'valuation_results',
                        engine,
                        if_exists='replace',
                        index=False,
                        chunksize=100
                    )
                    logger.info(f"Worker executado com sucesso. {len(df_results)} resultados salvos.")
                else:
                    logger.warning("Worker não gerou resultados.")
            else:
                logger.warning("Dados insuficientes para executar o worker.")
                
    except Exception as e:
        logger.error(f"Erro ao executar worker de valuation: {e}", exc_info=True)

# --- Inicialização da Aplicação ---
db_engine = create_db_engine()
df_tickers = load_ticker_mapping()
companies_list = get_companies_list(db_engine, df_tickers)

# Garante que a tabela de valuation existe e executa o worker se necessário
if db_engine:
    if ensure_valuation_table_exists(db_engine):
        run_valuation_worker_if_needed(db_engine)

@app.route('/', methods=['GET', 'POST'])
def index():
    try:
        if request.method == 'GET':
            return render_template('index.html', companies=companies_list)

        cvm_code = int(request.form.get('cvm_code'))
        start_year = int(request.form.get('start_year'))
        end_year = int(request.form.get('end_year'))
        years_to_analyze = list(range(start_year, end_year + 1))

        with db_engine.connect() as connection:
            query = text('SELECT * FROM financial_data WHERE "cd_cvm" = :cvm_code') # Corrigido para minúsculo
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
        inspector = inspect(db_engine)
        if not inspector.has_table('valuation_results'):
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
    """Endpoint para executar o worker de valuation manualmente."""
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
