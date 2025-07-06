import pandas as pd
import json
from flask import Flask, render_template, request
from flask.json.provider import DefaultJSONProvider
from core.analysis import run_multi_year_analysis
import os
import numpy as np
from sqlalchemy import create_engine, text, exc
import logging
from dotenv import load_dotenv

# Carrega variáveis de ambiente de um arquivo .env, se existir (para desenvolvimento local)
load_dotenv()

app = Flask(__name__)

# --- Configuração de Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# --- Serializador JSON Customizado ---
class CustomJSONProvider(DefaultJSONProvider):
    def default(self, obj):
        if isinstance(obj, np.integer): return int(obj)
        if isinstance(obj, np.floating): return float(obj)
        if isinstance(obj, np.ndarray): return obj.tolist()
        if isinstance(obj, pd.Timestamp): return obj.isoformat()
        return super().default(obj)

app.json = CustomJSONProvider(app)

# --- Configuração do Banco de Dados ---
def create_db_engine():
    """Cria uma engine SQLAlchemy a partir da variável de ambiente DATABASE_URL."""
    database_url = os.environ.get('DATABASE_URL')
    if not database_url:
        logger.error("A variável de ambiente DATABASE_URL não foi definida.")
        raise ValueError("A variável de ambiente DATABASE_URL não foi definida.")

    conn_str = database_url.replace("postgresql://", "postgresql+psycopg2://", 1)
    try:
        engine = create_engine(conn_str, pool_pre_ping=True)
        with engine.connect():
            logger.info("Conexão com o banco de dados estabelecida com sucesso.")
        return engine
    except Exception as e:
        logger.error(f"Falha ao criar a engine do banco de dados: {e}", exc_info=True)
        return None

# --- Carregamento de Dados Iniciais (LÓGICA CORRIGIDA) ---

def load_ticker_mapping(file_path='data/mapeamento_tickers.csv'):
    """Carrega o mapeamento de tickers de um arquivo CSV."""
    logger.info(f"A carregar mapeamento de tickers de {file_path}...")
    try:
        if not os.path.exists(file_path):
            logger.error(f"Arquivo de mapeamento de tickers não encontrado em: {file_path}")
            return pd.DataFrame()

        df_tickers = pd.read_csv(file_path, sep=',', encoding='utf-8')
        df_tickers.columns = [col.strip().upper() for col in df_tickers.columns]
        
        required_cols = {'CD_CVM', 'TICKER'}
        if not required_cols.issubset(df_tickers.columns):
            logger.error(f"Arquivo de mapeamento não contém as colunas obrigatórias: {required_cols}")
            return pd.DataFrame()
        
        # Garante que CD_CVM seja numérico para o merge
        df_tickers['CD_CVM'] = pd.to_numeric(df_tickers['CD_CVM'], errors='coerce')
        df_tickers.dropna(subset=['CD_CVM'], inplace=True)
        df_tickers['CD_CVM'] = df_tickers['CD_CVM'].astype(int)
        
        df_tickers = df_tickers[['CD_CVM', 'TICKER']].drop_duplicates(subset=['CD_CVM'])
        logger.info(f"{len(df_tickers)} mapeamentos de tickers carregados.")
        return df_tickers
    except Exception as e:
        logger.error(f"Erro ao carregar o arquivo de mapeamento de tickers: {e}", exc_info=True)
        return pd.DataFrame()

def get_companies_list(engine, ticker_mapping_df):
    """Busca a lista de empresas do banco e junta com os tickers."""
    if engine is None:
        logger.error("Engine do banco de dados não está disponível.")
        return []
        
    logger.info("A buscar lista de empresas do banco de dados...")
    try:
        with engine.connect() as connection:
            query = text('SELECT DISTINCT "DENOM_CIA", "CD_CVM" FROM financial_data ORDER BY "DENOM_CIA";')
            df_companies_db = pd.read_sql(query, connection)
            df_companies_db['CD_CVM'] = pd.to_numeric(df_companies_db['CD_CVM'], errors='coerce').astype('Int64')
            logger.info(f"{len(df_companies_db)} empresas únicas encontradas no banco.")

            if df_companies_db.empty:
                return []

            # Renomeia a coluna para o nome que o template espera
            df_companies_db.rename(columns={'DENOM_CIA': 'NOME_EMPRESA'}, inplace=True)

            # Junta (merge) com os tickers se o mapeamento foi carregado
            if not ticker_mapping_df.empty:
                final_df = pd.merge(df_companies_db, ticker_mapping_df, on='CD_CVM', how='left')
                # Se uma empresa não tiver ticker, preenche com um valor padrão
                final_df['TICKER'].fillna('S/TICKER', inplace=True)
            else:
                final_df = df_companies_db
                final_df['TICKER'] = 'S/TICKER'
            
            return final_df.to_dict(orient='records')

    except Exception as e:
        logger.error(f"Um erro inesperado ocorreu ao buscar e juntar a lista de empresas: {e}", exc_info=True)
        return []

# Inicializa a engine e carrega os dados na inicialização da aplicação
db_engine = create_db_engine()
df_tickers = load_ticker_mapping()
companies_list = get_companies_list(db_engine, df_tickers)
if not companies_list:
    logger.warning("A lista de empresas está vazia após a inicialização.")

# --- Rotas da Aplicação ---
@app.route('/', methods=['GET', 'POST'])
def index():
    try:
        if not companies_list:
            error_msg = "Não foi possível carregar a lista de empresas. Verifique os logs para mais detalhes."
            return render_template('index.html', companies=[], error=error_msg)

        if request.method == 'GET':
            return render_template('index.html', companies=companies_list)

        cvm_code_str = request.form.get('cvm_code')
        start_year_str = request.form.get('start_year')
        end_year_str = request.form.get('end_year')

        if not all([cvm_code_str, start_year_str, end_year_str]):
            return render_template('index.html', companies=companies_list, error="Todos os campos são obrigatórios.")

        try:
            cvm_code = int(cvm_code_str)
            start_year = int(start_year_str)
            end_year = int(end_year_str)
            years_to_analyze = list(range(start_year, end_year + 1))
        except (ValueError, TypeError):
            return render_template('index.html', companies=companies_list, error="Valores inválidos para código CVM ou anos.")

        logger.info(f"A iniciar busca de dados para CVM {cvm_code}")
        
        df_company = pd.DataFrame()
        try:
            with db_engine.connect() as connection:
                query = text('SELECT * FROM financial_data WHERE "CD_CVM" = :cvm_code')
                df_company = pd.read_sql(query, connection, params={'cvm_code': cvm_code})
            
            if df_company.empty:
                error = f"Nenhum dado encontrado no banco para a empresa com CVM {cvm_code}."
                return render_template('index.html', companies=companies_list, error=error)
            
            logger.info(f"Dados carregados do banco: {len(df_company)} registos para CVM {cvm_code}")

        except exc.SQLAlchemyError as e:
            error = "Erro ao aceder ao banco de dados durante a busca de dados da empresa."
            return render_template('index.html', companies=companies_list, error=error)

        logger.info(f"A iniciar análise para CVM {cvm_code} para os anos {years_to_analyze}")
        analysis_results, error = run_multi_year_analysis(df_company, cvm_code, years_to_analyze)
        
        if error:
            return render_template('index.html', companies=companies_list, error=error)
        
        return render_template(
            'index.html', 
            companies=companies_list, 
            results=analysis_results, 
            selected_company=cvm_code_str,
            chart_data_json=json.dumps(analysis_results.get('chart_data', {}))
        )

    except Exception as e:
        error = "Ocorreu um erro crítico na aplicação."
        logger.error(f"{error} Detalhes: {e}", exc_info=True)
        return render_template('500.html', error=error), 500

# --- Handlers de Erro e Headers ---
@app.errorhandler(404)
def page_not_found(e):
    return render_template('404.html'), 404

@app.errorhandler(500)
def internal_server_error(e):
    logger.error(f"Erro interno no servidor: {e}", exc_info=True)
    return render_template('500.html'), 500

@app.after_request
def add_header(response):
    response.headers['X-Content-Type-Options'] = 'nosniff'
    return response

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
