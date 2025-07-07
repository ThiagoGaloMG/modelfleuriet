import pandas as pd
import json
from flask import Flask, render_template, request, jsonify
from flask.json.provider import DefaultJSONProvider
from core.analysis import run_multi_year_analysis
from core.valuation_analysis import run_full_valuation_analysis # <-- NOVO: Importa a análise de valuation
import os
import numpy as np
from sqlalchemy import create_engine, text, exc
import logging
from dotenv import load_dotenv

# Carrega variáveis de ambiente
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
        if pd.isna(obj): return None # <-- NOVO: Converte NaN do Pandas para None
        return super().default(obj)

app.json = CustomJSONProvider(app)

# --- Configuração do Banco de Dados ---
def create_db_engine():
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

# --- Carregamento de Dados Iniciais ---
def load_ticker_mapping(file_path='data/mapeamento_tickers.csv'):
    logger.info(f"A carregar mapeamento de tickers de {file_path}...")
    try:
        df_tickers = pd.read_csv(file_path, sep=',', encoding='utf-8')
        df_tickers.columns = [col.strip().upper() for col in df_tickers.columns]
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
    if engine is None: return []
    logger.info("A buscar lista de empresas do banco de dados...")
    try:
        with engine.connect() as connection:
            query = text('SELECT DISTINCT "DENOM_CIA", "CD_CVM" FROM financial_data ORDER BY "DENOM_CIA";')
            df_companies_db = pd.read_sql(query, connection)
        
        df_companies_db.rename(columns={'DENOM_CIA': 'NOME_EMPRESA'}, inplace=True)
        final_df = pd.merge(df_companies_db, ticker_mapping_df, on='CD_CVM', how='left')
        final_df['TICKER'].fillna('S/TICKER', inplace=True)
        logger.info(f"{len(final_df)} empresas únicas encontradas e mapeadas.")
        return final_df.to_dict(orient='records')
    except Exception as e:
        logger.error(f"Erro ao buscar lista de empresas: {e}", exc_info=True)
        return []

# --- Inicialização da Aplicação ---
db_engine = create_db_engine()
df_tickers = load_ticker_mapping()
companies_list = get_companies_list(db_engine, df_tickers)
# Cache para os dados de valuation
valuation_cache = None

# --- ROTAS ---

@app.route('/', methods=['GET', 'POST'])
def index():
    try:
        if request.method == 'GET':
            return render_template('index.html', companies=companies_list, fleuriet_results=None, valuation_results=None)

        # Lógica POST para análise Fleuriet
        cvm_code_str = request.form.get('cvm_code')
        start_year_str = request.form.get('start_year')
        end_year_str = request.form.get('end_year')

        if not all([cvm_code_str, start_year_str, end_year_str]):
            return render_template('index.html', companies=companies_list, error="Todos os campos são obrigatórios.")

        cvm_code = int(cvm_code_str)
        years_to_analyze = list(range(int(start_year_str), int(end_year_str) + 1))

        # 1. Puxa todos os dados da empresa do banco
        with db_engine.connect() as connection:
            query = text('SELECT * FROM financial_data WHERE "CD_CVM" = :cvm_code')
            df_company = pd.read_sql(query, connection, params={'cvm_code': cvm_code})

        if df_company.empty:
            return render_template('index.html', companies=companies_list, error=f"Nenhum dado encontrado para a empresa CVM {cvm_code}.")

        # 2. Roda a análise Fleuriet
        logger.info(f"Iniciando análise Fleuriet para CVM {cvm_code}")
        fleuriet_results, fleuriet_error = run_multi_year_analysis(df_company, cvm_code, years_to_analyze)
        if fleuriet_error:
            return render_template('index.html', companies=companies_list, error=fleuriet_error)

        # O resultado final agora contém a análise Fleuriet. A análise de Valuation será carregada via JavaScript.
        return render_template(
            'index.html', 
            companies=companies_list, 
            fleuriet_results=fleuriet_results,
            valuation_results=None, # Valuation será carregado dinamicamente
            selected_company=cvm_code_str
        )

    except Exception as e:
        error_msg = "Ocorreu um erro crítico na aplicação."
        logger.error(f"{error_msg} Detalhes: {e}", exc_info=True)
        return render_template('500.html', error=error_msg), 500

@app.route('/get_valuation_data', methods=['GET'])
def get_valuation_data():
    """
    NOVA ROTA: Rota dedicada para a análise de valuation.
    Ela é chamada via JavaScript para não bloquear o carregamento inicial da página.
    """
    global valuation_cache
    if valuation_cache is not None:
        logger.info("Retornando dados de valuation do cache.")
        return jsonify(valuation_cache)
        
    logger.info("Cache de valuation vazio. Executando a análise completa...")
    try:
        with db_engine.connect() as connection:
            # Carrega todos os dados necessários de uma vez
            query = text('SELECT "CD_CVM", "DENOM_CIA", "DT_REFER", "ORDEM_EXERC", "CD_CONTA", "VL_CONTA" FROM financial_data')
            df_full_data = pd.read_sql(query, connection)

        if df_full_data.empty:
            return jsonify({"error": "Nenhum dado financeiro encontrado no banco de dados."}), 500

        valuation_results = run_full_valuation_analysis(df_full_data, df_tickers)
        valuation_cache = valuation_results # Armazena em cache
        
        logger.info("Análise de valuation concluída e armazenada em cache.")
        return jsonify(valuation_results)

    except Exception as e:
        logger.error(f"Erro crítico na rota de valuation: {e}", exc_info=True)
        return jsonify({"error": "Falha ao executar a análise de valuation."}), 500

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
