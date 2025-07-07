import pandas as pd
import json
from flask import Flask, render_template, request, jsonify
from flask.json.provider import DefaultJSONProvider
from core.analysis import run_multi_year_analysis
import os
import numpy as np
from sqlalchemy import create_engine, text, exc
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
    conn_str = database_url.replace("postgresql://", "postgresql+psycopg2://", 1)
    try:
        engine = create_engine(conn_str, pool_pre_ping=True)
        with engine.connect(): logger.info("Conexão com o banco de dados estabelecida.")
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
    if engine is None: return []
    logger.info("Buscando lista de empresas do banco...")
    try:
        with engine.connect() as connection:
            query = text('SELECT DISTINCT "DENOM_CIA", "CD_CVM" FROM financial_data ORDER BY "DENOM_CIA";')
            df_companies_db = pd.read_sql(query, connection)
        df_companies_db.rename(columns={'DENOM_CIA': 'NOME_EMPRESA'}, inplace=True)
        final_df = pd.merge(df_companies_db, ticker_mapping_df, on='CD_CVM', how='left')
        final_df['TICKER'] = final_df['TICKER'].fillna('S/TICKER')
        logger.info(f"{len(final_df)} empresas encontradas e mapeadas.")
        return final_df.to_dict(orient='records')
    except Exception as e:
        logger.error(f"Erro ao buscar lista de empresas: {e}", exc_info=True)
        return []

db_engine = create_db_engine()
df_tickers = load_ticker_mapping()
companies_list = get_companies_list(db_engine, df_tickers)

@app.route('/', methods=['GET', 'POST'])
def index():
    try:
        if request.method == 'GET':
            return render_template('index.html', companies=companies_list)

        cvm_code = request.form.get('cvm_code')
        if not cvm_code:
            return render_template('index.html', companies=companies_list, error="Por favor, selecione uma empresa antes de enviar o formulário.")
        cvm_code = int(cvm_code)
        start_year = int(request.form.get('start_year'))
        end_year = int(request.form.get('end_year'))
        years_to_analyze = list(range(start_year, end_year + 1))

        with db_engine.connect() as connection:
            query = text('SELECT * FROM financial_data WHERE "CD_CVM" = :cvm_code')
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
        with db_engine.connect() as connection:
            df_valuation = pd.read_sql_table('valuation_results', connection)
        if df_valuation.empty:
            return jsonify({"error": "A análise de valuation ainda não foi concluída pelo worker. Por favor, aguarde alguns minutos e tente novamente."}), 404
        return jsonify(df_valuation.to_dict(orient='records'))
    except Exception as e:
        logger.error(f"Erro ao buscar dados de valuation: {e}", exc_info=True)
        return jsonify({"error": "Falha ao acessar os resultados da análise de valuation."}), 500

@app.errorhandler(404)
def page_not_found(e): return render_template('404.html'), 404
@app.errorhandler(500)
def internal_server_error(e): return render_template('500.html'), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 10000)))
