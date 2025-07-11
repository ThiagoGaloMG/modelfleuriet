#!/usr/bin/env python3
import pandas as pd
import json
from flask import Flask, render_template, request, jsonify, g
from flask.json.provider import DefaultJSONProvider
import os
import numpy as np
from sqlalchemy import create_engine, text, exc, inspect
import logging
from dotenv import load_dotenv
import gc
import psutil
from pathlib import Path
import time

load_dotenv()
app = Flask(__name__)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('app.log')
    ]
)
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

def format_currency(value):
    try:
        return f"R$ {float(value):,.2f}"
    except (ValueError, TypeError):
        return "N/D"

def format_percentage(value):
    try:
        return f"{float(value)*100:.2f}%"
    except (ValueError, TypeError):
        return "N/D"

app.jinja_env.filters['format_currency'] = format_currency
app.jinja_env.filters['format_percentage'] = format_percentage

def log_memory_usage():
    mem = psutil.virtual_memory()
    logger.info(f"Memória: {mem.used/(1024*1024):.2f}MB / {mem.total/(1024*1024):.2f}MB ({mem.percent}%)")

def is_db_connected(engine):
    if engine is None:
        return False
    try:
        with engine.connect() as conn:
            conn.execute(text("select 1"))
        return True
    except Exception as e:
        logger.error(f"Erro conexão banco: {e}")
        return False

def create_db_engine():
    database_url = os.environ.get('database_url')
    if not database_url:
        logger.error("DATABASE_URL não definida.")
        raise ValueError("DATABASE_URL não definida.")
    
    database_url = database_url.strip()
    if database_url.startswith("postgres://"):
        database_url = database_url.replace("postgres://", "postgresql+psycopg2://", 1)
    
    logger.info(f"Conectando: {database_url.split('@')[-1]}")
    
    try:
        engine = create_engine(
            database_url,
            pool_pre_ping=True,
            pool_size=20,
            max_overflow=30,
            pool_recycle=3600,
            connect_args={
                "connect_timeout": 30,
                "keepalives": 1,
                "keepalives_idle": 30,
                "keepalives_interval": 10,
                "keepalives_count": 5
            }
        )
        with engine.connect() as conn:
            conn.execute(text("select 1"))
            logger.info("[OK] Conexão estabelecida.")
            
            inspector = inspect(engine)
            if not inspector.has_table('financial_data'):
                logger.error("[ERRO] Tabela não encontrada")
                return None

            try:
                conn.execute(text("select 1 from financial_data limit 1"))
                logger.info("[OK] Acesso à tabela verificado")
            except Exception as e:
                logger.error(f"[ERRO] Leitura falhou: {str(e)}")
                return None
            
        return engine
    except Exception as e:
        logger.error(f"[ERRO] Falha conexão: {e}", exc_info=True)
        return None

def load_ticker_mapping(file_path=None):
    if file_path is None:
        file_path = os.path.join(os.path.dirname(__file__), 'mapeamento_tickers.csv')
    
    logger.info(f"Carregando: {file_path}...")
    try:
        if not os.path.exists(file_path):
            logger.error(f"Arquivo não encontrado: {file_path}")
            return pd.DataFrame()
        
        df_tickers = pd.read_csv(
            file_path, 
            sep=",",
            dtype={'cd_cvm': str},
            encoding='utf-8'
        )
        
        df_tickers.columns = [col.strip().lower().replace('"', '') for col in df_tickers.columns]
        
        df_tickers['cd_cvm'] = pd.to_numeric(df_tickers['cd_cvm'], errors='coerce')
        df_tickers = df_tickers.dropna(subset=['cd_cvm'])
        df_tickers['cd_cvm'] = df_tickers['cd_cvm'].astype(int)
        df_tickers = df_tickers.drop_duplicates(subset=['cd_cvm'], keep='first')
        
        logger.info(f"[OK] {len(df_tickers)} mapeamentos")
        return df_tickers[['cd_cvm', 'ticker', 'nome_empresa']]
    except Exception as e:
        logger.error(f"[ERRO] Carregamento falhou: {e}", exc_info=True)
        return pd.DataFrame()

def get_companies_list(engine, ticker_mapping_df):
    if not is_db_connected(engine):
        logger.error("Conexão inativa")
        return []
    
    logger.info("Buscando empresas...")
    try:
        with engine.connect() as connection:
            inspector = inspect(engine)
            if not inspector.has_table('financial_data'):
                logger.error("Tabela não existe")
                return []
                
            columns = [col['name'] for col in inspector.get_columns('financial_data')]
            
            required_columns = {'denom_cia', 'cd_cvm'}  
            if not required_columns.issubset(columns):
                missing = required_columns - set(columns)
                logger.error(f"Colunas faltando: {missing}")
                return []
            
            query = text('select distinct denom_cia as nome_empresa, cd_cvm as cd_cvm from financial_data order by denom_cia')
            df_companies_db = pd.read_sql(query, connection)
            
            df_companies_db.columns = [col.lower() for col in df_companies_db.columns]

            if not ticker_mapping_df.empty:
                ticker_subset = ticker_mapping_df[['cd_cvm', 'ticker']]
                
                final_df = pd.merge(
                    df_companies_db,
                    ticker_subset,
                    on='cd_cvm',
                    how='left'
                )
                final_df['ticker'] = final_df['ticker'].fillna(final_df['nome_empresa'])
            else:
                final_df = df_companies_db
                final_df['ticker'] = final_df['nome_empresa']
            
            cols = ['cd_cvm', 'ticker', 'nome_empresa']
            return (
                final_df[cols]
                .dropna(subset=['cd_cvm'])
                .drop_duplicates(subset=['cd_cvm'])
                .to_dict(orient='records')
            )
    except Exception as e:
        logger.error(f"[ERRO] Busca falhou: {e}", exc_info=True)
        return []

def ensure_valuation_table_exists(engine):
    if engine is None:
        logger.error("Engine não disponível")
        return False
    
    try:
        inspector = inspect(engine)
        
        if inspector.has_table('valuation_results'):
            logger.info("Tabela existe.")
            return True
            
        logger.info("Criando tabela...")
        create_query = text("""
            create table if not exists valuation_results (
                id serial primary key,
                ticker varchar(20) not null,
                nome varchar(255),
                upside numeric,
                roic numeric,
                wacc numeric,
                spread numeric,
                preco_atual numeric,
                preco_justo numeric,
                market_cap bigint,
                eva numeric,
                capital_empregado numeric,
                nopat numeric,
                beta numeric,
                data_calculo date,
                data_atualizacao timestamp default current_timestamp,
                constraint unique_ticker_date unique (ticker, data_calculo)
            )
        """)
        
        with engine.begin() as conn:
            conn.execute(create_query)
        
        logger.info("[OK] Tabela criada")
        return True
        
    except Exception as e:
        logger.error(f"[ERRO] Criação falhou: {e}", exc_info=True)
        return False

def run_valuation_worker_if_needed(engine):
    if engine is None:
        return
    
    try:
        with engine.connect() as connection:
            result = connection.execute(text("select count(*) from valuation_results")).fetchone()
            count = result[0] if result else 0
            
        if count == 0:
            logger.info("Tabela vazia. Executando worker...")
            from core.valuation_analysis import run_full_valuation_analysis
            
            df_tickers = load_ticker_mapping()
            if df_tickers.empty:
                logger.error("Nenhum ticker")
                return
                
            with engine.connect() as connection:
                cols = ['cd_cvm', 'cd_conta', 'vl_conta', 'dt_refer', 'denom_cia']
                df_full_data = pd.read_sql(
                    text(f'select {", ".join(cols)} from financial_data'),
                    connection
                )
                
                resultados = run_full_valuation_analysis(df_full_data, df_tickers)
                
                if resultados:
                    df_results = pd.DataFrame(resultados)
                    df_results.to_sql(
                        'valuation_results', 
                        connection, 
                        if_exists='append', 
                        index=False,
                        method='multi'
                    )
                    logger.info(f"Valuation: {len(df_results)} resultados")
                else:
                    logger.warning("Nenhum resultado")
    except Exception as e:
        logger.error(f"Erro worker: {e}", exc_info=True)

def check_db_health(engine):
    try:
        with engine.connect() as conn:
            result = conn.execute(text("select now() as current_time"))
            return result.fetchone()[0] is not None
    except Exception as e:
        logger.critical(f"Falha crítica: {e}")
        return False

@app.before_request
def before_request():
    g.db_engine = create_db_engine()
    if not g.db_engine:
        return jsonify({"error": "Conexão falhou"}), 500
    
    if not check_db_health(g.db_engine):
        return jsonify({"error": "Verificação falhou"}), 503

    ensure_valuation_table_exists(g.db_engine)
    run_valuation_worker_if_needed(g.db_engine)

@app.route('/')
def index():
    ticker_map = load_ticker_mapping()
    companies = get_companies_list(g.db_engine, ticker_map)
    
    try:
        with g.db_engine.connect() as conn:
            df_valuation = pd.read_sql(text("select * from valuation_results order by upside desc"), conn)
        valuation_data = df_valuation.to_dict(orient='records')
    except Exception as e:
        logger.error(f"Erro valuation: {e}")
        valuation_data = []
    
    return render_template('index.html', companies=companies, valuation_data=valuation_data)

@app.route('/analyze', methods=['POST'])
def analyze_company():
    data = request.json
    cvm_code = data.get('cvm_code')
    years = data.get('years', [])
    
    if not cvm_code or not years:
        return jsonify({"error": "Código CVM e anos obrigatórios"}), 400
    
    logger.info(f"Análise: CVM {cvm_code} | Anos {years}")
    
    try:
        with g.db_engine.connect() as conn:
            query = text("select * from financial_data where cd_cvm = :cvm_code")
            df_company = pd.read_sql(query, conn, params={'cvm_code': cvm_code})
            
        if df_company.empty:
            return jsonify({"error": "Dados não encontrados"}), 404
        
        from core.analysis import run_multi_year_analysis
        result, error = run_multi_year_analysis(df_company, cvm_code, years)
        
        if error:
            return jsonify({"error": error}), 500
        
        return jsonify(result)
    except Exception as e:
        logger.error(f"Erro análise: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route('/run_valuation', methods=['POST'])
def run_valuation():
    if not ensure_valuation_table_exists(g.db_engine):
        return jsonify({"error": "Tabela não disponível"}), 500
    
    try:
        with g.db_engine.connect() as connection:
            df_full_data = pd.read_sql(
                text('select cd_cvm, cd_conta, vl_conta, dt_refer, denom_cia from financial_data'),
                connection
            )
            
            ticker_map = load_ticker_mapping()
            if ticker_map.empty:
                return jsonify({"error": "Mapeamento falhou"}), 500
                
            from core.valuation_analysis import run_full_valuation_analysis
            resultados = run_full_valuation_analysis(df_full_data, ticker_map)
            
            if resultados:
                connection.execute(text("truncate table valuation_results restart identity"))
                df_results = pd.DataFrame(resultados)
                df_results.to_sql(
                    'valuation_results', 
                    connection, 
                    if_exists='append', 
                    index=False
                )
                return jsonify({"message": f"Concluído: {len(df_results)} empresas"})
            else:
                return jsonify({"error": "Sem resultados"}), 500
    except Exception as e:
        logger.error(f"Erro valuation: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
