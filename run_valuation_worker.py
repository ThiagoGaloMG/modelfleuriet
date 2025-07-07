import pandas as pd
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError
import os
import logging
from dotenv import load_dotenv
from core.valuation_analysis import run_full_valuation_analysis
from datetime import datetime

# --- Configuração ---
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - WORKER - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DB_URL = os.environ.get('DATABASE_URL')
if not DB_URL:
    logger.error("DATABASE_URL não definida.")
    exit(1)

CONN_STR = DB_URL.replace("postgresql://", "postgresql+psycopg2://", 1)
DB_ENGINE = create_engine(CONN_STR)
VALUATION_TABLE_NAME = 'valuation_results'
TICKER_MAPPING_PATH = 'data/mapeamento_tickers.csv'

def create_valuation_table():
    """Cria a tabela para armazenar os resultados do valuation, se não existir."""
    inspector = inspect(DB_ENGINE)
    if inspector.has_table(VALUATION_TABLE_NAME):
        logger.info(f"Tabela '{VALUATION_TABLE_NAME}' já existe.")
        return

    logger.info(f"Criando tabela '{VALUATION_TABLE_NAME}'...")
    create_query = text(f"""
    CREATE TABLE {VALUATION_TABLE_NAME} (
        "Ticker" VARCHAR(20) PRIMARY KEY,
        "Nome" VARCHAR(255),
        "Upside" NUMERIC, "ROIC" NUMERIC, "WACC" NUMERIC, "Spread" NUMERIC,
        "Preco_Atual" NUMERIC, "Preco_Justo" NUMERIC,
        "Market_Cap" BIGINT, "EVA" NUMERIC,
        "Capital_Empregado" NUMERIC, "NOPAT" NUMERIC
    );
    """)
    try:
        with DB_ENGINE.connect() as connection:
            connection.execute(create_query)
            connection.commit()
        logger.info("Tabela de valuation criada com sucesso.")
    except SQLAlchemyError as e:
        logger.error(f"Falha ao criar tabela de valuation: {e}")
        raise

def load_ticker_mapping(file_path=TICKER_MAPPING_PATH):
    """Carrega o mapeamento de tickers de um arquivo CSV."""
    try:
        df_tickers = pd.read_csv(file_path, sep=',')
        df_tickers.columns = [col.strip().upper() for col in df_tickers.columns]
        df_tickers['CD_CVM'] = pd.to_numeric(df_tickers['CD_CVM'], errors='coerce').dropna().astype(int)
        df_tickers = df_tickers[['CD_CVM', 'TICKER']].drop_duplicates(subset=['CD_CVM'])
        logger.info(f"{len(df_tickers)} mapeamentos carregados de {file_path}.")
        return df_tickers
    except Exception as e:
        logger.error(f"Erro ao carregar mapeamento de tickers: {e}")
        raise

def main():
    """Função principal do worker."""
    exec_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"--- INICIANDO WORKER DE ANÁLISE DE VALUATION às {exec_time} ---")
    
    try:
        # 1. Garante que a tabela de resultados exista
        create_valuation_table()

        # 2. Carrega o mapeamento de tickers do CSV
        df_tickers = load_ticker_mapping()

        # 3. Carrega todos os dados financeiros do banco
        logger.info("Carregando todos os dados financeiros do banco...")
        with DB_ENGINE.connect() as connection:
            df_full_data = pd.read_sql('SELECT * FROM financial_data', connection)
        
        if df_full_data.empty or df_tickers.empty:
            logger.error("Dados financeiros ou mapeamento de tickers não encontrados. Abortando.")
            return

        # 4. Executa a análise de valuation completa
        logger.info("Executando a análise de valuation para todas as empresas...")
        valuation_results = run_full_valuation_analysis(df_full_data, df_tickers)
        
        if not valuation_results:
            logger.warning("Nenhum resultado de valuation foi gerado.")
            return

        # 5. Salva os resultados no banco de dados
        df_results = pd.DataFrame(valuation_results)
        logger.info(f"Salvando {len(df_results)} resultados de valuation na tabela '{VALUATION_TABLE_NAME}'...")
        
        # Usando 'replace' para sempre ter os dados mais atualizados
        df_results.to_sql(
            VALUATION_TABLE_NAME,
            DB_ENGINE,
            if_exists='replace',
            index=False,
            chunksize=100
        )
        
        logger.info("--- WORKER DE ANÁLISE DE VALUATION CONCLUÍDO COM SUCESSO ---")

    except Exception as e:
        logger.critical(f"Erro crítico no worker de valuation: {e}", exc_info=True)

if __name__ == "__main__":
    main()
