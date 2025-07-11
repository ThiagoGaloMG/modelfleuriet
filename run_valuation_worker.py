#!/usr/bin/env python3
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError
import os
import logging
import gc
import psutil
from dotenv import load_dotenv
from core.valuation_analysis import run_full_valuation_analysis
from datetime import datetime

# Configuração de ambiente
load_dotenv()

# Constantes
VALUATION_TABLE_NAME = 'valuation_results'
BATCH_SIZE = 20
MAX_RETRIES = 3

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - WORKER - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('valuation_worker.log')
    ]
)
logger = logging.getLogger(__name__)

def get_db_engine():
    """Cria engine de conexão com o banco de dados PostgreSQL"""
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        logger.error("DATABASE_URL não definida.")
        raise ValueError("DATABASE_URL não definida.")

    # Corrige string de conexão para psycopg2
    database_url = database_url.strip()
    if database_url.startswith("postgres://"):
        database_url = database_url.replace("postgres://", "postgresql+psycopg2://", 1)
    elif database_url.startswith("postgresql://") and "+psycopg2" not in database_url:
        database_url = database_url.replace("postgresql://", "postgresql+psycopg2://", 1)

    try:
        engine = create_engine(
            database_url,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
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
            conn.execute(text("SELECT 1"))
        logger.info("✅ Engine do banco de dados criada com sucesso.")
        return engine
    except Exception as e:
        logger.error(f"❌ Falha ao criar engine: {e}", exc_info=True)
        raise

def log_memory_usage():
    """Exibe o uso de memória RAM"""
    mem = psutil.virtual_memory()
    logger.info(
        f"Memória: {mem.used / (1024 * 1024):.2f}MB / {mem.total / (1024 * 1024):.2f}MB ({mem.percent}%)"
    )

def create_valuation_table(engine):
    """Cria a tabela valuation_results (sobrescreve se já existir)"""
    inspector = inspect(engine)

    if inspector.has_table(VALUATION_TABLE_NAME):
        logger.info(f"Tabela '{VALUATION_TABLE_NAME}' já existe. Recriando...")
        try:
            with engine.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {VALUATION_TABLE_NAME}"))
        except Exception as e:
            logger.error(f"Erro ao dropar tabela: {e}", exc_info=True)
            raise

    logger.info(f"Criando tabela '{VALUATION_TABLE_NAME}'...")
    try:
        with engine.begin() as conn:
            conn.execute(text(f"""
                CREATE TABLE {VALUATION_TABLE_NAME} (
                    ticker VARCHAR(20) PRIMARY KEY,
                    nome VARCHAR(255),
                    cd_cvm INT,
                    upside NUMERIC,
                    roic NUMERIC,
                    wacc NUMERIC,
                    spread NUMERIC,
                    preco_atual NUMERIC,
                    preco_justo NUMERIC,
                    market_cap BIGINT,
                    eva NUMERIC,
                    capital_empregado NUMERIC,
                    nopat NUMERIC,
                    beta NUMERIC,
                    data_calculo DATE,
                    data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
        logger.info("✅ Tabela criada com sucesso.")
    except SQLAlchemyError as e:
        logger.error(f"❌ Falha ao criar tabela: {e}")
        raise

def load_ticker_mapping(file_path='mapeamento_tickers.csv'):
    """Carrega e valida o mapeamento de tickers"""
    try:
        logger.info(f"Carregando mapeamento de {file_path}...")
        df = pd.read_csv(file_path, dtype=str)
        df.columns = [col.strip().lower().replace('"', '') for col in df.columns]
        df['cd_cvm'] = pd.to_numeric(df['cd_cvm'], errors='coerce').astype('Int64')
        df = df.dropna(subset=['cd_cvm'])
        df = df.drop_duplicates(subset=['cd_cvm'])
        logger.info(f"✅ {len(df)} tickers carregados.")
        return df[['cd_cvm', 'ticker', 'nome_empresa']]
    except Exception as e:
        logger.error(f"❌ Erro ao carregar mapeamento: {e}")
        raise

def load_financial_data(engine):
    """Carrega os dados da tabela financial_data"""
    logger.info("Carregando dados financeiros...")
    inspector = inspect(engine)
    if not inspector.has_table("financial_data"):
        logger.error("Tabela 'financial_data' não existe.")
        return pd.DataFrame()

    query = text('SELECT cd_cvm, cd_conta, vl_conta, dt_refer, st_conta FROM financial_data')

    for attempt in range(MAX_RETRIES):
        try:
            with engine.connect() as conn:
                df = pd.read_sql(query, conn)
                logger.info(f"✅ {len(df)} registros carregados.")
                return df
        except Exception as e:
            logger.warning(f"Tentativa {attempt + 1} falhou: {e}")
    logger.critical("❌ Falha ao carregar dados financeiros após várias tentativas.")
    return pd.DataFrame()

def save_results_batch(engine, results_batch):
    """Salva os resultados em lote na tabela valuation_results"""
    if not results_batch:
        return
    try:
        df = pd.DataFrame(results_batch)
        df.to_sql(
            VALUATION_TABLE_NAME,
            engine,
            if_exists='append',
            index=False,
            chunksize=100,
            method='multi'
        )
        logger.info(f"✅ {len(df)} resultados salvos.")
    except Exception as e:
        logger.error(f"❌ Erro ao salvar resultados: {e}", exc_info=True)

def main():
    logger.info("\n" + "=" * 60)
    logger.info(" INICIANDO WORKER DE VALUATION ")
    logger.info("=" * 60)
    
    try:
        start = datetime.now()
        engine = get_db_engine()
        log_memory_usage()
        create_valuation_table(engine)
        df_tickers = load_ticker_mapping()
        df_financial = load_financial_data(engine)

        if df_financial.empty or df_tickers.empty:
            raise ValueError("Dados financeiros ou mapeamento de tickers vazios.")

        logger.info("Iniciando análise de valuation...")
        resultados = run_full_valuation_analysis(df_financial, df_tickers)

        if resultados:
            save_results_batch(engine, resultados)
        else:
            logger.warning("Nenhum resultado gerado pelo valuation.")

        elapsed = (datetime.now() - start).total_seconds()
        logger.info(f"✅ Concluído em {elapsed:.2f} segundos.")
        log_memory_usage()

    except Exception as e:
        logger.critical(f"❌ ERRO: {e}", exc_info=True)
    finally:
        if 'engine' in locals():
            engine.dispose()
        logger.info("Worker finalizado.\n")

if __name__ == "__main__":
    main()
