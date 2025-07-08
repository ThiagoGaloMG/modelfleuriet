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

# Configuração
load_dotenv()

# Constantes
VALUATION_TABLE_NAME = 'valuation_results'
BATCH_SIZE = 20
MAX_RETRIES = 3

# Logging
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
    """Cria engine de conexão com tratamento robusto de erros"""
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        logger.error("DATABASE_URL não definida.")
        raise ValueError("DATABASE_URL não definida.")

    database_url = database_url.strip()
    if " " in database_url:
        database_url = database_url.split(" ")[0]
    
    if database_url.startswith("postgresql://") and "postgresql+psycopg2://" not in database_url:
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
        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("✅ Engine do banco de dados criada com sucesso.")
        return engine
    except Exception as e:
        logger.error(f"❌ Falha ao criar engine: {e}", exc_info=True)
        raise

def log_memory_usage():
    """Loga uso de memória"""
    mem = psutil.virtual_memory()
    logger.info(
        f"Memória: {mem.used/(1024*1024):.2f}MB/{mem.total/(1024*1024):.2f}MB ({mem.percent}%)"
    )

def create_valuation_table(engine):
    """Cria tabela de resultados com tratamento de erros"""
    inspector = inspect(engine)
    
    if inspector.has_table(VALUATION_TABLE_NAME):
        logger.info(f"Tabela '{VALUATION_TABLE_NAME}' já existe. Recriando...")
        try:
            with engine.connect() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {VALUATION_TABLE_NAME}"))
                conn.commit()
        except Exception as e:
            logger.error(f"Erro ao dropar tabela: {e}")
            raise

    logger.info(f"Criando tabela '{VALUATION_TABLE_NAME}'...")
    try:
        create_query = text(f"""
        CREATE TABLE {VALUATION_TABLE_NAME} (
            "Ticker" VARCHAR(20) PRIMARY KEY,
            "Nome" VARCHAR(255),
            "Upside" NUMERIC, 
            "ROIC" NUMERIC, 
            "WACC" NUMERIC, 
            "Spread" NUMERIC,
            "Preco_Atual" NUMERIC, 
            "Preco_Justo" NUMERIC,
            "Market_Cap" BIGINT, 
            "EVA" NUMERIC,
            "Capital_Empregado" NUMERIC, 
            "NOPAT" NUMERIC,
            "Data_Atualizacao" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        with engine.connect() as conn:
            conn.execute(create_query)
            conn.commit()
        logger.info("✅ Tabela criada com sucesso.")
    except SQLAlchemyError as e:
        logger.error(f"❌ Falha ao criar tabela: {e}")
        raise

def load_ticker_mapping(file_path='mapeamento_tickers.csv'):
    """Carrega mapeamento de tickers com validação"""
    try:
        logger.info(f"Carregando mapeamento de {file_path}...")
        
        df = pd.read_csv(file_path, sep=',', dtype={'CD_CVM': str})
        df.columns = [col.strip().upper() for col in df.columns]
        
        # Validação
        df['CD_CVM'] = pd.to_numeric(df['CD_CVM'], errors='coerce')
        df = df.dropna(subset=['CD_CVM'])
        df['CD_CVM'] = df['CD_CVM'].astype(int)
        df = df.drop_duplicates(subset=['CD_CVM'], keep='first')
        
        logger.info(f"✅ {len(df)} tickers carregados.")
        return df[['CD_CVM', 'TICKER']]
    
    except Exception as e:
        logger.error(f"❌ Erro ao carregar mapeamento: {e}")
        raise

def load_financial_data(engine):
    """Carrega dados financeiros com otimização de memória"""
    logger.info("Carregando dados financeiros...")
    
    cols = ["CD_CVM", "CD_CONTA", "VL_CONTA", "DT_REFER", "DENOM_CIA"]
    
    for attempt in range(MAX_RETRIES):
        try:
            with engine.connect() as conn:
                df = pd.read_sql(
                    f'SELECT {",".join(cols)} FROM financial_data',
                    conn,
                    chunksize=50000
                )
                df = pd.concat(df, ignore_index=True)
            
            logger.info(f"✅ {len(df)} registros carregados.")
            return df
            
        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                raise
            logger.warning(f"Tentativa {attempt+1} falhou. Tentando novamente...")
            continue

def save_results_batch(engine, results_batch):
    """Salva resultados em lote"""
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
        logger.error(f"❌ Erro ao salvar resultados: {e}")
        raise

def main():
    """Função principal com tratamento completo de erros"""
    logger.info("\n" + "="*60)
    logger.info(" INICIANDO WORKER DE VALUATION ")
    logger.info("="*60)
    
    try:
        start_time = datetime.now()
        db_engine = get_db_engine()
        log_memory_usage()

        # Prepara tabela
        create_valuation_table(db_engine)

        # Carrega dados
        df_tickers = load_ticker_mapping()
        df_financial = load_financial_data(db_engine)
        
        if df_financial.empty or df_tickers.empty:
            raise ValueError("Dados insuficientes para análise")

        # Processa valuation
        logger.info(f"Processando {len(df_tickers)} empresas...")
        resultados = run_full_valuation_analysis(df_financial, df_tickers)
        
        if resultados:
            save_results_batch(db_engine, resultados)
            empresas_processadas = len(resultados)
        else:
            empresas_processadas = 0
            logger.warning("Nenhum resultado gerado")
        
        # Estatísticas
        elapsed = (datetime.now() - start_time).total_seconds()
        logger.info("\n" + "="*60)
        logger.info(f"✅ Processadas: {empresas_processadas}/{len(df_tickers)} empresas")
        logger.info(f"✅ Tempo total: {elapsed:.2f}s")
        if empresas_processadas > 0:
            logger.info(f"✅ Média: {elapsed/empresas_processadas:.2f}s/empresa")
        log_memory_usage()
        
    except Exception as e:
        logger.critical(f"❌ ERRO: {e}", exc_info=True)
        raise
    finally:
        if 'db_engine' in locals():
            db_engine.dispose()
        logger.info("Worker finalizado.\n")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Interrompido pelo usuário")
        exit(0)
    except Exception as e:
        logger.critical(f"FALHA: {e}")
        exit(1)
