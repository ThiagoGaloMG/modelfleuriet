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

# --- Configuração ---
load_dotenv()

# CORRIGIDO: Definir constantes no início
VALUATION_TABLE_NAME = 'valuation_results'
BATCH_SIZE = 20  # Número de empresas processadas por lote
MAX_RETRIES = 3  # Tentativas de reconexão ao banco

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
    """Cria e retorna a engine de conexão com o banco de dados."""
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        logger.error("DATABASE_URL não definida.")
        raise ValueError("DATABASE_URL não definida.")

    # Corrige a URL de conexão se necessário
    if "postgresql://" in database_url and "postgresql+psycopg2://" not in database_url:
        database_url = database_url.replace("postgresql://", "postgresql+psycopg2://")
    
    # Remove possíveis caracteres inválidos
    database_url = database_url.split(" ")[0].strip()
    
    try:
        engine = create_engine(
            database_url,
            pool_pre_ping=True,
            pool_recycle=3600,
            connect_args={"connect_timeout": 30}
        )
        logger.info("✅ Engine do banco de dados criada com sucesso.")
        return engine
    except Exception as e:
        logger.error(f"❌ Falha ao criar a engine do banco de dados: {e}")
        raise

def log_memory_usage():
    """Registra o uso atual de memória."""
    mem = psutil.virtual_memory()
    logger.info(
        f"Uso de memória: {mem.used/(1024*1024):.2f}MB / {mem.total/(1024*1024):.2f}MB "
        f"({mem.percent}%) - Swap: {mem.swap}%"
    )

def create_valuation_table(engine):
    """Cria a tabela para armazenar os resultados do valuation."""
    inspector = inspect(engine)
    
    # CORRIGIDO: Usar a constante definida
    if inspector.has_table(VALUATION_TABLE_NAME):
        logger.info(f"Tabela '{VALUATION_TABLE_NAME}' já existe. Recriando...")
        try:
            with engine.connect() as connection:
                connection.execute(text(f"DROP TABLE IF EXISTS {VALUATION_TABLE_NAME}"))
                connection.commit()
        except Exception as e:
            logger.error(f"Erro ao dropar tabela existente: {e}")
            raise

logger.info(f"Criando tabela '{VALUATION_TABLE_NAME}'...")
create_query = text(f"""
    CREATE TABLE IF NOT EXISTS {VALUATION_TABLE_NAME} (
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
    
    try:
        with engine.connect() as connection:
            connection.execute(create_query)
            connection.commit()
        logger.info("✅ Tabela de valuation criada com sucesso.")
    except SQLAlchemyError as e:
        logger.error(f"❌ Falha ao criar tabela de valuation: {e}")
        raise

def load_ticker_mapping(file_path='mapeamento_tickers.csv'):  # CORRIGIDO: removido 'data/'
    """Carrega e valida o mapeamento de tickers."""
    try:
        logger.info(f"Carregando mapeamento de tickers de {file_path}...")
        
        df_tickers = pd.read_csv(file_path, sep=',', dtype={'CD_CVM': str})
        df_tickers.columns = [col.strip().upper() for col in df_tickers.columns]
        
        # Validação e limpeza
        df_tickers['CD_CVM'] = pd.to_numeric(df_tickers['CD_CVM'], errors='coerce')
        df_tickers = df_tickers.dropna(subset=['CD_CVM'])
        df_tickers['CD_CVM'] = df_tickers['CD_CVM'].astype(int)
        
        # Remove duplicatas mantendo a primeira ocorrência
        df_tickers = df_tickers.drop_duplicates(subset=['CD_CVM'], keep='first')
        
        logger.info(f"✅ {len(df_tickers)} mapeamentos carregados.")
        return df_tickers[['CD_CVM', 'TICKER']]
    
    except Exception as e:
        logger.error(f"❌ Erro ao carregar mapeamento de tickers: {e}")
        raise

def load_financial_data(engine):
    """Carrega dados financeiros otimizando uso de memória."""
    logger.info("Carregando dados financeiros do banco...")
    
    # Seleciona apenas colunas necessárias
    cols = ["CD_CVM", "CD_CONTA", "VL_CONTA", "DT_REFER", "DENOM_CIA"]
    
    for attempt in range(MAX_RETRIES):
        try:
            with engine.connect() as connection:
                df = pd.read_sql(
                    f'SELECT {",".join(cols)} FROM financial_data',
                    connection,
                    chunksize=50000  # Carrega em blocos para economizar memória
                )
                # Concatena os chunks em um único DataFrame
                df = pd.concat(df, ignore_index=True)
            
            logger.info(f"✅ {len(df)} registros financeiros carregados.")
            return df
            
        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                raise
            logger.warning(f"Tentativa {attempt + 1} falhou. Tentando novamente...")
            continue

def save_results_batch(engine, results_batch):
    """Salva um lote de resultados no banco de dados."""
    if not results_batch:
        return
    
    try:
        df_batch = pd.DataFrame(results_batch)
        df_batch.to_sql(
            VALUATION_TABLE_NAME,  # CORRIGIDO: usar constante
            engine,
            if_exists='append',
            index=False,
            chunksize=100,
            method='multi'
        )
        logger.info(f"✅ Lote de {len(df_batch)} resultados salvo.")
    except Exception as e:
        logger.error(f"❌ Erro ao salvar lote de resultados: {e}")
        raise

def main():
    """Função principal do worker com processamento otimizado."""
    logger.info("\n" + "="*60)
    logger.info(" INICIANDO WORKER DE ANÁLISE DE VALUATION ")
    logger.info("="*60)
    
    try:
        # 1. Configuração inicial
        start_time = datetime.now()
        db_engine = get_db_engine()
        log_memory_usage()

        # 2. Prepara a tabela de resultados
        create_valuation_table(db_engine)

        # 3. Carrega dados necessários
        df_tickers = load_ticker_mapping()
        df_full_data = load_financial_data(db_engine)
        
        if df_full_data.empty or df_tickers.empty:
            raise ValueError("Dados insuficientes para análise")

        # 4. CORRIGIDO: Executa análise usando a função original corretamente
        logger.info(f"Iniciando análise de valuation para {len(df_tickers)} empresas...")
        
        # Usa a função original que processa todas as empresas de uma vez
        resultados = run_full_valuation_analysis(df_full_data, df_tickers)
        
        if resultados:
            # Salva todos os resultados
            save_results_batch(db_engine, resultados)
            empresas_processadas = len(resultados)
        else:
            empresas_processadas = 0
            logger.warning("Nenhum resultado gerado pela análise")
        
        # Estatísticas finais
        elapsed = (datetime.now() - start_time).total_seconds()
        logger.info("\n" + "="*60)
        logger.info(f" PROCESSAMENTO CONCLUÍDO ".center(60))
        logger.info("="*60)
        logger.info(f"✅ Empresas processadas: {empresas_processadas}/{len(df_tickers)}")
        logger.info(f"✅ Tempo total: {elapsed:.2f} segundos")
        if empresas_processadas > 0:
            logger.info(f"✅ Média: {elapsed/empresas_processadas:.2f} s/empresa")
        log_memory_usage()
        
    except Exception as e:
        logger.critical(f"❌ ERRO CRÍTICO: {str(e)}", exc_info=True)
        raise
    finally:
        if 'db_engine' in locals():
            db_engine.dispose()
        logger.info("Worker finalizado.\n")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Processo interrompido pelo usuário")
        exit(0)
    except Exception as e:
        logger.critical(f"Falha catastrófica: {e}")
        exit(1)
