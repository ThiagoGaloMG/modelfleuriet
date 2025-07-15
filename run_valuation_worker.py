#!/usr/bin/env python3
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError
import os
import logging
import gc
import psutil
from dotenv import load_dotenv
from datetime import datetime, timedelta # Importa timedelta

# Adiciona o diretório 'src' ao sys.path para importar os novos módulos
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'src')))

# Importa o sistema de análise de Valuation e o gerenciador de DB
from src.ibovespa_analysis_system import IbovespaAnalysisSystem
from src.database_manager import SupabaseDB
from src.ibovespa_data import get_ibovespa_tickers # Para obter a lista de tickers se o worker for autônomo

# Configuração de ambiente
load_dotenv()

# Constantes
VALUATION_TABLE_NAME = 'valuation_results' # Esta tabela será substituída pelas tabelas do Supabase
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

# Instância global do sistema de análise e DB manager para o worker
ibovespa_analysis_system_worker = None
supabase_db_worker = None

def get_worker_components():
    global ibovespa_analysis_system_worker, supabase_db_worker
    if ibovespa_analysis_system_worker is None:
        ibovespa_analysis_system_worker = IbovespaAnalysisSystem()
        supabase_db_worker = SupabaseDB()
        logger.info("Worker components initialized.")
    return ibovespa_analysis_system_worker, supabase_db_worker

def log_memory_usage():
    """Exibe o uso de memória RAM"""
    mem = psutil.virtual_memory()
    logger.info(
        f"Memória: {mem.used / (1024 * 1024):.2f}MB / {mem.total / (1024 * 1024):.2f}MB ({mem.percent}%)"
    )

# Esta função será o ponto de entrada principal do worker
def run_valuation_worker_main():
    logger.info("\n" + "=" * 60)
    logger.info(" INICIANDO WORKER DE VALUATION (MODIFIED) ")
    logger.info("=" * 60)
    
    try:
        start_time = datetime.now()
        log_memory_usage()

        system, db_manager = get_worker_components()

        if not system or not db_manager:
            raise Exception("Sistema de análise ou DB manager não inicializados para o worker.")

        # O worker agora simplesmente aciona a análise completa do sistema
        # A lógica de salvar no DB já está dentro de system.run_complete_analysis()
        logger.info("Acionando análise completa do Ibovespa para o worker...")
        report = system.run_complete_analysis(num_companies=None) # Roda para todas as empresas

        if report and report.get('status') == 'success':
            logger.info(f"✅ Worker concluído. Análise completa gerou {report.get('total_companies_analyzed')} resultados e salvou no DB.")
        else:
            logger.warning("Worker de valuation não gerou resultados ou a análise falhou.")
            
        elapsed = (datetime.now() - start_time).total_seconds()
        logger.info(f"✅ Concluído em {elapsed:.2f} segundos.")
        log_memory_usage()

    except Exception as e:
        logger.critical(f"❌ ERRO NO WORKER: {e}", exc_info=True)
    finally:
        logger.info("Worker finalizado.\n")

if __name__ == "__main__":
    # Este é o ponto de entrada se o worker for rodado como um script separado
    run_valuation_worker_main()
