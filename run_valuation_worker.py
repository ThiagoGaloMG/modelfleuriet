#!/usr/bin/env python3
import os
import logging
from datetime import datetime
import sys

# Adiciona o diretório 'core' ao sys.path para importar os módulos
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'core')))

# Importa o sistema de análise de Valuation (o novo principal)
from core.ibovespa_analysis_system import IbovespaAnalysisSystem
from core.db_manager import SupabaseDB # Mantendo o nome SupabaseDB
from core.utils import PerformanceMonitor # Importa o PerformanceMonitor
from core.ibovespa_utils import get_ibovespa_tickers # Para obter a lista de tickers se o worker for autônomo

logger = logging.getLogger(__name__)

# Instância global do sistema de análise e DB manager para este worker
_system_instance = None
_db_manager_instance = None
_ticker_mapping_df = None

def _get_worker_components():
    global _system_instance, _db_manager_instance, _ticker_mapping_df
    if _system_instance is None:
        _db_manager_instance = SupabaseDB() # Usa o DB do Render
        
        # Carrega o mapeamento de tickers (o worker também precisa dele)
        file_path = os.path.join(os.path.dirname(__file__), 'mapeamento_tickers.csv') # Assumindo que está na raiz
        try:
            ticker_mapping_df = pd.read_csv(file_path, sep=',')
            ticker_mapping_df.columns = [col.strip().upper() for col in ticker_mapping_df.columns]
            ticker_mapping_df['CD_CVM'] = pd.to_numeric(ticker_mapping_df['CD_CVM'], errors='coerce').dropna().astype(int)
            ticker_mapping_df = ticker_mapping_df[['CD_CVM', 'TICKER', 'NOME_EMPRESA']].drop_duplicates(subset=['CD_CVM'])
            logger.info(f"{len(ticker_mapping_df)} mapeamentos carregados para o worker.")
        except Exception as e:
            logger.error(f"Erro ao carregar mapeamento de tickers para o worker: {e}", exc_info=True)
            ticker_mapping_df = pd.DataFrame()
            
        _system_instance = IbovespaAnalysisSystem(db_manager=_db_manager_instance, ticker_mapping_df=ticker_mapping_df)
        logger.info("Worker components initialized.")
    return _system_instance, _db_manager_instance

def run_valuation_worker_main():
    logger.info("\n" + "=" * 60)
    logger.info(" INICIANDO WORKER DE VALUATION (ACIONANDO ANÁLISE COMPLETA) ")
    logger.info("=" * 60)
    
    try:
        start_time = datetime.now()
        
        system, db_manager = _get_worker_components()

        if not system or not db_manager:
            raise Exception("Sistema de análise ou DB manager não inicializados para o worker.")

        logger.info("Acionando análise completa do Ibovespa para o worker...")
        # Força a re-coleta de dados do DB
        report = system.run_complete_analysis(num_companies=None, force_recollect=True)

        if report and report.get('status') == 'success':
            logger.info(f"✅ Worker concluído. Análise completa gerou {report.get('total_companies_analyzed')} resultados e salvou no DB.")
        else:
            logger.error("Falha na execução da análise completa de Valuation.")
            
        elapsed = (datetime.now() - start_time).total_seconds()
        logger.info(f"✅ Concluído em {elapsed:.2f} segundos.")

    except Exception as e:
        logger.critical(f"❌ ERRO NO WORKER: {e}", exc_info=True)
    finally:
        logger.info("Worker finalizado.\n")

if __name__ == "__main__":
    run_valuation_worker_main()
