import pandas as pd
import zipfile
import os
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
import time
import logging
from tqdm import tqdm
from urllib.parse import quote_plus
import numpy as np
from typing import Dict, List, Optional
import chardet

# --- CONFIGURAÇÃO ---
class Config:
    """Configurações globais com validação básica"""
    MAX_STRING_LENGTHS = {
        'CNPJ_CIA': 18,
        'DENOM_CIA': 255,
        'CD_CONTA': 50,
        'DS_CONTA': 1000,
        'CD_CVM': 10
    }
    
    DB_CONFIG = {
        'host': 'modelfleuriet.mysql.pythonanywhere-services.com',
        'user': 'modelfleuriet',
        'password': quote_plus('thg312222'),  # URL encoded
        'database': 'modelfleuriet$default',
        'pool_size': 5,
        'max_overflow': 2,
        'pool_timeout': 30,
        'pool_recycle': 3600
    }
    
    VALID_YEARS = ['2022', '2023', '2024']
    CHUNK_SIZE = 1000  # Tamanho dos lotes para inserção
    MAX_RETRIES = 3    # Tentativas de reconexão

# --- LOGGING AVANÇADO ---
def setup_logging():
    """Configura logging estruturado"""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    if not logger.handlers:
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(module)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # File handler com rotação
        file_handler = logging.FileHandler('preprocess.log', encoding='utf-8')
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.INFO)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        console_handler.setLevel(logging.WARNING)
        
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
    
    return logger

logger = setup_logging()

# --- BANCO DE DADOS ---
class DatabaseManager:
    """Gerencia conexões e operações no banco de dados"""
    def __init__(self):
        self.engine = self._create_engine()
        self._test_connection()
    
    def _create_engine(self):
        """Cria engine SQLAlchemy com configurações otimizadas"""
        connection_string = (
            f"mysql+pymysql://{Config.DB_CONFIG['user']}:{Config.DB_CONFIG['password']}"
            f"@{Config.DB_CONFIG['host']}/{Config.DB_CONFIG['database']}"
            "?charset=utf8mb4&connect_timeout=10"
        )
        return create_engine(
            connection_string,
            pool_size=Config.DB_CONFIG['pool_size'],
            max_overflow=Config.DB_CONFIG['max_overflow'],
            pool_timeout=Config.DB_CONFIG['pool_timeout'],
            pool_recycle=Config.DB_CONFIG['pool_recycle'],
            pool_pre_ping=True
        )
    
    def _test_connection(self, retries: int = Config.MAX_RETRIES):
        """Testa a conexão com retry automático"""
        for attempt in range(retries):
            try:
                with self.engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                logger.info("✅ Conexão com o banco estabelecida")
                return True
            except Exception as e:
                logger.warning(f"❌ Tentativa {attempt + 1}/{retries} - Erro na conexão: {str(e)}")
                if attempt == retries - 1:
                    logger.error("Número máximo de tentativas excedido")
                    raise
                time.sleep(2 ** attempt)  # Backoff exponencial
    
    def execute_with_retry(self, query, params=None, retries: int = Config.MAX_RETRIES):
        """Executa query com mecanismo de retry"""
        for attempt in range(retries):
            try:
                with self.engine.begin() as conn:
                    result = conn.execute(text(query), params or {})
                    return result
            except (SQLAlchemyError, ConnectionError) as e:
                logger.warning(f"Tentativa {attempt + 1}/{retries} falhou: {str(e)}")
                if attempt == retries - 1:
                    raise
                time.sleep(1)
                self._test_connection()

# --- PROCESSAMENTO DE DADOS ---
class DataProcessor:
    """Responsável pela transformação e limpeza dos dados"""
    @staticmethod
    def clean_data(df: pd.DataFrame) -> pd.DataFrame:
        """Limpeza e preparação dos dados"""
        # Colunas para remover
        cols_to_drop = ['VERSAO', 'MOEDA', 'ESCALA_MOEDA', 'ORDEM_EXERC']
        df = df.drop(columns=[col for col in cols_to_drop if col in df.columns], errors='ignore')
        
        # Conversão de datas
        date_cols = ['DT_REFER', 'DT_FIM_EXERC', 'DT_INI_EXERC']
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Tratamento especial para CD_CVM
        if 'CD_CVM' in df.columns:
            df['CD_CVM'] = pd.to_numeric(df['CD_CVM'], errors='coerce').astype('Int64')
            if df['CD_CVM'].isnull().any():
                logger.warning("Valores inválidos/nulos encontrados em CD_CVM")
        
        # Tratamento de valores numéricos
        if 'VL_CONTA' in df.columns:
            df['VL_CONTA'] = pd.to_numeric(df['VL_CONTA'], errors='coerce')
            df['VL_CONTA'] = df['VL_CONTA'].replace([np.inf, -np.inf], np.nan)
        
        # Truncamento de strings
        for col, max_len in Config.MAX_STRING_LENGTHS.items():
            if col in df.columns:
                df[col] = df[col].astype(str).str.slice(0, max_len)
        
        return df.drop_duplicates()

    @staticmethod
    def detect_encoding(file_path: str) -> str:
        """Detecta a codificação do arquivo"""
        with open(file_path, 'rb') as f:
            rawdata = f.read(10000)
            return chardet.detect(rawdata)['encoding']

class DataLoader:
    """Carrega dados de diferentes fontes"""
    @staticmethod
    def load_from_zip(zip_path: str, year: str) -> Optional[pd.DataFrame]:
        """Carrega dados de arquivo ZIP"""
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                csv_files = [f for f in zip_ref.namelist() if f.endswith('.csv')]
                
                all_data = []
                for csv_file in tqdm(csv_files, desc=f"Processando {year}"):
                    try:
                        with zip_ref.open(csv_file) as f:
                            df = pd.read_csv(
                                f,
                                sep=';',
                                encoding='latin1',
                                decimal=',',
                                dtype={'CD_CONTA': str, 'CD_CVM': 'Int64'},
                                low_memory=False
                            )
                            all_data.append(df)
                    except Exception as e:
                        logger.error(f"Erro ao ler {csv_file}: {str(e)}")
                        continue
                
                if all_data:
                    return pd.concat(all_data, ignore_index=True)
                return None
                
        except Exception as e:
            logger.error(f"Erro ao processar ZIP {zip_path}: {str(e)}")
            return None

# --- GERENCIAMENTO DE PROCESSO ---
class ETLPipeline:
    """Orquestra todo o processo ETL"""
    def __init__(self):
        self.db = DatabaseManager()
        self.processor = DataProcessor()
        self.loader = DataLoader()
    
    def process_year(self, year: str):
        """Processa um ano específico"""
        zip_file = f'dfp_cia_aberta_{year}.zip'
        
        if not os.path.exists(zip_file):
            logger.warning(f"Arquivo {zip_file} não encontrado")
            return False
        
        logger.info(f"===> INICIANDO PROCESSAMENTO {year} <===")
        start_time = time.time()
        
        try:
            # Extração
            raw_df = self.loader.load_from_zip(zip_file, year)
            if raw_df is None or raw_df.empty:
                logger.error(f"Nenhum dado válido encontrado para {year}")
                return False
            
            # Transformação
            clean_df = self.processor.clean_data(raw_df)
            
            # Carregamento
            self._insert_data(clean_df, year)
            
            elapsed = time.time() - start_time
            logger.info(f"✅ {year} processado em {elapsed:.2f}s")
            return True
            
        except Exception as e:
            logger.error(f"❌ Falha no processamento de {year}: {str(e)}")
            return False
    
    def _insert_data(self, df: pd.DataFrame, year: str):
        """Estratégia de inserção otimizada"""
        try:
            # Tentativa com to_sql (mais rápido)
            df.to_sql(
                name='financial_data',
                con=self.db.engine,
                if_exists='append',
                index=False,
                chunksize=Config.CHUNK_SIZE,
                method='multi'
            )
            logger.info(f"Inserção em lote concluída para {len(df)} registros")
            
        except (SQLAlchemyError, IntegrityError) as e:
            logger.warning(f"Falha na inserção em lote: {str(e)}. Tentando método seguro...")
            self._insert_safely(df, year)
    
    def _insert_safely(self, df: pd.DataFrame, year: str):
        """Inserção linha por linha com tratamento de erros"""
        data = df.replace({np.nan: None, pd.NaT: None}).to_dict('records')
        total = len(data)
        success = 0
        
        query = """
            INSERT INTO financial_data 
            (CNPJ_CIA, CD_CVM, DENOM_CIA, DT_REFER, CD_CONTA, DS_CONTA, VL_CONTA)
            VALUES 
            (:CNPJ_CIA, :CD_CVM, :DENOM_CIA, :DT_REFER, :CD_CONTA, :DS_CONTA, :VL_CONTA)
            ON DUPLICATE KEY UPDATE
            DENOM_CIA = VALUES(DENOM_CIA),
            DS_CONTA = VALUES(DS_CONTA),
            VL_CONTA = VALUES(VL_CONTA)
        """
        
        with tqdm(total=total, desc=f"Inserindo {year}") as pbar:
            for row in data:
                try:
                    self.db.execute_with_retry(query, row)
                    success += 1
                except Exception as e:
                    logger.warning(f"Falha ao inserir registro: {str(e)}")
                finally:
                    pbar.update(1)
        
        logger.info(f"✅ {success}/{total} registros inseridos com sucesso")

def main():
    """Ponto de entrada principal"""
    logger.info("=" * 50)
    logger.info("INICIANDO PROCESSAMENTO")
    logger.info("=" * 50)
    
    try:
        pipeline = ETLPipeline()
        
        for year in Config.VALID_YEARS:
            pipeline.process_year(year)
            
    except Exception as e:
        logger.critical(f"ERRO GLOBAL: {str(e)}", exc_info=True)
    finally:
        logger.info("PROCESSAMENTO CONCLUÍDO")

if __name__ == "__main__":
    main()