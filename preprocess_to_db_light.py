import pandas as pd
import zipfile
import os
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
import time
import logging
from tqdm import tqdm
import numpy as np
from typing import Optional
from dotenv import load_dotenv
import gc
from psycopg2.extras import execute_batch

# Carrega variáveis de ambiente
load_dotenv()

class Config:
    VALID_YEARS = ['2022', '2023', '2024']
    CHUNK_SIZE = 2000
    TABLE_NAME = 'financial_data'
    BATCH_SIZE = 1000  # Tamanho do lote para inserção

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[logging.StreamHandler()]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

class DatabaseManager:
    def __init__(self):
        self.engine = self._create_engine()

    def _create_engine(self):
        database_url = os.getenv('DATABASE_URL')
        if not database_url:
            logger.error("DATABASE_URL não definida.")
            raise ValueError("DATABASE_URL não definida.")
        
        database_url = database_url.strip()
        if " " in database_url:
            database_url = database_url.split(" ")[0]
        
        if database_url.startswith("postgresql://"):
            database_url = database_url.replace("postgresql://", "postgresql+psycopg2://", 1)
        
        try:
            engine = create_engine(
                database_url,
                pool_pre_ping=True,
                pool_size=5,
                max_overflow=10,
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
            logger.error(f"❌ Falha ao criar a engine do banco de dados: {e}", exc_info=True)
            raise

    def create_table_if_not_exists(self):
        inspector = inspect(self.engine)
        if inspector.has_table(Config.TABLE_NAME):
            logger.info(f"Tabela '{Config.TABLE_NAME}' já existe. Recriando...")
            try:
                with self.engine.connect() as connection:
                    connection.execute(text(f"DROP TABLE IF EXISTS {Config.TABLE_NAME}"))
                    connection.commit()
            except Exception as e:
                logger.error(f"Erro ao dropar tabela existente: {e}")
                raise

        logger.info(f"Criando tabela '{Config.TABLE_NAME}'...")
        
        create_table_query = text(f"""
        CREATE TABLE {Config.TABLE_NAME} (
            "CNPJ_CIA" VARCHAR(20),
            "DT_REFER" DATE,
            "VERSAO" INTEGER,
            "DENOM_CIA" VARCHAR(255),
            "CD_CVM" INTEGER,
            "GRUPO_DFP" VARCHAR(255),
            "MOEDA" VARCHAR(10),
            "ESCALA_MOEDA" VARCHAR(20),
            "ORDEM_EXERC" VARCHAR(20),
            "DT_FIM_EXERC" DATE,
            "CD_CONTA" VARCHAR(50),
            "DS_CONTA" TEXT,
            "VL_CONTA" NUMERIC(20, 2),
            "ST_CONTA_FIXA" VARCHAR(10),
            "DT_INI_EXERC" DATE,
            "COLUNA_DF" VARCHAR(50),
            PRIMARY KEY ("CD_CVM", "DT_REFER", "CD_CONTA")
        );
        """)
        
        try:
            with self.engine.connect() as connection:
                connection.execute(create_table_query)
                connection.commit()
            logger.info(f"✅ Tabela '{Config.TABLE_NAME}' criada com sucesso.")
        except SQLAlchemyError as e:
            logger.error(f"❌ Falha ao criar tabela: {e}", exc_info=True)
            raise

class DataProcessor:
    @staticmethod
    def clean_data(df: pd.DataFrame) -> pd.DataFrame:
        # Converter colunas de data
        date_cols = ['DT_REFER', 'DT_FIM_EXERC', 'DT_INI_EXERC']
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Converter colunas numéricas
        numeric_cols = ['VL_CONTA', 'CD_CVM', 'VERSAO']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Remover linhas com valores essenciais faltando
        required_cols = ['CD_CVM', 'DT_REFER', 'CD_CONTA', 'VL_CONTA']
        df.dropna(subset=[c for c in required_cols if c in df.columns], inplace=True)
        
        # Garantir tipos de dados corretos
        if 'CD_CVM' in df.columns:
            df['CD_CVM'] = df['CD_CVM'].astype(int)
        if 'CD_CONTA' in df.columns:
            df['CD_CONTA'] = df['CD_CONTA'].astype(str)
        
        # Remover duplicatas baseadas na chave primária
        df.drop_duplicates(subset=['CD_CVM', 'DT_REFER', 'CD_CONTA'], keep='last', inplace=True)
        
        return df

class DataLoader:
    @staticmethod
    def load_from_zip(zip_path: str, year: str) -> Optional[pd.DataFrame]:
        if not os.path.exists(zip_path):
            logger.warning(f"Arquivo {zip_path} não encontrado.")
            return None
            
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                all_data = []
                csv_files = [f for f in zip_ref.namelist() if f.endswith('.csv') and 'con' in f]
                
                for csv_file in tqdm(csv_files, desc=f"Lendo arquivos de {year}"):
                    try:
                        with zip_ref.open(csv_file) as f:
                            df = pd.read_csv(
                                f, sep=';', encoding='latin1', decimal=',',
                                low_memory=False,
                                dtype={'CD_CONTA': str, 'CNPJ_CIA': str}
                            )
                            all_data.append(df)
                    except Exception as e:
                        logger.error(f"Erro ao ler {csv_file} do ZIP: {e}")
                
                if not all_data:
                    logger.warning(f"Nenhum arquivo CSV válido encontrado em {zip_path}")
                    return None

                combined_df = pd.concat(all_data, ignore_index=True)
                del all_data
                gc.collect()
                return combined_df
        except Exception as e:
            logger.error(f"Erro ao processar o arquivo ZIP {zip_path}: {e}", exc_info=True)
            return None

class ETLPipeline:
    def __init__(self):
        self.db = DatabaseManager()
        self.processor = DataProcessor()
        self.loader = DataLoader()

    def run(self):
        logger.info("="*50)
        logger.info("INICIANDO PIPELINE DE CARGA DE DADOS")
        logger.info("="*50)
        
        self.db.create_table_if_not_exists()
        
        for year in Config.VALID_YEARS:
            self._process_year(year)
            
        logger.info("✅ PIPELINE CONCLUÍDO COM SUCESSO.")

    def _process_year(self, year: str):
        zip_file = f'dfp_cia_aberta_{year}.zip'
        if not os.path.exists(zip_file):
            logger.error(f"Arquivo {zip_file} não encontrado. Pulando ano {year}.")
            return
        
        start_time = time.time()
        raw_df = self.loader.load_from_zip(zip_file, year)
        if raw_df is None or raw_df.empty:
            logger.error(f"Nenhum dado válido para {year}. Pulando.")
            return
        
        logger.info(f"Dados brutos carregados: {len(raw_df)} linhas.")
        
        clean_df = self.processor.clean_data(raw_df)
        logger.info(f"Dados limpos: {len(clean_df)} linhas.")
        
        if not clean_df.empty:
            self._upsert_data(clean_df, year)
        
        del raw_df, clean_df
        gc.collect()
        
        elapsed = time.time() - start_time
        logger.info(f"✅ {year} processado em {elapsed:.2f}s")

    def _upsert_data(self, df: pd.DataFrame, year: str):
        try:
            logger.info(f"Preparando {len(df)} registros para upsert do ano {year}...")
            
            # Converter DataFrame para lista de dicionários
            data = df.to_dict('records')
            
            # Query de UPSERT otimizada
            upsert_query = """
            INSERT INTO financial_data (
                "CNPJ_CIA", "DT_REFER", "VERSAO", "DENOM_CIA", "CD_CVM", 
                "GRUPO_DFP", "MOEDA", "ESCALA_MOEDA", "ORDEM_EXERC", "DT_FIM_EXERC",
                "CD_CONTA", "DS_CONTA", "VL_CONTA", "ST_CONTA_FIXA", "DT_INI_EXERC", "COLUNA_DF"
            ) VALUES (
                %(CNPJ_CIA)s, %(DT_REFER)s, %(VERSAO)s, %(DENOM_CIA)s, %(CD_CVM)s,
                %(GRUPO_DFP)s, %(MOEDA)s, %(ESCALA_MOEDA)s, %(ORDEM_EXERC)s, %(DT_FIM_EXERC)s,
                %(CD_CONTA)s, %(DS_CONTA)s, %(VL_CONTA)s, %(ST_CONTA_FIXA)s, %(DT_INI_EXERC)s, %(COLUNA_DF)s
            )
            ON CONFLICT ("CD_CVM", "DT_REFER", "CD_CONTA") 
            DO UPDATE SET
                "VL_CONTA" = EXCLUDED."VL_CONTA",
                "DS_CONTA" = EXCLUDED."DS_CONTA",
                "ST_CONTA_FIXA" = EXCLUDED."ST_CONTA_FIXA",
                "GRUPO_DFP" = EXCLUDED."GRUPO_DFP"
            """
            
            # Executar em lotes usando execute_batch para melhor performance
            with self.db.engine.connect().connection.cursor() as cursor:
                execute_batch(cursor, upsert_query, data, page_size=Config.BATCH_SIZE)
                self.db.engine.connect().connection.commit()
            
            logger.info(f"✅ Dados do ano {year} upserted com sucesso")
            
        except Exception as e:
            logger.error(f"❌ Erro durante upsert para o ano {year}: {e}", exc_info=True)
            raise

def main():
    try:
        pipeline = ETLPipeline()
        pipeline.run()
    except Exception as e:
        logger.critical(f"ERRO CRÍTICO: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    import psutil
    process = psutil.Process()
    logger.info(f"Memória inicial: {process.memory_info().rss / (1024 * 1024):.2f} MB")
    
    main()
    
    logger.info(f"Memória final: {process.memory_info().rss / (1024 * 1024):.2f} MB")
