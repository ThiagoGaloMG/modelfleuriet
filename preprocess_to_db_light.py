import pandas as pd
import zipfile
import os
from sqlalchemy import create_engine, text, inspect, MetaData, Table, Column
from sqlalchemy import String, Integer, Date, Numeric, Text
from sqlalchemy import PrimaryKeyConstraint
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from sqlalchemy.dialects.postgresql import insert
import time
import logging
from tqdm import tqdm
import numpy as np
from typing import Optional
from dotenv import load_dotenv
import gc

# Carrega variáveis de ambiente
load_dotenv()

class Config:
    VALID_YEARS = ['2022', '2023', '2024']
    CHUNK_SIZE = 2000
    TABLE_NAME = 'financial_data'
    BATCH_SIZE = 500  # Tamanho do lote para inserção

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
        self.metadata = MetaData()
        self.financial_table = Table(
            Config.TABLE_NAME, self.metadata,
            Column('CNPJ_CIA', String(20)),
            Column('DT_REFER', Date),
            Column('VERSAO', Integer),
            Column('DENOM_CIA', String(255)),
            Column('CD_CVM', Integer),
            Column('GRUPO_DFP', String(255)),
            Column('MOEDA', String(10)),
            Column('ESCALA_MOEDA', String(20)),
            Column('ORDEM_EXERC', String(20)),
            Column('DT_FIM_EXERC', Date),
            Column('CD_CONTA', String(50)),
            Column('DS_CONTA', Text),
            Column('VL_CONTA', Numeric(20, 2)),
            Column('ST_CONTA_FIXA', String(10)),
            Column('DT_INI_EXERC', Date),
            Column('COLUNA_DF', String(50)),
            PrimaryKeyConstraint('CD_CVM', 'DT_REFER', 'CD_CONTA')
        )

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
        try:
            self.metadata.create_all(self.engine)
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
                # Substituir NaT por None (que será NULL no banco)
                df[col] = df[col].apply(lambda x: None if pd.isna(x) else x)
        
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
            self._insert_data(clean_df, year)
        
        del raw_df, clean_df
        gc.collect()
        
        elapsed = time.time() - start_time
        logger.info(f"✅ {year} processado em {elapsed:.2f}s")

    def _insert_data(self, df: pd.DataFrame, year: str):
        try:
            logger.info(f"Inserindo {len(df)} registros para o ano {year}...")
            
            # Converter DataFrame para lista de dicionários
            data = []
            for _, row in df.iterrows():
                row_dict = {}
                for col in df.columns:
                    value = row[col]
                    if pd.isna(value):
                        row_dict[col] = None
                    else:
                        row_dict[col] = value
                data.append(row_dict)
            
            # Inserir em lotes usando SQLAlchemy Core
            table = self.db.financial_table
            chunks = [data[i:i + Config.BATCH_SIZE] 
                    for i in range(0, len(data), Config.BATCH_SIZE)]
            
            with self.db.engine.begin() as conn:
                for i, chunk in enumerate(chunks):
                    # Usar inserção com ON CONFLICT UPDATE (sintaxe PostgreSQL)
                    stmt = insert(table).values(chunk)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=['CD_CVM', 'DT_REFER', 'CD_CONTA'],
                        set_={
                            'VL_CONTA': stmt.excluded.VL_CONTA,
                            'DS_CONTA': stmt.excluded.DS_CONTA,
                            'ST_CONTA_FIXA': stmt.excluded.ST_CONTA_FIXA,
                            'GRUPO_DFP': stmt.excluded.GRUPO_DFP
                        }
                    )
                    conn.execute(stmt)
                    logger.info(f"Lote {i+1}/{len(chunks)} inserido.")
            
            logger.info(f"✅ Dados do ano {year} inseridos com sucesso")
            
        except Exception as e:
            logger.error(f"❌ Erro durante inserção para o ano {year}: {e}", exc_info=True)
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
