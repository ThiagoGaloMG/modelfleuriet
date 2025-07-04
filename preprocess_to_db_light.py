import pandas as pd
import zipfile
import os
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError
import time
import logging
from tqdm import tqdm
import numpy as np
from typing import Optional
from dotenv import load_dotenv

# Carrega variáveis de ambiente de um arquivo .env, se existir (para desenvolvimento local)
load_dotenv()

# --- Configuração ---

class Config:
    """Classe de configuração para o pipeline de ETL."""
    # Anos dos arquivos ZIP a serem processados
    VALID_YEARS = ['2022', '2023', '2024']
    # Tamanho do lote para inserção de dados no banco
    CHUNK_SIZE = 2000
    # Nome da tabela no banco de dados
    TABLE_NAME = 'financial_data'

def setup_logging():
    """Configura o logging para output na consola."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[logging.StreamHandler()] # Loga para a consola, ideal para a Render
    )
    return logging.getLogger(__name__)

logger = setup_logging()

# --- Módulos do Pipeline ---

class DatabaseManager:
    """Gerencia a conexão e as operações com o banco de dados PostgreSQL."""
    def __init__(self):
        self.engine = self._create_engine()

    def _create_engine(self):
        """Cria a engine do SQLAlchemy usando a variável de ambiente DATABASE_URL."""
        database_url = os.getenv('DATABASE_URL')
        if not database_url:
            logger.error("A variável de ambiente DATABASE_URL não foi definida.")
            raise ValueError("A variável de ambiente DATABASE_URL não foi definida.")
        
        # Adapta a URL de conexão para o driver psycopg2 do PostgreSQL
        conn_str = database_url.replace("postgresql://", "postgresql+psycopg2://", 1)
        
        try:
            engine = create_engine(conn_str, pool_pre_ping=True)
            logger.info("✅ Engine do banco de dados criada com sucesso.")
            return engine
        except Exception as e:
            logger.error(f"❌ Falha ao criar a engine do banco de dados: {e}", exc_info=True)
            raise

    def create_table_if_not_exists(self):
        """Cria a tabela 'financial_data' se ela não existir."""
        inspector = inspect(self.engine)
        if inspector.has_table(Config.TABLE_NAME):
            logger.info(f"Tabela '{Config.TABLE_NAME}' já existe. Nenhuma ação necessária.")
            return

        logger.info(f"Tabela '{Config.TABLE_NAME}' não encontrada. Criando...")
        
        # Query para criar a tabela com tipos de dados otimizados para PostgreSQL
        # e uma chave primária para garantir a integridade dos dados.
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
            "DS_CONTA" VARCHAR(255),
            "VL_CONTA" NUMERIC(20, 2),
            "ST_CONTA_FIXA" VARCHAR(10),
            PRIMARY KEY ("CD_CVM", "DT_REFER", "CD_CONTA")
        );
        """)
        try:
            with self.engine.connect() as connection:
                connection.execute(create_table_query)
            logger.info(f"✅ Tabela '{Config.TABLE_NAME}' criada com sucesso.")
        except SQLAlchemyError as e:
            logger.error(f"❌ Falha ao criar a tabela: {e}", exc_info=True)
            raise

class DataProcessor:
    """Responsável pela limpeza e transformação dos dados."""
    @staticmethod
    def clean_data(df: pd.DataFrame) -> pd.DataFrame:
        """Limpa e formata o DataFrame para inserção no banco."""
        # Converte colunas de data
        for col in ['DT_REFER', 'DT_FIM_EXERC']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Converte colunas numéricas
        df['VL_CONTA'] = pd.to_numeric(df['VL_CONTA'], errors='coerce')
        df['CD_CVM'] = pd.to_numeric(df['CD_CVM'], errors='coerce')

        # Remove linhas onde valores essenciais são nulos
        df.dropna(subset=['CD_CVM', 'DT_REFER', 'CD_CONTA', 'VL_CONTA'], inplace=True)
        
        # Garante tipos de dados corretos
        df['CD_CVM'] = df['CD_CVM'].astype(int)
        
        return df

class DataLoader:
    """Carrega dados dos arquivos ZIP."""
    @staticmethod
    def load_from_zip(zip_path: str, year: str) -> Optional[pd.DataFrame]:
        """Extrai e lê todos os CSVs de um arquivo ZIP para um único DataFrame."""
        if not os.path.exists(zip_path):
            logger.warning(f"Arquivo {zip_path} não encontrado.")
            return None
            
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                all_data = []
                # Filtra apenas pelos arquivos CSV relevantes (BPA, BPP, DRE, DFC_MD)
                csv_files = [f for f in zip_ref.namelist() if f.endswith('.csv') and 'con' in f]
                
                for csv_file in tqdm(csv_files, desc=f"Lendo arquivos de {year}"):
                    try:
                        with zip_ref.open(csv_file) as f:
                            df = pd.read_csv(
                                f, sep=';', encoding='latin1', decimal=',',
                                low_memory=False, dtype={'CD_CONTA': str}
                            )
                            all_data.append(df)
                    except Exception as e:
                        logger.error(f"Erro ao ler {csv_file} do ZIP: {e}")
                
                if not all_data:
                    logger.warning(f"Nenhum arquivo CSV válido encontrado em {zip_path}")
                    return None

                return pd.concat(all_data, ignore_index=True)
        except Exception as e:
            logger.error(f"Erro ao processar o arquivo ZIP {zip_path}: {e}", exc_info=True)
            return None

class ETLPipeline:
    """Orquestra o processo de Extração, Transformação e Carga."""
    def __init__(self):
        self.db = DatabaseManager()
        self.processor = DataProcessor()
        self.loader = DataLoader()

    def run(self):
        """Executa o pipeline completo."""
        logger.info("="*50)
        logger.info("INICIANDO PIPELINE DE CARGA DE DADOS")
        logger.info("="*50)
        
        # 1. Garante que a tabela exista
        self.db.create_table_if_not_exists()
        
        # 2. Processa os dados para cada ano configurado
        for year in Config.VALID_YEARS:
            self._process_year(year)
            
        logger.info("PIPELINE CONCLUÍDO COM SUCESSO.")

    def _process_year(self, year: str):
        """Processa os dados de um ano específico."""
        zip_file = f'dfp_cia_aberta_{year}.zip'
        logger.info(f"===> INICIANDO PROCESSAMENTO PARA O ANO: {year} <===")
        start_time = time.time()
        
        raw_df = self.loader.load_from_zip(zip_file, year)
        if raw_df is None or raw_df.empty:
            logger.error(f"Nenhum dado válido carregado para {year}. Pulando.")
            return
        
        logger.info(f"Dados brutos carregados para {year}: {len(raw_df)} linhas.")
        
        clean_df = self.processor.clean_data(raw_df)
        logger.info(f"Dados limpos para {year}: {len(clean_df)} linhas.")
        
        if not clean_df.empty:
            self._insert_data(clean_df)
        
        elapsed = time.time() - start_time
        logger.info(f"✅ Ano {year} processado em {elapsed:.2f} segundos.")

    def _insert_data(self, df: pd.DataFrame):
        """Insere os dados no banco de dados usando o método to_sql."""
        try:
            logger.info(f"Iniciando inserção de {len(df)} registros...")
            df.to_sql(
                name=Config.TABLE_NAME,
                con=self.db.engine,
                if_exists='append', # Adiciona os dados, não substitui
                index=False,
                chunksize=Config.CHUNK_SIZE,
                method='multi' # Usa inserção multi-valor, mais rápido
            )
            logger.info("Inserção em lote concluída com sucesso.")
        except IntegrityError:
            logger.warning("Violação de chave primária detectada. Os dados provavelmente já existem.")
        except SQLAlchemyError as e:
            logger.error(f"❌ Falha na inserção de dados: {e}", exc_info=True)
            # Em um cenário real, poderia haver um fallback para inserção linha a linha aqui.
            # Por simplicidade, vamos apenas logar o erro.

def main():
    """Função principal para executar o pipeline."""
    try:
        pipeline = ETLPipeline()
        pipeline.run()
    except Exception as e:
        logger.critical(f"ERRO CRÍTICO NO PIPELINE: {e}", exc_info=True)

if __name__ == "__main__":
    main()
