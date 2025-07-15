# core/market_data/hugging_face_client.py
import requests
import logging

logger = logging.getLogger(__name__)

class HuggingFaceClient:
    def __init__(self):
        # Este é o endpoint da API para listar arquivos, e não os dados brutos
        self.base_url = "https://huggingface.co/api/datasets/bwzheng2010/yahoo-finance-data/tree/main/"

    def list_parquet_files(self, table: str) -> list[str]:
        """Busca na API do Hugging Face a lista de todos os arquivos .parquet para uma tabela."""
        try:
            # Constrói a URL da API para o diretório da tabela (ex: .../tree/main/data/stock_prices)
            api_url = f"{self.base_url}data/{table}"
            response = requests.get(api_url)
            response.raise_for_status()
            
            # Filtra a resposta JSON para pegar apenas o caminho dos arquivos .parquet
            files_info = response.json()
            parquet_files = [
                f"https://huggingface.co/datasets/bwzheng2010/yahoo-finance-data/resolve/main/{f['path']}"
                for f in files_info if f['path'].endswith('.parquet')
            ]
            
            if not parquet_files:
                raise ValueError(f"Nenhum arquivo parquet encontrado para a tabela {table}")
                
            return parquet_files
        except Exception as e:
            logger.error(f"Falha ao listar arquivos do Hugging Face para a tabela {table}: {e}")
            return []
