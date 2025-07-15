# core/market_data/hugging_face_client.py
from typing import Dict, Any
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Constantes movidas para cá para autossuficiência
TABLES = ["stock_profile", "stock_summary", "stock_statement", "stock_prices"]

class HuggingFaceClient:
    def __init__(self, max_retries: int = 3, timeout: int = 30):
        self.base_url = "https://huggingface.co/datasets/bwzheng2010/yahoo-finance-data"
        self.timeout = timeout
        self.session = requests.Session()
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retry_strategy))

    def get_url_path(self, table: str) -> str:
        if table not in TABLES:
            raise ValueError(f"Tabela '{table}' inválida.")
        return f"{self.base_url}/resolve/main/data/{table}/"