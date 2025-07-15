# core/market_data/duckdb_client.py
import logging
import duckdb
import pandas as pd
from threading import Lock

_instance = None
_lock = Lock()

def get_duckdb_client():
    global _instance
    if _instance is None:
        with _lock:
            if _instance is None:
                _instance = DuckDBClient()
    return _instance

class DuckDBClient:
    def __init__(self):
        self.connection = duckdb.connect(":memory:")
        self.connection.execute("INSTALL httpfs;")
        self.connection.execute("LOAD httpfs;")
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info("Cliente DuckDB inicializado com cache HTTPFS.")

    def query(self, sql: str) -> pd.DataFrame:
        self.logger.debug(f"Executando query com DuckDB: {sql}")
        try:
            return self.connection.sql(sql).df()
        except Exception as e:
            self.logger.error(f"Query DuckDB falhou: {str(e)}")
            # Retorna DataFrame vazio em caso de erro para não quebrar a aplicação
            return pd.DataFrame()