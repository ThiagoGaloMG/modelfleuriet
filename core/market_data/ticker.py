# core/market_data/ticker.py
import pandas as pd
from .duckdb_client import get_duckdb_client
from .hugging_face_client import HuggingFaceClient

class Ticker:
    def __init__(self, symbol: str):
        self.symbol = symbol.upper()
        self.duckdb_client = get_duckdb_client()
        self.hf_client = HuggingFaceClient()

    def _execute_query(self, table_name: str, columns: str, extra_conditions: str = "") -> pd.DataFrame:
        url = self.hf_client.get_url_path(table_name)
        sql = f"""
        SELECT {columns}
        FROM read_parquet('{url}*.parquet', hive_partitioning=1)
        WHERE symbol = '{self.symbol}' {extra_conditions}
        """
        return self.duckdb_client.query(sql)

    def summary(self) -> pd.DataFrame:
        return self._execute_query('stock_summary', 'price, market_cap')

    def profile(self) -> pd.DataFrame:
        return self._execute_query('stock_profile', 'long_name')

    def price_history(self, period: str = '5y') -> pd.DataFrame:
        # O particionamento por data não é simples, então por agora vamos pegar tudo e filtrar depois
        # Para o futuro, a query pode ser otimizada se necessário.
        df = self._execute_query('stock_prices', 'date, close')
        df['date'] = pd.to_datetime(df['date'])
        # Filtra o período desejado
        if period.endswith('y'):
            years = int(period[:-1])
            start_date = pd.Timestamp.now() - pd.DateOffset(years=years)
            df = df[df['date'] >= start_date]
        return df