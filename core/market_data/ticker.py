# core/market_data/ticker.py
import pandas as pd
from .duckdb_client import get_duckdb_client
# A importação do hugging_face_client foi removida

class Ticker:
    def __init__(self, symbol: str):
        self.symbol = symbol.upper()
        self.duckdb_client = get_duckdb_client()
        # A URL base agora aponta para o diretório de dados brutos
        self.base_url = "https://huggingface.co/datasets/bwzheng2010/yahoo-finance-data/resolve/main/data/"

    def _execute_query(self, table_name: str, columns: str, extra_conditions: str = "") -> pd.DataFrame:
        # Constrói o caminho completo para a pasta da tabela (ex: .../stock_prices/)
        # A sintaxe `read_parquet('{url}*.parquet'...)` é a forma correta de fazer o DuckDB
        # ler todos os arquivos de um diretório remoto.
        url = f"{self.base_url}{table_name}/"
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
        df = self._execute_query('stock_prices', 'date, close')
        if df.empty:
            return pd.DataFrame()
            
        df['date'] = pd.to_datetime(df['date'])
        # Filtra o período desejado
        if period.endswith('y'):
            years = int(period[:-1])
            start_date = pd.Timestamp.now() - pd.DateOffset(years=years)
            df = df[df['date'] >= start_date]
        return df
