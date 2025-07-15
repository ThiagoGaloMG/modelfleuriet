# core/market_data/ticker.py
import pandas as pd
from .duckdb_client import get_duckdb_client

class Ticker:
    def __init__(self, symbol: str):
        self.symbol = symbol.upper()
        self.duckdb_client = get_duckdb_client()
        # ## CORREÇÃO FINAL E DEFINITIVA ##
        # Usamos o protocolo nativo 'hf://' do DuckDB. Esta é a forma canônica e robusta.
        # Ele aponta diretamente para o repositório no Hugging Face.
        self.repo_path = 'hf://datasets/bwzheng2010/yahoo-finance-data/data'

    def _execute_query(self, table_name: str, columns: str, extra_conditions: str = "") -> pd.DataFrame:
        # A URL agora usa o protocolo hf:// e aponta para o diretório da tabela.
        # Ex: hf://datasets/bwzheng2010/yahoo-finance-data/data/stock_prices
        # O DuckDB entende como ler todos os arquivos .parquet dentro deste caminho.
        full_path = f"{self.repo_path}/{table_name}"
        
        sql = f"""
        SELECT {columns}
        FROM read_parquet('{full_path}/*.parquet', hive_partitioning=1)
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
        if period.endswith('y'):
            years = int(period[:-1])
            start_date = pd.Timestamp.now() - pd.DateOffset(years=years)
            df = df[df['date'] >= start_date]
        return df
