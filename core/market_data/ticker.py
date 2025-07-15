# core/market_data/ticker.py
import pandas as pd
from .duckdb_client import get_duckdb_client
from .hugging_face_client import HuggingFaceClient # <- Importando de volta

class Ticker:
    def __init__(self, symbol: str):
        self.symbol = symbol.upper()
        self.duckdb_client = get_duckdb_client()
        self.hf_client = HuggingFaceClient()

    def _execute_query(self, table_name: str, columns: str, extra_conditions: str = "") -> pd.DataFrame:
        # Passo 1: Obter a lista de todos os arquivos .parquet da tabela
        file_list = self.hf_client.list_parquet_files(table_name)
        if not file_list:
            return pd.DataFrame() # Retorna DF vazio se não encontrar arquivos

        # Passo 2: O DuckDB lê a lista de arquivos
        # Esta é a forma robusta de ler múltiplos arquivos remotos
        sql = f"""
        SELECT {columns}
        FROM read_parquet({file_list})
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
