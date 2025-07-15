# modelfleuriet/core/company_ranking.py

import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Optional, Tuple, Union, Any

# Importa o dataclass da empresa e o calculador de métricas
from core.data_collector import CompanyFinancialData
from core.financial_metrics_calculator import FinancialMetricsCalculator
from core.utils import clean_data_for_json # Para limpar dados para JSON

logger = logging.getLogger(__name__)

class CompanyRanking:
    """
    Classifica empresas com base nas métricas financeiras.
    """
    def __init__(self, calculator: FinancialMetricsCalculator):
        self.calculator = calculator

    def _calculate_all_metrics(self, data: CompanyFinancialData) -> Dict[str, Union[float, str, Any]]:
        """Calcula todas as métricas para uma única empresa."""
        try:
            beta = 1.0 # Exemplo de beta (precisaria ser calculado pelo modelo Hamada)
            
            wacc = self.calculator._calculate_wacc(data, beta)
            if np.isnan(wacc): wacc = 0.0

            eva_abs, eva_pct = self.calculator.calculate_eva(data, beta)
            efv_abs, efv_pct = self.calculator.calculate_efv(data, beta)
            riqueza_atual = self.calculator.calculate_riqueza_atual(data, beta)
            riqueza_futura = self.calculator.calculate_riqueza_futura(data)
            upside = self.calculator.calculate_upside(data, efv_abs) if not np.isnan(efv_abs) else np.nan
            
            # Calcular o Score Combinado
            score_combinado = 0
            if not np.isnan(eva_pct): score_combinado += eva_pct * 0.4
            if not np.isnan(efv_pct): score_combinado += efv_pct * 0.4
            if not np.isnan(upside): score_combinado += upside * 0.2
            
            return {
                'ticker': data.ticker,
                'company_name': data.company_name,
                'market_cap': data.market_cap,
                'stock_price': data.stock_price,
                'wacc_percentual': wacc * 100 if not np.isnan(wacc) else None,
                'eva_abs': eva_abs,
                'eva_percentual': eva_pct,
                'efv_abs': efv_abs,
                'efv_percentual': efv_pct,
                'riqueza_atual': riqueza_atual,
                'riqueza_futura': riqueza_futura,
                'upside_percentual': upside,
                'combined_score': score_combinado,
                'raw_data': clean_data_for_json(data.__dict__) # Inclui todos os dados brutos da coleta
            }
        except Exception as e:
            logger.error(f"Erro ao calcular métricas para {data.ticker}: {e}")
            return {'ticker': data.ticker, 'company_name': data.company_name, 'error': str(e)}

    def generate_ranking_report(self, companies_data: Dict[str, CompanyFinancialData]) -> pd.DataFrame:
        """
        Gera um DataFrame com o relatório de ranking de todas as empresas.
        """
        report_data = []
        for ticker, data in companies_data.items():
            metrics = self._calculate_all_metrics(data)
            if 'error' not in metrics:
                report_data.append(metrics)
            else:
                logger.warning(f"Empresa {ticker} excluída do relatório devido a erro: {metrics['error']}")

        df = pd.DataFrame(report_data)
        
        # Limpar NaN/Inf para garantir que o sort funcione
        for col in ['wacc_percentual', 'eva_percentual', 'efv_percentual', 'riqueza_atual', 'riqueza_futura', 'upside_percentual', 'combined_score']:
            if col in df.columns:
                df[col] = df[col].replace([np.inf, -np.inf], np.nan)
                df[col] = df[col].fillna(0) # Substituir NaN por 0 para ranking, ou outro valor estratégico

        return df

    def rank_by_metric(self, df: pd.DataFrame, metric: str, ascending: bool = False) -> List[Tuple[str, float]]:
        """
        Classifica as empresas por uma métrica específica e retorna uma lista de tuplas.
        """
        if metric not in df.columns:
            logger.warning(f"Métrica '{metric}' não encontrada para ranking.")
            return []
        
        sorted_df = df.sort_values(by=metric, ascending=ascending)
        return sorted_df[['ticker', metric]].values.tolist()

    def rank_by_eva(self, df: pd.DataFrame) -> List[Tuple[str, float, float]]:
        """Classifica empresas por EVA (percentual)."""
        sorted_df = df.sort_values(by='eva_percentual', ascending=False)
        return sorted_df[['ticker', 'eva_abs', 'eva_percentual']].values.tolist()

    def rank_by_efv(self, df: pd.DataFrame) -> List[Tuple[str, float, float]]:
        """Classifica empresas por EFV (percentual)."""
        sorted_df = df.sort_values(by='efv_percentual', ascending=False)
        return sorted_df[['ticker', 'efv_abs', 'efv_percentual']].values.tolist()
    
    def rank_by_upside(self, df: pd.DataFrame) -> List[Tuple[str, float]]:
        """Classifica empresas por potencial de valorização (Upside)."""
        sorted_df = df.sort_values(by='upside_percentual', ascending=False)
        return sorted_df[['ticker', 'upside_percentual']].values.tolist()
    
    def rank_by_combined_score(self, df: pd.DataFrame) -> List[Tuple[str, float]]:
        """Classifica empresas por score combinado."""
        sorted_df = df.sort_values(by='combined_score', ascending=False)
        return sorted_df[['ticker', 'combined_score']].values.tolist()
