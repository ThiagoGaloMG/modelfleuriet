# modelfleuriet/core/advanced_ranking.py

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Union, Any
from dataclasses import dataclass
import logging
# ## CORREÇÃO PRINCIPAL ##: O nome do arquivo foi ajustado para o correto.
from core.financial_metrics_calculator import FinancialMetricsCalculator # Importa a calculadora
from core.data_collector import CompanyFinancialData # Importa o dataclass da empresa
from core.ibovespa_utils import get_market_sectors # Para rankings por setor
from sklearn.preprocessing import StandardScaler, MinMaxScaler # Para ML
from sklearn.cluster import KMeans # Para clustering
import warnings

warnings.filterwarnings('ignore')
logger = logging.getLogger(__name__)

@dataclass
class RankingCriteria:
    """Critérios para classificação personalizada."""
    eva_weight: float = 0.3
    efv_weight: float = 0.3
    upside_weight: float = 0.2
    profitability_weight: float = 0.1
    liquidity_weight: float = 0.1
    
    def normalize_weights(self):
        """Normaliza os pesos para somar 1.0."""
        total = (self.eva_weight + self.efv_weight + self.upside_weight + 
                self.profitability_weight + self.liquidity_weight)
        if total > 0:
            self.eva_weight /= total
            self.efv_weight /= total
            self.upside_weight /= total
            self.profitability_weight /= total
            self.liquidity_weight /= total
        else:
            logger.warning("Soma dos pesos é zero, não foi possível normalizar.")

class AdvancedRanking:
    """Classe para classificações avançadas e análises sofisticadas."""
    
    def __init__(self, calculator: FinancialMetricsCalculator):
        self.calculator = calculator
        self.scaler = StandardScaler()
        self.min_max_scaler = MinMaxScaler()
        
    def _prepare_data_for_ml(self, companies_data: Dict[str, CompanyFinancialData]) -> pd.DataFrame:
        """Prepara os dados para algoritmos de ML."""
        data_for_ml = []
        for ticker, data in companies_data.items():
            beta = 1.0 # Exemplo
            
            eva_abs, eva_pct = self.calculator.calculate_eva(data, beta)
            efv_abs, efv_pct = self.calculator.calculate_efv(data, beta)
            riqueza_atual = self.calculator.calculate_riqueza_atual(data, beta)
            riqueza_futura = self.calculator.calculate_riqueza_futura(data)
            upside = self.calculator.calculate_upside(data, efv_abs) if not np.isnan(efv_abs) else np.nan

            data_for_ml.append({
                'ticker': ticker,
                'company_name': data.company_name,
                'eva_pct': eva_pct,
                'efv_pct': efv_pct,
                'upside_pct': upside,
                'riqueza_atual': riqueza_atual,
                'riqueza_futura': riqueza_futura,
                'market_cap': data.market_cap,
                'revenue': data.revenue,
                'sector': data.sector
            })
        
        df = pd.DataFrame(data_for_ml)
        df = df.replace([np.inf, -np.inf], np.nan).fillna(0)
        return df

    def custom_rank_companies(self, companies_data: Dict[str, CompanyFinancialData], criteria: RankingCriteria) -> List[Dict]:
        """Realiza um ranking personalizado de empresas."""
        criteria.normalize_weights()
        
        processed_data = []
        all_scores = { 'eva': [], 'efv': [], 'upside': [], 'profitability': [], 'liquidity': [] }
        
        # Passo 1: Calcular todos os scores brutos para todas as empresas
        for ticker, data in companies_data.items():
            beta = 1.0
            _, eva_pct = self.calculator.calculate_eva(data, beta)
            efv_abs, efv_pct = self.calculator.calculate_efv(data, beta)
            upside = self.calculator.calculate_upside(data, efv_abs) if not np.isnan(efv_abs) else 0
            profitability_score = (data.net_income / data.revenue) if data.revenue is not None and data.revenue > 0 else 0
            liquidity_score = (data.current_assets / data.current_liabilities) if data.current_liabilities is not None and data.current_liabilities > 0 else 0
            
            # Adiciona os scores brutos às listas
            all_scores['eva'].append(eva_pct if not np.isnan(eva_pct) else 0)
            all_scores['efv'].append(efv_pct if not np.isnan(efv_pct) else 0)
            all_scores['upside'].append(upside if not np.isnan(upside) else 0)
            all_scores['profitability'].append(profitability_score if not np.isnan(profitability_score) else 0)
            all_scores['liquidity'].append(liquidity_score if not np.isnan(liquidity_score) else 0)
            
            processed_data.append({
                'ticker': ticker, 'company_name': data.company_name,
                'eva_pct': eva_pct, 'efv_pct': efv_pct, 'upside_pct': upside,
                'profitability_score': profitability_score, 'liquidity_score': liquidity_score
            })
            
        # Passo 2: Normalizar cada lista de scores de uma vez (mais eficiente e correto)
        scaled_scores = {}
        for key, values in all_scores.items():
            if values: # Garante que a lista não está vazia
                scaled_values = self.min_max_scaler.fit_transform(np.array(values).reshape(-1, 1))
                scaled_scores[key] = scaled_values.flatten()

        # Passo 3: Calcular o score final ponderado para cada empresa
        for i, company in enumerate(processed_data):
            company['final_score'] = (
                scaled_scores.get('eva', [0]*len(processed_data))[i] * criteria.eva_weight +
                scaled_scores.get('efv', [0]*len(processed_data))[i] * criteria.efv_weight +
                scaled_scores.get('upside', [0]*len(processed_data))[i] * criteria.upside_weight +
                scaled_scores.get('profitability', [0]*len(processed_data))[i] * criteria.profitability_weight +
                scaled_scores.get('liquidity', [0]*len(processed_data))[i] * criteria.liquidity_weight
            )

        processed_data.sort(key=lambda x: x['final_score'], reverse=True)
        return processed_data

    def identify_opportunities(self, companies_data: Dict[str, CompanyFinancialData]) -> Dict[str, Any]:
        """Identifica oportunidades de investimento."""
        opportunities = {
            'value_creators': [], 'growth_potential': [], 'undervalued': [],
            'best_opportunities': [], 'clusters': {}, 'sector_rankings': {}
        }
        all_metrics_df = self._prepare_data_for_ml(companies_data)
        if all_metrics_df.empty: return opportunities

        for _, row in all_metrics_df.iterrows():
            if row['eva_pct'] > 0: opportunities['value_creators'].append([row['ticker'], row['eva_pct']])
            if row['efv_pct'] > 0: opportunities['growth_potential'].append([row['ticker'], row['efv_pct']])
            if row['upside_pct'] > 0.20: opportunities['undervalued'].append([row['ticker'], row['upside_pct']])

        all_metrics_df['simple_combined_score'] = (all_metrics_df['eva_pct'] * 0.4 + all_metrics_df['efv_pct'] * 0.4 + all_metrics_df['upside_pct'] * 0.2)
        top_companies = all_metrics_df.sort_values(by='simple_combined_score', ascending=False).head(5)
        
        for _, row in top_companies.iterrows():
            reason = [r for r, c in [('EVA Positivo', 'eva_pct'), ('EFV Positivo', 'efv_pct'), ('Upside', 'upside_pct')] if row[c] > 0]
            opportunities['best_opportunities'].append([row['ticker'], ", ".join(reason), row['simple_combined_score']])

        features = all_metrics_df[['eva_pct', 'efv_pct', 'upside_pct']]
        # Ajuste para KMeans
        if len(features) >= 3:
            scaled_features = self.scaler.fit_transform(features)
            try:
                # ## MELHORIA ##: n_init='auto' para evitar FutureWarning
                kmeans = KMeans(n_clusters=min(len(features), 3), random_state=42, n_init='auto')
                all_metrics_df['cluster'] = kmeans.fit_predict(scaled_features)
                for cluster_id in sorted(all_metrics_df['cluster'].unique()):
                    cluster_companies = all_metrics_df[all_metrics_df['cluster'] == cluster_id]['ticker'].tolist()
                    opportunities['clusters'][f"Cluster {cluster_id + 1}"] = cluster_companies
            except Exception as e:
                logger.warning(f"Erro ao realizar clustering K-Means: {e}")
        else:
            logger.info("Dados insuficientes para clustering.")

        sectors_map = get_market_sectors()
        ticker_to_sector = {ticker: sector for sector, tickers in sectors_map.items() for ticker in tickers}
        all_metrics_df['sector'] = all_metrics_df['ticker'].map(ticker_to_sector)
        
        for sector_name in all_metrics_df['sector'].dropna().unique():
            sector_df = all_metrics_df[all_metrics_df['sector'] == sector_name]
            if not sector_df.empty:
                sector_ranking = sector_df.sort_values(by='simple_combined_score', ascending=False)[['ticker', 'simple_combined_score']].values.tolist()
                opportunities['sector_rankings'][sector_name] = sector_ranking
                
        return opportunities

class PortfolioOptimizer:
    """Classe para sugestão e otimização de portfólio."""
    def __init__(self, calculator: FinancialMetricsCalculator):
        self.calculator = calculator

    def suggest_portfolio_allocation(self, companies_data: Dict[str, CompanyFinancialData], profile: str = 'moderate') -> Dict[str, float]:
        """Sugere uma alocação de portfólio."""
        processed_data = []
        for ticker, data in companies_data.items():
            beta = 1.0
            _, eva_pct = self.calculator.calculate_eva(data, beta)
            efv_abs, efv_pct = self.calculator.calculate_efv(data, beta)
            upside = self.calculator.calculate_upside(data, efv_abs) if not np.isnan(efv_abs) else 0
            score = (eva_pct if not np.isnan(eva_pct) else 0) + \
                    (efv_pct * 1.5 if not np.isnan(efv_pct) else 0) + \
                    (upside if not np.isnan(upside) else 0)
            processed_data.append({'ticker': ticker, 'score': score})
        
        df = pd.DataFrame(processed_data).fillna(0)
        df = df[df['score'] > 0]
        if df.empty: return {}

        total_score = df['score'].sum()
        if total_score == 0: return {row['ticker']: 1/len(df) for _, row in df.iterrows()}
        
        df['weight'] = df['score'] / total_score
        
        # Normalização final para garantir que a soma seja 1.0
        final_weights = {row['ticker']: round(row['weight'], 4) for _, row in df.iterrows()}
        weight_sum = sum(final_weights.values())
        if weight_sum > 0:
            final_weights = {ticker: weight / weight_sum for ticker, weight in final_weights.items()}
            
        return final_weights

    def calculate_portfolio_eva(self, portfolio_weights: Dict[str, float], companies_data: Dict[str, CompanyFinancialData]) -> Tuple[float, float]:
        """Calcula o EVA (absoluto e percentual) de um portfólio."""
        total_portfolio_eva_abs, total_portfolio_capital_employed = 0.0, 0.0
        for ticker, weight in portfolio_weights.items():
            if ticker in companies_data:
                company_data = companies_data[ticker]
                beta = 1.0
                eva_abs, _ = self.calculator.calculate_eva(company_data, beta)
                capital_employed = self.calculator._calculate_capital_employed(company_data)
                if not np.isnan(eva_abs) and not np.isnan(capital_employed) and capital_employed > 0:
                    total_portfolio_eva_abs += eva_abs * weight
                    total_portfolio_capital_employed += capital_employed * weight
        
        portfolio_eva_pct = (total_portfolio_eva_abs / total_portfolio_capital_employed) * 100 if total_portfolio_capital_employed > 0 else np.nan
        return total_portfolio_eva_abs, portfolio_eva_pct
