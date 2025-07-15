# modelfleuriet/core/advanced_ranking.py 



 import pandas as pd 

 import numpy as np 

 from typing import Dict, List, Optional, Tuple, Union, Any 

 from dataclasses import dataclass 

 import logging 

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

     """Critérios para classificação personalizada. 

     Permite ajustar os pesos para diferentes métricas de valor. 

     """ 

     eva_weight: float = 0.3 

     efv_weight: float = 0.3 

     upside_weight: float = 0.2 

     profitability_weight: float = 0.1 # Placeholder, a ser calculado 

     liquidity_weight: float = 0.1     # Placeholder, a ser calculado 

      

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

     """Classe para classificações avançadas e análises sofisticadas, 

     baseadas nas análises de correlação e value drivers do TCC. 

     """ 

      

     def __init__(self, calculator: FinancialMetricsCalculator): 

         self.calculator = calculator 

         self.scaler = StandardScaler() 

         self.min_max_scaler = MinMaxScaler() 

          

     def _prepare_data_for_ml(self, companies_data: Dict[str, CompanyFinancialData]) -> pd.DataFrame: 

         """ 

         Prepara os dados para algoritmos de ML, calculando métricas e lidando com NaNs. 

         Foca em métricas que o TCC correlaciona (EVA%, EFV%, Upside%, Riqueza). 

         """ 

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

                 'sector': data.sector # Incluir setor para ranking por setor 

             }) 

          

         df = pd.DataFrame(data_for_ml) 

          

         df = df.replace([np.inf, -np.inf], np.nan) 

         df = df.fillna(0) # Preencher NaNs com 0 para não quebrar o scaler/kmeans. 

          

         return df 



     def custom_rank_companies(self, companies_data: Dict[str, CompanyFinancialData], criteria: RankingCriteria) -> List[Dict]: 

         """ 

         Realiza um ranking personalizado de empresas baseado em critérios ponderados. 

         """ 

         criteria.normalize_weights() 

          

         processed_data = [] 

         for ticker, data in companies_data.items(): 

             beta = 1.0 # Exemplo 



             eva_abs, eva_pct = self.calculator.calculate_eva(data, beta) 

             efv_abs, efv_pct = self.calculator.calculate_efv(data, beta) 

             upside = self.calculator.calculate_upside(data, efv_abs) if not np.isnan(efv_abs) else np.nan 

              

             # Calcular scores de rentabilidade e liquidez a partir dos dados da CVM 

             profitability_score = (data.net_income / data.revenue) * 100 if data.revenue > 0 else 0 

             liquidity_score = (data.current_assets / data.current_liabilities) * 100 if data.current_liabilities > 0 else 0 



             # Escalar as métricas para que os pesos funcionem corretamente. 

             # Fit_transform precisa de um array 2D, mesmo para um único valor. 

             scaled_eva = self.min_max_scaler.fit_transform(np.array([[eva_pct]])) if not np.isnan(eva_pct) else np.array([[0]]) 

             scaled_efv = self.min_max_scaler.fit_transform(np.array([[efv_pct]])) if not np.isnan(efv_pct) else np.array([[0]]) 

             scaled_upside = self.min_max_scaler.fit_transform(np.array([[upside]])) if not np.isnan(upside) else np.array([[0]]) 

             scaled_profitability = self.min_max_scaler.fit_transform(np.array([[profitability_score]])) if not np.isnan(profitability_score) else np.array([[0]]) 

             scaled_liquidity = self.min_max_scaler.fit_transform(np.array([[liquidity_score]])) if not np.isnan(liquidity_score) else np.array([[0]]) 



             final_score = (scaled_eva[0][0] * criteria.eva_weight + 

                            scaled_efv[0][0] * criteria.efv_weight + 

                            scaled_upside[0][0] * criteria.upside_weight + 

                            scaled_profitability[0][0] * criteria.profitability_weight + 

                            scaled_liquidity[0][0] * criteria.liquidity_weight) 

              

             processed_data.append({ 

                 'ticker': ticker, 

                 'company_name': data.company_name, 

                 'eva_pct': eva_pct, 

                 'efv_pct': efv_pct, 

                 'upside_pct': upside, 

                 'profitability_score': profitability_score, 

                 'liquidity_score': liquidity_score, 

                 'final_score': final_score 

             }) 

          

         # Ordenar pelo score final 

         processed_data.sort(key=lambda x: x['final_score'], reverse=True) 

         return processed_data 



     def identify_opportunities(self, companies_data: Dict[str, CompanyFinancialData]) -> Dict[str, Any]: 

         """ 

         Identifica oportunidades de investimento com base nas métricas de valor. 

         """ 

         opportunities = { 

             'value_creators': [],       # EVA > 0 

             'growth_potential': [],     # EFV > 0 

             'undervalued': [],          # Upside > 20% (exemplo de critério) 

             'best_opportunities': [],   # Combinação de critérios 

             'clusters': {},             # Agrupamento de empresas (se K-Means for usado) 

             'sector_rankings': {}       # Ranking por setor (se setores forem identificados) 

         } 



         all_metrics_df = self._prepare_data_for_ml(companies_data) 

          

         if all_metrics_df.empty: 

             return opportunities 



         for _, row in all_metrics_df.iterrows(): 

             if row['eva_pct'] > 0: 

                 opportunities['value_creators'].append([row['ticker'], row['eva_pct']]) 

             if row['efv_pct'] > 0: 

                 opportunities['growth_potential'].append([row['ticker'], row['efv_pct']]) 

             if row['upside_pct'] > 20: # Exemplo de threshold para subvalorizadas 

                 opportunities['undervalued'].append([row['ticker'], row['upside_pct']]) 



         all_metrics_df['simple_combined_score'] = (all_metrics_df['eva_pct'] * 0.4 +  

                                                    all_metrics_df['efv_pct'] * 0.4 +  

                                                    all_metrics_df['upside_pct'] * 0.2) 

          

         top_companies = all_metrics_df.sort_values(by='simple_combined_score', ascending=False).head(5) 

         for _, row in top_companies.iterrows(): 

             reason = [] 

             if row['eva_pct'] > 0: reason.append('EVA Positivo') 

             if row['efv_pct'] > 0: reason.append('EFV Positivo') 

             if row['upside_pct'] > 0: reason.append('Upside') 

             opportunities['best_opportunities'].append([row['ticker'], ", ".join(reason), row['simple_combined_score']]) 



         # Agrupamento (Clustering) - K-Means 

         features = all_metrics_df[['eva_pct', 'efv_pct', 'upside_pct', 'riqueza_atual', 'riqueza_futura']] 

         # Verifica se há dados suficientes para clustering (mínimo de n_clusters amostras) 

         if len(features) >= 3: # KMeans precisa de pelo menos n_clusters amostras 

             scaled_features = self.scaler.fit_transform(features) 

             try: 

                 kmeans = KMeans(n_clusters=min(len(features), 3), random_state=42, n_init=10) 

                 clusters = kmeans.fit_predict(scaled_features) 

                 all_metrics_df['cluster'] = clusters 



                 for cluster_id in sorted(all_metrics_df['cluster'].unique()): 

                     cluster_companies = all_metrics_df[all_metrics_df['cluster'] == cluster_id]['ticker'].tolist() 

                     opportunities['clusters'][f"Cluster {cluster_id + 1}"] = cluster_companies 

             except Exception as e: 

                 logger.warning(f"Erro ao realizar clustering K-Means: {e}. Ignorando clustering nesta execução.") 

                 opportunities['clusters']['Erro no Clustering'] = ["Não foi possível agrupar as empresas."] 

         else: 

             logger.info("Dados insuficientes para clustering (menos de 3 empresas).") 

             opportunities['clusters']['Dados Insuficientes'] = ["Mínimo de 3 empresas para agrupamento."] 



         # Ranking por Setor 

         from core.ibovespa_utils import get_market_sectors 

         sectors_map = get_market_sectors() 

         ticker_to_sector = {} 

         for sector, tickers in sectors_map.items(): 

             for ticker in tickers: 

                 ticker_to_sector[ticker] = sector 

          

         all_metrics_df['sector'] = all_metrics_df['ticker'].map(ticker_to_sector) 

          

         for sector_name in all_metrics_df['sector'].dropna().unique(): 

             sector_df = all_metrics_df[all_metrics_df['sector'] == sector_name] 

             if not sector_df.empty: 

                 sector_ranking = sector_df.sort_values(by='simple_combined_score', ascending=False)[['ticker', 'simple_combined_score']].values.tolist() 

                 opportunities['sector_rankings'][sector_name] = sector_ranking 

                  

         return opportunities 



 class PortfolioOptimizer: 

     """ 

     Classe para sugestão e otimização de portfólio. 

     """ 

     def __init__(self, calculator: FinancialMetricsCalculator): 

         self.calculator = calculator 



     def suggest_portfolio_allocation(self, companies_data: Dict[str, CompanyFinancialData], profile: str = 'moderate') -> Dict[str, float]: 

         """ 

         Sugere uma alocação de portfólio baseada nas métricas de valor e perfil de risco. 

         """ 

         processed_data = [] 

         for ticker, data in companies_data.items(): 

             beta = 1.0 # Exemplo 

             eva_abs, eva_pct = self.calculator.calculate_eva(data, beta) 

             efv_abs, efv_pct = self.calculator.calculate_efv(data, beta) 

             upside = self.calculator.calculate_upside(data, efv_abs) if not np.isnan(efv_abs) else np.nan 

              

             # Criar um score interno para ponderar a alocação 

             score = 0 

             if not np.isnan(eva_pct): score += eva_pct 

             if not np.isnan(efv_pct): score += efv_pct * 1.5 # Maior peso para potencial futuro 

             if not np.isnan(upside): score += upside 

              

             processed_data.append({'ticker': ticker, 'score': score}) 

          

         df = pd.DataFrame(processed_data).replace([np.inf, -np.inf], np.nan).fillna(0) 

          

         df = df[df['score'] > 0] # Filtra empresas com score positivo para alocação 

         if df.empty: 

             return {c.ticker: 0.0 for c in companies_data.values()} 



         total_score = df['score'].sum() 

         if total_score == 0: 

             return {c.ticker: 0.0 for c in companies_data.values()} 



         portfolio_weights = {} 

         if profile == 'conservative': 

             top_n = min(len(df), 5)  

             total_top_score = df['score'].head(top_n).sum() 

             if total_top_score > 0: 

                 for _, row in df.head(top_n).iterrows(): 

                     portfolio_weights[row['ticker']] = (row['score'] / total_top_score) * 0.7 

                 remaining_tickers = df.iloc[top_n:] 

                 remaining_total_score = remaining_tickers['score'].sum() 

                 if remaining_total_score > 0: 

                     for _, row in remaining_tickers.iterrows(): 

                         portfolio_weights[row['ticker']] = (row['score'] / remaining_total_score) * 0.3 

             else: 

                 for _, row in df.iterrows(): 

                     portfolio_weights[row['ticker']] = 1 / len(df) if len(df) > 0 else 0 

         elif profile == 'aggressive': 

             top_n = min(len(df), 3) 

             total_top_score = df['score'].head(top_n).sum() 

             if total_top_score > 0: 

                 for _, row in df.head(top_n).iterrows(): 

                     portfolio_weights[row['ticker']] = (row['score'] / total_top_score) * 0.8 

                 remaining_tickers = df.iloc[top_n:] 

                 remaining_total_score = remaining_tickers['score'].sum() 

                 if remaining_total_score > 0: 

                     for _, row in remaining_tickers.iterrows(): 

                         portfolio_weights[row['ticker']] = (row['score'] / remaining_total_score) * 0.2 

             else: 

                 for _, row in df.iterrows(): 

                     portfolio_weights[row['ticker']] = 1 / len(df) if len(df) > 0 else 0 

         else: # Moderate (default) 

             for _, row in df.iterrows(): 

                 portfolio_weights[row['ticker']] = row['score'] / total_score 

          

         current_sum = sum(portfolio_weights.values()) 

         if current_sum > 0: 

             for ticker in portfolio_weights: 

                 portfolio_weights[ticker] /= current_sum 

          

         return {k: round(v, 4) for k, v in portfolio_weights.items()} 



     def calculate_portfolio_eva(self, portfolio_weights: Dict[str, float], companies_data: Dict[str, CompanyFinancialData]) -> Tuple[float, float]: 

         """ 

         Calcula o EVA (absoluto e percentual) de um portfólio. 

         """ 

         total_portfolio_eva_abs = 0.0 

         total_portfolio_capital_employed = 0.0 



         for ticker, weight in portfolio_weights.items(): 

             if ticker in companies_data: 

                 company_data = companies_data[ticker] 

                 beta = 1.0 # Exemplo 

                 eva_abs, _ = self.calculator.calculate_eva(company_data, beta) 

                 capital_employed = self.calculator._calculate_capital_employed(company_data) 

                  

                 if not np.isnan(eva_abs) and not np.isnan(capital_employed): 

                     total_portfolio_eva_abs += eva_abs * weight 

                     total_portfolio_capital_employed += capital_employed * weight 

          

         if total_portfolio_capital_employed > 0: 

             portfolio_eva_pct = (total_portfolio_eva_abs / total_portfolio_capital_employed) * 100 

         else: 

             portfolio_eva_pct = np.nan 



         return total_portfolio_eva_abs, portfolio_eva_pct
