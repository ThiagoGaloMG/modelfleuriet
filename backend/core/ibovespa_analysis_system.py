# modelfleuriet/core/ibovespa_analysis_system.py

import pandas as pd
import numpy as np
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any

# Importa módulos da nova estrutura 'core'
from core.ibovespa_utils import get_ibovespa_tickers, get_selic_rate
from core.data_collector import FinancialDataCollector, CompanyFinancialData
from core.financial_metrics_calculator import FinancialMetricsCalculator
from core.company_ranking import CompanyRanking
from core.advanced_ranking import AdvancedRanking, PortfolioOptimizer, RankingCriteria
from core.db_manager import SupabaseDB # Mantendo o nome SupabaseDB para o DB do Render
from core.utils import PerformanceMonitor, clean_data_for_json

logger = logging.getLogger(__name__)

class IbovespaAnalysisSystem:
    """
    Sistema de Análise Financeira completo para o Ibovespa.
    Orquestra a coleta de dados (do DB), cálculos de métricas (EVA, EFV, Riqueza),
    classificação de empresas e identificação de oportunidades.
    Integrado com o PostgreSQL do Render para persistência de dados.
    """

    def __init__(self, db_manager: SupabaseDB, ticker_mapping_df: pd.DataFrame):
        self.monitor = PerformanceMonitor()
        self.db = db_manager
        self.ticker_mapping = ticker_mapping_df

        self.collector = FinancialDataCollector(self.db, self.ticker_mapping)
        
        self.selic_rate = get_selic_rate() # Selic ainda pode vir de web scraping se necessário
        if self.selic_rate is None:
            logger.warning("Não foi possível obter a taxa Selic. Usando valor padrão de 10%.")
            self.selic_rate = 10.0
        
        self.calculator = FinancialMetricsCalculator(selic_rate=self.selic_rate)
        self.company_ranking = CompanyRanking(self.calculator)
        self.advanced_ranking = AdvancedRanking(self.calculator)
        self.portfolio_optimizer = PortfolioOptimizer(self.calculator)
        
        self.ibovespa_tickers = get_ibovespa_tickers() # Lista de tickers do Ibovespa (manual)
        if not self.ibovespa_tickers:
            logger.error("Não foi possível carregar os tickers do Ibovespa. O sistema pode não funcionar corretamente.")
            self.ibovespa_tickers = []

    def _get_companies_data_for_valuation(self, tickers: Optional[List[str]] = None) -> Dict[str, CompanyFinancialData]:
        """
        Coleta dados para uma lista específica de tickers ou para todos do Ibovespa.
        Prioriza dados do DB se forem recentes.
        """
        tickers_to_process = tickers if tickers is not None else self.ibovespa_tickers
        
        companies_data = {}
        for ticker in tickers_to_process:
            # Tenta buscar do DB primeiro (dados completos de valuation)
            latest_metrics = self.db.get_company_latest_metrics(ticker)
            
            freshness_threshold = datetime.now() - timedelta(days=7)

            if latest_metrics and latest_metrics['metrics']['raw_data'] and \
               latest_metrics['metrics']['raw_data'].get('timestamp_collected') and \
               datetime.fromisoformat(latest_metrics['metrics']['raw_data']['timestamp_collected']) > freshness_threshold:
                
                logger.info(f"Usando dados recentes do DB para {ticker}.")
                companies_data[ticker] = CompanyFinancialData(
                    ticker=ticker,
                    company_name=latest_metrics['company_name'],
                    cd_cvm=latest_metrics['metrics']['raw_data'].get('cd_cvm'),
                    sector=latest_metrics['metrics']['raw_data'].get('sector'),
                    market_cap=latest_metrics['metrics']['market_cap'],
                    stock_price=latest_metrics['metrics']['stock_price'],
                    shares_outstanding=latest_metrics['metrics']['raw_data'].get('shares_outstanding', 0),
                    revenue=latest_metrics['metrics']['raw_data'].get('revenue', 0),
                    ebit=latest_metrics['metrics']['raw_data'].get('ebit', 0),
                    net_income=latest_metrics['metrics']['raw_data'].get('net_income', 0),
                    depreciation_amortization=latest_metrics['metrics']['raw_data'].get('depreciation_amortization', 0),
                    capex=latest_metrics['metrics']['raw_data'].get('capex', 0),
                    total_assets=latest_metrics['metrics']['raw_data'].get('total_assets', 0),
                    total_debt=latest_metrics['metrics']['raw_data'].get('total_debt', 0),
                    equity=latest_metrics['metrics']['raw_data'].get('equity', 0),
                    current_assets=latest_metrics['metrics']['raw_data'].get('current_assets', 0),
                    current_liabilities=latest_metrics['metrics']['raw_data'].get('current_liabilities', 0),
                    cash=latest_metrics['metrics']['raw_data'].get('cash', 0),
                    accounts_receivable=latest_metrics['metrics']['raw_data'].get('accounts_receivable', 0),
                    inventory=latest_metrics['metrics']['raw_data'].get('inventory', 0),
                    accounts_payable=latest_metrics['metrics']['raw_data'].get('accounts_payable', 0),
                    property_plant_equipment=latest_metrics['metrics']['raw_data'].get('property_plant_equipment', 0),
                    timestamp_collected=latest_metrics['metrics']['raw_data'].get('timestamp_collected')
                )
            else:
                # Se não houver dados recentes, coleta do DB (via data_collector.py)
                logger.info(f"Coletando dados do DB para {ticker} (não encontrado ou desatualizado).")
                self.monitor.start_timer(f"coleta_db_{ticker}")
                
                # Obtém o CVM code do mapeamento
                cvm_code_row = self.ticker_mapping[self.ticker_mapping['TICKER'] == ticker]['CD_CVM']
                cvm_code = int(cvm_code_row.iloc[0]) if not cvm_code_row.empty else None

                if cvm_code is None:
                    logger.warning(f"CVM code não encontrado para {ticker}. Pulando coleta.")
                    continue

                data = self.collector.get_company_data(ticker, cvm_code)
                self.monitor.end_timer(f"coleta_db_{ticker}")
                if data:
                    companies_data[ticker] = data
                else:
                    logger.warning(f"Não foi possível coletar dados para {ticker} do banco de dados.")
        return companies_data

    def run_complete_analysis(self, num_companies: Optional[int] = None, force_recollect: bool = False) -> Dict:
        """
        Executa a análise completa para as empresas do Ibovespa.
        Tenta carregar o último relatório completo do DB primeiro. Se não houver ou for forçado,
        executa a análise e salva no DB.
        """
        # Tenta carregar o último relatório completo do DB
        if not force_recollect and num_companies is None: # Só tenta carregar do DB para análise COMPLETA e se não for forçado
            logger.info("Tentando carregar o último relatório completo do DB...")
            latest_report_from_db = self.db.get_latest_full_analysis_report()
            if latest_report_from_db:
                # Define um limite de frescor para o relatório completo (ex: 1 dia)
                report_freshness_threshold = datetime.now() - timedelta(days=1)
                report_date = datetime.fromisoformat(latest_report_from_db['timestamp'])
                if report_date > report_freshness_threshold:
                    logger.info(f"Usando relatório completo recente do DB (gerado em {report_date.strftime('%Y-%m-%d %H:%M')}).")
                    
                    companies_data_for_advanced = {}
                    for item in latest_report_from_db['full_report_data']:
                        companies_data_for_advanced[item['ticker']] = CompanyFinancialData(
                            ticker=item['ticker'],
                            company_name=item['company_name'],
                            cd_cvm=item['raw_data'].get('cd_cvm'),
                            sector=item['raw_data'].get('sector'),
                            market_cap=item['market_cap'],
                            stock_price=item['stock_price'],
                            shares_outstanding=item['raw_data'].get('shares_outstanding', 0),
                            revenue=item['raw_data'].get('revenue', 0),
                            ebit=item['raw_data'].get('ebit', 0),
                            net_income=item['raw_data'].get('net_income', 0),
                            depreciation_amortization=item['raw_data'].get('depreciation_amortization', 0),
                            capex=item['raw_data'].get('capex', 0),
                            total_assets=item['raw_data'].get('total_assets', 0),
                            total_debt=item['raw_data'].get('total_debt', 0),
                            equity=item['raw_data'].get('equity', 0),
                            current_assets=item['raw_data'].get('current_assets', 0),
                            current_liabilities=item['raw_data'].get('current_liabilities', 0),
                            cash=item['raw_data'].get('cash', 0),
                            accounts_receivable=item['raw_data'].get('accounts_receivable', 0),
                            inventory=item['raw_data'].get('inventory', 0),
                            accounts_payable=item['raw_data'].get('accounts_payable', 0),
                            property_plant_equipment=item['raw_data'].get('property_plant_equipment', 0),
                            timestamp_collected=item['raw_data'].get('timestamp_collected')
                        )
                    
                    opportunities = self.advanced_ranking.identify_opportunities(companies_data_for_advanced)
                    portfolio_weights = self.portfolio_optimizer.suggest_portfolio_allocation(companies_data_for_advanced, 'moderate')
                    portfolio_eva_abs, portfolio_eva_pct = self.portfolio_optimizer.calculate_portfolio_eva(portfolio_weights, companies_data_for_advanced)

                    latest_report_from_db['opportunities'] = clean_data_for_json(opportunities)
                    latest_report_from_db['portfolio_suggestion'] = {
                        "weights": clean_data_for_json(portfolio_weights),
                        "portfolio_eva_abs": float(portfolio_eva_abs) if not np.isnan(portfolio_eva_abs) else None,
                        "portfolio_eva_pct": float(portfolio_eva_pct) if not np.isnan(portfolio_eva_pct) else None
                    }
                    return latest_report_from_db
                else:
                    logger.info("Relatório completo do DB desatualizado. Executando nova análise.")

        self.monitor.start_timer("analise_completa_ibovespa")
        
        tickers_to_use = self.ibovespa_tickers
        if num_companies is not None and num_companies > 0:
            tickers_to_use = self.ibovespa_tickers[:num_companies]
            logger.info(f"Executando análise rápida para as primeiras {num_companies} empresas.")
        else:
            logger.info(f"Executando análise completa para {len(tickers_to_use)} empresas do Ibovespa.")

        companies_data = self._get_companies_data_for_valuation(tickers=tickers_to_use)

        if not companies_data:
            return {"status": "error", "message": "Nenhum dado coletado para análise."}

        logger.info("Gerando relatório de métricas para todas as empresas...")
        self.monitor.start_timer("geracao_relatorio_metricas")
        report_df = self.company_ranking.generate_ranking_report(companies_data)
        self.monitor.end_timer("geracao_relatorio_metricas")

        if report_df.empty:
            return {"status": "error", "message": "Relatório de métricas está vazio. Verifique os cálculos."}

        for ticker_key, company_data_obj in companies_data.items():
            metrics_for_db = report_df[report_df['ticker'] == ticker_key].to_dict(orient='records')[0] if not report_df[report_df['ticker'] == ticker_key].empty else {}
            metrics_for_db['raw_data'] = clean_data_for_json(company_data_obj.__dict__)
            self.db.save_company_metrics(company_data_obj, metrics_for_db)

        logger.info("Gerando rankings...")
        top_10_eva = self.company_ranking.rank_by_eva(report_df)[:10]
        top_10_efv = self.company_ranking.rank_by_efv(report_df)[:10]
        top_10_upside = self.company_ranking.rank_by_upside(report_df)[:10]
        top_10_combined = self.company_ranking.rank_by_combined_score(report_df)[:10]

        logger.info("Identificando oportunidades avançadas e clusters...")
        opportunities = self.advanced_ranking.identify_opportunities(companies_data)

        logger.info("Gerando sugestão de portfólio...")
        portfolio_weights = self.portfolio_optimizer.suggest_portfolio_allocation(companies_data, 'moderate')
        portfolio_eva_abs, portfolio_eva_pct = self.portfolio_optimizer.calculate_portfolio_eva(portfolio_weights, companies_data)

        total_companies_analyzed = len(report_df)
        positive_eva_count = (report_df['eva_percentual'] > 0).sum()
        positive_efv_count = (report_df['efv_percentual'] > 0).sum()
        
        avg_eva = report_df['eva_percentual'].mean()
        avg_efv = report_df['efv_percentual'].mean()
        avg_upside = report_df['upside_percentual'].mean()
        
        best_eva = report_df.loc[report_df['eva_percentual'].idxmax()] if not report_df['eva_percentual'].empty else {}
        best_efv = report_df.loc[report_df['efv_percentual'].idxmax()] if not report_df['efv_percentual'].empty else {}
        best_combined = report_df.loc[report_df['combined_score'].idxmax()] if not report_df['combined_score'].empty else {}


        final_report = {
            "status": "success",
            "timestamp": datetime.now().isoformat(),
            "total_companies_analyzed": total_companies_analyzed,
            "summary_statistics": {
                "positive_eva_count": int(positive_eva_count),
                "positive_efv_count": int(positive_efv_count),
                "avg_eva_percentual": float(avg_eva) if not np.isnan(avg_eva) else None,
                "avg_efv_percentual": float(avg_efv) if not np.isnan(avg_efv) else None,
                "avg_upside_percentual": float(avg_upside) if not np.isnan(avg_upside) else None,
                "best_company_eva": {
                    "ticker": best_eva.get('ticker'),
                    "company_name": best_eva.get('company_name'),
                    "eva_percentual": float(best_eva.get('eva_percentual')) if not pd.isna(best_eva.get('eva_percentual')) else None
                } if not best_eva.empty else {},
                 "best_company_efv": {
                    "ticker": best_efv.get('ticker'),
                    "company_name": best_efv.get('company_name'),
                    "efv_percentual": float(best_efv.get('efv_percentual')) if not pd.isna(best_efv.get('efv_percentual')) else None
                } if not best_efv.empty else {},
                "best_company_combined": {
                    "ticker": best_combined.get('ticker'),
                    "company_name": best_combined.get('company_name'),
                    "combined_score": float(best_combined.get('combined_score')) if not pd.isna(best_combined.get('combined_score')) else None
                } if not best_combined.empty else {},
            },
            "rankings": {
                "top_10_eva": clean_data_for_json([r.to_dict() for _, r in report_df.sort_values(by='eva_percentual', ascending=False).head(10).iterrows()]),
                "top_10_efv": clean_data_for_json([r.to_dict() for _, r in report_df.sort_values(by='efv_percentual', ascending=False).head(10).iterrows()]),
                "top_10_upside": clean_data_for_json([r.to_dict() for _, r in report_df.sort_values(by='upside_percentual', ascending=False).head(10).iterrows()]),
                "top_10_riqueza_atual": clean_data_for_json([r.to_dict() for _, r in report_df.sort_values(by='riqueza_atual', ascending=False).head(10).iterrows()]),
                "top_10_riqueza_futura": clean_data_for_json([r.to_dict() for _, r in report_df.sort_values(by='riqueza_futura', ascending=False).head(10).iterrows()]),
                "top_10_combined": clean_data_for_json([r.to_dict() for _, r in report_df.sort_values(by='combined_score', ascending=False).head(10).iterrows()])
            },
            "opportunities": clean_data_for_json(opportunities),
            "portfolio_suggestion": {
                "weights": clean_data_for_json(portfolio_weights),
                "portfolio_eva_abs": float(portfolio_eva_abs) if not np.isnan(portfolio_eva_abs) else None,
                "portfolio_eva_pct": float(portfolio_eva_pct) if not np.isnan(portfolio_eva_pct) else None
            },
            "full_report_data": clean_data_for_json(report_df.to_dict(orient='records')) # Dados brutos de todas as empresas
        }
        
        # Salvar o relatório completo no DB
        self.db.save_analysis_report({
            'report_name': 'Análise Completa Ibovespa' if num_companies is None else f'Análise Rápida ({num_companies} Empresas)',
            'report_type': 'full' if num_companies is None else 'quick',
            'execution_time_seconds': (datetime.now() - self.monitor.timers.get("analise_completa_ibovespa", datetime.now())).total_seconds(),
            'summary_statistics': final_report['summary_statistics'],
            'full_report_data': final_report['full_report_data']
        })

        self.monitor.end_timer("analise_completa_ibovespa")
        return final_report

    def get_company_analysis(self, ticker: str, cvm_code: Optional[int] = None) -> Dict:
        """
        Realiza a análise de uma empresa específica.
        Tenta buscar as últimas métricas do DB primeiro, se não encontrar, coleta do DB (via data_collector).
        """
        self.monitor.start_timer(f"analise_empresa_{ticker}")
        
        # Tenta buscar do DB primeiro
        latest_metrics_from_db = self.db.get_company_latest_metrics(ticker)
        
        freshness_threshold = datetime.now() - timedelta(days=7)

        if latest_metrics_from_db and latest_metrics_from_db['metrics']['raw_data'] and \
           latest_metrics_from_db['metrics']['raw_data'].get('timestamp_collected') and \
           datetime.fromisoformat(latest_metrics_from_db['metrics']['raw_data']['timestamp_collected']) > freshness_threshold:
            
            logger.info(f"Usando métricas recentes do DB para {ticker}.")
            return latest_metrics_from_db
        else:
            logger.info(f"Métricas do DB para {ticker} desatualizadas ou não encontradas. Coletando do DB (via data_collector).")

        # Se não houver dados recentes no DB ou não encontrados, coleta via data_collector (que lê do DB)
        # Precisamos do CVM_CODE para o data_collector
        if not cvm_code:
            cvm_code_row = self.ticker_mapping[self.ticker_mapping['TICKER'] == ticker]['CD_CVM']
            cvm_code = int(cvm_code_row.iloc[0]) if not cvm_code_row.empty else None

            if cvm_code is None:
                logger.warning(f"CVM code não encontrado para {ticker}. Pulando coleta.")
                return {"status": "error", "message": f"CVM code não encontrado para {ticker}"}

        company_data = self.collector.get_company_data(ticker, cvm_code)
        
        if not company_data:
            logger.error(f"Não foi possível coletar dados para a empresa {ticker} do banco de dados.")
            return {"status": "error", "message": f"Não foi possível coletar dados para {ticker}"}

        try:
            beta = 1.0 # Exemplo de beta, ajuste para o cálculo do modelo Hamada
            wacc = self.calculator._calculate_wacc(company_data, beta)
            eva_abs, eva_pct = self.calculator.calculate_eva(company_data, beta)
            efv_abs, efv_pct = self.calculator.calculate_efv(company_data, beta)
            riqueza_atual = self.calculator.calculate_riqueza_atual(company_data, beta)
            riqueza_futura = self.calculator.calculate_riqueza_futura(company_data)
            upside = self.calculator.calculate_upside(company_data, efv_abs) if not np.isnan(efv_abs) else np.nan
            
            result = {
                "status": "success",
                "ticker": company_data.ticker,
                "company_name": company_data.company_name,
                "metrics": {
                    "market_cap": float(company_data.market_cap),
                    "stock_price": float(company_data.stock_price),
                    "wacc_percentual": float(wacc * 100) if not np.isnan(wacc) else None,
                    "eva_abs": float(eva_abs) if not np.isnan(eva_abs) else None,
                    "eva_percentual": float(eva_pct) if not np.isnan(eva_pct) else None,
                    "efv_abs": float(efv_abs) if not np.isnan(efv_abs) else None,
                    "efv_percentual": float(efv_pct) if not np.isnan(efv_pct) else None,
                    "riqueza_atual": float(riqueza_atual) if riqueza_atual is not None else None,
                    "riqueza_futura": float(riqueza_futura) if riqueza_futura is not None else None,
                    "upside_percentual": float(upside) if not np.isnan(upside) else None,
                    "combined_score": None, # Não calcula score combinado para análise única aqui
                    "raw_data": clean_data_for_json({**company_data.__dict__, "timestamp_collected": datetime.now().isoformat()})
                }
            }
            
            # Salvar as métricas recém-coletadas e calculadas no DB
            self.db.save_company_metrics(company_data, result['metrics'])

            return result
        except Exception as e:
            logger.error(f"Erro ao calcular métricas para {ticker}: {e}")
            return {"status": "error", "message": f"Erro ao processar dados para {ticker}: {str(e)}"}
        finally:
            self.monitor.end_timer(f"analise_empresa_{ticker}")

    def get_ibovespa_company_list(self) -> List[Dict]:
        """Retorna a lista de empresas do Ibovespa com tickers formatados e CVM_CODE."""
        ibov_companies_in_map = self.ticker_mapping[
            self.ticker_mapping['TICKER'].isin(self.ibovespa_tickers)
        ].copy()
        
        return [
            {
                'ticker': row['TICKER'],
                'ticker_clean': row['TICKER'].replace('.SA', ''),
                'company_name': row['NOME_EMPRESA'],
                'cvm_code': str(row['CD_CVM'])
            }
            for _, row in ibov_companies_in_map.iterrows()
        ]
