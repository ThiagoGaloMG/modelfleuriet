# modelfleuriet/core/db_manager.py

import psycopg2
import os
import json
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List
import pandas as pd
from sqlalchemy import create_engine, text, inspect

logger = logging.getLogger(__name__)

class SupabaseDB: # Mantive o nome da classe SupabaseDB por consistência com o que já gerei
    """
    Gerencia a conexão e operações com o banco de dados PostgreSQL (Render).
    """
    def __init__(self):
        self.conn_string = os.environ.get("DATABASE_URL")
        if not self.conn_string:
            logger.error("DATABASE_URL não configurada no ambiente. Conexão ao DB falhará.")
            # Para teste local, você precisaria de um .env com as variáveis abaixo
            # user = os.environ.get('DB_USER', 'postgres')
            # password = os.environ.get('DB_PASSWORD', 'sua_senha_local')
            # host = os.environ.get('DB_HOST', 'localhost')
            # port = os.environ.get('DB_PORT', '5432')
            # dbname = os.environ.get('DB_NAME', 'postgres')
            # self.conn_string = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
            # logger.warning("Usando variáveis de ambiente locais para conexão DB.")
        self._engine = None

    def get_engine(self):
        """Retorna uma engine SQLAlchemy."""
        if self._engine is None:
            if not self.conn_string:
                logger.error("String de conexão do DB não disponível para criar engine.")
                return None
            conn_str_sqlalchemy = self.conn_string.replace("postgresql://", "postgresql+psycopg2://", 1)
            try:
                self._engine = create_engine(conn_str_sqlalchemy, pool_pre_ping=True)
                with self._engine.connect():
                    logger.info("Conexão com a engine do banco de dados estabelecida com sucesso.")
            except Exception as e:
                logger.error(f"Falha ao criar a engine do banco de dados: {e}", exc_info=True)
                self._engine = None
                raise
        return self._engine

    def _get_connection(self):
        """Estabelece e retorna uma conexão psycopg2 direta."""
        if not self.conn_string:
            logger.error("String de conexão do DB não disponível para conexão direta.")
            raise ValueError("String de conexão do DB não disponível.")
        try:
            conn = psycopg2.connect(self.conn_string)
            return conn
        except Exception as e:
            logger.error(f"Erro ao conectar ao PostgreSQL: {e}")
            raise

    def save_analysis_report(self, report_data: Dict[str, Any]):
        """
        Salva os dados de um relatório de análise completo.
        """
        conn = None
        try:
            conn = self._get_connection()
            cur = conn.cursor()

            report_name = report_data.get('report_name', 'Análise Não Especificada')
            report_type = report_data.get('report_type', 'unknown')
            execution_time_seconds = report_data.get('execution_time_seconds')

            report_summary_json = json.dumps(report_data.get('summary_statistics', {}))
            full_ranking_data_json = json.dumps(report_data.get('full_report_data', []))
            
            cur.execute(
                """
                INSERT INTO public.analysis_reports (
                    report_name, report_type, execution_time_seconds, report_summary, full_ranking_data
                ) VALUES (%s, %s, %s, %s::jsonb, %s::jsonb)
                RETURNING id;
                """,
                (report_name, report_type, execution_time_seconds, report_summary_json, full_ranking_data_json)
            )
            report_id = cur.fetchone()[0]
            conn.commit()
            logger.info(f"Relatório de análise '{report_name}' salvo com ID: {report_id}")
            return str(report_id)
        except Exception as e:
            if conn: conn.rollback()
            logger.error(f"Erro ao salvar relatório no PostgreSQL: {e}")
            return None
        finally:
            if conn: conn.close()
    
    def save_company_metrics(self, company_data_obj: Any, metrics_data_dict: Dict[str, Any]):
        """
        Salva as métricas calculadas para uma única empresa.
        company_data_obj é uma instância de CompanyFinancialData.
        """
        conn = None
        try:
            conn = self._get_connection()
            cur = conn.cursor()

            # 1. Inserir ou atualizar na tabela companies (para garantir que a empresa existe)
            cur.execute(
                """
                INSERT INTO public.companies (ticker, company_name, sector, last_updated)
                VALUES (%s, %s, %s, now())
                ON CONFLICT (ticker) DO UPDATE SET
                    company_name = EXCLUDED.company_name,
                    sector = EXCLUDED.sector,
                    last_updated = now()
                RETURNING id;
                """,
                (company_data_obj.ticker, company_data_obj.company_name, company_data_obj.sector)
            )
            company_id = cur.fetchone()[0]

            # 2. Inserir as métricas financeiras detalhadas
            raw_data_json = json.dumps(metrics_data_dict.get('raw_data', {}))
            
            cur.execute(
                """
                INSERT INTO public.financial_metrics (
                    company_id, analysis_date, market_cap, stock_price,
                    wacc_percentual, eva_abs, eva_percentual, efv_abs, efv_percentual,
                    riqueza_atual, riqueza_futura, upside_percentual, combined_score, raw_data
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb);
                """,
                (
                    company_id, datetime.now(),
                    metrics_data_dict.get('market_cap'), metrics_data_dict.get('stock_price'),
                    metrics_data_dict.get('wacc_percentual'), metrics_data_dict.get('eva_abs'),
                    metrics_data_dict.get('eva_percentual'), metrics_data_dict.get('efv_abs'),
                    metrics_data_dict.get('efv_percentual'), metrics_data_dict.get('riqueza_atual'),
                    metrics_data_dict.get('riqueza_futura'), metrics_data_dict.get('upside_percentual'),
                    metrics_data_dict.get('combined_score'), raw_data_json
                )
            )
            conn.commit()
            logger.info(f"Métricas para {company_data_obj.ticker} salvas com sucesso.")
        except Exception as e:
            if conn: conn.rollback()
            logger.error(f"Erro ao salvar métricas da empresa no PostgreSQL para {company_data_obj.ticker}: {e}")
        finally:
            if conn: conn.close()

    def get_latest_full_analysis_report(self) -> Optional[Dict[str, Any]]:
        """Busca o relatório de análise completa mais recente do banco de dados."""
        conn = None
        try:
            conn = self._get_connection()
            cur = conn.cursor()
            cur.execute(
                """
                SELECT report_summary, full_ranking_data, report_date, execution_time_seconds, report_name, report_type
                FROM public.analysis_reports
                ORDER BY report_date DESC
                LIMIT 1;
                """
            )
            result = cur.fetchone()
            if result:
                summary_json, ranking_json, report_date, exec_time, report_name, report_type = result
                summary = json.loads(summary_json) if isinstance(summary_json, str) else summary_json
                ranking = json.loads(ranking_json) if isinstance(ranking_json, str) else ranking_json
                
                return {
                    "status": "success",
                    "timestamp": report_date.isoformat(),
                    "total_companies_analyzed": summary.get('total_companies_analyzed', 0),
                    "summary_statistics": summary,
                    "full_report_data": ranking,
                    "report_name": report_name,
                    "report_type": report_type,
                    "execution_time_seconds": float(exec_time)
                }
            return None
        except Exception as e:
            logger.error(f"Erro ao buscar relatório completo mais recente do PostgreSQL: {e}")
            return None
        finally:
            if conn: conn.close()

    def get_company_latest_metrics(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Busca as métricas mais recentes de uma empresa específica."""
        conn = None
        try:
            conn = self._get_connection()
            cur = conn.cursor()
            cur.execute(
                """
                SELECT
                    fm.market_cap, fm.stock_price, fm.wacc_percentual, fm.eva_abs, fm.eva_percentual,
                    fm.efv_abs, fm.efv_percentual, fm.riqueza_atual, fm.riqueza_futura,
                    fm.upside_percentual, fm.combined_score, fm.raw_data, c.company_name, c.ticker
                FROM public.financial_metrics fm
                JOIN public.companies c ON fm.company_id = c.id
                WHERE c.ticker = %s
                ORDER BY fm.analysis_date DESC
                LIMIT 1;
                """,
                (ticker,)
            )
            result = cur.fetchone()
            if result:
                (market_cap, stock_price, wacc_pct, eva_abs, eva_pct, efv_abs, efv_pct,
                 riqueza_atual, riqueza_futura, upside_pct, combined_score, raw_data_json,
                 company_name, ticker_from_db) = result
                
                raw_data = json.loads(raw_data_json) if isinstance(raw_data_json, str) else raw_data_json

                return {
                    "status": "success",
                    "ticker": ticker_from_db,
                    "company_name": company_name,
                    "metrics": {
                        "market_cap": float(market_cap) if market_cap is not None else None,
                        "stock_price": float(stock_price) if stock_price is not None else None,
                        "wacc_percentual": float(wacc_pct) if wacc_pct is not None else None,
                        "eva_abs": float(eva_abs) if eva_abs is not None else None,
                        "eva_percentual": float(eva_pct) if eva_pct is not None else None,
                        "efv_abs": float(efv_abs) if efv_abs is not None else None,
                        "efv_percentual": float(efv_pct) if efv_pct is not None else None,
                        "riqueza_atual": float(riqueza_atual) if riqueza_atual is not None else None,
                        "riqueza_futura": float(riqueza_futura) if riqueza_futura is not None else None,
                        "upside_percentual": float(upside_pct) if upside_pct is not None else None,
                        "combined_score": float(combined_score) if combined_score is not None else None,
                        "raw_data": raw_data
                    }
                }
            return None
        except Exception as e:
            logger.error(f"Erro ao buscar métricas da empresa {ticker} do PostgreSQL: {e}")
            return None
        finally:
            if conn: conn.close()

    def get_companies_for_fleuriet_dropdown(self) -> List[Dict]:
        """
        Busca a lista de empresas com dados financeiros na tabela 'financial_data'
        para popular o dropdown do Modelo Fleuriet.
        """
        engine = self.get_engine()
        if not engine:
            logger.error("Engine do DB não disponível para listar empresas Fleuriet.")
            return []
        try:
            # Esta query busca empresas que tenham dados na tabela financial_data
            # e tenta mapear com tickers se mapeamento_tickers.csv foi carregado.
            query = text("""
                SELECT DISTINCT fd."DENOM_CIA" AS company_name, fd."CD_CVM" AS cvm_code
                FROM public.financial_data fd
                ORDER BY fd."DENOM_CIA";
            """)
            with engine.connect() as connection:
                df_companies = pd.read_sql(query, connection)
            
            # Opcional: tentar mapear tickers se o mapeamento_tickers.csv for carregado em flask_app.py
            # e passado para cá, ou se tiver uma tabela 'companies' no DB que já tenha tickers.
            # Por enquanto, retorna apenas CVM e nome.
            return df_companies.to_dict(orient='records')
        except Exception as e:
            logger.error(f"Erro ao buscar empresas para dropdown Fleuriet do DB: {e}")
            return []
