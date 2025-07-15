# core/valuation_analysis.py
import pandas as pd
import requests
import warnings
import numpy as np
import logging
from scipy import stats
from functools import lru_cache
from typing import Dict, List, Any
from .market_data.ticker import Ticker # <- IMPORTANDO NOSSO MÓDULO LOCAL

# --- Configuração Básica ---
warnings.filterwarnings("ignore")
logger = logging.getLogger(__name__)

# --- Configurações (sem alterações) ---
VALUATION_CONFIG = {
    "PERIODO_BETA_IBOV": "5y",
    "CONTAS_CVM": { "EBIT": "3.05", "IMPOSTO_DE_RENDA_CSLL": "3.10", "LUCRO_ANTES_IMPOSTOS": "3.09", "ATIVO_NAO_CIRCULANTE": "1.02", "CAIXA_EQUIVALENTES": "1.01.01", "DIVIDA_CURTO_PRAZO": "2.01.04", "DIVIDA_LONGO_PRAZO": "2.02.01", "CONTAS_A_RECEBER": "1.01.03", "ESTOQUES": "1.01.04", "FORNECEDORES": "2.01.02", "DESPESAS_FINANCEIRAS": "3.07" },
    "EMPRESAS_EXCLUIDAS": ['ITUB4', 'BBDC4', 'BBAS3', 'SANB11', 'B3SA3']
}

# --- Funções de Lógica de Negócio ---

@lru_cache(maxsize=1)
def obter_dados_mercado() -> Dict[str, Any]:
    dados = {"risk_free_rate": 0.105, "premio_risco_mercado": 0.08, "cresc_perpetuo": 0.03}
    try:
        selic_url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.432/dados/ultimos/1?formato=json"
        response = requests.get(selic_url, timeout=10)
        response.raise_for_status()
        dados["risk_free_rate"] = float(response.json()[0]["valor"]) / 100.0
    except Exception as e:
        logger.warning(f"Não foi possível obter a SELIC do BCB. Usando valor padrão. Erro: {e}")

    try:
        ibov_ticker = Ticker('^BVSP')
        ibov_df = ibov_ticker.price_history(period=VALUATION_CONFIG["PERIODO_BETA_IBOV"])
        if ibov_df.empty: raise ValueError("Download do IBOV retornou um DataFrame vazio.")
        ibov_df.rename(columns={'close': 'Adj Close'}, inplace=True)
        dados["ibov_data"] = ibov_df
    except Exception as e:
        logger.error(f"Falha ao baixar dados do IBOV. Erro: {e}")
        dados["ibov_data"] = pd.DataFrame()
    return dados

def obter_valor_recente(df_empresa: pd.DataFrame, codigo_conta: str) -> float:
    historico = obter_historico_metrica(df_empresa, codigo_conta)
    return historico.iloc[-1] if not historico.empty else 0.0

def obter_historico_metrica(df_empresa: pd.DataFrame, codigo_conta: str) -> pd.Series:
    metric_df = df_empresa[(df_empresa["cd_conta"] == codigo_conta) & (df_empresa["ordem_exerc"] == "ÚLTIMO")]
    if metric_df.empty: return pd.Series(dtype=float)
    metric_df = metric_df.copy()
    metric_df["dt_refer"] = pd.to_datetime(metric_df["dt_refer"])
    return metric_df.set_index("dt_refer")["vl_conta"].sort_index()

def calcular_beta(ticker: str, ibov_data: pd.DataFrame) -> float:
    if ibov_data.empty: return 1.0
    try:
        acao_ticker = Ticker(ticker)
        dados_acao = acao_ticker.price_history(period=VALUATION_CONFIG["PERIODO_BETA_IBOV"])
        if dados_acao.empty or len(dados_acao) < 60: return 1.0
        
        dados_acao.rename(columns={'close': 'Adj Close'}, inplace=True)
        dados_combinados = pd.concat([dados_acao["Adj Close"], ibov_data["Adj Close"]], axis=1).dropna()
        retornos = dados_combinados.pct_change().dropna()
        if len(retornos) < 50: return 1.0
        
        retornos.columns = ['Acao', 'Ibov']
        slope, _, _, _, _ = stats.linregress(retornos['Ibov'], retornos['Acao'])
        beta_ajustado = 0.67 * slope + 0.33 * 1.0
        return beta_ajustado if not np.isnan(beta_ajustado) else 1.0
    except Exception as e:
        logger.warning(f"Não foi possível calcular o Beta para {ticker}. Usando valor 1.0. Erro: {e}")
        return 1.0

def processar_valuation_empresa(ticker: str, df_empresa: pd.DataFrame, market_data: Dict[str, Any], n_acoes: float) -> Dict[str, Any] | None:
    try:
        if df_empresa.empty: return None

        ticker_obj = Ticker(ticker)
        summary_df = ticker_obj.summary()
        if summary_df.empty:
            logger.warning(f"Não foi possível obter o sumário de mercado para {ticker}")
            return None
        
        summary_info = summary_df.iloc[0]
        market_cap = summary_info.get("market_cap")
        preco_atual = summary_info.get("price")
        
        try:
             nome_empresa = Ticker(ticker).profile()['long_name'].iloc[0]
        except:
             nome_empresa = ticker

        if not all([market_cap, preco_atual, n_acoes, n_acoes > 0, market_cap > 0]):
            return None

        C = VALUATION_CONFIG["CONTAS_CVM"]
        hist_ebit = obter_historico_metrica(df_empresa, C["EBIT"])
        if hist_ebit.empty or hist_ebit.iloc[-1] == 0: return None

        imposto_total = obter_historico_metrica(df_empresa, C["IMPOSTO_DE_RENDA_CSLL"]).sum()
        lucro_antes_ir = obter_historico_metrica(df_empresa, C["LUCRO_ANTES_IMPOSTOS"]).sum()
        aliquota_efetiva = abs(imposto_total / lucro_antes_ir) if lucro_antes_ir != 0 else 0.34
        aliquota_efetiva = max(0, min(aliquota_efetiva, 0.45))
        nopat_recente = hist_ebit.iloc[-1] * (1 - aliquota_efetiva)
        ncg = (obter_valor_recente(df_empresa, C["CONTAS_A_RECEBER"]) + obter_valor_recente(df_empresa, C["ESTOQUES"]) - obter_valor_recente(df_empresa, C["FORNECEDORES"]))
        ativo_nao_circulante = obter_valor_recente(df_empresa, C["ATIVO_NAO_CIRCULANTE"])
        capital_empregado = ncg + ativo_nao_circulante

        if capital_empregado <= 0: return None

        roic = nopat_recente / capital_empregado
        beta = calcular_beta(ticker, market_data["ibov_data"])
        ke = market_data["risk_free_rate"] + beta * market_data["premio_risco_mercado"]
        divida_total = (obter_valor_recente(df_empresa, C["DIVIDA_CURTO_PRAZO"]) + obter_valor_recente(df_empresa, C["DIVIDA_LONGO_PRAZO"]))
        despesa_financeira = abs(obter_valor_recente(df_empresa, C["DESPESAS_FINANCEIRAS"]))
        kd = min(despesa_financeira / divida_total, 0.35) if divida_total > 0 and despesa_financeira > 0 else ke * 0.7
        valor_total = market_cap + divida_total
        if valor_total <= 0: return None
        w_e = market_cap / valor_total
        w_d = divida_total / valor_total
        wacc = (w_e * ke) + (w_d * kd * (1 - aliquota_efetiva))
        g = market_data["cresc_perpetuo"]
        if wacc <= g: return None
        eva = (roic - wacc) * capital_empregado
        valor_firma = capital_empregado + (eva * (1 + g)) / (wacc - g)
        divida_liquida = divida_total - obter_valor_recente(df_empresa, C["CAIXA_EQUIVALENTES"])
        equity_value = valor_firma - divida_liquida
        preco_justo = equity_value / n_acoes
        upside = (preco_justo / preco_atual) - 1 if preco_atual > 0 else 0

        return {
            'Nome': nome_empresa[:30], 'Ticker': ticker,
            'Upside': upside, 'ROIC': roic, 'WACC': wacc, 'Spread': roic - wacc,
            'Preco_Atual': preco_atual, 'Preco_Justo': preco_justo,
            'Market_Cap': market_cap, 'EVA': eva,
            'Capital_Empregado': capital_empregado, 'NOPAT': nopat_recente
        }
    except Exception as e:
        logger.error(f"Erro inesperado ao processar valuation para {ticker}: {e}", exc_info=False)
        return None

def run_full_valuation_analysis(df_empresa_unica: pd.DataFrame, ticker_map: pd.DataFrame) -> List[Dict[str, Any]]:
    if df_empresa_unica.empty: return []
    codigo_cvm = df_empresa_unica['cd_cvm'].iloc[0]
    ticker_info = ticker_map[ticker_map['CD_CVM'] == codigo_cvm]
    if ticker_info.empty:
        logger.warning(f"Ticker não encontrado para o CVM {codigo_cvm}. Pulando análise.")
        return []
    ticker = ticker_info['TICKER'].iloc[0]
    if ticker in VALUATION_CONFIG["EMPRESAS_EXCLUIDAS"]: return []
    
    # Busca o número de ações da CVM, que é mais confiável.
    try:
        n_acoes = float(df_empresa_unica[df_empresa_unica['cd_conta'] == '3.99.01.01']['vl_conta'].iloc[0])
    except (IndexError, TypeError):
        logger.warning(f"Não foi possível obter o número de ações da CVM para {ticker}. Pulando.")
        return []

    market_data = obter_dados_mercado()
    resultado = processar_valuation_empresa(ticker, df_empresa_unica, market_data, n_acoes)
    
    if not resultado: return []
    
    wacc_ok = 0.01 < resultado.get('WACC', 1) < 0.40
    upside_ok = -0.99 < resultado.get('Upside', 0) < 10.0
    if wacc_ok and upside_ok:
        return [resultado]
    else:
        logger.warning(f"Filtrando empresa {resultado['Ticker']} por resultados extremos: WACC={resultado.get('WACC', 'N/A'):.2%}, Upside={resultado.get('Upside', 'N/A'):.2%}")
        return []
