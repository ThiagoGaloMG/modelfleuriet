#!/usr/bin/env python3
import pandas as pd
import yfinance as yf
import requests
from pathlib import Path
import warnings
import numpy as np
import logging
from scipy import stats
from functools import lru_cache
from datetime import datetime
from typing import Dict, List, Any
import gc
import psutil
import os
import time
import random

# --- Configuração Básica ---
warnings.filterwarnings("ignore")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)
BASE_DIR = Path(__file__).resolve().parent

# --- Configurações Centralizadas para a Análise de Valuation ---
VALUATION_CONFIG = {
    "periodo_beta_ibov": "5y",
    "contas_cvm": {
        "ebit": "3.05", 
        "imposto_de_renda_csll": "3.10", 
        "lucro_antes_impostos": "3.09",
        "ativo_nao_circulante": "1.02", 
        "caixa_equivalentes": "1.01.01",
        "divida_curto_prazo": "2.01.04", 
        "divida_longo_prazo": "2.02.01",
        "contas_a_receber": "1.01.03", 
        "estoques": "1.01.04", 
        "fornecedores": "2.01.02",
        "despesas_financeiras": "3.07" 
    },
    "empresas_excluidas": ['itub4', 'bbdc4', 'bbas3', 'sanb11', 'b3sa3'],
    "max_retries": 3,
    "timeout": 15,
    "request_delay": 1.5,
    "max_delay": 60,
    "min_delay": 0.5
}

# --- Funções de Lógica de Negócio e Cálculos ---

@lru_cache(maxsize=1)
def obter_dados_mercado() -> Dict[str, Any]:
    dados = {
        "risk_free_rate": 0.105, 
        "premio_risco_mercado": 0.08, 
        "cresc_perpetuo": 0.03,
        "ibov_data": pd.DataFrame()
    }
    
    # Obter taxa livre de risco (SELIC)
    for attempt in range(VALUATION_CONFIG["max_retries"]):
        try:
            selic_url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.432/dados/ultimos/1?formato=json"
            response = requests.get(selic_url, timeout=VALUATION_CONFIG["timeout"])
            response.raise_for_status()
            dados["risk_free_rate"] = float(response.json()[0]["valor"]) / 100.0
            break
        except Exception as e:
            if attempt == VALUATION_CONFIG["max_retries"] - 1:
                logger.warning(f"Não foi possível obter a SELIC. Erro: {e}")
            continue

    # Obter dados do IBOV
    for attempt in range(VALUATION_CONFIG["max_retries"]):
        try:
            ibov_data = yf.download(
                "^BVSP", 
                period=VALUATION_CONFIG["periodo_beta_ibov"], 
                progress=False, 
                timeout=VALUATION_CONFIG["timeout"]
            )
            if not ibov_data.empty:
                dados["ibov_data"] = ibov_data
                break
        except Exception as e:
            if attempt == VALUATION_CONFIG["max_retries"] - 1:
                logger.error(f"Falha ao baixar dados do IBOV. Erro: {e}")
    
    return dados

def obter_valor_recente(df_empresa: pd.DataFrame, codigo_conta: str) -> float:
    try:
        historico = obter_historico_metrica(df_empresa, codigo_conta)
        return historico.iloc[-1] if not historico.empty else 0.0
    except Exception as e:
        logger.warning(f"Erro ao obter valor recente: {e}")
        return 0.0

def obter_historico_metrica(df_empresa: pd.DataFrame, codigo_conta: str) -> pd.Series:
    try:
        metric_df = df_empresa[
            (df_empresa["cd_conta"] == codigo_conta) & 
            (df_empresa["st_conta"] == "VALOR")
        ].copy()
        
        if metric_df.empty:
            return pd.Series(dtype=float)
            
        metric_df["dt_refer"] = pd.to_datetime(metric_df["dt_refer"])
        metric_df = metric_df.sort_values("dt_refer")
        return metric_df.set_index("dt_refer")["vl_conta"]
    except Exception as e:
        logger.warning(f"Erro ao obter histórico: {e}")
        return pd.Series(dtype=float)

def calcular_beta(ticker: str, ibov_data: pd.DataFrame) -> float:
    if ibov_data.empty: 
        return 1.0
    
    try:
        for attempt in range(VALUATION_CONFIG["max_retries"]):
            try:
                dados_acao = yf.download(
                    ticker, 
                    period=VALUATION_CONFIG["periodo_beta_ibov"], 
                    progress=False, 
                    timeout=VALUATION_CONFIG["timeout"]
                )
                
                if dados_acao.empty or len(dados_acao) < 60: 
                    return 1.0
                
                dados_combinados = pd.concat(
                    [dados_acao["Adj Close"], ibov_data["Adj Close"]], 
                    axis=1
                ).dropna()
                dados_combinados.columns = ['acao', 'ibov']
                
                retornos = dados_combinados.pct_change().dropna()
                if len(retornos) < 50: 
                    return 1.0
                
                slope, _, _, _, _ = stats.linregress(retornos['ibov'], retornos['acao'])
                beta_ajustado = 0.67 * slope + 0.33 * 1.0
                return max(0.1, min(beta_ajustado, 2.5))
            
            except Exception as e:
                if "429" in str(e):
                    wait_time = min(
                        VALUATION_CONFIG["max_delay"],
                        VALUATION_CONFIG["min_delay"] * (2 ** attempt) + random.uniform(0, 1)
                    )
                    logger.warning(f"Rate limit atingido. Aguardando {wait_time:.1f}s")
                    time.sleep(wait_time)
                else:
                    logger.warning(f"Erro ao calcular beta: {e}")
                    if attempt == VALUATION_CONFIG["max_retries"] - 1:
                        return 1.0
    except Exception as e:
        logger.warning(f"Erro geral ao calcular beta: {e}")
        return 1.0

def obter_info_yfinance(ticker_sa: str) -> Dict[str, Any]:
    info = {}
    
    for attempt in range(VALUATION_CONFIG["max_retries"]):
        try:
            ticker = yf.Ticker(ticker_sa)
            info = ticker.info
            if not info:
                hist = ticker.history(period="1d")
                if not hist.empty:
                    info = {
                        "marketcap": hist["Close"].iloc[-1] * ticker.info.get("sharesoutstanding", 0),
                        "currentprice": hist["Close"].iloc[-1],
                        "previousclose": hist["Close"].iloc[-1],
                        "sharesoutstanding": ticker.info.get("sharesoutstanding", 0)
                    }
            if info:
                break
                
        except Exception as e:
            if "429" in str(e):
                wait_time = min(
                    VALUATION_CONFIG["max_delay"],
                    VALUATION_CONFIG["min_delay"] * (2 ** attempt) + random.uniform(0, 1)
                )
                logger.warning(f"Rate limit atingido. Aguardando {wait_time:.1f}s")
                time.sleep(wait_time)
            else:
                logger.warning(f"Erro ao obter info: {e}")
                if attempt == VALUATION_CONFIG["max_retries"] - 1:
                    return {}
    
    return info

def processar_valuation_empresa(ticker_sa: str, df_empresa: pd.DataFrame, market_data: Dict[str, Any]) -> Dict[str, Any]:
    try:
        if df_empresa.empty:
            return None

        C = VALUATION_CONFIG["contas_cvm"]
        ticker = ticker_sa.replace('.sa', '')
        
        # Obter informações de mercado
        info = obter_info_yfinance(ticker_sa)
        if not info:
            return None
            
        market_cap = info.get("marketcap")
        preco_atual = info.get("currentprice", info.get("previousclose"))
        n_acoes = info.get("sharesoutstanding")

        if not all([market_cap, preco_atual, n_acoes]) or n_acoes <= 0 or market_cap <= 0:
            return None

        # Cálculo do NOPAT
        hist_ebit = obter_historico_metrica(df_empresa, C["ebit"])
        if hist_ebit.empty or hist_ebit.iloc[-1] == 0:
            return None

        # Cálculo da alíquota efetiva de impostos
        hist_imposto = obter_historico_metrica(df_empresa, C["imposto_de_renda_csll"])
        hist_lucro_antes_ir = obter_historico_metrica(df_empresa, C["lucro_antes_impostos"])
        
        df_tributos = pd.DataFrame({
            'imposto': hist_imposto,
            'lucro_antes_ir': hist_lucro_antes_ir
        }).dropna()
        
        df_tributos = df_tributos[df_tributos['lucro_antes_ir'] > 0]
        df_tributos['aliquota'] = df_tributos['imposto'].abs() / df_tributos['lucro_antes_ir']
        
        if len(df_tributos) >= 1:
            aliquota_efetiva = df_tributos['aliquota'].iloc[-1]
        else:
            aliquota_efetiva = 0.34
        
        aliquota_efetiva = max(0.15, min(aliquota_efetiva, 0.45))

        nopat_recente = hist_ebit.iloc[-1] * (1 - aliquota_efetiva)

        # Cálculo do Capital Empregado (Método corrigido)
        ativo_total = obter_valor_recente(df_empresa, "1")
        caixa = obter_valor_recente(df_empresa, C["caixa_equivalentes"])
        passivo_circulante_total = obter_valor_recente(df_empresa, "2.01")
        divida_curto = obter_valor_recente(df_empresa, C["divida_curto_prazo"])
        passivo_operacional = passivo_circulante_total - divida_curto
        
        capital_empregado = ativo_total - caixa - passivo_operacional

        if capital_empregado <= 0:
            return None

        # Cálculo do ROIC
        roic = nopat_recente / capital_empregado

        # Cálculo do WACC
        beta = calcular_beta(ticker_sa, market_data["ibov_data"])
        ke = market_data["risk_free_rate"] + beta * market_data["premio_risco_mercado"]

        divida_total = (
            obter_valor_recente(df_empresa, C["divida_curto_prazo"]) + 
            obter_valor_recente(df_empresa, C["divida_longo_prazo"])
        )
        despesa_financeira = abs(obter_valor_recente(df_empresa, C["despesas_financeiras"]))
        
        kd = min(despesa_financeira / divida_total, 0.35) if divida_total > 0 and despesa_financeira > 0 else ke * 0.7

        valor_total = market_cap + divida_total
        if valor_total <= 0:
            return None
            
        w_e = market_cap / valor_total
        w_d = divida_total / valor_total
        wacc = (w_e * ke) + (w_d * kd * (1 - aliquota_efetiva))

        if wacc <= 0.01 or wacc >= 0.40:
            return None

        # Cálculo do Valuation
        g = market_data["cresc_perpetuo"]
        if wacc <= g:
            return None

        eva = (roic - wacc) * capital_empregado
        valor_firma = capital_empregado + (eva * (1 + g)) / (wacc - g)
        
        divida_liquida = divida_total - obter_valor_recente(df_empresa, C["caixa_equivalentes"])
        equity_value = valor_firma - divida_liquida
        preco_justo = equity_value / n_acoes
        
        upside = (preco_justo / preco_atual) - 1 if preco_atual > 0 else 0

        if not (-0.99 < upside < 10.0):
            return None

        return {
            'nome': info.get('shortname', ticker)[:30], 
            'ticker': ticker,
            'upside': upside, 
            'roic': roic, 
            'wacc': wacc, 
            'spread': roic - wacc,
            'preco_atual': preco_atual, 
            'preco_justo': preco_justo,
            'market_cap': market_cap, 
            'eva': eva,
            'capital_empregado': capital_empregado, 
            'nopat': nopat_recente,
            'beta': beta,
            'data_calculo': datetime.now().strftime('%Y-%m-%d')
        }
    except Exception as e:
        logger.error(f"Erro ao processar valuation: {e}", exc_info=True)
        return None

def run_full_valuation_analysis(df_full_data: pd.DataFrame, ticker_map: pd.DataFrame) -> List[Dict[str, Any]]:
    logger.info(">>>>>> INICIANDO ANÁLISE DE VALUATION <<<<<<")
    process = psutil.Process()
    logger.info(f"Memória inicial: {process.memory_info().rss / (1024 * 1024):.2f} MB")
    
    df_full_data.columns = [col.lower() for col in df_full_data.columns]
    ticker_map.columns = [col.lower() for col in ticker_map.columns]
    
    df_full_data['cd_cvm'] = pd.to_numeric(df_full_data['cd_cvm'], errors='coerce').astype('Int64')
    ticker_map['cd_cvm'] = pd.to_numeric(ticker_map['cd_cvm'], errors='coerce').astype('Int64')
    
    logger.info(f"Dados financeiros: {len(df_full_data)} registros")
    logger.info(f"Tickers mapeados: {len(ticker_map)} empresas")
    
    market_data = obter_dados_mercado()
    resultados_brutos = []
    
    for idx, row in enumerate(ticker_map.itertuples()):
        ticker = row.ticker
        codigo_cvm = row.cd_cvm
        nome_empresa = row.nome_empresa
        
        if ticker in VALUATION_CONFIG["empresas_excluidas"]:
            logger.info(f"Pulando empresa excluída: {ticker}")
            continue
        
        df_empresa_atual = df_full_data[df_full_data['cd_cvm'] == codigo_cvm].copy()
        
        if df_empresa_atual.empty:
            logger.warning(f"Dados não encontrados: {ticker} (CVM: {codigo_cvm})")
            continue

        try:
            logger.info(f"Processando: {ticker} ({nome_empresa})")
            resultado = processar_valuation_empresa(
                f"{ticker.upper()}.sa", 
                df_empresa_atual, 
                market_data
            )
            
            if resultado:
                resultado['cd_cvm'] = int(codigo_cvm)
                resultados_brutos.append(resultado)
                logger.info(f"Valuation calculado: {ticker} | Upside {resultado.get('upside', 0):.2%}")
            else:
                logger.warning(f"Falha no cálculo: {ticker}")
                
        except Exception as e:
            logger.error(f"Erro ao processar {ticker}: {str(e)}", exc_info=True)
        
        if len(resultados_brutos) % 5 == 0:
            gc.collect()
            logger.info(f"Memória intermediária: {process.memory_info().rss / (1024 * 1024):.2f} MB")
            
        if idx < len(ticker_map) - 1:
            delay = VALUATION_CONFIG["request_delay"] + random.uniform(-0.2, 0.2)
            time.sleep(delay)

    resultados_filtrados = [
        r for r in resultados_brutos 
        if r and 0.01 < r.get('wacc', 1) < 0.40 and -0.99 < r.get('upside', 0) < 10.0
    ]

    total_calculado = len(resultados_brutos)
    total_filtrado = len(resultados_filtrados)
    logger.info(f">>>>>> CONCLUSÃO: {total_filtrado}/{total_calculado} empresas <<<<<<")
    logger.info(f"Memória final: {process.memory_info().rss / (1024 * 1024):.2f} MB")
    
    return resultados_filtrados
