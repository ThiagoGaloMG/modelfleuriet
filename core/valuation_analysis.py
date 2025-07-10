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
    "PERIODO_BETA_IBOV": "5y",
    "CONTAS_CVM": {
        "EBIT": "3.05", 
        "IMPOSTO_DE_RENDA_CSLL": "3.10", 
        "LUCRO_ANTES_IMPOSTOS": "3.09",
        "ATIVO_NAO_CIRCULANTE": "1.02", 
        "CAIXA_EQUIVALENTES": "1.01.01",
        "DIVIDA_CURTO_PRAZO": "2.01.04", 
        "DIVIDA_LONGO_PRAZO": "2.02.01",
        "CONTAS_A_RECEBER": "1.01.03", 
        "ESTOQUES": "1.01.04", 
        "FORNECEDORES": "2.01.02",
        "DESPESAS_FINANCEIRAS": "3.07" 
    },
    "EMPRESAS_EXCLUIDAS": ['ITUB4', 'BBDC4', 'BBAS3', 'SANB11', 'B3SA3'],
    "MAX_RETRIES": 3,
    "TIMEOUT": 15,
    "REQUEST_DELAY": 1.5,  # Delay padrão entre requisições
    "MAX_DELAY": 60,       # Delay máximo para backoff exponencial
    "MIN_DELAY": 0.5       # Delay mínimo para backoff exponencial
}

# --- Funções de Lógica de Negócio e Cálculos ---

@lru_cache(maxsize=1)
def obter_dados_mercado() -> Dict[str, Any]:
    """Obtém premissas de mercado (taxa livre de risco, prêmio) e dados do IBOV para cálculo do Beta."""
    dados = {
        "risk_free_rate": 0.105, 
        "premio_risco_mercado": 0.08, 
        "cresc_perpetuo": 0.03,
        "ibov_data": pd.DataFrame()
    }
    
    # Obter taxa livre de risco (SELIC)
    for attempt in range(VALUATION_CONFIG["MAX_RETRIES"]):
        try:
            selic_url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.432/dados/ultimos/1?formato=json"
            response = requests.get(selic_url, timeout=VALUATION_CONFIG["TIMEOUT"])
            response.raise_for_status()
            dados["risk_free_rate"] = float(response.json()[0]["valor"]) / 100.0
            break
        except Exception as e:
            if attempt == VALUATION_CONFIG["MAX_RETRIES"] - 1:
                logger.warning(f"Não foi possível obter a SELIC do BCB. Usando valor padrão. Erro: {e}")
            continue

    # Obter dados do IBOV
    for attempt in range(VALUATION_CONFIG["MAX_RETRIES"]):
        try:
            ibov_data = yf.download(
                "^BVSP", 
                period=VALUATION_CONFIG["PERIODO_BETA_IBOV"], 
                progress=False, 
                timeout=VALUATION_CONFIG["TIMEOUT"]
            )
            if not ibov_data.empty:
                dados["ibov_data"] = ibov_data
                break
        except Exception as e:
            if attempt == VALUATION_CONFIG["MAX_RETRIES"] - 1:
                logger.error(f"Falha ao baixar dados do IBOV. O cálculo do Beta usará o valor padrão 1.0. Erro: {e}")
    
    return dados

def obter_valor_recente(df_empresa: pd.DataFrame, codigo_conta: str) -> float:
    """Obtém o valor mais recente de uma conta específica de um DataFrame de empresa."""
    try:
        historico = obter_historico_metrica(df_empresa, codigo_conta)
        return historico.iloc[-1] if not historico.empty else 0.0
    except Exception as e:
        logger.warning(f"Erro ao obter valor recente para conta {codigo_conta}: {e}")
        return 0.0

def obter_historico_metrica(df_empresa: pd.DataFrame, codigo_conta: str) -> pd.Series:
    """Extrai uma série temporal de uma métrica específica."""
    try:
        # As colunas do df_empresa já devem estar em minúsculas e sem aspas
        metric_df = df_empresa[
            (df_empresa["cd_conta"] == codigo_conta) & 
            (df_empresa["st_conta"] == "ÚLTIMO") # Assumindo que 'st_conta' é a coluna para 'ORDEM_EXERC'
        ].copy()
        
        if metric_df.empty:
            return pd.Series(dtype=float)
            
        metric_df["dt_refer"] = pd.to_datetime(metric_df["dt_refer"])
        return metric_df.set_index("dt_refer")["vl_conta"].sort_index()
    except Exception as e:
        logger.warning(f"Erro ao obter histórico para conta {codigo_conta}: {e}")
        return pd.Series(dtype=float)

def calcular_beta(ticker: str, ibov_data: pd.DataFrame) -> float:
    """Calcula o beta ajustado de uma ação em relação ao IBOV com tratamento de rate limiting."""
    if ibov_data.empty: 
        return 1.0
    
    try:
        # Implementação de backoff exponencial para rate limiting
        for attempt in range(VALUATION_CONFIG["MAX_RETRIES"]):
            try:
                dados_acao = yf.download(
                    ticker, 
                    period=VALUATION_CONFIG["PERIODO_BETA_IBOV"], 
                    progress=False, 
                    timeout=VALUATION_CONFIG["TIMEOUT"]
                )
                
                if dados_acao.empty or len(dados_acao) < 60: 
                    return 1.0
                
                dados_combinados = pd.concat(
                    [dados_acao["Adj Close"], ibov_data["Adj Close"]], 
                    axis=1
                ).dropna()
                dados_combinados.columns = ['Acao', 'Ibov']
                
                retornos = dados_combinados.pct_change().dropna()
                if len(retornos) < 50: 
                    return 1.0
                
                slope, _, _, _, _ = stats.linregress(retornos['Ibov'], retornos['Acao'])
                beta_ajustado = 0.67 * slope + 0.33 * 1.0
                return max(0.1, min(beta_ajustado, 2.5))  # Limites razoáveis para beta
            
            except Exception as e:
                if "429" in str(e):  # Tratamento específico para rate limiting
                    wait_time = min(
                        VALUATION_CONFIG["MAX_DELAY"],
                        VALUATION_CONFIG["MIN_DELAY"] * (2 ** attempt) + random.uniform(0, 1)
                    )
                    logger.warning(f"Rate limit atingido para {ticker}. Tentativa {attempt+1}. Aguardando {wait_time:.1f}s")
                    time.sleep(wait_time)
                else:
                    logger.warning(f"Erro ao calcular beta para {ticker}: {e}")
                    if attempt == VALUATION_CONFIG["MAX_RETRIES"] - 1:
                        return 1.0
    except Exception as e:
        logger.warning(f"Erro geral ao calcular beta para {ticker}: {e}")
        return 1.0

def obter_info_yfinance(ticker_sa: str) -> Dict[str, Any]:
    """Obtém informações do yfinance com tratamento de rate limiting e retry."""
    info = {}
    
    # Implementação de backoff exponencial para rate limiting
    for attempt in range(VALUATION_CONFIG["MAX_RETRIES"]):
        try:
            ticker = yf.Ticker(ticker_sa)
            info = ticker.info
            if not info:
                # Tentar obter informações básicas se o info estiver vazio
                hist = ticker.history(period="1d")
                if not hist.empty:
                    info = {
                        "marketCap": hist["Close"].iloc[-1] * ticker.info.get("sharesOutstanding", 0),
                        "currentPrice": hist["Close"].iloc[-1],
                        "previousClose": hist["Close"].iloc[-1],
                        "sharesOutstanding": ticker.info.get("sharesOutstanding", 0)
                    }
            if info:  # Se conseguiu informações, sai do loop
                break
                
        except Exception as e:
            if "429" in str(e):  # Tratamento específico para rate limiting
                wait_time = min(
                    VALUATION_CONFIG["MAX_DELAY"],
                    VALUATION_CONFIG["MIN_DELAY"] * (2 ** attempt) + random.uniform(0, 1)
                )
                logger.warning(f"Rate limit atingido para {ticker_sa}. Tentativa {attempt+1}. Aguardando {wait_time:.1f}s")
                time.sleep(wait_time)
            else:
                logger.warning(f"Erro ao obter info do yfinance para {ticker_sa}: {e}")
                if attempt == VALUATION_CONFIG["MAX_RETRIES"] - 1:
                    return {}
    
    return info

def processar_valuation_empresa(ticker_sa: str, df_empresa: pd.DataFrame, market_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Executa todos os cálculos de valuation para uma única empresa.
    Recebe o DataFrame já filtrado para a empresa.
    """
    try:
        if df_empresa.empty:
            return None

        C = VALUATION_CONFIG["CONTAS_CVM"]
        ticker = ticker_sa.replace(".SA", "")
        
        # Obter informações de mercado
        info = obter_info_yfinance(ticker_sa)
        if not info:
            return None
            
        market_cap = info.get("marketCap")
        preco_atual = info.get("currentPrice", info.get("previousClose"))
        n_acoes = info.get("sharesOutstanding")

        # Validações básicas
        if not all([market_cap, preco_atual, n_acoes]) or n_acoes <= 0 or market_cap <= 0:
            return None

        # Cálculo do NOPAT
        hist_ebit = obter_historico_metrica(df_empresa, C["EBIT"])
        if hist_ebit.empty or hist_ebit.iloc[-1] == 0:
            return None

        # Cálculo da alíquota efetiva de impostos
        imposto_total = obter_historico_metrica(df_empresa, C["IMPOSTO_DE_RENDA_CSLL"]).sum()
        lucro_antes_ir = obter_historico_metrica(df_empresa, C["LUCRO_ANTES_IMPOSTOS"]).sum()
        aliquota_efetiva = abs(imposto_total / lucro_antes_ir) if lucro_antes_ir != 0 else 0.34
        aliquota_efetiva = max(0.15, min(aliquota_efetiva, 0.45))  # Limites razoáveis

        nopat_recente = hist_ebit.iloc[-1] * (1 - aliquota_efetiva)

        # Cálculo do Capital Empregado
        ncg = (
            obter_valor_recente(df_empresa, C["CONTAS_A_RECEBER"]) + 
            obter_valor_recente(df_empresa, C["ESTOQUES"]) - 
            obter_valor_recente(df_empresa, C["FORNECEDORES"])
        )
        
        ativo_nao_circulante = obter_valor_recente(df_empresa, C["ATIVO_NAO_CIRCULANTE"])
        capital_empregado = ncg + ativo_nao_circulante

        if capital_empregado <= 0:
            return None

        # Cálculo do ROIC
        roic = nopat_recente / capital_empregado

        # Cálculo do WACC
        beta = calcular_beta(ticker_sa, market_data["ibov_data"])
        ke = market_data["risk_free_rate"] + beta * market_data["premio_risco_mercado"]

        divida_total = (
            obter_valor_recente(df_empresa, C["DIVIDA_CURTO_PRAZO"]) + 
            obter_valor_recente(df_empresa, C["DIVIDA_LONGO_PRAZO"])
        )
        despesa_financeira = abs(obter_valor_recente(df_empresa, C["DESPESAS_FINANCEIRAS"]))
        
        kd = min(despesa_financeira / divida_total, 0.35) if divida_total > 0 and despesa_financeira > 0 else ke * 0.7

        valor_total = market_cap + divida_total
        if valor_total <= 0:
            return None
            
        w_e = market_cap / valor_total
        w_d = divida_total / valor_total
        wacc = (w_e * ke) + (w_d * kd * (1 - aliquota_efetiva))

        # Validação do WACC
        if wacc <= 0.01 or wacc >= 0.40:
            return None

        # Cálculo do Valuation
        g = market_data["cresc_perpetuo"]
        if wacc <= g:
            return None

        eva = (roic - wacc) * capital_empregado
        valor_firma = capital_empregado + (eva * (1 + g)) / (wacc - g)
        
        divida_liquida = divida_total - obter_valor_recente(df_empresa, C["CAIXA_EQUIVALENTES"])
        preco_justo = (valor_firma - divida_liquida) / n_acoes
        
        upside = (preco_justo / preco_atual) - 1 if preco_atual > 0 else 0

        # Validação final dos resultados
        if not (-0.99 < upside < 10.0):  # Limites razoáveis para upside
            return None

        return {
            'Nome': info.get('shortName', ticker)[:30], 
            'Ticker': ticker,
            'Upside': upside, 
            'ROIC': roic, 
            'WACC': wacc, 
            'Spread': roic - wacc,
            'Preco_Atual': preco_atual, 
            'Preco_Justo': preco_justo,
            'Market_Cap': market_cap, 
            'EVA': eva,
            'Capital_Empregado': capital_empregado, 
            'NOPAT': nopat_recente,
            'Beta': beta,
            'Data_Calculo': datetime.now().strftime('%Y-%m-%d')
        }
    except Exception as e:
        logger.error(f"Erro ao processar valuation para {ticker_sa}: {e}", exc_info=True)
        return None

def run_full_valuation_analysis(df_full_data: pd.DataFrame, ticker_map: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Função principal que orquestra a análise de valuation para todas as empresas.
    Recebe os dados financeiros e o mapa de tickers como parâmetros.
    """
    logger.info(">>>>>> INICIANDO ANÁLISE DE VALUATION PARA TODAS AS EMPRESAS <<<<<<")
    process = psutil.Process()
    logger.info(f"Memória inicial: {process.memory_info().rss / (1024 * 1024):.2f} MB")
    
    df_full_data.columns = [col.lower().replace('"', '') for col in df_full_data.columns]
    ticker_map.columns = [col.lower().replace('"', '') for col in ticker_map.columns]
    # Converter cd_cvm para int em ambos os DataFrames para compatibilidade
    df_full_data['cd_cvm'] = pd.to_numeric(df_full_data['cd_cvm'], errors='coerce').astype('Int64')
    ticker_map['cd_cvm'] = pd.to_numeric(ticker_map['cd_cvm'], errors='coerce').astype('Int64')
    
    logger.info(f"Dados financeiros recebidos: {len(df_full_data)} registros")
    logger.info(f"Tickers mapeados: {len(ticker_map)} empresas")
    
    # Obter dados de mercado uma única vez
    market_data = obter_dados_mercado()
    resultados_brutos = []
    
    # Itera sobre o mapa de tickers
    for idx, row in enumerate(ticker_map.iterrows()):
        ticker = row[1]['ticker']
        codigo_cvm = row[1]['cd_cvm']
        nome_empresa = row[1]['nome_empresa']
        
        # Pular empresas na lista de exclusão
        if ticker in VALUATION_CONFIG["EMPRESAS_EXCLUIDAS"]:
            logger.info(f"Pulando empresa excluída: {ticker}")
            continue
        
        # Filtra os dados para a empresa atual (coluna 'cd_cvm' já deve estar em minúsculas)
        df_empresa_atual = df_full_data[df_full_data["cd_cvm"] == codigo_cvm].copy()
        
        if df_empresa_atual.empty:
            logger.warning(f"Nenhum dado financeiro encontrado para {ticker} (CVM: {codigo_cvm})")
            continue

        try:
            logger.info(f"Processando valuation para {ticker} ({nome_empresa})")
            resultado = processar_valuation_empresa(
                f"{ticker.upper()}.SA", 
                df_empresa_atual, 
                market_data
            )
            
            if resultado:
                # Adicionar informações básicas da empresa
                resultado['ticker'] = ticker
                resultado['nome'] = nome_empresa
                resultado['cd_cvm'] = int(codigo_cvm)
                
                resultados_brutos.append(resultado)
                logger.info(f"Valuation calculado para {ticker}: Upside {resultado.get('upside', 0):.2%}")
            else:
                logger.warning(f"Falha no cálculo de valuation para {ticker}")
                
        except Exception as e:
            logger.error(f"Erro ao processar {ticker}: {str(e)}", exc_info=True)
        
        # Liberar memória a cada 5 empresas
        if len(resultados_brutos) % 5 == 0:
            gc.collect()
            logger.info(f"Memória intermediária: {process.memory_info().rss / (1024 * 1024):.2f} MB")
            
        # Adicionar delay para evitar rate limiting
        if idx < len(ticker_map) - 1:
            delay = VALUATION_CONFIG["REQUEST_DELAY"] + random.uniform(-0.2, 0.2)
            time.sleep(delay)

    # Filtro final de sanidade
    resultados_filtrados = [
        r for r in resultados_brutos 
        if r and 0.01 < r.get('wacc', 1) < 0.40 and -0.99 < r.get('upside', 0) < 10.0
    ]

    total_calculado = len(resultados_brutos)
    total_filtrado = len(resultados_filtrados)
    logger.info(f">>>>>> ANÁLISE DE VALUATION CONCLUÍDA: {total_filtrado} de {total_calculado} empresas passaram no filtro. <<<<<<")
    logger.info(f"Memória final: {process.memory_info().rss / (1024 * 1024):.2f} MB")
    
    return resultados_filtrados
