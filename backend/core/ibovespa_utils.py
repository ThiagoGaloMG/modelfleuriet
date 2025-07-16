# modelfleuriet/core/ibovespa_utils.py

import requests
from bs4 import BeautifulSoup
from typing import List, Optional
import logging
import re # Para expressões regulares

logger = logging.getLogger(__name__)

def get_ibovespa_tickers() -> List[str]:
    """
    Obtém a lista de tickers das empresas que compõem o Ibovespa.
    Prioriza uma lista manual atualizada, já que não usaremos APIs externas para isso.
    """
    try:
        # Lista manual dos principais componentes do Ibovespa (atualizada com base em mapeamento_tickers.csv)
        # Esta lista é representativa e não exaustiva. Em um cenário real, você manteria
        # esta lista atualizada manualmente ou via um processo de ETL separado.
        ibovespa_tickers = [
            'ABEV3.SA', 'ALPA4.SA', 'ALUP11.SA', 'AMER3.SA', 'ARZZ3.SA', 'ASAI3.SA',
            'AZUL4.SA', 'B3SA3.SA', 'BBAS3.SA', 'BBDC3.SA', 'BBDC4.SA', 'BBSE3.SA',
            'BEEF3.SA', 'BPAC11.SA', 'BRAP4.SA', 'BRFS3.SA', 'BRKM5.SA', 'CASH3.SA',
            'CCRO3.SA', 'CMIG3.SA', 'CMIG4.SA', 'COGN3.SA', 'EMBR3.SA', 'ENBR3.SA',
            'ENEV3.SA', 'EQTL3.SA', 'EZTC3.SA', 'FLRY3.SA', 'GGBR3.SA', 'GGBR4.SA',
            'GOAU3.SA', 'GOAU4.SA', 'GOLL4.SA', 'HAPV3.SA', 'IRBR3.SA', 'ITSA3.SA',
            'ITSA4.SA', 'ITUB3.SA', 'ITUB4.SA', 'JBSS3.SA', 'KLBN11.SA', 'LREN3.SA',
            'LWSA3.SA', 'MGLU3.SA', 'MRFG3.SA', 'MRVE3.SA', 'NTCO3.SA', 'OMGE3.SA',
            'PCAR3.SA', 'PETR3.SA', 'PETR4.SA', 'PRIO3.SA', 'QUAL3.SA', 'RADL3.SA',
            'RAIL3.SA', 'RDOR3.SA', 'RENT3.SA', 'SANB11.SA', 'SBSP3.SA', 'SLCE3.SA',
            'SMTO3.SA', 'SUZB3.SA', 'TAEE11.SA', 'TIMS3.SA', 'TOTS3.SA', 'UGPA3.SA',
            'USIM3.SA', 'USIM5.SA', 'VALE3.SA', 'VIVT3.SA', 'WEGE3.SA', 'YDUQ3.SA',
        ]
        # Remover duplicatas e garantir o sufixo .SA (se não tiver)
        ibovespa_tickers = list(set([t.upper().strip() if t.endswith('.SA') else t.upper().strip() + '.SA' for t in ibovespa_tickers]))
        logger.info(f"Tickers do Ibovespa (lista manual): {len(ibovespa_tickers)} empresas.")
        return ibovespa_tickers
    except Exception as e:
        logger.error(f"Erro ao obter tickers do Ibovespa: {e}")
        return []

def get_selic_rate() -> Optional[float]:
    """
    Obtém a taxa Selic meta atual do site do Banco Central do Brasil.
    Retorna a taxa em percentual (ex: 13.75 para 13.75%).
    """
    try:
        url = "https://www.bcb.gov.br/"
        response = requests.get(url, timeout=10)
        response.raise_for_status() # Lança exceção para erros HTTP
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Tenta encontrar a Selic em diferentes elementos, pois a estrutura do site pode mudar.
        # Prioriza elementos com classe 'valor' ou 'blocoTaxaSelic'.
        selic_element = soup.find('p', class_='valor')
        if selic_element:
            selic_text = selic_element.text.replace(',', '.').strip()
            # Usa regex para extrair apenas o número, ignorando '%' ou outros caracteres
            match = re.search(r'(\d+\.?\d*)', selic_text)
            if match:
                selic_rate = float(match.group(1))
                logger.info(f"Taxa Selic obtida: {selic_rate}%")
                return selic_rate
        
        # Fallback: tentar encontrar em um div com id específico ou classe
        selic_alt_element = soup.find('div', id='blocoTaxaSelic')
        if selic_alt_element:
            text_content = selic_alt_element.get_text()
            match = re.search(r'(\d+,\d+)%', text_content)
            if match:
                selic_rate = float(match.group(1).replace(',', '.'))
                logger.info(f"Taxa Selic obtida (alternativa): {selic_rate}%")
                return selic_rate

        logger.error("Não foi possível encontrar a taxa Selic na página do Banco Central.")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Erro ao conectar com o Banco Central para obter a Selic: {e}")
        return None
    except Exception as e:
        logger.error(f"Erro ao processar a página do Banco Central para obter a Selic: {e}")
        return None

def validate_ticker(ticker: str) -> str:
    """
    Valida e formata um ticker para o padrão brasileiro (.SA).
    """
    ticker = ticker.upper().strip()
    if not ticker.endswith('.SA'):
        ticker += '.SA'
    return ticker

def get_market_sectors() -> dict:
    """
    Retorna um dicionário com setores e suas principais empresas (exemplos),
    ampliado com base no mapeamento_tickers.csv.
    """
    return {
        'Petróleo e Gás': ['PETR4.SA', 'PRIO3.SA', 'RECV3.SA'],
        'Mineração': ['VALE3.SA', 'USIM3.SA', 'USIM5.SA', 'CSNA3.SA', 'GGBR3.SA', 'GGBR4.SA', 'GOAU3.SA', 'GOAU4.SA'],
        'Bancos': ['ITUB3.SA', 'ITUB4.SA', 'BBDC3.SA', 'BBDC4.SA', 'BBAS3.SA', 'SANB11.SA', 'BBSE3.SA', 'BPAC11.SA'],
        'Bebidas': ['ABEV3.SA'],
        'Varejo': ['MGLU3.SA', 'LREN3.SA', 'ASAI3.SA', 'LWSA3.SA', 'PCAR3.SA', 'ARZZ3.SA', 'CASH3.SA', 'QUAL3.SA'],
        'Alimentos': ['JBSS3.SA', 'BEEF3.SA', 'MRFG3.SA', 'BRFS3.SA'],
        'Energia Elétrica': ['ELET3.SA', 'ALUP11.SA', 'CMIG3.SA', 'CMIG4.SA', 'ENBR3.SA', 'ENEV3.SA', 'EQTL3.SA', 'OMGE3.SA', 'SBSP3.SA', 'TAEE11.SA'],
        'Telecomunicações': ['VIVT3.SA', 'TIMS3.SA'],
        'Tecnologia': ['TOTS3.SA'],
        'Saúde': ['RADL3.SA', 'RDOR3.SA', 'FLRY3.SA', 'HAPV3.SA'],
        'Transporte': ['RENT3.SA', 'AZUL4.SA', 'GOLL4.SA', 'RAIL3.SA', 'CCRO3.SA'],
        'Construção Civil': ['EZTC3.SA', 'MRVE3.SA'],
        'Papel e Celulose': ['SUZB3.SA', 'KLBN11.SA'],
        'Aviação': ['AZUL4.SA', 'GOLL4.SA'],
        'Diversos': ['WEGE3.SA', 'COGN3.SA', 'EMBR3.SA', 'IRBR3.SA', 'NTCO3.SA', 'SLCE3.SA', 'SMTO3.SA', 'UGPA3.SA', 'YDUQ3.SA', 'B3SA3.SA'],
    }
