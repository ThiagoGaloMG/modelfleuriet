# modelfleuriet/core/utils.py

import pandas as pd
import numpy as np
import logging
import time
from typing import Dict, List, Optional, Tuple, Any

logger = logging.getLogger(__name__)

def format_currency(value: float, currency_symbol: str = "R$") -> str:
    """
    Formata um valor numérico como moeda brasileira, com sufixos para milhões, bilhões, etc.
    """
    if value is None or pd.isna(value) or np.isinf(value):
        return "N/D"
    if abs(value) >= 1e12:
        return f"{currency_symbol} {value/1e12:.2f}T"
    elif abs(value) >= 1e9:
        return f"{currency_symbol} {value/1e9:.2f}B"
    elif abs(value) >= 1e6:
        return f"{currency_symbol} {value/1e6:.2f}M"
    elif abs(value) >= 1e3:
        return f"{currency_symbol} {value/1e3:.2f}K"
    else:
        return f"{currency_symbol} {value:.2f}"

def format_percentage(value: float) -> str:
    """
    Formata um valor numérico como percentual.
    """
    if value is None or pd.isna(value) or np.isinf(value):
        return "N/D"
    return f"{value:.2f}%"

class ValidationUtils:
    """
    Utilitários para validação de dados.
    """
    @staticmethod
    def validate_financial_data(data: Dict) -> Tuple[bool, List[str]]:
        """
        Valida se os dados financeiros essenciais estão presentes e são válidos.
        Retorna True e lista vazia de erros se válido, False e lista de erros caso contrário.
        """
        errors = []
        required_float_fields = [
            'market_cap', 'revenue', 'ebit', 'net_income', 'total_assets',
            'total_debt', 'equity', 'current_assets', 'current_liabilities',
            'cash', 'property_plant_equipment', 'capex'
        ]

        for field in required_float_fields:
            if field not in data or not isinstance(data[field], (int, float)):
                errors.append(f"Campo '{field}' ausente ou inválido. Esperado número.")
            elif pd.isna(data[field]) or np.isinf(data[field]):
                errors.append(f"Campo '{field}' contém valor NaN ou infinito.")

        if 'ticker' not in data or not isinstance(data['ticker'], str) or not data['ticker']:
            errors.append("Campo 'ticker' ausente ou inválido.")
        if 'company_name' not in data or not isinstance(data['company_name'], str) or not data['company_name']:
            errors.append("Campo 'company_name' ausente ou inválido.")

        return len(errors) == 0, errors

class PerformanceMonitor:
    """
    Monitora o tempo de execução de operações.
    """
    def __init__(self):
        self.timers = {}

    def start_timer(self, name: str):
        """Inicia um temporizador com um nome."""
        self.timers[name] = time.time()
        logger.info(f"Iniciando temporizador: {name}...")

    def end_timer(self, name: str):
        """Finaliza um temporizador e imprime o tempo decorrido."""
        if name in self.timers:
            end_time = time.time()
            elapsed_time = end_time - self.timers[name]
            logger.info(f"Temporizador '{name}' finalizado. Tempo decorrido: {elapsed_time:.2f} segundos.")
            del self.timers[name]
        else:
            logger.warning(f"Temporizador '{name}' não encontrado.")

def clean_data_for_json(data: Any) -> Any:
    """Limpa dados para serialização JSON, convertendo NaN/Inf para None."""
    if isinstance(data, dict):
        return {key: clean_data_for_json(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [clean_data_for_json(item) for item in data]
    elif isinstance(data, float) and (np.isnan(data) or np.isinf(data)):
        return None
    elif isinstance(data, pd.Timestamp):
        return data.isoformat()
    else:
        return data
