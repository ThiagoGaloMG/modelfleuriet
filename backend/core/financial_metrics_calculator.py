# modelfleuriet/core/financial_metrics_calculator.py

import pandas as pd
import numpy as np
import logging
import math
from typing import Dict, List, Optional, Tuple, Union, Any
from dataclasses import dataclass
from datetime import datetime, timedelta

# Importa o novo coletor de dados (que lê do DB) e utilitários
from core.data_collector import CompanyFinancialData
from core.ibovespa_utils import get_selic_rate # Para Selic
from core.utils import ValidationUtils # Para validação

logger = logging.getLogger(__name__)

# CompanyFinancialData já está definido em data_collector.py e importado.
# Não precisamos redefini-lo aqui.

class FinancialMetricsCalculator:
    """Classe responsável pelo cálculo das métricas financeiras:
    WACC, EVA, EFV, Riqueza Atual, Riqueza Futura.
    Baseado nas Equações 1-5 e metodologia do TCC do usuário.
    """
    
    def __init__(self, selic_rate: Optional[float] = None):
        self.tax_rate = 0.34 # Alíquota de IR e CSLL para NOPAT e beta (34% conforme TCC p.17, 20)
        self.risk_free_rate = (selic_rate / 100) if selic_rate else 0.10 # Selic como taxa livre de risco, default 10%
        self.market_risk_premium = 0.06 # Prêmio de risco de mercado (exemplo: 6%)
        # Estes deveriam vir de uma fonte confiável ou ser ajustáveis

    def _calculate_nopat(self, ebit: float) -> float:
        """Calcula o NOPAT (Net Operating Profit After Taxes)."""
        return ebit * (1 - self.tax_rate)

    def _calculate_working_capital(self, data: CompanyFinancialData) -> float:
        """Calcula o Capital de Giro (Ativo Circulante - Passivo Circulante)."""
        return data.current_assets - data.current_liabilities

    def _calculate_ncg(self, data: CompanyFinancialData) -> float:
        """Calcula a Necessidade de Capital de Giro (NCG).
        NCG = Contas a Receber + Estoques - Fornecedores
        """
        return data.accounts_receivable + data.inventory - data.accounts_payable

    def _calculate_capital_employed(self, data: CompanyFinancialData) -> float:
        """Calcula o Capital Empregado (Imobilizado + NCG).
        Conforme TCC p. 17: "soma entre necessidade de capital de giro (NCG) e imobilizado".
        """
        imobilizado = data.property_plant_equipment if data.property_plant_equipment is not None else 0.0
        ncg = self._calculate_ncg(data)
        
        return imobilizado + ncg

    def _calculate_cost_of_equity_ke(self, beta: float) -> float:
        """Calcula o Custo do Capital Próprio (Ke) usando CAPM.
        Ke = Taxa sem Risco + Beta * Prêmio de Risco de Mercado
        """
        return self.risk_free_rate + beta * self.market_risk_premium
    
    def _calculate_cost_of_debt_kd(self, data: CompanyFinancialData) -> float:
        """Calcula o Custo do Capital de Terceiros (Kd).
        Simplificação: usar uma estimativa baseada na taxa livre de risco + spread.
        No TCC, Kd é "multiplicação entre despesas financeiras com juros (Kd) e a participação da dívida líquida no passivo oneroso (%Kd)" (p. 20)
        Como não estamos coletando despesas financeiras diretamente da CVM para Kd, usaremos uma proxy.
        """
        if data.total_debt > 0:
            return self.risk_free_rate * 1.2 # Selic + 20% de spread como proxy
        return 0.05 # Default 5% se não houver dívida relevante

    def _calculate_wacc(self, data: CompanyFinancialData, beta: float) -> float:
        """Calcula o WACC (Custo Médio Ponderado de Capital).
        CMPC = (Kd x %Kd) + (Ke x %Ke) - Equação 5 (p. 20)
        %Ke = Equity / (Equity + TotalDebt)
        %Kd = TotalDebt / (Equity + TotalDebt)
        """
        total_capital = data.equity + data.total_debt
        if total_capital <= 0: # Evita divisão por zero ou capital negativo
            logger.warning(f"Total Capital (Equity + Debt) é zero ou negativo para {data.ticker}. Não é possível calcular WACC.")
            return np.nan

        ke = self._calculate_cost_of_equity_ke(beta)
        kd = self._calculate_cost_of_debt_kd(data) # Kd é a taxa de custo da dívida
        
        percent_ke = data.equity / total_capital
        percent_kd = data.total_debt / total_capital
        
        # WACC ajustado pelo benefício fiscal da dívida: Kd * (1 - TaxRate) * %Kd
        wacc = (ke * percent_ke) + (kd * (1 - self.tax_rate) * percent_kd)
        return wacc

    def _calculate_roce(self, data: CompanyFinancialData, capital_employed: float) -> float:
        """Calcula o ROCE (Retorno do Capital Empregado).
        ROCE = Fluxo de Caixa Operacional / Capital Empregado (conforme TCC p. 18, derivado)
        Como não temos Fluxo de Caixa Operacional direto da CVM para este dataclass,
        usaremos NOPAT como proxy para o numerador, que é comum em algumas definições de ROCE.
        """
        if capital_employed == 0:
            return np.nan
        
        nopat = self._calculate_nopat(data.ebit)
        
        return nopat / capital_employed

    def calculate_eva(self, data: CompanyFinancialData, beta: float) -> Tuple[float, float]:
        """Calcula o EVA (Economic Value Added) absoluto e percentual.
        EVA = (Capital Empregado) x (Retorno do Capital Empregado - Custo Médio Ponderado de Capital) - Equação 1 (p. 17)
        """
        capital_employed = self._calculate_capital_employed(data)
        if capital_employed <= 0:
             return np.nan, np.nan

        wacc = self._calculate_wacc(data, beta)
        roce = self._calculate_roce(data, capital_employed)

        if np.isnan(wacc) or np.isnan(roce):
            return np.nan, np.nan
        
        eva_abs = capital_employed * (roce - wacc)
        eva_pct = (roce - wacc) * 100 # Em percentual, como no TCC

        return eva_abs, eva_pct

    def calculate_efv(self, data: CompanyFinancialData, beta: float) -> Tuple[float, float]:
        """Calcula o EFV (Economic Future Value) absoluto e percentual.
        EFV = Riqueza Futura Esperada - Riqueza Atual - Equação 2 (p. 19)
        """
        # Calcular Riqueza Atual e Futura primeiro
        riqueza_atual_abs = self.calculate_riqueza_atual(data, beta)
        riqueza_futura_esperada_abs = self.calculate_riqueza_futura(data)

        if np.isnan(riqueza_atual_abs) or np.isnan(riqueza_futura_esperada_abs):
            return np.nan, np.nan

        efv_abs = riqueza_futura_esperada_abs - riqueza_atual_abs
        
        # EFV percentual (TCC p. 103, Apêndice C: EFV % = EFV / Capital Empregado)
        capital_employed = self._calculate_capital_employed(data)
        if capital_employed <= 0:
            return np.nan, np.nan
        
        efv_pct = (efv_abs / capital_employed) * 100
        
        return efv_abs, efv_pct

    def calculate_riqueza_atual(self, data: CompanyFinancialData, beta: float) -> float:
        """Calcula a Riqueza Atual.
        Riqueza Atual = EVA / CMPC - Equação 4 (p. 20)
        """
        eva_abs, _ = self.calculate_eva(data, beta)
        wacc = self._calculate_wacc(data, beta)
        
        if np.isnan(eva_abs) or np.isnan(wacc) or wacc == 0:
            return np.nan
        
        return eva_abs / wacc

    def calculate_riqueza_futura(self, data: CompanyFinancialData) -> float:
        """Calcula a Riqueza Futura Esperada.
        Riqueza Futura Esperada = {(preço de ações ordinárias x quantidade de ações ordinárias emitidas)
                                    + (preço de ações preferenciais x quantidade de ações preferenciais emitidas)
                                    + valor da dívida da empresa - capital empregado} - Equação 3 (p. 20)
        
        Simplificação: usar Market Cap (ações ordinárias + preferenciais) + Dívida Total - Capital Empregado
        """
        market_value_equity = data.market_cap 
        total_debt = data.total_debt
        capital_employed = self._calculate_capital_employed(data)

        if np.isnan(market_value_equity) or np.isnan(total_debt) or np.isnan(capital_employed):
            return np.nan

        riqueza_futura = (market_value_equity + total_debt) - capital_employed
        return riqueza_futura

    def calculate_upside(self, data: CompanyFinancialData, efv_abs: float) -> float:
        """Calcula o potencial de valorização (Upside).
        Upside = (EFV Absoluto / Market Cap) * 100
        """
        if data.market_cap <= 0:
            return np.nan
        return (efv_abs / data.market_cap) * 100
