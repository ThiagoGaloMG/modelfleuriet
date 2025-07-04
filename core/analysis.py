import pandas as pd
from typing import Dict, Tuple, List
import numpy as np
import logging

# Configuração do logger para este módulo
logger = logging.getLogger(__name__)

FLEURIET_ACCOUNT_MAPPING = {
    '1': 'Ativo_Total', '1.01.01': 'T_Ativo', '1.01.02': 'T_Ativo',
    '1.01.03': 'AC_Clientes', '1.01.04': 'AC_Estoques', '1.01.06': 'AC',
    '1.01.07': 'AC', '1.01.08': 'AC', '1.02': 'ANC', '2.01.01': 'PC',
    '2.01.02': 'PC_Fornecedores', '2.01.03': 'PC', '2.01.04': 'T_Passivo',
    '2.01.05': 'PC', '2.02': 'PNC', '2.02.01': 'T_Passivo', '2.03': 'PL',
    '3.01': 'DRE_Receita', '3.02': 'DRE_Custo', '3.05': 'DRE_LucroOperacional',
    '3.09': 'DRE_ImpostoRenda', '3.11': 'DRE_LucroLiquido'
}

def get_specific_value(df: pd.DataFrame, code: str) -> float:
    """Busca um valor específico no DataFrame, retornando 0.0 se não encontrado."""
    row = df[df['CD_CONTA'] == code]
    return float(row['VL_CONTA'].iloc[0]) if not row.empty else 0.0

def reclassify_and_sum(df: pd.DataFrame) -> Dict:
    """Reclassifica as contas e soma os valores por categoria."""
    # Garante que CD_CONTA seja string para o mapeamento
    df['CATEGORY'] = df['CD_CONTA'].astype(str).str.strip().apply(
        lambda x: FLEURIET_ACCOUNT_MAPPING.get(x, 'Outros')
    )
    summed_data = df.groupby('CATEGORY')['VL_CONTA'].sum().to_dict()
    return summed_data

def calculate_fleuriet_indicators(reclassified_data: Dict) -> Tuple[Dict, str]:
    """Calcula os indicadores base do modelo Fleuriet."""
    AC_Operacional = reclassified_data.get('AC_Clientes', 0.0) + reclassified_data.get('AC_Estoques', 0.0)
    PC_Operacional = reclassified_data.get('PC_Fornecedores', 0.0)
    
    NCG = AC_Operacional - PC_Operacional
    
    PL = reclassified_data.get('PL', 0.0)
    PNC = reclassified_data.get('PNC', 0.0)
    ANC = reclassified_data.get('ANC', 0.0)
    
    CDG = PL + PNC - ANC
    T = CDG - NCG
    
    # Classificação da estrutura financeira
    tipo_estrutura = "Não identificado"
    if CDG > 0 and NCG > 0 and T > 0:
        tipo_estrutura = "Estrutura Ótima"
    elif CDG > 0 and NCG > 0 and T < 0:
        tipo_estrutura = "Alto Risco Financeiro"
    elif CDG > 0 and NCG < 0:
        tipo_estrutura = "Baixo Risco Financeiro"
    elif CDG < 0 and NCG < 0 and T > 0:
        tipo_estrutura = "Estrutura Péssima (Sobra)"
    elif CDG < 0 and NCG < 0 and T < 0:
        tipo_estrutura = "Estrutura Péssima (Falta)"
    elif CDG < 0 and NCG > 0:
        tipo_estrutura = "Insolvência Total"

    return {'NCG': NCG, 'CDG': CDG, 'T': T}, tipo_estrutura

def calculate_advanced_indicators(reclassified_data: Dict, base_indicators: Dict) -> Dict:
    """Calcula indicadores avançados como prazos médios e ROIC."""
    Vendas = reclassified_data.get('DRE_Receita', 0.0)
    Custo = reclassified_data.get('DRE_Custo', 0.0)
    AC_Clientes = reclassified_data.get('AC_Clientes', 0.0)
    AC_Estoques = reclassified_data.get('AC_Estoques', 0.0)
    PC_Fornecedores = reclassified_data.get('PC_Fornecedores', 0.0)
    LAJIR = reclassified_data.get('DRE_LucroOperacional', 0.0)
    Ativo_Total = reclassified_data.get('Ativo_Total', 0.0)
    
    Compras = Custo + AC_Estoques  # Suposição simplificada

    PMR = (AC_Clientes / Vendas) * 360 if Vendas else 'N/A'
    PME = (AC_Estoques / Custo) * 360 if Custo else 'N/A'
    PMP = (PC_Fornecedores / Compras) * 360 if Compras else 'N/A'
    
    Ciclo_Financeiro = 'N/A'
    if isinstance(PMR, float) and isinstance(PME, float) and isinstance(PMP, float):
        Ciclo_Financeiro = PMR + PME - PMP

    ROIC = (LAJIR / Ativo_Total) * 100 if Ativo_Total else 'N/A'
    ILD = base_indicators.get('T', 0.0) / Ativo_Total if Ativo_Total else 'N/A'

    return {
        'PMR': PMR, 'PME': PME, 'PMP': PMP, 'Ciclo_Financeiro': Ciclo_Financeiro,
        'ROIC': ROIC, 'ILD': ILD
    }

def calculate_z_score_prado(reclassified_data: Dict, base_indicators: Dict) -> Tuple[float, str]:
    """Calcula o Z-Score de Prado para predição de insolvência."""
    try:
        Ativo_Total = reclassified_data.get('Ativo_Total', 0.0)
        if Ativo_Total == 0:
            return 'N/A', 'Ativo Total é zero'

        Lucro_Retido = reclassified_data.get('PL', 0.0) - reclassified_data.get('DRE_LucroLiquido', 0.0)
        LAJIR = reclassified_data.get('DRE_LucroOperacional', 0.0)
        PL = reclassified_data.get('PL', 0.0)
        Passivo_Total = reclassified_data.get('PC', 0.0) + reclassified_data.get('PNC', 0.0)
        Vendas = reclassified_data.get('DRE_Receita', 0.0)

        X1 = (base_indicators.get('CDG', 0.0) - base_indicators.get('NCG', 0.0)) / Ativo_Total
        X2 = Lucro_Retido / Ativo_Total
        X3 = LAJIR / Ativo_Total
        X4 = PL / Passivo_Total if Passivo_Total else 0.0
        X5 = Vendas / Ativo_Total

        # Garante que todos os componentes são numéricos antes do cálculo
        if any(not isinstance(x, (int, float)) for x in [X1, X2, X3, X4, X5]):
             logger.warning(f"Cálculo do Z-Score pulado devido a valores não numéricos.")
             return 'N/A', 'Dados insuficientes'

        Z = 1.887 + (0.899 * X1) + (0.971 * X2) - (0.444 * X3) + (0.055 * X4) - (0.980 * X5)
        
        z_risk = "Risco Baixo"
        if Z < 4.35: z_risk = "Risco Médio"
        if Z < 2.9: z_risk = "Risco Elevado"
            
        return round(Z, 4), z_risk
    except Exception as e:
        logger.error(f"Erro inesperado no cálculo do Z-Score: {e}")
        return 'N/A', 'Erro de cálculo'

def run_multi_year_analysis(company_df: pd.DataFrame, cvm_code: int, years: List[int]) -> Tuple[Dict, str]:
    """Orquestra a análise completa para uma empresa ao longo de vários anos."""
    all_results = []
    company_name = company_df['DENOM_CIA'].iloc[0] if not company_df.empty else f"Empresa CVM {cvm_code}"

    company_df['DT_REFER'] = pd.to_datetime(company_df['DT_REFER'])

    for year in sorted(years):
        reference_date = pd.to_datetime(f"{year}-12-31")
        
        # **A CORREÇÃO PRINCIPAL**: Cria uma cópia explícita para evitar o SettingWithCopyWarning
        df_year = company_df[company_df['DT_REFER'] == reference_date].copy()
        
        if df_year.empty:
            logger.warning(f"Nenhum dado encontrado para o ano {year} para a empresa CVM {cvm_code}.")
            continue
        
        try:
            reclassified_data = reclassify_and_sum(df_year)
            base_indicators, tipo_estrutura = calculate_fleuriet_indicators(reclassified_data)
            advanced_indicators = calculate_advanced_indicators(reclassified_data, base_indicators)
            z_score, z_risk = calculate_z_score_prado(reclassified_data, base_indicators)
            
            advanced_indicators['Z_Score'] = z_score
            advanced_indicators['Z_Risk'] = z_risk
            
            all_results.append({
                'year': year,
                'base_indicators': base_indicators,
                'advanced_indicators': advanced_indicators,
                'structure': tipo_estrutura
            })
        except Exception as e:
            logger.error(f"Erro ao processar o ano {year} para a empresa CVM {cvm_code}: {e}", exc_info=True)
            return None, f"Erro ao analisar o ano {year}. Verifique os logs."
    
    if not all_results:
        return None, f"Não foram encontrados dados anuais para a empresa CVM {cvm_code} no período de {min(years)} a {max(years)}."
    
    chart_data = {
        'labels': [res['year'] for res in all_results],
        'ncg': [res['base_indicators']['NCG'] for res in all_results],
        'cdg': [res['base_indicators']['CDG'] for res in all_results],
        't': [res['base_indicators']['T'] for res in all_results],
    }
    
    final_result = {
        'company_name': company_name,
        'cvm_code': cvm_code,
        'yearly_results': all_results,
        'chart_data': chart_data
    }

    return final_result, None
