import pandas as pd
from typing import Dict, Tuple, List
import numpy as np

# ==============================================================================
# MAPEAMENTO DE CONTAS (sem alterações)
# ==============================================================================
FLEURIET_ACCOUNT_MAPPING = {
    '1': 'Ativo_Total', '1.01.01': 'T_Ativo', '1.01.02': 'T_Ativo',
    '1.01.03': 'AC_Clientes', '1.01.04': 'AC_Estoques', '1.01.06': 'AC',
    '1.01.07': 'AC', '1.01.08': 'AC', '1.02': 'ANC', '2.01.01': 'PC',
    '2.01.02': 'PC_Fornecedores', '2.01.03': 'PC', '2.01.04': 'T_Passivo',
    '2.01.05': 'PC', '2.02': 'PNC', '2.02.01': 'T_Passivo', '2.03': 'PL',
    '3.01': 'DRE_Receita', '3.02': 'DRE_Custo', '3.05': 'DRE_LucroOperacional',
    '3.09': 'DRE_ImpostoRenda', '3.11': 'DRE_LucroLiquido'
}

# ==============================================================================
# FUNÇÕES DE CÁLCULO OTIMIZADAS
# ==============================================================================

def get_specific_value(df: pd.DataFrame, code: str) -> float:
    row = df[df['CD_CONTA'] == code]
    return row['VL_CONTA'].iloc[0] if not row.empty else 0.0

def reclassify_and_sum(df_year: pd.DataFrame) -> Dict[str, float]:
    """
    Função otimizada para reclassificar e somar valores para um DataFrame já filtrado por ano.
    """
    data = {}
    # Mapeia cada conta para sua categoria
    df_year['CATEGORY'] = df_year['CD_CONTA'].apply(
        lambda x: next((cat for code, cat in FLEURIET_ACCOUNT_MAPPING.items() if x.startswith(code)), None)
    )
    summary = df_year.groupby('CATEGORY')['VL_CONTA'].sum().to_dict()
    data.update(summary)

    # Extrai valores individuais e agregados necessários para os cálculos
    for code in ['1', '1.02', '2.02', '2.03', '1.01.03', '1.01.04', '2.01.02', '3.01', '3.02', '3.05', '3.09']:
        category_name = FLEURIET_ACCOUNT_MAPPING.get(code, "").replace("DRE_", "").replace("AC_", "").replace("PC_", "")
        data[category_name] = get_specific_value(df_year, code)
    
    data['Total_AC'] = data.get('Clientes', 0) + data.get('Estoques', 0) + data.get('AC', 0)
    data['Total_PC'] = data.get('Fornecedores', 0) + data.get('PC', 0)
    return data

def calculate_fleuriet_indicators(data: Dict[str, float]) -> Tuple[Dict, int]:
    ac = data.get('Total_AC', 0.0); pc = data.get('Total_PC', 0.0)
    anc = data.get('ANC', 0.0); pnc = data.get('PNC', 0.0); pl = data.get('PL', 0.0)
    ncg = ac - pc; cdg = (pnc + pl) - anc; t = cdg - ncg
    indicators = {'NCG': ncg, 'CDG': cdg, 'T': t}
    tipo = 0
    if ncg > 0 and cdg > 0 and t > 0: tipo = 2
    elif ncg > 0 and cdg > 0 and t < 0: tipo = 3
    elif ncg < 0 and cdg > 0: tipo = 4
    elif ncg < 0 and cdg < 0 and t < 0: tipo = 5
    elif ncg < 0 and cdg < 0 and t > 0: tipo = 6
    elif ncg > 0 and cdg < 0: tipo = 1
    return indicators, tipo

def calculate_advanced_indicators(data: Dict, base_indicators: Dict) -> Dict:
    t = base_indicators.get('T', 0); anc = data.get('ANC', 0); ncg = base_indicators.get('NCG', 0)
    ild = t / (anc + ncg) if (anc + ncg) != 0 else 0
    receita_liquida = data.get('Receita', 0); custo_produtos = abs(data.get('Custo', 0))
    clientes = data.get('Clientes', 0); estoques = data.get('Estoques', 0); fornecedores = data.get('Fornecedores', 0)
    pmr = (clientes / receita_liquida) * 365 if receita_liquida != 0 else 0
    pme = (estoques / custo_produtos) * 365 if custo_produtos != 0 else 0
    pmp = (fornecedores / custo_produtos) * 365 if custo_produtos != 0 else 0
    lucro_op = data.get('LucroOperacional', 0); imposto_renda = abs(data.get('ImpostoRenda', 0))
    aliquota_ir = imposto_renda / lucro_op if lucro_op > 0 else 0
    nopat = lucro_op * (1 - aliquota_ir)
    capital_investido = data.get('PL', 0) + data.get('T_Passivo', 0)
    roic = nopat / capital_investido if capital_investido != 0 else 0
    return {'ILD': ild, 'PMR': pmr, 'PME': pme, 'PMP': pmp, 'Ciclo_Financeiro': pmr + pme - pmp, 'ROIC': roic}

def calculate_z_score_prado(data: Dict, base_indicators: Dict, tipo_estrutura: int) -> Tuple[float, str]:
    cdg = base_indicators.get('CDG', 0); ncg = base_indicators.get('NCG', 0); t = base_indicators.get('T', 0)
    ativo_total = data.get('Ativo_Total', 0); receita_liquida = data.get('Receita', 0); t_passivo = data.get('T_Passivo', 0)
    if ativo_total == 0 or receita_liquida == 0 or ncg == 0: return 0.0, "Dados insuficientes"
    X1 = cdg / ativo_total; X2 = ncg / receita_liquida; X3 = tipo_estrutura; X4 = t / abs(ncg); X5 = t_passivo / ativo_total
    Z = 1.887 + (0.899 * X1) + (0.971 * X2) - (0.444 * X3) + (0.055 * X4) - (0.980 * X5)
    if Z > 2.675: risco = "Classe A (Risco Mínimo)"
    elif Z > 2.0: risco = "Classe B"
    elif Z > 1.5: risco = "Classe C"
    elif Z > 1.0: risco = "Classe D (Atenção)"
    else: risco = "Classe E (Risco Elevado)"
    return Z, risco

def run_multi_year_analysis(df: pd.DataFrame, cvm_code: int, years: List[int]):
    """
    Função principal otimizada para orquestrar a análise.
    """
    # >>> OTIMIZAÇÃO PRINCIPAL: Filtra o DataFrame por CVM uma única vez! <<<
    company_df = df[df['CD_CVM'] == cvm_code].copy()

    if company_df.empty:
        return None, f"Empresa com CVM {cvm_code} não encontrada nos dados."

    company_name = company_df['DENOM_CIA'].iloc[0]
    all_results = []

    for year in sorted(years):
        reference_date = f"{year}-12-31"
        # Filtra o DataFrame já pequeno para o ano específico
        df_year = company_df[company_df['DT_REFER'] == reference_date]

        if df_year.empty:
            continue # Pula para o próximo ano se não houver dados

        reclassified_data = reclassify_and_sum(df_year)
        base_indicators, tipo_estrutura = calculate_fleuriet_indicators(reclassified_data)
        advanced_indicators = calculate_advanced_indicators(reclassified_data, base_indicators)
        z_score, z_risk = calculate_z_score_prado(reclassified_data, base_indicators, tipo_estrutura)
        advanced_indicators['Z_Score'] = z_score
        advanced_indicators['Z_Risk'] = z_risk
        
        structure_map = {1: "Tipo 1", 2: "Tipo 2", 3: "Tipo 3", 4: "Tipo 4", 5: "Tipo 5", 6: "Tipo 6", 0: "N/C"}
        
        all_results.append({
            'year': year,
            'base_indicators': base_indicators,
            'advanced_indicators': advanced_indicators,
            'structure': structure_map[tipo_estrutura]
        })
    
    if not all_results:
        return None, f"Não foram encontrados dados anuais para a empresa CVM {cvm_code} no período de {min(years)} a {max(years)}."

    chart_data = {
        'labels': [res['year'] for res in all_results],
        'ncg': [res['base_indicators']['NCG'] for res in all_results],
        'cdg': [res['base_indicators']['CDG'] for res in all_results],
        't': [res['base_indicators']['T'] for res in all_results],
    }

    return {
        'company_name': company_name, 'cvm_code': cvm_code,
        'yearly_results': all_results, 'chart_data': chart_data
    }, None