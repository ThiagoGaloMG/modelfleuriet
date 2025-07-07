import pandas as pd
from typing import Dict, Tuple, List, Optional
import numpy as np
import logging

# Configuração do logger para este módulo
logger = logging.getLogger(__name__)

def safe_divide(numerator, denominator):
    """Realiza a divisão de forma segura, retornando None se o denominador for 0, None ou NaN."""
    if denominator is None or pd.isna(denominator) or denominator == 0:
        return None
    if numerator is None or pd.isna(numerator):
        return None
    return numerator / denominator

# Mapeamento de contas unificado para ambas as análises (Fleuriet e Valuation)
UNIFIED_ACCOUNT_MAPPING = {
    # ATIVOS
    '1': 'Ativo_Total',
    '1.01.01': 'T_CaixaEquivalentes',
    '1.01.02': 'T_AplicacoesFinanceiras',
    '1.01.03': 'AC_Clientes',
    '1.01.04': 'AC_Estoques',
    '1.01.06': 'AC_Outros', # Ativos Biológicos
    '1.01.07': 'AC_Outros', # Tributos a Recuperar
    '1.01.08': 'AC_Outros', # Outras Contas a Receber
    '1.02': 'ANC',
    
    # PASSIVOS
    '2.01.01': 'PC_Outros', # Obrigações Sociais e Trabalhistas
    '2.01.02': 'PC_Fornecedores',
    '2.01.03': 'PC_Outros', # Obrigações Fiscais
    '2.01.04': 'T_DividaCurtoPrazo',
    '2.01.05': 'PC_Outros', # Outras Obrigações
    '2.02': 'PNC_Total', # Passivo Não Circulante Total
    '2.02.01': 'T_DividaLongoPrazo',
    '2.03': 'PL',
    
    # DRE
    '3.01': 'DRE_Receita',
    '3.02': 'DRE_Custo',
    '3.05': 'DRE_LucroOperacional', # Também é o EBIT
    '3.07': 'DRE_DespesasFinanceiras',
    '3.09': 'DRE_LucroAntesImpostos',
    '3.10': 'DRE_ImpostoRendaCSLL',
    '3.11': 'DRE_LucroLiquido'
}

def reclassify_and_sum(df: pd.DataFrame) -> Dict[str, float]:
    """Reclassifica as contas e soma os valores por categoria usando o mapeamento unificado."""
    df['CATEGORY'] = df['CD_CONTA'].astype(str).str.strip().apply(
        lambda x: UNIFIED_ACCOUNT_MAPPING.get(x, 'Outros')
    )
    summed_data = df.groupby('CATEGORY')['VL_CONTA'].sum().to_dict()
    return summed_data

def calculate_fleuriet_indicators(reclassified_data: Dict) -> Tuple[Dict, str]:
    """Calcula os indicadores base do modelo Fleuriet."""
    ac_clientes = reclassified_data.get('AC_Clientes', 0.0)
    ac_estoques = reclassified_data.get('AC_Estoques', 0.0)
    ac_outros = reclassified_data.get('AC_Outros', 0.0)
    
    pc_fornecedores = reclassified_data.get('PC_Fornecedores', 0.0)
    pc_outros = reclassified_data.get('PC_Outros', 0.0)

    # NCG (Necessidade de Capital de Giro) = Ativos Cíclicos Operacionais - Passivos Cíclicos Operacionais
    ativos_ciclicos = ac_clientes + ac_estoques + ac_outros
    passivos_ciclicos = pc_fornecedores + pc_outros
    NCG = ativos_ciclicos - passivos_ciclicos
    
    # CDG (Capital de Giro) = Recursos de Longo Prazo - Ativos de Longo Prazo
    pl = reclassified_data.get('PL', 0.0)
    pnc = reclassified_data.get('PNC_Total', 0.0)
    anc = reclassified_data.get('ANC', 0.0)
    CDG = (pl + pnc) - anc
    
    # T (Tesouraria) = Saldo entre CDG e NCG
    T = CDG - NCG
    
    # Classificação da estrutura financeira
    tipo_estrutura_num = 0
    if CDG > 0 and NCG > 0 and T >= 0: tipo_estrutura_num = 1; tipo_estrutura_desc = "Estrutura Ótima"
    elif CDG > 0 and NCG > 0 and T < 0: tipo_estrutura_num = 2; tipo_estrutura_desc = "Alto Risco"
    elif CDG > 0 and NCG < 0: tipo_estrutura_num = 3; tipo_estrutura_desc = "Estrutura Sólida"
    elif CDG < 0 and NCG > 0: tipo_estrutura_num = 4; tipo_estrutura_desc = "Risco Máximo"
    elif CDG < 0 and NCG < 0 and T < 0: tipo_estrutura_num = 5; tipo_estrutura_desc = "Estrutura Péssima"
    elif CDG < 0 and NCG < 0 and T >= 0: tipo_estrutura_num = 6; tipo_estrutura_desc = "Risco Elevado"
    else: tipo_estrutura_desc = "Não Identificada"

    base_indicators = {'NCG': NCG, 'CDG': CDG, 'T': T, 'TipoEstrutura': tipo_estrutura_num}
    return base_indicators, tipo_estrutura_desc

def calculate_advanced_indicators(reclassified_data: Dict, base_indicators: Dict) -> Dict:
    """Calcula indicadores avançados como prazos médios e ROIC de forma segura."""
    # Extração de valores
    vendas = reclassified_data.get('DRE_Receita')
    custo = reclassified_data.get('DRE_Custo')
    lucro_op = reclassified_data.get('DRE_LucroOperacional')
    imposto_renda = reclassified_data.get('DRE_ImpostoRendaCSLL')
    ac_clientes = reclassified_data.get('AC_Clientes')
    ac_estoques = reclassified_data.get('AC_Estoques')
    pc_fornecedores = reclassified_data.get('PC_Fornecedores')
    ativo_total = reclassified_data.get('Ativo_Total')
    t = base_indicators.get('T')
    anc = reclassified_data.get('ANC')
    ncg = base_indicators.get('NCG')

    # Cálculo dos Prazos
    compras = custo + ac_estoques if all(v is not None for v in [custo, ac_estoques]) else None
    ratio_pmr = safe_divide(ac_clientes, vendas)
    pmr = ratio_pmr * 365 if ratio_pmr is not None else None
    ratio_pme = safe_divide(ac_estoques, custo)
    pme = ratio_pme * 365 if ratio_pme is not None else None
    ratio_pmp = safe_divide(pc_fornecedores, compras)
    pmp = ratio_pmp * 365 if ratio_pmp is not None else None
    
    # Ciclo Financeiro
    ciclo_financeiro = None
    if all(p is not None for p in [pmr, pme, pmp]):
        ciclo_financeiro = pmr + pme - pmp
           
    # ILD (Índice de Liquidez Dinâmica)
    ild = safe_divide(t, (anc + ncg)) if all(v is not None for v in [t, anc, ncg]) else None

    # ROIC (Retorno sobre Capital Investido) - Simplificado
    roic = safe_divide(lucro_op, ativo_total)
    roic_percent = roic * 100 if roic is not None else None

    return {
        'PMR': pmr, 'PME': pme, 'PMP': pmp, 
        'Ciclo_Financeiro': ciclo_financeiro,
        'ILD': ild, 'ROIC': roic_percent
    }

def calculate_z_score_prado(reclassified_data: Dict, base_indicators: Dict) -> Tuple[Optional[float], str]:
    """Calcula o Z-Score de Prado para predição de insolvência."""
    # Extração de valores
    cdg = base_indicators.get('CDG')
    ncg = base_indicators.get('NCG')
    t = base_indicators.get('T')
    tipo_estrutura = base_indicators.get('TipoEstrutura')
    ativo_total = reclassified_data.get('Ativo_Total')
    receita_liquida = reclassified_data.get('DRE_Receita')
    pcf = reclassified_data.get('PC_Fornecedores', 0) + reclassified_data.get('PC_Outros', 0) + reclassified_data.get('T_DividaCurtoPrazo', 0)
    pnc = reclassified_data.get('PNC_Total')
    
    # Validação de dados essenciais
    if any(v is None for v in [cdg, ncg, t, tipo_estrutura, ativo_total, receita_liquida, pnc]):
        return None, "Dados insuficientes"
           
    # Cálculo dos componentes X
    x1 = safe_divide(cdg, ativo_total)
    x2 = safe_divide(ncg, receita_liquida)
    x3 = float(tipo_estrutura)
    x4 = safe_divide(t, abs(ncg)) if ncg != 0 else 0
    x5 = safe_divide((pcf + pnc), ativo_total)

    if any(x is None for x in [x1, x2, x4, x5]):
        return None, "Cálculo impossível"

    # Equação Z de Prado
    z = 1.887 + (0.899 * x1) + (0.971 * x2) - (0.444 * x3) + (0.055 * x4) - (0.980 * x5)

    # Classificação do Risco
    if z > 2.675: risk_class = "Classe A (Risco Mínimo)"
    elif 2.0 < z <= 2.675: risk_class = "Classe B"
    elif 1.5 < z <= 2.0: risk_class = "Classe C"
    elif 1.0 < z <= 1.5: risk_class = "Classe D (Atenção)"
    else: risk_class = "Classe E (Risco Elevado)"
       
    return round(z, 4), risk_class

def run_multi_year_analysis(company_df: pd.DataFrame, cvm_code: int, years: List[int]) -> Tuple[Dict, str]:
    """Orquestra a análise completa do Modelo Fleuriet para uma empresa ao longo de vários anos."""
    all_results = []
    company_name = company_df['DENOM_CIA'].iloc[0] if not company_df.empty else f"Empresa CVM {cvm_code}"
    company_df['DT_REFER'] = pd.to_datetime(company_df['DT_REFER'])

    for year in sorted(years):
        reference_date = pd.to_datetime(f"{year}-12-31")
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
