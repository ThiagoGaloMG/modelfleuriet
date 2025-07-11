import pandas as pd
from typing import Dict, Tuple, List, Optional
import numpy as np
import logging

logger = logging.getLogger(__name__)

def safe_divide(numerator, denominator):
    if denominator is None or pd.isna(denominator) or denominator == 0:
        return None
    if numerator is None or pd.isna(numerator):
        return None
    return numerator / denominator

UNIFIED_ACCOUNT_MAPPING = {
    '1': 'ativo_total',
    '1.01.01': 't_caixaequivalentes',
    '1.01.02': 't_aplicacoesfinanceiras',
    '1.01.03': 'ac_clientes',
    '1.01.04': 'ac_estoques',
    '1.01.06': 'ac_outros',
    '1.01.07': 'ac_outros',
    '1.01.08': 'ac_outros',
    '1.02': 'anc',
    '2.01.01': 'pc_outros',
    '2.01.02': 'pc_fornecedores',
    '2.01.03': 'pc_outros',
    '2.01.04': 't_dividacurtoprazo',
    '2.01.05': 'pc_outros',
    '2.02': 'pnc_total',
    '2.02.01': 't_dividalongoprazo',
    '2.03': 'pl',
    '3.01': 'dre_receita',
    '3.02': 'dre_custo',
    '3.05': 'dre_lucrooperacional',
    '3.07': 'dre_despesasfinanceiras',
    '3.09': 'dre_lucroantesimpostos',
    '3.10': 'dre_impostorendacsll',
    '3.11': 'dre_lucroliquido'
}

def reclassify_and_sum(df: pd.DataFrame) -> Dict[str, float]:
    df['category'] = df['cd_conta'].astype(str).str.strip().apply(
        lambda x: UNIFIED_ACCOUNT_MAPPING.get(x, 'outros')
    )
    summed_data = df.groupby('category')['vl_conta'].sum().to_dict()
    return summed_data

def calculate_fleuriet_indicators(reclassified_data: Dict) -> Tuple[Dict, str]:
    ac_clientes = reclassified_data.get('ac_clientes', 0.0)
    ac_estoques = reclassified_data.get('ac_estoques', 0.0)
    ac_outros = reclassified_data.get('ac_outros', 0.0)
    
    pc_fornecedores = reclassified_data.get('pc_fornecedores', 0.0)
    pc_outros = reclassified_data.get('pc_outros', 0.0)

    ativos_ciclicos = ac_clientes + ac_estoques + ac_outros
    passivos_ciclicos = pc_fornecedores + pc_outros
    ncg = ativos_ciclicos - passivos_ciclicos
    
    pl = reclassified_data.get('pl', 0.0)
    pnc = reclassified_data.get('pnc_total', 0.0)
    anc = reclassified_data.get('anc', 0.0)
    cdg = (pl + pnc) - anc
    
    t = cdg - ncg
    
    tipo_estrutura_num = 0
    if cdg > 0 and ncg > 0 and t >= 0: 
        tipo_estrutura_num = 1; tipo_estrutura_desc = "Estrutura Ótima"
    elif cdg > 0 and ncg > 0 and t < 0: 
        tipo_estrutura_num = 2; tipo_estrutura_desc = "Alto Risco"
    elif cdg > 0 and ncg < 0: 
        tipo_estrutura_num = 3; tipo_estrutura_desc = "Estrutura Sólida"
    elif cdg < 0 and ncg > 0: 
        tipo_estrutura_num = 4; tipo_estrutura_desc = "Risco Máximo"
    elif cdg < 0 and ncg < 0 and t < 0: 
        tipo_estrutura_num = 5; tipo_estrutura_desc = "Estrutura Péssima"
    elif cdg < 0 and ncg < 0 and t >= 0: 
        tipo_estrutura_num = 6; tipo_estrutura_desc = "Risco Elevado"
    else: 
        tipo_estrutura_desc = "Não Identificada"

    base_indicators = {'ncg': ncg, 'cdg': cdg, 't': t, 'tipoestrutura': tipo_estrutura_num}
    return base_indicators, tipo_estrutura_desc

def calculate_advanced_indicators(reclassified_data: Dict, base_indicators: Dict) -> Dict:
    vendas = reclassified_data.get('dre_receita')
    custo = reclassified_data.get('dre_custo')
    lucro_op = reclassified_data.get('dre_lucrooperacional')
    ac_clientes = reclassified_data.get('ac_clientes')
    ac_estoques = reclassified_data.get('ac_estoques')
    pc_fornecedores = reclassified_data.get('pc_fornecedores')
    ativo_total = reclassified_data.get('ativo_total')
    t = base_indicators.get('t')
    anc = reclassified_data.get('anc')
    ncg = base_indicators.get('ncg')

    compras = custo + ac_estoques if all(v is not None for v in [custo, ac_estoques]) else None
    ratio_pmr = safe_divide(ac_clientes, vendas)
    pmr = ratio_pmr * 365 if ratio_pmr is not None else None
    ratio_pme = safe_divide(ac_estoques, custo)
    pme = ratio_pme * 365 if ratio_pme is not None else None
    ratio_pmp = safe_divide(pc_fornecedores, compras)
    pmp = ratio_pmp * 365 if ratio_pmp is not None else None
    
    ciclo_financeiro = None
    if all(p is not None for p in [pmr, pme, pmp]):
        ciclo_financeiro = pmr + pme - pmp
           
    ild = safe_divide(t, (anc + ncg)) if all(v is not None for v in [t, anc, ncg]) else None

    roic = safe_divide(lucro_op, ativo_total)
    roic_percent = roic * 100 if roic is not None else None

    return {
        'pmr': pmr, 'pme': pme, 'pmp': pmp, 
        'ciclo_financeiro': ciclo_financeiro,
        'ild': ild, 'roic': roic_percent
    }

def calculate_z_score_prado(reclassified_data: Dict, base_indicators: Dict) -> Tuple[Optional[float], str]:
    cdg = base_indicators.get('cdg')
    ncg = base_indicators.get('ncg')
    t = base_indicators.get('t')
    tipo_estrutura = base_indicators.get('tipoestrutura')
    ativo_total = reclassified_data.get('ativo_total')
    receita_liquida = reclassified_data.get('dre_receita')
    pcf = reclassified_data.get('pc_fornecedores', 0) + reclassified_data.get('pc_outros', 0) + reclassified_data.get('t_dividacurtoprazo', 0)
    pnc = reclassified_data.get('pnc_total')
    
    if any(v is None for v in [cdg, ncg, t, tipo_estrutura, ativo_total, receita_liquida, pnc]):
        return None, "Dados insuficientes"
           
    x1 = safe_divide(cdg, ativo_total)
    x2 = safe_divide(ncg, receita_liquida)
    x3 = float(tipo_estrutura)
    x4 = safe_divide(t, abs(ncg)) if ncg != 0 else 0
    x5 = safe_divide((pcf + pnc), ativo_total)

    if any(x is None for x in [x1, x2, x4, x5]):
        return None, "Cálculo impossível"

    z = 1.887 + (0.899 * x1) + (0.971 * x2) - (0.444 * x3) + (0.055 * x4) - (0.980 * x5)

    if z > 2.675: 
        risk_class = "Classe A (Risco Mínimo)"
    elif 2.0 < z <= 2.675: 
        risk_class = "Classe B"
    elif 1.5 < z <= 2.0: 
        risk_class = "Classe C"
    elif 1.0 < z <= 1.5: 
        risk_class = "Classe D (Atenção)"
    else: 
        risk_class = "Classe E (Risco Elevado)"
       
    return round(z, 4), risk_class

def run_multi_year_analysis(company_df: pd.DataFrame, cvm_code: int, years: List[int]) -> Tuple[Dict, str]:
    company_df.columns = [col.lower() for col in company_df.columns]
    
    all_results = []
    company_name = company_df['denom_cia'].iloc[0] if not company_df.empty else f"Empresa CVM {cvm_code}"
    company_df['dt_refer'] = pd.to_datetime(company_df['dt_refer'])
    
    total_ncg = 0.0
    total_cdg = 0.0
    total_t = 0.0
    count = 0

    for year in sorted(years):
        reference_date = pd.to_datetime(f"{year}-12-31")
        df_year = company_df[company_df['dt_refer'] == reference_date].copy()
        
        if df_year.empty:
            logger.warning(f"Dados não encontrados: Ano {year} | CVM {cvm_code}")
            continue
        
        try:
            reclassified_data = reclassify_and_sum(df_year)
            base_indicators, tipo_estrutura = calculate_fleuriet_indicators(reclassified_data)
            advanced_indicators = calculate_advanced_indicators(reclassified_data, base_indicators)
            z_score, z_risk = calculate_z_score_prado(reclassified_data, base_indicators)
            
            advanced_indicators['z_score'] = z_score
            advanced_indicators['z_risk'] = z_risk
            
            total_assets = reclassified_data.get('ativo_total')
            if total_assets and total_assets > 0:
                total_ncg += base_indicators['ncg'] / total_assets
                total_cdg += base_indicators['cdg'] / total_assets
                total_t += base_indicators['t'] / total_assets
                count += 1
            
            all_results.append({
                'year': year,
                'base_indicators': base_indicators,
                'advanced_indicators': advanced_indicators,
                'structure': tipo_estrutura
            })
        except Exception as e:
            logger.error(f"Erro processamento: Ano {year} | CVM {cvm_code}: {e}", exc_info=True)
            return None, f"Erro ano {year}."
    
    if not all_results:
        return None, f"Sem dados: {min(years)} a {max(years)}."
    
    chart_data = {
        'labels': [res['year'] for res in all_results],
        'ncg': [res['base_indicators']['ncg'] for res in all_results],
        'cdg': [res['base_indicators']['cdg'] for res in all_results],
        't': [res['base_indicators']['t'] for res in all_results],
    }
    
    ncg_percentage = total_ncg / count if count > 0 else 0.0
    cdg_percentage = total_cdg / count if count > 0 else 0.0
    t_percentage = total_t / count if count > 0 else 0.0
    
    final_result = {
        'company_name': company_name,
        'cvm_code': cvm_code,
        'yearly_results': all_results,
        'chart_data': chart_data,
        'financial_status': all_results[-1]['structure'] if all_results else "N/D",
        'ncg_percentage': ncg_percentage,
        'cdg_percentage': cdg_percentage,
        't_percentage': t_percentage
    }

    return final_result, None
