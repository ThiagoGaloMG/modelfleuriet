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
    """Calcula indicadores avançados de forma segura."""
    
    # Extrai valores necessários do dicionário de dados reclassificados
    receita_liquida = reclassified_data.get("DRE_Receita")
    custo_produtos = reclassified_data.get("DRE_Custo")
    clientes = reclassified_data.get("AC_Clientes")
    estoques = reclassified_data.get("AC_Estoques")
    fornecedores = reclassified_data.get("PC_Fornecedores")
    ncg = base_indicators.get("NCG")
    lucro_operacional = reclassified_data.get("DRE_LucroOperacional")
    imposto_renda = reclassified_data.get("DRE_ImpostoRenda")
    ativo_total = reclassified_data.get("Ativo_Total")
    
    # Supõe-se que Compras = Custo dos Produtos. É uma simplificação comum.
    compras = custo_produtos
    
    # Calcula os prazos médios usando a função de divisão segura
    pmr = safe_divide(clientes, receita_liquida) * 365 if receita_liquida is not None else None
    pme = safe_divide(estoques, custo_produtos) * 365 if custo_produtos is not None else None
    pmp = safe_divide(fornecedores, compras) * 365 if compras is not None else None
    
    # Calcula o Ciclo de Caixa (Cash to Cash)
    ciclo_caixa = None
    if all(p is not None for p in [pmr, pme, pmp]):
        ciclo_caixa = pmr + pme - pmp
        
    # Calcula o Índice de Liquidez Dinâmica (ILD)
    t = base_indicators.get("T")
    anc = reclassified_data.get("ANC")
    ild = safe_divide(t, (anc + ncg)) if all(v is not None for v in [t, anc, ncg]) and (anc + ncg) != 0 else None

    # Calcula o ROIC (Return on Invested Capital)
    # NOPAT = Lucro Operacional - Imposto de Renda sobre o Lucro Operacional
    # Capital Investido = Ativo Total - Passivo Circulante Operacional (PC_Fornecedores)
    # Assumindo que o imposto de renda é sobre o lucro operacional para o cálculo do NOPAT
    roic = None
    if lucro_operacional is not None and imposto_renda is not None and ativo_total is not None and fornecedores is not None:
        nopat = lucro_operacional - imposto_renda
        capital_investido = ativo_total - fornecedores # Simplificação: Capital Investido = Ativo Total - Fornecedores
        roic = safe_divide(nopat, capital_investido) * 100 if capital_investido != 0 else None

    return {
        "PMR": pmr,
        "PME": pme,
        "PMP": pmp,
        "C2C": ciclo_caixa,
        "ILD": ild,
        "ROIC": roic,
    }

def calculate_z_score_prado(reclassified_data: Dict, base_indicators: Dict) -> Tuple[Optional[float], str]:
    """Calcula o Z-Score de Prado de forma segura."""
    
    cdg = base_indicators.get("CDG")
    ncg = base_indicators.get("NCG")
    t = base_indicators.get("T")
    ativo_total = reclassified_data.get("Ativo_Total")
    receita_liquida = reclassified_data.get("DRE_Receita")
    
    # A instrução original não menciona 'TipoEstrutura' no base_indicators, mas sim no reclassified_data.
    # Assumindo que 'TipoEstrutura' virá de reclassified_data ou que o valor padrão 0 é aceitável.
    # Se for um campo de texto, precisará de um mapeamento para número.
    # Por simplicidade, vou manter como está no arquivo de instrução, mas isso pode ser um ponto de atenção.
    # Para o cálculo do Z-Score, 'tipo_estrutura' deve ser um valor numérico.
    # Se 'tipo_estrutura' for uma string como "Estrutura Ótima", precisamos mapeá-la para um número.
    # Por enquanto, vou usar um valor padrão 0 se não for encontrado ou não for numérico.
    tipo_estrutura_str = base_indicators.get("TipoEstrutura", {}).get("tipo", "Não identificado")
    tipo_estrutura = 0 # Valor padrão
    if "Ótima" in tipo_estrutura_str or "Excelente" in tipo_estrutura_str: tipo_estrutura = 4
    elif "Boa" in tipo_estrutura_str: tipo_estrutura = 3
    elif "Risco" in tipo_estrutura_str: tipo_estrutura = 2
    elif "Péssima" in tipo_estrutura_str or "Insolvência" in tipo_estrutura_str: tipo_estrutura = 1

    # Extrai passivos circulantes e não circulantes totais
    # Estes campos não estão no FLEURIET_ACCOUNT_MAPPING fornecido, assumindo que são calculados ou obtidos de outra forma.
    # Para evitar KeyError, usarei .get com valor padrão None.
    # Adicionei PC e PNC ao mapeamento FLEURIET_ACCOUNT_MAPPING para que possam ser obtidos.
    pcf = reclassified_data.get("PC") # Passivo Circulante
    pncf = reclassified_data.get("PNC") # Passivo Não Circulante
    
    # Verifica se os valores essenciais existem
    if any(v is None for v in [cdg, ncg, t, ativo_total, receita_liquida, pcf, pncf]):
        return None, "Dados insuficientes"
        
    # Calcula os Xis usando safe_divide
    x1 = safe_divide(cdg, ativo_total)
    x2 = safe_divide(ncg, receita_liquida)
    x3 = float(tipo_estrutura) # Tipo de estrutura já deve ser um número
    x4 = safe_divide(t, abs(ncg)) if ncg is not None and ncg != 0 else None # Trata o caso de NCG ser 0 aqui
    x5 = safe_divide((pcf + pncf), ativo_total) if ativo_total != 0 else None

    # Se qualquer X for nulo, não é possível calcular o Z-Score
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
    
    return z, risk_class

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
            
            # Passa o tipo_estrutura para calculate_z_score_prado via base_indicators
            base_indicators['TipoEstrutura'] = {'tipo': tipo_estrutura} # Adiciona ao dicionário base_indicators
            
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
