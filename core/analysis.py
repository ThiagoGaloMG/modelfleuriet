# modelfleuriet/core/analysis.py

import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Tuple, Optional, Any

logger = logging.getLogger(__name__)

def calculate_fleuriet_metrics(df_company: pd.DataFrame, cvm_code: int, year: int) -> Dict[str, float]:
    """
    Calcula as métricas do Modelo Fleuriet para um ano específico a partir de um DataFrame de dados CVM.
    """
    # Filtrar dados da empresa para o ano e tipo de demonstração (DFP - Demonstrações Financeiras Padronizadas)
    # Prioriza ST_CONTA = 'D' (DFP Consolidado) ou 'DFP' se houver
    # Se não, pega o que tiver (pode ser 'I' de ITR)
    df_year = df_company[
        (df_company['DT_REFER'].dt.year == year) &
        (df_company['ST_CONTA'].isin(['D', 'DFP'])) # Prioriza DFP
    ].copy()

    if df_year.empty:
        df_year = df_company[
            (df_company['DT_REFER'].dt.year == year) &
            (df_company['ST_CONTA'].isin(['I', 'ITR'])) # Tenta ITR se DFP não encontrado
        ].copy()
        if df_year.empty:
            logger.warning(f"Nenhum dado DFP/ITR encontrado para o ano {year} para a empresa CVM {cvm_code}.")
            return {}

    # Função auxiliar para buscar valores de contas
    def get_account_value(df_filtered: pd.DataFrame, account_code: str, default_value: float = 0.0) -> float:
        # Pega o valor da conta mais recente para o ano, se houver duplicatas
        val = df_filtered[df_filtered['CD_CONTA'] == account_code]['VL_CONTA'].iloc[0] if not df_filtered[df_filtered['CD_CONTA'] == account_code].empty else default_value
        return float(val) if pd.notna(val) else default_value

    # Coleta de dados para o Modelo Fleuriet
    # As contas são baseadas nas nomenclaturas da CVM e no seu TCC.
    # É crucial que o preprocess_to_db_light.py insira essas contas corretamente.
    
    ac = get_account_value(df_year, '1.01') # Ativo Circulante
    pc = get_account_value(df_year, '2.01') # Passivo Circulante
    est = get_account_value(df_year, '1.01.04') # Estoques
    cr = get_account_value(df_year, '1.01.03') # Contas a Receber
    forn = get_account_value(df_year, '2.01.02') # Fornecedores
    
    # Ativo Realizável a Longo Prazo (ARLP) - Usar 1.02.01 (Ativo Não Circulante - Investimentos) ou 1.02 para Ativo Não Circulante Total
    # No TCC, ARLP é usado para calcular Capital de Giro Próprio (CGP)
    arlp = get_account_value(df_year, '1.02.01') # Ativo Não Circulante - Investimentos
    if arlp == 0: # Se 1.02.01 for zero, tenta 1.02 (Ativo Não Circulante total)
        arlp = get_account_value(df_year, '1.02')

    pnc = get_account_value(df_year, '2.02') # Passivo Não Circulante
    pl = get_account_value(df_year, '2.03') # Patrimônio Líquido
    
    # Ativo Permanente (AP) - Usar 1.02 (Ativo Não Circulante)
    ap = get_account_value(df_year, '1.02') # Ativo Não Circulante

    caixa = get_account_value(df_year, '1.01.01') # Caixa e Equivalentes

    # --- Cálculos do Modelo Fleuriet ---
    # Necessidade de Capital de Giro (NCG)
    ncg = (est + cr) - forn

    # Capital de Giro (CG)
    cg = ac - pc

    # Capital de Giro Próprio (CGP)
    cgp = pl + pnc - ap
    # Ou, se AP = Ativo Não Circulante: cgp = pl + pnc - Ativo Nao Circulante

    # Saldo em Tesouraria (T)
    t = cg - ncg

    # Situação Financeira (Tesouraria)
    situacao_financeira = ""
    interpretacao = ""

    if t > 0:
        situacao_financeira = "Saudável (Tesouraria Positiva)"
        interpretacao = "A empresa possui excedente de recursos de Capital de Giro, indicando uma boa saúde financeira e capacidade de honrar compromissos de curto prazo."
    elif t < 0:
        situacao_financeira = "Problemática (Tesouraria Negativa)"
        interpretacao = "A empresa está com escassez de Capital de Giro, podendo enfrentar dificuldades para honrar suas obrigações de curto prazo. Necessita de atenção e possíveis ajustes financeiros."
    else:
        situacao_financeira = "Equilibrada (Tesouraria Zero)"
        interpretacao = "A empresa possui um equilíbrio entre suas necessidades e fontes de Capital de Giro. Uma situação neutra que pode ser otimizada."

    return {
        'year': year,
        'ncg': ncg,
        'cg': cg,
        'cgp': cgp,
        't': t,
        'situacao_financeira': situacao_financeira,
        'interpretacao': interpretacao,
        'raw_data': { # Incluir dados brutos usados para depuração
            'ac': ac, 'pc': pc, 'est': est, 'cr': cr, 'forn': forn,
            'arlp': arlp, 'pnc': pnc, 'pl': pl, 'ap': ap, 'caixa': caixa
        }
    }

def run_multi_year_analysis(df_company: pd.DataFrame, cvm_code: int, years_to_analyze: List[int]) -> Tuple[Dict[str, Any], Optional[str]]:
    """
    Executa a análise do Modelo Fleuriet para múltiplos anos para uma empresa.
    Retorna os resultados e um erro se houver.
    """
    company_name = df_company['DENOM_CIA'].iloc[0] if not df_company.empty else f"Empresa CVM {cvm_code}"
    
    all_fleuriet_results = []
    chart_labels = []
    chart_ncg = []
    chart_cdg = []
    chart_t = []

    for year in sorted(years_to_analyze):
        metrics = calculate_fleuriet_metrics(df_company, cvm_code, year)
        if metrics:
            all_fleuriet_results.append(metrics)
            chart_labels.append(str(year))
            chart_ncg.append(metrics['ncg'])
            chart_cdg.append(metrics['cg']) # CDG é o Capital de Giro (CG)
            chart_t.append(metrics['t'])
        else:
            logger.warning(f"Não foi possível calcular métricas Fleuriet para {company_name} no ano {year}.")

    if not all_fleuriet_results:
        return {}, f"Nenhum resultado Fleuriet válido encontrado para a empresa CVM {cvm_code} nos anos {years_to_analyze}."

    # Determinar a situação financeira geral (do último ano analisado)
    latest_year_results = all_fleuriet_results[-1]

    return {
        'company_name': company_name,
        'cvm_code': str(cvm_code),
        'start_year': years_to_analyze[0],
        'end_year': years_to_analyze[-1],
        'results': { # Resumo do último ano
            'situacao_financeira': latest_year_results['situacao_financeira'],
            'interpretacao': latest_year_results['interpretacao'],
            'ncg_latest': latest_year_results['ncg'],
            'cg_latest': latest_year_results['cg'],
            't_latest': latest_year_results['t']
        },
        'chart_data': {
            'labels': chart_labels,
            'ncg': chart_ncg,
            'cdg': chart_cdg,
            't': chart_t
        },
        'details_by_year': all_fleuriet_results
    }, None # Retorna None para o erro, indicando sucesso
