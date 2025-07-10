#!/usr/bin/env python3
import pandas as pd
import numpy as np
import yfinance as yf
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def preprocess_data(file_path, cvm_codes_df):
    logging.info(f"Processando arquivo: {file_path}")
    df = pd.read_csv(file_path, sep=';', encoding='ISO-8859-1')

    # Padronizar nomes das colunas para minúsculas e sem aspas
    df.columns = [col.lower().replace('"', '') for col in df.columns]

    # Renomear colunas para facilitar o acesso
    df = df.rename(columns={
        'cd_cvm': 'CD_CVM',
        'dt_refer': 'DT_REFER',
        'versao': 'VERSAO',
        'cd_conta': 'CD_CONTA',
        'ds_conta': 'DS_CONTA',
        'vl_conta': 'VL_CONTA',
        'st_conta': 'ST_CONTA'
    })

    # Filtrar apenas empresas com CD_CVM presente no mapeamento
    df = df[df['CD_CVM'].isin(cvm_codes_df['CD_CVM'])]

    # Converter VL_CONTA para numérico, tratando vírgulas como separador decimal
    df['VL_CONTA'] = df['VL_CONTA'].astype(str).str.replace(',', '.', regex=False)
    df['VL_CONTA'] = pd.to_numeric(df['VL_CONTA'], errors='coerce')

    # Preencher valores NaN em VL_CONTA com 0
    df['VL_CONTA'] = df['VL_CONTA'].fillna(0)

    # Garantir que DT_REFER é datetime
    df['DT_REFER'] = pd.to_datetime(df['DT_REFER'])

    # Filtrar apenas o último exercício para cada empresa e conta
    df_filtered = df.loc[df.groupby(['CD_CVM', 'CD_CONTA'])['DT_REFER'].idxmax()]

    return df_filtered

def get_company_info(ticker):
    try:
        info = yf.Ticker(ticker + '.SA').info
        return info.get('longName', 'N/A')
    except Exception as e:
        logging.warning(f"Erro ao obter info do yfinance para {ticker}.SA: {e}")
        return 'N/A'

def process_all_data(base_path='.'):
    logging.info("Iniciando o processamento de todos os dados.")
    # Carregar mapeamento de tickers
    mapeamento_path = os.path.join(base_path, 'mapeamento_tickers.csv')
    if not os.path.exists(mapeamento_path):
        logging.error(f"Arquivo não encontrado: {mapeamento_path}")
        return None
    mapeamento_tickers_df = pd.read_csv(mapeamento_path, sep=';', encoding='ISO-8859-1')
    mapeamento_tickers_df.columns = [col.lower().replace('"', '') for col in mapeamento_tickers_df.columns]
    mapeamento_tickers_df = mapeamento_tickers_df.rename(columns={'cd_cvm': 'CD_CVM', 'ticker': 'TICKER', 'nome_empresa': 'NOME_EMPRESA'})

    # Filtrar mapeamento para garantir que CD_CVM é numérico e válido
    mapeamento_tickers_df['CD_CVM'] = pd.to_numeric(mapeamento_tickers_df['CD_CVM'], errors='coerce')
    mapeamento_tickers_df = mapeamento_tickers_df.dropna(subset=['CD_CVM'])
    mapeamento_tickers_df['CD_CVM'] = mapeamento_tickers_df['CD_CVM'].astype(int)

    # Obter nomes das empresas usando yfinance
    mapeamento_tickers_df['NOME_EMPRESA_YAHOO'] = mapeamento_tickers_df['TICKER'].apply(get_company_info)

    # Lista de arquivos a serem processados
    files = [
        'bpa_consolidado.csv',
        'bpp_consolidado.csv',
        'dfc_mi_consolidado.csv',
        'dre_consolidado.csv'
    ]

    all_data = []
    for f in files:
        file_path = os.path.join(base_path, f)
        if os.path.exists(file_path):
            processed_df = preprocess_data(file_path, mapeamento_tickers_df)
            if processed_df is not None:
                all_data.append(processed_df)
        else:
            logging.warning(f"Arquivo não encontrado, pulando: {file_path}")

    if not all_data:
        logging.error("Nenhum dado consolidado foi processado.")
        return None

    consolidated_df = pd.concat(all_data, ignore_index=True)

    # Juntar com o mapeamento de tickers para adicionar o nome da empresa
    consolidated_df = pd.merge(consolidated_df, mapeamento_tickers_df[['CD_CVM', 'TICKER', 'NOME_EMPRESA_YAHOO']], on='CD_CVM', how='left')

    # Renomear a coluna de nome da empresa para 'EMPRESA'
    consolidated_df = consolidated_df.rename(columns={'NOME_EMPRESA_YAHOO': 'EMPRESA'})

    # Selecionar e reordenar colunas relevantes
    final_columns = [
        'CD_CVM', 'DT_REFER', 'VERSAO', 'CD_CONTA', 'DS_CONTA', 'VL_CONTA', 'ST_CONTA', 'TICKER', 'EMPRESA'
    ]
    consolidated_df = consolidated_df[final_columns]

    logging.info("Processamento de dados concluído.")
    return consolidated_df

if __name__ == '__main__':
    # Exemplo de uso:
    # Certifique-se de que os arquivos CSV estão no mesmo diretório ou ajuste o base_path
    # Ex: python preprocess_to_db_light.py
    processed_data = process_all_data()
    if processed_data is not None:
        print(processed_data.head())
        print(processed_data['EMPRESA'].unique())
    else:
        print("Falha no processamento dos dados.")
