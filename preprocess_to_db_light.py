#!/usr/bin/env python3
import pandas as pd
import numpy as np
import yfinance as yf
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def preprocess_data(file_path, cvm_codes_df):
    logging.info(f"Processando: {file_path}")
    df = pd.read_csv(file_path, sep=';', encoding='ISO-8859-1')

    df.columns = [col.lower().replace('"', '') for col in df.columns]

    df = df.rename(columns={
        'cd_cvm': 'cd_cvm',
        'dt_refer': 'dt_refer',
        'versao': 'versao',
        'cd_conta': 'cd_conta',
        'ds_conta': 'ds_conta',
        'vl_conta': 'vl_conta',
        'st_conta': 'st_conta',
        'denom_cia': 'denom_cia'
    })

    df = df[df['cd_cvm'].isin(cvm_codes_df['cd_cvm'])]

    df['vl_conta'] = df['vl_conta'].astype(str).str.replace(',', '.', regex=False)
    df['vl_conta'] = pd.to_numeric(df['vl_conta'], errors='coerce')
    df['vl_conta'] = df['vl_conta'].fillna(0)
    df['dt_refer'] = pd.to_datetime(df['dt_refer'])
    df = df.sort_values(['cd_cvm', 'cd_conta', 'dt_refer', 'versao'], ascending=[True, True, False, False])
    df_filtered = df.drop_duplicates(subset=['cd_cvm', 'cd_conta', 'dt_refer'], keep='first')

    return df_filtered

def get_company_info(ticker):
    try:
        info = yf.Ticker(ticker + '.sa').info
        return info.get('longname', 'N/A')
    except Exception as e:
        logging.warning(f"Erro yfinance: {ticker}.sa: {e}")
        return 'N/A'

def process_all_data(base_path='.'):
    logging.info("Iniciando processamento")
    mapeamento_path = os.path.join(base_path, 'mapeamento_tickers.csv')
    if not os.path.exists(mapeamento_path):
        logging.error(f"Arquivo não encontrado: {mapeamento_path}")
        return None
    
    mapeamento_tickers_df = pd.read_csv(mapeamento_path, sep=",", encoding='utf-8')
    mapeamento_tickers_df.columns = [col.lower().replace('"', '') for col in mapeamento_tickers_df.columns]
    
    mapeamento_tickers_df = mapeamento_tickers_df.rename(columns={
        'cd_cvm': 'cd_cvm',
        'ticker': 'ticker',
        'nome_empresa': 'nome_empresa'})

    mapeamento_tickers_df['cd_cvm'] = pd.to_numeric(mapeamento_tickers_df['cd_cvm'], errors='coerce')
    mapeamento_tickers_df = mapeamento_tickers_df.dropna(subset=['cd_cvm'])
    mapeamento_tickers_df['cd_cvm'] = mapeamento_tickers_df['cd_cvm'].astype(int)
    mapeamento_tickers_df['nome_empresa_yahoo'] = mapeamento_tickers_df['ticker'].apply(get_company_info)

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
            logging.warning(f"Arquivo não encontrado: {file_path}")

    if not all_data:
        logging.error("Nenhum dado processado.")
        return None

    consolidated_df = pd.concat(all_data, ignore_index=True)
    consolidated_df = pd.merge(consolidated_df, mapeamento_tickers_df[['cd_cvm', 'ticker', 'nome_empresa_yahoo']], on='cd_cvm', how='left')
    consolidated_df = consolidated_df.rename(columns={'nome_empresa_yahoo': 'denom_cia'})
    final_columns = [
        'cd_cvm', 'dt_refer', 'versao', 'cd_conta', 'ds_conta', 'vl_conta', 'st_conta', 'ticker', 'denom_cia'
    ]
    consolidated_df = consolidated_df[final_columns]

    logging.info("Processamento concluído.")
    return consolidated_df

if __name__ == '__main__':
    processed_data = process_all_data()
    if processed_data is not None:
        print(processed_data.head())
        print(processed_data['denom_cia'].unique())
    else:
        print("Falha no processamento.")
