# modelfleuriet/flask_app.py

import pandas as pd
import json
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import os
import numpy as np
import logging
from datetime import datetime, timedelta
import traceback

# Configura logging para a aplicação Flask
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Adiciona o diretório 'core' ao sys.path para importar os módulos
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'core')))

# Importa os módulos da nova estrutura 'core'
from core.db_manager import SupabaseDB # Gerenciador do PostgreSQL do Render
from core.ibovespa_analysis_system import IbovespaAnalysisSystem # Sistema de análise de Valuation
from core.analysis import run_multi_year_analysis # Lógica do Modelo Fleuriet
from core.utils import clean_data_for_json # Utilitários para limpeza de JSON
from core.ibovespa_utils import get_ibovespa_tickers # Para a lista de tickers (usada internamente pelo sistema de análise)

# --- Inicialização da Aplicação Flask ---
# Configura o static_folder para servir os arquivos do frontend React (após o build)
app = Flask(__name__, static_folder=os.path.abspath(os.path.join(os.path.dirname(__file__), 'frontend', 'dist')))

# Habilita CORS para todas as rotas
CORS(app)

# Configuração para serialização JSON (para lidar com tipos numpy e pandas)
class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer): return int(obj)
        if isinstance(obj, np.floating): return float(obj)
        if isinstance(obj, np.ndarray): return obj.tolist()
        if isinstance(obj, pd.Timestamp): return obj.isoformat()
        if pd.isna(obj): return None
        return super().default(obj)

app.json_encoder = CustomJSONEncoder # Usa o encoder personalizado

# --- Inicialização Global de Componentes ---
# Instâncias dos sistemas que serão inicializadas uma vez
db_manager_instance = None
ibovespa_analysis_system_instance = None
ticker_mapping_df = None # Mapeamento CVM <-> Ticker (carregado de mapeamento_tickers.csv)

def get_db_manager():
    global db_manager_instance
    if db_manager_instance is None:
        logger.info("Inicializando SupabaseDB manager (PostgreSQL) pela primeira vez...")
        db_manager_instance = SupabaseDB()
        # Teste de conexão no startup
        try:
            db_manager_instance.get_engine() # Tenta criar a engine para testar a conexão
        except Exception as e:
            logger.critical(f"Falha crítica na inicialização da conexão com o DB: {e}. A aplicação pode não funcionar.")
            db_manager_instance = None # Reseta se falhar
    return db_manager_instance

def get_ticker_mapping_df():
    global ticker_mapping_df
    if ticker_mapping_df is None:
        file_path = os.path.join(os.path.dirname(__file__), 'data', 'mapeamento_tickers.csv') # Caminho na raiz do repositório
        logger.info(f"Carregando mapeamento de tickers de {file_path}...")
        try:
            # Garante que o mapeamento está no formato TICKER, CD_CVM, NOME_EMPRESA
            df_tickers_mapping = pd.read_csv(file_path, sep=',')
            df_tickers_mapping.columns = [col.strip().upper() for col in df_tickers_mapping.columns]
            df_tickers_mapping['CD_CVM'] = pd.to_numeric(df_tickers_mapping['CD_CVM'], errors='coerce').dropna().astype(int)
            # Seleciona colunas específicas para evitar problemas com colunas extras
            df_tickers_mapping = df_tickers_mapping[['CD_CVM', 'TICKER', 'NOME_EMPRESA']].drop_duplicates(subset=['CD_CVM'])
            logger.info(f"{len(df_tickers_mapping)} mapeamentos carregados.")
        except Exception as e:
            logger.error(f"Erro ao carregar mapeamento de tickers de '{file_path}': {e}", exc_info=True)
            ticker_mapping_df = pd.DataFrame() # Garante que seja um DataFrame vazio
    return df_tickers_mapping

def get_ibovespa_analysis_system():
    global ibovespa_analysis_system_instance
    if ibovespa_analysis_system_instance is None:
        db_manager = get_db_manager()
        ticker_map = get_ticker_mapping_df()
        if db_manager and not ticker_map.empty:
            logger.info("Inicializando IbovespaAnalysisSystem pela primeira vez...")
            ibovespa_analysis_system_instance = IbovespaAnalysisSystem(db_manager, ticker_map)
            logger.info("IbovespaAnalysisSystem inicializado.")
        else:
            logger.error("Não foi possível inicializar IbovespaAnalysisSystem: DB Manager ou mapeamento de tickers ausente/vazio.")
    return ibovespa_analysis_system_instance

# Tenta carregar o mapeamento e o DB manager no startup do Flask.
# Se o DB não conectar, a instância do manager será None.
get_db_manager()
get_ticker_mapping_df()


# --- Rotas Flask ---

# Rota de health check para o Render
@app.route('/health', methods=['GET'])
@app.route('/api/health', methods=['GET']) # Mantém compatibilidade com /api/health
@cross_origin()
def health_check():
    try:
        db_ok = get_db_manager() is not None and get_db_manager().get_engine() is not None # Verifica se engine foi criada
        ticker_map_ok = not get_ticker_mapping_df().empty
        system_ok = get_ibovespa_analysis_system() is not None # Tenta inicializar
        
        status = {
            'status': 'healthy',
            'message': 'API de Análise Financeira está funcionando',
            'timestamp': datetime.now().isoformat(),
            'db_connected': db_ok,
            'ticker_mapping_loaded': ticker_map_ok,
            'valuation_system_initialized': system_ok,
        }
        logger.info("Health check realizado com sucesso.")
        return jsonify(status), 200
    except Exception as e:
        logger.error(f"Erro no health check: {str(e)}\n{traceback.format_exc()}")
        return jsonify({
            'status': 'error',
            'message': f'Erro no health check: {str(e)}',
            'timestamp': datetime.now().isoformat()
        }), 500

# Rota para servir os arquivos estáticos do frontend React
@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def serve_frontend(path):
    """
    Serve os arquivos estáticos do frontend (build do React).
    Se o path for vazio ou não corresponder a um arquivo, serve o index.html.
    """
    if app.static_folder is None:
        logger.error("Static folder not configured for Flask app.")
        return "Static folder not configured", 404

    full_path = os.path.join(app.static_folder, path)
    
    # Se o caminho for um arquivo existente, sirva-o
    if path != "" and os.path.exists(full_path) and os.path.isfile(full_path):
        logger.info(f"Servindo arquivo estático: {path}")
        return send_from_directory(app.static_folder, path)
    # Caso contrário, serve o index.html (SPA fallback)
    else:
        logger.info(f"Servindo index.html para path: {path}")
        return send_from_directory(app.static_folder, 'index.html')

# --- Rotas para o Modelo Fleuriet (API para o Frontend React) ---
@app.route('/api/fleuriet/companies', methods=['GET'])
@cross_origin()
def get_fleuriet_companies_api():
    """Retorna a lista de empresas para o dropdown do Modelo Fleuriet."""
    db_manager = get_db_manager()
    ticker_map = get_ticker_mapping_df()
    if not db_manager or ticker_map.empty:
        logger.error("DB Manager ou mapeamento de tickers não disponível para Fleuriet companies.")
        return jsonify({"error": "Serviço indisponível. Tente novamente mais tarde."}), 503

    engine = db_manager.get_engine()
    if not engine:
        return jsonify({"error": "Conexão com o banco de dados não estabelecida."}), 500

    try:
        # Busca todas as empresas que têm dados financeiros (CD_CVM) no DB
        query = text("""
            SELECT DISTINCT "CD_CVM", "DENOM_CIA"
            FROM public.financial_data
            ORDER BY "DENOM_CIA";
        """)
        with engine.connect() as connection:
            df_companies_db = pd.read_sql(query, connection)
        
        # Junta com o mapeamento para obter o ticker
        df_companies_db.rename(columns={'denom_cia': 'company_name', 'cd_cvm': 'cvm_code'}, inplace=True)
        final_df = pd.merge(df_companies_db, ticker_map, on='cvm_code', how='left')
        
        # Filtra apenas empresas que têm ticker mapeado e garante formato
        final_df = final_df.dropna(subset=['TICKER'])
        
        companies_list = [
            {'company_id': str(row['cvm_code']), 'company_name': row['company_name'], 'ticker': row['TICKER']}
            for _, row in final_df.iterrows()
        ]
        
        return jsonify(companies_list), 200
    except Exception as e:
        logger.error(f"Erro ao buscar lista de empresas para Fleuriet: {e}", exc_info=True)
        return jsonify({"error": f"Ocorreu um erro ao carregar empresas: {str(e)}"}), 500


@app.route('/api/fleuriet/analyze', methods=['POST'])
@cross_origin()
def analyze_fleuriet_api():
    """Executa a análise do Modelo Fleuriet para a empresa e anos selecionados."""
    try:
        data = request.get_json()
        cvm_code_str = data.get('cvm_code')
        start_year = data.get('start_year')
        end_year = data.get('end_year')

        if not cvm_code_str or not start_year or not end_year:
            return jsonify({"error": "Parâmetros cvm_code, start_year e end_year são obrigatórios."}), 400

        cvm_code = int(cvm_code_str)
        years_to_analyze = list(range(int(start_year), int(end_year) + 1))

        db_manager = get_db_manager()
        if not db_manager:
            return jsonify({"error": "Conexão com o banco de dados não estabelecida."}), 500

        engine = db_manager.get_engine()
        if not engine:
            return jsonify({"error": "Conexão com o banco de dados não estabelecida."}), 500
        
        # Busca os dados financeiros da CVM para a empresa e anos selecionados
        df_company_query = text(f"""
            SELECT "CNPJ_CIA", "CD_CVM", "DENOM_CIA", "DT_REFER", "CD_CONTA", "DS_CONTA", "VL_CONTA", "ST_CONTA"
            FROM public.financial_data
            WHERE "CD_CVM" = :cvm_code
            AND EXTRACT(YEAR FROM "DT_REFER") BETWEEN :start_year AND :end_year
            ORDER BY "DT_REFER" ASC, "ST_CONTA" DESC, "CD_CONTA" ASC;
        """)
        with engine.connect() as connection:
            df_company = pd.read_sql(df_company_query, connection, params={'cvm_code': cvm_code, 'start_year': int(start_year), 'end_year': int(end_year)})

        if df_company.empty:
            return jsonify({"error": f"Nenhum dado financeiro encontrado para a empresa CVM {cvm_code} nos anos {start_year}-{end_year}. Por favor, verifique se os dados CVM foram pré-processados para esses anos."}), 404
        
        fleuriet_results, fleuriet_error = run_multi_year_analysis(df_company, cvm_code, years_to_analyze)

        if fleuriet_error:
            return jsonify({"error": fleuriet_error}), 500

        cleaned_results = clean_data_for_json(fleuriet_results)

        return jsonify(cleaned_results), 200

    except Exception as e:
        logger.error(f"Erro na análise Fleuriet via API: {e}", exc_info=True)
        return jsonify({"error": f"Ocorreu um erro inesperado na análise Fleuriet: {str(e)}"}), 500

# --- Rotas para o Valuation (API para o Frontend React) ---
@app.route('/api/financial/analyze/complete', methods=['POST'])
@cross_origin()
def run_complete_analysis_api():
    """
    Executa a análise completa do Ibovespa ou para um número limitado de empresas.
    """
    try:
        logger.info("Iniciando análise completa do Ibovespa via API")
        
        system = get_ibovespa_analysis_system()
        
        data = request.get_json(silent=True)
        num_companies = None
        if data and 'num_companies' in data:
            try:
                num_companies = int(data['num_companies'])
                if num_companies <= 0:
                    num_companies = None
                logger.info(f"Requisição para análise rápida de {num_companies} empresas.")
            except ValueError:
                logger.warning("Valor inválido para 'num_companies'. Analisando todas as empresas.")
                num_companies = None
        
        start_time = datetime.now()
        report = system.run_complete_analysis(num_companies=num_companies)
        end_time = datetime.now()
        
        report['execution_time_seconds'] = (end_time - start_time).total_seconds()
        
        return jsonify(report), 200
        
    except Exception as e:
        logger.error(f"Erro ao executar análise completa: {str(e)}\n{traceback.format_exc()}")
        return jsonify({
            'status': 'error',
            'message': 'Erro interno do servidor ao executar análise completa',
            'error_details': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/api/financial/analyze/company/<ticker>', methods=['GET'])
@cross_origin()
def get_company_analysis_api(ticker):
    """
    Obtém dados de análise detalhados para uma empresa específica.
    """
    try:
        logger.info(f"Iniciando análise para empresa específica: {ticker}")
        
        system = get_ibovespa_analysis_system()
        analysis_result = system.get_company_analysis(ticker)
        
        return jsonify(analysis_result), 200
        
    except Exception as e:
        logger.error(f"Erro ao obter dados para {ticker}: {str(e)}\n{traceback.format_exc()}")
        return jsonify({
            'status': 'error',
            'message': f'Erro interno do servidor ao obter dados para {ticker}',
            'error_details': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/api/financial/companies', methods=['GET'])
@cross_origin()
def get_ibovespa_companies_list_api():
    """
    Obtém a lista de todas as empresas do Ibovespa conhecidas pelo sistema de Valuation.
    """
    try:
        logger.info("Obtendo lista de empresas do Ibovespa para Valuation.")
        system = get_ibovespa_analysis_system()
        
        companies = system.get_ibovespa_company_list()
        
        result = {
            'companies': companies,
            'total': len(companies),
            'timestamp': datetime.now().isoformat(),
            'api_version': '1.0'
        }
        
        logger.info(f"Lista de empresas retornada: {len(companies)} empresas.")
        return jsonify(result), 200
        
    except Exception as e:
        logger.error(f"Erro ao obter lista de empresas do Ibovespa: {str(e)}\n{traceback.format_exc()}")
        return jsonify({
            'error': 'Erro interno do servidor ao obter lista de empresas',
            'message': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

# --- Rotas para o Worker de Valuation (se você quiser acionar manualmente) ---
@app.route('/api/valuation/run_worker', methods=['POST'])
@cross_origin()
def run_valuation_worker_api():
    """
    Aciona o worker de valuation para recalcular e persistir os dados de valuation.
    Este endpoint pode ser chamado pelo frontend ou por um agendador.
    """
    try:
        logger.info("Requisição para executar worker de valuation recebida.")
        
        system = get_ibovespa_analysis_system()
        
        if not system:
            return jsonify({"success": False, "error": "Sistema de análise de Valuation não inicializado. Verifique a conexão com o DB e o mapeamento de tickers."}), 500

        system.run_complete_analysis(num_companies=None, force_recollect=True) # Roda a análise completa e força re-coleta

        logger.info("Worker de valuation acionado com sucesso.")
        return jsonify({"success": True, "message": "Worker de valuation acionado. Os dados serão atualizados em breve."}), 200
    except Exception as e:
        logger.error(f"Erro ao acionar worker de valuation: {e}", exc_info=True)
        return jsonify({"success": False, "error": f"Falha ao acionar worker: {str(e)}"}), 500

# --- Rotas de Erro (para compatibilidade, mas o React lidará com a maioria) ---
@app.errorhandler(404)
def page_not_found(e):
    return send_from_directory(app.static_folder, 'index.html')

@app.errorhandler(500)
def internal_server_error(e):
    logger.error(f"Erro 500: {e}", exc_info=True)
    return jsonify({"error": "Ocorreu um erro interno no servidor."}), 500


if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8080))
    logger.info(f"Iniciando a aplicação Flask na porta {port}...")
    app.run(host='0.0.0.0', port=port, debug=os.environ.get('FLASK_DEBUG', 'False') == 'True')
