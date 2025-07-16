# ==============================================================================
# flask_app.py - Servidor Principal da Aplicação de Análise Financeira
# VERSÃO FINAL 2.0 - LÓGICA DE SERVIR ARQUIVOS CORRIGIDA
# ==============================================================================
# Este script inicializa e executa a aplicação Flask, que serve como backend
# para o frontend em React e expõe as APIs de análise.
# ==============================================================================

# --- Imports de Bibliotecas Padrão e de Terceiros ---
import os
import sys
import json
import logging
import traceback
from datetime import datetime
import numpy as np
import pandas as pd
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS, cross_origin
from sqlalchemy import text

# --- Configuração de Logging ---
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- Configuração de Caminhos (Paths) ---
# PROJECT_ROOT agora aponta para a pasta /backend, pois é onde este script está.
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

# Adiciona o diretório 'core' (que está dentro de 'backend') ao path do Python.
CORE_PATH = os.path.join(PROJECT_ROOT, 'core')
if CORE_PATH not in sys.path:
    sys.path.insert(0, CORE_PATH)

logger.info(f"Backend Project Root (PROJECT_ROOT): {PROJECT_ROOT}")
logger.info(f"Core Path (adicionado ao sys.path): {CORE_PATH}")

# --- Imports dos Módulos do Projeto ---
from db_manager import SupabaseDB
from ibovespa_analysis_system import IbovespaAnalysisSystem
from analysis import run_multi_year_analysis
from utils import clean_data_for_json
from ibovespa_utils import get_ibovespa_tickers

# --- Inicialização da Aplicação Flask ---
# O Flask agora procurará os arquivos estáticos em uma pasta 'public' dentro do próprio 'backend'.
FRONTEND_BUILD_PATH = os.path.join(PROJECT_ROOT, 'public')
logger.info(f"Configurando pasta estática para servir frontend de: {FRONTEND_BUILD_PATH}")
app = Flask(__name__, static_folder=FRONTEND_BUILD_PATH)
CORS(app)

# --- Encoder JSON Personalizado ---
class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer): return int(obj)
        if isinstance(obj, np.floating): return float(obj)
        if isinstance(obj, np.ndarray): return obj.tolist()
        if isinstance(obj, pd.Timestamp): return obj.isoformat()
        if pd.isna(obj): return None
        return super().default(obj)

app.json_encoder = CustomJSONEncoder

# --- Gerenciamento de Instâncias Globais (Singletons) ---
db_manager_instance = None
ibovespa_analysis_system_instance = None
ticker_mapping_df = None

def get_db_manager():
    global db_manager_instance
    if db_manager_instance is None:
        logger.info("Inicializando SupabaseDB manager (PostgreSQL) pela primeira vez...")
        try:
            db_manager_instance = SupabaseDB()
            db_manager_instance.get_engine()
            logger.info("DB Manager inicializado e conexão testada com sucesso.")
        except Exception as e:
            logger.critical(f"Falha crítica na inicialização da conexão com o DB: {e}. A aplicação pode não funcionar.")
            db_manager_instance = None
    return db_manager_instance

def get_ticker_mapping_df():
    global ticker_mapping_df
    if ticker_mapping_df is None:
        file_path = os.path.join(PROJECT_ROOT, 'data', 'mapeamento_tickers.csv')
        logger.info(f"Carregando mapeamento de tickers de {file_path}...")
        try:
            df = pd.read_csv(file_path, sep=',')
            df.columns = [col.strip().upper() for col in df.columns]
            df['CD_CVM'] = pd.to_numeric(df['CD_CVM'], errors='coerce').dropna().astype(int)
            ticker_mapping_df = df[['CD_CVM', 'TICKER', 'NOME_EMPRESA']].drop_duplicates(subset=['CD_CVM'])
            logger.info(f"{len(ticker_mapping_df)} mapeamentos carregados.")
        except FileNotFoundError:
            logger.error(f"ARQUIVO NÃO ENCONTRADO: Não foi possível encontrar '{file_path}'.")
            ticker_mapping_df = pd.DataFrame() 
        except Exception as e:
            logger.error(f"Erro ao carregar mapeamento de tickers de '{file_path}': {e}", exc_info=True)
            ticker_mapping_df = pd.DataFrame()
    return ticker_mapping_df

def get_ibovespa_analysis_system():
    global ibovespa_analysis_system_instance
    if ibovespa_analysis_system_instance is None:
        db_manager = get_db_manager()
        ticker_map = get_ticker_mapping_df()
        if db_manager and ticker_map is not None and not ticker_map.empty:
            logger.info("Inicializando IbovespaAnalysisSystem pela primeira vez...")
            ibovespa_analysis_system_instance = IbovespaAnalysisSystem(db_manager, ticker_map)
            logger.info("IbovespaAnalysisSystem inicializado.")
        else:
            logger.error("Não foi possível inicializar IbovespaAnalysisSystem.")
    return ibovespa_analysis_system_instance

get_db_manager()
get_ticker_mapping_df()

# ==============================================================================
# --- ROTAS DA APLICAÇÃO ---
# ==============================================================================

# ==============================================================================
# >>>>> CORREÇÃO FINAL ESTÁ AQUI <<<<<
# Lógica simplificada e robusta para servir a aplicação React.
# ==============================================================================
@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
@cross_origin()
def serve_react_app(path):
    """
    Serve a Single Page Application (SPA) React.
    - Se o caminho for um arquivo existente (como /assets/index.js), serve esse arquivo.
    - Para qualquer outro caminho (incluindo a raiz '/'), serve o index.html principal.
    """
    # Verifica se o caminho solicitado corresponde a um arquivo existente na pasta estática
    if path != "" and os.path.exists(os.path.join(app.static_folder, path)):
        return send_from_directory(app.static_folder, path)
    else:
        # Para todos os outros casos, serve o ponto de entrada do React
        return send_from_directory(app.static_folder, 'index.html')

# --- Rotas de API ---
@app.route('/api/health')
@cross_origin()
def health_check():
    """Verifica a saúde dos componentes principais da aplicação."""
    try:
        db_ok = get_db_manager() is not None
        ticker_map_ok = get_ticker_mapping_df() is not None and not get_ticker_mapping_df().empty
        system_ok = get_ibovespa_analysis_system() is not None
        status = {
            'status': 'healthy' if all([db_ok, ticker_map_ok, system_ok]) else 'degraded',
            'timestamp': datetime.now().isoformat(),
            'components': {
                'database_connection': 'ok' if db_ok else 'error',
                'ticker_mapping_loaded': 'ok' if ticker_map_ok else 'error',
                'valuation_system_initialized': 'ok' if system_ok else 'error',
            }
        }
        return jsonify(status), 200
    except Exception as e:
        logger.error(f"Erro no health check: {e}", exc_info=True)
        return jsonify({'status': 'unhealthy', 'error': str(e)}), 500

@app.route('/api/fleuriet/companies', methods=['GET'])
@cross_origin()
def get_fleuriet_companies_api():
    """Retorna a lista de empresas disponíveis para análise no Modelo Fleuriet."""
    db_manager = get_db_manager()
    ticker_map = get_ticker_mapping_df()
    if not db_manager or ticker_map.empty:
        return jsonify({"error": "Serviço temporariamente indisponível."}), 503
    try:
        with db_manager.get_engine().connect() as connection:
            query = text('SELECT DISTINCT "CD_CVM", "DENOM_CIA" FROM public.financial_data ORDER BY "DENOM_CIA";')
            df_companies_db = pd.read_sql(query, connection)
        df_companies_db.rename(columns={'DENOM_CIA': 'NOME_EMPRESA'}, inplace=True)
        final_df = pd.merge(df_companies_db, ticker_map, on='CD_CVM', how='left').dropna(subset=['TICKER'])
        companies_list = [
            {'company_id': str(row['CD_CVM']), 'company_name': row['NOME_EMPRESA_x'], 'ticker': row['TICKER']}
            for _, row in final_df.iterrows()
        ]
        return jsonify(companies_list)
    except Exception as e:
        logger.error(f"Erro ao buscar lista de empresas para Fleuriet: {e}", exc_info=True)
        return jsonify({"error": "Ocorreu um erro ao carregar a lista de empresas."}), 500

@app.route('/api/fleuriet/analyze', methods=['POST'])
@cross_origin()
def analyze_fleuriet_api():
    """Executa a análise do Modelo Fleuriet para uma empresa e período específicos."""
    try:
        data = request.get_json()
        cvm_code = int(data.get('cvm_code'))
        start_year = int(data.get('start_year'))
        end_year = int(data.get('end_year'))
        years_to_analyze = list(range(start_year, end_year + 1))
        db_manager = get_db_manager()
        with db_manager.get_engine().connect() as connection:
            query = text("""
                SELECT "CNPJ_CIA", "CD_CVM", "DENOM_CIA", "DT_REFER", "CD_CONTA", "DS_CONTA", "VL_CONTA", "ST_CONTA"
                FROM public.financial_data
                WHERE "CD_CVM" = :cvm_code AND EXTRACT(YEAR FROM "DT_REFER") BETWEEN :start_year AND :end_year
                ORDER BY "DT_REFER" ASC, "ST_CONTA" DESC, "CD_CONTA" ASC;
            """)
            df_company = pd.read_sql(query, connection, params={'cvm_code': cvm_code, 'start_year': start_year, 'end_year': end_year})
        if df_company.empty:
            return jsonify({"error": f"Nenhum dado financeiro encontrado para a empresa CVM {cvm_code} no período."}), 404
        fleuriet_results, fleuriet_error = run_multi_year_analysis(df_company, cvm_code, years_to_analyze)
        if fleuriet_error:
            return jsonify({"error": fleuriet_error}), 500
        return jsonify(clean_data_for_json(fleuriet_results))
    except (ValueError, TypeError) as e:
        return jsonify({"error": f"Parâmetros inválidos: {e}"}), 400
    except Exception as e:
        logger.error(f"Erro na análise Fleuriet via API: {e}", exc_info=True)
        return jsonify({"error": "Ocorreu um erro inesperado na análise."}), 500

@app.route('/api/financial/analyze/complete', methods=['POST'])
@cross_origin()
def run_complete_analysis_api():
    """Executa a análise de valuation completa para todas as empresas do Ibovespa."""
    try:
        logger.info("Iniciando análise completa de valuation via API")
        system = get_ibovespa_analysis_system()
        if not system:
            return jsonify({"error": "Sistema de análise não inicializado."}), 503
        data = request.get_json(silent=True) or {}
        num_companies = data.get('num_companies')
        start_time = datetime.now()
        report = system.run_complete_analysis(num_companies=num_companies)
        end_time = datetime.now()
        report['execution_time_seconds'] = (end_time - start_time).total_seconds()
        return jsonify(report)
    except Exception as e:
        logger.error(f"Erro ao executar análise completa: {e}", exc_info=True)
        return jsonify({'error': f"Erro interno: {e}"}), 500

@app.route('/api/financial/analyze/company/<ticker>', methods=['GET'])
@cross_origin()
def get_company_analysis_api(ticker):
    """Obtém os dados de análise de valuation para uma empresa específica."""
    try:
        logger.info(f"Buscando análise para o ticker: {ticker}")
        system = get_ibovespa_analysis_system()
        if not system:
            return jsonify({"error": "Sistema de análise não inicializado."}), 503
        analysis_result = system.get_company_analysis(ticker.upper())
        if not analysis_result or analysis_result.get('error'):
             return jsonify(analysis_result or {"error": "Análise não encontrada para o ticker."}), 404
        return jsonify(analysis_result)
    except Exception as e:
        logger.error(f"Erro ao obter dados para {ticker}: {e}", exc_info=True)
        return jsonify({'error': f"Erro interno ao buscar dados para {ticker}."}), 500

@app.route('/api/financial/companies', methods=['GET'])
@cross_origin()
def get_ibovespa_companies_list_api():
    """Obtém a lista de empresas do Ibovespa disponíveis para valuation."""
    try:
        system = get_ibovespa_analysis_system()
        if not system:
            return jsonify({"error": "Sistema de análise não inicializado."}), 503
        companies = system.get_ibovespa_company_list()
        return jsonify({'companies': companies, 'total': len(companies)})
    except Exception as e:
        logger.error(f"Erro ao obter lista de empresas do Ibovespa: {e}", exc_info=True)
        return jsonify({'error': "Erro interno ao obter lista de empresas."}), 500

@app.route('/api/valuation/run_worker', methods=['POST'])
@cross_origin()
def run_valuation_worker_api():
    """Aciona o worker para recalcular todos os dados de valuation."""
    try:
        logger.info("Requisição para executar worker de valuation recebida.")
        system = get_ibovespa_analysis_system()
        if not system:
            return jsonify({"success": False, "error": "Sistema de análise não inicializado."}), 503
        system.run_complete_analysis(num_companies=None, force_recollect=True)
        logger.info("Worker de valuation executado com sucesso.")
        return jsonify({"success": True, "message": "Worker de valuation concluído. Os dados foram atualizados."})
    except Exception as e:
        logger.error(f"Erro ao acionar worker de valuation: {e}", exc_info=True)
        return jsonify({"success": False, "error": f"Falha ao executar worker: {e}"}), 500

# --- Tratamento de Erros Globais ---
@app.errorhandler(404)
def page_not_found_handler(e):
    """Garante que qualquer rota não-API seja redirecionada para o React."""
    if not request.path.startswith('/api/'):
        return serve_react_app(path='')
    return jsonify(error=f"Endpoint da API não encontrado: {request.path}"), 404

@app.errorhandler(500)
def internal_server_error_handler(e):
    original_exception = getattr(e, "original_exception", e)
    logger.error(f"Erro 500: {original_exception}", exc_info=True)
    return jsonify({"error": "Ocorreu um erro interno no servidor."}), 500

# --- Ponto de Entrada da Aplicação ---
if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8080))
    # O debug deve ser sempre False em produção.
    app.run(host='0.0.0.0', port=port, debug=False)
