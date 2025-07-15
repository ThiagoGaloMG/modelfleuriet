# modelfleuriet/flask_app.py

import pandas as pd
import json
from flask import Flask, render_template, request, jsonify, send_from_directory
from flask_cors import CORS
import os
import numpy as np
from sqlalchemy import create_engine, text, exc, inspect
import logging
from dotenv import load_dotenv
import sys
from datetime import datetime, timedelta
import traceback

# Carrega variáveis de ambiente do arquivo .env (para uso local)
load_dotenv()

# Configura logging para a aplicação Flask
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Ajusta o sys.path para que o Flask possa encontrar os módulos dentro de 'core'
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'core')))

# Importa módulos da nova estrutura 'core'
from db_manager import SupabaseDB # Gerenciador do Supabase
from ibovespa_analysis_system import IbovespaAnalysisSystem # Sistema de análise de Valuation
from ibovespa_utils import get_ibovespa_tickers # Lista de tickers do Ibovespa
from analysis import run_multi_year_analysis # Lógica do Modelo Fleuriet original
from utils import clean_data_for_json # Utilitários (PerformanceMonitor, clean_data_for_json)


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

app.json_encoder = CustomJSONEncoder

# --- Inicialização Global de Componentes ---
# Instância do SupabaseDB
db_supabase_manager = None
# Mapeamento de tickers (CVM -> Ticker -> Nome da Empresa)
df_tickers_mapping = None

def initialize_global_components():
    """Inicializa componentes globais como DB e mapeamento de tickers."""
    global db_supabase_manager, df_tickers_mapping
    
    if db_supabase_manager is None:
        db_supabase_manager = SupabaseDB()
        logger.info("SupabaseDB manager inicializado.")

    if df_tickers_mapping is None or df_tickers_mapping.empty:
        # Carrega o mapeamento de tickers para uso global
        file_path = os.path.join(os.path.dirname(__file__), 'data', 'mapeamento_tickers.csv')
        logger.info(f"Carregando mapeamento de tickers de {file_path}...")
        try:
            df_tickers_mapping = pd.read_csv(file_path, sep=',')
            df_tickers_mapping.columns = [col.strip().upper() for col in df_tickers_mapping.columns]
            df_tickers_mapping['CD_CVM'] = pd.to_numeric(df_tickers_mapping['CD_CVM'], errors='coerce').dropna().astype(int)
            df_tickers_mapping = df_tickers_mapping[['CD_CVM', 'TICKER', 'NOME_EMPRESA']].drop_duplicates(subset=['CD_CVM'])
            logger.info(f"{len(df_tickers_mapping)} mapeamentos carregados.")
        except Exception as e:
            logger.error(f"Erro ao carregar mapeamento: {e}", exc_info=True)
            df_tickers_mapping = pd.DataFrame() # Garante que seja um DataFrame vazio

# Inicializa componentes globais no startup da aplicação
with app.app_context():
    initialize_global_components()

# Instância do sistema de análise de Valuation (inicializada após o DB e mapeamento)
ibovespa_analysis_system_instance = None
def get_ibovespa_analysis_system():
    global ibovespa_analysis_system_instance
    if ibovesopa_analysis_system_instance is None and db_supabase_manager and not df_tickers_mapping.empty:
        logger.info("Inicializando IbovespaAnalysisSystem pela primeira vez...")
        ibovespa_analysis_system_instance = IbovespaAnalysisSystem(
            db_manager=db_supabase_manager,
            ticker_mapping_df=df_tickers_mapping
        )
        logger.info("IbovespaAnalysisSystem inicializado.")
    return ibovespa_analysis_system_instance

# --- Rotas Flask ---

# Rota de health check para o Render
@app.route('/health', methods=['GET'])
@app.route('/api/health', methods=['GET']) # Mantém compatibilidade
@cross_origin()
def health_check():
    try:
        system_ready = False
        try:
            system = get_ibovespa_analysis_system()
            system_ready = system is not None
        except Exception as e:
            logger.warning(f"IbovespaAnalysisSystem não pronto no health check: {e}")

        status = {
            'status': 'healthy',
            'message': 'API de Análise Financeira está funcionando',
            'timestamp': datetime.now().isoformat(),
            'system_initialized': system_ready,
            'db_connected': db_supabase_manager is not None and db_supabase_manager.get_engine() is not None,
            'ticker_mapping_loaded': not df_tickers_mapping.empty if df_tickers_mapping is not None else False
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
    # Adapta a lista de empresas (CD_CVM, Ticker, Nome) para o formato esperado pelo frontend.
    # O frontend espera: {company_id: CVM_CODE, ticker: TICKER, company_name: NOME_EMPRESA}
    companies_list = []
    engine = db_supabase_manager.get_engine()
    if not engine:
        logger.error("Conexão com o banco de dados não estabelecida para Fleuriet companies list.")
        return jsonify({"error": "Conexão com o banco de dados não estabelecida."}), 500
    
    try:
        # Busca da tabela 'companies' (criada pelo script Supabase)
        query = text('SELECT id as company_id, ticker, company_name FROM public.companies ORDER BY company_name;')
        with engine.connect() as connection:
            df_companies = pd.read_sql(query, connection)
        
        # O frontend espera cd_cvm. Se a public.companies tiver id, use-o como company_id
        # ou adicione cd_cvm à tabela public.companies. Por enquanto, usar id como company_id.
        companies_list = df_companies.to_dict(orient='records')
        
        return jsonify(companies_list), 200
    except Exception as e:
        logger.error(f"Erro ao obter lista de empresas Fleuriet: {e}", exc_info=True)
        return jsonify({"error": "Falha ao obter lista de empresas para Fleuriet."}), 500

@app.route('/api/fleuriet/analyze', methods=['POST'])
@cross_origin()
def analyze_fleuriet_api():
    """Executa a análise do Modelo Fleuriet para a empresa e anos selecionados."""
    try:
        data = request.get_json()
        cvm_code_str = data.get('cvm_code') # Este é o ID da public.companies
        start_year = data.get('start_year')
        end_year = data.get('end_year')

        if not cvm_code_str or not start_year or not end_year:
            return jsonify({"error": "Parâmetros cvm_code (company_id), start_year e end_year são obrigatórios."}), 400

        # Converte company_id para o CD_CVM real se necessário, ou adapta run_multi_year_analysis para usar company_id
        # Vamos buscar o CD_CVM usando o company_id
        engine = db_supabase_manager.get_engine()
        if not engine:
            return jsonify({"error": "Conexão com o banco de dados não estabelecida."}), 500

        # Busca o CD_CVM e o ticker a partir do company_id fornecido pelo frontend
        cvm_info_query = text("SELECT ticker, cd_cvm FROM public.companies WHERE id = :company_id;")
        with engine.connect() as connection:
            cvm_info_df = pd.read_sql(cvm_info_query, connection, params={'company_id': cvm_code_str}) # cvm_code_str é o UUID do company_id
        
        if cvm_info_df.empty:
             return jsonify({"error": f"Empresa com ID {cvm_code_str} não encontrada."}), 404
        
        real_cvm_code = cvm_info_df.iloc[0]['cd_cvm']
        company_ticker = cvm_info_df.iloc[0]['ticker']

        years_to_analyze = list(range(int(start_year), int(end_year) + 1))

        with engine.connect() as connection:
            # Assumindo que financial_data tem cd_cvm como INT ou BIGINT
            query = text('SELECT * FROM public.financial_data WHERE "cd_cvm" = :cvm_code ORDER BY "DT_REFER" ASC')
            df_company_data_cvm = pd.read_sql(query, connection, params={'cvm_code': real_cvm_code})

        if df_company_data_cvm.empty:
            return jsonify({"error": f"Nenhum dado financeiro da CVM encontrado para a empresa CVM {real_cvm_code} nos anos selecionados."}), 404
        
        fleuriet_results, fleuriet_error = run_multi_year_analysis(df_company_data_cvm, real_cvm_code, years_to_analyze)

        if fleuriet_error:
            return jsonify({"error": fleuriet_error}), 500

        # Limpa NaNs/Infs e converte para JSON
        cleaned_results = clean_data_for_json(fleuriet_results)

        return jsonify(cleaned_results), 200

    except Exception as e:
        logger.error(f"Erro na análise Fleuriet via API: {e}", exc_info=True)
        return jsonify({"error": f"Ocorreu um erro inesperado na análise Fleuriet: {str(e)}"}), 500

# --- Registro do Blueprint de Valuation (financial_bp) ---
# Este Blueprint já contém as rotas /api/financial/analyze/complete, /api/financial/analyze/company/<ticker>
from core.routes.financial import financial_bp # O Blueprint agora está em core/routes
app.register_blueprint(financial_bp, url_prefix='/api/financial')


# --- Rotas de Erro ---
@app.errorhandler(404)
def page_not_found(e):
    # Para SPAs, o 404 deve retornar o index.html para que o React lide com a rota
    return send_from_directory(app.static_folder, 'index.html')

@app.errorhandler(500)
def internal_server_error(e):
    logger.error(f"Erro 500: {e}", exc_info=True)
    return jsonify({"error": "Ocorreu um erro interno no servidor."}), 500

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8080))
    logger.info(f"Iniciando a aplicação Flask na porta {port}...")
    app.run(host='0.0.0.0', port=port, debug=os.environ.get('FLASK_DEBUG', 'False') == 'True')
