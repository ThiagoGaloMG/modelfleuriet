import pandas as pd
from flask import Flask, render_template, request, json
from core.analysis import run_multi_year_analysis
import os
import numpy as np
from sqlalchemy import create_engine, text
import logging
from urllib.parse import quote_plus

# Inicializa a aplicação Flask
app = Flask(__name__)

# --- CONFIGURAÇÃO DE LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('flask_app.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- CONFIGURAÇÃO SEGURA DO BANCO DE DADOS ---
DB_CONFIG = {
    'username': "modelfleuriet",
    'password': quote_plus("#SalveMaria1!"),  # Codifica caracteres especiais
    'hostname': "modelfleuriet.mysql.pythonanywhere-services.com",
    'database': "modelfleuriet$default",
    'table_name': 'financial_data'
}

def get_db_connection_string():
    """Monta a string de conexão segura para o MySQL."""
    return (
        f"mysql+pymysql://{DB_CONFIG['username']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['hostname']}/{DB_CONFIG['database']}"
        "?charset=utf8mb4&connect_timeout=5"
    )

def create_db_engine():
    """Cria engine SQLAlchemy com configurações seguras"""
    return create_engine(
        get_db_connection_string(),
        pool_pre_ping=True,
        pool_recycle=3600,
        pool_size=5,
        max_overflow=10
    )

# Engine global com tratamento de reconexão
try:
    DB_ENGINE = create_db_engine()
    logger.info("Conexão com o banco de dados inicializada com sucesso")
except Exception as e:
    logger.error(f"Falha ao conectar ao banco de dados: {str(e)}")
    DB_ENGINE = None

# --- CARREGAMENTO RÁPIDO DOS DADOS PEQUENOS (APENAS TICKERS) ---
def safe_load_tickers(file_path):
    """Carrega o arquivo de mapeamento de tickers com tratamento robusto."""
    try:
        if not os.path.exists(file_path):
            logger.warning(f"Arquivo de tickers não encontrado: {file_path}")
            return None
            
        # Tentar diferentes codificações com fallback
        encodings = ['utf-8', 'latin1', 'iso-8859-1']
        for encoding in encodings:
            try:
                df = pd.read_csv(file_path, sep=';', encoding=encoding)
                logger.info(f"Tickers carregados com sucesso usando codificação {encoding}")
                return df
            except UnicodeDecodeError:
                continue
        
        logger.error("Falha ao decodificar arquivo de tickers com todas as codificações tentadas")
        return None
        
    except Exception as e:
        logger.error(f"Erro ao carregar tickers: {str(e)}", exc_info=True)
        return None

DF_TICKERS = safe_load_tickers('data/mapeamento_tickers.csv')
COMPANIES_LIST = []

if DF_TICKERS is not None:
    try:
        DF_TICKERS.drop_duplicates(subset=['CD_CVM'], inplace=True)
        DF_TICKERS.sort_values('TICKER', inplace=True)
        COMPANIES_LIST = DF_TICKERS[['TICKER', 'CD_CVM', 'DENOM_CIA_F']].to_dict('records')
        logger.info(f"Carregadas {len(COMPANIES_LIST)} empresas para seleção")
    except Exception as e:
        logger.error(f"Erro ao processar lista de empresas: {str(e)}", exc_info=True)

ANALYSIS_YEARS = [2021, 2022, 2023]

# Encoder JSON para lidar com tipos de dados do NumPy
class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer): return int(obj)
        if isinstance(obj, np.floating): return float(obj)
        if isinstance(obj, np.ndarray): return obj.tolist()
        if isinstance(obj, pd.Timestamp): return obj.isoformat()
        return super().default(obj)

app.json_encoder = CustomJSONEncoder

@app.route('/', methods=['GET', 'POST'])
def index():
    """Renderiza a página inicial e processa a análise sob demanda."""
    if request.method == 'GET':
        return render_template('index.html', companies=COMPANIES_LIST)

    if request.method == 'POST':
        try:
            # Validação segura do input
            cvm_code = request.form.get('cvm_code')
            if not cvm_code or not cvm_code.isdigit():
                error = "Código CVM inválido ou não fornecido."
                logger.warning(error)
                return render_template('index.html', companies=COMPANIES_LIST, error=error)

            cvm_code = int(cvm_code)
            logger.info(f"Iniciando análise para empresa CVM: {cvm_code}")

            # --- CONSULTA SEGURA AO BANCO DE DADOS ---
            try:
                query = text("SELECT * FROM financial_data WHERE CD_CVM = :cvm_code")
                df_company = pd.read_sql(query, DB_ENGINE, params={'cvm_code': cvm_code})
                
                if df_company.empty:
                    error = f"Nenhum dado encontrado para a empresa com CVM {cvm_code}."
                    logger.warning(error)
                    return render_template('index.html', companies=COMPANIES_LIST, error=error)
                    
                logger.info(f"Dados carregados: {len(df_company)} registros")
                
            except Exception as db_error:
                error = "Erro ao acessar o banco de dados."
                logger.error(f"{error} Detalhes: {str(db_error)}", exc_info=True)
                return render_template('index.html', companies=COMPANIES_LIST, error=error)

            # Executa a análise
            try:
                results, analysis_error = run_multi_year_analysis(df_company, cvm_code, ANALYSIS_YEARS)
                
                if analysis_error:
                    logger.error(f"Erro na análise: {analysis_error}")
                    return render_template('index.html', companies=COMPANIES_LIST, error=analysis_error)
                
                logger.info("Análise concluída com sucesso")
                return render_template(
                    'index.html', 
                    companies=COMPANIES_LIST, 
                    results=results, 
                    chart_data_json=json.dumps(results.get('chart_data', {}))
                    
            except Exception as analysis_exception:
                error = "Erro durante a análise dos dados."
                logger.error(f"{error} Detalhes: {str(analysis_exception)}", exc_info=True)
                return render_template('index.html', companies=COMPANIES_LIST, error=error)

        except Exception as e:
            error = "Ocorreu um erro inesperado."
            logger.error(f"{error} Detalhes: {str(e)}", exc_info=True)
            return render_template('index.html', companies=COMPANIES_LIST, error=error)

@app.errorhandler(404)
def page_not_found(e):
    logger.warning(f"Página não encontrada: {request.url}")
    return render_template('404.html'), 404

@app.errorhandler(500)
def internal_server_error(e):
    logger.error(f"Erro interno no servidor: {str(e)}", exc_info=True)
    return render_template('500.html'), 500

if __name__ == '__main__':
    # Configurações seguras para produção
    app.run(
        host='0.0.0.0',
        port=5000,
        debug=False,  # Desativado em produção
        threaded=True
    )