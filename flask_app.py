import pandas as pd
import json
from flask import Flask, render_template, request, json
from core.analysis import run_multi_year_analysis
import os
import numpy as np
from sqlalchemy import create_engine, text
import logging
from urllib.parse import quote_plus
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

# Configuração de Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuração do Banco de Dados via variáveis de ambiente
DB_CONFIG = {
    'username': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'hostname': os.environ.get('DB_HOST'),
    'database': os.environ.get('DB_NAME'),
    'table_name': 'financial_data'
}

def get_db_connection_string():
    """Monta a string de conexão segura para o MySQL."""
    return (
        f"mysql+pymysql://{DB_CONFIG['username']}:{quote_plus(DB_CONFIG['password'])}"
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

try:
    DB_ENGINE = create_db_engine()
    logger.info("Conexão com o banco de dados inicializada com sucesso")
except Exception as e:
    logger.error(f"Falha ao conectar ao banco de dados: {str(e)}")
    DB_ENGINE = None

# Carregamento dos tickers
def safe_load_tickers(file_path):
    """Carrega o arquivo de mapeamento de tickers com tratamento robusto."""
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        full_path = os.path.join(base_dir, file_path)
        
        if not os.path.exists(full_path):
            logger.warning(f"Arquivo de tickers não encontrado: {full_path}")
            return None
            
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

class CustomJSONProvider(DefaultJSONProvider):
    def default(self, obj):
        if isinstance(obj, np.integer): return int(obj)
        if isinstance(obj, np.floating): return float(obj)
        if isinstance(obj, np.ndarray): return obj.tolist()
        if isinstance(obj, pd.Timestamp): return obj.isoformat()
        return super().default(obj)

app.json_provider = CustomJSONProvider(app)
app.json = CustomJSONProvider(app)

@app.route('/', methods=['GET', 'POST'])
def index():
    """Renderiza a página inicial e processa a análise sob demanda."""
    if request.method == 'GET':
        return render_template('index.html', companies=COMPANIES_LIST)

    if request.method == 'POST':
        try:
            cvm_code = request.form.get('cvm_code')
            if not cvm_code or not cvm_code.isdigit():
                error = "Código CVM inválido ou não fornecido."
                logger.warning(error)
                return render_template('index.html', companies=COMPANIES_LIST, error=error)

            cvm_code = int(cvm_code)
            logger.info(f"Iniciando análise para empresa CVM: {cvm_code}")

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
                )
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

@app.after_request
def add_header(response):
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['Content-Security-Policy'] = "default-src 'self'"
    return response

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
