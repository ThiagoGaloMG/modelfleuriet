import pandas as pd
import json
from flask import Flask, render_template, request
from flask.json.provider import DefaultJSONProvider
from core.analysis import run_multi_year_analysis
import os
import numpy as np
from sqlalchemy import create_engine, text, exc
import logging
from dotenv import load_dotenv

# Carrega variáveis de ambiente de um arquivo .env, se existir (para desenvolvimento local)
load_dotenv()

app = Flask(__name__)

# --- Configuração de Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- Serializador JSON Customizado ---
# Para lidar com tipos de dados do numpy e pandas de forma segura
class CustomJSONProvider(DefaultJSONProvider):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        return super().default(obj)

app.json = CustomJSONProvider(app)


# --- Configuração do Banco de Dados (MODIFICADO E CORRIGIDO) ---
def create_db_engine():
    """
    Cria uma engine SQLAlchemy a partir da variável de ambiente DATABASE_URL.
    Esta é a forma recomendada de conectar em plataformas como a Render.
    """
    database_url = os.environ.get('DATABASE_URL')
    if not database_url:
        logger.error("A variável de ambiente DATABASE_URL não foi definida.")
        raise ValueError("A variável de ambiente DATABASE_URL não foi definida.")

    # A URL do Render vem como 'postgresql://...'. 
    # O SQLAlchemy precisa que o dialeto do driver seja especificado: 'postgresql+psycopg2://...'
    conn_str = database_url.replace("postgresql://", "postgresql+psycopg2://", 1)

    logger.info("Criando engine de banco de dados...")
    try:
        engine = create_engine(
            conn_str,
            pool_pre_ping=True, # Verifica a conexão antes de usar
            pool_recycle=300,   # Recicla conexões a cada 5 minutos
            pool_size=5,
            max_overflow=2,
            connect_args={'connect_timeout': 10}
        )
        # Testa a conexão na inicialização
        with engine.connect() as connection:
            logger.info("Conexão com o banco de dados estabelecida com sucesso.")
        return engine
    except exc.SQLAlchemyError as e:
        logger.error(f"Falha ao criar a engine do banco de dados: {e}", exc_info=True)
        return None


# --- Carregamento de Dados Iniciais ---
def get_companies_list(engine):
    """Busca a lista de empresas do banco de dados de forma segura."""
    if engine is None:
        logger.error("Engine do banco de dados não está disponível. Não é possível buscar a lista de empresas.")
        return []
        
    logger.info("Buscando lista de empresas do banco de dados...")
    try:
        with engine.connect() as connection:
            # Usar nomes de coluna entre aspas para garantir a correspondência no PostgreSQL
            query = text('SELECT DISTINCT "DENOM_CIA", "CD_CVM" FROM financial_data ORDER BY "DENOM_CIA";')
            df = pd.read_sql(query, connection)
            logger.info(f"{len(df)} empresas carregadas com sucesso.")
            return df.to_dict(orient='records')
    except exc.SQLAlchemyError as e:
        logger.error(f"Erro ao buscar lista de empresas no banco de dados: {e}", exc_info=True)
        return [] # Retorna lista vazia em caso de erro para não quebrar a aplicação
    except Exception as e:
        logger.error(f"Um erro inesperado ocorreu ao buscar a lista de empresas: {e}", exc_info=True)
        return []

# Inicializa a engine e carrega a lista de empresas na inicialização da aplicação
db_engine = create_db_engine()
companies_list = get_companies_list(db_engine)


# --- Rotas da Aplicação ---
@app.route('/', methods=['GET', 'POST'])
def index():
    try:
        if not companies_list:
            error_msg = "Não foi possível carregar a lista de empresas. Verifique a conexão com o banco de dados e se os dados foram populados."
            logger.warning(error_msg)
            return render_template('index.html', companies=[], error=error_msg)

        if request.method == 'GET':
            return render_template('index.html', companies=companies_list)

        # Lógica para o método POST
        cvm_code_str = request.form.get('cvm_code')
        start_year_str = request.form.get('start_year')
        end_year_str = request.form.get('end_year')

        if not all([cvm_code_str, start_year_str, end_year_str]):
            return render_template('index.html', companies=companies_list, error="Todos os campos são obrigatórios.")

        try:
            cvm_code = int(cvm_code_str)
            start_year = int(start_year_str)
            end_year = int(end_year_str)
            years_to_analyze = list(range(start_year, end_year + 1))
        except (ValueError, TypeError):
            return render_template('index.html', companies=companies_list, error="Valores inválidos para código CVM ou anos.")

        logger.info(f"Iniciando busca de dados para CVM {cvm_code}")
        
        # 1. Buscar os dados da empresa no banco
        df_company = pd.DataFrame()
        try:
            with db_engine.connect() as connection:
                query = text('SELECT * FROM financial_data WHERE "CD_CVM" = :cvm_code')
                df_company = pd.read_sql(query, connection, params={'cvm_code': cvm_code})
            
            if df_company.empty:
                error = f"Nenhum dado encontrado no banco para a empresa com CVM {cvm_code}."
                logger.warning(error)
                return render_template('index.html', companies=companies_list, error=error)
            
            logger.info(f"Dados carregados do banco: {len(df_company)} registros para CVM {cvm_code}")

        except exc.SQLAlchemyError as e:
            error = "Erro ao acessar o banco de dados durante a busca de dados da empresa."
            logger.error(f"{error} Detalhes: {e}", exc_info=True)
            return render_template('index.html', companies=companies_list, error=error)

        # 2. Chamar a função de análise com o DataFrame (LÓGICA CORRIGIDA)
        logger.info(f"Iniciando análise para CVM {cvm_code} para os anos {years_to_analyze}")
        analysis_results, error = run_multi_year_analysis(df_company, cvm_code, years_to_analyze)
        
        if error:
            logger.warning(f"Erro retornado pela função de análise para CVM {cvm_code}: {error}")
            return render_template('index.html', companies=companies_list, error=error)
        
        # Passa os resultados para o template
        return render_template(
            'index.html', 
            companies=companies_list, 
            results=analysis_results, 
            selected_company=cvm_code_str,
            chart_data_json=json.dumps(analysis_results.get('chart_data', {}))
        )

    except Exception as e:
        error = "Ocorreu um erro crítico na aplicação."
        logger.error(f"{error} Detalhes: {e}", exc_info=True)
        return render_template('500.html', error=error), 500


# --- Handlers de Erro ---
@app.errorhandler(404)
def page_not_found(e):
    logger.warning(f"Página não encontrada: {request.url}")
    return render_template('404.html'), 404

@app.errorhandler(500)
def internal_server_error(e):
    logger.error(f"Erro interno no servidor: {e}", exc_info=True)
    return render_template('500.html'), 500

# --- Headers de Segurança ---
@app.after_request
def add_header(response):
    response.headers['X-Content-Type-Options'] = 'nosniff'
    return response

if __name__ == '__main__':
    # A Render define a porta através da variável de ambiente PORT
    port = int(os.environ.get("PORT", 5000))
    # Para produção, debug=False é mais seguro e performático
    app.run(host='0.0.0.0', port=port, debug=False)
