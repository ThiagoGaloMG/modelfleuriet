#!/usr/bin/env python3
import pandas as pd
import json
from flask import Flask, render_template, request, jsonify
from flask.json.provider import DefaultJSONProvider
from core.analysis import run_multi_year_analysis
import os
import numpy as np
from sqlalchemy import create_engine, text, exc, inspect
import logging
from dotenv import load_dotenv
import gc
import psutil
from pathlib import Path
import time  # Adicionado para controle de rate limiting

load_dotenv()
app = Flask(__name__)

# Configuração de logging aprimorada
logging.basicConfig(
    level=logging.INFO,
    format=\'%(asctime)s - %(levelname)s - %(message)s\',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(\'app.log\')
    ]
)
logger = logging.getLogger(__name__)

class CustomJSONProvider(DefaultJSONProvider):
    def default(self, obj):
        if isinstance(obj, np.integer): return int(obj)
        if isinstance(obj, np.floating): return float(obj)
        if isinstance(obj, np.ndarray): return obj.tolist()
        if isinstance(obj, pd.Timestamp): return obj.isoformat()
        if pd.isna(obj): return None
        return super().default(obj)

app.json = CustomJSONProvider(app)

# Filtros Jinja2 para formatação
def format_currency(value):
    try:
        return f\"R$ {float(value):,.2f}\"
    except (ValueError, TypeError):
        return \"N/D\"

def format_percentage(value):
    try:
        return f\"{float(value)*100:.2f}%\"
    except (ValueError, TypeError):
        return \"N/D\"

app.jinja_env.filters[\'format_currency\'] = format_currency
app.jinja_env.filters[\'format_percentage\'] = format_percentage

def log_memory_usage():
    \"\"\"Registra o uso atual de memória\"\"\"
    mem = psutil.virtual_memory()
    logger.info(
        f\"Memória: {mem.used/(1024*1024):.2f}MB / {mem.total/(1024*1024):.2f}MB \"\
        f\"({mem.percent}%)\"
    )

def is_db_connected(engine):
    \"\"\"Verifica se a conexão com o banco está ativa\"\"\"
    if engine is None:
        return False
    try:
        with engine.connect() as conn:
            conn.execute(text(\"SELECT 1\"))
        return True
    except Exception as e:
        logger.error(f\"Erro ao verificar conexão com o banco: {e}\")
        return False

def create_db_engine():
    \"\"\"Cria a engine de conexão com o banco de dados com tratamento de erros\"\"\"
    database_url = os.environ.get(\'DATABASE_URL\')
    if not database_url:
        logger.error(\"DATABASE_URL não definida.\")
        raise ValueError(\"DATABASE_URL não definida.\")
    
    # Corrige a string de conexão para o formato adequado
    database_url = database_url.strip()
    if database_url.startswith(\"postgres://\"):
        database_url = database_url.replace(\"postgres://\", \"postgresql+psycopg2://\", 1)
    
    logger.info(f\"Conectando ao banco: {database_url.split(\'@\')[-1]}\")
    
    try:
        engine = create_engine(
            database_url,
            pool_pre_ping=True,
            pool_size=20,  # Aumentado para melhorar concorrência
            max_overflow=30,  # Aumentado para melhorar concorrência
            pool_recycle=3600,
            connect_args={
                \"connect_timeout\": 30,
                \"keepalives\": 1,
                \"keepalives_idle\": 30,
                \"keepalives_interval\": 10,
                \"keepalives_count\": 5
            }
        )
        # Testa a conexão
        with engine.connect() as conn:
            conn.execute(text(\"SELECT 1\"))
            logger.info(\"[OK] Conexão com o banco de dados estabelecida.\")
            
            # Verifica se a tabela financial_data existe
            inspector = inspect(engine)
            if not inspector.has_table(\'financial_data\'):
                logger.error(\"[ERRO] Tabela \'financial_data\' não encontrada no banco\")
                return None

            # Teste adicional: verifica se pode ler a tabela
            try:
                conn.execute(text(\"SELECT 1 FROM financial_data LIMIT 1\"))
                logger.info(\"[OK] Acesso à tabela financial_data verificado\")
            except Exception as e:
                logger.error(f\"[ERRO] Não foi possível ler a tabela: {str(e)}\")
                return None
            
        return engine
    except Exception as e:
        logger.error(f\"[ERRO] Falha ao conectar ao banco: {e}\", exc_info=True)
        return None

def load_ticker_mapping(file_path=None):
    \"\"\"Carrega o mapeamento de tickers com tratamento de erros\"\"\"
    if file_path is None:
        file_path = os.path.join(os.path.dirname(__file__), \'mapeamento_tickers.csv\')
    
    logger.info(f\"Carregando mapeamento de tickers de {file_path}...\")
    try:
        if not os.path.exists(file_path):
            logger.error(f\"Arquivo não encontrado: {file_path}\")
            return pd.DataFrame()
        
        # Lê o CSV e padroniza nomes de colunas
        df_tickers = pd.read_csv(
            file_path, 
            sep=\",\",
            dtype={\'cd_cvm\': str},
            encoding=\'utf-8\'
        )
        
        # Padroniza nomes de colunas para minúsculas e sem aspas
        df_tickers.columns = [col.strip().lower().replace(\'"\', \'\') for col in df_tickers.columns]
        
        # Processamento dos dados
        df_tickers[\'cd_cvm\'] = pd.to_numeric(df_tickers[\'cd_cvm\'], errors=\'coerce\')
        df_tickers = df_tickers.dropna(subset=[\'cd_cvm\'])
        df_tickers[\'cd_cvm\'] = df_tickers[\'cd_cvm\'].astype(int)
        df_tickers = df_tickers.drop_duplicates(subset=[\'cd_cvm\'], keep=\'first\')
        
        logger.info(f\"[OK] {len(df_tickers)} mapeamentos carregados.\")
        return df_tickers[[\'cd_cvm\', \'ticker\', \'nome_empresa\']]
    except Exception as e:
        logger.error(f\"[ERRO] Erro ao carregar mapeamento: {e}\", exc_info=True)
        return pd.DataFrame()

def get_companies_list(engine, ticker_mapping_df):
    \"\"\"Obtém lista de empresas com tratamento de erros\"\"\"
    if not is_db_connected(engine):
        logger.error(\"Conexão com o banco não está ativa\")
        return []
    
    logger.info(\"Buscando lista de empresas do banco...\")
    try:
        with engine.connect() as connection:
            # Verifica estrutura da tabela
            inspector = inspect(engine)
            if not inspector.has_table(\'financial_data\'):
                logger.error(\"Tabela financial_data não existe\")
                return []
                
            columns = [col[\'name\'] for col in inspector.get_columns(\'financial_data\')]
            
            # Verifica se as colunas necessárias existem (com nomes corretos)
            # Assumindo que as colunas no banco de dados estão em maiúsculas e com aspas duplas
            required_columns = {\'DENOM_CIA\', \'CD_CVM\'}  
            if not required_columns.issubset(columns):
                missing = required_columns - set(columns)
                logger.error(f\"Colunas obrigatórias faltando: {missing}\")
                return []
            
            # Executa a query com os nomes corretos das colunas
            query = text(\'SELECT DISTINCT \"DENOM_CIA\" AS nome_empresa, \"CD_CVM\" AS cd_cvm FROM financial_data ORDER BY \"DENOM_CIA\')
            df_companies_db = pd.read_sql(query, connection)
            
            # Padroniza as colunas do DataFrame resultante para minúsculas e sem aspas
            df_companies_db.columns = [col.lower().replace(\'"\', \'\') for col in df_companies_db.columns]

            # Merge com tickers: seleciona apenas cd_cvm e ticker do mapeamento
            if not ticker_mapping_df.empty:
                # Seleciona apenas as colunas necessárias
                ticker_subset = ticker_mapping_df[[\'cd_cvm\', \'ticker\']]
                
                final_df = pd.merge(
                    df_companies_db,
                    ticker_subset,
                    on=\'cd_cvm\',
                    how=\'left\'
                )
                # Preenche tickers faltantes com o nome da empresa do banco
                final_df[\'ticker\'] = final_df[\'ticker\'].fillna(final_df[\'nome_empresa\'])
            else:
                final_df = df_companies_db
                final_df[\'ticker\'] = final_df[\'nome_empresa\']
            
            # Retorna dados tratados
            cols = [\'cd_cvm\', \'ticker\', \'nome_empresa\']
            return (
                final_df[cols]
                .dropna(subset=[\'cd_cvm\'])
                .drop_duplicates(subset=[\'cd_cvm\'])
                .to_dict(orient=\'records\')
            )
    except Exception as e:
        logger.error(f\"[ERRO] Erro ao buscar lista de empresas: {e}\", exc_info=True)
        return []

def ensure_valuation_table_exists(engine):
    \"\"\"Garante que a tabela valuation_results existe com a estrutura correta\"\"\"
    if engine is None:
        logger.error(\"Engine do banco de dados não disponível\")
        return False
    
    try:
        inspector = inspect(engine)
        
        if inspector.has_table(\'valuation_results\'):
            logger.info(\"Tabela \'valuation_results\' existe.\")
            return True
            
        logger.info(\"Criando tabela \'valuation_results\'...\")
        create_query = text(\"\"\"\
            CREATE TABLE valuation_results (\
                ticker VARCHAR(20) PRIMARY KEY,\
                nome VARCHAR(255),\
                upside NUMERIC,\
                roic NUMERIC,\
                wacc NUMERIC,\
                spread NUMERIC,\
                preco_atual NUMERIC,\
                preco_justo NUMERIC,\
                market_cap BIGINT,\
                eva NUMERIC,\
                capital_empregado NUMERIC,\
                nopat NUMERIC,\
                beta NUMERIC,\
                data_calculo DATE,\
                data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP\
            )\
        \"\"\")
        
        with engine.begin() as conn:
            conn.execute(create_query)
        
        logger.info(\"[OK] Tabela \'valuation_results\' criada com sucesso.\")
        return True
        
    except Exception as e:
        logger.error(f\"[ERRO] Erro ao verificar/criar tabela valuation_results: {e}\", exc_info=True)
        return False

def run_valuation_worker_if_needed(engine):
    \"\"\"Executa o worker de valuation se a tabela estiver vazia\"\"\"
    if engine is None:
        return
    
    try:
        with engine.connect() as connection:
            result = connection.execute(text(\"SELECT COUNT(*) FROM valuation_results\")).fetchone()
            count = result[0] if result else 0
            
        if count == 0:
            logger.info(\"Tabela valuation_results está vazia. Executando worker...\")
            from core.valuation_analysis import run_full_valuation_analysis
            
            # Carrega dados necessários
            df_tickers = load_ticker_mapping()
            if df_tickers.empty:
                logger.error(\"Nenhum ticker carregado\")
                return
                
            with engine.connect() as connection:
                # Usar os nomes corretos das colunas conforme banco
                cols = [\'"CD_CVM"\', \'"CD_CONTA"\', \'"VL_CONTA"\', \'"DT_REFER"\', \'"DENOM_CIA"\']
                df_full_data = pd.read_sql(
                    text(f\'SELECT {\",\".join(cols)} FROM financial_data\'),
                    connection
                )
            
            # Padroniza as colunas do DataFrame resultante para minúsculas e sem aspas
            df_full_data.columns = [col.lower().replace(\'"\', \'\') for col in df_full_data.columns]

            if not df_full_data.empty and not df_tickers.empty:
                # Executar análise com dados financeiros e mapeamento de tickers
                valuation_results = run_full_valuation_analysis(df_full_data, df_tickers)
                
                if valuation_results:
                    df_results = pd.DataFrame(valuation_results)
                    df_results.to_sql(
                        \'valuation_results\',
                        engine,
                        if_exists=\'append\',
                        index=False,
                        chunksize=100,
                        method=\'multi\'
                    )
                    logger.info(f\"[OK] Worker executado. {len(df_results)} resultados salvos.\")
                else:
                    logger.warning(\"Worker não gerou resultados.\")
            else:
                logger.warning(\"Dados financeiros não encontrados ou mapeamento vazio.\")
                
    except Exception as e:
        logger.error(f\"[ERRO] Erro ao executar worker de valuation: {e}\", exc_info=True)

# Inicialização segura
try:
    db_engine = create_db_engine()
    df_tickers = load_ticker_mapping()
    
    if is_db_connected(db_engine):
        companies_list = get_companies_list(db_engine, df_tickers)
        logger.info(f\"Carregadas {len(companies_list)} empresas\")
        
        if not inspect(db_engine).has_table(\'financial_data\'):
            logger.error(\"[ERRO] TABELA FINANCIAL_DATA NÃO ENCONTRADA!\")
        
        # Verifica se deve executar o worker de valuation
        if ensure_valuation_table_exists(db_engine):
            # Executar worker apenas se necessário e em ambiente adequado
            if os.environ.get(\'RUN_VALUATION_WORKER\', \'true\').lower() == \'true\':
                run_valuation_worker_if_needed(db_engine)
            else:
                logger.info(\"Execução do worker de valuation desabilitada por configuração\")
        else:
            logger.error(\"Falha ao garantir tabela de valuation.\")
    else:
        logger.error(\"Falha na conexão com o banco de dados.\")
        companies_list = []
        
except Exception as e:
    logger.critical(f\"[ERRO] Falha na inicialização: {e}\", exc_info=True)
    companies_list = []

@app.route(\'/health\')
def health_check():
    \"\"\"Endpoint para verificação de saúde da aplicação\"\"\"
    db_status = \"active\" if db_engine else \"inactive\"
    tables = {
        \'financial_data\': False,
        \'valuation_results\': False
    }
    
    if db_engine:
        inspector = inspect(db_engine)
        tables[\'financial_data\'] = inspector.has_table(\'financial_data\')
        tables[\'valuation_results\'] = inspector.has_table(\'valuation_results\')
    
    return jsonify({
        \"status\": \"ok\",
        \"db_connection\": db_status,
        \"tables\": tables,
        \"companies_count\": len(companies_list)
    })

@app.route(\'/\', methods=[\'GET\', \'POST\'])
def index():
    try:
        if request.method == \'GET\':
            logger.info(f\"Renderizando página inicial com {len(companies_list)} empresas\")
            return render_template(\'index.html\', companies=companies_list)

        # Validação dos parâmetros
        try:
            cvm_code = int(request.form.get(\'cvm_code\'))
            start_year = int(request.form.get(\'start_year\'))
            end_year = int(request.form.get(\'end_year\'))
            if start_year > end_year:
                raise ValueError(\"Ano inicial maior que ano final\")
        except (ValueError, TypeError) as e:
            return render_template(
                \'index.html\', 
                companies=companies_list,
                error=\"Parâmetros inválidos. Verifique os anos selecionados.\"
            ), 400

        years_to_analyze = list(range(start_year, end_year + 1))

        # Busca dados da empresa (usando cd_cvm conforme estrutura)
        try:
            with db_engine.connect() as connection:
                # A query deve usar os nomes de coluna como estão no banco de dados
                query = text(\'SELECT * FROM financial_data WHERE \"CD_CVM\" = :cvm_code\')
                df_company = pd.read_sql(query, connection, params={\'cvm_code\': cvm_code})
            
            # Padroniza as colunas do DataFrame resultante para minúsculas e sem aspas
            df_company.columns = [col.lower().replace(\'"\', \'\') for col in df_company.columns]

        except Exception as e:
            logger.error(f\"Erro ao buscar dados da empresa: {e}\")
            return render_template(
                \'index.html\', 
                companies=companies_list,
                error=f\"Erro ao acessar dados da empresa CVM {cvm_code}\"
            ), 500

        if df_company.empty:
            return render_template(
                \'index.html\', 
                companies=companies_list, 
                error=f\"Nenhum dado encontrado para a empresa CVM {cvm_code}.\"
            )

        logger.info(f\"Iniciando análise Fleuriet para CVM {cvm_code}\")
        fleuriet_results, fleuriet_error = run_multi_year_analysis(df_company, cvm_code, years_to_analyze)
        
        if fleuriet_error:
            return render_template(
                \'index.html\', 
                companies=companies_list, 
                error=fleuriet_error
            )

        # Adicionar ticker aos resultados
        ticker = next((c[\'ticker\'] for c in companies_list if c[\'cd_cvm\'] == cvm_code), \'N/A\')
        fleuriet_results[\'ticker\'] = ticker

        return render_template(
            \'index.html\', 
            companies=companies_list, 
            fleuriet_results=fleuriet_results,
            selected_company=str(cvm_code),
            start_year=start_year,
            end_year=end_year
        )
    except Exception as e:
        logger.error(f\"Erro crítico na rota principal: {e}\", exc_info=True)
        return render_template(\'500.html\', error=\"Ocorreu um erro inesperado.\"), 500

@app.route(\'/get_valuation_data\', methods=[\'GET\'])
def get_valuation_data():
    \"\"\"Endpoint para obter dados de valuation\"\"\"
    logger.info(\"Buscando dados de valuation pré-calculados...\")
    try:
        if db_engine is None:
            return jsonify({\"error\": \"Banco de dados não disponível\"}), 500
            
        inspector = inspect(db_engine)
        if not inspector.has_table(\'valuation_results\'):
            return jsonify({
                \"error\": \"A análise de valuation ainda não foi executada.\",
                \"solution\": \"Execute manualmente em /run_valuation_worker\"
            }), 404
        
        with db_engine.connect() as connection:
            df_valuation = pd.read_sql(
                \'SELECT * FROM valuation_results LIMIT 1000\', 
                connection
            )
            
        # Padroniza as colunas do DataFrame resultante para minúsculas e sem aspas
        df_valuation.columns = [col.lower().replace(\'"\', \'\') for col in df_valuation.columns]

        if df_valuation.empty:
            return jsonify({
                \"error\": \"A análise de valuation ainda não foi concluída.\",
                \"solution\": \"Execute manualmente em /run_valuation_worker\"
            }), 404
            
        return jsonify(df_valuation.to_dict(orient=\'records\'))
        
    except Exception as e:
        logger.error(f\"Erro ao buscar dados de valuation: {e}\", exc_info=True)
        return jsonify({
            \"error\": \"Falha ao acessar os resultados da análise de valuation.\",
            \"details\": str(e)
        }), 500

@app.route(\'/run_valuation_worker\', methods=[\'POST\'])
def run_valuation_worker_manual():
    \"\"\"Endpoint para executar o worker de valuation manualmente\"\"\"
    try:
        logger.info(\"Executando worker de valuation manualmente...\")
        
        if db_engine is None:
            return jsonify({
                \"success\": False,
                \"error\": \"Banco de dados não disponível\"
            }), 500
            
        run_valuation_worker_if_needed(db_engine)
        
        return jsonify({
            \"success\": True, 
            \"message\": \"Worker de valuation executado com sucesso.\"
        })
    except Exception as e:
        logger.error(f\"Erro ao executar worker manualmente: {e}\", exc_info=True)
        return jsonify({
            \"success\": False, 
            \"error\": \"Falha ao executar o worker de valuation.\",
            \"details\": str(e)
        }), 500

@app.route(\'/reload_companies\', methods=[\'GET\'])
def reload_companies():
    \"\"\"Endpoint para recarregar a lista de empresas\"\"\"
    global companies_list
    try:
        db_engine = create_db_engine()
        df_tickers = load_ticker_mapping()
        companies_list = get_companies_list(db_engine, df_tickers) if db_engine else []
        return jsonify({
            \"status\": \"success\",
            \"count\": len(companies_list)
        })
    except Exception as e:
        return jsonify({
            \"status\": \"error\",
            \"message\": str(e)
        }), 500

@app.route(\'/debug/companies\')
def debug_companies():
    \"\"\"Endpoint para debug da lista de empresas\"\"\"
    return jsonify({
        \"count\": len(companies_list),
        \"companies\": companies_list[:10]  # Retorna apenas as primeiras 10 para exemplo
    })

@app.route(\'/debug/table_structure\')
def debug_table_structure():
    if db_engine is None:
        return jsonify({\"error\": \"Database not connected\"}), 500
    
    inspector = inspect(db_engine)
    
    if not inspector.has_table(\'financial_data\'):
        return jsonify({\"error\": \"Tabela financial_data não existe\"}), 404
    
    columns = inspector.get_columns(\'financial_data\')
    column_names = [col[\'name\'] for col in columns]
    
    return jsonify({
        \"table_exists\": True,
        \"columns\": column_names,
        \"column_details\": [{\"name\": col[\"name\"], \"type\": str(col[\"type\"])} for col in columns]
    })

@app.route(\'/debug/columns\')
def debug_columns():
    if db_engine is None:
        return jsonify({\"error\": \"Database not connected\"}), 500
    
    inspector = inspect(db_engine)
    columns = inspector.get_columns(\'financial_data\')
    
    # Adicione exemplos de dados para ajudar no debug
    sample_data = []
    with db_engine.connect() as conn:
        result = conn.execute(text(\'SELECT * FROM financial_data LIMIT 5\'))
        # Padroniza as colunas do DataFrame resultante para minúsculas e sem aspas
        keys = [key.lower().replace(\'"\', \'\') for key in result.keys()]
        for row in result:
            sample_data.append({key: str(value) for key, value in zip(keys, row)})
    
    return jsonify({
        \"columns\": [{\"name\": col[\"name\"], \"type\": str(col[\"type\"])} for col in columns],
        \"sample_data\": sample_data
    })

print(f\"Total de empresas carregadas: {len(companies_list)}\")
if len(companies_list) > 0:
    print(\"Exemplo de empresa:\", companies_list[0])

@app.errorhandler(404)
def page_not_found(e):
    return render_template(\'404.html\'), 404

@app.errorhandler(500)
def internal_server_error(e):
    logger.error(f\"Erro interno do servidor: {e}\", exc_info=True)
    return render_template(\'500.html\'), 500

if __name__ == \'__main__\':
    try:
        port = int(os.environ.get(\"PORT\", 5000))
        app.run(host=\'0.0.0.0\', port=port)
    except Exception as e:
        logger.critical(f\"Falha ao iniciar o servidor: {e}\", exc_info=True)
    finally:
        if \'db_engine\' in locals():
            db_engine.dispose()
