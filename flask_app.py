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

load_dotenv()
app = Flask(__name__)

# Configuração de logging aprimorada
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('app.log')
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

def log_memory_usage():
    """Registra o uso atual de memória"""
    mem = psutil.virtual_memory()
    logger.info(
        f"Memória: {mem.used/(1024*1024):.2f}MB / {mem.total/(1024*1024):.2f}MB "
        f"({mem.percent}%)"
    )

def create_db_engine():
    """Cria a engine de conexão com o banco de dados com tratamento de erros"""
    database_url = os.environ.get('DATABASE_URL')
    if not database_url:
        logger.error("DATABASE_URL não definida.")
        raise ValueError("DATABASE_URL não definida.")
    
    # Corrige a string de conexão para o novo banco
    database_url = database_url.strip()
    if "postgresql://" in database_url and "postgresql+psycopg2://" not in database_url:
        database_url = database_url.replace("postgresql://", "postgresql+psycopg2://")
    
    # Remove espaços e parâmetros extras
    database_url = database_url.split(" ")[0].split("?")[0]
    
    try:
        engine = create_engine(
            database_url,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
            pool_recycle=3600,
            connect_args={
                "connect_timeout": 10,
                "keepalives": 1,
                "keepalives_idle": 30,
                "keepalives_interval": 10,
                "keepalives_count": 5
            }
        )
        # Testa a conexão
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("✅ Conexão com o banco de dados estabelecida.")
        return engine
    except Exception as e:
        logger.error(f"❌ Falha ao criar a engine do banco de dados: {e}", exc_info=True)
        return None

def load_ticker_mapping(file_path='data/mapeamento_tickers.csv'):
    """Carrega o mapeamento de tickers com tratamento de erros"""
    logger.info(f"Carregando mapeamento de tickers de {file_path}...")
    try:
        df_tickers = pd.read_csv(
            file_path, 
            sep=',',
            dtype={'CD_CVM': str}
        )
        # Padroniza nomes de colunas
        df_tickers.columns = [col.strip().upper() for col in df_tickers.columns]
        
        # Converte e valida CD_CVM
        df_tickers['CD_CVM'] = pd.to_numeric(df_tickers['CD_CVM'], errors='coerce')
        df_tickers = df_tickers.dropna(subset=['CD_CVM'])
        df_tickers['CD_CVM'] = df_tickers['CD_CVM'].astype(int)
        
        # Remove duplicatas
        df_tickers = df_tickers.drop_duplicates(subset=['CD_CVM'], keep='first')
        
        logger.info(f"✅ {len(df_tickers)} mapeamentos carregados.")
        return df_tickers[['CD_CVM', 'TICKER']]
    except Exception as e:
        logger.error(f"❌ Erro ao carregar mapeamento: {e}", exc_info=True)
        return pd.DataFrame()

def get_companies_list(engine, ticker_mapping_df):
    """Obtém lista de empresas com tratamento de erros"""
    if engine is None: 
        return []
    
    logger.info("Buscando lista de empresas do banco...")
    try:
        with engine.connect() as connection:
            query = text('SELECT DISTINCT "DENOM_CIA", "CD_CVM" FROM financial_data ORDER BY "DENOM_CIA"')
            df_companies_db = pd.read_sql(query, connection)
        
        # Renomeia colunas e faz merge com tickers
        df_companies_db = df_companies_db.rename(columns={'DENOM_CIA': 'NOME_EMPRESA'})
        final_df = pd.merge(
            df_companies_db, 
            ticker_mapping_df, 
            on='CD_CVM', 
            how='left'
        )
        
        # Corrigido: Substitui fillna inplace por assign
        final_df = final_df.assign(TICKER=final_df['TICKER'].fillna('S/TICKER'))
        
        logger.info(f"✅ {len(final_df)} empresas encontradas e mapeadas.")
        return final_df.to_dict(orient='records')
    except Exception as e:
        logger.error(f"❌ Erro ao buscar lista de empresas: {e}", exc_info=True)
        return []

def ensure_valuation_table_exists(engine):
    """Garante que a tabela valuation_results existe com a estrutura correta"""
    if engine is None:
        logger.error("Engine do banco de dados não disponível")
        return False
    
    try:
        inspector = inspect(engine)
        
        # Verifica se a tabela existe e tem a estrutura correta
        if inspector.has_table('valuation_results'):
            logger.info("Tabela 'valuation_results' existe. Verificando estrutura...")
            
            # Lista de colunas obrigatórias com seus tipos esperados
            required_columns = {
                'ticker': 'varchar',
                'nome': 'varchar',
                'upside': 'numeric',
                'roic': 'numeric',
                'wacc': 'numeric',
                'spread': 'numeric',
                'preco_atual': 'numeric',
                'preco_justo': 'numeric',
                'market_cap': 'int8',  # bigint
                'eva': 'numeric',
                'capital_empregado': 'numeric',
                'nopat': 'numeric',
                'data_atualizacao': 'timestamp'  # Adicionado campo de timestamp
            }
            
            # Obtém os metadados das colunas existentes
            existing_columns = {}
            for column in inspector.get_columns('valuation_results'):
                existing_columns[column['name'].lower()] = column['type'].__visit_name__.lower()
            
            # Verifica se todas as colunas necessárias existem com os tipos corretos
            missing_or_invalid = False
            for col, expected_type in required_columns.items():
                if col not in existing_columns:
                    logger.warning(f"Coluna faltando: {col} (esperado: {expected_type})")
                    missing_or_invalid = True
                elif existing_columns[col] != expected_type:
                    logger.warning(f"Tipo incorreto para {col}: {existing_columns[col]} (esperado: {expected_type})")
                    missing_or_invalid = True
            
            if not missing_or_invalid:
                logger.info("✅ Estrutura da tabela está correta")
                return True
            
            logger.warning("Estrutura da tabela incompatível. Recriando...")
            with engine.begin() as conn:
                conn.execute(text("DROP TABLE IF EXISTS valuation_results"))
        
        # Cria a nova tabela com estrutura atualizada
        logger.info("Criando tabela 'valuation_results'...")
        create_query = text("""
        CREATE TABLE valuation_results (
            "Ticker" VARCHAR(20) PRIMARY KEY,
            "Nome" VARCHAR(255),
            "Upside" NUMERIC, 
            "ROIC" NUMERIC, 
            "WACC" NUMERIC, 
            "Spread" NUMERIC,
            "Preco_Atual" NUMERIC, 
            "Preco_Justo" NUMERIC,
            "Market_Cap" BIGINT, 
            "EVA" NUMERIC,
            "Capital_Empregado" NUMERIC, 
            "NOPAT" NUMERIC,
            "Beta" NUMERIC,  # Novo campo adicionado
            "Data_Calculo" DATE,  # Renomeado para padrão consistente
            "Data_Atualizacao" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        
        with engine.begin() as conn:  # Usando begin() para transação automática
            conn.execute(create_query)
        
        logger.info("✅ Tabela 'valuation_results' criada com sucesso.")
        return True
        
    except Exception as e:
        logger.error(f"❌ Erro ao verificar/criar tabela valuation_results: {e}", exc_info=True)
        return False

def run_valuation_worker_if_needed(engine):
    """Executa o worker de valuation se a tabela estiver vazia"""
    if engine is None:
        return
    
    try:
        with engine.connect() as connection:
            result = connection.execute(text("SELECT COUNT(*) FROM valuation_results")).fetchone()
            count = result[0] if result else 0
            
        if count == 0:
            logger.info("Tabela valuation_results está vazia. Executando worker...")
            from core.valuation_analysis import run_full_valuation_analysis
            
            # Carrega dados necessários com otimização de memória
            df_tickers = load_ticker_mapping()
            with engine.connect() as connection:
                # Carrega apenas colunas necessárias
                cols = ["CD_CVM", "CD_CONTA", "VL_CONTA", "DT_REFER"]
                df_full_data = pd.read_sql(
                    f'SELECT {",".join(cols)} FROM financial_data',
                    connection
                )
            
            if not df_full_data.empty and not df_tickers.empty:
                valuation_results = run_full_valuation_analysis(df_full_data, df_tickers)
                
                if valuation_results:
                    df_results = pd.DataFrame(valuation_results)
                    df_results.to_sql(
                        'valuation_results',
                        engine,
                        if_exists='append',  # Alterado para append
                        index=False,
                        chunksize=100,
                        method='multi'
                    )
                    logger.info(f"✅ Worker executado com sucesso. {len(df_results)} resultados salvos.")
                else:
                    logger.warning("Worker não gerou resultados.")
            else:
                logger.warning("Dados insuficientes para executar o worker.")
                
    except Exception as e:
        logger.error(f"❌ Erro ao executar worker de valuation: {e}", exc_info=True)

# Inicialização segura
try:
    db_engine = create_db_engine()
    df_tickers = load_ticker_mapping()
    companies_list = get_companies_list(db_engine, df_tickers)
    
    if ensure_valuation_table_exists(db_engine):
        run_valuation_worker_if_needed(db_engine)
    else:
        logger.error("Falha ao garantir tabela de valuation. Algumas funcionalidades podem não estar disponíveis.")
except Exception as e:
    logger.critical(f"❌ Falha na inicialização: {e}", exc_info=True)
    companies_list = []

@app.route('/', methods=['GET', 'POST'])
def index():
    try:
        if request.method == 'GET':
            return render_template('index.html', companies=companies_list)

        # Validação dos parâmetros
        try:
            cvm_code = int(request.form.get('cvm_code'))
            start_year = int(request.form.get('start_year'))
            end_year = int(request.form.get('end_year'))
            if start_year > end_year:
                raise ValueError("Ano inicial maior que ano final")
        except (ValueError, TypeError) as e:
            return render_template(
                'index.html', 
                companies=companies_list,
                error="Parâmetros inválidos. Verifique os anos selecionados."
            ), 400

        years_to_analyze = list(range(start_year, end_year + 1))

        # Busca dados da empresa
        try:
            with db_engine.connect() as connection:
                query = text('SELECT * FROM financial_data WHERE "CD_CVM" = :cvm_code')
                df_company = pd.read_sql(query, connection, params={'cvm_code': cvm_code})
        except Exception as e:
            logger.error(f"Erro ao buscar dados da empresa: {e}")
            return render_template(
                'index.html', 
                companies=companies_list,
                error=f"Erro ao acessar dados da empresa CVM {cvm_code}"
            ), 500

        if df_company.empty:
            return render_template(
                'index.html', 
                companies=companies_list, 
                error=f"Nenhum dado encontrado para a empresa CVM {cvm_code}."
            )

        logger.info(f"Iniciando análise Fleuriet para CVM {cvm_code}")
        fleuriet_results, fleuriet_error = run_multi_year_analysis(df_company, cvm_code, years_to_analyze)
        
        if fleuriet_error:
            return render_template(
                'index.html', 
                companies=companies_list, 
                error=fleuriet_error
            )

        return render_template(
            'index.html', 
            companies=companies_list, 
            fleuriet_results=fleuriet_results,
            selected_company=str(cvm_code),
            start_year=start_year,
            end_year=end_year
        )
    except Exception as e:
        logger.error(f"Erro crítico na rota principal: {e}", exc_info=True)
        return render_template('500.html', error="Ocorreu um erro inesperado."), 500

@app.route('/get_valuation_data', methods=['GET'])
def get_valuation_data():
    """Endpoint para obter dados de valuation"""
    logger.info("Buscando dados de valuation pré-calculados...")
    try:
        if db_engine is None:
            return jsonify({"error": "Banco de dados não disponível"}), 500
            
        inspector = inspect(db_engine)
        if not inspector.has_table('valuation_results'):
            return jsonify({
                "error": "A análise de valuation ainda não foi executada.",
                "solution": "Execute manualmente em /run_valuation_worker"
            }), 404
        
        with db_engine.connect() as connection:
            # Limita a 1000 registros para evitar sobrecarga
            df_valuation = pd.read_sql(
                'SELECT * FROM valuation_results LIMIT 1000', 
                connection
            )
            
        if df_valuation.empty:
            return jsonify({
                "error": "A análise de valuation ainda não foi concluída.",
                "solution": "Execute manualmente em /run_valuation_worker"
            }), 404
            
        return jsonify(df_valuation.to_dict(orient='records'))
        
    except Exception as e:
        logger.error(f"Erro ao buscar dados de valuation: {e}", exc_info=True)
        return jsonify({
            "error": "Falha ao acessar os resultados da análise de valuation.",
            "details": str(e)
        }), 500

@app.route('/run_valuation_worker', methods=['POST'])
def run_valuation_worker_manual():
    """Endpoint para executar o worker de valuation manualmente"""
    try:
        logger.info("Executando worker de valuation manualmente...")
        
        if db_engine is None:
            return jsonify({
                "success": False,
                "error": "Banco de dados não disponível"
            }), 500
            
        run_valuation_worker_if_needed(db_engine)
        
        return jsonify({
            "success": True, 
            "message": "Worker de valuation executado com sucesso."
        })
    except Exception as e:
        logger.error(f"Erro ao executar worker manualmente: {e}", exc_info=True)
        return jsonify({
            "success": False, 
            "error": "Falha ao executar o worker de valuation.",
            "details": str(e)
        }), 500

@app.errorhandler(404)
def page_not_found(e):
    return render_template('404.html'), 404

@app.errorhandler(500)
def internal_server_error(e):
    logger.error(f"Erro interno do servidor: {e}", exc_info=True)
    return render_template('500.html'), 500

def shutdown_server():
    """Desliga o servidor Flask de forma segura"""
    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        raise RuntimeError('Não está rodando com o servidor Werkzeug')
    func()

@app.route('/shutdown', methods=['POST'])
def shutdown():
    """Endpoint para desligar o servidor (apenas para desenvolvimento)"""
    shutdown_server()
    return 'Servidor desligando...'

if __name__ == '__main__':
    try:
        port = int(os.environ.get("PORT", 5000))
        app.run(host='0.0.0.0', port=port)
    except Exception as e:
        logger.critical(f"Falha ao iniciar o servidor: {e}", exc_info=True)
    finally:
        if 'db_engine' in locals():
            db_engine.dispose()
