# modelfleuriet/core/data_collector.py

import pandas as pd
import logging
from typing import Dict, Optional, Any
from dataclasses import dataclass
from datetime import datetime, timedelta

# Importa o gerenciador de banco de dados e utilitários
from core.db_manager import SupabaseDB
from core.ibovespa_utils import validate_ticker # Para formatar tickers

logger = logging.getLogger(__name__)

@dataclass
class CompanyFinancialData:
    """
    Classe para armazenar dados financeiros de uma empresa, unificando fontes.
    Todos os dados virão do banco de dados (CVM e possíveis dados de mercado pré-processados).
    """
    ticker: str
    company_name: str
    cd_cvm: Optional[str] = None # Código CVM, se disponível
    sector: Optional[str] = None # Setor da empresa (pode vir do mapeamento ou do DB)

    # Dados de Mercado (via DB - devem ser populados pelo preprocess_to_db_light.py ou worker)
    market_cap: float = 0.0
    stock_price: float = 0.0
    shares_outstanding: float = 0.0
    
    # DRE - Demonstração do Resultado do Exercício (via dados da CVM do DB)
    revenue: float = 0.0          # Receita Líquida (3.01)
    ebit: float = 0.0             # Lucro Operacional (3.05)
    net_income: float = 0.0       # Lucro Líquido
    depreciation_amortization: float = 0.0 # Depreciação e Amortização (DFC)
    
    # Balanço Patrimonial (via dados da CVM do DB)
    total_assets: float = 0.0
    total_debt: float = 0.0       # Dívida Total
    equity: float = 0.0           # Patrimônio Líquido
    current_assets: float = 0.0   # Ativo Circulante
    current_liabilities: float = 0.0 # Passivo Circulante
    cash: float = 0.0             # Caixa e Equivalentes
    property_plant_equipment: float = 0.0 # Imobilizado
    
    # DFC - Fluxo de Caixa (CAPEX via dados da CVM do DB)
    capex: float = 0.0 # Capital Expenditure (Investimento em capital fixo)
    
    # Dados para NCG / Direcionadores de Valor Operacionais (via dados da CVM do DB)
    accounts_receivable: float = 0.0 # Contas a Receber (1.01.03)
    inventory: float = 0.0        # Estoques (1.01.04)
    accounts_payable: float = 0.0 # Fornecedores (2.01.02)

    timestamp_collected: Optional[str] = None # Quando os dados foram coletados/atualizados

class FinancialDataCollector:
    """
    Classe responsável pela coleta de dados financeiros, EXCLUSIVAMENTE do banco de dados.
    Não faz chamadas a APIs externas.
    """
    
    def __init__(self, db_manager: SupabaseDB, ticker_mapping_df: pd.DataFrame):
        self.db = db_manager
        self.ticker_mapping = ticker_mapping_df
        # Mapeamento de contas CVM para campos do dataclass CompanyFinancialData
        # Estas são as contas que esperamos encontrar na tabela financial_data
        self.cvm_account_map = {
            '3.01': 'revenue', # Receita Líquida
            '3.05': 'ebit',   # Lucro Operacional (EBIT)
            '3.11': 'net_income', # Lucro Líquido (ou 3.99.01.01 para DRE Consolidado)
            '1.01.03': 'accounts_receivable', # Contas a Receber
            '1.01.04': 'inventory', # Estoques
            '1.02.01': 'property_plant_equipment', # Imobilizado (Ativo Imobilizado)
            # CAPEX da CVM é mais complexo, pode vir do DFC ou ser estimado.
            # Se não estiver em VL_CONTA para um CD_CONTA específico, será 0.0
            '1.02.03': 'capex', # Ativo Não Circulante - Investimentos (pode ser proxy para CAPEX)
            '1.01': 'current_assets', # Ativo Circulante Total
            '1': 'total_assets', # Ativo Total
            '2.01': 'current_liabilities', # Passivo Circulante Total
            '2.01.02': 'accounts_payable', # Fornecedores
            # Dívida Total pode ser 2.03 ou soma de 2.03.01 e 2.03.02
            '2.03': 'total_debt', # Dívidas (Passivo Oneroso)
            '2.04': 'equity', # Patrimônio Líquido
            '1.01.01': 'cash' # Caixa e Equivalentes
        }
        # Nomes de contas do DFC para depreciação/amortização (buscadas por descrição)
        self.dfc_depreciation_accounts = ['Depreciação e Amortização', 'Depreciação, Amortização e Exaustão']

    def _get_cvm_data_from_db(self, cvm_code: int, latest_year: int) -> Optional[Dict[str, float]]:
        """
        Busca os dados financeiros da CVM (DRE, BP, DFC) do banco de dados para um CVM e ano.
        Retorna um dicionário mapeando o nome do campo para o valor.
        Prioriza dados consolidados (ST_CONTA = 'D').
        """
        engine = self.db.get_engine()
        if not engine:
            logger.error("Engine do banco de dados não disponível para coletar dados CVM.")
            return None

        try:
            # Busca os dados mais recentes para o CVM no ano e tipo de demonstração (DFP - D)
            # Prioriza DFP (D) sobre ITR (I) se ambos existirem para o mesmo ano/data
            query = text(f"""
                SELECT "CD_CONTA", "DS_CONTA", "VL_CONTA", "DT_REFER", "ST_CONTA"
                FROM public.financial_data
                WHERE "cd_cvm" = :cvm_code
                AND EXTRACT(YEAR FROM "dt_refer") = :year_ref
                ORDER BY "dt_refer" DESC, "st_conta" DESC, "cd_conta" ASC;
            """) # ST_CONTA DESC para priorizar 'D' (DFP) sobre 'I' (ITR)

            with engine.connect() as connection:
                df_cvm = pd.read_sql(query, connection, params={'cvm_code': cvm_code, 'year_ref': latest_year})
            
            if df_cvm.empty:
                logger.warning(f"Nenhum dado CVM encontrado para {cvm_code} no ano {latest_year}.")
                return None
            
            cvm_data_processed = {}
            # Para cada conta, pega o valor mais recente (assumindo que a ordenação já fez isso)
            for _, row in df_cvm.iterrows():
                account_code = row['CD_CONTA']
                account_desc = row['DS_CONTA']
                value = row['VL_CONTA']

                # Mapeamento por código da conta
                if account_code in self.cvm_account_map and self.cvm_account_map[account_code] not in cvm_data_processed:
                    cvm_data_processed[self.cvm_account_map[account_code]] = float(value) if pd.notna(value) else 0.0
                
                # Mapeamento para Depreciação/Amortização por descrição (do DFC)
                if any(dep_str in account_desc for dep_str in self.dfc_depreciation_accounts) and \
                   'depreciation_amortization' not in cvm_data_processed:
                    cvm_data_processed['depreciation_amortization'] = float(value) if pd.notna(value) else 0.0
            
            # Tentar derivar shares_outstanding, stock_price e market_cap se não vierem da CVM
            # A CVM não fornece preço da ação ou market cap diretamente em financial_data.
            # Estes campos precisarão ser populados pelo preprocess_to_db_light.py se forem críticos para o Valuation.
            # Por enquanto, serão 0.0 se não estiverem no DB.
            
            return cvm_data_processed

        except Exception as e:
            logger.error(f"Erro ao buscar dados CVM do DB para {cvm_code} no ano {latest_year}: {e}")
            return None

    def get_company_data(self, ticker: str, cvm_code: Optional[int] = None) -> Optional[CompanyFinancialData]:
        """
        Coleta os dados financeiros mais recentes de uma empresa EXCLUSIVAMENTE do banco de dados.
        Não faz chamadas a APIs externas.
        """
        logger.info(f"Coletando dados para {ticker} (CVM: {cvm_code}) do banco de dados...")
        
        # 1. Encontrar o último ano de dados CVM disponível para este CVM_CODE no DB
        engine = self.db.get_engine()
        if not engine:
            logger.error("Engine do banco de dados não disponível para buscar último ano CVM.")
            return None
        
        try:
            query_latest_year = text("""
                SELECT MAX(EXTRACT(YEAR FROM "DT_REFER"))
                FROM public.financial_data
                WHERE "cd_cvm" = :cvm_code;
            """)
            with engine.connect() as connection:
                latest_year_result = connection.execute(query_latest_year, {'cvm_code': cvm_code}).scalar_one_or_none()
            
            if not latest_year_result:
                logger.warning(f"Nenhum ano de dados CVM encontrado para {cvm_code} no DB.")
                return None
            
            latest_year = int(latest_year_result)
            logger.info(f"Último ano de dados CVM para {cvm_code}: {latest_year}")

        except Exception as e:
            logger.error(f"Erro ao determinar o último ano CVM para {cvm_code}: {e}")
            return None

        # 2. Obter dados financeiros da CVM do banco de dados para o último ano
        cvm_financial_data = self._get_cvm_data_from_db(cvm_code, latest_year)
        if not cvm_financial_data:
            logger.warning(f"Não foi possível obter dados financeiros detalhados da CVM para {cvm_code} do DB.")
            return None
        
        # 3. Obter nome da empresa e ticker do mapeamento global ou do DB
        company_info = self.ticker_mapping[self.ticker_mapping['CD_CVM'] == cvm_code].iloc[0] if not self.ticker_mapping.empty and cvm_code in self.ticker_mapping['CD_CVM'].values else {}
        company_name = company_info.get('NOME_EMPRESA', f"Empresa CVM {cvm_code}")
        ticker_from_map = company_info.get('TICKER', ticker) # Usa o ticker passado se não encontrar no mapa

        # Constrói o CompanyFinancialData com dados do DB
        data = CompanyFinancialData(
            ticker=validate_ticker(ticker_from_map),
            company_name=company_name,
            cd_cvm=str(cvm_code),
            sector=None, # Setor não vem da CVM financial_data, precisaria de outra fonte ou ser adicionado ao preprocessamento
            
            # Dados de Mercado (devem vir do DB se preprocessados, senão serão 0.0)
            market_cap=cvm_financial_data.get('market_cap', 0.0), # Se o preprocess_to_db_light.py adicionar isso
            stock_price=cvm_financial_data.get('stock_price', 0.0), # Se o preprocess_to_db_light.py adicionar isso
            shares_outstanding=cvm_financial_data.get('shares_outstanding', 0.0), # Se o preprocess_to_db_light.py adicionar isso
            
            # Dados da CVM (garantir que os valores existam no dicionário)
            revenue=cvm_financial_data.get('revenue', 0.0),
            ebit=cvm_financial_data.get('ebit', 0.0),
            net_income=cvm_financial_data.get('net_income', 0.0),
            depreciation_amortization=cvm_financial_data.get('depreciation_amortization', 0.0),
            capex=cvm_financial_data.get('capex', 0.0),
            total_assets=cvm_financial_data.get('total_assets', 0.0),
            total_debt=cvm_financial_data.get('total_debt', 0.0),
            equity=cvm_financial_data.get('equity', 0.0),
            current_assets=cvm_financial_data.get('current_assets', 0.0),
            current_liabilities=cvm_financial_data.get('current_liabilities', 0.0),
            cash=cvm_financial_data.get('cash', 0.0),
            accounts_receivable=cvm_financial_data.get('accounts_receivable', 0.0),
            inventory=cvm_financial_data.get('inventory', 0.0),
            accounts_payable=cvm_financial_data.get('accounts_payable', 0.0),
            property_plant_equipment=cvm_financial_data.get('property_plant_equipment', 0.0),
            
            timestamp_collected=datetime.now().isoformat() # Timestamp da coleta
        )
        
        # Validação básica (pode usar ValidationUtils do utils.py)
        # is_valid, errors = ValidationUtils.validate_financial_data(data.__dict__)
        # if not is_valid:
        #     logger.warning(f"Dados coletados para {ticker} do DB são inválidos: {errors}")
        #     return None

        return data

    def get_multiple_companies(self, tickers_cvm_map: Dict[str, int]) -> Dict[str, CompanyFinancialData]:
        """
        Coleta dados para uma lista de empresas, usando o mapeamento ticker -> CVM.
        Todos os dados vêm do DB.
        """
        companies_data = {}
        for ticker, cvm_code in tickers_cvm_map.items():
            data = self.get_company_data(ticker, cvm_code)
            if data:
                companies_data[ticker] = data
        return companies_data
