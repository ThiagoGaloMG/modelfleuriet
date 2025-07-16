#!/usr/bin/env python3
"""
Script de teste para verificar se o sistema está funcionando corretamente.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def test_imports():
    """Testa se todos os imports estão funcionando."""
    print("Testando imports...")
    
    try:
        import pandas as pd
        print("✓ pandas importado com sucesso")
    except ImportError as e:
        print(f"✗ Erro ao importar pandas: {e}")
        return False
    
    try:
        from core.analysis import run_multi_year_analysis
        print("✓ core.analysis importado com sucesso")
    except ImportError as e:
        print(f"✗ Erro ao importar core.analysis: {e}")
        return False
    
    try:
        from core.valuation_analysis import run_full_valuation_analysis
        print("✓ core.valuation_analysis importado com sucesso")
    except ImportError as e:
        print(f"✗ Erro ao importar core.valuation_analysis: {e}")
        return False
    
    try:
        from flask import Flask
        print("✓ Flask importado com sucesso")
    except ImportError as e:
        print(f"✗ Erro ao importar Flask: {e}")
        return False
    
    return True

def test_data_files():
    """Testa se os arquivos de dados estão presentes."""
    print("\nTestando arquivos de dados...")
    
    ticker_file = "data/mapeamento_tickers.csv"
    if os.path.exists(ticker_file):
        print(f"✓ {ticker_file} encontrado")
        
        # Testa se o arquivo pode ser lido
        try:
            import pandas as pd
            df = pd.read_csv(ticker_file)
            print(f"✓ {ticker_file} pode ser lido ({len(df)} linhas)")
        except Exception as e:
            print(f"✗ Erro ao ler {ticker_file}: {e}")
            return False
    else:
        print(f"✗ {ticker_file} não encontrado")
        return False
    
    return True

def test_flask_app():
    """Testa se a aplicação Flask pode ser inicializada."""
    print("\nTestando aplicação Flask...")
    
    try:
        # Simula variável de ambiente para teste
        os.environ['DATABASE_URL'] = 'postgresql://test:test@localhost:5432/test'
        
        from flask_app import app
        print("✓ Aplicação Flask criada com sucesso")
        
        # Testa se as rotas estão definidas
        with app.test_client() as client:
            # Não faz requisição real, apenas verifica se o app está configurado
            print("✓ Cliente de teste criado com sucesso")
        
        return True
    except Exception as e:
        print(f"✗ Erro ao criar aplicação Flask: {e}")
        return False

def main():
    """Função principal de teste."""
    print("=== TESTE DO SISTEMA FLEURIET & VALUATION ===\n")
    
    tests = [
        ("Imports", test_imports),
        ("Arquivos de dados", test_data_files),
        ("Aplicação Flask", test_flask_app)
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"✗ Erro inesperado no teste {test_name}: {e}")
            results.append((test_name, False))
    
    print("\n=== RESUMO DOS TESTES ===")
    all_passed = True
    for test_name, passed in results:
        status = "✓ PASSOU" if passed else "✗ FALHOU"
        print(f"{test_name}: {status}")
        if not passed:
            all_passed = False
    
    if all_passed:
        print("\n🎉 Todos os testes passaram! O sistema está pronto para uso.")
        return 0
    else:
        print("\n❌ Alguns testes falharam. Verifique os erros acima.")
        return 1

if __name__ == "__main__":
    sys.exit(main())

