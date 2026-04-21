import duckdb
from pathlib import Path
from datetime import datetime

def executar_ingestao_erp(db_nome: str = "omnibox_erp.db") -> dict:
    """
    Executa a ingestão de múltiplas tabelas do ERP para a camada Bronze.
    Aplica filtros de data e dependências relacionais.
    """
    inicio_geral = datetime.now()
    
    diretorio_atual = Path(__file__).resolve().parent    
    diretorio_base = diretorio_atual.parents[1]

    # Caminho de entrada (erp.db)
    caminho_db = diretorio_base / "datalake" / "raw" / "vendas_erp" / db_nome

    # Caminho de saída (Bronze) 
    diretorio_bronze = diretorio_base / "datalake" / "bronze" 
    diretorio_bronze.mkdir(parents=True, exist_ok=True)
    
    data_corte = "2024-01-01"
    
    config_tabelas = {
        "category": "SELECT * FROM sqlite_scan(?, 'category')",
        "sub_category": "SELECT * FROM sqlite_scan(?, 'sub_category')",
        "products": "SELECT * FROM sqlite_scan(?, 'products')",
        "customers": "SELECT * FROM sqlite_scan(?, 'customers')",
        "orders": f"SELECT * FROM sqlite_scan(?, 'orders') WHERE order_date >= '{data_corte}'",
        "order_items": f"""
            SELECT * FROM sqlite_scan(?, 'order_items') 
            WHERE order_id IN (
                SELECT order_id FROM sqlite_scan(?, 'orders') WHERE order_date >= '{data_corte}'
            )
        """
    }

    relatorio_tabelas = []
    status_final = "sucesso"

    try:
        # Inicia conexão única para todas as extrações
        conn = duckdb.connect()
        conn.execute("INSTALL sqlite; LOAD sqlite;")

        for tabela, query in config_tabelas.items():
            inicio_tab = datetime.now()
            caminho_parquet = diretorio_bronze / f"{tabela}.parquet"
            
            try:
                # O DuckDB permite passar parâmetros (?) para evitar problemas de string
                # Note que para order_items passamos o caminho_db duas vezes devido à subquery
                params = [str(caminho_db)]
                if "order_items" in tabela:
                    params.append(str(caminho_db))

                # Executa a cópia direta para Parquet
                conn.execute(f"COPY ({query}) TO '{caminho_parquet}' (FORMAT 'parquet')", params)
                
                # Coleta contagem de linhas para o log
                linhas = conn.execute(f"SELECT COUNT(*) FROM '{caminho_parquet}'").fetchone()[0]
                
                relatorio_tabelas.append({
                    "tabela": tabela,
                    "status": "sucesso",
                    "linhas": linhas,
                    "tempo": round((datetime.now() - inicio_tab).total_seconds(), 2)
                })
            except Exception as e:
                status_final = "erro"
                relatorio_tabelas.append({"tabela": tabela, "status": "erro", "erro": str(e)})

        conn.close()

    except Exception as e:
        status_final = "erro"
        msg_erro = str(e)
    else:
        msg_erro = None

    fim_geral = datetime.now()

    return {
        "modulo": "omnibox_cla_carga_vendas",
        "status": status_final,
        "detalhes": relatorio_tabelas,
        "duracao_total": round((fim_geral - inicio_geral).total_seconds(), 2),
        "erro_critico": msg_erro
    }

if __name__ == "__main__":
    import json
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Iniciando Carga Relacional...")
    
    resultado = executar_ingestao_erp()
    print(json.dumps(resultado, indent=4, ensure_ascii=False))