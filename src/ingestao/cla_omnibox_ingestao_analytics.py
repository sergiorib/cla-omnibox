import duckdb
from pathlib import Path
from datetime import datetime

def executar_ingestao_analytics() -> dict:
    """
    Lê arquivos JSONL do diretório RAW (Google Analytics) e os converte 
    individualmente para Parquet na camada Bronze.
    """
    inicio_geral = datetime.now()
    
    # 1. Configuração de Caminhos
    diretorio_atual = Path(__file__).resolve().parent
    diretorio_base = diretorio_atual.parents[1]
    
    # Definimos onde estão os brutos e onde será a saída
    diretorio_raw = diretorio_base / "dados" / "raw" / "google_analytics" 
    diretorio_bronze = diretorio_base / "dados" / "bronze" / "google_analytics" 
    
    # Cria a pasta de destino se não existir
    diretorio_bronze.mkdir(parents=True, exist_ok=True)
    diretorio_raw.mkdir(parents=True, exist_ok=True)    

    # 2. Localização dos arquivos JSONL
    # Buscamos todos os arquivos que terminam em .jsonl na pasta raw
    arquivos_jsonl = list(diretorio_raw.glob("*.jsonl"))
    
    relatorio_arquivos = []
    status_final = "sucesso"
    
    if not arquivos_jsonl:
        return {
            "modulo": "omnibox_cla_carga_navegacao",
            "status": "aviso",
            "mensagem": "Nenhum arquivo .jsonl encontrado na pasta de origem.",
            "detalhes": []
        }

    # Inicia conexão DuckDB para o processamento
    conn = duckdb.connect()

    try:
        for arquivo_path in arquivos_jsonl:
            inicio_arq = datetime.now()
            nome_base = arquivo_path.stem # Ex: 'ga_202401'
            caminho_parquet = diretorio_bronze / f"{nome_base}.parquet"
            
            try:
                # O DuckDB lê o JSONL e grava o Parquet em uma única operação atômica
                # 'format=auto' para JSONL geralmente funciona perfeitamente
                query = f"""
                    COPY (
                        SELECT * FROM read_json_auto('{str(arquivo_path)}')
                    ) TO '{str(caminho_parquet)}' (FORMAT 'parquet');
                """
                conn.execute(query)
                
                # Coleta contagem de registros para o log
                contagem = conn.execute(f"SELECT COUNT(*) FROM '{str(caminho_parquet)}'").fetchone()[0]
                
                relatorio_arquivos.append({
                    "arquivo": arquivo_path.name,
                    "status": "sucesso",
                    "linhas_processadas": contagem,
                    "tempo": round((datetime.now() - inicio_arq).total_seconds(), 2)
                })
                
            except Exception as e:
                status_final = "erro"
                relatorio_arquivos.append({
                    "arquivo": arquivo_path.name, 
                    "status": "erro", 
                    "erro": str(e)
                })

    finally:
        conn.close()

    fim_geral = datetime.now()

    return {
        "modulo": "omnibox_cla_carga_navegacao",
        "status": status_final,
        "arquivos_total": len(arquivos_jsonl),
        "detalhes": relatorio_arquivos,
        "duracao_total": round((fim_geral - inicio_geral).total_seconds(), 2)
    }

if __name__ == "__main__":
    import json
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Iniciando Processamento de Analytics (JSONL)...")
    
    resultado = executar_ingestao_analytics()
    print(json.dumps(resultado, indent=4, ensure_ascii=False))