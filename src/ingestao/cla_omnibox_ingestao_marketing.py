import requests
import pandas as pd
import duckdb
from pathlib import Path
from datetime import datetime

def executar_ingestao_marketing(base_url: str = "http://127.0.0.1:8000", token: str = "omnibox_sec_token_2026") -> dict:
    """
    Consome a API REST de Marketing, lidando com autenticação e paginação,
    e salva os dados na camada Bronze em formato Parquet.
    """
    inicio_geral = datetime.now()
    
    # 1. Configuração de Caminhos

    diretorio_atual = Path(__file__).resolve().parent    
    diretorio_base = diretorio_atual.parents[1]

    # Caminho de saída (Bronze) 
    diretorio_bronze = diretorio_base / "datalake" / "bronze" 
    diretorio_bronze.mkdir(parents=True, exist_ok=True)
            
    endpoints = ["campaigns", "actions", "spend"]
    headers = {"Authorization": f"Bearer {token}"}
    
    relatorio_endpoints = []
    status_final = "sucesso"
    
    # Inicia o DuckDB para salvar os Parquets
    conn = duckdb.connect()

    for endpoint in endpoints:
        inicio_ep = datetime.now()
        url = f"{base_url}/{endpoint}"
        
        pagina_atual = 1
        tem_proxima = True
        dados_consolidados = []
        
        try:
            # 2. O Loop de Paginação
            while tem_proxima:
                # Requisição GET com parâmetros de URL (?page=X&size=50) e o Header de segurança
                resposta = requests.get(
                    url, 
                    headers=headers, 
                    params={"page": pagina_atual, "size": 50},
                    timeout=10 # Boa prática: nunca deixe uma requisição rodar infinitamente
                )
                
                # Se o token estiver errado ou a API cair, isso gera uma exceção (Fail-Fast)
                resposta.raise_for_status() 
                
                json_response = resposta.json()
                
                # Anexar os dados da página atual na nossa lista mestra
                dados_consolidados.extend(json_response["data"])
                
                # Atualizar o controle do loop baseado nos metadados da API
                tem_proxima = json_response["metadata"]["tem_proxima_pagina"]
                pagina_atual += 1
            
            # 3. Conversão e Gravação (Python -> Pandas -> DuckDB -> Parquet)
            if dados_consolidados:
                # Transforma a lista de dicionários em um DataFrame
                df_endpoint = pd.DataFrame(dados_consolidados)
                caminho_parquet = diretorio_bronze / f"mkt_{endpoint}.parquet"
                
                # DuckDB lê o DataFrame do Pandas nativamente!
                conn.execute(f"COPY (SELECT * FROM df_endpoint) TO '{caminho_parquet}' (FORMAT 'parquet')")
                linhas = len(df_endpoint)
            else:
                linhas = 0

            relatorio_endpoints.append({
                "endpoint": endpoint,
                "status": "sucesso",
                "linhas": linhas,
                "paginas_lidas": pagina_atual - 1,
                "tempo": round((datetime.now() - inicio_ep).total_seconds(), 2)
            })
            
        except Exception as e:
            status_final = "erro"
            relatorio_endpoints.append({"endpoint": endpoint, "status": "erro", "erro": str(e)})

    conn.close()
    fim_geral = datetime.now()

    return {
        "modulo": "omnibox_cla_carga_marketing",
        "status": status_final,
        "detalhes": relatorio_endpoints,
        "duracao_total": round((fim_geral - inicio_geral).total_seconds(), 2)
    }

if __name__ == "__main__":
    import json
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Iniciando Extração da API de Marketing...")
    resultado = executar_ingestao_marketing()
    print(json.dumps(resultado, indent=4, ensure_ascii=False))