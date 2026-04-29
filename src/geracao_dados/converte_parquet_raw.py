import os
import duckdb
from pathlib import Path
from dataclasses import dataclass
from typing import List

# ==============================================================================
# 1. ESTRUTURA E CONFIGURAÇÕES DE DIRETÓRIO
# ==============================================================================

DIRETORIO_ATUAL = Path(__file__).resolve()
DIRETORIO_BASE = DIRETORIO_ATUAL.parents[2] 

# Pasta onde os arquivos Parquet foram gerados pelo motor
BASE_DIR = DIRETORIO_BASE / "dados" / "gerado"

# Origens
DIR_DADOS = BASE_DIR 
DIR_GA_ORIGEM = DIR_DADOS / "google_analytics"

# Destinos (Camada Raw/Bronze)
DIR_DESTINO_JSONL =  DIRETORIO_BASE / "dados" / "raw" / "google_analytics"
DIR_DESTINO_JSON =   DIRETORIO_BASE / "dados" /  "raw" / "marketing_api"
DIR_DESTINO_SQLITE = DIRETORIO_BASE / "dados" / "raw" / "vendas_erp"

# Arquivo do Banco de Dados SQLite
DB_SQLITE_PATH = DIR_DESTINO_SQLITE / "omnibox_vendas_erp.db"

# Listas de Arquivos (Baseado no dicionário de dados do ERP e Marketing)
ARQUIVOS_ERP = ["category", "sub_category", "products", "customers", "orders", "order_items"]
ARQUIVOS_MKT = ["mkt_actions", "mkt_campaigns", "mkt_spend"]

@dataclass
class JobConversao:
    origem: Path
    destino: Path
    formato_alvo: str
    nome_tabela: str = "" # Usado apenas para o SQLite

# ==============================================================================
# 2. CONSTRUTOR DA FILA DE TAREFAS
# ==============================================================================
def montar_fila_de_conversoes() -> List[JobConversao]:
    fila = []
    
    # A. Mapear arquivos do ERP (Parquet -> SQLite)
    for tabela in ARQUIVOS_ERP:
        fila.append(JobConversao(
            origem=DIR_DADOS / f"{tabela}.parquet",
            destino=DB_SQLITE_PATH,
            formato_alvo="sqlite",
            nome_tabela=tabela
        ))
        
    # B. Mapear arquivos de Marketing (Parquet -> JSON padrão)
    for arq in ARQUIVOS_MKT:
        fila.append(JobConversao(
            origem=DIR_DADOS / f"{arq}.parquet",
            destino=DIR_DESTINO_JSON / f"{arq}.json",
            formato_alvo="json"
        ))
        
    # C. Mapear arquivos do Google Analytics Dinamicamente (Parquet -> JSONL)
    if DIR_GA_ORIGEM.exists():
        for arq_parquet in DIR_GA_ORIGEM.glob("*.parquet"):
            fila.append(JobConversao(
                origem=arq_parquet,
                destino=DIR_DESTINO_JSONL / f"{arq_parquet.stem}.jsonl",
                formato_alvo="jsonl"
            ))
    else:
        print(f"[AVISO] Pasta de origem do GA não encontrada: {DIR_GA_ORIGEM}")
        
    return fila

# ==============================================================================
# 3. MOTOR DE EXECUÇÃO E CONVERSÃO COM PRESERVAÇÃO DE TIPOS
# ==============================================================================
def executar_conversoes(fila_de_jobs: List[JobConversao]):
    # Garantir criação dos diretórios de destino
    DIR_DESTINO_JSONL.mkdir(parents=True, exist_ok=True)
    DIR_DESTINO_JSON.mkdir(parents=True, exist_ok=True)
    DIR_DESTINO_SQLITE.mkdir(parents=True, exist_ok=True)

    # Iniciar DuckDB com suporte a SQLite
    con = duckdb.connect()
    con.execute("INSTALL sqlite; LOAD sqlite;")
    
    sucessos = 0
    erros = 0
    
    print(f"--- Iniciando processamento de {len(fila_de_jobs)} arquivos ---")

    for job in fila_de_jobs:
        try:
            if not job.origem.exists():
                print(f"[IGNORADO] Arquivo origem não encontrado: {job.origem.name}")
                continue

            if job.formato_alvo == "jsonl":
                # Exportação para JSONL (Tipos complexos são convertidos para String/Number)
                query = f"COPY (SELECT * FROM '{job.origem}') TO '{job.destino}' (FORMAT JSON, ARRAY FALSE);"
                con.execute(query)

            elif job.formato_alvo == "json":
                # Exportação para JSON Array
                query = f"COPY (SELECT * FROM '{job.origem}') TO '{job.destino}' (FORMAT JSON, ARRAY TRUE);"
                con.execute(query)

            elif job.formato_alvo == "sqlite":
                # --- LÓGICA DE PRESERVAÇÃO DE SCHEMA (PARQUET -> SQLITE) ---
                
                # 1. Inspeciona o arquivo Parquet para obter nomes e tipos das colunas
                colunas_info = con.execute(f"DESCRIBE SELECT * FROM '{job.origem}'").fetchall()
                
                # 2. Constrói a definição com o TRADUTOR DE TIPOS para o SQLite
                lista_colunas = []
                for col in colunas_info:
                    nome_coluna = col[0]
                    tipo_original = col[1]
                    
                    # Intervenção manual para tipos que o SQLite não suporta bem
                    if tipo_original.startswith("DECIMAL"):
                        tipo_sqlite = "DOUBLE"
                    elif tipo_original == "TIMESTAMP":
                        tipo_sqlite = "DATETIME"
                    else:
                        tipo_sqlite = tipo_original
                        
                    lista_colunas.append(f'"{nome_coluna}" {tipo_sqlite}')
                
                ddl_colunas = ", ".join(lista_colunas)
                
                # 3. Execução no SQLite com Schema Explícito
                con.execute(f"ATTACH '{job.destino}' AS erp_db (TYPE sqlite);")
                con.execute(f"DROP TABLE IF EXISTS erp_db.{job.nome_tabela};")
                
                # Criamos a tabela primeiro com os tipos fortes
                con.execute(f"CREATE TABLE erp_db.{job.nome_tabela} ({ddl_colunas});")
                
                # Inserimos os dados na estrutura já preparada
                con.execute(f"INSERT INTO erp_db.{job.nome_tabela} SELECT * FROM '{job.origem}';")
                con.execute("DETACH erp_db;")

            print(f"[OK] {job.origem.name} convertido para {job.formato_alvo.upper()}")
            sucessos += 1
            
        except Exception as e:
            print(f"[ERRO] Falha em {job.origem.name}: {str(e)}")
            erros += 1

    # Resumo Final
    print("\n" + "="*40)
    print("RESUMO DA EXECUÇÃO")
    print("="*40)
    print(f"Arquivos processados com sucesso: {sucessos}")
    print(f"Falhas encontradas: {erros}")
    print("="*40)

if __name__ == "__main__":
    jobs = montar_fila_de_conversoes()
    if jobs:
        executar_conversoes(jobs)
    else:
        print("Nenhuma tarefa para executar.")