import duckdb
import json
from pathlib import Path
from datetime import datetime
import cla_omnibox_tratamento_funcoes as omni_val

def executar_tratamento_prata() -> dict:
    """
    Executa o tratamento da camada Prata respeitando a estrutura de pastas 
    da camada Bronze (Arquivos únicos ou pastas particionadas).
    """
    inicio_pipeline = datetime.now()
    
    diretorio_atual = Path(__file__).resolve()
    diretorio_base = diretorio_atual.parents[1]
    
    # Configuração de Caminhos
    path_config = diretorio_base / "config" / "cla_omnibox_schema_dados.json"
    dir_bronze = diretorio_base / "dados" / "bronze"
    dir_prata = diretorio_base / "dados" / "prata"
    dir_quarentena = diretorio_base / "dados" / "quarentena"
    
    dir_prata.mkdir(parents=True, exist_ok=True)
    dir_quarentena.mkdir(parents=True, exist_ok=True)

    with open(path_config, "r", encoding="utf-8") as f:
        config_mestre = json.load(f)

    con = duckdb.connect()
    con.execute("PRAGMA memory_limit='4GB'")

    relatorio_execucao = []
    status_geral_pipeline = "sucesso"

    print(f"[{inicio_pipeline.strftime('%H:%M:%S')}] Iniciando Tratamento e Validação da Camada Prata...\n")

    for entrada in config_mestre['esquema']:
        tabela_nome = entrada['tabela']
        campos = entrada['campos']
        tolerancia = entrada.get('tolerancia_erros_percentual', 0.0)
        
        # 1. Identifica a estrutura na Bronze
        busca_arquivo = list(dir_bronze.rglob(f"{tabela_nome}.parquet"))
        busca_pasta = [p for p in dir_bronze.rglob(tabela_nome) if p.is_dir()]
        
        arquivos_origem = []
        is_partitioned = False

        if busca_arquivo:
            arquivos_origem = [busca_arquivo[0]]
        elif busca_pasta:
            arquivos_origem = list(busca_pasta[0].glob("*.parquet"))
            is_partitioned = True

        if not arquivos_origem:
            print(f"Pulando '{tabela_nome}': Não encontrado na Bronze.")
            continue

        print(f"Processando: {tabela_nome} ({len(arquivos_origem)} arquivo(s) | Tolerância: {tolerancia}%)")
        inicio_tabela = datetime.now()

        # Acumuladores para o log da tabela
        total_lido, total_ok, total_erro = 0, 0, 0

        try:
            # Prepara pastas de destino se for particionado (Ex: Google Analytics)
            if is_partitioned:
                (dir_prata / tabela_nome).mkdir(parents=True, exist_ok=True)
                (dir_quarentena / tabela_nome).mkdir(parents=True, exist_ok=True)

            for arq in arquivos_origem:
                # Gera a query de qualidade para este arquivo específico
                caminho_input = arq.as_posix()
                query = omni_val.construir_query_qualidade(tabela_nome, campos, caminho_input)
                
                con.execute(f"CREATE OR REPLACE TEMP TABLE stg_processamento AS {query}")

                # Estatísticas do arquivo atual
                res = con.execute("""
                    SELECT 
                        COUNT(*), 
                        COUNT(*) FILTER (WHERE motivo_rejeicao = 'OK'),
                        COUNT(*) FILTER (WHERE motivo_rejeicao != 'OK')
                    FROM stg_processamento
                """).fetchone()

                total_lido += res[0]
                total_ok += res[1]
                total_erro += res[2]

                # Roteamento dos dados
                if is_partitioned:
                    caminho_out_prata = (dir_prata / tabela_nome / arq.name).as_posix()
                    caminho_out_dlq = (dir_quarentena / tabela_nome / f"{arq.stem}_dlq.parquet").as_posix()
                else:
                    caminho_out_prata = (dir_prata / f"{tabela_nome}.parquet").as_posix()
                    caminho_out_dlq = (dir_quarentena / f"{tabela_nome}_dlq.parquet").as_posix()

                if res[1] > 0:
                    con.execute(f"COPY (SELECT * EXCLUDE(motivo_rejeicao) FROM stg_processamento WHERE motivo_rejeicao = 'OK') TO '{caminho_out_prata}' (FORMAT 'parquet')")
                
                if res[2] > 0:
                    con.execute(f"COPY (SELECT * FROM stg_processamento WHERE motivo_rejeicao != 'OK') TO '{caminho_out_dlq}' (FORMAT 'parquet')")

            # Circuit Breaker (Validação após processar todos os arquivos da tabela)
            taxa_erro = (total_erro / total_lido * 100) if total_lido > 0 else 0
            if taxa_erro > tolerancia:
                raise Exception(f"CIRCUIT BREAKER: Taxa de erro de {taxa_erro:.2f}% superou o limite de {tolerancia}%")

            relatorio_execucao.append({
                "tabela": tabela_nome,
                "status": "sucesso_com_alertas" if total_erro > 0 else "sucesso_limpo",
                "linhas_lidas_bronze": total_lido,
                "linhas_gravadas_prata": total_ok,
                "linhas_quarentena": total_erro,
                "taxa_rejeicao_percentual": round(taxa_erro, 2),
                "tempo_segundos": round((datetime.now() - inicio_tabela).total_seconds(), 2)
            })

        except Exception as e:
            status_geral_pipeline = "falha_critica"
            print(f"Falha ao processar {tabela_nome}: {e}")
            relatorio_execucao.append({
                "tabela": tabela_nome,
                "status": "erro_critico",
                "detalhe": str(e)
            })

    con.close()
    
    # Salva o Log de Qualidade consolidado
    try:
        con_log = duckdb.connect()
        df_dq = con_log.execute("SELECT * FROM relatorio_execucao").df()
        df_dq.to_parquet(dir_quarentena / "log_qualidade_dados.parquet")
        con_log.close()
    except Exception: pass

    return {
        "pipeline": "Omnibox_Tratamento_Prata",
        "status_geral": status_geral_pipeline,
        "tabelas_processadas": len(relatorio_execucao),
        "duracao_total_segundos": round((datetime.now() - inicio_pipeline).total_seconds(), 2),
        "detalhes": relatorio_execucao
    }

if __name__ == "__main__":
    resultado = executar_tratamento_prata()
    print("\n" + "="*60 + "\nRELATÓRIO FINAL DA CAMADA PRATA (DATA QUALITY)\n" + "="*60)
    print(json.dumps(resultado, indent=4, ensure_ascii=False))