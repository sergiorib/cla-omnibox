import pandas as pd
import duckdb
import ipywidgets as widgets
from tqdm import tqdm
from IPython.display import display

def validar_nulos_e_brancos(con: duckdb.DuckDBPyConnection, nome_tabela: str, campos: list) -> pd.DataFrame:
    """
    Valida a presença de valores NULOS e BRANCOS em uma tabela no DuckDB,
    baseando-se nas regras (requerido, aceita_branco) definidas no JSON mestre.
    """
    resultados = []

    for campo in campos:
        nome_coluna = campo.get("nome")
        eh_requerido = campo.get("requerido")
        aceita_branco = campo.get("aceita_branco")
        tipo_dado = str(campo.get("tipo", "")).upper()

        # 1. Validação de Nulos (requerido: true -> NÃO aceita nulos)
        if eh_requerido is True:
            query_nulos = f"SELECT COUNT(*) FROM {nome_tabela} WHERE {nome_coluna} IS NULL"
            qtd_nulos = con.execute(query_nulos).fetchone()[0]
            
            if qtd_nulos > 0:
                resultados.append({
                    "Tabela": nome_tabela,
                    "Coluna": nome_coluna,
                    "Regra Violada": "Valor NULO não permitido",
                    "Qtd Erros": qtd_nulos
                })

        # 2. Validação de Brancos (aceita_branco: false -> NÃO aceita strings vazias)
        # O TRIM garante que strings contendo apenas espaços ("   ") também sejam pegas.
        if aceita_branco is False and "VARCHAR" in tipo_dado:
            query_brancos = f"SELECT COUNT(*) FROM {nome_tabela} WHERE {nome_coluna} IS NOT NULL AND TRIM({nome_coluna}::VARCHAR) = ''"
            qtd_brancos = con.execute(query_brancos).fetchone()[0]
            
            if qtd_brancos > 0:
                resultados.append({
                    "Tabela": nome_tabela,
                    "Coluna": nome_coluna,
                    "Regra Violada": "String VAZIA ou em BRANCO não permitida",
                    "Qtd Erros": qtd_brancos
                })

    # Se não houver erros na tabela, gera uma linha de sucesso
    if not resultados:
        return pd.DataFrame([{
            "Tabela": nome_tabela,
            "Coluna": "Todas",
            "Regra Violada": "-",
            "Qtd Erros": 0,
            "Status": "Aprovado (Nulos/Brancos)"
        }])

    return pd.DataFrame(resultados)

def validar_tipagem_dados(con: duckdb.DuckDBPyConnection, nome_tabela: str, campos: list) -> pd.DataFrame:
    """
    Valida se o conteúdo físico das colunas corresponde ao tipo de dado 
    especificado no JSON mestre usando TRY_CAST do DuckDB.
    """
    resultados = []
    
    # Dicionário de tradução: JSON Mestre -> Tipos Nativos do DuckDB
    mapa_tipos_duckdb = {
        "INTEGER": "BIGINT",  # Usamos BIGINT para evitar overflow com IDs longos
        "FLOAT": "DOUBLE",    # DOUBLE garante maior precisão decimal
        "DATE": "DATE",
        "DATETIME": "TIMESTAMP"
    }

    for campo in campos:
        nome_coluna = campo.get("nome")
        tipo_esperado = str(campo.get("tipo", "")).upper()

        # Só validamos se o tipo estiver na nossa lista de tipos estritos
        if tipo_esperado in mapa_tipos_duckdb:
            tipo_duckdb = mapa_tipos_duckdb[tipo_esperado]
            
            # A query verifica: O campo original TEM dado, mas a tentativa de conversão falhou (retornou NULL)
            query_tipagem = f"""
                SELECT COUNT(*) 
                FROM {nome_tabela} 
                WHERE {nome_coluna} IS NOT NULL 
                  AND TRY_CAST({nome_coluna} AS {tipo_duckdb}) IS NULL
            """
            
            qtd_erros_tipo = con.execute(query_tipagem).fetchone()[0]
            
            if qtd_erros_tipo > 0:
                resultados.append({
                    "Tabela": nome_tabela,
                    "Coluna": nome_coluna,
                    "Regra Violada": f"Incompatibilidade de Tipo (Esperado: {tipo_esperado})",
                    "Qtd Erros": qtd_erros_tipo
                })

    # Se não houver erros na tabela, gera uma linha de sucesso
    if not resultados:
        return pd.DataFrame([{
            "Tabela": nome_tabela,
            "Coluna": "Todas",
            "Regra Violada": "-",
            "Qtd Erros": 0,
            "Status": "✅ Aprovado (Tipagem)"
        }])

    return pd.DataFrame(resultados)

def validar_valores_unicos(con: duckdb.DuckDBPyConnection, nome_tabela: str, campos: list) -> pd.DataFrame:
    """
    Valida se os campos marcados com 'unico': true no JSON não possuem 
    valores duplicados na tabela. Valores nulos são ignorados nesta regra.
    """
    resultados = []

    for campo in campos:
        nome_coluna = campo.get("nome")
        eh_unico = campo.get("unico")

        if eh_unico is True:
            # A CTE (Common Table Expression) localiza os valores duplicados.
            # O SELECT final soma quantas linhas totais estão envolvidas nessas duplicidades.
            query_unicos = f"""
                WITH Duplicados AS (
                    SELECT {nome_coluna}, COUNT(*) as qtd
                    FROM {nome_tabela}
                    WHERE {nome_coluna} IS NOT NULL
                    GROUP BY {nome_coluna}
                    HAVING COUNT(*) > 1
                )
                SELECT SUM(qtd) FROM Duplicados
            """
            
            # Executa a query. Se não houver duplicados, o SUM retorna NULL (None no Python).
            resultado_query = con.execute(query_unicos).fetchone()[0]
            qtd_erros_unicos = int(resultado_query) if resultado_query is not None else 0
            
            if qtd_erros_unicos > 0:
                resultados.append({
                    "Tabela": nome_tabela,
                    "Coluna": nome_coluna,
                    "Regra Violada": "Valor ÚNICO exigido (Duplicidade encontrada)",
                    "Qtd Erros": qtd_erros_unicos
                })

    # Se não houver erros na tabela, gera uma linha de sucesso
    if not resultados:
        return pd.DataFrame([{
            "Tabela": nome_tabela,
            "Coluna": "Todas",
            "Regra Violada": "-",
            "Qtd Erros": 0,
            "Status": "✅ Aprovado (Valores Únicos)"
        }])

    return pd.DataFrame(resultados)

def validar_regras_numericas(con: duckdb.DuckDBPyConnection, nome_tabela: str, campos: list) -> pd.DataFrame:
    """
    Valida se os campos numéricos respeitam as restrições de 
    'aceita_zero' e 'aceita_negativo' definidas no JSON mestre.
    """
    resultados = []
    
    # Definimos quais tipos do JSON são considerados numéricos para esta regra
    tipos_numericos = ["INTEGER", "FLOAT"]

    for campo in campos:
        nome_coluna = campo.get("nome")
        tipo_dado = str(campo.get("tipo", "")).upper()
        aceita_zero = campo.get("aceita_zero")
        aceita_negativo = campo.get("aceita_negativo")

        # Só aplicamos a validação se o campo for numérico
        if tipo_dado in tipos_numericos:
            
            # 1. Validação de Zeros (aceita_zero: false)
            if aceita_zero is False:
                # TRY_CAST AS DOUBLE garante que a comparação numérica seja feita com segurança
                query_zeros = f"""
                    SELECT COUNT(*) FROM {nome_tabela} 
                    WHERE {nome_coluna} IS NOT NULL 
                      AND TRY_CAST({nome_coluna} AS DOUBLE) = 0
                """
                qtd_zeros = con.execute(query_zeros).fetchone()[0]
                
                if qtd_zeros > 0:
                    resultados.append({
                        "Tabela": nome_tabela,
                        "Coluna": nome_coluna,
                        "Regra Violada": "Valor ZERO não permitido",
                        "Qtd Erros": qtd_zeros
                    })

            # 2. Validação de Negativos (aceita_negativo: false)
            if aceita_negativo is False:
                query_negativos = f"""
                    SELECT COUNT(*) FROM {nome_tabela} 
                    WHERE {nome_coluna} IS NOT NULL 
                      AND TRY_CAST({nome_coluna} AS DOUBLE) < 0
                """
                qtd_negativos = con.execute(query_negativos).fetchone()[0]
                
                if qtd_negativos > 0:
                    resultados.append({
                        "Tabela": nome_tabela,
                        "Coluna": nome_coluna,
                        "Regra Violada": "Valor NEGATIVO não permitido",
                        "Qtd Erros": qtd_negativos
                    })

    # Retorno de sucesso caso a tabela passe limpa
    if not resultados:
        return pd.DataFrame([{
            "Tabela": nome_tabela,
            "Coluna": "Todas",
            "Regra Violada": "-",
            "Qtd Erros": 0,
            "Status": "✅ Aprovado (Regras Numéricas)"
        }])

    return pd.DataFrame(resultados)

def validar_regex(con: duckdb.DuckDBPyConnection, nome_tabela: str, campos: list) -> pd.DataFrame:
    """
    Valida se o conteúdo dos campos respeita as expressões regulares (regex)
    definidas no JSON mestre. Ignora valores nulos.
    """
    resultados = []

    for campo in campos:
        nome_coluna = campo.get("nome")
        padrao_regex = campo.get("regex")

        # Só aplica a validação se existir um regex definido (não nulo e não vazio)
        if padrao_regex:
            # Escapa aspas simples no regex para não quebrar a sintaxe do SQL no DuckDB
            padrao_regex_sql = padrao_regex.replace("'", "''")
            
            # A query verifica: O campo TEM dado, mas NÃO dá 'match' com o padrão Regex
            # Convertendo para VARCHAR para garantir que a função aceite o dado
            query_regex = f"""
                SELECT COUNT(*) 
                FROM {nome_tabela} 
                WHERE {nome_coluna} IS NOT NULL 
                  AND NOT regexp_matches({nome_coluna}::VARCHAR, '{padrao_regex_sql}')
            """
            
            try:
                qtd_erros_regex = con.execute(query_regex).fetchone()[0]
                
                if qtd_erros_regex > 0:
                    resultados.append({
                        "Tabela": nome_tabela,
                        "Coluna": nome_coluna,
                        "Regra Violada": f"Fora do padrão Regex ({padrao_regex})",
                        "Qtd Erros": qtd_erros_regex
                    })
            except duckdb.Error as e:
                # Captura erros caso a sintaxe do Regex fornecido no JSON seja incompatível com o motor
                resultados.append({
                    "Tabela": nome_tabela,
                    "Coluna": nome_coluna,
                    "Regra Violada": "Erro de sintaxe na expressão Regex do JSON",
                    "Qtd Erros": -1
                })

    # Retorno de sucesso caso a tabela passe limpa
    if not resultados:
        return pd.DataFrame([{
            "Tabela": nome_tabela,
            "Coluna": "Todas",
            "Regra Violada": "-",
            "Qtd Erros": 0,
            "Status": "Aprovado (Formato/Regex)"
        }])

    return pd.DataFrame(resultados)

def validar_lista_valores(con: duckdb.DuckDBPyConnection, nome_tabela: str, campos: list) -> pd.DataFrame:
    """
    Valida se o conteúdo dos campos pertence à lista de valores permitidos
    definida no parâmetro 'lista_validacao' do JSON mestre.
    """
    resultados = []

    for campo in campos:
        nome_coluna = campo.get("nome")
        lista_validacao_str = campo.get("lista_validacao")

        # Só aplica a validação se a chave existir e não for nula/vazia
        if lista_validacao_str:
            # Transforma a string "Valor1, Valor2" em uma lista limpa do Python
            valores_permitidos = [valor.strip() for valor in lista_validacao_str.split(",")]
            
            # Formata a lista para o padrão do SQL: ('Valor1', 'Valor2')
            valores_sql = ", ".join([f"'{v}'" for v in valores_permitidos])
            
            # A query verifica: O campo TEM dado, mas o valor NÃO ESTÁ na lista permitida.
            # O cast ::VARCHAR garante que a comparação textual funcione mesmo se a coluna for numérica.
            query_lista = f"""
                SELECT COUNT(*) 
                FROM {nome_tabela} 
                WHERE {nome_coluna} IS NOT NULL 
                  AND {nome_coluna}::VARCHAR NOT IN ({valores_sql})
            """
            
            qtd_erros_lista = con.execute(query_lista).fetchone()[0]
            
            if qtd_erros_lista > 0:
                resultados.append({
                    "Tabela": nome_tabela,
                    "Coluna": nome_coluna,
                    "Regra Violada": f"Valor fora da lista permitida ({lista_validacao_str})",
                    "Qtd Erros": qtd_erros_lista
                })

    # Retorno de sucesso caso a tabela passe limpa
    if not resultados:
        return pd.DataFrame([{
            "Tabela": nome_tabela,
            "Coluna": "Todas",
            "Regra Violada": "-",
            "Qtd Erros": 0,
            "Status": "✅ Aprovado (Lista de Validação)"
        }])

    return pd.DataFrame(resultados)

def validar_chave_primaria(con: duckdb.DuckDBPyConnection, nome_tabela: str, campos: list) -> pd.DataFrame:
    """
    Valida a integridade da Chave Primária (PK).
    Suporta chaves simples ou compostas agrupando dinamicamente os campos marcados com 'pk': true.
    """
    resultados = []

    # 1. Identifica todos os campos que compõem a Chave Primária
    campos_pk = [campo.get("nome") for campo in campos if campo.get("pk") is True]

    # Se a tabela não tiver PK definida no JSON, retornamos um aviso amigável
    if not campos_pk:
        return pd.DataFrame([{
            "Tabela": nome_tabela,
            "Coluna": "-",
            "Regra Violada": "-",
            "Qtd Erros": 0,
            "Status": "⚠️ Nenhuma PK configurada"
        }])

    # 2. Prepara as strings para o SQL e para o relatório visual
    colunas_sql = ", ".join(campos_pk) # Ex: "pedido_id, item_id"
    colunas_exibicao = " + ".join(campos_pk) # Ex: "pedido_id + item_id"

    # 3. A CTE agrupa pela combinação exata das colunas PK e filtra as que aparecem mais de uma vez
    query_pk = f"""
        WITH Duplicados AS (
            SELECT {colunas_sql}, COUNT(*) as qtd
            FROM {nome_tabela}
            GROUP BY {colunas_sql}
            HAVING COUNT(*) > 1
        )
        SELECT SUM(qtd) FROM Duplicados
    """
    
    try:
        resultado_query = con.execute(query_pk).fetchone()[0]
        qtd_erros_pk = int(resultado_query) if resultado_query is not None else 0
        
        if qtd_erros_pk > 0:
            resultados.append({
                "Tabela": nome_tabela,
                "Coluna": colunas_exibicao, # Mostra claramente se a falha foi numa chave composta
                "Regra Violada": "Violação de Chave Primária (Duplicidade)",
                "Qtd Erros": qtd_erros_pk
            })
            
    except duckdb.Error as e:
        resultados.append({
            "Tabela": nome_tabela,
            "Coluna": colunas_exibicao,
            "Regra Violada": f"Erro de SQL na validação: {e}",
            "Qtd Erros": -1
        })

    # Retorno de sucesso caso a tabela passe limpa
    if not resultados:
        return pd.DataFrame([{
            "Tabela": nome_tabela,
            "Coluna": colunas_exibicao,
            "Regra Violada": "-",
            "Qtd Erros": 0,
            "Status": "✅ Aprovado (Chave Primária)"
        }])

    return pd.DataFrame(resultados)

def validar_chaves_estrangeiras(con: duckdb.DuckDBPyConnection, nome_tabela: str, campos: list) -> pd.DataFrame:
    """
    Valida a Integridade Referencial (FK).
    1. Verifica se o valor existe na tabela relacionada.
    2. Verifica se o valor é único na tabela relacionada (Integridade de Lookup).
    """
    resultados = []

    for campo in campos:
        nome_coluna = campo.get("nome")
        eh_fk = campo.get("fk")
        tabela_pai = campo.get("fk_table")
        campo_pai = campo.get("fk_field")

        if eh_fk is True and tabela_pai and campo_pai:
            
            # --- Regra 1: Existência (Orfãos) ---
            # Procuramos registros na tabela atual que NÃO existem na tabela pai
            query_existencia = f"""
                SELECT COUNT(t1.{nome_coluna})
                FROM {nome_tabela} t1
                LEFT JOIN {tabela_pai} t2 ON t1.{nome_coluna} = t2.{campo_pai}
                WHERE t1.{nome_coluna} IS NOT NULL 
                  AND t2.{campo_pai} IS NULL
            """
            
            try:
                qtd_orfaos = con.execute(query_existencia).fetchone()[0]
                if qtd_orfaos > 0:
                    resultados.append({
                        "Tabela": nome_tabela,
                        "Coluna": nome_coluna,
                        "Regra Violada": f"FK Órfã (Não existe em {tabela_pai}.{campo_pai})",
                        "Qtd Erros": qtd_orfaos
                    })

                # --- Regra 2: Unicidade na Tabela Pai ---
                # Garante que o campo de destino na tabela pai não tenha duplicatas
                query_unicidade_pai = f"""
                    WITH Duplicados AS (
                        SELECT {campo_pai}, COUNT(*)
                        FROM {tabela_pai}
                        WHERE {campo_pai} IS NOT NULL
                        GROUP BY {campo_pai}
                        HAVING COUNT(*) > 1
                    )
                    SELECT COUNT(*) FROM Duplicados
                """
                
                qtd_duplicados_pai = con.execute(query_unicidade_pai).fetchone()[0]
                if qtd_duplicados_pai > 0:
                    resultados.append({
                        "Tabela": nome_tabela,
                        "Coluna": nome_coluna,
                        "Regra Violada": f"Destino FK Duplicado (Erro em {tabela_pai}.{campo_pai})",
                        "Qtd Erros": qtd_duplicados_pai
                    })

            except duckdb.Error as e:
                resultados.append({
                    "Tabela": nome_tabela,
                    "Coluna": nome_coluna,
                    "Regra Violada": f"Erro ao acessar tabela relacionada {tabela_pai}",
                    "Qtd Erros": -1
                })

    # Retorno de sucesso
    if not resultados:
        return pd.DataFrame([{
            "Tabela": nome_tabela,
            "Coluna": "Todas",
            "Regra Violada": "-",
            "Qtd Erros": 0,
            "Status": "✅ Aprovado (Chaves Estrangeiras)"
        }])

    return pd.DataFrame(resultados)

def validar_regra_negocio_sql(con: duckdb.DuckDBPyConnection, nome_regra: str, query_sql: str) -> pd.DataFrame:
    """
    Executa uma query SQL customizada para validar uma regra de negocio especifica.
    A query de entrada deve retornar a quantidade de erros encontrados (COUNT).
    """
    try:
        resultado = con.execute(query_sql).fetchone()
        qtd_erros = int(resultado[0]) if resultado else 0
        
        if qtd_erros > 0:
            status = "Divergencia Encontrada"
        else:
            status = "Aprovado"
            
        return pd.DataFrame([{
            "Validacao de Negocio": nome_regra,
            "Status": status,
            "Qtd Registros com Erro": qtd_erros
        }])
        
    except duckdb.Error as e:
        return pd.DataFrame([{
            "Validacao de Negocio": nome_regra,
            "Status": "Erro de Execucao SQL",
            "Qtd Registros com Erro": -1,
            "Detalhe": str(e)
        }])
    
def gerar_descricao_estatistica(con: duckdb.DuckDBPyConnection, nome_tabela: str) -> None:
    """
    Gera e exibe o resumo estatístico (numérico e categórico) de uma tabela.
    Correção: Usa con.query().to_df() para extrair os dados corretamente.
    """
    print(f"\n=== Resumo Estatístico: Tabela {nome_tabela.upper()} ===")
    
    try:
        # CORREÇÃO: O método to_df() deve ser chamado após a execução da query
        df = con.query(f"SELECT * FROM {nome_tabela}").to_df()
        
        if df.empty:
            print(f"A tabela {nome_tabela} está vazia.")
            return

        # 1. Resumo Numérico
        desc_num = df.describe()
        if not desc_num.empty:
            print("\n[Dados Numéricos]")
            display(desc_num)
        
        # 2. Resumo Categórico (include='O' para objetos/strings)
        try:
            desc_cat = df.describe(include=['O'])
            if not desc_cat.empty:
                print("\n[Dados Categóricos / Texto]")
                display(desc_cat)
        except ValueError:
            # Caso não existam colunas de texto, o pandas apenas ignora
            pass
            
        print("-" * 50)
        
    except Exception as e:
        print(f"Erro ao processar estatísticas da tabela {nome_tabela}: {e}")

def construir_query_qualidade(tabela_nome: str, campos: list, caminho_bronze: str) -> str:
    """
    Constrói dinamicamente a query de extração, limpeza, TIPAGEM e validação de qualidade (DLQ).
    """
    colunas_limpas = []
    regras_case = []

    # =========================================================================
    # 1. CONVERSÃO DE TIPOS DINÂMICA (JSON -> DUCKDB)
    # Mapeia os tipos do seu JSON para os tipos físicos suportados pelo DuckDB
    # =========================================================================
    mapa_tipos_duckdb = {
        "INTEGER": "BIGINT",      # BIGINT previne overflow em IDs grandes
        "FLOAT": "DOUBLE",        # DOUBLE garante precisão em valores monetários
        "DATE": "DATE",
        "DATETIME": "TIMESTAMP",  # No DuckDB, datetime é chamado de TIMESTAMP
        "VARCHAR": "VARCHAR",
        "BOOLEAN": "BOOLEAN"
    }

    for c in campos:
        nome = c["nome"]
        # .upper() garante que "datetime" ou "DATETIME" do JSON funcionem igual
        tipo_json = str(c.get("tipo", "VARCHAR")).upper()
        
        # Busca o tipo correspondente. Se não achar, converte para VARCHAR por segurança
        tipo_banco = mapa_tipos_duckdb.get(tipo_json, "VARCHAR")
        
        if tipo_banco == "VARCHAR":
            # Para textos: apenas remove espaços inúteis e transforma string vazia em nulo nativo
            colunas_limpas.append(f"NULLIF(TRIM({nome}::VARCHAR), '') AS {nome}")
        else:
            # Para os demais (Números, Datas, Booleanos): aplica a conversão estrutural.
            # O TRY_CAST impede que a query "estoure" caso encontre um texto num campo de data.
            colunas_limpas.append(f"TRY_CAST(NULLIF(TRIM({nome}::VARCHAR), '') AS {tipo_banco}) AS {nome}")

    # =========================================================================
    # 2. Construção das Regras de Rejeição (A Peneira de Quarentena)
    # =========================================================================
    
    # 2.1 - Chave Primária (PK)
    pk_cols = [c["nome"] for c in campos if c.get("pk")]
    if pk_cols:
        pk_str = ", ".join(pk_cols)
        regras_case.append(f"WHEN COUNT(*) OVER (PARTITION BY {pk_str}) > 1 THEN 'Duplicidade de Chave Primária ({pk_str})'")

    # 2.2 - Regras do JSON
    for c in campos:
        nome = c["nome"]
        tipo_json = str(c.get("tipo", "")).upper()
        
        if c.get("requerido"):
            regras_case.append(f"WHEN {nome} IS NULL THEN 'Nulo ou Vazio não permitido: {nome}'")
            
        if c.get("aceita_zero") is False and tipo_json in ["INTEGER", "FLOAT"]:
            regras_case.append(f"WHEN TRY_CAST({nome} AS DOUBLE) = 0 THEN 'Zero não permitido: {nome}'")
            
        if c.get("aceita_negativo") is False and tipo_json in ["INTEGER", "FLOAT"]:
            regras_case.append(f"WHEN TRY_CAST({nome} AS DOUBLE) < 0 THEN 'Negativo não permitido: {nome}'")
            
        if c.get("lista_validacao"):
            lista_sql = ", ".join([f"'{v}'" for v in c["lista_validacao"]])
            regras_case.append(f"WHEN {nome}::VARCHAR NOT IN ({lista_sql}) AND {nome} IS NOT NULL THEN 'Fora do Domínio na coluna: {nome}'")
            
        if c.get("regex"):
            rgx = c["regex"].replace("'", "''")
            regras_case.append(f"WHEN NOT regexp_matches({nome}::VARCHAR, '{rgx}') AND {nome} IS NOT NULL THEN 'Falha no padrão Regex: {nome}'")

    if regras_case:
        bloco_case = "CASE " + " ".join(regras_case) + " ELSE 'OK' END AS motivo_rejeicao"
    else:
        bloco_case = "'OK' AS motivo_rejeicao"

    # =========================================================================
    # 3. Macro-Query (Inclui exceção de Flattening para Google Analytics)
    # =========================================================================
    # if tabela_nome == 'google_analytics':
    #     colunas_raiz_ga = ['event_date', 'event_timestamp', 'event_name', 'user_id', 'user_pseudo_id']
    #     linhas_select_raw = []
        
    #     for c in campos:
    #         campo = c["nome"]
    #         if campo in colunas_raiz_ga:
    #             linhas_select_raw.append(campo)
    #         elif campo == 'region':
    #             linhas_select_raw.append(f"geo.{campo} AS {campo}")
    #         else:
    #             extrator = f"COALESCE((list_filter(event_params, x -> x.key == '{campo}')[1]).value.string_value, (list_filter(event_params, x -> x.key == '{campo}')[1]).value.int_value::VARCHAR) AS {campo}"
    #             linhas_select_raw.append(extrator)
                
    #     select_raw_sql = ",\n                ".join(linhas_select_raw)
        
    #     raw_data_cte = f"""
    #     raw_data AS (
    #         SELECT 
    #             {select_raw_sql}
    #         FROM read_parquet('{caminho_bronze}', union_by_name=true)
    #     )"""
    # else:
    raw_data_cte = f"""raw_data AS (SELECT * FROM read_parquet('{caminho_bronze}', union_by_name=true))"""

    query_completa = f"""
        WITH {raw_data_cte},
        cleaned AS (
            SELECT {', '.join(colunas_limpas)}
            FROM raw_data
        ),
        validated AS (
            SELECT *, {bloco_case}
            FROM cleaned
        )
        SELECT * FROM validated;
    """
    
    return query_completa


