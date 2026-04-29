import duckdb
from pathlib import Path
from datetime import datetime
import pandas as pd
import json

# --- 1. CONFIGURAÇÃO DE DIRETÓRIOS DINÂMICOS ---
DIRETORIO_ATUAL = Path(__file__).resolve()
DIRETORIO_BASE = DIRETORIO_ATUAL.parents[1] 

DIR_PRATA = DIRETORIO_BASE / "dados" / "prata"
DIR_OURO = DIRETORIO_BASE / "dados" / "ouro"

DIR_OURO.mkdir(parents=True, exist_ok=True)

# Arquivos PRATA
PRATA_CATEGORY = (DIR_PRATA / 'category.parquet').as_posix()
PRATA_SUB_CATEGORY = (DIR_PRATA / 'sub_category.parquet').as_posix()
PRATA_PRODUCTS = (DIR_PRATA / 'products.parquet').as_posix()
PRATA_CUSTOMERS = (DIR_PRATA / 'customers.parquet').as_posix()
PRATA_ANALYTICS = (DIR_PRATA / 'google_analytics' / '*.parquet').as_posix()
print(PRATA_ANALYTICS)
PRATA_ORDERS = (DIR_PRATA / 'orders.parquet').as_posix()
PRATA_ORDER_ITEMS = (DIR_PRATA / 'order_items.parquet').as_posix()
PRATA_MKT_CAMPAIGNS = (DIR_PRATA / 'mkt_campaigns.parquet').as_posix()
PRATA_MKT_ACTIONS = (DIR_PRATA / 'mkt_actions.parquet').as_posix()
PRATA_MKT_SPEND = (DIR_PRATA / 'mkt_spend.parquet').as_posix()

# Arquivos OURO (usados como dependência para CTEs/Joins de outras tabelas Ouro)
OURO_CALENDARIO = (DIR_OURO / 'calendario.parquet').as_posix()
OURO_CLIENTES = (DIR_OURO / 'clientes.parquet').as_posix()
OURO_VENDAS = (DIR_OURO / 'vendas.parquet').as_posix()
OURO_FUNIL = (DIR_OURO / 'funil.parquet').as_posix()

DATA_INICIO = '2024-01-01'
DATA_FIM    = '2025-12-31'

# --- 2. FUNÇÕES DE TRANSFORMAÇÃO (Regras de Negócio) ---

def gerar_calendario(con): 
    query_calendario = f"""
            SELECT 
                CAST(range AS DATE) as data,
                day(range) as dia,
                year(range) as ano,
                month(range) as mes,
                quarter(range) as trimestre,
                'Trimestre ' || CAST(quarter(range) AS VARCHAR) as trimestre_nome,
                CASE 
                    WHEN month(range) <= 6 THEN 1 
                    ELSE 2 
                END as semestre,
                CASE 
                    WHEN month(range) <= 6 THEN 'Primeiro semestre' 
                    ELSE 'Segundo semestre' 
                END as semestre_nome,
                strftime(range, '%y') AS ano_abrev,
                strftime(range, '%d/%m') as dia_mes,    
                strftime(range, '%Y%m') as ano_mes, 
                strftime(range, '%y%m') as ano_abrev_mes, 
                strftime(range, '%m') as mes_abrev, 
                CASE month(range)
                    WHEN 1 THEN 'Janeiro'
                    WHEN 2 THEN 'Fevereiro'
                    WHEN 3 THEN 'Março'
                    WHEN 4 THEN 'Abril'
                    WHEN 5 THEN 'Maio'
                    WHEN 6 THEN 'Junho'
                    WHEN 7 THEN 'Julho'
                    WHEN 8 THEN 'Agosto'
                    WHEN 9 THEN 'Setembro'
                    WHEN 10 THEN 'Outubro'
                    WHEN 11 THEN 'Novembro'
                    WHEN 12 THEN 'Dezembro'
                END as mes_nome_completo,
                CASE month(range)
                    WHEN 1 THEN 'Jan'
                    WHEN 2 THEN 'Fev'
                    WHEN 3 THEN 'Mar'
                    WHEN 4 THEN 'Abr'
                    WHEN 5 THEN 'Mai'
                    WHEN 6 THEN 'Jun'
                    WHEN 7 THEN 'Jul'
                    WHEN 8 THEN 'Ago'
                    WHEN 9 THEN 'Set'
                    WHEN 10 THEN 'Out'
                    WHEN 11 THEN 'Nov'
                    WHEN 12 THEN 'Dez'
                END as mes_nome_abrev,
                strftime(range, '%m/%Y') as mes_ano,
                (CASE month(range)
                    WHEN 1 THEN 'Jan' WHEN 2 THEN 'Fev' WHEN 3 THEN 'Mar'
                    WHEN 4 THEN 'Abr' WHEN 5 THEN 'Mai' WHEN 6 THEN 'Jun'
                    WHEN 7 THEN 'Jul' WHEN 8 THEN 'Ago' WHEN 9 THEN 'Set'
                    WHEN 10 THEN 'Out' WHEN 11 THEN 'Nov' WHEN 12 THEN 'Dez'
                END) || '/' || CAST(year(range) AS VARCHAR) as mes_ano_abrev,
                (mes_nome_abrev || '/' || CAST(ano_abrev AS VARCHAR)) as mes_abrev_ano_abrev,
                CASE dayofweek(range)
                    WHEN 0 THEN 'Domingo'
                    WHEN 1 THEN 'Segunda'
                    WHEN 2 THEN 'Terça'
                    WHEN 3 THEN 'Quarta'
                    WHEN 4 THEN 'Quinta'
                    WHEN 5 THEN 'Sexta'
                    WHEN 6 THEN 'Sábado'
                END as dia_semana
            FROM range(
                CAST('{DATA_INICIO}' AS DATE), 
                CAST('{DATA_FIM}' AS DATE) + INTERVAL '1 day',
                INTERVAL '1 day'
            )
    """
    path_out = (DIR_OURO / 'calendario.parquet').as_posix()
    con.execute(f"COPY ({query_calendario}) TO '{path_out}' (FORMAT PARQUET)")

def gerar_produtos(con): 
    query_produtos = f"""
    SELECT 
    CAST(p.product_id AS INTEGER) AS id_produto,
    CAST(c.name AS VARCHAR) AS categoria,
    CAST(sc.name AS VARCHAR) AS sub_categoria,
    CAST(p.name AS VARCHAR) AS nome,   
    CAST(sc.sub_category_id AS INTEGER) AS id_sub_categoria
    FROM read_parquet('{PRATA_PRODUCTS}') p
    LEFT JOIN read_parquet('{PRATA_SUB_CATEGORY}') sc ON p.sub_category_id = sc.sub_category_id
    LEFT JOIN read_parquet('{PRATA_CATEGORY}') c ON c.category_id = sc.category_id
    """
    path_out = (DIR_OURO / 'produtos.parquet').as_posix()
    con.execute(f"COPY ({query_produtos}) TO '{path_out}' (FORMAT PARQUET)") 

def gerar_clientes(con): 
    query_clientes = f"""
        WITH sessoes_unicas AS (
        SELECT DISTINCT 
            CAST(user_id AS BIGINT) AS id_cliente,
            CAST(ga_session_id AS VARCHAR) AS ga_session_id,
            CAST(source AS VARCHAR) AS source,
            CAST(medium AS VARCHAR) AS medium
        FROM read_parquet('{PRATA_ANALYTICS}')
        WHERE user_id IS NOT NULL
    ),
    primeira_sessao AS (
        SELECT 
            id_cliente,
            source,
            medium,
            ROW_NUMBER() OVER (PARTITION BY id_cliente ORDER BY ga_session_id ASC) as rank
        FROM sessoes_unicas
    ), 
    primeira_compra AS (
    SELECT 
        customer_id, 
        MIN(order_date) AS primeira_compra
    FROM read_parquet('{PRATA_ORDERS}')
    GROUP BY customer_id)
    SELECT 
        CAST(c.customer_id AS INTEGER) AS id_cliente,
        CAST(concat(c.first_name, ' ', c.last_name) AS VARCHAR) AS nome,
        CAST(c.gender AS VARCHAR) AS sexo,
        CAST(c.birth_date AS DATE) as dt_nascimento,
        CAST(c.city AS VARCHAR) AS cidade,
        CAST(c.state AS VARCHAR) AS UF,
        CAST(c.signup_date AS DATE) AS dt_cadastro,
        CAST(COALESCE(ps.source,'Outros') AS VARCHAR) AS canal,
        CAST(COALESCE(ps.medium, 'Outros') AS VARCHAR) AS midia,
        CAST(
        COALESCE(
        CASE 
            WHEN ps.source = 'instagram' AND ps.medium = 'cpc' THEN 'Pago' 
            WHEN ps.source = 'google' AND ps.medium = 'cpc' THEN 'Pago'
            WHEN ps.source = 'tiktok' AND ps.medium = 'cpc' THEN 'Pago'
            WHEN ps.source = 'facebook' AND ps.medium = 'cpc' THEN 'Pago'
            WHEN ps.source = 'google' AND ps.medium = 'organic' THEN 'Organico'
            WHEN ps.source = 'crm' AND ps.medium = 'email' THEN 'Email'
            WHEN ps.source = 'direct'  THEN 'Direct'
            ELSE 'Outros'
        END,'Outros') AS VARCHAR    
        ) AS trafego, 
        CAST(pc.primeira_compra AS DATE) as dt_primeira_compra
    FROM read_parquet('{PRATA_CUSTOMERS}') c
    LEFT JOIN primeira_sessao ps ON c.customer_id = ps.id_cliente AND ps.rank = 1 
    LEFT JOIN primeira_compra pc ON c.customer_id = pc. customer_id 
    """
    path_out = (DIR_OURO / 'clientes.parquet').as_posix()
    con.execute(f"COPY ({query_clientes}) TO '{path_out}' (FORMAT PARQUET)") 

def gerar_marketing_acoes(con): 
    query_marketing_acoes = f"""
    SELECT 
        CAST(mkt_act.action_id AS INTEGER) as id_acao, 
        CAST(mkt_act.action_code AS VARCHAR) AS codigo_acao,     
        CAST(mkt_camp.name AS VARCHAR) AS campanha, 
        CAST(mkt_act.objective AS VARCHAR) AS objetivo, 
        CAST(mkt_act.source AS VARCHAR) AS canal, 
        CASE 
            WHEN coalesce(mkt_act.medium, '(Nenhuma)') = '(none)' THEN CAST('(Nenhuma)' AS VARCHAR)
            ELSE CAST(coalesce(mkt_act.medium, '(Nenhuma)') AS VARCHAR)
        END AS midia, 
        CAST(mkt_act.start_date AS DATE) dt_inicio, 
        CAST(mkt_act.end_date AS DATE) dt_fim 
    FROM read_parquet('{PRATA_MKT_CAMPAIGNS}') mkt_camp
    LEFT JOIN read_parquet('{PRATA_MKT_ACTIONS}') mkt_act ON mkt_act.campaign_id = mkt_camp.campaign_id
    """
    path_out = (DIR_OURO / 'marketing_acoes.parquet').as_posix()
    con.execute(f"COPY ({query_marketing_acoes}) TO '{path_out}' (FORMAT PARQUET)") 

def gerar_marketing_despesas(con): 
    query_marketing_despesas = f"""
    SELECT 
        CAST(mkt_act.action_id AS INTEGER) as id_acao, 
        CAST(mkt_spend.date AS DATE) AS data, 
        CAST(mkt_act.action_code AS VARCHAR) AS codigo_acao,
        CAST(mkt_spend.spend_amount AS DECIMAL(10,2)) AS vlr_gasto, 
        CAST(mkt_spend.clicks AS BIGINT) AS cliques, 
        CAST(mkt_spend.impressions AS BIGINT) AS impressoes 
    FROM read_parquet('{PRATA_MKT_SPEND}') mkt_spend
    JOIN read_parquet('{PRATA_MKT_ACTIONS}') mkt_act ON mkt_spend.action_id = mkt_act.action_id
    JOIN read_parquet('{PRATA_MKT_CAMPAIGNS}') mkt_camp ON mkt_act.campaign_id = mkt_camp.campaign_id 
    """
    path_out = (DIR_OURO / 'marketing_despesas.parquet').as_posix()
    con.execute(f"COPY ({query_marketing_despesas}) TO '{path_out}' (FORMAT PARQUET)") 

def gerar_vendas(con): 
    query_vendas = f"""
    SELECT 
        CAST(o.order_id AS INTEGER) AS id_venda,
        CAST(oi.order_item_id AS INTEGER) AS id_item,
        CAST(oi.product_id AS INTEGER) AS id_produto,
        CAST(o.customer_id AS INTEGER) AS id_cliente,
        CAST(o.order_date AS DATE) AS dt_venda, 
        CAST(o.shipped_date AS DATE) AS dt_envio, 
        CAST(o.delivery_date AS DATE) AS dt_entrega, 
        CAST(o.order_status AS VARCHAR) AS status_venda, 
        CAST(transaction_id AS VARCHAR) AS id_trasancao,
        CAST(c.canal AS VARCHAR) AS codigo_acao,
        CAST(oi.quantity AS INTEGER) AS quantidade,
        CAST(round(oi.unit_cost, 2) AS DECIMAL(10,2)) AS vlr_custo_produto,
        CAST(oi.price AS DECIMAL(10,2)) as vlr_preco,  

        CAST(COALESCE(ROUND(o.discount * (oi.item_value / NULLIF((o.gross_amount + (o.discount - COALESCE(o.shipping_cost,0))), 0)), 2), 0) AS DECIMAL(18,2)) AS vlr_desconto,
       
        CAST(ROUND((oi.price * quantidade),2) AS DECIMAL(18,2)) vlr_fat_bruto,  
        CAST(ROUND((oi.unit_cost * quantidade),2) AS DECIMAL(18,2)) vlr_custo_venda,
        CAST(ROUND(((oi.price * quantidade) - o.discount),2) AS DECIMAL(18,2)) vlr_receita_liq,  
        -- Cálculo de Frete Empresa com proteção Divisão por Zero
        CASE
            WHEN o.free_shipping = 1 THEN 
                CAST(COALESCE(ROUND(COALESCE(o.shipping_cost,0) * (oi.item_value / NULLIF((o.gross_amount + (o.discount - COALESCE(o.shipping_cost,0))), 0)), 2), 0) AS DECIMAL(18,2))
            ELSE CAST(0 AS DECIMAL(10,2)) 
        END AS vlr_frete_empresa, 
        
        -- Cálculo de Frete Cliente com proteção Divisão por Zero
        CASE
            WHEN o.free_shipping = 0 THEN 
                CAST(COALESCE(ROUND(COALESCE(o.shipping_cost,0) * (oi.item_value / NULLIF((o.gross_amount + (o.discount - COALESCE(o.shipping_cost,0))), 0)), 2), 0) AS DECIMAL(18,2))
            ELSE CAST(0 AS DECIMAL(10,2)) 
        END AS vlr_frete_cliente,
        
        CAST(ROUND((oi.item_value - (oi.unit_cost * quantidade)),2) AS DECIMAL(18,2)) vlr_lucro_venda,
        date_diff('day', CAST(c.dt_primeira_compra AS DATE), CAST(o.order_date AS DATE)) AS tempo_decorrido_prim_compra

    FROM read_parquet('{PRATA_ORDERS}') o
    LEFT JOIN read_parquet('{PRATA_ORDER_ITEMS}') oi ON o.order_id = oi.order_id
    LEFT JOIN read_parquet('{OURO_CLIENTES}') c ON o.customer_id = c.id_cliente      
    WHERE UPPER(o.order_status) = 'COMPLETED' 
    """
    path_out = (DIR_OURO / 'vendas.parquet').as_posix()
    con.execute(f"COPY ({query_vendas}) TO '{path_out}' (FORMAT PARQUET)") 

def gerar_marketing(con): 
    query_marketing = f"""
    SELECT 
        CAST(mkt_spend.date AS DATE) AS data, 
        CAST(mkt_act.action_code AS VARCHAR) AS codigo_acao,
        CAST(mkt_spend.spend_amount AS DECIMAL(10,2)) AS investimento, 
        CAST(mkt_spend.clicks AS BIGINT) AS cliques, 
        CAST(mkt_spend.impressions AS BIGINT) AS impressoes 
    FROM read_parquet('{PRATA_MKT_SPEND}') mkt_spend
    JOIN read_parquet('{PRATA_MKT_ACTIONS}') mkt_act ON mkt_spend.action_id = mkt_act.action_id
    JOIN read_parquet('{PRATA_MKT_CAMPAIGNS}') mkt_camp ON mkt_act.campaign_id = mkt_camp.campaign_id 
    """
    path_out = (DIR_OURO / 'marketing.parquet').as_posix()
    con.execute(f"COPY ({query_marketing}) TO '{path_out}' (FORMAT PARQUET)") 

def gerar_rfm(con): 
    query_rfm = f"""
    WITH calendario_mensal AS (
        SELECT last_day(range) AS dt_referencia
        FROM range(
            CAST('{DATA_INICIO}' AS DATE),
            CAST('{DATA_FIM}' AS DATE),
            INTERVAL '1 month'
        )
    ),
    clientes_base AS (
        SELECT
            c.id_cliente,
            c.dt_primeira_compra,
            cal.dt_referencia,
        CASE 
            WHEN DATE_TRUNC('month', c.dt_cadastro) = DATE_TRUNC('month', cal.dt_referencia) THEN 1 ELSE 0
        END AS flag_cliente_novo      
        FROM read_parquet('{OURO_CLIENTES}') c
        JOIN calendario_mensal cal
            ON cal.dt_referencia >= c.dt_primeira_compra
    ),
    vendas_mensais AS (
        SELECT
            id_cliente,
            last_day(dt_venda) AS dt_referencia,
            MAX(dt_venda) AS ultima_compra_mes,
            COUNT(DISTINCT id_venda) AS freq_mes,
            SUM(vlr_lucro_venda) AS lucro_mes
        FROM read_parquet('{OURO_VENDAS}')
        GROUP BY
            id_cliente,
            last_day(dt_venda)
    ),
    base_join AS (
        SELECT
            cb.id_cliente,
            cb.dt_referencia,
            cb.flag_cliente_novo,
            vm.ultima_compra_mes,
            vm.freq_mes,
            vm.lucro_mes
        FROM clientes_base cb
        LEFT JOIN vendas_mensais vm
            ON cb.id_cliente = vm.id_cliente
            AND cb.dt_referencia = vm.dt_referencia
    ),
    acumulado AS (
        SELECT
            id_cliente,
            dt_referencia,
            flag_cliente_novo,
            LAST_VALUE(ultima_compra_mes IGNORE NULLS) OVER (
                PARTITION BY id_cliente
                ORDER BY dt_referencia
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS ultima_compra,
            SUM(COALESCE(freq_mes,0)) OVER (
                PARTITION BY id_cliente
                ORDER BY dt_referencia
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS frequencia,
            SUM(COALESCE(lucro_mes,0)) OVER (
                PARTITION BY id_cliente
                ORDER BY dt_referencia
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS monetizacao
        FROM base_join
    ),
    metricas AS (
        SELECT
            id_cliente,
            dt_referencia,
            flag_cliente_novo,
            DATEDIFF('day', ultima_compra, dt_referencia) AS recencia_dias,
            DATEDIFF('month', ultima_compra, dt_referencia) AS recencia_meses,
            COALESCE(frequencia, 0) AS frequencia,
            COALESCE(monetizacao, 0) AS monetizacao
        FROM acumulado
    ),
    classificacao_faixas AS (
        SELECT 
            *,
            CAST(CASE
                WHEN frequencia = 1 THEN '1'
                WHEN frequencia BETWEEN 2 AND 3 THEN '2-3'
                WHEN frequencia BETWEEN 4 AND 6 THEN '4-6'
                WHEN frequencia BETWEEN 7 AND 9 THEN '7-9'
                WHEN frequencia >= 10 THEN '10+'
                ELSE '0'
            END AS VARCHAR) AS faixa_frequencia,
            CAST(CASE
                WHEN frequencia = 1 THEN 1
                WHEN frequencia BETWEEN 2 AND 3 THEN 2
                WHEN frequencia BETWEEN 4 AND 6 THEN 3
                WHEN frequencia BETWEEN 7 AND 9 THEN 4
                WHEN frequencia >= 10 THEN 5
                ELSE 0
            END AS INTEGER) AS ordem_frequencia,
            CAST(CASE
                WHEN recencia_meses <= 1 THEN '0-1'
                WHEN recencia_meses BETWEEN 2 AND 3 THEN '2-3'
                WHEN recencia_meses BETWEEN 4 AND 6 THEN '4-6'
                WHEN recencia_meses BETWEEN 7 AND 12 THEN '7-12'
                WHEN recencia_meses > 12 THEN '12+'
                ELSE 'N/A'
            END AS VARCHAR) AS faixa_recencia,
            CAST(CASE
                WHEN recencia_meses <= 1 THEN 1
                WHEN recencia_meses BETWEEN 2 AND 3 THEN 2
                WHEN recencia_meses BETWEEN 4 AND 6 THEN 3
                WHEN recencia_meses BETWEEN 7 AND 12 THEN 4
                WHEN recencia_meses > 12 THEN 5
                ELSE 99
            END AS INTEGER) AS ordem_recencia
        FROM metricas
    ),
    scores AS (
        SELECT
            *,
            NTILE(5) OVER (PARTITION BY dt_referencia ORDER BY recencia_dias DESC) AS score_r,
            NTILE(5) OVER (PARTITION BY dt_referencia ORDER BY frequencia ASC) AS score_f,
            NTILE(5) OVER (PARTITION BY dt_referencia ORDER BY monetizacao ASC) AS score_m
        FROM classificacao_faixas
    ),
    ranking AS (
        SELECT
            *,
            (score_r + score_f + score_m) AS score_unificado
        FROM scores
    )
    SELECT
        *,
        CAST(CASE
            WHEN score_r = 5 AND (score_f BETWEEN 4 AND 5) THEN 'Campeões'
            WHEN (score_r BETWEEN 3 AND 4) AND (score_f BETWEEN 4 AND 5) THEN 'Clientes fiéis'
            WHEN (score_r BETWEEN 4 AND 5) AND (score_f BETWEEN 2 AND 3) THEN 'Potenciais fiéis'
            WHEN score_r = 5 AND score_f = 1 THEN 'Clientes recentes'
            WHEN score_r = 4 AND score_f = 1 THEN 'Promissores'
            WHEN score_r = 3 AND score_f = 3 THEN 'Precisam de atenção'
            WHEN score_r = 3 AND (score_f BETWEEN 1 AND 2) THEN 'Prestes a hibernar'
            WHEN (score_r BETWEEN 1 AND 2) AND (score_f BETWEEN 3 AND 5) THEN 'Em risco'
            WHEN score_r = 1 AND (score_f BETWEEN 4 AND 5) THEN 'Não posso perder'
            WHEN score_r = 2 AND (score_f BETWEEN 1 AND 2) THEN 'Hibernando'
            WHEN score_r = 1 AND (score_f BETWEEN 1 AND 2) THEN 'Perdidos'
            ELSE 'Outros'
        END AS VARCHAR) AS classificacao,
        CAST(CASE
            WHEN score_r = 5 AND (score_f BETWEEN 4 AND 5) THEN 1
            WHEN (score_r BETWEEN 3 AND 4) AND (score_f BETWEEN 4 AND 5) THEN 2
            WHEN (score_r BETWEEN 4 AND 5) AND (score_f BETWEEN 2 AND 3) THEN 3
            WHEN score_r = 5 AND score_f = 1 THEN 4
            WHEN score_r = 4 AND score_f = 1 THEN 5
            WHEN score_r = 3 AND score_f = 3 THEN 6
            WHEN score_r = 3 AND (score_f BETWEEN 1 AND 2) THEN 7
            WHEN (score_r BETWEEN 1 AND 2) AND (score_f BETWEEN 3 AND 5) THEN 8
            WHEN score_r = 1 AND (score_f BETWEEN 4 AND 5) THEN 9
            WHEN score_r = 2 AND (score_f BETWEEN 1 AND 2) THEN 10
            WHEN score_r = 1 AND (score_f BETWEEN 1 AND 2) THEN 11
            ELSE 99
        END AS INTEGER) AS classificacao_sequencia
    FROM ranking
    """
    path_out = (DIR_OURO / 'rfm_snapshot.parquet').as_posix()
    con.execute(f"COPY ({query_rfm}) TO '{path_out}' (FORMAT PARQUET)")

def gerar_rfm_legenda(con):
    dados_legenda = [
        {"classificacao": "Campeões", "descricao": "Compraram recentemente, compram com alta frequência e alto valor.", "recomendacao": "Premiar com exclusividade e fidelizar.", "Sequencia": 1},
        {"classificacao": "Clientes fiéis", "descricao": "Compram com regularidade e respondem bem a promoções.", "recomendacao": "Pedir avaliações e oferecer upsell.", "Sequencia": 2},
        {"classificacao": "Potenciais fiéis", "descricao": "Clientes recentes que já realizaram mais de uma compra.", "recomendacao": "Oferecer programa de fidelidade.", "Sequencia": 3},
        {"classificacao": "Clientes recentes", "descricao": "Realizaram a primeira compra nos últimos dias.", "recomendacao": "Focar em boas-vindas e suporte.", "Sequencia": 4},
        {"classificacao": "Promissores", "descricao": "Compraram recentemente, mas ainda têm baixa frequência.", "recomendacao": "Oferecer cupom para a 2ª compra.", "Sequencia": 5},
        {"classificacao": "Precisam de atenção", "descricao": "Recência e frequência médias. Estão no 'meio do caminho'.", "recomendacao": "Campanhas de lembrete e descontos limitados.", "Sequencia": 6},
        {"classificacao": "Prestes a hibernar", "descricao": "Abaixo da média em frequência e recência. Risco de esquecimento.", "recomendacao": "Enviar conteúdo de valor e novidades.", "Sequencia": 7},
        {"classificacao": "Em risco", "descricao": "Compravam com frequência, mas não retornam há algum tempo.", "recomendacao": 'Mensagens de "Sentimos sua falta" com desconto.', "Sequencia": 8},
        {"classificacao": "Não posso perder", "descricao": "Eram excelentes clientes que pararam de comprar há muito tempo.", "recomendacao": "Ofertas agressivas de reativação imediata.", "Sequencia": 9},
        {"classificacao": "Hibernando", "descricao": "Baixa frequência e última compra antiga. Difíceis de recuperar.", "recomendacao": "Campanhas de reativação de baixo custo.", "Sequencia": 10},
        {"classificacao": "Perdidos", "descricao": "Menor pontuação em recência e frequência.", "recomendacao": "Não investir muito esforço; focar em novos.", "Sequencia": 11},
        {"classificacao": "Outros", "descricao": "Clientes com dados insuficientes ou fora do padrão RFM.", "recomendacao": "Monitorar comportamento futuro.", "Sequencia": 12}
    ]
    df_rfm_legenda = pd.DataFrame(dados_legenda)
    df_rfm_legenda['classificacao'] = df_rfm_legenda['classificacao'].astype('string')
    df_rfm_legenda['descricao'] = df_rfm_legenda['descricao'].astype('string')
    df_rfm_legenda['recomendacao'] = df_rfm_legenda['recomendacao'].astype('string')
    df_rfm_legenda['Sequencia'] = df_rfm_legenda['Sequencia'].astype('int32')
    
    path_out = (DIR_OURO / 'rfm_legenda.parquet').as_posix()
    con.execute(f"COPY (SELECT * FROM df_rfm_legenda) TO '{path_out}' (FORMAT PARQUET)")

def gerar_navegacao(con): 
    query = f"""
    WITH  cte_sessoes AS 
    (SELECT 
        CAST(LAST_DAY(CAST(to_timestamp(event_timestamp / 1000000) AS DATE)  ) AS DATE) as dt_ref, 
        CASE event_name
            WHEN 'session_start'    THEN 'visitas'
            WHEN 'view_item'        THEN 'visualizações'
            WHEN 'add_to_cart'      THEN 'carrinhos'
            WHEN 'begin_checkout'   THEN 'checkouts'
            WHEN 'purchase'         THEN 'compras'
            ELSE '0 - outros'
        END AS etapa_nome,
        CASE event_name
            WHEN 'session_start'    THEN 1
            WHEN 'view_item'        THEN 2
            WHEN 'add_to_cart'      THEN 3
            WHEN 'begin_checkout'   THEN 4
            WHEN 'purchase'         THEN 5
            ELSE '0'
        END AS etapa_numero, 
        MD5(COALESCE(ga_session_id, 'N/A') || '-' || COALESCE(user_pseudo_id, 'anonimo' )) AS id_sessao 
        FROM read_parquet('{PRATA_ANALYTICS}') a 
    WHERE event_name IN ('session_start', 'view_item', 'add_to_cart', 'begin_checkout', 'purchase')
    AND dt_ref BETWEEN '{DATA_INICIO}' AND '{DATA_FIM}'), 
    cte_sessoes_contagem AS
    (SELECT 
        dt_ref, 
        etapa_nome, 
        etapa_numero, 
        count(DISTINCT id_sessao) qt_sessoes
        from cte_sessoes
        GROUP BY ALL 
    )
    SELECT etapa_numero, etapa_nome, dt_ref, qt_sessoes FROM cte_sessoes_contagem order by etapa_numero, dt_ref   
    """ 

    path_out = (DIR_OURO / 'navegacao.parquet').as_posix()
    con.execute(f"COPY ({query}) TO '{path_out}' (FORMAT PARQUET)")

def gerar_funil(con): 
    query_tabela_temp = f"""
    CREATE OR REPLACE TEMP TABLE temp_funil AS
    SELECT         
        CAST(to_timestamp(a.event_timestamp / 1000000) AS DATE) AS dt_ref,

        MD5(
            COALESCE(a.ga_session_id, 'N/A') || '-' ||
            COALESCE(a.user_pseudo_id, 'anonimo')
        ) AS id_sessao,
        CAST(a.user_id AS BIGINT) AS id_cliente,
        CAST(to_timestamp(a.event_timestamp / 1000000) AS DATE) AS data,
        CASE a.event_name
            WHEN 'session_start'    THEN 'visitas'
            WHEN 'view_item'        THEN 'visualizações'
            WHEN 'add_to_cart'      THEN 'carrinhos'
            WHEN 'begin_checkout'   THEN 'checkouts'
            WHEN 'purchase'         THEN 'compras'
            ELSE '0 - outros'
        END AS estagio_nome,
        CASE a.event_name
            WHEN 'session_start'    THEN 1
            WHEN 'view_item'        THEN 2
            WHEN 'add_to_cart'      THEN 3
            WHEN 'begin_checkout'   THEN 4
            WHEN 'purchase'         THEN 5
            ELSE '0'
        END AS estagio_numero 
    FROM read_parquet('{PRATA_ANALYTICS}') a 
    WHERE event_name IN ('session_start', 'view_item', 'add_to_cart', 'begin_checkout', 'purchase')
    AND CAST(to_timestamp(a.event_timestamp / 1000000) AS DATE) BETWEEN '{DATA_INICIO}' AND '{DATA_FIM}'
    """
    con.execute(query_tabela_temp)

    path_out = (DIR_OURO / 'funil.parquet').as_posix()
    con.execute(f"COPY temp_funil TO '{path_out}' (FORMAT PARQUET)")
    con.execute("DROP TABLE temp_funil")

def gerar_funil_conversao(con):
    query_funil_conversao = f"""
    WITH cte_ticket_medio AS (
        SELECT 
            CAST(LAST_DAY(dt_venda) AS DATE) as dt_ref,
            SUM(vlr_receita_liq) / NULLIF(COUNT(DISTINCT id_venda), 0) as ticket_medio
        FROM read_parquet('{OURO_VENDAS}')
        GROUP BY ALL
    ),
    cte_volumes_etapa AS (
        SELECT 
            CAST(LAST_DAY(data) AS DATE) as dt_ref,
            estagio_numero,
            COUNT(DISTINCT id_sessao) as qtd_sessoes
        FROM read_parquet('{OURO_FUNIL}')
        GROUP BY ALL
    ),
    cte_transicoes AS (
        SELECT 
            v1.dt_ref,
            v1.estagio_numero as seq_etapa,
            CASE v1.estagio_numero
                WHEN 1 THEN 'Visita-Visualização'
                WHEN 2 THEN 'Visualização-Carrinho'
                WHEN 3 THEN 'Carrinho-Checkout'
                WHEN 4 THEN 'Checkout-Compra'
            END as nome_etapa,
            v1.qtd_sessoes as vol_origem,
            COALESCE(v2.qtd_sessoes, 0) as conversao_qtd
        FROM cte_volumes_etapa v1
        LEFT JOIN cte_volumes_etapa v2 
            ON v1.dt_ref = v2.dt_ref 
            AND v1.estagio_numero + 1 = v2.estagio_numero
        WHERE v1.estagio_numero < 5
    )
    SELECT 
        t.dt_ref,
        t.seq_etapa,
        t.nome_etapa,
        (t.vol_origem - t.conversao_qtd) as abandono_qtd,
        t.conversao_qtd,
        CAST(
            CASE 
                WHEN t.seq_etapa >= 3 THEN COALESCE((t.vol_origem - t.conversao_qtd) * tm.ticket_medio, 0)
                ELSE 0 
            END 
        AS DECIMAL(18,2)) as vlr_perda_presumida
    FROM cte_transicoes t
    LEFT JOIN cte_ticket_medio tm  ON t.dt_ref = tm.dt_ref
    ORDER BY t.dt_ref, t.seq_etapa
    """
    path_out = (DIR_OURO / 'funil_conversao.parquet').as_posix()
    con.execute(f"COPY ({query_funil_conversao}) TO '{path_out}' (FORMAT PARQUET)")

def gerar_estatisticas(con): 
    query_estatisticas = f"""
    WITH calendario AS (
        SELECT DISTINCT data  
        FROM read_parquet('{OURO_CALENDARIO}')
    ), 
    clientes AS ( 
        SELECT id_cliente, LAST_DAY(dt_cadastro) as dt_ref
        FROM read_parquet('{OURO_CLIENTES}') 
    ),
    clientes_vendas AS ( 
        SELECT distinct id_cliente, LAST_DAY(dt_venda) dt_ref 
        FROM read_parquet('{OURO_VENDAS}')
    ),    
     clientes_novos AS (
        SELECT cli.dt_ref, count(id_cliente) novos
        FROM clientes cli
        JOIN calendario cal ON cal.data = cli.dt_ref
        WHERE cli.dt_ref = cal.data
        GROUP BY cli.dt_ref
    ),
    clientes_recorrentes AS (
        SELECT cli.dt_ref, count(id_cliente) recorrentes
        FROM clientes cli
        JOIN calendario cal ON cal.data = cli.dt_ref
        WHERE cli.dt_ref < cal.data
        GROUP BY cli.dt_ref
    ), 
    clientes_ativos AS (
        SELECT cv.dt_ref, count(cv.id_cliente) ativos
        FROM clientes_vendas cv
        JOIN calendario cal ON cal.data = cv.dt_ref
        GROUP BY cv.dt_ref
    ), 
    vendas AS ( 
        SELECT LAST_DAY(dt_venda) dt_ref, 
        SUM(v.vlr_receita_liq) vendas_receita, 
        SUM(v.vlr_lucro_venda) vendas_lucro
        FROM calendario cal
        LEFT JOIN read_parquet('{OURO_VENDAS}') v ON v.dt_venda = cal.data
        GROUP BY LAST_DAY(v.dt_venda)
    ),
    join_estatisticas as (
        SELECT CAST(cal.data AS DATE) AS dt_ref, 
        CAST(a.ativos AS INTEGER) AS clientes_ativos, 
        CAST(n.novos AS INTEGER) AS clientes_novos, 
        CAST((a.ativos - n.novos) AS INTEGER) AS clientes_recorrentes,
        CAST(v.vendas_receita AS DECIMAL(10,2)) AS vendas_receita,
        CAST(v.vendas_lucro AS DECIMAL(10,2)) AS vendas_lucro
        FROM calendario cal 
        LEFT JOIN clientes_novos n ON n.dt_ref = cal.data 
        LEFT JOIN clientes_ativos a  ON a.dt_ref = cal.data
        LEFT JOIN vendas v ON v.dt_ref = cal.data
    ) 
    SELECT * FROM join_estatisticas
    """
    path_out = (DIR_OURO / 'estatisticas.parquet').as_posix()
    con.execute(f"COPY ({query_estatisticas}) TO '{path_out}' (FORMAT PARQUET)") 

# --- 3. ORQUESTRADOR DO MÓDULO (Compatível com o pipeline.py) ---

def executar_transformacao_ouro() -> dict:
    """
    Executa a transformação da camada Prata para a Ouro (Modelagem Dimensional e Analítica).
    Retorna o log estruturado.
    """
    inicio_geral = datetime.now()
    
    con = duckdb.connect()
    con.execute("PRAGMA memory_limit='4GB'")

    relatorio_tabelas = []
    status_final = "sucesso"

    # A ordem de execução é rígida pois as views e tabelas Ouro dependem umas das outras
    rotinas_transformacao = [
        ("calendario", gerar_calendario,True),
        ("produtos", gerar_produtos,True),
        ("clientes", gerar_clientes,True),
        ("marketing_acoes", gerar_marketing_acoes,True),
        ("marketing_despesas", gerar_marketing_despesas,True),
        ("vendas", gerar_vendas,True),              # Depende de clientes e order_items
        ("rfm_snapshot", gerar_rfm,True),           # Depende de vendas e clientes
        ("rfm_legenda", gerar_rfm_legenda,True),
        ("navegacao", gerar_navegacao,True),
        ("funil", gerar_funil,True),                # Depende de vendas e google analytics
        ("funil_conversao", gerar_funil_conversao,True), # Depende do funil
        ("estatisticas", gerar_estatisticas,True)   # Depende de calendario, clientes e vendas
    ]

    for nome_tabela, funcao_geradora, executar in rotinas_transformacao:
        if executar: 
            inicio_tabela = datetime.now()
            try:
                funcao_geradora(con)
                relatorio_tabelas.append({
                    "tabela": nome_tabela,
                    "status": "sucesso",
                    "tempo_segundos": round((datetime.now() - inicio_tabela).total_seconds(), 2)
                })
            except Exception as e:
                status_final = "erro"
                relatorio_tabelas.append({
                    "tabela": nome_tabela,
                    "status": "erro",
                    "detalhe": str(e)
                })

    con.close()
    fim_geral = datetime.now()

    return {
        "modulo": "cla_omnibox_transformacao",
        "status": status_final,
        "tabelas_processadas": len(relatorio_tabelas),
        "duracao_total_segundos": round((fim_geral - inicio_geral).total_seconds(), 2),
        "detalhes": relatorio_tabelas
    }

if __name__ == "__main__":
    resultado = executar_transformacao_ouro()
    print("\n" + "="*60 + "\nRELATÓRIO FINAL DA CAMADA OURO (MODELAGEM)\n" + "="*60)
    print(json.dumps(resultado, indent=4, ensure_ascii=False))