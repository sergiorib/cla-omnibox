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
                CAST(year(range) AS INTEGER) as ano, 
                CAST(strftime(range, '%Y%m') AS INTEGER) as ano_mes,
                CAST(LAST_DAY(range) AS DATE) as mes_ano
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
            (SELECT cp.campaign_id from read_parquet('{PRATA_MKT_CAMPAIGNS}') cp WHERE cp.code = a.campaign)  as campaign_id,  
            CAST(source AS VARCHAR) AS source, 
            CAST(medium AS VARCHAR) AS medium
        FROM read_parquet('{PRATA_ANALYTICS}')  a 
        WHERE user_id IS NOT NULL
    ),
    primeira_sessao AS (
        SELECT 
            id_cliente,
            source,
            medium,  
            (select action_id from read_parquet('{PRATA_MKT_ACTIONS}') a WHERE a.campaign_id = s.campaign_id AND  s.medium = a.medium AND s.source = a.source) as  action_id, 
            ROW_NUMBER() OVER (PARTITION BY id_cliente ORDER BY ga_session_id ASC) as rank
        FROM sessoes_unicas s 
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
        CAST(COALESCE(ps.action_id,Null) AS VARCHAR) AS id_acao,
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
        CAST(c.id_acao AS INTEGER) AS id_acao,
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

def gerar_estatisticas_marketing(con): 
    query_marketing_despesas = f"""
    WITH calendario AS (
    SELECT dt_ref, id_acao FROM (SELECT DISTINCT LAST_DAY(data) dt_ref FROM read_parquet('{OURO_CALENDARIO}')), 
    (SELECT distinct action_id id_acao from read_parquet('{PRATA_MKT_ACTIONS}'))
    ), 
    mkt_vendas AS (
        -- Busca valores das vendas dos primeiros 6 meses e cujo canal teve investimento de marketing
        SELECT 
            CAST(LAST_DAY(v.dt_venda) AS DATE) AS dt_ref, 
            CAST(v.id_acao AS INTEGER) AS id_acao, 
            SUM(vlr_receita_liq) as vlr_faturamento, 
            SUM(vlr_lucro_venda) as vlr_lucro
        FROM read_parquet('{OURO_VENDAS}') as v    
        LEFT JOIN read_parquet('{PRATA_MKT_ACTIONS}') as a on a.action_id = v.id_acao 
        WHERE 
                v.tempo_decorrido_prim_compra <= 180
                AND NOT v.id_acao IS NULL 
                AND v.id_acao in (SELECT id_acao FROM read_parquet('{PRATA_MKT_SPEND}') s WHERE s.spend_amount > 0) 
        GROUP BY dt_ref, id_acao
    ), 
    mkt_gastos AS (
        SELECT 
            CAST(LAST_DAY(mkt_spend.date) AS DATE) AS dt_ref,         
            CAST(mkt_act.action_id AS INTEGER) as id_acao, 
            CAST(SUM(mkt_spend.spend_amount) AS DECIMAL(10,2)) AS vlr_gasto, 
            CAST(SUM(mkt_spend.clicks) AS BIGINT) AS cliques, 
            CAST(SUM(mkt_spend.impressions) AS BIGINT) AS impressoes 
        FROM read_parquet('{PRATA_MKT_SPEND}') mkt_spend
        JOIN read_parquet('{PRATA_MKT_ACTIONS}') mkt_act ON mkt_spend.action_id = mkt_act.action_id
        GROUP BY dt_ref, id_acao
    )
    SELECT  
        cal.dt_ref, 
        cal.id_acao, 
        v.vlr_faturamento , 
        v.vlr_lucro , 
        g.vlr_gasto, 
        g.cliques, 
        g.impressoes, 
        -- Cálculo de Payback em Meses (Base: 6 meses)
        CASE 
            WHEN v.vlr_lucro IS NULL OR v.vlr_lucro <= 0 THEN NULL 
            ELSE ROUND(6.0 * g.vlr_gasto / v.vlr_lucro, 2) 
        END AS mkt_payback_meses,
        
        -- Cálculo de Payback em Dias (Base: 180 dias)
        CASE 
            WHEN v.vlr_lucro IS NULL OR v.vlr_lucro <= 0 THEN NULL 
            ELSE CAST(ROUND(180.0 * g.vlr_gasto / v.vlr_lucro, 0) AS INTEGER) 
        END AS mkt_payback_dias
    FROM calendario cal
        LEFT JOIN mkt_vendas v ON v.dt_ref = cal.dt_ref AND v.id_acao = cal.id_acao
        LEFT JOIN mkt_gastos g ON g.dt_ref = cal.dt_ref AND g.id_acao = cal.id_acao
    """
    path_out = (DIR_OURO / 'estatisticas_marketing.parquet').as_posix()
    con.execute(f"COPY ({query_marketing_despesas}) TO '{path_out}' (FORMAT PARQUET)") 

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
    WITH 
        cte_calendario AS (
            SELECT last_day(range) AS dt_ref
            FROM range(
                CAST('{DATA_INICIO}' AS DATE),
                CAST('{DATA_FIM}' AS DATE),
                INTERVAL '1 month'
            )
        ), 
        cte_base_clientes AS (
        SELECT cal.dt_ref, c.id_cliente, 
            FROM cte_calendario cal
            INNER JOIN '{OURO_CLIENTES}' c ON c.dt_cadastro <= cal.dt_ref
        ),
        cte_agregacao_vendas AS (
            SELECT 
                b.dt_ref,
                b.id_cliente,
                MAX(v.dt_venda) AS dt_ref_recencia,
                COUNT(DISTINCT v.id_venda) AS qtd_compras,
                COALESCE(SUM(v.vlr_receita_liq), 0) AS vlr_compras,
                COALESCE(SUM(v.vlr_lucro_venda), 0) AS lucro_compras
            FROM cte_base_clientes b
            LEFT JOIN '{OURO_VENDAS}' v 
                ON v.id_cliente = b.id_cliente 
                AND v.dt_venda <= b.dt_ref -- Pega todo o histórico até a foto daquele mês
            GROUP BY 1, 2
        ),
        cte_rfm_bruto AS (
            SELECT 
                *,
                DATEDIFF('day', dt_ref_recencia, dt_ref) AS dias_sem_comprar
            FROM cte_agregacao_vendas
        ),
        -- 5. Aplica as notas NTILE (Apenas para quem comprou pelo menos 1 vez)
        cte_scores AS (
            SELECT 
                *,
                CASE WHEN qtd_compras > 0 THEN NTILE(5) OVER (PARTITION BY dt_ref ORDER BY dias_sem_comprar DESC) ELSE 0 END AS score_r,
                CASE WHEN qtd_compras > 0 THEN NTILE(5) OVER (PARTITION BY dt_ref ORDER BY qtd_compras ASC) ELSE 0 END AS score_f,
                CASE WHEN qtd_compras > 0 THEN NTILE(5) OVER (PARTITION BY dt_ref ORDER BY lucro_compras ASC) ELSE 0 END AS score_m
            FROM cte_rfm_bruto
        ),
        cte_classificacao AS (
            SELECT 
                *,
                (score_r + score_f + score_m) AS score_unificado, 
                CAST(
                    CASE
                        WHEN qtd_compras = 0 THEN 'Outros' -- Tratamento para quem nunca comprou
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
                        ELSE 'Outros,'
                    END AS VARCHAR
                ) AS classificacao
            FROM cte_scores
        ), 
        cte_rfm AS (
            SELECT 
            *,
            CAST(CASE
                WHEN classificacao = 'Campeões' THEN 1 
                WHEN classificacao = 'Clientes fiéis' THEN 2 
                WHEN classificacao = 'Potenciais fiéis' THEN 3 
                WHEN classificacao = 'Clientes recentes' THEN 4
                WHEN classificacao = 'Promissores' THEN 5
                WHEN classificacao = 'Precisam de atenção' THEN 6
                WHEN classificacao = 'Prestes a hibernar' THEN 7
                WHEN classificacao = 'Em risco' THEN 8
                WHEN classificacao = 'Não posso perder' THEN 9
                WHEN classificacao = 'Hibernando' THEN 10
                WHEN classificacao = 'Perdidos' THEN 11
                WHEN classificacao = 'Outros' THEN 12
                ELSE 99
            END AS INTEGER) AS ordem_classificacao
        FROM cte_classificacao
    )
    SELECT * FROM cte_rfm
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
        WITH cte_sessoes AS (
            SELECT 
                CAST(LAST_DAY(CAST(to_timestamp(event_timestamp / 1000000) AS DATE)) AS DATE) as dt_ref, 
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
                    ELSE 0
                END AS etapa_numero, 
                MD5(COALESCE(ga_session_id, 'N/A') || '-' || COALESCE(user_pseudo_id, 'anonimo' )) AS id_sessao 
            FROM read_parquet('{PRATA_ANALYTICS}') a 
            WHERE event_name IN ('session_start', 'view_item', 'add_to_cart', 'begin_checkout', 'purchase')
            AND dt_ref BETWEEN '{DATA_INICIO}' AND '{DATA_FIM}'
        ), 
        cte_sessoes_contagem AS (
            SELECT 
                dt_ref, 
                etapa_nome, 
                etapa_numero, 
                count(DISTINCT id_sessao) AS qt_sessoes
            FROM cte_sessoes
            GROUP BY ALL 
        ),
        cte_funil AS (
            SELECT 
                dt_ref,
                etapa_numero,
                etapa_nome,
                qt_sessoes,
                -- A função LAG busca o valor da linha anterior ordenada pela etapa, reiniciando o cálculo a cada 'dt_ref'
                LAG(qt_sessoes) OVER (PARTITION BY dt_ref ORDER BY etapa_numero) AS sessoes_anteriores
            FROM cte_sessoes_contagem
        )
        SELECT 
            dt_ref,
            etapa_numero, 
            etapa_nome, 
            qt_sessoes,
            -- Se for a Etapa 1, não há etapa anterior, então repetimos o valor atual para não ficar NULL
            COALESCE(sessoes_anteriores, qt_sessoes) AS qtd_sessoes_etapa_anterior,
            -- Abandono = Sessões da Etapa Anterior menos Sessões da Etapa Atual (Retorna 0 se for a Etapa 1)
            COALESCE(sessoes_anteriores - qt_sessoes, 0) AS qtd_abandonos
        FROM cte_funil 
        ORDER BY dt_ref, etapa_numero   
    """

    path_out = (DIR_OURO / 'navegacao.parquet').as_posix()
    con.execute(f"COPY ({query}) TO '{path_out}' (FORMAT PARQUET)")


def gerar_estatisticas(con): 
    query_estatisticas = f"""
    WITH 
    -- Calendario base para todas as estatisticas
    calendario AS (
        SELECT DISTINCT LAST_DAY(data) dt_ref   
        FROM read_parquet('{OURO_CALENDARIO}')
    ), 
    -- Estatisticas de clientes
    clientes_novos AS (
        SELECT LAST_DAY(dt_cadastro) dt_ref, count(distinct id_cliente) clientes_novos
        FROM read_parquet('{OURO_CLIENTES}')
        GROUP BY LAST_DAY(dt_cadastro)
    ),
    clientes_vendas AS ( 
        SELECT distinct id_cliente, dt_venda FROM read_parquet('{OURO_VENDAS}')
    ),
    clientes_ativos AS (
        SELECT LAST_DAY(cv.dt_venda) dt_ref, count(distinct cv.id_cliente) clientes_ativos
        FROM clientes_vendas cv
        GROUP BY LAST_DAY(cv.dt_venda) 
    ),
    estatisticas_cliente as (
        SELECT cal.dt_ref, n.clientes_novos, a.clientes_ativos, (a.clientes_ativos - n.clientes_novos) clientes_recorrentes
        FROM calendario cal
        LEFT JOIN clientes_novos n  ON n.dt_ref = cal.dt_ref
        LEFT JOIN clientes_ativos a ON a.dt_ref = cal.dt_ref 
    ),
    -- Estatisticas de vendas
    estatisticas_vendas AS ( 
    SELECT LAST_DAY(dt_venda) dt_ref, 
    SUM(v.vlr_receita_liq) vendas_receita, 
    SUM(v.vlr_lucro_venda) vendas_lucro
    FROM read_parquet('{OURO_VENDAS}') v 
    GROUP BY LAST_DAY(v.dt_venda) 
    ), 
    -- Unificação das estatisticas mensais 
    estatisticas_mensais  as (
        SELECT cal.dt_ref, 
        ec.clientes_novos, ec.clientes_ativos, ec.clientes_recorrentes,
        ev.vendas_receita,  ev.vendas_lucro 
        FROM calendario cal
        LEFT JOIN estatisticas_cliente ec  ON ec.dt_ref = cal.dt_ref
        LEFT JOIN estatisticas_vendas ev  ON ev.dt_ref = cal.dt_ref
    )
    SELECT * FROM estatisticas_mensais order by dt_ref
    """
    path_out = (DIR_OURO / 'estatisticas_mensais.parquet').as_posix()
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
        ("calendario", gerar_calendario,True ),
        ("produtos", gerar_produtos,True),
        ("clientes", gerar_clientes,True ),
        ("marketing_acoes", gerar_marketing_acoes,True),
        ("vendas", gerar_vendas,True ),              # Depende de clientes e order_items        
        ("estatisticas_marketing", gerar_estatisticas_marketing,True ),
        ("rfm_snapshot", gerar_rfm,True),           # Depende de vendas e clientes
        ("rfm_legenda", gerar_rfm_legenda,True),
        ("navegacao", gerar_navegacao,True),
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