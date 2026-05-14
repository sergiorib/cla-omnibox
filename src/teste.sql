    WITH 
    -- Calendario base para todas as estatisticas
    calendario AS (
        SELECT DISTINCT LAST_DAY(data) dt_ref   
        --FROM read_parquet('{OURO_CALENDARIO}')
        FROM 
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
    --FROM read_parquet('{OURO_VENDAS}') v 
    FROM vendas.parquet v 
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










-- WITH 
--     -- 1. Cria a espinha dorsal de meses
--     cte_calendario AS (
--         SELECT last_day(range) AS dt_ref
--         FROM range(
--             CAST('2024-01-01' AS DATE),
--             CAST('2025-12-31' AS DATE),
--             INTERVAL '1 month'
--         )
--     ), 
--     -- 2. Cria a base de Clientes x Meses (Foto de quem já era cliente naquele mês)
--     cte_base_clientes AS (
--         SELECT cal.dt_ref, c.id_cliente
--         FROM cte_calendario cal 
--         -- Usa INNER JOIN para garantir que o cliente já existia no mês da foto
--         INNER JOIN clientes.parquet c ON c.dt_cadastro <= cal.dt_ref
--     ),
--     -- 3. Agrega as Vendas por cliente/mes
--     cte_agregacao_vendas AS (
--         SELECT 
--             b.dt_ref,
--             b.id_cliente,
--             MAX(v.dt_venda) AS dt_ref_recencia,
--             COUNT(DISTINCT v.id_venda) AS qtd_compras,
--             COALESCE(SUM(v.vlr_receita_liq), 0) AS vlr_compras,
--             COALESCE(SUM(v.vlr_lucro_venda), 0) AS lucro_compras
--         FROM cte_base_clientes b
--         LEFT JOIN vendas.parquet v 
--             ON v.id_cliente = b.id_cliente 
--             AND v.dt_venda <= b.dt_ref -- Pega todo o histórico até a foto daquele mês
--         GROUP BY 1, 2
--     ),
--     -- 4. Calcula dias sem comprar (Ignora quem nunca comprou)
--     cte_rfm_bruto AS (
--         SELECT 
--             *,
--             DATEDIFF('day', dt_ref_recencia, dt_ref) AS dias_sem_comprar
--         FROM cte_agregacao_vendas
--     ),

--     -- 5. Aplica as notas NTILE (Apenas para quem comprou pelo menos 1 vez)
--     cte_scores AS (
--         SELECT 
--             *,
--             CASE WHEN qtd_compras > 0 THEN NTILE(5) OVER (PARTITION BY dt_ref ORDER BY dias_sem_comprar DESC) ELSE 0 END AS score_r,
--             CASE WHEN qtd_compras > 0 THEN NTILE(5) OVER (PARTITION BY dt_ref ORDER BY qtd_compras ASC) ELSE 0 END AS score_f,
--             CASE WHEN qtd_compras > 0 THEN NTILE(5) OVER (PARTITION BY dt_ref ORDER BY lucro_compras ASC) ELSE 0 END AS score_m
--         FROM cte_rfm_bruto
--     ),

--     -- 6. Define a Classificação
--     cte_classificacao AS (
--         SELECT 
--             *,
--             (score_r + score_f + score_m) AS score_unificado, 
--             CAST(
--                 CASE
--                     WHEN qtd_compras = 0 THEN 'Sem compras' -- Tratamento para quem nunca comprou
--                     WHEN score_r = 5 AND (score_f BETWEEN 4 AND 5) THEN 'Campeões'
--                     WHEN (score_r BETWEEN 3 AND 4) AND (score_f BETWEEN 4 AND 5) THEN 'Clientes fiéis'
--                     WHEN (score_r BETWEEN 4 AND 5) AND (score_f BETWEEN 2 AND 3) THEN 'Potenciais fiéis'
--                     WHEN score_r = 5 AND score_f = 1 THEN 'Clientes recentes'
--                     WHEN score_r = 4 AND score_f = 1 THEN 'Promissores'
--                     WHEN score_r = 3 AND score_f = 3 THEN 'Precisam de atenção'
--                     WHEN score_r = 3 AND (score_f BETWEEN 1 AND 2) THEN 'Prestes a hibernar'
--                     WHEN (score_r BETWEEN 1 AND 2) AND (score_f BETWEEN 3 AND 5) THEN 'Em risco'
--                     WHEN score_r = 1 AND (score_f BETWEEN 4 AND 5) THEN 'Não posso perder'
--                     WHEN score_r = 2 AND (score_f BETWEEN 1 AND 2) THEN 'Hibernando'
--                     WHEN score_r = 1 AND (score_f BETWEEN 1 AND 2) THEN 'Perdidos'
--                     ELSE 'Outros,'
--                 END AS VARCHAR
--             ) AS classificacao
--         FROM cte_scores
--     ), 
--     cte_rfm AS (
--         SELECT 
--         *,
--         CAST(CASE
--             WHEN classificacao = 'Campeões' THEN 1 
--             WHEN classificacao = 'Clientes fiéis' THEN 2 
--             WHEN classificacao = 'Potenciais fiéis' THEN 3 
--             WHEN classificacao = 'Clientes recentes' THEN 4
--             WHEN classificacao = 'Promissores' THEN 5
--             WHEN classificacao = 'Precisam de atenção' THEN 6
--             WHEN classificacao = 'Prestes a hibernar' THEN 7
--             WHEN classificacao = 'Em risco' THEN 8
--             WHEN classificacao = 'Não posso perder' THEN 9
--             WHEN classificacao = 'Hibernando' THEN 10
--             WHEN classificacao = 'Perdidos' THEN 11
--             WHEN classificacao = 'Apenas Cadastro' THEN 12
--             ELSE 99
--         END AS INTEGER) AS ordem_classificacao
--     FROM cte_classificacao
--     )
-- SELECT * FROM cte_rfm WHERE id_cliente = 17010 order by dt_ref


