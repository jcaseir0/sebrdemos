-- Consulta para saber quais clientes mais utilizam o cartão de crédito
SELECT
    C.ID_USUARIO                  AS CLIENTE_ID,
    COUNT (DISTINCT T.ID_USUARIO) AS TRANSACOES_COUNT
FROM
    BANCODEMO.CLIENTES          C
    LEFT JOIN BANCODEMO.TRANSACOES_CARTAO T
    ON C.ID_USUARIO = T.ID_USUARIO
GROUP BY
    C.ID_USUARIO
ORDER BY
    TRANSACOES_COUNT DESC
 -- 1. Total de gastos por cliente
    SELECT
        C.ID_USUARIO,
        C.NOME,
        SUM(T.VALOR) AS TOTAL_GASTOS
    FROM
        BANCODEMO.CLIENTES          C
        JOIN BANCODEMO.TRANSACOES_CARTAO T
        ON C.ID_USUARIO = T.ID_USUARIO
    GROUP BY
        C.ID_USUARIO,
        C.NOME
    ORDER BY
        TOTAL_GASTOS DESC;

-- 2. Número de transações por cliente
SELECT
    C.ID_USUARIO,
    C.NOME,
    COUNT(T.ID_USUARIO) AS TOTAL_TRANSACOES
FROM
    BANCODEMO.CLIENTES          C
    JOIN BANCODEMO.TRANSACOES_CARTAO T
    ON C.ID_USUARIO = T.ID_USUARIO
GROUP BY
    C.ID_USUARIO,
    C.NOME
ORDER BY
    TOTAL_TRANSACOES DESC;

-- 3. Média de gastos por transação para cada cliente
SELECT
    C.ID_USUARIO,
    C.NOME,
    AVG(T.VALOR) AS MEDIA_GASTOS
FROM
    BANCODEMO.CLIENTES          C
    JOIN BANCODEMO.TRANSACOES_CARTAO T
    ON C.ID_USUARIO = T.ID_USUARIO
GROUP BY
    C.ID_USUARIO,
    C.NOME
ORDER BY
    MEDIA_GASTOS DESC;

-- 4. Clientes com maior valor de transação única
SELECT
    C.ID_USUARIO,
    C.NOME,
    MAX(T.VALOR) AS MAIOR_TRANSACAO
FROM
    BANCODEMO.CLIENTES C
    JOIN BANCODEMO.TRANSACOES_CARTAO T
    ON C.ID_USUARIO = T.ID_USUARIO
GROUP BY
    C.ID_USUARIO,
    C.NOME
ORDER BY
    MAIOR_TRANSACAO DESC LIMIT 10;

-- 5. Total de gastos por categoria
SELECT
    T.CATEGORIA,
    SUM(T.VALOR) AS TOTAL_GASTOS
FROM
    BANCODEMO.TRANSACOES_CARTAO T
GROUP BY
    T.CATEGORIA
ORDER BY
    TOTAL_GASTOS DESC;

-- 6. Número de transações por status
SELECT
    T.STATUS,
    COUNT(*) AS TOTAL_TRANSACOES
FROM
    BANCODEMO.TRANSACOES_CARTAO T
GROUP BY
    T.STATUS
ORDER BY
    TOTAL_TRANSACOES DESC;

-- 7. Clientes com transações na categoria "Alimentação"
SELECT
    DISTINCT C.ID_USUARIO,
    C.NOME
FROM
    BANCODEMO.CLIENTES          C
    JOIN BANCODEMO.TRANSACOES_CARTAO T
    ON C.ID_USUARIO = T.ID_USUARIO
WHERE
    T.CATEGORIA = 'Alimentação';

-- 8. Total de gastos por cliente na categoria "Transporte"
SELECT
    C.ID_USUARIO,
    C.NOME,
    SUM(T.VALOR) AS TOTAL_GASTOS_TRANSPORTE
FROM
    BANCODEMO.CLIENTES          C
    JOIN BANCODEMO.TRANSACOES_CARTAO T
    ON C.ID_USUARIO = T.ID_USUARIO
WHERE
    T.CATEGORIA = 'Transporte'
GROUP BY
    C.ID_USUARIO,
    C.NOME
ORDER BY
    TOTAL_GASTOS_TRANSPORTE DESC;

-- 9. Clientes com transações negadas
SELECT
    DISTINCT C.ID_USUARIO,
    C.NOME
FROM
    BANCODEMO.CLIENTES          C
    JOIN BANCODEMO.TRANSACOES_CARTAO T
    ON C.ID_USUARIO = T.ID_USUARIO
WHERE
    T.STATUS = 'Negada';

-- 10. Total de gastos mensais por cliente
SELECT
    C.ID_USUARIO,
    C.NOME,
    DATE_FORMAT(T.DATA_TRANSACAO, '%Y-%m') AS MES,
    SUM(T.VALOR)                           AS TOTAL_GASTOS
FROM
    BANCODEMO.CLIENTES          C
    JOIN BANCODEMO.TRANSACOES_CARTAO T
    ON C.ID_USUARIO = T.ID_USUARIO
GROUP BY
    C.ID_USUARIO,
    C.NOME,
    MES
ORDER BY
    C.ID_USUARIO,
    MES;

-- 11. Análise de gastos por cliente e categoria, com ranking e percentual, filtrando por categoria
SELECT
    C.ID_USUARIO,
    C.NOME,
    T.CATEGORIA,
    SUM(T.VALOR)                                                      AS TOTAL_GASTOS,
    RANK() OVER (PARTITION BY T.CATEGORIA ORDER BY SUM(T.VALOR) DESC) AS RANKING_CATEGORIA,
    SUM(T.VALOR) / SUM(SUM(T.VALOR)) OVER (PARTITION BY C.ID_USUARIO) AS PERCENTUAL_GASTOS
FROM
    BANCODEMO.CLIENTES          C
    JOIN BANCODEMO.TRANSACOES_CARTAO T
    ON C.ID_USUARIO = T.ID_USUARIO
WHERE
    T.CATEGORIA = 'Alimentação'
GROUP BY
    C.ID_USUARIO,
    C.NOME,
    T.CATEGORIA;

-- 12. Detecção de padrões de gastos anômalos
WITH CLIENTE_STATS AS (
    SELECT
        ID_USUARIO,
        AVG(VALOR)    AS MEDIA_GASTO,
        STDDEV(VALOR) AS DESVIO_PADRAO_GASTO
    FROM
        BANCODEMO.TRANSACOES_CARTAO
    GROUP BY
        ID_USUARIO
)
SELECT
    C.ID_USUARIO,
    C.NOME,
    T.DATA_TRANSACAO,
    T.VALOR,
    T.CATEGORIA
FROM
    BANCODEMO.CLIENTES          C
    JOIN BANCODEMO.TRANSACOES_CARTAO T
    ON C.ID_USUARIO = T.ID_USUARIO
    JOIN CLIENTE_STATS CS
    ON C.ID_USUARIO = CS.ID_USUARIO
WHERE
    T.VALOR > CS.MEDIA_GASTO + (3 * CS.DESVIO_PADRAO_GASTO);

-- 13. Análise de tendências de gastos ao longo do tempo
SELECT 
  DATE_TRUNC('month', t.data_transacao) AS mes,
  t.categoria,
  COUNT(*) AS num_transacoes,
  SUM(t.valor) AS total_gastos,
  AVG(t.valor) AS media_gasto,
  LEAD(AVG(t.valor)) OVER (PARTITION BY t.categoria ORDER BY DATE_TRUNC('month', t.data_transacao)) AS media_gasto_proximo_mes,
  (LEAD(AVG(t.valor)) OVER (PARTITION BY t.categoria ORDER BY DATE_TRUNC('month', t.data_transacao)) - AVG(t.valor)) / AVG(t.valor) * 100 AS variacao_percentual
FROM 
  bancodemo.transacoes_cartao t
GROUP BY 
  DATE_TRUNC('month', t.data_transacao), t.categoria
ORDER BY 
  mes, t.categoria;

-- 14. Segmentação de clientes com base em padrões de gastos
WITH cliente_metricas AS (
  SELECT 
    c.id_usuario,
    c.nome,
    COUNT(DISTINCT DATE_TRUNC('month', t.data_transacao)) AS meses_ativos,
    SUM(t.valor) AS total_gastos,
    AVG(t.valor) AS media_gasto_por_transacao,
    COUNT(*) AS num_transacoes
  FROM 
    bancodemo.clientes c
  JOIN 
    bancodemo.transacoes_cartao t ON c.id_usuario = t.id_usuario
  GROUP BY 
    c.id_usuario, c.nome
)
SELECT 
  id_usuario,
  nome,
  CASE 
    WHEN meses_ativos >= 10 AND total_gastos > 50000 THEN 'VIP'
    WHEN meses_ativos >= 6 AND total_gastos > 25000 THEN 'Regular'
    WHEN meses_ativos >= 3 AND total_gastos > 10000 THEN 'Ocasional'
    ELSE 'Inativo'
  END AS segmento_cliente,
  meses_ativos,
  total_gastos,
  media_gasto_por_transacao,
  num_transacoes
FROM 
  cliente_metricas
ORDER BY 
  total_gastos DESC;

-- 15. Análise de correlação entre limite de crédito e gastos
WITH gastos_cliente AS (
  SELECT 
    id_usuario,
    SUM(valor) AS total_gastos,
    COUNT(*) AS num_transacoes
  FROM 
    bancodemo.transacoes_cartao
  GROUP BY 
    id_usuario
)
SELECT 
  c.id_usuario,
  c.nome,
  c.limite_credito,
  gc.total_gastos,
  gc.num_transacoes,
  (gc.total_gastos / c.limite_credito) * 100 AS percentual_limite_utilizado,
  CORR(c.limite_credito, gc.total_gastos) OVER () AS correlacao_limite_gastos
FROM 
  bancodemo.clientes c
JOIN 
  gastos_cliente gc ON c.id_usuario = gc.id_usuario;
