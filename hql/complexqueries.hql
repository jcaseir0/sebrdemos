-- 1. Análise de gastos por cliente e categoria, com ranking
WITH gastos_agregados AS (
  SELECT 
    t.id_usuario,
    t.categoria,
    SUM(t.valor) AS total_gastos
  FROM bancodemo.transacoes_cartao t
  GROUP BY t.id_usuario, t.categoria
)
SELECT 
  c.id_usuario,
  c.nome,
  g.categoria,
  g.total_gastos,
  RANK() OVER (PARTITION BY g.categoria ORDER BY g.total_gastos DESC) AS ranking_categoria
FROM gastos_agregados g
JOIN bancodemo.clientes c ON g.id_usuario = c.id_usuario;

-- 2. Detecção de padrões de gastos anômalos
WITH cliente_stats AS (
  SELECT 
    id_usuario,
    AVG(valor) AS media_gasto,
    STDDEV_POP(valor) AS desvio_padrao_gasto
  FROM bancodemo.transacoes_cartao
  GROUP BY id_usuario
)
SELECT 
  t.*,
  c.nome,
  s.media_gasto,
  s.desvio_padrao_gasto
FROM bancodemo.transacoes_cartao t
JOIN cliente_stats s ON t.id_usuario = s.id_usuario
JOIN bancodemo.clientes c ON t.id_usuario = c.id_usuario
WHERE t.valor > (s.media_gasto + (3 * s.desvio_padrao_gasto));

-- 3. Análise de tendências de gastos ao longo do tempo
SELECT 
  DATE_FORMAT(data_transacao, 'yyyy-MM') AS mes,
  categoria,
  COUNT(*) AS num_transacoes,
  SUM(valor) AS total_gastos,
  AVG(valor) AS media_gasto,
  LEAD(AVG(valor), 1) OVER (PARTITION BY categoria ORDER BY DATE_FORMAT(data_transacao, 'yyyy-MM')) AS media_gasto_proximo_mes,
  ((LEAD(AVG(valor), 1) OVER (PARTITION BY categoria ORDER BY DATE_FORMAT(data_transacao, 'yyyy-MM')) - AVG(valor)) / AVG(valor)) * 100 AS variacao_percentual
FROM bancodemo.transacoes_cartao
GROUP BY DATE_FORMAT(data_transacao, 'yyyy-MM'), categoria;

-- 4. Segmentação de clientes
WITH cliente_metricas AS (
  SELECT 
    t.id_usuario,
    COUNT(DISTINCT DATE_FORMAT(t.data_transacao, 'yyyy-MM')) AS meses_ativos,
    SUM(t.valor) AS total_gastos,
    AVG(t.valor) AS media_gasto_por_transacao,
    COUNT(*) AS num_transacoes
  FROM bancodemo.transacoes_cartao t
  GROUP BY t.id_usuario
)
SELECT 
  c.*,
  m.meses_ativos,
  m.total_gastos,
  m.media_gasto_por_transacao,
  m.num_transacoes,
  CASE
    WHEN m.meses_ativos >= 10 AND m.total_gastos > 50000 THEN 'VIP'
    WHEN m.meses_ativos >= 6 AND m.total_gastos > 25000 THEN 'Regular'
    WHEN m.meses_ativos >= 3 AND m.total_gastos > 10000 THEN 'Ocasional'
    ELSE 'Inativo'
  END AS segmento_cliente
FROM cliente_metricas m
JOIN bancodemo.clientes c ON m.id_usuario = c.id_usuario;

-- 5. Análise de correlação entre limite de crédito e gastos
WITH dados_correlacao AS (
  SELECT 
    c.id_usuario,
    c.limite_credito,
    SUM(t.valor) AS total_gastos
  FROM bancodemo.clientes c
  JOIN bancodemo.transacoes_cartao t ON c.id_usuario = t.id_usuario
  GROUP BY c.id_usuario, c.limite_credito
)
SELECT 
  (COUNT(*)*SUM(limite_credito*total_gastos) - SUM(limite_credito)*SUM(total_gastos)) /
  (SQRT((COUNT(*)*SUM(limite_credito*limite_credito) - POW(SUM(limite_credito),2)) * 
        (COUNT(*)*SUM(total_gastos*total_gastos) - POW(SUM(total_gastos),2)))) AS correlacao
FROM dados_correlacao;

-- 