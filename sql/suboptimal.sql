-- 1. Aggregation Spilled Partitions
SELECT 
  c.id_usuario,
  c.nome,
  c.email,
  c.data_nascimento,
  c.endereco,
  c.limite_credito,
  c.numero_cartao,
  c.id_uf,
  COUNT(t.id_usuario) AS total_transacoes,
  SUM(t.valor) AS valor_total_transacoes,
  AVG(t.valor) AS valor_medio_transacoes,
  MIN(t.data_transacao) AS primeira_transacao,
  MAX(t.data_transacao) AS ultima_transacao,
  COUNT(DISTINCT t.estabelecimento) AS estabelecimentos_unicos,
  COUNT(DISTINCT t.categoria) AS categorias_unicas,
  SUM(CASE WHEN t.status = 'Aprovada' THEN 1 ELSE 0 END) AS transacoes_aprovadas,
  SUM(CASE WHEN t.status = 'Negada' THEN 1 ELSE 0 END) AS transacoes_negadas,
  SUM(CASE WHEN t.status = 'Pendente' THEN 1 ELSE 0 END) AS transacoes_pendentes
FROM 
  bancodemo.clientes c
LEFT JOIN 
  bancodemo.transacoes_cartao t ON c.id_usuario = t.id_usuario
GROUP BY 
  c.id_usuario, c.nome, c.email, c.data_nascimento, c.endereco, c.limite_credito, c.numero_cartao, c.id_uf
ORDER BY 
  valor_total_transacoes DESC

-- 2. Bytes Read Skew
SELECT 
  t.id_usuario,
  c.nome,
  c.id_uf,
  t.categoria,
  t.estabelecimento,
  COUNT(*) AS num_transacoes,
  SUM(t.valor) AS valor_total,
  AVG(t.valor) AS valor_medio
FROM 
  bancodemo.transacoes_cartao t
JOIN 
  bancodemo.clientes c ON t.id_usuario = c.id_usuario
WHERE 
  t.data_transacao BETWEEN '2023-01-01' AND '2023-12-31'
  AND c.id_uf IN ('SP', 'RJ', 'MG')  -- Estados com provável maior volume de dados
  AND t.valor > 1000  -- Filtragem adicional para potencializar o skew
GROUP BY 
  t.id_usuario, c.nome, c.id_uf, t.categoria, t.estabelecimento
HAVING 
  COUNT(*) > 10  -- Filtragem adicional após a agregação
ORDER BY 
  valor_total DESC
LIMIT 1000

-- Criar uma nova tabela com distribuição desigual de dados
CREATE TABLE bancodemo.skewed_transacoes STORED AS PARQUET AS
SELECT 
  c.id_usuario,
  c.nome,
  c.id_uf,
  t.valor,
  t.data_transacao,
  t.categoria,
  t.status
FROM 
  bancodemo.clientes c
JOIN 
  bancodemo.transacoes_cartao t 
ON 
  c.id_usuario = t.id_usuario;

-- Carregar a tabela recém criada:
INSERT INTO bancodemo.skewed_transacoes 
SELECT 
  id_usuario,
  nome,
  CASE 
    WHEN RAND() < 0.7 THEN 'SP' -- Skewing most of the data to 'SP'
    WHEN RAND() < 0.9 THEN 'RJ' -- Skewing some data to 'RJ'
    ELSE id_uf -- Distributing the rest evenly
  END AS id_uf,
  valor,
  data_transacao,
  categoria,
  status
FROM 
  bancodemo.skewed_transacoes;

-- Consulta que pode gerar Bytes Read Skew
SELECT 
  id_uf, 
  COUNT(*) AS total_transacoes, 
  SUM(valor) AS total_valor, 
  AVG(valor) AS media_valor
FROM 
  bancodemo.skewed_transacoes
GROUP BY 
  id_uf
ORDER BY 
  total_transacoes DESC;

-- Corrupt Table Statistics
-- Criar e preencher a tabela
CREATE TABLE bancodemo.corrupttblstats AS
SELECT * FROM bancodemo.transacoes_cartao
WHERE data_transacao BETWEEN '2024-01-01' AND '2024-12-31';

-- Computar estatísticas iniciais
COMPUTE STATS bancodemo.corrupttblstats;

-- Modificar significativamente os dados sem atualizar as estatísticas
INSERT OVERWRITE TABLE bancodemo.corrupttblstats
SELECT 
  id_usuario,
  data_transacao,
  valor * 1000, -- Multiplicar o valor por 1000
  estabelecimento,
  categoria,
  CASE 
    WHEN status = 'Aprovada' THEN 'Negada'
    WHEN status = 'Negada' THEN 'Aprovada'
    ELSE status
  END AS status,
  data_execucao
FROM bancodemo.bancodemo.corrupttblstats;

-- Consulta que usará as estatísticas corrompidas
SELECT 
  categoria,
  AVG(valor) as valor_medio,
  COUNT(*) as total_transacoes
FROM bancodemo.bancodemo.corrupttblstats
GROUP BY categoria
ORDER BY valor_medio DESC;

-- Duration Skew
SELECT 
  c.id_uf,
  t.categoria,
  TRUNC(t.data_transacao, 'MM') AS mes,
  COUNT(*) AS num_transacoes,
  SUM(t.valor) AS valor_total,
  AVG(t.valor) AS valor_medio,
  COUNT(DISTINCT c.id_usuario) AS num_clientes_unicos,
  MAX(CASE WHEN t.valor > c.limite_credito THEN 'Acima do Limite' ELSE 'Dentro do Limite' END) AS status_limite
FROM 
  bancodemo.clientes c
JOIN 
  bancodemo.transacoes_cartao t ON c.id_usuario = t.id_usuario
WHERE 
  t.data_transacao BETWEEN '2025-01-01' AND '2025-12-31'
GROUP BY 
  c.id_uf, t.categoria, TRUNC(t.data_transacao, 'MM')
HAVING 
  COUNT(*) > 100
ORDER BY 
  valor_total DESC, num_clientes_unicos DESC
LIMIT 1000;

-- HashJoin Spilled Partitions
SELECT 
  c.id_usuario,
  c.nome,
  c.email,
  t.categoria,
  COUNT(*) AS num_transacoes,
  SUM(t.valor) AS valor_total,
  AVG(t.valor) AS valor_medio
FROM 
  bancodemo.clientes c
JOIN 
  bancodemo.transacoes_cartao t ON c.id_usuario = t.id_usuario
WHERE 
  t.data_transacao BETWEEN '2024-01-01' AND '2025-01-28'
GROUP BY 
  c.id_usuario, c.nome, c.email, t.categoria
HAVING 
  COUNT(*) > 100
ORDER BY 
  valor_total DESC
LIMIT 10000

-- Insufficient Partitioning
SELECT 
  c.id_usuario,
  c.nome,
  t.data_transacao,
  t.valor,
  t.categoria
FROM 
  bancodemo.clientes c
JOIN 
  bancodemo.transacoes_cartao t ON c.id_usuario = t.id_usuario
WHERE 
  t.data_transacao BETWEEN '2024-01-01' AND '2025-01-28'
  AND t.valor > 1000
ORDER BY 
  t.data_transacao, t.valor DESC

-- Many Materialized Columns
SELECT 
  c.id_usuario,
  c.nome,
  c.email,
  c.data_nascimento,
  c.endereco,
  c.limite_credito,
  c.numero_cartao,
  c.id_uf,
  t.data_transacao,
  t.valor,
  t.estabelecimento,
  t.categoria,
  t.status,
  YEAR(t.data_transacao) AS ano_transacao,
  MONTH(t.data_transacao) AS mes_transacao,
  DAY(t.data_transacao) AS dia_transacao,
  CASE WHEN t.valor > c.limite_credito THEN 'Acima do Limite' ELSE 'Dentro do Limite' END AS status_limite,
  CONCAT(c.nome, ' - ', c.id_uf) AS cliente_estado,
  ROUND(t.valor / c.limite_credito * 100, 2) AS percentual_limite_usado,
  DATEDIFF(CURRENT_DATE(), c.data_nascimento) / 365 AS idade_aproximada
FROM 
  bancodemo.clientes c
JOIN 
  bancodemo.transacoes_cartao t ON c.id_usuario = t.id_usuario
WHERE 
  t.data_transacao BETWEEN '2024-01-01' AND '2025-01-28'
ORDER BY 
  t.data_transacao DESC, t.valor DESC
LIMIT 10000

-- Missing Table Statistics
SELECT 
  c.id_uf,
  t.categoria,
  COUNT(DISTINCT c.id_usuario) AS num_clientes,
  COUNT(*) AS num_transacoes,
  AVG(t.valor) AS valor_medio,
  SUM(t.valor) AS valor_total,
  MIN(t.data_transacao) AS primeira_transacao,
  MAX(t.data_transacao) AS ultima_transacao
FROM 
  bancodemo.clientes c
JOIN 
  bancodemo.transacoes_cartao t ON c.id_usuario = t.id_usuario
WHERE 
  t.data_transacao BETWEEN '2024-01-01' AND '2025-01-28'
  AND t.valor > 100
GROUP BY 
  c.id_uf, t.categoria
HAVING 
  COUNT(*) > 50
ORDER BY 
  valor_total DESC
LIMIT 1000

-- Slow Aggregate
SELECT 
  c.id_uf,
  t.categoria,
  COUNT(DISTINCT c.id_usuario) AS num_clientes,
  AVG(t.valor) AS valor_medio,
  PERCENTILE(t.valor, 0.5) AS valor_mediano,
  STDDEV(t.valor) AS desvio_padrao,
  CORR(t.valor, c.limite_credito) AS correlacao_valor_limite
FROM 
  bancodemo.clientes c
JOIN 
  bancodemo.transacoes_cartao t ON c.id_usuario = t.id_usuario
WHERE 
  t.data_transacao BETWEEN '2024-01-01' AND '2025-01-28'
  AND LOWER(t.estabelecimento) LIKE '%a%'
GROUP BY 
  c.id_uf, t.categoria
HAVING 
  COUNT(*) > 1000
ORDER BY 
  num_clientes DESC, valor_medio DESC
LIMIT 1000

-- Slow Client
SELECT 
  c.id_usuario,
  c.nome,
  c.email,
  c.data_nascimento,
  c.endereco,
  c.limite_credito,
  c.numero_cartao,
  c.id_uf,
  t.data_transacao,
  t.valor,
  t.estabelecimento,
  t.categoria,
  t.status
FROM 
  bancodemo.clientes c
JOIN 
  bancodemo.transacoes_cartao t ON c.id_usuario = t.id_usuario
WHERE 
  t.data_transacao BETWEEN '2024-01-01' AND '2025-01-28'
ORDER BY 
  t.data_transacao DESC, t.valor DESC

-- Slow Code Generation
SELECT 
  c.id_usuario,
  c.nome,
  c.email,
  c.data_nascimento,
  c.endereco,
  c.limite_credito,
  c.numero_cartao,
  c.id_uf,
  t.data_transacao,
  t.valor,
  t.estabelecimento,
  t.categoria,
  t.status,
  CASE 
    WHEN t.valor > c.limite_credito THEN 'Acima do Limite'
    ELSE 'Dentro do Limite'
  END AS status_limite,
  CONCAT(c.nome, ' - ', c.id_uf) AS cliente_estado,
  ROUND(t.valor / c.limite_credito * 100, 2) AS percentual_limite_usado,
  DATEDIFF(CURRENT_DATE(), c.data_nascimento) / 365 AS idade_cliente
FROM 
  bancodemo.clientes c
JOIN 
  bancodemo.transacoes_cartao t ON c.id_usuario = t.id_usuario
WHERE 
  t.data_transacao BETWEEN '2025-01-01' AND '2025-01-01'
GROUP BY 
  c.id_usuario, c.nome, c.email, c.data_nascimento, c.endereco, 
  c.limite_credito, c.numero_cartao, c.id_uf, t.data_transacao, 
  t.valor, t.estabelecimento, t.categoria, t.status
ORDER BY 
  percentual_limite_usado DESC
LIMIT 1000;

-- Slow HDFS Scan
SELECT 
  c.id_usuario,
  c.nome,
  c.email,
  c.id_uf,
  t.data_transacao,
  t.valor,
  t.estabelecimento,
  t.categoria,
  t.status
FROM 
  bancodemo.clientes c
JOIN 
  bancodemo.transacoes_cartao t ON c.id_usuario = t.id_usuario
WHERE 
  t.data_transacao BETWEEN '2025-01-01' AND '2025-12-31'
  AND LOWER(t.estabelecimento) LIKE '%a%'
  AND t.valor > 0
ORDER BY 
  t.data_transacao DESC, t.valor DESC;
