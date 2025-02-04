-- 1. Aggregation Spilled Partitions
SELECT c.id_usuario, c.nome, c.email, c.data_nascimento, c.endereco, c.limite_credito, c.numero_cartao, c.id_uf, COUNT(t.id_usuario) AS total_transacoes, SUM(t.valor) AS valor_total_transacoes, AVG(t.valor) AS valor_medio_transacoes, MIN(t.data_transacao) AS primeira_transacao, MAX(t.data_transacao) AS ultima_transacao, COUNT(DISTINCT t.estabelecimento) AS estabelecimentos_unicos, COUNT(DISTINCT t.categoria) AS categorias_unicas, SUM(CASE WHEN t.status = 'Aprovada' THEN 1 ELSE 0 END) AS transacoes_aprovadas, SUM(CASE WHEN t.status = 'Negada' THEN 1 ELSE 0 END) AS transacoes_negadas, SUM(CASE WHEN t.status = 'Pendente' THEN 1 ELSE 0 END) AS transacoes_pendentes FROM bancodemo.clientes c LEFT JOIN bancodemo.transacoes_cartao t ON c.id_usuario = t.id_usuario GROUP BY c.id_usuario, c.nome, c.email, c.data_nascimento, c.endereco, c.limite_credito, c.numero_cartao, c.id_uf ORDER BY valor_total_transacoes DESC;

-- 2. Bytes Read Skew
SELECT t.id_usuario, c.nome, c.id_uf, t.categoria, t.estabelecimento, COUNT(*) AS num_transacoes, SUM(t.valor) AS valor_total, AVG(t.valor) AS valor_medio FROM bancodemo.transacoes_cartao t JOIN bancodemo.clientes c ON t.id_usuario = c.id_usuario WHERE t.data_transacao BETWEEN '2025-01-01' AND '2025-12-31' AND c.id_uf IN ('SP', 'RJ', 'MG') AND t.valor > 1000 GROUP BY t.id_usuario, c.nome, c.id_uf, t.categoria, t.estabelecimento HAVING COUNT(*) > 10 ORDER BY valor_total DESC LIMIT 1000;

-- 2.1. Bytes Read Skew
CREATE TABLE bancodemo.skewed_transacoes STORED AS PARQUET AS SELECT c.id_usuario, c.nome, c.id_uf, t.valor, t.data_transacao, t.categoria, t.status FROM bancodemo.clientes c JOIN bancodemo.transacoes_cartao t ON c.id_usuario = t.id_usuario;

-- 2.2. Carregar a tabela recém criada:
INSERT INTO bancodemo.skewed_transacoes SELECT id_usuario, nome, CASE WHEN RAND() < 0.7 THEN 'SP' WHEN RAND() < 0.9 THEN 'RJ' ELSE id_uf END AS id_uf, valor, data_transacao, categoria, status FROM bancodemo.skewed_transacoes;

-- 2.3. Consulta que pode gerar Bytes Read Skew
SELECT id_uf, COUNT(*) AS total_transacoes, SUM(valor) AS total_valor, AVG(valor) AS media_valor FROM bancodemo.skewed_transacoes GROUP BY id_uf ORDER BY total_transacoes DESC;

-- 3. Corrupt Table Statistics
CREATE TABLE bancodemo.corrupttblstats AS SELECT * FROM bancodemo.transacoes_cartao WHERE data_transacao BETWEEN '2024-01-01' AND '2024-12-31';
COMPUTE STATS bancodemo.corrupttblstats;
INSERT OVERWRITE TABLE bancodemo.corrupttblstats SELECT id_usuario, data_transacao, valor * 1000, estabelecimento, categoria, CASE WHEN status = 'Aprovada' THEN 'Negada' WHEN status = 'Negada' THEN 'Aprovada' ELSE status END AS status, data_execucao FROM bancodemo.corrupttblstats;
SELECT categoria, AVG(valor) AS valor_medio, COUNT(*) AS total_transacoes FROM bancodemo.corrupttblstats GROUP BY categoria ORDER BY valor_medio DESC;

-- 4. Duration Skew
SELECT c.id_uf, t.categoria, TRUNC(t.data_transacao, 'MM') AS mes, COUNT(*) AS num_transacoes, SUM(t.valor) AS valor_total, AVG(t.valor) AS valor_medio, COUNT(DISTINCT c.id_usuario) AS num_clientes_unicos, MAX(CASE WHEN t.valor > c.limite_credito THEN 'Acima do Limite' ELSE 'Dentro do Limite' END) AS status_limite FROM bancodemo.clientes c JOIN bancodemo.transacoes_cartao t ON c.id_usuario = t.id_usuario WHERE t.data_transacao BETWEEN '2025-01-01' AND '2025-12-31' GROUP BY c.id_uf, t.categoria, TRUNC(t.data_transacao, 'MM') HAVING COUNT(*) > 100 ORDER BY valor_total DESC, num_clientes_unicos DESC LIMIT 1000;

-- 5. HashJoin Spilled Partitions
SELECT c.id_usuario, c.nome, c.email, t.categoria, COUNT(*) AS num_transacoes, SUM(t.valor) AS valor_total, AVG(t.valor) AS valor_medio FROM bancodemo.clientes c JOIN bancodemo.transacoes_cartao t ON c.id_usuario = t.id_usuario WHERE t.data_transacao BETWEEN '2025-01-01' AND '2025-12-31' GROUP BY c.id_usuario, c.nome, c.email, t.categoria HAVING COUNT(*) > 100 ORDER BY valor_total DESC LIMIT 10000;

-- 6. Insufficient Partitioning
SELECT c.id_usuario, c.nome, t.data_transacao, t.valor, t.categoria FROM bancodemo.clientes c JOIN bancodemo.transacoes_cartao t ON c.id_usuario = t.id_usuario WHERE t.data_transacao BETWEEN '2024-01-01' AND '2025-01-28' AND t.valor > 1000 ORDER BY t.data_transacao, t.valor DESC;

-- 7. Many Materialized Columns
SELECT c.id_usuario, c.nome, c.email, c.data_nascimento, c.endereco, c.limite_credito, c.numero_cartao, c.id_uf, t.data_transacao, t.valor, t.estabelecimento, t.categoria, t.status, YEAR(t.data_transacao) AS ano_transacao, MONTH(t.data_transacao) AS mes_transacao, DAY(t.data_transacao) AS dia_transacao, CASE WHEN t.valor > c.limite_credito THEN 'Acima do Limite' ELSE 'Dentro do Limite' END AS status_limite, CONCAT(c.nome, ' - ', c.id_uf) AS cliente_estado, ROUND(t.valor / c.limite_credito * 100, 2) AS percentual_limite_usado, DATEDIFF(CURRENT_DATE(), c.data_nascimento) / 365 AS idade_aproximada FROM bancodemo.client c JOIN bancodemo.transacoes_cartao t ON c.id_usuario = t.id_usuario WHERE t.data_transacao BETWEEN '2025-01-01' AND '2025-12-31' AND t.valor > 100 ORDER BY t.data_transacao DESC, t.valor DESC LIMIT 10000;

-- 8. Missing Table Statistics
SELECT c.id_uf, t.categoria, COUNT(DISTINCT c.id_usuario) AS num_clientes, COUNT(*) AS num_transacoes, AVG(t.valor) AS valor_medio, SUM(t.valor) AS valor_total, MIN(t.data_transacao) AS primeira_transacao, MAX(t.data_transacao) AS ultima_transacao FROM bancodemo.client c JOIN bancodemo.transacoes_cartao t ON c.id_usuario = t.id_usuario WHERE t.data_transacao BETWEEN '2025-01-01' AND '2025-12-31' AND t.valor > 100 GROUP BY c.id_uf, t.categoria HAVING COUNT(*) > 50 ORDER BY valor_total DESC LIMIT 1000;

-- 9. Slow Aggregate
WITH transacoes_com_rank AS ( SELECT c.id_usuario, c.id_uf, t.categoria, t.valor, c.limite_credito, ROW_NUMBER() OVER (PARTITION BY c.id_uf, t.categoria ORDER BY t.valor) AS rank, COUNT(*) OVER (PARTITION BY c.id_uf, t.categoria) AS total_count FROM bancodemo.clientes c JOIN bancodemo.transacoes_cartao t ON c.id_usuario = t.id_usuario WHERE t.data_transacao BETWEEN '2025-01-01' AND '2025-12-31' AND LOWER(t.estabelecimento) LIKE '%a%' ) SELECT id_uf, categoria, COUNT(DISTINCT id_usuario) AS num_clientes, AVG(valor) AS valor_medio, MAX(CASE WHEN rank = FLOOR(total_count / 2) THEN valor END) AS valor_mediano, STDDEV(valor) AS desvio_padrao, CORR(valor, limite_credito) AS correlacao_valor_limite FROM transacoes_com_rank GROUP BY id_uf, categoria HAVING COUNT(*) > 1000 ORDER BY num_clientes DESC, valor_medio DESC LIMIT 1000;

-- 10. Slow Client
SELECT c.id_usuario, c.nome, c.email, c.data_nascimento, c.endereco, c.limite_credito, c.numero_cartao, c.id_uf, t.data_transacao, t.valor, t.estabelecimento, t.categoria, t.status FROM bancodemo.client c JOIN bancodemo.transacoes_cartao t ON c.id_usuario = t.id_usuario WHERE t.data_transacao BETWEEN '2024-01-01' AND '2025-01-28' ORDER BY t.data_transacao DESC, t.valor DESC;

-- 11. Slow Code Generation
SELECT c.id_usuario, c.nome, c.email, c.data_nascimento, c.endereco, c.limite_credito, c.numero_cartao, c.id_uf, t.data_transacao, t.valor, t.estabelecimento, t.categoria, t.status, CASE WHEN t.valor > c.limite_credito THEN 'Acima do Limite' ELSE 'Dentro do Limite' END AS status_limite, CONCAT(c.nome, ' - ', c.id_uf) AS cliente_estado, ROUND(t.valor / c.limite_credito * 100, 2) AS percentual_limite_usado, DATEDIFF(CURRENT_DATE(), c.data_nascimento) / 365 AS idade_cliente FROM bancodemo.clientes c JOIN bancodemo.transacoes_cartao t ON c.id_usuario = t.id_usuario WHERE t.data_transacao BETWEEN '2025-01-01' AND '2025-12-31' GROUP BY c.id_usuario, c.nome, c.email, c.data_nascimento, c.endereco, c.limite_credito, c.numero_cartao, c.id_uf, t.data_transacao, t.valor, t.estabelecimento, t.categoria, t.status ORDER BY percentual_limite_usado DESC LIMIT 1000;

-- 12. Slow HDFS Scan
SELECT c.id_usuario, c.nome, c.email, c.id_uf, t.data_transacao, t.valor, t.estabelecimento, t.categoria, t.status FROM bancodemo.client c JOIN bancodemo.transacoes_cartao t ON c.id_usuario = t.id_usuario WHERE t.data_transacao BETWEEN '2025-01-01' AND '2025-12-31' AND LOWER(t.estabelecimento) LIKE '%a%' AND t.valor > 0 ORDER BY t.data_transacao DESC, t.valor DESC;

-- 13. Slow Hash Join
SELECT c.id_usuario, c.nome, t.categoria, COUNT(*) AS num_transacoes, SUM(t.valor) AS valor_total, AVG(t.valor) AS valor_medio FROM bancodemo.clientes c JOIN bancodemo.transacoes_cartao t ON c.id_usuario = t.id_usuario WHERE t.data_transacao BETWEEN '2025-01-01' AND '2025-12-31' GROUP BY c.id_usuario, c.nome, t.categoria ORDER BY valor_total DESC LIMIT 1000;

-- 14. Slow Query Planning
WITH subquery1 AS ( SELECT t.id_usuario, t.categoria, t.data_transacao, SUM(t.valor) AS total_valor, COUNT(*) AS num_transacoes FROM bancodemo.transacoes_cartao t WHERE t.data_transacao BETWEEN '2020-01-01' AND '2025-01-01' GROUP BY t.id_usuario, t.categoria, t.data_transacao ), subquery2 AS ( SELECT c.id_usuario, c.id_uf, c.limite_credito, COUNT(DISTINCT c.email) AS num_emails FROM bancodemo.clientes c GROUP BY c.id_usuario, c.id_uf, c.limite_credito ) SELECT s1.categoria, s2.id_uf, AVG(s1.total_valor) AS media_valor_total, MAX(s1.num_transacoes) AS max_transacoes, SUM(s2.num_emails) AS total_emails FROM subquery1 s1 JOIN subquery2 s2 ON s1.id_usuario = s2.id_usuario GROUP BY s1.categoria, s2.id_uf ORDER BY media_valor_total DESC;

-- 15. Slow Row Materialization
SELECT c.id_usuario, c.nome, c.email, c.data_nascimento, c.endereco, c.limite_credito, c.numero_cartao, c.id_uf, t.data_transacao, t.valor, t.estabelecimento, t.categoria, t.status, YEAR(t.data_transacao) AS ano_transacao, MONTH(t.data_transacao) AS mes_transacao, DAY(t.data_transacao) AS dia_transacao, CASE WHEN t.valor > c.limite_credito THEN 'Acima do Limite' ELSE 'Dentro do Limite' END AS status_limite, CONCAT(c.nome, ' - ', c.id_uf) AS cliente_estado, ROUND(t.valor / c.limite_credito * 100, 2) AS percentual_limite_usado, DATEDIFF(CURRENT_DATE(), c.data_nascimento) / 365 AS idade_aproximada FROM bancodemo.clientes c JOIN bancodemo.transacoes_cartao t ON c.id_usuario = t.id_usuario WHERE t.data_transacao BETWEEN '2020-01-01' AND '2025-12-31' AND t.valor > 100 ORDER BY t.data_transacao DESC, t.valor DESC LIMIT 1000000;

-- 16. Slow Sorting
SELECT c.id_usuario, c.nome, t.data_transacao, t.valor, t.categoria FROM bancodemo.clientes c JOIN bancodemo.transacoes_cartao t ON c.id_usuario = t.id_usuario WHERE t.data_transacao BETWEEN '2020-01-01' AND '2025-12-31' ORDER BY t.valor DESC, t.data_transacao DESC LIMIT 1000000;

-- 17. Slow Write Speed
INSERT INTO bancodemo.transacoes_cartao PARTITION (data_execucao) SELECT c.id_usuario, to_timestamp(cast(now() as string), 'yyyy-MM-dd HH:mm:ss') AS data_transacao, ROUND(RAND() * 1000, 2) AS valor, CONCAT('Estabelecimento_', CAST(FLOOR(RAND() * 1000) AS STRING)) AS estabelecimento, CASE FLOOR(RAND() * 5) WHEN 0 THEN 'Alimentação' WHEN 1 THEN 'Transporte' WHEN 2 THEN 'Entretenimento' WHEN 3 THEN 'Saúde' ELSE 'Educação' END AS categoria, CASE WHEN RAND() > 0.9 THEN 'Negada' ELSE 'Aprovada' END AS status, CAST(to_timestamp(CAST(DATE_FORMAT(CURRENT_DATE(), 'dd-MM-yyyy') AS STRING)) AS STRING) AS data_execucao AS data_execucao FROM bancodemo clientes c CROSS JOIN (SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5) t WHERE RAND() < 0.1;
