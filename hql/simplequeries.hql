-- 1. Total de gastos por cliente
SELECT 
    c.id_usuario,
    c.nome,
    SUM(t.valor) AS total_gastos
FROM bancodemo.clientes c
JOIN bancodemo.transacoes_cartao t 
ON c.id_usuario = t.id_usuario
GROUP BY c.id_usuario, c.nome
ORDER BY total_gastos DESC 
LIMIT 10;

-- 2. Número de transações por cliente
SELECT 
    c.id_usuario,
    c.nome,
    COUNT(*) AS total_transacoes
FROM bancodemo.clientes c
JOIN bancodemo.transacoes_cartao t 
ON c.id_usuario = t.id_usuario
GROUP BY c.id_usuario, c.nome
ORDER BY total_transacoes DESC
LIMIT 10;

-- 3. Média de gastos por transação
SELECT 
    c.id_usuario,
    c.nome,
    AVG(t.valor) AS media_gastos
FROM bancodemo.clientes c
JOIN bancodemo.transacoes_cartao t 
ON c.id_usuario = t.id_usuario
GROUP BY c.id_usuario, c.nome
ORDER BY media_gastos DESC
LIMIT 10;

-- 4. Maiores transações únicas (Top 10)
SELECT 
    c.id_usuario,
    c.nome,
    MAX(t.valor) AS maior_transacao
FROM bancodemo.clientes c
JOIN bancodemo.transacoes_cartao t 
ON c.id_usuario = t.id_usuario
GROUP BY c.id_usuario, c.nome
ORDER BY maior_transacao DESC
LIMIT 10;

-- 5. Gastos por categoria
SELECT 
    categoria,
    SUM(valor) AS total_gastos
FROM bancodemo.transacoes_cartao
GROUP BY categoria
ORDER BY total_gastos DESC
LIMIT 10;

-- 6. Transações por status
SELECT 
    status,
    COUNT(*) AS total_transacoes
FROM bancodemo.transacoes_cartao
GROUP BY status
ORDER BY total_transacoes DESC
LIMIT 10;

-- 7. Clientes com transações em Alimentação
SELECT DISTINCT
    c.id_usuario,
    c.nome
FROM bancodemo.clientes c
JOIN bancodemo.transacoes_cartao t 
ON c.id_usuario = t.id_usuario
WHERE t.categoria = 'Alimentação'
LIMIT 10;

-- 8. Gastos em Transporte por cliente
SELECT 
    c.id_usuario,
    c.nome,
    SUM(t.valor) AS total_gastos_transporte
FROM bancodemo.clientes c
JOIN bancodemo.transacoes_cartao t 
ON c.id_usuario = t.id_usuario
WHERE t.categoria = 'Transporte'
GROUP BY c.id_usuario, c.nome
ORDER BY total_gastos_transporte DESC
LIMIT 10;

-- 9. Clientes com transações negadas
SELECT DISTINCT
    c.id_usuario,
    c.nome
FROM bancodemo.clientes c
JOIN bancodemo.transacoes_cartao t 
ON c.id_usuario = t.id_usuario
WHERE t.status = 'Negada'
LIMIT 10;

-- 10. Gastos mensais por cliente
SELECT 
    c.id_usuario,
    c.nome,
    TO_CHAR(t.data_transacao, 'yyyy-MM') AS mes,
    SUM(t.valor) AS total_gastos
FROM bancodemo.clientes c
JOIN bancodemo.transacoes_cartao t 
ON c.id_usuario = t.id_usuario
GROUP BY c.id_usuario, c.nome, TO_CHAR(t.data_transacao, 'yyyy-MM')
ORDER BY c.id_usuario, mes
LIMIT 10;
