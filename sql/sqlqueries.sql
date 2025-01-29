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
    TRANSACOES_COUNT DESC;

-- Melhorias
compute stats bancodemo.clientes;
compute stats bancodemo.transacoes_cartao;
SELECT
  c.id_usuario AS cliente_id,
  COUNT(DISTINCT t.id_usuario) AS transacoes_count
FROM
  bancodemo.clientes c
  LEFT JOIN bancodemo.transacoes_cartao t ON c.id_usuario = t.id_usuario
GROUP BY
  c.id_usuario
ORDER BY
  transacoes_count DESC
LIMIT 100;

