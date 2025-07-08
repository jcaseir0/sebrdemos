<img src="https://r2cdn.perplexity.ai/pplx-full-logo-primary-dark%402x.png" class="logo" width="120"/>

# Documentação dos Comandos HQL do Script Iceberg + Hive

Este documento apresenta uma explicação detalhada de cada comando HQL (Hive Query Language) presente no script fornecido, organizado por tópicos. Cada comando é apresentado em uma caixa de código SQL, seguido de uma explicação clara sobre seu propósito e funcionamento.

## 1. Criação de Tabela Iceberg com CTAS

```sql
CREATE EXTERNAL TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue
PARTITIONED BY (data_execucao)
STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler'
TBLPROPERTIES ('format-version'='2')
AS SELECT * FROM bancodemo.transacoes_cartao;
```

**Explicação:**
Cria uma tabela externa Iceberg no Hive, particionada por `data_execucao`, usando o storage handler do Iceberg. O comando copia todos os dados da tabela original `transacoes_cartao` para a nova tabela Iceberg, já no formato Iceberg e na versão 2 do formato.

## 2. Verificação de Metadados das Tabelas

```sql
DESCRIBE FORMATTED bancodemo.transacoes_cartao_iceberg_ctas_hue;
DESCRIBE FORMATTED bancodemo.transacoes_cartao;
```

**Explicação:**
Mostra os detalhes e propriedades das tabelas, como tipo de armazenamento, particionamento, localização e propriedades do Iceberg. Útil para comparar atributos entre a tabela original e a migrada.

## 3. Validação de Registros

```sql
SELECT COUNT(*) FROM bancodemo.transacoes_cartao;
SELECT COUNT(*) FROM bancodemo.transacoes_cartao_iceberg_ctas_hue;
```

**Explicação:**
Conta o número de registros em cada tabela, permitindo validar se a migração copiou todos os dados corretamente.

## 4. Validação de Integridade

```sql
SELECT * FROM bancodemo.transacoes_cartao LIMIT 10;
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue
WHERE id_usuario = ${hivetableid} AND valor = ${hivetablevalor};
```

**Explicação:**
Exibe amostras de dados das duas tabelas para validação manual e compara registros específicos usando filtros.

## 5. Validação Cruzada

```sql
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue
WHERE id_usuario IN (SELECT id_usuario FROM bancodemo.transacoes_cartao LIMIT 10)
AND valor IN (SELECT valor FROM bancodemo.transacoes_cartao LIMIT 10);
```

**Explicação:**
Compara registros entre as tabelas usando subconjuntos de valores, útil para checagem cruzada de integridade após migração.

## 6. Controle de Versão com TAGs

```sql
ALTER TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue
CREATE TAG pre_insert;
```

**Explicação:**
Cria uma tag (marcador de versão) antes de operações críticas, permitindo rastrear e voltar a este ponto posteriormente.

## 7. Inserção de Dados

```sql
INSERT INTO bancodemo.transacoes_cartao_iceberg_ctas_hue
VALUES ('000000036', '2024-06-24 15:10:06', 702.99, 'Mercado Bitcoin', 'Outros', 'Aprovada', '06-02-2025');
```

**Explicação:**
Insere um novo registro na tabela Iceberg, simulando uma transação de cartão.

## 8. Consulta de Histórico (Snapshots)

```sql
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue.history;
```

**Explicação:**
Lista todos os snapshots (versões) da tabela, permitindo auditoria e time travel.

## 9. Consulta com Snapshot Específico

```sql
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue
FOR SYSTEM_VERSION AS OF ${snapshot_id_insert}
WHERE id_usuario = '000000036' AND estabelecimento = 'Mercado Bitcoin';
```

**Explicação:**
Consulta a tabela como ela estava em um determinado snapshot, útil para auditoria e recuperação de versões anteriores.

## 10. Atualização de Dados

```sql
ALTER TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue
CREATE TAG pre_update;

UPDATE bancodemo.transacoes_cartao_iceberg_ctas_hue
SET valor = 510.99
WHERE id_usuario = '000000036' AND estabelecimento = 'Mercado Bitcoin';
```

**Explicação:**
Marca o estado anterior com uma tag e atualiza o valor de uma transação específica.

## 11. Exclusão de Dados

```sql
ALTER TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue
CREATE TAG pre_delete;

DELETE FROM bancodemo.transacoes_cartao_iceberg_ctas_hue
WHERE id_usuario = '000000036';
```

**Explicação:**
Cria uma tag antes da exclusão e remove registros de um usuário específico.

## 12. Evolução de Esquema (Schema Evolution)

```sql
ALTER TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue ADD COLUMNS (limite_credito INT);
```

**Explicação:**
Adiciona uma nova coluna à tabela Iceberg de forma dinâmica, sem recriar a tabela.

## 13. Atualização em Massa com MERGE

```sql
MERGE INTO bancodemo.transacoes_cartao_iceberg_ctas_hue AS t
USING (
  SELECT id_usuario, MAX(limite_credito) AS limite_credito
  FROM bancodemo.clientes
  GROUP BY id_usuario
) AS c
ON t.id_usuario = c.id_usuario
WHEN MATCHED THEN
UPDATE SET limite_credito = COALESCE(c.limite_credito, t.limite_credito);
```

**Explicação:**
Atualiza a coluna `limite_credito` na tabela Iceberg com valores vindos da tabela de clientes, usando merge (upsert).

## 14. Time Travel por Timestamp

```sql
SELECT *
FROM bancodemo.transacoes_cartao_iceberg_ctas_hue
FOR SYSTEM_TIME AS OF '${system_time}'
LIMIT 10;
```

**Explicação:**
Consulta a tabela conforme ela estava em um determinado momento no tempo, usando o recurso de time travel do Iceberg.

## 15. Tagging e Rollback

```sql
ALTER TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue CREATE TAG tag_insert FOR SYSTEM_VERSION AS OF ${snapshot_id_insert};
ALTER TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue EXECUTE ROLLBACK(${snapshot_parent_id});
```

**Explicação:**
Cria uma tag para um snapshot específico e faz rollback para um snapshot anterior, revertendo alterações.

## 16. Branching (Ramificações)

```sql
ALTER TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue CREATE BRANCH dev_branch;
INSERT INTO bancodemo.transacoes_cartao_iceberg_ctas_hue.branch_dev_branch VALUES (...);
```

**Explicação:**
Cria uma branch (ramificação) para desenvolvimento isolado, permitindo alterações sem afetar a branch principal.

## 17. Otimização e Compaction

```sql
OPTIMIZE TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue;
```

**Explicação:**
Compacta arquivos pequenos e reorganiza os dados da tabela para melhorar desempenho e eficiência.

## 18. Conversão de Tabela para Iceberg

```sql
ALTER TABLE bancodemo.transacoes_cartao CONVERT TO ICEBERG;
```

**Explicação:**
Converte uma tabela Hive tradicional para o formato Iceberg, preservando dados e metadados.

## 19. Análise de Estatísticas

```sql
ANALYZE TABLE bancodemo.transacoes_cartao COMPUTE STATISTICS;
ANALYZE TABLE bancodemo.transacoes_cartao COMPUTE STATISTICS FOR COLUMNS;
```

**Explicação:**
Calcula estatísticas da tabela e das colunas para otimizar o desempenho de consultas.

## 20. Propriedades Avançadas

```sql
ALTER TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue
SET TBLPROPERTIES('write.format.default'='parquet', 'write.metadata.previous-versions-max'='5');
```

**Explicação:**
Define propriedades avançadas, como formato padrão de escrita (Parquet) e número máximo de versões de metadados a serem mantidas.

### Observações Finais

- **Tags** e **branches** são recursos avançados do Iceberg no Hive, permitindo controle de versões, auditoria e desenvolvimento seguro.
- O **time travel** permite consultar dados históricos facilmente.
- O uso de comandos como **MERGE**, **ROLLBACK** e **OPTIMIZE** facilita a manutenção e governança de dados em ambientes analíticos modernos.

Se precisar de exemplos práticos ou dúvidas sobre algum comando específico, peça detalhes!

<div style="text-align: center">⁂</div>

[^1]: iceberg_hue_hive.hql

