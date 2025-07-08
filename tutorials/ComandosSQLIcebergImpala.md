# # Passo a passo HQL das funcionalidades do Iceberg no Impala

Este documento detalha cada comando SQL utilizado no script para operações com Iceberg no Impala, apresentando explicações claras e exemplos SQL.

A demonstração pode ser feita tanto no HUE, editor SQL da Cloudera, em Data Hubs de Data Mart ou no Cloudera Data Warehouse com Impala.

## 1. Criação de Tabela Iceberg com CTAS (Create Table As Select)

```sql
CREATE TABLE bancodemo.clientes_iceberg_ctas_hue
PARTITIONED BY (id_uf)
STORED AS ICEBERG
TBLPROPERTIES ('format-version'='2')
AS SELECT * FROM bancodemo.clientes;
```

**Explicação:**
Cria uma nova tabela Iceberg particionada por `id_uf`, copiando todos os dados da tabela original `bancodemo.clientes`. O parâmetro `'format-version'='2'` define a versão do formato Iceberg.

## 2. Verificação de Atributos das Tabelas

```sql
DESCRIBE FORMATTED bancodemo.clientes_iceberg_ctas_hue;
DESCRIBE FORMATTED bancodemo.clientes;
```

**Explicação:**
Exibe detalhes estruturais e propriedades das tabelas, como tipo de armazenamento, particionamento e localização, permitindo comparação entre a tabela original e a migrada.

## 3. Validação de Registros

```sql
SELECT COUNT(*) FROM bancodemo.clientes;
SELECT COUNT(*) FROM bancodemo.clientes_iceberg_ctas_hue;
```

**Explicação:**
Conta o número de registros em cada tabela para validar se a migração copiou todos os dados corretamente.

## 4. Validação de Integridade dos Dados

```sql
SELECT * FROM bancodemo.clientes
WHERE id_usuario IN ('896797859', '284689128', '103946766', '648027188', '187525572', '909350817', '091759804', '687691239', '951031954', '810429067');

SELECT * FROM bancodemo.clientes_iceberg_ctas_hue
WHERE id_usuario IN ('896797859', '284689128', '103946766', '648027188', '187525572', '909350817', '091759804', '687691239', '951031954', '810429067')
ORDER BY 2;
```

**Explicação:**
Seleciona e compara registros específicos em ambas as tabelas para garantir a integridade dos dados após a migração.

## 5. Exibição de Partições

```sql
SHOW PARTITIONS bancodemo.clientes_iceberg_ctas_hue;
```

**Explicação:**
Lista todas as partições existentes na tabela Iceberg, útil para verificar o particionamento após a migração.

## 6. Histórico de Snapshots

```sql
DESCRIBE HISTORY bancodemo.clientes_iceberg_ctas_hue;
DESCRIBE HISTORY bancodemo.clientes_iceberg_ctas_hue FROM now() - interval 5 days;
```

**Explicação:**
Exibe o histórico de snapshots (versões) da tabela Iceberg, permitindo auditoria e análise de alterações nos últimos dias.

## 7. Inserção de Dados

```sql
INSERT INTO bancodemo.clientes_iceberg_ctas_hue
VALUES ('000000035', 'João Silva', 'joao@email.com', '1990-01-01', 'Rua A, 123', 5000, '1234-5678-9012-3456', 'SP');
```

**Explicação:**
Insere um novo registro na tabela Iceberg, simulando a inclusão de um cliente.

## 8. Consulta com Snapshot Específico

```sql
SELECT * FROM bancodemo.clientes_iceberg_ctas_hue
FOR SYSTEM_VERSION AS OF ${snapshot_id_insert}
WHERE id_usuario = '000000035' AND nome = 'João Silva';
```

**Explicação:**
Consulta a tabela conforme o estado em um snapshot específico, permitindo auditoria de versões anteriores dos dados.

## 9. Consulta por Timestamp (Time Travel)

```sql
SELECT * FROM bancodemo.clientes_iceberg_ctas_hue
FOR SYSTEM_TIME AS OF ${system_time}
WHERE id_usuario = '000000035' AND nome = 'João Silva';
```

**Explicação:**
Permite consultar os dados conforme estavam em um momento específico no tempo, utilizando o recurso de time travel do Iceberg.

## 10. Rollback de Tabela

```sql
ALTER TABLE bancodemo.clientes_iceberg_ctas_hue EXECUTE ROLLBACK(${snapshot_parent_id});
```

**Explicação:**
Reverte a tabela para um snapshot anterior, desfazendo alterações e restaurando o estado anterior dos dados.

## 11. Propriedades Avançadas

```sql
ALTER TABLE bancodemo.clientes_iceberg_ctas_hue
SET TBLPROPERTIES('write.format.default'='parquet', 'write.metadata.previous-versions-max'='5');
```

**Explicação:**
Define propriedades avançadas, como o formato padrão de escrita (Parquet) e o número máximo de versões antigas de metadados a serem mantidas.

## 12. Evolução de Esquema (Schema Evolution)

```sql
ALTER TABLE bancodemo.clientes_iceberg_ctas_hue ADD COLUMNS (score FLOAT);
ALTER TABLE bancodemo.clientes_iceberg_ctas_hue DROP COLUMN score;
```

**Explicação:**
Adiciona ou remove colunas na tabela Iceberg de forma dinâmica, sem necessidade de recriação da tabela.

## 13. Otimização e Compaction

```sql
OPTIMIZE TABLE bancodemo.clientes_iceberg_ctas_hue;
```

**Explicação:**
Reorganiza e compacta os arquivos da tabela para melhorar desempenho e eficiência no acesso aos dados.

## 14. Conversão de Tabela para Iceberg

```sql
ALTER TABLE bancodemo.clientes CONVERT TO ICEBERG;
ALTER TABLE bancodemo.clientes SET TBLPROPERTIES('format-version'='2');
```

**Explicação:**
Converte uma tabela tradicional para o formato Iceberg, preservando dados e metadados, e define a versão do formato.

## 15. Consulta de Validação

```sql
SELECT * FROM bancodemo.clientes_iceberg_ctas_hue
WHERE id_usuario = '000000002' AND nome = 'Leonardo Gardom';
```

**Explicação:**
Verifica a existência de um registro específico na tabela Iceberg, útil após inserções ou rollbacks.

## 16. Inserção de Dados para Validação

```sql
INSERT INTO bancodemo.clientes_iceberg_ctas_hue
VALUES ('000000002', 'Leonardo Gardom', 'lgardom@email.com', '1990-01-01', 'Rua C, 127', 7000, '4321-8765-2109-6543', 'AM');
```

**Explicação:**
Insere manualmente um registro para validação de operações de rollback e auditoria.

## 17. Consulta com Limite

```sql
SELECT * FROM bancodemo.clientes_iceberg_ctas_hue LIMIT 10;
```

**Explicação:**
Retorna uma amostra dos dados da tabela, útil para validação visual e conferência rápida.

## 18. Consulta com Filtro Avançado

```sql
SELECT id_usuario, nome, score FROM bancodemo.clientes_iceberg_ctas_hue WHERE score > 50 LIMIT 10;
```

**Explicação:**
Exibe registros filtrados por valor de score, demonstrando o uso de colunas recém-adicionadas.

## 19. Otimização da Tabela Original

```sql
OPTIMIZE TABLE bancodemo.clientes;
```

**Explicação:**
Otimiza a tabela original após a conversão para Iceberg, melhorando o desempenho das consultas.

### Observações Finais

- Os comandos apresentados são compatíveis com Impala e Iceberg, aproveitando recursos de versionamento, time travel, rollback, evolução de esquema e otimização.
- O uso de snapshots e propriedades avançadas garante governança, rastreabilidade e eficiência no ambiente analítico.

<div style="text-align: center">⁂</div>

[^1]: iceberg_hue_impala.hql

