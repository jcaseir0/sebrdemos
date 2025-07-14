# Passo a passo HQL das funcionalidades do Iceberg no Impala

Este documento detalha cada comando SQL utilizado no script para operações com Iceberg no Impala, apresentando explicações claras e exemplos SQL.

A demonstração pode ser feita tanto no HUE, editor SQL da Cloudera, em Data Hubs de Data Mart ou no Cloudera Data Warehouse com Impala.

## 1. Criação de Tabela Iceberg com CTAS (Create Table As Select)

**Explicação:**
Cria uma nova tabela Iceberg particionada por `id_uf`, copiando todos os dados da tabela original `bancodemo_userXXX.clientes`. O parâmetro `'format-version'='2'` define a versão do formato Iceberg.

```sql
use database ${databasename};

CREATE TABLE ${databasename}.clientes_iceberg_ctas_hue_impala
PARTITIONED BY (id_uf)
STORED AS ICEBERG
TBLPROPERTIES ('format-version'='2')
AS SELECT * FROM ${databasename}.clientes;
```

## 2. Verificação de Atributos das Tabelas

**Explicação:**
Exibe detalhes estruturais e propriedades das tabelas, como tipo de armazenamento, particionamento e localização, permitindo comparação entre a tabela original e a migrada.

```sql
DESCRIBE FORMATTED ${databasename}.clientes_iceberg_ctas_hue_impala;
```

```sql
DESCRIBE FORMATTED ${databasename}.clientes;
```

## 3. Validação de Registros

**Explicação:**
Conta o número de registros em cada tabela para validar se a migração copiou todos os dados corretamente.

```sql
SELECT COUNT(*) FROM ${databasename}.clientes;
```

```sql
SELECT COUNT(*) FROM ${databasename}.clientes_iceberg_ctas_hue_impala;
```

## 4. Validação de Integridade dos Dados

**Explicação:**
Seleciona e compara registros específicos em ambas as tabelas para garantir a integridade dos dados após a migração.

```sql
SELECT * FROM ${databasename}.clientes
WHERE id_usuario IN ('896797859', '284689128', '103946766', '648027188', '187525572', '909350817', '091759804', '687691239', '951031954', '810429067');
```

```sql
SELECT * FROM ${databasename}.clientes_iceberg_ctas_hue_impala
WHERE id_usuario IN ('896797859', '284689128', '103946766', '648027188', '187525572', '909350817', '091759804', '687691239', '951031954', '810429067')
ORDER BY 2;
```

## 5. Exibição de Partições

**Explicação:**
Lista todas as partições existentes na tabela Iceberg, útil para verificar o particionamento após a migração.

```sql
SHOW PARTITIONS ${databasename}.clientes_iceberg_ctas_hue_impala;
```


## 6. Histórico de Snapshots

**Explicação:**
Exibe o histórico de snapshots (versões) da tabela Iceberg, permitindo auditoria e análise de alterações nos últimos dias.

```sql
DESCRIBE HISTORY ${databasename}.clientes_iceberg_ctas_hue_impala;
```

## 7. Inserção de Dados

**Explicação:**
Insere um novo registro na tabela Iceberg, simulando a inclusão de um cliente.

```sql
INSERT INTO ${databasename}.clientes_iceberg_ctas_hue_impala
VALUES ('000000035', 'João Silva', 'joao@email.com', '1990-01-01', 'Rua A, 123', 5000, '1234-5678-9012-3456', 'SP');
```

## 8. Consulta com Snapshot Específico

**Explicação:**
Consulta a tabela conforme o estado em um snapshot específico, permitindo auditoria de versões anteriores dos dados.

```sql
SELECT * FROM ${databasename}.clientes_iceberg_ctas_hue_impala
FOR SYSTEM_VERSION AS OF ${snapshot_id_insert}
WHERE id_usuario = '000000035' AND nome = 'João Silva';
```

## 9. Consulta por Timestamp (Time Travel)

**Explicação:**
Permite consultar os dados conforme estavam em um momento específico no tempo, utilizando o recurso de time travel do Iceberg.

```sql
SELECT * FROM ${databasename}.clientes_iceberg_ctas_hue_impala
FOR SYSTEM_TIME AS OF ${system_time}
WHERE id_usuario = '000000035' AND nome = 'João Silva';
```

## 10. Rollback de Tabela

**Explicação:**
Reverte a tabela para um snapshot anterior, desfazendo alterações e restaurando o estado anterior dos dados.

```sql
ALTER TABLE ${databasename}.clientes_iceberg_ctas_hue_impala EXECUTE ROLLBACK(${snapshot_parent_id});
```

## 11. Propriedades Avançadas

**Explicação:**
Define propriedades avançadas, como o formato padrão de escrita (Parquet) e o número máximo de versões antigas de metadados a serem mantidas.

```sql
ALTER TABLE ${databasename}.clientes_iceberg_ctas_hue_impala
SET TBLPROPERTIES('write.format.default'='parquet', 'write.metadata.previous-versions-max'='5');
```

## 12. Evolução de Esquema (Schema Evolution)

**Explicação:**
Adiciona ou remove colunas na tabela Iceberg de forma dinâmica, sem necessidade de recriação da tabela.

```sql
ALTER TABLE ${databasename}.clientes_iceberg_ctas_hue_impala ADD COLUMNS (score FLOAT);
```

```sql
ALTER TABLE ${databasename}.clientes_iceberg_ctas_hue_impala DROP COLUMN score;
```

## 13. Otimização e Compaction

**Explicação:**
Reorganiza e compacta os arquivos da tabela para melhorar desempenho e eficiência no acesso aos dados.

```sql
OPTIMIZE TABLE ${databasename}.clientes_iceberg_ctas_hue_impala;
```

## 14. Conversão de Tabela para Iceberg

**Explicação:**
Converte uma tabela tradicional para o formato Iceberg, preservando dados e metadados, e define a versão do formato.

```sql
ALTER TABLE ${databasename}.clientes CONVERT TO ICEBERG;
```

```sql
ALTER TABLE ${databasename}.clientes SET TBLPROPERTIES('format-version'='2');
```

## 15. Consulta de Validação

**Explicação:**
Verifica a existência de um registro específico na tabela Iceberg, útil após inserções ou rollbacks.

```sql
SELECT * FROM ${databasename}.clientes_iceberg_ctas_hue_impala
WHERE id_usuario = '000000002' AND nome = 'Leonardo Gardom';
```

## 16. Inserção de Dados para Validação

**Explicação:**
Insere manualmente um registro para validação de operações de rollback e auditoria.

```sql
INSERT INTO ${databasename}.clientes_iceberg_ctas_hue_impala
VALUES ('000000002', 'Leonardo Gardom', 'lgardom@email.com', '1990-01-01', 'Rua C, 127', 7000, '4321-8765-2109-6543', 'AM');
```

## 17. Consulta com Limite

**Explicação:**
Retorna uma amostra dos dados da tabela, útil para validação visual e conferência rápida.

```sql
SELECT * FROM ${databasename}.clientes_iceberg_ctas_hue_impala LIMIT 10;
```

## 18. Consulta com Filtro Avançado

**Explicação:**
Exibe registros filtrados por valor de score, demonstrando o uso de colunas recém-adicionadas.

```sql
SELECT id_usuario, nome, score FROM ${databasename}..clientes_iceberg_ctas_hue_impala WHERE score > 50 LIMIT 10;
```

### Observações Finais

- Os comandos apresentados são compatíveis com Impala e Iceberg, aproveitando recursos de versionamento, time travel, rollback, evolução de esquema e otimização.
- O uso de snapshots e propriedades avançadas garante governança, rastreabilidade e eficiência no ambiente analítico.

<div style="text-align: center">⁂</div>

[^1]: iceberg_hue_impala.hql

