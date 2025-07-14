# Passo a passo HQL das funcionalidades do Iceberg no Impala

Este documento detalha cada comando SQL utilizado no script para operações com Iceberg no Impala, apresentando explicações claras e exemplos SQL.

Para realizar as consultas, vamos utilizar o `Cloudera Data Warehouse`.

![alt text](../img/cdw.png)

Em seguida clique no Hue, do ambiente `impala-vw` que estiver disponivel.

![alt text](../img/hue_impala.png)

> [!WARNING]
> Será necessário usar o nome do banco de dados como parâmetro nas execuções.
> Na primeira execução, adicionar o nome do seu banco como parâmetro, por exemplo `bancodemo_user001`.

![alt text](../img/create_database_impala.png)


## 1. Criação de Tabela Iceberg com CTAS (Create Table As Select)

**Explicação:**
Cria uma nova tabela Iceberg particionada por `id_uf`, copiando todos os dados da tabela original `bancodemo_userXXX.clientes`. O parâmetro `'format-version'='2'` define a versão do formato Iceberg.

```sql
use database ${databasename};

CREATE TABLE ${databasename}.clientes_iceberg_ctas_hue
PARTITIONED BY (id_uf)
STORED AS ICEBERG
TBLPROPERTIES ('format-version'='2')
AS SELECT * FROM ${databasename}.clientes;
```

## 2. Verificação de Atributos das Tabelas

**Explicação:**

Com o comando DESCRIBE FORMATTED podemos ver os metadados associados a cada uma das tabelas. Mostra os detalhes e propriedades das tabelas, como tipo de armazenamento, particionamento, localização e propriedades do Iceberg. Útil para comparar atributos entre a tabela original e a migrada.

Perceba a diferança em relação ao tipo da tabela, qual é o parâmetro que foi alterado?

```sql
DESCRIBE FORMATTED ${databasename}.clientes;
```

```sql
DESCRIBE FORMATTED ${databasename}.clientes_iceberg_ctas_hue;
```

## 3. Validação de Registros

**Explicação:**
Conta o número de registros em cada tabela para validar se a migração copiou todos os dados corretamente.

```sql
SELECT COUNT(*) FROM ${databasename}.clientes;
```

```sql
SELECT COUNT(*) FROM ${databasename}.clientes_iceberg_ctas_hue;
```

## 4. Validação de Integridade dos Dados

**Explicação:**
Seleciona e compara registros específicos em ambas as tabelas para garantir a integridade dos dados após a migração.

```sql
SELECT * FROM ${databasename}.clientes LIMIT 10;
```

```sql
SELECT * FROM ${databasename}.clientes_iceberg_ctas_hue
WHERE id_usuario IN (SELECT id_usuario FROM ${databasename}.transacoes_cartao LIMIT 10);
```

## 5. Exibição de Partições

**Explicação:**
Lista todas as partições existentes na tabela Iceberg, útil para verificar o particionamento após a migração.

A tabela esta particionado por qual campo? Os dados estão bem distribuídos?

```sql
SHOW PARTITIONS ${databasename}.clientes_iceberg_ctas_hue;
```

## 6. Histórico de Snapshots

**Explicação:**
Exibe o histórico de snapshots (versões) da tabela Iceberg, permitindo auditoria e análise de alterações nos últimos dias.

```sql
DESCRIBE HISTORY ${databasename}.clientes_iceberg_ctas_hue;
```

## 7. Inserção de Dados

**Explicação:**
Insere um novo registro na tabela Iceberg, simulando a inclusão de um cliente.

```sql
INSERT INTO ${databasename}.clientes_iceberg_ctas_hue
VALUES ('000000035', 'João Silva', 'joao@email.com', '1990-01-01', 'Rua A, 123', 5000, '1234-5678-9012-3456', 'SP');
```

## 8. Validando os snapshots

**Explicação:**
Uma vez que fizemos um novo insert na tabela, o que acontece com os snapshots?

```sql
DESCRIBE HISTORY ${databasename}.clientes_iceberg_ctas_hue;
```

## 8. Consulta com Snapshot Específico

**Explicação:**
Agora vamos explorar os recursos do snapshot e vamos consultar a tabela conforme o estado em um snapshot específico, permitindo auditoria de versões anteriores dos dados.

Vamos alterar o valor do campo `${snapshot_id_insert}` na consulta. Na primeira execução, utilize o valor de `snapshot_id` cujo `parent_id` seja nulo.
Na segunda execução, utiliza o valor do último `creation_time`. 

Qual a diferença? 

```sql
SELECT * FROM ${databasename}.clientes_iceberg_ctas_hue
FOR SYSTEM_VERSION AS OF ${snapshot_id_insert}
WHERE id_usuario = '000000035' AND nome = 'João Silva';
```

## 9. Consulta por Timestamp (Time Travel)

**Explicação:**
Outra forma de utilizar o comando seria utilizando o campo do `timestamp` , permite consultar os dados conforme estavam em um momento específico no tempo, utilizando o recurso de time travel do Iceberg.

Pegue o valor do timestamp do item 8.

```sql
SELECT * FROM ${databasename}.clientes_iceberg_ctas_hue
FOR SYSTEM_TIME AS OF ${system_time}
WHERE id_usuario = '000000035' AND nome = 'João Silva';
```

Essa consulta rodou com sucesso? 

## 10. Rollback de Tabela

**Explicação:**
Reverte a tabela para um snapshot anterior, desfazendo alterações e restaurando o estado anterior dos dados.

Com essa alteração, vamos reverter o processo de insert que foi realizado.

Antes de fazer o rollback, vamos ver como está nossa tabela

```sql
SELECT * FROM ${databasename}.clientes_iceberg_ctas_hue
WHERE id_usuario = '000000035' AND nome = 'João Silva';
```

A linha foi encontrada. 

Agora vamos buscar o `snapshot_id`, o valor do `snapshot_id` é o valor cujo `parent_id` seja nulo. 

```sql
DESCRIBE HISTORY ${databasename}.clientes_iceberg_ctas_hue;
```

Agora sim, vamos alterar nosso snapshot:

```sql
ALTER TABLE ${databasename}.clientes_iceberg_ctas_hue EXECUTE ROLLBACK(${snapshot_parent_id});
```

Como ficaram os nossos dados que foram inseridos?

```sql
SELECT * FROM ${databasename}.clientes_iceberg_ctas_hue
WHERE id_usuario = '000000035' AND nome = 'João Silva';
```

E o que aconteu com a lista de snapshots?

```sql
DESCRIBE HISTORY ${databasename}.clientes_iceberg_ctas_hue;
```


## 11. Propriedades Avançadas

**Explicação:**
Podemos também definir propriedades avançadas, como o formato padrão de escrita (Parquet) e o número máximo de versões antigas de metadados a serem mantidas.

O Iceberg rastreia os metadados das tabelas usando arquivos JSON. Cada alteração em uma tabela produz um novo arquivo de metadados para garantir a atomicidade.

Os arquivos de metadados antigos são mantidos para o histórico por padrão. Tabelas com commits frequentes, como aquelas gravadas por tarefas de streaming, podem precisar limpar os arquivos de metadados regularmente, uma forma de limpar esses metadados automaticamente é utilizando o parâmetro `write.metadata.previous-versions-max`, por exemplo, bem como rotinas de otimização do Spark. 

```sql
ALTER TABLE ${databasename}.clientes_iceberg_ctas_hue
SET TBLPROPERTIES('write.format.default'='parquet', 'write.metadata.previous-versions-max'='5');
```

## 12. Evolução de Esquema (Schema Evolution)

**Explicação:**
Adiciona ou remove colunas na tabela Iceberg de forma dinâmica, sem necessidade de recriação da tabela.

```sql
ALTER TABLE ${databasename}.clientes_iceberg_ctas_hue ADD COLUMNS (score FLOAT);
```

As consultas anteriores continuam funcionando sem modificações:

```sql
SELECT * FROM ${databasename}.clientes_iceberg_ctas_hue
LIMIT 10;
```

O Iceberg suporta a evolução do esquema, mas consultar dados históricos pode apresentar desafios se o esquema tiver sido alterado, como adicionar ou modificar colunas. 

Se uma coluna foi renomeada de `customer_name` para `client_name`, uma consulta de viagem no tempo que faz referência a `customer_name` pode falhar.

Como podemos contar essa situação? Mantenha o versionamento do esquema adequado e atualize as consultas de acordo e também podemos usar aliases para garantir a compatibilidade com versões anteriores.

Agora vamos remover essa nova coluna.

```sql
ALTER TABLE ${databasename}.clientes_iceberg_ctas_hue DROP COLUMN score;
```

## 13. Otimização e Compaction

**Explicação:**

Uma funcionalidade presente no Impala é a de `OPTIMIZE` que reorganiza e compacta os arquivos da tabela para melhorar desempenho e eficiência no acesso aos dados.

Este processo ajuda a lidar com a degradação do desempenho causada por arquivos de dados fragmentados que se acumulam ao longo do tempo devido a atualizações e exclusões frequentes.

A instrução OPTIMIZE TABLE aciona um processo chamado compactação, que essencialmente reescreve os dados da tabela para combinar arquivos pequenos em arquivos maiores e mais eficientes (geralmente com mais de 100 MB). Ela também mescla arquivos excluídos com os arquivos de dados correspondentes.

```sql
OPTIMIZE TABLE ${databasename}.clientes_iceberg_ctas_hue;
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



### Observações Finais

- Os comandos apresentados são compatíveis com Impala e Iceberg, aproveitando recursos de versionamento, time travel, rollback, evolução de esquema e otimização.
- O uso de snapshots e propriedades avançadas garante governança, rastreabilidade e eficiência no ambiente analítico.

<div style="text-align: center">⁂</div>

[^1]: iceberg_hue_impala.hql

