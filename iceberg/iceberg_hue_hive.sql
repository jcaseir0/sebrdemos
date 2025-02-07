-- Work in progress
-- Hive SQL

-- ICEBERG SHADOW MIGRATION

-- Create table as SELECT - HiveIcebergStorageHandler, é o mecanismo padrão para lidar com tabelas Iceberg no Hive. O suporte a Iceberg no Hive é feito através de um mecanismo diferente, geralmente usando o STORED BY em vez de STORED AS.
CREATE EXTERNAL TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue_hive_2
PARTITIONED BY (data_execucao)
USING 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler'
TBLPROPERTIES ('format-version'='2')
AS SELECT * FROM bancodemo.transacoes_cartao;

-- Verificar os atributos da nova tabela Iceberg:
DESCRIBE FORMATTED bancodemo.transacoes_cartao_iceberg_ctas_hue_hive;
-- Verificar os atributos da antiga para comparação:
DESCRIBE FORMATTED bancodemo.transacoes_cartao;

-- Validação de registros entre Origem e Destino da migração:
SELECT COUNT(*) FROM bancodemo.transacoes_cartao;
SELECT COUNT(*) FROM bancodemo.transacoes_cartao_iceberg_ctas_hue_hive;

-- Validação de integridade dos dados entre Origem e Destino da migração:
SELECT * FROM bancodemo.transacoes_cartao LIMIT 10;
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue_hive LIMIT 10;

-- Verificar os snapshots da tabela  Iceberg
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue_hive.history;

-- Verificar o schema da tabela history:
DESCRIBE bancodemo.transacoes_cartao_iceberg_ctas_hue_hive.history

-- Verificar os snapshots dos últimos 5 dias
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue_hive.history
WHERE made_current_at >= date_sub(current_timestamp(), 5);

-- TIME TRAVEL
-- Consulta a partir de um snapshot ou tempo específico
SELECT * 
FROM bancodemo.transacoes_cartao_iceberg_ctas_hue_hive
FOR SYSTEM_TIME AS OF '2025-02-06 16:16:00'
LIMIT 10;

SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue_hive
FOR SYSTEM_VERSION AS OF ${snapshot_id_migration}
WHERE id_usuario = 1 AND estabelecimento = 'Mercado Bitcoin'
LIMIT 10;

/* ACID TRANSACTION
- Full ACID tables are created by default in Hive 3, while Impala creates INSERT-ONLY managed tables by default */
INSERT INTO bancodemo.transacoes_cartao_iceberg_ctas_hue_hive 
VALUES (1, '2024-06-24 15:10:06', 702.99, 'Mercado Bitcoin', 'Outros', 'Aprovada', '06-02-2025');

-- Verificar os snapshots da tabela após alteração
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue_hive.history;

-- Consulta com o novo snapshot_id
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue_hive
FOR SYSTEM_VERSION AS OF ${snapshot_id_update}
WHERE id_usuario = 1 AND estabelecimento = 'Mercado Bitcoin'
LIMIT 10;

-- Consulta com usando o creation_time - É importante que a clausula FOR esteja depois do SELECT
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue_hive
FOR SYSTEM_TIME AS OF '2025-02-06 20:44:00'
WHERE id_usuario = 1 AND estabelecimento = 'Mercado Bitcoin'
LIMIT 10;

-- Atualização
UPDATE bancodemo.transacoes_cartao_iceberg_ctas_hue_hive
SET valor = 510.99 
WHERE id_usuario = 1 AND estabelecimento = 'Mercado Bitcoin';

-- Validando a atualização
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue_hive
WHERE id_usuario = 1 AND estabelecimento = 'Mercado Bitcoin'
LIMIT 10;

-- Verificar os snapshots da tabela após alteração
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue_hive.history;

-- Validando das informações
SELECT * 
FROM bancodemo.transacoes_cartao_iceberg_ctas_hue_hive
WHERE id_usuario = 1;

-- Exclusão de dados
DELETE FROM bancodemo.transacoes_cartao_iceberg_ctas_hue_hive
WHERE id_usuario = 1;

-- Validando das informações após exclusão
SELECT * 
FROM bancodemo.transacoes_cartao_iceberg_ctas_hue_hive
WHERE id_usuario = 1;

-- TABLE ROLLBACK
-- Verificar os snapshots da tabela após alteração
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue_hive.history;

-- Vamos garantir que o formato de escrita seja Parquet e o número máximo de versões anteriores de metadados que devem ser mantidas.
ALTER TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue_hive 
SET TBLPROPERTIES('write.format.default'='parquet', 'write.metadata.previous-versions-max'='5');

-- Deve-se informar o snapshot-id do moento que deseja voltar, o campo parent_id auxilia nesse momento.
ALTER TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue_hive EXECUTE ROLLBACK(${snapshot_id_update});

-- Validando das informações após exclusão
SELECT * 
FROM bancodemo.transacoes_cartao_iceberg_ctas_hue_hive
WHERE id_usuario = 1;

-- O rollback é adicionado a lista para garantir rastreabilidade:
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue_hive.history;

-- IN-PLACE TABLE EVOLUTION:
-- Verificar as colunas antes da alteração:
DESCRIBE bancodemo.transacoes_cartao_iceberg_ctas_hue_hive;

ALTER TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue_hive ADD COLUMNS (limite_credito INT);

-- Verificar as colunas depois da alteração:
DESCRIBE bancodemo.transacoes_cartao_iceberg_ctas_hue_hive;

-- A sintaxe MERGE junta as tabelas transacoes_cartao e clientes com base na coluna id_usuario e atualiza a coluna limite_credito na tabela transacoes_cartao com o valor correspondente da tabela clientes.
USE bancodemo;

MERGE INTO transacoes_cartao_iceberg_ctas_hue_hive
USING clientes_iceberg_ctas_hue
ON transacoes_cartao_iceberg_ctas_hue_hive.id_usuario = clientes_iceberg_ctas_hue.id_usuario
WHEN MATCHED THEN UPDATE SET
  limite_credito = clientes_iceberg_ctas_hue.limite_credito;

-- Consulta de validação:
SELECT id_usuario, limite_credito
FROM transacoes_cartao_iceberg_ctas_hue_hive WHERE limite_credito > 2000 LIMIT 10;

/* Configurações da credencial da AWS:
rm -r ~/.aws/cli/cache
aws configure

* Colete as informações para configuração no portal da AWS.
  - Clique em Access Keys e vá para a última sessão e copie as seguintes informações AWS Access Key ID, AWS Secret Access Key, AWS Session token

* Configurações para a credencial:
```shell
AWS Access Key ID [None]: AWS_Access_Key  
AWS Secret Access Key [None]: AWS Secret_Access_Key
Default region name [None]: us-east-1
Default output format [None]: json
```

* Por fim adicione o token de sessão na credencaial:
```shell
aws configure set aws_session_token AWS_session_token
```

* Validar mais uma vez a credentials:
aws sts get-caller-identity
*/
-- Para verificar o antes e depois na camada de storage, você pode usar comandos do AWS CLI para listar os objetos no bucket S3 antes e após a otimização:
aws s3 ls s3://jcaseiro-aws-buk-e1c3ce14/data/warehouse/tablespace/external/hive/bancodemo.db/transacoes_cartao_iceberg_ctas_hue_hive/ --recursive --human-readable --summarize

Total Objects: 30
   Total Size: 34.1 MiB

-- Compacta arquivos pequenos, mescla deltas de exclusão e atualização, reescreve todos os arquivos, convertendo-os para o esquema mais recente da tabela, reescreve todas as partições de acordo com a especificação de partição mais recente
OPTIMIZE TABLE bancodemo.clientes_iceberg_ctas_hue;
  
-- Para verificar o antes e depois na camada de storage, você pode usar comandos do AWS CLI para listar os objetos no bucket S3 antes e após a otimização:
aws s3 ls s3://jcaseiro-aws-buk-e1c3ce14/data/warehouse/tablespace/external/hive/bancodemo.db/transacoes_cartao_iceberg_ctas_hue_hive/ --recursive --human-readable --summarize


---------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- ICEBERG MIGRATION IN-PLACE

-- Backup
CREATE TABLE bancodemo.clientes_inplace
AS SELECT * FROM bancodemo.clientes;
/*
Check if: 
Table Type: MANAGED_TABLE 
Table Parameters: transactional=True, transactional_properties=full_acid
*/

DESCRIBE FORMATTED bancodemo.categoria_estabelecimento
/*
- Use Hive to alter the table instead of Impala, as Hive fully supports ACID tables.
- If possible, convert the table to an INSERT-ONLY transactional table:
*/
ALTER TABLE bancodemo.categoria_estabelecimento SET TBLPROPERTIES ('EXTERNAL'='TRUE');

ALTER TABLE bancodemo.categoria_estabelecimento 
SET TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only');

ALTER TABLE bancodemo.categoria_estabelecimento
SET TBLPROPERTIES (
  'storage_handler' = 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler',
  'format-version' = '2'
);

DESCRIBE FORMATTED bancodemo.categoria_estabelecimento

ALTER TABLE bancodemo.categoria_estabelecimento RENAME TO bancodemo.categoria_estabelecimento_iceberg_inp_hue;

ALTER TABLE bancodemo.categoria_estabelecimento_bkp RENAME TO bancodemo.categoria_estabelecimento;
