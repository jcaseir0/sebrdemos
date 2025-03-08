-- Work in progress
-- Impala SQL

-- ICEBERG SHADOW MIGRATION

-- Create table as SELECT
CREATE TABLE bancodemo.clientes_iceberg_ctas_hue
PARTITIONED BY (id_uf)
STORED AS ICEBERG
TBLPROPERTIES ('format-version'='2')
AS SELECT * FROM bancodemo.clientes;

-- Verificar os atributos da nova tabela Iceberg:
DESCRIBE FORMATTED bancodemo.clientes_iceberg_ctas_hue;
-- Verificar os atributos da antiga para comparação:
DESCRIBE FORMATTED bancodemo.clientes;

-- Validação entre Origem e Destino da migração:
SELECT COUNT(*) FROM bancodemo.clientes;
SELECT COUNT(*) FROM bancodemo.clientes_iceberg_ctas_hue;

-- Validação de integridade dos dados
SELECT * FROM bancodemo.clientes LIMIT 10;
SELECT * FROM bancodemo.clientes_iceberg_ctas_hue LIMIT 10;

-- Verificar a migração de uma tabela com bucketing para partitioning
SHOW PARTITIONS bancodemo.clientes_iceberg_ctas_hue;

-- Verificar os snapshots da tabela  Iceberg
DESCRIBE HISTORY bancodemo.clientes_iceberg_ctas_hue
--[FROM timestamp]
--[BETWEEN timestamp AND timestamp];

-- Verificar os snapshots dos últimos 5 dias
DESCRIBE HISTORY bancodemo.clientes_iceberg_ctas_hue 
FROM now() - interval 5 days;

-- TIME TRAVEL
-- Consulta a partir de um snapshot ou tempo específico
SELECT * 
FROM bancodemo.clientes_iceberg_ctas_hue
FOR SYSTEM_TIME AS OF '2025-02-06 16:03:00'
LIMIT 10;

SELECT * FROM bancodemo.clientes_iceberg_ctas_hue
FOR SYSTEM_VERSION AS OF 3935914403179409639
WHERE id_usuario = 1 AND nome = 'João Silva'
LIMIT 10;-- 3935914403179409639 é o timestamp do snapshot

/* ACID TRANSACTION
- Impala supports reading from full ACID ORC tables, but cannot create, write to, or alter them
- Impala only supports INSERT-ONLY transactional tables for both read and write operations */
INSERT INTO bancodemo.clientes_iceberg_ctas_hue 
VALUES (1, 'João Silva', 'joao@email.com', '1990-01-01', 'Rua A, 123', 5000, '1234-5678-9012-3456', 'SP');

-- Verificar o novo snapshot gerado pela alteração:
DESCRIBE HISTORY bancodemo.clientes_iceberg_ctas_hue

-- Consulta com o novo snapshot_id
SELECT * FROM bancodemo.clientes_iceberg_ctas_hue
FOR SYSTEM_VERSION AS OF 7674875328413401272
WHERE id_usuario = 1 AND nome = 'João Silva'
LIMIT 10;

-- Consulta com usando o creation_time
SELECT * FROM bancodemo.clientes_iceberg_ctas_hue
FOR SYSTEM_TIME AS OF '2025-02-06 17:25:00'
WHERE id_usuario = 1 AND nome = 'João Silva'
LIMIT 10;

-- É importante que a clausula FOR esteja depois do SELECT
SELECT * FROM bancodemo.clientes_iceberg_ctas_hue 
FOR SYSTEM_VERSION AS OF 7674875328413401272
ORDER BY 1
LIMIT 10;

-- TABLE ROLLBACK
SELECT * FROM bancodemo.clientes_iceberg_ctas_hue
WHERE id_usuario = 2 AND nome = 'Leonardo Gardim'
LIMIT 10;

-- Insert do dado procurado anteriormente 
INSERT INTO bancodemo.clientes_iceberg_ctas_hue 
VALUES (2, 'Leonardo Gardim', 'lgardim@email.com', '1990-01-01', 'Rua C, 127', 7000, '4321-8765-2109-6543', 'AM');

-- Verificar o novo snapshot gerado pela alteração:
DESCRIBE HISTORY bancodemo.clientes_iceberg_ctas_hue

SELECT * FROM bancodemo.clientes_iceberg_ctas_hue
WHERE id_usuario = 2 AND nome = 'Leonardo Gardim'
LIMIT 10;

-- Verificar o snapshot
DESCRIBE HISTORY bancodemo.clientes_iceberg_ctas_hue

-- Vamos garantir que o formato de escrita seja Parquet e o número máximo de versões anteriores de metadados que devem ser mantidas.
ALTER TABLE bancodemo.clientes_iceberg_ctas_hue 
SET TBLPROPERTIES('write.format.default'='parquet', 'write.metadata.previous-versions-max'='5');

-- Deve-se informar o snapshot-id do moento que deseja voltar, o campo parent_id auxilia nesse momento.
ALTER TABLE bancodemo.clientes_iceberg_ctas_hue EXECUTE ROLLBACK(7674875328413401272);

-- O rollback é adicionado a lista para garantir rastreabilidade:
DESCRIBE HISTORY bancodemo.clientes_iceberg_ctas_hue

-- Informação foi removida
SELECT * FROM bancodemo.clientes_iceberg_ctas_hue
WHERE id_usuario = 2 AND nome = 'Leonardo Gardim'
LIMIT 10;

-- IN-PLACE TABLE EVOLUTION:
-- Verificar as colunas antes da alteração:
DESCRIBE bancodemo.clientes_iceberg_ctas_hue;

ALTER TABLE bancodemo.clientes_iceberg_ctas_hue ADD COLUMNS (score FLOAT);

-- Verificar as colunas depois da alteração:
DESCRIBE bancodemo.clientes_iceberg_ctas_hue;

-- Como o Impala não faz update, essa consulta deve-se executar no Hive
UPDATE bancodemo.clientes_iceberg_ctas_hue SET score = RAND() * 100;

-- Consulta de validação:
SELECT id_usuario, nome, score FROM bancodemo.clientes_iceberg_ctas_hue WHERE score > 50 LIMIT 10;

-- TABLE MAINTENANCE
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
aws s3 ls s3://jcaseiro-aws-buk-e1c3ce14/data/warehouse/tablespace/external/hive/bancodemo.db/clientes_iceberg_ctas_hue/ --recursive --human-readable --summarize

-- Compacta arquivos pequenos, mescla deltas de exclusão e atualização, reescreve todos os arquivos, convertendo-os para o esquema mais recente da tabela, reescreve todas as partições de acordo com a especificação de partição mais recente
OPTIMIZE TABLE bancodemo.clientes_iceberg_ctas_hue;
  
-- Para verificar o antes e depois na camada de storage, você pode usar comandos do AWS CLI para listar os objetos no bucket S3 antes e após a otimização:
aws s3 ls s3://jcaseiro-aws-buk-e1c3ce14/data/warehouse/tablespace/external/hive/bancodemo.db/clientes_iceberg_ctas_hue/ --recursive --human-readable --summarize
---------------------------------------------------------------------------------------------------------------------------------------------------------------------