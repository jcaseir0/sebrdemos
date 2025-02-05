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

-- Validação entre Origem e Destino da migração:
SELECT COUNT(*) FROM bancodemo.clientes;
SELECT COUNT(*) FROM bancodemo.clientes_iceberg_ctas_hue;

-- Validação de integridade dos dados
SELECT * FROM bancodemo.clientes LIMIT 10;
SELECT * FROM bancodemo.clientes_iceberg_ctas_hue LIMIT 10;

-- Verificar os snapshots da tabela  Iceberg
DESCRIBE HISTORY bancodemo.clientes_iceberg_ctas_hue
--[FROM timestamp]
--[BETWEEN timestamp AND timestamp];

-- Verificar os snapshots dos últimos 5 dias
DESCRIBE HISTORY bancodemo.clientes_iceberg_ctas_hue FROM now() - interval 5 days;

-- Consulta a partir de um snapshot específico
SELECT * FROM bancodemo.clientes_iceberg_ctas_hue WHERE id_usuario = 1 FOR SYSTEM_VERSION AS OF 4306980727184187691 LIMIT 100;

-- ACID Transaction
INSERT INTO bancodemo.clientes_iceberg_ctas_hue 
VALUES (1, 'João Silva', 'joao@email.com', '1990-01-01', 'Rua A, 123', 5000, '1234-5678-9012-3456', 'SP');

-- Consulta a partir de um snapshot específico
SELECT * FROM bancodemo.clientes_iceberg_ctas_hue FOR SYSTEM_VERSION AS OF NEW SNAPSHOT LIMIT 100;

-- Insert de 50 linhas
INSERT INTO bancodemo.clientes_iceberg_ctas_hue
SELECT 
  CAST(RANDOM() * 1000000 AS INT) AS id_usuario,
  CONCAT('Nome_', CAST(RANDOM() * 1000 AS INT)) AS nome,
  CONCAT('email_', CAST(RANDOM() * 1000 AS INT), '@example.com') AS email,
  DATE_ADD(CURRENT_DATE(), CAST(RANDOM() * 365 * 50 AS INT)) AS data_nascimento,
  CONCAT('Endereco_', CAST(RANDOM() * 1000 AS INT)) AS endereco,
  CAST(RANDOM() * 10000 AS INT) AS limite_credito,
  CONCAT('Card_', CAST(RANDOM() * 1000000000 AS INT)) AS numero_cartao,
  CHR(CAST(65 + RANDOM() * 26 AS INT)) || CHR(CAST(65 + RANDOM() * 26 AS INT)) AS id_uf
FROM (SELECT 1) t
LATERAL VIEW EXPLODE(SEQUENCE(1, 50)) t AS n;

-- Atualização
UPDATE bancodemo.clientes_iceberg_ctas_hue 
SET limite_credito = 6000 
WHERE id_usuario = 1;

-- Atualização complexa
UPDATE bancodemo.clientes_iceberg_ctas_hue
SET limite_credito = limite_credito * 1.1
WHERE id_uf IN ('SP', 'RJ', 'MG') AND data_nascimento > '1990-01-01';

-- Exclusão de dados
DELETE FROM bancodemo.clientes_iceberg_ctas_hue 
WHERE id_usuario = 1;

-- Exclusão de dados complexos
DELETE FROM bancodemo.clientes_iceberg_ctas_hue
WHERE limite_credito < 1000 AND YEAR(data_nascimento) < 1980;

-- Time Travel
SELECT * FROM bancodemo.clientes_iceberg_ctas_hue 
FOR SYSTEM_TIME AS OF '2025-02-01 10:00:00' LIMIT 10;

-- Table Rollback
ALTER TABLE bancodemo.clientes_iceberg_ctas_hue 
SET TBLPROPERTIES('write.format.default'='parquet', 'write.metadata.previous-versions-max'='5');
ALTER TABLE bancodemo.clientes_iceberg_ctas_hue 
EXECUTE ROLLBACK TO SNAPSHOT <snapshot_id>;

-- In-place Table Evolution:
ALTER TABLE bancodemo.clientes_iceberg_ctas_hue ADD COLUMNS (score FLOAT);
UPDATE bancodemo.clientes_iceberg_ctas_hue SET score = RANDOM() * 100;

-- Consulta de validação:
SELECT id_usuario, nome, score FROM bancodemo.clientes_iceberg_ctas_hue WHERE score > 50 LIMIT 10;

-- Table Maintenance:
ALTER TABLE bancodemo.clientes_iceberg_ctas_hue 
EXECUTE OPTIMIZE;

-- Validar
OPTIMIZE bancodemo.clientes_iceberg_ctas_hue;

-- A otimização realiza compactação de arquivos pequenos, mescla deltas de exclusão e atualização, e reescreve os arquivos de acordo com o esquema e especificação de partição mais recentes

-- Para verificar o antes e depois na camada de storage, você pode usar comandos do AWS CLI para listar os objetos no bucket S3 antes e após a otimização:
aws s3 ls s3://bucket-name/data/bancodemo/clientes_iceberg_ctas_hue/ --recursive

-- ICEBERG MIGRATION IN-PLACE

-- Backup
CREATE TABLE bancodemo.categoria_estabelecimento_bkp
AS SELECT * FROM bancodemo.categoria_estabelecimento;

/*
- Impala supports reading from full ACID ORC tables, but cannot create, write to, or alter them
- Impala only supports INSERT-ONLY transactional tables for both read and write operations
- Full ACID tables are created by default in Hive 3, while Impala creates INSERT-ONLY managed tables by default
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
