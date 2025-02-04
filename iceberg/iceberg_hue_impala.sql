-- Work in progress
-- Impala SQL

-- Iceberg Shadow Migration

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
SELECT * FROM bancodemo.categoria_estabelecimento LIMIT 10;

-- Iceberg Migration In-place

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
