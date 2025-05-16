-- Impala SQL
-- Runtime Version: 7.2.18-1.cdh7.2.18.p800.63673109
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
SELECT * FROM bancodemo.clientes 
WHERE id_usuario IN ('896797859', '284689128', '103946766', '648027188', '187525572', '909350817', '091759804', '687691239', '951031954', '810429067');
SELECT * FROM bancodemo.clientes_iceberg_ctas_hue
WHERE id_usuario IN ('896797859', '284689128', '103946766', '648027188', '187525572', '909350817', '091759804', '687691239', '951031954', '810429067')
ORDER BY 2;

-- Verificar a migração de uma tabela com bucketing para partitioning
SHOW PARTITIONS bancodemo.clientes_iceberg_ctas_hue;

-- Verificar os snapshots da tabela  Iceberg
DESCRIBE HISTORY bancodemo.clientes_iceberg_ctas_hue
--[FROM timestamp]
--[BETWEEN timestamp AND timestamp];

-- Verificar os snapshots dos últimos 5 dias
DESCRIBE HISTORY bancodemo.clientes_iceberg_ctas_hue 
FROM now() - interval 5 days;

/* ACID TRANSACTION
- Impala supports reading from full ACID ORC tables, but cannot create, write to, or alter them
- Impala only supports INSERT-ONLY transactional tables for both read and write operations */
INSERT INTO bancodemo.clientes_iceberg_ctas_hue 
VALUES ('000000035', 'João Silva', 'joao@email.com', '1990-01-01', 'Rua A, 123', 5000, '1234-5678-9012-3456', 'SP');

-- Verificar o novo snapshot gerado pela alteração:
DESCRIBE HISTORY bancodemo.clientes_iceberg_ctas_hue

-- Consulta com o novo snapshot_id
SELECT * FROM bancodemo.clientes_iceberg_ctas_hue
FOR SYSTEM_VERSION AS OF ${snapshot_id_insert}
WHERE id_usuario = '000000035' AND nome = 'João Silva';

-- Verificar o novo snapshot gerado pela alteração:
DESCRIBE HISTORY bancodemo.clientes_iceberg_ctas_hue

-- Consulta com usando o creation_time
SELECT * FROM bancodemo.clientes_iceberg_ctas_hue
FOR SYSTEM_TIME AS OF ${system_time}
WHERE id_usuario = '000000035' AND nome = 'João Silva';

-- Verificar o novo snapshot gerado pela alteração:
DESCRIBE HISTORY bancodemo.clientes_iceberg_ctas_hue

-- Consulta da versão anterior do INSERT (É importante que a clausula FOR esteja depois do SELECT)
SELECT * FROM bancodemo.clientes_iceberg_ctas_hue 
FOR SYSTEM_VERSION AS OF ${snapshot_id_migration}
WHERE id_usuario = '000000035' AND nome = 'João Silva';

-- TABLE ROLLBACK
-- Vamos garantir que o formato de escrita seja Parquet e o número máximo de versões anteriores de metadados que devem ser mantidas.
ALTER TABLE bancodemo.clientes_iceberg_ctas_hue 
SET TBLPROPERTIES('write.format.default'='parquet', 'write.metadata.previous-versions-max'='5');

-- Verificar se o registro existe
SELECT * FROM bancodemo.clientes_iceberg_ctas_hue
WHERE id_usuario = '000000002' AND nome = 'Leonardo Gardom';

-- Insert do dado procurado anteriormente 
INSERT INTO bancodemo.clientes_iceberg_ctas_hue 
VALUES ('000000002', 'Leonardo Gardom', 'lgardom@email.com', '1990-01-01', 'Rua C, 127', 7000, '4321-8765-2109-6543', 'AM');

-- Verificar se o registro existe
SELECT * FROM bancodemo.clientes_iceberg_ctas_hue
WHERE id_usuario = '000000002' AND nome = 'Leonardo Gardom';

-- Verificar o novo snapshot gerado pela alteração:
DESCRIBE HISTORY bancodemo.clientes_iceberg_ctas_hue

-- Deve-se informar o snapshot-id anterior ao snapshot-id do INSERT, ou o campo parent_id, que auxilia nesse momento.
ALTER TABLE bancodemo.clientes_iceberg_ctas_hue EXECUTE ROLLBACK(${snapshot_parent_id});

-- Verificar se o registro existe
SELECT * FROM bancodemo.clientes_iceberg_ctas_hue
WHERE id_usuario = '000000002' AND nome = 'Leonardo Gardom';

-- O rollback é adicionado a lista para garantir rastreabilidade:
DESCRIBE HISTORY bancodemo.clientes_iceberg_ctas_hue

-- TIME TRAVEL
-- Consulta a partir de um snapshot ou tempo específico
SELECT * 
FROM bancodemo.clientes_iceberg_ctas_hue
FOR SYSTEM_TIME AS OF ${system_time}
LIMIT 10;

-- Verificar os snapshot_ids gerados:
DESCRIBE HISTORY bancodemo.clientes_iceberg_ctas_hue

-- Consultar as informações baseado no snapshot_id
SELECT * FROM bancodemo.clientes_iceberg_ctas_hue
FOR SYSTEM_VERSION AS OF ${snapshot_id_insert}
WHERE id_usuario = '000000035' AND nome = 'João Silva';-- 3935914403179409639 é o timestamp do snapshot

-- IN-PLACE TABLE EVOLUTION:
-- Verificar as colunas antes da alteração:
DESCRIBE bancodemo.clientes_iceberg_ctas_hue;

-- Adicionar uma nova coluna:
ALTER TABLE bancodemo.clientes_iceberg_ctas_hue ADD COLUMNS (score FLOAT);

-- Verificar as colunas depois da alteração:
DESCRIBE bancodemo.clientes_iceberg_ctas_hue;

-- Consultar a tabela para validar que o acesso foi mantido:
SELECT * FROM bancodemo.clientes_iceberg_ctas_hue LIMIT 10;

-- Como o Impala não faz update, essa consulta deve-se executar no Hive
INSERT INTO bancodemo.clientes_iceberg_ctas_hue score = RAND() * 100;

-- Consulta de validação:
SELECT id_usuario, nome, score FROM bancodemo.clientes_iceberg_ctas_hue WHERE score > 50 LIMIT 10;

-- Verificar os snapshot_ids gerados:
DESCRIBE HISTORY bancodemo.clientes_iceberg_ctas_hue

-- Remover uma coluna existente:
ALTER TABLE bancodemo.clientes_iceberg_ctas_hue DROP COLUMN score;

-- Verificar os snapshot_ids gerados:
DESCRIBE HISTORY bancodemo.clientes_iceberg_ctas_hue

-- TABLE MAINTENANCE
-- Otimizar tabela para melhorar o desempenho e a eficiência da tabela ao reorganizar os arquivos de dados subjacentes:
OPTIMIZE TABLE bancodemo.clientes_iceberg_ctas_hue;

-- ICEBERG MIGRATION IN-PLACE
-- Verificar os atributos da tabela original:
DESCRIBE FORMATTED bancodemo.clientes;

-- Convert the Table to Iceberg
ALTER TABLE bancodemo.clientes CONVERT TO ICEBERG;
ALTER TABLE bancodemo.clientes SET TBLPROPERTIES('format-version'='2');

-- Verificar os atributos da nova tabela Iceberg:
DESCRIBE FORMATTED bancodemo.clientes;

-- Otimizar tabela para melhorar o desempenho e a eficiência da tabela ao reorganizar os arquivos de dados subjacentes:
OPTIMIZE TABLE bancodemo.clientes;