-- Hive SQL
-- Runtime Version: 7.2.18-1.cdh7.2.18.p800.63673109
-- ICEBERG SHADOW MIGRATION

-- Create table as SELECT - HiveIcebergStorageHandler, é o mecanismo padrão para lidar com tabelas Iceberg no Hive. 
-- O suporte a Iceberg no Hive é feito através de um mecanismo diferente, geralmente usando o STORED BY em vez de STORED AS.
CREATE EXTERNAL TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue
PARTITIONED BY (data_execucao)
STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler'
TBLPROPERTIES ('format-version'='2')
AS SELECT * FROM bancodemo.transacoes_cartao;

-- Verificar os atributos da nova tabela Iceberg:
DESCRIBE FORMATTED bancodemo.transacoes_cartao_iceberg_ctas_hue;
-- Verificar os atributos da antiga para comparação:
DESCRIBE FORMATTED bancodemo.transacoes_cartao;

-- Validação de registros entre Origem e Destino da migração:
SELECT COUNT(*) FROM bancodemo.transacoes_cartao;
SELECT COUNT(*) FROM bancodemo.transacoes_cartao_iceberg_ctas_hue;

-- Validação de integridade dos dados entre Origem e Destino da migração:
SELECT * FROM bancodemo.transacoes_cartao LIMIT 10;
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue LIMIT 10;

-- Verificar os snapshots da tabela  Iceberg
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue.history;

-- Verificar o schema da tabela history:
DESCRIBE bancodemo.transacoes_cartao_iceberg_ctas_hue.history;

-- Verificar os snapshots dos últimos 5 dias
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue.history
WHERE made_current_at >= date_sub(current_timestamp(), 5);

-- TIME TRAVEL
-- Verificar os snapshots da tabela Iceberg
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue.history;

-- Consulta a partir de um momento da modificação:
SELECT * 
FROM bancodemo.transacoes_cartao_iceberg_ctas_hue
FOR SYSTEM_TIME AS OF ${system_time}
LIMIT 10;

-- Verificar os snapshots da tabela Iceberg
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue.history;

-- Consulta através do snapshot_id
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue
FOR SYSTEM_VERSION AS OF ${snapshot_id_migration}
WHERE id_usuario = '000000036' AND estabelecimento = 'Mercado Bitcoin';

/* ACID TRANSACTION
Full ACID tables are created by default in Hive 3, while Impala creates INSERT-ONLY managed tables by default */
-- Insert
INSERT INTO bancodemo.transacoes_cartao_iceberg_ctas_hue 
VALUES ('000000036', '2024-06-24 15:10:06', 702.99, 'Mercado Bitcoin', 'Outros', 'Aprovada', '06-02-2025');

-- Verificar os snapshots da tabela após alteração
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue.history;

-- Consulta com o novo snapshot_id
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue
FOR SYSTEM_VERSION AS OF ${snapshot_id_insert}
WHERE id_usuario = '000000036' AND estabelecimento = 'Mercado Bitcoin';

-- Consulta do mesmo registro usando o snapshot_id antigo
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue
FOR SYSTEM_VERSION AS OF ${snapshot_id_migration}
WHERE id_usuario = '000000036' AND estabelecimento = 'Mercado Bitcoin';

-- Atualização
UPDATE bancodemo.transacoes_cartao_iceberg_ctas_hue
SET valor = 510.99 
WHERE id_usuario = '000000036' AND estabelecimento = 'Mercado Bitcoin';

-- Validando a atualização
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue
WHERE id_usuario = '000000036' AND estabelecimento = 'Mercado Bitcoin';

-- Verificar os snapshots da tabela após alteração
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue.history;

-- Consulta com o novo snapshot_id e npvp valor
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue
FOR SYSTEM_VERSION AS OF ${snapshot_id_update}
WHERE id_usuario = '000000036' AND estabelecimento = 'Mercado Bitcoin';

-- Consulta do mesmo registro usando o snapshot_id antigo
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue
FOR SYSTEM_VERSION AS OF ${snapshot_id_insert}
WHERE id_usuario = '000000036' AND estabelecimento = 'Mercado Bitcoin';

-- Exclusão de dados
DELETE FROM bancodemo.transacoes_cartao_iceberg_ctas_hue
WHERE id_usuario = '000000036';

-- Verificar os snapshots da tabela após alteração
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue.history;

-- Consulta com o novo snapshot_id com o registro excluído
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue
FOR SYSTEM_VERSION AS OF ${snapshot_id_delete}
WHERE id_usuario = '000000036' AND estabelecimento = 'Mercado Bitcoin';

-- Consulta do mesmo registro usando o snapshot_id antigo
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue
FOR SYSTEM_VERSION AS OF ${snapshot_id_update}
WHERE id_usuario = '000000036' AND estabelecimento = 'Mercado Bitcoin';

-- TABLE ROLLBACK
-- Vamos garantir que o formato de escrita seja Parquet e o número máximo de versões anteriores de metadados que devem ser mantidas.
ALTER TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue
SET TBLPROPERTIES('write.format.default'='parquet', 'write.metadata.previous-versions-max'='5');

-- Deve-se informar o snapshot-id do momento que deseja voltar, o campo parent_id auxilia nesse momento.
ALTER TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue EXECUTE ROLLBACK(${snapshot_parent_id});

-- Validando das informações após exclusão
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue
WHERE id_usuario = '000000036';

-- O rollback é adicionado a lista de snapshots para garantir rastreabilidade:
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue.history;

-- IN-PLACE TABLE EVOLUTION:
-- Verificar as colunas antes da alteração:
DESCRIBE bancodemo.transacoes_cartao_iceberg_ctas_hue;

-- Adicionar a nova coluna
ALTER TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue ADD COLUMNS (limite_credito INT);

-- Verificar as colunas depois da alteração:
DESCRIBE bancodemo.transacoes_cartao_iceberg_ctas_hue;

/*A sintaxe MERGE junta as tabelas transacoes_cartao_iceberg_ctas_hue e clientes com base na coluna id_usuario e atualiza 
a coluna limite_credito na tabela transacoes_cartao_iceberg_ctas_hue com o valor correspondente da tabela clientes. */
USE bancodemo;

MERGE INTO transacoes_cartao_iceberg_ctas_hue
USING clientes
ON transacoes_cartao_iceberg_ctas_hue.id_usuario = clientes.id_usuario
WHEN MATCHED THEN UPDATE SET
  limite_credito = clientes.limite_credito;

-- Consulta de validação:
SELECT id_usuario, limite_credito
FROM transacoes_cartao_iceberg_ctas_hue WHERE limite_credito > 2000 LIMIT 10;

/* Compacta arquivos pequenos, mescla deltas de exclusão e atualização, reescreve todos os arquivos, convertendo-os 
para o esquema mais recente da tabela, reescreve todas as partições de acordo com a especificação de partição mais recente. */
OPTIMIZE TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue;

-- Funções disponíveis apenas no Hive
-- BRANCHING
-- Consultar branches:
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue.REFS;

-- Verificar os snapshots da tabela:
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue.history;

-- Criar um branch baseado em um snapshot específico:
ALTER TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue CREATE BRANCH after_insert FOR SYSTEM_VERSION AS OF ${snapshot_id_insert};

-- Criar um branch baseado em um timestamp específico:
ALTER TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue CREATE BRANCH after_update FOR SYSTEM_TIME AS OF ${system_time_update};

-- Criar um branch no estado atual da tabela:
ALTER TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue CREATE BRANCH current_prod;

-- Criar um branch na tabela com retenção de snapshots:
ALTER TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue
CREATE BRANCH after_migration FOR SYSTEM_VERSION AS OF ${snapshot_id_migration}
WITH SNAPSHOT RETENTION 5 SNAPSHOTS;

-- Consultar branches:
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue.REFS;

-- Leia e escreva dados isoladamente, sem impactar a tabela principal:
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue.branch_after_insert LIMIT 10;

-- Consultar branches:
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue.REFS;

-- Atualize o estado de uma branch para refletir outro snapshot ou branch:
ALTER TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue EXECUTE FAST-FORWARD 'current_prod' 'main';

ALTER TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue DROP BRANCH after_insert;

-- TAGGING
-- Criar uma tag para marcar um snapshot específico:
ALTER TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue CREATE TAG tag_insert FOR SYSTEM_VERSION AS OF ${snapshot_id_insert};

-- Consultar tags (Igual Branches):
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue.REFS;

-- Auditoria e Controle de Versão com tag:
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue FOR SYSTEM_VERSION AS OF 'tag_insert' LIMIT 10;


-- ICEBERG MIGRATION IN-PLACE
-- Verificar os atributos da tabela original:
DESCRIBE FORMATTED bancodemo.transacoes_cartao;

-- Coletar estatísticas:
ANALYZE TABLE bancodemo.transacoes_cartao COMPUTE STATISTICS;
ANALYZE TABLE bancodemo.transacoes_cartao COMPUTE STATISTICS FOR COLUMNS;

-- Convert the Table to Iceberg
ALTER TABLE bancodemo.transacoes_cartao CONVERT TO ICEBERG;

-- Verificar os atributos da nova tabela Iceberg:
DESCRIBE FORMATTED bancodemo.transacoes_cartao;

-- Otimizar tabela para melhorar o desempenho e a eficiência da tabela ao reorganizar os arquivos de dados subjacentes:
OPTIMIZE TABLE bancodemo.transacoes_cartao;