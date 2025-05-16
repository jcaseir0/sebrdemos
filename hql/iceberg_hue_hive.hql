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
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue
WHERE id_usuario = ${hivetableid} AND valor = ${hivetablevalor};

-- Outra forma de validar
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue
WHERE id_usuario IN (SELECT id_usuario FROM bancodemo.transacoes_cartao LIMIT 10)
AND valor IN (SELECT valor FROM bancodemo.transacoes_cartao LIMIT 10);

/* ACID TRANSACTION, TIME TRAVEL/SNAPSHOTS AND TAGGING
Full ACID tables are created by default in Hive 3, while Impala creates INSERT-ONLY managed tables by default */

-- Criar tag antes da operação de insert para controle de versão:
ALTER TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue 
CREATE TAG pre_insert;

-- Insert
INSERT INTO bancodemo.transacoes_cartao_iceberg_ctas_hue 
VALUES ('000000036', '2024-06-24 15:10:06', 702.99, 'Mercado Bitcoin', 'Outros', 'Aprovada', '06-02-2025');

-- Verificar os snapshots da tabela após alteração (Coletar o snapshot_id_insert:946620425798109442 snapshot_id_migration:9118237431331321253)
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue.history;

-- Consulta após adição de dados
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue
WHERE id_usuario = '000000036' AND estabelecimento = 'Mercado Bitcoin';

-- Ou consulta com o novo snapshot_id
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue
FOR SYSTEM_VERSION AS OF ${snapshot_id_insert}
WHERE id_usuario = '000000036' AND estabelecimento = 'Mercado Bitcoin';

-- Consulta do mesmo registro usando o snapshot_id antigo
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue
FOR SYSTEM_VERSION AS OF ${snapshot_id_migration}
WHERE id_usuario = '000000036' AND estabelecimento = 'Mercado Bitcoin';

-- Verificar o valor antes da atualização:
SELECT id_usuario, valor, estabelecimento FROM bancodemo.transacoes_cartao_iceberg_ctas_hue
WHERE id_usuario = '000000036' AND estabelecimento = 'Mercado Bitcoin';

-- Criar tag antes da operação de insert para controle de versão:
ALTER TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue 
CREATE TAG pre_update;

-- Atualização
UPDATE bancodemo.transacoes_cartao_iceberg_ctas_hue
SET valor = 510.99 
WHERE id_usuario = '000000036' AND estabelecimento = 'Mercado Bitcoin';

-- Validando a atualização
SELECT id_usuario, valor, estabelecimento FROM bancodemo.transacoes_cartao_iceberg_ctas_hue
WHERE id_usuario = '000000036' AND estabelecimento = 'Mercado Bitcoin';

-- Verificar os snapshots da tabela após alteração (Coletar o snapshot_id_update:5783489381005925215)
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue.history;

-- Consulta a versão atual da tabela
SELECT id_usuario, valor, estabelecimento FROM bancodemo.transacoes_cartao_iceberg_ctas_hue
WHERE id_usuario = '000000036' AND estabelecimento = 'Mercado Bitcoin';

-- Ou consulta com o novo snapshot_id
SELECT id_usuario, valor, estabelecimento FROM bancodemo.transacoes_cartao_iceberg_ctas_hue
FOR SYSTEM_VERSION AS OF ${snapshot_id_update}
WHERE id_usuario = '000000036' AND estabelecimento = 'Mercado Bitcoin';

-- Consulta do mesmo registro usando o snapshot_id antigo
SELECT id_usuario, valor, estabelecimento FROM bancodemo.transacoes_cartao_iceberg_ctas_hue
FOR SYSTEM_VERSION AS OF ${snapshot_id_insert}
WHERE id_usuario = '000000036' AND estabelecimento = 'Mercado Bitcoin';

-- Criar tag antes da operação de insert para controle de versão:
ALTER TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue 
CREATE TAG pre_delete;

-- Exclusão de dados
DELETE FROM bancodemo.transacoes_cartao_iceberg_ctas_hue
WHERE id_usuario = '000000036';

-- Verificar os snapshots da tabela após alteração (Coletar o snapshot_id_delete:4422857726429682268)
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue.history;

-- Consulta a versão atual da tabela
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue
WHERE id_usuario = '000000036' AND estabelecimento = 'Mercado Bitcoin';

-- Consulta do mesmo registro usando o snapshot_id antigo
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue
FOR SYSTEM_VERSION AS OF ${snapshot_id_update}
WHERE id_usuario = '000000036' AND estabelecimento = 'Mercado Bitcoin';

-- Criação da Tag para utilização mais a frente:
ALTER TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue 
CREATE TAG deleted;

-- Consultar tags:
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue.REFS;

-- IN-PLACE TABLE EVOLUTION:
-- Verificar as colunas antes da alteração:
DESCRIBE bancodemo.transacoes_cartao_iceberg_ctas_hue;
DESCRIBE bancodemo.clientes;

-- Adicionar a nova coluna
ALTER TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue ADD COLUMNS (limite_credito INT);

-- Verificar as colunas depois da alteração:
DESCRIBE bancodemo.transacoes_cartao_iceberg_ctas_hue;

-- Consultar os dados com a nova coluna:
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue LIMIT 10;

/*A sintaxe MERGE junta as tabelas transacoes_cartao_iceberg_ctas_hue e clientes com base na coluna id_usuario e atualiza 
a coluna limite_credito na tabela transacoes_cartao_iceberg_ctas_hue com o valor correspondente da tabela clientes. */
USE bancodemo;

-- Apenas para garantir a demo, NÃO RECOMENDADO em ambiente PRODUTIVO
SET hive.merge.cardinality.check=false;

-- A falta da referência da tabela no UPDATE SET é proposital:
MERGE INTO bancodemo.transacoes_cartao_iceberg_ctas_hue AS t
USING (
  SELECT 
    id_usuario,
    MAX(limite_credito) AS limite_credito
  FROM bancodemo.clientes
  GROUP BY id_usuario
) AS c
ON t.id_usuario = c.id_usuario
WHEN MATCHED THEN 
  UPDATE SET limite_credito = COALESCE(c.limite_credito, t.limite_credito);  -- Mantém valor existente se NULL na origem

-- Consulta de validação:
SELECT id_usuario, limite_credito
FROM transacoes_cartao_iceberg_ctas_hue WHERE limite_credito > 2000 LIMIT 10;

-- TIME TRAVEL
-- Verificar os snapshots da tabela Iceberg
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue.history;

-- Verificar o schema da tabela history:
DESCRIBE bancodemo.transacoes_cartao_iceberg_ctas_hue.history;

-- Verificar os snapshots dos últimos 5 dias
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue.history
WHERE made_current_at >= date_sub(current_timestamp(), 5);

-- Consulta a partir de um momento da modificação: (Coletar system_time:2025-05-15 14:31:16.785)
SELECT * 
FROM bancodemo.transacoes_cartao_iceberg_ctas_hue
FOR SYSTEM_TIME AS OF '${system_time}'
LIMIT 10;

-- Verificar os snapshots dos últimos 5 dias
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue.history
WHERE made_current_at >= date_sub(current_timestamp(), 5);

-- Consulta através do snapshot_id
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue
FOR SYSTEM_VERSION AS OF ${snapshot_id_migration}
WHERE id_usuario = '000000036' AND estabelecimento = 'Mercado Bitcoin';

-- TAGGING  (Disponível apenas no Hive)
-- Criar uma tag para marcar um snapshot específico:
ALTER TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue CREATE TAG tag_insert FOR SYSTEM_VERSION AS OF ${snapshot_id_insert};

-- Consultar tags:
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue.REFS;

-- Consulta usando tags
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue FOR SYSTEM_VERSION AS OF 'deleted' LIMIT 10;

-- Auditoria e Controle de Versão com tag:
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue 
FOR SYSTEM_VERSION AS OF 'deleted' 
WHERE id_usuario = '000000036' AND estabelecimento = 'Mercado Bitcoin' LIMIT 10;

-- TABLE ROLLBACK and TAGGING
-- Vamos garantir que o formato de escrita seja Parquet e o número máximo de versões anteriores de metadados que devem ser mantidas.
ALTER TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue
SET TBLPROPERTIES('write.format.default'='parquet', 'write.metadata.previous-versions-max'='5');

-- Listar os snapshots: (Coletar o snapshot_parent_id:5783489381005925215)
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue.history;

-- Ou para facilitar, utilize as TAGs:
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue.REFS;

-- Deve-se informar o snapshot-id do momento que deseja voltar, o campo parent_id auxilia nesse momento.
ALTER TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue EXECUTE ROLLBACK(${snapshot_parent_id});

-- Validando das informações após exclusão
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue
WHERE id_usuario = '000000036';

-- Criar tag para controle de versão:
ALTER TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue 
CREATE TAG rollbacked;

-- O rollback é adicionado a lista de snapshots para garantir rastreabilidade:
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue.history;

-- BRANCHING (Disponível apenas no Hive)
-- Consultar branches:
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue.REFS;

-- Verificar os snapshots da tabela:
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue.history;

-- Criar um branch baseado em um snapshot específico:
ALTER TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue CREATE BRANCH after_insert FOR SYSTEM_VERSION AS OF ${snapshot_id_insert};

-- Criar um branch baseado em um timestamp específico:
ALTER TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue CREATE BRANCH after_update FOR SYSTEM_TIME AS OF '${system_time_update}';

-- Criar um branch no estado atual da tabela:
ALTER TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue CREATE BRANCH current_prod;

-- Criar um branch na tabela com retenção de snapshots:
ALTER TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue
CREATE BRANCH after_migration FOR SYSTEM_VERSION AS OF ${snapshot_id_migration}
WITH SNAPSHOT RETENTION 5 SNAPSHOTS;

-- Consultar branches:
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue.REFS;

-- Criar uma branch de dev
ALTER TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue CREATE BRANCH dev_branch;

-- Escreva dados isoladamente, sem impactar a tabela principal:
INSERT INTO bancodemo.transacoes_cartao_iceberg_ctas_hue.branch_dev_branch 
VALUES ('000000123', CURRENT_TIMESTAMP(), 100.0, 'Loja XYZ', 'Eletrônicos', 'Aprovada', date_format(current_date(), 'dd-MM-yyyy'), 15000);

UPDATE bancodemo.transacoes_cartao_iceberg_ctas_hue.branch_dev_branch 
SET valor=250.5 
WHERE id_usuario = '000000123' AND estabelecimento = 'Loja XYZ';

DELETE FROM bancodemo.transacoes_cartao_iceberg_ctas_hue.branch_dev_branch 
WHERE id_usuario = '000000036';

-- Verificar o insert:
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue.branch_dev_branch 
WHERE id_usuario IN ('000000123','000000036');

-- Verificar o insert na tabela da branch main:
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue 
WHERE id_usuario IN ('000000123','000000036');

-- Sincronizar main com dev_branch
-- 1. Obter o snapshot_id da branch "dev_branch"
SELECT * 
FROM bancodemo.transacoes_cartao_iceberg_ctas_hue.refs 
WHERE name IN ('dev_branch', 'main');

-- 2. Atualizar a branch "main" para o snapshot da "dev_branch"
ALTER TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue
SET TBLPROPERTIES (
  'current-snapshot' = '7488007968421805673',
  'branch.main' = '5783489381005925215'
);

ALTER TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue EXECUTE FAST-FORWARD 'dev_branch' 'main';

-- Verificar a main atualizada
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue 
WHERE id_usuario = '000000123';

ALTER TABLE <database>.<tabela> CREATE BRANCH after_insert FOR SYSTEM_VERSION AS OF <snapshot_id>;

-- Leia e escreva dados isoladamente, sem impactar a tabela principal:
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue.branch_after_insert LIMIT 10;

-- Consultar branches:
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue.REFS;

-- PAra remover a branch e todas alterações:
ALTER TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue DROP BRANCH after_insert;

/* Compacta arquivos pequenos, mescla deltas de exclusão e atualização, reescreve todos os arquivos, convertendo-os 
para o esquema mais recente da tabela, reescreve todas as partições de acordo com a especificação de partição mais recente. */
OPTIMIZE TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue;

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