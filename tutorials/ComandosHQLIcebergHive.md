# Laboratório Iceberg + Hive

Este documento apresenta uma explicação detalhada de cada comando HQL (Hive Query Language) presente no script fornecido, organizado por tópicos. Cada comando é apresentado em uma caixa de código SQL, seguido de uma explicação clara sobre seu propósito e funcionamento.

## 1. Criação de Tabela Iceberg com CTAS (CREATE TABLE AS SELECT)

Cria uma tabela externa Iceberg no Hive, particionada por `data_execucao`, usando o storage handler do Iceberg. O comando copia todos os dados da tabela original `transacoes_cartao` para a nova tabela Iceberg, já no formato Iceberg e na versão 2 do formato.

Se atente para preencher a caixa de texto da variável com o nome do seu banco de dados: `bancodemo_userXXX`

```sql
CREATE EXTERNAL TABLE ${database}.transacoes_cartao_iceberg_ctas_hue
PARTITIONED BY (data_execucao)
STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler'
TBLPROPERTIES ('format-version'='2')
AS SELECT * FROM ${database}.transacoes_cartao;
```

> [!Note]
> No Hue é possível utilizar essa estrutura de variável: ${variavel} que abre uma caixa de texto para manter a flexibilidade do uso das consultas.
> **Observação:** Cláusula diferente para o Impala = STORED BY

## 2. Verificação de Metadados das Tabelas

Mostra os detalhes e propriedades das tabelas, como tipo de armazenamento, particionamento, localização e propriedades do Iceberg. Útil para comparar atributos entre a tabela original e a migrada.

> [!Note]
> **Observação:** Observe que para a tabela Iceberg terá um parâmetro especificando o tipo de tabela: `Table Parameters`:`table_type`:`ICEBERG`

```sql
DESCRIBE FORMATTED ${database}.transacoes_cartao;
```

```sql
DESCRIBE FORMATTED ${database}.transacoes_cartao_iceberg_ctas_hue;
```

## 3. Validação de Registros

Conta o número de registros em cada tabela, permitindo validar se a migração copiou todos os dados corretamente.

```sql
SELECT COUNT(*) FROM ${database}.transacoes_cartao;
```

```sql
SELECT COUNT(*) FROM ${database}.transacoes_cartao_iceberg_ctas_hue;
```

## 4. Validação de Integridade

Exibe amostras de dados das duas tabelas para validação manual e compara registros específicos usando o id_usuario da consulta da tabela antes da conversão.

> [!Note]
> Após a primeira consulta, escolha uma linha e colete os dados das colunas id_usuario, valor e estabelecimento antes de executar a segunda consulta. Serão utilizados na segunda consulta.

```sql
SELECT * FROM ${database}.transacoes_cartao LIMIT 10;
```

```sql
SELECT id_usuario AS ID, valor AS VALOR, estabelecimento AS NOME_ESTABELECIMENTO 
FROM ${database}.transacoes_cartao_iceberg_ctas_hue
WHERE id_usuario = ${hivetableid} AND valor = ${hivetablevalor};
```

## 5. Validação Cruzada

Compara registros entre as tabelas, usando subconjuntos de valores da tabela antes da conversão, útil para checagem cruzada de integridade após migração.

```sql
SELECT id_usuario AS ID, valor AS VALOR, estabelecimento AS NOME_ESTABELECIMENTO, categoria AS CATEGORIA 
FROM ${database}.transacoes_cartao_iceberg_ctas_hue
WHERE id_usuario IN (SELECT id_usuario FROM ${database}.transacoes_cartao LIMIT 10)
AND valor IN (SELECT valor FROM ${database}.transacoes_cartao LIMIT 10);
```

## 6. Controle de Versão com TAGs

A funcionalidade de Tagging do Iceberg no Hive permite criar rótulos imutáveis (tags) para snapshots específicos de uma tabela Iceberg, facilitando o controle de versões e o rastreamento de retenção de dados para auditorias e conformidade (por exemplo, com GDPR).

**Como funciona o Tagging**

- Tags identificam snapshots importantes e ajudam a proteger versões de tabela contra deleção automática, possibilitando retenção por tempo determinado.
- É possível criar tags usando comandos SQL no Hive, baseando-se em versões (snapshot IDs), timestamp, ou no branch atual.
- Consultas SQL podem referenciar um tag diretamente, facilitando acesso a dados históricos sem manipular IDs de snapshot.

**Benefícios**

- Facilita a auditoria, conformidade e reprodução isolada de dados.
- Simplifica a recuperação e referência a versões sem a necessidade de lidar com IDs complexos de snapshots.
- Ajuda a implementar políticas de retenção e proteção de dados conforme exigências regulatórias.

**Laboratório:** Criar uma tag antes de operações críticas, permitindo rastrear e voltar a este ponto posteriormente de forma facilitada.

```sql
ALTER TABLE ${database}.transacoes_cartao_iceberg_ctas_hue
CREATE TAG pre_insert;
```

## 7. Inserção de Dados

Insere um novo registro na tabela Iceberg, simulando uma transação de cartão.

```sql
INSERT INTO ${database}.transacoes_cartao_iceberg_ctas_hue
VALUES ('000000036', '2024-06-24 15:10:06', 702.99, 'Mercado Bitcoin', 'Outros', 'Aprovada', '06-02-2025');
```

É possível consultar Tags e Branches criadas assim como o snapshot que está vinculado com o objeto:

> [!Note]
> No resultado da consulta a seguir, sempre a BRANCH main será a sua versão corrente.

```sql
SELECT * FROM ${database}.transacoes_cartao_iceberg_ctas_hue.refs;
```

## 8. Consulta de Histórico de Snapshots

Lista todos os snapshots da tabela, permitindo auditoria e time travel.

```sql
SELECT * FROM ${database}.transacoes_cartao_iceberg_ctas_hue.history;
```

E consultar o dado recem criado na versão atual do seu snapshot:

```sql
SELECT id_usuario AS ID, valor AS VALOR, estabelecimento AS NOME_ESTABELECIMENTO, categoria AS CATEGORIA
FROM ${database}.transacoes_cartao_iceberg_ctas_hue
WHERE estabelecimento = 'Mercado Bitcoin';
```

## Como funcionam os Snapshots no Iceberg?

Os snapshots no Iceberg funcionam como versões imutáveis de uma tabela, criadas automaticamente a cada operação de escrita, como INSERT, UPDATE ou MERGE. Cada snapshot representa o estado exato da tabela em um momento, sendo essencial para processos de auditoria, conformidade e recuperação de dados.

### Operações comuns com Snapshots (Apenas para conhecimento, não faz parte do laboratório)

- Cada modificação na tabela gera um novo snapshot, acumulando um histórico de alterações.
- É possível consultar a tabela em versões antigas usando o snapshot correspondente, viabilizando queries de "viagem no tempo".
- Pode-se definir qual snapshot será usado como referência, modificando o estado atual da tabela para aquele ponto histórico:
  ```sql
  ALTER TABLE <nome_tabela> EXECUTE SET_CURRENT_SNAPSHOT (<snapshot_id>);
  ```
- Snapshots podem ser expirados/removidos para evitar crescimento excessivo do metadados e garantir compliance, inclusive selecionando por data, ID ou faixa de tempo:
  ```sql
  ALTER TABLE <nome_tabela> EXECUTE EXPIRE_SNAPSHOTS('<snapshot_id>');
  ALTER TABLE <nome_tabela> EXECUTE EXPIRE_SNAPSHOTS('2022-08-15 13:50:00');
  ```
- É possível configurar o mínimo de snapshots a manter usando a propriedade `history.expire.min-snapshots-to-keep`, protegendo versões recentes por intervalo ou quantidade.

Definição de propriedades como formato padrão de escrita (Parquet) e número máximo de versões de metadados a serem mantidas iguais a 5:

```sql
ALTER TABLE ${database}.transacoes_cartao_iceberg_ctas_hue
SET TBLPROPERTIES('write.format.default'='parquet', 'write.metadata.previous-versions-max'='5');
```

### Benefícios dos snapshots

- Permitem recuperação rápida de dados após erros ou incidentes.
- Facilitam auditoria e conformidade, já que cada estado da tabela pode ser acessado e mantido conforme política regulatória.[1][5]
- Evitam leituras e escritas diretas em arquivos, mantendo metadados otimizados no sistema.[2][5]

## 9. Consulta com snapshot específico usando o time travel

Consultar as Tags e o snapshot para executar a próxima consulta:

```sql
SELECT * FROM ${database}.transacoes_cartao_iceberg_ctas_hue.refs
WHERE type = 'TAG';
```

Consulta a tabela como ela estava em um determinado snapshot, útil para auditoria e recuperação de versões anteriores.

Efetuar a consulta para encontrar a informação do `id_usuario = '000000036'` com o snapshot_id da TAG pre_insert:

```sql
SELECT * FROM ${database}.transacoes_cartao_iceberg_ctas_hue
FOR SYSTEM_VERSION AS OF ${snapshot_id_preinsert}
WHERE id_usuario = '000000036' AND estabelecimento = 'Mercado Bitcoin';
```

Nenhum dado será encontrado, pois o insert ainda não havia sido executado. Mas no current_snapshot_id, o dado é encontrado.

Lista todos os snapshots da tabela e coletar o último snapshot_id para executar a próxima consulta:

```sql
SELECT * FROM ${database}.transacoes_cartao_iceberg_ctas_hue.history;
```

```sql
SELECT * FROM ${database}.transacoes_cartao_iceberg_ctas_hue
FOR SYSTEM_VERSION AS OF ${current_snapshot_id}
WHERE id_usuario = '000000036' AND estabelecimento = 'Mercado Bitcoin';
```

## Full ACID no Hive com Apache Iceberg

O termo **ACID** (Atomicity, Consistency, Isolation, Durability) é fundamental em sistemas de banco de dados e garante transações de dados **confiáveis**.

O Apache Hive (um componente central do Cloudera Data Platform - CDP) suporta transações **Full ACID** desde as versões mais recentes, o que permite operações de **UPDATE**, **DELETE**, e **MERGE** de forma eficiente e segura, além do tradicional **INSERT**.

### Por que o Iceberg é relevante?

Tradicionalmente, o Hive utiliza formatos como ORC e Parquet e gerencia o ACID com um mecanismo chamado **"record-level writes"** e **"compaction"** (escritas em nível de registro e compactação).

O Apache Iceberg é um formato de tabela de código aberto que foi projetado para resolver as deficiências de formatos de tabela mais antigos, especialmente em ambientes de data lake em crescimento massivo.O Iceberg gerencia os metadados e os arquivos de dados de uma forma que garante que as transações (incluindo UPSERTs complexos) sejam Atômicas e Consistentes. Isso significa que ou toda a operação é concluída (commit), ou nenhuma parte dela é (rollback), mesmo em caso de falha.

A Cloudera utiliza a capacidade **Full ACID do Hive** (e de outros engines como o Spark/Impala) em conjunto com o formato **Iceberg** para oferecer uma experiência de Data Lakehouse robusta, permitindo que a plataforma trate o armazenamento de dados (HDFS/Ozone/S3/ADLS) com a mesma confiabilidade transacional de um banco de dados tradicional.

## 10. Marcação com Tagging do momento corrente

Marca o estado corrente do objeto com uma tag:

```sql
ALTER TABLE ${database}.transacoes_cartao_iceberg_ctas_hue
CREATE TAG pre_update;
```

Lista as tags criadas e verifica o valor antes da atualização:

```sql
SELECT * FROM ${database}.transacoes_cartao_iceberg_ctas_hue.refs
WHERE type = 'TAG';
```

```sql
SELECT * FROM ${database}.transacoes_cartao_iceberg_ctas_hue
WHERE id_usuario = '000000036' AND estabelecimento = 'Mercado Bitcoin';
```

Atualização do valor de uma transação específica:

```sql
UPDATE ${database}.transacoes_cartao_iceberg_ctas_hue
SET valor = 510.99
WHERE id_usuario = '000000036' AND estabelecimento = 'Mercado Bitcoin';
```

Nova validação do valor:

```sql
SELECT * FROM ${database}.transacoes_cartao_iceberg_ctas_hue
WHERE id_usuario = '000000036' AND estabelecimento = 'Mercado Bitcoin';
```

Caso queira, é possível rodar o [exercício 9](#9-consulta-com-snapshot-específico-usando-o-time-travel) novamente com os novos valores dos snapshot_ids para validação.

## 11. Exclusão de Dados

Criação de uma tag antes da exclusão, listagem das tags criadas e verifica o valor antes da exclusão:

```sql
ALTER TABLE ${database}.transacoes_cartao_iceberg_ctas_hue
CREATE TAG pre_delete;
```

```sql
SELECT * FROM ${database}.transacoes_cartao_iceberg_ctas_hue.refs
WHERE type = 'TAG';
```

```sql
SELECT * FROM ${database}.transacoes_cartao_iceberg_ctas_hue
WHERE id_usuario = '000000036' AND estabelecimento = 'Mercado Bitcoin';
```

Remoção do registro de um usuário específico e validação:

```sql
DELETE FROM ${database}.transacoes_cartao_iceberg_ctas_hue
WHERE id_usuario = '000000036';
```

```sql
SELECT * FROM ${database}.transacoes_cartao_iceberg_ctas_hue
WHERE id_usuario = '000000036' AND estabelecimento = 'Mercado Bitcoin';
```

O registro não deverá ser encontrado, pois efetuamos a exclusão.

Observe que um novo snapshot é gerado, caso queira, valide novamente conforme o [exercício 9](#9-consulta-com-snapshot-específico-usando-o-time-travel).

## 12. Evolução de Esquema (Schema Evolution)

O schema evolution no Iceberg permite alterar dinamicamente a estrutura das tabelas, possibilitando adaptar o modelo conforme novos requisitos, sem afetar a leitura dos dados antigos.

### Principais funcionalidades

- É possível adicionar, remover, renomear, atualizar o tipo ou reordenar colunas usando comandos SQL do Hive (ALTER TABLE).
- Mudanças são realizadas no metadado (metadata.json), tornando o processo eficiente e sem necessidade de reescrever arquivos de dados.
- Cada coluna possui um identificador único, o que garante integridade e evita problemas em operações como remoções e renomeações.
- Alterações seguras de tipo, como int para long, float para double, ou aumento de precisão de decimal, são permitidas.
- Após a evolução do schema, o metadado é atualizado e um commit é registrado, mas não é criado um novo snapshot da tabela.

### Benefícios

- Permite atualização e adaptação contínua dos modelos de dados, inclusive em produção.
- Garante leitura consistente dos dados, mesmo após múltiplas mudanças no schema.
- Evita custos e interrupções causadas por reprocessamento de grandes volumes de dados.

As limitações do schema evolution no Iceberg incluem principalmente restrições de tipos de alterações, suporte parcial a mudanças e considerações específicas para garantir integridade dos dados.

### Limitações principais

- Alterações consideradas inseguras, que exigiriam atualização linha a linha dos dados, não são permitidas (ex.: alterações complexas de tipo que não sejam ampliação segura como int para long).
- Tipos de mudança suportados incluem adicionar colunas, renomear, remover e mudar tipos de forma segura, mas renomeação não é completamente transparente em todos os cenários e pode causar problemas em alguns formatos legados.
- Em engines ou formatos que usam posição (ex: CSV/TSV), schema evolution é limitado ou não suportado, pois alterações podem causar deslocamento incorreto de dados.
- Mudanças feitas fora do Hive (por exemplo, via Spark) devem ser sincronizadas para refletir no schema do Hive/Impala e vice-versa, sendo essa sincronização um ponto de atenção.

### O Hive Metastore (HMS) como Fonte da Verdade

No CDP, o Hive Metastore (HMS) atua como o catálogo de tabelas central para todos os três engines.

Para tabelas Iceberg, o HMS não armazena o schema da tabela em si (Iceberg faz isso em seus arquivos de metadados), mas armazena um ponteiro crucial: o caminho do arquivo de metadados mais recente do Iceberg (metadata file pointer).

A sincronização se resume a garantir que, após uma alteração (e.g., adição de uma coluna ou novos dados) ser concluída por um engine (e.g., Spark), os outros engines (Hive/Impala) leiam o novo ponteiro de metadados do HMS.

O **Impala** é o ponto mais crítico e onde a sincronização manual é obrigatória, diferente do **Spark** e **Hive**, após alterações externas. O Impala mantém um cache de metadados persistente em seus Daemons para garantir baixíssima latência nas consultas. Se o Spark atualizar a tabela no HMS, o Impala Daemon continuará usando a versão antiga em seu cache interno até que seja notificado. O administrador ou o usuário deve forçar o Impala a recarregar o schema do HMS.

| Comando Impala | Uso e Efeito |
| :--- | :---: |
| `INVALIDATE METADATA table_name;` | Recomendado para mudanças de schema. Limpa o cache de metadados da tabela em todos os Impala Daemons. Isso força o Impala a reler todas as informações da tabela (incluindo o novo schema e o novo ponteiro Iceberg) no HMS. É o comando mais seguro para mudanças estruturais. |
| `REFRESH table_name;` | Recomendado para adição de novos dados. Mais leve que o INVALIDATE METADATA. Geralmente suficiente para Iceberg/Parquet quando apenas novos dados foram adicionados, mas a estrutura da tabela (schema) permaneceu a mesma. |

**Laboratório:** Verificar o schema antes, adicionar uma nova coluna à tabela Iceberg de forma dinâmica, sem recriar a tabela e validar o novo schema:

```sql
DESCRIBE FORMATTED ${database}.transacoes_cartao_iceberg_ctas_hue
```

```sql
ALTER TABLE ${database}.transacoes_cartao_iceberg_ctas_hue ADD COLUMNS (limite_credito INT);
```

```sql
DESCRIBE FORMATTED ${database}.transacoes_cartao_iceberg_ctas_hue
```

Verificar também que a nova coluna está com os valores nulos:

```sql
SELECT id_usuario AS ID, valor AS VALOR, estabelecimento AS NOME_ESTABELECIMENTO, categoria AS CATEGORIA, limite_credito AS LIMITE_DO_CARTAO
FROM ${database}.transacoes_cartao_iceberg_ctas_hue
LIMIT 10
```

## 13. Atualização em Massa com MERGE

A cláusula MERGE no Hive usando Iceberg permite unir dados de uma tabela fonte com uma tabela destino, realizando atualizações, inserções ou exclusões condicionais com base em uma condição de junção. As operações de merge com Iceberg provê atomicidade e isolamento, aproveitando o gerenciamento de metadados.

**Detalhes relevantes relacionados a chaves primárias**

- Embora seja possível usar colunas para identificar unicamente linhas em operações como MERGE, essas chaves são lógicas para as operações e não garantem restrições físicas de unicidade.
- O Iceberg gerencia integridade e consistência por meio de snapshots e metadados, e não pelo modelo tradicional de chave primária.
- Em algumas integrações, a lógica de unicidade pode ser implementada via aplicação ou processos ETL que usam o Iceberg, mas isso fica fora do controle nativo do formato.
- O Hive tradicional não suporta criação de constraints físicas (como primary keys), nem o Iceberg no Hive da Cloudera adiciona essa funcionalidade disponível hoje.

**Laboratório:** Atualiza a coluna `limite_credito` na tabela Iceberg com valores vindos da tabela de clientes, usando merge (**UPSERT** em outras distribuições).

> [!Note]
> Essa execução é um pouco mais demorada

```sql
MERGE INTO ${database}.transacoes_cartao_iceberg_ctas_hue AS t
USING (
  SELECT id_usuario, MAX(limite_credito) AS limite_credito
  FROM ${database}.clientes
  GROUP BY id_usuario
) AS c
ON t.id_usuario = c.id_usuario
WHEN MATCHED THEN
UPDATE SET limite_credito = COALESCE(c.limite_credito, t.limite_credito);
```

Validar a união dos novos dados:

```sql
SELECT id_usuario AS ID, valor AS VALOR, estabelecimento AS NOME_ESTABELECIMENTO, categoria AS CATEGORIA, limite_credito AS LIMITE_DO_CARTAO
FROM ${database}.transacoes_cartao_iceberg_ctas_hue
LIMIT 10
```

## 14. Time Travel por Timestamp

O time travel usando Iceberg permite consultar versões históricas de uma tabela, acessando os dados em estados anteriores através de snapshots que armazenam o estado da tabela em momentos específicos.

### Como funciona

- Cada modificação cria um snapshot imutável da tabela, que guarda o estado completo naquele instante.
- É possível fazer consultas apontando para um snapshot por ID (FOR SYSTEM_VERSION AS OF) ou por timestamp (FOR SYSTEM_TIME AS OF) para ver dados antigos sem alterar a tabela atual.
- O Hive usa o schema mais recente para interpretar dados nos snapshots anteriores, garantindo consistência mesmo com evolução do esquema.

### Benefícios

- Facilita auditoria, compliance e análise histórica.
- Permite debug e validação de qualidade de dados, acessando estados anteriores.
- Possibilita rollback para estado anterior da tabela em caso de erro.
- Usa snapshots incrementais que mantêm a eficiência e gerenciam o tamanho do histórico.

**Laboratório:** Consulta a tabela conforme ela estava em um determinado momento no tempo, usando o recurso de time travel do Iceberg.

> [!Note]
> Após a listagem dos snapshots a partir do histório ou tags/branches, coletar o valor da coluna `made_current_at` onde é encontrado a informação com timestamp com o fuso horário local, caso queira verificar as colunas disponíveis na view history é possível verificar com o comando:
>  `DESCRIBE FORMATTED ${database}.transacoes_cartao_iceberg_ctas_hue.history`

Para facilitar a identificação do momento e a partir de qual alteração realizada na tabela para a consulta dos dados, é possível consultar a tag criada, coletar o snapshot_id e usá-la para identificar qual é o timestamp para uso no comando abaixo:

```sql
SELECT
    -- Seleciona o timestamp do histórico (t2)
    t2.made_current_at AS Timestamp_Criacao_Tag,    
    -- Seleciona o nome e ID da tag (t1)
    t1.name AS Nome_Tag, t1.snapshot_id AS Snapshot_ID
FROM ${database}.transacoes_cartao_iceberg_ctas_hue.refs t1
JOIN ${database}.transacoes_cartao_iceberg_ctas_hue.history t2
ON t1.snapshot_id = t2.snapshot_id
WHERE
    -- Filtra a referência pelo nome da tag e garante que é uma TAG e não uma BRANCH
    t1.name = '${TAG_NOME}' AND t1.type = 'TAG';
```

```sql
SELECT *
FROM ${database}.transacoes_cartao_iceberg_ctas_hue
FOR SYSTEM_TIME AS OF '${system_time}'
LIMIT 10;
```

> [!Warning]
> Não há necessidade de copiar o UTC no final do valor do timestamp. A partir do exemplo de uma linha: `2025-11-26 18:54:01.828 UTC`, utilizar apenas a primeira parte: `2025-11-26 18:54:01.828`

## 15. Tagging e Rollback

O rollback usando Iceberg permite restaurar uma tabela a um estado anterior com base em um snapshot específico ou timestamp, criando um novo snapshot correspondente a essa versão antiga.

### Como funciona

- Cada modificação gera um snapshot imutável da tabela.
- O rollback cria um novo snapshot com a data e hora da operação, mas o snapshot_id permanece o mesmo do snapshot para o qual se está fazendo rollback.
- Só é possível fazer rollback para snapshots que ainda sejam "current ancestor" na história da tabela — não é possível "rollback a um rollback".

### Benefícios

- Permite corrigir problemas ocorridos em alterações recentes da tabela.
- Mantém o histórico completo das versões da tabela para auditoria e recuperação.
- É suportado tanto no Hive quanto no Impala no Cloudera.

Em resumo, o rollback é uma função de restauração de versão que garante segurança e integridade, criando novos snapshots para manter a linha do tempo dos dados.

**Laboratório:** Cria uma tag para um snapshot específico e faz rollback para um snapshot anterior, revertendo alterações.

#### Listar as tags e snapshots de forma inteligente:

Com a consulta abaixo, é possível observar tanto as tags/branches quanto os timestamps, e o melhor, ordenado pelo momento de geração, com isso apresentando uma ordem cronológica para avaliação de alguma alteração que não deveria ser executada ou para reverter uma mudança que tenha gerado muito problema e impactado o negócio.

```sql
SELECT
    -- Referência e Tipo (serão NULL para snapshots sem tag/branch)
    t1.name AS Referencia, t1.type AS Tipo,
    -- Dados do Histórico (sempre preenchidos)
    t2.made_current_at AS Timestamp_Geracao, t2.snapshot_id AS Snapshot_ID, t2.parent_id AS Snapshot_Anterior,
    -- Indicador para facilitar a leitura
    CASE
        WHEN t1.name IS NULL THEN 'Sem Referência'
        ELSE 'Referenciado'
    END AS Status_Referencia
FROM ${database}.transacoes_cartao_iceberg_ctas_hue.history t2  -- Tabela PRIMÁRIA (Esquerda)
LEFT OUTER JOIN ${database}.transacoes_cartao_iceberg_ctas_hue.refs t1      -- Tabela SECUNDÁRIA (Direita)
ON t2.snapshot_id = t1.snapshot_id
ORDER BY Timestamp_Geracao ASC;
```

Vamos analisar e identificar a alteração que não deveria ter sido executada:

```sql
-- Referência baseada na tag pre_update
SELECT * FROM ${database}.transacoes_cartao_iceberg_ctas_hue
FOR SYSTEM_VERSION AS OF ${insert_snapshot_id}
WHERE id_usuario = '000000036' AND estabelecimento = 'Mercado Bitcoin';
```

```sql
-- Consulta no momento atual da tabela:
SELECT * FROM ${database}.transacoes_cartao_iceberg_ctas_hue
WHERE id_usuario = '000000036' AND estabelecimento = 'Mercado Bitcoin';
```

O registro que foi apagado de forma incorreta foi encontrado, entretanto, pelo histórico das nossas atividades, o valor gasto no estabelecimento foi atualizado. E preciso desse dado atualizado. Consultar mais uma vez as tags e snapshots, conforme [comando acima](#listar-as-tags-e-snapshots-de-forma-inteligente) e consultar a tag que tenha o valor atualizado.

**Dica:** Se tenho uma tag chamada pre_update, então devo usar o snapshot_id da próxima alteração.

```sql
-- Referência baseada na próxima tag criada depois de pre_update
SELECT * FROM ${database}.transacoes_cartao_iceberg_ctas_hue
FOR SYSTEM_VERSION AS OF ${updated_snapshot_id}
WHERE id_usuario = '000000036' AND estabelecimento = 'Mercado Bitcoin';
```

***

#### Processo de renomeação da TAG

É comum utilizar um nome para tag que em algum momento não faz sentido ou está faltando informação, para isso é possível seguir um processo de renomeação da TAG.

Atualmente, não existe um comando SQL nativo no Hive (HiveQL) ou Impala que permita renomear diretamente uma tag do Iceberg, mas você pode tratar a operação de renomear uma tag como uma sequência de duas operações separadas:

- **Criação:** Criar a nova tag com o nome desejado, apontando para o mesmo Snapshot ID da tag antiga.
- **Exclusão:** Remover a tag antiga. Criar uma nova tag a partir de um snapshot que já tenha uma tag vinculada

Para nosso caso, a tag pre_update não fornece nenhum detalhe adicional sobre a atualização efetuada. Vamos então melhorar o detalhamento da tag.

Identificar o Snapshot ID da tag que precise ser renomeada:

```sql
SELECT snapshot_id
FROM ${database}.transacoes_cartao_iceberg_ctas_hue.refs
WHERE name = '${tag_antiga}' AND type = 'TAG';
```

Coletar o snapshot_id encontrado e utilizar para criação da nova tag:

```sql
ALTER TABLE ${database}.transacoes_cartao_iceberg_ctas_hue
CREATE TAG ${tag_nova} FOR SYSTEM_VERSION AS OF ${snapshot_id_tag_antiga};
```

Agora basta remover a tag antiga:

```sql
ALTER TABLE ${database}.transacoes_cartao_iceberg_ctas_hue 
DROP TAG IF EXISTS ${tag_antiga};
```

***

Para finalizar o **Laboratório 15**, uma vez que o registro foi encontrado e deveria existir, podemos seguir com a recuperação dos dados baseados no snapshot_id (rollback). Iniciar identificando o snapshot_id que foi identificado com o registro que precisa estar disponível:

```sql
SELECT t1.name AS Referencia, t1.type AS Tipo, t2.made_current_at AS Timestamp_Geracao, t1.snapshot_id AS Snapshot_ID
FROM ${database}.transacoes_cartao_iceberg_ctas_hue.refs t1
JOIN ${database}.transacoes_cartao_iceberg_ctas_hue.history t2
ON t1.snapshot_id = t2.snapshot_id
WHERE t1.name = '${tag_prx_preupdate}' AND t1.type = 'TAG';
```

Coletar o snapshot id ou timestamp e seguir com o rollback:

```sql
ALTER TABLE ${database}.transacoes_cartao_iceberg_ctas_hue EXECUTE ROLLBACK(${snapshot_rollback});
```

```sql
--Ou com o timestamp
ALTER TABLE ${database}.transacoes_cartao_iceberg_ctas_hue EXECUTE ROLLBACK('${timestamp_id_insert}');
```

Validar se o registro está na versão atual da sua tabela e se o valor está atualizado:

```sql
SELECT * FROM ${database}.transacoes_cartao_iceberg_ctas_hue
WHERE id_usuario = '000000036' AND estabelecimento = 'Mercado Bitcoin';
```

## 16. Branching (Ramificações)

No Iceberg do Cloudera no Hive, branching permite criar ramificações (branches) de uma tabela para isolar alterações, facilitando desenvolvimento paralelo, testes ou experimentos sem impactar a tabela principal.

### Funcionamento do Branching

- Um branch é uma linha independente de snapshots derivada do snapshot atual ou específico da tabela.
- Operações de leitura e escrita podem ser feitas de forma isolada em cada branch.
- Branches facilitam workflows de desenvolvimento, QA, e prototipagem em paralelo com a produção.

### Limitações

- O recurso está em technical preview no Hive, não recomendado para produção crítica.
- Não suportado por todas engines, especialmente não disponível no Impala.
- Pode haver restrições no gerenciamento de snapshots e na sincronização entre branches.
- Branches adicionam complexidade na governança, exigindo controle cuidadoso para evitar conflitos ou perda de dados.
- Ainda não há suporte completo para operações avançadas, como merges automáticos entre branches, o que exige intervenção manual.

### Recomendação

- Usar branches para experimentação e ambientes de desenvolvimento.
- Testar extensivamente antes de adotar em ambientes produtivos.
- Acompanhar atualizações da Cloudera para a estabilidade futura do recurso.

**Laboratório:** Criar uma branch para desenvolvimento isolado, permitindo alterações sem afetar a branch principal.

```sql
ALTER TABLE ${database}.transacoes_cartao_iceberg_ctas_hue
CREATE BRANCH dev_branch;
```

Listar as branches:

```sql
SELECT * FROM ${database}.transacoes_cartao_iceberg_ctas_hue.refs
WHERE type = 'BRANCH';
```

## Bônus - Caso de uso real financeiro

Gerenciar o desenvolvimento e teste de transformações de dados de forma isolada e segura, sem afetar os dados de produção (main)

### Cenário: Teste de Nova Regra de Risco (Modelagem de Dados)

Imagine que você precisa adicionar uma nova coluna (`score_risco_v2`) à tabela de transações e rodar um novo pipeline de ETL para preenchê-la, antes de liberá-la para produção.

**Tabela de Produção:** transacoes_cartao_iceberg_ctas_hue

1. Branch de Desenvolvimento criada no passo anterior: Criação de uma nova branch (`dev_branch`) que é uma cópia lógica (ponteiro de snapshot) da branch principal (`main`).
2. Realizar Modificações e Testes (DDL e DML): Adicione a nova coluna necessária para o novo modelo de risco.

```sql
ALTER TABLE ${database}.transacoes_cartao_iceberg_ctas_hue.branch_dev_branch
ADD COLUMNS (score_risco_v2 DECIMAL(5, 2));
```

3. Inserção de Dados (DML)

```sql
INSERT INTO ${database}.transacoes_cartao_iceberg_ctas_hue.branch_dev_branch
SELECT t.id_usuario, t.data_transacao, t.valor, t.estabelecimento, t.categoria, t.status, t.data_execucao, t.limite_credito,
    CASE 
        WHEN t.status = 'Aprovada' OR t.status = 'Extornada' THEN 
            CASE 
                WHEN RAND() < 0.7 THEN 4 
                ELSE 5 
            END
        WHEN t.status = 'Negada' THEN 1
        WHEN t.status = 'Cancelada' THEN 
            CASE 
                WHEN RAND() < 0.5 THEN 2 
                ELSE 3 
            END
        ELSE NULL -- Garante NULL se o status não for mapeado
    END AS score_risco_v2
FROM ${database}.transacoes_cartao_iceberg_ctas_hue t;
```

4. Ler e Validar os Resultados

```sql
-- Leitura de teste e validação na branch atual (dev_branch)
SELECT score_risco_v2, COUNT(*) 
FROM ${database}.transacoes_cartao_iceberg_ctas_hue.branch_dev_branch
GROUP BY score_risco_v2;
```

```sql
-- Observe que a coluna score_risco_v2 NÃO EXISTE neste SELECT, pois ela não está na main.
SELECT * FROM ${database}.transacoes_cartao_iceberg_ctas_hue LIMIT 10;
```

5. Promover Mudanças para a Main (Merge)

```sql
ALTER TABLE ${database}.transacoes_cartao_iceberg_ctas_hue EXECUTE FAST-FORWARD 'main' 'dev_branch';
```

6. Validar se as alterações aparece na main:

```sql
SELECT * FROM ${database}.transacoes_cartao_iceberg_ctas_hue LIMIT 10;
```

7. Excluir a branch de desenvolvimento:

```sql
ALTER TABLE ${database}.transacoes_cartao_iceberg_ctas_hue
DROP BRANCH dev_branch;
```

## Melhores práticas para publicação de branches e situações de conflito

Para publicar (fazer merge) de uma branch `dev_branch` para a branch `main` no Iceberg, utiliza-se um comando de merge SQL padrão, conforme exemplo:

> [!Note]
> Alterar os valores das variáveis que estão entre <variáveis>

```sql
MERGE INTO main AS T
USING dev_branch AS S
T.<common_column> = S.<common_column>
WHEN MATCHED THEN UPDATE SET <colunas_valores>
WHEN NOT MATCHED THEN INSERT VALUES (<valores>);
```

Na prática, o procedimento seria:

1. Criar e trabalhar na branch `dev_branch`.
2. Quando apta, fazer merge dos dados para `main` com a cláusula MERGE.
3. Opcionalmente, deletar a branch `dev_branch` após o merge.

***

Quanto à situação de duas branches criadas simultaneamente a partir da mesma versão inicial da main e com alterações independentes em paralelo:

- Ambas as branches possuem a mesma origem (snapshot base).
- Cada branch pode acumular alterações diferentes isoladamente.
- Quando for feita a publicação (merge) de uma branch para a main, o merge só terá sucesso se a main não tiver sido alterada desde o snapshot base daquela branch.
- Se tentar publicar a segunda branch sem atualizar primeiro a base (main), ocorrerá conflito de commit, pois o snapshot base da branch não é mais o atual da main.
- Portanto, a main final após duas publicações concorrentes não conterá automaticamente as alterações de ambas as branches.

Para garantir que a main contenha ambas as alterações, a segunda publicação deve:

1. Atualizar a branch (rebase/fast-forward) com o estado atual da main (com as alterações da primeira branch já publicadas).
2. Resolver possíveis conflitos.
3. Fazer o merge atualizado na main.

Sem esses passos, a publicação da segunda branch falhará ou sobrescreverá o conteúdo, omitindo alterações da outra branch.

### Atualização fast-forward de branch

No Hive, para atualizar um branch com as últimas alterações de outro (por exemplo, atualizar a branch `main` para o estado atual da branch `dev_branch`) usa-se o comando `EXECUTE FAST FORWARD`. Este comando move o ponteiro do branch alvo para o snapshot atual do branch de origem, sem criar merge commits.

```sql
ALTER TABLE ${database}.transacoes_cartao_iceberg_ctas_hue EXECUTE FAST-FORWARD 'main' 'dev_branch';
```

Isso atualiza a `main` para o mesmo estado da `dev_branch` exatamente, se `main` for um ancestral de `dev_branch` (fast-forward possível).

***

### Considerações finais

- O fast-forward somente aplica mudanças quando a branch alvo é ancestral da branch de origem.
- Caso contrário, será necessário resolver conflitos manualmente via operações de merge.
- Não há mecanismo nativo automático de merge ou rebase como no Git, exige coordenação e scripts para manter branches sincronizados.
- Essas operações garantem que as alterações das branches paralelas sejam aplicadas ordenadamente sem perder mudanças.

***

Esse comportamento é similar ao modelo de controle de versão distribuído (como git), exigindo cuidado para sincronizar branches antes de publicar em produção em Iceberg com Hive no Cloudera.

## 18. Conversão de Tabela para Iceberg In-place

```sql
ALTER TABLE ${database}.transacoes_cartao CONVERT TO ICEBERG;
```

**Explicação:**
Converte uma tabela Hive tradicional para o formato Iceberg, preservando dados e metadados.

## 20. Análise de Estatísticas

Mesmo com a atualização automática de estatísticas pelo Iceberg a cada snapshot, ainda é recomendado executar a análise de estatísticas no Hive para otimizar o planejamento e execução de queries.

### Quando executar a análise de estatísticas

- Após operações significativas de escrita na tabela, como inserções em grande volume, atualizações e deleções que impactem dados e distribuição.
- Antes de cargas de trabalho analíticas críticas que dependam fortemente de planos de execução otimizados.
- Periodicamente em tabelas com uso frequente para garantir que as estatísticas estejam atualizadas em sistemas de consulta.

### Melhores práticas

- Executar o comando ANALYZE TABLE do Hive após processos de ingestão ou em janelas de manutenção programadas:

  ```sql
  ANALYZE TABLE ${database}.transacoes_cartao_iceberg_ctas_hue COMPUTE STATISTICS;
  ```

- Utilizar a função de análise granular para colunas específicas, quando aplicável:

  ```sql
  ANALYZE TABLE ${database}.transacoes_cartao_iceberg_ctas_hue COMPUTE STATISTICS FOR COLUMNS limite_credito;
  ```

- Integrar a análise de estatísticas em pipelines de dados para manter estatísticas atualizadas automaticamente.
- Monitorar a validade das estatísticas no ambiente e reexecutar análises conforme necessidade, equilibrando custo de processamento e ganho de performance.

### Racional

- Iceberg mantém metadados detalhados e estatísticas por arquivo, mas o Hive precisa das estatísticas agregadas para seu otimizador.
- A análise no Hive complementa as estatísticas do Iceberg, ajudando em melhor geração de planos e filtragens.
- Ignorar a análise pode levar a planos subótimos e maior tempo de consulta apesar dos dados recentes estarem atualizados.

Assim, a execução da análise de estatísticas no momento certo é um passo recomendado para garantir desempenho consistente no Hive com tabelas Iceberg na Cloudera.

### Observações Finais

- **Tags** e **branches** são recursos avançados do Iceberg no Hive, mas ainda em Tech Preview, permitindo controle de versões, auditoria e desenvolvimento seguro. Entretanto não é recomendável ainda usar em produção. (Registro efetuado em 28/11/2025)
- O **time travel** permite consultar dados históricos facilmente.
- O uso de comandos como **MERGE**, **ROLLBACK** e **OPTIMIZE** facilita a manutenção e governança de dados em ambientes analíticos modernos.