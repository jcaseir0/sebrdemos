<img src="https://r2cdn.perplexity.ai/pplx-full-logo-primary-dark%402x.png" class="logo" width="120"/>

# Documentação dos Comandos HQL do Script Iceberg + Hive

Este documento apresenta uma explicação detalhada de cada comando HQL (Hive Query Language) presente no script fornecido, organizado por tópicos. Cada comando é apresentado em uma caixa de código SQL, seguido de uma explicação clara sobre seu propósito e funcionamento.

## 1. Criação de Tabela Iceberg com CTAS

Cria uma tabela externa Iceberg no Hive, particionada por `data_execucao`, usando o storage handler do Iceberg. O comando copia todos os dados da tabela original `transacoes_cartao` para a nova tabela Iceberg, já no formato Iceberg e na versão 2 do formato.

```sql
CREATE EXTERNAL TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue
PARTITIONED BY (data_execucao)
STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler'
TBLPROPERTIES ('format-version'='2')
AS SELECT * FROM bancodemo.transacoes_cartao;
```

**Observação:** Cláusula diferente para o Impala = STORED BY

## 2. Verificação de Metadados das Tabelas

Mostra os detalhes e propriedades das tabelas, como tipo de armazenamento, particionamento, localização e propriedades do Iceberg. Útil para comparar atributos entre a tabela original e a migrada.

**Atenção:** Observe que para a tabela Iceberg, dentro de `Table Parameters`:`table_type`:`ICEBERG`

```sql
DESCRIBE FORMATTED bancodemo.transacoes_cartao;

DESCRIBE FORMATTED bancodemo.transacoes_cartao_iceberg_ctas_hue;
```

## 3. Validação de Registros

Conta o número de registros em cada tabela, permitindo validar se a migração copiou todos os dados corretamente.

```sql
SELECT COUNT(*) FROM bancodemo.transacoes_cartao;

SELECT COUNT(*) FROM bancodemo.transacoes_cartao_iceberg_ctas_hue;
```

## 4. Validação de Integridade

Exibe amostras de dados das duas tabelas para validação manual e compara registros específicos usando o id_usuario da consulta da tabela antes da conversão.

**Dica:** Após a primeira consulta, escolha uma linha e colete os dados das colunas id_usuario e valor para ser utilizada na segunda consulta.

```sql
SELECT * FROM bancodemo.transacoes_cartao LIMIT 10;

SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue
WHERE id_usuario = ${hivetableid} AND valor = ${hivetablevalor};
```

## 5. Validação Cruzada

Compara registros entre as tabelas, usando subconjuntos de valores da tabela antes da conversão, útil para checagem cruzada de integridade após migração.

```sql
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue
WHERE id_usuario IN (SELECT id_usuario FROM bancodemo.transacoes_cartao LIMIT 10)
AND valor IN (SELECT valor FROM bancodemo.transacoes_cartao LIMIT 10);
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

Criar uma tag antes de operações críticas, permitindo rastrear e voltar a este ponto posteriormente de forma facilitada.

```sql
ALTER TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue
CREATE TAG pre_insert;
```

## 7. Inserção de Dados

Insere um novo registro na tabela Iceberg, simulando uma transação de cartão.

```sql
INSERT INTO bancodemo.transacoes_cartao_iceberg_ctas_hue
VALUES ('000000036', '2024-06-24 15:10:06', 702.99, 'Mercado Bitcoin', 'Outros', 'Aprovada', '06-02-2025');
```

## Como funcionam os Snapshots no Iceberg?

Os snapshots no Iceberg funcionam como versões imutáveis de uma tabela, criadas automaticamente a cada operação de escrita, como INSERT, UPDATE ou MERGE. Cada snapshot representa o estado exato da tabela em um momento, sendo essencial para processos de auditoria, conformidade e recuperação de dados.

### Operações comuns com Snapshots

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
ALTER TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue
SET TBLPROPERTIES('write.format.default'='parquet', 'write.metadata.previous-versions-max'='5');
```

### Benefícios dos snapshots

- Permitem recuperação rápida de dados após erros ou incidentes.
- Facilitam auditoria e conformidade, já que cada estado da tabela pode ser acessado e mantido conforme política regulatória.[1][5]
- Evitam leituras e escritas diretas em arquivos, mantendo metadados otimizados no sistema.[2][5]

## 8. Consulta de Histórico

Lista todos os snapshots da tabela, permitindo auditoria e time travel.

```sql
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue.history;
```

## 9. Consulta com snapshot específico usando o time travel

Consulta a tabela como ela estava em um determinado snapshot, útil para auditoria e recuperação de versões anteriores.

```sql
SELECT * FROM bancodemo.transacoes_cartao_iceberg_ctas_hue
FOR SYSTEM_VERSION AS OF ${snapshot_id_insert}
WHERE id_usuario = '000000036' AND estabelecimento = 'Mercado Bitcoin';
```

## 10. Marcação com Tagging do momento

Marca o estado corrente com uma tag e atualiza o valor de uma transação específica.

```sql
ALTER TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue
CREATE TAG pre_update;

UPDATE bancodemo.transacoes_cartao_iceberg_ctas_hue
SET valor = 510.99
WHERE id_usuario = '000000036' AND estabelecimento = 'Mercado Bitcoin';
```

## 11. Exclusão de Dados

Criação de uma tag antes da exclusão e remove registros de um usuário específico.

```sql
ALTER TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue
CREATE TAG pre_delete;

DELETE FROM bancodemo.transacoes_cartao_iceberg_ctas_hue
WHERE id_usuario = '000000036';
```

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

As limitações do schema evolution no Iceberg incluem principalmente restrições de tipos de alterações, suporte parcial a mudanças e considerações específicas para garantir integridade dos dados:

### Limitações principais

- Alterações consideradas inseguras, que exigiriam atualização linha a linha dos dados, não são permitidas (ex.: alterações complexas de tipo que não sejam ampliação segura como int para long).
- Tipos de mudança suportados incluem adicionar colunas, renomear, remover e mudar tipos de forma segura, mas renomeação não é completamente transparente em todos os cenários e pode causar problemas em alguns formatos legados.
- Em engines ou formatos que usam posição (ex: CSV/TSV), schema evolution é limitado ou não suportado, pois alterações podem causar deslocamento incorreto de dados.
- Mudanças feitas fora do Hive (por exemplo, via Spark) devem ser sincronizadas para refletir no schema do Hive/Impala e vice-versa, sendo essa sincronização um ponto de atenção.

Adiciona uma nova coluna à tabela Iceberg de forma dinâmica, sem recriar a tabela.

```sql
ALTER TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue ADD COLUMNS (limite_credito INT);
```

## 13. Atualização em Massa com MERGE

A cláusula MERGE no Hive usando Iceberg permite unir dados de uma tabela fonte com uma tabela destino, realizando atualizações, inserções ou exclusões condicionais com base em uma condição de junção. As operações de merge com Iceberg provê atomicidade e isolamento, aproveitando o gerenciamento de metadados.

**Detalhes relevantes relacionados a chaves primárias**

- Embora seja possível usar colunas para identificar unicamente linhas em operações como MERGE, essas chaves são lógicas para as operações e não garantem restrições físicas de unicidade.
- O Iceberg gerencia integridade e consistência por meio de snapshots e metadados, e não pelo modelo tradicional de chave primária.
- Em algumas integrações, a lógica de unicidade pode ser implementada via aplicação ou processos ETL que usam o Iceberg, mas isso fica fora do controle nativo do formato.
- O Hive tradicional não suporta criação de constraints físicas (como primary keys), nem o Iceberg no Hive da Cloudera adiciona essa funcionalidade disponível hoje.

Atualiza a coluna `limite_credito` na tabela Iceberg com valores vindos da tabela de clientes, usando merge (upsert em outras distribuições).

```sql
MERGE INTO bancodemo.transacoes_cartao_iceberg_ctas_hue AS t
USING (
  SELECT id_usuario, MAX(limite_credito) AS limite_credito
  FROM bancodemo.clientes
  GROUP BY id_usuario
) AS c
ON t.id_usuario = c.id_usuario
WHEN MATCHED THEN
UPDATE SET limite_credito = COALESCE(c.limite_credito, t.limite_credito);
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

Consulta a tabela conforme ela estava em um determinado momento no tempo, usando o recurso de time travel do Iceberg.

```sql
SELECT *
FROM bancodemo.transacoes_cartao_iceberg_ctas_hue
FOR SYSTEM_TIME AS OF '${system_time}'
LIMIT 10;
```

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

Cria uma tag para um snapshot específico e faz rollback para um snapshot anterior, revertendo alterações.

```sql
ALTER TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue CREATE TAG tag_insert FOR SYSTEM_VERSION AS OF ${snapshot_id_insert};

ALTER TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue EXECUTE ROLLBACK(${snapshot_rollback});

--Ou com o timestamp
ALTER TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue EXECUTE ROLLBACK('${timestamp_id_insert}');
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

Cria uma branch para desenvolvimento isolado, permitindo alterações sem afetar a branch principal.

```sql
ALTER TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue CREATE BRANCH dev_branch;

INSERT INTO bancodemo.transacoes_cartao_iceberg_ctas_hue.branch_dev_branch VALUES (...);
```

## 17. Melhores práticas para publicação de branches e situações de conflito

Para publicar (fazer merge) de uma branch `dev_branch` para a branch `main` no Iceberg, utiliza-se um comando de merge SQL padrão, conforme exemplo:

```sql
MERGE INTO main AS T
USING dev_branch AS S
T.id_usuario = S.id_usuario
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
ALTER TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue EXECUTE FAST-FORWARD 'main' TO 'dev_branch';
```

Ou via procedimento:

```sql
CALL hive_catalog.system.fast_forward(
  table => 'bancodemo.transacoes_cartao_iceberg_ctas_hue',
  branch => 'main',
  to => 'dev_branch'
);
```

Isso atualiza a `main` para o mesmo estado da `dev_branch` exatamente, se `main` for um ancestral de `dev_branch` (fast-forward possível).

***

### Rebase de branches no Iceberg do Hive Cloudera

Apesar de o Iceberg não possuir comando explícito para rebase como no Git, a ideia equivalente é:

1. Atualizar o branch de destino (`main`) com um fast-forward para a versão mais atual.
2. Criar ou atualizar outro branch baseado no novo estado do `main`.
3. Aplicar as alterações da branch paralela "rebaseando" seus commits sobre o estado atualizado.

Embora não haja sintaxe SQL direta para "rebase", um padrão é:

- Atualizar `main` (como no exemplo fast-forward)
- Excluir a branch paralela e criar novamente ela baseada em `main` atualizado
- Reaplicar as alterações na branch paralela (via inserções, merges etc.)

```sql
-- Atualizar main para dev_branch
ALTER TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue EXECUTE FAST FORWARD 'main' TO 'dev_branch';

-- Apagar branch antiga
ALTER TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue DROP BRANCH dev_branch2;

-- Criar branch dev_branch2 baseada em main atualizada
ALTER TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue CREATE BRANCH dev_branch2 FOR REF main;

-- Aplicar alterações antigas da dev_branch2 na dev_branch2 nova através de comandos de MERGE ou INSERT
```

***

### Considerações finais

- O fast-forward somente aplica mudanças quando a branch alvo é ancestral da branch de origem.
- Caso contrário, será necessário resolver conflitos manualmente via operações de merge.
- Não há mecanismo nativo automático de merge ou rebase como no Git, exige coordenação e scripts para manter branches sincronizados.
- Essas operações garantem que as alterações das branches paralelas sejam aplicadas ordenadamente sem perder mudanças.

***

Esse comportamento é similar ao modelo de controle de versão distribuído (como git), exigindo cuidado para sincronizar branches antes de publicar em produção em Iceberg com Hive no Cloudera.

## 18. Otimização e Compaction

O table maintenance no Iceberg envolve operações para otimizar o desempenho e a gestão dos dados da tabela, principalmente para compaction e limpeza de arquivos antigos.

### Comando OPTIMIZE

- O comando `OPTIMIZE` é usado para compactar arquivos pequenos em arquivos maiores e otimizados para leitura.
- Essa compactação melhora a performance de queries, reduz overhead de metadados e melhora o gerenciamento do armazenamento.
- Sintaxe típica no Hive com Iceberg:
- Pode ser configurado para otimizar toda a tabela ou particionamento específico, dependendo das propriedades definidas.

### Benefícios da manutenção da tabela

- Reduz fragmentação de arquivos e melhora a eficiência de leitura.
- Ajuda a controlar o crescimento do número de arquivos pequenos após muitas inserções, atualizações ou deleções.
- Mantém o catálogo da tabela enxuto e gerenciável, evitando lentidão no acesso.

### Considerações adicionais

- O Iceberg gerencia automaticamente os snapshots e operações atômicas, garantindo consistência mesmo durante operações de manutenção.
- Outras operações de manutenção incluem expiração de snapshots antigos (`EXPIRE SNAPSHOTS`) e limpeza de arquivos não referenciados (`REMOVE ORPHAN FILES`).
- É recomendado executar o `OPTIMIZE` regularmente em tabelas com muitas operações de escrita para manter performance ideal.

Essa combinação de manutenção com `OPTIMIZE` permite garantir a saúde da tabela e a performance consistente no Hive com Iceberg na plataforma Cloudera.

Compacta arquivos pequenos e reorganiza os dados da tabela para melhorar desempenho e eficiência:

```sql
OPTIMIZE TABLE bancodemo.transacoes_cartao_iceberg_ctas_hue;
```

## 19. Conversão de Tabela para Iceberg

```sql
ALTER TABLE bancodemo.transacoes_cartao CONVERT TO ICEBERG;
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
  ANALYZE TABLE bancodemo.transacoes_cartao COMPUTE STATISTICS;
  ```
- Utilizar a função de análise granular para colunas específicas, quando aplicável:
  ```sql
  ANALYZE TABLE bancodemo.transacoes_cartao COMPUTE STATISTICS FOR COLUMNS limite_credito;
  ```
- Integrar a análise de estatísticas em pipelines de dados para manter estatísticas atualizadas automaticamente.
- Monitorar a validade das estatísticas no ambiente e reexecutar análises conforme necessidade, equilibrando custo de processamento e ganho de performance.

### Racional

- Iceberg mantém metadados detalhados e estatísticas por arquivo, mas o Hive precisa das estatísticas agregadas para seu otimizador.
- A análise no Hive complementa as estatísticas do Iceberg, ajudando em melhor geração de planos e filtragens.
- Ignorar a análise pode levar a planos subótimos e maior tempo de consulta apesar dos dados recentes estarem atualizados.

Assim, a execução da análise de estatísticas no momento certo é um passo recomendado para garantir desempenho consistente no Hive com tabelas Iceberg na Cloudera.

### Observações Finais

- **Tags** e **branches** são recursos avançados do Iceberg no Hive, mas ainda em Tech Preview, permitindo controle de versões, auditoria e desenvolvimento seguro. Entretanto não é recomendável ainda usar em produção. (Registro efetuado em 09/10/2025)
- O **time travel** permite consultar dados históricos facilmente.
- O uso de comandos como **MERGE**, **ROLLBACK** e **OPTIMIZE** facilita a manutenção e governança de dados em ambientes analíticos modernos.

## BÔNUS - Aprofundamento no funcionamento do Iceberg no HDFS

**Em Desenvolvimento**