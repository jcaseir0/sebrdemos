# Demonstração: Implantação da Migração Iceberg no Cloudera Data Engineering (CDE)

## Requisitos

- Python 3.7+
- Spark 3.5+
- Faker

## Funcionalidades das aplicações python e arquivos complementares

A seguir, os scripts principais para implantação da migração no CDE:

- **[common_functions.py](https://github.com/jcaseir0/sebrdemos/blob/main/common_functions.py)**: Consolidação de funções que serão utilizadas pelas outras aplicações python como: validação de metastore, análise de tabelas, geração de dados sintéticos e manipulação de schemas.
- **[create_table.py](https://github.com/jcaseir0/sebrdemos/blob/main/create_table.py)**: Criação de tabelas Hive/Parquet com suporte a particionamento e bucketing, validação de estruturas e remoção segura de tabelas antigas caso existam.
- **[insert_table.py](https://github.com/jcaseir0/sebrdemos/blob/main/insert_table.py)**: Inserção e atualização de dados nas tabelas, com controle de particionamento e bucketing, geração de amostras e validação de integridade dos dados.
- **[schemas/clientes.json](https://github.com/jcaseir0/sebrdemos/blob/main/schemas/clientes.json) e [schemas/transacoes_cartao.json](https://github.com/jcaseir0/sebrdemos/blob/main/schemas/transacoes_cartao.json)**: Schemas JSON para as tabelas de clientes e transações, garantindo consistência dos dados e facilidade na visualização e alteração dos tipos de dados das colunas.
- **[requirements.txt](https://github.com/jcaseir0/sebrdemos/blob/main/requirements.txt)**: Dependências de bibliotecas python, incluindo geração de dados sintéticos com Faker.
- **[config.ini](https://github.com/jcaseir0/sebrdemos/blob/main/config.ini)**: Parâmetros para personalização das tabelas.

## Parametrizações na criação das tabelas

As aplicações python utiliza um arquivo de configuração `config.ini` para definir parâmetros como o nome do banco de dados, o número de registros a serem gerados para cada tabela, e opções de particionamento e bucketing.

Estrutura do `config.ini`:

```ini
[DEFAULT]
dbname = bancodemo
tables = clientes,transacoes_cartao
apenas_arquivos = False
formato_arquivo = parquet # opções: parquet, orc, avro, csv

[clientes]
num_records = 1000000
num_records_update = 500000
particionamento = False
partition_by = None
bucketing = True
clustered_by = id_uf
num_buckets = 5

[transacoes_cartao]
num_records = 2000000
num_records_update = 1000000
particionamento = True
partition_by = data_execucao
bucketing = False
clustered_by = None
num_buckets = 0
```

Ajuste estes valores conforme necessário antes de executar o script. As configurações permitem que você controle o número de registros gerados para cada tabela através da variável `num_records` para a criação e `num_records_update` para a aplicação de ingestão. O código lê este arquivo para determinar o nome do banco de dados, o número de registros a serem gerados para cada tabela e se a tabela será particionada/bucketing.

## Criação do recurso Python e do repositório

Um recurso no Cloudera Data Engineering é uma coleção nomeada de arquivos usados por um trabalho ou uma sessão. Os recursos podem incluir código de aplicativo, arquivos de configuração, imagens personalizadas do Docker e especificações de ambiente virtual Python (requirements.txt).

Os repositórios Git permitem que as equipes colaborem, gerenciem artefatos de projetos e promovam aplicativos de ambientes não-produtivos para ambientes produtivos. Atualmente, a Cloudera oferece suporte a provedores de Git, como GitHub, GitLab e Bitbucket.

Para a nossa demonstração iremos criar um recurso de ambiente virtual python para fornecer a biblioteca adicional para nossas aplicações e um repositório apontando para o repositório https://github.com/jcaseir0/sebrdemos.git na branch main.

## Laboratórios:

- [Demonstração: Implantação da Migração Iceberg no Cloudera Data Engineering (CDE)](#demonstração-implantação-da-migração-iceberg-no-cloudera-data-engineering-cde)
  - [Requisitos](#requisitos)
  - [Funcionalidades das aplicações python e arquivos complementares](#funcionalidades-das-aplicações-python-e-arquivos-complementares)
  - [Parametrizações na criação das tabelas](#parametrizações-na-criação-das-tabelas)
  - [Criação do recurso Python e do repositório](#criação-do-recurso-python-e-do-repositório)
  - [Laboratórios:](#laboratórios)
  - [Lab. 1 - Preparação do ambiente virtual Python e configuração do repositório no Github](#lab-1---preparação-do-ambiente-virtual-python-e-configuração-do-repositório-no-github)
    - [Criação do recurso de ambiente virtual Python](#criação-do-recurso-de-ambiente-virtual-python)
    - [Criação do repositório do Git](#criação-do-repositório-do-git)
  - [Lab. 2 - Criação dos Jobs para criação dos dados e validação](#lab-2---criação-dos-jobs-para-criação-dos-dados-e-validação)
    - [Criação dos Jobs Spark no CDE](#criação-dos-jobs-spark-no-cde)
  - [Lab. 3 - Criação dos Jobs Airflow e agendado no CDE](#lab-3---criação-dos-jobs-airflow-e-agendado-no-cde)
    - [Criação do job Airflow a partir de uma aplicação Python](#criação-do-job-airflow-a-partir-de-uma-aplicação-python)
  - [Lab. 4 - Monitorando o job do airflow através do Airflow UI](#lab-4---monitorando-o-job-do-airflow-através-do-airflow-ui)
  - [Lab. 5 - Migração das tabelas para o formato de tabelas Iceberg](#lab-5---migração-das-tabelas-para-o-formato-de-tabelas-iceberg)
    - [Criação do job airflow a partir do editor para criação da DAG](#criação-do-job-airflow-a-partir-do-editor-para-criação-da-dag)
  - [Para finalizar, clique em **Run** e acompanhe a execução no menu **Job Status** e através do **Airflow UI**.](#para-finalizar-clique-em-run-e-acompanhe-a-execução-no-menu-job-status-e-através-do-airflow-ui)

## Lab. 1 - Preparação do ambiente virtual Python e configuração do repositório no Github

### Criação do recurso de ambiente virtual Python

1. Baixar o arquivo **[requirements.txt](https://github.com/jcaseir0/sebrdemos/blob/main/requirements.txt)** local para seu desktop;
2. Acessar o **console do Cloudera Data Platform (CDP)** e depois no **Data Engineering**;
3. Clicar em **Resources**, no menu da coluna à esquerda e na nova página, clicar no botão **Create a Resource** (O botão aparecerá centralizado caso não exista nenhum recurso criado ainda ou no canto superior à direita.);
4. Na janela aberta, preencher os campos:
   **Create Resource**
   - **Resource Name:** nome do recurso: env-py
   - **Type:** Python Environment
   - Clicar em **Create**
5. Depois clicar em **Upload File** e selecionar o arquivo requirements.txt baixado anteriormente.
6. Após confirmar o upload do arquivo, será iniciado o processo de criação do ambiente virtual com a biblioteca(s) selecionada(s). Quando o botão de upload file aparecer novamente é que o processo foi encerrado e será apresentado as bibliotecas instaladas.

### Criação do repositório do Git

1. Acessar o **console do Cloudera Data Platform (CDP)** e depois no **Data Engineering**;
2. Clicar em **Repositories**, no menu da coluna à esquerda e na nova página, clicar no botão **Create Repository** (O botão aparecerá centralizado caso não exista nenhum recurso criado ainda ou no canto superior à direita.);
3. Na janela aberta, preencher os campos:
   **Create A Repository**
   - **Repository Name:** nome do repositório: iceberg-demo
   - **URL:** https://github.com/jcaseir0/sebrdemos.git
   - **Branch:** main
   - **Manter o resto das configurações padrão**
   - Clicar em **Create**

## Lab. 2 - Criação dos Jobs para criação dos dados e validação

No CDE, um job é uma tarefa automatizada que executa pipelines de dados, podendo ser de diversos tipos, como Spark, Python, Bash e principalmente Airflow. Os jobs podem ser executados sob demanda ou de forma agendada, conforme a necessidade do fluxo de dados da empresa.

### Criação dos Jobs Spark no CDE

1. No painel do CDE, clique em **Jobs** e depois em **Create Job**.
**Job de criação das tabelas e dados**
2. Selecione o tipo **Spark 3.5.1** (Ou a versão desjada).
3. **Name:** nome do job: userXXX-create-table
4. **Select Application Files:** Repository
5. **+ Add from Repository** -> Selecione o repositório criado: **iceberg-demo**
6. Selecione o arquivo **create_table.py** -> **Select File**
7. **Arguments (Optional):** userXXX **(Obrigatório)**
8. Em **Python Environment**, clique em **Select Python Environment**, selecione o ambiente criado: **env-py** e clicar em **Select Resource**
9.  Em **Advanced Options** é possivel adicionar mais fontes de bibliotecas e classes para sua aplicação, além de aumentar a quantidade de recurso para seu job. Para o nosso caso iremos definir esse perfil de recursos para o nosso job:
    - **Executor Cores:** 2
    - **Driver Memory:** 4
    - **Executor Memory:** 4
    - **Manter o resto das configurações padrão**
10.  Por fim, **NÃO CLICAR EM** Create and Run, passar o mouse sobre a seta ao lado e clique em **Create**

Iremos criar os outros Jobs necessários para o laboratório, **siga as instruções acima repetindo os passos de 3 a 10**, mas alterando os seguintes itens:

**Job para a validação da criação das tabelas**
3. **Name:** nome do job: userXXX-create-table-validation
6. Selecione o diretório **spark** e depois o arquivo **simplequeries.py** -> **Select File**
7.  Não há necessidade de alterar o perfil de recursos, manter padrão

**Job para nova ingestão de dados usando o particionamento e bucketing das tabelas existentes**
3. **Name:** nome do job: userXXX-insert-table
6. Selecione o arquivo **insert_table.py** -> **Select File**

**Job para a validação da ingestão das tabelas**
3. **Name:** nome do job: userXXX-insert-table-validation
6. Selecione o diretório **spark** e depois o arquivo **complexqueries.py** -> **Select File**
7.  Não há necessidade de alterar o perfil de recursos, manter padrão

## Lab. 3 - Criação dos Jobs Airflow e agendado no CDE

O Apache Airflow é uma plataforma de orquestração de workflows baseada em DAGs (Directed Acyclic Graphs), muito utilizada para automatizar pipelines de dados. No CDE, cada cluster virtual já inclui uma instância embutida do Airflow, facilitando a criação, agendamento e monitoramento de workflows sem necessidade de infraestrutura adicional

**Jobs do Tipo Airflow no CDE**

- **Criação de jobs Airflow:** O usuário desenvolve um arquivo Python contendo o DAG do Airflow. Esse arquivo é enviado via interface web ou CLI do CDE, podendo incluir recursos adicionais necessários para o workflow

- **Operadores específicos:** O CDE oferece operadores Airflow nativos, como o CdeRunJobOperator (para acionar outros jobs Spark no CDE) e operadores para executar queries em Data Warehouses do Cloudera, ampliando a integração entre serviços

- **Execução e Monitoramento:** Os jobs Airflow podem ser executados manualmente ("Run Now") ou de acordo com um agendamento. O monitoramento é feito pela interface do CDE, que fornece logs, alertas e notificações sobre o status do job, facilitando o troubleshooting

- **Extensibilidade:** É possível instalar operadores customizados e bibliotecas Python adicionais, permitindo integração com sistemas externos e personalização dos workflows

**Jobs Agendados no CDE**

- **Agendamento via interface:** Ao criar ou editar um job, é possível definir um agendamento usando expressões cron, especificando horários, datas de início e fim, e frequência de execução.

- **Configurações Avançadas:**
  - Enable Catchup: Permite que execuções perdidas sejam realizadas retroativamente.
  - Depends on Previous: Garante que cada execução só ocorra após o sucesso da anterior.
  - Start/End Time: Define o período de vigência do agendamento.

- **Execução sob demanda:**Mesmo jobs agendados podem ser disparados manualmente, se necessário.

**Integração Airflow com o CDE e vantagens**

- **Orquestração centralizada:** Permite gerenciar pipelines complexos de dados de ponta a ponta.
  
- **Escalabilidade:** Cada cluster virtual pode rodar múltiplos jobs simultâneos de forma isolada.

- **Governança e Segurança:** Integração nativa com os mecanismos de segurança e auditoria do Cloudera.

- **Facilidade de uso:** Interface amigável para criação, agendamento e monitoramento dos jobs, além de integração com CLI para automação.

### Criação do job Airflow a partir de uma aplicação Python

> [!WARNING]
> Será necessário editar o arquivo [**job-malha-airflow.py**](../airflow/job-malha-airflow.py), baixe (Download Raw File) ou altere no seu repositório local. Para maiores informações sobre a gestão do repositório local, siga esse [tutorial](../tutorials/PreparacaoDemo.md).

**Edições necessárias:**

Adicionar o prefixo: `userXXX_` nas linhas abaixo, exemplo: De `dag_id='malha_airflow',` para `dag_id='userXXX_malha_airflow',`:

**Linha 08:** `dag_id='malha_airflow',`
**Linha 19:** `job_name='create-table',`
**Linha 27:** `job_name='create-table-validation',`
**Linha 35:** `job_name='insert-table',`
**Linha 43:** `job_name='insert-table-validation',`

Depois de efetuar as alterações no arquivo, seguir conforme abaixo:

1. No painel do CDE, clique em **Jobs** e depois em **Create Job**.
2. Selecione o tipo **Airflow**.
3. **Name:** nome do job: userXXX-malha-airflow
4. **DAG File:** Selecionar Resource > **Upload** > 
5. Na nova janela aberta: 
   1. Selecionar o arquivo editado: **job-malha-airflow.py**
   2. **Select a Resource:** Create a Resource
   3. **Resource Name:** demofiles
6. Manter as outras opções sem preenchimento
7. Por fim, clicar em **Create and Run**

Caso tenha feito a alteração no seu repositório local e feito um push no seu fork, siga os passos abaixo:

1. No painel do CDE, clique em Repositories, garanta que o seu virtual cluster esteja selecionado e clique no menu de Actions selecione **Sync** para syncronizar o repositório com as suas alterações recentes.
2. Depois clique em **Jobs** e depois em **Create Job**.
3. Selecione o tipo **Airflow**.
4. **Name:** nome do job: userXXX-malha-airflow
5. **DAG File:** Repository 
6. **+ Add from Repository** -> Selecione o repositório criado: **iceberg-demo**
7. Selecione diretório **airflow** e o arquivo **job-malha-airflow.py** -> **Select File**
8. Manter as outras opções sem preenchimento
9. Por fim, clicar em **Create and Run**

## Lab. 4 - Monitorando o job do airflow através do Airflow UI

1. Com o job do airflow em execução, no painel do CDE, clique em **Administration**
2. Clicar no serviço do CDE habilitado em cima do seu ambiente
3. Abrirá à direita, os Virtual Clusters existentes nesse ambiente, clicar no botão do meio, que se passar o mouse em cima apresentará **Virtual Cluster Details**
4. Na tela de detalhes, clicar à direita no link **Airflow UI**, onde abrirá a interface com as DAGs
5. Clicar no job em execução: **userXXX-malha-airflow**, onde será apresentado os detalhes do job em execução
6. A primeira aba **Details** apresenta um resumo de execução
7. A segunda aba **Graph** apresenta A ordem de execução dos CDE jobs
8. A quarta aba **Code** apresenta o seu código
9. Na quinta e sexta aba, **Run Duration** e **Task Duration** uma comparação das execusões dos jobs airflow e de cada CDE jobs, respectivamente
10. E por fim em Calendar um mapa de calor com a quantidade de sucesso ou falha.

## Lab. 5 - Migração das tabelas para o formato de tabelas Iceberg

Repetir o passo a passo a partir do item 3 até o 10 da sessão [Criação de Job no CDE](#Criação-dos-Jobs-Spark-no-CDE) e mudar apenas os itens abaixo para a criação do novo job:

**Job para a migração das tabelas para Iceberg**
3. **Name:** nome do job: userXXX-iceberg-miginplace
6. Selecione o diretório **iceberg** e depois o arquivo **iceberg_miginplace.py** -> **Select File**

### Criação do job airflow a partir do editor para criação da DAG

1. No painel do CDE, clique em **Jobs** e depois em **Create Job**
2. Selecione o tipo **Airflow**
3. **Name:** nome do job: userXXX-malha-airflow-iceberg
4. **DAG File:** Editor
5. Clique em **Create** 

O Editor será carregado para abertura do Canvas e criação do pipeline

**Migração para o Iceberg**
1. Na coluna à direita **Pipeline Steps**, clicar, segurar e arrastar o CDE job para o Canvas
2. Clique no objeto adicionado e à esquerda abrirá uma janela para edição, preencher conforme próximos passos
3. **Primeiro Campo:** (onde está **cde_job_1**) definir o nome: **Iceberg Migration**
4. Na aba **Configure**, em **Select job**, clicar no campo para carregar os jobs existentes e selecione **userXXX-iceberg-miginplace**

Seguir os passos acima para a criação de mais objetos alterando apenas os itens abaixo:

**Execução de consultas na tabela migrada**
1. Mesmo que o informado anteriormente, mas coloque à frente do CDE job recentemente criado
3. **Primeiro Campo:** Simple Migration Validation
4. **Select job:** **userXXX_create-table-validation**
5. Em **Variables**, **Name:** tableformat e **Value:** iceberg
6. Na aba **Advanced**, selecione a opção **Depends on past**
7. Por fim, passe o mouse em cima do primeiro **CDE job**, aparecerá pontos nas laterais e em cima, irá aparecer um símbolo de mais, então clique, segure e ligue no próximo CDE job.

**Nova ingestão com o mesmo script anterior**
1. Instrução identica ao passo 1 anterior
3. **Primeiro Campo:** Data Ingestion
4. **Select job:** **userXXX_insert-table**
5. Na aba **Advanced**, selecione a opção **Depends on past**
6. Por fim, passe o mouse em cima do primeiro **CDE job**, aparecerá pontos nas laterais e em cima, irá aparecer um símbolo de mais, então clique, segure e ligue no próximo CDE job.

**Execução de consultas após ingestão na tabela Iceberg**
1. Instrução identica ao passo 1 anterior
3. **Primeiro Campo:** Complex Migration Validation
4. **Select job:** **userXXX_insert-table-validation**
5. Em **Variables**, **Name:** tableformat e **Value:** iceberg
6. Na aba **Advanced**, selecione a opção **Depends on past**
7. Por fim, passe o mouse em cima do primeiro **CDE job**, aparecerá pontos nas laterais e em cima, irá aparecer um símbolo de mais, então clique, segure e ligue no próximo CDE job.

Depois da criação de toda a malha, clicar em **Save**. Abrirá uma notificação de **Saving to job...** e ao finalizar o aviso de **Pipeline saved to job**

Para finalizar, clique em **Run** e acompanhe a execução no menu **Job Status** e através do [**Airflow UI**](#lab-4---monitorando-o-job-do-airflow-através-do-airflow-ui).
---

> Para detalhes completos das aplicações python e exemplos de uso, consulte o repositório e utilize os arquivos conforme o fluxo descrito acima.