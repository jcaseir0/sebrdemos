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

> [!WARNING]
> Para a criação dos Jobs será necessário estar com o Data Engineering Data Hub criado ou o Cloudera Data Warehouse habilitado. Para mariores informações de como fazê-los, acesse esse [tutorial](tutorials/PreparacaoDemo.md).

A necessidade se faz necessária para popular o metadados do catálogo de dados, utilizando o engine do Hive. Para isso, será necessário copiar a URL do JDBC. Para isso, siga o passo-a-passo abaixo:

1. Acessar o **console do Cloudera Data Platform (CDP)** e depois no **Data Warehouse**;
2. Na aba **Virtual Warehouses**, encontre o cluster **credito-vw**.
3. Clique no menu com três pontos na vertical e na opção **Copy JDBC URL**
4. Anote essa informação, será algo conforme abaixo:
   - jdbc:hive2://hs2-<cluster_name>.dw-<environment_name>.a472-9q3k.cloudera.site/default;transportMode=http;httpPath=cliservice;socketTimeout=60;ssl=true;auth=browser;

### Criação dos Jobs Spark no CDE

1. No painel do CDE, clique em **Jobs** e depois em **Create Job**.
2. **Job de criação das tabelas e dados**
3. Selecione o tipo **Spark 3.5.1** (Ou a versão desjada).
4. **Name:** nome do job: userXXX-create-table
5. **Select Application Files:** Repository
6. **+ Add from Repository** -> Selecione o repositório criado: **iceberg-demo**
7. Selecione o arquivo **create_table.py** -> **Select File**
8. **Arguments (Optional):**  jdbc:hive2://hs2-<cluster_name>.dw-<environment_name>.a472-9q3k.cloudera.site/default;transportMode=http;httpPath=cliservice;socketTimeout=60;ssl=true;auth=browser;
9. Em **Python Environment**, clique em **Select Python Environment**, selecione o ambiente criado: **env-py** e clicar em **Select Resource**
10. Em **Advanced Options** é possivel adicionar mais fontes de bibliotecas e classes para sua aplicação, além de aumentar a quantidade de recurso para seu job. PAra o nosso caso iremos definir esse perfil de recursos para o nosso job:
   - **Executor Cores:** 2
   - **Driver Memory:** 4
   - **Executor Memory:** 4
   - **Manter o resto das configurações padrão**
11. Por fim, **NÃO CLICAR EM** Create and Run, passar o mouse sobre a seta ao lado e clique em **Create**

Iremos criar os outros Jobs necessários para o laboratório, **siga as instruções acima repetindo os passos de 3 a 11**, mas alterando os seguintes itens:

**Job para a validação da criação das tabelas**
4. **Name:** nome do job: userXXX-create-table-validation
7. Selecione o diretório spark e depois o arquivo **simplequeries.py** -> **Select File**
8. Deixe **Arguments (Optional):** sem preencher
9. Não há necessidade de selecionar o **Python Environment**
10. Não há necessidade de alterar o perfil de recursos, manter padrão

**Job para nova ingestão de dados usando o particionamento e bucketing das tabelas existentes**
4. **Name:** nome do job: userXXX-insert-table
7. Selecione o arquivo **insert_table.py** -> **Select File**

**Job para a validação da ingestão das tabelas**
4. **Name:** nome do job: userXXX-insert-table-validation
7. Selecione o diretório spark e depois o arquivo **complexqueries.py** -> **Select File**
8. Deixe **Arguments (Optional):** sem preencher
9. Não há necessidade de selecionar o **Python Environment**
10. Não há necessidade de alterar o perfil de recursos, manter padrão

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

> [!WARNING]
> Garantir que o virtual Warehouse de Hive que o JDBC URL foi copiado esteja iniciado.

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

---

> Para detalhes completos dos scripts e exemplos de uso, consulte o repositório e utilize os scripts conforme o fluxo descrito acima.