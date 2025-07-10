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
- **[requirements.txt](https://github.com/jcaseir0/sebrdemos/blob/main/requirements.txt)**: Dependências do projeto, incluindo geração de dados sintéticos com Faker.
- **[config.ini](https://github.com/jcaseir0/sebrdemos/blob/main/config.ini)**: Parâmetros para personalização das tabelas.

## Parametrizações na criação das tabelas

O projeto utiliza um arquivo de configuração `config.ini` para definir parâmetros como o nome do banco de dados, o número de registros a serem gerados para cada tabela, e opções de particionamento e bucketing.

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

Para a nossa demonstração iremos criar um recurso de ambiente virtual python para fornecer a biblioteca adicional para nossas aplicações e um repositório apontando para o projeto https://github.com/jcaseir0/sebrdemos.git na branch main.

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

## Criação do Job no CDE

Para a criação dos Jobs será necessário estar com o Data Engineering Data Hub criado ou o Cloudera Data Warehouse habilitado. Para mariores informações de como fazê-los, acesse esse [tutorial](tutorials/PreparacaoDemo.md).

A necessidade se faz necessária para popular o metadados do catálogo de dados, utilizando o engine do Hive. Para isso, será necessário copiar a URL do JDBC. Para isso, siga o passo-a-passo abaixo:

1. Acessar o **console do Cloudera Data Platform (CDP)** e depois no **Data Warehouse**;
2. Na aba **Virtual Warehouses**, encontre o cluster **credito-vw**.
3. Clique no menu com três pontos na vertical e na opção **Copy JDBC URL**
4. Anote essa informação, será algo conforme abaixo:
   - jdbc:hive2://hs2-<cluster_name>.dw-<environment_name>.a472-9q3k.cloudera.site/default;transportMode=http;httpPath=cliservice;socketTimeout=60;ssl=true;auth=browser;

### Criação do Job

1. No painel do CDE, clique em **Jobs** e depois em **Create Job**.
2. Na janela aberta, preencher os campos:
   **Job de criação das tabelas e dados**
   1. Selecione o tipo **Spark 3.5.1** (Ou a versão desejada).
   2. **Name:** nome do job: user001-create-table
   3. **Select Application Files:** Repository
   4. **+ Add from Repository** -> Selecione o repositório criado: **iceberg-demo**
   5. Selecione o arquivo **create_table.py** -> **Select File**
   6. **Arguments (Optional):**  jdbc:hive2://hs2-<cluster_name>.dw-<environment_name>.a472-9q3k.cloudera.site/default;transportMode=http;httpPath=cliservice;socketTimeout=60;ssl=true;auth=browser;
   7. Em **Python Environment**, clique em **Select Python Environment**, selecione o ambiente criado: **env-py** e clicar em **Select Resource**
   8. Em **Advanced Options** é possivel adicionar mais fontes de bibliotecas e classes para sua aplicação, além de aumentar a quantidade de recurso para seu job. PAra o nosso caso iremos definir esse perfil de recursos para o nosso job:
     - **Executor Cores:** 2
     - **Driver Memory:** 4
     - **Executor Memory:** 4
     - **Manter o resto das configurações padrão**
   9. Por fim, **NÃO CLICAR EM** Create and Run, passar o mouse sobre a seta ao lado e clique em **Create**

Iremos criar os outros Jobs necessários para o laboratório, siga as instruções acima, de 1 a 9, mas alterando os seguintes itens:

   **Job para a validação da criação das tabelas**
   2. **Name:** nome do job: user001-create-table-validation
   5. Selecione o diretório spark e depois o arquivo **simplequeries.py** -> **Select File**
   6. Deixe **Arguments (Optional):** sem preencher
   7. Não há necessidade de selecionar o **Python Environment**

---

> Para detalhes completos dos scripts e exemplos de uso, consulte o repositório do projeto e utilize os scripts conforme o fluxo descrito acima.