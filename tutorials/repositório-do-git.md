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
   - **Branch:** main 
   - Clicar em **Create**

## Criação do Job no CDE

Para a criação dos Jobs será necessário estar com o Data Engineering Data Hub criado ou o Cloudera Data Warehouse habilitado. Para mariores informações de como fazê-los, acesse esse [tutorial](tutorials/PreparacaoDemo.md).

1. **Upload dos arquivos**: Envie os scripts Python (`common_functions.py`, `create_table.py`, `insert_table.py`), arquivos de schema e `requirements.txt` para o workspace do CDE.
2. **Configuração do ambiente**: Defina as variáveis de ambiente e paths necessários (ex: JDBC URL, paths de schemas).
3. **Criação do Job**:
   - No painel do CDE, clique em "Create Job".
   - Selecione o tipo "Spark".
   - Informe o script principal (`create_table.py` ou `insert_table.py`).
   - Adicione argumentos conforme necessário (exemplo: JDBC URL).
   - Configure recursos (CPU, memória, número de executores).
   - Anexe o arquivo `requirements.txt` para garantir as dependências.
4. **Execução**: Inicie o job e monitore os logs diretamente pelo CDE.
5. **Validação**: Após a execução, valide as tabelas e dados conforme os logs e queries de amostragem.

---

## Principais programas em Python

- **common_functions.py**: Centraliza funções para logging, validação do Hive Metastore, análise de estrutura de tabelas (particionamento, bucketing), geração de dados sintéticos para clientes e transações, e manipulação de schemas JSON[1].
- **create_table.py**: Automatiza a criação de tabelas no Hive/Parquet, suportando diferentes estratégias de particionamento e bucketing, além de remover tabelas antigas e validar a estrutura criada[2].
- **insert_table.py**: Realiza a inserção de dados nas tabelas criadas, gerando dados sintéticos e garantindo integridade e consistência, inclusive para tabelas particionadas e bucketed[3].
- **clientes.json / transacoes_cartao.json**: Definem os schemas das tabelas para garantir que os dados gerados estejam no formato esperado pelo Spark e Hive[5][6].
- **requirements.txt**: Lista as dependências necessárias para execução dos scripts, como o pacote `faker` para geração de dados fictícios[4].

---

> Para detalhes completos dos scripts e exemplos de uso, consulte o repositório do projeto e utilize os scripts conforme o fluxo descrito acima.