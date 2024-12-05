# Simulador de Dados Bancários

Este projeto simula dados bancários, incluindo transações de cartão de crédito e informações de clientes, utilizando PySpark e Hive.

## Estrutura do Projeto

- `main.py`: Script principal que cria ou atualiza as tabelas Hive e insere os dados simulados.
- `utils.py`: Contém funções auxiliares para geração de dados aleatórios.
- `config.ini`: Arquivo de configuração para definir parâmetros das tabelas.
- `transacoes_cartao.json`: Define o esquema da tabela de transações.
- `clientes.json`: Define o esquema da tabela de clientes.

## Requisitos

- Python 3.7+
- PySpark
- Faker

## Instalação

1. Clone o repositório:

```bash
git clone https://github.com/jcaseir0/banco_demo.git
cd banco_demo
```

2. Instale as dependências:
```bash
echo "pyspark
faker[pt_BR]" > requirements.txt
pip install -r requirements.txt 
```

## Configuração

O projeto utiliza um arquivo de configuração `config.ini` para definir parâmetros como o nome do banco de dados, o número de registros a serem gerados para cada tabela, e opções de particionamento e bucketing.

Estrutura do `config.ini`:

```ini
[DEFAULT]
database_name = banco_simulado

[transacoes_cartao]
num_records = 10000
particionamento = True
bucketing = False
num_buckets = 10

[clientes]
num_records = 1000
particionamento = False
bucketing = True
num_buckets = 5
```

Ajuste estes valores conforme necessário antes de executar o script. As configurações permitem que você controle o número de registros gerados para cada tabela através da variável `num_records`. O código lê este arquivo para determinar o nome do banco de dados, o número de registros a serem gerados para cada tabela e se a tabela será particionada/bucketing.

## Uso

Para criar as tabelas e inserir os dados simulados, execute o script `main.py` especificando quais tabelas você deseja criar:

```bash
python main.py transacoes_cartao clientes
```

Você pode especificar uma ou ambas as tabelas:

- `transacoes_cartao`: Cria a tabela de transações de cartão de crédito.
- `clientes`: Cria a tabela de informações de clientes.

## Logging
O script utiliza o módulo logging do Python para fornecer informações detalhhadas sobre o processo de execução. As mensagens de log incluem:

- Informações sobre o carregamento de configurações
- Verificação de existência de tabelas
- Detalhes sobre a criação ou atualização de tabelas
- Erros e exceções que possam ocorrer durante a execução

Os logs são exibidos no console e podem ser redirecionados para um arquivo se necessário.

## Descrição das Tabelas

### transacoes_cartao

Contém informações sobre transações de cartão de crédito:

- `id_usuario`: ID do usuário que realizou a transação
- `data_transacao`: Data e hora da transação
- `valor`: Valor da transação
- `estabelecimento`: Nome do estabelecimento
- `categoria`: Categoria da transação
- `status`: Status da transação (Aprovada, Negada, Pendente)

### clientes

Contém informações sobre os clientes:

- `id_usuario`: ID único do cliente
- `nome`: Nome do cliente
- `email`: Endereço de e-mail do cliente
- `data_nascimento`: Data de nascimento do cliente
- `endereco`: Endereço do cliente
- `limite_credito`: Limite de crédito do cliente
- `numero_cartao`: Número do cartão de crédito do cliente

## Customização

- Para modificar o esquema das tabelas, edite os arquivos JSON correspondentes (`transacoes_cartao.json` e `clientes.json`).
- Para alterar a lógica de geração de dados, modifique as funções no arquivo `utils.py`.
- Para ajustar as configurações de particionamento, bucketing ou número de registros, edite o arquivo `config.ini`.

## Contribuindo

Contribuições são bem-vindas! Por favor, sinta-se à vontade para submeter um Pull Request.

## Licença

Este projeto está licenciado sob a licença MIT. Veja o arquivo `LICENSE` para mais detalhes.