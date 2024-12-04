# Simulador de Dados Bancários

Este projeto simula dados bancários, incluindo transações de cartão de crédito e informações de clientes, utilizando PySpark e Hive.

## Estrutura do Projeto

- `main.py`: Script principal que cria as tabelas Hive e insere os dados simulados.
- `utils.py`: Contém funções auxiliares para geração de dados aleatórios.
- `transacoes_cartao.json`: Define o esquema da tabela de transações.
- `clientes.json`: Define o esquema da tabela de clientes.

## Requisitos

- Python 3.7+
- PySpark
- Faker

## Instalação

1. Clone o repositório:
```bash
git clone https://github.com/seu-usuario/simulador-dados-bancarios.git
cd simulador-dados-bancarios
```

2. Instale as dependências:
```bash
pip install pyspark faker
```

## Uso

Para criar as tabelas e inserir os dados simulados, execute o script `main.py` especificando quais tabelas você deseja criar:
```bash
python main.py transacoes_cartao clientes
```

Você pode especificar uma ou ambas as tabelas:

- `transacoes_cartao`: Cria a tabela de transações de cartão de crédito.
- `clientes`: Cria a tabela de informações de clientes.

## Configuração

O projeto utiliza um arquivo de configuração `config.ini` para definir parâmetros como o nome do banco de dados e o número de registros a serem gerados para cada tabela.

Estrutura do `config.ini`:

```ini
[DEFAULT]
database_name = banco_simulado

[transacoes_cartao]
num_records = 10000

[clientes]
num_records = 1000
```

Você pode ajustar estes valores conforme necessário antes de executar o script.

Estas modificações permitem que você controle o número de registros gerados para cada tabela através do arquivo de configuração `config.ini`. O código agora lê este arquivo para determinar o nome do banco de dados e o número de registros a serem gerados para cada tabela.

Para executar o script, você ainda usaria o mesmo comando:

```bash
python main.py transacoes_cartao clientes
```

O número de registros gerados será baseado nas configurações do arquivo `config.ini`.

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

Para modificar o esquema das tabelas, edite os arquivos JSON correspondentes (`transacoes_cartao.json` e `clientes.json`).

Para alterar a lógica de geração de dados, modifique as funções no arquivo `utils.py`.

## Contribuindo

Contribuições são bem-vindas! Por favor, sinta-se à vontade para submeter um Pull Request.

## Licença

Este projeto está licenciado sob a licença MIT. Veja o arquivo `LICENSE` para mais detalhes.
