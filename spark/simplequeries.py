from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, avg, max, date_format
import logging, sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common_functions import load_config

# Initialize Spark session
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize Spark session
spark = SparkSession.builder.appName("SimpleFinancialAnalysis").getOrCreate()

# Load Variables
config = load_config(logger)
username = sys.argv[1]
logger.debug(f"Loading username correctly? Var: {username}")
database_name = config['DEFAULT'].get('dbname') + '_' + username
logger.debug(f"Database name: {database_name}")

# Load tables
clientes = spark.table(f"{database_name}.clientes")
transacoes = spark.table(f"{database_name}.transacoes_cartao")

# Contagem de linhas em cada tabela
num_clientes = clientes.count()
num_transacoes = transacoes.count()

# 1. Total de gastos por cliente
total_gastos = clientes.join(transacoes, "id_usuario") \
    .groupBy("id_usuario", "nome") \
    .agg(sum("valor").alias("total_gastos")) \
    .orderBy("total_gastos", ascending=False)

# 2. Número de transações por cliente
num_transacoes = clientes.join(transacoes, "id_usuario") \
    .groupBy("id_usuario", "nome") \
    .agg(count("*").alias("total_transacoes")) \
    .orderBy("total_transacoes", ascending=False)

# 3. Média de gastos por transação para cada cliente
media_gastos = clientes.join(transacoes, "id_usuario") \
    .groupBy("id_usuario", "nome") \
    .agg(avg("valor").alias("media_gastos")) \
    .orderBy("media_gastos", ascending=False)

# 4. Clientes com maior valor de transação única
maior_transacao = clientes.join(transacoes, "id_usuario") \
    .groupBy("id_usuario", "nome") \
    .agg(max("valor").alias("maior_transacao")) \
    .orderBy("maior_transacao", ascending=False) \
    .limit(10)

# 5. Total de gastos por categoria
gastos_categoria = transacoes.groupBy("categoria") \
    .agg(sum("valor").alias("total_gastos")) \
    .orderBy("total_gastos", ascending=False)

# 6. Número de transações por status
transacoes_status = transacoes.groupBy("status") \
    .agg(count("*").alias("total_transacoes")) \
    .orderBy("total_transacoes", ascending=False)

# 7. Clientes com transações na categoria "Alimentação"
clientes_alimentacao = clientes.join(transacoes, "id_usuario") \
    .filter(transacoes.categoria == "Alimentação") \
    .select("id_usuario", "nome").distinct()

# 8. Total de gastos por cliente na categoria "Transporte"
gastos_transporte = clientes.join(transacoes, "id_usuario") \
    .filter(transacoes.categoria == "Transporte") \
    .groupBy("id_usuario", "nome") \
    .agg(sum("valor").alias("total_gastos_transporte")) \
    .orderBy("total_gastos_transporte", ascending=False)

# 9. Clientes com transações negadas
clientes_negados = clientes.join(transacoes, "id_usuario") \
    .filter(transacoes.status == "Negada") \
    .select("id_usuario", "nome").distinct()

# 10. Total de gastos mensais por cliente
gastos_mensais = clientes.join(transacoes, "id_usuario") \
    .withColumn("mes", date_format("data_transacao", "yyyy-MM")) \
    .groupBy("id_usuario", "nome", "mes") \
    .agg(sum("valor").alias("total_gastos")) \
    .orderBy("id_usuario", "mes")

# Show results (first 10 rows for each query)
print(f"\nNúmero de linhas da tabela clientes: {num_clientes}")
print(f"\nNúmero de linhas da tabela transacoes_cartao: {num_transacoes}")
print("\nTotal de gastos por cliente:")
total_gastos.show(10)
print("Número de transações por cliente:")
num_transacoes.show(10)
print("Média de gastos por transação para cada cliente:")
media_gastos.show(10)
print("Clientes com maior valor de transação única:")
maior_transacao.show(10)
print("Total de gastos por categoria:")
gastos_categoria.show(10)
print("Número de transações por status:")
transacoes_status.show(10)
print("\Clientes com transações na categoria 'Alimentação':")
clientes_alimentacao.show(10)
print("Total de gastos por cliente na categoria 'Transporte':")
gastos_transporte.show(10)
print("Clientes com transações negadas:")
clientes_negados.show(10)
print("Total de gastos mensais por cliente:")
gastos_mensais.show(10)

print("Consultas simples executadas com sucesso!")

# Stop Spark session
spark.stop()
