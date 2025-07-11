from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, avg, rank, stddev, lead, date_trunc, when, corr, col, countDistinct, lit
from pyspark.sql.window import Window
import logging, sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common_functions import load_config, create_spark_session

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize Spark session
logger.info("Initializing Spark session")

app_name = "ComplexFinancialAnalysis"
extra_conf = {"spark.sql.catalogImplementation": "hive"}
spark = create_spark_session(logger, app_name, extra_conf)

logger.info("Spark session initialized successfully")

# Load Variables
config = load_config(logger)
username = sys.argv[1] if len(sys.argv) > 1 else 'forgetArguments'
logger.debug(f"Loading username correctly? Var: {username}")
database_name = config['DEFAULT'].get('dbname') + '_' + username
logger.debug(f"Database name: {database_name}")

# Check if the 'iceberg' argument is there
tableformat_iceberg = len(sys.argv) > 2 and sys.argv[2] == 'iceberg'

# Get tables from config
tables = config['DEFAULT']['tables'].split(',')

# If sys.argv[2] exists and 'iceberg' value, change the table names
if tableformat_iceberg:
    tables = [f"{table.strip()}_miginplace" for table in tables]
else:
    tables = [table.strip() for table in tables]

# Carrega as tabelas dinamicamente em variáveis
for table in tables:
    # Remove o sufixo para nomear a variável corretamente
    var_name = table.replace('_miginplace', '') if tableformat_iceberg else table
    globals()[var_name] = spark.table(f"{database_name}.{table}")

# Exibir amostras das tabelas para verificar os dados
logger.info("Displaying sample data from tables\n")
logger.info("Transações")
transacoes_cartao.show(5)
logger.info("Clientes")
clientes.show(5)

# Contagem de linhas em cada tabela
num_clientes = clientes.count()
num_transacoes = transacoes_cartao.count()

# 1. Análise de gastos por cliente e categoria, com ranking
def gastos_por_cliente_categoria():
    # Verificar se as tabelas foram carregadas corretamente
    if 'id_usuario' not in transacoes_cartao.columns or 'categoria' not in transacoes_cartao.columns:
        raise ValueError("As colunas esperadas não estão presentes na tabela transacoes_cartao.")
    
    # Calcular o total de gastos por usuário e categoria
    gastos_agregados = transacoes_cartao.groupBy("id_usuario", "categoria") \
        .agg(sum("valor").alias("total_gastos"))
    
    # Criar uma janela para o ranking por categoria
    window_spec = Window.partitionBy("categoria").orderBy(col("total_gastos").desc())
    
    # Aplicar o ranking e juntar com a tabela de clientes
    return gastos_agregados \
        .withColumn("ranking_categoria", rank().over(window_spec)) \
        .join(clientes, "id_usuario")

# 2. Detecção de padrões de gastos anômalos
def gastos_anomalos():
    cliente_stats = transacoes_cartao.groupBy("id_usuario") \
        .agg(avg("valor").alias("media_gasto"), stddev("valor").alias("desvio_padrao_gasto"))
    
    return transacoes_cartao.join(cliente_stats, "id_usuario") \
        .join(clientes, "id_usuario") \
        .filter(transacoes_cartao.valor > (cliente_stats.media_gasto + (3 * cliente_stats.desvio_padrao_gasto)))

# 3. Análise de tendências de gastos ao longo do tempo
def tendencias_gastos():
    return transacoes_cartao.groupBy(date_trunc("month", "data_transacao").alias("mes"), "categoria") \
        .agg(count("*").alias("num_transacoes"), 
             sum("valor").alias("total_gastos"), 
             avg("valor").alias("media_gasto")) \
        .withColumn("media_gasto_proximo_mes", lead("media_gasto").over(Window.partitionBy("categoria").orderBy("mes"))) \
        .withColumn("variacao_percentual", ((col("media_gasto_proximo_mes") - col("media_gasto")) / col("media_gasto") * 100))

# 4. Segmentação de clientes com base em padrões de gastos
def segmentacao_clientes():
    cliente_metricas = transacoes_cartao.groupBy("id_usuario") \
        .agg(
            countDistinct(date_trunc("month", "data_transacao")).alias("meses_ativos"),
            sum("valor").alias("total_gastos"),
            avg("valor").alias("media_gasto_por_transacao"),
            count("*").alias("num_transacoes")
        ) \
        .join(clientes, "id_usuario")
    
    return cliente_metricas.withColumn("segmento_cliente", 
        when((col("meses_ativos") >= 10) & (col("total_gastos") > 50000), "VIP")
        .when((col("meses_ativos") >= 6) & (col("total_gastos") > 25000), "Regular")
        .when((col("meses_ativos") >= 3) & (col("total_gastos") > 10000), "Ocasional")
        .otherwise("Inativo"))

# 5. Análise de correlação entre limite de crédito e gastos
def correlacao_limite_gastos():
    gastos_cliente = transacoes_cartao.groupBy("id_usuario") \
        .agg(sum("valor").alias("total_gastos"), count("*").alias("num_transacoes"))
    
    df_joined = clientes.join(gastos_cliente, "id_usuario") \
        .withColumn("percentual_limite_utilizado", (gastos_cliente.total_gastos / clientes.limite_credito) * 100)
    
    correlacao = df_joined.select(corr("limite_credito", "total_gastos")).collect()[0][0]
    
    return df_joined.withColumn("correlacao_limite_gastos", lit(correlacao))

# Execute and show results
print(f"\nNúmero de linhas da tabela clientes: {num_clientes}")
print(f"\nNúmero de linhas da tabela transacoes_cartao: {num_transacoes}")

logger.info("\nExecuting financial analysis queries\n")

logger.info("1. Gastos por cliente e categoria, com ranking")
gastos_por_cliente_categoria().show()
logger.info("2. Detecção de padrões de gastos anômalos")
gastos_anomalos().show()
logger.info("3. Análise de tendências de gastos ao longo do tempo")
tendencias_gastos().show()
logger.info("4. Segmentação de clientes com base em padrões de gastos")
segmentacao_clientes().show()
logger.info("5. Análise de correlação entre limite de crédito e gastos")
correlacao_limite_gastos().show()

logger.info("Financial analysis completed")
spark.stop()
