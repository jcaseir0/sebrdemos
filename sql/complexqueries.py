from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, avg, rank, stddev, lead, date_trunc, when, corr
from pyspark.sql.window import Window
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize Spark session
logger.info("Initializing Spark session")
spark = SparkSession.builder \
    .appName("FinancialAnalysis") \
    .config("spark.sql.catalogImplementation", "hive") \
    .enableHiveSupport() \
    .getOrCreate()

logger.info("Spark session initialized successfully")

# Read tables
clientes = spark.table("bancodemo.clientes")
transacoes = spark.table("bancodemo.transacoes_cartao")

# 1. Análise de gastos por cliente e categoria, com ranking
def gastos_por_cliente_categoria():
    return transacoes.groupBy("id_usuario", "categoria") \
        .agg(sum("valor").alias("total_gastos")) \
        .join(clientes, "id_usuario") \
        .withColumn("ranking_categoria", rank().over(Window.partitionBy("categoria").orderBy(sum("valor").desc())))

# 2. Detecção de padrões de gastos anômalos
def gastos_anomalos():
    cliente_stats = transacoes.groupBy("id_usuario") \
        .agg(avg("valor").alias("media_gasto"), stddev("valor").alias("desvio_padrao_gasto"))
    
    return transacoes.join(cliente_stats, "id_usuario") \
        .join(clientes, "id_usuario") \
        .filter(transacoes.valor > (cliente_stats.media_gasto + (3 * cliente_stats.desvio_padrao_gasto)))

# 3. Análise de tendências de gastos ao longo do tempo
def tendencias_gastos():
    return transacoes.groupBy(date_trunc("month", "data_transacao").alias("mes"), "categoria") \
        .agg(count("*").alias("num_transacoes"), 
             sum("valor").alias("total_gastos"), 
             avg("valor").alias("media_gasto")) \
        .withColumn("media_gasto_proximo_mes", lead("media_gasto").over(Window.partitionBy("categoria").orderBy("mes"))) \
        .withColumn("variacao_percentual", (lead("media_gasto").over(Window.partitionBy("categoria").orderBy("mes")) - avg("valor")) / avg("valor") * 100)

# 4. Segmentação de clientes com base em padrões de gastos
def segmentacao_clientes():
    cliente_metricas = transacoes.groupBy("id_usuario") \
        .agg(count(date_trunc("month", "data_transacao").alias("meses_ativos")),
             sum("valor").alias("total_gastos"),
             avg("valor").alias("media_gasto_por_transacao"),
             count("*").alias("num_transacoes")) \
        .join(clientes, "id_usuario")
    
    return cliente_metricas.withColumn("segmento_cliente", 
        when((cliente_metricas.meses_ativos >= 10) & (cliente_metricas.total_gastos > 50000), "VIP")
        .when((cliente_metricas.meses_ativos >= 6) & (cliente_metricas.total_gastos > 25000), "Regular")
        .when((cliente_metricas.meses_ativos >= 3) & (cliente_metricas.total_gastos > 10000), "Ocasional")
        .otherwise("Inativo"))

# 5. Análise de correlação entre limite de crédito e gastos
def correlacao_limite_gastos():
    gastos_cliente = transacoes.groupBy("id_usuario") \
        .agg(sum("valor").alias("total_gastos"), count("*").alias("num_transacoes"))
    
    return clientes.join(gastos_cliente, "id_usuario") \
        .withColumn("percentual_limite_utilizado", (gastos_cliente.total_gastos / clientes.limite_credito) * 100) \
        .withColumn("correlacao_limite_gastos", corr(clientes.limite_credito, gastos_cliente.total_gastos).over())

# Execute and show results
logger.info("Executing financial analysis queries")

gastos_por_cliente_categoria().show()
gastos_anomalos().show()
tendencias_gastos().show()
segmentacao_clientes().show()
correlacao_limite_gastos().show()

logger.info("Financial analysis completed")
spark.stop()
