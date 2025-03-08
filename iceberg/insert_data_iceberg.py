from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand, current_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, DoubleType, StringType
import random
from faker import Faker

fake = Faker()

def gerar_transacao(id_usuario):
    """
    Generate a random transaction record.

    Args:
        id_usuario (int): ID do usuário.

    Returns:
        dict: A dictionary containing transaction details.
    """
    logger.debug("Generating transaction record")

    # Calculate the date range for the last 10 years
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365 * 10)

    return {
        "id_usuario": id_usuario,
        "data_transacao": fake.date_time_between(start_date=start_date, end_date=end_date),
        "valor": round(random.uniform(10, 100000), 2),
        "estabelecimento": fake.company(),
        "categoria": random.choice(["Alimentação", "Transporte", "Entretenimento", "Saúde", "Educação", "Outros"]),
        "status": random.choice(["Aprovada", "Negada", "Pendente", "Estornada", "Cancelada", "Fraude"])
    }

def criar_transacoes(spark: SparkSession, num_transacoes: int) -> None:
    """
    Cria transações aleatórias e as insere na tabela transacoes_cartao.

    Args:
        spark (SparkSession): Sessão Spark ativa.
        num_transacoes (int): Número de transações a serem geradas.
    """
    logger.info("Gerando transações aleatórias...")
    
    # Carregar IDs de usuários da tabela clientes
    ids_usuarios = spark.sql("SELECT id_usuario FROM bancodemo.clientes_iceberg_ctas_hue").rdd.flatMap(lambda x: x).collect()
    
    # Gerar transações aleatórias
    transacoes = []
    for _ in range(num_transacoes):
        id_usuario = random.choice(ids_usuarios)
        transacao = gerar_transacao(id_usuario)
        transacoes.append(transacao)
    
    # Criar DataFrame com as transações
    schema = StructType([
        StructField("id_usuario", IntegerType(), True),
        StructField("data_transacao", TimestampType(), True),
        StructField("valor", DoubleType(), True),
        StructField("estabelecimento", StringType(), True),
        StructField("categoria", StringType(), True),
        StructField("status", StringType(), True)
    ])
    
    df_transacoes = spark.createDataFrame(transacoes, schema)
    
    # Inserir transações na tabela
    df_transacoes.write.format("iceberg").mode("append").saveAsTable("bancodemo.transacoes_cartao")

# Criar sessão Spark
spark = SparkSession.builder.appName("Transacoes").getOrCreate()

# Gerar e inserir transações
criar_transacoes(spark, 500)
