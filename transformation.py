import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand
from common_functions import load_config

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def initialize_spark_session(app_name="Transformation"):
    """
    Initialize a Spark session.

    Args:
        app_name (str): The name of the Spark application.

    Returns:
        SparkSession: The initialized Spark session.
    """
    logger.info("Initializing Spark session")
    return SparkSession.builder.appName(app_name).getOrCreate()

def load_tables(spark, database_name):
    """
    Load the clientes and transacoes_cartao tables.

    Args:
        spark (SparkSession): The active Spark session.

    Returns:
        tuple: A tuple containing the clientes and transacoes_cartao DataFrames.
    """
    logger.info("Loading tables")
    clientes = spark.sql(f"SELECT id_usuario FROM {database_name}.clientes")
    transacoes_cartao = spark.sql(f"SELECT * FROM {database_name}.transacoes_cartao")
    logger.debug(f"Loaded {clientes.count()} records from clientes")
    logger.debug(f"Loaded {transacoes_cartao.count()} records from transacoes_cartao")
    return clientes, transacoes_cartao

def repeat_clientes(clientes, transacoes_count):
    """
    Repeat and randomize the id_usuario column from clientes if necessary.

    Args:
        clientes (DataFrame): The clientes DataFrame.
        transacoes_count (int): The number of records in transacoes_cartao.

    Returns:
        DataFrame: The repeated and randomized clientes DataFrame.
    """
    clientes_count = clientes.count()
    logger.debug(f"Clientes count: {clientes_count}, Transacoes count: {transacoes_count}")
    if clientes_count < transacoes_count:
        logger.info("Repeating and randomizing id_usuario from clientes")
        repetition_factor = (transacoes_count // clientes_count) + 1
        clientes_repeated = clientes.withColumn("repeat", rand()).repartition("repeat")
        clientes_repeated = clientes_repeated.selectExpr("id_usuario").rdd.flatMap(lambda x: [x] * repetition_factor).toDF(["id_usuario"])
    else:
        logger.info("No need to repeat clientes")
        clientes_repeated = clientes
    return clientes_repeated

def update_transacoes_cartao(spark, database_name, clientes_repeated):
    """
    Update the transacoes_cartao table with id_usuario from clientes_repeated.

    Args:
        spark (SparkSession): The active Spark session.
        clientes_repeated (DataFrame): The repeated and randomized clientes DataFrame.

    Returns:
        DataFrame: The updated transacoes_cartao DataFrame.
    """
    logger.info("Updating transacoes_cartao with id_usuario from clientes_repeated")

    clientes_repeated.createOrReplaceTempView("clientes_repeated")

    # Load the transacoes_cartao table
    df_transacoes = spark.sql(f"SELECT * FROM {database_name}.transacoes_cartao")

    # Drop the old id_usuario column
    df_transacoes_cleaned = df_transacoes.drop("id_usuario")
    df_transacoes_cleaned.show(10, False)

    # Create a temporary view without the old id_usuario column
    df_transacoes_cleaned.createOrReplaceTempView(f"{database_name}.transacoes_cartao_cleaned")

    # Pré-calcular a fração
    frac = 1.0 / clientes_repeated.count()
    percentage = round(frac * 100, 6)

    updated_transacoes = spark.sql(f"""
        SELECT t.*, c.id_usuario
        FROM {database_name}.transacoes_cartao_cleaned t
        CROSS JOIN (SELECT * FROM clientes_repeated TABLESAMPLE ({percentage:.6f} PERCENT)) c
    """)
    return updated_transacoes

def save_updated_transacoes(spark, updated_transacoes, database_name):
    """
    Overwrite the existing transacoes_cartao table with the updated data.

    Args:
        spark (SparkSession): The active Spark session.
        updated_transacoes (DataFrame): The updated transacoes_cartao DataFrame.
        database_name (str): The name of the database.
    """
    logger.info("Saving updated transacoes_cartao table")
    temp_table_name = f"{database_name}.temp_transacoes_cartao"

    updated_transacoes.createOrReplaceTempView("temp_transacoes_view")

    spark.sql(f"""
        CREATE TABLE {temp_table_name}
        USING parquet
        PARTITIONED BY (data_execucao)
        AS SELECT 
            id_usuario,
            data_transacao,
            valor,
            estabelecimento,
            categoria,
            status
        FROM temp_transacoes_view t
    """)
    
    spark.sql(f"DROP TABLE IF EXISTS {database_name}.transacoes_cartao")
    spark.sql(f"ALTER TABLE {temp_table_name} RENAME TO {database_name}.transacoes_cartao")

    logger.info("Updated transacoes_cartao table saved successfully")

def main():
    """
    Main function to execute the transformation script.
    """
    spark = initialize_spark_session()
    config = load_config()
    database_name = config['DEFAULT'].get('dbname')
    clientes, transacoes_cartao = load_tables(spark, database_name)
    clientes_repeated = repeat_clientes(clientes, transacoes_cartao.count())
    updated_transacoes = update_transacoes_cartao(spark, database_name, clientes_repeated)
    save_updated_transacoes(spark, updated_transacoes, database_name)
    logger.info("Transformation completed successfully")

if __name__ == "__main__":
    main()
