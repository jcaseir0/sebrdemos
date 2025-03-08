from pyspark.sql import SparkSession
from datetime import datetime
import sys, os, logging, time
from pyspark.sql.functions import List, col
# Adicionar o diretório pai ao caminho de busca do Python do pacote common_functions
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common_functions import load_config

# Configuração do logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session() -> SparkSession:
    """
    Cria uma sessão Spark com configurações específicas para o Iceberg.

    Args:
        username (str): Nome do usuário para o schema do banco de dados.

    Returns:
        SparkSession: Sessão Spark configurada.
    """
    logger.info("Criando sessão Spark...")
    spark = SparkSession \
        .builder \
        .appName("ICEBERG LOAD") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")\
        .config("spark.sql.catalog.spark_catalog.type", "hive")\
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")\
        .getOrCreate()
    logger.info("Sessão Spark criada com sucesso.")
    return spark

def show_partitions(spark: SparkSession, database_name: str, table_name: str) -> None:
    """
    Exibe as partições da tabela de informações antes da migração para o Iceberg.

    Args:
        spark (SparkSession): Sessão Spark ativa.
        database_name (str): Nome do banco de dados onde as tabelas se encontram.
        table_name (str): Nome do banco de dados onde as tabelas se encontram.
    """
    logger.info("Exibindo partições da tabela {table_name}...")
    print("PRE-ICEBERG MIGRATION TABLE PARTITIONS: \n")
    print(f"SHOW PARTITIONS {database_name}.{table_name}\n")
    spark.sql(f"SHOW PARTITIONS {database_name}.{table_name}").show(truncate=False)

def get_iceberg_tables(spark: SparkSession, database_name: str) -> List[str]:
    """
    Obtem uma lista de tabelas que contenham "iceberg" no nome da tabela no catálogo.

    Args:
        spark (SparkSession): Sessão Spark ativa.
        database_name (str): Nome do banco de dados.

    Returns:
        List[str]: Lista de nomes de tabelas que contenham "iceberg" no nome.
    """
    logger.info("Obtendo lista de tabelas Iceberg...")
    tblLst = spark.catalog.listTables(database_name)
    iceberg_tables = [table.name for table in tblLst if "iceberg" in table.name.lower()]
    logger.info(f"Encontradas {len(iceberg_tables)} tabelas com 'iceberg' no nome.")
    logger.debug(f"Lista de tabelas Iceberg: {iceberg_tables}")
    return iceberg_tables

def table_exists(spark, table_name):
            try:
                spark.sql(f"SELECT 1 FROM {table_name} LIMIT 1")
                return True
            except Exception:
                return False

def partition_exists(spark, table_name, partition_by, partition_date):
                partitions = spark.sql(f"SHOW PARTITIONS {table_name}").collect()
                for row in partitions:
                    if f"{partition_by}={partition_date}" in row[0]:
                        return True
                return False

def migrate_to_iceberg_ctas(spark: SparkSession, database_name: str, table_name: str, partition_by: str) -> None:
    """
    Migrates a table to Iceberg format using CREATE TABLE AS SELECT (CTAS).

    This function migrates the source table {database_name}.{table_name} to an Iceberg table
    named {database_name}.iceberg_{table_name}_ctas. It handles cases where the Iceberg
    table already exists and adds a new partition if required.

    Args:
        spark (SparkSession): The active Spark session.
        database_name (str): The name of the database containing the source table.
        table_name (str): The name of the source table to be migrated.

    Raises:
        Exception: If any error occurs during the migration process.
    """
    logger.info(f"Starting Iceberg migration for table {database_name}.{table_name} using CTAS...")

    source_table = f"{database_name}.{table_name}"
    iceberg_table = f"{database_name}.iceberg_{table_name}_ctas"
    partition_date = time.strftime("%d-%m-%Y")

    try:
        # Function to check if a table exists
        table_exists(spark, partition_by, database_name, table_name)

        if table_exists(spark, iceberg_table):
            logger.info(f"Iceberg table {iceberg_table} already exists.")

            # Function to check if a specific partition exists
            partition_exists(spark, table_name, partition_by, partition_date)

            if not partition_exists(spark, iceberg_table, partition_by, partition_date):
                logger.info(f"New partition detected. Inserting data for {partition_by}={partition_date}...")
                insert_query = f"""
                    INSERT INTO {iceberg_table} 
                    SELECT * FROM {source_table} 
                    WHERE frameworkdate = '{partition_date}'
                """
                spark.sql(insert_query)
                logger.info(f"Data inserted successfully into {iceberg_table} for {partition_by}={partition_date}.")
            else:
                logger.info(f"Partition for {partition_by}={partition_date} already loaded.")
                logger.info("Migration completed.")
        else:
            logger.info(f"Creating Iceberg table {iceberg_table} using CTAS...")

            # Unset the 'TRANSLATED_TO_EXTERNAL' property on the source table
            logger.info(f"Unsetting TBLPROPERTIES ('TRANSLATED_TO_EXTERNAL') on {source_table}...")
            spark.sql(f"ALTER TABLE {source_table} UNSET TBLPROPERTIES ('TRANSLATED_TO_EXTERNAL')")

            # Create the Iceberg table using CTAS
            create_table_query = f"""
                CREATE TABLE {iceberg_table} 
                USING iceberg 
                PARTITIONED BY ({partition_by}) 
                AS SELECT * FROM {source_table}
            """
            logger.info(f"Executing CTAS query: {create_table_query}")
            spark.sql(create_table_query)
            logger.info(f"Iceberg table {iceberg_table} created successfully.")

        logger.info("Iceberg migration completed successfully.")

    except Exception as e:
        logger.error(f"An error occurred during Iceberg migration: {str(e)}", exc_info=True)
        raise

def describe_table(spark: SparkSession, database_name: str, table_name: str) -> None:
    """
    Descreve a estrutura da tabela Iceberg.

    Args:
        spark (SparkSession): Sessão Spark ativa.
        username (str): Nome do usuário para o schema do banco de dados.
        table_name (str): Nome da tabela a ser descrita.
    """
    logger.info("Descrevendo tabela Iceberg...")
    print("DESCRIBE TABLE spark_catalog.{database_name}.{table_name}\n")
    spark.sql("DESCRIBE TABLE spark_catalog.{database_name}.{table_name}\n").show(20, False)

def show_partitions_post_migration(spark: SparkSession, username: str, table_name: str) -> None:
    """
    Exibe as partições da tabela Iceberg após a migração.

    Args:
        spark (SparkSession): Sessão Spark ativa.
        username (str): Nome do usuário para o schema do banco de dados.
        table_name (str): Nome da tabela.
    """
    logger.info("Exibindo partições da tabela Iceberg após a migração...")
    print("CUSTOMER TABLE POST-ICEBERG MIGRATION PARTITIONS: \n")
    spark.sql("SELECT * FROM spark_catalog.{}_CUSTOMER.{}.PARTITIONS".format(username, table_name)).show()

def show_iceberg_snapshots(spark: SparkSession, username: str, table_name: str) -> None:
    """
    Exibe os snapshots da tabela Iceberg.

    Args:
        spark (SparkSession): Sessão Spark ativa.
        username (str): Nome do usuário para o schema do banco de dados.
        table_name (str): Nome da tabela.
    """
    logger.info("Exibindo snapshots da tabela Iceberg...")
    print("#---------------------------------------------------")
    print("#            SHOW ICEBERG TABLE SNAPSHOTS           ")
    print("#---------------------------------------------------")
    print("\n")
    spark.sql("SELECT * FROM spark_catalog.{}_CUSTOMER.{}.history".format(username, table_name)).show(20, False)
    spark.sql("SELECT * FROM spark_catalog.{}_CUSTOMER.{}.snapshots".format(username, table_name)).show(20, False)

def insert_data(spark: SparkSession, username: str, table_name: str) -> None:
    """
    Insere dados na tabela Iceberg.

    Args:
        spark (SparkSession): Sessão Spark ativa.
        username (str): Nome do usuário para o schema do banco de dados.
        table_name (str): Nome da tabela.
    """
    logger.info("Inserindo dados na tabela Iceberg...")
    print("#---------------------------------------------------")
    print("#               INSERT DATA                         ")
    print("#---------------------------------------------------")
    print("\n")
    # PRE-INSERT COUNT
    print("PRE-INSERT COUNT")
    spark.sql("SELECT COUNT(*) FROM spark_catalog.{}_CUSTOMER.{}".format(username, table_name)).show()
    print("\n")
    
    # INSERT DATA VIA DATAFRAME API
    print("#---------------------------------------------------")
    print("#        INSERT DATA VIA DATAFRAME API              ")
    print("#---------------------------------------------------")
    temp_df = spark.sql("SELECT * FROM spark_catalog.{}_CUSTOMER.{}".format(username, table_name)).sample(fraction=0.3, seed=3)
    temp_df.writeTo("spark_catalog.{}_CUSTOMER.{}".format(username, table_name)).append()
    print("\n")
    
    # INSERT DATA VIA SPARK SQL
    print("#---------------------------------------------------")
    print("#        INSERT DATA VIA SPARK SQL                  ")
    print("#---------------------------------------------------")
    temp_df.createOrReplaceTempView("CUSTOMER_SAMPLE".format(username))
    insert_qry = "INSERT INTO spark_catalog.{0}_CUSTOMER.{1} SELECT * FROM CUSTOMER_SAMPLE".format(username, table_name)
    print(insert_qry)
    spark.sql(insert_qry)
    print("\n")

def time_travel(spark: SparkSession, username: str, table_name: str) -> None:
    """
    Realiza operações de time travel na tabela Iceberg.

    Args:
        spark (SparkSession): Sessão Spark ativa.
        username (str): Nome do usuário para o schema do banco de dados.
        table_name (str): Nome da tabela.
    """
    logger.info("Realizando operações de time travel...")
    print("#---------------------------------------------------")
    print("#               TIME TRAVEL                         ")
    print("#---------------------------------------------------")
    
    # NOTICE SNAPSHOTS HAVE BEEN ADDED
    spark.sql("SELECT * FROM spark_catalog.{}_CUSTOMER.{}.history".format(username, table_name)).show(20, False)
    spark.sql("SELECT * FROM spark_catalog.{}_CUSTOMER.{}.snapshots".format(username, table_name)).show(20, False)

    # POST-INSERT COUNT
    print("\n")
    print("POST-INSERT COUNT")
    spark.sql("SELECT COUNT(*) FROM spark_catalog.{}_CUSTOMER.{}".format(username, table_name)).show()

    # TIME TRAVEL AS OF PREVIOUS TIMESTAMP
    now = datetime.now()
    timestamp = datetime.timestamp(now)
    df = spark.read.option("as-of-timestamp", int(timestamp*1000)).format("iceberg").load("spark_catalog.{}_CUSTOMER.{}".format(username, table_name))

    # POST TIME TRAVEL COUNT
    print("\n")
    print("POST-TIME TRAVEL COUNT")
    print(df.count())

def incremental_read(spark: SparkSession, username: str, table_name: str) -> None:
    """
    Realiza leitura incremental da tabela Iceberg.

    Args:
        spark (SparkSession): Sessão Spark ativa.
        username (str): Nome do usuário para o schema do banco de dados.
        table_name (str): Nome da tabela.
    """
    logger.info("Realizando leitura incremental...")
    print("#---------------------------------------------------")
    print("#               INCREMENTAL READ                    ")
    print("#---------------------------------------------------")
    
    print("\n")
    print("INCREMENTAL READ")
    print("\n")
    print("ICEBERG TABLE HISTORY (SHOWS EACH SNAPSHOT AND TIMESTAMP)")
    print("SELECT * FROM {}_CUSTOMER.{}.history;".format(username, table_name))
    spark.sql("SELECT * FROM {}_CUSTOMER.{}.history;".format(username, table_name)).show()
    print
