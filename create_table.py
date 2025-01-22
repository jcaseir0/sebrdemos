import os
import json
import logging
import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark import SparkConf
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import lit
from common_functions import load_config, gerar_dados, table_exists

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_table(spark, table_name, config):
    """
    Create a table in Hive with the specified configuration.

    Args:
        spark (SparkSession): The Spark session.
        table_name (str): The fully qualified name of the table (database.table_name).
        config (ConfigParser): The configuration object containing table settings.

    Raises:
        Exception: If an error occurs during table creation.
    """
    logger.info(f"Creating table: {table_name}")
    try:
        partition = config.getboolean(table_name, 'particionamento', fallback=False)
        partition_by = config.get(table_name, 'partition_by', fallback=None)
        bucketing = config.getboolean(table_name, 'bucketing', fallback=False)
        clustered_by = config.get(table_name, 'clustered_by', fallback=None)
        num_buckets = config.getint(table_name, 'num_buckets', fallback=0)

        logger.debug(f"Table configuration: partition={partition}, partition_by={partition_by}, bucketing={bucketing}, clustered_by={clustered_by}, num_buckets={num_buckets}")

        if partition:
            logger.info(f"Creating partitioned table: {table_name}")
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {table_name}
                USING parquet
                PARTITIONED BY ({partition_by})
                AS SELECT * FROM temp_view
            """)
        elif bucketing:
            logger.info(f"Creating bucketed table: {table_name}")
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {table_name}
                USING parquet
                CLUSTERED BY ({clustered_by}) INTO {num_buckets} BUCKETS
                AS SELECT * FROM temp_view
            """)
        else:
            logger.info(f"Creating standard table: {table_name}")
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {table_name}
                USING parquet
                AS SELECT * FROM temp_view
            """)
        logger.info(f"Table '{table_name}' created successfully.\n")
    except Exception as e:
        logger.error(f"Error creating table '{table_name}': {str(e)}")
        raise

def validate_partition_and_bucketing(config, table_name):
    """
    Validate that a table does not have both partitioning and bucketing enabled.

    Args:
        config (ConfigParser): The configuration object containing table settings.
        table_name (str): The name of the table to validate.

    Raises:
        SystemExit: If both partitioning and bucketing are enabled for the table.
    """
    logger.info(f"Validating partitioning and bucketing for table: {table_name}")
    partitioning = config.getboolean(table_name, 'particionamento', fallback=False)
    bucketing = config.getboolean(table_name, 'bucketing', fallback=False)

    if partitioning and bucketing:
        logger.error(f"Erro: A tabela '{table_name}' não pode ter particionamento e bucketing ativados ao mesmo tempo.")
        sys.exit(1)

def get_schema_path(base_path, table_name):
    """
    Get the schema file path for a given table.

    Args:
        base_path (str): The base path where schema files are stored.
        table_name (str): The name of the table.

    Returns:
        str: The full path to the schema file.
    """
    logger.info(f"Getting schema path for table: {table_name}")
    return f"{base_path}/{table_name}.json"

def validate_hive_metastore(spark, max_retries=3, retry_delay=5):
    """
    Validate the connection to the Hive metastore with retry logic.

    Args:
        spark (SparkSession): The Spark session.
        max_retries (int): Maximum number of retries.
        retry_delay (int): Delay between retries in seconds.

    Returns:
        bool: True if the connection is successful, False otherwise.

    Raises:
        AnalysisException: If the connection fails after all retries.
    """
    logger.info("Validating Hive metastore connection")
    for attempt in range(max_retries):
        try:
            spark.sql("SHOW DATABASES").show()
            logger.info("Conexão com o Hive metastore estabelecida com sucesso")
            return True
        except AnalysisException as e:
            if attempt < max_retries - 1:
                logger.warning(f"Tentativa {attempt + 1} falhou. Tentando novamente em {retry_delay} segundos...")
                time.sleep(retry_delay)
            else:
                logger.error("Falha ao conectar ao Hive metastore após várias tentativas")
                raise
    return False

def validate_table_creation(spark, database_name, table_name):
    """
    Validate the creation of a table in the Hive Metastore and provide a summary of its structure and content.

    This function checks if the table exists, retrieves its structure, counts the number of records,
    and displays the first 10 rows.

    Args:
        spark (SparkSession): The active Spark session.
        database_name (str): The name of the database containing the table.
        table_name (str): The name of the table to validate.

    Returns:
        dict: A dictionary containing the table's existence status, structure, record count, and sample rows.
              Returns False if an error occurs during validation.
    """
    try:
        full_table_name = f"{database_name}.{table_name}"
        logger.debug(f"Validating table: {full_table_name}")

        result = spark.sql(f"SHOW TABLES IN {database_name} LIKE '{table_name}'").count() > 0

        if result:
            logger.info(f"Table '{full_table_name}' exists in the Hive Metastore.")

            # Get table structure
            logger.debug(f"Retrieving structure for table: {full_table_name}")
            table_structure = spark.sql(f"SHOW CREATE TABLE {full_table_name}").collect()[0][0]
            logger.info(f"Table structure for '{full_table_name}':\n{table_structure}")

            # Count records
            logger.debug(f"Counting records in table: {full_table_name}")
            record_count = spark.sql(f"SELECT COUNT(*) FROM {full_table_name}").collect()[0][0]
            logger.info(f"Table '{full_table_name}' contains {record_count} records.")

            # Get first 10 rows
            logger.debug(f"Retrieving sample rows from table: {full_table_name}")
            sample_rows = spark.sql(f"SELECT * FROM {full_table_name} LIMIT 10").collect()
            logger.info(f"First 10 rows of '{full_table_name}':")
            for row in sample_rows:
                logger.info(str(row))

            return {
                "exists": True,
                "structure": table_structure,
                "record_count": record_count,
                "sample_rows": sample_rows
            }
        else:
            logger.warning(f"Table '{full_table_name}' does not exist in the Hive Metastore.")
            return {"exists": False}

    except AnalysisException as e:
        logger.error(f"Error validating table '{table_name}' in database '{database_name}': {str(e)}")
        return False

def remove_database_and_tables(spark: SparkSession, database_name: str):
    """
    Remove a database and all its tables from the Hive Metastore.

    This function attempts to drop all tables within the specified database
    and then drops the database itself. It logs the process and any errors encountered.

    Args:
        spark (SparkSession): The active Spark session.
        database_name (str): The name of the database to be removed.

    Returns:
        bool: True if the database and all its tables were successfully removed, False otherwise.
    """
    logger.info(f"Starting removal of database '{database_name}' and all its tables")
    
    try:
        # Check if the database exists
        databases = spark.sql("SHOW DATABASES").collect()
        if database_name not in [row.databaseName for row in databases]:
            logger.warning(f"Database '{database_name}' does not exist. Nothing to remove.")
            return True

        # List all tables in the database
        logger.debug(f"Listing tables in database '{database_name}'")
        tables = spark.sql(f"SHOW TABLES IN {database_name}").collect()
        
        # Drop each table
        for table in tables:
            table_name = table.tableName
            full_table_name = f"{database_name}.{table_name}"
            logger.info(f"Attempting to drop table '{full_table_name}'")
            try:
                spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
                logger.info(f"Successfully dropped table '{full_table_name}'")
            except AnalysisException as e:
                logger.error(f"Failed to drop table '{full_table_name}': {str(e)}")
                return False

        # Drop the database
        logger.info(f"Attempting to drop database '{database_name}'")
        spark.sql(f"DROP DATABASE IF EXISTS {database_name}")
        logger.info(f"Successfully dropped database '{database_name}'")

        return True

    except Exception as e:
        logger.error(f"An error occurred while removing database '{database_name}': {str(e)}")
        return False

def main():
    """
    Main function to create tables based on configuration.

    This function validates the Hive metastore connection, iterates through the tables
    defined in the configuration, and creates them if they do not already exist.
    """
    logger.info("Starting main function")
    config = load_config()
    jdbc_url = config['DEFAULT'].get('hmsUrl')
    thrift_server = config['DEFAULT'].get('thriftServer')

    logger.debug(f"JDBC URL: {jdbc_url}")
    logger.debug(f"Thrift Server: {thrift_server}")

    spark_conf = SparkConf()
    spark_conf.set("hive.metastore.client.factory.class", "com.cloudera.spark.hive.metastore.HivemetastoreClientFactory")
    spark_conf.set("hive.metastore.uris", thrift_server)
    spark_conf.set("spark.sql.hive.metastore.jars", "builtin")
    spark_conf.set("spark.sql.hive.hiveserver2.jdbc.url", jdbc_url)
    
    spark = SparkSession.builder.config(conf=spark_conf).appName("CreateTable").enableHiveSupport().getOrCreate()
    
    validate_hive_metastore(spark)

    database_name = config['DEFAULT'].get('dbname')
    if remove_database_and_tables(spark, database_name):
        logger.info(f"Successfully removed database '{database_name}' and all its tables")
    else:
        logger.error(f"Failed to remove database '{database_name}' and all its tables")
    logger.info(f"Creating database: {database_name}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")

    tables = config['DEFAULT']['tables'].split(',')
    base_path = "/app/mount"

    logger.info(f"Processing tables: {tables}")

    for table_name in tables:
        logger.info(f"Processing table: {table_name}")
        partition = config.getboolean(table_name, 'particionamento', fallback=False)
        partition_by = config.get(table_name, 'partition_by', fallback=None)
        table = f"{database_name}.{table_name}"
        validate_partition_and_bucketing(config, table_name)
        schema_path = get_schema_path(base_path, table_name)

        if not os.path.exists(schema_path):
            logger.error(f"Schema file not found for table '{table}': {schema_path}")
            continue

        with open(schema_path, 'r') as f:
            schema = json.load(f)

        if not table_exists(spark, table_name):
            logger.info(f"Table '{table}' does not exist. Creating...")
            current_date = time.strftime("%d-%m-%Y")
            num_records = config.getint(table_name, 'num_records', fallback=100)
            partition_by = config.get(table_name, 'partition_by', fallback=None)
            data = gerar_dados(table_name, num_records)
            df = spark.createDataFrame(data, schema=StructType.fromJson(schema))
            if partition:
                df = df.withColumn(partition_by, lit(current_date))
            df.createOrReplaceTempView("temp_view")
            create_table(spark, table, config)
        else:
            logger.info(f"Table '{table}' already exists. Skipping creation.")

    logger.info("Table creation process completed.")

    validate_table_creation(spark, database_name, table_name)

    spark.stop()

if __name__ == "__main__":
    main()
