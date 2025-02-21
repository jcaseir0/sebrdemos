import os, json, logging, sys, time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark import SparkConf
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import lit
from common_functions import load_config, gerar_dados, table_exists, validate_hive_metastore, get_schema_path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_table(spark, database_name, table_name, config):
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
        database_name = config['DEFAULT'].get('dbname')

        logger.debug(f"Table configuration: partition={partition}, partition_by={partition_by}, bucketing={bucketing}, clustered_by={clustered_by}, num_buckets={num_buckets}")

        if partition:
            logger.info(f"Creating partitioned table: {database_name}.{table_name}")
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {database_name}.{table_name}
                USING parquet
                PARTITIONED BY ({partition_by})
                AS SELECT * FROM temp_view
            """)
        elif bucketing:
            logger.info(f"Creating bucketed table: {database_name}.{table_name}")
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {database_name}.{table_name}
                USING parquet
                CLUSTERED BY ({clustered_by}) INTO {num_buckets} BUCKETS
                AS SELECT * FROM temp_view
            """)
        else:
            logger.info(f"Creating standard table: {database_name}.{table_name}")
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {database_name}.{table_name}
                USING parquet
                AS SELECT * FROM temp_view
            """)
        logger.info(f"Table '{database_name}.{table_name}' created successfully.\n")
    except Exception as e:
        logger.error(f"Error creating table '{database_name}.{table_name}': {str(e)}")
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
        logger.error(f"Error: The '{table_name}' table cannot have partition and bucketing True.")
        sys.exit(1)

def validate_table_creation(spark, database_name, table_name):
    """
    Validate the creation of all tables in the specified database and provide a summary of their structure and content.

    Args:
        spark (SparkSession): The active Spark session.
        database_name (str): The name of the database containing the tables.
        table_name (str): The name of the table to validate.

    Returns:
        list: A list of dictionaries containing each table's existence status, structure, record count, and sample rows.
              Returns a list with a single error dictionary if an error occurs during validation.
    """
    results = []
    try:
        # Get all tables in the database
        tables = spark.sql(f"SHOW TABLES IN {database_name}").collect()

        for table in tables:
            table_name = table.tableName
            full_table_name = f"{database_name}.{table_name}"

            if table_name == "temp_view":
                continue

            try:
                # Get table structure
                table_structure = spark.sql(f"SHOW CREATE TABLE {full_table_name}").collect()[0][0]

                # Count records
                record_count = spark.sql(f"SELECT COUNT(*) FROM {full_table_name}").collect()[0][0]

                # Get first 10 rows
                sample_rows = spark.sql(f"SELECT * FROM {full_table_name} LIMIT 3").collect()

                results.append({
                    "table_name": full_table_name,
                    "exists": True,
                    "structure": table_structure,
                    "record_count": record_count,
                    "sample_rows": [row.asDict() for row in sample_rows]
                })
                
                logger.info(f"Validated table: {full_table_name}")
                logger.info(f"Record count: {record_count}")
                logger.info(f"Table structure:\n{table_structure}")
                logger.info("Sample rows:")
                for row in sample_rows:
                    logger.info(str(row))
                
            except Exception as e:
                results.append({
                    "table_name": full_table_name,
                    "exists": False,
                    "error": str(e)
                })
                logger.error(f"Error validating table {full_table_name}: {str(e)}")

        return results

    except Exception as e:
        logger.error(f"Failed to retrieve tables from database '{database_name}': {str(e)}")
        return [{"error": f"Failed to retrieve tables from database '{database_name}': {str(e)}"}]

def remove_specified_tables(spark: SparkSession, database_name: str, config):
    """
    Remove specified tables from the Hive Metastore.

    This function attempts to drop the tables specified in the config file
    within the given database. It logs the process and any errors encountered.

    Args:
        spark (SparkSession): The active Spark session.
        database_name (str): The name of the database containing the tables.
        config (ConfigParser): The configuration object containing table names.

    Returns:
        bool: True if all specified tables were successfully removed, False otherwise.
    """
    logger.info(f"Starting removal of specified tables in database '{database_name}'")
    
    try:
        # Get the list of tables to remove from config
        tables_to_remove = config['DEFAULT']['tables'].split(',')
        
        # Check if the database exists
        databases = spark.sql("SHOW DATABASES").collect()
        if database_name not in [row.namespace for row in databases]:
            logger.warning(f"Database '{database_name}' does not exist. Cannot remove tables.")
            return False

        # Drop each specified table
        for table_name in tables_to_remove:
            full_table_name = f"{database_name}.{table_name.strip()}"
            logger.debug(f"Attempting to drop table '{full_table_name}'")
            try:
                spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
                logger.info(f"Successfully dropped table '{full_table_name}'")
            except AnalysisException as e:
                logger.error(f"Failed to drop table '{full_table_name}': {str(e)}")

        return True

    except Exception as e:
        logger.error(f"An error occurred while removing specified tables in database '{database_name}': {str(e)}")
        return False

def main():
    """
    Main function to create tables based on configuration.

    This function validates the Hive metastore connection, iterates through the tables
    defined in the configuration, and creates them if they do not already exist.
    """
    logger.info("Starting main function")
    config = load_config()
    # JDBC URL is now passed as a command line argument
    jdbc_url = sys.argv[1]
    logger.debug(f"JDBC URL: {jdbc_url}")

    # Extract the server DNS from the JDBC URL to construct the Thrift server URL
    server_dns = jdbc_url.split('//')[1].split('/')[0]
    thrift_server = f"thrift://{server_dns}:9083"
    logger.debug(f"Thrift Server: {thrift_server}")

    spark_conf = SparkConf()
    spark_conf.set("hive.metastore.client.factory.class", "com.cloudera.spark.hive.metastore.HivemetastoreClientFactory")
    spark_conf.set("hive.metastore.uris", thrift_server)
    spark_conf.set("spark.sql.hive.metastore.jars", "builtin")
    spark_conf.set("spark.sql.hive.hiveserver2.jdbc.url", jdbc_url)
    spark_conf.set("spark.security.credentials.hiveserver2.enabled", "true")
    
    spark = SparkSession.builder.config(conf=spark_conf).appName("CreateTable").enableHiveSupport().getOrCreate()
    
    validate_hive_metastore(spark)

    database_name = config['DEFAULT'].get('dbname')
    tables = config['DEFAULT']['tables'].split(',')
    base_path = "/app/mount"

    # Remove as tabelas se existirem
    for table_name in tables:
        if remove_specified_tables(spark, database_name, config):
            logger.info(f"Successfully removed table '{table_name}'\n")
        else:
            logger.error(f"Failed to remove table '{table_name}'\n")

    logger.debug(f"Creating database: {database_name}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
    logger.info(f"Database: {database_name} created successfully\n")

    logger.info(f"Processing tables: {tables}")

    # Generate clientes data first
    clientes_table = [table for table in tables if 'clientes' in table][0]
    clientes_num_records = config.getint(clientes_table, 'num_records', fallback=100)
    clientes_data = gerar_dados(clientes_table, clientes_num_records)
    clientes_id_usuarios = [cliente['id_usuario'] for cliente in clientes_data]

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

        if not table_exists(spark, database_name, table_name):
            logger.info(f"Table '{table}' does not exist. Creating...")
            current_date = time.strftime("%d-%m-%Y")
            num_records = config.getint(table_name, 'num_records', fallback=100)
            partition_by = config.get(table_name, 'partition_by', fallback=None)
            
            if 'transacoes_cartao' in table_name:
                data = gerar_dados(table_name, num_records, clientes_id_usuarios)
            else:
                data = gerar_dados(table_name, num_records)

            df = spark.createDataFrame(data, schema=StructType.fromJson(schema))
            
            if partition:
                df = df.withColumn(partition_by, lit(current_date))
            df.createOrReplaceTempView("temp_view")
            logger.info("temp_view sample rows with new column:")
            sample_rows = spark.sql(f"SELECT * FROM temp_view LIMIT 3").collect()
            for row in sample_rows:
                    logger.info(str(row))
            print()
            create_table(spark, database_name, table_name, config)
        else:
            logger.info(f"Table '{table}' already exists. Skipping creation.")

        validate_table_creation(spark, database_name, table_name)
        logger.info("Table creation process completed.")

    spark.stop()

if __name__ == "__main__":
    main()
