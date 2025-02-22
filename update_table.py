import os, json, logging, sys
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import StructType
from pyspark.sql.functions import lit
from common_functions import load_config, gerar_dados, table_exists, get_schema_path
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_table_columns(spark: SparkSession, database_name: str, table_name: str) -> list:
    """
    Retrieves a list of valid column names from the table schema.

    This function fetches the schema of the specified table and extracts a list of column
    names, excluding partition information and special columns (e.g., '# col_name', 'data_type').

    Args:
        spark (SparkSession): The active Spark session.
        database_name (str): The name of the database containing the table.
        table_name (str): The name of the table.

    Returns:
        list: A list of valid column names for the table.

    Raises:
        Exception: If an error occurs while retrieving the table schema.
    """
    logger.debug(f"Retrieving table schema for {database_name}.{table_name}")
    try:
        table_schema = spark.sql(f"DESCRIBE {database_name}.{table_name}").collect()
        logger.debug(f"table_schema: {table_schema}")
        
        # Filter out rows with col_name='# col_name' or data_type='data_type'
        columns = [row['col_name'] for row in table_schema
                   if row['data_type'] != ''
                   and row['col_name'] != '# col_name'
                   and row['data_type'] != 'data_type']
        logger.info(f"Columns: {', '.join(columns)}")
        return columns
    except Exception as e:
        logger.error(f"Error retrieving table schema for {database_name}.{table_name}: {str(e)}")
        raise

def insert_data(spark: SparkSession, database_name: str, table_name: str, columns: list,
                partition_by: str = None, is_bucketed: bool = False) -> None:
    """
    Inserts data into the specified table, handling partitioning and bucketing.

    This function inserts data into the specified table from a temporary view named 'temp_view'.
    It supports tables that are partitioned, bucketed, or neither.

    Args:
        spark (SparkSession): The active Spark session.
        database_name (str): The name of the database containing the table.
        table_name (str): The name of the table to update.
        columns (list): A list of valid column names for the table.
        partition_by (str, optional): The column to partition by, if any. Defaults to None.
        is_bucketed (bool, optional): Whether the table is bucketed. Defaults to False.

    Raises:
        Exception: If an error occurs during the data insertion process.
    """
    logger.info(f"Inserting data into table: {database_name}.{table_name}")
    logger.debug(f"Partition by: {partition_by}, Is bucketed: {is_bucketed}")

    try:
        if partition_by:
            current_date = spark.sql("SELECT date_format(CURRENT_DATE(), 'dd-MM-yyyy') as date").collect()[0]['date']
            logger.debug(f"Inserting data with partition: {partition_by}='{current_date}'")
            non_partition_columns = [col for col in columns if col != partition_by]
            column_list = ", ".join(non_partition_columns)
            spark.sql(f"""
                INSERT INTO {database_name}.{table_name}
                PARTITION ({partition_by}='{current_date}')
                SELECT {column_list}
                FROM temp_view
            """)
        elif is_bucketed:
            logger.debug("Inserting data into bucketed table")
            column_list = ", ".join(columns)
            logger.info(f"Columns: {column_list}")  # Show columns for bucketed table
            spark.sql(f"""
                INSERT INTO {database_name}.{table_name}
                SELECT {column_list}
                FROM temp_view
            """)
        else:
            logger.debug("Inserting data without partition or bucketing")
            spark.sql(f"INSERT INTO {database_name}.{table_name} SELECT * FROM temp_view")

        logger.info(f"Data inserted into table '{table_name}' successfully.")
    except Exception as e:
        logger.error(f"Error inserting data into table '{table_name}': {str(e)}")
        raise

def update_table(spark: SparkSession, database_name: str, table_name: str, partition_by: str = None, is_bucketed: bool = False):
    """
    Update a table with new data, handling partitioning and bucketing.

    Args:
        spark (SparkSession): The active Spark session.
        database_name (str): The name of the database.
        table_name (str): The name of the table to update.
        partition_by (str, optional): The column to partition by, if any.
        is_bucketed (bool, optional): Whether the table is bucketed.
    """
    logger.info(f"Updating table: {table_name}")
    logger.debug(f"Partition by: {partition_by}")
    logger.debug(f"Is bucketed: {is_bucketed}")

    try:
        columns = get_table_columns(spark, database_name, table_name)
        insert_data(spark, database_name, table_name, columns, partition_by, is_bucketed)

    except Exception as e:
        logger.error(f"Error updating table '{table_name}': {str(e)}")
        raise

def create_spark_session(jdbc_url, thrift_server):
    """Creates and configures a Spark session.

    Args:
        jdbc_url (str): JDBC URL for Hive metastore.
        thrift_server (str): Thrift server URI.

    Returns:
        SparkSession: Configured Spark session.
    """
    logger.info("Creating Spark session")
    try:
        spark_conf = SparkConf()
        spark_conf.set("hive.metastore.client.factory.class", "com.cloudera.spark.hive.metastore.HivemetastoreClientFactory")
        spark_conf.set("hive.metastore.uris", thrift_server)
        spark_conf.set("spark.sql.hive.metastore.jars", "builtin")
        spark_conf.set("spark.sql.hive.hiveserver2.jdbc.url", jdbc_url)

        spark = SparkSession.builder.config(conf=spark_conf).appName("UpdateTable").enableHiveSupport().getOrCreate()
        logger.info("Spark session created successfully")
        return spark
    except Exception as e:
        logger.error(f"Error creating Spark session: {e}")
        raise

def generate_and_write_data(spark, config, table_name, clientes_id_usuarios=None):
    """Generates data and writes it to the specified table.

    Args:
        spark (SparkSession): The active Spark session.
        config (ConfigParser): The configuration object.
        table_name (str): The name of the table to update.
        clientes_id_usuarios (list, optional): List of client IDs if needed. Defaults to None.
    """
    logger.info(f"Generating and writing data for table: {table_name}")
    try:
        database_name = config.get("DEFAULT", "dbname")
        num_records_update = config.getint(table_name, 'num_records_update', fallback=100)
        partition_by = config.get(table_name, 'partition_by', fallback=None)
        base_path = "/app/mount"
        schema_path = get_schema_path(base_path, table_name)

        if not os.path.exists(schema_path):
            logger.error(f"Schema file not found for table '{table_name}': {schema_path}")
            return

        with open(schema_path, 'r') as f:
            schema = json.load(f)
        logger.debug("Schema loaded")

        if 'transacoes_cartao' in table_name:
            data = gerar_dados(table_name, num_records_update, clientes_id_usuarios)
            current_date = datetime.now().strftime("%d-%m-%Y")
            df = spark.createDataFrame(data, schema=StructType.fromJson(schema))
            df = df.withColumn(partition_by, lit(current_date))
            df.createOrReplaceTempView("temp_view")
            update_table(spark, database_name, table_name, partition_by)
        elif 'clientes' in table_name:
            data = gerar_dados(table_name, num_records_update)
            df = spark.createDataFrame(data, schema=StructType.fromJson(schema))
            # Apply bucketing for clientes table
            num_buckets = config.getint(table_name, 'num_buckets', fallback=5)
            df = df.repartition(num_buckets, "id_uf")
            df.createOrReplaceTempView("temp_view")
            update_table(spark, database_name, table_name, is_bucketed=True)
        else:
            data = gerar_dados(table_name, num_records_update)
            df = spark.createDataFrame(data, schema=StructType.fromJson(schema))
            df.createOrReplaceTempView("temp_view")
            update_table(spark, database_name, table_name)

        record_count = spark.sql(f"SELECT COUNT(*) FROM {database_name}.{table_name}").collect()[0][0]
        logger.info(f"Total records in table '{table_name}': {record_count}")

        logger.info("temp_view sample rows:")
        sample_rows = spark.sql(f"SELECT * FROM temp_view LIMIT 3").collect()
        for row in sample_rows:
            logger.info(str(row))

    except Exception as e:
        logger.error(f"Error generating and writing data for table '{table_name}': {e}")
        raise

def main():
    """
    Main function to update tables based on configuration.

    This function loads the configuration, iterates through the tables,
    generates new data, and updates each existing table.
    """
    logger.info("Starting table update process")
    config = load_config()
    logger.debug("Configuration loaded")

    # JDBC URL is now passed as a command line argument
    jdbc_url = sys.argv[1]
    logger.debug(f"JDBC URL: {jdbc_url}")

    # Extract the server DNS from the JDBC URL to construct the Thrift server URL
    server_dns = jdbc_url.split('//')[1].split('/')[0]
    thrift_server = f"thrift://{server_dns}:9083"
    logger.debug(f"Thrift Server: {thrift_server}")

    spark = create_spark_session(jdbc_url, thrift_server)
    database_name = config.get("DEFAULT", "dbname")
    tables = config.get("DEFAULT", "tables").split(",")

    # Generate clientes data first
    clientes_table = [table for table in tables if 'clientes' in table][0]
    logger.debug(f"Clientes table: {clientes_table}")
    clientes_num_records = config.getint(clientes_table, 'num_records', fallback=100)
    clientes_data = gerar_dados(clientes_table, clientes_num_records)
    clientes_id_usuarios = [cliente['id_usuario'] for cliente in clientes_data]

    # Acessando a lista de tabelas diretamente da seção DEFAULT
    for table_name in tables:
        table_name = table_name.strip()  # Remove espaços em branco se houver
        logger.info(f"Processing table: {table_name}")

        if table_exists(spark, database_name, table_name):
            try:
                generate_and_write_data(spark, config, table_name, clientes_id_usuarios)
            except Exception as e:
                logger.error(f"Failed to generate and write data for table '{table_name}': {e}")
        else:
            logger.warning(f"Table '{table_name}' does not exist. Cannot update.")

    logger.info("Table update process completed")
    spark.stop()
    logger.debug("Spark session stopped")

if __name__ == "__main__":
    main()