import logging, sys
from configparser import ConfigParser
from pyspark.sql import SparkSession
from pyspark import SparkConf
from common_functions import load_config, gerar_dados, table_exists, get_table_columns
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_spark_session(logger:logging, jdbc_url: str, thrift_server: str) -> SparkSession:
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

def insert_data(logger:logging, spark: SparkSession, database_name: str, table_name: str, columns: list,
                partition_by: str = None, is_bucketed: bool = False) -> None:
    """
    Inserts data into the specified table, handling partitioning and bucketing.

    This function inserts data into the specified table from a temporary view.
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
    if partition_by:
        logger.info(f"Partition by: {partition_by}")
    elif is_bucketed: 
        logger.info(f"Is bucketed: {is_bucketed}")
    else:
        logger.info("No partitioning or bucketing")

    try:
        column_list = ", ".join(columns)
        logger.info(f"Inserting Columns: {column_list}")

        if partition_by:
            current_date = datetime.now().strftime("%d-%m-%Y")
            logger.info(f"Inserting data with partition: {partition_by}='{current_date}'")
            spark.sql(f"""
                INSERT INTO {database_name}.{table_name}
                PARTITION ({partition_by}='{current_date}')
                SELECT {column_list}
                FROM temp_view
            """)
        else:
            logger.debug("Inserting data without partition or with bucketing")
            spark.sql(f"""
                INSERT INTO {database_name}.{table_name}
                SELECT {column_list}
                FROM temp_view
            """)

        logger.info(f"Data inserted into table '{table_name}' successfully.")
    except Exception as e:
        logger.error(f"Error inserting data into table '{table_name}': {str(e)}")
        raise

def display_table_samples(logger:logging, tables: list, generated_data: dict) -> None:
    """
    Displays sample rows from specified tables and checks for matching IDs.
    
    Args:
        spark (SparkSession): The active Spark session.
        database_name (str): The name of the database containing the table.
        tables (list): A list of tables of the database.
        generated_data (dict): A dictionary containing the generated data for each table
    """
    logger.info("Displaying sample rows from tables")

    for table_name in tables:
        if table_name in generated_data:
            sample_rows = generated_data[table_name][:3]
            logger.info(f"Sample rows from table '{table_name}':")
            for row in sample_rows:
                logger.info(str(row))
        else:
            logger.warning(f"No generated data found for table '{table_name}'")
        
    if 'transacoes_cartao' in tables and 'clientes' in tables:
        transacoes_ids = [row['id_usuario'] for row in sample_rows]
        logger.info(f"Sampled id_usuario from 'transacoes_cartao' table: {transacoes_ids}")

        clientes_sample = [row for row in generated_data['clientes'] if row['id_usuario'] in transacoes_ids][:3]

        clientes_ids = [row['id_usuario'] for row in clientes_sample]
        logger.info(f"Sampled id_usuario from 'clientes' table: {clientes_ids}")

        logger.info("Matching sample rows from 'clientes' table:")
        for row in clientes_sample:
            logger.info(str(row))

def generate_and_write_data(logger: logging, spark: SparkSession, config: ConfigParser, database_name: str, table_name: str) -> None:
    """Generates data and writes it to the specified table.

    Args:
        spark (SparkSession): The active Spark session.
        config (ConfigParser): The configuration object.
        table_name (str): The name of the table to update.
        clientes_data (list): Data for the 'clientes' table, structured as a list of dictionaries.
    """
    logger.info(f"Generating and writing data for table: {database_name}.{table_name}")
    
    try:
        num_records_update = config.getint(table_name, 'num_records_update', fallback=100)
        logger.info(f"Number of records to update: {num_records_update}")
        partition_by = config.get(table_name, 'partition_by', fallback=None)
        bucketing_column = config.get(table_name, 'clustered_by', fallback=None)
        is_bucketed = config.getboolean(table_name, 'bucketing', fallback=False)
        logger.info(f"Is bucketed: {is_bucketed}")
        num_buckets = config.getint(table_name, 'num_buckets', fallback=5) if is_bucketed else 0
        logger.info(f"Partition by: {partition_by}, Bucketing column: {bucketing_column}, Num Buckets: {num_buckets}")

        tables = spark.sql(f"SHOW TABLES IN {database_name}").select("tableName").rdd.flatMap(lambda x: x).collect()
        logger.info(f"Tables in database {database_name}: {tables}")
        clientes_table = [table for table in tables if 'clientes' in table][0]
        num_records_update = config.getint(clientes_table, 'num_records_update', fallback=100)
        clientes_data = gerar_dados(clientes_table, num_records_update)

        if 'transacoes_cartao' in table_name:
            clientes_ids = [cliente['id_usuario'] for cliente in clientes_data] if clientes_data else None
            data = gerar_dados(table_name, num_records_update, clientes_ids)
        elif 'clientes' in table_name:
            data = gerar_dados(table_name, num_records_update) if clientes_data is None else clientes_data
        else:
            data = gerar_dados(table_name, num_records_update)
            
        logger.debug(f"Sample data: {data[:3]}")

        record_count_before = spark.sql(f"SELECT COUNT(*) FROM {database_name}.{table_name}").collect()[0][0]
        logger.info(f"Total records in table '{table_name}' before insert: {record_count_before}")

        columns = get_table_columns(logger, spark, database_name, table_name)
        logger.debug(f"Columns: {columns}")

        table_schema = spark.table(f"{database_name}.{table_name}").schema
        logger.debug(f"Table schema: {table_schema}")

        df = spark.createDataFrame(data, schema=table_schema)

        if is_bucketed:
            logger.info(f"Repartitioning dataframe by {bucketing_column} into {num_buckets} buckets")
            df = df.repartition(num_buckets, bucketing_column)

        df.createOrReplaceTempView("temp_view")

        insert_columns = [col for col in columns if col != 'data_execucao'] if 'transacoes_cartao' in table_name else columns

        insert_data(spark, database_name, table_name, insert_columns, partition_by if not is_bucketed else None, is_bucketed)
        
        record_count_after = spark.sql(f"SELECT COUNT(*) FROM {database_name}.{table_name}").collect()[0][0]
        logger.info(f"Total records in table '{table_name}' after insert: {record_count_after}")

        return data
    
    except Exception as e:
        logger.error(f"Error generating and writing data for table '{table_name}': {e}", exc_info=True)
        raise

def main():
    """
    Main function to update tables based on configuration.

    This function loads the configuration, iterates through the tables,
    generates new data, and updates each existing table.
    """

    logger = logging.getLogger(__name__)

    logger.info("Starting table update process")
    config = load_config(logger)
    logger.debug("Configuration loaded")

    if not config:
        logger.error("Failed to load configuration. Exiting.")
        return
    logger.debug("Configuration loaded")

    spark = None
    try:
        jdbc_url = sys.argv[1]
        logger.debug(f"JDBC URL: {jdbc_url}")

        server_dns = jdbc_url.split('//')[1].split('/')[0]
        thrift_server = f"thrift://{server_dns}:9083"
        logger.debug(f"Thrift Server: {thrift_server}")

        spark = create_spark_session(logger, jdbc_url, thrift_server)
        spark.sql("SET spark.sql.sources.partitionOverwriteMode=dynamic")
        database_name = config.get("DEFAULT", "dbname")
        tables = spark.sql(f"SHOW TABLES IN {database_name}").select("tableName").rdd.flatMap(lambda x: x).collect()
        logger.info(f"Tables: {tables}")
        
        generated_data = {}       
        for table_name in tables:
            table_name = table_name.strip()
            logger.info(f"Processing table: {table_name}")

            if table_exists(logger, spark, database_name, table_name):
                try:
                    data = generate_and_write_data(logger, spark, config, database_name, table_name)
                    generated_data[table_name] = data
                except Exception as e:
                    logger.error(f"Failed to generate and write data for table '{table_name}': {e}")
            else:
                logger.warning(f"Table '{table_name}' does not exist. Cannot update.")
        logger.info("Table update process completed")

        display_table_samples(logger, tables, generated_data)

    except Exception as e:
        logger.error(f"Error updating tables: {e}", exc_info=True)
    finally:
        if spark:
            logger.info("Stopping Spark session")
            spark.stop()
            logger.debug("Spark session stopped")

if __name__ == "__main__":
    main()