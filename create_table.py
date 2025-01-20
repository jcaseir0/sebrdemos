import os
import json
import logging
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark import SparkConf
from common_functions import load_config, gerar_dados, table_exists

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_table(spark, table_name, config):
    try:
        partition_by = config.getboolean(table_name, 'particionamento', fallback=False)
        bucketing = config.getboolean(table_name, 'bucketing', fallback=False)
        num_buckets = config.getint(table_name, 'num_buckets', fallback=0)

        if partition_by and not bucketing:
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {table_name}
                USING parquet
                PARTITIONED BY (data_execucao)
                AS SELECT * FROM temp_view
            """)
        elif bucketing and not partition_by:
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {table_name}
                USING parquet
                CLUSTERED BY (id_uf) INTO {num_buckets} BUCKETS
                AS SELECT * FROM temp_view
            """)
        else:
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {table_name}
                USING parquet
                AS SELECT * FROM temp_view
            """)
        logger.info(f"Table '{table_name}' created successfully.")
    except Exception as e:
        logger.error(f"Error creating table '{table_name}': {str(e)}")
        raise

def validate_partition_and_bucketing(config, table_name):
    partitioning = config.getboolean(table_name, 'particionamento', fallback=False)
    bucketing = config.getboolean(table_name, 'bucketing', fallback=False)

    if partitioning and bucketing:
        print(f"Erro: A tabela '{table_name}' não pode ter particionamento e bucketing ativados ao mesmo tempo.")
        sys.exit(1)

def get_schema_path(base_path, table_name):
    return f"{base_path}/{table_name}.json"

def main():
    # Configurar a URI do metastore como uma string de conexão JDBC
    jdbc_url = config['DEFAULT'].get('hmsUrl')
    thrift_server = config['DEFAULT'].get('thriftServer')
    # Criar uma SparkConf com as configurações
    spark_conf = SparkConf()
    spark_conf.set("hive.metastore.client.factory.class", "com.cloudera.spark.hive.metastore.HivemetastoreClientFactory")
    spark_conf.set("hive.metastore.uris", thrift_server)
    spark_conf.set("spark.sql.hive.metastore.jars", "builtin")
    spark_conf.set("spark.sql.hive.hiveserver2.jdbc.url", jdbc_url)
    spark = SparkSession.builder.spark_conf.appName("CreateTable").enableHiveSupport().getOrCreate()
    config = load_config()

    database_name = config['DEFAULT'].get('dbname')
    tables = config['DEFAULT']['tables'].split(',')
    base_path = "/app/mount"

    for table_name in tables:
        table_name = database_name.table_name
        validate_partition_and_bucketing(config, table_name)
        schema_path = get_schema_path(base_path, table_name)

        if not os.path.exists(schema_path):
            logger.error(f"Schema file not found for table '{table_name}': {schema_path}")
            continue

        with open(schema_path, 'r') as f:
            schema = json.load(f)

        if not table_exists(spark, table_name):
            num_records = config.getint(table_name, 'num_records', fallback=100)
            data = gerar_dados(table_name, num_records)
            df = spark.createDataFrame(data, schema=StructType.fromJson(schema))
            df.createOrReplaceTempView("temp_view")
            create_table(spark, table_name, config)
        else:
            logger.info(f"Table '{table_name}' already exists. Skipping creation.")

    spark.stop()

if __name__ == "__main__":
    main()
