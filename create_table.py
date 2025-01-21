import os
import json
import logging
import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark import SparkConf
from pyspark.sql.utils import AnalysisException
from common_functions import load_config, gerar_dados, table_exists

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_table(spark, table_name, config):
    try:
        partition = config.getboolean(table_name, 'particionamento', fallback=False)
        partition_by = config.get(table_name, 'partition_by', fallback=None)
        bucketing = config.getboolean(table_name, 'bucketing', fallback=False)
        clustered_by = config.get(table_name, 'clustered_by', fallback=None)
        num_buckets = config.getint(table_name, 'num_buckets', fallback=0)

        if partition and not bucketing:
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {table_name}
                USING parquet
                PARTITIONED BY (${partition_by})
                AS SELECT * FROM temp_view
            """)
        elif bucketing and not partition:
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {table_name}
                USING parquet
                CLUSTERED BY (${clustered_by}) INTO {num_buckets} BUCKETS
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

def validate_hive_metastore(spark, max_retries=3, retry_delay=5):
    logger = logging.getLogger(__name__)
    
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

def main():
    config = load_config()
    jdbc_url = config['DEFAULT'].get('hmsUrl')
    thrift_server = config['DEFAULT'].get('thriftServer')
    # Criar uma SparkConf com as configurações
    spark_conf = SparkConf()
    spark_conf.set("hive.metastore.client.factory.class", "com.cloudera.spark.hive.metastore.HivemetastoreClientFactory")
    spark_conf.set("hive.metastore.uris", thrift_server)
    spark_conf.set("spark.sql.hive.metastore.jars", "builtin")
    spark_conf.set("spark.sql.hive.hiveserver2.jdbc.url", jdbc_url)
    
    spark = SparkSession.builder.config(conf=spark_conf).appName("CreateTable").enableHiveSupport().getOrCreate()
    
    validate_hive_metastore(spark)

    database_name = config['DEFAULT'].get('dbname')
    tables = config['DEFAULT']['tables'].split(',')
    base_path = "/app/mount"

    for table_name in tables:
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
            current_date = time.strftime("%d-%m-%Y")
            num_records = config.getint(table_name, 'num_records', fallback=100)
            partition_by = config.get(table_name, 'partition_by', fallback=None)
            data = gerar_dados(table_name, num_records)
            df = spark.createDataFrame(data, schema=StructType.fromJson(schema))
            if partition:
                df.withColumn(partition_by, current_date)
            df.createOrReplaceTempView("temp_view")
            create_table(spark, table, config)
        else:
            logger.info(f"Table '{table}' already exists. Skipping creation.")

    spark.stop()

if __name__ == "__main__":
    main()
