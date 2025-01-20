import os
import json
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import current_date
from common_functions import load_config, generate_data, table_exists

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def update_table(spark, table_name, partition_by=None):
    try:
        if partition_by:
            spark.sql(f"""
                INSERT INTO {table_name}
                PARTITION ({partition_by}=current_date())
                SELECT * FROM temp_view
            """)
        else:
            spark.sql(f"INSERT INTO {table_name} SELECT * FROM temp_view")
        logger.info(f"Data inserted into table '{table_name}' successfully.")
    except Exception as e:
        logger.error(f"Error updating table '{table_name}': {str(e)}")
        raise

def main():
    spark = SparkSession.builder.appName("UpdateTable").getOrCreate()
    config = load_config()

    for table_name, table_config in config.get("tables", {}).items():
        schema_path = table_config.get("schema_path")
        num_records = table_config.get("num_records", 100)
        partition_by = table_config.get("partition_by")

        if not os.path.exists(schema_path):
            logger.error(f"Schema file not found for table '{table_name}': {schema_path}")
            continue

        with open(schema_path, 'r') as f:
            schema = json.load(f)

        if table_exists(spark, table_name):
            data = generate_data(table_name, num_records)
            df = spark.createDataFrame(data, schema=StructType.fromJson(schema))
            df = df.withColumn("execution_date", current_date())
            df.createOrReplaceTempView("temp_view")
            update_table(spark, table_name, partition_by)
        else:
            logger.warning(f"Table '{table_name}' does not exist. Cannot update.")

    spark.stop()

if __name__ == "__main__":
    main()
