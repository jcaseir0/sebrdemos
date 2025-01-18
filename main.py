import os
import json
import random
from datetime import datetime
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import current_date

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_config(config_path='/app/mount/config.ini'):
    """Load configuration from a file."""
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    with open(config_path, 'r') as f:
        config = json.load(f)
    logger.info("Configuration loaded successfully.")
    return config

def table_exists(spark, table_name):
    """Check if a table exists in the catalog."""
    try:
        result = spark.sql(f"SHOW TABLES LIKE '{table_name}'").count() > 0
        logger.info(f"Table '{table_name}' exists: {result}")
        return result
    except Exception as e:
        logger.error(f"Error checking table existence '{table_name}': {str(e)}")
        raise

def generate_data(table_name, num_records):
    """Generate random data for a given table."""
    fake_data = []
    if table_name == 'transactions':
        for _ in range(num_records):
            fake_data.append({
                "id_user": random.randint(1, 1000),
                "transaction_date": datetime.now().isoformat(),
                "amount": round(random.uniform(10, 1000), 2),
                "merchant": f"Merchant_{random.randint(1, 100)}",
                "category": random.choice(["Food", "Transport", "Entertainment"]),
                "status": random.choice(["Approved", "Declined"]),
                "execution_date": datetime.now().date().isoformat()
            })
    return fake_data

def create_table(spark, table_name, schema, partition_by=None):
    """Create a table with the given schema."""
    try:
        partition_clause = f"PARTITIONED BY ({partition_by})" if partition_by else ""
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_name}
            USING parquet
            {partition_clause}
            AS SELECT * FROM temp_view
        """)
        logger.info(f"Table '{table_name}' created successfully.")
    except Exception as e:
        logger.error(f"Error creating table '{table_name}': {str(e)}")
        raise

def update_table(spark, table_name):
    """Insert data into an existing table."""
    try:
        spark.sql(f"INSERT INTO {table_name} SELECT * FROM temp_view")
        logger.info(f"Data inserted into table '{table_name}' successfully.")
    except Exception as e:
        logger.error(f"Error updating table '{table_name}': {str(e)}")
        raise

def process_table(spark, table_name, schema, data, partition_by=None):
    """Process a table by creating or updating it with the given data."""
    df = spark.createDataFrame(data, schema=StructType.fromJson(schema))
    df = df.withColumn("execution_date", current_date())
    df.createOrReplaceTempView("temp_view")

    if not table_exists(spark, table_name):
        create_table(spark, table_name, schema, partition_by)
    else:
        update_table(spark, table_name)

def main():
    """Main execution function."""
    spark = SparkSession.builder.appName("DataProcessing").getOrCreate()
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

        data = generate_data(table_name, num_records)
        process_table(spark, table_name, schema, data, partition_by)

    spark.stop()

if __name__ == "__main__":
    main()
