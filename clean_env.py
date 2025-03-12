import logging
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from common_functions import load_config

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_spark_session(logger: logging.Logger) -> SparkSession:
    """
    Create and return a SparkSession.

    Returns:
        SparkSession: The created SparkSession object.
    """
    logger.info("Creating SparkSession")
    spark = SparkSession.builder \
        .appName("CleanEnvironment") \
        .enableHiveSupport() \
        .getOrCreate()
    logger.info("SparkSession created successfully")
    return spark

def remove_database_and_tables(logger: logging.Logger, spark: SparkSession, database_name: str) -> bool:
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
        logger.debug(f"Attempting to remove database: '{database_name}'")
        if database_name not in [row.namespace for row in databases]:
            logger.warning(f"Database '{database_name}' does not exist. Nothing to remove.")
            return True

        # List all tables in the database
        logger.debug(f"Listing tables in database '{database_name}'")
        tables = spark.sql(f"SHOW TABLES IN {database_name}").collect()
        
        # Drop each table
        for table in tables:
            table_name = table.tableName
            full_table_name = f"{database_name}.{table_name}"
            logger.debug(f"Attempting to drop table '{full_table_name}'")
            try:
                spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
                logger.info(f"Successfully dropped table '{full_table_name}'")
            except AnalysisException as e:
                logger.error(f"Failed to drop table '{full_table_name}': {str(e)}")

        # Drop the database
        logger.debug(f"Attempting to drop database '{database_name}'")
        spark.sql(f"DROP DATABASE IF EXISTS {database_name} CASCADE")
        logger.info(f"Successfully dropped database '{database_name}'")

        return True

    except Exception as e:
        logger.error(f"An error occurred while removing database '{database_name}': {str(e)}")
        return False

def main():
    """
    Main function to execute the database and table removal process.
    """

    logger = logging.getLogger(__name__)

    logger.info("Starting clean environment process")
    spark = create_spark_session(logger)

    try:
        config = load_config(logger)
        database_name = config['DEFAULT'].get('dbname')
        success = remove_database_and_tables(logger, spark, database_name)
        
        if success:
            logger.info(f"Successfully cleaned environment for database '{database_name}'")
        else:
            logger.warning(f"Failed to clean environment for database '{database_name}'")
    
    except Exception as e:
        logger.error(f"An unexpected error occurred: {str(e)}")
    
    finally:
        logger.info("Stopping SparkSession")
        spark.stop()

if __name__ == "__main__":
    main()
