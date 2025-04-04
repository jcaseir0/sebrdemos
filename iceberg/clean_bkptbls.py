#!/usr/bin/env python3
import logging
import configparser
from pyspark.sql import SparkSession

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def drop_snapshot_table_if_exists(logger: logging.Logger, spark: SparkSession, database_name: str, table_name: str) -> None:
    """
    Drop the Iceberg snapshot table if it already exists.

    Args:
        logger (logging.Logger): Logger instance for logging
        spark (SparkSession): Active Spark session
        database_name (str): Name of the database
        table_name (str): Name of the original table
    """
    logger.info("Starting snapshot table cleanup")
    snapshot_table_name = f"{table_name}_SNPICEBERG"
    full_snapshot_table_name = f"{database_name}.{snapshot_table_name}"

    try:
        logger.debug(f"Checking existence of snapshot table: {full_snapshot_table_name}")
        table_exists = spark.sql(f"SHOW TABLES IN {database_name} LIKE '{snapshot_table_name}'").count() > 0
        
        if table_exists:
            logger.info(f"Dropping snapshot table: {full_snapshot_table_name}")
            spark.sql(f"DROP TABLE IF EXISTS {full_snapshot_table_name}")
            logger.info(f"Successfully dropped snapshot table: {full_snapshot_table_name}")
        else:
            logger.debug(f"Snapshot table {full_snapshot_table_name} not found")

    except Exception as e:
        logger.error(f"Error dropping snapshot table {full_snapshot_table_name}: {str(e)}")
        raise

def drop_backup_tables(logger: logging.Logger, spark: SparkSession, database_name: str) -> None:
    """
    Drop all tables with '_backup_' suffix in the specified database.

    Args:
        logger (logging.Logger): Logger instance for logging
        spark (SparkSession): Active Spark session
        database_name (str): Name of the database
    """
    logger.info("Starting backup tables cleanup")
    
    try:
        # Listar todas as tabelas do banco
        logger.debug(f"Listing tables in database {database_name}")
        tables = [table for table in spark.sql(f"SHOW TABLES IN {database_name}").select("tableName").rdd.flatMap(lambda x: x).collect()]
        logger.info(f"Found {len(tables)} tables in database {database_name}")
        logger.info(f"Tables found: {tables}")
        # Filtrar tabelas backup
        backup_tables = [t for t in tables if '_backup_' in t]
        logger.info(f"Found {len(backup_tables)} backup tables to drop")
        logger.info(f"Backup tables found: {backup_tables}")
        if not backup_tables:
            logger.info("No backup tables found to drop")
            return
        # Dropar cada tabela backup
        for table in backup_tables:
            full_table_name = f"{database_name}.{table}"
            logger.info(f"Attempting to drop table: {full_table_name}")
            spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
            logger.info(f"Successfully dropped backup table: {full_table_name}")

    except Exception as e:
        logger.error(f"Error dropping backup tables: {str(e)}")
        raise

def main():
    """
    Main execution function for table cleanup operations.
    Handles Spark session initialization and executes cleanup tasks.
    """
    try:
        logger.info("Starting table cleanup process")
        
        # Inicializar Spark
        spark = SparkSession.builder \
            .appName("TableCleanup") \
            .config("spark.sql.catalogImplementation", "hive") \
            .enableHiveSupport() \
            .getOrCreate()

        # Carregar configurações
        logger.debug("Loading configuration")
        config = configparser.ConfigParser()
        config.read('config.ini')
        
        database_name = config.get("DEFAULT", "dbname")
        logger.info(f"Database name: {database_name}")
        tables_to_process = config.get("DEFAULT", "tables").split(",")
        logger.info(f"Tables to process: {tables_to_process}")

        # Executar limpeza para cada tabela
        for table in tables_to_process:
            table = table.strip()
            if table:
                logger.info(f"Processing table: {table}")
                drop_snapshot_table_if_exists(logger, spark, database_name, table)

        # Limpar tabelas backup
        drop_backup_tables(logger, spark, database_name)

        logger.info("Table cleanup completed successfully")

    except Exception as e:
        logger.error(f"Fatal error during table cleanup: {str(e)}", exc_info=True)
        raise
    finally:
        if 'spark' in locals():
            spark.stop()
            logger.info("Spark session stopped")

if __name__ == "__main__":
    main()