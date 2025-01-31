from pyspark.sql import SparkSession
from pyspark import SparkConf
from datetime import datetime
import logging
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) # Adiciona o diretório pai ao sys.path
from common_functions import load_config, table_exists, validate_hive_metastore

# Configuração do logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def iceberg_migration_snaptable(spark, database_name, table_name):
    logger.info("Create a Iceberg snapshot table:\n")
    snaptbl = f"{database_name}.{table_name}_SNPICEBERG"
    spark.sql(f"CALL spark_catalog.system.snapshot('{database_name}.{table_name}', '{snaptbl}')")
    logger.info(f"Iceberg snapshot table created: {snaptbl}\n")
    return snaptbl

def compare_query_results(spark, query1, query2, description):
    """
    Compara os resultados de duas queries SQL.
    
    :param spark: SparkSession
    :param query1: Primeira query SQL
    :param query2: Segunda query SQL
    :param description: Descrição da comparação
    :return: Tuple contendo os resultados das duas queries e um booleano indicando se são iguais
    """
    result1 = spark.sql(query1).collect()
    result2 = spark.sql(query2).collect()
    
    are_equal = result1 == result2
    
    if are_equal:
        logger.info(f"{description} match.")
    else:
        logger.warning(f"{description} do not match.")
    
    return result1, result2, are_equal

def iceberg_sanity_checks(spark, database_name, table_name, snaptable):
    """
    Realiza verificações de sanidade entre uma tabela original e sua versão Iceberg.
    
    :param spark: SparkSession
    :param database_name: Nome do banco de dados da tabela original
    :param table_name: Nome da tabela original
    :param snaptable: Nome da tabela Iceberg
    :return: Boolean indicando se todos os checks passaram
    """
    logger.info("Run sanity checks on the Iceberg snapshot table and Original table")

    checks_passed = True

    try:
        # Compare row counts
        count_query1 = f"SELECT COUNT(*) as count FROM {snaptable}"
        count_query2 = f"SELECT COUNT(*) as count FROM {database_name}.{table_name}"
        count1, count2, counts_match = compare_query_results(spark, count_query1, count_query2, "Row counts")
        checks_passed = checks_passed and counts_match
        
        logger.info(f"Iceberg snapshot table row count: {count1[0]['count']}")
        logger.info(f"Original table row count: {count2[0]['count']}")

        # Compare sample rows (just log, don't affect checks_passed)
        sample_query = f"SELECT * FROM"
        sample1 = spark.sql(f"{sample_query} {snaptable}").limit(5).collect()
        sample2 = spark.sql(f"{sample_query} {database_name}.{table_name}").limit(5).collect()
        logger.info("Iceberg snapshot table sample rows:")
        for row in sample1:
            logger.info(row)
        logger.info("Original table sample rows:")
        for row in sample2:
            logger.info(row)

        # Compare table descriptions (just log, don't affect checks_passed)
        describe_query = "DESCRIBE TABLE"
        describe1 = spark.sql(f"{describe_query} {snaptable}").collect()
        describe2 = spark.sql(f"{describe_query} {database_name}.{table_name}").collect()
        logger.info("Snapshot Iceberg DESCRIBE TABLE:")
        for row in describe1:
            logger.info(row)
        logger.info("Original DESCRIBE TABLE:")
        for row in describe2:
            logger.info(row)

        # Compare CREATE TABLE statements (just log, don't affect checks_passed)
        create_query = "SHOW CREATE TABLE"
        create1 = spark.sql(f"{create_query} {snaptable}").collect()
        create2 = spark.sql(f"{create_query} {database_name}.{table_name}").collect()
        logger.info("Snapshot Iceberg SHOW CREATE TABLE:")
        logger.info(create1[0]['createtab_stmt'])
        logger.info("Original SHOW CREATE TABLE:")
        logger.info(create2[0]['createtab_stmt'])

        # Compare partitions
        partition_query1 = f"SELECT * FROM {snaptable}.PARTITIONS"
        partition_query2 = f"SHOW PARTITIONS {database_name}.{table_name}"
        partitions1, partitions2, partitions_match = compare_query_results(spark, partition_query1, partition_query2, "Partitions")
        checks_passed = checks_passed and partitions_match
        
        logger.info("Iceberg snapshot table PARTITIONS:")
        for row in partitions1:
            logger.info(row)
        logger.info("Original table PARTITIONS:")
        for row in partitions2:
            logger.info(row)

    except Exception as e:
        logger.error(f"An error occurred while checking tables: {str(e)}")
        checks_passed = False

    if checks_passed:
        logger.info("All sanity checks passed successfully.")
    else:
        logger.warning("Some sanity checks failed. Please review the logs for details.")

    return checks_passed

def drop_snaptable(spark, snaptable):
    try:
        logger.info(f"DROP TABLE {snaptable}:")
        spark.sql("fDROP TABLE spark_catalog.{snaptable}").show()
    except Exception as e:
        logger.error(f"An error ocurred while removing table: {str(e)}")

def migrate_inplace_to_iceberg(spark, database_name, table_name):
    logger.info("Starting Iceberg Migration In-place...")
    try:
        logger.info("This ensures that a table backup is created by renaming the table in Hive metastore (HMS) instead of moving the physical location of the table:")
        logger.info(f"ALTER TABLE {database_name}.{table_name} UNSET TBLPROPERTIES ('TRANSLATED_TO_EXTERNAL')\n")
        spark.sql(f"ALTER TABLE {database_name}.{table_name} UNSET TBLPROPERTIES ('TRANSLATED_TO_EXTERNAL')")
        logger.info("Starting Iceberg table migration in-place:")
        logger.info(f"CALL spark_catalog.system.migrate('{database_name}.{table_name}')\n")
        spark.sql(f"CALL spark_catalog.system.migrate('{database_name}.{table_name}')")
        logger.info(f"{database_name}.{table_name} table migrated to Iceberg Format.\n")
    except Exception as e:
        logger.info(f"Error occurred while migrating to Iceberg table: {str(e)}")
    logger.info("Iceberg Migration In-place finished.\n")

def checks_on_migrated_to_iceberg(spark, database_name, table_name):
    logger.info("Iceberg migrated DESCRIBE TABLE:\n")
    try:
        spark.sql(f"DESCRIBE TABLE spark_catalog.{database_name}.{table_name}").show(30, False)
        logger.info("Iceberg migrated SHOW CREATE TABLE:\n")
        spark.sql(f"SHOW CREATE TABLE spark_catalog.{database_name}.{table_name}").show(truncate=False)
        logger.info("Post-Iceberg migration table partitions:\n")
        spark.sql(f"SELECT * FROM spark_catalog.{database_name}.{table_name}.partitions").show()
        logger.info("Iceberg migrated table history:\n")
        spark.sql(f"SELECT * FROM spark_catalog.{database_name}.{table_name}.history").show(20, False)
        logger.info("Iceberg migrated table snapshots:\n")
        spark.sql(f"SELECT * FROM spark_catalog.{database_name}.{table_name}.snapshots").show(20, False)
    except Exception as e:
        logger.info(f"Error occurred while checking Iceberg migrated table: {str(e)}")

def rename_migrated_table(spark, database_name, table_name):
    logger.info("Changing table name to keep data life cycle")
    try:
        iceberg_table = f"{database_name}.iceberg_{table_name}"
        logger.info(f"Change table name from original to {iceberg_table}:\n")
        logger.info(f"ALTER TABLE {database_name}.{table_name} RENAME TO {iceberg_table}\n")
        spark.sql(f"ALTER TABLE {database_name}.{table_name} RENAME TO {iceberg_table}")
        logger.info(f"Table {table_name} renamed to {iceberg_table}.\n")
        return iceberg_table
    except Exception as e:
        logger.info(f"Error occurred while renaming table: {str(e)}")
        return None

def main():
    """
    Main function to create tables based on configuration.

    This function validates the Hive metastore connection, iterates through the tables
    defined in the configuration, and creates them if they do not already exist.
    """
    logger.info("Starting main function")
    config = load_config()

    spark_conf = SparkConf()
    spark_conf.set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
    spark_conf.set("spark.sql.catalog.spark_catalog.type", "hive")
    spark_conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

    spark = SparkSession.builder.appName("ICEBERG LOAD").spark_conf.getOrCreate()

    validate_hive_metastore(spark)

    tables = config['DEFAULT']['tables'].split(',')
    database_name = config['DEFAULT'].get('dbname')

    for table_name in tables:
        snaptable = iceberg_migration_snaptable(spark, database_name, table_name)
        # Executar sanity checks
        result = iceberg_sanity_checks(spark, database_name, table_name, snaptable)
    
        if result:
            logger.info("All sanity checks passed!")
            drop_snaptable(spark, snaptable)
            migrate_inplace_to_iceberg(spark, database_name, table_name)
            checks_on_migrated_to_iceberg(spark, database_name, table_name)
            new_table_name = rename_migrated_table(spark, database_name, table_name)
            logger.info(f"Iceberg table migrated and table renamed to {new_table_name}")
        else:
            print("Some checks failed. Review the logs for details.")

    # Encerrar SparkSession
    spark.stop()

if __name__ == "__main__":
    main()