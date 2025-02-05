from pyspark.sql import SparkSession
from pyspark import SparkConf
import sys, os, logging, re

# Adicionar o diretório pai ao caminho de busca do Python do pacote common_functions
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common_functions import load_config, validate_hive_metastore, analyze_table_structure, collect_statistics

# Configuração do logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def drop_snapshot_table_if_exists(spark, database_name, table_name):
    """
    Drop the Iceberg snapshot table if it already exists.

    Args:
        spark (SparkSession): The active Spark session.
        database_name (str): The name of the database.
        table_name (str): The name of the original table.

    Returns:
        None
    """
    snapshot_table_name = f"{table_name}_SNPICEBERG"
    full_snapshot_table_name = f"{database_name}.{snapshot_table_name}"

    logger.info(f"Checking if snapshot table {full_snapshot_table_name} exists")
    
    # Check if the table exists
    table_exists = spark.sql(f"SHOW TABLES IN {database_name} LIKE '{snapshot_table_name}'").count() > 0

    if table_exists:
        logger.info(f"Snapshot table {full_snapshot_table_name} exists. Dropping it.")
        try:
            spark.sql(f"DROP TABLE IF EXISTS {full_snapshot_table_name}")
            logger.info(f"Successfully dropped snapshot table {full_snapshot_table_name}\n")
        except Exception as e:
            logger.error(f"Failed to drop snapshot table {full_snapshot_table_name}: {str(e)}\n")
            raise
    else:
        logger.info(f"Snapshot table {full_snapshot_table_name} does not exist. No action needed.\n")

def iceberg_migration_snaptable(spark, database_name, table_name):
    """
    Create an Iceberg snapshot table from an existing table.

    This function creates a snapshot of the specified table using Iceberg format.
    The snapshot table name will be the original table name with '_SNPICEBERG' suffix.

    Args:
        spark (SparkSession): The active Spark session.
        database_name (str): The name of the database containing the original table.
        table_name (str): The name of the table to be snapshotted.

    Returns:
        str: The name of the created Iceberg snapshot table (without database prefix).

    Raises:
        Exception: If there's an error during the snapshot creation process.
    """
    logger.info(f"Creating Iceberg snapshot table for {database_name}.{table_name}")
    
    snaptbl = f"{table_name}_SNPICEBERG"
    full_snaptbl = f"{database_name}.{snaptbl}"
    
    try:
        logger.debug(f"Executing Iceberg snapshot system call")
        spark.sql(f"CALL spark_catalog.system.snapshot('{database_name}.{table_name}', '{full_snaptbl}')")
        logger.info(f"Iceberg snapshot table created successfully: {full_snaptbl}\n")
    except Exception as e:
        logger.error(f"Failed to create Iceberg snapshot table: {str(e)}\n")
        raise

    logger.debug(f"Returning snapshot table name: {snaptbl}\n")
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
        # Analyze table structure
        original_structure = analyze_table_structure(spark, database_name, [table_name])[0]
        snapshot_structure = analyze_table_structure(spark, database_name, [snaptable])[0]

        logger.info(f"Original table structure: {original_structure['structure']}")
        logger.info(f"Snapshot table structure: {snapshot_structure['structure']}\n")

        # Compare row counts
        count_query1 = f"SELECT COUNT(*) as count FROM {database_name}.{snaptable}"
        count_query2 = f"SELECT COUNT(*) as count FROM {database_name}.{table_name}"
        count1, count2, counts_match = compare_query_results(spark, count_query1, count_query2, "Row counts")
        checks_passed = checks_passed and counts_match
        
        logger.info(f"Iceberg snapshot table row count: {count1[0]['count']}")
        logger.info(f"Original table row count: {count2[0]['count']}\n")

        # Compare sample rows (just log, don't affect checks_passed)
        sample_query = f"SELECT * FROM"
        sample1 = spark.sql(f"{sample_query} {database_name}.{snaptable}").limit(5).collect()
        sample2 = spark.sql(f"{sample_query} {database_name}.{table_name}").limit(5).collect()
        logger.info("Iceberg snapshot table sample rows:")
        for row in sample1:
            logger.info(row)
        print()
        logger.info("Original table sample rows:")
        for row in sample2:
            logger.info(row)
        print()
        # Compare table descriptions (just log, don't affect checks_passed)
        describe_query = "DESCRIBE FORMATTED"
        describe1 = spark.sql(f"{describe_query} {database_name}.{snaptable}").collect()
        describe2 = spark.sql(f"{describe_query} {database_name}.{table_name}").collect()
        logger.info("Snapshot Iceberg DESCRIBE FORMATTED:")
        for row in describe1:
            logger.info(row)
        print()
        logger.info("Original DESCRIBE FORMATTED:")
        for row in describe2:
            logger.info(row)
        print()
        # Compare CREATE TABLE statements (just log, don't affect checks_passed)
        create_query = "SHOW CREATE TABLE"
        create1 = spark.sql(f"{create_query} {database_name}.{snaptable}").collect()
        create2 = spark.sql(f"{create_query} {database_name}.{table_name}").collect()
        logger.info("Snapshot Iceberg SHOW CREATE TABLE:")
        logger.info(create1[0]['createtab_stmt'])
        logger.info("Original SHOW CREATE TABLE:")
        logger.info(create2[0]['createtab_stmt'])

        # Check structure-specific details
        if original_structure['structure'] == "Particionada":
            # Compare partitions
            partition_query1 = f"SELECT * FROM {database_name}.{snaptable}.PARTITIONS"
            partition_query2 = f"SHOW PARTITIONS {database_name}.{table_name}"
            partitions1 = spark.sql(partition_query1).collect()
            partitions2 = spark.sql(partition_query2).collect()
            
            iceberg_partitions = [row.partition.data_execucao for row in partitions1 if row.partition.data_execucao is not None]
            original_partitions = [row.partition.split('=')[1] for row in partitions2 if row.partition.split('=')[1] != '__HIVE_DEFAULT_PARTITION__']
            
            partitions_match = set(iceberg_partitions) == set(original_partitions)
            checks_passed = checks_passed and partitions_match
            
            logger.info(f"Iceberg snapshot table partitions count: {len(iceberg_partitions)}")
            logger.info(f"Original table partitions count: {len(original_partitions)}")
            logger.info("Iceberg snapshot table PARTITIONS:")
            for partition in iceberg_partitions:
                logger.info(f"data_execucao='{partition}'")
            logger.info("Original table PARTITIONS:")
            for partition in original_partitions:
                logger.info(f"data_execucao={partition}")
            
            if partitions_match:
                logger.info("Partitions match between Iceberg snapshot and original table.")
            else:
                logger.warning("Partitions do not match between Iceberg snapshot and original table.")

        elif original_structure['structure'] == "Bucketed":
            # Extract bucketing information from CREATE TABLE statements
            def extract_bucket_info(create_stmt):
                bucket_info = re.search(r'CLUSTERED BY \((.*?)\) INTO (\d+) BUCKETS', create_stmt)
                if bucket_info:
                    return bucket_info.group(1), int(bucket_info.group(2))
                return None, None

            original_bucket_cols, original_num_buckets = extract_bucket_info(create2[0]['createtab_stmt'])
            snapshot_bucket_cols, snapshot_num_buckets = extract_bucket_info(create1[0]['createtab_stmt'])

            logger.info(f"Original table bucketing: Columns: {original_bucket_cols}, Num buckets: {original_num_buckets}")
            logger.info(f"Snapshot table bucketing: Columns: {snapshot_bucket_cols}, Num buckets: {snapshot_num_buckets}")

            checks_passed = checks_passed and (original_bucket_cols == snapshot_bucket_cols) and (original_num_buckets == snapshot_num_buckets)

        elif original_structure['structure'] == "Nenhuma":
            logger.info("Table has no partitioning or bucketing. No additional checks needed.")

    except Exception as e:
        logger.error(f"An error occurred while checking tables: {str(e)}")
        checks_passed = False

    if checks_passed:
        logger.info("All sanity checks passed successfully.")
    else:
        logger.warning("Some sanity checks failed. Please review the logs for details.")

    return checks_passed

def get_bucket_info(describe_result):
    bucket_columns = None
    num_buckets = None
    for row in describe_result:
        if row['col_name'].strip() == '# Bucket Columns':
            bucket_columns = row['data_type'].strip()
        elif row['col_name'].strip() == '# Num Buckets':
            num_buckets = row['data_type'].strip()
        if bucket_columns and num_buckets:
            break
    return f"Bucket Columns: {bucket_columns}, Num Buckets: {num_buckets}"

def drop_snaptable(spark, database_name, snaptable):
    """
    Drop the specified snapshot table from the database.

    This function attempts to drop the given snapshot table and logs the process.

    Args:
        spark (SparkSession): The active Spark session.
        database_name (str): The name of the database containing the snapshot table.
        snaptable (str): The name of the snapshot table to be dropped.

    Returns:
        None

    Raises:
        Exception: If an error occurs while dropping the table, it's caught and logged.
    """
    full_table_name = f"spark_catalog.{database_name}.{snaptable}"
    
    logger.info(f"Attempting to drop table: {full_table_name}")
    
    try:
        logger.debug(f"Executing SQL: DROP TABLE IF EXISTS {full_table_name}")
        spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
        logger.info(f"Successfully dropped table: {full_table_name}\n")
    except Exception as e:
        logger.error(f"Failed to drop table {full_table_name}: {str(e)}", exc_info=True)
        raise

def migrate_inplace_to_iceberg(spark, database_name, table_name):
    """
    Migrate a Hive table to Iceberg format in-place.

    This function performs an in-place migration of a Hive table to Iceberg format.
    It first unsets the 'TRANSLATED_TO_EXTERNAL' table property and then calls the
    Iceberg migration procedure.

    Args:
        spark (SparkSession): The active Spark session.
        database_name (str): The name of the database containing the table.
        table_name (str): The name of the table to be migrated.

    Raises:
        Exception: If an error occurs during the migration process.

    Returns:
        None
    """
    logger.info(f"Starting Iceberg Migration In-place for table {database_name}.{table_name}")
    
    try:
        # Unset TRANSLATED_TO_EXTERNAL property
        logger.info("Unsetting 'TRANSLATED_TO_EXTERNAL' table property")
        unset_query = f"ALTER TABLE {database_name}.{table_name} UNSET TBLPROPERTIES ('TRANSLATED_TO_EXTERNAL')"
        logger.debug(f"Executing query: {unset_query}")
        spark.sql(unset_query)
        logger.info("Successfully unset 'TRANSLATED_TO_EXTERNAL' property")

        # Perform Iceberg migration
        logger.info("Initiating Iceberg table migration")
        migrate_query = f"CALL spark_catalog.system.migrate('{database_name}.{table_name}')"
        logger.debug(f"Executing query: {migrate_query}")
        spark.sql(migrate_query)
        logger.info(f"Successfully migrated {database_name}.{table_name} to Iceberg format")

    except Exception as e:
        logger.error(f"Error occurred while migrating {database_name}.{table_name} to Iceberg: {str(e)}", exc_info=True)
        raise

    logger.info(f"Iceberg Migration In-place finished for table {database_name}.{table_name}\n")

def checks_on_migrated_to_iceberg(spark, database_name, table_name):
    """
    Perform checks on a table that has been migrated to Iceberg format.

    This function executes various SQL queries to inspect the structure,
    partitions, history, and snapshots of the migrated Iceberg table.

    Args:
        spark (SparkSession): The active Spark session.
        database_name (str): The name of the database containing the table.
        table_name (str): The name of the table that was migrated to Iceberg.

    Raises:
        Exception: If an error occurs during any of the check operations.

    Returns:
        None
    """
    full_table_name = f"spark_catalog.{database_name}.{table_name}"
    logger.info(f"Performing checks on Iceberg migrated table: {full_table_name}")

    try:
        # Describe table
        logger.info("Executing DESCRIBE TABLE command")
        describe_result = spark.sql(f"DESCRIBE TABLE {full_table_name}")
        logger.debug("DESCRIBE TABLE result:")
        describe_result.show(30, False)

        # Show create table
        logger.info("Executing SHOW CREATE TABLE command")
        create_table_result = spark.sql(f"SHOW CREATE TABLE {full_table_name}")
        logger.debug("SHOW CREATE TABLE result:")
        create_table_result.show(truncate=False)

        # Show partitions
        logger.info("Retrieving table partitions")
        partitions_result = spark.sql(f"SELECT * FROM {full_table_name}.partitions")
        logger.debug("Table partitions:")
        partitions_result.show()

        # Show table history
        logger.info("Retrieving table history")
        history_result = spark.sql(f"SELECT * FROM {full_table_name}.history")
        logger.debug("Table history:")
        history_result.show(20, False)

        # Show table snapshots
        logger.info("Retrieving table snapshots")
        snapshots_result = spark.sql(f"SELECT * FROM {full_table_name}.snapshots")
        logger.debug("Table snapshots:")
        snapshots_result.show(20, False)

        logger.info(f"All checks completed successfully for table: {full_table_name}\n")

    except Exception as e:
        logger.error(f"Error occurred while checking Iceberg migrated table {full_table_name}: {str(e)}", exc_info=True)
        raise

def rename_migrated_table(spark, database_name, table_name):
    """
    Rename a migrated table to maintain data lifecycle.

    This function renames the original table to a new name with 'iceberg_' prefix.

    Args:
        spark (SparkSession): The active Spark session.
        database_name (str): The name of the database containing the table.
        table_name (str): The name of the table to be renamed.

    Returns:
        str or None: The new table name if successful, None if an error occurs.

    Raises:
        Exception: If an error occurs during the renaming process.
    """
    logger.info(f"Initiating table rename process for {database_name}.{table_name}")
    
    iceberg_table = f"{database_name}.iceberg_{table_name}"
    
    try:
        logger.debug(f"Constructing ALTER TABLE query: ALTER TABLE {database_name}.{table_name} RENAME TO {iceberg_table}")
        
        spark.sql(f"ALTER TABLE {database_name}.{table_name} RENAME TO {iceberg_table}")
        
        logger.info(f"Successfully renamed table from {database_name}.{table_name} to {iceberg_table}\n")
        return iceberg_table
    
    except Exception as e:
        logger.error(f"Failed to rename table {database_name}.{table_name}: {str(e)}\n", exc_info=True)
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

    spark = SparkSession.builder.config(conf=spark_conf).appName("ICEBERG LOAD").getOrCreate()

    validate_hive_metastore(spark)

    tables = config['DEFAULT']['tables'].split(',')
    database_name = config['DEFAULT'].get('dbname')

    for table_name in tables:
        drop_snapshot_table_if_exists(spark, database_name, table_name)
        snaptable = iceberg_migration_snaptable(spark, database_name, table_name)
        # Executar sanity checks
        result = iceberg_sanity_checks(spark, database_name, table_name, snaptable)
    
        if result:
            logger.info("All sanity checks passed!")
            drop_snaptable(spark, database_name, snaptable)
            migrate_inplace_to_iceberg(spark, database_name, table_name)
            checks_on_migrated_to_iceberg(spark, database_name, table_name)
            new_table_name = rename_migrated_table(spark, database_name, table_name)
            logger.info(f"Iceberg table migrated and table renamed to {new_table_name}")
        else:
            print("Some checks failed. Review the logs for details. Iceberg Migration In-place Cancelled.")

    # Encerrar SparkSession
    spark.stop()

if __name__ == "__main__":
    main()