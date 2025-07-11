from pyspark.sql import SparkSession
from pyspark import SparkConf
import sys, os, logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common_functions import setup_logging, load_config, create_spark_session, validate_hive_metastore, create_table_for_miginplace, analyze_table_structure, extract_bucket_info

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def drop_snapshot_table_if_exists(logger: logging.Logger, spark: SparkSession, database_name: str, table_name: str) -> None:
    """
    Drop the Iceberg snapshot table if it already exists.

    Args:
        spark (SparkSession): The active Spark session.
        database_name (str): The name of the database.
        table_name (str): The name of the original table.

    Returns:
        None
    """
    
    logger.info("Drop the Iceberg snapshot table if it already exists.")
    
    snapshot_table_name = f"{table_name}_SNPICEBERG"
    full_snapshot_table_name = f"{database_name}.{snapshot_table_name}"

    logger.info(f"Checking if snapshot table {full_snapshot_table_name} exists")
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

def iceberg_migration_snaptable(logger: logging.Logger, spark: SparkSession, database_name: str, table_name: str) -> str:
    """
    Create an Iceberg snapshot table from an existing table.

    This function first removes any existing backup tables, then creates a snapshot 
    of the specified table using Iceberg format. The snapshot table name will be 
    the original table name with '_SNPICEBERG' suffix.

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
    backup_table = f"{database_name}.{table_name}_backup_"
    
    try:
        logger.info(f"Checking for existing backup tables like {backup_table}")
        existing_backups = spark.sql(f"SHOW TABLES IN {database_name} LIKE '{table_name}_backup_%'").collect()
        logger.info(f"Found {len(existing_backups)} existing backup tables")
        
        for backup in existing_backups:
            backup_name = f"{database_name}.{backup['tableName']}"
            logger.info(f"Removing existing backup table: {backup_name}")
            spark.sql(f"DROP TABLE IF EXISTS {backup_name}")
            logger.info(f"Successfully dropped backup table: {backup_name}")
        
        logger.info(f"Executing Iceberg snapshot system call")
        spark.sql(f"CALL spark_catalog.system.snapshot('{database_name}.{table_name}', '{full_snaptbl}')")
        logger.info(f"Iceberg snapshot table created successfully: {full_snaptbl}\n")

    except Exception as e:
        logger.error(f"Failed to create Iceberg snapshot table: {str(e)}")
        raise

    logger.debug(f"Returning snapshot table name: {snaptbl}")
    return snaptbl

def compare_query_results(logger: logging.Logger, spark: SparkSession, query1: str, query2: str, description: str) -> tuple:
    """Compares the results of two SQL queries.

    Args:
        spark (SparkSession): The active Spark session.
        query1 (str): The first SQL query.
        query2 (str): The second SQL query.
        description (str): A description of the comparison.

    Returns:
        tuple: A tuple containing the results of the two queries and a boolean
            indicating whether the results match.
    """
    try:
        result1 = spark.sql(query1).collect()
        result2 = spark.sql(query2).collect()
        match = result1 == result2

        logger.info(f"{description} match: {match}")
        return result1, result2, match

    except Exception as e:
        logger.error(f"Error comparing query results: {str(e)}", exc_info=True)
        return None, None, False

def iceberg_sanity_checks(logger: logging.Logger, spark: SparkSession, database_name: str, table_name: str, snaptable: str) -> bool:
    """
    Realiza verificações de sanidade entre uma tabela original e sua versão Iceberg.
    
    :param spark: SparkSession
    :param database_name: Nome do banco de dados da tabela original
    :param table_name: Nome da tabela original
    :param snaptable: Nome da tabela Iceberg
    :return: Boolean indicando se todos os checks passaram
    """

    logger.info("Run sanity checks on the Iceberg snapshot and Original tables")

    checks_passed = True

    try:
        logger.info(f"Analyze table structures for {database_name}.{table_name}\n")
        original_structure = analyze_table_structure(logger, spark, database_name, [table_name])[0]
        snapshot_structure = analyze_table_structure(logger, spark, database_name, [snaptable])[0]

        logger.info(f"Original table structure: {original_structure['structure']}")
        logger.info(f"Snapshot table structure: {snapshot_structure['structure']}\n")

        logger.info(f"Compare row counts for {database_name}.{table_name}\n")
        count_query1 = f"SELECT COUNT(*) as count FROM {database_name}.{snaptable}"
        logger.debug(f"Count query 1: {count_query1}")
        count_query2 = f"SELECT COUNT(*) as count FROM {database_name}.{table_name}"
        logger.debug(f"Count query 2: {count_query2}")
        count1, count2, counts_match = compare_query_results(logger, spark, count_query1, count_query2, "Row counts")
        logger.debug(f"Count query results match: {counts_match}")
        logger.debug(f"Counts Iceberg Snapshot Table: {count1}")
        logger.debug(f"Counts Original Table: {count2}")
        checks_passed = checks_passed and counts_match
        logger.debug(f"Checks passed: {checks_passed}")
        
        logger.info(f"Iceberg snapshot table row count: {count1[0]['count']}")
        logger.info(f"Original table row count: {count2[0]['count']}\n")

        logger.info(f"Compare sample rows for {database_name}.{table_name} (just log, don't affect checks_passed)\n")
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
        
        logger.info(f"Compare table descriptions for {database_name}.{table_name} (just log, don't affect checks_passed)\n")
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

        logger.info(f"Compare CREATE TABLE statements for {database_name}.{table_name} (just log, don't affect checks_passed)\n")
        create_query = "SHOW CREATE TABLE"
        create1 = spark.sql(f"{create_query} {database_name}.{snaptable}").collect()
        create2 = spark.sql(f"{create_query} {database_name}.{table_name}").collect()
        logger.info("Snapshot Iceberg SHOW CREATE TABLE:")
        logger.info(create1[0]['createtab_stmt'])
        logger.info("Original SHOW CREATE TABLE:")
        logger.info(create2[0]['createtab_stmt'])

        logger.info(f"Compare partitions or bucketing information for {database_name}.{table_name}\n")
        if original_structure['structure'] == "Particionada":
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
            
            logger.info("\nIceberg snapshot table PARTITIONS:")
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

            original_bucket_cols, original_num_buckets = extract_bucket_info(logger, create2[0]['createtab_stmt'])
            snapshot_bucket_cols, snapshot_num_buckets = extract_bucket_info(logger, create1[0]['createtab_stmt'])

            logger.info(f"Original table bucketing: Columns: {original_bucket_cols}, Num buckets: {original_num_buckets}")
            logger.info(f"Snapshot table bucketing: Columns: {snapshot_bucket_cols}, Num buckets: {snapshot_num_buckets}")

            checks_passed = checks_passed and (original_bucket_cols == snapshot_bucket_cols) and (original_num_buckets == snapshot_num_buckets)
            logger.debug(f"Checks passed: {checks_passed}")

        elif original_structure['structure'] == "Nenhuma":
            logger.info("Table has no partitioning or bucketing. No additional checks needed.")

    except Exception as e:
        logger.error(f"An error occurred while checking tables: {str(e)}")
        checks_passed = False

    if checks_passed:
        logger.info("All sanity checks passed successfully.")
    else:
        logger.warning("Some sanity checks failed. Please review the logs for details.\n")

    return checks_passed

def get_bucket_info(logger: logging.Logger, describe_result: list) -> str:
    """
    Extract the bucketing information from the DESCRIBE FORMATTED result.

    Args:
        describe_result (list): The result of the DESCRIBE FORMATTED command.

    Returns:
        str: A string containing the bucketing information.
    """
    logger.info("Extracting bucketing information from DESCRIBE FORMATTED result")

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

def drop_snaptable(logger: logging.Logger, spark: SparkSession, database_name: str, snaptable: str) -> None:
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
    
    logger.info("Drop the specified snapshot table from the database")

    full_table_name = f"spark_catalog.{database_name}.{snaptable}"
    
    logger.info(f"Attempting to drop table: {full_table_name}")
    try:
        logger.debug(f"Executing SQL: DROP TABLE IF EXISTS {full_table_name}")
        spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
        logger.info(f"Successfully dropped table: {full_table_name}\n")

    except Exception as e:
        logger.error(f"Failed to drop table {full_table_name}: {str(e)}", exc_info=True)
        raise

def migrate_inplace_to_iceberg(logger: logging.Logger, spark: SparkSession, database_name: str, table_name: str) -> None:
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
        logger.info("Unsetting 'TRANSLATED_TO_EXTERNAL' table property")
        unset_query = f"ALTER TABLE {database_name}.{table_name} UNSET TBLPROPERTIES ('TRANSLATED_TO_EXTERNAL')"
        logger.debug(f"Executing query: {unset_query}")
        spark.sql(unset_query)
        logger.info("Successfully unsetted 'TRANSLATED_TO_EXTERNAL' property")

        logger.info("Initiating Iceberg table migration")
        migrate_query = f"CALL spark_catalog.system.migrate('{database_name}.{table_name}')"
        logger.debug(f"Executing query: {migrate_query}")
        spark.sql(migrate_query)
        logger.info(f"Successfully migrated {database_name}.{table_name} to Iceberg format")

    except Exception as e:
        logger.error(f"Error occurred while migrating {database_name}.{table_name} to Iceberg: {str(e)}", exc_info=True)
        raise

    logger.info(f"Iceberg Migration In-place finished for table {database_name}.{table_name}\n")

def checks_on_migrated_to_iceberg(logger: logging.Logger, spark: SparkSession, database_name: str, table_name: str) -> None:
    """
    Perform checks on a table that has been migrated to Iceberg format, enhancing log readability.

    This function executes SQL queries to inspect the structure, partitions, history, and snapshots 
    of the migrated Iceberg table. It enhances log readability by formatting outputs, 
    limiting string lengths, and using appropriate log levels for concise summaries and detailed insights.

    Args:
        logger (logging.Logger): Logger instance for logging.
        spark (SparkSession): Active Spark session.
        database_name (str): Name of the database containing the table.
        table_name (str): Name of the table that was migrated to Iceberg.

    Raises:
        Exception: If an error occurs during any of the check operations.
    """

    full_table_name = f"spark_catalog.{database_name}.{table_name}"
    logger.info(f"Performing checks on Iceberg migrated table: {full_table_name}")

    try:
        logger.info(f"Retrieving table schema for {database_name}.{table_name}")
        schema_df = spark.sql(f"DESCRIBE {full_table_name}")
        schema_str = "\n".join([f"{row.col_name:<30} {row.data_type}" for row in schema_df.collect()])
        logger.info(f"Table schema:\n{schema_str}")

        logger.info(f"Executing DESCRIBE TABLE command for {database_name}.{table_name}")
        describe_result = spark.sql(f"DESCRIBE TABLE {full_table_name}")
        describe_str = "\n".join([f"{row.col_name:<30} {row.data_type:<20} {row.comment}" for row in describe_result.collect()])
        logger.info(f"DESCRIBE TABLE result:\n{describe_str}")

        logger.info(f"Executing SHOW CREATE TABLE command for {database_name}.{table_name}")
        create_table_result = spark.sql(f"SHOW CREATE TABLE {full_table_name}")
        create_stmt = create_table_result.collect()[0]["createtab_stmt"]
        truncated_create_stmt = create_stmt[:500] + "..." if len(create_stmt) > 500 else create_stmt
        logger.info(f"SHOW CREATE TABLE result:\n{truncated_create_stmt}")

        logger.info(f"Retrieving table partitions for {database_name}.{table_name}")
        partitions_result = spark.sql(f"SELECT * FROM {full_table_name}.partitions")
        partitions_count = partitions_result.count()
        logger.info(f"Table has {partitions_count} partitions")
        logger.debug("Table partitions:")
        partitions_result.show(truncate=False)

        logger.info(f"Retrieving table history for {database_name}.{table_name}")
        history_result = spark.sql(f"SELECT * FROM {full_table_name}.history")
        history_count = history_result.count()
        logger.info(f"Table has {history_count} history entries")
        logger.debug("Table history:")
        history_result.show(truncate=False)

        logger.info(f"Retrieving table snapshots for {database_name}.{table_name}")
        snapshots_result = spark.sql(f"SELECT * FROM {full_table_name}.snapshots")
        snapshots_count = snapshots_result.count()
        logger.info(f"Table has {snapshots_count} snapshots")
        logger.debug("Table snapshots:")
        snapshots_result.show(truncate=False)

        logger.info(f"All checks completed successfully for table: {full_table_name}\n")

    except Exception as e:
        logger.error(f"Error occurred while checking Iceberg migrated table {full_table_name}: {str(e)}", exc_info=True)
        raise

def main() -> None:
    """
    Main function to create tables based on configuration.

    This function validates the Hive metastore connection, iterates through the tables
    defined in the configuration, and creates them if they do not already exist.
    """

    logger = setup_logging()

    logger.info("Starting main function")
    config = load_config(logger)

    username = sys.argv[1] if len(sys.argv) > 1 else 'forgetArguments'
    logger.debug(f"Loading username correctly? Var: {username}")
    database_name = config['DEFAULT'].get('dbname') + '_' + username
    logger.debug(f"Database name: {database_name}")

    app_name = "IcebergMigrationInplace"
    extra_conf = {
                    "spark.sql.catalogImplementation": "hive", \
                    "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkSessionCatalog", \
                    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
                 }

    try:
        spark = create_spark_session(logger, app_name, extra_conf)
        logger.info("Spark session created successfully")

        validate_hive_metastore(logger, spark)

        create_table_for_miginplace(logger, spark, database_name, table_name)

        tables = [f"{table.strip()}_miginplace" for table in config['DEFAULT']['tables'].split(',')]
        logger.info(f"Tables to be migrated: {tables}")

        for table_name in tables:
            drop_snapshot_table_if_exists(logger, spark, database_name, table_name)
            snaptable = iceberg_migration_snaptable(logger, spark, database_name, table_name)

            result = iceberg_sanity_checks(logger, spark, database_name, table_name, snaptable)

            if result:
                logger.info("All sanity checks passed!")
                drop_snaptable(logger, spark, database_name, snaptable)
                migrate_inplace_to_iceberg(logger, spark, database_name, table_name)
                checks_on_migrated_to_iceberg(logger, spark, database_name, table_name)
            else:
                logger.warning("Some checks failed. Review the logs for details. Iceberg Migration In-place Cancelled.")

        spark.stop()

    except Exception as e:
        logger.error(f"An error occurred during the Iceberg migration process: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()