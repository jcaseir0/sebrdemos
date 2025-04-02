import os, logging, random, time, re
from itertools import count as itertools_count
import configparser
from datetime import datetime, timedelta
from pyspark.sql.utils import AnalysisException
from pyspark.sql import SparkSession
from faker import Faker
from pyspark.sql.functions import col

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

fake = Faker('pt_BR')
logger.debug(f"Faker instance created: {fake}")
id_counter = itertools_count(1)
logger.debug(f"ID counter created: {id_counter}")

def setup_logging():
    '''
    Configura o logging para exibir mensagens de INFO e DEBUG, com uma variável LOGLEVEL para definir o nível de log.

    Returns:
        logging.Logger: Objeto de log configurado.
    '''

    # Define o nível de log padrão
    loglevel = os.getenv("LOGLEVEL", "INFO").upper() # Padrão: INFO
    numeric_level = getattr(logging, loglevel, None) # Numeric value of log level options: DEBUG=10, INFO=20, WARNING=30, ERROR=40, CRITICAL=50
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {loglevel}")
    
    # Configura o logging
    logging.basicConfig(level=numeric_level, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    logger.info(f"Logging level set to: {loglevel}")
    return logger

def load_config(logger: logging.Logger, config_path: str='/app/mount/config.ini') -> configparser.ConfigParser:
    """
    Load configuration from a specified file.

    Args:
        logger (logging.Logger): Logger instance.
        config_path (str): Path to the configuration file.

    Returns:
        ConfigParser: Loaded configuration object.

    Raises:
        FileNotFoundError: If the configuration file does not exist.
        Exception: If there is an error loading the configuration.
    """
    logger.debug(f"Attempting to load configuration from: {config_path}")
    if not os.path.exists(config_path):
        logger.error(f"Configuration file not found: {config_path}")
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    try:
        config = configparser.ConfigParser()
        config.read(config_path)
        logger.info("Configuration loaded successfully.")
        return config
    except Exception as e:
        logger.error(f"Error loading configuration: {str(e)}")
        raise

def table_exists(logger: logging.Logger, spark: SparkSession, database_name: str, table_name: str) -> bool:
    """
    Check if a table exists in the Hive Metastore.

    Args:
        logger (logging.Logger): Logger instance.
        spark (SparkSession): The active Spark session.
        database_name (str): The name of the database containing the table.
        table_name (str): The name of the table to check.

    Returns:
        bool: True if the table exists, False otherwise.

    Raises:
        Exception: If there is an error checking the table existence.
    """
    logger.debug(f"Checking existence of table: {database_name}.{table_name}")
    try:
        result = spark.sql(f"SHOW TABLES IN {database_name} LIKE '{table_name}'").count() > 0
        logger.info(f"Table '{database_name}.{table_name}' exists: {result}")
        return result
    except Exception as e:
        logger.error(f"Error checking table existence '{database_name}.{table_name}': {str(e)}")
        raise

def validate_hive_metastore(logger: logging.Logger, spark: SparkSession, max_retries: int=3, retry_delay: int=5) -> bool:
    """
    Validate the connection to the Hive metastore with retry logic.

    Args:
        logger (logging.Logger): Logger instance.
        spark (SparkSession): The Spark session.
        max_retries (int): Maximum number of retries.
        retry_delay (int): Delay between retries in seconds.

    Returns:
        bool: True if the connection is successful, False otherwise.

    Raises:
        AnalysisException: If the connection fails after all retries.
    """
    logger.info("Validating Hive metastore connection")
    for attempt in range(max_retries):
        try:
            spark.sql("SHOW DATABASES").show()
            logger.info("Hive metastore connection stabilished successfully\n")
            return True
        except AnalysisException as e:
            if attempt < max_retries - 1:
                logger.warning(f"Trying {attempt + 1} failed. Trying again in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error("Failure trying to stabilish connection with Hive Metastore after several tries")
                raise
    return False

def get_schema_path(logger: logging.Logger, base_path: str, table_name: str) -> str:
    """
    Get the schema file path for a given table.

    Args:
        logger (logging.Logger): Logger instance.
        base_path (str): The base path where schema files are stored.
        table_name (str): The name of the table.

    Returns:
        str: The full path to the schema file.
    """

    logger.info(f"Getting schema path for table: {table_name}")
    
    schema_filename = f"{table_name}.json"
    
    return os.path.join(base_path, "schemas", schema_filename)

def analyze_table_structure(logger: logging.Logger, spark: SparkSession, database_name: str, tables: str) -> list:
    """
    Analyze the structure of given tables in a database.

    This function examines each table's structure to determine if it's partitioned,
    bucketed, both, or neither.

    Args:
        logger (logging.Logger): Logger instance.
        spark (SparkSession): The active Spark session.
        database_name (str): The name of the database containing the tables.
        tables (list): A list of table names to analyze.

    Returns:
        list: A list of dictionaries containing the structure information for each table.

    Raises:
        Exception: If an error occurs while analyzing table structure.
    """

    logger.info(f"Analyzing structure of tables in database: {database_name}")

    results = []
    for table_name in tables:
        logger.info(f"Analyzing structure of table: {database_name}.{table_name}")
        try:
            create_table_stmt = spark.sql(f"SHOW CREATE TABLE {database_name}.{table_name}").collect()[0]['createtab_stmt']
            logger.debug(f"Create table statement: {create_table_stmt}")
            
            is_partitioned = 'PARTITIONED BY' in create_table_stmt
            logger.debug(f"Is partitioned: {is_partitioned}")
            is_bucketed = 'CLUSTERED BY' in create_table_stmt and 'INTO' in create_table_stmt and 'BUCKETS' in create_table_stmt
            logger.debug(f"Is bucketed: {is_bucketed}")
            
            if is_partitioned and is_bucketed:
                structure = "Particionada e Bucketed"
            elif is_partitioned:
                structure = "Particionada"
            elif is_bucketed:
                structure = "Bucketed"
            else:
                structure = "Nenhuma"
            
            logger.debug(f"Table structure for {table_name}: {structure}")
            
            results.append({
                "database": database_name,
                "table": table_name,
                "structure": structure
            })
            logger.debug(f"Results: {results}")

            logger.info(f"Structure analysis completed for {database_name}.{table_name}")

        except Exception as e:
            logger.error(f"Error analyzing structure of {database_name}.{table_name}: {str(e)}", exc_info=True)
    
    return results

def extract_bucket_info(logger: logging.Logger, create_stmt: str) -> tuple:
    """
    Extract bucket information from a CREATE TABLE statement.

    Args:
        logger (logging.Logger): Logger instance.
        create_stmt (str): The CREATE TABLE statement.

    Returns:
        tuple: A tuple containing the bucketed column and number of buckets, or (None, None) if not bucketed.
    """

    logger.info("Extracting bucket information from CREATE TABLE statement")

    bucket_pattern = r'CLUSTERED BY \((.*?)\)\s+INTO (\d+) BUCKETS'
    logger.debug(f"Bucket pattern: {bucket_pattern}")
    bucket_info = re.search(bucket_pattern, create_stmt, re.IGNORECASE | re.DOTALL)
    logger.debug(f"Bucket info: {bucket_info}")

    if bucket_info:
        bucketed_column = bucket_info.group(1).strip()
        num_buckets = int(bucket_info.group(2))
        logger.debug(f"Bucketed column: {bucketed_column}, Number of buckets: {num_buckets}")
        return bucketed_column, num_buckets
    
    return None, None

def get_table_columns(logger: logging.Logger, spark: SparkSession, database_name: str, table_name: str) -> list:
    """
    Retrieves a list of valid column names from the table schema.

    This function fetches the schema of the specified table and extracts a list of column
    names, excluding partition information and special columns (e.g., '# col_name', 'data_type').

    Args:
        logger (logging.Logger): Logger instance for logging.
        spark (SparkSession): Active Spark session.
        database_name (str): Name of the database containing the table.
        table_name (str): Name of the table.

    Returns:
        list: A list of valid column names for the table.

    Raises:
        Exception: If an error occurs while retrieving the table schema.
    """

    logger.info(f"Retrieving table schema for {database_name}.{table_name}")

    try:
        df = spark.sql(f"DESCRIBE {database_name}.{table_name}")
        logger.debug(f"Table schema for {database_name}.{table_name}:\n{df.show()}")
        
        valid_columns = df.filter(
            (~col("col_name").isin("# col_name", "data_type")) &
            (~col("col_name").startswith("#")) &
            (~col("col_name").startswith("Part")) &
            (col("col_name") != "")
        ).select("col_name").rdd.flatMap(lambda x: x).collect()
        
        logger.info(f"Valid columns: {', '.join(valid_columns)}")

        return valid_columns
    
    except Exception as e:
        logger.error(f"Error retrieving table schema for {database_name}.{table_name}: {str(e)}")
        raise

def rename_backup_tables(logger: logging.Logger, spark: SparkSession, tables: list, database_name: str) -> None:
    """
    Renames tables with '_backup_' in their name to '_original'.

    Args:
        spark (SparkSession): The active Spark session.
        database_name (str): The name of the database containing the tables.

    Raises:
        Exception: If an error occurs during the renaming process.
    """
    
    logger.info(f"Renaming backup tables in database {database_name}")
    
    try:

        tables_to_rename = [table.tableName for table in tables if '_backup_' in table.tableName]
        
        for table_name in tables_to_rename:
            logger.debug(f"Renaming table {table_name} to {table_name.replace('_backup_', '_original')}")
            spark.sql(f"ALTER TABLE {database_name}.{table_name} RENAME TO {database_name}.{table_name.replace('_backup_', '_original')}")
            logger.info(f"Table {table_name} renamed to {table_name.replace('_backup_', '_original')}")
    
    except Exception as e:
        logger.error(f"Error renaming tables: {str(e)}")
        raise

def gerar_numero_cartao(logger: logging.Logger):
    """
    Generate a random credit card number.

    Args:
        logger (logging.Logger): Logger instance.

    Returns:
        str: A 16-digit random credit card number.
    """

    logger.debug("Generating credit card number")
    
    return ''.join([str(random.randint(0, 9)) for _ in range(16)])

def gerar_cliente(logger: logging.Logger, fake: Faker) -> dict:
    """
    Generate a random client record with a unique, formatted user ID.
    
    The function ensures that the id_usuario is unique and formatted with leading zeros 
    to have a length of 9 digits. It also maintains a 1:1 relationship between id_usuario and nome.

    Args:
        logger (logging.Logger): Logger instance.
        fake (Faker): Faker instance for generating fake data.

    Returns:
        dict: A dictionary containing client details.
    """

    logger.debug("Generating client record")

    ufs = ["AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO"]
    
    id_usuario = str(next(id_counter)).zfill(9)
    
    return {
        "id_usuario": id_usuario,
        "nome": fake.name(),
        "email": fake.email(),
        "data_nascimento": fake.date_of_birth(minimum_age=18, maximum_age=90),
        "endereco": fake.address().replace('\n', ', '),
        "limite_credito": random.choice([1000, 1500, 2000, 2500, 3000, 3500, 4000, 4500, 5000, 5500, 6000, 7000, 8000, 9000, 10000, 20000]),
        "numero_cartao": gerar_numero_cartao(logger),
        "id_uf": random.choice(ufs)
    }

def gerar_transacao(logger: logging.Logger, fake: Faker, clientes_id_usuarios: list) -> dict:
    """
    Generate a random transaction record.

    Args:
        logger (logging.Logger): Logger instance.
        fake (Faker): Faker instance for generating fake data.
        clientes_id_usuarios (list): Optional list of client user IDs.

    Returns:
        dict: A dictionary containing transaction details.
    """
    
    logger.debug("Generating transaction record")
    
    end_date = datetime.now()
    logger.debug(f"End date: {end_date}")
    start_date = end_date - timedelta(days=365 * 10)  # 10 years ago
    logger.debug(f"Start date: {start_date}")

    return {
        "id_usuario": random.choice(clientes_id_usuarios) if clientes_id_usuarios else random.randint(1, 100000),
        "data_transacao": fake.date_time_between(start_date=start_date, end_date=end_date),
        "valor": round(random.uniform(1, 99999), 2),
        "estabelecimento": fake.company(),
        "categoria": random.choice(["Alimentação", "Transporte", "Entretenimento", "Saúde", "Educação", "Outros"]),
        "status": random.choice(["Aprovada", "Negada", "Pendente", "Cancelada", "Extornada"])
    }

def gerar_dados(logger: logging.Logger, table_name: str, num_records: int, clientes_id_usuarios: list=None) -> list:
    """
    Generate random data for a given table.

    Args:
        logger (logging.Logger): Logger instance.
        table_name (str): Name of the table to generate data for.
        num_records (int): Number of records to generate.
        clientes_id_usuarios (list): Optional list of client user IDs.

    Returns:
        list: A list of dictionaries containing the generated data.
    """

    logger.info(f"Generating {num_records} data registries for table: {table_name}")

    try:

        if table_name == 'clientes':
            logger.info(f"Data generated for table: {table_name}")
            return [gerar_cliente(logger, fake) for _ in range(num_records)]
        
        elif table_name == 'transacoes_cartao':
            if not clientes_id_usuarios:
                    raise ValueError("IDs de clientes necessários para gerar transações")
            
            logger.info(f"Data generated for table: {table_name}")
            return [gerar_transacao(logger, fake, clientes_id_usuarios) for _ in range(num_records)]
        else:
            raise ValueError(f"Tabela desconhecida: {table_name}")
        
    except Exception as e:
        logger.error(f"Erro na geração de dados: {str(e)}")
        raise