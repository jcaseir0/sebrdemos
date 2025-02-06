import os, logging, random, time
import configparser
from datetime import datetime, timedelta
from pyspark.sql.utils import AnalysisException
from faker import Faker

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)  # Set logging level to DEBUG

fake = Faker('pt_BR')

def load_config(config_path='/app/mount/config.ini'):
    """
    Load configuration from a specified file.

    Args:
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

def table_exists(spark, database_name, table_name):
    """
    Check if a table exists in the Hive Metastore.

    Args:
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

def validate_hive_metastore(spark, max_retries=3, retry_delay=5):
    """
    Validate the connection to the Hive metastore with retry logic.

    Args:
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

def get_schema_path(base_path, table_name):
    """
    Get the schema file path for a given table.

    Args:
        base_path (str): The base path where schema files are stored.
        table_name (str): The name of the table.

    Returns:
        str: The full path to the schema file.
    """
    logger.info(f"Getting schema path for table: {table_name}")
    schema_filename = f"{table_name}.json"
    return os.path.join(base_path, "schemas", schema_filename)

def analyze_table_structure(spark, database_name, tables):
    """
    Analyze the structure of given tables in a database.

    This function examines each table's structure to determine if it's partitioned,
    bucketed, both, or neither.

    Args:
        spark (SparkSession): The active Spark session.
        database_name (str): The name of the database containing the tables.
        tables (list): A list of table names to analyze.

    Returns:
        list: A list of dictionaries containing the structure information for each table.

    Raises:
        Exception: If an error occurs while analyzing table structure.
    """
    results = []
    for table_name in tables:
        logger.info(f"Analyzing structure of table: {database_name}.{table_name}")
        try:
            create_table_stmt = spark.sql(f"SHOW CREATE TABLE {database_name}.{table_name}").collect()[0]['createtab_stmt']
            
            is_partitioned = 'PARTITIONED BY' in create_table_stmt
            is_bucketed = 'CLUSTERED BY' in create_table_stmt and 'INTO' in create_table_stmt and 'BUCKETS' in create_table_stmt
            
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
            
            logger.info(f"Structure analysis completed for {database_name}.{table_name}")
        except Exception as e:
            logger.error(f"Error analyzing structure of {database_name}.{table_name}: {str(e)}", exc_info=True)
    
    return results

def collect_statistics(df, columns=None):
    """
    Coleta estatísticas de um DataFrame PySpark.
    
    :param df: DataFrame PySpark
    :param columns: Lista de colunas para analisar (opcional, padrão: todas as colunas numéricas)
    :return: DataFrame com estatísticas
    """
    if columns is None:
        # Seleciona apenas colunas numéricas se nenhuma for especificada
        columns = [c for c, t in df.dtypes if t in ('int', 'long', 'float', 'double')]
    
    # Calcula estatísticas usando o método summary
    stats = df.select(columns).summary(
        "count", "mean", "stddev", "min", "25%", "50%", "75%", "max"
    )
    
    return stats

def gerar_numero_cartao():
    """
    Generate a random credit card number.

    Returns:
        str: A 16-digit random credit card number.
    """
    logger.debug("Generating credit card number")
    return ''.join([str(random.randint(0, 9)) for _ in range(16)])

def gerar_cliente():
    """
    Generate a random client record.

    Returns:
        dict: A dictionary containing client details.
    """
    logger.debug("Generating client record")
    ufs = ["AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO"]
    return {
        "id_usuario": random.randint(1, 1000),
        "nome": fake.name(),
        "email": fake.email(),
        "data_nascimento": datetime.strptime(fake.date_of_birth(minimum_age=18, maximum_age=90).isoformat(), "%Y-%m-%d").date(),
        "endereco": fake.address().replace('\n', ', '),
        "limite_credito": random.choice([1000, 2000, 5000, 10000, 20000]),
        "numero_cartao": gerar_numero_cartao(),
        "id_uf": random.choice(ufs)
    }

def gerar_transacao(clientes_id_usuarios=None):
    """
    Generate a random transaction record.

    Args:
    clientes_id_usuarios (list): Optional list of client user IDs.

    Returns:
    dict: A dictionary containing transaction details.
    """
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365 * 10)  # 10 years ago
    
    return {
        "id_usuario": random.choice(clientes_id_usuarios) if clientes_id_usuarios else random.randint(1, 1000),
        "data_transacao": fake.date_time_between(start_date=start_date, end_date=end_date),
        "valor": round(random.uniform(10, 1000), 2),
        "estabelecimento": fake.company(),
        "categoria": random.choice(["Alimentação", "Transporte", "Entretenimento", "Saúde", "Educação", "Outros"]),
        "status": random.choice(["Aprovada", "Negada", "Pendente", "Cancelada", "Extornada"])
    }

def gerar_dados(table_name, num_records, clientes_id_usuarios=None):
    """
    Generate random data for a given table.

    Args:
    table_name (str): Name of the table to generate data for.
    num_records (int): Number of records to generate.
    clientes_id_usuarios (list): Optional list of client user IDs.

    Returns:
    list: A list of dictionaries containing the generated data.
    """
    if table_name == 'clientes':
        return [gerar_cliente() for _ in range(num_records)]
    elif table_name == 'transacoes_cartao':
        return [gerar_transacao(clientes_id_usuarios) for _ in range(num_records)]
    else:
        raise ValueError(f"Tabela desconhecida: {table_name}")
