from pyspark.sql import SparkSession
from datetime import datetime
import sys, os, logging, re
from pyspark.sql.functions import col
# Adicionar o diretório pai ao caminho de busca do Python do pacote common_functions
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common_functions import load_config

# Configuração do logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session() -> SparkSession:
    """
    Cria uma sessão Spark com configurações específicas para o Iceberg.

    Args:
        username (str): Nome do usuário para o schema do banco de dados.

    Returns:
        SparkSession: Sessão Spark configurada.
    """
    logger.info("Criando sessão Spark...")
    spark = SparkSession \
        .builder \
        .appName("ICEBERG LOAD") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")\
        .config("spark.sql.catalog.spark_catalog.type", "hive")\
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")\
        .getOrCreate()
    logger.info("Sessão Spark criada com sucesso.")
    return spark

def show_partitions(spark: SparkSession, database_name: str, table_name: str) -> None:
    """
    Exibe as partições da tabela de informações antes da migração para o Iceberg.

    Args:
        spark (SparkSession): Sessão Spark ativa.
        database_name (str): Nome do banco de dados onde as tabelas se encontram.
        table_name (str): Nome do banco de dados onde as tabelas se encontram.
    """
    logger.info("Exibindo partições da tabela {table_name}...")
    print("PRE-ICEBERG MIGRATION TABLE PARTITIONS: \n")
    print(f"SHOW PARTITIONS {database_name}.{table_name}\n")
    spark.sql(f"SHOW PARTITIONS {database_name}.{table_name}").show(truncate=False)

def check_table_exists(spark: SparkSession, database_name: str, table_name: str) -> bool:
    """
    Verifica se a tabela Iceberg existe no catálogo.

    Args:
        spark (SparkSession): Sessão Spark ativa.
        database_name (str): Nome do banco de dados onde a tabela está localizada.
        table_name (str): Nome da tabela a ser verificada.

    Returns:
        bool: True se a tabela existir, False caso contrário.
    """
    logger.info(f"Verificando existência da tabela '{table_name}' no banco de dados '{database_name}'...")
    
    try:
        # Listar tabelas no banco de dados especificado
        tblLst = spark.catalog.listTables(database_name)
        
        # Extrair nomes das tabelas
        table_names_in_db = [table.name for table in tblLst]
        
        # Verificar se a tabela existe
        table_exists = table_name in table_names_in_db
        
        logger.info(f"Tabela '{table_name}' existe: {table_exists}")
        return table_exists
    
    except Exception as e:
        logger.error(f"Erro ao verificar existência da tabela: {e}")
        return False

def migrate_to_iceberg(spark: SparkSession, database_name: str, table_name: str) -> None:
    """
    Migra a tabela de informações para o formato Iceberg.

    Args:
        spark (SparkSession): Sessão Spark ativa.
        username (str): Nome do usuário para o schema do banco de dados.
        table_name (str): Nome da tabela a ser migrada.
    """
    logger.info("Migrando tabela para o Iceberg...")
    db_name = "{}_CUSTOMER".format(username)
    
    if check_table_exists(spark, username, table_name):
        logger.info("Tabela já existe no formato Iceberg.")
        rcbi = spark.sql("SELECT COUNT(*) FROM spark_catalog.{}_CUSTOMER.{}".format(username, table_name)).collect()[0][0]
        lastpart = spark.sql("SELECT * FROM spark_catalog.{}_CUSTOMER.{}.PARTITIONS".format(username, table_name)).collect()[-1][0]
        # Partition validation
        part = "Row(frameworkdate='" + "2025-02-06" + "')"
        if str(lastpart) != part:
            logger.info("Nova partição detectada. Inserindo dados...")
            spark.sql("INSERT INTO {0}_CUSTOMER.{1} SELECT * FROM {0}_CUSTOMER.INFORMATION WHERE frameworkdate = '2025-02-06'".format(username, table_name))
        else:
            logger.info("Partição já carregada.")
            sys.exit(1)
    else:
        logger.info("Criando tabela no formato Iceberg...")
        print("#----------------------------------------------------")
        print("#    CTAS MIGRATION SPARK TABLES TO ICEBERG TABLE    ")
        print("#----------------------------------------------------\n")
        print("\tALTER TABLE {}_CUSTOMER.INFORMATION UNSET TBLPROPERTIES ('TRANSLATED_TO_EXTERNAL')\n".format(username))
        spark.sql("ALTER TABLE {}_CUSTOMER.INFORMATION UNSET TBLPROPERTIES ('TRANSLATED_TO_EXTERNAL')".format(username))
        print("\tCREATE TABLE spark_catalog.{0}_CUSTOMER.{1} USING iceberg PARTITIONED BY (frameworkdate) AS SELECT * FROM {0}_CUSTOMER.INFORMATION\n".format(username, table_name))
        spark.sql("CREATE TABLE spark_catalog.{0}_CUSTOMER.{1} USING iceberg PARTITIONED BY (frameworkdate) AS SELECT * FROM {0}_CUSTOMER.INFORMATION".format(username, table_name))
        logger.info("Tabela criada no formato Iceberg.")

def describe_table(spark: SparkSession, username: str, table_name: str) -> None:
    """
    Descreve a estrutura da tabela Iceberg.

    Args:
        spark (SparkSession): Sessão Spark ativa.
        username (str): Nome do usuário para o schema do banco de dados.
        table_name (str): Nome da tabela a ser descrita.
    """
    logger.info("Descrevendo tabela Iceberg...")
    print("DESCRIBE TABLE spark_catalog.{}_CUSTOMER.{}\n".format(username, table_name))
    spark.sql("DESCRIBE TABLE spark_catalog.{}_CUSTOMER.{}".format(username, table_name)).show(20, False)

def show_partitions_post_migration(spark: SparkSession, username: str, table_name: str) -> None:
    """
    Exibe as partições da tabela Iceberg após a migração.

    Args:
        spark (SparkSession): Sessão Spark ativa.
        username (str): Nome do usuário para o schema do banco de dados.
        table_name (str): Nome da tabela.
    """
    logger.info("Exibindo partições da tabela Iceberg após a migração...")
    print("CUSTOMER TABLE POST-ICEBERG MIGRATION PARTITIONS: \n")
    spark.sql("SELECT * FROM spark_catalog.{}_CUSTOMER.{}.PARTITIONS".format(username, table_name)).show()

def show_iceberg_snapshots(spark: SparkSession, username: str, table_name: str) -> None:
    """
    Exibe os snapshots da tabela Iceberg.

    Args:
        spark (SparkSession): Sessão Spark ativa.
        username (str): Nome do usuário para o schema do banco de dados.
        table_name (str): Nome da tabela.
    """
    logger.info("Exibindo snapshots da tabela Iceberg...")
    print("#---------------------------------------------------")
    print("#            SHOW ICEBERG TABLE SNAPSHOTS           ")
    print("#---------------------------------------------------")
    print("\n")
    spark.sql("SELECT * FROM spark_catalog.{}_CUSTOMER.{}.history".format(username, table_name)).show(20, False)
    spark.sql("SELECT * FROM spark_catalog.{}_CUSTOMER.{}.snapshots".format(username, table_name)).show(20, False)

def insert_data(spark: SparkSession, username: str, table_name: str) -> None:
    """
    Insere dados na tabela Iceberg.

    Args:
        spark (SparkSession): Sessão Spark ativa.
        username (str): Nome do usuário para o schema do banco de dados.
        table_name (str): Nome da tabela.
    """
    logger.info("Inserindo dados na tabela Iceberg...")
    print("#---------------------------------------------------")
    print("#               INSERT DATA                         ")
    print("#---------------------------------------------------")
    print("\n")
    # PRE-INSERT COUNT
    print("PRE-INSERT COUNT")
    spark.sql("SELECT COUNT(*) FROM spark_catalog.{}_CUSTOMER.{}".format(username, table_name)).show()
    print("\n")
    
    # INSERT DATA VIA DATAFRAME API
    print("#---------------------------------------------------")
    print("#        INSERT DATA VIA DATAFRAME API              ")
    print("#---------------------------------------------------")
    temp_df = spark.sql("SELECT * FROM spark_catalog.{}_CUSTOMER.{}".format(username, table_name)).sample(fraction=0.3, seed=3)
    temp_df.writeTo("spark_catalog.{}_CUSTOMER.{}".format(username, table_name)).append()
    print("\n")
    
    # INSERT DATA VIA SPARK SQL
    print("#---------------------------------------------------")
    print("#        INSERT DATA VIA SPARK SQL                  ")
    print("#---------------------------------------------------")
    temp_df.createOrReplaceTempView("CUSTOMER_SAMPLE".format(username))
    insert_qry = "INSERT INTO spark_catalog.{0}_CUSTOMER.{1} SELECT * FROM CUSTOMER_SAMPLE".format(username, table_name)
    print(insert_qry)
    spark.sql(insert_qry)
    print("\n")

def time_travel(spark: SparkSession, username: str, table_name: str) -> None:
    """
    Realiza operações de time travel na tabela Iceberg.

    Args:
        spark (SparkSession): Sessão Spark ativa.
        username (str): Nome do usuário para o schema do banco de dados.
        table_name (str): Nome da tabela.
    """
    logger.info("Realizando operações de time travel...")
    print("#---------------------------------------------------")
    print("#               TIME TRAVEL                         ")
    print("#---------------------------------------------------")
    
    # NOTICE SNAPSHOTS HAVE BEEN ADDED
    spark.sql("SELECT * FROM spark_catalog.{}_CUSTOMER.{}.history".format(username, table_name)).show(20, False)
    spark.sql("SELECT * FROM spark_catalog.{}_CUSTOMER.{}.snapshots".format(username, table_name)).show(20, False)

    # POST-INSERT COUNT
    print("\n")
    print("POST-INSERT COUNT")
    spark.sql("SELECT COUNT(*) FROM spark_catalog.{}_CUSTOMER.{}".format(username, table_name)).show()

    # TIME TRAVEL AS OF PREVIOUS TIMESTAMP
    now = datetime.now()
    timestamp = datetime.timestamp(now)
    df = spark.read.option("as-of-timestamp", int(timestamp*1000)).format("iceberg").load("spark_catalog.{}_CUSTOMER.{}".format(username, table_name))

    # POST TIME TRAVEL COUNT
    print("\n")
    print("POST-TIME TRAVEL COUNT")
    print(df.count())

def incremental_read(spark: SparkSession, username: str, table_name: str) -> None:
    """
    Realiza leitura incremental da tabela Iceberg.

    Args:
        spark (SparkSession): Sessão Spark ativa.
        username (str): Nome do usuário para o schema do banco de dados.
        table_name (str): Nome da tabela.
    """
    logger.info("Realizando leitura incremental...")
    print("#---------------------------------------------------")
    print("#               INCREMENTAL READ                    ")
    print("#---------------------------------------------------")
    
    print("\n")
    print("INCREMENTAL READ")
    print("\n")
    print("ICEBERG TABLE HISTORY (SHOWS EACH SNAPSHOT AND TIMESTAMP)")
    print("SELECT * FROM {}_CUSTOMER.{}.history;".format(username, table_name))
    spark.sql("SELECT * FROM {}_CUSTOMER.{}.history;".format(username, table_name)).show()
    print
