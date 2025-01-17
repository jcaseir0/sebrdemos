# -*- coding: utf-8 -*-
import sys, json, logging, os, time, configparser
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import current_date
from pyspark.conf import SparkConf
from pyspark.sql.utils import AnalysisException

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

logger.debug(f"Diretório de trabalho atual: {os.getcwd()}")
logger.debug(f"Conteúdo do diretório /app/mount/: {os.listdir('/app/mount/')}")

# Adicionar o diretório /app/mount/ ao sys.path
sys.path.append('/app/mount')

from utils import gerar_dados

def carregar_configuracao(config_path='/app/mount/config.ini'):
    try:
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Arquivo de configuração não encontrado: {config_path}")
        
        config = configparser.ConfigParser()
        config.read(config_path)
        logger.info("Configuração carregada com sucesso.")
        logger.debug(f"Seções encontradas na configuração: {config.sections()}")
        return config
    except Exception as e:
        logger.error(f"Erro ao carregar configuração: {str(e)}")
        raise

def tabela_existe(spark, nome_tabela):
    try:
        result = spark.sql(f"SHOW TABLES LIKE '{nome_tabela}'").count() > 0
        logger.info(f"Verificação de existência da tabela '{nome_tabela}': {'Existe' if result else 'Não existe'}")
        return result
    except Exception as e:
        logger.error(f"Erro ao verificar existência da tabela '{nome_tabela}': {str(e)}")
        raise

def criar_ou_atualizar_tabela(spark, nome_tabela, config):
    try:
        schema_base_path = '/app/mount/'
        schema_paths = [
            os.path.join(schema_base_path, f'{nome_tabela}.json'),
            os.path.join(schema_base_path, f'{nome_tabela}s.json')
        ]
        esquema = None
        for schema_path in schema_paths:
            logger.info(f"Tentando carregar esquema da tabela '{nome_tabela}' de: {schema_path}")
            if os.path.exists(schema_path):
                with open(schema_path, 'r') as f:
                    schema_content = f.read()
                    logger.info(f"Conteúdo do esquema de {nome_tabela}: {schema_content}")
                    esquema = StructType.fromJson(json.loads(schema_content))
                logger.info(f"Esquema da tabela '{nome_tabela}' carregado com sucesso de {schema_path}.")
                break
        if esquema is None:
            raise FileNotFoundError(f"Nenhum arquivo de esquema encontrado para a tabela '{nome_tabela}'")
        
        num_records = config.getint(nome_tabela, 'num_records')
        particionamento = config.getboolean(nome_tabela, 'particionamento')
        bucketing = config.getboolean(nome_tabela, 'bucketing')
        num_buckets = config.getint(nome_tabela, 'num_buckets')
        apenas_arquivos = config.getboolean('DEFAULT', 'apenas_arquivos', fallback=False)
        formato_arquivo = config['DEFAULT'].get('formato_arquivo', 'parquet')
        logger.debug(f"Configurações para '{nome_tabela}':\n num_records={num_records},\n particionamento={particionamento},\n bucketing={bucketing},\n num_buckets={num_buckets}, \n only_files={apenas_arquivos},\n formato={formato_arquivo}")

        dados = gerar_dados(nome_tabela, num_records)
        df = spark.createDataFrame(dados, schema=esquema)
        df = df.withColumn("data_execucao", current_date())
        logger.info(f"Dados gerados para '{nome_tabela}': {num_records} registros")
        logger.info(f"Primeiras 10 linhas do DataFrame '{nome_tabela}':")
        df.show(10, truncate=False)

        storage = config['storage'].get('storage_type', 'S3')
        if storage == 'S3':
            base_path = config['storage'].get('base_path')
        elif storage == 'ADLS':
            base_path = config['storage']['base_path']
        else:
            raise ValueError(f"Armazenamento não suportado: {storage}")

        if apenas_arquivos:
            output_path = f"{base_path}{nome_tabela}"
            df.createOrReplaceTempView("temp_view")
            if particionamento:
                spark.sql(f"""
                    CREATE TABLE IF NOT EXISTS SPARK_CATALOG.{nome_tabela}
                    USING {formato_arquivo}
                    PARTITIONED BY (data_execucao)
                    LOCATION '{output_path}'
                    AS SELECT * FROM temp_view
                """)
                logger.info(f"Arquivos {formato_arquivo.upper()} para '{nome_tabela}' criados com particionamento por data_execucao em {output_path}")
            elif bucketing:
                spark.sql(f"""
                    CREATE TABLE IF NOT EXISTS SPARK_CATALOG.{nome_tabela}
                    USING {formato_arquivo}
                    CLUSTERED BY (id_uf) INTO {num_buckets} BUCKETS
                    LOCATION '{output_path}'
                    AS SELECT * FROM temp_view
                """)
                logger.info(f"Arquivos {formato_arquivo.upper()} para '{nome_tabela}' criados com bucketing por id_uf em {num_buckets} buckets em {output_path}")
            else:
                spark.sql(f"""
                    CREATE TABLE IF NOT EXISTS SPARK_CATALOG.{nome_tabela}
                    USING {formato_arquivo}
                    LOCATION '{output_path}'
                    AS SELECT * FROM temp_view
                """)
                logger.info(f"Arquivos {formato_arquivo.upper()} para '{nome_tabela}' criados sem particionamento ou bucketing em {output_path}")
        else:
            existe = tabela_existe(spark, nome_tabela)
            logger.debug(f"Tabela existe: {existe}")
            if not existe:
                df.createOrReplaceTempView("temp_view")
                if particionamento:
                    spark.sql(f"""
                        CREATE TABLE IF NOT EXISTS SPARK_CATALOG.{nome_tabela}
                        USING parquet
                        PARTITIONED BY (data_execucao)
                        AS SELECT * FROM temp_view
                    """)
                    logger.info(f"Tabela '{nome_tabela}' criada com particionamento por data_execucao")
                elif bucketing:
                    spark.sql(f"""
                        CREATE TABLE IF NOT EXISTS SPARK_CATALOG.{nome_tabela}
                        USING parquet
                        CLUSTERED BY (id_uf) INTO {num_buckets} BUCKETS
                        AS SELECT * FROM temp_view
                    """)
                    logger.info(f"Tabela '{nome_tabela}' criada com bucketing por id_uf em {num_buckets} buckets")
                else:
                    spark.sql(f"""
                        CREATE TABLE IF NOT EXISTS SPARK_CATALOG.{nome_tabela}
                        USING parquet
                        AS SELECT * FROM temp_view
                    """)
                    logger.info(f"Tabela '{nome_tabela}' criada sem particionamento ou bucketing")
            else:
                df.createOrReplaceTempView("temp_view")
                spark.sql(f"INSERT INTO SPARK_CATALOG.{nome_tabela} SELECT * FROM temp_view")
                logger.info(f"Dados inseridos na tabela '{nome_tabela}'")

    except Exception as e:
        logger.error(f"Erro ao criar ou atualizar tabela '{nome_tabela}': {str(e)}")
        raise

def main():
    try:
        config_path = '/app/mount/config.ini'
        config = carregar_configuracao(config_path)
        
        # Configurar a URI do metastore como uma string de conexão JDBC
        jdbc_url = config['DEFAULT'].get('hmsUrl')
        thrift_server = config['DEFAULT'].get('thriftServer')

        # Criar uma SparkConf com as configurações
        spark_conf = SparkConf()
        spark_conf.set("hive.metastore.client.factory.class", "com.cloudera.spark.hive.metastore.HivemetastoreClientFactory")
        spark_conf.set("hive.metastore.uris", thrift_server)
        spark_conf.set("spark.sql.hive.metastore.jars", "builtin")
        spark_conf.set("spark.sql.hive.hiveserver2.jdbc.url", jdbc_url)
        spark_conf.set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
        spark_conf.set("spark.sql.catalog.spark_catalog.type", "hive")

        # Criar a SparkSession usando a SparkConf
        spark = SparkSession.builder \
            .appName("SimulacaoDadosBancarios") \
            .config(conf=spark_conf) \
            .enableHiveSupport() \
            .getOrCreate()
        logger.info("Sessão Spark iniciada com sucesso.")

        max_retries = 3
        retry_delay = 5

        for attempt in range(max_retries):
            try:
                spark.sql("SHOW DATABASES").show()
                logger.info("Conexão com o Hive metastore estabelecida com sucesso")
                break
            except AnalysisException as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Tentativa {attempt + 1} falhou. Tentando novamente em {retry_delay} segundos...")
                    time.sleep(retry_delay)
                else:
                    logger.error("Falha ao conectar ao Hive metastore após várias tentativas")
                    raise

        apenas_arquivos = config.getboolean('DEFAULT', 'apenas_arquivos', fallback=False)
        if not apenas_arquivos:
            database_name = config['DEFAULT'].get('dbname', 'bancodemo')
            spark.sql(f"CREATE DATABASE IF NOT EXISTS SPARK_CATALOG.{database_name}")
            spark.sql(f"USE {database_name}")
            logger.info(f"Usando banco de dados: {database_name}")

        # Processamento das tabelas
        tabelas = config['DEFAULT'].get('tabelas', '').split(',')
        if tabelas:
            for tabela in tabelas:
                tabela = tabela.strip()
                logger.info(f"Processando tabela: '{tabela}'")
                if tabela in config.sections():
                    try:
                        criar_ou_atualizar_tabela(spark, tabela, config)
                    except Exception as e:
                        logger.error(f"Erro ao processar a tabela '{tabela}': {str(e)}")
                else:
                    logger.warning(f"Configuração não encontrada para a tabela '{tabela}'")
        else:
            logger.error("Nenhuma tabela especificada.")

    except Exception as e:
        logger.error(f"Erro na execução principal: {str(e)}")
        logger.debug("Detalhes do erro:", exc_info=True)
        sys.exit(1)
        raise
if __name__ == "__main__":
    main()
