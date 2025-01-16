# -*- coding: utf-8 -*-
import sys, json, logging, os, argparse, configparser
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import current_date

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

logger.info(f"Diretório de trabalho atual: {os.getcwd()}")
logger.info(f"Conteúdo do diretório /app/mount/: {os.listdir('/app/mount/')}")

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
        existe = spark.catalog.tableExists(nome_tabela)
        logger.info(f"Verificação de existência da tabela '{nome_tabela}': {'Existe' if existe else 'Não existe'}")
        return existe
    except Exception as e:
        logger.error(f"Erro ao verificar existência da tabela '{nome_tabela}': {str(e)}")
        raise

def criar_ou_atualizar_tabela(spark, nome_tabela, config, apenas_arquivos=False, formato_arquivo='parquet'):
    try:
        schema_base_path = '/app/mount/'
        schema_path = os.path.join(schema_base_path, f'{nome_tabela}.json')
        
        if not os.path.exists(schema_path):
            raise FileNotFoundError(f"Arquivo de esquema não encontrado: {schema_path}")

        with open(schema_path, 'r') as f:
            esquema = StructType.fromJson(json.load(f))
        logger.info(f"Esquema da tabela '{nome_tabela}' carregado com sucesso.")

        num_records = config.getint(nome_tabela, 'num_records')
        particionamento = config.getboolean(nome_tabela, 'particionamento')
        bucketing = config.getboolean(nome_tabela, 'bucketing')
        num_buckets = config.getint(nome_tabela, 'num_buckets')
        logger.info(f"Configurações para '{nome_tabela}': num_records={num_records}, particionamento={particionamento}, bucketing={bucketing}, num_buckets={num_buckets}")

        dados = gerar_dados(nome_tabela, num_records)
        df = spark.createDataFrame(dados, schema=esquema)
        df = df.withColumn("data_execucao", current_date())
        logger.info(f"Dados gerados para '{nome_tabela}': {num_records} registros")

        storage = config['storage'].get('storage_type', 'S3')
        if storage == 'S3':
            base_path = "s3a://goes-se-sandbox/data/bancodemo/"
        elif storage == 'ADLS':
            base_path = config['storage']['base_path']
        else:
            raise ValueError(f"Armazenamento não suportado: {storage}")

        if apenas_arquivos:
            output_path = f"{base_path}{nome_tabela}"
            write_options = {"mode": "overwrite", "format": formato_arquivo}
            
            if particionamento:
                df.write.partitionBy("data_execucao").options(**write_options).save(output_path)
                logger.info(f"Arquivos {formato_arquivo.upper()} para '{nome_tabela}' criados com particionamento por data_execucao em {output_path}")
            elif bucketing:
                df.write.bucketBy(num_buckets, "id_uf").options(**write_options).save(output_path)
                logger.info(f"Arquivos {formato_arquivo.upper()} para '{nome_tabela}' criados com bucketing por id_uf em {num_buckets} buckets em {output_path}")
            else:
                df.write.options(**write_options).save(output_path)
                logger.info(f"Arquivos {formato_arquivo.upper()} para '{nome_tabela}' criados sem particionamento ou bucketing em {output_path}")
        else:
            write_mode = "overwrite" if not tabela_existe(spark, nome_tabela) else "append"
            if write_mode == "append":
                spark.sql(f"REFRESH TABLE {nome_tabela}")
            write_options = {"mode": write_mode, "format": "parquet"}

            if particionamento:
                df.write.partitionBy("data_execucao").options(**write_options).saveAsTable(nome_tabela)
                logger.info(f"Tabela '{nome_tabela}' {write_mode} com particionamento por data_execucao")
            elif bucketing:
                df.write.bucketBy(num_buckets, "id_uf").options(**write_options).saveAsTable(nome_tabela)
                logger.info(f"Tabela '{nome_tabela}' {write_mode} com bucketing por id_uf em {num_buckets} buckets")
            else:
                df.write.options(**write_options).saveAsTable(nome_tabela)
                logger.info(f"Tabela '{nome_tabela}' {write_mode} sem particionamento ou bucketing")

    except Exception as e:
        logger.error(f"Erro ao criar ou atualizar tabela '{nome_tabela}': {str(e)}")
        raise

def main(tabelas, apenas_arquivos=False):
    try:
        config_path = '/app/mount/config.ini'
        config = carregar_configuracao(config_path)
        erro_encontrado = False
        
        # Iniciar sessão Spark
        spark = SparkSession \
            .builder \
            .appName("Simulacao Dados Bancarios") \
            .getOrCreate()
        logger.info("Sessão Spark iniciada com sucesso.")

        if not apenas_arquivos:
            database_name = config['DEFAULT'].get('dbname', 'bancodemo')
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
            spark.sql(f"USE {database_name}")
            logger.info(f"Usando banco de dados: {database_name}")

        # Processamento das tabelas
        if args.tabelas:
            tabelas = [tabela.strip() for tabela in args.tabelas.split(',')]
            for tabela in tabelas:
                logger.info(f"Processando tabela: '{tabela}'")
                if tabela in config.sections():
                    try:
                        criar_ou_atualizar_tabela(spark, tabela, config, args.apenas_arquivos, args.formato_arquivo)
                    except Exception as e:
                        logger.error(f"Erro ao processar a tabela '{tabela}': {str(e)}")
                else:
                    logger.warning(f"Configuração não encontrada para a tabela '{tabela}'")
        else:
            logger.error("Nenhuma tabela especificada. Use o argumento --tabelas.")

    except Exception as e:
        logger.error(f"Erro na execução principal: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    # Configuração do parser de argumentos
    parser = argparse.ArgumentParser(description='Processamento de tabelas')
    parser.add_argument('--tabelas', type=str, help='Nomes das tabelas separados por vírgula')
    parser.add_argument("--onlyfiles", action="store_true", help="Criar apenas arquivos sem criar tabelas Hive")
    parser.add_argument("--formato", choices=['parquet', 'orc', 'csv'], default='parquet', help="Formato dos arquivos a serem criados")
    args = parser.parse_args()

    logger.info(f"Iniciando processamento para as tabelas: {', '.join(args.tabelas)}")
    main(args.tabelas, args.onlyfiles, args.formato)
