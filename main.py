# -*- coding: utf-8 -*-
import sys
import configparser
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import current_date
import json
import logging
import os

# Configurar logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
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
        existe = spark.sql(f"SHOW TABLES LIKE '{nome_tabela}'").count() > 0
        logger.info(f"Verificação de existência da tabela '{nome_tabela}': {'Existe' if existe else 'Não existe'}")
        return existe
    except Exception as e:
        logger.error(f"Erro ao verificar existência da tabela '{nome_tabela}': {str(e)}")
        raise

def criar_ou_atualizar_tabela(spark, nome_tabela, config):
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

        if not tabela_existe(spark, nome_tabela):
            if particionamento:
                df.write.partitionBy("data_execucao").saveAsTable(nome_tabela)
                logger.info(f"Tabela '{nome_tabela}' criada com particionamento por data_execucao")
            elif bucketing:
                df.write.bucketBy(num_buckets, "id_usuario").saveAsTable(nome_tabela)
                logger.info(f"Tabela '{nome_tabela}' criada com bucketing por id_usuario em {num_buckets} buckets")
            else:
                df.write.saveAsTable(nome_tabela)
                logger.info(f"Tabela '{nome_tabela}' criada sem particionamento ou bucketing")
        else:
            if particionamento:
                df.write.mode("append").partitionBy("data_execucao").saveAsTable(nome_tabela)
                logger.info(f"Dados adicionados à tabela '{nome_tabela}' com particionamento por data_execucao")
            elif bucketing:
                df.write.mode("append").bucketBy(num_buckets, "id_usuario").saveAsTable(nome_tabela)
                logger.info(f"Dados adicionados à tabela '{nome_tabela}' com bucketing por id_usuario em {num_buckets} buckets")
            else:
                df.write.mode("append").saveAsTable(nome_tabela)
                logger.info(f"Dados adicionados à tabela '{nome_tabela}' sem particionamento ou bucketing")

    except Exception as e:
        logger.error(f"Erro ao criar ou atualizar tabela '{nome_tabela}': {str(e)}")
        raise

def main(tabelas):
    try:
        config_path = '/app/mount/config.ini'
        config = carregar_configuracao(config_path)
        erro_encontrado = False

        logger.debug(f"Tabelas recebidas como argumento: {tabelas}")
        logger.debug(f"Tipo de 'tabelas': {type(tabelas)}")

        spark = SparkSession.builder \
            .appName("SimulacaoDadosBancarios") \
            .enableHiveSupport() \
            .getOrCreate()
        logger.info("Sessão Spark iniciada com sucesso.")

        database_name = config['DEFAULT'].get('database_name', 'bancodemo')
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
        spark.sql(f"USE {database_name}")
        logger.info(f"Usando banco de dados: {database_name}")

        for tabela in tabelas:
            logger.debug(f"Processando tabela: '{tabela}'")
            if tabela in config.sections():
                try:
                    criar_ou_atualizar_tabela(spark, tabela, config)
                except Exception as e:
                    logger.error(f"Erro ao processar a tabela '{tabela}': {str(e)}")
                    erro_encontrado = True
            else:
                logger.warning(f"Configuração não encontrada para a tabela '{tabela}'")
                logger.debug(f"Seções disponíveis na configuração: {config.sections()}")
                erro_encontrado = True

        spark.stop()
        logger.info("Sessão Spark encerrada.")

        if erro_encontrado:
            logger.error("Execução concluída com erros ou avisos.")
            sys.exit(1)
        else:
            logger.info("Execução concluída com sucesso.")

    except Exception as e:
        logger.error(f"Erro na execução principal: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    logger.debug(f"Argumentos recebidos: {sys.argv}")
    if len(sys.argv) < 2:
        logger.error("Uso: python main.py <nome_tabela1> <nome_tabela2> ...")
        sys.exit(1)
    
    tabelas = sys.argv[1].split()
    logger.info(f"Iniciando processamento para as tabelas: {', '.join(tabelas)}")
    logger.debug(f"Lista de tabelas a serem processadas: {tabelas}")
    main(tabelas)
