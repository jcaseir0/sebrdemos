# -*- coding: utf-8 -*-
import sys
import configparser
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import current_date
import json
from utils import gerar_dados
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def carregar_configuracao():
    try:
        config = configparser.ConfigParser()
        config.read('config.ini')
        logger.info("Configuração carregada com sucesso.")
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
        # Carregar esquema da tabela
        with open(f'{nome_tabela}.json', 'r') as f:
            esquema = StructType.fromJson(json.load(f))
        logger.info(f"Esquema da tabela '{nome_tabela}' carregado com sucesso.")

        # Obter configurações
        num_records = config.getint(nome_tabela, 'num_records')
        particionamento = config.getboolean(nome_tabela, 'particionamento')
        bucketing = config.getboolean(nome_tabela, 'bucketing')
        num_buckets = config.getint(nome_tabela, 'num_buckets')
        logger.info(f"Configurações para '{nome_tabela}': num_records={num_records}, particionamento={particionamento}, bucketing={bucketing}, num_buckets={num_buckets}")

        # Gerar dados
        dados = gerar_dados(nome_tabela, num_records)
        df = spark.createDataFrame(dados, schema=esquema)
        df = df.withColumn("data_execucao", current_date())
        logger.info(f"Dados gerados para '{nome_tabela}': {num_records} registros")

        if not tabela_existe(spark, nome_tabela):
            # Criar nova tabela
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
            # Atualizar tabela existente
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
        config = carregar_configuracao()

        # Inicializar Spark Session
        spark = SparkSession.builder \
            .appName("SimulacaoDadosBancarios") \
            .enableHiveSupport() \
            .getOrCreate()
        logger.info("Sessão Spark iniciada com sucesso.")

        # Criar banco de dados
        database_name = config['default']['database_name']
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
        spark.sql(f"USE {database_name}")
        logger.info(f"Usando banco de dados: {database_name}")

        # Criar ou atualizar tabelas especificadas
        for tabela in tabelas:
            if tabela in config.sections():
                criar_ou_atualizar_tabela(spark, tabela, config)
            else:
                logger.warning(f"Configuração não encontrada para a tabela '{tabela}'")

        # Encerrar a sessão Spark
        spark.stop()
        logger.info("Sessão Spark encerrada.")

    except Exception as e:
        logger.error(f"Erro na execução principal: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        logger.error("Uso: python main.py <nome_tabela1> <nome_tabela2> ...")
        sys.exit(1)
    
    tabelas = sys.argv[1:]
    logger.info(f"Iniciando processamento para as tabelas: {', '.join(tabelas)}")
    main(tabelas)
