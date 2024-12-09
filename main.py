# -*- coding: utf-8 -*-
import sys
import configparser
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import current_date
import json
import logging
import os
import argparse

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
            if particionamento:
                df.write.partitionBy("data_execucao").format(formato_arquivo).save(output_path, mode="overwrite")
                logger.info(f"Arquivos {formato_arquivo.upper()} para '{nome_tabela}' criados com particionamento por data_execucao em {output_path}")
            elif bucketing:
                df.write.bucketBy(num_buckets, "id_uf").format(formato_arquivo).save(output_path, mode="overwrite")
                logger.info(f"Arquivos {formato_arquivo.upper()} para '{nome_tabela}' criados com bucketing por id_uf em {num_buckets} buckets em {output_path}")
            else:
                df.write.format(formato_arquivo).save(output_path, mode="overwrite")
                logger.info(f"Arquivos {formato_arquivo.upper()} para '{nome_tabela}' criados sem particionamento ou bucketing em {output_path}")
        else:
            if not tabela_existe(spark, nome_tabela):
                write_mode = "overwrite"
            else:
                write_mode = "append"
                spark.sql(f"REFRESH TABLE {nome_tabela}")

            if particionamento:
                df.write.mode(write_mode).partitionBy("data_execucao").format("parquet").saveAsTable(nome_tabela)
                logger.info(f"Tabela '{nome_tabela}' {write_mode} com particionamento por data_execucao")
            elif bucketing:
                df.write.mode(write_mode).bucketBy(num_buckets, "id_uf").format("parquet").saveAsTable(nome_tabela)
                logger.info(f"Tabela '{nome_tabela}' {write_mode} com bucketing por id_uf em {num_buckets} buckets")
            else:
                df.write.mode(write_mode).format("parquet").saveAsTable(nome_tabela)
                logger.info(f"Tabela '{nome_tabela}' {write_mode} sem particionamento ou bucketing")

    except Exception as e:
        logger.error(f"Erro ao criar ou atualizar tabela '{nome_tabela}': {str(e)}")
        raise

def main(tabelas, apenas_arquivos, formato_arquivo):
    try:
        config_path = '/app/mount/config.ini'
        config = carregar_configuracao(config_path)
        erro_encontrado = False

        spark = SparkSession.builder \
            .appName("SimulacaoDadosBancarios") \
            .enableHiveSupport() \
            .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
            .config("hive.metastore.uris", "thrift://localhost:9083") \
            .config("hive.metastore.warehouse.dir", "/user/hive/warehouse") \
            .config("spark.sql.hive.convertMetastoreParquet", "false") \
            .getOrCreate()
        logger.info("Sessão Spark iniciada com sucesso.")

        if not apenas_arquivos:
            database_name = config['DEFAULT'].get('database_name', 'bancodemo')
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
            spark.sql(f"USE {database_name}")
            logger.info(f"Usando banco de dados: {database_name}")

        for tabela in tabelas:
            logger.info(f"Processando tabela: '{tabela}'")
            if tabela in config.sections():
                try:
                    criar_ou_atualizar_tabela(spark, tabela, config, apenas_arquivos, formato_arquivo)
                except Exception as e:
                    logger.error(f"Erro ao processar a tabela '{tabela}': {str(e)}")
                    erro_encontrado = True
            else:
                logger.warning(f"Configuração não encontrada para a tabela '{tabela}'")
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
    parser = argparse.ArgumentParser(description="Processador de dados bancários")
    parser.add_argument("tabelas", nargs="+", help="Nomes das tabelas a serem processadas")
    parser.add_argument("--onlyfiles", action="store_true", help="Criar apenas arquivos sem criar tabelas Hive")
    parser.add_argument("--formato", choices=['parquet', 'orc', 'csv'], default='parquet', help="Formato dos arquivos a serem criados")
    args = parser.parse_args()

    logger.info(f"Iniciando processamento para as tabelas: {', '.join(args.tabelas)}")
    main(args.tabelas, args.onlyfiles, args.formato)
