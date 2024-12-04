# -*- coding: utf-8 -*-
import sys
import configparser
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
import json
from utils import gerar_dados

def carregar_configuracao():
    config = configparser.ConfigParser()
    config.read('config.ini')
    return config

def criar_tabela(spark, nome_tabela, config):
    # Carregar esquema da tabela
    with open(f'{nome_tabela}.json', 'r') as f:
        esquema = StructType.fromJson(json.load(f))

    # Obter número de registros da configuração
    num_records = config.getint(nome_tabela, 'num_records')

    # Gerar dados
    dados = gerar_dados(nome_tabela, num_records)

    # Criar DataFrame
    df = spark.createDataFrame(dados, schema=esquema)

    # Criar e salvar tabela Hive
    df.write.mode("overwrite").saveAsTable(nome_tabela)

    print(f"Tabela {nome_tabela} criada com {num_records} registros.")

def main(tabelas):
    config = carregar_configuracao()

    # Inicializar Spark Session
    spark = SparkSession.builder \
        .appName("SimulacaoDadosBancarios") \
        .enableHiveSupport() \
        .getOrCreate()

    # Criar banco de dados
    database_name = config['DEFAULT']['database_name']
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
    spark.sql(f"USE {database_name}")

    # Criar tabelas especificadas
    for tabela in tabelas:
        if tabela in config.sections():
            criar_tabela(spark, tabela, config)
        else:
            print(f"Aviso: Configuração não encontrada para a tabela {tabela}")

    # Encerrar a sessão Spark
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python main.py <nome_tabela1> <nome_tabela2> ...")
        sys.exit(1)
    
    tabelas = sys.argv[1:]
    main(tabelas)
