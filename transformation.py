from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand

# Initialize Spark session
spark = SparkSession.builder.appName("Transformation").getOrCreate()

# Load the tables
clientes = spark.sql("SELECT id_usuario FROM clientes")
transacoes_cartao = spark.sql("SELECT * FROM transacoes_cartao")

# Count the number of records in each table
clientes_count = clientes.count()
transacoes_count = transacoes_cartao.count()

# Create a temporary view of clientes with repeated and randomized id_usuario
if clientes_count < transacoes_count:
    repetition_factor = (transacoes_count // clientes_count) + 1
    clientes_repeated = clientes.withColumn("repeat", rand()).repartition("repeat")
    clientes_repeated = clientes_repeated.selectExpr("id_usuario").rdd.flatMap(lambda x: [x] * repetition_factor).toDF(["id_usuario"])
else:
    clientes_repeated = clientes

clientes_repeated.createOrReplaceTempView("clientes_repeated")

# Update transacoes_cartao with id_usuario from clientes
updated_transacoes = spark.sql("""
    SELECT t.*, c.id_usuario
    FROM transacoes_cartao t
    JOIN clientes_repeated c
    ON RAND() < 1.0 / (SELECT COUNT(*) FROM clientes_repeated)
""")

# Overwrite the existing transacoes_cartao table with the updated data
updated_transacoes.write.mode("overwrite").saveAsTable("transacoes_cartao")

print("Transformation completed successfully.")
