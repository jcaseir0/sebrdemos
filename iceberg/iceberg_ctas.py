# Work in progress
from pyspark.sql import SparkSession
from datetime import datetime
import sys
import configparser

print("#-------------------------------------------------------------")
print("#  VARIABLES DEFINITION FROM EXTERNAL FILE - parameters.conf  ")
print("#-------------------------------------------------------------\n")
config = configparser.ConfigParser()
config.read("/app/mount/parameters.conf")
username=config.get("general","username")
current_datetime = datetime.now().strftime("%Y%m%d%H")
partdate = current_datetime
partition = "frameworkdate=" + partdate

print("Running as Username: {}\n".format(username))

#---------------------------------------------------
#               CREATE SPARK SESSION
#---------------------------------------------------
spark = SparkSession \
    .builder \
    .appName("ICEBERG LOAD") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")\
    .config("spark.sql.catalog.spark_catalog.type", "hive")\
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")\
    .getOrCreate()

print("CUSTOMER TABLE PRE-ICEBERG MIGRATION PARTITIONS: \n")
print("\tSHOW PARTITIONS {}_CUSTOMER.INFORMATION\n".format(username))
spark.sql("SHOW PARTITIONS {}_CUSTOMER.INFORMATION".format(username)).show(truncate=False)

table_name = 'information_iceberg'
db_name = "{}_CUSTOMER".format(username)
tblLst = spark.catalog.listTables(db_name)
table_names_in_db = [table.name for table in tblLst]
table_exists = table_name in table_names_in_db

if table_exists == True :
    print("\nInfo table exists: ", table_exists, "\n")
    rcbi = spark.sql("SELECT COUNT(*) FROM spark_catalog.{}_CUSTOMER.INFORMATION_ICEBERG".format(username)).collect()[0][0]
    lastpart = spark.sql("SELECT * FROM spark_catalog.{}_CUSTOMER.INFORMATION_ICEBERG.PARTITIONS".format(username)).collect()[-1][0]
    # Partition validation
    part = "Row(frameworkdate='" + partdate + "')"
    if str(lastpart) != part :
        print("##---------------------------------------------------")
        print("##   NEW LOAD - PARTITION: ", partition, "           ")
        print("##---------------------------------------------------\n")
        spark.sql("INSERT INTO {0}_CUSTOMER.INFO_ICEBERG SELECT * FROM {0}_CUSTOMER.INFORMATION WHERE {1}".format(username, partition))
    else :
        print("\nPartition ", partition, " already loaded!\n")
        sys.exit(1)
else:
    print("#----------------------------------------------------")
    print("#    CTAS MIGRATION SPARK TABLES TO ICEBERG TABLE    ")
    print("#----------------------------------------------------\n")
    print("\tALTER TABLE {}_CUSTOMER.INFORMATION UNSET TBLPROPERTIES ('TRANSLATED_TO_EXTERNAL')\n".format(username))
    spark.sql("ALTER TABLE {}_CUSTOMER.INFORMATION UNSET TBLPROPERTIES ('TRANSLATED_TO_EXTERNAL')".format(username))
    print("\tCREATE TABLE spark_catalog.{0}_CUSTOMER.INFORMATION_ICEBERG USING iceberg PARTITIONED BY (frameworkdate) AS SELECT * FROM {0}_CUSTOMER.INFORMATION\n".format(username))
    spark.sql("CREATE TABLE spark_catalog.{0}_CUSTOMER.INFORMATION_ICEBERG USING iceberg PARTITIONED BY (frameworkdate) AS SELECT * FROM {0}_CUSTOMER.INFORMATION".format(username))
    print("\tNew Iceberg Format for Customer Table.\n")
    
print("DESCRIBE TABLE spark_catalog.{}_CUSTOMER.INFORMATION_ICEBERG".format(username))
spark.sql("DESCRIBE TABLE spark_catalog.{}_CUSTOMER.INFORMATION_ICEBERG".format(username)).show(20, False)
print("\n")
print("CUSTOMER TABLE POST-ICEBERG MIGRATION PARTITIONS: \n")
spark.sql("SELECT * FROM spark_catalog.{}_CUSTOMER.INFORMATION_ICEBERG.PARTITIONS".format(username)).show()

print("#---------------------------------------------------")
print("#            SHOW ICEBERG TABLE SNAPSHOTS           ")
print("#---------------------------------------------------")
print("\n")
spark.sql("SELECT * FROM spark_catalog.{}_CUSTOMER.INFORMATION_ICEBERG.history".format(username)).show(20, False)
spark.sql("SELECT * FROM spark_catalog.{}_CUSTOMER.INFORMATION_ICEBERG.snapshots".format(username)).show(20, False)

# STORE TIMESTAMP BEFORE INSERTS
now = datetime.now()
timestamp = datetime.timestamp(now)
print("PRE-INSERT TIMESTAMP: ", timestamp)
print("\n")
print("#---------------------------------------------------")
print("#               INSERT DATA                         ")
print("#---------------------------------------------------")
print("\n")
# PRE-INSERT COUNT
print("PRE-INSERT COUNT")
spark.sql("SELECT COUNT(*) FROM spark_catalog.{}_CUSTOMER.INFORMATION_ICEBERG".format(username)).show()
print("\n")
print("#---------------------------------------------------")
print("#        INSERT DATA VIA DATAFRAME API              ")
print("#---------------------------------------------------")
temp_df = spark.sql("SELECT * FROM spark_catalog.{}_CUSTOMER.INFORMATION_ICEBERG".format(username)).sample(fraction=0.3, seed=3)
temp_df.writeTo("spark_catalog.{}_CUSTOMER.INFORMATION_ICEBERG".format(username)).append()
print("\n")
# INSERT DATA VIA SQL
print("#---------------------------------------------------")
print("#        INSERT DATA VIA SPARK SQL                  ")
print("#---------------------------------------------------")
temp_df.createOrReplaceTempView("CUSTOMER_SAMPLE".format(username))
insert_qry = "INSERT INTO spark_catalog.{0}_CUSTOMER.INFORMATION_ICEBERG SELECT * FROM CUSTOMER_SAMPLE".format(username)
print(insert_qry)
spark.sql(insert_qry)
print("\n")
print("#---------------------------------------------------")
print("#               TIME TRAVEL                         ")
print("#---------------------------------------------------")

# NOTICE SNAPSHOTS HAVE BEEN ADDED
spark.sql("SELECT * FROM spark_catalog.{}_CUSTOMER.INFORMATION_ICEBERG.history".format(username)).show(20, False)
spark.sql("SELECT * FROM spark_catalog.{}_CUSTOMER.INFORMATION_ICEBERG.snapshots".format(username)).show(20, False)

# POST-INSERT COUNT
print("\n")
print("POST-INSERT COUNT")
spark.sql("SELECT COUNT(*) FROM spark_catalog.{}_CUSTOMER.INFORMATION_ICEBERG".format(username)).show()

# TIME TRAVEL AS OF PREVIOUS TIMESTAMP
df = spark.read.option("as-of-timestamp", int(timestamp*1000)).format("iceberg").load("spark_catalog.{}_CUSTOMER.INFORMATION_ICEBERG".format(username))

# POST TIME TRAVEL COUNT
print("\n")
print("POST-TIME TRAVEL COUNT")
print(df.count())

print("#---------------------------------------------------")
print("#               INCREMENTAL READ                    ")
print("#---------------------------------------------------")

print("\n")
print("INCREMENTAL READ")
print("\n")
print("ICEBERG TABLE HISTORY (SHOWS EACH SNAPSHOT AND TIMESTAMP)")
print("SELECT * FROM {}_CUSTOMER.INFORMATION_ICEBERG.history;".format(username))
spark.sql("SELECT * FROM {}_CUSTOMER.INFORMATION_ICEBERG.history;".format(username)).show()
print("\n")
print("ICEBERG TABLE SNAPSHOTS (USEFUL FOR INCREMENTAL QUERIES AND TIME TRAVEL)")
print("SELECT * FROM {}_CUSTOMER.INFORMATION_ICEBERG.snapshots;".format(username))
spark.sql("SELECT * FROM {}_CUSTOMER.INFORMATION_ICEBERG.snapshots;".format(username)).show()
print("\n")
print("STORE FIRST AND LAST SNAPSHOT ID'S FROM SNAPSHOTS TABLE")
print("SELECT * FROM {}_CUSTOMER.INFORMATION_ICEBERG.snapshots;".format(username))
snapshots_df = spark.sql("SELECT * FROM {}_CUSTOMER.INFORMATION_ICEBERG.snapshots;".format(username))
snapshots_df.show()

last_snapshot = snapshots_df.select("snapshot_id").tail(1)[0][0]
first_snapshot = snapshots_df.select("snapshot_id").head(1)[0][0]
print("\n")
print("READ BETWEEN FIRST SNAPSHOP: {0} AND LAST SNAPSHOP: {1}".format(first_snapshot, last_snapshot))
spark.read\
    .format("iceberg")\
    .option("start-snapshot-id", first_snapshot)\
    .option("end-snapshot-id", last_snapshot)\
    .load("spark_catalog.{}_CUSTOMER.INFORMATION_ICEBERG".format(username)).show()