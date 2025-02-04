# Work in progress
# STORE TIMESTAMP BEFORE INSERTS
print("#-----------------------------------------------------------------------")
print("#  SHOW THAT ICEBERG TABLE SNAPSHOTS ARE CREATED AT EVERY MODIFICATION  ")
print("#-----------------------------------------------------------------------\n")
now = datetime.now()
timestamp = datetime.timestamp(now)
print("PRE-INSERT TIMESTAMP:", timestamp)
print("\n#---------------------------------------------------")
print("#               INSERT DATA                         ")
print("#---------------------------------------------------")
# PRE-INSERT COUNT
print("PRE-INSERT COUNT")
spark.sql("SELECT COUNT(*) FROM spark_catalog.{}_CUSTOMER.INFO".format(username)).show()
print("#---------------------------------------------------")
print("#        INSERT DATA VIA DATAFRAME API              ")
print("#---------------------------------------------------\n")
print("GENERATING A DATA SET WITH 30% FROM ORIGINAL TABLE\n")
temp_df = spark.sql("SELECT * FROM spark_catalog.{}_CUSTOMER.INFO".format(username)).sample(fraction=0.3, seed=3)
print("APPENDING NEW LOAD TO TEMP DATAFRAME\n")
temp_df.writeTo("spark_catalog.{}_CUSTOMER.INFO".format(username)).append()
# INSERT DATA VIA SQL
print("#---------------------------------------------------")
print("#        INSERT DATA VIA SPARK SQL                  ")
print("#---------------------------------------------------\n")
temp_df.createOrReplaceTempView("CUSTOMER_SAMPLE".format(username))
insert_qry = "INSERT INTO spark_catalog.{0}_CUSTOMER.INFO SELECT * FROM CUSTOMER_SAMPLE".format(username)
print(insert_qry)
spark.sql(insert_qry)
print("\n")
print("#---------------------------------------------------")
print("#               TIME TRAVEL                         ")
print("#---------------------------------------------------\n")

# NOTICE SNAPSHOTS HAVE BEEN ADDED
print("SELECT * FROM spark_catalog.{}_CUSTOMER.INFO.history".format(username))
spark.sql("SELECT * FROM spark_catalog.{}_CUSTOMER.INFO.history".format(username)).show(20, False)
print("SELECT * FROM spark_catalog.{}_CUSTOMER.INFO.snapshots\n".format(username))
spark.sql("SELECT * FROM spark_catalog.{}_CUSTOMER.INFO.snapshots".format(username)).show(20, False)

# POST-INSERT COUNT
print("POST-INSERT COUNT")
spark.sql("SELECT COUNT(*) FROM spark_catalog.{}_CUSTOMER.INFO".format(username)).show()

# TIME TRAVEL AS OF PREVIOUS TIMESTAMP
print("TIME TRAVEL AS OF PREVIOUS TIMESTAMP\n")
df = spark.read.option("as-of-timestamp", int(timestamp*1000)).format("iceberg").load("spark_catalog.{}_CUSTOMER.INFO".format(username))

# POST TIME TRAVEL COUNT
print("POST-TIME TRAVEL COUNT")
print(df.count())
print("#---------------------------------------------------")
print("#               INCREMENTAL READ                    ")
print("#---------------------------------------------------\n")
print("INCREMENTAL READ")
print("ICEBERG TABLE HISTORY (SHOWS EACH SNAPSHOT AND TIMESTAMP)")
print("SELECT * FROM {}_CUSTOMER.INFO.history\n".format(username))
spark.sql("SELECT * FROM {}_CUSTOMER.INFO.history".format(username)).show(truncate=False)
print("ICEBERG TABLE SNAPSHOTS (USEFUL FOR INCREMENTAL QUERIES AND TIME TRAVEL)")
print("SELECT * FROM {}_CUSTOMER.INFO.snapshots\n".format(username))
spark.sql("SELECT * FROM {}_CUSTOMER.INFO.snapshots".format(username)).show(truncate=False)
print("STORE FIRST AND LAST SNAPSHOT ID'S FROM SNAPSHOTS TABLE")
print("SELECT * FROM {}_CUSTOMER.INFO.snapshots\n".format(username))

snapshots_df = spark.sql("SELECT * FROM {}_CUSTOMER.INFO.snapshots".format(username))
snapshots_df.show(truncate=False)
last_snapshot = snapshots_df.select("snapshot_id").tail(1)[0][0]
first_snapshot = snapshots_df.select("snapshot_id").head(1)[0][0]

print("READ BETWEEN FIRST SNAPSHOT: {0} AND LAST SNAPSHOT: {1}".format(first_snapshot, last_snapshot))
print("\n")
spark.read\
    .format("iceberg")\
    .option("start-snapshot-id", first_snapshot)\
    .option("end-snapshot-id", last_snapshot)\
    .load("spark_catalog.{}_CUSTOMER.INFO".format(username)).show()

print("\n")
print("#---------------------------------------------------")
print("#       RESTORE TABLE FROM ICEBERG MIGRATION        ")
print("#---------------------------------------------------\n")
print("CHANGE TABLE NAME FROM ORIGINAL BACKED UP TABLE BEFORE MIGRATION:\n")
print("ALTER TABLE {0}_CUSTOMER.INFO_BACKUP_ RENAME TO {0}_CUSTOMER.ORIGINAL_INFO\n".format(username))
spark.sql("ALTER TABLE {0}_CUSTOMER.INFO_BACKUP_ RENAME TO {0}_CUSTOMER.ORIGINAL_INFO".format(username))
print("SELECT * FROM {0}_CUSTOMER.ORIGINAL_INFO".format(username))
spark.sql("SELECT * FROM {0}_CUSTOMER.ORIGINAL_INFO".format(username)).show()

print("#---------------------------------------------------")
print("#       CLEANING PROCESS - TABLE INFO               ")
print("#---------------------------------------------------\n")
print("DROP TABLE {0}_CUSTOMER.ORIGINAL_INFO PURGE".format(username))
spark.sql("DROP TABLE {0}_CUSTOMER.ORIGINAL_INFO PURGE".format(username))
print("DROP TABLE {0}_CUSTOMER.INFO PURGE".format(username))
spark.sql("DROP TABLE {0}_CUSTOMER.INFO PURGE".format(username))
# Azure cloud porta > Storage accounts > jcaseiro02stor19fa30fc > Access Keys > key1 - Connection string = conn_str
dirClient = DataLakeDirectoryClient.from_connection_string(conn_str=conn_str, file_system_name=container, directory_name=dir_path)
dirClient.delete_directory()
print(ADLSPath + dir_path + " directory deleted!\n")