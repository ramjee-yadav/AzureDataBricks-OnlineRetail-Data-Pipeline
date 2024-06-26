# Databricks notebook source
#%run ./mount_adls_container

# COMMAND ----------

dbutils.notebook.run("./mount_adls_container", timeout_seconds=600)

# COMMAND ----------

# MAGIC %md
# MAGIC read the input files from landing zone

# COMMAND ----------

def read_file_from_landing_zone():  
  from pyspark.sql.types import StringType,IntegerType,DoubleType,DateType
  #from pyspark.sql.functions import current_timestamp,inpute_file_name
  from pyspark.sql import functions as F
  invoice_schema = StructType(fields=[StructField("InvoiceNo", IntegerType()),
  StructField("StockCode", StringType()),
  StructField("Description", StringType()),
  StructField("Quantity", IntegerType()),
  StructField("InvoiceDate", DateType()),
  StructField("UnitPrice", DoubleType()),
  StructField("CustomerID", IntegerType()), 
  StructField("Country", StringType())
  ])
  
  print("Reading the raw invoice data: ",end='')
  df_read = spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "csv")
  .option("header", "true")
  .option("cloudFiles.includeExistingFiles", "true") // to include pre-existing files in the mount path
  .Schema(invoice_schema)
  .load(landing_mount_point)
  .withColumn("LoadTime",F.current_timestamp)
  .withColumn("SourceFile",F.inpute_file_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ingest data to bronze

# COMMAND ----------

def ingest_raw_file():
    df_read.writeStream \
    #.trigger(availableNow=True)
    .trigger(once=True)   
    .format("delta") \
    .option("checkpointLocation", checkpoint_mount_point+"/sales_invoices") \
    .outputMode('append') \   
    .totable('bronze.sales_invoices')
