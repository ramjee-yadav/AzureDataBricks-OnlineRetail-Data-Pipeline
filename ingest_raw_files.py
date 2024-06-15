# Databricks notebook source
#%run ./mount_adls_container

# COMMAND ----------

dbutils.notebook.run("./mount_adls_container", timeout_seconds=600)

# COMMAND ----------

checkpoint_path=f"{landing_mount_point}/CheckPoint"

# COMMAND ----------

# MAGIC %md
# MAGIC read the input files from landing zone

# COMMAND ----------

df_read = spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "csv")
  .option("cloudFiles.includeExistingFiles", "true") // to include pre-existing files in the mount path
  .option("inferSchema", "true")
  .load(landing_mount_point) // replace with your mount path

# COMMAND ----------

df_read.writeStream \
    .format("delta") \
    .option("checkpointLocation", checkpoint_path) \
    .start(bronze_mount_path)
