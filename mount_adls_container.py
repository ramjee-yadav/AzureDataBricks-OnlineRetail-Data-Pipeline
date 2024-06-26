# Databricks notebook source
# MAGIC %md
# MAGIC ##### Get Secretes of service app (access to Azure ADLS granted to it)

# COMMAND ----------

# List all the secret scopes
dbutils.secrets.listScopes()

# COMMAND ----------

adb_secrete_scope_name='ramg-de-scope'
tenant_id=dbutils.secrets.get(scope = adb_secrete_scope_name, key = 'ramg-adb-app-tenant-id')
client_id=dbutils.secrets.get(scope = adb_secrete_scope_name, key = 'ramg-adb-app-client-id')
client_secret=dbutils.secrets.get(scope = adb_secrete_scope_name, key = 'ramg-adb-app-client-secret')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Mount ADLS to ADB

# COMMAND ----------

storage_account_name = "onlineretaildataramg"
conatiner_name = "onlineretail-lakehouse"
landing_directory_name = "landing-zone"
bronze_directory_name = "bronze"
silver_directory_name = "silver"
gold_directory_name='gold'
checkpoint_directory_name = "checkpoints"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### common function to mount containers

# COMMAND ----------

def mount_container(storage_account_name,container_name,directory_name,mount_path):
    # Check if mount point exists
    if not any(mount.mountPoint == mount_path for mount in dbutils.fs.mounts()):
        # Mount the container
      configs = {"fs.azure.account.auth.type": "OAuth",
                "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                "fs.azure.account.oauth2.client.id": client_id,
                "fs.azure.account.oauth2.client.secret": client_secret,
                "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
      print('Configs initialized')
      dbutils.fs.mount(
        source=f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{directory_name}",
        mount_point=mount_path,
        extra_configs=configs)
      print(f'{mount_path} mounted successfully.')  
    else:
      print('Mount already created')

# COMMAND ----------

# MAGIC %md
# MAGIC #### ADB Mount Paths

# COMMAND ----------

# Mount landing 
landing_mount_point = f"/mnt/{container_name}/{landing_directory_name}"
mount_container(storage_account_name,container_name,landing_directory_name,landing_mount_point)

# Mount bronze 
bronze_mount_point = f"/mnt/{container_name}/{bronze_directory_name}"
mount_container(storage_account_name,container_name,bronze_directory_name,bronze_mount_point)

# Mount silver 
silver_mount_point = f"/mnt/{container_name}/{silver_directory_name}"
mount_container(storage_account_name,container_name,silver_directory_name,silver_mount_point)

# Mount gold 
gold_mount_point = f"/mnt/{container_name}/{gold_directory_name}"
mount_container(storage_account_name,container_name,gold_directory_name,gold_mount_point)

# Mount checkpoint
checkpoint_mount_point = f"/mnt/{container_name}/{checkpoint_directory_name}"
mount_container(storage_account_name,container_name,checkpoint_directory_name,checkpoint_mount_point)

# COMMAND ----------

# To unmount the container, uncomment the following line
#dbutils.fs.unmount(watermark_mount_point)

# COMMAND ----------

display(dbutils.fs.mounts())


# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/
