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
landing_container_name = "landing-zone"
bronze_container_name = "bronze"
silver_container_name = "silver"
gold_container_name='gold'

# COMMAND ----------

# MAGIC %md
# MAGIC ##### common function to mount containers

# COMMAND ----------

def mount_container(mount_path,container_name,storage_account_name):
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
        source=f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net",
        mount_point=mount_path,
        extra_configs=configs)
      print(f'{mount_path} mounted successfully.')  
    else:
      print('Mount already created')

# COMMAND ----------

# MAGIC %md
# MAGIC #### ADB Mount Paths

# COMMAND ----------

# Mount landing container
landing_mount_point = f"/mnt/{landing_container_name}"
mount_container(landing_mount_point,landing_container_name,storage_account_name)

# Mount bronze container
bronze_mount_point = f"/mnt/{bronze_container_name}"
mount_container(bronze_mount_point,bronze_container_name,storage_account_name)

# Mount silver container
silver_mount_point = f"/mnt/{silver_container_name}"
mount_container(silver_mount_point,silver_container_name,storage_account_name)

# Mount gold container
gold_mount_point = f"/mnt/{gold_container_name}"
mount_container(gold_mount_point,gold_container_name,storage_account_name)

# COMMAND ----------

# To unmount the container, uncomment the following line
#dbutils.fs.unmount(watermark_mount_point)

# COMMAND ----------

display(dbutils.fs.mounts())


# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/
