# Databricks notebook source
application_id = "********application_id**************"
tenant_id = "***********tenant_id****************"
secret = "***************secret*******************"

# COMMAND ----------

configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": "********application_id**************",
    "fs.azure.account.oauth2.client.secret": "***************secret*******************",
    "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/***********tenant_id****************/oauth2/token",
}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
    source="abfss://bronze@00x00dlorders.dfs.core.windows.net/",
    mount_point="/mnt/bronze",
    extra_configs=configs,
)

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/

# COMMAND ----------

dbutils.fs.ls("/")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/bronze"))

# COMMAND ----------

spark.read.csv("dbfs:/mnt/bronze/countries.csv", header=True).display()

# COMMAND ----------

# To unmount the container
# dbutils.fs.unmount("/mnt/bronze")

# COMMAND ----------


