# Databricks notebook source
application_id = dbutils.secrets.get(scope="adls_to_databricks_new01", key="app-id")
tenant_id = dbutils.secrets.get(scope="adls_to_databricks_new01", key="dir-id")
secret = dbutils.secrets.get(scope="adls_to_databricks_new01", key="secret-value")

container_name = "streaming-demo"
account_name = "00x00dlorders"
mount_point = "/mnt/streaming-demo"

configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": application_id,
    "fs.azure.account.oauth2.client.secret": secret,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
}

dbutils.fs.mount(
    source=f"abfss://{container_name}@{account_name}.dfs.core.windows.net/",
    mount_point=mount_point,
    extra_configs=configs,
)

# COMMAND ----------


