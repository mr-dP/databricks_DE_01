# Databricks notebook source
application_id = dbutils.secrets.get(scope="adls_to_databricks_new01", key="app-id")
tenant_id = dbutils.secrets.get(scope="adls_to_databricks_new01", key="dir-id")
secret = dbutils.secrets.get(scope="adls_to_databricks_new01", key="secret-value")

# COMMAND ----------

container_name = "health-updates"
account_name = "00x00dlorders"
mount_point = "/mnt/health-updates"

# COMMAND ----------

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

from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DateType,
    StringType,
    DoubleType,
)

# COMMAND ----------

health_schema = StructType(
    [
        StructField("STATUS_UPDATE_ID", IntegerType(), False),
        StructField("PATIENT_ID", IntegerType(), False),
        StructField("DATE_PROVIDED", StringType(), False),
        StructField("FEELING_TODAY", StringType(), True),
        StructField("IMPACT", StringType(), True),
        StructField("INJECTION_SITE_SYMPTOMS", StringType(), True),
        StructField("HIGHEST_TEMP", DoubleType(), True),
        StructField("FEVERISH_TODAY", StringType(), True),
        StructField("GENERAL_SYMPTOMS", StringType(), True),
        StructField("HEALTHCARE_VISIT", StringType(), True),
    ]
)

# COMMAND ----------

health_df = (
    spark.read.schema(health_schema)
    .format("csv")
    .option("header", "true")
    .load("dbfs:/mnt/health-updates/bronze/health_status_updates.csv")
)

# COMMAND ----------

display(health_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_date

health_data = health_df.withColumn(
    "DATE_PROVIDED", to_date("DATE_PROVIDED", "MM/dd/yyyy")
).withColumn("UPDATED_TIMESTAMP", current_timestamp())

display(health_data)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE healthcare

# COMMAND ----------

health_data.write.format("delta").option(
    "path", "dbfs:/mnt/health-updates/silver/health_data"
).saveAsTable("healthcare.health_data")

# COMMAND ----------


