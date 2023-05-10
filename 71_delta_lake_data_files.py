# Databricks notebook source
container_name = "delta-lake-demo"
mount_point = f"/mnt/{container_name}"
account_name = "00x00dlorders"

# COMMAND ----------

application_id = dbutils.secrets.get("adls_to_databricks_new01", "app-id")
tenant_id = dbutils.secrets.get("adls_to_databricks_new01", "dir-id")
secret = dbutils.secrets.get("adls_to_databricks_new01", "secret-value")

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

configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": application_id,
    "fs.azure.account.oauth2.client.secret": secret,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
}

dbutils.fs.mount(
    source=f"abfss://bronze@{account_name}.dfs.core.windows.net/",
    mount_point="/mnt/bronze",
    extra_configs=configs,
)

# COMMAND ----------

countries = spark.read.csv(
    "dbfs:/mnt/bronze/countries.csv", header=True, inferSchema=True
)

# COMMAND ----------

display(countries)

# COMMAND ----------

countries.write.format("delta").save("dbfs:/mnt/delta-lake-demo/countries_delta")

# COMMAND ----------

countries.write.format("parquet").save("dbfs:/mnt/delta-lake-demo/countries_parquet")

# COMMAND ----------

display(spark.read.format("delta").load("dbfs:/mnt/delta-lake-demo/countries_delta"))

# COMMAND ----------

countries.write.format("delta").partitionBy("REGION_ID").save(
    "dbfs:/mnt/delta-lake-demo/countries_delta_part"
)

# COMMAND ----------

countries.write.format("delta").partitionBy("REGION_ID").mode("overwrite").save(
    "dbfs:/mnt/delta-lake-demo/countries_delta_part"
)

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE delta_lake_db;

# COMMAND ----------

countries = spark.read.format("delta").load("dbfs:/mnt/delta-lake-demo/countries_delta")

# COMMAND ----------

countries.write.saveAsTable("delta_lake_db.countries_managed_delta")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended delta_lake_db.countries_managed_delta;
# MAGIC
# MAGIC -- Type         MANAGED
# MAGIC -- Location     dbfs:/user/hive/warehouse/delta_lake_db.db/countries_managed_delta
# MAGIC -- Provider     delta

# COMMAND ----------

countries.write.option("path", "dbfs:/mnt/delta-lake-demo/countries_delta").mode(
    "overwrite"
).saveAsTable("delta_lake_db.countries_ext_delta")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*)
# MAGIC FROM
# MAGIC   delta_lake_db.countries_ext_delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC EXTENDED delta_lake_db.countries_ext_delta;
# MAGIC
# MAGIC -- Type        EXTERNAL
# MAGIC -- Location    dbfs:/mnt/delta-lake-demo/countries_delta
# MAGIC -- Provider    delta

# COMMAND ----------


