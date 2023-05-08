# Databricks notebook source
# dbutils.fs.unmount("/mnt/silver")
# dbutils.fs.unmount("/mnt/gold")

# COMMAND ----------

application_id = dbutils.secrets.get(
    scope="databricks-secrets-00x00", key="application-id"
)
tenant_id = dbutils.secrets.get(scope="databricks-secrets-00x00", key="tenant-id")
secret = dbutils.secrets.get(scope="databricks-secrets-00x00", key="secret")

# COMMAND ----------

container_name = "silver"
account_name = "00x00dlorders"
mount_point = "/mnt/silver"

# COMMAND ----------

configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": application_id,
    "fs.azure.account.oauth2.client.secret": secret,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
    source=f"abfss://{container_name}@{account_name}.dfs.core.windows.net/",
    mount_point=mount_point,
    extra_configs=configs,
)

# COMMAND ----------

container_name = "gold"
account_name = "00x00dlorders"
mount_point = "/mnt/gold"

# COMMAND ----------

configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": application_id,
    "fs.azure.account.oauth2.client.secret": secret,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
    source=f"abfss://{container_name}@{account_name}.dfs.core.windows.net/",
    mount_point=mount_point,
    extra_configs=configs,
)

# COMMAND ----------

countries_path = "/mnt/bronze/countries.csv"

from pyspark.sql.types import (
    StructType,
    StructField,
    DoubleType,
    StringType,
    IntegerType,
)

countries_schema = StructType(
    [
        StructField("COUNTRY_ID", IntegerType(), False),
        StructField("NAME", StringType(), False),
        StructField("NATIONALITY", StringType(), False),
        StructField("COUNTRY_CODE", StringType(), False),
        StructField("ISO_ALPHA2", StringType(), False),
        StructField("CAPITAL", StringType(), False),
        StructField("POPULATION", IntegerType(), False),
        StructField("AREA_KM2", DoubleType(), False),
        StructField("REGION_ID", IntegerType(), True),
        StructField("SUB_REGION_ID", IntegerType(), True),
        StructField("INTERMEDIATE_REGION_ID", IntegerType(), True),
        StructField("ORGANIZATION_REGION_ID", IntegerType(), True),
    ]
)

# COMMAND ----------

countries = spark.read.csv(countries_path, schema=countries_schema, header=True)

# COMMAND ----------

countries.display()

# COMMAND ----------

countries = countries.drop(
    "SUB_REGION_ID", "INTERMEDIATE_REGION_ID", "ORGANIZATION_REGION_ID"
)

# COMMAND ----------

countries.display()

# COMMAND ----------

countries.write.parquet("/mnt/silver/countries", mode="overwrite")

# COMMAND ----------

regions_path = "/mnt/bronze/country_regions.csv"

regions_schema = StructType(
    [
        StructField("Id", StringType(), False),
        StructField("NAME", StringType(), False),
    ]
)

# COMMAND ----------

regions = spark.read.schema(regions_schema).csv(regions_path, header=True)

# COMMAND ----------

regions.display()

# COMMAND ----------

regions = regions.withColumnRenamed("NAME", "REGION_NAME")

# COMMAND ----------

display(regions)

# COMMAND ----------

regions.write.mode("overwrite").parquet("/mnt/silver/regions")

# COMMAND ----------

countries = spark.read.parquet("/mnt/silver/countries")

# COMMAND ----------

regions = spark.read.parquet("/mnt/silver/regions")

# COMMAND ----------

display(countries)
display(regions)

# COMMAND ----------

country_data = countries.join(
    regions, countries["REGION_ID"] == regions.Id, "left"
).drop("Id", "REGION_ID", "COUNTRY_CODE", "ISO_ALPHA2")

# COMMAND ----------

display(country_data)

# COMMAND ----------

country_data.write.parquet("/mnt/gold/country_data", mode="overwrite")

# COMMAND ----------


