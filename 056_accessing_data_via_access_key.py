# Databricks notebook source
# When you create a Storage Account, Azure generates two 512-bit Storage Account Access Keys for that account
# These keys can be used to authorize access to data in your Storage Account via Shared Key Authorization

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.00x00dlorders.dfs.core.windows.net",
    "****************************************************************************************",
)

# COMMAND ----------

# Now we can access the data using a simple "spark.read" command.
# However for the file path we need to use an URI (Uniform Resource Identifier)

# ABFS - Azure Blob System Driver

# URI scheme to reference data
# abfs[s]://file_system@account_name.dfs.core.windows.net/<path>/<path>/<file_name>

# abfss://bronze@00x00dlorders.dfs.core.windows.net/countries.csv

# COMMAND ----------

countries = (
    spark.read.format("csv")
    .option("header", "true")
    .load("abfss://bronze@00x00dlorders.dfs.core.windows.net/countries.csv")
)

# COMMAND ----------

display(countries)

# COMMAND ----------

country_regions = (
    spark.read.format("csv")
    .option("header", "true")
    .load("abfss://bronze@00x00dlorders.dfs.core.windows.net/country_regions.csv")
)

# COMMAND ----------

country_regions.display()
