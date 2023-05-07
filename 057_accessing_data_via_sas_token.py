# Databricks notebook source
# Shared access signatures (SAS)

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.00x00dlorders.dfs.core.windows.net", "SAS")
spark.conf.set(
    "fs.azure.sas.token.provider.type.00x00dlorders.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider",
)
spark.conf.set(
    "fs.azure.sas.fixed.token.00x00dlorders.dfs.core.windows.net",
    "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-05-07T19:57:17Z&st=2023-05-07T11:57:17Z&spr=https&sig=OLRTFixT9BFBtZJp0VO4Rb2Rng3AHEDRjILoMFv%2FLMs%3D",
)

# COMMAND ----------

countries = spark.read.csv(
    "abfss://bronze@00x00dlorders.dfs.core.windows.net/countries.csv", header=True
)

# COMMAND ----------

countries.display()

# COMMAND ----------

regions = spark.read.csv(
    "abfss://bronze@00x00dlorders.dfs.core.windows.net/country_regions.csv", header=True
)

# COMMAND ----------

regions.display()
