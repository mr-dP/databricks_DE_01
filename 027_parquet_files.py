# Databricks notebook source
df = spark.read.csv(
    "dbfs:/FileStore/tables/countries.csv", header=True, inferSchema=True
)

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.parquet("dbfs:/FileStore/tables/output/countries.parquet")

# COMMAND ----------

display(spark.read.parquet("dbfs:/FileStore/tables/output/countries.parquet"))

# COMMAND ----------


