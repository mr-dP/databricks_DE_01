# Databricks notebook source
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

countries_df = spark.read.csv(
    "dbfs:/FileStore/tables/countries.csv", schema=countries_schema, header=True
)

# COMMAND ----------

display(countries_df)

# COMMAND ----------

countries_df.write.option("header", True).csv("dbfs:/FileStore/tables/countries_out")

# COMMAND ----------

df = spark.read.csv("dbfs:/FileStore/tables/countries_out", header=True)

# COMMAND ----------

display(df)

# COMMAND ----------

countries_df.write.option("header", "true").csv(
    "dbfs:/FileStore/tables/output/countries_out"
)

# COMMAND ----------

countries_df.write.option("header", "true").csv(
    "dbfs:/FileStore/tables/output/countries_out"
)

# AnalysisException: path dbfs:/FileStore/tables/output/countries_out already exists.

# COMMAND ----------

countries_df.write.options(header=True).mode("overwrite").csv(
    "dbfs:/FileStore/tables/output/countries_out"
)

# COMMAND ----------

countries_df.write.csv(
    "dbfs:/FileStore/tables/output/countries_out", mode="overwrite", header=True
)

# COMMAND ----------

# Partitioning is a way to split a very large dataset into smaller datasets based on one or more partition keys. This can provide faster access to data as well as the ability to perform operations on a smaller subset of data because not all partitions will be used

# COMMAND ----------

display(countries_df)

# COMMAND ----------

countries_df.write.partitionBy("REGION_ID").options(header=True).mode("overwrite").csv(
    "dbfs:/FileStore/tables/output/countries_out"
)

# COMMAND ----------

display(
    spark.read.option("header", "true").csv(
        "dbfs:/FileStore/tables/output/countries_out"
    )
)

# COMMAND ----------

display(
    spark.read.csv(
        "dbfs:/FileStore/tables/output/countries_out/REGION_ID=10", header=True
    )
)

# COMMAND ----------


