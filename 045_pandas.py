# Databricks notebook source
countries_path = "dbfs:/FileStore/tables/countries.csv"

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

countries = spark.read.csv(countries_path, schema=countries_schema, header=True)

# COMMAND ----------

# you can convert the "countries" Spark DataFrame to a Pandas Dataframe using the toPandas() method

# COMMAND ----------

import pandas

# COMMAND ----------

countries_pd = countries.toPandas()

# COMMAND ----------

countries_pd

# COMMAND ----------

countries_pd.head()

# COMMAND ----------

countries_pd.head(3)

# COMMAND ----------

countries_pd.iloc[0]

# COMMAND ----------


