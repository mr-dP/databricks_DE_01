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

countries.dtypes

# COMMAND ----------

countries_dt = spark.read.csv(countries_path, header=True)

# COMMAND ----------

countries_dt.dtypes

# COMMAND ----------

countries_dt.select(
    "name", "capital", countries_dt["POPULATION"].cast(IntegerType())
).dtypes

# "cast()" function is part of the pyspark.sql.Column API

# COMMAND ----------

countries_dt.select(
    "name", "capital", countries_dt["POPULATION"].cast(IntegerType())
).display()

# COMMAND ----------

countries.select(countries.POPULATION.cast(StringType())).dtypes

# COMMAND ----------


