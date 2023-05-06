# Databricks notebook source
# The CASE statement is SQL goes through conditions and returns a value when the first condition is met

# COMMAND ----------

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

display(countries)

# COMMAND ----------

from pyspark.sql.functions import when

countries.withColumn(
    "name_length",
    when(countries["population"] > 100000000, "large").when(
        countries["population"] <= 100000000, "not large"
    ),
).display()

# COMMAND ----------

from pyspark.sql.functions import when

countries.withColumn(
    "name_length",
    when(countries["population"] > 100000000, "large").otherwise("not large"),
).display()

# COMMAND ----------


