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

# COMMAND ----------

countries = spark.read.csv(countries_path, schema=countries_schema, header=True)

# COMMAND ----------

from pyspark.sql.functions import current_date

countries.withColumn("current_date", current_date()).display()

# COMMAND ----------

from pyspark.sql.functions import lit

countries.withColumn("updated_by", lit("mr-dP")).display()

# COMMAND ----------

countries.withColumn("population_m", countries.POPULATION / 1000000).display()

# COMMAND ----------


