# Databricks notebook source
# Sometimes in your expression you might need to use a function that is not part of the Spark SQL API. In that case, you can use the expr() function that allows you to use SQL syntax

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

countries.display()

# COMMAND ----------

from pyspark.sql.functions import expr

# COMMAND ----------

countries.select("name", expr("name as country_name")).display()

# COMMAND ----------

countries.select(expr("left(name, 2) AS first_2_chars")).display()

# COMMAND ----------

countries.withColumn(
    "population_class",
    expr(
        "CASE WHEN population > 100000000 THEN 'very large' WHEN population > 50000000 THEN 'medium' ELSE 'small' END"
    ),
).display()

# COMMAND ----------

countries.withColumn(
    "area_class",
    expr(
        "CASE WHEN AREA_KM2 > 1000000 THEN 'large' WHEN AREA_KM2 > 300000 THEN 'medium' ELSE 'small' END"
    ),
).display()

# COMMAND ----------


