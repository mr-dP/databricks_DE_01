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

display(countries)

# COMMAND ----------

countries.display()

# COMMAND ----------

countries.select("name", "capital", "population").display()

# COMMAND ----------

countries.select(
    countries["name"], countries["capital"], countries["population"]
).display()

# when you use this notation. you can add on methods

# COMMAND ----------

countries.select(countries.NAME, countries.CAPITAL, countries.POPULATION).display()

# We have to pass in the column names in a case-sensitive way while using dot notation

# COMMAND ----------

from pyspark.sql.functions import col

countries.select(col("name"), col("capital"), col("population")).display()

# COMMAND ----------

countries.select(
    countries.NAME.alias("country_name"),
    countries["capital"].alias("capital_city"),
    col("population"),
).display()

# COMMAND ----------

countries.select("name", "capital", "population").withColumnRenamed(
    "name", "country_name"
).withColumnRenamed("capital", "capital_city").display()

# COMMAND ----------

regions = (
    spark.read.options(header=True)
    .schema("id INT, name STRING")
    .csv("dbfs:/FileStore/tables/country_regions.csv")
)

# COMMAND ----------

regions.printSchema()

# COMMAND ----------

regions.display()

# COMMAND ----------

display(regions.withColumnRenamed("name", "continent"))

# COMMAND ----------

display(regions.select(col("name").alias("continent"), regions["id"]))

# COMMAND ----------


