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

display(countries)

# COMMAND ----------

from pyspark.sql.functions import upper

countries.select(upper("name").alias("NAME_IN_UPPER")).display()

# COMMAND ----------

from pyspark.sql.functions import initcap

countries.select(initcap("name").alias("NAME_IN_UPPER")).display()

# For each word, the first letter is in upper case

# COMMAND ----------

from pyspark.sql.functions import length

countries.select("name", length("name").alias("length_of_name")).display()

# COMMAND ----------

countries.display()

# COMMAND ----------

from pyspark.sql.functions import concat_ws

# COMMAND ----------

countries.select(concat_ws(" ", "name", "country_code")).display()

# COMMAND ----------

countries.select(concat_ws(" ", "name", "country_code", "population")).display()

# COMMAND ----------

countries.select(concat_ws("-", "name", "country_code", "population")).display()

# COMMAND ----------

from pyspark.sql.functions import lower

countries.select(
    concat_ws(" ", countries["name"], lower("country_code"), countries["population"])
).display()

# COMMAND ----------

from pyspark.sql.functions import concat, lit

countries.select(
    concat("name", lit(" "), "country_code", lit(" "), "population")
).display()

# COMMAND ----------


