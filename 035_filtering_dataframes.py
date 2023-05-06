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

countries.display()

# COMMAND ----------

countries.filter(countries["population"] > 1000000000).display()

# COMMAND ----------

from pyspark.sql.functions import locate

countries.filter(locate("B", countries["capital"]) == 1).display()

# COMMAND ----------

countries.filter(
    (countries["population"] > 1000000000) & (locate("B", countries["capital"]) == 1)
).display()

# Each condition must be in a parenthesis

# COMMAND ----------

countries.filter(
    (countries["population"] > 1000000000) | (locate("B", countries["capital"]) == 1)
).display()

# COMMAND ----------

countries.filter("region_id == 10").display()

# COMMAND ----------

countries.filter("region_id <> 10").display()

# COMMAND ----------

countries.filter("region_id <> 10 AND population == 0").display()

# COMMAND ----------

countries.filter("region_id <> 10 & population == 0").display()

# COMMAND ----------

from pyspark.sql.functions import length

countries.filter(
    (length(countries["name"]) > 15) & (countries.REGION_ID != 10)
).display()

# COMMAND ----------

countries.filter("len(name) > 15 AND region_id != 10").display()

# COMMAND ----------


