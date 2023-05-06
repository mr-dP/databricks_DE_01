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

from pyspark.sql.functions import sum

# COMMAND ----------

countries.groupBy(countries["region_id"], countries["sub_region_id"]).agg(
    sum(countries["population"]).alias("total_population")
).display()

# COMMAND ----------

# you apply the pivot() method after the groupBy() method but before the aggregation

# COMMAND ----------

countries.groupBy(countries["region_id"], countries["sub_region_id"]).pivot(
    "region_id"
).agg(sum(countries["population"]).alias("total_population")).display()

# COMMAND ----------

countries.groupBy("sub_region_id").pivot("region_id").agg(
    sum(countries["population"]).alias("total_population")
).display()

# COMMAND ----------


