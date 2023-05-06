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

countries.groupBy("region_id")

# It returns a GroupedData object
# To display something we need to perform an aggregation

# COMMAND ----------

countries.groupBy("region_id").sum("population").display()

# COMMAND ----------

countries.groupBy("region_id").min("population").display()

# COMMAND ----------

countries.groupBy("region_id").mean("population").display()

# COMMAND ----------

countries.groupBy("region_id").sum("population", "area_km2").display()

# COMMAND ----------

# If you want to perform different aggreagtions on multiple columns, you need to use the agg() method. Within the agg() method, you can pass in all of your aggregations

# COMMAND ----------

from pyspark.sql.functions import sum, mean

countries.groupBy("region_id").agg(mean("population"), sum("area_km2")).display()

# COMMAND ----------

from pyspark.sql.functions import sum, mean

countries.groupBy("region_id").agg(
    mean("population").alias("mean_population"), sum("area_km2").alias("sum_area_km2")
).display()

# COMMAND ----------

countries.groupBy("region_id", "sub_region_id").agg(
    mean(countries["population"]).alias("mean_population"),
    sum(countries.AREA_KM2).alias("sum_area_km2"),
).orderBy("region_id", "sub_region_id").display()

# COMMAND ----------

from pyspark.sql.functions import max, min

countries.groupBy(countries.REGION_ID, countries.SUB_REGION_ID).agg(
    max(countries.POPULATION).alias("max_pop"),
    min(countries.POPULATION).alias("min_pop"),
).orderBy("region_id", ascending=True).display()

# COMMAND ----------


