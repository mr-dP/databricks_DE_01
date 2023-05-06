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

regions_path = "dbfs:/FileStore/tables/country_regions.csv"

regions_schema = StructType(
    [
        StructField("Id", StringType(), False),
        StructField("NAME", StringType(), False),
    ]
)

regions = spark.read.schema(regions_schema).csv(regions_path, header=True)

# COMMAND ----------

display(countries)
display(regions)

# COMMAND ----------

countries = (
    countries.join(regions, countries["region_id"] == regions["id"], "inner")
    .select(
        countries["name"].alias("country_name"),
        regions["name"].alias("region_name"),
        countries["population"],
    )
    .orderBy(countries["population"], ascending=False)
)

# COMMAND ----------

countries.display()

# COMMAND ----------

countries_pivot = (
    countries.groupBy("country_name").pivot("region_name").sum("population")
)

# COMMAND ----------

countries_pivot.display()

# COMMAND ----------

from pyspark.sql.functions import expr

countries_pivot.select(
    "country_name",
    expr(
        "stack(5, 'Africa', Africa, 'America', America, 'Asia', Asia, 'Europe', Europe, 'Oceania', Oceania) AS (region_name, population)"
    ),
).where("population is not null").display()

# COMMAND ----------


