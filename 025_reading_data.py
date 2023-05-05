# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/tables/countries.csv

# COMMAND ----------

spark.read.csv("dbfs:/FileStore/tables/countries.csv")

# COMMAND ----------

countries_df = spark.read.csv("dbfs:/FileStore/tables/countries.csv")

# COMMAND ----------

type(countries_df)

# COMMAND ----------

countries_df.show()

# COMMAND ----------

countries_df.show(n=15, truncate=False)

# COMMAND ----------

display(countries_df)

# COMMAND ----------

countries_df = spark.read.csv("dbfs:/FileStore/tables/countries.csv", header="true")

# COMMAND ----------

display(countries_df)

# COMMAND ----------

countries_df = spark.read.option("header", True).csv(
    "dbfs:/FileStore/tables/countries.csv"
)

# COMMAND ----------

display(countries_df)

# COMMAND ----------

countries_df = spark.read.options(header=True).csv(
    "dbfs:/FileStore/tables/countries.csv"
)

# COMMAND ----------

display(countries_df)

# COMMAND ----------

countries_df.dtypes

# COMMAND ----------

countries_df.schema

# COMMAND ----------

# "StructType" represents values with a structure defined by a sequence of StructFields
# "StructField" represents a field in a StructType

# COMMAND ----------

countries_df.printSchema()

# COMMAND ----------

countries_df.describe()

# COMMAND ----------

countries_df.describe("CAPITAL")

# COMMAND ----------

countries_df = spark.read.options(header=True, inferSchema=True).csv(
    "dbfs:/FileStore/tables/countries.csv"
)

# COMMAND ----------

countries_df = (
    spark.read.options(header=True)
    .option("inferSchema", True)
    .csv("dbfs:/FileStore/tables/countries.csv")
)

# COMMAND ----------

# "inferSchema" is not very efficient as it means that the data is read twice
# So we have 2 Spark jobs, once to infer the schema and then again to read the data. With large datasets this is not optimal

# COMMAND ----------

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

# Inside "StructType" we have a list of all the StructFields

# StructType defines the structure of the structure of the DataFrame
# StructField defines the metadata of the columns within a DataFrame

# COMMAND ----------

display(countries_schema)

# COMMAND ----------

countries_df = spark.read.csv(
    "dbfs:/FileStore/tables/countries.csv", schema=countries_schema, header=True
)

# COMMAND ----------

countries_df.printSchema()

# COMMAND ----------

display(countries_df)

# COMMAND ----------

countries_df = spark.read.schema(countries_schema).csv(
    "dbfs:/FileStore/tables/countries.csv", header=True
)

# COMMAND ----------

countries_df.dtypes

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/countries_single_line.json

# COMMAND ----------

countries_sl_json_df = spark.read.json(
    "dbfs:/FileStore/tables/countries_single_line.json"
)

# COMMAND ----------

display(countries_sl_json_df)

# COMMAND ----------

countries_sl_json_df.printSchema()

# COMMAND ----------

countries_ml_json_df = spark.read.json(
    "dbfs:/FileStore/tables/countries_multi_line.json"
)

# COMMAND ----------

display(countries_ml_json_df)

# COMMAND ----------

countries_ml_json_df = spark.read.json(
    "dbfs:/FileStore/tables/countries_multi_line.json", multiLine=True
)

# COMMAND ----------

display(countries_ml_json_df)

# COMMAND ----------

countries_txt_df = spark.read.csv(
    "dbfs:/FileStore/tables/countries.txt", sep="\t", header=True
)

# COMMAND ----------

display(countries_txt_df)

# COMMAND ----------


