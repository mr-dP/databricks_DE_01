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

from pyspark.sql.functions import current_timestamp

countries = countries.withColumn("timestamp", current_timestamp())
countries.display()

# COMMAND ----------

from pyspark.sql.functions import month, year

countries.select(
    "timestamp", month(countries["timestamp"]), year(countries["timestamp"])
).display()

# COMMAND ----------

from pyspark.sql.functions import lit

countries = countries.withColumn("date_literal", lit("15-05-2023"))
countries.display()

# COMMAND ----------

countries.printSchema()

# COMMAND ----------

from pyspark.sql.functions import to_date

countries = countries.select(
    "date_literal", to_date("date_literal", "dd-MM-yyyy").alias("date_format_new")
)
countries.printSchema()

# COMMAND ----------

countries.select("date_literal", "date_format_new").display()

# COMMAND ----------

from pyspark.sql.functions import to_timestamp

countries = countries.select(
    "date_literal", to_timestamp("date_literal", "dd-MM-yyyy").alias("date_format_new")
)
countries.display()

# COMMAND ----------

countries.printSchema()

# COMMAND ----------


