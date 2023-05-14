# Databricks notebook source
import dlt
from pyspark.sql.functions import to_date, current_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
)

# COMMAND ----------

# We cannot import the "dlt" module to this notebook, we are actually coding blind. we cannot test out code in the notebook. We have to test our code in the pipeline

# COMMAND ----------

orders_path = "dbfs:/mnt/streaming-demo/full_dataset/orders_full.csv"

orders_schema = StructType(
    [
        StructField("ORDER_ID", IntegerType(), False),
        StructField("ORDER_DATETIME", StringType(), False),
        StructField("CUSTOMER_ID", IntegerType(), False),
        StructField("ORDER_STATUS", StringType(), False),
        StructField("STORE_ID", IntegerType(), False),
    ]
)

# COMMAND ----------

# here we are defining the first Delta Live Table


@dlt.table
def orders_bronze():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .schema(orders_schema)
        .load(orders_path)
    )


# We are using the create table decorator "@dlt.table"
#   In Python, a Decorator just takes a function and extends the behaviour of that function

# Whatever the name is of the function that you specify, that will be the name of the Delta Live Table
# You then need to return a query inside of that function. This query must be a DataFrame

# COMMAND ----------

# @dlt.table
# def orders_bronze():
#     df = (
#         spark.read.format("csv")
#         .option("header", "true")
#         .schema(orders_schema)
#         .load(orders_path)
#     )
#     return df

# This is also a valid syntax

# COMMAND ----------

order_items_path = "dbfs:/mnt/streaming-demo/full_dataset/order_items_full.csv"

order_itmes_schema = StructType(
    [
        StructField("ORDER_ID", IntegerType(), False),
        StructField("LINE_ITEM_ID", IntegerType(), False),
        StructField("PRODUCT_ID", IntegerType(), False),
        StructField("UNIT_PRICE", DoubleType(), False),
        StructField("QUANTITY", IntegerType(), False),
    ]
)

# COMMAND ----------

@dlt.table
def order_items_bronze():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .schema(order_itmes_schema)
        .load(order_items_path)
    )

# COMMAND ----------

@dlt.table
def orders_silver():
    return dlt.read("orders_bronze").select(
        "ORDER_ID",
        to_date("ORDER_DATETIME", "dd-MM-yy kk.mm.ss.SS").alias("ORDER_DATE"),
        "CUSTOMER_ID",
        "ORDER_STATUS",
        "STORE_ID",
        current_timestamp().alias("MODIFIED_DATE"),
    )

    # When you reference a Delta Live Table, you need to use "dlt.read()". You cannot use "spark.read()"

# COMMAND ----------

@dlt.table
def order_items_silver():
    return dlt.read("order_items_bronze").select(
        "ORDER_ID",
        "PRODUCT_ID",
        "UNIT_PRICE",
        "QUANTITY",
        current_timestamp().alias("MODIFIED_DATE"),
    )

# COMMAND ----------


