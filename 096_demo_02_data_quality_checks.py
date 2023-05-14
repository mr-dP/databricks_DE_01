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

@dlt.table
@dlt.expect("ORDER_STATUS_COMPLETE", "ORDER_STATUS == 'COMPLETE'")
@dlt.expect("ORDER_ID_NOT_NULL", "ORDER_ID IS NOT NULL")
def orders_bronze():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .schema(orders_schema)
        .load(orders_path)
    )


# Any violations to @dlt.expect() expections will still be present in the table

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
@dlt.expect("ORDER_ID_NOT_NULL", "ORDER_ID IS NOT NULL")
def order_items_bronze():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .schema(order_itmes_schema)
        .load(order_items_path)
    )

# COMMAND ----------



# COMMAND ----------

@dlt.table
@dlt.expect_or_drop("ORDER_STATUS_COMPLETE", "ORDER_STATUS == 'COMPLETE'")
@dlt.expect_or_drop("ORDER_ID_NOT_NULL", "ORDER_ID IS NOT NULL")
def orders_silver():
    return dlt.read("orders_bronze").select(
        "ORDER_ID",
        to_date("ORDER_DATETIME", "dd-MM-yy kk.mm.ss.SS").alias("ORDER_DATE"),
        "CUSTOMER_ID",
        "ORDER_STATUS",
        "STORE_ID",
        current_timestamp().alias("MODIFIED_DATE"),
    )

# COMMAND ----------

@dlt.table
@dlt.expect_or_drop("ORDER_ID_NOT_NULL", "ORDER_ID IS NOT NULL")
def order_items_silver():
    return dlt.read("order_items_bronze").select(
        "ORDER_ID",
        "PRODUCT_ID",
        "UNIT_PRICE",
        "QUANTITY",
        current_timestamp().alias("MODIFIED_DATE"),
    )

# COMMAND ----------


