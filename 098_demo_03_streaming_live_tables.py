# Databricks notebook source
import dlt
from pyspark.sql.functions import to_date, date_format, round
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    DoubleType,
)

# COMMAND ----------

orders_path = "/mnt/streaming-demo/streaming_dataset/orders_streaming.csv"

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
def orders_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", True)
        .schema(orders_schema)
        .load(orders_path)
    )

# COMMAND ----------

order_items_path = "/mnt/streaming-demo/streaming_dataset/order_items_streaming.csv"

order_items_schema = StructType(
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
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", True)
        .schema(order_items_schema)
        .load(order_items_path)
    )

# COMMAND ----------



# COMMAND ----------

@dlt.table
def orders_silver():
    return dlt.read_stream("orders_bronze").select(
        "ORDER_ID",
        to_date("ORDER_DATETIME", "dd-MMM-yy kk.mm.ss.SS").alias("ORDER_DATE"),
        "CUSTOMER_ID",
        "ORDER_STATUS",
        "STORE_ID",
    )

# COMMAND ----------

@dlt.table
def order_items_silver():
    return dlt.read_stream("order_items_bronze").select(
        "ORDER_ID", "PRODUCT_ID", "UNIT_PRICE", "QUANTITY"
    )

# COMMAND ----------



# COMMAND ----------

@dlt.table
def order_details_gold():

    orders_silver = dlt.read("orders_silver")
    order_items_silver = dlt.read("order_items_silver")

    order_details = orders_silver.join(
        order_items_silver,
        orders_silver["order_id"] == order_items_silver["order_id"],
        "left",
    )
    order_details = order_details.select(
        orders_silver["ORDER_ID"],
        orders_silver["ORDER_DATE"],
        orders_silver["CUSTOMER_ID"],
        order_items_silver["PRODUCT_ID"],
        order_items_silver["UNIT_PRICE"],
        order_items_silver["QUANTITY"],
    )
    order_details = order_details.withColumn(
        "SUB_TOTAL_AMOUNT", order_details["UNIT_PRICE"] * order_details["QUANTITY"]
    )

    return order_details

# COMMAND ----------

@dlt.table
def monthly_sales_gold():

    order_details = dlt.read("order_details_gold")

    monthly_sales = order_details.withColumn(
        "MONTH_YEAR", date_format(order_details["ORDER_DATE"], "yyyy-MM")
    )
    monthly_sales = monthly_sales.groupBy("MONTH_YEAR").sum("SUB_TOTAL_AMOUNT")
    monthly_sales = monthly_sales.withColumn(
        "TOTAL_SALES", round("sum(SUB_TOTAL_AMOUNT)", 2)
    ).sort(monthly_sales["MONTH_YEAR"].desc())
    monthly_sales = monthly_sales.select("MONTH_YEAR", "TOTAL_SALES")

    return monthly_sales
