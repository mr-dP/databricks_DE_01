# Databricks notebook source
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    TimestampType,
    DoubleType,
)

# COMMAND ----------

orders_path = "dbfs:/FileStore/tables/bronze/orders.csv"

orders_schema = StructType(
    [
        StructField("ORDER_ID", IntegerType(), False),
        StructField("ORDER_DATETIME", StringType(), False),
        StructField("CUSTOMER_ID", IntegerType(), False),
        StructField("ORDER_STATUS", StringType(), False),
        StructField("STORE_ID", IntegerType(), False),
    ]
)

orders = (
    spark.read.format("csv")
    .option("header", "true")
    .schema(orders_schema)
    .load(orders_path)
)

display(orders)
orders.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp

orders_silver = orders.select(
    col("ORDER_ID"),
    to_timestamp(col("ORDER_DATETIME"), "dd-MMM-yy HH.mm.ss.SS").alias(
        "ORDER_TIMESTAMP"
    ),
    col("CUSTOMER_ID"),
    col("ORDER_STATUS"),
    col("STORE_ID"),
).where(col("ORDER_STATUS") == "COMPLETE")

orders_silver.display()
orders_silver.printSchema()

# COMMAND ----------

stores_path = "dbfs:/FileStore/tables/bronze/stores.csv"

stores_schema = StructType(
    [
        StructField("STORE_ID", IntegerType(), False),
        StructField("STORE_NAME", StringType(), False),
        StructField("WEB_ADDRESS", StringType(), False),
        StructField("LATITUDE", DoubleType(), False),
        StructField("LONGITUDE", DoubleType(), False),
    ]
)

stores = spark.read.csv(stores_path, header=True, schema=stores_schema)

# COMMAND ----------

display(stores)

# COMMAND ----------

orders_final = orders_silver.join(stores, "store_id", "inner").select(
    orders_silver["ORDER_ID"],
    orders_silver["ORDER_TIMESTAMP"],
    orders_silver["CUSTOMER_ID"],
    orders_silver["ORDER_STATUS"],
    orders_silver["STORE_ID"],
    stores["STORE_NAME"],
)

orders_final.display()

# COMMAND ----------

orders_final.write.parquet("dbfs:/FileStore/tables/silver/orders", mode="OVERWRITE")

# COMMAND ----------

order_items_path = "dbfs:/FileStore/tables/bronze/order_items.csv"

order_items_schema = StructType(
    [
        StructField("ORDER_ID", IntegerType(), False),
        StructField("LINE_ITEM_ID", StringType(), False),
        StructField("PRODUCT_ID", StringType(), False),
        StructField("UNIT_PRICE", DoubleType(), False),
        StructField("QUANTITY", IntegerType(), False),
    ]
)

order_items = spark.read.csv(order_items_path, header=True, schema=order_items_schema)

order_items.display()

# COMMAND ----------

order_items = order_items.drop("LINE_ITEM_ID")
display(order_items)

# COMMAND ----------

order_items.write.mode("OVERWRITE").parquet("dbfs:/FileStore/tables/silver/order_items")

# COMMAND ----------

products_path = "dbfs:/FileStore/tables/bronze/products.csv"

products_schema = StructType(
    [
        StructField("PRODUCT_ID", IntegerType(), False),
        StructField("PRODUCT_NAME", StringType(), False),
        StructField("UNIT_PRICE", DoubleType(), False),
    ]
)

products = spark.read.csv(products_path, header=True, schema=products_schema)

products.display()

# COMMAND ----------

products.write.parquet("dbfs:/FileStore/tables/silver/products", mode="OVERWRITE")

# COMMAND ----------

customers_path = "dbfs:/FileStore/tables/bronze/customers.csv"

customers_schema = StructType(
    [
        StructField("CUTOMER_ID", IntegerType(), False),
        StructField("FULL_NAME", StringType(), False),
        StructField("EMAIL_ADDRESS", StringType(), False),
    ]
)

customers = spark.read.csv(customers_path, header=True, schema=customers_schema)

customers.display()

# COMMAND ----------

customers.write.parquet("dbfs:/FileStore/tables/silver/customers", mode="OVERWRITE")
