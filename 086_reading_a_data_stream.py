# Databricks notebook source
# before reading a data stream, you first need to define the schema of that data stream

# COMMAND ----------

from pyspark.sql.types import (
    IntegerType,
    StringType,
    DoubleType,
    StructField,
    StructType,
)

orders_streaming_path = (
    "dbfs:/mnt/streaming-demo/streaming_dataset/orders_streaming.csv"
)

order_schema = StructType(
    [
        StructField("ORDER_ID", IntegerType(), False),
        StructField("ORDER_DATETIME", StringType(), False),
        StructField("CUSTOMER_ID", IntegerType(), False),
        StructField("ORDER_STATUS", StringType(), False),
        StructField("STORE_ID", IntegerType(), False),
    ]
)

# COMMAND ----------

orders_sdf = spark.readStream.csv(
    orders_streaming_path, schema=order_schema, header=True
)

# COMMAND ----------

orders_sdf.isStreaming

# COMMAND ----------

orders_sdf.display()

# We have created a Streaming query. It is infinitely streaming for new data

# COMMAND ----------

print("test")

# COMMAND ----------


