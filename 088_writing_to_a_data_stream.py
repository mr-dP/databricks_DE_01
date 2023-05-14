# Databricks notebook source
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

orders_sdf.display()

# COMMAND ----------

streamQuery = (
    orders_sdf.writeStream.format("delta")
    .option(
        "checkpointLocation",
        "dbfs:/mnt/streaming-demo/streaming_dataset/orders_stream_sink/_checkpointLocation",
    )
    .start("dbfs:/mnt/streaming-demo/streaming_dataset/orders_stream_sink")
)

# Checkpoints are used to periodically store the metadata about the state of the streaming query uncluding information about which data has been processed and which transformations have been applied
# Each dataset that is a Data Stream sink should have its own checkpointLocation

# COMMAND ----------

streamQuery.isActive

# COMMAND ----------

streamQuery.recentProgress

# COMMAND ----------

spark.read.format("delta").load(
    "dbfs:/mnt/streaming-demo/streaming_dataset/orders_stream_sink"
).display()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE streaming_db;

# COMMAND ----------

streamQuery = (
    orders_sdf.writeStream.format("delta")
    .option(
        "checkpointLocation",
        "dbfs:/mnt/streaming-demo/streaming_dataset/streaming_db/managed/_checkpointLocation",
    )
    .toTable("streaming_db.orders_m")
)

# Managed Table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   streaming_db.orders_m;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE streaming_db.orders_m;

# COMMAND ----------


