# Databricks notebook source
# Using Auto Laoder in Spark Structured Streaming provides a more efficient and scalable solution for ingesting and processing continuous streams of data compared to traditional file formats
# It is a complimentary feature of Spark Structured Streaming

#   Auto Loader automatically discovers new data as it becomes available in the source system
#   It simplifies the data ingestion process and ensures that the latest data is always available for processing

#   It also supports various data sources
#   This includes file systems like Apache Kafka and TCP Sockets.
#   It also makes it easy to integrate with existing data pipelines and work with different types of data

#   Auto Loader also includes Data Cleansing and Transofmration capabilities allowing for the removal of invalid or duplicate records, filterinf and enrichment of data and other transformations as needed to prepare the data for downtream processing.

#   Auto Loader automatically batches incoming data, reducing the overhead of processing small amounts of data and improving overall performance

#   It also allows users to configure various parameters such as batch sizes, cehckpoint intervals and trigger mechanisms to meet specified data processing requirements

#   It can also ingest JSON, CSV, PARQUET, AVRO, ORC, TEXT and BINARY file formats

# COMMAND ----------

# Auto Loader provides a Structured Streaming cloud source called "cloudFiles". Given an input directory path on the cloud file storage, the "cloudFiles" source automatically processes new files as they arrive, with the option of also processing existing files in that directory
# It has support for both Python and SQL in Delta Live Tables

# COMMAND ----------

from pyspark.sql.types import (
    IntegerType,
    StringType,
    DoubleType,
    StructField,
    StructType,
)

# COMMAND ----------

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

orders_sdf = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .schema(order_schema)
    .load(orders_streaming_path)
)

# "cloudFiles.format" = format of the source file

# COMMAND ----------

orders_sdf.display()

# COMMAND ----------


