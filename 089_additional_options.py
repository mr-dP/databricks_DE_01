# Databricks notebook source
streamQuery = (
    orders_sdf.writeStream.format("delta")
    .option(
        "checkpointLocation",
        "dbfs:/mnt/streaming-demo/streaming_dataset/orders_stream_sink/_checkpointLocation",
    )
    .outputMode("append")
    .start("dbfs:/mnt/streaming-demo/streaming_dataset/orders_stream_sink")
)

# output mode "append", this is set by default
# you can also pass in "complete" or "update"

# COMMAND ----------

streamQuery = (
    orders_sdf.writeStream.format("delta")
    .option(
        "checkpointLocation",
        "dbfs:/mnt/streaming-demo/streaming_dataset/orders_stream_sink/_checkpointLocation",
    )
    .outputMode("append")
    .partitionBy("column")
    .start("dbfs:/mnt/streaming-demo/streaming_dataset/orders_stream_sink")
)

# specify a column to partition the data

# COMMAND ----------

streamQuery = (
    orders_sdf.writeStream.format("delta")
    .option(
        "checkpointLocation",
        "dbfs:/mnt/streaming-demo/streaming_dataset/orders_stream_sink/_checkpointLocation",
    )
    .trigger(processingTime="5 seconds")
    .start("dbfs:/mnt/streaming-demo/streaming_dataset/orders_stream_sink")
)

# processing time 5 seconds trigger

# COMMAND ----------

streamQuery = (
    orders_sdf.writeStream.format("delta")
    .option(
        "checkpointLocation",
        "dbfs:/mnt/streaming-demo/streaming_dataset/orders_stream_sink/_checkpointLocation",
    )
    .trigger(once=True)
    .start("dbfs:/mnt/streaming-demo/streaming_dataset/orders_stream_sink")
)

# run only once trigger
