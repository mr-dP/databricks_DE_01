# Databricks notebook source
orders_full = (
    spark.read.format("csv")
    .options(header=True)
    .load("dbfs:/mnt/streaming-demo/full_dataset/orders_full.csv")
)

# COMMAND ----------

display(orders_full)

# COMMAND ----------

orders_full.filter(orders_full.ORDER_ID == 1).display()

# COMMAND ----------

order_1 = orders_full.filter(orders_full.ORDER_ID == 1)

order_1.write.format("csv").option("header", True).mode("append").save(
    "dbfs:/mnt/streaming-demo/streaming_dataset/orders_streaming.csv"
)

# COMMAND ----------

orders_streaming = (
    spark.read.format("csv")
    .option("header", True)
    .load("dbfs:/mnt/streaming-demo/streaming_dataset/orders_streaming.csv")
)

display(orders_streaming)

# COMMAND ----------

order_2 = orders_full.filter(orders_full.ORDER_ID == 2)

order_2.write.format("csv").option("header", True).mode("append").save(
    "dbfs:/mnt/streaming-demo/streaming_dataset/orders_streaming.csv"
)

# COMMAND ----------

order_2.display()

# COMMAND ----------

orders_streaming = (
    spark.read.format("csv")
    .option("header", True)
    .load("dbfs:/mnt/streaming-demo/streaming_dataset/orders_streaming.csv")
)

display(orders_streaming)

# COMMAND ----------

order_3 = orders_full.filter(orders_full.ORDER_ID == 3)

order_3.write.format("csv").option("header", True).mode("append").save(
    "dbfs:/mnt/streaming-demo/streaming_dataset/orders_streaming.csv"
)

# COMMAND ----------

orders_streaming = (
    spark.read.format("csv")
    .option("header", True)
    .load("dbfs:/mnt/streaming-demo/streaming_dataset/orders_streaming.csv")
)

display(orders_streaming)

# COMMAND ----------

order_4_5 = orders_full.filter(
    (orders_full.ORDER_ID == 5) | (orders_full.ORDER_ID == 4)
)

order_4_5.write.format("csv").option("header", True).mode("append").save(
    "dbfs:/mnt/streaming-demo/streaming_dataset/orders_streaming.csv"
)

# COMMAND ----------

orders_streaming = (
    spark.read.format("csv")
    .option("header", True)
    .load("dbfs:/mnt/streaming-demo/streaming_dataset/orders_streaming.csv")
)

display(orders_streaming)

# COMMAND ----------

order_6_7 = orders_full.filter(
    (orders_full.ORDER_ID == 6) | (orders_full.ORDER_ID == 7)
)

order_6_7.write.format("csv").option("header", True).mode("append").save(
    "dbfs:/mnt/streaming-demo/streaming_dataset/orders_streaming.csv"
)

# COMMAND ----------

orders_streaming = (
    spark.read.format("csv")
    .option("header", True)
    .load("dbfs:/mnt/streaming-demo/streaming_dataset/orders_streaming.csv")
)

display(orders_streaming)orders_streaming = (
    spark.read.format("csv")
    .option("header", True)
    .load("dbfs:/mnt/streaming-demo/streaming_dataset/orders_streaming.csv")
)

display(orders_streaming)

# COMMAND ----------

dbutils.fs.rm(
    "dbfs:/mnt/streaming-demo/streaming_dataset/orders_streaming.csv", recurse=True
)

# COMMAND ----------


