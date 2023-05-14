# Databricks notebook source
orders_full = (
    spark.read.format("csv")
    .options(header=True)
    .load("dbfs:/mnt/streaming-demo/full_dataset/orders_full.csv")
)

order_items_full = (
    spark.read.format("csv")
    .options(header=True)
    .load("dbfs:/mnt/streaming-demo/full_dataset/order_items_full.csv")
)

# COMMAND ----------

display(orders_full)

display(order_items_full)

# COMMAND ----------

order_1 = orders_full.filter(orders_full.ORDER_ID == 1)
order_1.write.format("csv").option("header", True).mode("append").save(
    "dbfs:/mnt/streaming-demo/streaming_dataset/orders_streaming.csv"
)

order_item_1 = order_items_full.filter(order_items_full.ORDER_ID == 1)
order_item_1.write.format("csv").option("header", True).mode("append").save(
    "dbfs:/mnt/streaming-demo/streaming_dataset/order_items_streaming.csv"
)

# COMMAND ----------

orders_streaming = (
    spark.read.format("csv")
    .option("header", True)
    .load("dbfs:/mnt/streaming-demo/streaming_dataset/orders_streaming.csv")
)
display(orders_streaming)

order_items_streaming = (
    spark.read.format("csv")
    .option("header", True)
    .load("dbfs:/mnt/streaming-demo/streaming_dataset/order_items_streaming.csv")
)
display(order_items_streaming)

# COMMAND ----------

order_2 = orders_full.filter(orders_full.ORDER_ID == 2)
order_2.write.format("csv").option("header", True).mode("append").save(
    "dbfs:/mnt/streaming-demo/streaming_dataset/orders_streaming.csv"
)

order_item_2 = order_items_full.filter(order_items_full.ORDER_ID == 2)
order_item_2.write.format("csv").option("header", True).mode("append").save(
    "dbfs:/mnt/streaming-demo/streaming_dataset/order_items_streaming.csv"
)

# COMMAND ----------

orders_streaming = (
    spark.read.format("csv")
    .option("header", True)
    .load("dbfs:/mnt/streaming-demo/streaming_dataset/orders_streaming.csv")
)
display(orders_streaming)

order_items_streaming = (
    spark.read.format("csv")
    .option("header", True)
    .load("dbfs:/mnt/streaming-demo/streaming_dataset/order_items_streaming.csv")
)
display(order_items_streaming)

# COMMAND ----------

order_3 = orders_full.filter(orders_full.ORDER_ID == 3)
order_3.write.format("csv").option("header", True).mode("append").save(
    "dbfs:/mnt/streaming-demo/streaming_dataset/orders_streaming.csv"
)

order_item_3 = order_items_full.filter(order_items_full.ORDER_ID == 3)
order_item_3.write.format("csv").option("header", True).mode("append").save(
    "dbfs:/mnt/streaming-demo/streaming_dataset/order_items_streaming.csv"
)

# COMMAND ----------

orders_streaming = (
    spark.read.format("csv")
    .option("header", True)
    .load("dbfs:/mnt/streaming-demo/streaming_dataset/orders_streaming.csv")
)
display(orders_streaming)

order_items_streaming = (
    spark.read.format("csv")
    .option("header", True)
    .load("dbfs:/mnt/streaming-demo/streaming_dataset/order_items_streaming.csv")
)
display(order_items_streaming)

# COMMAND ----------

order_4_5 = orders_full.filter(
    (orders_full.ORDER_ID == 5) | (orders_full.ORDER_ID == 4)
)
order_4_5.write.format("csv").option("header", True).mode("append").save(
    "dbfs:/mnt/streaming-demo/streaming_dataset/orders_streaming.csv"
)

order_item_4_5 = order_items_full.filter(
    (order_items_full.ORDER_ID == 5) | (order_items_full.ORDER_ID == 4)
)
order_item_4_5.write.format("csv").option("header", True).mode("append").save(
    "dbfs:/mnt/streaming-demo/streaming_dataset/order_items_streaming.csv"
)

# COMMAND ----------

orders_streaming = (
    spark.read.format("csv")
    .option("header", True)
    .load("dbfs:/mnt/streaming-demo/streaming_dataset/orders_streaming.csv")
)
display(orders_streaming)

order_items_streaming = (
    spark.read.format("csv")
    .option("header", True)
    .load("dbfs:/mnt/streaming-demo/streaming_dataset/order_items_streaming.csv")
)
display(order_items_streaming)

# COMMAND ----------



# COMMAND ----------

# dbutils.fs.rm(
#     "dbfs:/mnt/streaming-demo/streaming_dataset/orders_streaming.csv", recurse=True
# )

# dbutils.fs.rm(
#     "dbfs:/mnt/streaming-demo/streaming_dataset/order_items_streaming.csv", recurse=True
# )

# COMMAND ----------


