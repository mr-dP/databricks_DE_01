# Databricks notebook source
# when we created the Delta Live Table pipeline we defined a Storage Location.
# We have 2 folders - "system" and "tables"
# "system" contains the metadata about the pipeline events. It is stored in "delta" format
# "tables" contains the underlying data for the Delta Live Tables

# COMMAND ----------

display(
    spark.read.format("delta").load("dbfs:/mnt/streaming-demo/delta_db/system/events")
)

# Pipeline metadata

# COMMAND ----------

display(
    spark.read.format("delta").load(
        "dbfs:/mnt/streaming-demo/delta_db/tables/order_items_bronze"
    )
)

# COMMAND ----------


