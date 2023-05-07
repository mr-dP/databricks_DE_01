# Databricks notebook source
# Azure Databricks Notebooks have built-in support for charts and visualizations.
# The visualizations are available when you use the display() functions to view a data table as a result of either a pandas or a Spark dataframe in a notebook cell.

# COMMAND ----------

order_details = spark.read.parquet("dbfs:/FileStore/tables/gold/order_details")

monthly_sales = spark.read.parquet("dbfs:/FileStore/tables/gold/monthly_sales")

# COMMAND ----------

display(order_details)

# COMMAND ----------

order_details.display()

# COMMAND ----------

display(monthly_sales)

# COMMAND ----------

# MAGIC %md
# MAGIC Some text for my notebook

# COMMAND ----------

# MAGIC %md
# MAGIC # Title for my dashboard

# COMMAND ----------


