# Databricks notebook source
print("Hello World")

# COMMAND ----------

print("hello")

# COMMAND ----------

# MAGIC %md
# MAGIC This is some text
# MAGIC
# MAGIC This is some additional text

# COMMAND ----------

# MAGIC %md
# MAGIC # This is a header
# MAGIC
# MAGIC ## This is a subheader
# MAGIC
# MAGIC ### Lower level subheader

# COMMAND ----------

println("Hello")

# COMMAND ----------

# MAGIC %scala
# MAGIC println("Hello, World")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 17

# COMMAND ----------



# COMMAND ----------

# "dbutils" provides utilities for working with File Systems in Databricks

# COMMAND ----------

dbutils.help()

# This module provides various utilities for users to interact with the rest of Databricks.

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

help(dbutils.fs.cp)

# COMMAND ----------

dbutils.fs.ls("/")

# COMMAND ----------

display(dbutils.fs.ls("/"))

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/databricks-datasets/"))

# COMMAND ----------

# MAGIC %fs
# MAGIC ls 

# COMMAND ----------

# just typing "ls" will automatically look at the root directory

# COMMAND ----------

# MAGIC %fs ls /

# COMMAND ----------

# MAGIC %fs ls '/'

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/databricks-datasets/'

# COMMAND ----------

dbutils.fs.ls()

# keeping ls() as blank would not work. You need to pass something into the parenthesis

# COMMAND ----------


