# Databricks notebook source
dbutils.fs.help()

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/tables/countries.txt")

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/tables/countries_multi_line.json")

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/tables/countries_single_line.json")

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/tables/countries_out")

# org.apache.hadoop.fs.FileAlreadyExistsException: Operation failed: "The recursive query parameter value must be true to delete a non-empty directory."

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/tables/countries_out", recurse=True)
dbutils.fs.rm("dbfs:/FileStore/tables/output", recurse=True)

# COMMAND ----------


