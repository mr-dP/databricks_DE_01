# Databricks notebook source
# Create temporary views of DataFrames to query them using SQL syntax

# COMMAND ----------

countries = spark.read.csv("dbfs:/FileStore/tables/countries.csv", header=True)

# COMMAND ----------

display(countries)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   'dbfs:/FileStore/tables/countries.csv';
# MAGIC   
# MAGIC -- [PARSE_SYNTAX_ERROR] Syntax error at or near ''dbfs:/FileStore/tables/countries.csv''(line 1, pos 14)

# COMMAND ----------

countries.createTempView("countries_tv")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   countries_tv;

# COMMAND ----------

spark.sql("SELECT * FROM countries_tv").display()

# COMMAND ----------

spark.sql("SELECT * FROM countries_tv").show()

# COMMAND ----------

display(spark.sql("SELECT * FROM countries_tv"))

# COMMAND ----------

# spark.sql() allows you the flexibility of being able to use f-strings so you can pass in variables

# COMMAND ----------

table_name = "countries_tv"

# COMMAND ----------

spark.sql(f"SELECT * FROM {table_name}").display()

# COMMAND ----------

countries.createTempView("countries_tv")

# AnalysisException: Temporary view 'countries_tv' already exists

# COMMAND ----------

countries.createOrReplaceTempView("countries_tv")

# COMMAND ----------

# The limitations of a local temporary view is it is only available in the current notebook
# Once this session ends, the view is also dropped. If you terminate the cluster and restart it, the view is also lost

# COMMAND ----------

# if you want to create a view that is available on all notebooks running in the cluster, then you need to create a Global Temporary View

# COMMAND ----------

countries.createOrReplaceGlobalTempView("countries_gv")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   countries_gv;
# MAGIC
# MAGIC When AnalysisException: Table or view not found: countries_gv; line 4 pos 2;

# COMMAND ----------

# When it comes to Global Views, you need to qualify it with "global_temp". This is beacuse it stores it in a different database

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   global_temp.countries_gv;

# COMMAND ----------

# As with Local Temporary Views, once the session ends, the Global Temp view will be dropped and it will be unavailable

# COMMAND ----------


