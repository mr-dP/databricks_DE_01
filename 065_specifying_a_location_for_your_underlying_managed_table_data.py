# Databricks notebook source
# MAGIC %sql
# MAGIC DROP DATABASE countries;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE countries;

# COMMAND ----------

countries = spark.read.csv("/mnt/bronze/countries.csv", header=True)

# COMMAND ----------

countries.write.saveAsTable("countries.countries_default_loc")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC EXTENDED countries.countries_default_loc;
# MAGIC
# MAGIC -- Location   dbfs:/user/hive/warehouse/countries.db/countries_default_loc

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE countries.countries_default_loc;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE countries;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE countries LOCATION '/mnt/bronze/managed_data';

# COMMAND ----------

countries.write.saveAsTable("countries.countries_specified_loc")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC EXTENDED countries.countries_specified_loc;
# MAGIC
# MAGIC -- Location   dbfs:/mnt/bronze/managed_data/countries_specified_loc

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE countries.countries_specified_loc;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE countries;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE countries;

# COMMAND ----------


