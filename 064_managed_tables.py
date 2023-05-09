# Databricks notebook source
# With Managed Tables, Databricks manages both the metadata and the underlying data for a Managed Table
# When you drop a table, you also delete the underlying data

# COMMAND ----------

# Managed Tables are the default when creating a table
# The data for a Managed Table resides in the location of the database that it is registered to
# In order to move a Managed Table to a new database, you must re-write all the data to the new location

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   current_database();

# COMMAND ----------

# MAGIC %sql
# MAGIC USE countries;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

countries = (
    spark.read.format("csv")
    .option("header", "true")
    .load("dbfs:/FileStore/tables/countries.csv")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_database();

# COMMAND ----------

# MAGIC %sql
# MAGIC USE default;

# COMMAND ----------

countries.write.saveAsTable("countries.countries_mt")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED countries.countries_mt;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE countries.countries_mt;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE countries.countries_mt_empty (
# MAGIC   country_id INT,
# MAGIC   name STRING,
# MAGIC   nationality STRING,
# MAGIC   country_code STRING,
# MAGIC   iso_alpha_2 STRING,
# MAGIC   capital STRING,
# MAGIC   population INT,
# MAGIC   area_km2 INT,
# MAGIC   region_id INT,
# MAGIC   sub_region_id INT,
# MAGIC   intermediate_region_id INT,
# MAGIC   organizationa_region_id INT
# MAGIC ) USING CSV

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   countries.countries_mt_empty;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED countries.countries_mt_empty;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE countries.countries_copy AS
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   countries.countries_mt;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   countries.countries_copy;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC EXTENDED countries.countries_copy;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE countries.countries_copy_csv USING CSV AS
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   countries.countries_mt;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC EXTENDED countries.countries_copy_csv;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE countries.countries_mt;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE countries.countries_mt_empty;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE countries.countries_copy;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE countries.countries_copy_csv;

# COMMAND ----------


