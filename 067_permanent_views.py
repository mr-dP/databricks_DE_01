# Databricks notebook source
countries = spark.read.csv("dbfs:/FileStore/tables/countries.csv", header=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE countries;

# COMMAND ----------

countries.write.saveAsTable("countries.countries_mt")

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   countries.countries_mt;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE VIEW countries.view_region_10 AS
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   countries.countries_mt
# MAGIC WHERE
# MAGIC   region_id = 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   countries.view_region_10;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE countries CASCADE;

# COMMAND ----------


