# Databricks notebook source
# Databricks only manages the metadata for unmanaged, a.k.a, externally managed tables
# When you drop a table, you do not affect the underlying data

# COMMAND ----------

# Unmanaged (External) Tables will always specify a location during table creation
# You can either register an existing directory of data files as a table or provide a path where the table is first defined
# Because data and metadata are managed independently you can rename a table or register it to a new database without needing to move any data

# COMMAND ----------

countries = (
    spark.read.format("csv")
    .options(header=True)
    .load("dbfs:/FileStore/tables/countries.csv")
)

# COMMAND ----------

countries.write.mode("overwrite").option("path", "dbfs:/FileStore/external/countries").saveAsTable(
    "countries.countries_ext_python"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED countries.countries_ext_python;
# MAGIC
# MAGIC -- Type      EXTERNAL
# MAGIC -- Location  dbfs:/FileStore/external/countries

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE countries.countries_ext_python;

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/external/countries"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE countries.countries_ext_sql (
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
# MAGIC ) USING CSV LOCATION "dbfs:/FileStore/tables/countries.csv";

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   countries.countries_ext_sql;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE countries.countries_ext_sql;

# COMMAND ----------

# MAGIC %fs rm -r dbfs:/FileStore/external

# COMMAND ----------


