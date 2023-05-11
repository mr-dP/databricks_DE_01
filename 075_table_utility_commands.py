# Databricks notebook source
# MAGIC %sql
# MAGIC DESCRIBE HISTORY delta_lake_db.countries_1;
# MAGIC -- Each operations that we have performed on the table

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY 'dbfs:/user/hive/warehouse/delta_lake_db.db/countries_1';

# COMMAND ----------

from delta.tables import *

deltaTable = DeltaTable.forPath(
    spark, "dbfs:/user/hive/warehouse/delta_lake_db.db/countries_1"
)

deltaTable.history().display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   delta_lake_db.countries_1 VERSION AS OF 0;
# MAGIC -- Access the data in version 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   delta_lake_db.countries_1 TIMESTAMP AS OF '2023-05-11T04:55:35.000+0000';

# COMMAND ----------

display(
    spark.read.format("delta")
    .option("versionAsOf", 1)
    .load("dbfs:/user/hive/warehouse/delta_lake_db.db/countries_1")
)

# COMMAND ----------

display(
    spark.read.format("delta")
    .option("timestampAsOf", "2023-05-11T04:55:35.000+0000")
    .load("dbfs:/user/hive/warehouse/delta_lake_db.db/countries_1")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended delta_lake_db.countries_managed_pq;
# MAGIC -- Type      MANAGED
# MAGIC -- Provider  parquet
# MAGIC -- Location  dbfs:/user/hive/warehouse/delta_lake_db.db/countries_managed_pq

# COMMAND ----------

# converting Parquet table to a Delta table
deltaTable = DeltaTable.convertToDelta(
    spark, "parquet.`dbfs:/user/hive/warehouse/delta_lake_db.db/countries_managed_pq`"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended delta_lake_db.countries_managed_pq;

# COMMAND ----------

display(
    dbutils.fs.ls("dbfs:/user/hive/warehouse/delta_lake_db.db/countries_managed_pq")
)

# COMMAND ----------

deltaTable = DeltaTable.forPath(
    spark, "dbfs:/user/hive/warehouse/delta_lake_db.db/countries_managed_pq"
)

deltaTable.history().display()

# COMMAND ----------


