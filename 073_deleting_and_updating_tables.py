# Databricks notebook source
# MAGIC %sql
# MAGIC desc extended delta_lake_db.countries_managed_delta;
# MAGIC
# MAGIC -- Type          MANAGED
# MAGIC -- Location      dbfs:/user/hive/warehouse/delta_lake_db.db/countries_managed_delta
# MAGIC -- Provider      delta

# COMMAND ----------

countries = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("dbfs:/mnt/bronze/countries.csv")
)

# COMMAND ----------

countries.display()

# COMMAND ----------

countries.write.format("parquet").saveAsTable("delta_lake_db.countries_managed_pq")

# parquet managed table

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended delta_lake_db.countries_managed_pq;
# MAGIC
# MAGIC -- Type        MANAGED
# MAGIC -- Provider    parquet
# MAGIC -- Location    dbfs:/user/hive/warehouse/delta_lake_db.db/countries_managed_pq

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW DATABASES;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE delta_lake_db;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   countries_managed_pq;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   countries_managed_delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM
# MAGIC   countries_managed_pq
# MAGIC WHERE
# MAGIC   REGION_ID = 20;
# MAGIC
# MAGIC -- AnalysisException: [UNSUPPORTED_FEATURE.TABLE_OPERATION] The feature is not supported: Table `spark_catalog`.`delta_lake_db`.`countries_managed_pq` does not support DELETE

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM
# MAGIC   countries_managed_delta
# MAGIC WHERE
# MAGIC   REGION_ID = 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   countries_managed_delta;

# COMMAND ----------

spark.read.format("delta").load(
    "dbfs:/user/hive/warehouse/delta_lake_db.db/countries_managed_delta"
).display()

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *

deltaTable = DeltaTable.forPath(
    spark, "dbfs:/user/hive/warehouse/delta_lake_db.db/countries_managed_delta"
)

# COMMAND ----------

deltaTable.delete("REGION_ID = 50")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   countries_managed_delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   countries_managed_delta
# MAGIC WHERE
# MAGIC   REGION_ID = 50;

# COMMAND ----------

deltaTable.delete("REGION_ID = 40 AND POPULATION > 200000")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   countries_managed_delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   countries_managed_delta
# MAGIC WHERE
# MAGIC   REGION_ID = 40
# MAGIC   AND POPULATION > 200000

# COMMAND ----------

deltaTable.delete(col("REGION_ID") == 50)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   countries_managed_delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE
# MAGIC   countries_managed_delta
# MAGIC SET
# MAGIC   COUNTRY_CODE = 'XXX'
# MAGIC WHERE
# MAGIC   REGION_ID = 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   countries_managed_delta;

# COMMAND ----------

deltaTable.update("REGION_ID = 30 AND AREA_KM2 > 600000", {"COUNTRY_CODE": "'YYY'"})

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   countries_managed_delta;

# COMMAND ----------

deltaTable.update(col("REGION_ID") == 10, {"COUNTRY_CODE": lit("XXX")})

# COMMAND ----------


