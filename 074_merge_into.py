# Databricks notebook source
# MERGE INTO operation combines the update, delete and insert operations 

# COMMAND ----------

countries = spark.read.csv(
    "dbfs:/mnt/bronze/countries.csv", header=True, inferSchema=True
)

# COMMAND ----------

display(countries)

# COMMAND ----------

countries_1 = countries.filter("REGION_ID IN (10, 20, 30)")

# COMMAND ----------

countries_1.display()

# COMMAND ----------

from pyspark.sql.functions import col

countries_2 = countries.filter(col("REGION_ID").isin("20", "30", "40", "50"))

# COMMAND ----------

display(countries_2)

# COMMAND ----------

countries_1.write.format("delta").saveAsTable("delta_lake_db.countries_1")

# COMMAND ----------

countries_2.write.format("delta").saveAsTable("delta_lake_db.countries_2")

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE
# MAGIC   delta_lake_db.countries_2
# MAGIC SET
# MAGIC   NAME = UPPER(NAME);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   delta_lake_db.countries_2;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   delta_lake_db.countries_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO delta_lake_db.countries_1 tgt
# MAGIC USING delta_lake_db.countries_2 src
# MAGIC 	ON tgt.COUNTRY_ID = src.COUNTRY_ID
# MAGIC WHEN MATCHED
# MAGIC 	THEN
# MAGIC 		UPDATE
# MAGIC 		SET tgt.NAME = src.NAME
# MAGIC WHEN NOT MATCHED
# MAGIC 	THEN
# MAGIC 		INSERT (
# MAGIC 			tgt.COUNTRY_ID,
# MAGIC 			tgt.NAME,
# MAGIC 			tgt.NATIONALITY,
# MAGIC 			tgt.COUNTRY_CODE,
# MAGIC 			tgt.ISO_ALPHA2,
# MAGIC 			tgt.CAPITAL,
# MAGIC 			tgt.POPULATION,
# MAGIC 			tgt.AREA_KM2,
# MAGIC 			tgt.REGION_ID,
# MAGIC 			tgt.SUB_REGION_ID,
# MAGIC 			tgt.INTERMEDIATE_REGION_ID,
# MAGIC 			tgt.ORGANIZATION_REGION_ID
# MAGIC 			)
# MAGIC 		VALUES (
# MAGIC 			src.COUNTRY_ID,
# MAGIC 			src.NAME,
# MAGIC 			UPPER(src.NATIONALITY),     -- We can also use functions in the VALUES as well
# MAGIC 			src.COUNTRY_CODE,
# MAGIC 			src.ISO_ALPHA2,
# MAGIC 			src.CAPITAL,
# MAGIC 			src.POPULATION,
# MAGIC 			src.AREA_KM2,
# MAGIC 			src.REGION_ID,
# MAGIC 			src.SUB_REGION_ID,
# MAGIC 			src.INTERMEDIATE_REGION_ID,
# MAGIC 			src.ORGANIZATION_REGION_ID
# MAGIC 			)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   delta_lake_db.countries_1;

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *

deltaTable = DeltaTable.forPath(spark, 'dbfs:/user/hive/warehouse/delta_lake_db.db/countries_1')

# COMMAND ----------

# The target table must be a DataFrame
deltaTable.alias("target").merge(
    countries_2.alias("source"),
    "target.COUNTRY_ID = source.COUNTRY_ID AND target.NAME = source.NAME",
).whenMatchedUpdate(set={"NAME": "source.NAME"}).whenNotMatchedInsert(
    values={
        "COUNTRY_ID": "source.COUNTRY_ID",
        "NAME": "source.NAME",
        "NATIONALITY": "source.NATIONALITY",
        "COUNTRY_CODE": "source.COUNTRY_CODE",
        "ISO_ALPHA2": "source.ISO_ALPHA2",
        "CAPITAL": "source.CAPITAL",
        "POPULATION": "source.POPULATION",
        "AREA_KM2": "source.AREA_KM2",
        "REGION_ID": "source.REGION_ID",
        "SUB_REGION_ID": "source.SUB_REGION_ID",
        "INTERMEDIATE_REGION_ID": "source.INTERMEDIATE_REGION_ID",
        "ORGANIZATION_REGION_ID": "source.ORGANIZATION_REGION_ID",
    }
).execute()

# COMMAND ----------


