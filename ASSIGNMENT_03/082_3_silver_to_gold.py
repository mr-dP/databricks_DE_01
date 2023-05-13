# Databricks notebook source
health_data = spark.read.format("delta").load(
    "dbfs:/mnt/health-updates/silver/health_data"
)

# COMMAND ----------

feeling_count_day = health_data.groupBy("date_provided", "feeling_today").count()

# COMMAND ----------

feeling_count_day.display()

# COMMAND ----------

feeling_count_day.write.format("delta").mode("overwrite").option(
    "path", "dbfs:/mnt/health-updates/gold/feeling_count_day"
).saveAsTable("healthcare.feeling_count_day")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   healthcare.feeling_count_day;

# COMMAND ----------

symptoms_count_day = health_data.groupBy("date_provided", "GENERAL_SYMPTOMS").count()

# COMMAND ----------

symptoms_count_day.display()

# COMMAND ----------

symptoms_count_day.write.format("delta").mode("overwrite").option(
    "path", "dbfs:/mnt/health-updates/gold/symptoms_count_day"
).saveAsTable("healthcare.symptoms_count_day")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   healthcare.symptoms_count_day;

# COMMAND ----------

healthcare_visit_day = health_data.groupBy("date_provided", "healthcare_visit").count()

# COMMAND ----------

healthcare_visit_day.write.format("delta").option(
    "path", "dbfs:/mnt/health-updates/gold/healthcare_visit_day"
).mode("overwrite").saveAsTable("healthcare.healthcare_visit_day")

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   healthcare.healthcare_visit_day;

# COMMAND ----------

dbutils.notebook.exit("Gold Processing Complete")

# COMMAND ----------


