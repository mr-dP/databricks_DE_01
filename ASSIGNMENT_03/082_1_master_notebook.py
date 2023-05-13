# Databricks notebook source
dbutils.notebook.run("/Repos/GithubRepo/databricks_DE_01/ASSIGNMENT_03/082_2_bronze_to_silver", 60)

# COMMAND ----------

dbutils.notebook.run("/Repos/GithubRepo/databricks_DE_01/ASSIGNMENT_03/082_3_silver_to_gold", 60)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   healthcare.health_data;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   healthcare.symptoms_count_day;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   healthcare.feeling_count_day;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   healthcare.healthcare_visit_day;
