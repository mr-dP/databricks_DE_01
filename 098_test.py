# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   delta_db.orders_bronze
# MAGIC ORDER BY
# MAGIC   order_id ASC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   delta_db.orders_silver
# MAGIC ORDER BY
# MAGIC   order_id ASC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   delta_db.order_items_bronze
# MAGIC ORDER BY
# MAGIC   order_id ASC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   delta_db.order_items_silver
# MAGIC ORDER BY
# MAGIC   order_id ASC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   delta_db.order_details_gold
# MAGIC ORDER BY
# MAGIC   order_id ASC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   delta_db.monthly_sales_gold;

# COMMAND ----------


