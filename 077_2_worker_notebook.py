# Databricks notebook source
dbutils.widgets.text("input_widget", "", "provide an input")

# COMMAND ----------

dbutils.widgets.get("input_widget")

# COMMAND ----------

dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
# /Repos/GithubRepo/databricks_DE_01/077_2_worker_notebook

# COMMAND ----------


