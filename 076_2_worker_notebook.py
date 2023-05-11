# Databricks notebook source
print("Printed from Worker Notebook")

# COMMAND ----------

dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
# /Repos/GithubRepo/databricks_DE_01/076_2_worker_notebook

# To get the file path

# COMMAND ----------

dbutils.notebook.exit("Worker Notebook Executed Successfully")

# COMMAND ----------

# The exit commnad should be on the last cell of the notebook. This is beacuse it "exits" the notebook after the cell has run, so any code below will not get executed

# COMMAND ----------


