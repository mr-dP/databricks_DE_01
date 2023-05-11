# Databricks notebook source
dbutils.help()

# COMMAND ----------

dbutils.notebook.help()

# The notebook module.
# exit(value: String): void -> This method lets you exit a notebook with a value
# run(path: String, timeoutSeconds: int, arguments: Map): String -> This method runs a notebook and returns its exit value

# COMMAND ----------

dbutils.notebook.run("076_2_worker_notebook", 60)

# COMMAND ----------

dbutils.notebook.run("076_2_worker_notebook", 60)

# COMMAND ----------

dbutils.notebook.run(
    "/Repos/GithubRepo/databricks_DE_01/076_3_worker_notebook_fail", 60
)

# com.databricks.WorkflowException: com.databricks.NotebookExecutionException: FAILED

# COMMAND ----------

# if you are running multiple notebooks in the master notebook and then you get error message then the next cell would not run. so you need to try and avoid that if possible
# one way to deal with that is to use try-except block in Python

# COMMAND ----------

try:
    dbutils.notebook.run(
        "/Repos/GithubRepo/databricks_DE_01/076_3_worker_notebook_fail", 60
    )
except:
    print("Error occured")

# COMMAND ----------


