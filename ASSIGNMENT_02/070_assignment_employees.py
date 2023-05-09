# Databricks notebook source
# Mount the 'employees' container from ADLS to the DBFS - ideally use secret scopes to ensure that your IDs and secrets are not exposed

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list("adls_to_databricks_new01")

# COMMAND ----------

application_id = dbutils.secrets.get("adls_to_databricks_new01", "app-id")
client_id = dbutils.secrets.get("adls_to_databricks_new01", "dir-id")
secret = dbutils.secrets.get("adls_to_databricks_new01", "secret-value")

# COMMAND ----------

configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": application_id,
    "fs.azure.account.oauth2.client.secret": secret,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{client_id}/oauth2/token",
}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
    source="abfss://employees@00x00dlorders.dfs.core.windows.net/bronze",
    mount_point="/mnt/employees/bronze",
    extra_configs=configs,
)

# COMMAND ----------

configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": application_id,
    "fs.azure.account.oauth2.client.secret": secret,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{client_id}/oauth2/token",
}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
    source="abfss://employees@00x00dlorders.dfs.core.windows.net/silver",
    mount_point="/mnt/employees/silver",
    extra_configs=configs,
)

# COMMAND ----------

configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": application_id,
    "fs.azure.account.oauth2.client.secret": secret,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{client_id}/oauth2/token",
}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
    source="abfss://employees@00x00dlorders.dfs.core.windows.net/gold",
    mount_point="/mnt/employees/gold",
    extra_configs=configs,
)

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

display(dbutils.fs.ls("/mnt/employees"))

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/mnt/employees/bronze/"))

# COMMAND ----------

# Adding Parquet files to the "silver" folder of the "employees" container

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DateType

# COMMAND ----------

employees_path = "dbfs:/mnt/employees/bronze/employees.csv"

employees_schema = StructType(
    [
        StructField("EMPLOYEE_ID", IntegerType(), False),
        StructField("FIRST_NAME", StringType(), False),
        StructField("LAST_NAME", StringType(), False),
        StructField("EMAIL", StringType(), False),
        StructField("PHONE_NUMBER", StringType(), False),
        StructField("HIRE_DATE", StringType(), False),
        StructField("JOB_ID", StringType(), False),
        StructField("SALARY", IntegerType(), False),
        StructField("MANAGER_ID", IntegerType(), True),
        StructField("DEPARTMENT_ID", IntegerType(), False),
    ]
)

employees = spark.read.csv(employees_path, header=True, schema=employees_schema)

# COMMAND ----------

employees.printSchema()

# COMMAND ----------

employees.display()

# COMMAND ----------

employees = employees.drop("EMAIL", "PHONE_NUMBER")

# COMMAND ----------

employees.display()

# COMMAND ----------

from pyspark.sql.functions import to_date

employees = employees.select(
    "EMPLOYEE_ID",
    "FIRST_NAME",
    "LAST_NAME",
    to_date("HIRE_DATE", "MM/dd/yyyy").alias("HIRE_DATE"),
    "JOB_ID",
    "SALARY",
    "MANAGER_ID",
    "DEPARTMENT_ID",
)

# COMMAND ----------

employees.display()

# COMMAND ----------

employees.write.parquet("dbfs:/mnt/employees/silver/employees", mode="overwrite")

# COMMAND ----------

departments_path = "dbfs:/mnt/employees/bronze/departments.csv"

departments_schema = StructType(
    [
        StructField("DEPARTMENT_ID", IntegerType(), False),
        StructField("DEPARTMENT_NAME", StringType(), False),
        StructField("MANAGER_ID", IntegerType(), False),
        StructField("LOCATION_ID", IntegerType(), False),
    ]
)

departments = spark.read.csv(departments_path, schema=departments_schema, header=True)

# COMMAND ----------

departments.printSchema()

# COMMAND ----------

departments.display()

# COMMAND ----------

departments = departments.drop("MANAGER_ID", "LOCATION_ID")

# COMMAND ----------

departments.display()

# COMMAND ----------

departments.write.mode("overwrite").parquet("dbfs:/mnt/employees/silver/departments")

# COMMAND ----------

countries_path = "dbfs:/mnt/employees/bronze/countries.csv"

countries_schema = StructType(
    [
        StructField("COUNTRY_ID", StringType(), False),
        StructField("COUNTRY_NAME", StringType(), False),
    ]
)

countries = (
    spark.read.option("header", True)
    .schema(countries_schema)
    .format("csv")
    .load(countries_path)
)

# COMMAND ----------

countries.printSchema()

# COMMAND ----------

countries.display()

# COMMAND ----------

countries.write.parquet("dbfs:/mnt/employees/silver/countries", mode="overwrite")

# COMMAND ----------

# Adding Parquet files to the "gold" folder of the "employees" container

# COMMAND ----------

employees = spark.read.parquet("dbfs:/mnt/employees/silver/employees")

# COMMAND ----------

display(employees)

# COMMAND ----------

from pyspark.sql.functions import concat_ws

employees = employees.withColumn("FULL_NAME", concat_ws(" ", "FIRST_NAME", "LAST_NAME"))

# COMMAND ----------

display(employees)

# COMMAND ----------

employees = employees.drop("FIRST_NAME", "LAST_NAME", "MANAGER_ID")

# COMMAND ----------

display(employees)

# COMMAND ----------

departments = spark.read.parquet("dbfs:/mnt/employees/silver/departments")

# COMMAND ----------

departments.display()

# COMMAND ----------

employees = employees.join(
    departments, employees["DEPARTMENT_ID"] == departments["DEPARTMENT_ID"], "left"
).select("EMPLOYEE_ID", "FULL_NAME", "HIRE_DATE", "JOB_ID", "SALARY", "DEPARTMENT_NAME")

# COMMAND ----------

employees.display()

# COMMAND ----------

employees.write.parquet("dbfs:/mnt/employees/gold/employees")

# COMMAND ----------

# Create the "employees" database and loading the gold layer table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE employees.employees(
# MAGIC   EMPLOYEE_ID INT,
# MAGIC   FULL_NAME STRING,
# MAGIC   HIRE_DATE DATE,
# MAGIC   JOB_ID STRING,
# MAGIC   SALARY INT,
# MAGIC   DEPARTMENT_ID STRING
# MAGIC ) USING PARQUET LOCATION "dbfs:/mnt/employees/gold/employees"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   employees.employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC EXTENDED employees.employees;

# COMMAND ----------


