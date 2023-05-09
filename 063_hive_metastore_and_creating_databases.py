# Databricks notebook source
# The hive metastore allows you to treat objects in Databricks like Relational Databases
# These objects can be databases, tables and views

# COMMAND ----------

# The Hive Metastore is able to store metadata about the undelying data files the form the tables and views. This metadata can include location of the data as well as the data types.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS countries;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP DATABASE IF EXISTS countries;

# COMMAND ----------


