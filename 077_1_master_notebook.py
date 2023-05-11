# Databricks notebook source
dbutils.widgets.help()

# dbutils.widgets provides utilities for working with notebook widgets. You can create different types of widgets and get their bound value. For more info about a method, use dbutils.widgets.help("methodName").
# combobox(name: String, defaultValue: String, choices: Seq, label: String): void -> Creates a combobox input widget with a given name, default value and choices
# dropdown(name: String, defaultValue: String, choices: Seq, label: String): void -> Creates a dropdown input widget a with given name, default value and choices
# get(name: String): String -> Retrieves current value of an input widget
# getArgument(name: String, optional: String): String -> (DEPRECATED) Equivalent to get
# multiselect(name: String, defaultValue: String, choices: Seq, label: String): void -> Creates a multiselect input widget with a given name, default value and choices
# remove(name: String): void -> Removes an input widget from the notebook
# removeAll: void -> Removes all widgets in the notebook
# text(name: String, defaultValue: String, label: String): void -> Creates a text input widget with a given name and default value

# COMMAND ----------

dbutils.notebook.run(
    "/Repos/GithubRepo/databricks_DE_01/077_2_worker_notebook",
    60,
    {"input_widget": "From Master Notebook"},
)
