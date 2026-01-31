# Databricks notebook source
# DBTITLE 1,Data Sources Configuration for Analysis
datasets = [
    {"file_name": "orders"},
    {"file_name": "customers"},
    {"file_name": "products"},
]

# COMMAND ----------

# DBTITLE 1,Set Output Datasets for Job Task Execution
dbutils.jobs.taskValues.set("output_datasets", datasets)
