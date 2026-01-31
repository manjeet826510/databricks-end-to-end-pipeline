# List of datasets to be used, each represented as a dictionary with a file name
datasets = [
    {"file_name": "orders"},
    {"file_name": "customers"},
    {"file_name": "products"},
]

# Store the datasets list as a task value for use in other Databricks job tasks
dbutils.jobs.taskValues.set("output_datasets", datasets)