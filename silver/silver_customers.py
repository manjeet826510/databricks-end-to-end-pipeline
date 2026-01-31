# Importing Config Paths for PySpark Data Processing
from pyspark.sql.functions import *
from pyspark.sql.types import *
from configs.paths import abfss_path, checkpoint_path

# Loading Customer Data from Parquet Format
df = spark.read.format("parquet")\
    .load(abfss_path("bronze", "customers"))

# Cleaning DataFrame by Removing Rescued Data Column
df = df.drop("_rescued_data")

# Extracting Domain from Email Addresses in DataFrame
df = df.withColumn("domains", split(col("email"), "@")[1])

# Customer Count by Domain in Descending Order
display(
    df.groupBy("domains")
      .agg(count("customer_id").alias("total_customers"))
      .sort(desc("total_customers"))
)

# Filtering DataFrame for Gmail, Yahoo, and Hotmail Domains
df_gmail = df.filter(col("domains")=="gmail.com")
df_yahoo = df.filter(col("domains")=="yahoo.com")
df_hotmail = df.filter(col("domains")=="hotmail.com")

# Concatenating First and Last Names into Full Name Column
df = df.withColumn("full_name", concat(col("first_name"), lit(" "), col("last_name")))
df = df.drop("first_name", "last_name")

# Saving DataFrame to Delta Format in Azure Storage
df.write.format("delta").mode("overwrite").save(abfss_path("silver", "customers"))

# Creating Silver Schema in Databricks Catalog
spark.sql("CREATE SCHEMA IF NOT EXISTS databricks_cata.silver")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS databricks_cata.silver.customers_silver
USING DELTA
LOCATION '{abfss_path("silver", "customers")}'
""")