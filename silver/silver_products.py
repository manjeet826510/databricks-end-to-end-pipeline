# Importing PySpark SQL Functions and Data Types
from pyspark.sql.functions import *
from pyspark.sql.types import *
from configs.paths import abfss_path, checkpoint_path

# Load and Display Products Data from Parquet Source
df = spark.read.format("parquet")\
    .load(abfss_path("bronze", "products"))

# Remove Unneeded Column and Display DataFrame
df = df.drop("_rescued_data")

# Create Temporary View for Products DataFrame
df.createOrReplaceTempView("products")

# Set Current Catalog and Database in Spark SQL
spark.sql("USE CATALOG databricks_cata")
spark.sql("USE bronze")

# Compute Discounted Prices with Price Adjustment Function
df = df.withColumn("discounted_price", expr("discount_func(price)"))

# Append Products Data to Delta Format in Azure Storage
df.write.format("delta").mode("overwrite").save(abfss_path("silver", "products"))

# Create Delta Table for Products in Databricks Silver Layer
spark.sql(f"""
CREATE TABLE IF NOT EXISTS databricks_cata.silver.products_silver
USING DELTA
LOCATION '{abfss_path("silver", "products")}'
""")