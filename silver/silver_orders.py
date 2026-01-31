# Import PySpark Functions and Types for DataFrame Analysis
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from configs.paths import abfss_path, checkpoint_path

# Load and Display Orders Data from Parquet Format
df = spark.read.format("parquet")\
    .load(abfss_path("bronze", "orders"))

# Drop Rescued Data Column from DataFrame
df = df.withColumnRenamed("_rescued_data", "rescued_data")
df = df.drop("rescued_data")

# Transform Order Date and Extract Year in DataFrame
df = df.withColumn("order_date", to_timestamp(col('order_date')))
df = df.withColumn("year", year(col("order_date")))

# Rank Transactions by Total Amount per Year in DataFrame
df1 = df.withColumn("flag", dense_rank().over(Window.partitionBy("year").orderBy(desc("total_amount"))))

# Rank Transactions with Flags by Yearly Total Amounts
df1 = df1.withColumn("rank_flag", rank().over(Window.partitionBy("year").orderBy(desc("total_amount"))))

# Rank Transactions with Row Flags by Total Amount in DataFrame
df1 = df.withColumn("row_flag", row_number().over(Window.partitionBy("year").orderBy(desc("total_amount"))))

# Calculate Dense Rank, Rank, and Row Number by Year
class windows:
    def dense_rank(self, df):
        df_dense_rank = df.withColumn("flag", dense_rank().over(Window.partitionBy("year").orderBy(desc("total_amount"))))
        return df_dense_rank
    
    def rank(self, df):
        df_rank = df.withColumn("rank_flag", rank().over(Window.partitionBy("year").orderBy(desc("total_amount"))))
        return df_rank
    
    def row_number(self, df):
        df_row_number = df.withColumn("row_flag", row_number().over(Window.partitionBy("year").orderBy(desc("total_amount"))))
        return df_row_number

# Display DataFrame with New Variable Assignment
df_new = df

# Calculate Dense Rank for New DataFrame Transactions
obj = windows()
df_res = obj.dense_rank(df_new)

# Append Orders Data to Delta Lake in Azure Storage
df.write.format("delta").mode("overwrite").save(abfss_path("silver", "orders"))

# Create Silver Orders Table in Databricks Delta Format
spark.sql(f"""
CREATE TABLE IF NOT EXISTS databricks_cata.silver.orders_silver
USING DELTA
LOCATION '{abfss_path("silver", "orders")}'
""")