# Import Libraries for Data Processing and Analysis
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from configs.paths import abfss_path, checkpoint_path

# Initialize Load Flag from Widget Input
init_load_flag = int(dbutils.widgets.get("init_load_flag"))

# Retrieve All Customer Data from Silver Layer
df = spark.sql("select * from databricks_cata.silver.customers_silver")

# Remove Duplicate Customer Entries from DataFrame
df = df.dropDuplicates(subset = ['customer_id'])

# Add Row Numbers to Customer DataFrame for Indexing
df = df.withColumn("DimCustomerKey", row_number().over(Window.orderBy("customer_id")))

# Load Customer Data Based on Initialization Flag
if init_load_flag == 0:
    df_old = spark.sql("select DimCustomerKey, customer_id, create_date, update_date from databricks_cata.gold.DimCustomers")
else:
    df_old = spark.sql("select 0 DimCustomerKey, 0 customer_id, 0 create_date, 0 update_date from databricks_cata.silver.customers_silver where 1=0")

# Rename Customer Columns for DataFrame Consistency
df_old = df_old.withColumnRenamed("DimCustomerKey", "old_DimCustomerKey")\
                .withColumnRenamed("customer_id", "old_customer_id")\
                .withColumnRenamed("create_date", "old_create_date")\
                .withColumnRenamed("update_date", "old_update_date")

# Join Current Customer Data with Historical Records
df_join = df.join(df_old, df.customer_id == df_old.old_customer_id, 'left')

# Filter Customer Data into New and Old Dimensional Sets
df_new = df_join.filter(df_join['old_DimCustomerKey'].isNull())
df_old = df_join.filter(df_join['old_DimCustomerKey'].isNotNull())

# Clean and Update Customer DataFrame Columns and Timestamps for Old Records
df_old = df_old.drop("old_customer_id", "old_DimCustomerKey", "old_update_date")
df_old = df_old.withColumnRenamed("old_create_date", "create_date")
df_old = df_old.withColumn("create_date", to_timestamp(col("create_date")))
df_old = df_old.withColumn("update_date", current_timestamp())

# Update Timestamps and Clean Unnecessary Data Columns for New Records
df_new = df_new.drop("old_customer_id", "old_DimCustomerKey", "old_update_date", "old_create_date")
df_new = df_new.withColumn("create_date", current_timestamp())
df_new = df_new.withColumn("update_date", current_timestamp())

# Generate Unique Customer Index with Row Numbering for New Records
df_new = df_new.withColumn("DimCustomerKey", row_number().over(Window.orderBy("customer_id")))

# Determine Max Surrogate Key from Customer Data
if init_load_flag == 1:
    max_surrogate_key = 0
else:
    df_maxsur = spark.sql("select max(DimCustomerKey) as max_surrogate_key from databricks_cata.gold.DimCustomers")
    max_surrogate_key = df_maxsur.collect()[0]['max_surrogate_key']

# Update DimCustomerKey with Surrogate Key Offset
df_new = df_new.withColumn("DimCustomerKey", col("DimCustomerKey") + max_surrogate_key)

# Combine New and Old DataFrames for Final Output
df_final = df_new.unionByName(df_old)
df_final.limit(2).display()

# Merge or Insert Customer Data into Gold Layer Table
from delta.tables import DeltaTable

dimcustomers_path = abfss_path("gold", "DimCustomers")

if spark.catalog.tableExists("databricks_cata.gold.DimCustomers"):
    dlt_obj = DeltaTable.forPath(spark, dimcustomers_path)
    dlt_obj.alias("trg").merge(df_final.alias("src"), "trg.DimCustomerKey = src.DimCustomerKey")\
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
            .execute()
else:
    df_final.write\
            .mode("overwrite")\
            .option("path", dimcustomers_path)\
            .saveAsTable("databricks_cata.gold.DimCustomers")