# Fetch All Records from Orders Silver Table in Spark
df = spark.sql("select * from databricks_cata.silver.orders_silver")
from configs.paths import abfss_path, checkpoint_path

# Load and Display Sample Data from DimCustomers Table
df_temp = spark.sql("select * from databricks_cata.gold.dimcustomers")
df_temp.limit(2).display()

# Retrieve Complete Data from DimProducts Gold Table
df_temp = spark.sql("select * from databricks_cata.gold.dimproducts")

# Retrieve Customer and Product IDs from Dimension Tables
df_dimcus = spark.sql("select DimCustomerKey, customer_id as dim_customer_id from databricks_cata.gold.dimcustomers")
df_dimpro = spark.sql("select product_id as DimProductKey, product_id as dim_product_id from databricks_cata.gold.dimproducts")

# Combine Customer and Product Data for Analysis
df_fact = df.join(df_dimcus, df.customer_id == df_dimcus.dim_customer_id, "left").join(df_dimpro, df.product_id == df_dimpro.dim_product_id, "left")
df_fact_new = df_fact.drop("dim_customer_id", "dim_product_id", "customer_id", "product_id")

# Import DeltaTable for Data Management in Spark
from delta.tables import DeltaTable

# Merge New Fact Orders Data into Delta Table in Spark
factorders_path = abfss_path("gold", "FactOrders")
if spark.catalog.tableExists("databricks_cata.gold.FactOrders"):
    dlt_obj = DeltaTable.forName(spark, "databricks_cata.gold.FactOrders")
    dlt_obj.alias("trg").merge(
        df_fact_new.alias("src"),
        "trg.order_id = src.order_id AND trg.DimCustomerKey = src.DimCustomerKey AND trg.DimProductKey = src.DimProductKey"
    ).whenMatchedUpdateAll()\
     .whenNotMatchedInsertAll()\
     .execute()
else:
    df_fact_new.write.format("delta")\
        .option("path", factorders_path)\
        .saveAsTable("databricks_cata.gold.FactOrders")