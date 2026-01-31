# Import required modules for Lakeflow Declarative Pipelines
import dlt
from pyspark.sql.functions import *

# Data quality expectations for products
my_rules = {
    "rule1": "product_id IS NOT NULL",
    "rule2": "product_name IS NOT NULL",
}

# Stage table for products with expectations applied
@dlt.table()
@dlt.expect_all_or_drop(my_rules)
def DimProducts_stage():
    df = spark.readStream.table("databricks_cata.silver.products_silver")
    return df

# View for staged products
@dlt.view()
def DimProducts_view():
    df = spark.readStream.table("Live.DimProducts_stage")
    return df

# Create the target streaming table for products
dlt.create_streaming_table("DimProducts")

# Apply changes from the staged view to the target table using Auto CDC (SCD Type 2)
dlt.apply_changes(
    target="DimProducts",
    source="Live.DimProducts_view",
    keys=["product_id"],
    sequence_by="product_id",
    stored_as_scd_type=2
)