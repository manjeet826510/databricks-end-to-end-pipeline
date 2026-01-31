# Import required paths from configs
from configs.paths import abfss_path, checkpoint_path

# Load and Display Regions Data from Bronze Table
df = spark.read.table("databricks_cata.bronze.regions")
df = df.drop("_rescued_data")

# Save Regions DataFrame to Delta Format in Overwrite Mode
df.write.format("delta").mode("overwrite").save(abfss_path("silver", "regions"))

# Load and Display Regions Data from Delta Format
df1 = spark.read.format("delta").load(abfss_path("silver", "regions"))
display(df1)

# Create Silver Table for Regions Data in Delta Format
spark.sql(f"""
CREATE TABLE IF NOT EXISTS databricks_cata.silver.regions_silver
USING DELTA
LOCATION '{abfss_path("silver", "regions")}'
""")