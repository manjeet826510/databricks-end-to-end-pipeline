from configs.paths import abfss_path, checkpoint_path

# Initialize file name input widget and retrieve its value
dbutils.widgets.text("file_name", "")
p_file_name = dbutils.widgets.get("file_name")

# Load Parquet data from Azure source
df = spark.read.format("parquet").load(
    abfss_path("source", p_file_name)
)
display(df)

# Stream data loading from Azure Parquet source files
df_stream = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option(
            "cloudFiles.schemaLocation",
            checkpoint_path("bronze", p_file_name)
        )
        .load(
            abfss_path("source", p_file_name)
        )
)

# Stream Parquet write to Azure with checkpointing setup
df_stream.writeStream.format("parquet")\
    .option("checkpointLocation", checkpoint_path("bronze", p_file_name))\
    .option("path", abfss_path("bronze", p_file_name))\
    .trigger(once=True)\
    .start()

# Read and display Parquet data from Azure storage path
df_bronze = spark.read.format("parquet").load(abfss_path("bronze", p_file_name))
display(df_bronze)