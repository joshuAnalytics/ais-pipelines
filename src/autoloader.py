# autoloader.py - Auto Loader consumer for Databricks
# This script sets up a streaming read from the landing volume and writes to a Delta table

from pyspark.sql.types import *
from pyspark.sql.functions import to_timestamp

# Define schema for CSV files
schema = StructType([
    StructField("event_time", StringType()),
    StructField("user_id", StringType()),
    StructField("action", StringType()),
    StructField("value", StringType()),
])

# Configuration (update these to match your environment)
catalog = "main"
schema_name = "streaming"
landing_volume = "landing"

# Construct paths
landing_path = f"/Volumes/{catalog}/{schema_name}/{landing_volume}"
schema_location = f"{landing_path}/_schemas/events"
checkpoint_location = f"{landing_path}/_checkpoints/events"
target_table = "main.events_raw"

# Read stream using Auto Loader (cloudFiles)
df = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("cloudFiles.schemaLocation", schema_location)
      .option("header", "true")
      .schema(schema)
      .load(landing_path))

# Transform: convert event_time to timestamp
df = df.withColumn("event_ts", to_timestamp("event_time"))

# Write stream to Delta table
(df.writeStream
   .format("delta")
   .option("checkpointLocation", checkpoint_location)
   .toTable(target_table))
