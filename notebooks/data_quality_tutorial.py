# Databricks notebook source
# MAGIC %md
# MAGIC # AIS Data Quality Tutorial
# MAGIC
# MAGIC This notebook demonstrates:
# MAGIC 1. Decompressing `.csv.zst` files from the Unity Catalog volume
# MAGIC 2. Loading CSV data into a Delta table
# MAGIC 3. Basic data quality checks
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Files downloaded to Unity Catalog volume (via `download_ais` job)
# MAGIC - Python wheel built and uploaded via Databricks Asset Bundle

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Install Dependencies
# MAGIC
# MAGIC Install required Python packages for decompression and data processing.

# COMMAND ----------

%pip install zstandard requests beautifulsoup4 tqdm --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup and Configuration

# COMMAND ----------

import io
import zstandard as zstd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, to_timestamp, min, max
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

# COMMAND ----------

# Configuration - Update these values based on your environment
CATALOG = "dbacademy"
SCHEMA = "your_schema_name"  # Replace with your schema (derived from username)
SOURCE_VOLUME = "full_history"
TARGET_TABLE = "ais_data"

# Example file to process
EXAMPLE_FILE = "ais-2025-01-01.csv.zst"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Verify File Exists

# COMMAND ----------

# Construct volume path
volume_path = f"/Volumes/{CATALOG}/{SCHEMA}/{SOURCE_VOLUME}"
file_path = f"{volume_path}/{EXAMPLE_FILE}"

print(f"Looking for file: {file_path}")

# List files in the volume to verify
try:
    files = dbutils.fs.ls(volume_path)
    print(f"\nFound {len(files)} files in volume:")
    for f in files[:5]:  # Show first 5 files
        print(f"  - {f.name} ({f.size:,} bytes)")
    if len(files) > 5:
        print(f"  ... and {len(files) - 5} more files")
except Exception as e:
    print(f"Error listing volume: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Decompress the .zst File
# MAGIC
# MAGIC The `.csv.zst` files are compressed using Zstandard compression. We'll decompress them in memory and read the CSV data.

# COMMAND ----------

def decompress_zst_file(file_path: str) -> str:
    """
    Decompress a .zst file and return the decompressed content as a string.
    
    Args:
        file_path: Path to the .zst file in Unity Catalog volume
        
    Returns:
        Decompressed content as string
    """
    # Read the compressed file
    with open(file_path, 'rb') as compressed_file:
        compressed_data = compressed_file.read()
    
    # Decompress using zstandard
    dctx = zstd.ZstdDecompressor()
    decompressed_data = dctx.decompress(compressed_data)
    
    # Convert bytes to string
    return decompressed_data.decode('utf-8')

# COMMAND ----------

# Decompress the file
print(f"Decompressing {EXAMPLE_FILE}...")
decompressed_content = decompress_zst_file(file_path)

# Show file size information
compressed_size = dbutils.fs.ls(file_path)[0].size
decompressed_size = len(decompressed_content)
compression_ratio = compressed_size / decompressed_size * 100

print(f"Compressed size: {compressed_size:,} bytes")
print(f"Decompressed size: {decompressed_size:,} bytes")
print(f"Compression ratio: {compression_ratio:.1f}%")
print(f"\nFirst 500 characters of decompressed data:")
print(decompressed_content[:500])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Load CSV Data into Spark DataFrame
# MAGIC
# MAGIC Now we'll parse the decompressed CSV content into a Spark DataFrame.

# COMMAND ----------

# Define the schema for AIS data
# Note: Actual AIS schema may vary - adjust based on your data
ais_schema = StructType([
    StructField("MMSI", IntegerType(), True),
    StructField("BaseDateTime", StringType(), True),
    StructField("LAT", DoubleType(), True),
    StructField("LON", DoubleType(), True),
    StructField("SOG", DoubleType(), True),  # Speed over ground
    StructField("COG", DoubleType(), True),  # Course over ground
    StructField("Heading", DoubleType(), True),
    StructField("VesselName", StringType(), True),
    StructField("IMO", StringType(), True),
    StructField("CallSign", StringType(), True),
    StructField("VesselType", IntegerType(), True),
    StructField("Status", StringType(), True),
    StructField("Length", DoubleType(), True),
    StructField("Width", DoubleType(), True),
    StructField("Draft", DoubleType(), True),
    StructField("Cargo", IntegerType(), True),
])

# COMMAND ----------

# Create Spark DataFrame from CSV string
# We'll use StringIO to treat the string as a file-like object
from io import StringIO

# For demonstration, we can read directly from the CSV file using Spark's built-in CSV reader
# But first, we need to write the decompressed content to a temporary location
temp_path = f"{volume_path}/temp_decompressed.csv"

# Write decompressed content to temp file
with open(temp_path, 'w') as f:
    f.write(decompressed_content)

print(f"Wrote decompressed data to: {temp_path}")

# COMMAND ----------

# Read CSV into Spark DataFrame
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(temp_path)

# Show schema and sample data
print("DataFrame Schema:")
df.printSchema()

print("\nSample Data (first 10 rows):")
display(df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Data Quality Checks
# MAGIC
# MAGIC Perform basic data quality checks before loading to Delta table.

# COMMAND ----------

# Get basic statistics
total_rows = df.count()
print(f"Total rows: {total_rows:,}")

# Check for nulls in critical columns
print("\nNull counts by column:")
null_counts = df.select([count(col(c)).alias(c) for c in df.columns]).collect()[0]
for col_name in df.columns:
    null_count = total_rows - null_counts[col_name]
    if null_count > 0:
        print(f"  {col_name}: {null_count:,} nulls ({null_count/total_rows*100:.1f}%)")

# COMMAND ----------

# Check unique vessels (MMSI)
unique_mmsi = df.select(countDistinct("MMSI")).collect()[0][0]
print(f"Unique vessels (MMSI): {unique_mmsi:,}")

# Show timestamp range
df_with_timestamp = df.withColumn(
    "timestamp",
    to_timestamp(col("BaseDateTime"), "yyyy-MM-dd'T'HH:mm:ss")
)

timestamp_stats = df_with_timestamp.select(
    min("timestamp").alias("min_time"),
    max("timestamp").alias("max_time")
).collect()[0]

print(f"\nTimestamp range:")
print(f"  Earliest: {timestamp_stats['min_time']}")
print(f"  Latest: {timestamp_stats['max_time']}")

# COMMAND ----------

# Geographic bounds check
geo_stats = df.select(
    min("LAT").alias("min_lat"),
    max("LAT").alias("max_lat"),
    min("LON").alias("min_lon"),
    max("LON").alias("max_lon")
).collect()[0]

print("Geographic bounds:")
print(f"  Latitude: {geo_stats['min_lat']:.4f} to {geo_stats['max_lat']:.4f}")
print(f"  Longitude: {geo_stats['min_lon']:.4f} to {geo_stats['max_lon']:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Create Delta Table
# MAGIC
# MAGIC Now we'll write the data to a Delta table in Unity Catalog.

# COMMAND ----------

# Ensure schema exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# Create full table name
full_table_name = f"{CATALOG}.{SCHEMA}.{TARGET_TABLE}"

# COMMAND ----------

# Write to Delta table
# Using overwrite mode for this example - use append for incremental loads
print(f"Writing data to Delta table: {full_table_name}")

df_with_timestamp.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(full_table_name)

print(f"Successfully created Delta table: {full_table_name}")

# COMMAND ----------

# Verify the table was created
print("Table information:")
spark.sql(f"DESCRIBE EXTENDED {full_table_name}").show(truncate=False)

# COMMAND ----------

# Query the table
print(f"Sample query from {full_table_name}:")
result_df = spark.sql(f"""
    SELECT 
        MMSI,
        timestamp,
        LAT,
        LON,
        SOG,
        VesselName
    FROM {full_table_name}
    ORDER BY timestamp
    LIMIT 10
""")

display(result_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Cleanup
# MAGIC
# MAGIC Remove temporary files.

# COMMAND ----------

# Remove the temporary decompressed file
try:
    dbutils.fs.rm(temp_path)
    print(f"Cleaned up temporary file: {temp_path}")
except Exception as e:
    print(f"Error cleaning up temp file: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC This notebook demonstrated:
# MAGIC 1. ✅ Decompressing `.csv.zst` files using zstandard
# MAGIC 2. ✅ Loading CSV data into Spark DataFrames
# MAGIC 3. ✅ Performing data quality checks
# MAGIC 4. ✅ Creating Delta tables in Unity Catalog
# MAGIC
# MAGIC ### Next Steps
# MAGIC - Implement incremental loading for multiple files
# MAGIC - Add more comprehensive data quality checks
# MAGIC - Set up data quality monitoring
# MAGIC - Create views or derived tables for analysis

# COMMAND ----------
