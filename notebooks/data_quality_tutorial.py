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

# MAGIC %pip install zstandard requests beautifulsoup4 tqdm folium --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

import io
import zstandard as zstd
from pyspark.sql import SparkSession
import folium 
from pyspark.sql.functions import col, count, countDistinct, to_timestamp, min, max
from pyspark.databricks.sql import functions as dbf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

# COMMAND ----------

# Configuration - Update these values based on your environment
CATALOG = "dbacademy"
SCHEMA = "labuser12249714_1761120614"  # Replace with your schema (derived from username)
SOURCE_VOLUME = "landing"
TARGET_TABLE = "ais_data"

# Example file to process
EXAMPLE_FILE = "ais-2025-01-01.csv"

# Construct volume path
volume_path = f"/Volumes/{CATALOG}/{SCHEMA}/{SOURCE_VOLUME}"
file_path = f"{volume_path}/{EXAMPLE_FILE}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load CSV Data into Spark DataFrame
# MAGIC
# MAGIC Now we'll parse the decompressed CSV content into a Spark DataFrame.

# COMMAND ----------

# Read CSV into Spark DataFrame
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(file_path)

# Show schema and sample data
print("DataFrame Schema:")
df.printSchema()

print("\nSample Data (first 10 rows):")
display(df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Checks
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
    to_timestamp(col("base_date_time"), "yyyy-MM-dd'T'HH:mm:ss")
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
    min("latitude").alias("min_lat"),
    max("latitude").alias("max_lat"),
    min("longitude").alias("min_lon"),
    max("longitude").alias("max_lon")
).collect()[0]

print("Geographic bounds:")
print(f"  Latitude: {geo_stats['min_lat']:.4f} to {geo_stats['max_lat']:.4f}")
print(f"  Longitude: {geo_stats['min_lon']:.4f} to {geo_stats['max_lon']:.4f}")

# COMMAND ----------


m = folium.Map(location=[20,0], zoom_start=2)
folium.Rectangle([[0.5566, -174.5605], [50.1100, 157.8722]],
                 weight=2, fill=True, fill_opacity=0.15).add_to(m)
m  # renders in the notebook output


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

# MAGIC %md
# MAGIC ## 7. Spatial Data Processing & H3 Indexing
# MAGIC
# MAGIC Now we'll enhance the data with spatial types and H3 indices for geospatial analysis.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.1 Cast LAT/LON to Spatial Point Type
# MAGIC
# MAGIC We'll use Databricks' built-in spatial functions to create POINT geometries from latitude and longitude coordinates.
# MAGIC Note: ST_Point expects (longitude, latitude) order per WGS84 standard.

# COMMAND ----------

from pyspark.sql.functions import expr

# Read the Delta table we created
spatial_df = spark.table(full_table_name)

# Add spatial point column
# ST_Point(lon, lat) creates a POINT geometry in WGS84 (SRID 4326)
spatial_df = spatial_df.withColumn(
    "point_geom",
    expr("st_point(longitude, latitude, 4326)")
)

print("Added spatial point geometry column")

# COMMAND ----------

#write back down to delta table
spatial_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(full_table_name)

# COMMAND ----------

spark.sql(f"SELECT base_date_time,point_geom FROM {full_table_name} LIMIT 5").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.2 Validate Spatial Data
# MAGIC
# MAGIC Use ST_IsValid to check that our point geometries are valid.
# MAGIC This ensures the LAT/LON values create proper spatial objects.

# COMMAND ----------

# Add validation for each row - ST_IsValid Returns true if the input GEOMETRY value is a valid geometry in the OGC sense.
spatial_df = spatial_df.withColumn(
    "is_valid_geom",
    expr("ST_IsValid(point_geom)")
)

# COMMAND ----------

# Add the new column with a default value
spark.sql(
    f"""
    ALTER TABLE {full_table_name}
    ADD COLUMNS (is_valid_geom BOOLEAN)
    """
)

# Update the new column with the computed value
spark.sql(
    f"""
    UPDATE {full_table_name}
    SET is_valid_geom = ST_IsValid(point_geom)
    """
)

# COMMAND ----------

# check if there are any invalid rows 
counts_df = spark.sql(
    f"""
    SELECT
        is_valid_geom,
        COUNT(*) AS count
    FROM
        {full_table_name}
    GROUP BY
        is_valid_geom
    """
)
display(counts_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.3 Build H3 Spatial Indices
# MAGIC
# MAGIC Create H3 hexagonal indices at multiple resolutions for efficient spatial queries.
# MAGIC
# MAGIC H3 Resolution Reference:
# MAGIC - **Resolution 9**: ~174m average hexagon edge length (~0.10 km²) - Good for regional analysis
# MAGIC - **Resolution 10**: ~65m average hexagon edge length (~0.01 km²) - Good for local area analysis  
# MAGIC - **Resolution 11**: ~25m average hexagon edge length (~0.001 km²) - Good for precise location tracking

# COMMAND ----------

# Add new columns for H3 indices (remove IF NOT EXISTS)
spark.sql(
    f"""
    ALTER TABLE {full_table_name}
    ADD COLUMNS (
        h3_res9 STRING,
        h3_res10 STRING,
        h3_res11 STRING
    )
    """
)

# COMMAND ----------

spark.sql(
    f"""
    UPDATE {full_table_name}
    SET
        h3_res9 = h3_pointash3(st_astext(point_geom), 9),
        h3_res10 = h3_pointash3(st_astext(point_geom), 10),
        h3_res11 = h3_pointash3(st_astext(point_geom), 11)
    """
)

# COMMAND ----------

spark.sql(f"SELECT base_date_time,point_geom,h3_res9 FROM {full_table_name} LIMIT 5").show(truncate=False)