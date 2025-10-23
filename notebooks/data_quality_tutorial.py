# Databricks notebook source
# MAGIC %md
# MAGIC # AIS Data Quality Tutorial
# MAGIC
# MAGIC This notebook demonstrates:
# MAGIC * Loading CSV data into a Delta table
# MAGIC * Basic data quality checks
# MAGIC

# COMMAND ----------

# MAGIC %pip install -e ../ --quiet
dbutils.library.restartPython()

# COMMAND ----------

import folium
from pyspark.sql.functions import col, count, countDistinct, to_timestamp, min, max
from pyspark.databricks.sql import functions as dbf
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    TimestampType,
)

# COMMAND ----------

# Configuration - Update these values based on your environment
CATALOG = "dbacademy"
SCHEMA = (
    "labuser12249714_1761120614"  # Replace with your schema (derived from username)
)
SOURCE_VOLUME = "landing"
TARGET_TABLE = "ais_data_sample"

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
df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)

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
        print(
            f"  {col_name}: {null_count:,} nulls ({null_count / total_rows * 100:.1f}%)"
        )

# COMMAND ----------

# Check unique vessels (MMSI)
unique_mmsi = df.select(countDistinct("MMSI")).collect()[0][0]
print(f"Unique vessels (MMSI): {unique_mmsi:,}")

# Show timestamp range
df_with_timestamp = df.withColumn(
    "timestamp", to_timestamp(col("base_date_time"), "yyyy-MM-dd'T'HH:mm:ss")
)

timestamp_stats = df_with_timestamp.select(
    min("timestamp").alias("min_time"), max("timestamp").alias("max_time")
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
    max("longitude").alias("max_lon"),
).collect()[0]

print("Geographic bounds:")
print(f"  Latitude: {geo_stats['min_lat']:.4f} to {geo_stats['max_lat']:.4f}")
print(f"  Longitude: {geo_stats['min_lon']:.4f} to {geo_stats['max_lon']:.4f}")

# COMMAND ----------

# Calculate center from actual data bounds
center_lat = (geo_stats['min_lat'] + geo_stats['max_lat']) / 2
center_lon = (geo_stats['min_lon'] + geo_stats['max_lon']) / 2

# Create map centered on actual data with dynamic bounds
m = folium.Map(location=[center_lat, center_lon], zoom_start=2)
folium.Rectangle(
    [[geo_stats['min_lat'], geo_stats['min_lon']], 
     [geo_stats['max_lat'], geo_stats['max_lon']]], 
    weight=2, 
    fill=True, 
    fill_opacity=0.15
).add_to(m)
m  # renders in the notebook output


# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Delta Table
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

df_with_timestamp.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(full_table_name)

print(f"Successfully created Delta table: {full_table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spatial Data Processing & H3 Indexing
# MAGIC
# MAGIC Now we'll enhance the data with spatial types and H3 indices for geospatial analysis using a single SQL operation.
# MAGIC This approach lets Databricks infer the GEOMETRY type automatically from the ST_Point function.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add All Spatial Columns in One Operation
# MAGIC
# MAGIC We use CREATE OR REPLACE TABLE with SELECT to add all spatial columns at once:
# MAGIC - **point_geom**: POINT geometry created from lat/lon using ST_Point (longitude, latitude, SRID)
# MAGIC - **is_valid_geom**: Boolean validation using ST_IsValid
# MAGIC - **h3_res9/10/11**: H3 indices at multiple resolutions for spatial indexing
# MAGIC
# MAGIC H3 Resolution Reference:
# MAGIC - **Resolution 9**: ~174m average hexagon edge length (~0.10 km²) - Good for regional analysis
# MAGIC - **Resolution 10**: ~65m average hexagon edge length (~0.01 km²) - Good for local area analysis
# MAGIC - **Resolution 11**: ~25m average hexagon edge length (~0.001 km²) - Good for precise location tracking

# COMMAND ----------

print(f"Adding spatial columns to {full_table_name}...")

# Create table with all spatial columns using CREATE OR REPLACE TABLE AS SELECT
# This lets Databricks infer the GEOMETRY type from ST_Point automatically
spark.sql(f"""
    CREATE OR REPLACE TABLE {full_table_name} AS
    SELECT 
        *,
        ST_Point(longitude, latitude, 4326) AS point_geom,
        ST_IsValid(ST_Point(longitude, latitude, 4326)) AS is_valid_geom,
        h3_pointash3(ST_AsText(ST_Point(longitude, latitude, 4326)), 9) AS h3_res9,
        h3_pointash3(ST_AsText(ST_Point(longitude, latitude, 4326)), 10) AS h3_res10,
        h3_pointash3(ST_AsText(ST_Point(longitude, latitude, 4326)), 11) AS h3_res11
    FROM {full_table_name}
""")

print("Successfully added all spatial columns!")

# COMMAND ----------

# Verify all spatial columns were created correctly
print("Sample data with spatial columns:")
spark.sql(f"""
    SELECT 
        base_date_time,
        latitude,
        longitude,
        point_geom,
        is_valid_geom,
        h3_res9,
        h3_res10,
        h3_res11
    FROM {full_table_name} 
    LIMIT 5
""").show(truncate=False)

# COMMAND ----------

# Check if there are any invalid geometries
print("\nGeometry validation summary:")
counts_df = spark.sql(f"""
    SELECT
        is_valid_geom,
        COUNT(*) AS count
    FROM {full_table_name}
    GROUP BY is_valid_geom
""")

display(counts_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregate Data by H3 Resolution 9 and Hour of Day
# MAGIC
# MAGIC Group the data by H3 index (resolution 9) and hour of day to count unique vessels in each spatial-temporal bucket.

# COMMAND ----------

# Create aggregation query
aggregation_query = f"""
    SELECT 
        h3_res9,
        HOUR(timestamp) AS hour_of_day,
        COUNT(DISTINCT mmsi) AS unique_vessels,
        COUNT(*) AS total_records
    FROM {full_table_name}
    GROUP BY h3_res9, HOUR(timestamp)
    ORDER BY h3_res9, hour_of_day
"""

# Create aggregation table name
agg_table_name = f"{CATALOG}.{SCHEMA}.{TARGET_TABLE}_agg"

print(f"Creating aggregation table: {agg_table_name}")

# Execute aggregation and write to Delta table
spark.sql(aggregation_query).write.format("delta").mode("overwrite").saveAsTable(agg_table_name)

print(f"Successfully created aggregation table: {agg_table_name}")

# Display sample of aggregated data
print("\nSample of aggregated data:")
display(spark.table(agg_table_name).limit(20))
