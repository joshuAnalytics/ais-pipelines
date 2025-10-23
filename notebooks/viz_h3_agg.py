# Databricks notebook source
# MAGIC %md
# MAGIC # H3 Hexagon Heatmap Visualization
# MAGIC
# MAGIC This notebook creates an animated pydeck visualization showing vessel activity by H3 hexagon and hour of day.
# MAGIC
# MAGIC **Features:**
# MAGIC * Reads aggregated data from Delta table
# MAGIC * Creates 2D hexagon heatmap colored by unique vessel count
# MAGIC * Animated time slider to view activity across 24 hours
# MAGIC * Interactive tooltips showing hour and vessel counts

# COMMAND ----------

# MAGIC %pip install pydeck h3 --quiet
dbutils.library.restartPython()

# COMMAND ----------

import pydeck as pdk
import pandas as pd
import h3
from pyspark.sql.functions import col

# COMMAND ----------

# Configuration - Update these to match your environment
CATALOG = "dbacademy"
SCHEMA = "labuser12249714_1761120614"  # Replace with your schema
TARGET_TABLE = "ais_data_sample"

# Aggregation table name
agg_table_name = f"{CATALOG}.{SCHEMA}.{TARGET_TABLE}_agg"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Aggregated Data
# MAGIC
# MAGIC Read the pre-aggregated data from the Delta table created in the data quality tutorial.

# COMMAND ----------

print(f"Loading aggregated data from: {agg_table_name}")

# Load aggregated data
agg_df = spark.table(agg_table_name)

print(f"Total records: {agg_df.count():,}")
print(f"Unique H3 cells: {agg_df.select('h3_res9').distinct().count():,}")
print(f"Hours of day: {agg_df.select('hour_of_day').distinct().count()}")

# Display sample
display(agg_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare Data for Visualization
# MAGIC
# MAGIC Convert to pandas and add color mapping based on vessel counts.

# COMMAND ----------

# Convert to pandas
agg_pdf = agg_df.toPandas()

print(f"Pandas DataFrame shape: {agg_pdf.shape}")
print(f"\nVessel count statistics:")
print(agg_pdf['unique_vessels'].describe())

# COMMAND ----------

# Normalize unique_vessels for color scaling (0-1 range)
max_vessels = agg_pdf['unique_vessels'].max()
min_vessels = agg_pdf['unique_vessels'].min()

print(f"Vessel count range: {min_vessels} to {max_vessels}")

agg_pdf['normalized_vessels'] = (agg_pdf['unique_vessels'] - min_vessels) / (max_vessels - min_vessels)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Color Mapping Function
# MAGIC
# MAGIC Map normalized vessel counts to a color gradient:
# MAGIC * Low activity: Blue
# MAGIC * Medium activity: Yellow
# MAGIC * High activity: Red

# COMMAND ----------

def get_color(normalized_value):
    """
    Convert normalized value (0-1) to RGB color.
    Blue (0) -> Yellow (0.5) -> Red (1.0)
    """
    if normalized_value < 0.5:
        # Blue to Yellow transition
        r = int(normalized_value * 2 * 255)
        g = int(normalized_value * 2 * 255)
        b = int((1 - normalized_value * 2) * 255)
    else:
        # Yellow to Red transition
        r = 255
        g = int((1 - (normalized_value - 0.5) * 2) * 255)
        b = 0
    
    return [r, g, b, 180]  # RGB + Alpha

# Apply color mapping
agg_pdf['color'] = agg_pdf['normalized_vessels'].apply(get_color)

print("Color mapping applied successfully")
print("\nSample with colors:")
print(agg_pdf[['h3_res9', 'hour_of_day', 'unique_vessels', 'normalized_vessels', 'color']].head())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculate View State
# MAGIC
# MAGIC Determine the center point and zoom level based on H3 cell locations.

# COMMAND ----------

# Get center coordinates from H3 cells
sample_h3 = agg_pdf['h3_res9'].iloc[0]
center_coords = h3.h3_to_geo(sample_h3)

# Calculate bounds from all H3 cells
all_coords = [h3.h3_to_geo(h3_idx) for h3_idx in agg_pdf['h3_res9'].unique()[:100]]  # Sample for performance
lats = [coord[0] for coord in all_coords]
lons = [coord[1] for coord in all_coords]

center_lat = sum(lats) / len(lats)
center_lon = sum(lons) / len(lons)

print(f"View center: ({center_lat:.4f}, {center_lon:.4f})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Pydeck Visualization
# MAGIC
# MAGIC Build an interactive H3 hexagon layer with time-based filtering.

# COMMAND ----------

# Create initial view state
view_state = pdk.ViewState(
    latitude=center_lat,
    longitude=center_lon,
    zoom=4,
    pitch=0,  # Top-down view (no 3D)
    bearing=0
)

print("View state created")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Interactive Visualization with Hour Selection
# MAGIC
# MAGIC Use a widget to select which hour to display.

# COMMAND ----------

# Create widget for hour selection
dbutils.widgets.dropdown("hour", "0", [str(h) for h in range(24)], "Hour of Day")

# Get selected hour
selected_hour = int(dbutils.widgets.get("hour"))

# Filter data for selected hour
hour_data = agg_pdf[agg_pdf['hour_of_day'] == selected_hour]

print(f"Displaying data for hour: {selected_hour}")
print(f"Number of hexagons: {len(hour_data)}")
print(f"Total unique vessels: {hour_data['unique_vessels'].sum()}")

# COMMAND ----------

# Create H3 hexagon layer
layer = pdk.Layer(
    'H3HexagonLayer',
    hour_data,
    get_hexagon='h3_res9',
    get_fill_color='color',
    opacity=0.6,
    pickable=True,
    auto_highlight=True,
    extruded=False  # Flat 2D hexagons
)

# Create deck
deck = pdk.Deck(
    layers=[layer],
    initial_view_state=view_state,
    tooltip={
        "html": "<b>H3 Index:</b> {h3_res9}<br/>"
                "<b>Hour:</b> {hour_of_day}<br/>"
                "<b>Unique Vessels:</b> {unique_vessels}<br/>"
                "<b>Total Records:</b> {total_records}",
        "style": {
            "backgroundColor": "steelblue",
            "color": "white"
        }
    }
)

# Display the map
deck.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Statistics by Hour
# MAGIC
# MAGIC View vessel activity trends across all hours.

# COMMAND ----------

# Aggregate statistics by hour
hourly_stats = agg_pdf.groupby('hour_of_day').agg({
    'unique_vessels': 'sum',
    'total_records': 'sum',
    'h3_res9': 'count'
}).reset_index()

hourly_stats.columns = ['hour_of_day', 'total_unique_vessels', 'total_records', 'num_hexagons']

print("Hourly Statistics:")
display(hourly_stats)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Usage Instructions
# MAGIC
# MAGIC **To animate through time:**
# MAGIC 1. Use the "Hour of Day" dropdown widget at the top of the notebook
# MAGIC 2. Select different hours (0-23) to see how vessel activity changes
# MAGIC 3. Re-run the visualization cell to update the map
# MAGIC
# MAGIC **Color Legend:**
# MAGIC * 🔵 Blue: Low vessel activity
# MAGIC * 🟡 Yellow: Medium vessel activity  
# MAGIC * 🔴 Red: High vessel activity
# MAGIC
# MAGIC **Interactive Features:**
# MAGIC * Hover over hexagons to see detailed statistics
# MAGIC * Use mouse to pan and zoom
# MAGIC * Click and drag to explore the map
