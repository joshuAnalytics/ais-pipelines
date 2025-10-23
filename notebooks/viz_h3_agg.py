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

# MAGIC %pip install -e ../ --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import numpy as np
import pydeck as pdk
import pandas as pd
import h3
from pyspark.sql.functions import col

# COMMAND ----------

# Configuration - Update these values to match your environment
CATALOG = "dbacademy"
SCHEMA = "labuser12249714_1761120614"  # Replace with your schema
TARGET_TABLE = "ais_data_sample"

# Resolution selection - Choose: 6, 7, 8, or 9
SELECTED_RESOLUTION = 6

print(f"Using H3 Resolution: {SELECTED_RESOLUTION}")
print(f"\nResolution details:")
resolution_info = {
    6: "~36 km² per hex - Continental/ocean-wide patterns",
    7: "~5 km² per hex - Regional shipping lanes",
    8: "~0.7 km² per hex - Port areas and coastal zones",
    9: "~0.1 km² per hex - Detailed vessel movements"
}
print(f"  {resolution_info[SELECTED_RESOLUTION]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load and Aggregate Data
# MAGIC
# MAGIC Read the pre-aggregated hourly data and aggregate across all 24 hours to show total daily activity.

# COMMAND ----------

# Construct table name based on selected resolution
agg_table_name = f"{CATALOG}.{SCHEMA}.{TARGET_TABLE}_agg_res{SELECTED_RESOLUTION}"
h3_column = f"h3_res{SELECTED_RESOLUTION}"

print(f"Loading aggregated data from: {agg_table_name}")

# Load hourly aggregated data
hourly_agg_df = spark.table(agg_table_name)

print(f"Total hourly records: {hourly_agg_df.count():,}")
print(f"Unique H3 cells: {hourly_agg_df.select(h3_column).distinct().count():,}")
print(f"Hours of day: {hourly_agg_df.select('hour_of_day').distinct().count()}")

# Aggregate across all hours to get daily totals per hexagon
from pyspark.sql.functions import sum as _sum

agg_df = hourly_agg_df.groupBy(h3_column).agg(
    _sum('unique_vessels').alias('total_unique_vessels'),
    _sum('total_records').alias('total_records')
)

print(f"\nDaily aggregated records: {agg_df.count():,}")
print(f"Sample of daily aggregated data:")
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
print(f"\nDaily vessel count statistics:")
print(agg_pdf['total_unique_vessels'].describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Color Mapping Function
# MAGIC
# MAGIC Map normalized vessel counts to a fire color gradient:
# MAGIC * Low activity: Yellow
# MAGIC * Medium activity: Orange
# MAGIC * High activity: Red

# COMMAND ----------

def get_color(normalized_value):
    """
    Fire colormap: yellow -> orange -> red
    Low activity: Yellow [255, 255, 0]
    Medium activity: Orange [255, 165, 0]
    High activity: Red [255, 0, 0]
    """
    if normalized_value < 0.5:
        # Yellow to Orange (first half)
        # Interpolate between yellow [255, 255, 0] and orange [255, 165, 0]
        t = normalized_value * 2  # Scale 0-0.5 to 0-1
        r = 255
        g = int(255 - (90 * t))  # 255 -> 165
        b = 0
    else:
        # Orange to Red (second half)
        # Interpolate between orange [255, 165, 0] and red [255, 0, 0]
        t = (normalized_value - 0.5) * 2  # Scale 0.5-1 to 0-1
        r = 255
        g = int(165 * (1 - t))  # 165 -> 0
        b = 0
    
    return [r, g, b, 255]  # Full opacity

# Transform data: use log scale for better color distribution
agg_pdf['log_vessels'] = np.log1p(agg_pdf['total_unique_vessels'])
agg_pdf['normalized_vessels'] = (agg_pdf['log_vessels'] - agg_pdf['log_vessels'].min()) / (agg_pdf['log_vessels'].max() - agg_pdf['log_vessels'].min())

# Apply color mapping
agg_pdf['color'] = agg_pdf['normalized_vessels'].apply(get_color)

print("Fire colormap applied successfully")
print(f"\nVessel count range: {agg_pdf['total_unique_vessels'].min()} to {agg_pdf['total_unique_vessels'].max()}")
print("\nSample with colors:")
print(agg_pdf[[h3_column, 'total_unique_vessels', 'normalized_vessels', 'color']].head())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculate View State
# MAGIC
# MAGIC Determine the center point and zoom level based on H3 cell locations.

# COMMAND ----------

# Convert h3 indices from integer to hex string format (15 characters, zero-padded)
h3_hex_column = f"{h3_column}_hex"
agg_pdf[h3_hex_column] = agg_pdf[h3_column].apply(lambda x: format(x, '015x'))

# Get center coordinates from H3 cells
sample_h3 = agg_pdf[h3_hex_column].iloc[0]
center_coords = h3.cell_to_latlng(sample_h3)

# Calculate bounds from all H3 cells
all_coords = [h3.cell_to_latlng(h3_idx) for h3_idx in agg_pdf[h3_hex_column].unique()]

# Sample for performance
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

# Before creating the view_state, check where your data is:
print(f"Center calculated: lat={center_lat:.4f}, lon={center_lon:.4f}")
print(f"Sample H3: {sample_h3}")
print(f"Sample coords: {center_coords}")

# COMMAND ----------

# Determine appropriate zoom level based on resolution
zoom_levels = {
    6: 4,
    7: 5,
    8: 5.5,
    9: 6
}
zoom_level = zoom_levels.get(SELECTED_RESOLUTION, 5)

# Create initial view state
view_state = pdk.ViewState(
    latitude=center_lat,
    longitude=center_lon,
    zoom=zoom_level,
    pitch=0,  # Top-down view (no 3D)
    bearing=0
)

print(f"View state created with zoom level: {zoom_level}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Interactive Visualization
# MAGIC
# MAGIC Display daily vessel activity aggregated across all 24 hours.

# COMMAND ----------

print(f"Displaying total daily activity (all 24 hours aggregated)")
print(f"Number of hexagons: {len(agg_pdf)}")
print(f"Total unique vessels: {agg_pdf['total_unique_vessels'].sum()}")

# COMMAND ----------

# Create H3 hexagon layer
layer = pdk.Layer(
    'H3HexagonLayer',
    agg_pdf,
    get_hexagon=h3_hex_column,
    get_fill_color='color',
    opacity=0.9,
    pickable=True,
    auto_highlight=True,
    extruded=False  # Flat 2D hexagons
)

# Create deck
deck = pdk.Deck(
    layers=[layer],
    initial_view_state=view_state,
    map_style='dark',
    tooltip={
        "html": f"<b>H3 Index (res {SELECTED_RESOLUTION}):</b> {{{h3_column}}}<br/>"
                "<b>Total Unique Vessels (24h):</b> {total_unique_vessels}<br/>"
                "<b>Total Records (24h):</b> {total_records}",
        "style": {
            "backgroundColor": "steelblue",
            "color": "white"
        }
    }
)

# Display the map
deck.show()

# COMMAND ----------

print(f"Min vessels: {agg_pdf['total_unique_vessels'].min()}")
print(f"Max vessels: {agg_pdf['total_unique_vessels'].max()}")
print(f"Mean vessels: {agg_pdf['total_unique_vessels'].mean():.1f}")
print(f"\nNormalized values sample:")
print(agg_pdf['normalized_vessels'].describe())

# COMMAND ----------

print(f"Visualization data shape: {agg_pdf.shape}")
print(f"Sample {h3_column} values:\n{agg_pdf[h3_column].head()}")
print(f"\nSample colors:\n{agg_pdf['color'].head()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Usage Instructions
# MAGIC
# MAGIC **To change resolution:**
# MAGIC 1. Edit the `SELECTED_RESOLUTION` variable in the configuration cell
# MAGIC 2. Choose from: 6, 7, 8, or 9
# MAGIC 3. Lower resolutions (6, 7) show broader patterns and render faster
# MAGIC 4. Higher resolutions (8, 9) show more detailed local patterns
# MAGIC 5. Re-run the notebook from the configuration cell onwards
# MAGIC
# MAGIC **Visualization Details:**
# MAGIC * Shows total vessel activity aggregated across all 24 hours of the day
# MAGIC * Each hexagon represents the total unique vessels observed in that location throughout the entire day
# MAGIC * Fire colormap provides clear visual distinction between high and low activity areas
# MAGIC
# MAGIC **Color Legend (Fire colormap):**
# MAGIC * 🟡 Yellow: Low daily vessel activity
# MAGIC * 🟠 Orange: Medium daily vessel activity  
# MAGIC * 🔴 Red: High daily vessel activity
# MAGIC
# MAGIC **Interactive Features:**
# MAGIC * Hover over hexagons to see detailed statistics
# MAGIC * Use mouse to pan and zoom
# MAGIC * Click and drag to explore the map
