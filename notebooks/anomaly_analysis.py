# Databricks notebook source
# MAGIC %md
# MAGIC # Vessel Anomaly Detection Analysis
# MAGIC
# MAGIC Analysis of anomaly detection results from the AIS pipeline.
# MAGIC
# MAGIC **Generated tables:**
# MAGIC - `ais.ais_assets.vessel_ml_features` - ML features
# MAGIC - `ais.ais_assets.vessel_anomaly_detection_results` - Anomaly scores and classifications
# MAGIC
# MAGIC **Sections:**
# MAGIC 1. Overview Statistics
# MAGIC 2. Anomaly Distribution Analysis
# MAGIC 3. Spatial Analysis
# MAGIC 4. Feature Analysis
# MAGIC 5. Sample Anomalies

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup and Data Loading

# COMMAND ----------

# Install required packages
%pip install folium -q

# Note: Adjust the path above to match your Databricks Repos location
# Or if the wheel is already installed in the cluster environment, you can skip this step

# COMMAND ----------

# Restart Python kernel to use newly installed packages
dbutils.library.restartPython()

# COMMAND ----------

import sys
import os

from typing import Tuple
import pandas as pd
import numpy as np
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
import matplotlib.pyplot as plt
import seaborn as sns
import folium
from folium.plugins import HeatMap

# Get the parent directory of the notebooks folder
notebook_path = os.path.dirname(os.getcwd())
src_path = os.path.join(notebook_path, 'src')
sys.path.insert(0, src_path)

# Import vessel type mapping utilities from installed wheel
from ais_pipelines.vessel_types import map_vessel_types_spark, map_vessel_types_pandas

# Set visualization style - using default style for light backgrounds
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)

# Configure catalog and schema
CATALOG = "ais"
SCHEMA = "ais_assets"

# COMMAND ----------

# Load anomaly detection results
anomaly_results = spark.table(f"{CATALOG}.{SCHEMA}.vessel_anomaly_detection_results")
print(f"Loaded {anomaly_results.count():,} anomaly detection records")

# Load ML features for deeper analysis
ml_features = spark.table(f"{CATALOG}.{SCHEMA}.vessel_ml_features")
print(f"Loaded {ml_features.count():,} ML feature records")

# Join results with features
combined = ml_features.join(
    anomaly_results.select("mmsi", "timestamp", "reconstruction_error", "anomaly_score", "is_anomaly", "anomaly_severity"),
    on=["mmsi", "timestamp"],
    how="inner"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Overview Statistics

# COMMAND ----------

# Basic counts
overview = anomaly_results.select(
    F.countDistinct("mmsi").alias("unique_vessels"),
    F.count("*").alias("total_records"),
    F.sum(F.when(F.col("is_anomaly"), 1).otherwise(0)).alias("anomaly_count"),
    F.min("timestamp").alias("start_time"),
    F.max("timestamp").alias("end_time")
).toPandas()

anomaly_rate = (overview['anomaly_count'].iloc[0] / overview['total_records'].iloc[0]) * 100

print(f"{'='*60}")
print("ANOMALY DETECTION OVERVIEW")
print(f"{'='*60}")
print(f"Time Range: {overview['start_time'].iloc[0]} to {overview['end_time'].iloc[0]}")
print(f"Unique Vessels: {overview['unique_vessels'].iloc[0]:,}")
print(f"Total Records: {overview['total_records'].iloc[0]:,}")
print(f"Anomalies Detected: {overview['anomaly_count'].iloc[0]:,}")
print(f"Anomaly Rate: {anomaly_rate:.2f}%")
print(f"{'='*60}")

# COMMAND ----------

# Reconstruction error distribution
error_stats = anomaly_results.select(
    F.mean("reconstruction_error").alias("mean_error"),
    F.expr("percentile(reconstruction_error, 0.5)").alias("median_error"),
    F.stddev("reconstruction_error").alias("std_error"),
    F.expr("percentile(reconstruction_error, 0.95)").alias("p95_error"),
    F.expr("percentile(reconstruction_error, 0.99)").alias("p99_error")
).toPandas()

print("\nRECONSTRUCTION ERROR STATISTICS")
print(f"{'='*60}")
print(f"Mean: {error_stats['mean_error'].iloc[0]:.6f}")
print(f"Median: {error_stats['median_error'].iloc[0]:.6f}")
print(f"Std Dev: {error_stats['std_error'].iloc[0]:.6f}")
print(f"95th Percentile: {error_stats['p95_error'].iloc[0]:.6f}")
print(f"99th Percentile: {error_stats['p99_error'].iloc[0]:.6f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Anomaly Distribution Analysis

# COMMAND ----------

# Severity distribution
severity_dist = anomaly_results.groupBy("anomaly_severity").agg(
    F.count("*").alias("count")
).orderBy("anomaly_severity").toPandas()

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

# Bar chart
severity_dist.plot(x="anomaly_severity", y="count", kind="bar", ax=ax1, color="steelblue", legend=False)
ax1.set_title("Anomaly Severity Distribution", fontsize=14, fontweight='bold')
ax1.set_xlabel("Severity Level")
ax1.set_ylabel("Count")
ax1.tick_params(axis='x', rotation=45)
for i, v in enumerate(severity_dist['count']):
    ax1.text(i, v + v*0.01, f'{v:,}', ha='center', va='bottom')

# Pie chart (excluding normal)
severity_dist_anomalies = severity_dist[severity_dist['anomaly_severity'] != 'normal']
ax2.pie(severity_dist_anomalies['count'], labels=severity_dist_anomalies['anomaly_severity'], 
        autopct='%1.1f%%', startangle=90, colors=sns.color_palette("Reds", len(severity_dist_anomalies)))
ax2.set_title("Anomaly Severity Breakdown\n(Excluding Normal)", fontsize=14, fontweight='bold')

plt.tight_layout()
plt.show()

# COMMAND ----------

# Top vessels with most anomalies
top_anomalous_vessels = anomaly_results.filter(F.col("is_anomaly")).groupBy(
    "mmsi", "vessel_name"
).agg(
    F.count("*").alias("anomaly_count"),
    F.mean("anomaly_score").alias("avg_anomaly_score"),
    F.max("anomaly_score").alias("max_anomaly_score")
).orderBy(F.desc("anomaly_count")).limit(10).toPandas()

print("\nTOP 10 VESSELS BY ANOMALY COUNT")
print(f"{'='*80}")
print(f"{'MMSI':<12} {'Vessel Name':<25} {'Anomalies':<12} {'Avg Score':<12} {'Max Score':<12}")
print(f"{'='*80}")
for _, row in top_anomalous_vessels.iterrows():
    print(f"{row['mmsi']:<12} {row['vessel_name']:<25} {row['anomaly_count']:<12} {row['avg_anomaly_score']:<12.2f} {row['max_anomaly_score']:<12.2f}")

# COMMAND ----------

# Anomaly rate by vessel type
vessel_type_anomalies_spark = combined.groupBy("vessel_type").agg(
    F.count("*").alias("total_observations"),
    F.sum(F.when(F.col("is_anomaly"), 1).otherwise(0)).alias("anomaly_count"),
    F.countDistinct("mmsi").alias("vessel_count")
).withColumn(
    "anomaly_rate", (F.col("anomaly_count") / F.col("total_observations") * 100)
).orderBy(F.desc("anomaly_rate"))

# Map vessel type codes to names
vessel_type_anomalies_spark = map_vessel_types_spark(
    vessel_type_anomalies_spark,
    code_column="vessel_type",
    simplified=True,
    target_column="vessel_type_name"
)

vessel_type_anomalies = vessel_type_anomalies_spark.toPandas()

# Plot vessel type anomaly rates
fig, ax = plt.subplots(figsize=(12, 6))
vessel_type_anomalies_plot = vessel_type_anomalies.nlargest(15, 'anomaly_rate')
ax.barh(vessel_type_anomalies_plot['vessel_type_name'], vessel_type_anomalies_plot['anomaly_rate'], color='coral')
ax.set_xlabel('Anomaly Rate (%)', fontsize=12)
ax.set_ylabel('Vessel Type', fontsize=12)
ax.set_title('Anomaly Rate by Vessel Type (Top 15)', fontsize=14, fontweight='bold')
ax.grid(axis='x', alpha=0.3)
plt.tight_layout()
plt.show()

# Display table with vessel type names
print("\nVESSEL TYPE ANOMALY RATES (Top 15)")
print(f"{'='*80}")
for _, row in vessel_type_anomalies_plot.iterrows():
    print(f"{row['vessel_type_name']:<25} | Code: {row['vessel_type']:<4} | Rate: {row['anomaly_rate']:<6.2f}% | Observations: {int(row['total_observations']):>8,} | Vessels: {int(row['vessel_count']):>5,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Spatial Analysis

# COMMAND ----------

# Anomalies by H3 cell
spatial_anomalies = combined.filter(F.col("is_anomaly")).groupBy(
    "h3_res7", "h3_res6"
).agg(
    F.count("*").alias("anomaly_count"),
    F.mean("latitude").alias("avg_lat"),
    F.mean("longitude").alias("avg_lon"),
    F.countDistinct("mmsi").alias("vessel_count")
).orderBy(F.desc("anomaly_count")).toPandas()

print(f"\nSPATIAL DISTRIBUTION: Top H3 Cells by Anomaly Count")
print(f"{'='*80}")
print(f"Found {len(spatial_anomalies):,} H3 cells with anomalies")
print(f"\nTop 10 cells:")
print(spatial_anomalies.head(10)[['h3_res7', 'anomaly_count', 'vessel_count', 'avg_lat', 'avg_lon']])

# COMMAND ----------

# Create map of anomaly hotspots
anomaly_locations = combined.filter(
    F.col("is_anomaly") & (F.col("anomaly_severity").isin(['high', 'critical']))
).select("latitude", "longitude", "anomaly_score").limit(5000).toPandas()

if len(anomaly_locations) > 0:
    center_lat = anomaly_locations['latitude'].mean()
    center_lon = anomaly_locations['longitude'].mean()
    
    m = folium.Map(location=[center_lat, center_lon], zoom_start=6, tiles='OpenStreetMap')
    
    # Add heatmap
    heat_data = [[row['latitude'], row['longitude'], row['anomaly_score']] 
                 for _, row in anomaly_locations.iterrows()]
    HeatMap(heat_data, radius=15, blur=25, max_zoom=13).add_to(m)
    
    # Add title
    title_html = '''
    <div style="position: fixed; 
                top: 10px; left: 50px; width: 400px; height: 50px; 
                background-color: white; border:2px solid grey; z-index:9999; 
                font-size:16px; font-weight: bold; padding: 10px">
    High/Critical Anomaly Heatmap
    </div>
    '''
    m.get_root().html.add_child(folium.Element(title_html))
    
    display(m)
else:
    print("No high/critical anomalies found for mapping")

# COMMAND ----------

# Transit corridors vs stationary areas
location_context = combined.filter(F.col("is_anomaly")).groupBy(
    "is_transit_corridor", "is_stationary_area"
).agg(
    F.count("*").alias("anomaly_count")
).toPandas()

location_context['area_type'] = location_context.apply(
    lambda x: 'Transit Corridor' if x['is_transit_corridor'] == 1 
    else 'Stationary Area' if x['is_stationary_area'] == 1 
    else 'Other', axis=1
)

fig, ax = plt.subplots(figsize=(10, 6))
location_context.groupby('area_type')['anomaly_count'].sum().plot(kind='bar', ax=ax, color='teal')
ax.set_title('Anomalies by Location Type', fontsize=14, fontweight='bold')
ax.set_xlabel('Location Type')
ax.set_ylabel('Anomaly Count')
ax.tick_params(axis='x', rotation=45)
for i, v in enumerate(location_context.groupby('area_type')['anomaly_count'].sum()):
    ax.text(i, v + v*0.01, f'{int(v):,}', ha='center', va='bottom')
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Feature Analysis

# COMMAND ----------

# Correlations between features and anomaly scores
feature_cols = [
    'sog', 'distance_moved_km', 'speed_change', 'course_change',
    'hours_to_next_signal', 'speed_discrepancy_kmh'
]

# Sample for correlation analysis
correlation_sample = combined.filter(F.col("is_anomaly")).select(
    feature_cols + ['anomaly_score']
).fillna(0).sample(False, 0.1, seed=42).toPandas()

if len(correlation_sample) > 0:
    correlations = correlation_sample.corr()['anomaly_score'].drop('anomaly_score').sort_values(ascending=False)
    
    fig, ax = plt.subplots(figsize=(10, 6))
    correlations.plot(kind='barh', ax=ax, color=['darkgreen' if x > 0 else 'darkred' for x in correlations])
    ax.set_title('Feature Correlation with Anomaly Score', fontsize=14, fontweight='bold')
    ax.set_xlabel('Correlation Coefficient')
    ax.axvline(x=0, color='black', linestyle='--', linewidth=0.8)
    plt.tight_layout()
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Sample Anomalies

# COMMAND ----------

# Get top critical anomalies
top_anomalies = anomaly_results.filter(
    F.col("anomaly_severity") == "critical"
).orderBy(F.desc("anomaly_score")).limit(5).toPandas()

print("\nTOP 5 CRITICAL ANOMALIES")
print(f"{'='*100}")
print(f"{'MMSI':<12} {'Vessel Name':<25} {'Timestamp':<20} {'Score':<10} {'Latitude':<12} {'Longitude':<12}")
print(f"{'='*100}")
for _, row in top_anomalies.iterrows():
    print(f"{row['mmsi']:<12} {row['vessel_name']:<25} {str(row['timestamp']):<20} {row['anomaly_score']:<10.2f} {row['latitude']:<12.4f} {row['longitude']:<12.4f}")

# COMMAND ----------

# Detailed inspection of a specific anomaly
if len(top_anomalies) > 0:
    # Convert pandas/numpy types to native Python types for PySpark compatibility
    sample_mmsi = int(top_anomalies.iloc[0]['mmsi'])
    sample_time = top_anomalies.iloc[0]['timestamp']
    
    # Get context around the anomaly
    vessel_context = combined.filter(
        (F.col("mmsi") == sample_mmsi) &
        (F.col("timestamp").between(
            F.date_sub(F.lit(sample_time), 1),
            F.date_add(F.lit(sample_time), 1)
        ))
    ).orderBy("timestamp").toPandas()
    
    print(f"\n\nDETAILED CONTEXT FOR MMSI {sample_mmsi} AROUND {sample_time}")
    print(f"{'='*100}")
    print(f"Total observations in ±1 day: {len(vessel_context)}")
    print(f"Anomalies in this period: {vessel_context['is_anomaly'].sum()}")
    
    # Plot trajectory
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    
    # Speed over time
    axes[0, 0].plot(vessel_context.index, vessel_context['sog'], marker='o', linewidth=2)
    axes[0, 0].scatter(vessel_context[vessel_context['is_anomaly']].index, 
                      vessel_context[vessel_context['is_anomaly']]['sog'], 
                      color='red', s=100, zorder=5, label='Anomaly')
    axes[0, 0].set_title('Speed Over Ground', fontweight='bold')
    axes[0, 0].set_ylabel('SOG (knots)')
    axes[0, 0].legend()
    axes[0, 0].grid(alpha=0.3)
    
    # Course over time
    axes[0, 1].plot(vessel_context.index, vessel_context['cog'], marker='o', linewidth=2)
    axes[0, 1].scatter(vessel_context[vessel_context['is_anomaly']].index, 
                      vessel_context[vessel_context['is_anomaly']]['cog'], 
                      color='red', s=100, zorder=5, label='Anomaly')
    axes[0, 1].set_title('Course Over Ground', fontweight='bold')
    axes[0, 1].set_ylabel('COG (degrees)')
    axes[0, 1].legend()
    axes[0, 1].grid(alpha=0.3)
    
    # Anomaly score over time
    axes[1, 0].plot(vessel_context.index, vessel_context['anomaly_score'], 
                   marker='o', linewidth=2, color='coral')
    axes[1, 0].axhline(y=1.0, color='red', linestyle='--', label='Threshold')
    axes[1, 0].set_title('Anomaly Score', fontweight='bold')
    axes[1, 0].set_ylabel('Score')
    axes[1, 0].set_xlabel('Observation Index')
    axes[1, 0].legend()
    axes[1, 0].grid(alpha=0.3)
    
    # Geographic trajectory
    axes[1, 1].plot(vessel_context['longitude'], vessel_context['latitude'], 
                   marker='o', linewidth=2, markersize=4)
    axes[1, 1].scatter(vessel_context[vessel_context['is_anomaly']]['longitude'], 
                      vessel_context[vessel_context['is_anomaly']]['latitude'], 
                      color='red', s=100, zorder=5, label='Anomaly')
    axes[1, 1].set_title('Geographic Trajectory', fontweight='bold')
    axes[1, 1].set_xlabel('Longitude')
    axes[1, 1].set_ylabel('Latitude')
    axes[1, 1].legend()
    axes[1, 1].grid(alpha=0.3)
    
    plt.tight_layout()
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC This notebook provides analysis of the anomaly detection results:
# MAGIC
# MAGIC 1. **Overview** - Overall statistics and anomaly rates
# MAGIC 2. **Distribution** - Severity breakdown, top anomalous vessels, and vessel type analysis
# MAGIC 3. **Spatial** - Geographic patterns, hotspots, and location type analysis
# MAGIC 4. **Features** - Key features that correlate with anomalies
# MAGIC 5. **Samples** - Detailed inspection of specific critical anomalies
# MAGIC
# MAGIC **Next Steps:**
# MAGIC - Review critical anomalies for operational action
# MAGIC - Monitor vessel types with high anomaly rates
# MAGIC - Investigate geographic hotspots
