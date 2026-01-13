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
# MAGIC ## 2.5. Feature Table Overview

# COMMAND ----------

# Get row counts for all feature tables
feature_tables = {
    "Behavioral Features": f"{CATALOG}.{SCHEMA}.vessel_behavioral_features",
    "Rolling Patterns": f"{CATALOG}.{SCHEMA}.vessel_rolling_patterns",
    "H3 Normal Patterns": f"{CATALOG}.{SCHEMA}.h3_normal_patterns",
    "H3 Cell Statistics": f"{CATALOG}.{SCHEMA}.h3_cell_statistics",
    "Cell Hourly Stats": f"{CATALOG}.{SCHEMA}.cell_hourly_statistics",
    "Spatial Context": f"{CATALOG}.{SCHEMA}.vessel_spatial_context",
    "ML Features": f"{CATALOG}.{SCHEMA}.vessel_ml_features"
}

print(f"{'='*80}")
print("FEATURE TABLE OVERVIEW")
print(f"{'='*80}")
print(f"{'Table Name':<30} {'Row Count':>15} {'Unique Vessels':>15}")
print(f"{'='*80}")

for name, table in feature_tables.items():
    try:
        df = spark.table(table)
        count = df.count()
        vessels = df.select("mmsi").distinct().count() if "mmsi" in df.columns else "N/A"
        print(f"{name:<30} {count:>15,} {str(vessels):>15}")
    except Exception as e:
        print(f"{name:<30} {'Table not found':>15}")

print(f"{'='*80}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Session Analysis

# COMMAND ----------

# Load behavioral features for session analysis
behavioral_features = spark.table(f"{CATALOG}.{SCHEMA}.vessel_behavioral_features")

# Session statistics
session_stats = behavioral_features.groupBy("session_id").agg(
    F.count("*").alias("observations_in_session"),
    F.first("mmsi").alias("mmsi"),
    F.min("timestamp").alias("session_start"),
    F.max("timestamp").alias("session_end"),
    F.first("session_duration_hours").alias("duration_hours")
).select("mmsi", "observations_in_session", "duration_hours")

# Aggregate statistics
session_summary = session_stats.select(
    F.mean("observations_in_session").alias("avg_obs_per_session"),
    F.expr("percentile(observations_in_session, 0.5)").alias("median_obs"),
    F.expr("percentile(observations_in_session, 0.95)").alias("p95_obs"),
    F.mean("duration_hours").alias("avg_duration_hours"),
    F.expr("percentile(duration_hours, 0.5)").alias("median_duration"),
    F.expr("percentile(duration_hours, 0.95)").alias("p95_duration")
).toPandas()

print(f"{'='*80}")
print("SESSION STATISTICS")
print(f"{'='*80}")
print(f"Avg Observations per Session: {session_summary['avg_obs_per_session'].iloc[0]:.1f}")
print(f"Median Observations: {session_summary['median_obs'].iloc[0]:.0f}")
print(f"95th Percentile Observations: {session_summary['p95_obs'].iloc[0]:.0f}")
print(f"\nAvg Session Duration: {session_summary['avg_duration_hours'].iloc[0]:.1f} hours")
print(f"Median Duration: {session_summary['median_duration'].iloc[0]:.1f} hours")
print(f"95th Percentile Duration: {session_summary['p95_duration'].iloc[0]:.1f} hours")
print(f"{'='*80}")

# COMMAND ----------

# Session characteristics distribution
session_sample = session_stats.sample(False, 0.1, seed=42).toPandas()

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

# Observations per session
ax1.hist(session_sample['observations_in_session'], bins=50, color='steelblue', edgecolor='black')
ax1.set_title('Observations per Session', fontsize=14, fontweight='bold')
ax1.set_xlabel('Number of Observations')
ax1.set_ylabel('Frequency')
ax1.set_xlim(0, session_sample['observations_in_session'].astype(float).quantile(0.95))
ax1.grid(alpha=0.3)

# Session duration
ax2.hist(session_sample['duration_hours'], bins=50, color='coral', edgecolor='black')
ax2.set_title('Session Duration', fontsize=14, fontweight='bold')
ax2.set_xlabel('Duration (hours)')
ax2.set_ylabel('Frequency')
ax2.set_xlim(0, session_sample['duration_hours'].astype(float).quantile(0.95))
ax2.grid(alpha=0.3)

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Anomaly Distribution Analysis

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
# MAGIC ## 4.5. Rolling Pattern Analysis

# COMMAND ----------

# Load rolling patterns table
rolling_patterns = spark.table(f"{CATALOG}.{SCHEMA}.vessel_rolling_patterns")

# Rolling pattern statistics
rolling_stats = rolling_patterns.select(
    F.mean("avg_speed_6h").alias("mean_6h_speed"),
    F.expr("percentile(avg_speed_6h, 0.5)").alias("median_6h_speed"),
    F.mean("avg_speed_24h").alias("mean_24h_speed"),
    F.expr("percentile(avg_speed_24h, 0.5)").alias("median_24h_speed"),
    F.mean("avg_course_change_6h").alias("mean_course_change"),
    F.expr("percentile(h3_changes_6h, 0.95)").alias("p95_h3_changes"),
    F.mean("data_quality_score").alias("avg_data_quality")
).toPandas()

print(f"{'='*80}")
print("ROLLING PATTERN STATISTICS")
print(f"{'='*80}")
print(f"6-Hour Window:")
print(f"  Mean Speed: {rolling_stats['mean_6h_speed'].iloc[0]:.2f} knots")
print(f"  Median Speed: {rolling_stats['median_6h_speed'].iloc[0]:.2f} knots")
print(f"\n24-Hour Window:")
print(f"  Mean Speed: {rolling_stats['mean_24h_speed'].iloc[0]:.2f} knots")
print(f"  Median Speed: {rolling_stats['median_24h_speed'].iloc[0]:.2f} knots")
print(f"\nMovement Patterns:")
print(f"  Mean Course Change (6h): {rolling_stats['mean_course_change'].iloc[0]:.2f}°")
print(f"  95th Percentile H3 Changes: {rolling_stats['p95_h3_changes'].iloc[0]:.0f}")
print(f"\nData Quality:")
print(f"  Avg Quality Score: {rolling_stats['avg_data_quality'].iloc[0]:.2f}")
print(f"{'='*80}")

# COMMAND ----------

# Data quality distribution
quality_dist = rolling_patterns.groupBy("data_quality_score").count().orderBy("data_quality_score").toPandas()

fig, axes = plt.subplots(1, 3, figsize=(15, 4))

# Data quality score distribution
axes[0].bar(quality_dist['data_quality_score'], quality_dist['count'], color='steelblue', edgecolor='black')
axes[0].set_title('Data Quality Score Distribution', fontsize=12, fontweight='bold')
axes[0].set_xlabel('Quality Score')
axes[0].set_ylabel('Count')
axes[0].grid(alpha=0.3, axis='y')

# Speed stability (6h window)
speed_sample = rolling_patterns.select("avg_speed_6h", "stddev_speed_6h").fillna(0).sample(False, 0.1, seed=42).toPandas()
axes[1].scatter(speed_sample['avg_speed_6h'], speed_sample['stddev_speed_6h'], alpha=0.3, s=10)
axes[1].set_title('Speed Stability (6h Window)', fontsize=12, fontweight='bold')
axes[1].set_xlabel('Avg Speed (knots)')
axes[1].set_ylabel('Std Dev Speed')
axes[1].grid(alpha=0.3)
axes[1].set_xlim(0, 30)

# Erratic behavior score
erratic_sample = rolling_patterns.select("erratic_score_6h").fillna(0).filter(F.col("erratic_score_6h") > 0).sample(False, 0.1, seed=42).toPandas()
if len(erratic_sample) > 0:
    axes[2].hist(erratic_sample['erratic_score_6h'], bins=50, color='coral', edgecolor='black')
    axes[2].set_title('Erratic Behavior Score', fontsize=12, fontweight='bold')
    axes[2].set_xlabel('Erratic Score')
    axes[2].set_ylabel('Frequency')
    axes[2].set_xlim(0, erratic_sample['erratic_score_6h'].astype(float).quantile(0.95))
    axes[2].grid(alpha=0.3)

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.6. Spatial Context Analysis

# COMMAND ----------

# Load spatial context table
spatial_context = spark.table(f"{CATALOG}.{SCHEMA}.vessel_spatial_context")

# Spatial context statistics
spatial_stats = spatial_context.select(
    F.mean("vessels_in_same_cell").alias("avg_same_cell"),
    F.expr("percentile(vessels_in_same_cell, 0.95)").alias("p95_same_cell"),
    F.mean("vessels_in_kring1").alias("avg_kring1"),
    F.sum(F.when(F.col("is_isolated") == 1, 1).otherwise(0)).alias("isolated_count"),
    F.count("*").alias("total_observations")
).toPandas()

isolation_rate = (spatial_stats['isolated_count'].iloc[0] / spatial_stats['total_observations'].iloc[0]) * 100

print(f"{'='*80}")
print("SPATIAL CONTEXT STATISTICS")
print(f"{'='*80}")
print(f"Vessel Density:")
print(f"  Avg Vessels in Same Cell: {spatial_stats['avg_same_cell'].iloc[0]:.2f}")
print(f"  95th Percentile Same Cell: {spatial_stats['p95_same_cell'].iloc[0]:.0f}")
print(f"  Avg Vessels in KRing-1: {spatial_stats['avg_kring1'].iloc[0]:.2f}")
print(f"\nIsolation:")
print(f"  Isolated Observations: {spatial_stats['isolated_count'].iloc[0]:,}")
print(f"  Isolation Rate: {isolation_rate:.2f}%")
print(f"{'='*80}")

# COMMAND ----------

# Density distribution
density_sample = spatial_context.select("vessels_in_same_cell", "vessels_in_kring1", "local_density_ratio").sample(False, 0.1, seed=42).toPandas()

fig, axes = plt.subplots(1, 3, figsize=(15, 4))

# Vessels in same cell
axes[0].hist(density_sample['vessels_in_same_cell'], bins=30, color='steelblue', edgecolor='black')
axes[0].set_title('Vessels in Same H3 Cell', fontsize=12, fontweight='bold')
axes[0].set_xlabel('Vessel Count')
axes[0].set_ylabel('Frequency')
axes[0].set_xlim(0, density_sample['vessels_in_same_cell'].astype(float).quantile(0.95))
axes[0].grid(alpha=0.3)

# Vessels in neighborhood
axes[1].hist(density_sample['vessels_in_kring1'], bins=30, color='teal', edgecolor='black')
axes[1].set_title('Vessels in Neighborhood (KRing-1)', fontsize=12, fontweight='bold')
axes[1].set_xlabel('Vessel Count')
axes[1].set_ylabel('Frequency')
axes[1].set_xlim(0, density_sample['vessels_in_kring1'].astype(float).quantile(0.95))
axes[1].grid(alpha=0.3)

# Local density ratio
axes[2].hist(density_sample['local_density_ratio'], bins=30, color='coral', edgecolor='black')
axes[2].set_title('Local Density Ratio', fontsize=12, fontweight='bold')
axes[2].set_xlabel('Ratio')
axes[2].set_ylabel('Frequency')
axes[2].grid(alpha=0.3)

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.7. Historical Baseline Comparison

# COMMAND ----------

# Load H3 normal patterns
h3_patterns = spark.table(f"{CATALOG}.{SCHEMA}.h3_normal_patterns")

# Historical pattern statistics
pattern_stats = h3_patterns.select(
    F.mean("median_speed").alias("avg_historical_median"),
    F.expr("percentile(median_speed, 0.5)").alias("median_historical"),
    F.mean("avg_signal_gap").alias("avg_signal_gap"),
    F.mean("p95_signal_gap").alias("avg_p95_gap"),
    F.count("*").alias("total_patterns")
).toPandas()

print(f"{'='*80}")
print("HISTORICAL BASELINE STATISTICS")
print(f"{'='*80}")
print(f"Total Location-Type-Hour Patterns: {pattern_stats['total_patterns'].iloc[0]:,}")
print(f"\nSpeed Baselines:")
print(f"  Avg Historical Median Speed: {pattern_stats['avg_historical_median'].iloc[0]:.2f} knots")
print(f"  Median of Historical Medians: {pattern_stats['median_historical'].iloc[0]:.2f} knots")
print(f"\nSignal Gap Patterns:")
print(f"  Avg Signal Gap: {pattern_stats['avg_signal_gap'].iloc[0]:.2f} hours")
print(f"  Avg 95th Percentile Gap: {pattern_stats['avg_p95_gap'].iloc[0]:.2f} hours")
print(f"{'='*80}")

# COMMAND ----------

# Current vs historical speed comparison (for anomalies)
speed_comparison = combined.filter(F.col("is_anomaly")).select(
    "sog",
    "historical_median_speed",
    "historical_q95_speed"
).fillna(0).sample(False, 0.2, seed=42).toPandas()

if len(speed_comparison) > 0:
    speed_comparison['speed_deviation'] = speed_comparison['sog'] - speed_comparison['historical_median_speed']
    speed_comparison['exceeds_p95'] = speed_comparison['sog'] > speed_comparison['historical_q95_speed']
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
    
    # Speed deviation distribution
    ax1.hist(speed_comparison['speed_deviation'], bins=50, color='steelblue', edgecolor='black')
    ax1.axvline(x=0, color='red', linestyle='--', linewidth=2, label='No Deviation')
    ax1.set_title('Speed Deviation from Historical Baseline\n(Anomalies Only)', fontsize=12, fontweight='bold')
    ax1.set_xlabel('Speed Deviation (knots)')
    ax1.set_ylabel('Frequency')
    ax1.legend()
    ax1.grid(alpha=0.3)
    
    # Exceeding P95 analysis
    exceeds_counts = speed_comparison['exceeds_p95'].value_counts()
    ax2.bar(['Below P95', 'Exceeds P95'], [exceeds_counts.get(False, 0), exceeds_counts.get(True, 0)], 
            color=['green', 'red'], edgecolor='black')
    ax2.set_title('Anomalies Exceeding Historical 95th Percentile', fontsize=12, fontweight='bold')
    ax2.set_ylabel('Count')
    ax2.grid(alpha=0.3, axis='y')
    
    for i, v in enumerate([exceeds_counts.get(False, 0), exceeds_counts.get(True, 0)]):
        ax2.text(i, v + v*0.01, f'{int(v):,}', ha='center', va='bottom')
    
    plt.tight_layout()
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Feature Analysis

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
# MAGIC This notebook provides comprehensive analysis of the anomaly detection results and feature engineering:
# MAGIC
# MAGIC 1. **Setup & Data Loading** - Initialize environment and load feature tables
# MAGIC 2. **Overview Statistics** - Overall statistics, anomaly rates, and reconstruction errors
# MAGIC 3. **Feature Table Overview** - Summary of all 7 feature tables created by the pipeline
# MAGIC 4. **Session Analysis** - Vessel entry/exit patterns and session characteristics
# MAGIC 5. **Anomaly Distribution** - Severity breakdown, top anomalous vessels, and vessel type analysis
# MAGIC 6. **Spatial Analysis** - Geographic patterns, hotspots, and location type analysis
# MAGIC 7. **Rolling Pattern Analysis** - Time-windowed statistics, speed stability, and data quality
# MAGIC 8. **Spatial Context Analysis** - Vessel density patterns, isolation rates, and neighborhood dynamics
# MAGIC 9. **Historical Baseline Comparison** - Current vs historical speed deviations and pattern violations
# MAGIC 10. **Feature Analysis** - Feature correlations with anomaly scores
# MAGIC 11. **Sample Anomalies** - Detailed inspection of specific critical anomalies
# MAGIC
# MAGIC **Key Insights:**
# MAGIC - **Session-Based Approach**: Handles vessels entering/exiting coverage areas
# MAGIC - **Multi-Resolution Features**: Combines behavioral, temporal, spatial, and historical patterns
# MAGIC - **Data Quality Tracking**: Monitors observation sufficiency for reliable detection
# MAGIC - **Spatial Context**: Accounts for vessel density and location characteristics
# MAGIC
# MAGIC **Next Steps:**
# MAGIC - Review critical anomalies for operational action
# MAGIC - Monitor vessel types with high anomaly rates
# MAGIC - Investigate geographic hotspots
# MAGIC - Analyze session boundary patterns for coverage gaps
# MAGIC - Compare anomaly rates across different spatial contexts
