# Databricks notebook source
# MAGIC %md
# MAGIC # AIS Vessel Anomaly Detection using Autoencoder
# MAGIC 
# MAGIC This notebook implements an Autoencoder-based anomaly detection system for vessel behavior.
# MAGIC The Autoencoder learns normal vessel behavior patterns and identifies anomalies based on reconstruction error.
# MAGIC 
# MAGIC **Prerequisites**: Run the SQL notebook first to generate the `vessel_ml_features` table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup and Imports

# COMMAND ----------

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from typing import List, Tuple, Dict

# TensorFlow/Keras imports
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers, models, callbacks
from tensorflow.keras.optimizers import Adam

# Spark imports
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# MLlib for preprocessing
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml import Pipeline

print(f"TensorFlow version: {tf.__version__}")
print(f"GPU available: {tf.config.list_physical_devices('GPU')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuration

# COMMAND ----------

# Catalog and schema
CATALOG = "ais"
SCHEMA = "ais_assets"

# Feature selection - exclude identifiers and target columns
FEATURE_COLUMNS = [
    # Vessel behavior features
    'sog', 'cog', 'hours_to_next_signal', 'hours_since_last_signal',
    'distance_moved_km', 'speed_change', 'course_change',
    'changed_h3_cell', 'changed_h3_parent',
    'implied_speed_kmh', 'speed_discrepancy_kmh', 'avg_sog_kmh',
    
    # Rolling pattern features
    'avg_speed_6h', 'stddev_speed_6h', 'min_speed_6h', 'max_speed_6h',
    'avg_course_change_6h', 'max_course_change_6h', 'h3_changes_6h',
    'observation_count_6h', 'avg_speed_24h', 'stddev_speed_24h',
    'unique_h3_cells_24h', 'pct_low_speed_6h', 'erratic_score_6h',
    
    # Historical pattern features
    'historical_median_speed', 'historical_q75_speed', 'historical_q95_speed',
    'historical_q05_speed', 'historical_avg_course_change',
    'historical_stddev_course_change', 'historical_avg_signal_gap',
    'historical_p95_signal_gap',
    
    # Spatial context features (NEW)
    'vessels_in_same_cell', 'vessels_in_kring1', 'vessel_types_nearby',
    'is_isolated', 'is_neighborhood_isolated', 'local_density_ratio',
    
    # H3 cell statistics features (NEW)
    'cell_avg_vessel_count', 'cell_p95_vessel_count', 'cell_vessel_type_diversity',
    'cell_historical_avg_speed', 'cell_historical_stddev_speed',
    'is_transit_corridor', 'is_stationary_area',
    
    # Derived spatial anomaly indicators (NEW)
    'is_unusually_crowded', 'is_unexpectedly_isolated',
    
    # Time features
    'hour_of_day', 'day_of_week'
]

# Autoencoder hyperparameters
ENCODING_DIM = 16  # Bottleneck layer size
EPOCHS = 50
BATCH_SIZE = 256
LEARNING_RATE = 0.001
VALIDATION_SPLIT = 0.2

# Anomaly threshold (percentile of reconstruction error on training data)
ANOMALY_THRESHOLD_PERCENTILE = 95

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Load and Prepare Data

# COMMAND ----------

# Load features from SQL-generated table
df = spark.table(f"{CATALOG}.{SCHEMA}.vessel_ml_features")

print(f"Total records: {df.count():,}")
print(f"Features shape: {len(FEATURE_COLUMNS)} features")

# Display sample
display(df.limit(5))

# COMMAND ----------

# Check for missing values
null_counts = df.select([
    F.sum(F.col(c).isNull().cast("int")).alias(c) 
    for c in FEATURE_COLUMNS
]).toPandas()

print("\nMissing values per feature:")
print(null_counts.T.sort_values(by=0, ascending=False).head(10))

# COMMAND ----------

# Handle missing values - fill with 0 (or you could use median imputation)
df_filled = df.fillna(0, subset=FEATURE_COLUMNS)

# Verify no more nulls
null_check = df_filled.select([
    F.sum(F.col(c).isNull().cast("int")).alias(c) 
    for c in FEATURE_COLUMNS
]).toPandas()

print("Remaining nulls after imputation:")
print(null_check.sum().sum())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Feature Scaling

# COMMAND ----------

# Assemble features into a vector
assembler = VectorAssembler(
    inputCols=FEATURE_COLUMNS,
    outputCol="features_raw",
    handleInvalid="skip"
)

# Standardize features (zero mean, unit variance)
scaler = StandardScaler(
    inputCol="features_raw",
    outputCol="features_scaled",
    withMean=True,
    withStd=True
)

# Create and fit pipeline
pipeline = Pipeline(stages=[assembler, scaler])
pipeline_model = pipeline.fit(df_filled)

# Transform data
df_scaled = pipeline_model.transform(df_filled)

print("Features scaled successfully")

# COMMAND ----------

# Convert to Pandas for training (collect scaled features to driver)
# Note: For very large datasets, consider using distributed training or sampling
pdf = df_scaled.select(
    "mmsi", "vessel_name", "timestamp", "latitude", "longitude",
    "features_scaled"
).toPandas()

# Extract feature vectors as numpy array
X = np.array([row.toArray() for row in pdf['features_scaled']])
n_features = X.shape[1]

print(f"Feature matrix shape: {X.shape}")
print(f"Number of features: {n_features}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Build Autoencoder Model

# COMMAND ----------

def build_autoencoder(input_dim: int, encoding_dim: int) -> Tuple[keras.Model, keras.Model]:
    """
    Build an Autoencoder model for anomaly detection.
    
    Args:
        input_dim: Dimensionality of input features
        encoding_dim: Dimensionality of encoded representation (bottleneck)
    
    Returns:
        Tuple of (autoencoder, encoder) models
    """
    # Encoder
    input_layer = layers.Input(shape=(input_dim,))
    encoded = layers.Dense(128, activation='relu')(input_layer)
    encoded = layers.BatchNormalization()(encoded)
    encoded = layers.Dropout(0.2)(encoded)
    
    encoded = layers.Dense(64, activation='relu')(encoded)
    encoded = layers.BatchNormalization()(encoded)
    encoded = layers.Dropout(0.2)(encoded)
    
    encoded = layers.Dense(32, activation='relu')(encoded)
    encoded = layers.BatchNormalization()(encoded)
    
    # Bottleneck
    bottleneck = layers.Dense(encoding_dim, activation='relu', name='bottleneck')(encoded)
    
    # Decoder
    decoded = layers.Dense(32, activation='relu')(bottleneck)
    decoded = layers.BatchNormalization()(decoded)
    
    decoded = layers.Dense(64, activation='relu')(decoded)
    decoded = layers.BatchNormalization()(decoded)
    decoded = layers.Dropout(0.2)(decoded)
    
    decoded = layers.Dense(128, activation='relu')(decoded)
    decoded = layers.BatchNormalization()(decoded)
    decoded = layers.Dropout(0.2)(decoded)
    
    # Output layer
    decoded = layers.Dense(input_dim, activation='linear')(decoded)
    
    # Autoencoder model
    autoencoder = models.Model(inputs=input_layer, outputs=decoded)
    
    # Encoder model (for extracting embeddings if needed)
    encoder = models.Model(inputs=input_layer, outputs=bottleneck)
    
    return autoencoder, encoder


# Build model
autoencoder, encoder = build_autoencoder(n_features, ENCODING_DIM)

# Compile
autoencoder.compile(
    optimizer=Adam(learning_rate=LEARNING_RATE),
    loss='mse',
    metrics=['mae']
)

print("Autoencoder architecture:")
autoencoder.summary()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Train Autoencoder

# COMMAND ----------

# Split data (train on majority, use for threshold calculation)
train_size = int(0.8 * len(X))
X_train = X[:train_size]
X_val = X[train_size:]

print(f"Training samples: {len(X_train):,}")
print(f"Validation samples: {len(X_val):,}")

# Callbacks
early_stopping = callbacks.EarlyStopping(
    monitor='val_loss',
    patience=5,
    restore_best_weights=True
)

reduce_lr = callbacks.ReduceLROnPlateau(
    monitor='val_loss',
    factor=0.5,
    patience=3,
    min_lr=1e-6
)

# Train
history = autoencoder.fit(
    X_train, X_train,
    epochs=EPOCHS,
    batch_size=BATCH_SIZE,
    validation_data=(X_val, X_val),
    callbacks=[early_stopping, reduce_lr],
    verbose=1
)

# COMMAND ----------

# Plot training history
fig, axes = plt.subplots(1, 2, figsize=(15, 5))

# Loss
axes[0].plot(history.history['loss'], label='Training Loss')
axes[0].plot(history.history['val_loss'], label='Validation Loss')
axes[0].set_xlabel('Epoch')
axes[0].set_ylabel('Loss (MSE)')
axes[0].set_title('Training and Validation Loss')
axes[0].legend()
axes[0].grid(True)

# MAE
axes[1].plot(history.history['mae'], label='Training MAE')
axes[1].plot(history.history['val_mae'], label='Validation MAE')
axes[1].set_xlabel('Epoch')
axes[1].set_ylabel('MAE')
axes[1].set_title('Training and Validation MAE')
axes[1].legend()
axes[1].grid(True)

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Calculate Reconstruction Error and Anomaly Threshold

# COMMAND ----------

# Predict on training data to calculate reconstruction error
X_train_pred = autoencoder.predict(X_train, batch_size=BATCH_SIZE)
train_mse = np.mean(np.square(X_train - X_train_pred), axis=1)

# Calculate threshold at specified percentile
anomaly_threshold = np.percentile(train_mse, ANOMALY_THRESHOLD_PERCENTILE)

print(f"Reconstruction error statistics on training data:")
print(f"  Mean: {np.mean(train_mse):.6f}")
print(f"  Median: {np.median(train_mse):.6f}")
print(f"  Std: {np.std(train_mse):.6f}")
print(f"  Min: {np.min(train_mse):.6f}")
print(f"  Max: {np.max(train_mse):.6f}")
print(f"\nAnomaly threshold ({ANOMALY_THRESHOLD_PERCENTILE}th percentile): {anomaly_threshold:.6f}")

# COMMAND ----------

# Plot reconstruction error distribution
fig, axes = plt.subplots(1, 2, figsize=(15, 5))

# Histogram
axes[0].hist(train_mse, bins=50, alpha=0.7, color='blue', edgecolor='black')
axes[0].axvline(anomaly_threshold, color='red', linestyle='--', linewidth=2, 
                label=f'Threshold ({ANOMALY_THRESHOLD_PERCENTILE}th percentile)')
axes[0].set_xlabel('Reconstruction Error (MSE)')
axes[0].set_ylabel('Frequency')
axes[0].set_title('Distribution of Reconstruction Error (Training Data)')
axes[0].legend()
axes[0].grid(True, alpha=0.3)

# Log scale
axes[1].hist(train_mse, bins=50, alpha=0.7, color='blue', edgecolor='black')
axes[1].axvline(anomaly_threshold, color='red', linestyle='--', linewidth=2,
                label=f'Threshold ({ANOMALY_THRESHOLD_PERCENTILE}th percentile)')
axes[1].set_xlabel('Reconstruction Error (MSE)')
axes[1].set_ylabel('Frequency')
axes[1].set_title('Distribution of Reconstruction Error (Log Scale)')
axes[1].set_yscale('log')
axes[1].legend()
axes[1].grid(True, alpha=0.3)

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Detect Anomalies on All Data

# COMMAND ----------

# Predict on all data
X_pred = autoencoder.predict(X, batch_size=BATCH_SIZE)

# Calculate reconstruction error for each sample
reconstruction_errors = np.mean(np.square(X - X_pred), axis=1)

# Identify anomalies
is_anomaly = reconstruction_errors > anomaly_threshold

# Calculate anomaly scores (normalized reconstruction error)
anomaly_scores = reconstruction_errors / anomaly_threshold

print(f"Total vessels examined: {len(X):,}")
print(f"Anomalies detected: {is_anomaly.sum():,} ({100 * is_anomaly.sum() / len(X):.2f}%)")
print(f"Normal behavior: {(~is_anomaly).sum():,} ({100 * (~is_anomaly).sum() / len(X):.2f}%)")

# COMMAND ----------

# Add results to dataframe
pdf['reconstruction_error'] = reconstruction_errors
pdf['anomaly_score'] = anomaly_scores
pdf['is_anomaly'] = is_anomaly

# Display top anomalies
top_anomalies = pdf.nlargest(20, 'reconstruction_error')[
    ['mmsi', 'vessel_name', 'timestamp', 'latitude', 'longitude', 
     'reconstruction_error', 'anomaly_score', 'is_anomaly']
]

print("\nTop 20 Anomalies:")
display(top_anomalies)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Analyze Anomaly Characteristics

# COMMAND ----------

# Create comparison plots
fig, axes = plt.subplots(2, 2, figsize=(15, 12))

# Reconstruction error by vessel type
anomaly_df = pdf[pdf['is_anomaly']]
vessel_counts = anomaly_df.groupby('vessel_name').size().sort_values(ascending=False).head(15)

axes[0, 0].barh(range(len(vessel_counts)), vessel_counts.values)
axes[0, 0].set_yticks(range(len(vessel_counts)))
axes[0, 0].set_yticklabels(vessel_counts.index)
axes[0, 0].set_xlabel('Number of Anomalies')
axes[0, 0].set_title('Top 15 Vessels with Most Anomalies')
axes[0, 0].grid(True, alpha=0.3)

# Anomaly score distribution comparison
axes[0, 1].hist(pdf[~pdf['is_anomaly']]['anomaly_score'], bins=50, alpha=0.5, 
                label='Normal', color='green', edgecolor='black')
axes[0, 1].hist(pdf[pdf['is_anomaly']]['anomaly_score'], bins=50, alpha=0.5, 
                label='Anomaly', color='red', edgecolor='black')
axes[0, 1].axvline(1.0, color='black', linestyle='--', linewidth=2, label='Threshold')
axes[0, 1].set_xlabel('Anomaly Score')
axes[0, 1].set_ylabel('Frequency')
axes[0, 1].set_title('Anomaly Score Distribution')
axes[0, 1].legend()
axes[0, 1].grid(True, alpha=0.3)

# Time series of anomalies
pdf['timestamp_dt'] = pd.to_datetime(pdf['timestamp'])
hourly_anomalies = pdf.groupby(pdf['timestamp_dt'].dt.floor('H'))['is_anomaly'].sum()
axes[1, 0].plot(hourly_anomalies.index, hourly_anomalies.values, marker='o', linestyle='-')
axes[1, 0].set_xlabel('Time')
axes[1, 0].set_ylabel('Number of Anomalies')
axes[1, 0].set_title('Anomalies Over Time (Hourly)')
axes[1, 0].grid(True, alpha=0.3)
axes[1, 0].tick_params(axis='x', rotation=45)

# Severity distribution
severity_bins = [0, 1.0, 1.5, 2.0, 3.0, float('inf')]
severity_labels = ['Normal', 'Low', 'Medium', 'High', 'Critical']
pdf['severity'] = pd.cut(pdf['anomaly_score'], bins=severity_bins, labels=severity_labels)
severity_counts = pdf['severity'].value_counts().sort_index()

axes[1, 1].bar(range(len(severity_counts)), severity_counts.values, 
               color=['green', 'yellow', 'orange', 'red', 'darkred'])
axes[1, 1].set_xticks(range(len(severity_counts)))
axes[1, 1].set_xticklabels(severity_counts.index, rotation=45)
axes[1, 1].set_ylabel('Count')
axes[1, 1].set_title('Anomaly Severity Distribution')
axes[1, 1].grid(True, alpha=0.3)

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Save Results to Delta Table

# COMMAND ----------

# Convert results back to Spark DataFrame
results_pdf = pdf[[
    'mmsi', 'vessel_name', 'timestamp', 'latitude', 'longitude',
    'reconstruction_error', 'anomaly_score', 'is_anomaly'
]]

# Add severity classification
results_pdf['anomaly_severity'] = pd.cut(
    results_pdf['anomaly_score'],
    bins=[0, 1.0, 1.5, 2.0, 3.0, float('inf')],
    labels=['normal', 'low', 'medium', 'high', 'critical']
).astype(str)

# Convert to Spark DataFrame
results_sdf = spark.createDataFrame(results_pdf)

# Save to Delta table
results_sdf.write \
    .mode("overwrite") \
    .saveAsTable(f"{CATALOG}.{SCHEMA}.vessel_anomaly_detection_results")

print(f"Results saved to {CATALOG}.{SCHEMA}.vessel_anomaly_detection_results")

# Display sample
display(spark.table(f"{CATALOG}.{SCHEMA}.vessel_anomaly_detection_results").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Export Model (Optional)

# COMMAND ----------

# Save model to DBFS
model_path = "/dbfs/models/ais_autoencoder_model"
autoencoder.save(model_path)
print(f"Model saved to {model_path}")

# Save threshold and scaler info
import json

model_metadata = {
    "anomaly_threshold": float(anomaly_threshold),
    "anomaly_threshold_percentile": ANOMALY_THRESHOLD_PERCENTILE,
    "encoding_dim": ENCODING_DIM,
    "n_features": n_features,
    "feature_columns": FEATURE_COLUMNS,
    "training_samples": len(X_train),
    "tensorflow_version": tf.__version__
}

with open("/dbfs/models/ais_autoencoder_metadata.json", "w") as f:
    json.dump(model_metadata, f, indent=2)

print("Model metadata saved")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Statistics

# COMMAND ----------

# Generate summary report
summary = {
    "Total Vessels Analyzed": len(pdf['mmsi'].unique()),
    "Total Records": len(pdf),
    "Anomalies Detected": int(is_anomaly.sum()),
    "Anomaly Rate (%)": f"{100 * is_anomaly.sum() / len(X):.2f}",
    "Anomaly Threshold": f"{anomaly_threshold:.6f}",
    "Mean Reconstruction Error": f"{np.mean(reconstruction_errors):.6f}",
    "Median Reconstruction Error": f"{np.median(reconstruction_errors):.6f}",
    "Max Reconstruction Error": f"{np.max(reconstruction_errors):.6f}",
    "Model Encoding Dimension": ENCODING_DIM,
    "Training Epochs": len(history.history['loss'])
}

print("\n" + "="*60)
print("ANOMALY DETECTION SUMMARY")
print("="*60)
for key, value in summary.items():
    print(f"{key:.<40} {value}")
print("="*60)
