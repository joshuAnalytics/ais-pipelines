"""Vessel Anomaly Detection using Autoencoder.

This module implements an autoencoder-based anomaly detection system for vessel behavior.
The autoencoder learns normal vessel behavior patterns and identifies anomalies based on
reconstruction error.
"""

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Tuple, Dict, Any

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession, functions as F
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml import Pipeline

# TensorFlow imports
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers, models, callbacks
from tensorflow.keras.optimizers import Adam


@dataclass
class AnomalyDetectionConfig:
    """Configuration for anomaly detection."""
    
    catalog: str
    schema: str
    encoding_dim: int = 16
    epochs: int = 50
    batch_size: int = 256
    learning_rate: float = 0.001
    validation_split: float = 0.2
    anomaly_threshold_percentile: int = 95
    
    # Feature columns to use for detection
    feature_columns: list = None
    
    def __post_init__(self) -> None:
        if self.feature_columns is None:
            self.feature_columns = [
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
                
                # Spatial context features
                'vessels_in_same_cell', 'vessels_in_kring1', 'vessel_types_nearby',
                'is_isolated', 'is_neighborhood_isolated', 'local_density_ratio',
                
                # H3 cell statistics features
                'cell_avg_vessel_count', 'cell_p95_vessel_count', 'cell_vessel_type_diversity',
                'cell_historical_avg_speed', 'cell_historical_stddev_speed',
                'is_transit_corridor', 'is_stationary_area',
                
                # Derived spatial anomaly indicators
                'is_unusually_crowded', 'is_unexpectedly_isolated',
                
                # Time features
                'hour_of_day', 'day_of_week'
            ]


class AutoencoderModel:
    """Autoencoder model for anomaly detection."""
    
    def __init__(self, input_dim: int, encoding_dim: int, learning_rate: float) -> None:
        self.input_dim = input_dim
        self.encoding_dim = encoding_dim
        self.learning_rate = learning_rate
        self.autoencoder: keras.Model = None
        self.encoder: keras.Model = None
        
    def build(self) -> None:
        """Build the autoencoder architecture."""
        # Encoder
        input_layer = layers.Input(shape=(self.input_dim,))
        encoded = layers.Dense(128, activation='relu')(input_layer)
        encoded = layers.BatchNormalization()(encoded)
        encoded = layers.Dropout(0.2)(encoded)
        
        encoded = layers.Dense(64, activation='relu')(encoded)
        encoded = layers.BatchNormalization()(encoded)
        encoded = layers.Dropout(0.2)(encoded)
        
        encoded = layers.Dense(32, activation='relu')(encoded)
        encoded = layers.BatchNormalization()(encoded)
        
        # Bottleneck
        bottleneck = layers.Dense(self.encoding_dim, activation='relu', name='bottleneck')(encoded)
        
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
        decoded = layers.Dense(self.input_dim, activation='linear')(decoded)
        
        # Create models
        self.autoencoder = models.Model(inputs=input_layer, outputs=decoded)
        self.encoder = models.Model(inputs=input_layer, outputs=bottleneck)
        
        # Compile
        self.autoencoder.compile(
            optimizer=Adam(learning_rate=self.learning_rate),
            loss='mse',
            metrics=['mae']
        )
        
    def train(
        self, 
        X_train: np.ndarray, 
        X_val: np.ndarray,
        epochs: int,
        batch_size: int
    ) -> keras.callbacks.History:
        """Train the autoencoder model."""
        if self.autoencoder is None:
            raise ValueError("Model not built. Call build() first.")
        
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
        
        history = self.autoencoder.fit(
            X_train, X_train,
            epochs=epochs,
            batch_size=batch_size,
            validation_data=(X_val, X_val),
            callbacks=[early_stopping, reduce_lr],
            verbose=1
        )
        
        return history
    
    def predict(self, X: np.ndarray, batch_size: int) -> np.ndarray:
        """Predict reconstructions for input data."""
        if self.autoencoder is None:
            raise ValueError("Model not built. Call build() first.")
        return self.autoencoder.predict(X, batch_size=batch_size)
    
    def save(self, path: str) -> None:
        """Save model to disk."""
        if self.autoencoder is None:
            raise ValueError("Model not built. Call build() first.")
        self.autoencoder.save(path)
    
    def get_summary(self) -> str:
        """Get model architecture summary."""
        if self.autoencoder is None:
            raise ValueError("Model not built. Call build() first.")
        
        from io import StringIO
        import sys
        
        stream = StringIO()
        old_stdout = sys.stdout
        sys.stdout = stream
        self.autoencoder.summary()
        sys.stdout = old_stdout
        return stream.getvalue()


class AnomalyDetector:
    """Main anomaly detection orchestrator."""
    
    def __init__(self, config: AnomalyDetectionConfig) -> None:
        self.config = config
        self.spark = SparkSession.builder.getOrCreate()
        self.model: AutoencoderModel = None
        self.anomaly_threshold: float = None
        self.pipeline_model = None
        
    def load_features(self) -> pd.DataFrame:
        """Load and prepare feature data."""
        table_name = f"{self.config.catalog}.{self.config.schema}.vessel_ml_features"
        print(f"Loading features from {table_name}...")
        
        df = self.spark.table(table_name)
        print(f"Total records: {df.count():,}")
        
        # Handle missing values
        df_filled = df.fillna(0, subset=self.config.feature_columns)
        
        # Scale features
        assembler = VectorAssembler(
            inputCols=self.config.feature_columns,
            outputCol="features_raw",
            handleInvalid="skip"
        )
        
        scaler = StandardScaler(
            inputCol="features_raw",
            outputCol="features_scaled",
            withMean=True,
            withStd=True
        )
        
        pipeline = Pipeline(stages=[assembler, scaler])
        self.pipeline_model = pipeline.fit(df_filled)
        df_scaled = self.pipeline_model.transform(df_filled)
        
        # Convert to Pandas
        pdf = df_scaled.select(
            "mmsi", "vessel_name", "timestamp", "latitude", "longitude",
            "features_scaled"
        ).toPandas()
        
        print(f"Prepared {len(pdf):,} records for training")
        return pdf
    
    def prepare_training_data(self, pdf: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """Extract and split feature arrays."""
        X = np.array([row.toArray() for row in pdf['features_scaled']])
        
        train_size = int(0.8 * len(X))
        X_train = X[:train_size]
        X_val = X[train_size:]
        
        print(f"Training samples: {len(X_train):,}")
        print(f"Validation samples: {len(X_val):,}")
        
        return X, X_train, X_val
    
    def train_model(self, X_train: np.ndarray, X_val: np.ndarray) -> None:
        """Build and train the autoencoder model."""
        n_features = X_train.shape[1]
        print(f"\nBuilding autoencoder with {n_features} features...")
        
        self.model = AutoencoderModel(
            input_dim=n_features,
            encoding_dim=self.config.encoding_dim,
            learning_rate=self.config.learning_rate
        )
        self.model.build()
        
        print("\nModel architecture:")
        print(self.model.get_summary())
        
        print("\nTraining model...")
        history = self.model.train(
            X_train, X_val,
            epochs=self.config.epochs,
            batch_size=self.config.batch_size
        )
        
        print(f"Training completed in {len(history.history['loss'])} epochs")
    
    def calculate_threshold(self, X_train: np.ndarray) -> None:
        """Calculate anomaly detection threshold."""
        print("\nCalculating anomaly threshold...")
        
        X_train_pred = self.model.predict(X_train, self.config.batch_size)
        train_mse = np.mean(np.square(X_train - X_train_pred), axis=1)
        
        self.anomaly_threshold = np.percentile(
            train_mse, 
            self.config.anomaly_threshold_percentile
        )
        
        print(f"Reconstruction error statistics:")
        print(f"  Mean: {np.mean(train_mse):.6f}")
        print(f"  Median: {np.median(train_mse):.6f}")
        print(f"  Std: {np.std(train_mse):.6f}")
        print(f"  {self.config.anomaly_threshold_percentile}th percentile: {self.anomaly_threshold:.6f}")
    
    def detect_anomalies(self, X: np.ndarray, pdf: pd.DataFrame) -> pd.DataFrame:
        """Detect anomalies on all data."""
        print("\nDetecting anomalies...")
        
        X_pred = self.model.predict(X, self.config.batch_size)
        reconstruction_errors = np.mean(np.square(X - X_pred), axis=1)
        
        is_anomaly = reconstruction_errors > self.anomaly_threshold
        anomaly_scores = reconstruction_errors / self.anomaly_threshold
        
        print(f"Anomalies detected: {is_anomaly.sum():,} ({100 * is_anomaly.sum() / len(X):.2f}%)")
        
        # Add results to dataframe
        pdf['reconstruction_error'] = reconstruction_errors
        pdf['anomaly_score'] = anomaly_scores
        pdf['is_anomaly'] = is_anomaly
        
        # Add severity classification
        pdf['anomaly_severity'] = pd.cut(
            pdf['anomaly_score'],
            bins=[0, 1.0, 1.5, 2.0, 3.0, float('inf')],
            labels=['normal', 'low', 'medium', 'high', 'critical']
        ).astype(str)
        
        return pdf
    
    def save_results(self, pdf: pd.DataFrame) -> None:
        """Save anomaly detection results to Delta table."""
        results_pdf = pdf[[
            'mmsi', 'vessel_name', 'timestamp', 'latitude', 'longitude',
            'reconstruction_error', 'anomaly_score', 'is_anomaly', 'anomaly_severity'
        ]]
        
        results_sdf = self.spark.createDataFrame(results_pdf)
        
        table_name = f"{self.config.catalog}.{self.config.schema}.vessel_anomaly_detection_results"
        results_sdf.write.mode("overwrite").saveAsTable(table_name)
        
        print(f"\nResults saved to {table_name}")
    
    def save_model(self, model_dir: str = "/dbfs/models") -> None:
        """Save model and metadata."""
        model_path = f"{model_dir}/ais_autoencoder_model"
        self.model.save(model_path)
        print(f"Model saved to {model_path}")
        
        metadata = {
            "anomaly_threshold": float(self.anomaly_threshold),
            "anomaly_threshold_percentile": self.config.anomaly_threshold_percentile,
            "encoding_dim": self.config.encoding_dim,
            "feature_columns": self.config.feature_columns,
            "tensorflow_version": tf.__version__
        }
        
        metadata_path = f"{model_dir}/ais_autoencoder_metadata.json"
        Path(metadata_path).parent.mkdir(parents=True, exist_ok=True)
        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=2)
        
        print(f"Metadata saved to {metadata_path}")
    
    def run(self) -> Dict[str, Any]:
        """Execute the complete anomaly detection pipeline."""
        print("="*70)
        print("Starting Vessel Anomaly Detection")
        print("="*70)
        print(f"Catalog: {self.config.catalog}")
        print(f"Schema: {self.config.schema}")
        print(f"Encoding dimension: {self.config.encoding_dim}")
        print("="*70)
        
        # Ensure schema exists
        self.spark.sql(f"USE CATALOG {self.config.catalog}")
        self.spark.sql(f"USE SCHEMA {self.config.schema}")
        
        # Load and prepare data
        pdf = self.load_features()
        X, X_train, X_val = self.prepare_training_data(pdf)
        
        # Train model
        self.train_model(X_train, X_val)
        
        # Calculate threshold
        self.calculate_threshold(X_train)
        
        # Detect anomalies
        pdf = self.detect_anomalies(X, pdf)
        
        # Save results
        self.save_results(pdf)
        
        # Save model
        self.save_model()
        
        # Generate summary
        summary = {
            "total_vessels": len(pdf['mmsi'].unique()),
            "total_records": len(pdf),
            "anomalies_detected": int(pdf['is_anomaly'].sum()),
            "anomaly_rate": f"{100 * pdf['is_anomaly'].sum() / len(pdf):.2f}%",
            "anomaly_threshold": f"{self.anomaly_threshold:.6f}"
        }
        
        print("\n" + "="*70)
        print("Anomaly Detection Completed Successfully!")
        print("="*70)
        for key, value in summary.items():
            print(f"{key}: {value}")
        print("="*70)
        
        return summary


def main() -> None:
    """Main entry point for anomaly detection."""
    parser = argparse.ArgumentParser(
        description="Detect vessel anomalies using autoencoder"
    )
    parser.add_argument(
        "--catalog",
        required=True,
        help="Unity Catalog catalog name",
    )
    parser.add_argument(
        "--schema",
        required=True,
        help="Unity Catalog schema name",
    )
    parser.add_argument(
        "--encoding-dim",
        type=int,
        default=16,
        help="Autoencoder bottleneck dimension (default: 16)",
    )
    parser.add_argument(
        "--epochs",
        type=int,
        default=50,
        help="Training epochs (default: 50)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=256,
        help="Training batch size (default: 256)",
    )
    parser.add_argument(
        "--learning-rate",
        type=float,
        default=0.001,
        help="Learning rate (default: 0.001)",
    )
    parser.add_argument(
        "--anomaly-threshold-percentile",
        type=int,
        default=95,
        help="Percentile for anomaly threshold (default: 95)",
    )
    
    args = parser.parse_args()
    
    config = AnomalyDetectionConfig(
        catalog=args.catalog,
        schema=args.schema,
        encoding_dim=args.encoding_dim,
        epochs=args.epochs,
        batch_size=args.batch_size,
        learning_rate=args.learning_rate,
        anomaly_threshold_percentile=args.anomaly_threshold_percentile
    )
    
    detector = AnomalyDetector(config)
    detector.run()


if __name__ == "__main__":
    main()
