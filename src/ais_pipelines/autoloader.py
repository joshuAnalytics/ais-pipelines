# autoloader.py - Auto Loader consumer for Databricks
"""
Sets up a streaming read from the landing volume using Auto Loader and writes to a Delta table.
This enables continuous ingestion of files as they arrive in the landing volume.
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import to_timestamp


def run_autoloader(catalog: str, schema: str, landing_volume: str,
                   schema_location: str, checkpoint_location: str, target_table: str) -> None:
    """
    Run Auto Loader to continuously ingest files from landing volume to Delta table.
    
    Args:
        catalog: Unity Catalog catalog name
        schema: Unity Catalog schema name
        landing_volume: Landing volume name
        schema_location: Relative path within landing volume for schema inference
        checkpoint_location: Relative path within landing volume for checkpoints
        target_table: Fully qualified target Delta table name (e.g., catalog.schema.table)
    """
    spark = SparkSession.builder.getOrCreate()
    
    # Define schema for CSV files
    file_schema = StructType([
        StructField("event_time", StringType()),
        StructField("user_id", StringType()),
        StructField("action", StringType()),
        StructField("value", StringType()),
    ])

    # Construct paths
    landing_path = f"/Volumes/{catalog}/{schema}/{landing_volume}"
    schema_loc = f"{landing_path}/{schema_location}"
    checkpoint_loc = f"{landing_path}/{checkpoint_location}"

    # Read stream using Auto Loader (cloudFiles)
    df = (spark.readStream
          .format("cloudFiles")
          .option("cloudFiles.format", "csv")
          .option("cloudFiles.schemaLocation", schema_loc)
          .option("header", "true")
          .schema(file_schema)
          .load(landing_path))

    # Transform: convert event_time to timestamp
    df = df.withColumn("event_ts", to_timestamp("event_time"))

    # Write stream to Delta table
    (df.writeStream
       .format("delta")
       .option("checkpointLocation", checkpoint_loc)
       .toTable(target_table))


def main() -> None:
    """Main entry point for the autoloader script."""
    parser = argparse.ArgumentParser(
        description="Run Auto Loader to ingest files from landing volume"
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
        "--landing-volume",
        required=True,
        help="Landing volume name",
    )
    parser.add_argument(
        "--schema-location",
        required=True,
        help="Relative path within landing volume for schema inference",
    )
    parser.add_argument(
        "--checkpoint-location",
        required=True,
        help="Relative path within landing volume for checkpoints",
    )
    parser.add_argument(
        "--target-table",
        required=True,
        help="Fully qualified target Delta table name",
    )
    
    args = parser.parse_args()
    
    run_autoloader(
        catalog=args.catalog,
        schema=args.schema,
        landing_volume=args.landing_volume,
        schema_location=args.schema_location,
        checkpoint_location=args.checkpoint_location,
        target_table=args.target_table,
    )


if __name__ == "__main__":
    main()
