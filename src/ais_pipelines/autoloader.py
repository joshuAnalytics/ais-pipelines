# autoloader.py - Auto Loader consumer for Databricks
"""
Sets up a streaming read from the landing volume using Auto Loader and writes to a Delta table.
This enables continuous ingestion of files as they arrive in the landing volume.
"""

import argparse
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.functions import to_timestamp


def parse_schema_from_username(username: str) -> str:
    """Extract schema name from username by removing domain suffix.
    
    Args:
        username: Full username/email)
    
    Returns:
        Schema name without domain
    """
    return username.split("@")[0]


class StreamReader:
    """Handles Auto Loader stream reading configuration."""

    def __init__(
        self, spark: SparkSession, landing_path: str, schema_location: str
    ) -> None:
        self.spark = spark
        self.landing_path = landing_path
        self.schema_location = schema_location

    def read_stream(self) -> DataFrame:
        """Read stream using Auto Loader with schema inference."""
        return (
            self.spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.schemaLocation", self.schema_location)
            .option("header", "true")
            .load(self.landing_path)
        )


class StreamTransformer:
    """Handles data transformations on streaming DataFrame."""

    @staticmethod
    def transform(df: DataFrame) -> DataFrame:
        """Transform streaming data: convert event_time to timestamp."""
        return df.withColumn("event_ts", to_timestamp("event_time"))


class StreamWriter:
    """Handles stream writing to Delta table."""

    def __init__(self, checkpoint_location: str, target_table: str) -> None:
        self.checkpoint_location = checkpoint_location
        self.target_table = target_table

    def write_stream(self, df: DataFrame) -> StreamingQuery:
        """Write stream to Delta table with batch trigger."""
        return (
            df.writeStream.format("delta")
            .option("checkpointLocation", self.checkpoint_location)
            .trigger(availableNow=True)
            .toTable(self.target_table)
            .start()
        )


class AutoLoaderOrchestrator:
    """Orchestrates the Auto Loader streaming process."""

    def __init__(
        self,
        catalog: str,
        schema: str,
        landing_volume: str,
        schema_location: str,
        checkpoint_location: str,
        target_table: str,
    ) -> None:
        self.spark = SparkSession.builder.getOrCreate()
        self.catalog = catalog
        self.schema = schema
        self.landing_volume = landing_volume
        self.target_table = target_table

        self.landing_path = f"/Volumes/{catalog}/{schema}/{landing_volume}"
        self.schema_loc = f"{self.landing_path}/{schema_location}"
        self.checkpoint_loc = f"{self.landing_path}/{checkpoint_location}"

        self.reader = StreamReader(self.spark, self.landing_path, self.schema_loc)
        self.writer = StreamWriter(self.checkpoint_loc, target_table)

    def run(self) -> StreamingQuery:
        """Execute the Auto Loader workflow."""
        print(f"Starting Auto Loader: {self.landing_path} -> {self.target_table}")
        df = self.reader.read_stream()
        df_transformed = StreamTransformer.transform(df)
        query = self.writer.write_stream(df_transformed)
        query.awaitTermination()
        return query


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
        "--username",
        required=True,
        help="Workspace username (email) - schema name will be derived from this",
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

    # Parse schema from username
    schema = parse_schema_from_username(args.username)

    autoloader = AutoLoaderOrchestrator(
        catalog=args.catalog,
        schema=schema,
        landing_volume=args.landing_volume,
        schema_location=args.schema_location,
        checkpoint_location=args.checkpoint_location,
        target_table=args.target_table,
    )
    autoloader.run()


if __name__ == "__main__":
    main()
