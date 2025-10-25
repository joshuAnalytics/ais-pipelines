# autoloader.py - Auto Loader consumer for Databricks
"""
Sets up a streaming read from the landing volume using Auto Loader and writes to a Delta table.
This enables continuous ingestion of files as they arrive in the landing volume.
"""

import argparse
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.functions import to_timestamp


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


class PostProcessor:
    """Handles post-processing operations on Delta table after stream write."""

    def __init__(self, spark: SparkSession, target_table: str) -> None:
        self.spark = spark
        self.target_table = target_table

    def _column_exists(self, column_name: str) -> bool:
        """Check if a column exists in the target table."""
        try:
            columns = self.spark.table(self.target_table).columns
            return column_name in columns
        except Exception:
            return False

    def add_spatial_columns(self) -> None:
        """Add spatial geometry and H3 index columns to the table."""
        print(f"Starting spatial post-processing for {self.target_table}...")

        # Step 1: Add point_geom column if it doesn't exist
        if not self._column_exists("point_geom"):
            print("  - Adding point_geom column...")
            self.spark.sql(f"""
                ALTER TABLE {self.target_table}
                ADD COLUMNS (point_geom GEOMETRY)
            """)
        else:
            print("  - point_geom column already exists")

        # Step 2: Populate point_geom from lat/lon
        print("  - Populating point geometries from lat/lon...")
        self.spark.sql(f"""
            UPDATE {self.target_table}
            SET point_geom = ST_Point(longitude, latitude, 4326)
            WHERE point_geom IS NULL
        """)

        # Step 3: Add validation column
        if not self._column_exists("is_valid_geom"):
            print("  - Adding is_valid_geom column...")
            self.spark.sql(f"""
                ALTER TABLE {self.target_table}
                ADD COLUMNS (is_valid_geom BOOLEAN)
            """)
        else:
            print("  - is_valid_geom column already exists")

        # Step 4: Validate geometries
        print("  - Validating geometries...")
        self.spark.sql(f"""
            UPDATE {self.target_table}
            SET is_valid_geom = ST_IsValid(point_geom)
            WHERE is_valid_geom IS NULL
        """)

        # Step 5: Add H3 index columns
        h3_columns_exist = (
            self._column_exists("h3_res9")
            and self._column_exists("h3_res10")
            and self._column_exists("h3_res11")
        )
        if not h3_columns_exist:
            print("  - Adding H3 index columns...")
            self.spark.sql(f"""
                ALTER TABLE {self.target_table}
                ADD COLUMNS (
                    h3_res9 STRING,
                    h3_res10 STRING,
                    h3_res11 STRING
                )
            """)
        else:
            print("  - H3 index columns already exist")

        # Step 6: Populate H3 indices
        print("  - Generating H3 indices at resolutions 9-11...")
        self.spark.sql(f"""
            UPDATE {self.target_table}
            SET
                h3_res9 = h3_pointash3(st_astext(point_geom), 9),
                h3_res10 = h3_pointash3(st_astext(point_geom), 10),
                h3_res11 = h3_pointash3(st_astext(point_geom), 11)
            WHERE h3_res9 IS NULL
        """)

        print(f"Spatial processing completed for {self.target_table}")


class AutoLoaderOrchestrator:
    """Orchestrates the Auto Loader streaming process with optional post-processing."""

    def __init__(
        self,
        catalog: str,
        schema: str,
        landing_volume: str,
        schema_location: str,
        checkpoint_location: str,
        target_table: str,
        enable_spatial_processing: bool = True,
    ) -> None:
        self.spark = SparkSession.builder.getOrCreate()
        self.catalog = catalog
        self.schema = schema
        self.landing_volume = landing_volume
        self.target_table = target_table
        self.enable_spatial_processing = enable_spatial_processing

        self.landing_path = f"/Volumes/{catalog}/{schema}/{landing_volume}"
        self.schema_loc = f"{self.landing_path}/{schema_location}"
        self.checkpoint_loc = f"{self.landing_path}/{checkpoint_location}"

        self.reader = StreamReader(self.spark, self.landing_path, self.schema_loc)
        self.writer = StreamWriter(self.checkpoint_loc, target_table)
        self.post_processor = PostProcessor(self.spark, target_table)

    def run(self) -> StreamingQuery:
        """Execute the Auto Loader workflow with optional spatial post-processing."""
        print(f"Starting Auto Loader: {self.landing_path} -> {self.target_table}")
        df = self.reader.read_stream()
        df_transformed = StreamTransformer.transform(df)
        query = self.writer.write_stream(df_transformed)
        query.awaitTermination()
        
        # Run post-processing if enabled
        if self.enable_spatial_processing:
            self.post_processor.add_spatial_columns()
        
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
    parser.add_argument(
        "--enable-spatial-processing",
        action="store_true",
        default=True,
        help="Enable spatial post-processing (point_geom, validation, H3 indices)",
    )
    parser.add_argument(
        "--disable-spatial-processing",
        action="store_false",
        dest="enable_spatial_processing",
        help="Disable spatial post-processing",
    )

    args = parser.parse_args()

    schema = args.schema

    autoloader = AutoLoaderOrchestrator(
        catalog=args.catalog,
        schema=schema,
        landing_volume=args.landing_volume,
        schema_location=args.schema_location,
        checkpoint_location=args.checkpoint_location,
        target_table=args.target_table,
        enable_spatial_processing=args.enable_spatial_processing,
    )
    autoloader.run()


if __name__ == "__main__":
    main()
