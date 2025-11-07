# create_ais_records.py - Batch processor for AIS records with spatial indexing
"""
Reads all CSV files from the landing volume and creates the ais_records table
with spatial geometry columns and H3 indices at multiple resolutions.
"""

import argparse
from typing import List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import to_timestamp, col


class CsvReader:
    """Handles reading CSV files from volume."""

    def __init__(self, spark: SparkSession, volume_path: str, limit: int = 0) -> None:
        self.spark = spark
        self.volume_path = volume_path
        self.limit = limit

    def read_all_csvs(self) -> DataFrame:
        """Read CSV files from the volume using glob pattern, optionally limiting the number of files."""
        import glob
        import os
        
        csv_pattern = f"{self.volume_path}/*.csv"
        
        # Get list of CSV files
        csv_files = sorted(glob.glob(csv_pattern))
        total_files = len(csv_files)
        
        # Apply limit if specified
        if self.limit > 0 and self.limit < total_files:
            csv_files = csv_files[:self.limit]
            print(f"Reading {len(csv_files)} of {total_files} CSV files (limit={self.limit})")
        else:
            print(f"Reading all {total_files} CSV files")
        
        if not csv_files:
            raise ValueError(f"No CSV files found in {self.volume_path}")
        
        # Read the selected files
        df = (
            self.spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(csv_files)
        )
        
        row_count = df.count()
        print(f"Loaded {row_count:,} records from {len(csv_files)} CSV file(s)")
        return df


class DataTransformer:
    """Handles data transformations."""

    @staticmethod
    def add_timestamp(df: DataFrame) -> DataFrame:
        """Add timestamp column from BaseDateTime string."""
        return df.withColumn(
            "timestamp",
            to_timestamp(col("BaseDateTime"), "yyyy-MM-dd'T'HH:mm:ss")
        )


class SpatialTableCreator:
    """Creates Delta table with spatial columns using SQL."""

    def __init__(self, spark: SparkSession, full_table_name: str) -> None:
        self.spark = spark
        self.full_table_name = full_table_name

    def create_table_with_spatial_columns(self, df: DataFrame) -> None:
        """
        Create Delta table with all spatial columns in a single operation.
        
        Adds:
        - point_geom: GEOMETRY point from longitude/latitude
        - is_valid_geom: Boolean validation of geometry
        - h3_res5-9: H3 indices at resolutions 5, 6, 7, 8, 9
        """
        print(f"Creating table: {self.full_table_name}")
        
        # First write the base data to a temp view
        temp_view = "ais_records_temp"
        df.createOrReplaceTempView(temp_view)
        
        # Create table with all spatial columns using CREATE OR REPLACE TABLE AS SELECT
        print("Adding spatial columns and H3 indices...")
        self.spark.sql(f"""
            CREATE OR REPLACE TABLE {self.full_table_name} AS
            SELECT 
                *,
                ST_Point(LON, LAT, 4326) AS point_geom,
                ST_IsValid(ST_Point(LON, LAT, 4326)) AS is_valid_geom,
                h3_pointash3(ST_AsText(ST_Point(LON, LAT, 4326)), 5) AS h3_res5,
                h3_pointash3(ST_AsText(ST_Point(LON, LAT, 4326)), 6) AS h3_res6,
                h3_pointash3(ST_AsText(ST_Point(LON, LAT, 4326)), 7) AS h3_res7,
                h3_pointash3(ST_AsText(ST_Point(LON, LAT, 4326)), 8) AS h3_res8,
                h3_pointash3(ST_AsText(ST_Point(LON, LAT, 4326)), 9) AS h3_res9
            FROM {temp_view}
        """)
        
        print(f"Successfully created table: {self.full_table_name}")


class DataQualityValidator:
    """Validates data quality of created table."""

    def __init__(self, spark: SparkSession, full_table_name: str) -> None:
        self.spark = spark
        self.full_table_name = full_table_name

    def print_summary_stats(self) -> None:
        """Print summary statistics for the created table."""
        print("\n" + "="*60)
        print("Data Quality Summary")
        print("="*60)
        
        # Row count
        total_rows = self.spark.table(self.full_table_name).count()
        print(f"Total records: {total_rows:,}")
        
        # Geometry validation
        geom_validation = self.spark.sql(f"""
            SELECT
                is_valid_geom,
                COUNT(*) AS count
            FROM {self.full_table_name}
            GROUP BY is_valid_geom
        """).collect()
        
        print("\nGeometry validation:")
        for row in geom_validation:
            print(f"  Valid={row['is_valid_geom']}: {row['count']:,} records")
        
        # Sample data
        print("\nSample records with spatial columns:")
        self.spark.sql(f"""
            SELECT 
                BaseDateTime,
                LAT,
                LON,
                point_geom,
                is_valid_geom,
                h3_res5,
                h3_res9
            FROM {self.full_table_name} 
            LIMIT 3
        """).show(truncate=False)
        
        print("="*60 + "\n")


class AisRecordsOrchestrator:
    """Orchestrates the AIS records creation process."""

    def __init__(
        self,
        catalog: str,
        schema: str,
        landing_volume: str,
        target_table: str = "ais_records",
        limit: int = 0
    ) -> None:
        self.spark = SparkSession.builder.getOrCreate()
        self.catalog = catalog
        self.schema = schema
        self.landing_volume = landing_volume
        self.target_table = target_table
        self.limit = limit
        
        self.volume_path = f"/Volumes/{catalog}/{schema}/{landing_volume}"
        self.full_table_name = f"{catalog}.{schema}.{target_table}"
        
        self.reader = CsvReader(self.spark, self.volume_path, limit)
        self.table_creator = SpatialTableCreator(self.spark, self.full_table_name)
        self.validator = DataQualityValidator(self.spark, self.full_table_name)

    def run(self) -> None:
        """Execute the AIS records creation workflow."""
        print("Starting AIS records creation process...")
        print(f"Source: {self.volume_path}")
        print(f"Target: {self.full_table_name}\n")
        
        # Ensure schema exists
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.schema}")
        
        # Read all CSVs
        df = self.reader.read_all_csvs()
        
        # Transform data
        df_transformed = DataTransformer.add_timestamp(df)
        
        # Create table with spatial columns
        self.table_creator.create_table_with_spatial_columns(df_transformed)
        
        # Validate results
        self.validator.print_summary_stats()
        
        print("AIS records creation completed successfully!")


def main() -> None:
    """Main entry point for the create_ais_records script."""
    parser = argparse.ArgumentParser(
        description="Create AIS records table with spatial columns from all CSVs in volume"
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
        help="Landing volume name containing CSV files",
    )
    parser.add_argument(
        "--target-table",
        default="ais_records",
        help="Target table name (default: ais_records)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Maximum number of CSV files to process (0 = all files)",
    )

    args = parser.parse_args()

    orchestrator = AisRecordsOrchestrator(
        catalog=args.catalog,
        schema=args.schema,
        landing_volume=args.landing_volume,
        target_table=args.target_table,
        limit=args.limit,
    )
    orchestrator.run()


if __name__ == "__main__":
    main()
