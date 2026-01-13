# spatial_features.py - Spatial enrichment with EEZ boundaries
"""
Enriches AIS records with Exclusive Economic Zone (EEZ) spatial features.
Performs spatial joins between vessel positions and EEZ polygons to determine
which maritime zone each vessel is in.
"""

import argparse
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, lit


class EEZTableLoader:
    """Loads EEZ polygons from shapefile to Unity Catalog."""

    def __init__(
        self,
        spark: SparkSession,
        full_table_name: str,
        shapefile_path: str
    ) -> None:
        self.spark = spark
        self.full_table_name = full_table_name
        self.shapefile_path = shapefile_path

    def load(self) -> int:
        """Load EEZ polygons to Unity Catalog table and return row count."""
        print(f"\nLoading EEZ polygons from: {self.shapefile_path}")
        print(f"Target table: {self.full_table_name}")

        # Read GeoParquet file (optimized for North America + Caribbean + Mexico)
        parquet_path = f"{self.shapefile_path}/eez_north_america.parquet"
        df_raw = self.spark.read.parquet(parquet_path)

        print(f"Read {df_raw.count():,} EEZ polygons from parquet")

        # Create structured table with geometry conversion
        query = f"""
        CREATE OR REPLACE TABLE {self.full_table_name} AS
        SELECT
            MRGID as mrgid,
            GEONAME as geoname,
            SOVEREIGN1 as sovereign1,
            TERRITORY1 as territory1,
            POL_TYPE as pol_type,
            AREA_KM2 as area_km2,
            ST_GeomFromWKB(geometry) as polygon_geom,
            ST_IsValid(ST_GeomFromWKB(geometry)) as is_valid_geom
        FROM __this_df__
        """

        # Register temp view and create table
        df_raw.createOrReplaceTempView("__this_df__")
        self.spark.sql(query)

        row_count = self.spark.table(self.full_table_name).count()
        print(f"Created {self.full_table_name}: {row_count:,} EEZ polygons")

        # Validate geometries
        invalid_count = self.spark.sql(f"""
            SELECT COUNT(*) as cnt
            FROM {self.full_table_name}
            WHERE is_valid_geom = false
        """).collect()[0]['cnt']

        if invalid_count > 0:
            print(f"WARNING: {invalid_count} invalid geometries found")
        else:
            print("All geometries are valid")

        return row_count


class SpatialEnrichment:
    """Enriches AIS records with EEZ spatial features."""

    def __init__(
        self,
        spark: SparkSession,
        ais_records_table: str,
        eez_table: str,
        output_table: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> None:
        self.spark = spark
        self.ais_records_table = ais_records_table
        self.eez_table = eez_table
        self.output_table = output_table
        self.start_date = start_date
        self.end_date = end_date

    def enrich(self) -> int:
        """Perform spatial join to enrich AIS records with EEZ features."""
        print(f"\nEnriching AIS records with EEZ spatial features")
        print(f"Source: {self.ais_records_table}")
        print(f"EEZ table: {self.eez_table}")
        print(f"Target: {self.output_table}")

        # Build date filter clause
        date_filter = ""
        if self.start_date and self.end_date:
            date_filter = f"""
            WHERE ais.timestamp >= '{self.start_date}'
              AND ais.timestamp <= '{self.end_date}'
            """
            print(f"Date range: {self.start_date} to {self.end_date}")
        elif self.start_date:
            date_filter = f"WHERE ais.timestamp >= '{self.start_date}'"
            print(f"Start date: {self.start_date}")
        elif self.end_date:
            date_filter = f"WHERE ais.timestamp <= '{self.end_date}'"
            print(f"End date: {self.end_date}")
        else:
            print("Processing all records")

        # Perform spatial join with H3-based optimization
        query = f"""
        CREATE OR REPLACE TABLE {self.output_table} AS
        WITH ais_with_bounds AS (
          -- Get H3 cells at resolution 4 for broad spatial filtering
          SELECT
            ais.mmsi,
            ais.timestamp,
            ais.latitude,
            ais.longitude,
            ais.h3_res8,
            ais.h3_res7,
            ais.h3_res6,
            ais.point_geom,
            h3_pointash3(ST_AsText(ais.point_geom), 4) as h3_res4_filter
          FROM {self.ais_records_table} ais
          {date_filter}
        ),
        eez_with_h3_coverage AS (
          -- Get H3 cells that each EEZ polygon covers (resolution 4)
          SELECT
            eez.*,
            explode(h3_polyfill(eez.polygon_geom, 4)) as h3_res4_filter
          FROM {self.eez_table} eez
          WHERE eez.is_valid_geom = true
        ),
        spatial_join AS (
          -- Join on H3 cells first, then validate with precise spatial check
          SELECT
            ais.mmsi,
            ais.timestamp,
            ais.latitude,
            ais.longitude,
            ais.h3_res8,
            ais.h3_res7,
            ais.h3_res6,
            eez.mrgid as eez_mrgid,
            eez.geoname as eez_name,
            eez.sovereign1 as eez_sovereign,
            eez.territory1 as eez_territory,
            eez.pol_type as eez_pol_type,
            eez.area_km2 as eez_area_km2,
            -- Precise spatial check
            ST_Contains(eez.polygon_geom, ais.point_geom) as is_in_eez_precise,
            -- Track if multiple EEZs contain this point (overlapping claims)
            COUNT(*) OVER (PARTITION BY ais.mmsi, ais.timestamp) as eez_match_count
          FROM ais_with_bounds ais
          LEFT JOIN eez_with_h3_coverage eez
            ON ais.h3_res4_filter = eez.h3_res4_filter
            AND ST_Contains(eez.polygon_geom, ais.point_geom)
        )
        SELECT
          -- Join keys (for efficient joins with ais_records)
          mmsi,
          timestamp,
          h3_res8,
          h3_res7,
          h3_res6,
          
          -- Spatial reference (optional, for quick lookup)
          latitude,
          longitude,
          
          -- EEZ spatial features
          eez_mrgid,
          eez_name,
          eez_sovereign,
          eez_territory,
          eez_pol_type,
          CAST(eez_area_km2 AS DOUBLE) as eez_area_km2,
          CASE WHEN is_in_eez_precise = true THEN 1 ELSE 0 END as is_in_eez,
          CASE WHEN eez_match_count > 1 THEN 1 ELSE 0 END as is_in_overlapping_eez,
          eez_match_count
          
        FROM spatial_join
        """

        self.spark.sql(query)

        # Optimize with Z-ordering
        print(f"Optimizing {self.output_table} with Z-ordering...")
        self.spark.sql(f"""
        OPTIMIZE {self.output_table}
        ZORDER BY (mmsi, timestamp, h3_res8)
        """)

        row_count = self.spark.table(self.output_table).count()
        print(f"Created {self.output_table}: {row_count:,} records")

        return row_count


class EnrichmentValidator:
    """Validates spatial enrichment results."""

    def __init__(self, spark: SparkSession, output_table: str) -> None:
        self.spark = spark
        self.output_table = output_table

    def print_summary_stats(self) -> None:
        """Print summary statistics for enrichment results."""
        print("\n" + "="*70)
        print("Spatial Enrichment Summary")
        print("="*70)

        # Total records
        total_rows = self.spark.table(self.output_table).count()
        print(f"Total records: {total_rows:,}")

        # EEZ coverage
        eez_stats = self.spark.sql(f"""
            SELECT
                SUM(is_in_eez) as in_eez_count,
                SUM(CASE WHEN is_in_eez = 0 THEN 1 ELSE 0 END) as outside_eez_count,
                SUM(is_in_overlapping_eez) as overlapping_eez_count,
                COUNT(*) as total
            FROM {self.output_table}
        """).collect()[0]

        print(f"\nEEZ Coverage:")
        print(f"  Inside EEZ: {eez_stats['in_eez_count']:,} ({100*eez_stats['in_eez_count']/total_rows:.1f}%)")
        print(f"  Outside EEZ: {eez_stats['outside_eez_count']:,} ({100*eez_stats['outside_eez_count']/total_rows:.1f}%)")
        print(f"  In overlapping zones: {eez_stats['overlapping_eez_count']:,}")

        # Top EEZs by vessel observations
        print("\nTop 10 EEZs by vessel observations:")
        top_eezs = self.spark.sql(f"""
            SELECT
                eez_name,
                eez_sovereign,
                COUNT(*) as observation_count,
                COUNT(DISTINCT mmsi) as unique_vessels
            FROM {self.output_table}
            WHERE is_in_eez = 1
            GROUP BY eez_name, eez_sovereign
            ORDER BY observation_count DESC
            LIMIT 10
        """)
        top_eezs.show(truncate=False)

        # Sample enriched records
        print("\nSample enriched records:")
        self.spark.sql(f"""
            SELECT
                mmsi,
                timestamp,
                latitude,
                longitude,
                eez_name,
                eez_sovereign,
                is_in_eez,
                is_in_overlapping_eez
            FROM {self.output_table}
            WHERE is_in_eez = 1
            LIMIT 5
        """).show(truncate=False)

        print("="*70 + "\n")


class SpatialFeaturesOrchestrator:
    """Orchestrates the spatial features enrichment process."""

    def __init__(
        self,
        catalog: str,
        schema: str,
        shapefile_path: str = "notebooks/data/eez",
        ais_records_table: str = "ais_records",
        eez_table: str = "eez_polygons",
        output_table: str = "vessel_eez_features",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        skip_eez_load: bool = False
    ) -> None:
        self.spark = SparkSession.builder.getOrCreate()
        self.catalog = catalog
        self.schema = schema
        self.shapefile_path = shapefile_path
        self.start_date = start_date
        self.end_date = end_date
        self.skip_eez_load = skip_eez_load

        # Table names
        self.ais_records_table = f"{catalog}.{schema}.{ais_records_table}"
        self.eez_table = f"{catalog}.{schema}.{eez_table}"
        self.output_table = f"{catalog}.{schema}.{output_table}"

    def run(self) -> None:
        """Execute the spatial features enrichment workflow."""
        print("="*70)
        print("Starting Spatial Features Enrichment")
        print("="*70)
        print(f"Catalog: {self.catalog}")
        print(f"Schema: {self.schema}")
        print(f"AIS records: {self.ais_records_table}")
        print(f"EEZ table: {self.eez_table}")
        print(f"Output table: {self.output_table}")
        print("="*70)

        # Ensure schema exists
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.schema}")
        self.spark.sql(f"USE CATALOG {self.catalog}")
        self.spark.sql(f"USE SCHEMA {self.schema}")

        # Step 1: Load EEZ polygons (if not skipped)
        if not self.skip_eez_load:
            eez_loader = EEZTableLoader(
                self.spark,
                self.eez_table,
                self.shapefile_path
            )
            eez_count = eez_loader.load()

            if eez_count == 0:
                print("\nERROR: No EEZ polygons loaded. Stopping pipeline.")
                return
        else:
            print("\nSkipping EEZ table load (--skip-eez-load flag set)")
            eez_count = self.spark.table(self.eez_table).count()
            print(f"Using existing EEZ table: {eez_count:,} polygons")

        # Step 2: Enrich AIS records with spatial features
        enrichment = SpatialEnrichment(
            self.spark,
            self.ais_records_table,
            self.eez_table,
            self.output_table,
            self.start_date,
            self.end_date
        )
        output_count = enrichment.enrich()

        if output_count == 0:
            print("\nWARNING: No records created. Check date range and data availability.")
            return

        # Step 3: Validate results
        validator = EnrichmentValidator(self.spark, self.output_table)
        validator.print_summary_stats()

        print("="*70)
        print("Spatial Features Enrichment Completed Successfully!")
        print("="*70)


def main() -> None:
    """Main entry point for the spatial features script."""
    parser = argparse.ArgumentParser(
        description="Enrich AIS records with EEZ spatial features"
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
        "--shapefile-path",
        default="notebooks/data/eez",
        help="Path to EEZ shapefile directory (default: notebooks/data/eez)",
    )
    parser.add_argument(
        "--ais-records-table",
        default="ais_records",
        help="AIS records table name (default: ais_records)",
    )
    parser.add_argument(
        "--eez-table",
        default="eez_polygons",
        help="EEZ polygons table name (default: eez_polygons)",
    )
    parser.add_argument(
        "--output-table",
        default="vessel_eez_features",
        help="Output table name (default: vessel_eez_features)",
    )
    parser.add_argument(
        "--start-date",
        help="Start date filter (YYYY-MM-DD, optional)",
    )
    parser.add_argument(
        "--end-date",
        help="End date filter (YYYY-MM-DD, optional)",
    )
    parser.add_argument(
        "--skip-eez-load",
        action="store_true",
        help="Skip loading EEZ table (use existing table)",
    )

    args = parser.parse_args()

    orchestrator = SpatialFeaturesOrchestrator(
        catalog=args.catalog,
        schema=args.schema,
        shapefile_path=args.shapefile_path,
        ais_records_table=args.ais_records_table,
        eez_table=args.eez_table,
        output_table=args.output_table,
        start_date=args.start_date,
        end_date=args.end_date,
        skip_eez_load=args.skip_eez_load,
    )
    orchestrator.run()


if __name__ == "__main__":
    main()
