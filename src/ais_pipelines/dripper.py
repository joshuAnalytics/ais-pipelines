"""Drip files from source volume to landing volume for controlled ingestion."""

import argparse
from typing import List
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils


class UnityUtilities:
    """Handles Unity Catalog operations for catalog, schema, and volume management."""

    def __init__(self, spark: SparkSession, catalog: str, schema: str) -> None:
        self.spark = spark
        self.catalog = catalog
        self.schema = schema

    def ensure_catalog_exists(self) -> None:
        """Create catalog if it doesn't exist."""
        self.spark.sql(f"CREATE CATALOG IF NOT EXISTS {self.catalog}")

    def ensure_schema_exists(self) -> None:
        """Create schema if it doesn't exist."""
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.schema}")

    def ensure_volume_exists(self, volume: str) -> None:
        """Create volume if it doesn't exist."""
        self.spark.sql(
            f"CREATE VOLUME IF NOT EXISTS {self.catalog}.{self.schema}.{volume}"
        )


class FileManager:
    """Manages file listing and filtering operations."""

    def __init__(self, spark: SparkSession, source_path: str, landing_path: str) -> None:
        self.spark = spark
        self.source_path = source_path
        self.landing_path = landing_path
        self.dbutils = DBUtils(spark)

    def get_candidate_files(self, n_per_run: int) -> List:
        """Get list of unprocessed candidate files to process."""
        all_files = self._list_files()
        filtered_by_extension = self._filter_by_extension(all_files)
        landing_files = self._get_landing_files()
        unprocessed_files = self._filter_unprocessed(filtered_by_extension, landing_files)
        sorted_files = sorted(unprocessed_files, key=lambda x: x.name)
        return sorted_files[:n_per_run]

    def _list_files(self) -> List:
        """List all files in the source volume."""
        return self.dbutils.fs.ls(self.source_path)

    def _filter_by_extension(self, files: List) -> List:
        """Filter files by allowed extensions."""
        return [
            f for f in files
            if f.path.endswith(".csv.zst") or f.path.endswith(".zip")
        ]

    def _get_landing_files(self) -> set:
        """Get set of filenames already in landing volume."""
        try:
            landing_files = self.dbutils.fs.ls(self.landing_path)
            return {f.name for f in landing_files}
        except Exception:
            return set()

    def _filter_unprocessed(self, files: List, landing_files: set) -> List:
        """Filter out files that already exist in landing volume."""
        return [f for f in files if f.name not in landing_files]


class FileDripper:
    """Handles file move/copy operations between volumes."""

    def __init__(self, spark: SparkSession, destination_path: str) -> None:
        self.spark = spark
        self.destination_path = destination_path
        self.dbutils = DBUtils(spark)

    def drip_file(self, file_info, delete_source: bool) -> None:
        """Move or copy a single file to destination."""
        dest_path = f"{self.destination_path}/{file_info.name}"

        if delete_source:
            self.dbutils.fs.mv(file_info.path, dest_path)
        else:
            self.dbutils.fs.cp(file_info.path, dest_path)


class DripperOrchestrator:
    """Orchestrates the file dripping process."""

    def __init__(
        self,
        catalog: str,
        schema: str,
        source_volume: str,
        landing_volume: str,
        n_per_run: int,
        delete_source: bool,
    ) -> None:
        self.spark = SparkSession.builder.getOrCreate()
        self.catalog = catalog
        self.schema = schema
        self.source_volume = source_volume
        self.landing_volume = landing_volume
        self.n_per_run = n_per_run
        self.delete_source = delete_source

        self.unity = UnityUtilities(self.spark, catalog, schema)
        self.source_path = f"/Volumes/{catalog}/{schema}/{source_volume}"
        self.landing_path = f"/Volumes/{catalog}/{schema}/{landing_volume}"
        self.file_manager = FileManager(self.spark, self.source_path, self.landing_path)
        self.file_dripper = FileDripper(self.spark, self.landing_path)

    def run(self) -> None:
        """Execute the file dripping workflow."""
        self._setup_infrastructure()
        
        # Check if landing already contains all source files
        if self._check_if_landing_full():
            print("Landing volume already contains all source files. Exiting gracefully.")
            return
        
        candidates = self._get_candidate_files()
        self._process_files(candidates)
        self._print_summary(len(candidates))

    def _setup_infrastructure(self) -> None:
        """Ensure catalog, schema, and volumes exist."""
        self.unity.ensure_catalog_exists()
        self.unity.ensure_schema_exists()
        self.unity.ensure_volume_exists(self.source_volume)
        self.unity.ensure_volume_exists(self.landing_volume)
        print(f"Volumes ready: {self.catalog}.{self.schema}.{self.source_volume} -> {self.catalog}.{self.schema}.{self.landing_volume}")

    def _check_if_landing_full(self) -> bool:
        """Check if landing volume already contains all source files.
        
        Returns:
            bool: True if landing contains all source files, False otherwise.
        """
        # Get all source files filtered by extension
        all_source_files = self.file_manager._list_files()
        filtered_source_files = self.file_manager._filter_by_extension(all_source_files)
        source_filenames = {f.name for f in filtered_source_files}
        
        # Get all landing files
        landing_filenames = self.file_manager._get_landing_files()
        
        # Check if landing contains all source files
        return source_filenames.issubset(landing_filenames) and len(source_filenames) > 0

    def _get_candidate_files(self) -> List:
        """Get list of files to process."""
        return self.file_manager.get_candidate_files(self.n_per_run)

    def _process_files(self, candidates: List) -> None:
        """Process each candidate file."""
        for file_info in candidates:
            self.file_dripper.drip_file(file_info, self.delete_source)

    def _print_summary(self, file_count: int) -> None:
        """Print processing summary."""
        print(f"Released {file_count} file(s) -> {self.landing_path}")


def main() -> None:
    """Main entry point for the dripper script."""
    parser = argparse.ArgumentParser(
        description="Drip files from source volume to landing volume"
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
        "--source-volume",
        required=True,
        help="Source volume name",
    )
    parser.add_argument(
        "--landing-volume",
        required=True,
        help="Landing volume name",
    )
    parser.add_argument(
        "--n-per-run",
        type=int,
        required=True,
        help="Number of files to release per run",
    )
    parser.add_argument(
        "--delete-source",
        type=lambda x: x if isinstance(x, bool) else x.lower() == "true",
        default=True,
        help="Whether to delete source files after copying (true/false)",
    )

    args = parser.parse_args()

    dripper = DripperOrchestrator(
        catalog=args.catalog,
        schema=args.schema,
        source_volume=args.source_volume,
        landing_volume=args.landing_volume,
        n_per_run=args.n_per_run,
        delete_source=args.delete_source,
    )
    dripper.run()


if __name__ == "__main__":
    main()
