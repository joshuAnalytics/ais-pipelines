# dripper.py - Drip files from source volume to landing volume
"""
Gradually releases files from a source volume to a landing volume for streaming ingestion.
This allows for controlled, time-partitioned data ingestion.
"""

import argparse
import uuid
import datetime
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils


def run_dripper(catalog: str, schema: str, source_volume: str, landing_volume: str, 
                n_per_run: int, delete_source: bool = True) -> None:
    """
    Release files from source volume to landing volume.
    
    Args:
        catalog: Unity Catalog catalog name
        schema: Unity Catalog schema name
        source_volume: Source volume name containing files to drip
        landing_volume: Destination volume name for landing files
        n_per_run: Number of files to release per run
        delete_source: Whether to delete source files after copying
    """
    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)

    src_root = f"/Volumes/{catalog}/{schema}/{source_volume}"
    dst_root = f"/Volumes/{catalog}/{schema}/{landing_volume}"
    staging = f"{dst_root}/_staging"

    def now_parts():
        utc = datetime.datetime.utcnow()
        return f"dt={utc.date()}/hr={utc.hour}"

    # Ensure staging dir exists
    dbutils.fs.mkdirs(staging)

    # List a batch from source volume
    candidates = [f for f in dbutils.fs.ls(src_root) if f.path.endswith(".csv")]
    candidates = sorted(candidates, key=lambda x: x.name)[:n_per_run]

    for f in candidates:
        # Copy to staging so readers never see partials; cp on cloud storage creates a full object
        tmp_name = f"{uuid.uuid4().hex}.csv"
        dbutils.fs.cp(f.path, f"{staging}/{tmp_name}")  # copy
        # Move into a time-partitioned folder in landing
        rel = now_parts()
        dest_dir = f"{dst_root}/{rel}"
        dbutils.fs.mkdirs(dest_dir)
        dbutils.fs.mv(f"{staging}/{tmp_name}", f"{dest_dir}/{f.name}")  # move within landing
        # Optionally delete source (or keep as archive)
        if delete_source:
            dbutils.fs.rm(f.path)

    print(f"Released {len(candidates)} file(s) -> {dst_root}")


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
        type=lambda x: x.lower() == "true",
        default=True,
        help="Whether to delete source files after copying (true/false)",
    )
    
    args = parser.parse_args()
    
    run_dripper(
        catalog=args.catalog,
        schema=args.schema,
        source_volume=args.source_volume,
        landing_volume=args.landing_volume,
        n_per_run=args.n_per_run,
        delete_source=args.delete_source,
    )


if __name__ == "__main__":
    main()
