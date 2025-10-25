"""Decompress .csv.zst files from history volume to landing volume for Auto Loader ingestion."""

import argparse
import io
from typing import List
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

try:
    import zstandard as zstd
except ImportError:
    zstd = None


class UnityUtilities:
    """Handles Unity Catalog operations for catalog, schema, and volume management."""

    def __init__(self, spark: SparkSession, catalog: str, schema: str) -> None:
        self.spark = spark
        self.catalog = catalog
        self.schema = schema

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

    def get_compressed_files(self, limit: int = None) -> List:
        """Get list of compressed files to process from source volume."""
        source_files = self._list_files()
        compressed_files = self._filter_compressed(source_files)
        
        # Get existing decompressed files in landing volume
        landing_files = self._list_landing_files()
        decompressed_names = self._get_decompressed_filenames(landing_files)
        
        # Filter out files that already have decompressed versions in landing
        files_to_process = []
        for f in compressed_files:
            expected_csv = f.name.replace('.csv.zst', '.csv').replace('.zip', '.csv')
            if expected_csv not in decompressed_names:
                files_to_process.append(f)
        
        sorted_files = sorted(files_to_process, key=lambda x: x.name)
        
        if limit and limit > 0:
            return sorted_files[:limit]
        return sorted_files

    def _list_files(self) -> List:
        """List all files in the source (history) volume."""
        try:
            return self.dbutils.fs.ls(self.source_path)
        except Exception:
            return []

    def _list_landing_files(self) -> List:
        """List all files in the landing volume."""
        try:
            return self.dbutils.fs.ls(self.landing_path)
        except Exception:
            return []

    def _filter_compressed(self, files: List) -> List:
        """Filter files by compressed extensions."""
        return [
            f for f in files 
            if f.path.endswith(".csv.zst") or f.path.endswith(".zip")
        ]

    def _get_decompressed_filenames(self, files: List) -> set:
        """Get set of decompressed CSV filenames already present."""
        return {
            f.name for f in files 
            if f.path.endswith(".csv") and not f.path.endswith(".csv.zst")
        }


class FileDecompressor:
    """Handles file decompression operations."""

    def __init__(self, spark: SparkSession, landing_path: str, delete_compressed: bool) -> None:
        self.spark = spark
        self.landing_path = landing_path
        self.delete_compressed = delete_compressed
        self.dbutils = DBUtils(spark)

    def decompress_file(self, file_info) -> bool:
        """Decompress a single file.
        
        Args:
            file_info: FileInfo object with path and name attributes
            
        Returns:
            bool: True if decompression succeeded, False otherwise
        """
        try:
            if file_info.path.endswith('.csv.zst'):
                return self._decompress_zstd(file_info)
            elif file_info.path.endswith('.zip'):
                return self._decompress_zip(file_info)
            else:
                print(f"Unsupported compression format: {file_info.name}")
                return False
        except Exception as e:
            print(f"Error decompressing {file_info.name}: {str(e)}")
            return False

    def _decompress_zstd(self, file_info) -> bool:
        """Decompress a .csv.zst file using streaming zstandard decompression.
        
        Reads from source volume and writes to landing volume with streaming
        to handle large files efficiently without temp files or memory issues.
        """
        if zstd is None:
            raise ImportError("zstandard library is not installed. Please install it to decompress .zst files.")
        
        # Define chunk size for reading decompressed data (50MB)
        CHUNK_SIZE = 50 * 1024 * 1024
        
        # Convert dbfs:// paths to local file system paths for direct access
        input_path = file_info.path.replace('dbfs:', '')
        output_filename = file_info.name.replace('.csv.zst', '.csv')
        landing_path_local = self.landing_path.replace('dbfs:', '')
        output_path = f"{landing_path_local}/{output_filename}"
        
        # Create decompressor
        dctx = zstd.ZstdDecompressor()
        
        # Use native Python file operations for direct Unity Catalog volume access
        with open(input_path, 'rb') as input_file:
            with open(output_path, 'wb') as output_file:
                # Create a streaming reader that decompresses on-the-fly
                with dctx.stream_reader(input_file) as reader:
                    while True:
                        # Read decompressed data in chunks
                        chunk = reader.read(CHUNK_SIZE)
                        if not chunk:
                            break
                        # Write chunk to output
                        output_file.write(chunk)
        
        print(f"Decompressed: {file_info.name} -> {output_filename}")
        
        # Delete compressed file if requested
        if self.delete_compressed:
            self.dbutils.fs.rm(file_info.path)
            print(f"Deleted compressed file: {file_info.name}")
        
        return True

    def _decompress_zip(self, file_info) -> bool:
        """Decompress a .zip file using direct file system access."""
        import zipfile
        
        # Convert dbfs:// path to local file system path for direct access
        input_path = file_info.path.replace('dbfs:', '')
        landing_path_local = self.landing_path.replace('dbfs:', '')
        
        # Extract files from zip using direct file access
        with zipfile.ZipFile(input_path, 'r') as zf:
            for member in zf.namelist():
                if member.endswith('.csv'):
                    # Extract to landing volume
                    output_path = f"{landing_path_local}/{member}"
                    
                    # Extract directly to the output path
                    with zf.open(member) as source:
                        with open(output_path, 'wb') as target:
                            target.write(source.read())
                    
                    print(f"Extracted: {member} from {file_info.name}")
        
        # Delete compressed file if requested
        if self.delete_compressed:
            self.dbutils.fs.rm(file_info.path)
            print(f"Deleted compressed file: {file_info.name}")
        
        return True


class DecompressOrchestrator:
    """Orchestrates the file decompression process from history to landing volume."""

    def __init__(
        self,
        catalog: str,
        schema: str,
        history_volume: str,
        landing_volume: str,
        limit: int,
        delete_compressed: bool,
    ) -> None:
        self.spark = SparkSession.builder.getOrCreate()
        self.catalog = catalog
        self.schema = schema
        self.history_volume = history_volume
        self.landing_volume = landing_volume
        self.limit = limit
        self.delete_compressed = delete_compressed

        self.unity = UnityUtilities(self.spark, catalog, schema)
        self.source_path = f"/Volumes/{catalog}/{schema}/{history_volume}"
        self.landing_path = f"/Volumes/{catalog}/{schema}/{landing_volume}"
        self.file_manager = FileManager(self.spark, self.source_path, self.landing_path)
        self.file_decompressor = FileDecompressor(
            self.spark, self.landing_path, delete_compressed
        )

    def run(self) -> None:
        """Execute the file decompression workflow."""
        self._setup_infrastructure()
        
        candidates = self._get_candidate_files()
        
        if not candidates:
            print("No compressed files found that need decompression.")
            return
        
        success_count = self._process_files(candidates)
        self._print_summary(len(candidates), success_count)

    def _setup_infrastructure(self) -> None:
        """Ensure catalog, schema, and both volumes exist."""
        self.unity.ensure_schema_exists()
        self.unity.ensure_volume_exists(self.history_volume)
        self.unity.ensure_volume_exists(self.landing_volume)
        print(f"History volume: {self.catalog}.{self.schema}.{self.history_volume}")
        print(f"Landing volume: {self.catalog}.{self.schema}.{self.landing_volume}")

    def _get_candidate_files(self) -> List:
        """Get list of files to process."""
        return self.file_manager.get_compressed_files(self.limit)

    def _process_files(self, candidates: List) -> int:
        """Process each candidate file and return success count."""
        success_count = 0
        for file_info in candidates:
            if self.file_decompressor.decompress_file(file_info):
                success_count += 1
        return success_count

    def _print_summary(self, total: int, success: int) -> None:
        """Print processing summary."""
        print(f"\nDecompression complete:")
        print(f"  Total files processed: {total}")
        print(f"  Successfully decompressed: {success}")
        print(f"  Failed: {total - success}")


def main() -> None:
    """Main entry point for the decompress script."""
    parser = argparse.ArgumentParser(
        description="Decompress files from history volume to landing volume"
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
        "--history-volume",
        required=True,
        help="Source volume containing compressed files",
    )
    parser.add_argument(
        "--landing-volume",
        required=True,
        help="Destination volume for decompressed files",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Maximum number of files to decompress (0 = all files)",
    )
    parser.add_argument(
        "--delete-compressed",
        type=lambda x: x if isinstance(x, bool) else x.lower() == "true",
        default=False,
        help="Whether to delete compressed files after decompressing (true/false)",
    )

    args = parser.parse_args()

    schema = args.schema

    decompressor = DecompressOrchestrator(
        catalog=args.catalog,
        schema=schema,
        history_volume=args.history_volume,
        landing_volume=args.landing_volume,
        limit=args.limit,
        delete_compressed=args.delete_compressed,
    )
    decompressor.run()


if __name__ == "__main__":
    main()
