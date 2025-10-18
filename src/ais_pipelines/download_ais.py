"""Download AIS data from NOAA to Unity Catalog volumes."""

import argparse
from typing import List, Set, Optional
import requests
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession


class UnityUtilities:
    """Handles Unity Catalog operations for catalog, schema, and volume management."""

    def __init__(self, spark: SparkSession, catalog: str, schema: str, volume: str) -> None:
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
        self.volume = volume
        self.volume_path = f"/Volumes/{catalog}/{schema}/{volume}"

    def ensure_catalog_exists(self) -> None:
        """Create catalog if it doesn't exist."""
        self.spark.sql(f"CREATE CATALOG IF NOT EXISTS {self.catalog}")
        print(f"Catalog '{self.catalog}' ready")

    def ensure_schema_exists(self) -> None:
        """Create schema if it doesn't exist."""
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.schema}")
        print(f"Schema '{self.catalog}.{self.schema}' ready")

    def ensure_volume_exists(self) -> None:
        """Create volume if it doesn't exist."""
        self.spark.sql(
            f"CREATE VOLUME IF NOT EXISTS {self.catalog}.{self.schema}.{self.volume}"
        )
        print(f"Volume '{self.catalog}.{self.schema}.{self.volume}' ready")

    def get_existing_files(self) -> Set[str]:
        """Return set of existing filenames in the volume."""
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(self.spark)
            files = dbutils.fs.ls(self.volume_path)
            return {f.name for f in files if f.name.endswith(".zip")}
        except Exception:
            return set()


class WebScraper:
    """Fetches and parses AIS file listings from NOAA website."""

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url

    def fetch_file_list(self, year: int) -> List[str]:
        """Fetch list of .csv.zst file URLs for the given year."""
        url = f"{self.base_url}/{year}/index.html"
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return self._parse_html_for_csv_links(response.text, year)

    def _parse_html_for_csv_links(self, html_content: str, year: int) -> List[str]:
        """Extract .zip file URLs from HTML content."""
        soup = BeautifulSoup(html_content, "html.parser")
        links = []
        for link in soup.find_all("a"):
            href = link.get("href")
            if href and href.endswith(".zip"):
                full_url = f"{self.base_url}/{year}/{href}"
                links.append(full_url)
        return links


class FileDownloader:
    """Handles file downloads to Unity Catalog volumes."""

    def __init__(self, spark: SparkSession, volume_path: str) -> None:
        self.spark = spark
        self.volume_path = volume_path

    def download_file(self, url: str, filename: str) -> None:
        """Download a file from URL directly to the volume with progress bar."""
        from tqdm import tqdm
        
        dest_path = f"{self.volume_path}/{filename}"
        
        with requests.get(url, stream=True, timeout=300) as r:
            r.raise_for_status()
            
            # Get total file size from headers
            total_size = int(r.headers.get("content-length", 0))
            
            with open(dest_path, "wb") as f:
                with tqdm(
                    total=total_size, unit="B", unit_scale=True, desc=filename
                ) as pbar:
                    for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                        if chunk:
                            f.write(chunk)
                            pbar.update(len(chunk))

    def filter_existing_files(
        self, all_urls: List[str], existing_files: Set[str]
    ) -> List[str]:
        """Filter out URLs for files that already exist."""
        filtered = []
        for url in all_urls:
            filename = url.split("/")[-1]
            if filename not in existing_files:
                filtered.append(url)
        return filtered


class AISDownloader:
    """Orchestrates the AIS data download process."""

    def __init__(
        self, catalog: str, schema: str, volume: str, year: int, limit: int = None
    ) -> None:
        self.spark = SparkSession.builder.getOrCreate()
        self.unity = UnityUtilities(self.spark, catalog, schema, volume)
        self.scraper = WebScraper("https://coast.noaa.gov/htdata/CMSP/AISDataHandler")
        self.downloader = FileDownloader(self.spark, self.unity.volume_path)
        self.year = year
        self.limit = limit

    def run(self) -> None:
        """Execute the full download workflow."""
        self._setup_infrastructure()
        files_to_download = self._get_files_to_download()
        self._download_files(files_to_download)

    def _setup_infrastructure(self) -> None:
        """Ensure catalog, schema, and volume exist."""
        self.unity.ensure_catalog_exists()
        self.unity.ensure_schema_exists()
        self.unity.ensure_volume_exists()

    def _get_files_to_download(self) -> List[str]:
        """Fetch file list and filter out existing files."""
        print(f"Fetching file list for year {self.year}")
        all_files = self.scraper.fetch_file_list(self.year)
        print(f"Found {len(all_files)} files on remote")
        
        existing_files = self.unity.get_existing_files()
        print(f"Found {len(existing_files)} files already in volume")
        
        files_to_download = self.downloader.filter_existing_files(
            all_files, existing_files
        )
        print(f"Need to download {len(files_to_download)} files")
        
        return files_to_download

    def _download_files(self, urls: List[str]) -> None:
        """Download all files in the list."""
        urls_to_process = urls[:self.limit] if self.limit and self.limit > 0 else urls
        total_files = len(urls_to_process)
        
        for i, url in enumerate(urls_to_process, 1):
            filename = url.split("/")[-1]
            print(f"Downloading {i}/{total_files}: {filename}")
            self.downloader.download_file(url, filename)
        print(f"Download complete: {total_files} files")


def main() -> None:
    """Main entry point for the download script."""
    parser = argparse.ArgumentParser(
        description="Download AIS data from NOAA to Unity Catalog volumes"
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
        "--volume",
        required=True,
        help="Unity Catalog volume name",
    )
    parser.add_argument(
        "--year",
        type=int,
        required=True,
        help="Calendar year to download",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of files to download (default: all files)",
    )
    
    args = parser.parse_args()
    
    # Use all parameters from command-line args
    catalog = args.catalog
    schema = args.schema
    volume = args.volume
    year = args.year
    limit = args.limit
    
    downloader = AISDownloader(
        catalog=catalog,
        schema=schema,
        volume=volume,
        year=year,
        limit=limit,
    )
    downloader.run()


if __name__ == "__main__":
    main()
