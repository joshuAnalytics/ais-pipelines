# ais-pipelines

## About AIS Data

This project works with Automatic Identification System (AIS) data from NOAA's Office for Coastal Management. AIS is a maritime vessel tracking system that broadcasts ship positions, speed, course, and other vessel information. NOAA's Office for Coastal Management serves to increase the resilience of the nation's coastal zone by helping communities and businesses take the actions needed to keep coastal residents safe, the economy sound, and natural resources functioning. The AIS data provided by NOAA supports critical coastal management decisions, marine transportation planning, environmental protection, and maritime safety analysis.

## Project Overview

This project implements a complete data pipeline for processing AIS data on Databricks:

1. **Download** - Retrieves compressed AIS files from NOAA's public archive
2. **Decompress** - Extracts .csv.zst and .zip files for processing
3. **Dripper** - Gradually releases files to a landing volume for controlled ingestion
4. **Auto Loader** - Streams data from landing volume to Delta tables using Databricks Auto Loader


## download ais

download_ais.py downloads source files from https://coast.noaa.gov/htdata/CMSP/AISDataHandler/{year}/index.html 
where {year} is the calendar year. Each page has a list of AIS data in .csv.zst format.

### Configuration

Settings are defined in the `variables` section of `databricks.yml`:

```yaml
variables:
  # Unity Catalog Configuration
  catalog:
    description: "Unity Catalog catalog name"
    default: ais
  
  schema:
    description: "Unity Catalog schema name"
    default: ais_assets
  
  # Download Configuration
  download_target_volume:
    description: "Volume where AIS files will be downloaded"
    default: full_history
  
  download_year:
    description: "Year to download from NOAA"
    default: 2024
  
  download_limit:
    description: "Max files to download (null for all files)"
    default: 1
```

For testing, set `download_limit: 1`. For production, set `download_limit: null` to download all available files.

### Parameters

The script accepts these parameters passed from the bundle variables:

- `--catalog`: catalog name in Unity Catalog (from `${var.catalog}`)
- `--schema`: schema name in Unity Catalog (from `${var.schema}`)
- `--volume`: target Unity Catalog volume for files (from `${var.download_target_volume}`)
- `--year`: calendar year to download (from `${var.download_year}`)
- `--limit`: max number of files to download (from `${var.download_limit}`)

### Pre-download checks

Before downloading, the script:

* Creates catalog and schema if they don't exist
* Creates volume if it doesn't exist
* Checks if files already exist on the volume - skips re-downloading duplicates

### Building the package

The project is packaged as a Python wheel using uv:

```bash
uv build
```

This creates a `.whl` file in the `dist/` directory that contains the package and its dependencies.

### Deployment

Deploy using Databricks Asset Bundles:

```
# Deploy to dev environment
databricks bundle deploy

# Download data
databricks bundle run download_ais_test

```

# Clear local terraform cache
if you are switching between different workspaces, you may need to clear the terraform cache in your local .databricks file. 

```bash
rm -rf .databricks
```

### Variable overrides

Override variables when deploying or running:

```bash
# Override variables for deployment
databricks bundle deploy --var="download_year=2023" --var="download_limit=5"

# Run with overridden variables
databricks bundle run download_ais_test --var="download_year=2023" --var="download_limit=5"
```

### Verification

Check downloaded files in your Unity Catalog volume:

```sql
LIST '/Volumes/main/streaming/full_history/'
```

Or via Python:

```python
files = dbutils.fs.ls("/Volumes/main/streaming/full_history/")
for f in files:
    print(f.path, f.size)
```

Expected filenames: `AIS_2024_01_01.csv.zst`, `AIS_2024_01_02.csv.zst`, etc.

## dripper

Gradually releases files from source volume to landing volume for streaming ingestion.

- Runs on schedule (configured in `databricks.yml`)
- Processes `n_per_run` files per execution
- Skips already-processed files
- Supports both copy and move operations

Configuration in `databricks.yml`:
- `source_volume`: source volume name (default: `full_history`)
- `landing_volume`: destination volume name (default: `landing`)
- `dripper_n_per_run`: files to process per run (default: `1`)
- `dripper_delete_source`: delete source after copy (default: `false`)

Deploy: `databricks bundle deploy`
