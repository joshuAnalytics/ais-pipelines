# ⛵ ais-pipelines

## About the dataset

This project works with Automatic Identification System (AIS) data from NOAA's Office for Coastal Management. 

* AIS is a maritime vessel tracking system that broadcasts ship positions, speed, course, and other vessel information. 
* NOAA's Office for Coastal Management serves to increase the resilience of the nation's coastal zone by helping communities and businesses take the actions needed to keep coastal residents safe, the economy sound, and natural resources functioning. 
* The dataset offers a deep time series lookback over multiple years of vessel movement
* The dataset is limited spatially to waters close to North America and other US jurisdictions 

## Data Pipeline 

This project implements a data pipeline for processing AIS data on Databricks:

1. **Download** - Retrieves compressed AIS files from NOAA's public archive
2. **Decompress** - Extracts .csv.zst and .zip files for processing

The pipeline is deployed as a single Databricks job with tasks that run sequentially: `download_ais` → `decompress_files`.

## Geospatial Analytics

The `notebooks/` directory contains:

- **data_quality_tutorial.py** - Demonstrates loading AIS CSV data into Delta tables, performing data quality checks, creating spatial columns with H3 indices at multiple resolutions (6-9), and generating pre-aggregated tables for visualization.

- **salish_sea_deep_dive.ipynb** - Analyzes vessel movements in the Salish Sea region using spatial data analysis. Loads port reference data, filters AIS events to the region, identifies vessels in port using spatial intersections, sessionizes vessel journeys, and computes origin-destination (O/D) journey counts between ports.

- **viz_h3_agg.py** - Creates interactive pydeck visualizations of vessel activity using H3 hexagonal aggregations. Shows daily vessel activity patterns with a fire colormap (yellow to red) across different H3 resolutions, with interactive tooltips and zoom controls.

## Configuration

Key settings in `databricks.yml`:

```yaml
variables:
  # Unity Catalog
  catalog: ais
  schema: ais_assets
  
  # Volumes
  source_volume: full_history        # Downloaded/compressed files
  landing_volume: landing            # Decompressed files
  download_target_volume: full_history
  
  # Download settings
  download_year: 2025
  download_limit: 1                  # Set to 0 for all files
  
  # Decompressor settings
  decompressor_limit: 0              # 0 = all files
  decompressor_delete_compressed: false
```

## Building and Deployment

### Build the package

The project is packaged as a Python wheel using uv:

```bash
uv build
```

This creates a `.whl` file in the `dist/` directory that contains the package and its dependencies.

### Deploy and run

Deploy using Databricks Asset Bundles:

```bash
# Deploy to dev environment (default)
databricks bundle deploy

# Run the pipeline job
databricks bundle run ais_pipeline

# Override variables for specific runs
databricks bundle run ais_pipeline --var="download_year=2023" --var="download_limit=5"
```

### Clear local cache

If switching between workspaces, clear the terraform cache:

```bash
rm -rf .databricks
```