"""Quick analysis script to examine the EEZ boundaries dataset."""

import geopandas as gpd
import pandas as pd

def analyze_eez_data(shapefile_path: str) -> None:
    """Analyze the EEZ boundaries shapefile and print findings."""
    
    print("=" * 80)
    print("EEZ BOUNDARIES DATASET ANALYSIS")
    print("=" * 80)
    
    # Load the shapefile
    print(f"\nLoading shapefile: {shapefile_path}")
    gdf = gpd.read_file(shapefile_path)
    
    # Basic information
    print(f"\n{'='*80}")
    print("BASIC INFORMATION")
    print(f"{'='*80}")
    print(f"Number of features: {len(gdf)}")
    print(f"Coordinate Reference System: {gdf.crs}")
    print(f"Geometry type: {gdf.geometry.type.unique()}")
    
    # Column information
    print(f"\n{'='*80}")
    print("COLUMNS AND DATA TYPES")
    print(f"{'='*80}")
    print(gdf.dtypes)
    
    # Spatial extent
    print(f"\n{'='*80}")
    print("SPATIAL EXTENT (BOUNDING BOX)")
    print(f"{'='*80}")
    bounds = gdf.total_bounds
    print(f"Min X (West): {bounds[0]:.4f}")
    print(f"Min Y (South): {bounds[1]:.4f}")
    print(f"Max X (East): {bounds[2]:.4f}")
    print(f"Max Y (North): {bounds[3]:.4f}")
    
    # Sample records
    print(f"\n{'='*80}")
    print("SAMPLE RECORDS (first 5)")
    print(f"{'='*80}")
    # Display all columns except geometry for first 5 records
    sample_cols = [col for col in gdf.columns if col != 'geometry']
    print(gdf[sample_cols].head())
    
    # Key statistics
    print(f"\n{'='*80}")
    print("KEY STATISTICS")
    print(f"{'='*80}")
    
    # Check common EEZ field names
    for col in gdf.columns:
        if col.lower() in ['territory', 'sovereign', 'country', 'name', 'geoname', 'territory1']:
            unique_count = gdf[col].nunique()
            print(f"\nUnique values in '{col}': {unique_count}")
            print(f"Top 10 most common:")
            print(gdf[col].value_counts().head(10))
    
    # Area calculation (if not already present)
    if 'area_km2' in gdf.columns:
        print(f"\nTotal area covered: {gdf['area_km2'].sum():,.2f} km²")
        print(f"Mean area per feature: {gdf['area_km2'].mean():,.2f} km²")
    else:
        # Calculate area in km² (assuming WGS84 or similar)
        print("\nCalculating areas...")
        gdf_proj = gdf.to_crs("EPSG:6933")  # Equal area projection
        areas_km2 = gdf_proj.geometry.area / 1_000_000
        print(f"Total area covered: {areas_km2.sum():,.2f} km²")
        print(f"Mean area per feature: {areas_km2.mean():,.2f} km²")
        print(f"Largest feature: {areas_km2.max():,.2f} km²")
        print(f"Smallest feature: {areas_km2.min():,.2f} km²")
    
    # Full column list
    print(f"\n{'='*80}")
    print("ALL COLUMNS")
    print(f"{'='*80}")
    for i, col in enumerate(gdf.columns, 1):
        print(f"{i}. {col}")
    
    print(f"\n{'='*80}")
    print("ANALYSIS COMPLETE")
    print(f"{'='*80}\n")


if __name__ == "__main__":
    shapefile_path = "notebooks/data/eez/eez_boundaries_v12.shp"
    analyze_eez_data(shapefile_path)
