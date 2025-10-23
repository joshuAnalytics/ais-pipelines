import os

from collections.abc import Iterator

import geopandas as gpd
import pandas as pd

from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import BinaryType, IntegerType, StringType, StructField, StructType


class ShapefilePartition(InputPartition):
  def __init__(self, file_path: str):
    self.file_path = file_path

class ShapefileDataSourceReader(DataSourceReader):
  def __init__(self, schema: StructType, options: dict[str, str]):
    self.schema = schema
    self.options = options
    self.input_path = self.options.get("path")
    if not (self.input_path):
      raise ValueError("The 'path' option is required.")
    self.layer_name = self.options.get("layer_name")
    if not (self.layer_name):
      raise ValueError("The 'layer_name' option is required.")

  def partitions(self) -> list[ShapefilePartition]:
    files = []
    for root, _, filenames in os.walk(self.input_path):
      for filename in filenames:
        if filename.endswith(".shp") or filename.endswith(".zip"):
          files.append(ShapefilePartition(os.path.join(root, filename)))
    return files

  def read(self, partition: ShapefilePartition) -> Iterator[tuple[bytes, int, str]]:
    import geopandas as gpd
    gdf = gpd.read_file(partition.file_path, layer=self.layer_name)
    property_cols = [c for c in gdf.columns if c != "geometry"]
    for r, g in zip(gdf[property_cols].iterrows(), gdf["geometry"].to_wkb()):
      yield (g, gdf.crs.to_epsg(), pd.Series(r[1]).to_json())
    
class ShapefileDataSource(DataSource):
  
  @classmethod
  def name(cls):
    return "shapefile"
    
  def schema(self) -> StructType:
    return StructType([
      StructField('geometry', BinaryType(), True),
      StructField('crs', IntegerType(), True),
      StructField('properties', StringType(), True)
      ])

  def reader(self, schema: StructType) -> DataSourceReader:
    return ShapefileDataSourceReader(schema, self.options)