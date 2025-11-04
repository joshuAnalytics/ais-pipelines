# Databricks notebook source
# MAGIC %md
# MAGIC # Deciphering vessel movements in the Salish Sea
# MAGIC
# MAGIC ![](assets/tally-ho.png)

# COMMAND ----------

# MAGIC %md
# MAGIC __Join me on a voyage of discovery as we sail through some basic spatial data analysis together.__
# MAGIC
# MAGIC We'll take a look at vessel movements around the Salish Sea in the Pacific North West of the USA and Canada. This is an expanse of water separated from the Pacific Ocean by Vancouver Island and the Olympic Peninsula.
# MAGIC
# MAGIC It's home to some of the largest ports on the west coast of North America, including the Port of Vancouver, the Port of Seattle, and the Port of Tacoma (which together form the Northwest Seaport Alliance). It sees immense container ship traffic, bulk carriers, cruise ships, and one of the largest ferry networks in the world (BC Ferries and Washington State Ferries).
# MAGIC
# MAGIC It is also a world-renowned destination for recreational boating. The protected waters, stunning scenery, and the thousands of islands, inlets, and anchorages (like the San Juan Islands and the Gulf Islands) make it a paradise for sailors, yachters, and kayakers.

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

# MAGIC %pip install -e ../ --quiet

# COMMAND ----------

import os
import shutil

import geopandas as gpd

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import ResourceAlreadyExists
from databricks.sdk.service.catalog import VolumeType

import pyspark.databricks.sql.functions as DBF
import pyspark.sql.functions as F
from pyspark.sql.window import Window

from utils.reader import ShapefileDataSource

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load reference dataset (ports in the area)

# COMMAND ----------

w = WorkspaceClient()

user_name = w.current_user.me().user_name

CATALOG = "ais"
SCHEMA = "ais_assets"
VOLUME = "reference"

INPUT_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
LAYER_NAME = "salish-ports"

# COMMAND ----------

try:
    w.volumes.create(
        name=VOLUME,
        catalog_name=CATALOG,
        schema_name=SCHEMA,
        volume_type=VolumeType.MANAGED,
    )
except ResourceAlreadyExists:
    print("Skipping volume creation. Already exists")

# COMMAND ----------

# Get the directory containing this notebook
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
notebook_dir = os.path.dirname(notebook_path)
data_path = f"/Workspace{notebook_dir}/data/salish-ports.shp"
shutil.copy(data_path, INPUT_PATH)

# COMMAND ----------

ports_raw = (
  spark.read.format("shapefile")
  .option("layer_name", LAYER_NAME)
  .load(INPUT_PATH)
  )
  
display(ports_raw)

# COMMAND ----------

# MAGIC %md
# MAGIC Expand out feature properties using `from_json` and `schema_of_json` expressions from `pyspark.sql.functions`.

# COMMAND ----------

props_schema = F.schema_of_json(
  """{"OID":"1726.0","WPIN":"18460.0","REGION":"Canada West Coast -- 18080","PORT":"Comox Harbor","UN_LOCODE":"CA COX","COUNTRY":"Canada","WORLD_WATE":"Alaska-Canada coastal waters; North Pacific Ocean","SIZE":"Small","TYPE":"Coastal (Natural)","LAT":49.666667,"LON":-124.916667}"""
  )

# COMMAND ----------

ports = (
  ports_raw
  .withColumn("properties", F.from_json(F.col("properties"), schema=props_schema))
  .select("geometry", "crs", "properties.*")
)

display(ports)

# COMMAND ----------

# MAGIC %md
# MAGIC Quickly visualise these using GeoPandas `explore()` method.

# COMMAND ----------

gpd.GeoSeries.from_wkb(ports_raw.toPandas()["geometry"].values, crs=26910).explore()

# COMMAND ----------

# MAGIC %md
# MAGIC Construct a Databricks GEOMETRY typed column from well-known-binary geometries and transform coordinates from the projected CRS (UTM Zone 10 N) to WGS84 geographic CRS (EPSG:26910 to EPSG:4326)

# COMMAND ----------

port_id = "OID"
port_name = "PORT"

property_cols = [
  "OID",
  "WPIN",
  "PORT",
  "UN_LOCODE",
  "COUNTRY",
  "REGION",
  "WORLD_WATE",
  "SIZE",
  "TYPE",
  "LAT",
  "LON",
]

UTM10N = 26910
WGS84 = 4326

ports_reprojected = (
  ports
  .withColumn("geometry_UTM10N", DBF.st_geomfromwkb("geometry", UTM10N))
  .withColumn("geometry_wgs84", DBF.st_transform("geometry_UTM10N", WGS84))
  .where(DBF.st_isvalid("geometry_wgs84"))
  .select(*property_cols, "geometry_UTM10N", "geometry_wgs84")
)

display(ports_reprojected)

# COMMAND ----------

# MAGIC %md
# MAGIC Filter these to remove the 'very small' ports. We'll ignore these for the purposes of this analysis.

# COMMAND ----------

ports_filtered = ports_reprojected.filter(~(F.col("SIZE")=="Very Small"))
ports_filtered.count()

# COMMAND ----------

ports_tref = f"{CATALOG}.{SCHEMA}.ports"
spark.sql(f"DROP TABLE IF EXISTS {ports_tref}")

ports_filtered.write.saveAsTable(ports_tref, mode="overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Connecting the dots
# MAGIC
# MAGIC Great, let's bring this together with our AIS data

# COMMAND ----------

ais_events_tref = f"{CATALOG}.{SCHEMA}.ais_data_sample"

spark.table(ais_events_tref).display()

# COMMAND ----------

record_count = spark.table(ais_events_tref).count()
print(f"{record_count:,} records in {ais_events_tref}")

# COMMAND ----------

# MAGIC %md
# MAGIC There's a lot of data here, let's subset it to just our area of interest.

# COMMAND ----------

# MAGIC %md
# MAGIC We're going to create a buffer of 1 NM (Nautical Mile, not nanometre!) around each of our ports, then compute an envelope around this region to perform a very rough filtering of the AIS events.

# COMMAND ----------

buffered_ports = (
    spark.table(ports_tref)
    # 1. Buffer in the projected CRS for accurate distance
    .withColumn(
        "buffered_geom_projected", DBF.st_buffer("geometry_UTM10N", F.lit(1 * 1852))
    )
    # 2. Reproject the accurate buffer to CRS84 to match AIS data
    .withColumn(
        "buffered_geom_wgs84",
        DBF.st_transform("buffered_geom_projected", WGS84),
    )
)

buffered_ports.display()

# COMMAND ----------

salish_sea_envelope = (
    buffered_ports
    .groupBy()
    .agg(
        DBF.st_envelope_agg("buffered_geom_wgs84").alias("filter_envelope")
        )
    .withColumns({
            "xmin": DBF.st_xmin("filter_envelope"),
            "xmax": DBF.st_xmax("filter_envelope"),
            "ymin": DBF.st_ymin("filter_envelope"),
            "ymax": DBF.st_ymax("filter_envelope"),
        })
    .drop("filter_envelope")
    ).collect()[0]
salish_sea_envelope

# COMMAND ----------

# MAGIC %md
# MAGIC (OPTIONAL!)
# MAGIC
# MAGIC A simple optimisation we can apply here is to order the AIS events using longitude and latitude. When we then filter the rows, this should push down a filter into the table scan operation and obviate the need to read every file.
# MAGIC
# MAGIC We could also ensure this is done on a continuous basis using liquid clustering.

# COMMAND ----------

# OPTIONAL - requires several minutes to run
# spark.sql(f"OPTIMIZE {ais_events_tref} ZORDER BY (longitude, latitude)")

# COMMAND ----------

filtered_ais_events = (
  spark.table(ais_events_tref)
  .where(
    (F.col("longitude")>=salish_sea_envelope.xmin) &
    (F.col("longitude")<=salish_sea_envelope.xmax) &
    (F.col("latitude")>=salish_sea_envelope.ymin) &
    (F.col("latitude")<=salish_sea_envelope.ymax)
    )
  )
filtered_ais_events.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Identify vessels in port

# COMMAND ----------

# MAGIC %md
# MAGIC Now we can go ahead and find all of the AIS events corresponding to vessels in the buffer zone around a port.

# COMMAND ----------

ais_in_ports = (
    filtered_ais_events.alias("e")
    .join(
        buffered_ports.hint("broadcast").alias("p"), 
        on=DBF.st_intersects(F.col("e.point_geom"), F.col("p.buffered_geom_wgs84"))
    )
    .select(
        "mmsi", 
        "vessel_name",
        "timestamp",
        port_id,
        port_name
    )
)

ais_in_ports.display()

# COMMAND ----------

# MAGIC %md
# MAGIC By 'sessionizing' this trail of events, we can trace out the journeys between ports.

# COMMAND ----------

vessel_window = Window.partitionBy("mmsi").orderBy("timestamp")

# COMMAND ----------

ais_with_gaps = (
    ais_in_ports
    .withColumn("prev_port", F.lag(port_id).over(vessel_window))
    .withColumn("prev_time", F.lag("timestamp").over(vessel_window))
    .withColumn(
        "is_new_session",
        (
            (F.col("prev_port") != F.col(port_id)) | # Port changed
            (F.col("prev_port").isNull()) # First-ever ping
        ).cast("integer")
    )
)

# COMMAND ----------

session_window = Window.partitionBy("mmsi").orderBy("timestamp")

ais_with_sessions = (
    ais_with_gaps
    .withColumn("session_id", F.sum("is_new_session").over(session_window))
)

# COMMAND ----------

# MAGIC %md
# MAGIC We'll apply a criteria that port stays require loitering in the buffer for at least 15 minutes

# COMMAND ----------

port_stays = (
    ais_with_sessions
    .groupBy("mmsi", "vessel_name", port_id, port_name, "session_id")
    .agg(
        F.min("timestamp").alias("arrival_time"),
        F.max("timestamp").alias("departure_time")
    )
    .withColumn("duration_seconds", 
                F.col("departure_time").cast("long") - F.col("arrival_time").cast("long"))
    .filter(F.col("duration_seconds") > (15 * 60)) 
)

port_stays.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compute O/D journey counts

# COMMAND ----------

journey_window = Window.partitionBy("mmsi").orderBy("arrival_time")

# 2. Get the 'origin' (current port) and 'destination' (next port)
journeys = (
    port_stays
    .withColumn("destination_port", F.lead(port_name).over(journey_window))
    .withColumn("origin_port", F.col(port_name))
    .filter(F.col("destination_port").isNotNull()) # Remove last-known journeys
    .filter(F.col("origin_port") != F.col("destination_port")) 
)

# 3. Compute the O/D matrix!
od_matrix = (
    journeys
    .groupBy("origin_port", "destination_port")
    .count()
    .orderBy(F.col("count").desc())
)

od_matrix.display()
