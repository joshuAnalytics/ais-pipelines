# ais-pipelines

general style instructions: 

- Follow PEP 8, when sensible.
- follow SOLID principles, when sensible. 
- use type hints for every function. 
- use classes and methods, where sensible. 
- do not create try catch blocks which don't catch specific errors 
- do not write multi-line print code (maximum one line, keep consise)
- everything in this project will run in databricks, packaged as a .whl file, on serverless compute

## download ais

download_ais.py downloads source files from https://coast.noaa.gov/htdata/CMSP/AISDataHandler/{year}/index.html 
where {year} is the calendar year. Each page has a list of AIS data in .csv.zst format.

### Configuration

Settings are defined in `config/config.yaml`:

```yaml
catalog: main
schema: streaming

download:
  target_volume: full_history    # Volume where AIS files will be downloaded
  year: 2024                     # Year to download from NOAA
  limit: 1                       # Max files to download (null for all files)
```

For testing, set `limit: 1`. For production, set `limit: null` to download all available files.

### Parameters

The script accepts these parameters (via config or CLI):

- `target_catalog`: catalog name in Unity Catalog (default from config)
- `target_schema`: schema name in Unity Catalog (default from config)
- `target_volume`: target Unity Catalog volume for files (default from config)
- `year`: calendar year to download (default from config)
- `limit`: max number of files to download (default from config, null = all)

### Pre-download checks

Before downloading, the script:

* Creates catalog and schema if they don't exist
* Creates volume if it doesn't exist
* Checks if files already exist on the volume - skips re-downloading duplicates

### Building the package

The project is packaged as a Python wheel for deployment to Databricks:

```bash
# Build the wheel (requires setuptools and wheel)
python -m pip install build
python -m build
```

This creates a `.whl` file in the `dist/` directory that contains the package and its dependencies.

### Deployment

Deploy using Databricks Asset Bundles:

```bash
# Build the wheel first
python -m build

# Deploy to dev environment
databricks bundle deploy

# Run test (downloads 1 file based on config.yaml)
databricks bundle run download_ais_test

# Deploy to production
databricks bundle deploy --target prod
databricks bundle run download_ais_test --target prod
```

### CLI overrides

Override config values via command line:

```bash
# Override year and limit
databricks bundle run download_ais_test \
  --python-named-params year=2023 limit=5

# Override volume
databricks bundle run download_ais_test \
  --python-named-params target_volume=ais_archive
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

You can run a small Python “dripper” on Databricks serverless Jobs that moves/slices files from a Unity Catalog Volume with your full history into a separate landing Volume on a schedule. 

How it fits together

Source: /Volumes/<catalog>/<schema>/<full_history_volume>/...

Landing (“stream”): /Volumes/<catalog>/<schema>/<src_volume>/...

Job: a Python script task on Serverless compute (scheduled every N seconds/minutes) that copies a handful of files from Source → Landing (optionally adding date/hour folders). Databricks serverless Jobs support Python script tasks and cron-like schedules. 

## auto loader

Auto Loader: points at the Landing path and ingests incrementally. 

Storage/paths: Unity Catalog Volumes are the right place for file data; you access them via /Volumes/<cat>/<schema>/<vol>/…. 

Minimal “dripper” script (Python task on a Job)

This version “releases” up to N_PER_RUN files each run. It copies via a staging subfolder so the landing dir only ever sees complete files.

# dripper.py  (run as a Python script task)
# Params via job params or env vars
CAT = dbutils.widgets.get("CAT")
SCH = dbutils.widgets.get("SCH")
SRC_VOL = dbutils.widgets.get("SRC_VOL")        # e.g., full_history
DST_VOL = dbutils.widgets.get("DST_VOL")        # e.g., src_path
N_PER_RUN = int(dbutils.widgets.get("N_PER_RUN")) if "N_PER_RUN" in [w.name for w in dbutils.widgets.getArgumentInfo()] else 5

import time, uuid, datetime

src_root = f"/Volumes/{CAT}/{SCH}/{SRC_VOL}"
dst_root = f"/Volumes/{CAT}/{SCH}/{DST_VOL}"
staging = f"{dst_root}/_staging"

def now_parts():
    utc = datetime.datetime.utcnow()
    return f"dt={utc.date()}/hr={utc.hour}"

# ensure staging dir exists
dbutils.fs.mkdirs(staging)

# list a batch from a holding/backlog folder
candidates = [f for f in dbutils.fs.ls(src_root) if f.path.endswith(".csv")]
candidates = sorted(candidates, key=lambda x: x.name)[:N_PER_RUN]

for f in candidates:
    # copy to staging so readers never see partials; cp on cloud storage creates a full object
    tmp_name = f"{uuid.uuid4().hex}.csv"
    dbutils.fs.cp(f.path, f"{staging}/{tmp_name}")  # copy
    # move into a time-partitioned folder in landing
    rel = now_parts()
    dest_dir = f"{dst_root}/{rel}"
    dbutils.fs.mkdirs(dest_dir)
    dbutils.fs.mv(f"{staging}/{tmp_name}", f"{dest_dir}/{f.name}")  # move within landing
    # optionally delete source (or keep as archive)
    dbutils.fs.rm(f.path)

print(f"Released {len(candidates)} file(s) -> {dst_root}")


Schedule it: Create a Workflow (Job) → Python script task → select Serverless compute → add widgets/parameters (CAT, SCH, SRC_VOL, DST_VOL, N_PER_RUN) → set a schedule (e.g., every minute). 
Databricks Documentation
+1

Tip: Instead of slicing one huge CSV at read time, pre-split the full history into many medium-sized CSVs in your source Volume once, then let the job “release” N per run. It’s simpler and very reliable.

Auto Loader consumer (points at the landing Volume)
from pyspark.sql.types import *
from pyspark.sql.functions import to_timestamp

schema = StructType([
    StructField("event_time", StringType()),
    StructField("user_id", StringType()),
    StructField("action", StringType()),
    StructField("value", StringType()),
])

landing = "/Volumes/<catalog>/<schema>/<src_volume>"

df = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("cloudFiles.schemaLocation", "/Volumes/<catalog>/<schema>/<src_volume>/_schemas/events")
      .option("header", "true")
      .schema(schema)
      .load(landing))

df = df.withColumn("event_ts", to_timestamp("event_time"))

(df.writeStream
   .format("delta")
   .option("checkpointLocation", "/Volumes/<catalog>/<schema>/<src_volume>/_checkpoints/events")
   .toTable("main.events_raw"))

Notes & best practices

Permissions: Grant yourself/the job READ FILES on the source Volume and WRITE FILES on the landing Volume in Unity Catalog. 
Databricks Documentation

File size: Aim for ~20–200 MB per CSV for nice micro-batch cadence.

Isolation: Keep _staging inside the landing Volume; point Auto Loader at the landing root, not _staging, to avoid ever seeing temp files.

Notifications (optional): For very high throughput, enable Auto Loader’s file-notification mode on the landing location to speed up discovery. 
Databricks Documentation

Why serverless: Fully managed autoscaling and first-class support for scheduled Python script tasks. 
Databricks Documentation
+1

If you share your <catalog>.<schema> and volume names, I’ll tailor the exact paths and a ready-to-import Job JSON.
