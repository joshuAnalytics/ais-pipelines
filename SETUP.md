# AIS Pipelines Setup Guide

This guide explains how to deploy and configure the dripper and Auto Loader components.

## Prerequisites

- Databricks workspace with Unity Catalog enabled
- Access to create Volumes, Jobs, and Tables
- Appropriate permissions on catalog and schema

## Configuration

Edit `config/config.yaml` to match your environment:

```yaml
catalog: main              # Your Unity Catalog name
schema: streaming          # Your schema name
source_volume: full_history    # Volume with full history files
landing_volume: landing        # Volume for incremental landing
```

## Deployment Steps

### 1. Create Unity Catalog Volumes

```sql
-- Create source volume for full history
CREATE VOLUME IF NOT EXISTS main.streaming.full_history;

-- Create landing volume
CREATE VOLUME IF NOT EXISTS main.streaming.landing;
```

### 2. Upload Source Files

Upload your CSV files to the source volume:
```
/Volumes/main/streaming/full_history/file1.csv
/Volumes/main/streaming/full_history/file2.csv
...
```

### 3. Deploy Dripper Script

Upload `src/dripper.py` to Databricks:
- Option A: Upload to DBFS at `/FileStore/scripts/dripper.py`
- Option B: Use Databricks Repos to sync from Git

### 4. Create Databricks Job

**Option A: Import Job JSON**
1. Go to Databricks Workflows
2. Click "Create Job" → "Import from JSON"
3. Upload `deployment/job_config.json`
4. Update the `python_file` path if needed
5. Save and run

**Option B: Create Job Manually**
1. Go to Databricks Workflows → Create Job
2. Add a Python Script task:
   - Task name: `dripper_task`
   - Type: Python script
   - Script path: Path to your uploaded `dripper.py`
   - Compute: Select Serverless
3. Add Job Parameters:
   - `CAT`: `main`
   - `SCH`: `streaming`
   - `SRC_VOL`: `full_history`
   - `DST_VOL`: `landing`
   - `N_PER_RUN`: `5`
4. Set Schedule: Every 1 minute (or as needed)
5. Save and run

### 5. Deploy Auto Loader Consumer

Create a Databricks notebook or script with the contents of `src/autoloader.py`:

1. Update configuration variables in the script if needed
2. Run the script to start the streaming query
3. The Auto Loader will continuously process files as they land

## Testing

1. **Verify dripper is running:**
   - Check Job run history in Databricks Workflows
   - Verify files are appearing in landing volume with time partitions:
     ```
     /Volumes/main/streaming/landing/dt=2025-01-17/hr=10/file1.csv
     ```

2. **Verify Auto Loader is processing:**
   - Check the Delta table: `SELECT * FROM main.events_raw LIMIT 10`
   - Monitor the streaming query in the Databricks notebook

## File Format

Expected CSV format for source files:
```csv
event_time,user_id,action,value
2025-01-17 10:30:00,user123,click,button_a
2025-01-17 10:31:00,user456,view,page_home
```

## Monitoring

- **Dripper Job:** Check Databricks Workflows for job run history and logs
- **Auto Loader:** Monitor streaming query metrics in the notebook
- **Landing Volume:** Inspect files in `/Volumes/main/streaming/landing/`
- **Delta Table:** Query `main.events_raw` for ingested data

## Troubleshooting

**No files being copied:**
- Verify source volume contains CSV files
- Check Job logs for errors
- Verify permissions on source and landing volumes

**Auto Loader not processing:**
- Verify landing volume path is correct
- Check checkpoint and schema locations
- Review streaming query logs for errors

**Duplicate files:**
- Ensure only one dripper job instance is running
- Check `max_concurrent_runs: 1` in job configuration

## Best Practices

1. **File Size:** Pre-split large files into 20-200 MB chunks for optimal performance
2. **Schedule:** Adjust dripper schedule based on throughput needs (every 1-5 minutes typical)
3. **Batch Size:** Tune `N_PER_RUN` to control ingestion rate
4. **Monitoring:** Set up alerts on Job failures and streaming query status
5. **Permissions:** Use service principals for production jobs
