-- Databricks notebook source
select min(timestamp),max(timestamp) from ais_data_sample

-- COMMAND ----------

use catalog ais;
use schema ais_assets;

CREATE OR REPLACE TABLE vessel_behavioral_features AS
WITH vessel_trajectory AS (
  SELECT 
    mmsi,
    vessel_name,
    vessel_type,
    timestamp,
    latitude,
    longitude,
    sog,
    cog,
    h3_res8,
    h3_res7,
    h3_res6,
    -- Window functions for trajectory analysis
    LAG(timestamp, 1) OVER w as prev_timestamp,
    LAG(latitude, 1) OVER w as prev_lat,
    LAG(longitude, 1) OVER w as prev_lon,
    LAG(sog, 1) OVER w as prev_sog,
    LAG(cog, 1) OVER w as prev_cog,
    LAG(h3_res8, 1) OVER w as prev_h3_res8,
    LAG(h3_res7, 1) OVER w as prev_h3_res7,
    -- Look ahead for gap detection
    LEAD(timestamp, 1) OVER w as next_timestamp,
    -- Historical patterns
    COUNT(*) OVER w as position_count,
    ROW_NUMBER() OVER w as position_sequence
  FROM ais_data_sample
  WHERE timestamp >= '2025-01-01'
  WINDOW w AS (PARTITION BY mmsi ORDER BY timestamp)
),
computed_features AS (
  SELECT 
    *,
    -- Time gap detection (going dark)
    (unix_timestamp(next_timestamp) - unix_timestamp(timestamp)) / 3600.0 as hours_to_next_signal,
    (unix_timestamp(timestamp) - unix_timestamp(prev_timestamp)) / 3600.0 as hours_since_last_signal,
    
    -- Movement features
    -- NOTE: ST_Distance behavior depends on Databricks configuration
    -- This assumes ST_Distance returns degrees and converts to km using 111.32 km/degree
    -- Verify ST_Distance output units in your environment before production use
    CASE 
      WHEN prev_lat IS NOT NULL THEN
        ST_Distance(
          ST_Point(longitude, latitude),
          ST_Point(prev_lon, prev_lat)
        ) * 111.32  -- Convert degrees to km (approximate)
      ELSE NULL
    END as distance_moved_km,
    
    -- Speed consistency
    ABS(sog - prev_sog) as speed_change,
    
    -- Course consistency  
    CASE 
      WHEN ABS(cog - prev_cog) > 180 THEN 360 - ABS(cog - prev_cog)
      ELSE ABS(cog - prev_cog)
    END as course_change,
    
    -- H3 cell changes (erratic movement indicator)
    CASE WHEN h3_res8 != prev_h3_res8 THEN 1 ELSE 0 END as changed_h3_cell,
    CASE WHEN h3_res7 != prev_h3_res7 THEN 1 ELSE 0 END as changed_h3_parent
    
  FROM vessel_trajectory
  WHERE prev_timestamp IS NOT NULL
)
SELECT 
  mmsi,
  vessel_name,
  vessel_type,
  timestamp,
  latitude,
  longitude,
  sog,
  cog,
  h3_res8,
  h3_res7,
  h3_res6,
  hours_to_next_signal,
  hours_since_last_signal,
  distance_moved_km,
  speed_change,
  course_change,
  changed_h3_cell,
  changed_h3_parent,
  
  -- Derived anomaly indicators
  CASE WHEN hours_to_next_signal > 6 THEN 1 ELSE 0 END as potential_dark_period,
  CASE WHEN sog < 0.5 AND prev_sog > 5 THEN 1 ELSE 0 END as sudden_stop,
  CASE WHEN course_change > 90 AND sog > 5 THEN 1 ELSE 0 END as sharp_turn,
  
  -- Calculate implied speed from distance/time
  CASE 
    WHEN hours_since_last_signal > 0.1 AND hours_since_last_signal < 2 THEN
      distance_moved_km / hours_since_last_signal
    ELSE NULL
  END as implied_speed_kmh,
  
  -- Speed discrepancy (reported vs calculated)
  -- Note: Compares average speed over time period with instantaneous SOG
  -- Higher threshold accounts for acceleration/deceleration between measurements
  CASE 
    WHEN hours_since_last_signal > 0.1 AND hours_since_last_signal < 2 THEN
      ABS((distance_moved_km / hours_since_last_signal) - (sog * 1.852))
    ELSE NULL
  END as speed_discrepancy_kmh,
  
  -- Average of current and previous SOG for better comparison
  (sog + prev_sog) / 2 * 1.852 as avg_sog_kmh
FROM computed_features;



-- COMMAND ----------

select * from vessel_behavioral_features limit 5;

-- COMMAND ----------

CREATE OR REPLACE TABLE vessel_rolling_patterns AS
SELECT 
  mmsi,
  vessel_name,
  vessel_type,
  timestamp,
  h3_res8,
  sog,
  
  -- Rolling statistics (6 hour window)
  AVG(sog) OVER w6h as avg_speed_6h,
  STDDEV(sog) OVER w6h as stddev_speed_6h,
  MIN(sog) OVER w6h as min_speed_6h,
  MAX(sog) OVER w6h as max_speed_6h,
  
  AVG(course_change) OVER w6h as avg_course_change_6h,
  MAX(course_change) OVER w6h as max_course_change_6h,
  
  SUM(changed_h3_cell) OVER w6h as h3_changes_6h,
  COUNT(*) OVER w6h as observation_count_6h,
  
  -- Rolling statistics (24 hour window)
  AVG(sog) OVER w24h as avg_speed_24h,
  STDDEV(sog) OVER w24h as stddev_speed_24h,
  
  -- Alternative to COUNT(DISTINCT) - approximate unique count
  APPROX_COUNT_DISTINCT(h3_res7) OVER w24h as unique_h3_cells_24h,
  
  -- Loitering detection (low speed, same area)
  AVG(CASE WHEN sog < 2 THEN 1 ELSE 0 END) OVER w6h as pct_low_speed_6h,
  
  -- Erratic behavior score
  (AVG(course_change) OVER w6h + STDDEV(sog) OVER w6h) as erratic_score_6h
FROM vessel_behavioral_features
WINDOW 
  w6h AS (PARTITION BY mmsi ORDER BY unix_timestamp(timestamp) RANGE BETWEEN 21600 PRECEDING AND CURRENT ROW),
  w24h AS (PARTITION BY mmsi ORDER BY unix_timestamp(timestamp) RANGE BETWEEN 86400 PRECEDING AND CURRENT ROW);

-- COMMAND ----------

select * from vessel_rolling_patterns limit 5;

-- COMMAND ----------

CREATE OR REPLACE TABLE h3_normal_patterns AS
SELECT 
  h3_res7,
  vessel_type,
  hour(timestamp) as hour_of_day,
  
  -- Speed patterns
  PERCENTILE_APPROX(sog, 0.5) as median_speed,
  PERCENTILE_APPROX(sog, 0.25) as q25_speed,
  PERCENTILE_APPROX(sog, 0.75) as q75_speed,
  PERCENTILE_APPROX(sog, 0.95) as q95_speed,
  PERCENTILE_APPROX(sog, 0.05) as q05_speed,
  
  -- Course variation patterns
  AVG(course_change) as avg_course_change,
  STDDEV(course_change) as stddev_course_change,
  
  -- Density patterns
  COUNT(*) as total_observations,
  COUNT(DISTINCT mmsi) as unique_vessels,
  COUNT(*) / COUNT(DISTINCT mmsi) as avg_obs_per_vessel,
  
  -- Dark period patterns
  AVG(hours_to_next_signal) as avg_signal_gap,
  PERCENTILE_APPROX(hours_to_next_signal, 0.95) as p95_signal_gap

FROM vessel_behavioral_features
WHERE timestamp BETWEEN '2025-01-01' AND '2025-01-31'  -- Historical baseline: 30 days for robust pattern detection
GROUP BY h3_res7, vessel_type, hour(timestamp);

-- COMMAND ----------

-- COMMAND ----------

-- Create H3 cell statistics for spatial context
-- Pre-aggregate historical patterns for each H3 cell
CREATE OR REPLACE TABLE h3_cell_statistics AS
SELECT 
  h3_res7,
  h3_res8,
  hour(timestamp) as hour_of_day,
  
  -- Vessel density patterns
  COUNT(DISTINCT mmsi) as avg_vessel_count,
  PERCENTILE_APPROX(COUNT(DISTINCT mmsi), 0.95) as p95_vessel_count,
  PERCENTILE_APPROX(COUNT(DISTINCT mmsi), 0.05) as p05_vessel_count,
  
  -- Vessel type distribution
  MODE(vessel_type) as dominant_vessel_type,
  COUNT(DISTINCT vessel_type) as vessel_type_diversity,
  
  -- Activity characterization
  AVG(sog) as cell_avg_speed,
  STDDEV(sog) as cell_stddev_speed,
  PERCENTILE_APPROX(sog, 0.5) as cell_median_speed,
  
  -- Classification flags
  CASE 
    WHEN AVG(sog) > 8 THEN 1 
    ELSE 0 
  END as is_transit_corridor,
  
  CASE 
    WHEN AVG(sog) < 2 AND COUNT(DISTINCT mmsi) > 3 THEN 1 
    ELSE 0 
  END as is_stationary_area,
  
  COUNT(*) as total_observations

FROM vessel_behavioral_features
WHERE timestamp BETWEEN '2025-01-01' AND '2025-01-31'
GROUP BY h3_res7, h3_res8, hour(timestamp);

-- COMMAND ----------

select * from h3_cell_statistics limit 10;

-- COMMAND ----------

-- Create vessel spatial context features
-- Count nearby vessels using H3 k-ring for spatial proximity
CREATE OR REPLACE TABLE vessel_spatial_context AS
WITH vessel_positions AS (
  SELECT 
    mmsi,
    vessel_name,
    vessel_type,
    timestamp,
    h3_res7,
    h3_res8,
    sog,
    -- Create time bucket (1 hour windows)
    date_trunc('hour', timestamp) as time_bucket
  FROM vessel_behavioral_features
  WHERE timestamp >= current_timestamp() - INTERVAL 7 DAYS
),
-- Expand each position to include k-ring neighbors (k=1)
vessel_with_neighbors AS (
  SELECT 
    mmsi,
    vessel_name,
    vessel_type,
    timestamp,
    h3_res7,
    h3_res8,
    time_bucket,
    sog,
    explode(h3_kring(h3_res8, 1)) as neighbor_cell
  FROM vessel_positions
),
-- Count vessels in neighborhood
neighborhood_counts AS (
  SELECT 
    v1.mmsi,
    v1.timestamp,
    v1.h3_res8,
    v1.time_bucket,
    
    -- Count distinct vessels in same cell (exact location)
    COUNT(DISTINCT CASE 
      WHEN v2.h3_res8 = v1.h3_res8 
        AND v2.mmsi != v1.mmsi 
        AND v2.time_bucket = v1.time_bucket
      THEN v2.mmsi 
    END) as vessels_in_same_cell,
    
    -- Count distinct vessels in k=1 neighborhood
    COUNT(DISTINCT CASE 
      WHEN v2.neighbor_cell = v1.h3_res8
        AND v2.mmsi != v1.mmsi
        AND v2.time_bucket = v1.time_bucket
      THEN v2.mmsi 
    END) as vessels_in_kring1,
    
    -- Count distinct vessel types nearby
    COUNT(DISTINCT CASE 
      WHEN v2.neighbor_cell = v1.h3_res8
        AND v2.mmsi != v1.mmsi
        AND v2.time_bucket = v1.time_bucket
      THEN v2.vessel_type 
    END) as vessel_types_nearby
    
  FROM vessel_with_neighbors v1
  LEFT JOIN vessel_with_neighbors v2
    ON v1.time_bucket = v2.time_bucket
  GROUP BY v1.mmsi, v1.timestamp, v1.h3_res8, v1.time_bucket
)
SELECT 
  vp.mmsi,
  vp.vessel_name,
  vp.timestamp,
  vp.h3_res8,
  
  -- Density features
  COALESCE(nc.vessels_in_same_cell, 0) as vessels_in_same_cell,
  COALESCE(nc.vessels_in_kring1, 0) as vessels_in_kring1,
  COALESCE(nc.vessel_types_nearby, 0) as vessel_types_nearby,
  
  -- Isolation indicators
  CASE WHEN COALESCE(nc.vessels_in_same_cell, 0) = 0 THEN 1 ELSE 0 END as is_isolated,
  CASE WHEN COALESCE(nc.vessels_in_kring1, 0) = 0 THEN 1 ELSE 0 END as is_neighborhood_isolated,
  
  -- Relative density (vs k-ring)
  CASE 
    WHEN COALESCE(nc.vessels_in_kring1, 0) > 0 
    THEN CAST(COALESCE(nc.vessels_in_same_cell, 0) AS DOUBLE) / COALESCE(nc.vessels_in_kring1, 1)
    ELSE 0 
  END as local_density_ratio

FROM vessel_positions vp
LEFT JOIN neighborhood_counts nc
  ON vp.mmsi = nc.mmsi 
  AND vp.timestamp = nc.timestamp
  AND vp.h3_res8 = nc.h3_res8;

-- COMMAND ----------

select * from vessel_spatial_context limit 10;

-- COMMAND ----------

-- Combine all features for ML-based anomaly detection
-- This table will be used by the Autoencoder model
CREATE OR REPLACE TABLE vessel_ml_features AS
SELECT 
  v.mmsi,
  v.vessel_name,
  v.vessel_type,
  v.timestamp,
  v.latitude,
  v.longitude,
  v.h3_res8,
  v.h3_res7,
  v.h3_res6,
  v.sog,
  v.cog,
  v.hours_to_next_signal,
  v.hours_since_last_signal,
  v.distance_moved_km,
  v.speed_change,
  v.course_change,
  v.changed_h3_cell,
  v.changed_h3_parent,
  v.potential_dark_period,
  v.sudden_stop,
  v.sharp_turn,
  v.implied_speed_kmh,
  v.speed_discrepancy_kmh,
  v.avg_sog_kmh,
  
  -- Rolling pattern features
  r.avg_speed_6h,
  r.stddev_speed_6h,
  r.min_speed_6h,
  r.max_speed_6h,
  r.avg_course_change_6h,
  r.max_course_change_6h,
  r.h3_changes_6h,
  r.observation_count_6h,
  r.avg_speed_24h,
  r.stddev_speed_24h,
  r.unique_h3_cells_24h,
  r.pct_low_speed_6h,
  r.erratic_score_6h,
  
  -- Historical pattern features
  n.median_speed as historical_median_speed,
  n.q75_speed as historical_q75_speed,
  n.q95_speed as historical_q95_speed,
  n.q05_speed as historical_q05_speed,
  n.avg_course_change as historical_avg_course_change,
  n.stddev_course_change as historical_stddev_course_change,
  n.avg_signal_gap as historical_avg_signal_gap,
  n.p95_signal_gap as historical_p95_signal_gap,
  
  -- Spatial context features (nearby vessels)
  COALESCE(sc.vessels_in_same_cell, 0) as vessels_in_same_cell,
  COALESCE(sc.vessels_in_kring1, 0) as vessels_in_kring1,
  COALESCE(sc.vessel_types_nearby, 0) as vessel_types_nearby,
  COALESCE(sc.is_isolated, 0) as is_isolated,
  COALESCE(sc.is_neighborhood_isolated, 0) as is_neighborhood_isolated,
  COALESCE(sc.local_density_ratio, 0) as local_density_ratio,
  
  -- H3 cell statistics (historical patterns for this location)
  COALESCE(cs.avg_vessel_count, 0) as cell_avg_vessel_count,
  COALESCE(cs.p95_vessel_count, 0) as cell_p95_vessel_count,
  COALESCE(cs.vessel_type_diversity, 0) as cell_vessel_type_diversity,
  COALESCE(cs.cell_avg_speed, 0) as cell_historical_avg_speed,
  COALESCE(cs.cell_stddev_speed, 0) as cell_historical_stddev_speed,
  COALESCE(cs.is_transit_corridor, 0) as is_transit_corridor,
  COALESCE(cs.is_stationary_area, 0) as is_stationary_area,
  
  -- Derived spatial anomaly indicators
  CASE 
    WHEN COALESCE(sc.vessels_in_same_cell, 0) > COALESCE(cs.p95_vessel_count, 10) THEN 1 
    ELSE 0 
  END as is_unusually_crowded,
  
  CASE 
    WHEN COALESCE(sc.vessels_in_kring1, 0) = 0 
      AND COALESCE(cs.avg_vessel_count, 0) > 2 THEN 1 
    ELSE 0 
  END as is_unexpectedly_isolated,
  
  -- Time features
  hour(v.timestamp) as hour_of_day,
  dayofweek(v.timestamp) as day_of_week
  
FROM vessel_behavioral_features v

LEFT JOIN vessel_rolling_patterns r 
  ON v.mmsi = r.mmsi 
  AND v.timestamp = r.timestamp

LEFT JOIN h3_normal_patterns n 
  ON v.h3_res7 = n.h3_res7 
  AND v.vessel_type = n.vessel_type
  AND hour(v.timestamp) = n.hour_of_day

LEFT JOIN vessel_spatial_context sc
  ON v.mmsi = sc.mmsi
  AND v.timestamp = sc.timestamp
  AND v.h3_res8 = sc.h3_res8

LEFT JOIN h3_cell_statistics cs
  ON v.h3_res8 = cs.h3_res8
  AND hour(v.timestamp) = cs.hour_of_day

WHERE v.timestamp >= current_timestamp() - INTERVAL 7 DAYS;

-- COMMAND ----------

select * from vessel_ml_features limit 10;
