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
    CASE 
      WHEN prev_lat IS NOT NULL THEN
        ST_Distance(
          ST_Point(longitude, latitude),
          ST_Point(prev_lon, prev_lat)
        ) * 111.32  -- Convert to km
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
  
  -- Derived anomaly indicators
  CASE WHEN hours_to_next_signal > 6 THEN 1 ELSE 0 END as potential_dark_period,
  CASE WHEN sog < 0.5 AND prev_sog > 5 THEN 1 ELSE 0 END as sudden_stop,
  CASE WHEN course_change > 90 AND sog > 5 THEN 1 ELSE 0 END as sharp_turn,
  
  -- Calculate implied speed from distance/time
  CASE 
    WHEN hours_since_last_signal > 0 AND hours_since_last_signal < 2 THEN
      distance_moved_km / hours_since_last_signal
    ELSE NULL
  END as implied_speed_kmh,
  
  -- Speed discrepancy (reported vs calculated)
  CASE 
    WHEN hours_since_last_signal > 0 AND hours_since_last_signal < 2 THEN
      ABS((distance_moved_km / hours_since_last_signal) - (sog * 1.852))
    ELSE NULL
  END as speed_discrepancy_kmh
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
WHERE timestamp BETWEEN '2025-01-01' AND '2025-01-02'  -- Historical baseline
GROUP BY h3_res7, vessel_type, hour(timestamp);

-- COMMAND ----------

CREATE OR REPLACE TABLE vessel_anomaly_scores AS
WITH enriched_features AS (
  SELECT 
    v.*,
    r.avg_speed_6h,
    r.stddev_speed_6h,
    r.avg_course_change_6h,
    r.h3_changes_6h,
    r.observation_count_6h,
    r.pct_low_speed_6h,
    r.erratic_score_6h,
    r.unique_h3_cells_24h,
    n.median_speed as historical_median_speed,
    n.q75_speed as historical_q75_speed,
    n.q95_speed as historical_q95_speed,
    n.avg_course_change as historical_avg_course_change,
    n.p95_signal_gap as historical_p95_signal_gap
  FROM vessel_behavioral_features v
  LEFT JOIN vessel_rolling_patterns r 
    ON v.mmsi = r.mmsi AND v.timestamp = r.timestamp
  LEFT JOIN h3_normal_patterns n 
    ON v.h3_res7 = n.h3_res7 
    AND v.vessel_type = n.vessel_type
    AND hour(v.timestamp) = n.hour_of_day
),
anomaly_components AS (
  SELECT 
    *,
    CASE 
      WHEN hours_to_next_signal > COALESCE(historical_p95_signal_gap, 2) * 2 THEN 
        LEAST(hours_to_next_signal / NULLIF(historical_p95_signal_gap, 0), 10)
      ELSE 0
    END as dark_score,
    CASE 
      WHEN sog > COALESCE(historical_q95_speed, 20) * 1.5 THEN 
        (sog - historical_q95_speed) / NULLIF(historical_q95_speed, 0)
      WHEN sog < 1 AND avg_speed_6h > 5 THEN 3.0
      WHEN speed_discrepancy_kmh > 10 THEN 2.0
      ELSE 0
    END as speed_anomaly_score,
    CASE 
      WHEN avg_course_change_6h > COALESCE(historical_avg_course_change, 30) * 2 THEN
        (avg_course_change_6h - historical_avg_course_change) / NULLIF(historical_avg_course_change, 1)
      WHEN h3_changes_6h > 20 THEN 3.0
      ELSE 0
    END as movement_anomaly_score,
    CASE 
      WHEN vessel_type IN ('Cargo', 'Tanker') AND pct_low_speed_6h > 0.6 THEN 2.0
      WHEN vessel_type = 'Fishing' AND avg_speed_6h > 15 THEN 2.0
      ELSE 0
    END as vessel_type_anomaly_score
  FROM enriched_features
)
SELECT 
  mmsi,
  vessel_name,
  vessel_type,
  timestamp,
  latitude,
  longitude,
  h3_res8,
  sog,
  dark_score,
  speed_anomaly_score,
  movement_anomaly_score,
  vessel_type_anomaly_score,
  (
    dark_score * 2.0 +
    speed_anomaly_score * 1.5 +
    movement_anomaly_score * 1.5 +
    vessel_type_anomaly_score * 1.0
  ) as total_anomaly_score,
  CASE 
    WHEN (dark_score * 2.0 + speed_anomaly_score * 1.5 + movement_anomaly_score * 1.5 + vessel_type_anomaly_score * 1.0) > 15 
    THEN 'critical'
    WHEN (dark_score * 2.0 + speed_anomaly_score * 1.5 + movement_anomaly_score * 1.5 + vessel_type_anomaly_score * 1.0) > 8 
    THEN 'high'
    WHEN (dark_score * 2.0 + speed_anomaly_score * 1.5 + movement_anomaly_score * 1.5 + vessel_type_anomaly_score * 1.0) > 3 
    THEN 'medium'
    ELSE 'low'
  END as anomaly_severity,
  hours_to_next_signal,
  speed_discrepancy_kmh,
  avg_course_change_6h,
  h3_changes_6h,
  pct_low_speed_6h,
  unique_h3_cells_24h
FROM anomaly_components
WHERE timestamp >= current_timestamp() - INTERVAL 7 DAYS;

-- COMMAND ----------

select * from vessel_anomaly_scores limit 10;