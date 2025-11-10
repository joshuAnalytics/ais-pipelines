# anomaly_features.py - Vessel anomaly detection feature engineering
"""
Creates anomaly detection features for AIS vessel data with session-based segmentation.
Handles vessels entering/exiting US waters by creating "sessions" separated by configurable gaps.
"""

import argparse
from dataclasses import dataclass
from typing import Optional
from pyspark.sql import SparkSession


@dataclass
class AnomalyFeaturesConfig:
    """Configuration for anomaly feature generation."""
    
    start_date: str
    end_date: str
    session_gap_hours: int = 24
    min_obs_6h: int = 3
    min_obs_24h: int = 10
    historical_baseline_days: int = 30
    spatial_context_days: int = 7
    ml_features_days: int = 7
    vessel_type_filter: int = 0


class BehavioralFeaturesCreator:
    """Creates vessel behavioral features with session-based segmentation."""
    
    def __init__(
        self, 
        spark: SparkSession, 
        full_table_name: str,
        ais_records_table: str,
        config: AnomalyFeaturesConfig
    ) -> None:
        self.spark = spark
        self.full_table_name = full_table_name
        self.ais_records_table = ais_records_table
        self.config = config
    
    def create(self) -> int:
        """Create vessel_behavioral_features table and return row count."""
        print(f"\nCreating table: {self.full_table_name}")
        print(f"Date range: {self.config.start_date} to {self.config.end_date}")
        print(f"Session gap threshold: {self.config.session_gap_hours} hours")
        
        # Build vessel type filter clause
        vessel_type_filter_clause = ""
        if self.config.vessel_type_filter and self.config.vessel_type_filter > 0:
            vessel_type_filter_clause = f"AND vessel_type = {self.config.vessel_type_filter}"
            print(f"Filtering for vessel type: {self.config.vessel_type_filter}")
        else:
            print("Processing all vessel types")
        
        query = f"""
        CREATE OR REPLACE TABLE {self.full_table_name} AS
        WITH vessel_sessions AS (
          -- Identify session boundaries where vessels enter/exit US waters
          -- A gap > {self.config.session_gap_hours} hours indicates the vessel left coverage area
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
            -- Calculate time gap from previous observation
            (unix_timestamp(timestamp) - unix_timestamp(LAG(timestamp) OVER w)) / 3600.0 as hours_since_last,
            -- Mark session boundaries (gap > {self.config.session_gap_hours} hours or first observation)
            CASE 
              WHEN (unix_timestamp(timestamp) - unix_timestamp(LAG(timestamp) OVER w)) / 3600.0 > {self.config.session_gap_hours}
                OR LAG(timestamp) OVER w IS NULL 
              THEN 1 
              ELSE 0 
            END as is_new_session
          FROM {self.ais_records_table}
          WHERE timestamp >= '{self.config.start_date}'
            AND timestamp <= '{self.config.end_date}'
            {vessel_type_filter_clause}
          WINDOW w AS (PARTITION BY mmsi ORDER BY timestamp)
        ),
        vessel_sessions_with_id AS (
          -- Assign unique session_id to each continuous period in US waters
          SELECT 
            *,
            -- Cumulative sum creates unique session identifier
            CONCAT(mmsi, '_', 
              SUM(is_new_session) OVER (PARTITION BY mmsi ORDER BY timestamp)
            ) as session_id,
            -- Track position within session
            ROW_NUMBER() OVER (PARTITION BY mmsi, 
              SUM(is_new_session) OVER (PARTITION BY mmsi ORDER BY timestamp)
              ORDER BY timestamp
            ) as position_in_session
          FROM vessel_sessions
        ),
        vessel_trajectory AS (
          -- Calculate trajectory features and session characteristics
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
            session_id,
            is_new_session,
            position_in_session,
            -- Window functions for trajectory analysis (session-aware)
            LAG(timestamp, 1) OVER w as prev_timestamp,
            LAG(latitude, 1) OVER w as prev_lat,
            LAG(longitude, 1) OVER w as prev_lon,
            LAG(sog, 1) OVER w as prev_sog,
            LAG(cog, 1) OVER w as prev_cog,
            LAG(h3_res8, 1) OVER w as prev_h3_res8,
            LAG(h3_res7, 1) OVER w as prev_h3_res7,
            -- Look ahead for gap detection
            LEAD(timestamp, 1) OVER w as next_timestamp,
            -- Session statistics
            COUNT(*) OVER w as session_observation_count,
            MIN(timestamp) OVER (PARTITION BY session_id) as session_start_time,
            MAX(timestamp) OVER (PARTITION BY session_id) as session_end_time,
            -- Entry location for this session
            FIRST_VALUE(latitude) OVER (PARTITION BY session_id ORDER BY timestamp) as session_entry_lat,
            FIRST_VALUE(longitude) OVER (PARTITION BY session_id ORDER BY timestamp) as session_entry_lon
          FROM vessel_sessions_with_id
          WINDOW w AS (PARTITION BY session_id ORDER BY timestamp)
        ),
        computed_features AS (
          SELECT 
            *,
            -- Session duration and maturity
            (unix_timestamp(timestamp) - unix_timestamp(session_start_time)) / 3600.0 as time_since_session_start,
            (unix_timestamp(session_end_time) - unix_timestamp(session_start_time)) / 3600.0 as session_duration_hours,
            CASE 
              WHEN (unix_timestamp(timestamp) - unix_timestamp(session_start_time)) / 3600.0 < 6 THEN 1 
              ELSE 0 
            END as is_session_start,
            CASE 
              WHEN (unix_timestamp(timestamp) - unix_timestamp(session_start_time)) / 3600.0 >= 24 
                AND session_observation_count >= 50 
              THEN 1 
              ELSE 0 
            END as is_session_mature,
            
            -- Distance from session entry point
            CASE 
              WHEN session_entry_lat IS NOT NULL THEN
                ST_Distance(
                  ST_Point(longitude, latitude),
                  ST_Point(session_entry_lon, session_entry_lat)
                ) * 111.32  -- Convert degrees to km
              ELSE NULL
            END as distance_from_entry_km,
            
            -- Time gap detection with classification
            (unix_timestamp(next_timestamp) - unix_timestamp(timestamp)) / 3600.0 as hours_to_next_signal,
            (unix_timestamp(timestamp) - unix_timestamp(prev_timestamp)) / 3600.0 as hours_since_last_signal,
            
            -- Classify gap types
            CASE 
              WHEN (unix_timestamp(next_timestamp) - unix_timestamp(timestamp)) / 3600.0 > {self.config.session_gap_hours} THEN 'session_boundary'
              WHEN (unix_timestamp(next_timestamp) - unix_timestamp(timestamp)) / 3600.0 > 6 THEN 'potential_anomaly'
              WHEN (unix_timestamp(next_timestamp) - unix_timestamp(timestamp)) / 3600.0 > 1 THEN 'normal_gap'
              ELSE 'continuous'
            END as gap_type,
            
            -- Movement features (within same session)
            CASE 
              WHEN prev_lat IS NOT NULL THEN
                ST_Distance(
                  ST_Point(longitude, latitude),
                  ST_Point(prev_lon, prev_lat)
                ) * 111.32  -- Convert degrees to km
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
          WHERE prev_timestamp IS NOT NULL OR is_new_session = 1
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
          
          -- Session identification
          session_id,
          is_new_session,
          position_in_session,
          session_observation_count,
          time_since_session_start,
          session_duration_hours,
          is_session_start,
          is_session_mature,
          distance_from_entry_km,
          
          -- Gap detection
          hours_to_next_signal,
          hours_since_last_signal,
          gap_type,
          
          -- Movement features
          distance_moved_km,
          speed_change,
          course_change,
          changed_h3_cell,
          changed_h3_parent,
          
          -- Derived anomaly indicators (session-aware)
          CASE 
            WHEN gap_type = 'potential_anomaly' AND is_session_mature = 1 THEN 1 
            ELSE 0 
          END as potential_dark_period,
          CASE WHEN sog < 0.5 AND prev_sog > 5 THEN 1 ELSE 0 END as sudden_stop,
          CASE WHEN course_change > 90 AND sog > 5 THEN 1 ELSE 0 END as sharp_turn,
          
          -- Calculate implied speed from distance/time (within same session)
          CASE 
            WHEN hours_since_last_signal > 0.1 
              AND hours_since_last_signal < 2 
              AND is_new_session = 0 THEN
              distance_moved_km / hours_since_last_signal
            ELSE NULL
          END as implied_speed_kmh,
          
          -- Speed discrepancy (reported vs calculated)
          CASE 
            WHEN hours_since_last_signal > 0.1 
              AND hours_since_last_signal < 2 
              AND is_new_session = 0 THEN
              ABS((distance_moved_km / hours_since_last_signal) - (sog * 1.852))
            ELSE NULL
          END as speed_discrepancy_kmh,
          
          -- Average of current and previous sog
          (sog + COALESCE(prev_sog, sog)) / 2 * 1.852 as avg_sog_kmh,
          
          -- Data quality indicators
          CASE 
            WHEN is_session_start = 1 THEN 0
            WHEN session_observation_count < {self.config.min_obs_6h} THEN 0
            ELSE 1 
          END as has_sufficient_history_6h,
          
          CASE 
            WHEN time_since_session_start < 24 THEN 0
            WHEN session_observation_count < {self.config.min_obs_24h} THEN 0
            ELSE 1 
          END as has_sufficient_history_24h
        FROM computed_features
        """
        
        self.spark.sql(query)
        
        # Add Z-ordering for query performance
        print(f"Optimizing {self.full_table_name} with Z-ordering...")
        self.spark.sql(f"""
        OPTIMIZE {self.full_table_name}
        ZORDER BY (h3_res8, timestamp, mmsi)
        """)
        
        row_count = self.spark.table(self.full_table_name).count()
        print(f"Created {self.full_table_name}: {row_count:,} records")
        
        if row_count == 0:
            print("WARNING: Zero records created. Check date range and data availability.")
        
        return row_count


class RollingPatternsCreator:
    """Creates rolling pattern statistics for vessels."""
    
    def __init__(
        self, 
        spark: SparkSession, 
        full_table_name: str,
        behavioral_features_table: str,
        config: AnomalyFeaturesConfig
    ) -> None:
        self.spark = spark
        self.full_table_name = full_table_name
        self.behavioral_features_table = behavioral_features_table
        self.config = config
    
    def create(self) -> int:
        """Create vessel_rolling_patterns table and return row count."""
        print(f"\nCreating table: {self.full_table_name}")
        
        query = f"""
        CREATE OR REPLACE TABLE {self.full_table_name} AS
        SELECT 
          mmsi,
          vessel_name,
          vessel_type,
          timestamp,
          session_id,
          h3_res8,
          sog,
          
          -- Data quality flags
          has_sufficient_history_6h,
          has_sufficient_history_24h,
          
          -- Rolling statistics (6 hour window) - session-aware
          CASE WHEN has_sufficient_history_6h = 1 THEN AVG(sog) OVER w6h ELSE NULL END as avg_speed_6h,
          CASE WHEN has_sufficient_history_6h = 1 THEN STDDEV(sog) OVER w6h ELSE NULL END as stddev_speed_6h,
          CASE WHEN has_sufficient_history_6h = 1 THEN MIN(sog) OVER w6h ELSE NULL END as min_speed_6h,
          CASE WHEN has_sufficient_history_6h = 1 THEN MAX(sog) OVER w6h ELSE NULL END as max_speed_6h,
          
          CASE WHEN has_sufficient_history_6h = 1 THEN AVG(course_change) OVER w6h ELSE NULL END as avg_course_change_6h,
          CASE WHEN has_sufficient_history_6h = 1 THEN MAX(course_change) OVER w6h ELSE NULL END as max_course_change_6h,
          
          CASE WHEN has_sufficient_history_6h = 1 THEN SUM(changed_h3_cell) OVER w6h ELSE NULL END as h3_changes_6h,
          COUNT(*) OVER w6h as observation_count_6h,
          
          -- Rolling statistics (24 hour window) - session-aware
          CASE WHEN has_sufficient_history_24h = 1 THEN AVG(sog) OVER w24h ELSE NULL END as avg_speed_24h,
          CASE WHEN has_sufficient_history_24h = 1 THEN STDDEV(sog) OVER w24h ELSE NULL END as stddev_speed_24h,
          
          CASE WHEN has_sufficient_history_24h = 1 THEN APPROX_COUNT_DISTINCT(h3_res7) OVER w24h ELSE NULL END as unique_h3_cells_24h,
          
          -- Loitering detection (low speed, same area)
          CASE WHEN has_sufficient_history_6h = 1 THEN AVG(CASE WHEN sog < 2 THEN 1 ELSE 0 END) OVER w6h ELSE NULL END as pct_low_speed_6h,
          
          -- Erratic behavior score
          CASE 
            WHEN has_sufficient_history_6h = 1 THEN 
              (AVG(course_change) OVER w6h + COALESCE(STDDEV(sog) OVER w6h, 0))
            ELSE NULL 
          END as erratic_score_6h,
          
          -- Overall data quality score (0-1)
          CASE 
            WHEN has_sufficient_history_24h = 1 THEN 1.0
            WHEN has_sufficient_history_6h = 1 THEN 0.5
            ELSE 0.0
          END as data_quality_score
          
        FROM {self.behavioral_features_table}
        WINDOW 
          w6h AS (PARTITION BY session_id ORDER BY unix_timestamp(timestamp) RANGE BETWEEN 21600 PRECEDING AND CURRENT ROW),
          w24h AS (PARTITION BY session_id ORDER BY unix_timestamp(timestamp) RANGE BETWEEN 86400 PRECEDING AND CURRENT ROW)
        """
        
        self.spark.sql(query)
        
        row_count = self.spark.table(self.full_table_name).count()
        print(f"Created {self.full_table_name}: {row_count:,} records")
        
        return row_count


class H3NormalPatternsCreator:
    """Creates historical normal patterns for H3 cells."""
    
    def __init__(
        self, 
        spark: SparkSession, 
        full_table_name: str,
        behavioral_features_table: str,
        config: AnomalyFeaturesConfig
    ) -> None:
        self.spark = spark
        self.full_table_name = full_table_name
        self.behavioral_features_table = behavioral_features_table
        self.config = config
        
        # Calculate baseline date range
        from datetime import datetime, timedelta
        end_date = datetime.strptime(config.start_date, '%Y-%m-%d')
        start_date = end_date + timedelta(days=config.historical_baseline_days)
        self.baseline_end = start_date.strftime('%Y-%m-%d')
    
    def create(self) -> int:
        """Create h3_normal_patterns table and return row count."""
        print(f"\nCreating table: {self.full_table_name}")
        print(f"Historical baseline: {self.config.start_date} to {self.baseline_end}")
        
        query = f"""
        CREATE OR REPLACE TABLE {self.full_table_name} AS
        SELECT 
          h3_res7,
          vessel_type,
          hour(timestamp) as hour_of_day,
          
          -- Speed patterns (excluding session boundaries)
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
          
          -- Signal gap patterns (excluding session boundaries)
          AVG(CASE WHEN gap_type != 'session_boundary' THEN hours_to_next_signal ELSE NULL END) as avg_signal_gap,
          PERCENTILE_APPROX(CASE WHEN gap_type != 'session_boundary' THEN hours_to_next_signal ELSE NULL END, 0.95) as p95_signal_gap
        
        FROM {self.behavioral_features_table}
        WHERE timestamp BETWEEN '{self.config.start_date}' AND '{self.baseline_end}'
          AND gap_type != 'session_boundary'
          AND is_session_start = 0
        GROUP BY h3_res7, vessel_type, hour(timestamp)
        """
        
        self.spark.sql(query)
        
        row_count = self.spark.table(self.full_table_name).count()
        print(f"Created {self.full_table_name}: {row_count:,} records")
        
        return row_count


class H3CellStatisticsCreator:
    """Creates H3 cell statistics for spatial context."""
    
    def __init__(
        self, 
        spark: SparkSession, 
        full_table_name: str,
        behavioral_features_table: str,
        config: AnomalyFeaturesConfig
    ) -> None:
        self.spark = spark
        self.full_table_name = full_table_name
        self.behavioral_features_table = behavioral_features_table
        self.config = config
        
        # Calculate baseline date range
        from datetime import datetime, timedelta
        end_date = datetime.strptime(config.start_date, '%Y-%m-%d')
        start_date = end_date + timedelta(days=config.historical_baseline_days)
        self.baseline_end = start_date.strftime('%Y-%m-%d')
    
    def create(self) -> int:
        """Create h3_cell_statistics table and return row count."""
        print(f"\nCreating table: {self.full_table_name}")
        
        query = f"""
        CREATE OR REPLACE TABLE {self.full_table_name} AS
        WITH base_aggregations AS (
          -- First level: aggregate vessel counts and activity metrics per time period
          SELECT 
            h3_res7,
            h3_res8,
            hour(timestamp) as hour_of_day,
            COUNT(DISTINCT mmsi) as vessel_count,
            COUNT(DISTINCT vessel_type) as vessel_type_diversity,
            AVG(sog) as cell_avg_speed,
            STDDEV(sog) as cell_stddev_speed,
            PERCENTILE_APPROX(sog, 0.5) as cell_median_speed,
            COUNT(*) as total_observations,
            MODE(vessel_type) as dominant_vessel_type
          FROM {self.behavioral_features_table}
          WHERE timestamp BETWEEN '{self.config.start_date}' AND '{self.baseline_end}'
            AND gap_type != 'session_boundary'
            AND is_session_start = 0
          GROUP BY h3_res7, h3_res8, hour(timestamp)
        )
        SELECT 
          h3_res7,
          h3_res8,
          hour_of_day,
          
          -- Vessel density patterns (calculated from pre-aggregated counts)
          AVG(vessel_count) as avg_vessel_count,
          PERCENTILE_APPROX(vessel_count, 0.95) as p95_vessel_count,
          PERCENTILE_APPROX(vessel_count, 0.05) as p05_vessel_count,
          
          -- Vessel type distribution
          dominant_vessel_type,
          AVG(vessel_type_diversity) as vessel_type_diversity,
          
          -- Activity characterization
          AVG(cell_avg_speed) as cell_avg_speed,
          AVG(cell_stddev_speed) as cell_stddev_speed,
          AVG(cell_median_speed) as cell_median_speed,
          
          -- Classification flags
          CASE 
            WHEN AVG(cell_avg_speed) > 8 THEN 1 
            ELSE 0 
          END as is_transit_corridor,
          
          CASE 
            WHEN AVG(cell_avg_speed) < 2 AND AVG(vessel_count) > 3 THEN 1 
            ELSE 0 
          END as is_stationary_area,
          
          SUM(total_observations) as total_observations
        
        FROM base_aggregations
        GROUP BY h3_res7, h3_res8, hour_of_day, dominant_vessel_type
        """
        
        self.spark.sql(query)
        
        row_count = self.spark.table(self.full_table_name).count()
        print(f"Created {self.full_table_name}: {row_count:,} records")
        
        return row_count


class CellHourlyStatisticsCreator:
    """Creates pre-aggregated cell-level statistics for spatial context."""
    
    def __init__(
        self, 
        spark: SparkSession, 
        full_table_name: str,
        behavioral_features_table: str,
        config: AnomalyFeaturesConfig
    ) -> None:
        self.spark = spark
        self.full_table_name = full_table_name
        self.behavioral_features_table = behavioral_features_table
        self.config = config
        
        # Calculate spatial context date range
        from datetime import datetime, timedelta
        end_date = datetime.strptime(config.end_date, '%Y-%m-%d')
        start_date = end_date - timedelta(days=config.spatial_context_days)
        self.spatial_start = start_date.strftime('%Y-%m-%d')
    
    def create(self) -> int:
        """Create cell_hourly_statistics table and return row count."""
        print(f"\nCreating table: {self.full_table_name}")
        print(f"Spatial context window: {self.spatial_start} to {self.config.end_date}")
        
        query = f"""
        CREATE OR REPLACE TABLE {self.full_table_name}
        PARTITIONED BY (date_partition)
        AS
        SELECT 
          h3_res8,
          h3_res7,
          date_trunc('hour', timestamp) as time_bucket,
          date(timestamp) as date_partition,
          
          -- Vessel counts
          COUNT(DISTINCT mmsi) as vessel_count,
          COUNT(DISTINCT vessel_type) as vessel_type_count,
          
          -- Activity metrics
          AVG(sog) as avg_speed,
          COUNT(*) as observation_count
          
        FROM {self.behavioral_features_table}
        WHERE timestamp >= '{self.spatial_start}'
          AND timestamp <= '{self.config.end_date}'
          AND is_session_start = 0
        GROUP BY h3_res8, h3_res7, date_trunc('hour', timestamp), date(timestamp)
        """
        
        self.spark.sql(query)
        
        # Add Z-ordering for query performance
        print(f"Optimizing {self.full_table_name} with Z-ordering...")
        self.spark.sql(f"""
        OPTIMIZE {self.full_table_name}
        ZORDER BY (h3_res8, time_bucket)
        """)
        
        row_count = self.spark.table(self.full_table_name).count()
        print(f"Created {self.full_table_name}: {row_count:,} records")
        
        return row_count


class SpatialContextCreator:
    """Creates vessel spatial context features."""
    
    def __init__(
        self, 
        spark: SparkSession, 
        full_table_name: str,
        behavioral_features_table: str,
        cell_hourly_stats_table: str,
        config: AnomalyFeaturesConfig
    ) -> None:
        self.spark = spark
        self.full_table_name = full_table_name
        self.behavioral_features_table = behavioral_features_table
        self.cell_hourly_stats_table = cell_hourly_stats_table
        self.config = config
        
        # Calculate spatial context date range
        from datetime import datetime, timedelta
        end_date = datetime.strptime(config.end_date, '%Y-%m-%d')
        start_date = end_date - timedelta(days=config.spatial_context_days)
        self.spatial_start = start_date.strftime('%Y-%m-%d')
    
    def create(self) -> int:
        """Create vessel_spatial_context table and return row count."""
        print(f"\nCreating table: {self.full_table_name}")
        print(f"Spatial context window: {self.spatial_start} to {self.config.end_date}")
        
        query = f"""
        CREATE OR REPLACE TABLE {self.full_table_name}
        PARTITIONED BY (date_partition)
        AS
        WITH vessel_positions AS (
          SELECT 
            mmsi,
            vessel_name,
            timestamp,
            h3_res8,
            date(timestamp) as date_partition,
            date_trunc('hour', timestamp) as time_bucket
          FROM {self.behavioral_features_table}
          WHERE timestamp >= '{self.spatial_start}'
            AND timestamp <= '{self.config.end_date}'
            AND is_session_start = 0
        ),
        vessel_with_same_cell_stats AS (
          -- Join with own cell statistics
          SELECT 
            vp.*,
            COALESCE(chs.vessel_count - 1, 0) as vessels_in_same_cell,
            COALESCE(chs.vessel_type_count, 0) as vessel_types_in_same_cell
          FROM vessel_positions vp
          LEFT JOIN {self.cell_hourly_stats_table} chs
            ON vp.h3_res8 = chs.h3_res8
            AND vp.time_bucket = chs.time_bucket
            AND vp.date_partition = chs.date_partition
        ),
        neighbor_stats AS (
          -- Aggregate stats from neighbor cells (kring=1)
          SELECT 
            vwcs.mmsi,
            vwcs.timestamp,
            vwcs.h3_res8,
            vwcs.date_partition,
            vwcs.vessels_in_same_cell,
            
            -- Sum vessel counts from all neighbor cells (including own cell)
            SUM(COALESCE(chs.vessel_count, 0)) as vessels_in_kring1,
            SUM(COALESCE(chs.vessel_type_count, 0)) as vessel_types_nearby
            
          FROM vessel_with_same_cell_stats vwcs
          CROSS JOIN LATERAL explode(h3_kring(vwcs.h3_res8, 1)) as (neighbor_cell)
          LEFT JOIN {self.cell_hourly_stats_table} chs
            ON neighbor_cell = chs.h3_res8
            AND vwcs.time_bucket = chs.time_bucket
            AND vwcs.date_partition = chs.date_partition
          GROUP BY 
            vwcs.mmsi, vwcs.timestamp, vwcs.h3_res8, 
            vwcs.date_partition, vwcs.vessels_in_same_cell
        )
        SELECT 
          vp.mmsi,
          vp.vessel_name,
          vp.timestamp,
          vp.h3_res8,
          vp.date_partition,
          
          ns.vessels_in_same_cell,
          ns.vessels_in_kring1,
          ns.vessel_types_nearby,
          
          CASE WHEN ns.vessels_in_same_cell = 0 THEN 1 ELSE 0 END as is_isolated,
          CASE WHEN ns.vessels_in_kring1 = 0 THEN 1 ELSE 0 END as is_neighborhood_isolated,
          
          CASE 
            WHEN ns.vessels_in_kring1 > 0 
            THEN CAST(ns.vessels_in_same_cell AS DOUBLE) / ns.vessels_in_kring1
            ELSE 0 
          END as local_density_ratio
          
        FROM vessel_positions vp
        INNER JOIN neighbor_stats ns
          ON vp.mmsi = ns.mmsi 
          AND vp.timestamp = ns.timestamp
          AND vp.h3_res8 = ns.h3_res8
        """
        
        self.spark.sql(query)
        
        # Add Z-ordering for query performance
        print(f"Optimizing {self.full_table_name} with Z-ordering...")
        self.spark.sql(f"""
        OPTIMIZE {self.full_table_name}
        ZORDER BY (mmsi, h3_res8, timestamp)
        """)
        
        row_count = self.spark.table(self.full_table_name).count()
        print(f"Created {self.full_table_name}: {row_count:,} records")
        
        return row_count


class MLFeaturesCreator:
    """Creates combined ML features table."""
    
    def __init__(
        self, 
        spark: SparkSession, 
        full_table_name: str,
        behavioral_features_table: str,
        rolling_patterns_table: str,
        h3_normal_patterns_table: str,
        h3_cell_statistics_table: str,
        spatial_context_table: str,
        config: AnomalyFeaturesConfig
    ) -> None:
        self.spark = spark
        self.full_table_name = full_table_name
        self.behavioral_features_table = behavioral_features_table
        self.rolling_patterns_table = rolling_patterns_table
        self.h3_normal_patterns_table = h3_normal_patterns_table
        self.h3_cell_statistics_table = h3_cell_statistics_table
        self.spatial_context_table = spatial_context_table
        self.config = config
        
        # Calculate ML features date range
        from datetime import datetime, timedelta
        end_date = datetime.strptime(config.end_date, '%Y-%m-%d')
        start_date = end_date - timedelta(days=config.ml_features_days)
        self.ml_start = start_date.strftime('%Y-%m-%d')
    
    def create(self) -> int:
        """Create vessel_ml_features table and return row count."""
        print(f"\nCreating table: {self.full_table_name}")
        print(f"ML features window: {self.ml_start} to {self.config.end_date}")
        
        query = f"""
        CREATE OR REPLACE TABLE {self.full_table_name} AS
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
          
          -- Session features
          v.session_id,
          v.is_new_session,
          v.position_in_session,
          v.session_observation_count,
          v.time_since_session_start,
          v.session_duration_hours,
          v.is_session_start,
          v.is_session_mature,
          v.distance_from_entry_km,
          
          -- Gap detection
          v.hours_to_next_signal,
          v.hours_since_last_signal,
          v.gap_type,
          
          -- Movement features
          v.distance_moved_km,
          v.speed_change,
          v.course_change,
          v.changed_h3_cell,
          v.changed_h3_parent,
          
          -- Anomaly indicators
          v.potential_dark_period,
          v.sudden_stop,
          v.sharp_turn,
          v.implied_speed_kmh,
          v.speed_discrepancy_kmh,
          v.avg_sog_kmh,
          
          -- Data quality
          v.has_sufficient_history_6h,
          v.has_sufficient_history_24h,
          
          -- Rolling pattern features (session-aware)
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
          r.data_quality_score,
          
          -- Historical pattern features (excluding session boundaries)
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
          
          -- Derived spatial anomaly indicators (session-aware)
          CASE 
            WHEN COALESCE(sc.vessels_in_same_cell, 0) > COALESCE(cs.p95_vessel_count, 10) THEN 1 
            ELSE 0 
          END as is_unusually_crowded,
          
          CASE 
            WHEN COALESCE(sc.vessels_in_kring1, 0) = 0 
              AND COALESCE(cs.avg_vessel_count, 0) > 2 
              AND v.is_session_mature = 1 THEN 1
            ELSE 0 
          END as is_unexpectedly_isolated,
          
          -- Time features
          hour(v.timestamp) as hour_of_day,
          dayofweek(v.timestamp) as day_of_week
          
        FROM {self.behavioral_features_table} v
        
        LEFT JOIN {self.rolling_patterns_table} r 
          ON v.mmsi = r.mmsi 
          AND v.timestamp = r.timestamp
        
        LEFT JOIN {self.h3_normal_patterns_table} n 
          ON v.h3_res7 = n.h3_res7 
          AND v.vessel_type = n.vessel_type
          AND hour(v.timestamp) = n.hour_of_day
        
        LEFT JOIN {self.spatial_context_table} sc
          ON v.mmsi = sc.mmsi
          AND v.timestamp = sc.timestamp
          AND v.h3_res8 = sc.h3_res8
        
        LEFT JOIN {self.h3_cell_statistics_table} cs
          ON v.h3_res8 = cs.h3_res8
          AND hour(v.timestamp) = cs.hour_of_day
        
        WHERE v.timestamp >= '{self.ml_start}'
          AND v.timestamp <= '{self.config.end_date}'
        """
        
        self.spark.sql(query)
        
        row_count = self.spark.table(self.full_table_name).count()
        print(f"Created {self.full_table_name}: {row_count:,} records")
        
        return row_count


class VesselAnomalyFeaturesOrchestrator:
    """Orchestrates the vessel anomaly feature creation process."""
    
    def __init__(
        self,
        catalog: str,
        schema: str,
        start_date: str,
        end_date: str,
        session_gap_hours: int = 24,
        min_obs_6h: int = 3,
        min_obs_24h: int = 10,
        historical_baseline_days: int = 30,
        spatial_context_days: int = 7,
        ml_features_days: int = 7,
        vessel_type_filter: int = 0
    ) -> None:
        self.spark = SparkSession.builder.getOrCreate()
        self.catalog = catalog
        self.schema = schema
        
        self.config = AnomalyFeaturesConfig(
            start_date=start_date,
            end_date=end_date,
            session_gap_hours=session_gap_hours,
            min_obs_6h=min_obs_6h,
            min_obs_24h=min_obs_24h,
            historical_baseline_days=historical_baseline_days,
            spatial_context_days=spatial_context_days,
            ml_features_days=ml_features_days,
            vessel_type_filter=vessel_type_filter
        )
        
        # Table names
        self.ais_records_table = f"{catalog}.{schema}.ais_records"
        self.behavioral_features_table = f"{catalog}.{schema}.vessel_behavioral_features"
        self.rolling_patterns_table = f"{catalog}.{schema}.vessel_rolling_patterns"
        self.h3_normal_patterns_table = f"{catalog}.{schema}.h3_normal_patterns"
        self.h3_cell_statistics_table = f"{catalog}.{schema}.h3_cell_statistics"
        self.cell_hourly_stats_table = f"{catalog}.{schema}.cell_hourly_statistics"
        self.spatial_context_table = f"{catalog}.{schema}.vessel_spatial_context"
        self.ml_features_table = f"{catalog}.{schema}.vessel_ml_features"
    
    def run(self) -> None:
        """Execute the vessel anomaly features workflow."""
        print("="*70)
        print("Starting Vessel Anomaly Features Creation")
        print("="*70)
        print(f"Catalog: {self.catalog}")
        print(f"Schema: {self.schema}")
        print(f"Date range: {self.config.start_date} to {self.config.end_date}")
        print(f"Session gap threshold: {self.config.session_gap_hours} hours")
        if self.config.vessel_type_filter and self.config.vessel_type_filter > 0:
            print(f"Vessel type filter: {self.config.vessel_type_filter}")
        else:
            print("Vessel type filter: All types")
        print("="*70)
        
        # Ensure schema exists
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.schema}")
        self.spark.sql(f"USE CATALOG {self.catalog}")
        self.spark.sql(f"USE SCHEMA {self.schema}")
        
        # Step 1: Create behavioral features
        behavioral_creator = BehavioralFeaturesCreator(
            self.spark,
            self.behavioral_features_table,
            self.ais_records_table,
            self.config
        )
        behavioral_count = behavioral_creator.create()
        
        if behavioral_count == 0:
            print("\nERROR: No behavioral features created. Stopping pipeline.")
            return
        
        # Step 2: Create rolling patterns
        rolling_creator = RollingPatternsCreator(
            self.spark,
            self.rolling_patterns_table,
            self.behavioral_features_table,
            self.config
        )
        rolling_creator.create()
        
        # Step 3: Create H3 normal patterns
        h3_patterns_creator = H3NormalPatternsCreator(
            self.spark,
            self.h3_normal_patterns_table,
            self.behavioral_features_table,
            self.config
        )
        h3_patterns_creator.create()
        
        # Step 4: Create H3 cell statistics
        h3_stats_creator = H3CellStatisticsCreator(
            self.spark,
            self.h3_cell_statistics_table,
            self.behavioral_features_table,
            self.config
        )
        h3_stats_creator.create()
        
        # Step 5: Create cell hourly statistics (for spatial context optimization)
        cell_hourly_stats_creator = CellHourlyStatisticsCreator(
            self.spark,
            self.cell_hourly_stats_table,
            self.behavioral_features_table,
            self.config
        )
        cell_hourly_stats_creator.create()
        
        # Step 6: Create spatial context (using cell hourly statistics)
        spatial_creator = SpatialContextCreator(
            self.spark,
            self.spatial_context_table,
            self.behavioral_features_table,
            self.cell_hourly_stats_table,
            self.config
        )
        spatial_creator.create()
        
        # Step 7: Create ML features
        ml_creator = MLFeaturesCreator(
            self.spark,
            self.ml_features_table,
            self.behavioral_features_table,
            self.rolling_patterns_table,
            self.h3_normal_patterns_table,
            self.h3_cell_statistics_table,
            self.spatial_context_table,
            self.config
        )
        ml_creator.create()
        
        print("\n" + "="*70)
        print("Vessel Anomaly Features Creation Completed Successfully!")
        print("="*70)
        print(f"\nCreated tables:")
        print(f"  1. {self.behavioral_features_table}")
        print(f"  2. {self.rolling_patterns_table}")
        print(f"  3. {self.h3_normal_patterns_table}")
        print(f"  4. {self.h3_cell_statistics_table}")
        print(f"  5. {self.cell_hourly_stats_table}")
        print(f"  6. {self.spatial_context_table}")
        print(f"  7. {self.ml_features_table}")
        print("="*70)


def main() -> None:
    """Main entry point for the anomaly features script."""
    parser = argparse.ArgumentParser(
        description="Create vessel anomaly detection features with session-based segmentation"
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
        "--start-date",
        required=True,
        help="Start date for feature generation (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        required=True,
        help="End date for feature generation (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--session-gap-hours",
        type=int,
        default=24,
        help="Hours gap to define session boundaries (default: 24)",
    )
    parser.add_argument(
        "--min-obs-6h",
        type=int,
        default=3,
        help="Minimum observations for 6h rolling statistics (default: 3)",
    )
    parser.add_argument(
        "--min-obs-24h",
        type=int,
        default=10,
        help="Minimum observations for 24h rolling statistics (default: 10)",
    )
    parser.add_argument(
        "--historical-baseline-days",
        type=int,
        default=30,
        help="Days for historical baseline patterns (default: 30)",
    )
    parser.add_argument(
        "--spatial-context-days",
        type=int,
        default=7,
        help="Days for spatial context window (default: 7)",
    )
    parser.add_argument(
        "--ml-features-days",
        type=int,
        default=7,
        help="Days for ML features window (default: 7)",
    )
    parser.add_argument(
        "--vessel-type-filter",
        type=int,
        default=0,
        help="Vessel type code to filter (e.g., 55 for Law Enforcement, 52 for Tug, 70 for Cargo). "
             "Set to 0 for all types. See ais_pipelines.vessel_types.VESSEL_TYPES for complete mapping. "
             "(default: 0)",
    )

    args = parser.parse_args()

    orchestrator = VesselAnomalyFeaturesOrchestrator(
        catalog=args.catalog,
        schema=args.schema,
        start_date=args.start_date,
        end_date=args.end_date,
        session_gap_hours=args.session_gap_hours,
        min_obs_6h=args.min_obs_6h,
        min_obs_24h=args.min_obs_24h,
        historical_baseline_days=args.historical_baseline_days,
        spatial_context_days=args.spatial_context_days,
        ml_features_days=args.ml_features_days,
        vessel_type_filter=args.vessel_type_filter,
    )
    orchestrator.run()


if __name__ == "__main__":
    main()
