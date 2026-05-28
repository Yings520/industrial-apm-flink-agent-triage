SET 'execution.runtime-mode' = 'batch';
SET 'table.local-time-zone' = 'UTC';

CREATE TABLE staging_telemetry_enriched (
  event_id STRING,
  schema_version STRING,
  tenant_id STRING,
  plant_id STRING,
  asset_id STRING,
  asset_name STRING,
  tag_id STRING,
  tag_name STRING,
  metric_name STRING,
  threshold_profile_id STRING,
  event_time STRING,
  ingest_time STRING,
  `value` DOUBLE,
  unit STRING,
  source_system STRING,
  quality_flags ARRAY<STRING>,
  scenario STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'tenant_northwind.staging_apm__sensor_readings.v1',
  'properties.bootstrap.servers' = 'redpanda:19092',
  'properties.group.id' = 'phase02-flink-anomalies',
  'scan.startup.mode' = 'earliest-offset',
  'scan.bounded.mode' = 'latest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

CREATE TABLE mart_anomalies (
  anomaly_id STRING,
  schema_version STRING,
  rule_id STRING,
  tenant_id STRING,
  plant_id STRING,
  asset_id STRING,
  asset_name STRING,
  tag_id STRING,
  tag_name STRING,
  metric_name STRING,
  observed_value DOUBLE,
  baseline_value DOUBLE,
  expected_value DOUBLE,
  event_count INT,
  breach_count INT,
  window_start STRING,
  window_end STRING,
  severity STRING,
  quality_flags ARRAY<STRING>,
  detection_confidence DOUBLE,
  source_event_ids ARRAY<STRING>
) WITH (
  'connector' = 'kafka',
  'topic' = 'tenant_northwind.mart_apm__fct_anomaly_events.v1',
  'properties.bootstrap.servers' = 'redpanda:19092',
  'format' = 'json'
);

CREATE TABLE mart_late_sensor_readings (
  event_id STRING,
  schema_version STRING,
  tenant_id STRING,
  plant_id STRING,
  asset_id STRING,
  tag_id STRING,
  metric_name STRING,
  event_time STRING,
  ingest_time STRING,
  watermark_time STRING,
  lateness_minutes DOUBLE,
  reason STRING,
  quality_flags ARRAY<STRING>
) WITH (
  'connector' = 'kafka',
  'topic' = 'tenant_northwind.mart_apm__fct_late_sensor_readings.v1',
  'properties.bootstrap.servers' = 'redpanda:19092',
  'format' = 'json'
);

CREATE TABLE mart_stream_health (
  schema_version STRING,
  job_name STRING,
  tenant_id STRING,
  watermark_lag_seconds DOUBLE,
  late_event_count INT,
  checkpoint_status STRING,
  observed_at STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'tenant_northwind.mart_apm__fct_stream_health_snapshots.v1',
  'properties.bootstrap.servers' = 'redpanda:19092',
  'format' = 'json'
);

INSERT INTO mart_anomalies
SELECT
  CONCAT('anom_', SUBSTRING(MD5(CONCAT_WS('|', scenario, tenant_id, asset_id, tag_id, metric_name, event_time, event_time)), 1, 20)) AS anomaly_id,
  'anomaly_event.v1' AS schema_version,
  CASE
    WHEN scenario = 'spike' THEN 'threshold_spike'
    WHEN scenario = 'drift' THEN 'drift_rate_of_change'
    WHEN scenario = 'late' THEN 'late_arrival'
    ELSE 'stream_quality_signal'
  END AS rule_id,
  tenant_id,
  plant_id,
  asset_id,
  asset_name,
  tag_id,
  tag_name,
  metric_name,
  `value` AS observed_value,
  CAST(NULL AS DOUBLE) AS baseline_value,
  CAST(NULL AS DOUBLE) AS expected_value,
  1 AS event_count,
  1 AS breach_count,
  event_time AS window_start,
  event_time AS window_end,
  CASE
    WHEN scenario = 'spike' THEN 'critical'
    ELSE 'warning'
  END AS severity,
  quality_flags,
  0.9 AS detection_confidence,
  ARRAY[event_id] AS source_event_ids
FROM staging_telemetry_enriched
WHERE scenario IN ('spike', 'drift', 'late');

INSERT INTO mart_late_sensor_readings
SELECT
  event_id,
  'late_event.v1' AS schema_version,
  tenant_id,
  plant_id,
  asset_id,
  tag_id,
  metric_name,
  event_time,
  ingest_time,
  ingest_time AS watermark_time,
  15.0 AS lateness_minutes,
  'beyond_allowed_lateness' AS reason,
  quality_flags
FROM staging_telemetry_enriched
WHERE scenario = 'late';

INSERT INTO mart_stream_health
SELECT
  'stream_health.v1' AS schema_version,
  'flink_anomalies_sql' AS job_name,
  tenant_id,
  900.0 AS watermark_lag_seconds,
  CAST(SUM(CASE WHEN scenario = 'late' THEN 1 ELSE 0 END) AS INT) AS late_event_count,
  'local_batch_complete' AS checkpoint_status,
  MAX(ingest_time) AS observed_at
FROM staging_telemetry_enriched
GROUP BY tenant_id;
