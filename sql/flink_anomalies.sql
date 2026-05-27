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
  'topic' = 'tenant_northwind.staging_telemetry_enriched.v1',
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
  'topic' = 'tenant_northwind.mart_anomalies.v1',
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
