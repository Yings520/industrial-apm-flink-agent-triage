SET 'execution.runtime-mode' = 'streaming';
SET 'table.local-time-zone' = 'UTC';
SET 'pipeline.name' = '{{tenant_id}}-anomaly';

CREATE TABLE {{tenant_id}}_anomaly_staging (
  event_id STRING, tenant_id STRING, tag_id STRING, tag_name STRING,
  asset_id STRING, asset_name STRING, component_id STRING,
  component_type STRING, metric_name STRING,
  measurement_point STRING, expected_unit STRING,
  `value` DOUBLE, unit STRING, event_time STRING,
  quality_flags ARRAY<STRING>, asset_type STRING, site_id STRING,
  event_ts AS TO_TIMESTAMP(REPLACE(REPLACE(event_time, 'Z', ''), 'T', ' ')),
  WATERMARK FOR event_ts AS event_ts - INTERVAL '5' MINUTE
) WITH (
  'connector' = 'kafka',
  'topic' = '{{tenant_id}}.staging_apm__sensor_readings.v1',
  'properties.bootstrap.servers' = 'redpanda:19092',
  'properties.group.id' = '{{tenant_id}}-anomaly',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

CREATE TABLE {{tenant_id}}_anomaly_mart (
  anomaly_id STRING, schema_version STRING, rule_id STRING,
  tenant_id STRING, plant_id STRING, asset_id STRING,
  asset_name STRING, component_id STRING, tag_id STRING,
  tag_name STRING, metric_name STRING,
  observed_value DOUBLE, baseline_value DOUBLE, expected_value DOUBLE,
  event_count INT, breach_count INT,
  window_start STRING, window_end STRING, severity STRING,
  quality_flags ARRAY<STRING>, quality_event_ids ARRAY<STRING>,
  fault_type STRING, failure_mode_id STRING,
  detection_confidence DOUBLE, source_event_ids ARRAY<STRING>
) WITH (
  'connector' = 'kafka',
  'topic' = '{{tenant_id}}.mart_apm__fct_anomaly_events.v1',
  'properties.bootstrap.servers' = 'redpanda:19092',
  'format' = 'json'
);

INSERT INTO {{tenant_id}}_anomaly_mart
SELECT
  CONCAT('anom_', event_id) AS anomaly_id,
  'anomaly_event.v1' AS schema_version, 'threshold_spike_e2e' AS rule_id,
  tenant_id, site_id AS plant_id, asset_id,
  COALESCE(asset_name, ''), component_id, tag_id,
  COALESCE(tag_name, ''), COALESCE(metric_name, ''),
  `value` AS observed_value, CAST(0.0 AS DOUBLE), CAST(0.0 AS DOUBLE),
  1, 1, event_time, event_time,
  'warning' AS severity, quality_flags,
  CAST(NULL AS ARRAY<STRING>), 'sensor_fault',
  CAST(NULL AS STRING), 0.85, ARRAY[event_id]
FROM {{tenant_id}}_anomaly_staging
WHERE tenant_id = '{{tenant_id}}'
  AND (
    (metric_name IN ('temperature') AND `value` > 150.0)
    OR (metric_name IN ('vibration', 'vibration_rms') AND `value` > 10.0)
    OR (metric_name IN ('pressure') AND `value` > 25.0)
    OR (metric_name IN ('flow_rate') AND `value` > 300.0)
    OR (metric_name IN ('power_draw', 'power') AND `value` > 600.0)
    OR (metric_name IN ('motor_current') AND `value` > 250.0)
  );
