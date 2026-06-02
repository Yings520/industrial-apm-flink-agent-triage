SET 'execution.runtime-mode' = 'streaming';
SET 'table.local-time-zone' = 'UTC';
SET 'pipeline.name' = 'tenant_northwind-late_events';

CREATE TABLE tenant_northwind_late_events_sensor_readings (
  event_id STRING, tenant_id STRING, tag_id STRING, tag_name STRING,
  asset_id STRING, asset_name STRING, component_id STRING,
  component_type STRING, metric_name STRING,
  measurement_point STRING, expected_unit STRING,
  `value` DOUBLE, unit STRING, event_time STRING, ingest_time STRING,
  quality_flags ARRAY<STRING>, asset_type STRING, site_id STRING,
  event_ts AS TO_TIMESTAMP(REPLACE(REPLACE(event_time, 'Z', ''), 'T', ' ')),
  ingest_ts AS TO_TIMESTAMP(REPLACE(REPLACE(ingest_time, 'Z', ''), 'T', ' ')),
  WATERMARK FOR event_ts AS event_ts - INTERVAL '5' MINUTE
) WITH (
  'connector' = 'kafka',
  'topic' = 'tenant_northwind.staging_apm__sensor_readings.v1',
  'properties.bootstrap.servers' = 'redpanda:19092',
  'properties.group.id' = 'tenant_northwind-late-events',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

CREATE TABLE tenant_northwind_late_events_sink (
  event_id STRING,
  schema_version STRING,
  tenant_id STRING,
  plant_id STRING,
  asset_id STRING,
  component_id STRING,
  tag_id STRING,
  tag_name STRING,
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

INSERT INTO tenant_northwind_late_events_sink
SELECT
  event_id,
  'late_event.v1',
  tenant_id,
  site_id,
  asset_id,
  component_id,
  tag_id,
  tag_name,
  metric_name,
  event_time,
  ingest_time,
  CAST(event_ts - INTERVAL '5' MINUTE AS STRING),
  TIMESTAMPDIFF(SECOND, event_ts, ingest_ts) / 60.0,
  CASE
    WHEN TIMESTAMPDIFF(SECOND, event_ts, ingest_ts) > 600 THEN 'ingestion_lag_exceeded'
    ELSE 'late_detected'
  END,
  quality_flags
FROM tenant_northwind_late_events_sensor_readings
WHERE tenant_id = 'tenant_northwind'
  AND TIMESTAMPDIFF(SECOND, event_ts, ingest_ts) > 600;
