SET 'execution.runtime-mode' = 'batch';
SET 'table.local-time-zone' = 'UTC';

CREATE TABLE raw_sensor_events (
  event_id STRING,
  event_time STRING,
  ingest_time STRING,
  quality_flags ARRAY<STRING>,
  scenario STRING,
  schema_version STRING,
  source_system STRING,
  tag_id STRING,
  tag_name STRING,
  tenant_id STRING,
  unit STRING,
  `value` DOUBLE
) WITH (
  'connector' = 'kafka',
  'topic' = 'tenant_northwind.raw_apm__sensor_readings.v1',
  'properties.bootstrap.servers' = 'redpanda:19092',
  'properties.group.id' = 'phase02-flink-enrichment',
  'scan.startup.mode' = 'earliest-offset',
  'scan.bounded.mode' = 'latest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

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
  'format' = 'json'
);

INSERT INTO staging_telemetry_enriched
SELECT
  event_id,
  'enriched_telemetry.v1' AS schema_version,
  tenant_id,
  'plant_northwind_mfg_01' AS plant_id,
  CASE
    WHEN tag_id LIKE 'tag_northwind_mfg_01_0001_%' THEN 'asset_northwind_mfg_01_0001'
    WHEN tag_id LIKE 'tag_northwind_mfg_01_0002_%' THEN 'asset_northwind_mfg_01_0002'
    ELSE 'asset_unknown'
  END AS asset_id,
  CASE
    WHEN tag_id LIKE 'tag_northwind_mfg_01_0001_%' THEN 'Cnc Machine 0001'
    WHEN tag_id LIKE 'tag_northwind_mfg_01_0002_%' THEN 'Conveyor 0002'
    ELSE 'Unknown Asset'
  END AS asset_name,
  tag_id,
  tag_name,
  CASE
    WHEN tag_id LIKE '%_temperature' THEN 'temperature'
    WHEN tag_id LIKE '%_vibration' THEN 'vibration'
    WHEN tag_id LIKE '%_power_draw' THEN 'power_draw'
    ELSE 'unknown'
  END AS metric_name,
  'threshold_manufacturing_rotating' AS threshold_profile_id,
  event_time,
  ingest_time,
  `value`,
  unit,
  source_system,
  quality_flags,
  scenario
FROM raw_sensor_events
WHERE tenant_id = 'tenant_northwind';
