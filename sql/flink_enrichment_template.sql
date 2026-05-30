-- Flink enrichment job template
-- Usage: flink-sql-client.sh -f sql/flink_enrichment_template.sql -Dtenant_id=tenant_northwind

SET 'execution.runtime-mode' = 'batch';
SET 'table.local-time-zone' = 'UTC';

CREATE TEMPORARY TABLE raw_sensor_events (
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
  'topic' = '${tenant_id}.raw_apm__sensor_readings.v1',
  'properties.bootstrap.servers' = 'redpanda:19092',
  'properties.group.id' = 'flink-enrichment-${tenant_id}',
  'scan.startup.mode' = 'earliest-offset',
  'scan.bounded.mode' = 'latest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

CREATE TEMPORARY TABLE tag_metadata (
  tag_id_prefix STRING,
  plant_id STRING,
  asset_id STRING,
  asset_name STRING,
  metric_name STRING,
  threshold_profile_id STRING
) WITH (
  'connector' = 'filesystem',
  'path' = '/opt/flink/data/tag_metadata.csv',
  'format' = 'csv',
  'csv.ignore-parse-errors' = 'true'
);

CREATE TEMPORARY TABLE staging_telemetry_enriched (
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
  'topic' = '${tenant_id}.staging_apm__sensor_readings.v1',
  'properties.bootstrap.servers' = 'redpanda:19092',
  'format' = 'json'
);

INSERT INTO staging_telemetry_enriched
SELECT
  r.event_id,
  'enriched_telemetry.v1' AS schema_version,
  r.tenant_id,
  COALESCE(m.plant_id, 'plant_unknown') AS plant_id,
  COALESCE(m.asset_id, 'asset_unknown') AS asset_id,
  COALESCE(m.asset_name, 'Unknown Asset') AS asset_name,
  r.tag_id,
  r.tag_name,
  COALESCE(m.metric_name, 'unknown') AS metric_name,
  COALESCE(m.threshold_profile_id, 'threshold_default') AS threshold_profile_id,
  r.event_time,
  r.ingest_time,
  r.`value`,
  r.unit,
  r.source_system,
  r.quality_flags,
  r.scenario
FROM raw_sensor_events r
LEFT JOIN tag_metadata m
  ON r.tag_id LIKE CONCAT(m.tag_id_prefix, '%');
