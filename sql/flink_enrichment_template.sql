-- Flink enrichment job template (production target)
-- Usage: flink-sql-client.sh -f sql/flink_enrichment_template.sql -Dtenant_id=tenant_northwind
-- Metadata source: PostgreSQL via JDBC lookup join (non-blocking, cached)
-- Production target: PostgreSQL + Debezium CDC → Kafka changelog → Flink state store

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
  'topic' = '${tenant_id}.raw_apm__sensor_readings.v1',
  'properties.bootstrap.servers' = 'redpanda:19092',
  'properties.group.id' = 'flink-enrichment-${tenant_id}',
  'scan.startup.mode' = 'earliest-offset',
  'scan.bounded.mode' = 'latest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

CREATE TABLE sensor_tags_lookup (
  tenant_id STRING,
  tag_id_pattern STRING,
  plant_id STRING,
  asset_id STRING,
  asset_name STRING,
  metric_name STRING,
  threshold_profile_id STRING
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://postgres:5432/apm_metadata',
  'table-name' = 'sensor_tags',
  'username' = 'apm',
  'password' = 'apm_secret',
  'lookup.cache' = 'PARTIAL',
  'lookup.partial-cache.max-rows' = '500',
  'lookup.partial-cache.expire-after-write' = '60min',
  'lookup.partial-cache.expire-after-access' = '10min'
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
LEFT JOIN sensor_tags_lookup FOR SYSTEM_TIME AS OF r.event_time AS m
  ON r.tenant_id = m.tenant_id
  AND r.tag_id LIKE CONCAT(m.tag_id_pattern, '%');
