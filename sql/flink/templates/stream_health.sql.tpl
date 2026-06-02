SET 'execution.runtime-mode' = 'streaming';
SET 'table.local-time-zone' = 'UTC';
SET 'pipeline.name' = '{{tenant_id}}-stream_health';

CREATE TABLE {{tenant_id}}_stream_health_sensor_readings (
  event_id STRING,
  tenant_id STRING,
  event_time STRING,
  ingest_time STRING,
  source_system STRING
) WITH (
  'connector' = 'kafka',
  'topic' = '{{tenant_id}}.staging_apm__sensor_readings.v1',
  'properties.bootstrap.servers' = 'redpanda:19092',
  'properties.group.id' = '{{tenant_id}}-stream-health',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

CREATE TABLE {{tenant_id}}_stream_health_snapshots (
  schema_version STRING,
  job_name STRING,
  tenant_id STRING,
  watermark_lag_seconds DOUBLE,
  late_event_count INT,
  dlq_count INT,
  missing_heartbeat_count INT,
  processing_time_ms DOUBLE,
  checkpoint_status STRING,
  observed_at STRING
) WITH (
  'connector' = 'kafka',
  'topic' = '{{tenant_id}}.mart_apm__fct_stream_health_snapshots.v1',
  'properties.bootstrap.servers' = 'redpanda:19092',
  'format' = 'json'
);

INSERT INTO {{tenant_id}}_stream_health_snapshots
SELECT
  'stream_health.v1',
  '{{tenant_id}}-stream_health',
  tenant_id,
  CAST(0 AS DOUBLE),
  CAST(0 AS INT),
  CAST(0 AS INT),
  CAST(0 AS INT),
  CAST(0 AS DOUBLE),
  'streaming_active',
  CAST(CURRENT_TIMESTAMP AS STRING)
FROM {{tenant_id}}_stream_health_sensor_readings
WHERE tenant_id = '{{tenant_id}}';
