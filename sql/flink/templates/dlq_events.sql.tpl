SET 'execution.runtime-mode' = 'streaming';
SET 'table.local-time-zone' = 'UTC';
SET 'pipeline.name' = '{{tenant_id}}-dlq_events';

CREATE TABLE {{tenant_id}}_dlq_events_raw_sensor_events (
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
  'topic' = '{{tenant_id}}.raw_apm__sensor_readings.v1',
  'properties.bootstrap.servers' = 'redpanda:19092',
  'properties.group.id' = '{{tenant_id}}-dlq-events',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

CREATE TABLE {{tenant_id}}_dlq_events_sink (
  original_record STRING,
  error_code STRING,
  field_path STRING,
  message STRING,
  schema_name STRING,
  schema_version STRING,
  validation_time STRING
) WITH (
  'connector' = 'kafka',
  'topic' = '{{tenant_id}}.raw_apm__dlq_events.v1',
  'properties.bootstrap.servers' = 'redpanda:19092',
  'format' = 'json'
);

INSERT INTO {{tenant_id}}_dlq_events_sink
SELECT
  JSON_OBJECT(
    'event_id' VALUE event_id,
    'schema_version' VALUE schema_version,
    'tenant_id' VALUE tenant_id,
    'tag_id' VALUE tag_id,
    'tag_name' VALUE tag_name,
    'event_time' VALUE event_time,
    'ingest_time' VALUE ingest_time,
    'value' VALUE `value`,
    'unit' VALUE unit,
    'source_system' VALUE source_system,
    'quality_flags' VALUE CAST(quality_flags AS STRING),
    'scenario' VALUE scenario
  ),
  'MISSING_REQUIRED_FIELD',
  CASE
    WHEN event_id IS NULL THEN '$.event_id'
    WHEN tenant_id IS NULL THEN '$.tenant_id'
    WHEN tag_id IS NULL THEN '$.tag_id'
    WHEN event_time IS NULL THEN '$.event_time'
    WHEN ingest_time IS NULL THEN '$.ingest_time'
    WHEN `value` IS NULL THEN '$.value'
    WHEN unit IS NULL THEN '$.unit'
    WHEN source_system IS NULL THEN '$.source_system'
  END,
  'Required raw sensor event field is missing or null',
  'raw_sensor_event.v1',
  'raw_sensor_event.v1',
  CAST(CURRENT_TIMESTAMP AS STRING)
FROM {{tenant_id}}_dlq_events_raw_sensor_events
WHERE tenant_id = '{{tenant_id}}'
  AND (event_id IS NULL OR tenant_id IS NULL OR tag_id IS NULL
    OR event_time IS NULL OR ingest_time IS NULL OR `value` IS NULL
    OR unit IS NULL OR source_system IS NULL);
