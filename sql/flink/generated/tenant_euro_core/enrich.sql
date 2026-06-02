SET 'execution.runtime-mode' = 'streaming';
SET 'table.local-time-zone' = 'UTC';
SET 'pipeline.name' = 'tenant_euro_core-enrich';

CREATE TABLE tenant_euro_core_enrich_raw_sensor_events (
  event_id STRING, event_time STRING, ingest_time STRING,
  event_ts AS TO_TIMESTAMP(REPLACE(REPLACE(event_time, 'Z', ''), 'T', ' ')),
  `value` DOUBLE, unit STRING, tenant_id STRING,
  tag_id STRING, tag_name STRING, source_system STRING,
  quality_flags ARRAY<STRING>,
  WATERMARK FOR event_ts AS event_ts - INTERVAL '5' MINUTE
) WITH (
  'connector' = 'kafka',
  'topic' = 'tenant_euro_core.raw_apm__sensor_readings.v1',
  'properties.bootstrap.servers' = 'redpanda:19092',
  'properties.group.id' = 'tenant_euro_core-enrich',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

CREATE TABLE tenant_euro_core_enrich_cdc_tags (
  tenant_id STRING, asset_id STRING, component_id STRING,
  tag_id STRING, metric_name STRING,
  measurement_point STRING, expected_unit STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'pg_apm.public.asset_signal_tags_mapping',
  'properties.bootstrap.servers' = 'redpanda:19092',
  'properties.group.id' = 'tenant_euro_core-enrich-cdc-tags',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

CREATE TABLE tenant_euro_core_enrich_cdc_assets (
  tenant_id STRING, asset_id STRING, asset_name STRING,
  asset_type STRING, asset_class STRING, site_id STRING,
  area_id STRING, line_id STRING, functional_location STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'pg_apm.public.assets',
  'properties.bootstrap.servers' = 'redpanda:19092',
  'properties.group.id' = 'tenant_euro_core-enrich-cdc-assets',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

CREATE TABLE tenant_euro_core_enrich_cdc_comps (
  tenant_id STRING, component_id STRING, asset_id STRING,
  component_type STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'pg_apm.public.asset_components',
  'properties.bootstrap.servers' = 'redpanda:19092',
  'properties.group.id' = 'tenant_euro_core-enrich-cdc-comps',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

CREATE TABLE tenant_euro_core_enrich_staging_enriched (
  event_id STRING, tenant_id STRING, tag_id STRING, tag_name STRING,
  asset_id STRING, asset_name STRING, component_id STRING,
  component_type STRING, metric_name STRING,
  measurement_point STRING, expected_unit STRING,
  `value` DOUBLE, unit STRING, event_time STRING, ingest_time STRING,
  quality_flags ARRAY<STRING>,
  asset_type STRING, site_id STRING,
  PRIMARY KEY (event_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'tenant_euro_core.staging_apm__sensor_readings.v1',
  'properties.bootstrap.servers' = 'redpanda:19092',
  'key.format' = 'json',
  'value.format' = 'json'
);

INSERT INTO tenant_euro_core_enrich_staging_enriched
SELECT r.event_id, r.tenant_id, r.tag_id, r.tag_name,
  m.asset_id, a.asset_name, m.component_id,
  c.component_type, m.metric_name,
  m.measurement_point, m.expected_unit,
  r.`value`, r.unit, r.event_time, r.ingest_time,
  r.quality_flags,
  a.asset_type, a.site_id
FROM tenant_euro_core_enrich_raw_sensor_events r
LEFT JOIN tenant_euro_core_enrich_cdc_tags m
  ON r.tenant_id = m.tenant_id AND r.tag_id = m.tag_id
LEFT JOIN tenant_euro_core_enrich_cdc_assets a
  ON m.tenant_id = a.tenant_id AND m.asset_id = a.asset_id
LEFT JOIN tenant_euro_core_enrich_cdc_comps c
  ON m.tenant_id = c.tenant_id AND m.asset_id = c.asset_id AND m.component_id = c.component_id
WHERE r.tenant_id = 'tenant_euro_core'
  AND m.asset_id IS NOT NULL;
