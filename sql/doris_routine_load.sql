USE industrial_apm;

CREATE ROUTINE LOAD industrial_apm.rl_staging_apm_sensor_readings
ON dwd_apm_sensor_readings_rt
COLUMNS(
  event_id,
  schema_version,
  tenant_id,
  plant_id,
  asset_id,
  asset_name,
  tag_id,
  tag_name,
  metric_name,
  threshold_profile_id,
  event_time,
  ingest_time,
  value,
  unit,
  source_system,
  quality_flags,
  scenario,
  source_topic = 'tenant_northwind.staging_apm__sensor_readings.v1'
)
PROPERTIES (
  "format" = "json",
  "desired_concurrent_number" = "1",
  "max_error_number" = "100"
)
FROM KAFKA (
  "kafka_broker_list" = "redpanda:19092",
  "kafka_topic" = "tenant_northwind.staging_apm__sensor_readings.v1",
  "property.group.id" = "doris_staging_apm_sensor_readings_from_beginning",
  "property.kafka_default_offsets" = "OFFSET_BEGINNING"
);

CREATE ROUTINE LOAD industrial_apm.rl_mart_apm_anomaly_events
ON fact_apm_anomaly_events
COLUMNS(
  anomaly_id,
  schema_version,
  rule_id,
  tenant_id,
  plant_id,
  asset_id,
  asset_name,
  tag_id,
  tag_name,
  metric_name,
  observed_value,
  baseline_value,
  expected_value,
  event_count,
  breach_count,
  window_start,
  window_end,
  severity,
  quality_flags,
  detection_confidence,
  source_event_ids,
  source_topic = 'tenant_northwind.mart_apm__fct_anomaly_events.v1'
)
PROPERTIES (
  "format" = "json",
  "desired_concurrent_number" = "1",
  "max_error_number" = "100"
)
FROM KAFKA (
  "kafka_broker_list" = "redpanda:19092",
  "kafka_topic" = "tenant_northwind.mart_apm__fct_anomaly_events.v1",
  "property.group.id" = "doris_mart_apm_anomaly_events_from_beginning",
  "property.kafka_default_offsets" = "OFFSET_BEGINNING"
);

CREATE ROUTINE LOAD industrial_apm.rl_mart_apm_late_sensor_readings
ON fact_apm_late_sensor_readings
COLUMNS(
  event_id,
  schema_version,
  tenant_id,
  plant_id,
  asset_id,
  tag_id,
  metric_name,
  event_time,
  ingest_time,
  watermark_time,
  lateness_minutes,
  reason,
  quality_flags,
  source_topic = 'tenant_northwind.mart_apm__fct_late_sensor_readings.v1'
)
PROPERTIES (
  "format" = "json",
  "desired_concurrent_number" = "1",
  "max_error_number" = "100"
)
FROM KAFKA (
  "kafka_broker_list" = "redpanda:19092",
  "kafka_topic" = "tenant_northwind.mart_apm__fct_late_sensor_readings.v1",
  "property.group.id" = "doris_mart_apm_late_sensor_readings_from_beginning",
  "property.kafka_default_offsets" = "OFFSET_BEGINNING"
);

CREATE ROUTINE LOAD industrial_apm.rl_mart_apm_stream_health_snapshots
ON fact_apm_stream_health_snapshots
COLUMNS(
  schema_version,
  job_name,
  tenant_id,
  watermark_lag_seconds,
  late_event_count,
  checkpoint_status,
  observed_at,
  source_topic = 'tenant_northwind.mart_apm__fct_stream_health_snapshots.v1'
)
PROPERTIES (
  "format" = "json",
  "desired_concurrent_number" = "1",
  "max_error_number" = "100"
)
FROM KAFKA (
  "kafka_broker_list" = "redpanda:19092",
  "kafka_topic" = "tenant_northwind.mart_apm__fct_stream_health_snapshots.v1",
  "property.group.id" = "doris_mart_apm_stream_health_snapshots_from_beginning",
  "property.kafka_default_offsets" = "OFFSET_BEGINNING"
);
