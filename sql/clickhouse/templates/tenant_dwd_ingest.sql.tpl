USE industrial_apm;

-- ============================================================
-- ClickHouse Tenant DWD Ingestion Template
-- Replaces: sql/doris/templates/routine_load.sql.tpl Section 2
-- Pattern: Kafka engine table → Materialized View → DWD target table
-- Format: JSONEachRow
-- Broker: redpanda:19092
--
-- Placeholders:
--   ${tenant_id}  - Tenant identifier (e.g. tenant_xxxx)
--
-- Usage: cat tenant_dwd_ingest.sql.tpl | sed "s/\${tenant_id}/<YOUR_TENANT>/g" | clickhouse-client -mn
-- ============================================================

-- ============================================================
-- Section 1: Flink Kafka → DWD tables (tenant-scoped)
-- Each tenant gets its own Kafka engine table, MV, and consumer group
-- Topic naming: ${tenant_id}.{dataset}.v1
-- Consumer group: clickhouse_${tenant_id}_<target_table>
-- ============================================================

-- 1a. ${tenant_id}.staging_apm__sensor_readings.v1 → dwd_apm_realtime_signal_readings
--   Kafka source topic: ${tenant_id}.staging_apm__sensor_readings.v1
--   Target table:       dwd_apm_realtime_signal_readings
--   Consumer group:     clickhouse_${tenant_id}_dwd_realtime_signal_readings
--   Special:            schema_version = '1.0' constant (not in Kafka payload)

CREATE TABLE IF NOT EXISTS kafka_${tenant_id}_dwd_realtime_signal_readings
(
    event_id             String,
    tenant_id            String,
    plant_id             String,
    asset_id             String,
    asset_name           String,
    tag_id               String,
    tag_name             String,
    metric_name          String,
    threshold_profile_id String,
    event_time           String,
    ingest_time          String,
    value                String,
    unit                 String,
    source_system        String,
    quality_flags        String,
    scenario             String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:19092',
    kafka_topic_list = '${tenant_id}.staging_apm__sensor_readings.v1',
    kafka_group_name = 'clickhouse_${tenant_id}_dwd_realtime_signal_readings',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_${tenant_id}_dwd_realtime_signal_readings
TO dwd_apm_realtime_signal_readings
AS
SELECT
    event_id,
    '1.0'                                                                   AS schema_version,
    tenant_id,
    plant_id,
    asset_id,
    asset_name,
    tag_id,
    tag_name,
    metric_name,
    threshold_profile_id,
    parseDateTimeBestEffortOrNull(event_time)                               AS event_time,
    parseDateTimeBestEffortOrNull(ingest_time)                              AS ingest_time,
    toFloat64OrNull(value)                                                  AS value,
    unit,
    source_system,
    quality_flags,
    scenario,
    '${tenant_id}.staging_apm__sensor_readings.v1'                         AS source_topic
FROM kafka_${tenant_id}_dwd_realtime_signal_readings;

-- 1b. ${tenant_id}.mart_apm__fct_anomaly_events.v1 → dwd_apm_anomaly_events
--   Kafka source topic: ${tenant_id}.mart_apm__fct_anomaly_events.v1
--   Target table:       dwd_apm_anomaly_events
--   Consumer group:     clickhouse_${tenant_id}_dwd_anomaly_events

CREATE TABLE IF NOT EXISTS kafka_${tenant_id}_dwd_anomaly_events
(
    anomaly_id           String,
    schema_version       String,
    rule_id              String,
    tenant_id            String,
    plant_id             String,
    asset_id             String,
    asset_name           String,
    tag_id               String,
    tag_name             String,
    metric_name          String,
    observed_value       String,
    baseline_value       String,
    expected_value       String,
    event_count          String,
    breach_count         String,
    window_start         String,
    window_end           String,
    severity             String,
    quality_flags        String,
    detection_confidence String,
    source_event_ids     String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:19092',
    kafka_topic_list = '${tenant_id}.mart_apm__fct_anomaly_events.v1',
    kafka_group_name = 'clickhouse_${tenant_id}_dwd_anomaly_events',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_${tenant_id}_dwd_anomaly_events
TO dwd_apm_anomaly_events
AS
SELECT
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
    toFloat64OrNull(observed_value)                                         AS observed_value,
    toFloat64OrNull(baseline_value)                                         AS baseline_value,
    toFloat64OrNull(expected_value)                                         AS expected_value,
    toInt32OrNull(event_count)                                              AS event_count,
    toInt32OrNull(breach_count)                                             AS breach_count,
    parseDateTimeBestEffortOrNull(window_start)                             AS window_start,
    parseDateTimeBestEffortOrNull(window_end)                               AS window_end,
    severity,
    quality_flags,
    toFloat64OrNull(detection_confidence)                                   AS detection_confidence,
    source_event_ids,
    '${tenant_id}.mart_apm__fct_anomaly_events.v1'                        AS source_topic
FROM kafka_${tenant_id}_dwd_anomaly_events;

-- 1c. ${tenant_id}.mart_apm__fct_late_sensor_readings.v1 → dwd_apm_late_sensor_readings
--   Kafka source topic: ${tenant_id}.mart_apm__fct_late_sensor_readings.v1
--   Target table:       dwd_apm_late_sensor_readings
--   Consumer group:     clickhouse_${tenant_id}_dwd_late_sensor_readings

CREATE TABLE IF NOT EXISTS kafka_${tenant_id}_dwd_late_sensor_readings
(
    event_id         String,
    schema_version   String,
    tenant_id        String,
    plant_id         String,
    asset_id         String,
    tag_id           String,
    metric_name      String,
    event_time       String,
    ingest_time      String,
    watermark_time   String,
    lateness_minutes String,
    reason           String,
    quality_flags    String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:19092',
    kafka_topic_list = '${tenant_id}.mart_apm__fct_late_sensor_readings.v1',
    kafka_group_name = 'clickhouse_${tenant_id}_dwd_late_sensor_readings',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_${tenant_id}_dwd_late_sensor_readings
TO dwd_apm_late_sensor_readings
AS
SELECT
    event_id,
    schema_version,
    tenant_id,
    plant_id,
    asset_id,
    tag_id,
    metric_name,
    parseDateTimeBestEffortOrNull(event_time)                               AS event_time,
    parseDateTimeBestEffortOrNull(ingest_time)                              AS ingest_time,
    parseDateTimeBestEffortOrNull(watermark_time)                           AS watermark_time,
    toFloat64OrNull(lateness_minutes)                                       AS lateness_minutes,
    reason,
    quality_flags,
    '${tenant_id}.mart_apm__fct_late_sensor_readings.v1'                  AS source_topic
FROM kafka_${tenant_id}_dwd_late_sensor_readings;

-- 1d. ${tenant_id}.mart_apm__fct_stream_health_snapshots.v1 → dwd_apm_stream_health_snapshots
--   Kafka source topic: ${tenant_id}.mart_apm__fct_stream_health_snapshots.v1
--   Target table:       dwd_apm_stream_health_snapshots
--   Consumer group:     clickhouse_${tenant_id}_dwd_stream_health_snapshots

CREATE TABLE IF NOT EXISTS kafka_${tenant_id}_dwd_stream_health_snapshots
(
    schema_version        String,
    job_name              String,
    tenant_id             String,
    watermark_lag_seconds String,
    late_event_count      String,
    checkpoint_status     String,
    observed_at           String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:19092',
    kafka_topic_list = '${tenant_id}.mart_apm__fct_stream_health_snapshots.v1',
    kafka_group_name = 'clickhouse_${tenant_id}_dwd_stream_health_snapshots',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_${tenant_id}_dwd_stream_health_snapshots
TO dwd_apm_stream_health_snapshots
AS
SELECT
    schema_version,
    job_name,
    tenant_id,
    toFloat64OrNull(watermark_lag_seconds)                                  AS watermark_lag_seconds,
    toInt32OrNull(late_event_count)                                         AS late_event_count,
    checkpoint_status,
    parseDateTimeBestEffortOrNull(observed_at)                              AS observed_at,
    '${tenant_id}.mart_apm__fct_stream_health_snapshots.v1'               AS source_topic
FROM kafka_${tenant_id}_dwd_stream_health_snapshots;

-- 1e. ${tenant_id}.mart_apm__fct_realtime_diagnosis_events.v1 → dwd_apm_realtime_diagnosis_events
--   Kafka source topic: ${tenant_id}.mart_apm__fct_realtime_diagnosis_events.v1
--   Target table:       dwd_apm_realtime_diagnosis_events
--   Consumer group:     clickhouse_${tenant_id}_dwd_realtime_diagnosis_events
--   Special:            JSON field anomaly_event_id → target column anomaly_id
--                       JSON field generated_at       → target column created_at

CREATE TABLE IF NOT EXISTS kafka_${tenant_id}_dwd_realtime_diagnosis_events
(
    diagnosis_id        String,
    tenant_id           String,
    asset_id            String,
    component_id        String,
    anomaly_event_id    String,
    risk_level          String,
    escalation_required String,
    confidence          String,
    quality_caveats     String,
    generated_at        String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:19092',
    kafka_topic_list = '${tenant_id}.mart_apm__fct_realtime_diagnosis_events.v1',
    kafka_group_name = 'clickhouse_${tenant_id}_dwd_realtime_diagnosis_events',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_${tenant_id}_dwd_realtime_diagnosis_events
TO dwd_apm_realtime_diagnosis_events
AS
SELECT
    diagnosis_id,
    tenant_id,
    anomaly_event_id                                                        AS anomaly_id,
    asset_id,
    component_id,
    risk_level,
    toBool(escalation_required)                                             AS escalation_required,
    toFloat64OrNull(confidence)                                             AS confidence,
    quality_caveats,
    parseDateTimeBestEffortOrNull(generated_at)                             AS created_at,
    '${tenant_id}.mart_apm__fct_realtime_diagnosis_events.v1'             AS source_topic
FROM kafka_${tenant_id}_dwd_realtime_diagnosis_events;

-- 1f. ${tenant_id}.raw_apm__dlq_events.v1 → dwd_apm_dlq_events
--   Kafka source topic: ${tenant_id}.raw_apm__dlq_events.v1
--   Target table:       dwd_apm_dlq_events
--   Consumer group:     clickhouse_${tenant_id}_dwd_dlq_events

CREATE TABLE IF NOT EXISTS kafka_${tenant_id}_dwd_dlq_events
(
    dlq_event_id   String,
    tenant_id      String,
    event_time     String,
    raw_payload    String,
    original_topic String,
    error_type     String,
    error_message  String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:19092',
    kafka_topic_list = '${tenant_id}.raw_apm__dlq_events.v1',
    kafka_group_name = 'clickhouse_${tenant_id}_dwd_dlq_events',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_${tenant_id}_dwd_dlq_events
TO dwd_apm_dlq_events
AS
SELECT
    dlq_event_id,
    tenant_id,
    parseDateTimeBestEffortOrNull(event_time)                               AS event_time,
    raw_payload,
    original_topic,
    error_type,
    error_message,
    '${tenant_id}.raw_apm__dlq_events.v1'                                 AS source_topic
FROM kafka_${tenant_id}_dwd_dlq_events;
