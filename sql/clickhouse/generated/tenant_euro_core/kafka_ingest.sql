USE industrial_apm;

-- ============================================================
-- ClickHouse Kafka Ingestion Template
-- Replaces: sql/doris/templates/routine_load.sql.tpl (Doris Routine Load)
-- Format: JSONEachRow
-- Broker: redpanda:19092
--
-- Placeholders:
--   tenant_euro_core      - Tenant identifier (used ONLY in DWD section; omit for CDC DIM)
--   ${source_topic}   - Kafka topic name
--   ${target_table}   - Target MergeTree / ReplacingMergeTree table
--   ${consumer_group} - Kafka consumer group ID
--
-- Pattern: Kafka engine table → Materialized View → target table
-- Usage: sed "s/\tenant_euro_core/tenant_northwind/g; s/\${source_topic}/.../g; ..." kafka_ingest.sql.tpl | clickhouse-client -mn
-- ============================================================

-- ============================================================
-- Section 1: PostgreSQL CDC → DIM tables (NOT tenant-scoped)
-- Source: Debezium flattened JSON from pg_apm.public.* topics
-- Debezium fields: table columns + __op (String) + __ts_ms (Int64)
-- MV maps __op → cdc_op, __ts_ms → cdc_updated_at
-- ============================================================

-- 1a. pg_apm.public.assets → dim_apm_assets
--   ${source_topic}   = pg_apm.public.assets
--   ${target_table}   = dim_apm_assets
--   ${consumer_group} = ch_cdc_dim_apm_assets

CREATE TABLE IF NOT EXISTS kafka_cdc_dim_apm_assets
(
    tenant_id               String,
    asset_id                String,
    company_id              String,
    site_id                 String,
    plant_id                String,
    area_id                 String,
    line_id                 String,
    asset_name              String,
    asset_type              String,
    parent_asset_id         String,
    functional_location     String,
    manufacturer            String,
    model                   String,
    serial_number           String,
    commissioned_at         String,
    lifecycle_state         String,
    operating_context_json  String,
    criticality             String,
    archetype               String,
    threshold_profile_id    String,
    expected_metrics        String,
    valid_from              String,
    valid_to                String,
    is_current              String,
    __op                    String,
    __ts_ms                 Int64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:19092',
    kafka_topic_list = '${source_topic}',
    kafka_group_name = '${consumer_group}',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_cdc_dim_apm_assets
TO ${target_table}
AS
SELECT
    tenant_id,
    asset_id,
    company_id,
    site_id,
    plant_id,
    area_id,
    line_id,
    asset_name,
    asset_type,
    parent_asset_id,
    functional_location,
    manufacturer,
    model,
    serial_number,
    commissioned_at,
    lifecycle_state,
    operating_context_json,
    criticality,
    archetype,
    threshold_profile_id,
    expected_metrics,
    valid_from,
    valid_to,
    is_current,
    __op                                                               AS cdc_op,
    toDateTime(__ts_ms / 1000)                                         AS cdc_updated_at
FROM kafka_cdc_dim_apm_assets;

-- 1b. pg_apm.public.asset_components → dim_apm_asset_components
--   ${source_topic}   = pg_apm.public.asset_components
--   ${target_table}   = dim_apm_asset_components
--   ${consumer_group} = ch_cdc_dim_apm_asset_components

CREATE TABLE IF NOT EXISTS kafka_cdc_dim_apm_asset_components
(
    tenant_id            String,
    component_id         String,
    asset_id             String,
    component_type       String,
    component_name       String,
    functional_location  String,
    manufacturer         String,
    model                String,
    serial_number        String,
    commissioned_at      String,
    lifecycle_state      String,
    criticality          String,
    expected_metrics     String,
    valid_from           String,
    valid_to             String,
    is_current           String,
    __op                 String,
    __ts_ms              Int64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:19092',
    kafka_topic_list = '${source_topic}',
    kafka_group_name = '${consumer_group}',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_cdc_dim_apm_asset_components
TO ${target_table}
AS
SELECT
    tenant_id,
    component_id,
    asset_id,
    component_type,
    component_name,
    functional_location,
    manufacturer,
    model,
    serial_number,
    commissioned_at,
    lifecycle_state,
    criticality,
    expected_metrics,
    valid_from,
    valid_to,
    is_current,
    __op                                                               AS cdc_op,
    toDateTime(__ts_ms / 1000)                                         AS cdc_updated_at
FROM kafka_cdc_dim_apm_asset_components;

-- 1c. pg_apm.public.asset_signal_tags_mapping → dim_apm_signal_tags_mapping
--   ${source_topic}   = pg_apm.public.asset_signal_tags_mapping
--   ${target_table}   = dim_apm_signal_tags_mapping
--   ${consumer_group} = ch_cdc_dim_apm_signal_tags_mapping

CREATE TABLE IF NOT EXISTS kafka_cdc_dim_apm_signal_tags_mapping
(
    tenant_id              String,
    tag_id                 String,
    tag_name               String,
    plant_id               String,
    asset_id               String,
    component_id           String,
    metric_name            String,
    unit                   String,
    source_system          String,
    signal_type            String,
    sampling_rate_hz       String,
    measurement_point      String,
    normal_range_min       String,
    normal_range_max       String,
    engineering_limit_min  String,
    engineering_limit_max  String,
    calibration_status     String,
    last_calibrated_at     String,
    valid_from             String,
    valid_to               String,
    is_current             String,
    __op                   String,
    __ts_ms                Int64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:19092',
    kafka_topic_list = '${source_topic}',
    kafka_group_name = '${consumer_group}',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_cdc_dim_apm_signal_tags_mapping
TO ${target_table}
AS
SELECT
    tenant_id,
    tag_id,
    tag_name,
    plant_id,
    asset_id,
    component_id,
    metric_name,
    unit,
    source_system,
    signal_type,
    sampling_rate_hz,
    measurement_point,
    normal_range_min,
    normal_range_max,
    engineering_limit_min,
    engineering_limit_max,
    calibration_status,
    last_calibrated_at,
    valid_from,
    valid_to,
    is_current,
    __op                                                               AS cdc_op,
    toDateTime(__ts_ms / 1000)                                         AS cdc_updated_at
FROM kafka_cdc_dim_apm_signal_tags_mapping;

-- 1d. pg_apm.public.apm_failuremode → dim_apm_failuremode
--   ${source_topic}   = pg_apm.public.apm_failuremode
--   ${target_table}   = dim_apm_failuremode
--   ${consumer_group} = ch_cdc_dim_apm_failuremode

CREATE TABLE IF NOT EXISTS kafka_cdc_dim_apm_failuremode
(
    failure_mode_id        String,
    asset_type             String,
    component_type         String,
    failure_mode_name      String,
    failure_effect         String,
    criticality            String,
    detectability          String,
    related_metrics_json   String,
    recommended_checks     String,
    runbook_refs           String,
    pf_interval_hint_hours String,
    maintenance_strategy   String,
    created_at             String,
    __op                   String,
    __ts_ms                Int64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:19092',
    kafka_topic_list = '${source_topic}',
    kafka_group_name = '${consumer_group}',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_cdc_dim_apm_failuremode
TO ${target_table}
AS
SELECT
    failure_mode_id,
    asset_type,
    component_type,
    failure_mode_name,
    failure_effect,
    criticality,
    detectability,
    related_metrics_json,
    recommended_checks,
    runbook_refs,
    pf_interval_hint_hours,
    maintenance_strategy,
    created_at,
    __op                                                               AS cdc_op,
    toDateTime(__ts_ms / 1000)                                         AS cdc_updated_at
FROM kafka_cdc_dim_apm_failuremode;

-- 1e. pg_apm.public.apm_failureevent → dim_apm_failureevent
--   ${source_topic}   = pg_apm.public.apm_failureevent
--   ${target_table}   = dim_apm_failureevent
--   ${consumer_group} = ch_cdc_dim_apm_failureevent

CREATE TABLE IF NOT EXISTS kafka_cdc_dim_apm_failureevent
(
    failure_event_id  String,
    tenant_id         String,
    asset_id          String,
    component_id      String,
    failure_mode_id   String,
    status            String,
    suspected_at      String,
    confirmed_at      String,
    description       String,
    __op              String,
    __ts_ms           Int64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:19092',
    kafka_topic_list = '${source_topic}',
    kafka_group_name = '${consumer_group}',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_cdc_dim_apm_failureevent
TO ${target_table}
AS
SELECT
    failure_event_id,
    tenant_id,
    asset_id,
    component_id,
    failure_mode_id,
    status,
    suspected_at,
    confirmed_at,
    description,
    __op                                                               AS cdc_op,
    toDateTime(__ts_ms / 1000)                                         AS cdc_updated_at
FROM kafka_cdc_dim_apm_failureevent;

-- 1f. pg_apm.public.workorder → dim_apm_workorder
--   ${source_topic}   = pg_apm.public.workorder
--   ${target_table}   = dim_apm_workorder
--   ${consumer_group} = ch_cdc_dim_apm_workorder

CREATE TABLE IF NOT EXISTS kafka_cdc_dim_apm_workorder
(
    work_order_id            String,
    tenant_id                String,
    asset_id                 String,
    component_id             String,
    failure_mode_id          String,
    anomaly_id               String,
    incident_id              String,
    work_type                String,
    priority                 String,
    problem_code             String,
    cause_code               String,
    remedy_code              String,
    technician_notes         String,
    downtime_minutes         String,
    parts_json               String,
    inspection_results_json  String,
    source_system            String,
    created_at               String,
    completed_at             String,
    status                   String,
    __op                     String,
    __ts_ms                  Int64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:19092',
    kafka_topic_list = '${source_topic}',
    kafka_group_name = '${consumer_group}',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_cdc_dim_apm_workorder
TO ${target_table}
AS
SELECT
    work_order_id,
    tenant_id,
    asset_id,
    component_id,
    failure_mode_id,
    anomaly_id,
    incident_id,
    work_type,
    priority,
    problem_code,
    cause_code,
    remedy_code,
    technician_notes,
    downtime_minutes,
    parts_json,
    inspection_results_json,
    source_system,
    created_at,
    completed_at,
    status,
    __op                                                                    AS cdc_op,
    toDateTime(__ts_ms / 1000)                                              AS cdc_updated_at
FROM kafka_cdc_dim_apm_workorder;

-- 1g. pg_apm.public.maintenance → dim_apm_maintenance
--   ${source_topic}   = pg_apm.public.maintenance
--   ${target_table}   = dim_apm_maintenance
--   ${consumer_group} = ch_cdc_dim_apm_maintenance

CREATE TABLE IF NOT EXISTS kafka_cdc_dim_apm_maintenance
(
    maintenance_id    String,
    tenant_id         String,
    asset_id          String,
    component_id      String,
    maintenance_type  String,
    maintenance_date  String,
    description       String,
    __op              String,
    __ts_ms           Int64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:19092',
    kafka_topic_list = '${source_topic}',
    kafka_group_name = '${consumer_group}',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_cdc_dim_apm_maintenance
TO ${target_table}
AS
SELECT
    maintenance_id,
    tenant_id,
    asset_id,
    component_id,
    maintenance_type,
    maintenance_date,
    description,
    __op                                                               AS cdc_op,
    toDateTime(__ts_ms / 1000)                                         AS cdc_updated_at
FROM kafka_cdc_dim_apm_maintenance;

-- 1h. pg_apm.public.tenants → dim_apm_tenants
--   ${source_topic}   = pg_apm.public.tenants
--   ${target_table}   = dim_apm_tenants
--   ${consumer_group} = ch_cdc_dim_apm_tenants

CREATE TABLE IF NOT EXISTS kafka_cdc_dim_apm_tenants
(
    tenant_id     String,
    company_id    String,
    tenant_name   String,
    status        String,
    __op          String,
    __ts_ms       Int64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:19092',
    kafka_topic_list = '${source_topic}',
    kafka_group_name = '${consumer_group}',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_cdc_dim_apm_tenants
TO ${target_table}
AS
SELECT
    tenant_id,
    company_id,
    tenant_name,
    status,
    __op                                                               AS cdc_op,
    toDateTime(__ts_ms / 1000)                                         AS cdc_updated_at
FROM kafka_cdc_dim_apm_tenants;

-- ============================================================
-- Section 2: Flink / Agent Kafka → DWD tables (tenant-scoped)
-- Source: Flink output JSON (JSONEachRow compatible)
-- Topic naming: {tenant_id}.{dataset}.v1
-- Each tenant gets its own Kafka engine table and consumer group
-- ============================================================

-- 2a. {tenant_id}.staging_apm__sensor_readings.v1 → dwd_apm_realtime_signal_readings
--   tenant_euro_core      = tenant_northwind
--   ${source_topic}   = tenant_euro_core.staging_apm__sensor_readings.v1
--   ${target_table}   = dwd_apm_realtime_signal_readings
--   ${consumer_group} = ch_tenant_euro_core_dwd_realtime_signal_readings

CREATE TABLE IF NOT EXISTS kafka_tenant_euro_core_dwd_realtime_signal_readings
(
    event_id             String,
    schema_version       String,
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
    kafka_topic_list = '${source_topic}',
    kafka_group_name = '${consumer_group}',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_tenant_euro_core_dwd_realtime_signal_readings
TO ${target_table}
AS
SELECT
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
    '${source_topic}'                                                  AS source_topic
FROM kafka_tenant_euro_core_dwd_realtime_signal_readings;

-- 2b. {tenant_id}.mart_apm__fct_anomaly_events.v1 → dwd_apm_anomaly_events
--   tenant_euro_core      = tenant_northwind
--   ${source_topic}   = tenant_euro_core.mart_apm__fct_anomaly_events.v1
--   ${target_table}   = dwd_apm_anomaly_events
--   ${consumer_group} = ch_tenant_euro_core_dwd_anomaly_events

CREATE TABLE IF NOT EXISTS kafka_tenant_euro_core_dwd_anomaly_events
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
    kafka_topic_list = '${source_topic}',
    kafka_group_name = '${consumer_group}',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_tenant_euro_core_dwd_anomaly_events
TO ${target_table}
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
    '${source_topic}'                                                  AS source_topic
FROM kafka_tenant_euro_core_dwd_anomaly_events;

-- 2c. {tenant_id}.mart_apm__fct_late_sensor_readings.v1 → dwd_apm_late_sensor_readings
--   tenant_euro_core      = tenant_northwind
--   ${source_topic}   = tenant_euro_core.mart_apm__fct_late_sensor_readings.v1
--   ${target_table}   = dwd_apm_late_sensor_readings
--   ${consumer_group} = ch_tenant_euro_core_dwd_late_sensor_readings

CREATE TABLE IF NOT EXISTS kafka_tenant_euro_core_dwd_late_sensor_readings
(
    event_id        String,
    schema_version  String,
    tenant_id       String,
    plant_id        String,
    asset_id        String,
    tag_id          String,
    metric_name     String,
    event_time       String,
    ingest_time      String,
    watermark_time   String,
    lateness_minutes String,
    reason          String,
    quality_flags   String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:19092',
    kafka_topic_list = '${source_topic}',
    kafka_group_name = '${consumer_group}',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_tenant_euro_core_dwd_late_sensor_readings
TO ${target_table}
AS
SELECT
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
    '${source_topic}'                                                  AS source_topic
FROM kafka_tenant_euro_core_dwd_late_sensor_readings;

-- 2d. {tenant_id}.mart_apm__fct_stream_health_snapshots.v1 → dwd_apm_stream_health_snapshots
--   tenant_euro_core      = tenant_northwind
--   ${source_topic}   = tenant_euro_core.mart_apm__fct_stream_health_snapshots.v1
--   ${target_table}   = dwd_apm_stream_health_snapshots
--   ${consumer_group} = ch_tenant_euro_core_dwd_stream_health_snapshots

CREATE TABLE IF NOT EXISTS kafka_tenant_euro_core_dwd_stream_health_snapshots
(
    schema_version       String,
    job_name             String,
    tenant_id            String,
    watermark_lag_seconds String,
    late_event_count     String,
    checkpoint_status    String,
    observed_at          String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:19092',
    kafka_topic_list = '${source_topic}',
    kafka_group_name = '${consumer_group}',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_tenant_euro_core_dwd_stream_health_snapshots
TO ${target_table}
AS
SELECT
    schema_version,
    job_name,
    tenant_id,
    watermark_lag_seconds,
    late_event_count,
    checkpoint_status,
    observed_at,
    '${source_topic}'                                                  AS source_topic
FROM kafka_tenant_euro_core_dwd_stream_health_snapshots;

-- 2e. {tenant_id}.mart_apm__fct_realtime_diagnosis_events.v1 → dwd_apm_realtime_diagnosis_events
--   tenant_euro_core      = tenant_northwind
--   ${source_topic}   = tenant_euro_core.mart_apm__fct_realtime_diagnosis_events.v1
--   ${target_table}   = dwd_apm_realtime_diagnosis_events
--   ${consumer_group} = ch_tenant_euro_core_dwd_realtime_diagnosis_events

CREATE TABLE IF NOT EXISTS kafka_tenant_euro_core_dwd_realtime_diagnosis_events
(
    diagnosis_id        String,
    tenant_id           String,
    anomaly_id          String,
    asset_id            String,
    component_id        String,
    risk_level          String,
    escalation_required String,
    confidence          String,
    quality_caveats     String,
    created_at          String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:19092',
    kafka_topic_list = '${source_topic}',
    kafka_group_name = '${consumer_group}',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_tenant_euro_core_dwd_realtime_diagnosis_events
TO ${target_table}
AS
SELECT
    diagnosis_id,
    tenant_id,
    anomaly_id,
    asset_id,
    component_id,
    risk_level,
    escalation_required,
    confidence,
    quality_caveats,
    created_at,
    '${source_topic}'                                                  AS source_topic
FROM kafka_tenant_euro_core_dwd_realtime_diagnosis_events;

-- 2f. {tenant_id}.raw_apm__dlq_events.v1 → dwd_apm_dlq_events
--   tenant_euro_core      = tenant_northwind
--   ${source_topic}   = tenant_euro_core.raw_apm__dlq_events.v1
--   ${target_table}   = dwd_apm_dlq_events
--   ${consumer_group} = ch_tenant_euro_core_dwd_dlq_events

CREATE TABLE IF NOT EXISTS kafka_tenant_euro_core_dwd_dlq_events
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
    kafka_topic_list = '${source_topic}',
    kafka_group_name = '${consumer_group}',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_tenant_euro_core_dwd_dlq_events
TO ${target_table}
AS
SELECT
    dlq_event_id,
    tenant_id,
    event_time,
    raw_payload,
    original_topic,
    error_type,
    error_message,
    '${source_topic}'                                                  AS source_topic
FROM kafka_tenant_euro_core_dwd_dlq_events;
