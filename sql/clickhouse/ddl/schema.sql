CREATE DATABASE IF NOT EXISTS industrial_apm;

-- ============================================================
-- POSTGRESQL CDC RAW LANDING TABLES
-- These tables mirror PostgreSQL source table columns only.
-- Derived/SCD/wide fields belong in later ClickHouse serving layers.
-- ============================================================

CREATE TABLE IF NOT EXISTS dim_apm_assets (
    tenant_id String,
    asset_id String,
    asset_name String,
    asset_type Nullable(String),
    asset_class Nullable(String),
    parent_asset_id Nullable(String),
    site_id Nullable(String),
    area_id Nullable(String),
    line_id Nullable(String),
    functional_location Nullable(String),
    criticality Nullable(String),
    lifecycle_state Nullable(String),
    created_at DateTime,
    updated_at DateTime
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (tenant_id, asset_id)
PARTITION BY tuple();

CREATE TABLE IF NOT EXISTS dim_apm_asset_components (
    tenant_id String,
    component_id String,
    asset_id String,
    parent_component_id Nullable(String),
    component_name String,
    component_type Nullable(String),
    lifecycle_state Nullable(String),
    created_at DateTime,
    updated_at DateTime
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (tenant_id, component_id)
PARTITION BY tuple();

CREATE TABLE IF NOT EXISTS dim_apm_signal_tags_mapping (
    tenant_id String,
    mapping_id String,
    asset_id String,
    component_id Nullable(String),
    tag_id String,
    tag_name String,
    source_system String,
    measurement_point Nullable(String),
    metric_name String,
    signal_type Nullable(String),
    unit Nullable(String),
    expected_unit Nullable(String),
    sampling_rate_seconds Nullable(Float64),
    normal_range_min Nullable(Float64),
    normal_range_max Nullable(Float64),
    calibration_status Nullable(String),
    mapping_status String,
    valid_from DateTime,
    valid_to Nullable(DateTime),
    created_at DateTime,
    updated_at DateTime
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (tenant_id, mapping_id)
PARTITION BY tuple();

CREATE TABLE IF NOT EXISTS dim_apm_workorder (
    tenant_id String,
    work_order_id String,
    asset_id String,
    component_id Nullable(String),
    source_system Nullable(String),
    external_work_order_id Nullable(String),
    work_order_type String,
    work_order_status String,
    priority Nullable(String),
    title Nullable(String),
    description Nullable(String),
    problem_code Nullable(String),
    cause_code Nullable(String),
    remedy_code Nullable(String),
    requested_at Nullable(DateTime),
    planned_start_at Nullable(DateTime),
    planned_end_at Nullable(DateTime),
    actual_start_at Nullable(DateTime),
    completed_at Nullable(DateTime),
    downtime_minutes Nullable(Float64),
    technician_notes Nullable(String),
    failure_mode_id Nullable(Int64),
    related_anomaly_id Nullable(String),
    related_tag_id Nullable(String),
    created_at DateTime,
    updated_at DateTime
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (tenant_id, work_order_id)
PARTITION BY tuple();

CREATE TABLE IF NOT EXISTS dim_apm_maintenance (
    tenant_id String,
    maintenance_id String,
    asset_id String,
    component_id Nullable(String),
    work_order_id Nullable(String),
    maintenance_type String,
    maintenance_status String,
    maintenance_reason Nullable(String),
    maintenance_result Nullable(String),
    failure_mode_id Nullable(Int64),
    suspected_failure_mode Nullable(String),
    confirmed_fault_description Nullable(String),
    performed_by Nullable(String),
    performed_at DateTime,
    completed_at Nullable(DateTime),
    parts_used Nullable(String),
    labor_hours Nullable(Float64),
    downtime_minutes Nullable(Float64),
    related_tag_id Nullable(String),
    related_anomaly_id Nullable(String),
    sensor_anomaly_related Nullable(Bool),
    evidence_notes Nullable(String),
    created_at DateTime,
    updated_at DateTime
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (tenant_id, maintenance_id)
PARTITION BY tuple();

CREATE TABLE IF NOT EXISTS dim_apm_failuredomain (
    tenant_id String,
    id Int64,
    yaml_id Nullable(String),
    code String,
    label String,
    description Nullable(String),
    active Bool,
    created_by_id Nullable(Int64),
    modified_by_id Nullable(Int64),
    created_date DateTime,
    modified_date DateTime
)
ENGINE = ReplacingMergeTree(modified_date)
ORDER BY (tenant_id, id)
PARTITION BY tuple();

CREATE TABLE IF NOT EXISTS dim_apm_failureclass (
    tenant_id String,
    id Int64,
    domain_id Int64,
    yaml_id Nullable(String),
    code String,
    label String,
    description Nullable(String),
    active Bool,
    created_by_id Nullable(Int64),
    modified_by_id Nullable(Int64),
    created_date DateTime,
    modified_date DateTime
)
ENGINE = ReplacingMergeTree(modified_date)
ORDER BY (tenant_id, id)
PARTITION BY tuple();

CREATE TABLE IF NOT EXISTS dim_apm_failuremode (
    tenant_id String,
    id Int64,
    failure_class_id Int64,
    yaml_id Nullable(String),
    code String,
    label String,
    description Nullable(String),
    asset_type Nullable(String),
    component_type Nullable(String),
    typical_effect Nullable(String),
    related_metrics Nullable(String),
    active Bool,
    created_by_id Nullable(Int64),
    modified_by_id Nullable(Int64),
    created_date DateTime,
    modified_date DateTime
)
ENGINE = ReplacingMergeTree(modified_date)
ORDER BY (tenant_id, id)
PARTITION BY tuple();

CREATE TABLE IF NOT EXISTS dim_apm_failureevent (
    tenant_id String,
    failure_event_id String,
    asset_id String,
    component_id Nullable(String),
    failure_mode_id Nullable(Int64),
    event_status String,
    severity Nullable(String),
    occurred_at Nullable(DateTime),
    detected_at DateTime,
    resolved_at Nullable(DateTime),
    source_system Nullable(String),
    source_event_type Nullable(String),
    source_event_id Nullable(String),
    work_order_id Nullable(String),
    maintenance_id Nullable(String),
    related_anomaly_id Nullable(String),
    related_tag_ids Nullable(String),
    evidence_summary Nullable(String),
    label_confidence Nullable(Float64),
    created_at DateTime,
    updated_at DateTime
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (tenant_id, failure_event_id)
PARTITION BY tuple();

CREATE TABLE IF NOT EXISTS dim_apm_anomaly_rule (
    tenant_id String,
    rule_id String,
    rule_name String,
    method String,
    asset_type Nullable(String),
    component_type Nullable(String),
    metric_name String,
    unit Nullable(String),
    severity String,
    parameters String,
    failure_mode_id Nullable(Int64),
    approval_status String,
    status String,
    owner Nullable(String),
    version String,
    effective_from DateTime,
    effective_to Nullable(DateTime),
    created_at DateTime,
    updated_at DateTime,
    created_by Nullable(String),
    updated_by Nullable(String)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (tenant_id, rule_id)
PARTITION BY tuple();

CREATE TABLE IF NOT EXISTS dim_apm_threshold_profiles (
    tenant_id String,
    profile_id String,
    rule_id String,
    profile_name Nullable(String),
    asset_type Nullable(String),
    component_type Nullable(String),
    metric_name String,
    unit Nullable(String),
    normal_range_min Nullable(Float64),
    normal_range_max Nullable(Float64),
    warning_min Nullable(Float64),
    warning_max Nullable(Float64),
    critical_min Nullable(Float64),
    critical_max Nullable(Float64),
    parameters Nullable(String),
    status String,
    version String,
    effective_from DateTime,
    effective_to Nullable(DateTime),
    created_at DateTime,
    updated_at DateTime
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (tenant_id, profile_id)
PARTITION BY tuple();

CREATE TABLE IF NOT EXISTS dim_apm_tenants (
    tenant_id String,
    tenant_name String,
    region Nullable(String),
    tenant_status String,
    created_at DateTime,
    updated_at DateTime
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (tenant_id)
PARTITION BY tuple();

-- ============================================================
-- DWD LAYER: Detail Data Layer (streaming append data)
-- Engine: MergeTree for high-frequency append-only ingestion
-- Partitioned by toYYYYMM(time_column) for efficient time-range queries
-- ORDER BY designed for Grafana filter patterns: tenant, asset/tag, time, id
-- ============================================================

-- dwd_apm_realtime_signal_readings: curated sensor readings (high-frequency)
-- Grain: one row per (tenant_id, tag_id, event_time, event_id)
-- Source: Kafka via ClickHouse materialized view
-- Engine: MergeTree — append-only streaming data
-- ORDER BY: tenant_id, asset_id, tag_id, event_time (optimized for Grafana filters)
CREATE TABLE IF NOT EXISTS dwd_apm_realtime_signal_readings (
    event_id String NOT NULL,
    tenant_id String NOT NULL,
    tag_id String NOT NULL,
    event_time DateTime NOT NULL,
    schema_version String NOT NULL,
    plant_id String,
    asset_id String,
    asset_name String,
    component_id String,
    operating_mode String,
    tag_name String,
    metric_name String,
    threshold_profile_id String,
    ingest_time DateTime,
    value Float64,
    unit String,
    source_system String,
    quality_flags String,
    scenario String,
    source_topic String,
    load_time DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (tenant_id, asset_id, tag_id, event_time, event_id)
PARTITION BY toYYYYMM(event_time);

-- dwd_apm_anomaly_events: anomaly detection results from Flink
-- Grain: one row per (anomaly_id)
-- Source: Kafka via ClickHouse materialized view
-- Engine: MergeTree — append-only
CREATE TABLE IF NOT EXISTS dwd_apm_anomaly_events (
    anomaly_id String NOT NULL,
    tenant_id String NOT NULL,
    window_start DateTime NOT NULL,
    schema_version String NOT NULL,
    rule_id String,
    plant_id String,
    asset_id String,
    asset_name String,
    tag_id String,
    tag_name String,
    component_id String,
    quality_event_ids String,
    fault_type String,
    metric_name String,
    observed_value Float64,
    baseline_value Float64,
    expected_value Float64,
    event_count Int32,
    breach_count Int32,
    window_end DateTime NOT NULL,
    severity String,
    quality_flags String,
    detection_confidence Float64,
    source_event_ids String,
    source_topic String,
    load_time DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (tenant_id, asset_id, anomaly_id, window_start)
PARTITION BY toYYYYMM(window_start);

-- dwd_apm_late_sensor_readings: late-arriving sensor events
-- Grain: one row per (event_id, tenant_id)
-- Source: Kafka via ClickHouse materialized view
-- Engine: MergeTree — append-only
CREATE TABLE IF NOT EXISTS dwd_apm_late_sensor_readings (
    event_id String NOT NULL,
    tenant_id String NOT NULL,
    event_time DateTime NOT NULL,
    schema_version String,
    plant_id String,
    asset_id String,
    tag_id String,
    metric_name String,
    ingest_time DateTime,
    watermark_time DateTime,
    lateness_minutes Float64,
    reason String,
    quality_flags String,
    source_topic String,
    load_time DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (tenant_id, tag_id, event_time, event_id)
PARTITION BY toYYYYMM(event_time);

-- dwd_apm_stream_health_snapshots: per-job stream health snapshots from Flink
-- Grain: one row per (job_name, tenant_id, observed_at)
-- Source: Kafka via ClickHouse materialized view
-- Engine: MergeTree — append-only
CREATE TABLE IF NOT EXISTS dwd_apm_stream_health_snapshots (
    job_name String NOT NULL,
    tenant_id String NOT NULL,
    observed_at DateTime NOT NULL,
    schema_version String,
    watermark_lag_seconds Float64,
    late_event_count Int32,
    checkpoint_status String,
    source_topic String,
    load_time DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (tenant_id, job_name, observed_at)
PARTITION BY toYYYYMM(observed_at);

-- dwd_apm_realtime_diagnosis_events: LLM-assisted diagnosis events from Flink Agent
-- Grain: one row per (diagnosis_id, tenant_id)
-- Source: Kafka via ClickHouse materialized view
-- Engine: MergeTree — append-only, possible duplicates handled by ReplacingMergeTree
CREATE TABLE IF NOT EXISTS dwd_apm_realtime_diagnosis_events (
    diagnosis_id String NOT NULL,
    tenant_id String NOT NULL,
    anomaly_id String,
    asset_id String,
    component_id String,
    risk_level String,
    escalation_required Bool DEFAULT false,
    confidence Float64,
    quality_caveats String,
    created_at DateTime NOT NULL,
    source_topic String,
    load_time DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(load_time)
ORDER BY (tenant_id, asset_id, diagnosis_id)
PARTITION BY toYYYYMM(created_at);

-- dwd_apm_dlq_events: dead-letter queue events from stream processing
-- Grain: one row per (dlq_event_id, tenant_id)
-- Source: Kafka via ClickHouse materialized view
-- Engine: MergeTree — append-only
CREATE TABLE IF NOT EXISTS dwd_apm_dlq_events (
    dlq_event_id String NOT NULL,
    tenant_id String NOT NULL,
    event_time DateTime NOT NULL,
    raw_payload String,
    original_topic String,
    error_type String,
    error_message String,
    source_topic String,
    load_time DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (tenant_id, dlq_event_id, event_time)
PARTITION BY toYYYYMM(event_time);

-- ============================================================
-- ADS LAYER: Application Data Service (Grafana Serving Model)
-- Engine: ReplacingMergeTree(load_time) — periodically refreshed
-- Partitioned by snapshot/window time column
-- ORDER BY supports Grafana dashboard filter patterns
-- ============================================================

-- ads_apm_asset_condition_snapshot: asset-level condition snapshot (15min aggregation)
-- Grain: one row per (tenant_id, asset_id, snapshot_time)
-- Source: ads_refresh SQL
CREATE TABLE IF NOT EXISTS ads_apm_asset_condition_snapshot (
    tenant_id String NOT NULL,
    asset_id String NOT NULL,
    snapshot_time DateTime NOT NULL,
    asset_name String,
    condition_status String NOT NULL,
    critical_anomaly_count_15m Int32 DEFAULT 0,
    warning_anomaly_count_15m Int32 DEFAULT 0,
    high_risk_diagnosis_count_15m Int32 DEFAULT 0,
    stale_tag_count_15m Int32 DEFAULT 0,
    missing_heartbeat_count_15m Int32 DEFAULT 0,
    last_event_time DateTime,
    last_anomaly_time DateTime,
    last_diagnosis_time DateTime,
    load_time DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(load_time)
ORDER BY (tenant_id, asset_id, snapshot_time)
PARTITION BY toYYYYMM(snapshot_time);

-- ads_apm_asset_condition_timeseries_1m: asset condition timeseries at 1-minute granularity
-- Grain: one row per (tenant_id, asset_id, window_start)
-- Source: ads_refresh SQL (fills current gap: no existing Doris INSERT defined)
CREATE TABLE IF NOT EXISTS ads_apm_asset_condition_timeseries_1m (
    tenant_id String NOT NULL,
    asset_id String NOT NULL,
    window_start DateTime NOT NULL,
    window_end DateTime NOT NULL,
    condition_status String,
    critical_count Int32 DEFAULT 0,
    warning_count Int32 DEFAULT 0,
    anomaly_count Int32 DEFAULT 0,
    active_tags Int32 DEFAULT 0,
    stale_tags Int32 DEFAULT 0,
    quality_flags String,
    load_time DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(load_time)
ORDER BY (tenant_id, asset_id, window_start)
PARTITION BY toYYYYMM(window_start);

-- ads_apm_tag_signal_timeseries_1m: per-tag signal aggregation at 1-minute granularity
-- Grain: one row per (tenant_id, asset_id, component_id, tag_id, metric_name, bucket_minute)
-- Source: ads_refresh SQL
CREATE TABLE IF NOT EXISTS ads_apm_tag_signal_timeseries_1m (
    tenant_id String NOT NULL,
    asset_id String NOT NULL,
    component_id String,
    tag_id String NOT NULL,
    metric_name String NOT NULL,
    bucket_minute DateTime NOT NULL,
    avg_value Float64,
    min_value Float64,
    max_value Float64,
    last_value Float64,
    event_count Int32 DEFAULT 0,
    quality_flags String,
    has_late_arrival Bool DEFAULT false,
    has_flatline Bool DEFAULT false,
    has_missing_heartbeat Bool DEFAULT false,
    normal_range_min Float64,
    normal_range_max Float64,
    load_time DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(load_time)
ORDER BY (tenant_id, asset_id, component_id, tag_id, metric_name, bucket_minute)
PARTITION BY toYYYYMM(bucket_minute);

-- ads_apm_anomaly_event_realtime: enriched anomaly events with DIM context
-- Grain: one row per (anomaly_id)
-- Source: ads_refresh SQL
CREATE TABLE IF NOT EXISTS ads_apm_anomaly_event_realtime (
    anomaly_id String NOT NULL,
    tenant_id String NOT NULL,
    window_start DateTime NOT NULL,
    window_end DateTime,
    asset_id String,
    asset_name String,
    component_type String,
    tag_id String,
    metric_name String,
    rule_id String,
    severity String,
    observed_value Float64,
    expected_value Float64,
    detection_confidence Float64,
    quality_flags String,
    failure_mode_id String,
    failure_mode_label String,
    has_related_workorder Bool DEFAULT false,
    has_related_maintenance Bool DEFAULT false,
    load_time DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(load_time)
ORDER BY (tenant_id, asset_id, anomaly_id, window_start)
PARTITION BY toYYYYMM(window_start);

-- ads_apm_diagnosis_kpi_snapshot: LLM-assisted diagnosis KPI snapshot (last 24h)
-- Grain: one row per (tenant_id, asset_id, snapshot_time)
-- Source: ads_refresh SQL
CREATE TABLE IF NOT EXISTS ads_apm_diagnosis_kpi_snapshot (
    tenant_id String NOT NULL,
    asset_id String NOT NULL,
    snapshot_time DateTime NOT NULL,
    diagnosis_count_24h Int32 DEFAULT 0,
    high_risk_diagnosis_count_24h Int32 DEFAULT 0,
    critical_risk_diagnosis_count_24h Int32 DEFAULT 0,
    escalation_required_count_24h Int32 DEFAULT 0,
    avg_confidence_24h Float64,
    diagnosis_with_quality_caveats_24h Int32 DEFAULT 0,
    diagnosis_linked_to_anomaly_count_24h Int32 DEFAULT 0,
    latest_diagnosis_time DateTime,
    load_time DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(load_time)
ORDER BY (tenant_id, asset_id, snapshot_time)
PARTITION BY toYYYYMM(snapshot_time);

-- ads_apm_stream_health_snapshot: stream health aggregated snapshot
-- Grain: one row per (tenant_id, source_system, pipeline_name, snapshot_time)
-- Source: ads_refresh SQL
CREATE TABLE IF NOT EXISTS ads_apm_stream_health_snapshot (
    tenant_id String NOT NULL,
    source_system String NOT NULL,
    pipeline_name String NOT NULL,
    snapshot_time DateTime NOT NULL,
    message_count_5m Int32 DEFAULT 0,
    late_event_count_5m Int32 DEFAULT 0,
    missing_heartbeat_count Int32 DEFAULT 0,
    dlq_count_5m Int32 DEFAULT 0,
    event_lag_seconds Float64,
    ingest_lag_seconds Float64,
    health_status String,
    load_time DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(load_time)
ORDER BY (tenant_id, source_system, pipeline_name, snapshot_time)
PARTITION BY toYYYYMM(snapshot_time);

-- ads_apm_failure_evidence_summary: failure/workorder/maintenance evidence (last 30d)
-- Grain: one row per (tenant_id, asset_id, component_id, snapshot_time)
-- Source: ads_refresh SQL
CREATE TABLE IF NOT EXISTS ads_apm_failure_evidence_summary (
    tenant_id String NOT NULL,
    asset_id String NOT NULL,
    component_id String NOT NULL,
    snapshot_time DateTime NOT NULL,
    suspected_failure_count Int32 DEFAULT 0,
    confirmed_failure_count Int32 DEFAULT 0,
    open_workorder_count Int32 DEFAULT 0,
    completed_workorder_count Int32 DEFAULT 0,
    maintenance_count Int32 DEFAULT 0,
    sensor_related_maintenance_count Int32 DEFAULT 0,
    latest_failure_time DateTime,
    latest_workorder_time DateTime,
    latest_maintenance_time DateTime,
    load_time DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(load_time)
ORDER BY (tenant_id, asset_id, component_id, snapshot_time)
PARTITION BY toYYYYMM(snapshot_time);

-- ============================================================
-- PHASE 4 TABLES: Serving and Fact Tables
-- Engine: ReplacingMergeTree(load_time) — Python writer inserts
-- ORDER BY natural keys (tenant_id first for tenant isolation)
-- ============================================================

-- serving_apm_triage_evidence: persisted evidence bundles for local triage demos
-- Grain: one row per (evidence_id, tenant_id)
CREATE TABLE IF NOT EXISTS serving_apm_triage_evidence (
    evidence_id String NOT NULL,
    evidence_version String NOT NULL,
    tenant_id String NOT NULL,
    anomaly_id String,
    asset_id String,
    tag_id String,
    metric_name String,
    window_start DateTime,
    window_end DateTime,
    evidence_json String,
    created_at DateTime NOT NULL,
    source_topic String,
    load_time DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(load_time)
ORDER BY (tenant_id, evidence_id)
PARTITION BY tuple();

-- fact_apm_quality_events: data-quality evidence
-- Grain: one row per (quality_event_id, tenant_id)
CREATE TABLE IF NOT EXISTS fact_apm_quality_events (
    quality_event_id String NOT NULL,
    tenant_id String NOT NULL,
    plant_id String,
    asset_id String,
    tag_id String,
    metric_name String,
    check_name String,
    check_status String,
    severity String,
    observed_value Float64,
    expected_value Float64,
    window_start DateTime,
    window_end DateTime,
    evidence_json String,
    created_at DateTime NOT NULL,
    source_topic String,
    load_time DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(load_time)
ORDER BY (tenant_id, quality_event_id)
PARTITION BY tuple();

-- fact_apm_agent_recommendations: LLM or fallback recommendation output
-- Grain: one row per (recommendation_id, tenant_id, anomaly_id)
CREATE TABLE IF NOT EXISTS fact_apm_agent_recommendations (
    recommendation_id String NOT NULL,
    tenant_id String NOT NULL,
    anomaly_id String NOT NULL,
    evidence_version String,
    summary String,
    findings String,
    possible_causes String,
    recommended_checks String,
    runbook_references String,
    confidence_label String,
    quality_caveats String,
    model_name String,
    prompt_version String,
    token_cost Float64,
    latency_ms Int32,
    status String,
    validation_status String,
    quarantine_reason String,
    created_at DateTime NOT NULL,
    load_time DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(load_time)
ORDER BY (tenant_id, anomaly_id, recommendation_id)
PARTITION BY tuple();

-- fact_apm_incidents: incident-level alert groupings
-- Grain: one row per (incident_id, tenant_id, correlation_key)
CREATE TABLE IF NOT EXISTS fact_apm_incidents (
    incident_id String NOT NULL,
    tenant_id String NOT NULL,
    correlation_key String NOT NULL,
    asset_id String,
    metric_name String,
    rule_id String,
    first_seen_at DateTime NOT NULL,
    last_seen_at DateTime NOT NULL,
    anomaly_count Int32,
    max_severity String,
    source_anomaly_ids String,
    status String,
    routing_channel String,
    load_time DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(load_time)
ORDER BY (tenant_id, incident_id)
PARTITION BY tuple();

-- fact_apm_alert_routing: alert routing delivery records
-- Grain: one row per (route_id, incident_id, tenant_id)
CREATE TABLE IF NOT EXISTS fact_apm_alert_routing (
    route_id String NOT NULL,
    incident_id String NOT NULL,
    tenant_id String NOT NULL,
    severity String,
    routing_channel String,
    routing_status String,
    notification_target String,
    error_message String,
    created_at DateTime NOT NULL,
    load_time DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(load_time)
ORDER BY (tenant_id, incident_id, route_id)
PARTITION BY tuple();

-- fact_apm_operator_feedback: operator feedback on alerts/triage
-- Grain: one row per (feedback_id, tenant_id)
CREATE TABLE IF NOT EXISTS fact_apm_operator_feedback (
    feedback_id String NOT NULL,
    tenant_id String NOT NULL,
    incident_id String,
    work_order_id String,
    component_id String,
    anomaly_id String,
    operator_id String,
    is_true_positive Bool,
    severity_correction String,
    explanation_usefulness Int32,
    resolution_note String,
    created_at DateTime NOT NULL,
    load_time DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(load_time)
ORDER BY (tenant_id, feedback_id)
PARTITION BY tuple();

-- fact_apm_llm_invocations: LLM API call audit trail
-- Grain: one row per (invocation_id)
CREATE TABLE IF NOT EXISTS fact_apm_llm_invocations (
    invocation_id String NOT NULL,
    tenant_id String NOT NULL,
    incident_id String,
    anomaly_id String,
    evidence_id String,
    provider String,
    model_name String,
    prompt_version String,
    request_hash String,
    raw_response String,
    parsed_status String,
    validation_status String,
    quarantine_reason String,
    token_cost Float64,
    latency_ms Int32,
    created_at DateTime NOT NULL,
    load_time DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(load_time)
ORDER BY (tenant_id, invocation_id)
PARTITION BY tuple();
