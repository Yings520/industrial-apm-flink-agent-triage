CREATE DATABASE IF NOT EXISTS industrial_apm;
USE industrial_apm;

-- dwd_apm_sensor_readings_rt: realtime curated sensor readings
-- Grain: one row per (tenant_id, tag_id, event_time, event_id)
-- Source: tenant_*.staging_apm__sensor_readings.v1 via Routine Load
CREATE TABLE IF NOT EXISTS dwd_apm_sensor_readings_rt (
  event_id VARCHAR(128) NOT NULL,
  tenant_id VARCHAR(128) NOT NULL,
  tag_id VARCHAR(256) NOT NULL,
  event_time DATETIME NOT NULL,
  schema_version VARCHAR(64) NOT NULL,
  plant_id VARCHAR(128),
  asset_id VARCHAR(128),
  asset_name VARCHAR(256),
  component_id VARCHAR(128),
  operating_mode VARCHAR(64),
  tag_name VARCHAR(256),
  metric_name VARCHAR(128),
  threshold_profile_id VARCHAR(128),
  ingest_time DATETIME,
  value DOUBLE,
  unit VARCHAR(32),
  source_system VARCHAR(128),
  quality_flags VARCHAR(1024),
  scenario VARCHAR(128),
  source_topic VARCHAR(256),
  load_time DATETIME DEFAULT CURRENT_TIMESTAMP
)
DUPLICATE KEY(event_id, tenant_id, tag_id, event_time)
DISTRIBUTED BY HASH(tenant_id, tag_id) BUCKETS 8
PROPERTIES ("replication_num" = "1");

-- fact_apm_anomaly_events: anomaly detection results
-- Grain: one row per (anomaly_id) — one anomaly per rule-asset-tag-window
-- Source: tenant_*.mart_apm__fct_anomaly_events.v1 via Routine Load
CREATE TABLE IF NOT EXISTS fact_apm_anomaly_events (
  anomaly_id VARCHAR(256) NOT NULL,
  tenant_id VARCHAR(128) NOT NULL,
  window_start DATETIME NOT NULL,
  schema_version VARCHAR(64) NOT NULL,
  rule_id VARCHAR(128),
  plant_id VARCHAR(128),
  asset_id VARCHAR(128),
  asset_name VARCHAR(256),
  tag_id VARCHAR(256),
  tag_name VARCHAR(256),
  component_id VARCHAR(128),
  quality_event_ids VARCHAR(2048),
  fault_type VARCHAR(64),
  metric_name VARCHAR(128),
  observed_value DOUBLE,
  baseline_value DOUBLE,
  expected_value DOUBLE,
  event_count INT,
  breach_count INT,
  window_end DATETIME NOT NULL,
  severity VARCHAR(32),
  quality_flags VARCHAR(1024),
  detection_confidence DOUBLE,
  source_event_ids VARCHAR(2048),
  source_topic VARCHAR(256),
  load_time DATETIME DEFAULT CURRENT_TIMESTAMP
)
DUPLICATE KEY(anomaly_id, tenant_id, window_start)
DISTRIBUTED BY HASH(tenant_id, anomaly_id) BUCKETS 8
PROPERTIES ("replication_num" = "1");

-- fact_apm_late_sensor_readings: late-arriving sensor events
-- Grain: one row per (event_id, tenant_id)
-- Source: tenant_*.mart_apm__fct_late_sensor_readings.v1 via Routine Load
CREATE TABLE IF NOT EXISTS fact_apm_late_sensor_readings (
  event_id VARCHAR(128) NOT NULL,
  tenant_id VARCHAR(128) NOT NULL,
  event_time DATETIME NOT NULL,
  schema_version VARCHAR(64),
  plant_id VARCHAR(128),
  asset_id VARCHAR(128),
  tag_id VARCHAR(256),
  metric_name VARCHAR(128),
  ingest_time DATETIME,
  watermark_time DATETIME,
  lateness_minutes DOUBLE,
  reason VARCHAR(256),
  quality_flags VARCHAR(1024),
  source_topic VARCHAR(256),
  load_time DATETIME DEFAULT CURRENT_TIMESTAMP
)
DUPLICATE KEY(event_id, tenant_id, event_time)
DISTRIBUTED BY HASH(tenant_id, tag_id) BUCKETS 8
PROPERTIES ("replication_num" = "1");

-- fact_apm_stream_health_snapshots: per-job stream health snapshots
-- Grain: one row per (job_name, tenant_id, observed_at)
-- Source: tenant_*.mart_apm__fct_stream_health_snapshots.v1 via Routine Load
CREATE TABLE IF NOT EXISTS fact_apm_stream_health_snapshots (
  job_name VARCHAR(128) NOT NULL,
  tenant_id VARCHAR(128) NOT NULL,
  observed_at DATETIME NOT NULL,
  schema_version VARCHAR(64),
  watermark_lag_seconds DOUBLE,
  late_event_count INT,
  checkpoint_status VARCHAR(128),
  source_topic VARCHAR(256),
  load_time DATETIME DEFAULT CURRENT_TIMESTAMP
)
DUPLICATE KEY(job_name, tenant_id, observed_at)
DISTRIBUTED BY HASH(tenant_id, job_name) BUCKETS 4
PROPERTIES ("replication_num" = "1");

-- fact_apm_quality_events: data quality check results
-- Grain: one row per (quality_event_id)
-- Source: Python quality event builder
CREATE TABLE IF NOT EXISTS fact_apm_quality_events (
  quality_event_id VARCHAR(256) NOT NULL,
  tenant_id VARCHAR(128) NOT NULL,
  check_name VARCHAR(128) NOT NULL,
  plant_id VARCHAR(128),
  asset_id VARCHAR(128),
  tag_id VARCHAR(256),
  metric_name VARCHAR(128),
  check_status VARCHAR(64) NOT NULL,
  severity VARCHAR(32),
  observed_value VARCHAR(256),
  expected_value VARCHAR(256),
  window_start DATETIME,
  window_end DATETIME,
  evidence_json VARCHAR(4096),
  created_at DATETIME NOT NULL,
  source_topic VARCHAR(256),
  load_time DATETIME DEFAULT CURRENT_TIMESTAMP
)
DUPLICATE KEY(quality_event_id, tenant_id, check_name)
DISTRIBUTED BY HASH(tenant_id, check_name) BUCKETS 8
PROPERTIES ("replication_num" = "1");

-- serving_apm_triage_evidence: deterministic triage evidence payloads
-- Grain: one row per (evidence_id, tenant_id, anomaly_id)
-- Source: Python triage evidence builder
CREATE TABLE IF NOT EXISTS serving_apm_triage_evidence (
  evidence_id VARCHAR(256) NOT NULL,
  tenant_id VARCHAR(128) NOT NULL,
  anomaly_id VARCHAR(256) NOT NULL,
  evidence_version VARCHAR(64) NOT NULL,
  asset_id VARCHAR(128),
  tag_id VARCHAR(256),
  metric_name VARCHAR(128),
  window_start DATETIME,
  window_end DATETIME,
  evidence_json VARCHAR(8192),
  created_at DATETIME NOT NULL,
  source_topic VARCHAR(256),
  load_time DATETIME DEFAULT CURRENT_TIMESTAMP
)
DUPLICATE KEY(evidence_id, tenant_id, anomaly_id)
DISTRIBUTED BY HASH(tenant_id, anomaly_id) BUCKETS 8
PROPERTIES ("replication_num" = "1");

-- fact_apm_agent_recommendations: LLM or fallback recommendation output
-- Grain: one row per (recommendation_id, tenant_id, anomaly_id)
CREATE TABLE IF NOT EXISTS fact_apm_agent_recommendations (
  recommendation_id VARCHAR(256) NOT NULL,
  tenant_id VARCHAR(128) NOT NULL,
  anomaly_id VARCHAR(256) NOT NULL,
  evidence_version VARCHAR(64),
  summary VARCHAR(2048),
  findings VARCHAR(4096),
  possible_causes VARCHAR(4096),
  recommended_checks VARCHAR(4096),
  runbook_references VARCHAR(2048),
  confidence_label VARCHAR(32),
  quality_caveats VARCHAR(2048),
  model_name VARCHAR(128),
  prompt_version VARCHAR(64),
  token_cost DOUBLE,
  latency_ms INT,
  status VARCHAR(64),
  validation_status VARCHAR(64),
  quarantine_reason VARCHAR(512),
  created_at DATETIME NOT NULL,
  load_time DATETIME DEFAULT CURRENT_TIMESTAMP
)
DUPLICATE KEY(recommendation_id, tenant_id, anomaly_id)
DISTRIBUTED BY HASH(tenant_id, anomaly_id) BUCKETS 8
PROPERTIES ("replication_num" = "1");

-- fact_apm_incidents: incident-level alert groupings
-- Grain: one row per (incident_id, tenant_id, correlation_key)
CREATE TABLE IF NOT EXISTS fact_apm_incidents (
  incident_id VARCHAR(256) NOT NULL,
  tenant_id VARCHAR(128) NOT NULL,
  correlation_key VARCHAR(512) NOT NULL,
  asset_id VARCHAR(128),
  metric_name VARCHAR(128),
  rule_id VARCHAR(128),
  first_seen_at DATETIME NOT NULL,
  last_seen_at DATETIME NOT NULL,
  anomaly_count INT,
  max_severity VARCHAR(32),
  source_anomaly_ids VARCHAR(4096),
  status VARCHAR(64),
  routing_channel VARCHAR(64),
  load_time DATETIME DEFAULT CURRENT_TIMESTAMP
)
DUPLICATE KEY(incident_id, tenant_id, correlation_key)
DISTRIBUTED BY HASH(tenant_id, incident_id) BUCKETS 8
PROPERTIES ("replication_num" = "1");

-- fact_apm_alert_routing: alert routing delivery records
-- Grain: one row per (route_id, incident_id, tenant_id)
CREATE TABLE IF NOT EXISTS fact_apm_alert_routing (
  route_id VARCHAR(256) NOT NULL,
  incident_id VARCHAR(256) NOT NULL,
  tenant_id VARCHAR(128) NOT NULL,
  severity VARCHAR(32),
  routing_channel VARCHAR(64),
  routing_status VARCHAR(64),
  notification_target VARCHAR(512),
  error_message VARCHAR(1024),
  created_at DATETIME NOT NULL,
  load_time DATETIME DEFAULT CURRENT_TIMESTAMP
)
DUPLICATE KEY(route_id, incident_id, tenant_id)
DISTRIBUTED BY HASH(tenant_id, incident_id) BUCKETS 8
PROPERTIES ("replication_num" = "1");

-- fact_apm_operator_feedback: operator feedback on alerts/triage
-- Grain: one row per (feedback_id, tenant_id)
CREATE TABLE IF NOT EXISTS fact_apm_operator_feedback (
  feedback_id VARCHAR(256) NOT NULL,
  tenant_id VARCHAR(128) NOT NULL,
  incident_id VARCHAR(256),
  work_order_id VARCHAR(256),
  component_id VARCHAR(128),
  anomaly_id VARCHAR(256),
  operator_id VARCHAR(128),
  is_true_positive BOOLEAN,
  severity_correction VARCHAR(32),
  explanation_usefulness INT,
  resolution_note VARCHAR(2048),
  created_at DATETIME NOT NULL,
  load_time DATETIME DEFAULT CURRENT_TIMESTAMP
)
DUPLICATE KEY(feedback_id, tenant_id, incident_id)
DISTRIBUTED BY HASH(tenant_id, feedback_id) BUCKETS 8
PROPERTIES ("replication_num" = "1");

-- fact_apm_llm_invocations: LLM API call audit trail
-- Grain: one row per (invocation_id)
CREATE TABLE IF NOT EXISTS fact_apm_llm_invocations (
  invocation_id VARCHAR(256) NOT NULL,
  tenant_id VARCHAR(128) NOT NULL,
  incident_id VARCHAR(256),
  anomaly_id VARCHAR(256),
  evidence_id VARCHAR(256),
  provider VARCHAR(128),
  model_name VARCHAR(128),
  prompt_version VARCHAR(64),
  request_hash VARCHAR(128),
  raw_response VARCHAR(32768),
  parsed_status VARCHAR(64),
  validation_status VARCHAR(64),
  quarantine_reason VARCHAR(1024),
  token_cost DOUBLE,
  latency_ms INT,
  created_at DATETIME NOT NULL,
  load_time DATETIME DEFAULT CURRENT_TIMESTAMP
)
DUPLICATE KEY(invocation_id, tenant_id, incident_id)
DISTRIBUTED BY HASH(tenant_id, invocation_id) BUCKETS 8
PROPERTIES ("replication_num" = "1");

-- ============================================================
-- DIMENSION TABLES
-- ============================================================

-- dim_asset: one row per asset version (SCD Type 2)
-- Grain: one row per (tenant_id, asset_id, valid_from)
CREATE TABLE IF NOT EXISTS dim_asset (
  asset_sk BIGINT NOT NULL AUTO_INCREMENT,
  tenant_id VARCHAR(128) NOT NULL,
  site_id VARCHAR(128),
  plant_id VARCHAR(128),
  area_id VARCHAR(128),
  line_id VARCHAR(128),
  asset_id VARCHAR(128) NOT NULL,
  asset_name VARCHAR(256),
  asset_type VARCHAR(128),
  parent_asset_id VARCHAR(128),
  functional_location VARCHAR(256),
  manufacturer VARCHAR(256),
  model VARCHAR(256),
  serial_number VARCHAR(256),
  commissioned_at DATETIME,
  lifecycle_state VARCHAR(64),
  operating_context_json VARCHAR(4096),
  criticality VARCHAR(32),
  archetype VARCHAR(128),
  threshold_profile_id VARCHAR(128),
  expected_metrics VARCHAR(2048),
  valid_from DATETIME NOT NULL,
  valid_to DATETIME,
  is_current BOOLEAN DEFAULT '1',
  load_time DATETIME DEFAULT CURRENT_TIMESTAMP
)
DUPLICATE KEY(asset_sk, tenant_id, asset_id)
DISTRIBUTED BY HASH(tenant_id, asset_id) BUCKETS 8
PROPERTIES ("replication_num" = "1");

-- dim_component: one row per component version (SCD Type 2)
-- Grain: one row per (tenant_id, component_id, valid_from)
CREATE TABLE IF NOT EXISTS dim_component (
  component_sk BIGINT NOT NULL AUTO_INCREMENT,
  tenant_id VARCHAR(128) NOT NULL,
  asset_id VARCHAR(128) NOT NULL,
  component_id VARCHAR(128) NOT NULL,
  component_type VARCHAR(128),
  component_name VARCHAR(256),
  functional_location VARCHAR(256),
  manufacturer VARCHAR(256),
  model VARCHAR(256),
  serial_number VARCHAR(256),
  commissioned_at DATETIME,
  lifecycle_state VARCHAR(64),
  criticality VARCHAR(32),
  expected_metrics VARCHAR(2048),
  valid_from DATETIME NOT NULL,
  valid_to DATETIME,
  is_current BOOLEAN DEFAULT '1',
  load_time DATETIME DEFAULT CURRENT_TIMESTAMP
)
DUPLICATE KEY(component_sk, tenant_id, component_id)
DISTRIBUTED BY HASH(tenant_id, component_id) BUCKETS 8
PROPERTIES ("replication_num" = "1");

-- dim_sensor_tag: one row per tag version (SCD Type 2)
-- Grain: one row per (tenant_id, tag_id, valid_from)
CREATE TABLE IF NOT EXISTS dim_sensor_tag (
  tag_sk BIGINT NOT NULL AUTO_INCREMENT,
  tenant_id VARCHAR(128) NOT NULL,
  tag_id VARCHAR(256) NOT NULL,
  tag_name VARCHAR(256),
  plant_id VARCHAR(128),
  asset_id VARCHAR(128),
  component_id VARCHAR(128),
  metric_name VARCHAR(128),
  unit VARCHAR(32),
  source_system VARCHAR(128),
  signal_type VARCHAR(64),
  sampling_rate_hz DOUBLE,
  measurement_point VARCHAR(256),
  normal_range_min DOUBLE,
  normal_range_max DOUBLE,
  engineering_limit_min DOUBLE,
  engineering_limit_max DOUBLE,
  calibration_status VARCHAR(32),
  last_calibrated_at DATETIME,
  valid_from DATETIME NOT NULL,
  valid_to DATETIME,
  is_current BOOLEAN DEFAULT '1',
  load_time DATETIME DEFAULT CURRENT_TIMESTAMP
)
DUPLICATE KEY(tag_sk, tenant_id, tag_id)
DISTRIBUTED BY HASH(tenant_id, tag_id) BUCKETS 8
PROPERTIES ("replication_num" = "1");

-- dim_metric: one row per metric definition
-- Grain: one row per (metric_name, unit)
CREATE TABLE IF NOT EXISTS dim_metric (
  metric_name VARCHAR(128) NOT NULL,
  unit VARCHAR(32) NOT NULL,
  description VARCHAR(512),
  category VARCHAR(64)
)
UNIQUE KEY(metric_name, unit)
DISTRIBUTED BY HASH(metric_name, unit) BUCKETS 4
PROPERTIES ("replication_num" = "1");

-- dim_threshold_profile: one row per rule version (SCD Type 2)
-- Grain: one row per (profile_id, version, effective_from)
CREATE TABLE IF NOT EXISTS dim_threshold_profile (
  profile_sk BIGINT NOT NULL AUTO_INCREMENT,
  profile_id VARCHAR(128) NOT NULL,
  tenant_id VARCHAR(128),
  asset_type VARCHAR(128),
  component_type VARCHAR(128),
  metric_name VARCHAR(128),
  unit VARCHAR(32),
  rule_id VARCHAR(128) NOT NULL,
  rule_name VARCHAR(256),
  method VARCHAR(128),
  severity VARCHAR(32),
  parameters_json VARCHAR(4096),
  description VARCHAR(1024),
  version VARCHAR(32) NOT NULL,
  effective_from DATETIME NOT NULL,
  effective_to DATETIME,
  owner VARCHAR(256),
  approval_status VARCHAR(32),
  status VARCHAR(32),
  created_at DATETIME,
  created_by VARCHAR(128),
  is_current BOOLEAN DEFAULT '1',
  load_time DATETIME DEFAULT CURRENT_TIMESTAMP
)
DUPLICATE KEY(profile_sk, profile_id, tenant_id)
DISTRIBUTED BY HASH(profile_id, tenant_id) BUCKETS 8
PROPERTIES ("replication_num" = "1");

-- dim_site: one row per site
-- Grain: one row per (tenant_id, site_id)
CREATE TABLE IF NOT EXISTS dim_site (
  tenant_id VARCHAR(128) NOT NULL,
  site_id VARCHAR(128) NOT NULL,
  site_name VARCHAR(256),
  city VARCHAR(128),
  country VARCHAR(128),
  timezone VARCHAR(64),
  load_time DATETIME DEFAULT CURRENT_TIMESTAMP
)
UNIQUE KEY(tenant_id, site_id)
DISTRIBUTED BY HASH(tenant_id, site_id) BUCKETS 4
PROPERTIES ("replication_num" = "1");

-- dim_failure_mode: FMECA catalog
-- Grain: one row per (failure_mode_id)
CREATE TABLE IF NOT EXISTS dim_failure_mode (
  failure_mode_id VARCHAR(128) NOT NULL,
  asset_type VARCHAR(128),
  component_type VARCHAR(128),
  failure_mode_name VARCHAR(256) NOT NULL,
  failure_effect VARCHAR(2048),
  criticality VARCHAR(32),
  detectability VARCHAR(32),
  related_metrics_json VARCHAR(4096),
  recommended_checks VARCHAR(4096),
  runbook_refs VARCHAR(2048),
  pf_interval_hint_hours DOUBLE,
  maintenance_strategy VARCHAR(64),
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  load_time DATETIME DEFAULT CURRENT_TIMESTAMP
)
UNIQUE KEY(failure_mode_id)
DISTRIBUTED BY HASH(failure_mode_id) BUCKETS 4
PROPERTIES ("replication_num" = "1");

-- dim_work_order: work order and inspection records
-- Grain: one row per (work_order_id)
CREATE TABLE IF NOT EXISTS dim_work_order (
  work_order_id VARCHAR(256) NOT NULL,
  tenant_id VARCHAR(128) NOT NULL,
  asset_id VARCHAR(128),
  component_id VARCHAR(128),
  failure_mode_id VARCHAR(128),
  anomaly_id VARCHAR(256),
  incident_id VARCHAR(256),
  work_type VARCHAR(64),
  priority VARCHAR(32),
  problem_code VARCHAR(128),
  cause_code VARCHAR(128),
  remedy_code VARCHAR(128),
  technician_notes VARCHAR(4096),
  downtime_minutes DOUBLE,
  parts_json VARCHAR(4096),
  inspection_results_json VARCHAR(4096),
  source_system VARCHAR(64),
  created_at DATETIME NOT NULL,
  completed_at DATETIME,
  status VARCHAR(64),
  load_time DATETIME DEFAULT CURRENT_TIMESTAMP
)
UNIQUE KEY(work_order_id, tenant_id)
DISTRIBUTED BY HASH(tenant_id, work_order_id) BUCKETS 8
PROPERTIES ("replication_num" = "1");
