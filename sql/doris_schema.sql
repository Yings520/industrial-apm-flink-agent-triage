CREATE DATABASE IF NOT EXISTS industrial_apm;
USE industrial_apm;

CREATE TABLE IF NOT EXISTS dwd_apm_sensor_readings_rt (
  event_id VARCHAR(128) NOT NULL,
  tenant_id VARCHAR(128) NOT NULL,
  tag_id VARCHAR(256) NOT NULL,
  event_time DATETIME NOT NULL,
  schema_version VARCHAR(64) NOT NULL,
  plant_id VARCHAR(128),
  asset_id VARCHAR(128),
  asset_name VARCHAR(256),
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
