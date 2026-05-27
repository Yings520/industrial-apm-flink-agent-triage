USE industrial_apm;

CREATE VIEW IF NOT EXISTS vw_apm_incident_queue AS
SELECT
  tenant_id,
  anomaly_id,
  rule_id,
  plant_id,
  asset_id,
  asset_name,
  tag_id,
  metric_name,
  severity,
  detection_confidence,
  window_start,
  window_end,
  quality_flags,
  source_event_ids
FROM fact_apm_anomaly_events;

CREATE VIEW IF NOT EXISTS vw_apm_stream_health_current AS
SELECT
  tenant_id,
  job_name,
  MAX(observed_at) AS latest_observed_at,
  MAX(watermark_lag_seconds) AS watermark_lag_seconds,
  SUM(late_event_count) AS late_event_count,
  MAX(checkpoint_status) AS checkpoint_status
FROM fact_apm_stream_health_snapshots
GROUP BY tenant_id, job_name;

CREATE VIEW IF NOT EXISTS vw_apm_tenant_asset_health AS
SELECT
  tenant_id,
  asset_id,
  MAX(asset_name) AS asset_name,
  COUNT(*) AS anomaly_count,
  SUM(CASE WHEN severity = 'critical' THEN 1 ELSE 0 END) AS critical_anomaly_count,
  MAX(window_end) AS latest_anomaly_time
FROM fact_apm_anomaly_events
GROUP BY tenant_id, asset_id;

CREATE VIEW IF NOT EXISTS vw_apm_anomaly_trends AS
SELECT
  tenant_id,
  rule_id,
  severity,
  DATE_TRUNC(window_start, 'hour') AS anomaly_hour,
  COUNT(*) AS anomaly_count
FROM fact_apm_anomaly_events
GROUP BY tenant_id, rule_id, severity, DATE_TRUNC(window_start, 'hour');

CREATE VIEW IF NOT EXISTS vw_apm_late_event_rate AS
SELECT
  tenant_id,
  DATE_TRUNC(event_time, 'hour') AS event_hour,
  COUNT(*) AS late_event_count,
  AVG(lateness_minutes) AS avg_lateness_minutes
FROM fact_apm_late_sensor_readings
GROUP BY tenant_id, DATE_TRUNC(event_time, 'hour');

CREATE VIEW IF NOT EXISTS serving_apm_triage_evidence AS
SELECT
  a.tenant_id,
  a.anomaly_id,
  a.asset_id,
  a.tag_id,
  a.metric_name,
  a.window_start,
  a.window_end,
  a.observed_value,
  a.baseline_value,
  a.expected_value,
  a.quality_flags,
  a.source_event_ids,
  h.watermark_lag_seconds,
  h.late_event_count,
  h.checkpoint_status,
  COUNT(q.quality_event_id) AS quality_event_count
FROM fact_apm_anomaly_events a
LEFT JOIN vw_apm_stream_health_current h
  ON a.tenant_id = h.tenant_id
LEFT JOIN fact_apm_quality_events q
  ON a.tenant_id = q.tenant_id
  AND a.asset_id = q.asset_id
  AND a.tag_id = q.tag_id
GROUP BY
  a.tenant_id,
  a.anomaly_id,
  a.asset_id,
  a.tag_id,
  a.metric_name,
  a.window_start,
  a.window_end,
  a.observed_value,
  a.baseline_value,
  a.expected_value,
  a.quality_flags,
  a.source_event_ids,
  h.watermark_lag_seconds,
  h.late_event_count,
  h.checkpoint_status;
