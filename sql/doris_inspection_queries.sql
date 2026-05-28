USE industrial_apm;

SELECT 'dwd_apm_sensor_readings_rt' AS table_name, COUNT(*) AS row_count FROM dwd_apm_sensor_readings_rt;
SELECT 'fact_apm_anomaly_events' AS table_name, COUNT(*) AS row_count FROM fact_apm_anomaly_events;
SELECT 'fact_apm_late_sensor_readings' AS table_name, COUNT(*) AS row_count FROM fact_apm_late_sensor_readings;
SELECT 'fact_apm_stream_health_snapshots' AS table_name, COUNT(*) AS row_count FROM fact_apm_stream_health_snapshots;
SELECT 'fact_apm_quality_events' AS table_name, COUNT(*) AS row_count FROM fact_apm_quality_events;
SELECT 'serving_apm_triage_evidence' AS table_name, COUNT(*) AS row_count FROM serving_apm_triage_evidence;
SELECT 'fact_apm_agent_recommendations' AS table_name, COUNT(*) AS row_count FROM fact_apm_agent_recommendations;

SELECT tenant_id, event_id, tag_id, metric_name, event_time, value
FROM dwd_apm_sensor_readings_rt
ORDER BY event_time DESC
LIMIT 5;

SELECT tenant_id, anomaly_id, rule_id, asset_id, tag_id, metric_name, severity, window_start, window_end
FROM fact_apm_anomaly_events
ORDER BY window_start DESC
LIMIT 5;

SELECT tenant_id, event_id, tag_id, metric_name, event_time, lateness_minutes, reason
FROM fact_apm_late_sensor_readings
ORDER BY event_time DESC
LIMIT 5;

SELECT tenant_id, job_name, observed_at, late_event_count, checkpoint_status
FROM fact_apm_stream_health_snapshots
ORDER BY observed_at DESC
LIMIT 5;

SELECT tenant_id, anomaly_id, evidence_version, created_at
FROM serving_apm_triage_evidence
ORDER BY created_at DESC
LIMIT 5;

SELECT tenant_id, anomaly_id, confidence_label, status, validation_status
FROM fact_apm_agent_recommendations
ORDER BY created_at DESC
LIMIT 5;
