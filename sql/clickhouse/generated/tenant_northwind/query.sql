-- ClickHouse smoke query template. Replace tenant_northwind before execution.
-- Usage: sed "s/\tenant_northwind/tenant_northwind/g" query.sql.tpl | clickhouse-client -mn

-- ============================================================
-- DWD: Row counts for core detail tables
-- ============================================================
SELECT 'dwd_apm_realtime_signal_readings' AS table_name, count() AS row_count FROM dwd_apm_realtime_signal_readings WHERE tenant_id = 'tenant_northwind';
SELECT 'dwd_apm_anomaly_events' AS table_name, count() AS row_count FROM dwd_apm_anomaly_events WHERE tenant_id = 'tenant_northwind';
SELECT 'dwd_apm_late_sensor_readings' AS table_name, count() AS row_count FROM dwd_apm_late_sensor_readings WHERE tenant_id = 'tenant_northwind';
SELECT 'dwd_apm_stream_health_snapshots' AS table_name, count() AS row_count FROM dwd_apm_stream_health_snapshots WHERE tenant_id = 'tenant_northwind';
SELECT 'dwd_apm_realtime_diagnosis_events' AS table_name, count() AS row_count FROM dwd_apm_realtime_diagnosis_events WHERE tenant_id = 'tenant_northwind';

-- ============================================================
-- DWD: Recent rows (last 5) for each core table
-- ============================================================

-- dwd_apm_realtime_signal_readings
SELECT tenant_id, event_id, tag_id, metric_name, event_time, value, unit, quality_flags
FROM dwd_apm_realtime_signal_readings
WHERE tenant_id = 'tenant_northwind'
ORDER BY event_time DESC
LIMIT 5;

-- dwd_apm_anomaly_events
SELECT tenant_id, anomaly_id, rule_id, asset_id, tag_id, metric_name, severity, window_start, window_end, detection_confidence
FROM dwd_apm_anomaly_events
WHERE tenant_id = 'tenant_northwind'
ORDER BY window_start DESC
LIMIT 5;

-- dwd_apm_late_sensor_readings
SELECT tenant_id, event_id, tag_id, metric_name, event_time, ingest_time, lateness_minutes, reason
FROM dwd_apm_late_sensor_readings
WHERE tenant_id = 'tenant_northwind'
ORDER BY event_time DESC
LIMIT 5;

-- dwd_apm_stream_health_snapshots
SELECT tenant_id, job_name, observed_at, watermark_lag_seconds, late_event_count, checkpoint_status
FROM dwd_apm_stream_health_snapshots
WHERE tenant_id = 'tenant_northwind'
ORDER BY observed_at DESC
LIMIT 5;

-- dwd_apm_realtime_diagnosis_events
SELECT tenant_id, diagnosis_id, anomaly_id, asset_id, component_id, risk_level, escalation_required, confidence, quality_caveats, created_at
FROM dwd_apm_realtime_diagnosis_events
WHERE tenant_id = 'tenant_northwind'
ORDER BY created_at DESC
LIMIT 5;

-- ============================================================
-- FACT: Row counts and recent rows for fact tables
-- ============================================================

-- fact_apm_agent_recommendations
SELECT 'fact_apm_agent_recommendations' AS table_name, count() AS row_count FROM fact_apm_agent_recommendations WHERE tenant_id = 'tenant_northwind';
SELECT tenant_id, recommendation_id, anomaly_id, confidence_label, status, validation_status, model_name, latency_ms, created_at
FROM fact_apm_agent_recommendations
WHERE tenant_id = 'tenant_northwind'
ORDER BY created_at DESC
LIMIT 5;

-- fact_apm_incidents
SELECT 'fact_apm_incidents' AS table_name, count() AS row_count FROM fact_apm_incidents WHERE tenant_id = 'tenant_northwind';
SELECT tenant_id, incident_id, correlation_key, asset_id, metric_name, max_severity, anomaly_count, routing_channel, status, last_seen_at
FROM fact_apm_incidents
WHERE tenant_id = 'tenant_northwind'
ORDER BY last_seen_at DESC
LIMIT 5;

-- fact_apm_alert_routing
SELECT 'fact_apm_alert_routing' AS table_name, count() AS row_count FROM fact_apm_alert_routing WHERE tenant_id = 'tenant_northwind';
SELECT tenant_id, route_id, incident_id, severity, routing_channel, routing_status, error_message, created_at
FROM fact_apm_alert_routing
WHERE tenant_id = 'tenant_northwind'
ORDER BY created_at DESC
LIMIT 5;

-- fact_apm_operator_feedback
SELECT 'fact_apm_operator_feedback' AS table_name, count() AS row_count FROM fact_apm_operator_feedback WHERE tenant_id = 'tenant_northwind';
SELECT tenant_id, feedback_id, incident_id, anomaly_id, operator_id, is_true_positive, severity_correction, explanation_usefulness, created_at
FROM fact_apm_operator_feedback
WHERE tenant_id = 'tenant_northwind'
ORDER BY created_at DESC
LIMIT 5;

-- fact_apm_llm_invocations
SELECT 'fact_apm_llm_invocations' AS table_name, count() AS row_count FROM fact_apm_llm_invocations WHERE tenant_id = 'tenant_northwind';
SELECT tenant_id, invocation_id, incident_id, anomaly_id, provider, model_name, prompt_version, parsed_status, validation_status, quarantine_reason, latency_ms, created_at
FROM fact_apm_llm_invocations
WHERE tenant_id = 'tenant_northwind'
ORDER BY created_at DESC
LIMIT 5;

-- ============================================================
-- ADS: Dashboard parity queries (Grafana panel equivalents)
-- ============================================================

-- ads_apm_asset_condition_snapshot: condition_status distribution (Grafana: asset health pie/bar)
SELECT tenant_id, condition_status, count() AS asset_count, snapshot_time
FROM ads_apm_asset_condition_snapshot
WHERE tenant_id = 'tenant_northwind'
GROUP BY tenant_id, condition_status, snapshot_time
ORDER BY snapshot_time DESC
LIMIT 5;

-- ads_apm_tag_signal_timeseries_1m: recent per-tag signal aggregates (Grafana: tag signal timeseries)
SELECT tenant_id, asset_id, component_id, tag_id, metric_name, bucket_minute, avg_value, min_value, max_value, event_count
FROM ads_apm_tag_signal_timeseries_1m
WHERE tenant_id = 'tenant_northwind'
ORDER BY bucket_minute DESC
LIMIT 5;

-- ads_apm_anomaly_event_realtime: recent enriched anomalies (Grafana: anomaly timeline/alerts)
SELECT tenant_id, anomaly_id, asset_id, asset_name, component_type, metric_name, rule_id, severity, observed_value, expected_value, detection_confidence, failure_mode_id, failure_mode_label, window_start
FROM ads_apm_anomaly_event_realtime
WHERE tenant_id = 'tenant_northwind'
ORDER BY window_start DESC
LIMIT 5;

-- ads_apm_diagnosis_kpi_snapshot: diagnosis KPI per asset (Grafana: LLM triage KPI dashboard)
SELECT tenant_id, asset_id, snapshot_time, diagnosis_count_24h, high_risk_diagnosis_count_24h, critical_risk_diagnosis_count_24h, escalation_required_count_24h, avg_confidence_24h, latest_diagnosis_time
FROM ads_apm_diagnosis_kpi_snapshot
WHERE tenant_id = 'tenant_northwind'
ORDER BY snapshot_time DESC
LIMIT 5;

-- ads_apm_stream_health_snapshot: stream health status (Grafana: pipeline health monitor)
SELECT tenant_id, source_system, pipeline_name, snapshot_time, message_count_5m, late_event_count_5m, missing_heartbeat_count, dlq_count_5m, event_lag_seconds, ingest_lag_seconds, health_status
FROM ads_apm_stream_health_snapshot
WHERE tenant_id = 'tenant_northwind'
ORDER BY snapshot_time DESC
LIMIT 5;

-- ads_apm_failure_evidence_summary: evidence counts by asset/component (Grafana: failure evidence overview)
SELECT tenant_id, asset_id, component_id, snapshot_time, suspected_failure_count, confirmed_failure_count, open_workorder_count, completed_workorder_count, maintenance_count, sensor_related_maintenance_count
FROM ads_apm_failure_evidence_summary
WHERE tenant_id = 'tenant_northwind'
ORDER BY snapshot_time DESC
LIMIT 5;

-- ads_apm_failure_evidence_summary: aggregated evidence counts (Grafana: evidence summary KPI)
SELECT tenant_id, sum(suspected_failure_count) AS total_suspected, sum(confirmed_failure_count) AS total_confirmed, sum(open_workorder_count) AS total_open_wo, sum(completed_workorder_count) AS total_completed_wo, sum(maintenance_count) AS total_maintenance
FROM ads_apm_failure_evidence_summary
WHERE tenant_id = 'tenant_northwind'
GROUP BY tenant_id;
