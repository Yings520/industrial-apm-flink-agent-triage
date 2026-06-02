USE industrial_apm;

-- Runtime inspection template. Replace tenant_northwind before execution.
-- ClickHouse equivalent to Doris inspection.sql.tpl
-- Covers all DIM / DWD / ADS / Serving / Fact tables: row count, time range, tenant filter, system health

-- ============================================================
-- Row Count: DIM tables (12)
-- ============================================================
SELECT 'dim_apm_assets' AS table_name, COUNT(*) AS row_count FROM dim_apm_assets WHERE tenant_id = 'tenant_northwind';
SELECT 'dim_apm_asset_components' AS table_name, COUNT(*) AS row_count FROM dim_apm_asset_components WHERE tenant_id = 'tenant_northwind';
SELECT 'dim_apm_signal_tags_mapping' AS table_name, COUNT(*) AS row_count FROM dim_apm_signal_tags_mapping WHERE tenant_id = 'tenant_northwind';
SELECT 'dim_apm_failuremode' AS table_name, COUNT(*) AS row_count FROM dim_apm_failuremode;
SELECT 'dim_apm_failureevent' AS table_name, COUNT(*) AS row_count FROM dim_apm_failureevent WHERE tenant_id = 'tenant_northwind';
SELECT 'dim_apm_workorder' AS table_name, COUNT(*) AS row_count FROM dim_apm_workorder WHERE tenant_id = 'tenant_northwind';
SELECT 'dim_apm_maintenance' AS table_name, COUNT(*) AS row_count FROM dim_apm_maintenance WHERE tenant_id = 'tenant_northwind';
SELECT 'dim_apm_metrics' AS table_name, COUNT(*) AS row_count FROM dim_apm_metrics;
SELECT 'dim_apm_sites' AS table_name, COUNT(*) AS row_count FROM dim_apm_sites WHERE tenant_id = 'tenant_northwind';
SELECT 'dim_apm_threshold_profiles' AS table_name, COUNT(*) AS row_count FROM dim_apm_threshold_profiles WHERE tenant_id = 'tenant_northwind';
SELECT 'dim_apm_measurement_points' AS table_name, COUNT(*) AS row_count FROM dim_apm_measurement_points WHERE tenant_id = 'tenant_northwind';
SELECT 'dim_apm_tenants' AS table_name, COUNT(*) AS row_count FROM dim_apm_tenants WHERE tenant_id = 'tenant_northwind';

-- ============================================================
-- Row Count: DWD tables (6)
-- ============================================================
SELECT 'dwd_apm_realtime_signal_readings' AS table_name, COUNT(*) AS row_count FROM dwd_apm_realtime_signal_readings WHERE tenant_id = 'tenant_northwind';
SELECT 'dwd_apm_anomaly_events' AS table_name, COUNT(*) AS row_count FROM dwd_apm_anomaly_events WHERE tenant_id = 'tenant_northwind';
SELECT 'dwd_apm_late_sensor_readings' AS table_name, COUNT(*) AS row_count FROM dwd_apm_late_sensor_readings WHERE tenant_id = 'tenant_northwind';
SELECT 'dwd_apm_stream_health_snapshots' AS table_name, COUNT(*) AS row_count FROM dwd_apm_stream_health_snapshots WHERE tenant_id = 'tenant_northwind';
SELECT 'dwd_apm_realtime_diagnosis_events' AS table_name, COUNT(*) AS row_count FROM dwd_apm_realtime_diagnosis_events WHERE tenant_id = 'tenant_northwind';
SELECT 'dwd_apm_dlq_events' AS table_name, COUNT(*) AS row_count FROM dwd_apm_dlq_events WHERE tenant_id = 'tenant_northwind';

-- ============================================================
-- Row Count: ADS tables (7)
-- ============================================================
SELECT 'ads_apm_asset_condition_snapshot' AS table_name, COUNT(*) AS row_count FROM ads_apm_asset_condition_snapshot WHERE tenant_id = 'tenant_northwind';
SELECT 'ads_apm_asset_condition_timeseries_1m' AS table_name, COUNT(*) AS row_count FROM ads_apm_asset_condition_timeseries_1m WHERE tenant_id = 'tenant_northwind';
SELECT 'ads_apm_tag_signal_timeseries_1m' AS table_name, COUNT(*) AS row_count FROM ads_apm_tag_signal_timeseries_1m WHERE tenant_id = 'tenant_northwind';
SELECT 'ads_apm_anomaly_event_realtime' AS table_name, COUNT(*) AS row_count FROM ads_apm_anomaly_event_realtime WHERE tenant_id = 'tenant_northwind';
SELECT 'ads_apm_diagnosis_kpi_snapshot' AS table_name, COUNT(*) AS row_count FROM ads_apm_diagnosis_kpi_snapshot WHERE tenant_id = 'tenant_northwind';
SELECT 'ads_apm_stream_health_snapshot' AS table_name, COUNT(*) AS row_count FROM ads_apm_stream_health_snapshot WHERE tenant_id = 'tenant_northwind';
SELECT 'ads_apm_failure_evidence_summary' AS table_name, COUNT(*) AS row_count FROM ads_apm_failure_evidence_summary WHERE tenant_id = 'tenant_northwind';

-- ============================================================
-- Row Count: Serving table (1)
-- ============================================================
SELECT 'serving_apm_triage_evidence' AS table_name, COUNT(*) AS row_count FROM serving_apm_triage_evidence WHERE tenant_id = 'tenant_northwind';

-- ============================================================
-- Row Count: Fact tables (6)
-- ============================================================
SELECT 'fact_apm_quality_events' AS table_name, COUNT(*) AS row_count FROM fact_apm_quality_events WHERE tenant_id = 'tenant_northwind';
SELECT 'fact_apm_agent_recommendations' AS table_name, COUNT(*) AS row_count FROM fact_apm_agent_recommendations WHERE tenant_id = 'tenant_northwind';
SELECT 'fact_apm_incidents' AS table_name, COUNT(*) AS row_count FROM fact_apm_incidents WHERE tenant_id = 'tenant_northwind';
SELECT 'fact_apm_alert_routing' AS table_name, COUNT(*) AS row_count FROM fact_apm_alert_routing WHERE tenant_id = 'tenant_northwind';
SELECT 'fact_apm_operator_feedback' AS table_name, COUNT(*) AS row_count FROM fact_apm_operator_feedback WHERE tenant_id = 'tenant_northwind';
SELECT 'fact_apm_llm_invocations' AS table_name, COUNT(*) AS row_count FROM fact_apm_llm_invocations WHERE tenant_id = 'tenant_northwind';

-- ============================================================
-- Field Completeness: DESCRIBE core tables
-- ============================================================
DESCRIBE TABLE dim_apm_assets;
DESCRIBE TABLE dim_apm_signal_tags_mapping;
DESCRIBE TABLE dwd_apm_realtime_signal_readings;
DESCRIBE TABLE dwd_apm_anomaly_events;
DESCRIBE TABLE dwd_apm_dlq_events;
DESCRIBE TABLE ads_apm_asset_condition_snapshot;
DESCRIBE TABLE ads_apm_tag_signal_timeseries_1m;
DESCRIBE TABLE ads_apm_anomaly_event_realtime;

-- ============================================================
-- Time Range: MIN/MAX per DWD table (6)
-- ============================================================
SELECT 'dwd_apm_realtime_signal_readings' AS table_name, MIN(event_time) AS min_time, MAX(event_time) AS max_time FROM dwd_apm_realtime_signal_readings WHERE tenant_id = 'tenant_northwind';
SELECT 'dwd_apm_anomaly_events' AS table_name, MIN(window_start) AS min_time, MAX(window_start) AS max_time FROM dwd_apm_anomaly_events WHERE tenant_id = 'tenant_northwind';
SELECT 'dwd_apm_late_sensor_readings' AS table_name, MIN(event_time) AS min_time, MAX(event_time) AS max_time FROM dwd_apm_late_sensor_readings WHERE tenant_id = 'tenant_northwind';
SELECT 'dwd_apm_stream_health_snapshots' AS table_name, MIN(observed_at) AS min_time, MAX(observed_at) AS max_time FROM dwd_apm_stream_health_snapshots WHERE tenant_id = 'tenant_northwind';
SELECT 'dwd_apm_realtime_diagnosis_events' AS table_name, MIN(created_at) AS min_time, MAX(created_at) AS max_time FROM dwd_apm_realtime_diagnosis_events WHERE tenant_id = 'tenant_northwind';
SELECT 'dwd_apm_dlq_events' AS table_name, MIN(event_time) AS min_time, MAX(event_time) AS max_time FROM dwd_apm_dlq_events WHERE tenant_id = 'tenant_northwind';

-- ============================================================
-- Time Range: MIN/MAX per ADS table (7)
-- ============================================================
SELECT 'ads_apm_asset_condition_snapshot' AS table_name, MIN(snapshot_time) AS min_time, MAX(snapshot_time) AS max_time FROM ads_apm_asset_condition_snapshot WHERE tenant_id = 'tenant_northwind';
SELECT 'ads_apm_asset_condition_timeseries_1m' AS table_name, MIN(window_start) AS min_time, MAX(window_start) AS max_time FROM ads_apm_asset_condition_timeseries_1m WHERE tenant_id = 'tenant_northwind';
SELECT 'ads_apm_tag_signal_timeseries_1m' AS table_name, MIN(bucket_minute) AS min_time, MAX(bucket_minute) AS max_time FROM ads_apm_tag_signal_timeseries_1m WHERE tenant_id = 'tenant_northwind';
SELECT 'ads_apm_anomaly_event_realtime' AS table_name, MIN(window_start) AS min_time, MAX(window_start) AS max_time FROM ads_apm_anomaly_event_realtime WHERE tenant_id = 'tenant_northwind';
SELECT 'ads_apm_diagnosis_kpi_snapshot' AS table_name, MIN(snapshot_time) AS min_time, MAX(snapshot_time) AS max_time FROM ads_apm_diagnosis_kpi_snapshot WHERE tenant_id = 'tenant_northwind';
SELECT 'ads_apm_stream_health_snapshot' AS table_name, MIN(snapshot_time) AS min_time, MAX(snapshot_time) AS max_time FROM ads_apm_stream_health_snapshot WHERE tenant_id = 'tenant_northwind';
SELECT 'ads_apm_failure_evidence_summary' AS table_name, MIN(snapshot_time) AS min_time, MAX(snapshot_time) AS max_time FROM ads_apm_failure_evidence_summary WHERE tenant_id = 'tenant_northwind';

-- ============================================================
-- Sample Data: core tables with tenant filter
-- ============================================================
SELECT 'dim_apm_assets' AS query, tenant_id, asset_id, asset_name, asset_type, lifecycle_state FROM dim_apm_assets WHERE tenant_id = 'tenant_northwind' LIMIT 5;
SELECT 'dwd_apm_realtime_signal_readings' AS query, tenant_id, event_id, tag_id, metric_name, event_time, value FROM dwd_apm_realtime_signal_readings WHERE tenant_id = 'tenant_northwind' ORDER BY event_time DESC LIMIT 5;
SELECT 'dwd_apm_anomaly_events' AS query, tenant_id, anomaly_id, rule_id, asset_id, tag_id, metric_name, severity, window_start FROM dwd_apm_anomaly_events WHERE tenant_id = 'tenant_northwind' ORDER BY window_start DESC LIMIT 5;
SELECT 'serving_apm_triage_evidence' AS query, tenant_id, evidence_id, anomaly_id, asset_id, metric_name, window_start, created_at FROM serving_apm_triage_evidence WHERE tenant_id = 'tenant_northwind' ORDER BY created_at DESC LIMIT 5;
SELECT 'fact_apm_quality_events' AS query, tenant_id, quality_event_id, asset_id, tag_id, check_name, check_status, severity, created_at FROM fact_apm_quality_events WHERE tenant_id = 'tenant_northwind' ORDER BY created_at DESC LIMIT 5;

-- ============================================================
-- Grafana Panel Mapping Queries
-- ============================================================

-- APM Overview: Asset Condition Distribution
SELECT tenant_id, condition_status, COUNT(*) AS asset_count
FROM ads_apm_asset_condition_snapshot
WHERE tenant_id = 'tenant_northwind'
GROUP BY tenant_id, condition_status;

-- Realtime Signal: Signal Current Value (per tag)
SELECT tenant_id, asset_id, tag_id, metric_name, last_value, bucket_minute
FROM ads_apm_tag_signal_timeseries_1m
WHERE tenant_id = 'tenant_northwind' AND asset_id = 'asset_northwind_mfg_01_0001'
ORDER BY bucket_minute DESC
LIMIT 10;

-- Anomaly Events: Recent anomalies with DIM context
SELECT anomaly_id, tenant_id, asset_id, asset_name, component_type,
       tag_id, metric_name, rule_id, severity, observed_value, expected_value,
       detection_confidence, quality_flags, failure_mode_label,
       has_related_workorder, has_related_maintenance
FROM ads_apm_anomaly_event_realtime
WHERE tenant_id = 'tenant_northwind'
ORDER BY window_start DESC
LIMIT 10;

-- LLM Triage KPI: Diagnosis statistics per asset
SELECT tenant_id, asset_id, diagnosis_count_24h, high_risk_diagnosis_count_24h,
       escalation_required_count_24h, avg_confidence_24h,
       diagnosis_with_quality_caveats_24h, diagnosis_linked_to_anomaly_count_24h,
       latest_diagnosis_time
FROM ads_apm_diagnosis_kpi_snapshot
WHERE tenant_id = 'tenant_northwind'
ORDER BY diagnosis_count_24h DESC
LIMIT 10;

-- Pipeline Health: Stream health status
SELECT tenant_id, source_system, pipeline_name, health_status,
       message_count_5m, late_event_count_5m, dlq_count_5m,
       event_lag_seconds, ingest_lag_seconds
FROM ads_apm_stream_health_snapshot
WHERE tenant_id = 'tenant_northwind'
ORDER BY snapshot_time DESC
LIMIT 10;

-- Failure Evidence: Suspected vs confirmed failures
SELECT tenant_id, asset_id, component_id,
       suspected_failure_count, confirmed_failure_count,
       open_workorder_count, completed_workorder_count,
       maintenance_count, sensor_related_maintenance_count,
       latest_failure_time, latest_workorder_time, latest_maintenance_time
FROM ads_apm_failure_evidence_summary
WHERE tenant_id = 'tenant_northwind'
ORDER BY suspected_failure_count + confirmed_failure_count DESC
LIMIT 10;

-- ============================================================
-- CDC Audit: cdc_op distribution on DIM tables
-- ============================================================
SELECT 'dim_apm_assets' AS dim_table, cdc_op, COUNT(*) AS cnt FROM dim_apm_assets GROUP BY cdc_op;
SELECT 'dim_apm_workorder' AS dim_table, cdc_op, COUNT(*) AS cnt FROM dim_apm_workorder GROUP BY cdc_op;
SELECT 'dim_apm_failuremode' AS dim_table, cdc_op, COUNT(*) AS cnt FROM dim_apm_failuremode GROUP BY cdc_op;

-- ============================================================
-- ClickHouse System Health Checks
-- ============================================================

-- System Tables: Verify all tables exist and check row counts
SELECT
    database,
    name AS table_name,
    engine,
    total_rows,
    formatReadableSize(total_bytes) AS total_size,
    metadata_modification_time
FROM system.tables
WHERE database = 'industrial_apm'
ORDER BY name;

-- System Parts: Latest partition per table (check data freshness)
SELECT
    database,
    table,
    partition,
    rows,
    modification_time
FROM system.parts
WHERE database = 'industrial_apm' AND active
ORDER BY table, modification_time DESC;

-- Kafka Engine Tables: Verify Kafka source tables exist
SELECT
    database,
    name AS table_name,
    engine,
    engine_full,
    total_rows
FROM system.tables
WHERE database = 'industrial_apm' AND engine LIKE '%Kafka%'
ORDER BY name;

-- Materialized Views: Verify MV status
SELECT
    database,
    name AS mv_name,
    engine,
    total_rows
FROM system.tables
WHERE database = 'industrial_apm' AND engine = 'MaterializedView'
ORDER BY name;

-- Table Sizes: Storage overview per table
SELECT
    database,
    table,
    sum(rows) AS rows,
    formatReadableSize(sum(bytes_on_disk)) AS disk_size,
    min(modification_time) AS oldest_part,
    max(modification_time) AS newest_part
FROM system.parts
WHERE database = 'industrial_apm' AND active
GROUP BY database, table
ORDER BY rows DESC;
