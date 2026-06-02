-- ============================================================
-- ClickHouse ADS Refresh Template
-- Runtime INSERT template. Replace ${tenant_id} before execution.
-- Each INSERT refreshes one tenant slice in shared multi-tenant ADS tables.
-- All queries are all-tenant-safe (GROUP BY tenant_id).
-- Templates accept a tenant_id variable but still GROUP BY tenant_id.
-- Removing WHERE tenant_id = '${tenant_id}' enables all-tenant execution.
-- ============================================================
-- Engine: ReplacingMergeTree(load_time) for all ADS tables
-- ReplacingMergeTree DIM tables use subquery dedup pattern:
--   (SELECT ... FROM dim_table WHERE is_current = 1
--    ORDER BY cdc_updated_at DESC LIMIT 1 BY tenant_id, natural_key)
-- ============================================================

-- ============================================================
-- S3.1: ads_apm_asset_condition_snapshot  (CH-009)
-- Asset-level condition snapshot (last 15 min aggregation)
-- Grain: one row per (tenant_id, asset_id, snapshot_time)
-- Condition priority: critical > warning > unknown > healthy
-- ============================================================

INSERT INTO ads_apm_asset_condition_snapshot
WITH
base_assets AS (
    SELECT DISTINCT tenant_id, asset_id FROM dwd_apm_anomaly_events
    WHERE window_end >= now() - INTERVAL 15 MINUTE
    UNION
    SELECT DISTINCT tenant_id, asset_id FROM dwd_apm_realtime_diagnosis_events
    WHERE created_at >= now() - INTERVAL 15 MINUTE
    UNION
    SELECT DISTINCT tenant_id, asset_id FROM dwd_apm_realtime_signal_readings
    WHERE event_time >= now() - INTERVAL 15 MINUTE
),
anomaly_agg AS (
    SELECT tenant_id, asset_id,
           uniqExactIf(anomaly_id, severity = 'critical') AS critical_anomaly_count_15m,
           uniqExactIf(anomaly_id, severity = 'warning')  AS warning_anomaly_count_15m,
           max(window_end) AS last_anomaly_time
    FROM dwd_apm_anomaly_events
    WHERE window_end >= now() - INTERVAL 15 MINUTE
    GROUP BY tenant_id, asset_id
),
diagnosis_agg AS (
    SELECT tenant_id, asset_id,
           uniqExactIf(diagnosis_id, risk_level IN ('critical', 'high')) AS high_risk_diagnosis_count_15m,
           max(created_at) AS last_diagnosis_time
    FROM dwd_apm_realtime_diagnosis_events
    WHERE created_at >= now() - INTERVAL 15 MINUTE
    GROUP BY tenant_id, asset_id
),
signal_agg AS (
    SELECT tenant_id, asset_id,
           max(event_time) AS last_event_time
    FROM dwd_apm_realtime_signal_readings
    WHERE event_time >= now() - INTERVAL 15 MINUTE
    GROUP BY tenant_id, asset_id
),
active_tags AS (
    SELECT DISTINCT tenant_id, asset_id, tag_id
    FROM dwd_apm_realtime_signal_readings
    WHERE event_time >= now() - INTERVAL 15 MINUTE
),
expected_tag_count AS (
    SELECT tenant_id, asset_id, count(DISTINCT tag_id) AS total_expected_tags
    FROM (SELECT tenant_id, asset_id, tag_id FROM dim_apm_signal_tags_mapping WHERE is_current = 1 ORDER BY cdc_updated_at DESC LIMIT 1 BY tenant_id, tag_id)
    GROUP BY tenant_id, asset_id
),
active_tag_count AS (
    SELECT tenant_id, asset_id, count(DISTINCT tag_id) AS active_tags
    FROM active_tags
    GROUP BY tenant_id, asset_id
),
stale_calc AS (
    SELECT COALESCE(e.tenant_id, a.tenant_id) AS tenant_id,
           COALESCE(e.asset_id, a.asset_id)   AS asset_id,
           COALESCE(e.total_expected_tags, 0) AS total_expected,
           COALESCE(a.active_tags, 0)         AS active_count
    FROM expected_tag_count e
    FULL OUTER JOIN active_tag_count a
      ON e.tenant_id = a.tenant_id AND e.asset_id = a.asset_id
),
heartbeat_agg AS (
    SELECT tenant_id,
           uniqExactIf(job_name, late_event_count > 0) AS missing_heartbeat_count_15m
    FROM dwd_apm_stream_health_snapshots
    WHERE observed_at >= now() - INTERVAL 15 MINUTE
    GROUP BY tenant_id
),
asset_dim AS (
    SELECT tenant_id, asset_id, asset_name
    FROM (SELECT tenant_id, asset_id, asset_name FROM dim_apm_assets WHERE is_current = 1 ORDER BY cdc_updated_at DESC LIMIT 1 BY tenant_id, asset_id)
)
SELECT
    b.tenant_id,
    b.asset_id,
    now()                                                           AS snapshot_time,
    COALESCE(d.asset_name, '')                                      AS asset_name,
    multiIf(
        COALESCE(a.critical_anomaly_count_15m, 0) > 0, 'critical',
        COALESCE(a.warning_anomaly_count_15m, 0)   > 0, 'warning',
        COALESCE(s.stale_count, 0)                  > 0, 'unknown',
        'healthy'
    )                                                               AS condition_status,
    COALESCE(a.critical_anomaly_count_15m, 0)                       AS critical_anomaly_count_15m,
    COALESCE(a.warning_anomaly_count_15m, 0)                        AS warning_anomaly_count_15m,
    COALESCE(diag.high_risk_diagnosis_count_15m, 0)                 AS high_risk_diagnosis_count_15m,
    COALESCE(s.stale_count, 0)                                      AS stale_tag_count_15m,
    COALESCE(hb.missing_heartbeat_count_15m, 0)                     AS missing_heartbeat_count_15m,
    sig.last_event_time                                             AS last_event_time,
    a.last_anomaly_time                                             AS last_anomaly_time,
    diag.last_diagnosis_time                                        AS last_diagnosis_time,
    now()                                                           AS load_time
FROM base_assets b
LEFT JOIN anomaly_agg a
    ON b.tenant_id = a.tenant_id AND b.asset_id = a.asset_id
LEFT JOIN diagnosis_agg diag
    ON b.tenant_id = diag.tenant_id AND b.asset_id = diag.asset_id
LEFT JOIN signal_agg sig
    ON b.tenant_id = sig.tenant_id AND b.asset_id = sig.asset_id
LEFT JOIN (
    SELECT tenant_id, asset_id,
           greatest(total_expected - active_count, 0) AS stale_count
    FROM stale_calc
) s
    ON b.tenant_id = s.tenant_id AND b.asset_id = s.asset_id
LEFT JOIN heartbeat_agg hb
    ON b.tenant_id = hb.tenant_id
LEFT JOIN asset_dim d
    ON b.tenant_id = d.tenant_id AND b.asset_id = d.asset_id
WHERE b.tenant_id = '${tenant_id}'
  AND b.asset_id IS NOT NULL
  AND b.asset_id <> '';


-- ============================================================
-- S3.2: ads_apm_tag_signal_timeseries_1m  (CH-010)
-- Per-tag signal aggregation at 1-minute granularity
-- Grain: one row per (tenant_id, asset_id, component_id, tag_id, metric_name, bucket_minute)
-- ============================================================

INSERT INTO ads_apm_tag_signal_timeseries_1m
WITH
signal_buckets AS (
    SELECT
        tenant_id,
        asset_id,
        component_id,
        tag_id,
        metric_name,
        toStartOfMinute(event_time)     AS bucket_minute,
        avg(value)                      AS avg_value,
        min(value)                      AS min_value,
        max(value)                      AS max_value,
        argMax(value, event_time)       AS last_value,
        count()                         AS event_count,
        arrayStringConcat(groupUniqArray(quality_flags), '|') AS quality_flags_concat,
        min(event_time)                 AS first_event_in_bucket,
        max(event_time)                 AS last_event_in_bucket,
        min(ingest_time)                AS first_ingest_time,
        max(event_time) - min(event_time) AS event_time_span_seconds
    FROM dwd_apm_realtime_signal_readings
    WHERE tenant_id = '${tenant_id}' AND event_time >= now() - INTERVAL 1 HOUR
    GROUP BY tenant_id, asset_id, component_id, tag_id, metric_name, toStartOfMinute(event_time)
),
tag_dim AS (
    SELECT
        tenant_id,
        tag_id,
        normal_range_min,
        normal_range_max,
        sampling_rate_hz
    FROM (SELECT tenant_id, tag_id, normal_range_min, normal_range_max, sampling_rate_hz
          FROM dim_apm_signal_tags_mapping WHERE is_current = 1
          ORDER BY cdc_updated_at DESC LIMIT 1 BY tenant_id, tag_id)
)
SELECT
    s.tenant_id,
    s.asset_id,
    s.component_id,
    s.tag_id,
    s.metric_name,
    s.bucket_minute,
    s.avg_value,
    s.min_value,
    s.max_value,
    s.last_value,
    s.event_count,
    s.quality_flags_concat                          AS quality_flags,
    if(like(s.quality_flags_concat, '%late%'), true, false) AS has_late_arrival,
    if(s.min_value = s.max_value AND s.event_count > 1, true, false) AS has_flatline,
    multiIf(
        td.sampling_rate_hz IS NOT NULL AND td.sampling_rate_hz > 0
          AND s.event_count < (td.sampling_rate_hz * 60 * 0.5), true,
        s.event_count = 0, true,
        false
    )                                               AS has_missing_heartbeat,
    td.normal_range_min,
    td.normal_range_max,
    now()                                           AS load_time
FROM signal_buckets s
LEFT JOIN tag_dim td
    ON s.tenant_id = td.tenant_id AND s.tag_id = td.tag_id
WHERE s.tenant_id = '${tenant_id}';


-- ============================================================
-- S3.3: ads_apm_anomaly_event_realtime  (CH-011)
-- Enriched anomaly events with DIM context (asset name, component type, failure mode labels)
-- Grain: one row per (anomaly_id)
-- ============================================================

INSERT INTO ads_apm_anomaly_event_realtime
(
    anomaly_id,
    tenant_id,
    window_start,
    window_end,
    asset_id,
    asset_name,
    component_type,
    tag_id,
    metric_name,
    rule_id,
    severity,
    observed_value,
    expected_value,
    detection_confidence,
    quality_flags,
    failure_mode_id,
    failure_mode_label,
    has_related_workorder,
    has_related_maintenance,
    load_time
)
SELECT
    a.anomaly_id,
    a.tenant_id,
    a.window_start,
    a.window_end,
    a.asset_id,
    COALESCE(ast.asset_name, a.asset_name)           AS asset_name,
    comp.component_type,
    a.tag_id,
    a.metric_name,
    a.rule_id,
    a.severity,
    a.observed_value,
    a.expected_value,
    a.detection_confidence,
    a.quality_flags,
    a.fault_type                                      AS failure_mode_id,
    COALESCE(fm.failure_mode_name, '')                AS failure_mode_label,
    if(
        (SELECT count() > 0 FROM dim_apm_workorder FINAL
         WHERE tenant_id = a.tenant_id AND asset_id = a.asset_id),
        true, false
    )                                                 AS has_related_workorder,
    if(
        (SELECT count() > 0 FROM dim_apm_maintenance FINAL
         WHERE tenant_id = a.tenant_id AND asset_id = a.asset_id),
        true, false
    )                                                 AS has_related_maintenance,
    now()                                             AS load_time
FROM dwd_apm_anomaly_events a
LEFT JOIN (SELECT tenant_id, asset_id, asset_name FROM dim_apm_assets
           WHERE is_current = 1 ORDER BY cdc_updated_at DESC LIMIT 1 BY tenant_id, asset_id) AS ast
    ON a.tenant_id = ast.tenant_id AND a.asset_id = ast.asset_id
LEFT JOIN (SELECT tenant_id, component_id, component_type FROM dim_apm_asset_components
           WHERE is_current = 1 ORDER BY cdc_updated_at DESC LIMIT 1 BY tenant_id, component_id) AS comp
    ON a.tenant_id = comp.tenant_id AND a.component_id = comp.component_id
LEFT JOIN (SELECT failure_mode_id, failure_mode_name FROM dim_apm_failuremode
           ORDER BY load_time DESC LIMIT 1 BY failure_mode_id) AS fm
    ON a.fault_type = fm.failure_mode_id
WHERE a.tenant_id = '${tenant_id}'
  AND a.window_start >= now() - INTERVAL 30 DAY;


-- ============================================================
-- S3.4: ads_apm_diagnosis_kpi_snapshot  (CH-012)
-- LLM-assisted diagnosis KPI snapshot (last 24 hours)
-- Grain: one row per (tenant_id, asset_id)
-- Only aggregate statistics. No raw LLM text fields.
-- ============================================================

INSERT INTO ads_apm_diagnosis_kpi_snapshot
SELECT
    tenant_id,
    asset_id,
    now()                                              AS snapshot_time,
    uniqExact(diagnosis_id)                            AS diagnosis_count_24h,
    uniqExactIf(diagnosis_id, risk_level = 'high')     AS high_risk_diagnosis_count_24h,
    uniqExactIf(diagnosis_id, risk_level = 'critical') AS critical_risk_diagnosis_count_24h,
    uniqExactIf(diagnosis_id, escalation_required = 1) AS escalation_required_count_24h,
    avg(confidence)                                    AS avg_confidence_24h,
    uniqExactIf(diagnosis_id, quality_caveats IS NOT NULL AND quality_caveats <> '') AS diagnosis_with_quality_caveats_24h,
    uniqExactIf(diagnosis_id, anomaly_id IS NOT NULL)  AS diagnosis_linked_to_anomaly_count_24h,
    max(created_at)                                    AS latest_diagnosis_time,
    now()                                              AS load_time
FROM dwd_apm_realtime_diagnosis_events
WHERE tenant_id = '${tenant_id}' AND created_at >= now() - INTERVAL 24 HOUR
GROUP BY tenant_id, asset_id;


-- ============================================================
-- S3.5: ads_apm_stream_health_snapshot  (CH-013)
-- Stream health aggregated snapshot per source_system and pipeline_name
-- Grain: one row per (tenant_id, source_system, pipeline_name, snapshot_time)
-- ============================================================

INSERT INTO ads_apm_stream_health_snapshot
WITH
stream_health_5m AS (
    SELECT
        tenant_id,
        job_name,
        count()                                            AS message_count_5m,
        max(watermark_lag_seconds)                         AS event_lag_seconds,
        avg(watermark_lag_seconds)                         AS ingest_lag_seconds,
        countIf(late_event_count > 0)                      AS missing_heartbeat_count
    FROM dwd_apm_stream_health_snapshots
    WHERE tenant_id = '${tenant_id}' AND observed_at >= now() - INTERVAL 5 MINUTE
    GROUP BY tenant_id, job_name
),
late_events_5m AS (
    SELECT
        tenant_id,
        count()                                            AS late_event_count_5m
    FROM dwd_apm_late_sensor_readings
    WHERE tenant_id = '${tenant_id}' AND event_time >= now() - INTERVAL 5 MINUTE
    GROUP BY tenant_id
),
dlq_events_5m AS (
    SELECT
        tenant_id,
        count()                                            AS dlq_count_5m
    FROM dwd_apm_dlq_events
    WHERE tenant_id = '${tenant_id}' AND event_time >= now() - INTERVAL 5 MINUTE
    GROUP BY tenant_id
)
SELECT
    sh.tenant_id,
    'kafka'                                            AS source_system,
    sh.job_name                                        AS pipeline_name,
    now()                                              AS snapshot_time,
    sh.message_count_5m,
    COALESCE(late.late_event_count_5m, 0)              AS late_event_count_5m,
    sh.missing_heartbeat_count                         AS missing_heartbeat_count,
    COALESCE(dlq.dlq_count_5m, 0)                      AS dlq_count_5m,
    sh.event_lag_seconds,
    sh.ingest_lag_seconds,
    multiIf(
        sh.event_lag_seconds > 300
          OR COALESCE(dlq.dlq_count_5m, 0) > 0, 'critical',
        sh.event_lag_seconds > 60, 'degraded',
        'healthy'
    )                                                  AS health_status,
    now()                                              AS load_time
FROM stream_health_5m sh
LEFT JOIN late_events_5m late
    ON sh.tenant_id = late.tenant_id
LEFT JOIN dlq_events_5m dlq
    ON sh.tenant_id = dlq.tenant_id
WHERE sh.tenant_id = '${tenant_id}'

UNION ALL

SELECT
    sh.tenant_id,
    'flink'                                            AS source_system,
    sh.job_name                                        AS pipeline_name,
    now()                                              AS snapshot_time,
    sh.message_count_5m,
    COALESCE(late.late_event_count_5m, 0)              AS late_event_count_5m,
    sh.missing_heartbeat_count                         AS missing_heartbeat_count,
    COALESCE(dlq.dlq_count_5m, 0)                      AS dlq_count_5m,
    sh.event_lag_seconds,
    sh.ingest_lag_seconds,
    multiIf(
        sh.event_lag_seconds > 300
          OR COALESCE(dlq.dlq_count_5m, 0) > 0, 'critical',
        sh.event_lag_seconds > 60, 'degraded',
        'healthy'
    )                                                  AS health_status,
    now()                                              AS load_time
FROM stream_health_5m sh
LEFT JOIN late_events_5m late
    ON sh.tenant_id = late.tenant_id
LEFT JOIN dlq_events_5m dlq
    ON sh.tenant_id = dlq.tenant_id
WHERE sh.tenant_id = '${tenant_id}'

UNION ALL

SELECT
    sh.tenant_id,
    'clickhouse'                                       AS source_system,
    sh.job_name                                        AS pipeline_name,
    now()                                              AS snapshot_time,
    sh.message_count_5m,
    COALESCE(late.late_event_count_5m, 0)              AS late_event_count_5m,
    sh.missing_heartbeat_count                         AS missing_heartbeat_count,
    COALESCE(dlq.dlq_count_5m, 0)                      AS dlq_count_5m,
    sh.event_lag_seconds,
    sh.ingest_lag_seconds,
    multiIf(
        sh.event_lag_seconds > 300
          OR COALESCE(dlq.dlq_count_5m, 0) > 0, 'critical',
        sh.event_lag_seconds > 60, 'degraded',
        'healthy'
    )                                                  AS health_status,
    now()                                              AS load_time
FROM stream_health_5m sh
LEFT JOIN late_events_5m late
    ON sh.tenant_id = late.tenant_id
LEFT JOIN dlq_events_5m dlq
    ON sh.tenant_id = dlq.tenant_id
WHERE sh.tenant_id = '${tenant_id}';


-- ============================================================
-- S3.6: ads_apm_failure_evidence_summary  (CH-014)
-- Failure event, workorder, and maintenance evidence aggregation (last 30 days)
-- Grain: one row per (tenant_id, asset_id, component_id, snapshot_time)
-- Clearly separates suspected vs confirmed statistics
-- ============================================================

INSERT INTO ads_apm_failure_evidence_summary
WITH
base_components AS (
    SELECT DISTINCT tenant_id, asset_id, component_id FROM dim_apm_failureevent
    WHERE (suspected_at >= now() - INTERVAL 30 DAY
       OR confirmed_at >= now() - INTERVAL 30 DAY)
    UNION
    SELECT DISTINCT tenant_id, asset_id, component_id FROM dim_apm_workorder
    WHERE (created_at >= now() - INTERVAL 30 DAY
       OR completed_at >= now() - INTERVAL 30 DAY)
    UNION
    SELECT DISTINCT tenant_id, asset_id, component_id FROM dim_apm_maintenance
    WHERE maintenance_date >= now() - INTERVAL 30 DAY
    UNION
    SELECT DISTINCT tenant_id, asset_id, component_id
    FROM (SELECT tenant_id, asset_id, component_id FROM dim_apm_asset_components
          WHERE is_current = 1 ORDER BY cdc_updated_at DESC LIMIT 1 BY tenant_id, component_id)
),
failure_agg AS (
    SELECT
        tenant_id,
        asset_id,
        component_id,
        uniqExactIf(failure_event_id, status = 'suspected') AS suspected_failure_count,
        uniqExactIf(failure_event_id, status = 'confirmed') AS confirmed_failure_count,
        max(suspected_at)                                    AS latest_failure_time
    FROM dim_apm_failureevent
    WHERE (suspected_at >= now() - INTERVAL 30 DAY
       OR confirmed_at >= now() - INTERVAL 30 DAY)
    GROUP BY tenant_id, asset_id, component_id
),
workorder_agg AS (
    SELECT
        tenant_id,
        asset_id,
        component_id,
        uniqExactIf(work_order_id, status <> 'completed') AS open_workorder_count,
        uniqExactIf(work_order_id, status  = 'completed') AS completed_workorder_count,
        max(created_at)                                    AS latest_workorder_time
    FROM dim_apm_workorder
    WHERE created_at >= now() - INTERVAL 30 DAY
    GROUP BY tenant_id, asset_id, component_id
),
maintenance_agg AS (
    SELECT
        tenant_id,
        asset_id,
        component_id,
        uniqExact(maintenance_id)                                           AS maintenance_count,
        uniqExactIf(maintenance_id,
            description LIKE '%sensor%'
              OR maintenance_type LIKE '%sensor%')                          AS sensor_related_maintenance_count,
        max(maintenance_date)                                               AS latest_maintenance_time
    FROM dim_apm_maintenance
    WHERE maintenance_date >= now() - INTERVAL 30 DAY
    GROUP BY tenant_id, asset_id, component_id
)
SELECT
    bc.tenant_id,
    bc.asset_id,
    bc.component_id,
    now()                                                   AS snapshot_time,
    COALESCE(f.suspected_failure_count, 0)                  AS suspected_failure_count,
    COALESCE(f.confirmed_failure_count, 0)                  AS confirmed_failure_count,
    COALESCE(w.open_workorder_count, 0)                     AS open_workorder_count,
    COALESCE(w.completed_workorder_count, 0)                AS completed_workorder_count,
    COALESCE(m.maintenance_count, 0)                        AS maintenance_count,
    COALESCE(m.sensor_related_maintenance_count, 0)         AS sensor_related_maintenance_count,
    f.latest_failure_time                                   AS latest_failure_time,
    w.latest_workorder_time                                 AS latest_workorder_time,
    m.latest_maintenance_time                               AS latest_maintenance_time,
    now()                                                   AS load_time
FROM base_components bc
LEFT JOIN failure_agg f
    ON bc.tenant_id = f.tenant_id
   AND bc.asset_id  = f.asset_id
   AND bc.component_id = f.component_id
LEFT JOIN workorder_agg w
    ON bc.tenant_id = w.tenant_id
   AND bc.asset_id  = w.asset_id
   AND bc.component_id = w.component_id
LEFT JOIN maintenance_agg m
    ON bc.tenant_id = m.tenant_id
   AND bc.asset_id  = m.asset_id
   AND bc.component_id = m.component_id
WHERE bc.tenant_id = '${tenant_id}'
  AND bc.asset_id IS NOT NULL
  AND bc.asset_id <> '';


-- ============================================================
-- S3.7: ads_apm_asset_condition_timeseries_1m  (CH-015)
-- Asset condition timeseries at 1-minute granularity (fills current gap)
-- Roll up asset condition from anomaly events and signal readings
-- Grain: one row per (tenant_id, asset_id, window_start)
-- Condition priority: critical > warning > unknown > healthy
-- ============================================================

INSERT INTO ads_apm_asset_condition_timeseries_1m
WITH
signal_buckets AS (
    SELECT
        tenant_id,
        asset_id,
        toStartOfMinute(event_time)          AS window_start,
        toStartOfMinute(event_time) + INTERVAL 1 MINUTE AS window_end,
        count(DISTINCT tag_id)              AS active_tags
    FROM dwd_apm_realtime_signal_readings
    WHERE tenant_id = '${tenant_id}' AND event_time >= now() - INTERVAL 1 HOUR
    GROUP BY tenant_id, asset_id, window_start
),
anomaly_buckets AS (
    SELECT
        tenant_id,
        asset_id,
        toStartOfMinute(window_start)        AS window_start,
        count()                             AS anomaly_count,
        countIf(severity = 'critical')       AS critical_count,
        countIf(severity = 'warning')        AS warning_count,
        arrayStringConcat(groupUniqArray(quality_flags), '|') AS quality_flags_concat
    FROM dwd_apm_anomaly_events
    WHERE tenant_id = '${tenant_id}' AND window_start >= now() - INTERVAL 1 HOUR
    GROUP BY tenant_id, asset_id, window_start
),
expected_tags AS (
    SELECT
        tenant_id,
        asset_id,
        count(DISTINCT tag_id)              AS total_expected_tags
    FROM (SELECT tenant_id, asset_id, tag_id FROM dim_apm_signal_tags_mapping
          WHERE is_current = 1 ORDER BY cdc_updated_at DESC LIMIT 1 BY tenant_id, tag_id)
    GROUP BY tenant_id, asset_id
)
SELECT
    COALESCE(s.tenant_id, a.tenant_id)                      AS tenant_id,
    COALESCE(s.asset_id, a.asset_id)                        AS asset_id,
    COALESCE(s.window_start, a.window_start)                AS window_start,
    COALESCE(s.window_end, a.window_start + INTERVAL 1 MINUTE) AS window_end,
    multiIf(
        COALESCE(a.critical_count, 0) > 0, 'critical',
        COALESCE(a.warning_count, 0)   > 0, 'warning',
        COALESCE(s.active_tags, 0)      = 0, 'unknown',
        greatest(COALESCE(e.total_expected_tags, 0) - COALESCE(s.active_tags, 0), 0) > 0, 'unknown',
        'healthy'
    )                                                       AS condition_status,
    COALESCE(a.critical_count, 0)                           AS critical_count,
    COALESCE(a.warning_count, 0)                            AS warning_count,
    COALESCE(a.anomaly_count, 0)                            AS anomaly_count,
    COALESCE(s.active_tags, 0)                              AS active_tags,
    greatest(COALESCE(e.total_expected_tags, 0) - COALESCE(s.active_tags, 0), 0) AS stale_tags,
    a.quality_flags_concat                                  AS quality_flags,
    now()                                                   AS load_time
FROM signal_buckets s
FULL OUTER JOIN anomaly_buckets a
    ON s.tenant_id = a.tenant_id AND s.asset_id = a.asset_id AND s.window_start = a.window_start
LEFT JOIN expected_tags e
    ON COALESCE(s.tenant_id, a.tenant_id) = e.tenant_id
   AND COALESCE(s.asset_id, a.asset_id)   = e.asset_id
WHERE COALESCE(s.tenant_id, a.tenant_id) = '${tenant_id}'
  AND COALESCE(s.asset_id, a.asset_id) IS NOT NULL
  AND COALESCE(s.asset_id, a.asset_id) <> '';
