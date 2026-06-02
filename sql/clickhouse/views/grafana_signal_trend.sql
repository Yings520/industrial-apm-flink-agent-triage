CREATE OR REPLACE VIEW industrial_apm.vw_grafana_signal_trend_by_tag AS
WITH
asset_dim AS (
    SELECT
        tenant_id,
        asset_id,
        anyLast(asset_name) AS asset_name,
        anyLast(site_id) AS site_id,
        anyLast(line_id) AS line_id
    FROM industrial_apm.dim_apm_assets
    GROUP BY tenant_id, asset_id
),
tag_dim AS (
    SELECT
        tenant_id,
        asset_id,
        tag_id,
        anyLast(tag_name) AS tag_name,
        anyLast(unit) AS unit,
        anyLast(source_system) AS source_system,
        anyLast(measurement_point) AS measurement_point
    FROM industrial_apm.dim_apm_signal_tags_mapping
    GROUP BY tenant_id, asset_id, tag_id
)
SELECT
    s.tenant_id AS tenant_id,
    s.asset_id AS asset_id,
    a.asset_name AS asset_name,
    a.site_id AS site_id,
    a.line_id AS line_id,
    s.component_id AS component_id,
    s.tag_id AS tag_id,
    t.tag_name AS tag_name,
    s.metric_name AS metric_name,
    coalesce(
        nullIf(t.unit, ''),
        multiIf(
            s.metric_name = 'temperature', 'celsius',
            s.metric_name = 'power_draw', 'kW',
            s.metric_name = 'pressure', 'bar',
            s.metric_name = 'flow_rate', 'm3/h',
            s.metric_name = 'vibration', 'mm/s',
            ''
        )
    ) AS unit,
    t.source_system AS source_system,
    t.measurement_point AS measurement_point,
    s.bucket_minute AS signal_time,
    s.avg_value AS signal_value,
    s.min_value AS min_value,
    s.max_value AS max_value,
    s.last_value AS last_value,
    s.event_count AS event_count,
    s.normal_range_min AS normal_range_min,
    s.normal_range_max AS normal_range_max,
    s.quality_flags AS quality_flags,
    s.has_late_arrival AS has_late_arrival,
    s.has_flatline AS has_flatline,
    s.has_missing_heartbeat AS has_missing_heartbeat,
    concat(s.tag_id, ' / ', s.metric_name) AS series_name,
    s.load_time AS load_time
FROM industrial_apm.ads_apm_tag_signal_timeseries_1m AS s
LEFT JOIN asset_dim AS a
    ON s.tenant_id = a.tenant_id
   AND s.asset_id = a.asset_id
LEFT JOIN tag_dim AS t
    ON s.tenant_id = t.tenant_id
   AND s.asset_id = t.asset_id
   AND s.tag_id = t.tag_id;
