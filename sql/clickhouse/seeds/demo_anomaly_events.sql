INSERT INTO industrial_apm.ads_apm_anomaly_event_realtime (
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
    has_related_maintenance
)
WITH metric_candidates AS (
    SELECT
        tenant_id,
        asset_id,
        asset_name,
        tag_id,
        metric_name,
        multiIf(
            positionCaseInsensitive(asset_name, 'aerator') > 0, 'aerator',
            positionCaseInsensitive(asset_name, 'clarifier') > 0, 'clarifier',
            positionCaseInsensitive(asset_name, 'chiller') > 0, 'chiller',
            positionCaseInsensitive(asset_name, 'boiler') > 0, 'boiler',
            positionCaseInsensitive(asset_name, 'fan') > 0, 'fan',
            positionCaseInsensitive(asset_name, 'pump') > 0, 'pump',
            positionCaseInsensitive(asset_name, 'compressor') > 0, 'compressor',
            positionCaseInsensitive(asset_name, 'conveyor') > 0, 'conveyor',
            positionCaseInsensitive(asset_name, 'caster') > 0, 'caster',
            positionCaseInsensitive(asset_name, 'air handler') > 0, 'air_handler',
            'asset'
        ) AS component_type,
        row_number() OVER (
            PARTITION BY tenant_id, metric_name
            ORDER BY asset_name, asset_id, tag_id
        ) AS rn
    FROM industrial_apm.vw_grafana_signal_trend_by_tag
    WHERE metric_name IN ('flow_rate', 'pressure', 'vibration', 'power_draw')
    GROUP BY tenant_id, asset_id, asset_name, tag_id, metric_name
),
selected_metrics AS (
    SELECT
        tenant_id,
        asset_id,
        asset_name,
        tag_id,
        metric_name,
        component_type
    FROM metric_candidates
    WHERE rn = 1
),
generated AS (
    SELECT
        concat('seed_', tenant_id, '_', metric_name, '_', toString(minutes_ago), '_', replaceAll(rule_id, 'rule_', '')) AS anomaly_id,
        tenant_id,
        now() - toIntervalMinute(minutes_ago) AS window_start,
        (now() - toIntervalMinute(minutes_ago)) + toIntervalMinute(duration_minutes) AS window_end,
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
        toBool(has_related_workorder) AS has_related_workorder,
        toBool(has_related_maintenance) AS has_related_maintenance
    FROM (
        SELECT
            s.*,
            tupleElement(tpl, 1) AS minutes_ago,
            tupleElement(tpl, 2) AS duration_minutes,
            tupleElement(tpl, 3) AS severity,
            tupleElement(tpl, 4) AS rule_id,
            tupleElement(tpl, 5) AS observed_value,
            tupleElement(tpl, 6) AS expected_value,
            tupleElement(tpl, 7) AS detection_confidence,
            tupleElement(tpl, 8) AS quality_flags,
            tupleElement(tpl, 9) AS failure_mode_id,
            tupleElement(tpl, 10) AS failure_mode_label,
            tupleElement(tpl, 11) AS has_related_workorder,
            tupleElement(tpl, 12) AS has_related_maintenance
        FROM selected_metrics AS s
        ARRAY JOIN multiIf(
            metric_name = 'flow_rate',
            [
                tuple(300, 10, 'warning',  'rule_flow_drift',      82.4, 65.0, 0.78, 'late_arrival=0;flatline=0', 'fm_flow_restriction',   'Flow restriction',   0, 0)
            ],
            metric_name = 'pressure',
            [
                tuple(240, 10, 'critical', 'rule_pressure_spike',  4.8,  2.1,  0.93, 'late_arrival=0;flatline=0', 'fm_pressure_overload', 'Pressure overload', 1, 0),
                tuple(25,  10, 'warning',  'rule_pressure_spike',  3.9,  2.0,  0.81, 'late_arrival=0;flatline=0', 'fm_pressure_overload', 'Pressure overload', 0, 0)
            ],
            metric_name = 'vibration',
            [
                tuple(180, 10, 'critical', 'rule_vibration_high', 11.2,  5.5,  0.91, 'late_arrival=0;flatline=1', 'fm_bearing_wear',      'Bearing wear',      1, 1),
                tuple(70,  10, 'warning',  'rule_vibration_high',  7.1,  4.2,  0.74, 'late_arrival=0;flatline=0', 'fm_shaft_misalignment','Shaft misalignment',0, 1)
            ],
            metric_name = 'power_draw',
            [
                tuple(130, 10, 'info',     'rule_power_spike',    19.8, 16.2,  0.62, 'late_arrival=0;flatline=0', 'fm_load_shift',        'Load shift',        0, 0),
                tuple(50,  10, 'warning',  'rule_power_spike',    21.4, 17.5,  0.77, 'late_arrival=0;flatline=0', 'fm_motor_drag',        'Motor drag',        0, 0)
            ],
            []
        ) AS tpl
    )
)
SELECT
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
    has_related_maintenance
FROM generated
WHERE anomaly_id NOT IN (
    SELECT anomaly_id
    FROM industrial_apm.ads_apm_anomaly_event_realtime
);
