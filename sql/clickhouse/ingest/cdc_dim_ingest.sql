USE industrial_apm;

-- PostgreSQL CDC raw landing ingestion.
-- Kafka Engine tables include Debezium unwrap's __deleted flag so materialized
-- views can skip delete records. Target dim_apm_* tables keep only PostgreSQL
-- source columns; derived/SCD/wide fields are intentionally excluded here.

CREATE TABLE IF NOT EXISTS kafka_cdc_dim_apm_assets
(
    tenant_id Nullable(String), asset_id Nullable(String), asset_name Nullable(String),
    asset_type Nullable(String), asset_class Nullable(String), parent_asset_id Nullable(String),
    site_id Nullable(String), area_id Nullable(String), line_id Nullable(String),
    functional_location Nullable(String), criticality Nullable(String), lifecycle_state Nullable(String),
    created_at Nullable(String), updated_at Nullable(String), __deleted Nullable(String)
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'redpanda:19092', kafka_topic_list = 'pg_apm.public.assets',
    kafka_group_name = 'clickhouse_cdc_dim_apm_assets', kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_cdc_dim_apm_assets TO dim_apm_assets AS
SELECT tenant_id, asset_id, asset_name, asset_type, asset_class, parent_asset_id, site_id,
       area_id, line_id, functional_location, criticality, lifecycle_state,
       COALESCE(parseDateTimeBestEffortOrNull(created_at), toDateTime(0)) AS created_at,
       COALESCE(parseDateTimeBestEffortOrNull(updated_at), toDateTime(0)) AS updated_at
FROM kafka_cdc_dim_apm_assets
WHERE coalesce(__deleted, 'false') != 'true';

CREATE TABLE IF NOT EXISTS kafka_cdc_dim_apm_asset_components
(
    tenant_id Nullable(String), component_id Nullable(String), asset_id Nullable(String),
    parent_component_id Nullable(String), component_name Nullable(String),
    component_type Nullable(String), lifecycle_state Nullable(String),
    created_at Nullable(String), updated_at Nullable(String), __deleted Nullable(String)
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'redpanda:19092', kafka_topic_list = 'pg_apm.public.asset_components',
    kafka_group_name = 'clickhouse_cdc_dim_apm_asset_components', kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_cdc_dim_apm_asset_components TO dim_apm_asset_components AS
SELECT tenant_id, component_id, asset_id, parent_component_id, component_name, component_type,
       lifecycle_state,
       COALESCE(parseDateTimeBestEffortOrNull(created_at), toDateTime(0)) AS created_at,
       COALESCE(parseDateTimeBestEffortOrNull(updated_at), toDateTime(0)) AS updated_at
FROM kafka_cdc_dim_apm_asset_components
WHERE coalesce(__deleted, 'false') != 'true';

CREATE TABLE IF NOT EXISTS kafka_cdc_dim_apm_signal_tags_mapping
(
    tenant_id Nullable(String), mapping_id Nullable(String), asset_id Nullable(String),
    component_id Nullable(String), tag_id Nullable(String), tag_name Nullable(String),
    source_system Nullable(String), measurement_point Nullable(String), metric_name Nullable(String),
    signal_type Nullable(String), unit Nullable(String), expected_unit Nullable(String),
    sampling_rate_seconds Nullable(String), normal_range_min Nullable(String), normal_range_max Nullable(String),
    calibration_status Nullable(String), mapping_status Nullable(String), valid_from Nullable(String),
    valid_to Nullable(String), created_at Nullable(String), updated_at Nullable(String),
    __deleted Nullable(String)
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'redpanda:19092', kafka_topic_list = 'pg_apm.public.asset_signal_tags_mapping',
    kafka_group_name = 'clickhouse_cdc_dim_apm_signal_tags_mapping', kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_cdc_dim_apm_signal_tags_mapping TO dim_apm_signal_tags_mapping AS
SELECT tenant_id, mapping_id, asset_id, component_id, tag_id, tag_name, source_system,
       measurement_point, metric_name, signal_type, unit, expected_unit,
       toFloat64OrNull(sampling_rate_seconds) AS sampling_rate_seconds,
       toFloat64OrNull(normal_range_min) AS normal_range_min,
       toFloat64OrNull(normal_range_max) AS normal_range_max,
       calibration_status, mapping_status,
       COALESCE(parseDateTimeBestEffortOrNull(valid_from), toDateTime(0)) AS valid_from,
       parseDateTimeBestEffortOrNull(valid_to) AS valid_to,
       COALESCE(parseDateTimeBestEffortOrNull(created_at), toDateTime(0)) AS created_at,
       COALESCE(parseDateTimeBestEffortOrNull(updated_at), toDateTime(0)) AS updated_at
FROM kafka_cdc_dim_apm_signal_tags_mapping
WHERE coalesce(__deleted, 'false') != 'true';

CREATE TABLE IF NOT EXISTS kafka_cdc_dim_apm_workorder
(
    tenant_id Nullable(String), work_order_id Nullable(String), asset_id Nullable(String),
    component_id Nullable(String), source_system Nullable(String), external_work_order_id Nullable(String),
    work_order_type Nullable(String), work_order_status Nullable(String), priority Nullable(String),
    title Nullable(String), description Nullable(String), problem_code Nullable(String),
    cause_code Nullable(String), remedy_code Nullable(String), requested_at Nullable(String),
    planned_start_at Nullable(String), planned_end_at Nullable(String), actual_start_at Nullable(String),
    completed_at Nullable(String), downtime_minutes Nullable(String), technician_notes Nullable(String),
    failure_mode_id Nullable(String), related_anomaly_id Nullable(String), related_tag_id Nullable(String),
    created_at Nullable(String), updated_at Nullable(String), __deleted Nullable(String)
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'redpanda:19092', kafka_topic_list = 'pg_apm.public.workorder',
    kafka_group_name = 'clickhouse_cdc_dim_apm_workorder', kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_cdc_dim_apm_workorder TO dim_apm_workorder AS
SELECT tenant_id, work_order_id, asset_id, component_id, source_system, external_work_order_id,
       work_order_type, work_order_status, priority, title, description, problem_code, cause_code,
       remedy_code, parseDateTimeBestEffortOrNull(requested_at) AS requested_at,
       parseDateTimeBestEffortOrNull(planned_start_at) AS planned_start_at,
       parseDateTimeBestEffortOrNull(planned_end_at) AS planned_end_at,
       parseDateTimeBestEffortOrNull(actual_start_at) AS actual_start_at,
       parseDateTimeBestEffortOrNull(completed_at) AS completed_at,
       toFloat64OrNull(downtime_minutes) AS downtime_minutes, technician_notes,
       toInt64OrNull(failure_mode_id) AS failure_mode_id, related_anomaly_id, related_tag_id,
       COALESCE(parseDateTimeBestEffortOrNull(created_at), toDateTime(0)) AS created_at,
       COALESCE(parseDateTimeBestEffortOrNull(updated_at), toDateTime(0)) AS updated_at
FROM kafka_cdc_dim_apm_workorder
WHERE coalesce(__deleted, 'false') != 'true';

CREATE TABLE IF NOT EXISTS kafka_cdc_dim_apm_maintenance
(
    tenant_id Nullable(String), maintenance_id Nullable(String), asset_id Nullable(String),
    component_id Nullable(String), work_order_id Nullable(String), maintenance_type Nullable(String),
    maintenance_status Nullable(String), maintenance_reason Nullable(String), maintenance_result Nullable(String),
    failure_mode_id Nullable(String), suspected_failure_mode Nullable(String),
    confirmed_fault_description Nullable(String), performed_by Nullable(String), performed_at Nullable(String),
    completed_at Nullable(String), parts_used Nullable(String), labor_hours Nullable(String),
    downtime_minutes Nullable(String), related_tag_id Nullable(String), related_anomaly_id Nullable(String),
    sensor_anomaly_related Nullable(String), evidence_notes Nullable(String), created_at Nullable(String),
    updated_at Nullable(String), __deleted Nullable(String)
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'redpanda:19092', kafka_topic_list = 'pg_apm.public.maintenance',
    kafka_group_name = 'clickhouse_cdc_dim_apm_maintenance', kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_cdc_dim_apm_maintenance TO dim_apm_maintenance AS
SELECT tenant_id, maintenance_id, asset_id, component_id, work_order_id, maintenance_type,
       maintenance_status, maintenance_reason, maintenance_result, toInt64OrNull(failure_mode_id) AS failure_mode_id,
       suspected_failure_mode, confirmed_fault_description, performed_by,
       COALESCE(parseDateTimeBestEffortOrNull(performed_at), toDateTime(0)) AS performed_at,
       parseDateTimeBestEffortOrNull(completed_at) AS completed_at, parts_used,
       toFloat64OrNull(labor_hours) AS labor_hours, toFloat64OrNull(downtime_minutes) AS downtime_minutes,
       related_tag_id, related_anomaly_id,
       multiIf(
           isNull(sensor_anomaly_related), NULL,
           lowerUTF8(sensor_anomaly_related) IN ('true', 't', '1', 'yes'), true,
           lowerUTF8(sensor_anomaly_related) IN ('false', 'f', '0', 'no'), false,
           NULL
       ) AS sensor_anomaly_related,
       evidence_notes,
       COALESCE(parseDateTimeBestEffortOrNull(created_at), toDateTime(0)) AS created_at,
       COALESCE(parseDateTimeBestEffortOrNull(updated_at), toDateTime(0)) AS updated_at
FROM kafka_cdc_dim_apm_maintenance
WHERE coalesce(__deleted, 'false') != 'true';

CREATE TABLE IF NOT EXISTS kafka_cdc_dim_apm_failuredomain
(
    tenant_id Nullable(String), id Nullable(String), yaml_id Nullable(String), code Nullable(String),
    label Nullable(String), description Nullable(String), active Nullable(String),
    created_by_id Nullable(String), modified_by_id Nullable(String), created_date Nullable(String),
    modified_date Nullable(String), __deleted Nullable(String)
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'redpanda:19092', kafka_topic_list = 'pg_apm.public.apm_failuredomain',
    kafka_group_name = 'clickhouse_cdc_dim_apm_failuredomain', kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_cdc_dim_apm_failuredomain TO dim_apm_failuredomain AS
SELECT tenant_id, toInt64OrNull(id) AS id, yaml_id, code, label, description,
       toBool(active) AS active, toInt64OrNull(created_by_id) AS created_by_id,
       toInt64OrNull(modified_by_id) AS modified_by_id,
       COALESCE(parseDateTimeBestEffortOrNull(created_date), toDateTime(0)) AS created_date,
       COALESCE(parseDateTimeBestEffortOrNull(modified_date), toDateTime(0)) AS modified_date
FROM kafka_cdc_dim_apm_failuredomain
WHERE coalesce(__deleted, 'false') != 'true';

CREATE TABLE IF NOT EXISTS kafka_cdc_dim_apm_failureclass
(
    tenant_id Nullable(String), id Nullable(String), domain_id Nullable(String), yaml_id Nullable(String),
    code Nullable(String), label Nullable(String), description Nullable(String), active Nullable(String),
    created_by_id Nullable(String), modified_by_id Nullable(String), created_date Nullable(String),
    modified_date Nullable(String), __deleted Nullable(String)
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'redpanda:19092', kafka_topic_list = 'pg_apm.public.apm_failureclass',
    kafka_group_name = 'clickhouse_cdc_dim_apm_failureclass', kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_cdc_dim_apm_failureclass TO dim_apm_failureclass AS
SELECT tenant_id, toInt64OrNull(id) AS id, toInt64OrNull(domain_id) AS domain_id,
       yaml_id, code, label, description, toBool(active) AS active,
       toInt64OrNull(created_by_id) AS created_by_id, toInt64OrNull(modified_by_id) AS modified_by_id,
       COALESCE(parseDateTimeBestEffortOrNull(created_date), toDateTime(0)) AS created_date,
       COALESCE(parseDateTimeBestEffortOrNull(modified_date), toDateTime(0)) AS modified_date
FROM kafka_cdc_dim_apm_failureclass
WHERE coalesce(__deleted, 'false') != 'true';

CREATE TABLE IF NOT EXISTS kafka_cdc_dim_apm_failuremode
(
    tenant_id Nullable(String), id Nullable(String), failure_class_id Nullable(String),
    yaml_id Nullable(String), code Nullable(String), label Nullable(String), description Nullable(String),
    asset_type Nullable(String), component_type Nullable(String), typical_effect Nullable(String),
    related_metrics Nullable(String), active Nullable(String), created_by_id Nullable(String),
    modified_by_id Nullable(String), created_date Nullable(String), modified_date Nullable(String),
    __deleted Nullable(String)
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'redpanda:19092', kafka_topic_list = 'pg_apm.public.apm_failuremode',
    kafka_group_name = 'clickhouse_cdc_dim_apm_failuremode', kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_cdc_dim_apm_failuremode TO dim_apm_failuremode AS
SELECT tenant_id, toInt64OrNull(id) AS id, toInt64OrNull(failure_class_id) AS failure_class_id,
       yaml_id, code, label, description, asset_type, component_type, typical_effect,
       related_metrics, toBool(active) AS active, toInt64OrNull(created_by_id) AS created_by_id,
       toInt64OrNull(modified_by_id) AS modified_by_id,
       COALESCE(parseDateTimeBestEffortOrNull(created_date), toDateTime(0)) AS created_date,
       COALESCE(parseDateTimeBestEffortOrNull(modified_date), toDateTime(0)) AS modified_date
FROM kafka_cdc_dim_apm_failuremode
WHERE coalesce(__deleted, 'false') != 'true';

CREATE TABLE IF NOT EXISTS kafka_cdc_dim_apm_failureevent
(
    tenant_id Nullable(String), failure_event_id Nullable(String), asset_id Nullable(String),
    component_id Nullable(String), failure_mode_id Nullable(String), event_status Nullable(String),
    severity Nullable(String), occurred_at Nullable(String), detected_at Nullable(String),
    resolved_at Nullable(String), source_system Nullable(String), source_event_type Nullable(String),
    source_event_id Nullable(String), work_order_id Nullable(String), maintenance_id Nullable(String),
    related_anomaly_id Nullable(String), related_tag_ids Nullable(String), evidence_summary Nullable(String),
    label_confidence Nullable(String), created_at Nullable(String), updated_at Nullable(String),
    __deleted Nullable(String)
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'redpanda:19092', kafka_topic_list = 'pg_apm.public.apm_failureevent',
    kafka_group_name = 'clickhouse_cdc_dim_apm_failureevent', kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_cdc_dim_apm_failureevent TO dim_apm_failureevent AS
SELECT tenant_id, failure_event_id, asset_id, component_id, toInt64OrNull(failure_mode_id) AS failure_mode_id,
       event_status, severity, parseDateTimeBestEffortOrNull(occurred_at) AS occurred_at,
       COALESCE(parseDateTimeBestEffortOrNull(detected_at), toDateTime(0)) AS detected_at,
       parseDateTimeBestEffortOrNull(resolved_at) AS resolved_at, source_system, source_event_type,
       source_event_id, work_order_id, maintenance_id, related_anomaly_id, related_tag_ids,
       evidence_summary, toFloat64OrNull(label_confidence) AS label_confidence,
       COALESCE(parseDateTimeBestEffortOrNull(created_at), toDateTime(0)) AS created_at,
       COALESCE(parseDateTimeBestEffortOrNull(updated_at), toDateTime(0)) AS updated_at
FROM kafka_cdc_dim_apm_failureevent
WHERE coalesce(__deleted, 'false') != 'true';

CREATE TABLE IF NOT EXISTS kafka_cdc_dim_apm_anomaly_rule
(
    tenant_id Nullable(String), rule_id Nullable(String), rule_name Nullable(String),
    method Nullable(String), asset_type Nullable(String), component_type Nullable(String),
    metric_name Nullable(String), unit Nullable(String), severity Nullable(String),
    parameters Nullable(String), failure_mode_id Nullable(String), approval_status Nullable(String),
    status Nullable(String), owner Nullable(String), version Nullable(String),
    effective_from Nullable(String), effective_to Nullable(String), created_at Nullable(String),
    updated_at Nullable(String), created_by Nullable(String), updated_by Nullable(String),
    __deleted Nullable(String)
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'redpanda:19092', kafka_topic_list = 'pg_apm.public.apm_anomaly_rule',
    kafka_group_name = 'clickhouse_cdc_dim_apm_anomaly_rule', kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_cdc_dim_apm_anomaly_rule TO dim_apm_anomaly_rule AS
SELECT tenant_id, rule_id, rule_name, method, asset_type, component_type, metric_name, unit,
       severity, parameters, toInt64OrNull(failure_mode_id) AS failure_mode_id,
       approval_status, status, owner, version,
       COALESCE(parseDateTimeBestEffortOrNull(effective_from), toDateTime(0)) AS effective_from,
       parseDateTimeBestEffortOrNull(effective_to) AS effective_to,
       COALESCE(parseDateTimeBestEffortOrNull(created_at), toDateTime(0)) AS created_at,
       COALESCE(parseDateTimeBestEffortOrNull(updated_at), toDateTime(0)) AS updated_at,
       created_by, updated_by
FROM kafka_cdc_dim_apm_anomaly_rule
WHERE coalesce(__deleted, 'false') != 'true';

CREATE TABLE IF NOT EXISTS kafka_cdc_dim_apm_threshold_profiles
(
    tenant_id Nullable(String), profile_id Nullable(String), rule_id Nullable(String),
    profile_name Nullable(String), asset_type Nullable(String), component_type Nullable(String),
    metric_name Nullable(String), unit Nullable(String), normal_range_min Nullable(String),
    normal_range_max Nullable(String), warning_min Nullable(String), warning_max Nullable(String),
    critical_min Nullable(String), critical_max Nullable(String), parameters Nullable(String),
    status Nullable(String), version Nullable(String), effective_from Nullable(String),
    effective_to Nullable(String), created_at Nullable(String), updated_at Nullable(String),
    __deleted Nullable(String)
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'redpanda:19092', kafka_topic_list = 'pg_apm.public.apm_threshold_profile',
    kafka_group_name = 'clickhouse_cdc_dim_apm_threshold_profiles', kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_cdc_dim_apm_threshold_profiles TO dim_apm_threshold_profiles AS
SELECT tenant_id, profile_id, rule_id, profile_name, asset_type, component_type, metric_name,
       unit, toFloat64OrNull(normal_range_min) AS normal_range_min,
       toFloat64OrNull(normal_range_max) AS normal_range_max,
       toFloat64OrNull(warning_min) AS warning_min, toFloat64OrNull(warning_max) AS warning_max,
       toFloat64OrNull(critical_min) AS critical_min, toFloat64OrNull(critical_max) AS critical_max,
       parameters, status, version,
       COALESCE(parseDateTimeBestEffortOrNull(effective_from), toDateTime(0)) AS effective_from,
       parseDateTimeBestEffortOrNull(effective_to) AS effective_to,
       COALESCE(parseDateTimeBestEffortOrNull(created_at), toDateTime(0)) AS created_at,
       COALESCE(parseDateTimeBestEffortOrNull(updated_at), toDateTime(0)) AS updated_at
FROM kafka_cdc_dim_apm_threshold_profiles
WHERE coalesce(__deleted, 'false') != 'true';

CREATE TABLE IF NOT EXISTS kafka_cdc_dim_apm_tenants
(
    tenant_id Nullable(String), tenant_name Nullable(String), region Nullable(String),
    tenant_status Nullable(String), created_at Nullable(String), updated_at Nullable(String),
    __deleted Nullable(String)
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'redpanda:19092', kafka_topic_list = 'pg_apm.public.tenants',
    kafka_group_name = 'clickhouse_cdc_dim_apm_tenants', kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_cdc_dim_apm_tenants TO dim_apm_tenants AS
SELECT tenant_id, tenant_name, region, tenant_status,
       COALESCE(parseDateTimeBestEffortOrNull(created_at), toDateTime(0)) AS created_at,
       COALESCE(parseDateTimeBestEffortOrNull(updated_at), toDateTime(0)) AS updated_at
FROM kafka_cdc_dim_apm_tenants
WHERE coalesce(__deleted, 'false') != 'true';
