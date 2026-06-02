-- Flink CDC source tables for PostgreSQL metadata via Debezium
-- Phase 2 Task A3.1: CREATE TABLE DDL for all 12 PostgreSQL tables as CDC sources
-- Kafka broker: localhost:19092
-- Debezium topic prefix: pg_apm (topic naming: pg_apm.public.{table_name})
-- Format: debezium-json without schema (after fields only)

SET 'table.local-time-zone' = 'UTC';

-- =============================================================================
-- 1. pg_tenants
-- =============================================================================
CREATE TABLE pg_tenants (
    tenant_id      STRING,
    tenant_name    STRING,
    region         STRING,
    tenant_status  STRING,
    created_at     TIMESTAMP(3),
    updated_at     TIMESTAMP(3)
) WITH (
    'connector' = 'kafka',
    'topic' = 'pg_apm.public.tenants',
    'properties.bootstrap.servers' = 'localhost:19092',
    'properties.group.id' = 'flink-cdc-tenants',
    'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

-- =============================================================================
-- 2. pg_assets
-- =============================================================================
CREATE TABLE pg_assets (
    tenant_id            STRING,
    asset_id             STRING,
    asset_name           STRING,
    asset_type           STRING,
    asset_class          STRING,
    parent_asset_id      STRING,
    site_id              STRING,
    area_id              STRING,
    line_id              STRING,
    functional_location  STRING,
    criticality          STRING,
    lifecycle_state      STRING,
    created_at           TIMESTAMP(3),
    updated_at           TIMESTAMP(3)
) WITH (
    'connector' = 'kafka',
    'topic' = 'pg_apm.public.assets',
    'properties.bootstrap.servers' = 'localhost:19092',
    'properties.group.id' = 'flink-cdc-assets',
    'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

-- =============================================================================
-- 3. pg_asset_components
-- =============================================================================
CREATE TABLE pg_asset_components (
    tenant_id            STRING,
    component_id         STRING,
    asset_id             STRING,
    parent_component_id  STRING,
    component_name       STRING,
    component_type       STRING,
    lifecycle_state      STRING,
    created_at           TIMESTAMP(3),
    updated_at           TIMESTAMP(3)
) WITH (
    'connector' = 'kafka',
    'topic' = 'pg_apm.public.asset_components',
    'properties.bootstrap.servers' = 'localhost:19092',
    'properties.group.id' = 'flink-cdc-components',
    'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

-- =============================================================================
-- 4. pg_asset_signal_tags_mapping
-- =============================================================================
CREATE TABLE pg_asset_signal_tags_mapping (
    tenant_id              STRING,
    mapping_id             STRING,
    asset_id               STRING,
    component_id           STRING,
    tag_id                 STRING,
    tag_name               STRING,
    source_system          STRING,
    measurement_point      STRING,
    metric_name            STRING,
    signal_type            STRING,
    unit                   STRING,
    expected_unit          STRING,
    sampling_rate_seconds  DECIMAL(12, 3),
    normal_range_min       DECIMAL(18, 6),
    normal_range_max       DECIMAL(18, 6),
    calibration_status     STRING,
    mapping_status         STRING,
    valid_from             TIMESTAMP(3),
    valid_to               TIMESTAMP(3),
    created_at             TIMESTAMP(3),
    updated_at             TIMESTAMP(3)
) WITH (
    'connector' = 'kafka',
    'topic' = 'pg_apm.public.asset_signal_tags_mapping',
    'properties.bootstrap.servers' = 'localhost:19092',
    'properties.group.id' = 'flink-cdc-tag-mapping',
    'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

-- =============================================================================
-- 5. pg_apm_anomaly_rule
-- =============================================================================
CREATE TABLE pg_apm_anomaly_rule (
    tenant_id          STRING,
    rule_id            STRING,
    rule_name          STRING,
    method             STRING,
    asset_type         STRING,
    component_type     STRING,
    metric_name        STRING,
    unit               STRING,
    severity           STRING,
    parameters         STRING,
    failure_mode_id    BIGINT,
    approval_status    STRING,
    status             STRING,
    owner              STRING,
    version            STRING,
    effective_from     TIMESTAMP(3),
    effective_to       TIMESTAMP(3),
    created_at         TIMESTAMP(3),
    updated_at         TIMESTAMP(3),
    created_by         STRING,
    updated_by         STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'pg_apm.public.apm_anomaly_rule',
    'properties.bootstrap.servers' = 'localhost:19092',
    'properties.group.id' = 'flink-cdc-anomaly-rule',
    'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

-- =============================================================================
-- 6. pg_apm_threshold_profile
-- =============================================================================
CREATE TABLE pg_apm_threshold_profile (
    tenant_id          STRING,
    profile_id         STRING,
    rule_id            STRING,
    profile_name       STRING,
    asset_type         STRING,
    component_type     STRING,
    metric_name        STRING,
    unit               STRING,
    normal_range_min   DECIMAL(18, 6),
    normal_range_max   DECIMAL(18, 6),
    warning_min        DECIMAL(18, 6),
    warning_max        DECIMAL(18, 6),
    critical_min       DECIMAL(18, 6),
    critical_max       DECIMAL(18, 6),
    parameters         STRING,
    status             STRING,
    version            STRING,
    effective_from     TIMESTAMP(3),
    effective_to       TIMESTAMP(3),
    created_at         TIMESTAMP(3),
    updated_at         TIMESTAMP(3)
) WITH (
    'connector' = 'kafka',
    'topic' = 'pg_apm.public.apm_threshold_profile',
    'properties.bootstrap.servers' = 'localhost:19092',
    'properties.group.id' = 'flink-cdc-threshold-profile',
    'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

-- =============================================================================
-- 7. pg_apm_failuredomain
-- =============================================================================
CREATE TABLE pg_apm_failuredomain (
    tenant_id       STRING,
    id              BIGINT,
    yaml_id         STRING,
    code            STRING,
    label           STRING,
    description     STRING,
    active          BOOLEAN,
    created_by_id   BIGINT,
    modified_by_id  BIGINT,
    created_date    TIMESTAMP(3),
    modified_date   TIMESTAMP(3)
) WITH (
    'connector' = 'kafka',
    'topic' = 'pg_apm.public.apm_failuredomain',
    'properties.bootstrap.servers' = 'localhost:19092',
    'properties.group.id' = 'flink-cdc-failuredomain',
    'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

-- =============================================================================
-- 8. pg_apm_failureclass
-- =============================================================================
CREATE TABLE pg_apm_failureclass (
    tenant_id       STRING,
    id              BIGINT,
    domain_id       BIGINT,
    yaml_id         STRING,
    code            STRING,
    label           STRING,
    description     STRING,
    active          BOOLEAN,
    created_by_id   BIGINT,
    modified_by_id  BIGINT,
    created_date    TIMESTAMP(3),
    modified_date   TIMESTAMP(3)
) WITH (
    'connector' = 'kafka',
    'topic' = 'pg_apm.public.apm_failureclass',
    'properties.bootstrap.servers' = 'localhost:19092',
    'properties.group.id' = 'flink-cdc-failureclass',
    'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

-- =============================================================================
-- 9. pg_apm_failuremode
-- =============================================================================
CREATE TABLE pg_apm_failuremode (
    tenant_id          STRING,
    id                 BIGINT,
    failure_class_id   BIGINT,
    yaml_id            STRING,
    code               STRING,
    label              STRING,
    description        STRING,
    asset_type         STRING,
    component_type     STRING,
    typical_effect     STRING,
    related_metrics    STRING,
    active             BOOLEAN,
    created_by_id      BIGINT,
    modified_by_id     BIGINT,
    created_date       TIMESTAMP(3),
    modified_date      TIMESTAMP(3)
) WITH (
    'connector' = 'kafka',
    'topic' = 'pg_apm.public.apm_failuremode',
    'properties.bootstrap.servers' = 'localhost:19092',
    'properties.group.id' = 'flink-cdc-failuremode',
    'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

-- =============================================================================
-- 10. pg_apm_failureevent
-- =============================================================================
CREATE TABLE pg_apm_failureevent (
    tenant_id             STRING,
    failure_event_id      STRING,
    asset_id              STRING,
    component_id          STRING,
    failure_mode_id       BIGINT,
    event_status          STRING,
    severity              STRING,
    occurred_at           TIMESTAMP(3),
    detected_at           TIMESTAMP(3),
    resolved_at           TIMESTAMP(3),
    source_system         STRING,
    source_event_type     STRING,
    source_event_id       STRING,
    work_order_id         STRING,
    maintenance_id        STRING,
    related_anomaly_id    STRING,
    related_tag_ids       STRING,
    evidence_summary      STRING,
    label_confidence      DECIMAL(5, 4),
    created_at            TIMESTAMP(3),
    updated_at            TIMESTAMP(3)
) WITH (
    'connector' = 'kafka',
    'topic' = 'pg_apm.public.apm_failureevent',
    'properties.bootstrap.servers' = 'localhost:19092',
    'properties.group.id' = 'flink-cdc-failureevent',
    'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

-- =============================================================================
-- 11. pg_workorder
-- =============================================================================
CREATE TABLE pg_workorder (
    tenant_id               STRING,
    work_order_id           STRING,
    asset_id                STRING,
    component_id            STRING,
    source_system           STRING,
    external_work_order_id  STRING,
    work_order_type         STRING,
    work_order_status       STRING,
    priority                STRING,
    title                   STRING,
    description             STRING,
    problem_code            STRING,
    cause_code              STRING,
    remedy_code             STRING,
    requested_at            TIMESTAMP(3),
    planned_start_at        TIMESTAMP(3),
    planned_end_at          TIMESTAMP(3),
    actual_start_at         TIMESTAMP(3),
    completed_at            TIMESTAMP(3),
    downtime_minutes        DECIMAL(12, 2),
    technician_notes        STRING,
    failure_mode_id         BIGINT,
    related_anomaly_id      STRING,
    related_tag_id          STRING,
    created_at              TIMESTAMP(3),
    updated_at              TIMESTAMP(3)
) WITH (
    'connector' = 'kafka',
    'topic' = 'pg_apm.public.workorder',
    'properties.bootstrap.servers' = 'localhost:19092',
    'properties.group.id' = 'flink-cdc-workorder',
    'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

-- =============================================================================
-- 12. pg_maintenance
-- =============================================================================
CREATE TABLE pg_maintenance (
    tenant_id                    STRING,
    maintenance_id               STRING,
    asset_id                     STRING,
    component_id                 STRING,
    work_order_id                STRING,
    maintenance_type             STRING,
    maintenance_status           STRING,
    maintenance_reason           STRING,
    maintenance_result           STRING,
    failure_mode_id              BIGINT,
    suspected_failure_mode       STRING,
    confirmed_fault_description  STRING,
    performed_by                 STRING,
    performed_at                 TIMESTAMP(3),
    completed_at                 TIMESTAMP(3),
    parts_used                   STRING,
    labor_hours                  DECIMAL(12, 2),
    downtime_minutes             DECIMAL(12, 2),
    related_tag_id               STRING,
    related_anomaly_id           STRING,
    sensor_anomaly_related       BOOLEAN,
    evidence_notes               STRING,
    created_at                   TIMESTAMP(3),
    updated_at                   TIMESTAMP(3)
) WITH (
    'connector' = 'kafka',
    'topic' = 'pg_apm.public.maintenance',
    'properties.bootstrap.servers' = 'localhost:19092',
    'properties.group.id' = 'flink-cdc-maintenance',
    'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);
