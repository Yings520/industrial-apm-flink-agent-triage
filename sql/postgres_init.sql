-- PostgreSQL metadata store for Flink enrichment lookup
-- Phase 1: Data Source Ingestion Layer - 12 tables per data model spec

CREATE TABLE tenants (
    tenant_id      VARCHAR(64) PRIMARY KEY,
    tenant_name    VARCHAR(255) NOT NULL,
    region         VARCHAR(64),
    tenant_status  VARCHAR(32) NOT NULL DEFAULT 'active',
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE assets (
    tenant_id            VARCHAR(64) NOT NULL,
    asset_id             VARCHAR(64) NOT NULL,
    asset_name           VARCHAR(255) NOT NULL,
    asset_type           VARCHAR(100),
    asset_class          VARCHAR(100),
    parent_asset_id      VARCHAR(64),
    site_id              VARCHAR(64),
    area_id              VARCHAR(64),
    line_id              VARCHAR(64),
    functional_location  VARCHAR(255),
    criticality          VARCHAR(32),
    lifecycle_state      VARCHAR(32) DEFAULT 'active',
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (tenant_id, asset_id),
    FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id),
    FOREIGN KEY (tenant_id, parent_asset_id) REFERENCES assets (tenant_id, asset_id)
);

CREATE TABLE asset_components (
    tenant_id            VARCHAR(64) NOT NULL,
    component_id         VARCHAR(64) NOT NULL,
    asset_id             VARCHAR(64) NOT NULL,
    parent_component_id  VARCHAR(64),
    component_name       VARCHAR(255) NOT NULL,
    component_type       VARCHAR(100),
    lifecycle_state      VARCHAR(32) DEFAULT 'active',
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (tenant_id, component_id),
    FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id),
    FOREIGN KEY (tenant_id, asset_id) REFERENCES assets (tenant_id, asset_id),
    FOREIGN KEY (tenant_id, parent_component_id) REFERENCES asset_components (tenant_id, component_id)
);

CREATE TABLE asset_signal_tags_mapping (
    tenant_id              VARCHAR(64) NOT NULL,
    mapping_id             VARCHAR(64) NOT NULL,
    asset_id               VARCHAR(64) NOT NULL,
    component_id           VARCHAR(64),
    tag_id                 VARCHAR(128) NOT NULL,
    tag_name               VARCHAR(255) NOT NULL,
    source_system          VARCHAR(100) NOT NULL,
    measurement_point      VARCHAR(255),
    metric_name            VARCHAR(100) NOT NULL,
    signal_type            VARCHAR(50),
    unit                   VARCHAR(50),
    expected_unit          VARCHAR(50),
    sampling_rate_seconds  NUMERIC(12, 3),
    normal_range_min       NUMERIC(18, 6),
    normal_range_max       NUMERIC(18, 6),
    calibration_status     VARCHAR(50),
    mapping_status         VARCHAR(32) NOT NULL DEFAULT 'active',
    valid_from             TIMESTAMPTZ NOT NULL DEFAULT now(),
    valid_to               TIMESTAMPTZ,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (tenant_id, mapping_id),
    FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id),
    FOREIGN KEY (tenant_id, asset_id) REFERENCES assets (tenant_id, asset_id),
    FOREIGN KEY (tenant_id, component_id) REFERENCES asset_components (tenant_id, component_id)
);

CREATE TABLE workorder (
    tenant_id               VARCHAR(64) NOT NULL,
    work_order_id           VARCHAR(64) NOT NULL,
    asset_id                VARCHAR(64) NOT NULL,
    component_id            VARCHAR(64),
    source_system           VARCHAR(100),
    external_work_order_id  VARCHAR(128),
    work_order_type         VARCHAR(50) NOT NULL,
    work_order_status       VARCHAR(50) NOT NULL,
    priority                VARCHAR(32),
    title                   VARCHAR(255),
    description             TEXT,
    problem_code            VARCHAR(100),
    cause_code              VARCHAR(100),
    remedy_code             VARCHAR(100),
    requested_at            TIMESTAMPTZ,
    planned_start_at        TIMESTAMPTZ,
    planned_end_at          TIMESTAMPTZ,
    actual_start_at         TIMESTAMPTZ,
    completed_at            TIMESTAMPTZ,
    downtime_minutes        NUMERIC(12, 2),
    technician_notes        TEXT,
    failure_mode_id         BIGINT,
    related_anomaly_id      VARCHAR(64),
    related_tag_id          VARCHAR(128),
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (tenant_id, work_order_id),
    FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id),
    FOREIGN KEY (tenant_id, asset_id) REFERENCES assets (tenant_id, asset_id),
    FOREIGN KEY (tenant_id, component_id) REFERENCES asset_components (tenant_id, component_id)
);

CREATE TABLE maintenance (
    tenant_id                    VARCHAR(64) NOT NULL,
    maintenance_id               VARCHAR(64) NOT NULL,
    asset_id                     VARCHAR(64) NOT NULL,
    component_id                 VARCHAR(64),
    work_order_id                VARCHAR(64),
    maintenance_type             VARCHAR(50) NOT NULL,
    maintenance_status           VARCHAR(50) NOT NULL DEFAULT 'completed',
    maintenance_reason           TEXT,
    maintenance_result           TEXT,
    failure_mode_id              BIGINT,
    suspected_failure_mode       VARCHAR(255),
    confirmed_fault_description  TEXT,
    performed_by                 VARCHAR(128),
    performed_at                 TIMESTAMPTZ NOT NULL,
    completed_at                 TIMESTAMPTZ,
    parts_used                   JSONB,
    labor_hours                  NUMERIC(12, 2),
    downtime_minutes             NUMERIC(12, 2),
    related_tag_id               VARCHAR(128),
    related_anomaly_id           VARCHAR(64),
    sensor_anomaly_related       BOOLEAN DEFAULT false,
    evidence_notes               TEXT,
    created_at                   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                   TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (tenant_id, maintenance_id),
    FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id),
    FOREIGN KEY (tenant_id, asset_id) REFERENCES assets (tenant_id, asset_id),
    FOREIGN KEY (tenant_id, component_id) REFERENCES asset_components (tenant_id, component_id),
    FOREIGN KEY (tenant_id, work_order_id) REFERENCES workorder (tenant_id, work_order_id)
);

CREATE TABLE apm_failuredomain (
    tenant_id         VARCHAR(64) NOT NULL,
    id                BIGINT GENERATED ALWAYS AS IDENTITY,
    yaml_id           VARCHAR(255),
    code              VARCHAR(100) NOT NULL,
    label             VARCHAR(255) NOT NULL,
    description       TEXT,
    active            BOOLEAN NOT NULL DEFAULT true,
    created_by_id     BIGINT,
    modified_by_id    BIGINT,
    created_date      TIMESTAMPTZ NOT NULL DEFAULT now(),
    modified_date     TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (tenant_id, id),
    FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id)
);

CREATE TABLE apm_failureclass (
    tenant_id         VARCHAR(64) NOT NULL,
    id                BIGINT GENERATED ALWAYS AS IDENTITY,
    domain_id         BIGINT NOT NULL,
    yaml_id           VARCHAR(255),
    code              VARCHAR(100) NOT NULL,
    label             VARCHAR(255) NOT NULL,
    description       TEXT,
    active            BOOLEAN NOT NULL DEFAULT true,
    created_by_id     BIGINT,
    modified_by_id    BIGINT,
    created_date      TIMESTAMPTZ NOT NULL DEFAULT now(),
    modified_date     TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (tenant_id, id),
    FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id),
    FOREIGN KEY (tenant_id, domain_id) REFERENCES apm_failuredomain (tenant_id, id)
);

CREATE TABLE apm_failuremode (
    tenant_id            VARCHAR(64) NOT NULL,
    id                   BIGINT GENERATED ALWAYS AS IDENTITY,
    failure_class_id     BIGINT NOT NULL,
    yaml_id              VARCHAR(255),
    code                 VARCHAR(100) NOT NULL,
    label                VARCHAR(255) NOT NULL,
    description          TEXT,
    asset_type           VARCHAR(100),
    component_type       VARCHAR(100),
    typical_effect       TEXT,
    related_metrics      JSONB,
    active               BOOLEAN NOT NULL DEFAULT true,
    created_by_id        BIGINT,
    modified_by_id       BIGINT,
    created_date         TIMESTAMPTZ NOT NULL DEFAULT now(),
    modified_date        TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (tenant_id, id),
    FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id),
    FOREIGN KEY (tenant_id, failure_class_id) REFERENCES apm_failureclass (tenant_id, id)
);

CREATE TABLE apm_failureevent (
    tenant_id              VARCHAR(64) NOT NULL,
    failure_event_id       VARCHAR(64) NOT NULL,
    asset_id               VARCHAR(64) NOT NULL,
    component_id           VARCHAR(64),
    failure_mode_id        BIGINT,
    event_status           VARCHAR(32) NOT NULL DEFAULT 'suspected',
    severity               VARCHAR(32),
    occurred_at            TIMESTAMPTZ,
    detected_at            TIMESTAMPTZ NOT NULL,
    resolved_at            TIMESTAMPTZ,
    source_system          VARCHAR(100),
    source_event_type      VARCHAR(50),
    source_event_id        VARCHAR(128),
    work_order_id          VARCHAR(64),
    maintenance_id         VARCHAR(64),
    related_anomaly_id     VARCHAR(64),
    related_tag_ids        JSONB,
    evidence_summary       TEXT,
    label_confidence       NUMERIC(5, 4),
    created_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (tenant_id, failure_event_id),
    FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id),
    FOREIGN KEY (tenant_id, asset_id) REFERENCES assets (tenant_id, asset_id),
    FOREIGN KEY (tenant_id, component_id) REFERENCES asset_components (tenant_id, component_id),
    FOREIGN KEY (tenant_id, failure_mode_id) REFERENCES apm_failuremode (tenant_id, id),
    FOREIGN KEY (tenant_id, work_order_id) REFERENCES workorder (tenant_id, work_order_id),
    FOREIGN KEY (tenant_id, maintenance_id) REFERENCES maintenance (tenant_id, maintenance_id)
);

CREATE TABLE apm_anomaly_rule (
    tenant_id              VARCHAR(64) NOT NULL,
    rule_id                VARCHAR(64) NOT NULL,
    rule_name              VARCHAR(255) NOT NULL,
    method                 VARCHAR(64) NOT NULL,
    asset_type             VARCHAR(100),
    component_type         VARCHAR(100),
    metric_name            VARCHAR(100) NOT NULL,
    unit                   VARCHAR(50),
    severity               VARCHAR(32) NOT NULL,
    parameters             JSONB NOT NULL,
    failure_mode_id        BIGINT,
    approval_status        VARCHAR(32) NOT NULL DEFAULT 'draft',
    status                 VARCHAR(32) NOT NULL DEFAULT 'active',
    owner                  VARCHAR(128),
    version                VARCHAR(32) NOT NULL DEFAULT '1.0.0',
    effective_from         TIMESTAMPTZ NOT NULL DEFAULT now(),
    effective_to           TIMESTAMPTZ,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by             VARCHAR(128),
    updated_by             VARCHAR(128),

    PRIMARY KEY (tenant_id, rule_id),
    FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id),
    FOREIGN KEY (tenant_id, failure_mode_id) REFERENCES apm_failuremode (tenant_id, id)
);

CREATE TABLE apm_threshold_profile (
    tenant_id              VARCHAR(64) NOT NULL,
    profile_id             VARCHAR(64) NOT NULL,
    rule_id                VARCHAR(64) NOT NULL,
    profile_name           VARCHAR(255),
    asset_type             VARCHAR(100),
    component_type         VARCHAR(100),
    metric_name            VARCHAR(100) NOT NULL,
    unit                   VARCHAR(50),
    normal_range_min       NUMERIC(18, 6),
    normal_range_max       NUMERIC(18, 6),
    warning_min            NUMERIC(18, 6),
    warning_max            NUMERIC(18, 6),
    critical_min           NUMERIC(18, 6),
    critical_max           NUMERIC(18, 6),
    parameters             JSONB,
    status                 VARCHAR(32) NOT NULL DEFAULT 'active',
    version                VARCHAR(32) NOT NULL DEFAULT '1.0.0',
    effective_from         TIMESTAMPTZ NOT NULL DEFAULT now(),
    effective_to           TIMESTAMPTZ,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (tenant_id, profile_id),
    FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id),
    FOREIGN KEY (tenant_id, rule_id) REFERENCES apm_anomaly_rule (tenant_id, rule_id)
);

-- Seed data: 3 tenants
INSERT INTO tenants (tenant_id, tenant_name, region) VALUES
    ('tenant_northwind', 'Northwind Industrial', 'americas'),
    ('tenant_apac_ops', 'APAC Operations', 'apac'),
    ('tenant_euro_core', 'Euro Core Utilities', 'emea');
