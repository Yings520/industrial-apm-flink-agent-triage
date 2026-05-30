-- PostgreSQL metadata store for Flink enrichment lookup
-- Operational naming (no dim_/fact_ prefix)

CREATE TABLE assets (
    tenant_id      VARCHAR(128) NOT NULL,
    plant_id       VARCHAR(128) NOT NULL,
    asset_id       VARCHAR(128) NOT NULL,
    asset_name     VARCHAR(256) NOT NULL,
    asset_type     VARCHAR(128),
    criticality    VARCHAR(32) DEFAULT 'medium',
    archetype      VARCHAR(128),
    threshold_profile_id VARCHAR(128),
    created_at     TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (tenant_id, asset_id)
);

CREATE TABLE components (
    tenant_id      VARCHAR(128) NOT NULL,
    asset_id       VARCHAR(128) NOT NULL,
    component_id   VARCHAR(128) NOT NULL,
    component_type VARCHAR(128),
    component_name VARCHAR(256),
    criticality    VARCHAR(32) DEFAULT 'medium',
    created_at     TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (tenant_id, component_id)
);

CREATE TABLE sensor_tags (
    tenant_id      VARCHAR(128) NOT NULL,
    tag_id_pattern VARCHAR(256) NOT NULL,
    plant_id       VARCHAR(128) NOT NULL,
    asset_id       VARCHAR(128) NOT NULL,
    asset_name     VARCHAR(256) NOT NULL,
    component_id   VARCHAR(128),
    metric_name    VARCHAR(128) NOT NULL,
    unit           VARCHAR(32),
    source_system  VARCHAR(128) DEFAULT 'opc_ua',
    threshold_profile_id VARCHAR(128) NOT NULL,
    created_at     TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (tenant_id, tag_id_pattern)
);

CREATE TABLE threshold_profiles (
    tenant_id      VARCHAR(128) NOT NULL,
    profile_id     VARCHAR(128) NOT NULL,
    asset_type     VARCHAR(128),
    metric_name    VARCHAR(128),
    unit           VARCHAR(32),
    rule_id        VARCHAR(128),
    rule_name      VARCHAR(256),
    method         VARCHAR(128),
    severity       VARCHAR(32),
    parameters     JSONB DEFAULT '{}',
    version        VARCHAR(32) DEFAULT '1.0.0',
    status         VARCHAR(32) DEFAULT 'active',
    created_at     TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (tenant_id, profile_id)
);

-- Seed data: tenant_northwind tag-to-asset mapping (matches E2E smoke test)
INSERT INTO assets (tenant_id, plant_id, asset_id, asset_name, asset_type, threshold_profile_id, archetype)
VALUES
    ('tenant_northwind', 'plant_northwind_mfg_01', 'asset_northwind_mfg_01_0001', 'Cnc Machine 0001', 'cnc_machine', 'threshold_manufacturing_rotating', 'manufacturing_factory'),
    ('tenant_northwind', 'plant_northwind_mfg_01', 'asset_northwind_mfg_01_0002', 'Conveyor 0002',   'conveyor',    'threshold_manufacturing_rotating', 'manufacturing_factory');

INSERT INTO sensor_tags (tenant_id, tag_id_pattern, plant_id, asset_id, asset_name, metric_name, threshold_profile_id)
VALUES
    ('tenant_northwind', 'tag_northwind_mfg_01_0001_%',   'plant_northwind_mfg_01', 'asset_northwind_mfg_01_0001', 'Cnc Machine 0001', 'temperature', 'threshold_manufacturing_rotating'),
    ('tenant_northwind', 'tag_northwind_mfg_01_0002_%',   'plant_northwind_mfg_01', 'asset_northwind_mfg_01_0002', 'Conveyor 0002',   'temperature', 'threshold_manufacturing_rotating'),
    ('tenant_northwind', 'tag_northwind_mfg_01_0001_%',   'plant_northwind_mfg_01', 'asset_northwind_mfg_01_0001', 'Cnc Machine 0001', 'vibration',   'threshold_manufacturing_rotating'),
    ('tenant_northwind', 'tag_northwind_mfg_01_0002_%',   'plant_northwind_mfg_01', 'asset_northwind_mfg_01_0002', 'Conveyor 0002',   'vibration',   'threshold_manufacturing_rotating'),
    ('tenant_northwind', 'tag_northwind_mfg_01_0001_%',   'plant_northwind_mfg_01', 'asset_northwind_mfg_01_0001', 'Cnc Machine 0001', 'power_draw',  'threshold_manufacturing_rotating'),
    ('tenant_northwind', 'tag_northwind_mfg_01_0002_%',   'plant_northwind_mfg_01', 'asset_northwind_mfg_01_0002', 'Conveyor 0002',   'power_draw',  'threshold_manufacturing_rotating');

INSERT INTO threshold_profiles (tenant_id, profile_id, asset_type, metric_name, rule_id, rule_name, method, severity, parameters)
VALUES
    ('tenant_northwind', 'threshold_manufacturing_rotating', 'cnc_machine', 'temperature', 'threshold_spike', 'CNC Motor Temperature Spike', 'static_threshold', 'critical', '{"multiplier": 2.5}'),
    ('tenant_northwind', 'threshold_manufacturing_rotating', 'conveyor',    'vibration',   'rolling_zscore', 'Conveyor Bearing Vibration',     'rolling_baseline_zscore', 'warning', '{"window_points": 24, "zscore_threshold": 3.0}');
