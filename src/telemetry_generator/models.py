from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import NotRequired, TypedDict


class MetricConfig(TypedDict):
    name: str
    unit: str


class ArchetypeConfig(TypedDict):
    asset_types: list[str]
    expected_metrics: list[str]
    units: list[str]
    threshold_profile_id: str
    plausible_scenarios: list[str]


class PlantConfig(TypedDict):
    plant_id: str
    site_id: str
    archetype: str


class TenantConfig(TypedDict):
    tenant_id: str
    plants: list[PlantConfig]


class AssetConfig(TypedDict):
    schema_version: str
    plants_per_tenant: int
    assets_per_plant: int
    metric_types: list[MetricConfig]
    archetypes: dict[str, ArchetypeConfig]
    tenants: list[TenantConfig]


class TagMappingConfig(TypedDict):
    schema_version: str
    description: str
    tag_id_pattern: str
    tag_name_pattern: str
    required_fields: list[str]


class StreamingTopicConfig(TypedDict):
    raw_sensor_events: str
    raw_dlq: str
    staging_telemetry_enriched: str
    staging_dlq: str
    mart_anomalies: str
    mart_late_events: str
    mart_stream_health: str


class StreamingFlinkConfig(TypedDict):
    jobmanager_ui: str
    watermark_delay_minutes: int
    allowed_lateness_minutes: int


class StreamingBrokerConfig(TypedDict):
    bootstrap_servers: str
    redpanda_admin: str


class StreamingConfig(TypedDict):
    schema_version: str
    broker: StreamingBrokerConfig
    flink: StreamingFlinkConfig
    topics: StreamingTopicConfig


class ScenarioTypeConfig(TypedDict):
    description: str
    quality_flags: list[str]
    valid: bool


class RawScenarioProfile(TypedDict):
    tenants: int
    plants_per_tenant: int
    assets_per_plant: int
    metric_types: list[str]
    scenarios: list[str]
    start_time: str
    periods_per_metric: int
    anomaly_scenarios: NotRequired[list[str]]


class ScenarioConfig(TypedDict):
    schema_version: str
    event_contract: str
    scenario_types: dict[str, ScenarioTypeConfig]
    profiles: dict[str, RawScenarioProfile]


class RawSensorEvent(TypedDict):
    event_id: str
    schema_version: str
    tenant_id: str
    tag_id: str
    tag_name: str
    event_time: str
    ingest_time: str
    value: float
    unit: str
    source_system: str
    quality_flags: list[str]
    scenario: str


class SensorEvent(RawSensorEvent):
    """Backward-compatible alias for Phase 1 imports."""


class TagMetadata(TypedDict):
    tenant_id: str
    plant_id: str
    asset_id: str
    asset_name: str
    tag_id: str
    tag_name: str
    metric_name: str
    unit: str
    threshold_profile_id: str


class ComponentConfig(TypedDict):
    component_type: str
    expected_metrics: list[str]
    criticality: str


class ComponentCatalogConfig(TypedDict):
    schema_version: str
    description: str
    components_per_asset_default: int
    archetype_components: dict[str, list[ComponentConfig]]


class AssetArchetypeComponents(TypedDict):
    component_type: str
    count_per_asset: int


class UpgradedArchetypeConfig(TypedDict):
    asset_types: list[str]
    expected_metrics: list[str]
    units: list[str]
    threshold_profile_id: str
    plausible_scenarios: list[str]
    components: list[AssetArchetypeComponents]


class SensorTagConfig(TypedDict):
    schema_version: str
    tenant_id: str
    tag_id: str
    tag_name: str
    plant_id: str
    asset_id: str
    component_id: str
    metric_name: str
    unit: str
    source_system: str
    signal_type: str
    sampling_rate_hz: float
    measurement_point: str
    normal_range_min: float
    normal_range_max: float
    engineering_limit_min: float
    engineering_limit_max: float
    calibration_status: str
    last_calibrated_at: str
    quality_flags: list[str]
    valid_from: str
    valid_to: str | None
    is_current: bool


class ThresholdProfileConfig(TypedDict):
    schema_version: str
    profile_id: str
    tenant_id: str
    asset_type: str
    component_type: str | None
    metric_name: str
    unit: str
    rule_id: str
    rule_name: str
    method: str
    severity: str
    parameters: dict[str, object]
    description: str
    version: str
    effective_from: str
    effective_to: str | None
    owner: str
    approval_status: str
    status: str
    created_at: str
    created_by: str


@dataclass(frozen=True)
class ScenarioProfile:
    name: str
    tenant_count: int
    plants_per_tenant: int
    assets_per_plant: int
    total_assets: int
    metric_types: list[str]
    anomaly_scenarios: list[str]
    scenarios: list[str]
    start_time: str
    periods_per_metric: int


@dataclass(frozen=True)
class GenerationResult:
    profile: ScenarioProfile
    valid_events: list[RawSensorEvent]
    invalid_events: list[dict[str, object]]


@dataclass(frozen=True)
class OutputPaths:
    valid_events_path: Path
    invalid_events_path: Path
