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
