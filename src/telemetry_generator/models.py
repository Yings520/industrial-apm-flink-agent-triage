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


class SensorEvent(TypedDict):
    event_id: str
    schema_version: str
    tenant_id: str
    plant_id: str
    asset_id: str
    metric_name: str
    event_time: str
    ingest_time: str
    value: float
    unit: str
    source_system: str
    quality_flags: list[str]
    scenario: str


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
    valid_events: list[SensorEvent]
    invalid_events: list[dict[str, object]]


@dataclass(frozen=True)
class OutputPaths:
    valid_events_path: Path
    invalid_events_path: Path
