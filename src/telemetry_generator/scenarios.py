from __future__ import annotations

from src.telemetry_generator.config import load_scenario_config
from src.telemetry_generator.models import AssetConfig, ScenarioConfig, ScenarioProfile, ScenarioTypeConfig

REQUIRED_SCENARIOS = (
    "normal",
    "late",
    "missing_heartbeat",
    "flatline",
    "spike",
    "drift",
    "invalid_event",
)

REQUIRED_ARCHETYPES = (
    "manufacturing_factory",
    "chemical_plant",
    "power_plant",
    "metallurgy_steel",
    "water_treatment_utilities",
    "campus_facilities",
    "industrial_asset_management_generic",
)


def load_scenarios() -> dict[str, ScenarioTypeConfig]:
    config = load_scenario_config()
    return config["scenario_types"]


def resolve_profile(
    profile_name: str,
    scenario_config: ScenarioConfig,
    asset_config: AssetConfig,
) -> ScenarioProfile:
    profiles = scenario_config["profiles"]
    if profile_name not in profiles:
        raise ValueError(f"Unknown scenario profile: {profile_name}")

    raw_profile = profiles[profile_name]
    tenant_count = raw_profile["tenants"]
    plants_per_tenant = raw_profile["plants_per_tenant"]
    assets_per_plant = raw_profile["assets_per_plant"]
    metric_types = list(raw_profile["metric_types"])
    scenarios = list(raw_profile["scenarios"])
    anomaly_scenarios = list(
        raw_profile.get("anomaly_scenarios", [name for name in scenarios if name not in {"normal", "invalid_event"}])
    )

    if tenant_count > len(asset_config["tenants"]):
        raise ValueError("Profile tenant count exceeds asset config")
    if plants_per_tenant > asset_config["plants_per_tenant"]:
        raise ValueError("Profile plant count exceeds asset config")
    if assets_per_plant > asset_config["assets_per_plant"]:
        raise ValueError("Profile asset count exceeds asset config")

    return ScenarioProfile(
        name=profile_name,
        tenant_count=tenant_count,
        plants_per_tenant=plants_per_tenant,
        assets_per_plant=assets_per_plant,
        total_assets=tenant_count * plants_per_tenant * assets_per_plant,
        metric_types=metric_types,
        anomaly_scenarios=anomaly_scenarios,
        scenarios=scenarios,
        start_time=raw_profile["start_time"],
        periods_per_metric=raw_profile["periods_per_metric"],
    )
