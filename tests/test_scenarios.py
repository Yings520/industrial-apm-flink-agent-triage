import copy

import pytest

from src.telemetry_generator.config import load_asset_config, load_scenario_config
from src.telemetry_generator.scenarios import (
    REQUIRED_ARCHETYPES,
    REQUIRED_SCENARIOS,
    load_scenarios,
    resolve_profile,
)


def test_required_scenarios_load_from_yaml() -> None:
    scenarios = load_scenarios()

    assert set(REQUIRED_SCENARIOS).issubset(scenarios.keys())


def test_demo_profile_resolves_locked_baseline() -> None:
    scenario_config = load_scenario_config()
    asset_config = load_asset_config()

    profile = resolve_profile("demo", scenario_config, asset_config)

    assert profile.tenant_count == 3
    assert profile.plants_per_tenant == 3
    assert profile.assets_per_plant == 20
    assert profile.total_assets == 180
    assert profile.metric_types == [
        "temperature",
        "vibration",
        "pressure",
        "flow_rate",
        "power_draw",
    ]
    assert profile.anomaly_scenarios == [
        "late",
        "missing_heartbeat",
        "flatline",
        "spike",
        "drift",
    ]


def test_demo_archetype_coverage() -> None:
    asset_config = load_asset_config()
    archetypes = {
        plant["archetype"]
        for tenant in asset_config["tenants"]
        for plant in tenant["plants"]
    }

    assert set(REQUIRED_ARCHETYPES).issubset(archetypes)


def test_profile_cannot_request_more_tenants_than_asset_config() -> None:
    scenario_config = copy.deepcopy(load_scenario_config())
    asset_config = load_asset_config()
    scenario_config["profiles"]["demo"]["tenants"] = len(asset_config["tenants"]) + 1

    with pytest.raises(ValueError, match="tenant count exceeds"):
        resolve_profile("demo", scenario_config, asset_config)


def test_profile_cannot_request_more_assets_than_asset_config() -> None:
    scenario_config = copy.deepcopy(load_scenario_config())
    asset_config = load_asset_config()
    scenario_config["profiles"]["smoke"]["assets_per_plant"] = asset_config["assets_per_plant"] + 1

    with pytest.raises(ValueError, match="asset count exceeds"):
        resolve_profile("smoke", scenario_config, asset_config)
