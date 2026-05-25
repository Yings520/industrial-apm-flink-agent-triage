from __future__ import annotations

from pathlib import Path
from typing import cast

import yaml

from src.telemetry_generator.models import (
    ArchetypeConfig,
    AssetConfig,
    MetricConfig,
    PlantConfig,
    RawScenarioProfile,
    ScenarioConfig,
    ScenarioTypeConfig,
    TenantConfig,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = REPO_ROOT / "configs"


def _as_mapping(value: object, context: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f"Expected mapping for {context}")
    raw = cast(dict[object, object], value)
    result: dict[str, object] = {}
    for key, item in raw.items():
        if not isinstance(key, str):
            raise ValueError(f"Expected string key in {context}")
        result[key] = item
    return result


def _as_sequence(value: object, context: str) -> list[object]:
    if not isinstance(value, list):
        raise ValueError(f"Expected list for {context}")
    return cast(list[object], value)


def _as_str(value: object, context: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"Expected string for {context}")
    return value


def _as_int(value: object, context: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"Expected integer for {context}")
    return value


def _as_bool(value: object, context: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"Expected boolean for {context}")
    return value


def _as_str_list(value: object, context: str) -> list[str]:
    return [_as_str(item, f"{context}[]") for item in _as_sequence(value, context)]


def load_yaml_config(path: Path) -> dict[str, object]:
    with path.open("r", encoding="utf-8") as handle:
        payload = cast(object, yaml.safe_load(handle))
    return _as_mapping(payload, str(path))


def load_tenant_config() -> dict[str, object]:
    return load_yaml_config(CONFIG_DIR / "tenants.yml")


def load_asset_config() -> AssetConfig:
    raw = load_yaml_config(CONFIG_DIR / "assets.yml")
    archetypes_raw = _as_mapping(raw["archetypes"], "assets.archetypes")
    tenants_raw = _as_sequence(raw["tenants"], "assets.tenants")
    metric_types_raw = _as_sequence(raw["metric_types"], "assets.metric_types")

    archetypes: dict[str, ArchetypeConfig] = {}
    for name, value in archetypes_raw.items():
        archetype = _as_mapping(value, f"assets.archetypes.{name}")
        archetypes[name] = {
            "asset_types": _as_str_list(archetype["asset_types"], f"assets.archetypes.{name}.asset_types"),
            "expected_metrics": _as_str_list(
                archetype["expected_metrics"], f"assets.archetypes.{name}.expected_metrics"
            ),
            "units": _as_str_list(archetype["units"], f"assets.archetypes.{name}.units"),
            "threshold_profile_id": _as_str(
                archetype["threshold_profile_id"], f"assets.archetypes.{name}.threshold_profile_id"
            ),
            "plausible_scenarios": _as_str_list(
                archetype["plausible_scenarios"], f"assets.archetypes.{name}.plausible_scenarios"
            ),
        }

    tenants: list[TenantConfig] = []
    for index, value in enumerate(tenants_raw):
        tenant = _as_mapping(value, f"assets.tenants[{index}]")
        plants_raw = _as_sequence(tenant["plants"], f"assets.tenants[{index}].plants")
        plants: list[PlantConfig] = []
        for plant_index, plant_value in enumerate(plants_raw):
            plant = _as_mapping(plant_value, f"assets.tenants[{index}].plants[{plant_index}]")
            plants.append(
                {
                    "plant_id": _as_str(plant["plant_id"], f"assets.tenants[{index}].plants[{plant_index}].plant_id"),
                    "site_id": _as_str(plant["site_id"], f"assets.tenants[{index}].plants[{plant_index}].site_id"),
                    "archetype": _as_str(
                        plant["archetype"], f"assets.tenants[{index}].plants[{plant_index}].archetype"
                    ),
                }
            )
        tenants.append(
            {
                "tenant_id": _as_str(tenant["tenant_id"], f"assets.tenants[{index}].tenant_id"),
                "plants": plants,
            }
        )

    metric_types: list[MetricConfig] = []
    for index, value in enumerate(metric_types_raw):
        metric = _as_mapping(value, f"assets.metric_types[{index}]")
        metric_types.append(
            {
                "name": _as_str(metric["name"], f"assets.metric_types[{index}].name"),
                "unit": _as_str(metric["unit"], f"assets.metric_types[{index}].unit"),
            }
        )

    return {
        "schema_version": _as_str(raw["schema_version"], "assets.schema_version"),
        "plants_per_tenant": _as_int(raw["plants_per_tenant"], "assets.plants_per_tenant"),
        "assets_per_plant": _as_int(raw["assets_per_plant"], "assets.assets_per_plant"),
        "metric_types": metric_types,
        "archetypes": archetypes,
        "tenants": tenants,
    }


def load_anomaly_rules_config() -> dict[str, object]:
    return load_yaml_config(CONFIG_DIR / "anomaly_rules.yml")


def load_scenario_config() -> ScenarioConfig:
    raw = load_yaml_config(CONFIG_DIR / "scenarios.yml")
    scenario_types_raw = _as_mapping(raw["scenario_types"], "scenarios.scenario_types")
    profiles_raw = _as_mapping(raw["profiles"], "scenarios.profiles")

    scenario_types: dict[str, ScenarioTypeConfig] = {}
    for name, value in scenario_types_raw.items():
        scenario = _as_mapping(value, f"scenarios.scenario_types.{name}")
        scenario_types[name] = {
            "description": _as_str(scenario["description"], f"scenarios.scenario_types.{name}.description"),
            "quality_flags": _as_str_list(scenario["quality_flags"], f"scenarios.scenario_types.{name}.quality_flags"),
            "valid": _as_bool(scenario["valid"], f"scenarios.scenario_types.{name}.valid"),
        }

    profiles: dict[str, RawScenarioProfile] = {}
    for name, value in profiles_raw.items():
        profile = _as_mapping(value, f"scenarios.profiles.{name}")
        raw_anomaly_scenarios = profile.get("anomaly_scenarios")
        parsed_profile: RawScenarioProfile = {
            "tenants": _as_int(profile["tenants"], f"scenarios.profiles.{name}.tenants"),
            "plants_per_tenant": _as_int(profile["plants_per_tenant"], f"scenarios.profiles.{name}.plants_per_tenant"),
            "assets_per_plant": _as_int(profile["assets_per_plant"], f"scenarios.profiles.{name}.assets_per_plant"),
            "metric_types": _as_str_list(profile["metric_types"], f"scenarios.profiles.{name}.metric_types"),
            "scenarios": _as_str_list(profile["scenarios"], f"scenarios.profiles.{name}.scenarios"),
            "start_time": _as_str(profile["start_time"], f"scenarios.profiles.{name}.start_time"),
            "periods_per_metric": _as_int(
                profile["periods_per_metric"], f"scenarios.profiles.{name}.periods_per_metric"
            ),
        }
        if raw_anomaly_scenarios is not None:
            parsed_profile["anomaly_scenarios"] = _as_str_list(
                raw_anomaly_scenarios, f"scenarios.profiles.{name}.anomaly_scenarios"
            )
        profiles[name] = parsed_profile

    return {
        "schema_version": _as_str(raw["schema_version"], "scenarios.schema_version"),
        "event_contract": _as_str(raw["event_contract"], "scenarios.event_contract"),
        "scenario_types": scenario_types,
        "profiles": profiles,
    }
