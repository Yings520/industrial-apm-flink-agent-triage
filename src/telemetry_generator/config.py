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
    StreamingConfig,
    StreamingTopicConfig,
    TagMappingConfig,
    TagMetadata,
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


def load_tag_mapping_config() -> TagMappingConfig:
    raw = load_yaml_config(CONFIG_DIR / "tag_mapping.yml")
    return {
        "schema_version": _as_str(raw["schema_version"], "tag_mapping.schema_version"),
        "description": _as_str(raw["description"], "tag_mapping.description"),
        "tag_id_pattern": _as_str(raw["tag_id_pattern"], "tag_mapping.tag_id_pattern"),
        "tag_name_pattern": _as_str(raw["tag_name_pattern"], "tag_mapping.tag_name_pattern"),
        "required_fields": _as_str_list(raw["required_fields"], "tag_mapping.required_fields"),
    }


def _plant_slug(plant_id: str) -> str:
    return plant_id.removeprefix("plant_")


def build_tag_id(plant_id: str, asset_index: int, metric_name: str) -> str:
    return f"tag_{_plant_slug(plant_id)}_{asset_index:04d}_{metric_name}"


def build_asset_id(plant_id: str, asset_index: int) -> str:
    return f"asset_{_plant_slug(plant_id)}_{asset_index:04d}"


def build_asset_name(asset_type: str, asset_index: int) -> str:
    return f"{asset_type.replace('_', ' ').title()} {asset_index:04d}"


def build_tag_metadata(asset_config: AssetConfig | None = None) -> dict[str, TagMetadata]:
    config = asset_config if asset_config is not None else load_asset_config()
    metric_units = {metric["name"]: metric["unit"] for metric in config["metric_types"]}
    mappings: dict[str, TagMetadata] = {}

    for tenant in config["tenants"]:
        tenant_id = tenant["tenant_id"]
        for plant in tenant["plants"]:
            plant_id = plant["plant_id"]
            archetype_config = config["archetypes"][plant["archetype"]]
            asset_types = archetype_config["asset_types"]
            threshold_profile_id = archetype_config["threshold_profile_id"]
            for asset_index in range(1, config["assets_per_plant"] + 1):
                asset_type = asset_types[(asset_index - 1) % len(asset_types)]
                asset_id = build_asset_id(plant_id, asset_index)
                asset_name = build_asset_name(asset_type, asset_index)
                for metric_name in archetype_config["expected_metrics"]:
                    unit = metric_units[metric_name]
                    if unit not in archetype_config["units"]:
                        continue
                    tag_id = build_tag_id(plant_id, asset_index, metric_name)
                    if tag_id in mappings and mappings[tag_id]["tenant_id"] != tenant_id:
                        raise ValueError(f"tag_id maps across tenants: {tag_id}")
                    mappings[tag_id] = {
                        "tenant_id": tenant_id,
                        "plant_id": plant_id,
                        "asset_id": asset_id,
                        "asset_name": asset_name,
                        "tag_id": tag_id,
                        "tag_name": f"{asset_type}_{metric_name}_{asset_index:04d}",
                        "metric_name": metric_name,
                        "unit": unit,
                        "threshold_profile_id": threshold_profile_id,
                    }
    return mappings


def load_streaming_config() -> StreamingConfig:
    raw = load_yaml_config(CONFIG_DIR / "streaming.yml")
    broker = _as_mapping(raw["broker"], "streaming.broker")
    flink = _as_mapping(raw["flink"], "streaming.flink")
    topics_raw = _as_mapping(raw["topics"], "streaming.topics")
    topics: StreamingTopicConfig = {
        "raw_sensor_events": _as_str(topics_raw["raw_sensor_events"], "streaming.topics.raw_sensor_events"),
        "raw_dlq": _as_str(topics_raw["raw_dlq"], "streaming.topics.raw_dlq"),
        "staging_telemetry_enriched": _as_str(
            topics_raw["staging_telemetry_enriched"], "streaming.topics.staging_telemetry_enriched"
        ),
        "staging_dlq": _as_str(topics_raw["staging_dlq"], "streaming.topics.staging_dlq"),
        "mart_anomalies": _as_str(topics_raw["mart_anomalies"], "streaming.topics.mart_anomalies"),
        "mart_late_events": _as_str(topics_raw["mart_late_events"], "streaming.topics.mart_late_events"),
        "mart_stream_health": _as_str(topics_raw["mart_stream_health"], "streaming.topics.mart_stream_health"),
    }
    return {
        "schema_version": _as_str(raw["schema_version"], "streaming.schema_version"),
        "broker": {
            "bootstrap_servers": _as_str(broker["bootstrap_servers"], "streaming.broker.bootstrap_servers"),
            "redpanda_admin": _as_str(broker["redpanda_admin"], "streaming.broker.redpanda_admin"),
        },
        "flink": {
            "jobmanager_ui": _as_str(flink["jobmanager_ui"], "streaming.flink.jobmanager_ui"),
            "watermark_delay_minutes": _as_int(flink["watermark_delay_minutes"], "streaming.flink.watermark_delay_minutes"),
            "allowed_lateness_minutes": _as_int(
                flink["allowed_lateness_minutes"], "streaming.flink.allowed_lateness_minutes"
            ),
        },
        "topics": topics,
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
