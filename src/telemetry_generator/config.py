from __future__ import annotations

import hashlib
from pathlib import Path
from typing import cast

import yaml

from src.telemetry_generator.models import (
    ArchetypeConfig,
    AssetConfig,
    MeasurementPointCatalogConfig,
    MeasurementPointConfig,
    MetricConfig,
    PlantConfig,
    RawScenarioProfile,
    ScenarioConfig,
    ScenarioTypeConfig,
    StreamingConfig,
    StreamingTopicConfig,
    SyntheticScenarioConfig,
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
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"Expected integer for {context}")
    return value


def _as_float(value: object, context: str) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError(f"Expected number for {context}")
    return float(value)


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
        parsed: ArchetypeConfig = {
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
        if "components" in archetype:
            raw_comps = _as_sequence(archetype["components"], f"assets.archetypes.{name}.components")
            parsed["components"] = [
                {
                    "component_type": _as_str(
                        _as_mapping(c, f"assets.archetypes.{name}.components[{i}]")["component_type"],
                        f"assets.archetypes.{name}.components[{i}].component_type",
                    ),
                    "count_per_asset": _as_int(
                        _as_mapping(c, f"assets.archetypes.{name}.components[{i}]")["count_per_asset"],
                        f"assets.archetypes.{name}.components[{i}].count_per_asset",
                    ),
                }
                for i, c in enumerate(raw_comps)
            ]
        archetypes[name] = parsed

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


def load_measurement_point_config() -> MeasurementPointCatalogConfig:
    raw = load_yaml_config(CONFIG_DIR / "measurement_points.yml")
    points_raw = _as_mapping(raw["measurement_points"], "measurement_points.measurement_points")
    points: dict[str, list[MeasurementPointConfig]] = {}

    for asset_type, value in points_raw.items():
        records: list[MeasurementPointConfig] = []
        for index, item in enumerate(_as_sequence(value, f"measurement_points.{asset_type}")):
            record = _as_mapping(item, f"measurement_points.{asset_type}[{index}]")
            records.append(
                {
                    "component_type": _as_str(
                        record["component_type"], f"measurement_points.{asset_type}[{index}].component_type"
                    ),
                    "measurement_point": _as_str(
                        record["measurement_point"], f"measurement_points.{asset_type}[{index}].measurement_point"
                    ),
                    "metric_name": _as_str(
                        record["metric_name"], f"measurement_points.{asset_type}[{index}].metric_name"
                    ),
                    "tag_suffix": _as_str(record["tag_suffix"], f"measurement_points.{asset_type}[{index}].tag_suffix"),
                    "unit": _as_str(record["unit"], f"measurement_points.{asset_type}[{index}].unit"),
                    "sampling_rate_hz": _as_float(
                        record["sampling_rate_hz"], f"measurement_points.{asset_type}[{index}].sampling_rate_hz"
                    ),
                    "normal_range_min": _as_float(
                        record["normal_range_min"], f"measurement_points.{asset_type}[{index}].normal_range_min"
                    ),
                    "normal_range_max": _as_float(
                        record["normal_range_max"], f"measurement_points.{asset_type}[{index}].normal_range_max"
                    ),
                    "engineering_limit_min": _as_float(
                        record["engineering_limit_min"],
                        f"measurement_points.{asset_type}[{index}].engineering_limit_min",
                    ),
                    "engineering_limit_max": _as_float(
                        record["engineering_limit_max"],
                        f"measurement_points.{asset_type}[{index}].engineering_limit_max",
                    ),
                }
            )
        points[asset_type] = records

    return {
        "schema_version": _as_str(raw["schema_version"], "measurement_points.schema_version"),
        "asset_types": _as_str_list(raw["asset_types"], "measurement_points.asset_types"),
        "measurement_points": points,
    }


def load_synthetic_scenario_config() -> SyntheticScenarioConfig:
    raw = load_yaml_config(CONFIG_DIR / "synthetic_scenarios.yml")
    timelines_raw = _as_mapping(raw["failure_timelines"], "synthetic_scenarios.failure_timelines")
    timelines = {}

    for name, value in timelines_raw.items():
        timeline = _as_mapping(value, f"synthetic_scenarios.failure_timelines.{name}")
        phases = []
        for index, phase_value in enumerate(
            _as_sequence(timeline["phases"], f"synthetic_scenarios.failure_timelines.{name}.phases")
        ):
            phase = _as_mapping(phase_value, f"synthetic_scenarios.failure_timelines.{name}.phases[{index}]")
            phases.append(
                {
                    "state": _as_str(
                        phase["state"], f"synthetic_scenarios.failure_timelines.{name}.phases[{index}].state"
                    ),
                    "start_day": _as_int(
                        phase["start_day"], f"synthetic_scenarios.failure_timelines.{name}.phases[{index}].start_day"
                    ),
                    "end_day": _as_int(
                        phase["end_day"], f"synthetic_scenarios.failure_timelines.{name}.phases[{index}].end_day"
                    ),
                }
            )
        timelines[name] = {
            "asset_type": _as_str(timeline["asset_type"], f"synthetic_scenarios.failure_timelines.{name}.asset_type"),
            "failure_mode_id": _as_str(
                timeline["failure_mode_id"], f"synthetic_scenarios.failure_timelines.{name}.failure_mode_id"
            ),
            "component_type": _as_str(
                timeline["component_type"], f"synthetic_scenarios.failure_timelines.{name}.component_type"
            ),
            "phases": phases,
        }

    return {
        "schema_version": _as_str(raw["schema_version"], "synthetic_scenarios.schema_version"),
        "default_start_time": _as_str(raw["default_start_time"], "synthetic_scenarios.default_start_time"),
        "default_days": _as_int(raw["default_days"], "synthetic_scenarios.default_days"),
        "operating_states": _as_str_list(raw["operating_states"], "synthetic_scenarios.operating_states"),
        "failure_timelines": timelines,
    }


def _plant_slug(plant_id: str) -> str:
    return plant_id.removeprefix("plant_")


def build_tag_id(plant_id: str, asset_index: int, metric_name: str) -> str:
    return f"tag_{_plant_slug(plant_id)}_{asset_index:04d}_{metric_name}"


def build_measurement_tag_id(plant_id: str, asset_index: int, measurement_point_id: str, tag_suffix: str) -> str:
    return f"tag_{_plant_slug(plant_id)}_{asset_index:04d}_{measurement_point_id}_{tag_suffix}"


def build_asset_id(plant_id: str, asset_index: int) -> str:
    return f"asset_{_plant_slug(plant_id)}_{asset_index:04d}"


def build_asset_name(asset_type: str, asset_index: int) -> str:
    return f"{asset_type.replace('_', ' ').title()} {asset_index:04d}"


def build_component_id(tenant_id: str, asset_id: str, component_type: str, ordinal: int = 1) -> str:
    digest = hashlib.sha1(f"{tenant_id}|{asset_id}|{component_type}|{ordinal}".encode()).hexdigest()[:16]
    return f"comp_{digest}"


def _legacy_measurement_point(asset_type: str, metric_name: str, unit: str) -> dict[str, object]:
    digest = hashlib.sha1(f"{asset_type}|generic_component|{metric_name}".encode()).hexdigest()[:10]
    defaults = {
        "temperature": (0.0, 100.0, -40.0, 180.0),
        "vibration": (0.0, 5.0, 0.0, 25.0),
        "pressure": (0.0, 10.0, 0.0, 40.0),
        "flow_rate": (0.0, 200.0, 0.0, 400.0),
        "power_draw": (0.0, 500.0, 0.0, 1200.0),
    }
    normal_min, normal_max, limit_min, limit_max = defaults.get(metric_name, (0.0, 100.0, 0.0, 1000.0))
    return {
        "asset_type": asset_type,
        "component_type": "generic_component",
        "measurement_point": metric_name,
        "metric_name": metric_name,
        "tag_suffix": f"{metric_name}_{unit}",
        "unit": unit,
        "sampling_rate_hz": 1.0,
        "normal_range_min": normal_min,
        "normal_range_max": normal_max,
        "engineering_limit_min": limit_min,
        "engineering_limit_max": limit_max,
        "measurement_point_id": f"mp_{asset_type}_{digest}",
    }


def _measurement_point_id(asset_type: str, component_type: str, measurement_point: str) -> str:
    digest = hashlib.sha1(f"{asset_type}|{component_type}|{measurement_point}".encode()).hexdigest()[:10]
    return f"mp_{asset_type}_{digest}"


def _measurement_points_for_asset(asset_type: str) -> list[dict[str, object]]:
    config = load_measurement_point_config()
    if asset_type not in config["measurement_points"]:
        raise ValueError(f"Unknown asset_type for measurement points: {asset_type}")
    return [
        {
            **point,
            "asset_type": asset_type,
            "measurement_point_id": _measurement_point_id(
                asset_type,
                point["component_type"],
                point["measurement_point"],
            ),
        }
        for point in config["measurement_points"][asset_type]
    ]


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
                try:
                    points = _measurement_points_for_asset(asset_type)
                except ValueError:
                    points = [
                        _legacy_measurement_point(asset_type, metric_name, metric_units[metric_name])
                        for metric_name in archetype_config["expected_metrics"]
                        if metric_units[metric_name] in archetype_config["units"]
                    ]
                for point in points:
                    metric_name = str(point["metric_name"])
                    unit = str(point["unit"])
                    if metric_name not in archetype_config["expected_metrics"] or unit not in archetype_config["units"]:
                        continue
                    component_type = str(point["component_type"])
                    measurement_point_id = str(point["measurement_point_id"])
                    tag_suffix = str(point["tag_suffix"])
                    tag_id = build_measurement_tag_id(plant_id, asset_index, measurement_point_id, tag_suffix)
                    if tag_id in mappings and mappings[tag_id]["tenant_id"] != tenant_id:
                        raise ValueError(f"tag_id maps across tenants: {tag_id}")
                    component_id = build_component_id(tenant_id, asset_id, component_type)
                    _sampling = cast(float, point["sampling_rate_hz"])
                    _norm_min = cast(float, point["normal_range_min"])
                    _norm_max = cast(float, point["normal_range_max"])
                    _eng_min = cast(float, point["engineering_limit_min"])
                    _eng_max = cast(float, point["engineering_limit_max"])
                    mappings[tag_id] = {
                        "tenant_id": tenant_id,
                        "plant_id": plant_id,
                        "asset_id": asset_id,
                        "asset_name": asset_name,
                        "asset_type": asset_type,
                        "component_id": component_id,
                        "component_type": component_type,
                        "measurement_point_id": measurement_point_id,
                        "measurement_point": str(point["measurement_point"]),
                        "tag_id": tag_id,
                        "tag_name": f"{asset_type}_{point['measurement_point']}_{tag_suffix}_{asset_index:04d}",
                        "metric_name": metric_name,
                        "unit": unit,
                        "sampling_rate_hz": _sampling,
                        "normal_range_min": _norm_min,
                        "normal_range_max": _norm_max,
                        "engineering_limit_min": _eng_min,
                        "engineering_limit_max": _eng_max,
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
        "mart_realtime_diagnosis": _as_str(
            topics_raw["mart_realtime_diagnosis"], "streaming.topics.mart_realtime_diagnosis"
        ),
    }
    return {
        "schema_version": _as_str(raw["schema_version"], "streaming.schema_version"),
        "broker": {
            "bootstrap_servers": _as_str(broker["bootstrap_servers"], "streaming.broker.bootstrap_servers"),
            "redpanda_admin": _as_str(broker["redpanda_admin"], "streaming.broker.redpanda_admin"),
        },
        "flink": {
            "jobmanager_ui": _as_str(flink["jobmanager_ui"], "streaming.flink.jobmanager_ui"),
            "watermark_delay_minutes": _as_int(
                flink["watermark_delay_minutes"], "streaming.flink.watermark_delay_minutes"
            ),
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
