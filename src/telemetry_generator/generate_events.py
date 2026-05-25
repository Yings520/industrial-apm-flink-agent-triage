from __future__ import annotations

import argparse
import hashlib
import json
import random
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from src.telemetry_generator.config import load_asset_config, load_scenario_config
from src.telemetry_generator.models import (
    ArchetypeConfig,
    AssetConfig,
    GenerationResult,
    OutputPaths,
    SensorEvent,
    TenantConfig,
)
from src.telemetry_generator.scenarios import load_scenarios, resolve_profile

BASE_VALUES = {
    "temperature": 68.0,
    "vibration": 2.4,
    "pressure": 6.0,
    "flow_rate": 120.0,
    "power_draw": 340.0,
}

SOURCE_BY_ARCHETYPE = {
    "campus_facilities": "bms",
    "industrial_asset_management_generic": "historian",
}


@dataclass(frozen=True)
class GenerateCliArgs:
    profile: str
    seed: int
    output_dir: Path


class GenerateCliNamespace(argparse.Namespace):
    profile: str = "smoke"
    seed: int = 42
    output_dir: Path = Path("data")


def _parse_utc(value: str) -> datetime:
    if not value.endswith("Z"):
        raise ValueError("UTC timestamp must end with Z")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("UTC timestamp must be timezone-aware")
    return parsed.astimezone(UTC)


def _format_utc(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _stable_event_id(parts: list[str]) -> str:
    digest = hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:20]
    return f"evt_{digest}"


def _select_tenants(asset_config: AssetConfig, tenant_count: int) -> list[TenantConfig]:
    return list(asset_config["tenants"])[:tenant_count]


def _asset_type(archetype_config: ArchetypeConfig, index: int) -> str:
    asset_types = list(archetype_config["asset_types"])
    return asset_types[index % len(asset_types)]


def _metric_units(asset_config: AssetConfig) -> dict[str, str]:
    return {metric["name"]: metric["unit"] for metric in asset_config["metric_types"]}


def _metrics_for_archetype(
    profile_metrics: list[str],
    archetype_config: ArchetypeConfig,
    metric_units: dict[str, str],
) -> list[str]:
    expected_metrics = set(archetype_config["expected_metrics"])
    archetype_units = set(archetype_config["units"])
    metric_names = [
        metric_name
        for metric_name in profile_metrics
        if metric_name in expected_metrics and metric_units[metric_name] in archetype_units
    ]
    if not metric_names:
        raise ValueError("Archetype has no metrics enabled for this profile")
    return metric_names


def _scenario_value(scenario_name: str, base: float, rng: random.Random, period: int) -> float:
    jitter = rng.uniform(-0.75, 0.75)
    if scenario_name == "flatline":
        return round(base, 3)
    if scenario_name == "spike":
        return round(base * 2.7 + jitter, 3)
    if scenario_name == "drift":
        return round(base + period * 1.25 + jitter, 3)
    if scenario_name == "missing_heartbeat":
        return round(base + jitter * 0.5, 3)
    if scenario_name == "late":
        return round(base + jitter, 3)
    return round(base + jitter, 3)


def _event_times(start: datetime, ordinal: int, scenario_name: str, period: int) -> tuple[str, str]:
    ingest_dt = start + timedelta(minutes=ordinal)
    if scenario_name == "late":
        event_dt = ingest_dt - timedelta(minutes=15)
    elif scenario_name == "missing_heartbeat":
        event_dt = ingest_dt + timedelta(minutes=period * 10)
        ingest_dt = event_dt
    else:
        event_dt = ingest_dt
    return _format_utc(event_dt), _format_utc(ingest_dt)


def _valid_scenarios_for_archetype(
    profile_scenarios: list[str],
    archetype_config: ArchetypeConfig,
) -> list[str]:
    plausible = set(archetype_config["plausible_scenarios"])
    return [name for name in profile_scenarios if name != "invalid_event" and name in plausible]


def generate_events(profile_name: str, seed: int) -> GenerationResult:
    scenario_config = load_scenario_config()
    asset_config = load_asset_config()
    scenario_inventory = load_scenarios()
    profile = resolve_profile(profile_name, scenario_config, asset_config)
    metric_units = _metric_units(asset_config)
    rng = random.Random(seed)
    start = _parse_utc(profile.start_time)

    events: list[SensorEvent] = []
    invalid_events: list[dict[str, object]] = []
    ordinal = 0

    for tenant in _select_tenants(asset_config, profile.tenant_count):
        tenant_id = str(tenant["tenant_id"])
        for plant in tenant["plants"][: profile.plants_per_tenant]:
            plant_id = str(plant["plant_id"])
            archetype = str(plant["archetype"])
            archetype_config = asset_config["archetypes"][archetype]
            source_system = SOURCE_BY_ARCHETYPE.get(archetype, "opc_ua")
            metric_names = _metrics_for_archetype(profile.metric_types, archetype_config, metric_units)
            valid_scenarios = _valid_scenarios_for_archetype(profile.scenarios, archetype_config)
            if "normal" not in valid_scenarios:
                valid_scenarios.insert(0, "normal")

            for asset_index in range(1, profile.assets_per_plant + 1):
                asset_id = f"asset_{plant_id.removeprefix('plant_')}_{asset_index:04d}"
                asset_type = _asset_type(archetype_config, asset_index - 1)
                threshold_profile_id = str(archetype_config["threshold_profile_id"])
                for metric_index, metric_name in enumerate(metric_names):
                    scenario_name = valid_scenarios[(asset_index + metric_index + seed) % len(valid_scenarios)]
                    for period in range(profile.periods_per_metric):
                        base = BASE_VALUES[metric_name] + (asset_index % 7) + metric_index
                        value = _scenario_value(scenario_name, base, rng, period)
                        event_time, ingest_time = _event_times(start, ordinal, scenario_name, period)
                        quality_flags = list(scenario_inventory[scenario_name]["quality_flags"])
                        event_id = _stable_event_id(
                            [
                                profile.name,
                                str(seed),
                                tenant_id,
                                plant_id,
                                asset_id,
                                metric_name,
                                scenario_name,
                                str(period),
                            ]
                        )
                        events.append(
                            SensorEvent(
                                event_id=event_id,
                                schema_version="sensor_event.v1",
                                tenant_id=tenant_id,
                                plant_id=plant_id,
                                asset_id=asset_id,
                                metric_name=metric_name,
                                event_time=event_time,
                                ingest_time=ingest_time,
                                value=value,
                                unit=metric_units[metric_name],
                                source_system=source_system,
                                quality_flags=quality_flags,
                                scenario=scenario_name,
                            )
                        )
                        ordinal += 1

                if "invalid_event" in profile.scenarios and asset_index == 1:
                    invalid_events.append(
                        {
                            "event_id": _stable_event_id(
                                [profile.name, str(seed), tenant_id, plant_id, asset_id, "invalid"]
                            ),
                            "schema_version": "sensor_event.v0",
                            "tenant_id": tenant_id,
                            "plant_id": plant_id,
                            "asset_id": asset_id,
                            "metric_name": "temperature",
                            "event_time": _format_utc(start),
                            "ingest_time": _format_utc(start),
                            "unit": "celsius",
                            "source_system": source_system,
                            "quality_flags": ["invalid_contract"],
                            "scenario": "invalid_event",
                            "asset_type": asset_type,
                            "threshold_profile_id": threshold_profile_id,
                        }
                    )

    return GenerationResult(profile=profile, valid_events=events, invalid_events=invalid_events)


def _write_jsonl(path: Path, records: Iterable[Mapping[str, object]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            _ = handle.write(json.dumps(record, sort_keys=True, separators=(",", ":")))
            _ = handle.write("\n")


def write_profile_output(result: GenerationResult, output_dir: Path) -> OutputPaths:
    generated_dir = output_dir / "generated"
    generated_dir.mkdir(parents=True, exist_ok=True)
    valid_events_path = generated_dir / f"sensor_events_{result.profile.name}.jsonl"
    invalid_events_path = generated_dir / f"invalid_events_{result.profile.name}.jsonl"
    _write_jsonl(valid_events_path, list(result.valid_events))
    _write_jsonl(invalid_events_path, result.invalid_events)
    return OutputPaths(valid_events_path=valid_events_path, invalid_events_path=invalid_events_path)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate deterministic industrial telemetry JSONL.")
    _ = parser.add_argument("--profile", choices=["smoke", "demo"], default="smoke")
    _ = parser.add_argument("--seed", type=int, default=42)
    _ = parser.add_argument("--output-dir", type=Path, default=Path("data"))
    return parser


def _parse_args(argv: list[str] | None) -> GenerateCliArgs:
    namespace = GenerateCliNamespace(profile="smoke", seed=42, output_dir=Path("data"))
    _ = build_parser().parse_args(argv, namespace=namespace)
    return GenerateCliArgs(profile=namespace.profile, seed=namespace.seed, output_dir=namespace.output_dir)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    result = generate_events(profile_name=args.profile, seed=args.seed)
    paths = write_profile_output(result, args.output_dir)
    print(f"wrote {len(result.valid_events)} valid events to {paths.valid_events_path}")
    print(f"wrote {len(result.invalid_events)} invalid candidate events to {paths.invalid_events_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
