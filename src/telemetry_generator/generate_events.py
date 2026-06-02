from __future__ import annotations

import argparse
import hashlib
import json
import random
from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from src.telemetry_generator.config import (
    build_asset_id,
    build_tag_id,
    build_tag_metadata,
    load_asset_config,
    load_scenario_config,
)
from src.telemetry_generator.models import (
    ArchetypeConfig,
    AssetConfig,
    GenerationResult,
    OutputPaths,
    RawSensorEvent,
    ScenarioProfile,
    TenantConfig,
)
from src.telemetry_generator.scenarios import load_scenarios, resolve_profile
from src.telemetry_generator.signal_profiles import OperatingState, SignalContext, signal_value

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


def _industrial_state(day: int) -> tuple[OperatingState, str]:
    if day < 10:
        return OperatingState.NORMAL, "baseline"
    if day < 22:
        return OperatingState.DEGRADATION, "early_degradation"
    if day < 26:
        return OperatingState.WARNING, "warning"
    if day < 28:
        return OperatingState.CRITICAL, "critical"
    if day < 29:
        return OperatingState.MAINTENANCE, "maintenance"
    return OperatingState.NORMAL, "recovery"


def _failure_mode_for_metric(metric_name: str) -> str:
    if metric_name == "flow_rate":
        return "fm_pump_cavitation"
    if metric_name == "pressure":
        return "fm_seal_leakage"
    if metric_name == "temperature":
        return "fm_motor_winding_overheat"
    return "fm_bearing_inner_race_wear"


def generate_industrial_events(seed: int) -> GenerationResult:
    all_tags = list(build_tag_metadata().values())
    if not all_tags:
        raise RuntimeError("build_tag_metadata returned zero tags")
    start = _parse_utc("2026-01-01T00:00:00Z")
    events: list[RawSensorEvent] = []

    for _tag_index, metadata in enumerate(all_tags):
        failure_mode_id = _failure_mode_for_metric(metadata["metric_name"])
        scenario_end = start + timedelta(days=30)
        for hour_offset in range(30 * 24):
            timestamp = start + timedelta(hours=hour_offset)
            day = hour_offset // 24
            operating_state, _degradation_stage = _industrial_state(day)
            context = SignalContext(
                metric_name=metadata["metric_name"],
                normal_min=metadata["normal_range_min"],
                normal_max=metadata["normal_range_max"],
                engineering_min=metadata["engineering_limit_min"],
                engineering_max=metadata["engineering_limit_max"],
                failure_mode_id=failure_mode_id,
                operating_state=operating_state,
                scenario_start=start,
                scenario_end=scenario_end,
                seed=f"{seed}|{metadata['asset_id']}|{metadata['tag_id']}",
            )
            event_time = _format_utc(timestamp)
            quality_flags: list[str] = []
            if operating_state in {OperatingState.WARNING, OperatingState.CRITICAL}:
                quality_flags.append("threshold_breach")
            if operating_state == OperatingState.DEGRADATION:
                quality_flags.append("degraded_signal")
            event_id = _stable_event_id(
                [
                    "industrial_demo",
                    str(seed),
                    metadata["tenant_id"],
                    metadata["tag_id"],
                    str(hour_offset),
                ]
            )
            events.append(
                RawSensorEvent(
                    event_id=event_id,
                    schema_version="raw_sensor_event.v1",
                    tenant_id=metadata["tenant_id"],
                    tag_id=metadata["tag_id"],
                    tag_name=metadata["tag_name"],
                    event_time=event_time,
                    ingest_time=event_time,
                    value=signal_value(context, timestamp),
                    unit=metadata["unit"],
                    source_system="historian",
                    quality_flags=quality_flags,
                    scenario=operating_state.value,
                    synthetic_metadata={
                        "generator_version": "2.0",
                        "producer_mode": "backfill_replay",
                        "original_sampling_rate_hz": 1.0,
                        "replay_acceleration": 900,
                        "operating_state": operating_state.value,
                    },
                )
            )

    tenants = sorted({metadata["tenant_id"] for metadata in all_tags})
    plants = sorted({metadata["plant_id"] for metadata in all_tags})
    profile = ScenarioProfile(
        name="industrial_demo",
        tenant_count=len(tenants),
        plants_per_tenant=len(plants) // max(len(tenants), 1),
        assets_per_plant=10,
        total_assets=len(tenants) * 3 * 10,
        metric_types=sorted({metadata["metric_name"] for metadata in all_tags}),
        anomaly_scenarios=["degradation", "warning", "critical", "maintenance"],
        scenarios=["normal", "degradation", "warning", "critical", "maintenance"],
        start_time="2026-01-01T00:00:00Z",
        periods_per_metric=30 * 24,
    )
    return GenerationResult(profile=profile, valid_events=events, invalid_events=[])


def _valid_scenarios_for_archetype(
    profile_scenarios: list[str],
    archetype_config: ArchetypeConfig,
) -> list[str]:
    plausible = set(archetype_config["plausible_scenarios"])
    return [name for name in profile_scenarios if name != "invalid_event" and name in plausible]


def generate_events(profile_name: str, seed: int) -> GenerationResult:
    if profile_name == "industrial_demo":
        return generate_industrial_events(seed)

    scenario_config = load_scenario_config()
    asset_config = load_asset_config()
    scenario_inventory = load_scenarios()
    profile = resolve_profile(profile_name, scenario_config, asset_config)
    metric_units = _metric_units(asset_config)
    tag_metadata_by_id = build_tag_metadata(asset_config)
    rng = random.Random(seed)
    start = _parse_utc(profile.start_time)

    events: list[RawSensorEvent] = []
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
                asset_id = build_asset_id(plant_id, asset_index)
                asset_type = _asset_type(archetype_config, asset_index - 1)
                threshold_profile_id = str(archetype_config["threshold_profile_id"])
                asset_tags = [
                    metadata
                    for metadata in tag_metadata_by_id.values()
                    if metadata["tenant_id"] == tenant_id
                    and metadata["plant_id"] == plant_id
                    and metadata["asset_id"] == asset_id
                    and metadata["metric_name"] in metric_names
                ]
                for metric_index, tag_metadata in enumerate(asset_tags):
                    metric_name = tag_metadata["metric_name"]
                    tag_id = tag_metadata["tag_id"]
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
                                tag_id,
                                metric_name,
                                scenario_name,
                                str(period),
                            ]
                        )
                        events.append(
                            RawSensorEvent(
                                event_id=event_id,
                                schema_version="raw_sensor_event.v1",
                                tenant_id=tenant_id,
                                tag_id=tag_id,
                                tag_name=tag_metadata["tag_name"],
                                event_time=event_time,
                                ingest_time=ingest_time,
                                value=value,
                                unit=metric_units[metric_name],
                                source_system=source_system,
                                quality_flags=quality_flags,
                                scenario=scenario_name,
                                synthetic_metadata={
                                    "generator_version": "1.0",
                                    "scenario": scenario_name,
                                    "seed_asset_index": asset_index,
                                },
                            )
                        )
                        ordinal += 1

                if "invalid_event" in profile.scenarios and asset_index == 1:
                    invalid_events.append(
                        {
                            "event_id": _stable_event_id(
                                [profile.name, str(seed), tenant_id, plant_id, asset_id, "invalid_missing_value"]
                            ),
                            "schema_version": "raw_sensor_event.v1",
                            "tenant_id": tenant_id,
                            "tag_id": build_tag_id(plant_id, asset_index, "temperature"),
                            "tag_name": f"{asset_type}_temperature_{asset_index:04d}",
                            "event_time": _format_utc(start),
                            "ingest_time": _format_utc(start),
                            "unit": "celsius",
                            "source_system": source_system,
                            "quality_flags": ["missing_required_field"],
                            "scenario": "invalid_event",
                            "violation_type": "missing_field",
                            "violation_detail": "Missing required field: value",
                            "asset_type": asset_type,
                            "threshold_profile_id": threshold_profile_id,
                        }
                    )
                    invalid_events.append(
                        {
                            "event_id": _stable_event_id(
                                [profile.name, str(seed), tenant_id, plant_id, asset_id, "invalid_unit"]
                            ),
                            "schema_version": "raw_sensor_event.v1",
                            "tenant_id": tenant_id,
                            "tag_id": build_tag_id(plant_id, asset_index, "temperature"),
                            "tag_name": f"{asset_type}_temperature_{asset_index:04d}",
                            "event_time": _format_utc(start),
                            "ingest_time": _format_utc(start),
                            "value": round(BASE_VALUES["temperature"] + (asset_index % 7), 3),
                            "unit": "bananas",
                            "source_system": source_system,
                            "quality_flags": ["invalid_unit"],
                            "scenario": "invalid_event",
                            "violation_type": "unit_anomaly",
                            "violation_detail": "Invalid unit: bananas",
                            "asset_type": asset_type,
                            "threshold_profile_id": threshold_profile_id,
                        }
                    )
                    invalid_events.append(
                        {
                            "event_id": _stable_event_id(
                                [profile.name, str(seed), tenant_id, plant_id, asset_id, "invalid_contract"]
                            ),
                            "schema_version": "raw_sensor_event.v99",
                            "tenant_id": tenant_id,
                            "tag_id": build_tag_id(plant_id, asset_index, "temperature"),
                            "tag_name": f"{asset_type}_temperature_{asset_index:04d}",
                            "event_time": _format_utc(start),
                            "ingest_time": _format_utc(start),
                            "value": round(BASE_VALUES["temperature"] + (asset_index % 7), 3),
                            "unit": "celsius",
                            "source_system": source_system,
                            "quality_flags": ["invalid_contract"],
                            "scenario": "invalid_event",
                            "violation_type": "contract_violation",
                            "violation_detail": "Invalid schema_version: raw_sensor_event.v99",
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
    valid_events_path = generated_dir / f"raw_sensor_events_{result.profile.name}.jsonl"
    invalid_events_path = generated_dir / f"raw_sensor_events_invalid_{result.profile.name}.jsonl"
    _write_jsonl(valid_events_path, list(result.valid_events))
    _write_jsonl(invalid_events_path, result.invalid_events)
    return OutputPaths(valid_events_path=valid_events_path, invalid_events_path=invalid_events_path)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate deterministic industrial telemetry JSONL.")
    _ = parser.add_argument("--profile", choices=["smoke", "demo", "industrial_demo"], default="smoke")
    _ = parser.add_argument("--seed", type=int, default=42)
    _ = parser.add_argument("--output-dir", type=Path, default=Path("data"))
    return parser


def _parse_args(argv: list[str] | None) -> GenerateCliArgs:
    namespace = GenerateCliNamespace(profile="smoke", seed=42, output_dir=Path("data"))
    _ = build_parser().parse_args(argv, namespace=namespace)
    return GenerateCliArgs(profile=namespace.profile, seed=namespace.seed, output_dir=namespace.output_dir)


def _write_summary(result: GenerationResult, _output_dir: Path) -> Path:
    from src.telemetry_generator.config import REPO_ROOT, build_tag_metadata, load_asset_config

    reports_dir = REPO_ROOT / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    tag_meta = build_tag_metadata(load_asset_config())
    tag_to_metric: dict[str, str] = {tid: meta["metric_name"] for tid, meta in tag_meta.items()}

    by_scenario: Counter[str] = Counter()
    by_tenant: Counter[str] = Counter()
    by_metric: Counter[str] = Counter()
    all_times: list[str] = []

    for ev in result.valid_events:
        by_scenario[str(ev.get("scenario", "unknown"))] += 1
        by_tenant[str(ev["tenant_id"])] += 1
        by_metric[tag_to_metric.get(str(ev["tag_id"]), "unknown")] += 1
        all_times.append(str(ev["event_time"]))

    for ev in result.invalid_events:
        by_scenario[str(ev.get("scenario", "unknown"))] += 1
        tenant = str(ev.get("tenant_id", "unknown"))
        by_tenant[tenant] += 1
        tid = str(ev.get("tag_id", ""))
        metric = tag_to_metric.get(tid, "unknown")
        if metric == "unknown":
            parts = tid.split("_")
            if len(parts) >= 2 and parts[0] == "tag":
                metric = parts[-1]
        by_metric[metric] += 1
        all_times.append(str(ev.get("event_time", "")))

    time_start = min(all_times) if all_times else ""
    time_end = max(all_times) if all_times else ""

    summary: dict[str, object] = {
        "total_events": len(result.valid_events) + len(result.invalid_events),
        "valid_events": len(result.valid_events),
        "invalid_events": len(result.invalid_events),
        "by_scenario": dict(by_scenario),
        "by_tenant": dict(by_tenant),
        "by_metric": dict(by_metric),
        "time_range": {"start": time_start, "end": time_end},
    }

    summary_path = reports_dir / "phase1-historian-generation-summary.json"
    with summary_path.open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2, ensure_ascii=False)
    return summary_path


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    result = generate_events(profile_name=args.profile, seed=args.seed)
    paths = write_profile_output(result, args.output_dir)
    print(f"wrote {len(result.valid_events)} valid events to {paths.valid_events_path}")
    print(f"wrote {len(result.invalid_events)} invalid candidate events to {paths.invalid_events_path}")
    summary_path = _write_summary(result, args.output_dir)
    print(f"wrote generation summary to {summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
