import json
from datetime import datetime
from pathlib import Path
from typing import cast

import pytest

from src.telemetry_generator.generate_events import _parse_utc, generate_events, write_profile_output


REQUIRED_EVENT_FIELDS = {
    "event_id",
    "schema_version",
    "tenant_id",
    "plant_id",
    "asset_id",
    "metric_name",
    "event_time",
    "ingest_time",
    "value",
    "unit",
    "source_system",
    "quality_flags",
    "scenario",
}


def test_generate_smoke_events_include_required_fields() -> None:
    result = generate_events(profile_name="smoke", seed=42)

    assert result.valid_events
    assert REQUIRED_EVENT_FIELDS.issubset(result.valid_events[0])
    assert result.invalid_events


def test_generation_is_deterministic_for_same_seed() -> None:
    first = generate_events(profile_name="smoke", seed=42)
    second = generate_events(profile_name="smoke", seed=42)

    assert first.valid_events == second.valid_events
    assert first.invalid_events == second.invalid_events


def test_write_profile_output_creates_jsonl_files(tmp_path: Path) -> None:
    result = generate_events(profile_name="smoke", seed=42)

    paths = write_profile_output(result, tmp_path)

    assert paths.valid_events_path.exists()
    assert paths.invalid_events_path.exists()
    first_payload = cast(object, json.loads(paths.valid_events_path.read_text().splitlines()[0]))
    if not isinstance(first_payload, dict):
        raise AssertionError("generated JSONL record must be an object")
    first_record = cast(dict[str, object], first_payload)
    assert REQUIRED_EVENT_FIELDS.issubset(first_record)


def test_demo_generation_covers_full_scale_and_scenarios() -> None:
    result = generate_events(profile_name="demo", seed=42)

    assert result.profile.total_assets == 180
    assert len(result.profile.metric_types) == 5
    assert len(result.profile.anomaly_scenarios) == 5
    assert {
        "normal",
        "late",
        "missing_heartbeat",
        "flatline",
        "spike",
        "drift",
    }.issubset({event["scenario"] for event in result.valid_events})
    assert {event["scenario"] for event in result.invalid_events} == {"invalid_event"}


def test_generation_uses_archetype_expected_metrics() -> None:
    result = generate_events(profile_name="demo", seed=42)

    manufacturing_metrics = {
        event["metric_name"]
        for event in result.valid_events
        if event["plant_id"] == "plant_northwind_mfg_01"
    }

    assert manufacturing_metrics == {"temperature", "vibration", "power_draw"}


def test_missing_heartbeat_scenario_creates_event_time_gap() -> None:
    result = generate_events(profile_name="demo", seed=42)
    grouped_times: dict[tuple[object, object], list[str]] = {}
    for event in result.valid_events:
        if event["scenario"] == "missing_heartbeat":
            key = (event["asset_id"], event["metric_name"])
            grouped_times.setdefault(key, []).append(event["event_time"])

    assert grouped_times
    assert any(
        len(times) >= 2
        and (
            datetime.fromisoformat(times[1].replace("Z", "+00:00"))
            - datetime.fromisoformat(times[0].replace("Z", "+00:00"))
        ).total_seconds()
        >= 600
        for times in grouped_times.values()
    )


def test_parse_utc_rejects_non_z_timestamp() -> None:
    with pytest.raises(ValueError, match="must end with Z"):
        _parse_utc("2026-01-01T00:00:00")
