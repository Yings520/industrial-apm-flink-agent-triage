import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import cast

import pytest

from src.telemetry_generator.config import build_tag_metadata
from src.telemetry_generator.generate_events import _parse_utc, generate_events, write_profile_output


REQUIRED_EVENT_FIELDS = {
    "event_id",
    "schema_version",
    "tenant_id",
    "tag_id",
    "tag_name",
    "event_time",
    "ingest_time",
    "value",
    "unit",
    "source_system",
    "quality_flags",
}


def test_generate_smoke_events_include_required_fields() -> None:
    result = generate_events(profile_name="smoke", seed=42)

    assert result.valid_events
    assert REQUIRED_EVENT_FIELDS.issubset(result.valid_events[0])
    assert "asset_id" not in result.valid_events[0]
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
    }.issubset({event.get("scenario", "") for event in result.valid_events})
    assert {event.get("scenario", "") for event in result.invalid_events} == {"invalid_event"}


def test_generation_uses_archetype_expected_metrics() -> None:
    result = generate_events(profile_name="demo", seed=42)
    tag_metadata = build_tag_metadata()

    manufacturing_tags = {
        event["tag_id"]
        for event in result.valid_events
        if str(event["tag_id"]).startswith("tag_northwind_mfg_01_")
    }

    assert manufacturing_tags
    assert {tag_metadata[tag]["metric_name"] for tag in manufacturing_tags}.issubset(
        {"temperature", "vibration", "power_draw"}
    )


def test_missing_heartbeat_scenario_creates_event_time_gap() -> None:
    result = generate_events(profile_name="demo", seed=42)
    grouped_times: dict[tuple[object, object], list[str]] = {}
    for event in result.valid_events:
        if event.get("scenario") == "missing_heartbeat":
            key = (event["tenant_id"], event["tag_id"])
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
        _ = _parse_utc("2026-01-01T00:00:00")


def test_generated_raw_tags_have_unique_tenant_mapping() -> None:
    result = generate_events(profile_name="demo", seed=42)
    tags_by_tenant: dict[str, set[str]] = {}
    for event in result.valid_events:
        tags_by_tenant.setdefault(event["tag_id"], set()).add(event["tenant_id"])

    assert tags_by_tenant
    assert all(len(tenants) == 1 for tenants in tags_by_tenant.values())


def test_industrial_demo_generates_measurement_point_raw_events() -> None:
    result = generate_events(profile_name="industrial_demo", seed=42)

    first = result.valid_events[0]
    metadata = first.get("synthetic_metadata")
    assert metadata is not None, "industrial_demo events must have synthetic_metadata"
    assert first["schema_version"] == "raw_sensor_event.v1"
    assert "asset_id" not in first
    assert isinstance(metadata, dict)
    assert metadata["producer_mode"] == "backfill_replay"
    assert str(metadata["measurement_point_id"]).startswith("mp_")
    assert metadata["operating_state"] in {
        "normal",
        "degradation",
        "warning",
        "critical",
        "maintenance",
    }


def test_industrial_demo_event_time_spans_30_days() -> None:
    result = generate_events(profile_name="industrial_demo", seed=42)
    event_times = [
        datetime.fromisoformat(event["event_time"].replace("Z", "+00:00")) for event in result.valid_events
    ]

    assert max(event_times) - min(event_times) >= timedelta(days=29)
