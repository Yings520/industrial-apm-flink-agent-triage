import json
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import cast

import pytest

from src.telemetry_generator.generate_events import generate_events, write_profile_output
from src.telemetry_generator.schema_validation import validate_input_dir


def _valid_event() -> dict[str, object]:
    return {
        "event_id": "evt_test_0001",
        "schema_version": "raw_sensor_event.v1",
        "tenant_id": "tenant_test",
        "tag_id": "tag_test_0001_temperature",
        "tag_name": "pump_temperature_0001",
        "event_time": "2026-01-01T00:00:00Z",
        "ingest_time": "2026-01-01T00:00:05Z",
        "value": 72.1,
        "unit": "celsius",
        "source_system": "opc_ua",
        "quality_flags": [],
        "scenario": "normal",
    }


def _write_jsonl(path: Path, records: Iterable[Mapping[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    _ = path.write_text("\n".join(json.dumps(record) for record in records) + "\n")


def _read_jsonl(path: Path) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for line in path.read_text().splitlines():
        payload = cast(object, json.loads(line))
        if not isinstance(payload, dict):
            raise AssertionError("JSONL record must be an object")
        records.append(cast(dict[str, object], payload))
    return records


def _read_json_object(path: Path) -> dict[str, object]:
    payload = cast(object, json.loads(path.read_text()))
    if not isinstance(payload, dict):
        raise AssertionError("JSON payload must be an object")
    return cast(dict[str, object], payload)


def test_missing_tenant_id_is_rejected_with_structured_error(tmp_path: Path) -> None:
    bad_record = _valid_event()
    _ = bad_record.pop("tenant_id")
    _write_jsonl(tmp_path / "input" / "events.jsonl", [bad_record])

    result = validate_input_dir(tmp_path / "input", tmp_path / "output")

    rejected = _read_jsonl(result.rejected_path)
    assert result.exit_code == 1
    assert rejected[0]["original_record"] == bad_record
    assert rejected[0]["error_code"] == "missing_required_field"
    assert rejected[0]["field_path"] == "/tenant_id"
    assert rejected[0]["schema_name"] == "raw_sensor_event"
    assert rejected[0]["schema_version"] == "raw_sensor_event.v1"
    validation_time = rejected[0]["validation_time"]
    assert isinstance(validation_time, str)
    assert validation_time.endswith("Z")


def test_invalid_event_time_is_rejected_with_structured_error(tmp_path: Path) -> None:
    bad_record = _valid_event()
    bad_record["event_time"] = "2026-01-01 00:00:00"
    _write_jsonl(tmp_path / "input" / "events.jsonl", [bad_record])

    result = validate_input_dir(tmp_path / "input", tmp_path / "output")

    rejected = _read_jsonl(result.rejected_path)
    assert result.exit_code == 1
    assert rejected[0]["error_code"] == "invalid_format"
    assert rejected[0]["field_path"] == "/event_time"


def test_validation_collects_multiple_invalid_records(tmp_path: Path) -> None:
    missing_tenant = _valid_event()
    _ = missing_tenant.pop("tenant_id")
    bad_time = _valid_event()
    bad_time["event_id"] = "evt_test_0002"
    bad_time["event_time"] = "not-a-date"
    _write_jsonl(tmp_path / "input" / "events.jsonl", [missing_tenant, bad_time])

    result = validate_input_dir(tmp_path / "input", tmp_path / "output")
    summary = _read_json_object(result.summary_path)
    errors_by_code = summary["errors_by_code"]
    assert isinstance(errors_by_code, dict)
    error_counts = cast(dict[str, object], errors_by_code)

    assert result.exit_code == 1
    assert summary["total_records"] == 2
    assert summary["accepted_records"] == 0
    assert summary["rejected_records"] == 2
    assert error_counts["missing_required_field"] == 1
    assert error_counts["invalid_format"] == 1


def test_generator_to_validator_smoke_path_writes_artifacts(tmp_path: Path) -> None:
    generation = generate_events(profile_name="smoke", seed=42)
    _ = write_profile_output(generation, tmp_path)

    result = validate_input_dir(tmp_path / "generated", tmp_path)
    summary = _read_json_object(result.summary_path)

    assert result.exit_code == 1
    assert result.accepted_path.exists()
    assert result.rejected_path.exists()
    assert result.summary_path.exists()
    assert summary["total_records"] == len(generation.valid_events) + len(generation.invalid_events)
    assert summary["accepted_records"] == len(generation.valid_events)
    assert summary["rejected_records"] == len(generation.invalid_events)
    assert summary["schemas_checked"] == ["raw_sensor_event.schema.json"]

    accepted = _read_jsonl(result.accepted_path)
    assert "tag_id" in accepted[0]
    assert "asset_id" not in accepted[0]


def test_validation_can_filter_to_current_profile_files(tmp_path: Path) -> None:
    smoke = generate_events(profile_name="smoke", seed=42)
    demo = generate_events(profile_name="demo", seed=42)
    _ = write_profile_output(smoke, tmp_path)
    _ = write_profile_output(demo, tmp_path)

    result = validate_input_dir(tmp_path / "generated", tmp_path, profile_name="demo")
    summary = _read_json_object(result.summary_path)

    assert summary["total_records"] == len(demo.valid_events) + len(demo.invalid_events)
    assert summary["accepted_records"] == len(demo.valid_events)
    assert summary["rejected_records"] == len(demo.invalid_events)


def test_validation_fails_when_input_has_no_records(tmp_path: Path) -> None:
    result = validate_input_dir(tmp_path / "missing", tmp_path / "output")
    summary = _read_json_object(result.summary_path)
    errors_by_code = summary["errors_by_code"]
    assert isinstance(errors_by_code, dict)

    assert result.exit_code == 1
    assert summary["total_records"] == 0
    assert summary["accepted_records"] == 0
    assert summary["rejected_records"] == 0
    assert errors_by_code["no_input_records"] == 1


def test_profile_validation_requires_both_profile_files(tmp_path: Path) -> None:
    generation = generate_events(profile_name="demo", seed=42)
    paths = write_profile_output(generation, tmp_path)
    paths.invalid_events_path.unlink()

    with pytest.raises(FileNotFoundError, match="Missing profile input file"):
        validate_input_dir(tmp_path / "generated", tmp_path, profile_name="demo")
