import json
from pathlib import Path

from src.flink_jobs.replay_verification import compare_jsonl, normalize


def _record(anomaly_id: str, window_start: str) -> dict[str, object]:
    return {
        "anomaly_id": anomaly_id,
        "schema_version": "anomaly_event.v1",
        "rule_id": "threshold_spike",
        "tenant_id": "tenant_northwind",
        "plant_id": "plant_northwind_mfg_01",
        "asset_id": "asset_northwind_mfg_01_0001",
        "asset_name": "Cnc Machine 0001",
        "tag_id": "tag_northwind_mfg_01_0001_temperature",
        "tag_name": "cnc_machine_temperature_0001",
        "metric_name": "temperature",
        "observed_value": 10.0,
        "baseline_value": 2.0,
        "expected_value": 5.0,
        "event_count": 3,
        "breach_count": 1,
        "window_start": window_start,
        "window_end": "2026-01-01T00:05:00Z",
        "severity": "critical",
        "quality_flags": [],
        "detection_confidence": 0.9,
        "source_event_ids": ["evt_1"],
    }


def _write_jsonl(path: Path, records: list[dict[str, object]]) -> None:
    path.write_text("\n".join(json.dumps(record) for record in records) + "\n", encoding="utf-8")


def test_normalize_sorts_by_deterministic_keys() -> None:
    records = [_record("anom_b", "2026-01-01T00:02:00Z"), _record("anom_a", "2026-01-01T00:01:00Z")]

    normalized = normalize(records)

    assert [record["anomaly_id"] for record in normalized] == ["anom_a", "anom_b"]


def test_compare_jsonl_ignores_runtime_output_order(tmp_path: Path) -> None:
    first = [_record("anom_b", "2026-01-01T00:02:00Z"), _record("anom_a", "2026-01-01T00:01:00Z")]
    second = list(reversed(first))
    actual = tmp_path / "actual.jsonl"
    expected = tmp_path / "expected.jsonl"
    _write_jsonl(actual, first)
    _write_jsonl(expected, second)

    assert compare_jsonl(actual, expected) is True

