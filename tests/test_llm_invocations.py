from src.triage_service.invocations import (
    invocation_id_for,
    invocation_row,
    request_hash_for,
)
from src.serving.triage_evidence import build_triage_evidence, evidence_json


def _evidence() -> dict[str, object]:
    return build_triage_evidence(
        tenant_id="tenant_northwind",
        anomaly={
            "tenant_id": "tenant_northwind",
            "anomaly_id": "anom_1",
            "asset_id": "asset_1",
            "tag_id": "tag_1",
            "metric_name": "temperature",
            "window_start": "2026-01-01T00:00:00Z",
            "window_end": "2026-01-01T00:05:00Z",
            "observed_value": 90,
            "baseline_value": 60,
            "expected_value": 65,
            "quality_flags": [],
            "source_event_ids": ["evt_1"],
        },
        sensor_window=[],
        runbook_refs=[{"runbook_id": "generic_asset_triage", "section": "temperature"}],
    )


def test_request_hash_is_deterministic() -> None:
    e1 = _evidence()
    e2 = _evidence()
    assert request_hash_for(e1) == request_hash_for(e2)


def test_request_hash_changes_with_different_evidence() -> None:
    e1 = _evidence()
    e2 = build_triage_evidence(
        tenant_id="tenant_northwind",
        anomaly={
            "tenant_id": "tenant_northwind",
            "anomaly_id": "anom_2",
            "asset_id": "asset_1",
            "tag_id": "tag_1",
            "metric_name": "vibration",
            "window_start": "2026-01-01T00:00:00Z",
            "window_end": "2026-01-01T00:05:00Z",
            "observed_value": 8,
            "baseline_value": 3,
            "expected_value": 4,
            "quality_flags": [],
            "source_event_ids": ["evt_2"],
        },
        sensor_window=[],
        runbook_refs=[{"runbook_id": "generic_asset_triage", "section": "vibration"}],
    )
    assert request_hash_for(e1) != request_hash_for(e2)


def test_invocation_id_is_deterministic() -> None:
    id1 = invocation_id_for("t1", "inc_1", "anom_1", "ev_1", "hash_abc")
    id2 = invocation_id_for("t1", "inc_1", "anom_1", "ev_1", "hash_abc")
    assert id1 == id2
    assert id1.startswith("inv_")


def test_invocation_row_contains_required_fields() -> None:
    row = invocation_row(
        tenant_id="t1",
        incident_id="inc_1",
        anomaly_id="anom_1",
        evidence_id="ev_1",
        provider="openai",
        model_name="gpt-4o-mini",
        prompt_version="triage_prompt.v1",
        raw_response="{}",
        parsed_status="parsed_json",
        validation_status="valid",
        quarantine_reason="",
        token_cost=0.001,
        latency_ms=500,
    )
    for field in (
        "invocation_id",
        "tenant_id",
        "incident_id",
        "anomaly_id",
        "evidence_id",
        "provider",
        "model_name",
        "prompt_version",
        "request_hash",
        "raw_response",
        "parsed_status",
        "validation_status",
        "quarantine_reason",
        "token_cost",
        "latency_ms",
        "created_at",
    ):
        assert field in row, f"missing {field}"


def test_invocation_row_preserves_raw_response() -> None:
    raw = '{"non_json": true}'
    row = invocation_row(
        tenant_id="t1",
        incident_id="inc_1",
        anomaly_id="anom_1",
        evidence_id="ev_1",
        provider="openai",
        model_name="gpt-4o-mini",
        prompt_version="triage_prompt.v1",
        raw_response=raw,
        parsed_status="non_json",
        validation_status="quarantined",
        quarantine_reason="non_json",
        token_cost=0,
        latency_ms=0,
    )
    assert row["raw_response"] == raw
    assert row["parsed_status"] == "non_json"
    assert row["quarantine_reason"] == "non_json"
