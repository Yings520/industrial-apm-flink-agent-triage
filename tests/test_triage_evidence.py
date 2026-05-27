import pytest

from src.serving.triage_evidence import build_triage_evidence


def _anomaly() -> dict[str, object]:
    return {
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
        "quality_flags": ["threshold_spike"],
        "source_event_ids": ["evt_2", "evt_1", "evt_2"],
    }


def test_build_triage_evidence_is_deterministic_and_bounded() -> None:
    evidence = build_triage_evidence(
        tenant_id="tenant_northwind",
        anomaly=_anomaly(),
        sensor_window=[{"tenant_id": "tenant_northwind", "event_time": "2026-01-01T00:01:00Z"}],
        quality_events=[{"tenant_id": "tenant_northwind", "quality_event_id": "q1"}],
        stream_health=[{"tenant_id": "tenant_northwind", "job_name": "flink"}],
        runbook_refs=[{"runbook_id": "generic_asset_triage", "section": "temperature"}],
    )
    assert evidence["tenant_id"] == "tenant_northwind"
    assert evidence["anomaly_id"] == "anom_1"
    assert evidence["source_event_ids"] == ["evt_1", "evt_2"]
    assert evidence["runbook_refs"] == [{"runbook_id": "generic_asset_triage", "section": "temperature"}]
    assert "finding" not in evidence
    assert "recommendation" not in evidence
    assert "recommended_action" not in evidence
    assert "root_cause" not in evidence


def test_build_triage_evidence_rejects_cross_tenant_context() -> None:
    with pytest.raises(ValueError, match="tenant mismatch"):
        build_triage_evidence(
            tenant_id="tenant_northwind",
            anomaly=_anomaly(),
            quality_events=[{"tenant_id": "tenant_southridge", "quality_event_id": "q1"}],
        )
