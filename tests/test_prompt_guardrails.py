from src.serving.triage_evidence import build_triage_evidence
from src.triage_service.recommendations import generate_fallback_recommendation
from src.triage_service.validation import validate_recommendation


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
        sensor_window=[{"tenant_id": "tenant_northwind", "event_time": "2026-01-01T00:01:00Z"}],
        runbook_refs=[{"runbook_id": "generic_asset_triage", "section": "temperature"}],
    )


def test_cross_tenant_recommendation_is_quarantined() -> None:
    evidence = _evidence()
    rec = generate_fallback_recommendation(evidence)
    rec["tenant_id"] = "tenant_southridge"
    result = validate_recommendation(rec, evidence)
    assert not result.valid
    assert "cross_tenant" in (result.quarantine_reason or "")


def test_evidence_free_recommendation_is_quarantined() -> None:
    evidence = _evidence()
    rec = generate_fallback_recommendation(evidence)
    rec["findings"] = []
    result = validate_recommendation(rec, evidence)
    assert not result.valid
    assert "evidence_free" in (result.quarantine_reason or "")


def test_autonomous_claim_is_quarantined() -> None:
    evidence = _evidence()
    rec = generate_fallback_recommendation(evidence)
    rec["summary"] = "Definitive root cause found and automatic remediation executed."
    result = validate_recommendation(rec, evidence)
    assert not result.valid
    assert "banned_autonomous_claim" in (result.quarantine_reason or "")
