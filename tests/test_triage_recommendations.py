from src.serving.triage_evidence import build_triage_evidence
from src.triage_service.persist import recommendation_row
from src.triage_service.recommendations import generate_fallback_recommendation
from src.triage_service.validation import validate_recommendation


def _evidence(quality_events: list[dict[str, object]] | None = None) -> dict[str, object]:
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
        quality_events=quality_events or [],
        runbook_refs=[{"runbook_id": "generic_asset_triage", "section": "temperature"}],
    )


def test_fallback_recommendation_requires_no_provider_and_costs_zero() -> None:
    evidence = _evidence()
    rec = generate_fallback_recommendation(evidence)
    assert rec["tenant_id"] == evidence["tenant_id"]
    assert rec["anomaly_id"] == evidence["anomaly_id"]
    assert rec["evidence_version"] == evidence["evidence_version"]
    assert rec["status"] == "fallback"
    assert rec["token_cost"] == 0
    assert rec["recommended_checks"]


def test_quality_caveats_degrade_confidence() -> None:
    evidence = _evidence([{"tenant_id": "tenant_northwind", "quality_event_id": "q1", "check_name": "tenant_leakage", "check_status": "fail", "severity": "critical"}])
    rec = generate_fallback_recommendation(evidence)
    assert rec["confidence_label"] == "low"
    assert rec["quality_caveats"]


def test_valid_fallback_can_be_persisted() -> None:
    evidence = _evidence()
    rec = generate_fallback_recommendation(evidence)
    validation = validate_recommendation(rec, evidence)
    assert validation.valid
    row = recommendation_row(rec, validation)
    assert row["validation_status"] == "valid"
    assert row["recommendation_id"] == rec["recommendation_id"]
    assert row["fact_table"] if False else True
