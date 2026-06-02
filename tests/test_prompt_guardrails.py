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


def test_unsupported_runbook_ref_is_quarantined() -> None:
    evidence = _evidence()
    rec = generate_fallback_recommendation(evidence)
    rec["runbook_references"] = ["unsupported_runbook#section"]
    result = validate_recommendation(rec, evidence)
    assert not result.valid
    assert "unsupported_runbook_ref" in (result.quarantine_reason or "")


def test_missing_schema_version_is_quarantined() -> None:
    evidence = _evidence()
    rec = generate_fallback_recommendation(evidence)
    del rec["schema_version"]
    result = validate_recommendation(rec, evidence)
    assert not result.valid


def test_schema_invalid_confidence_label_is_quarantined() -> None:
    evidence = _evidence()
    rec = generate_fallback_recommendation(evidence)
    rec["confidence_label"] = "extreme"
    result = validate_recommendation(rec, evidence)
    assert not result.valid


def test_autonomous_diagnosis_wording_is_quarantined() -> None:
    evidence = _evidence()
    rec = generate_fallback_recommendation(evidence)
    rec["findings"] = ["This is an autonomous diagnosis of bearing failure."]
    result = validate_recommendation(rec, evidence)
    assert not result.valid
    assert "banned_autonomous_claim" in (result.quarantine_reason or "")


def test_definitive_root_cause_wording_is_quarantined() -> None:
    evidence = _evidence()
    rec = generate_fallback_recommendation(evidence)
    rec["recommended_checks"] = ["Definitive root cause: pump cavitation."]
    result = validate_recommendation(rec, evidence)
    assert not result.valid
    assert "banned_autonomous_claim" in (result.quarantine_reason or "")


def test_automatic_remediation_wording_is_quarantined() -> None:
    evidence = _evidence()
    rec = generate_fallback_recommendation(evidence)
    rec["summary"] = "Alert resolved by automatic remediation."
    result = validate_recommendation(rec, evidence)
    assert not result.valid
    assert "banned_autonomous_claim" in (result.quarantine_reason or "")


def test_non_json_provider_output_is_handled() -> None:
    from src.triage_service.openai_provider import generate_llm_recommendation

    def fake_transport(*, url: str, headers: dict[str, str], body: bytes, timeout: int) -> tuple[int, bytes]:
        import json as _json
        return 200, _json.dumps({"choices": [{"message": {"content": "not json"}}], "usage": {"prompt_tokens": 1, "completion_tokens": 1}}).encode()

    env = {"OPENAI_API_KEY": "sk-test", "OPENAI_MODEL": "gpt-4o-mini"}
    result = generate_llm_recommendation(_evidence(), env=env, transport=fake_transport)
    assert result["parsed_status"] == "non_json"
    assert result["recommendation"] is None

