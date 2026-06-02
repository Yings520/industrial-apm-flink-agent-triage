from src.flink_agents.output_validator import validate_diagnosis


def _evidence() -> dict[str, object]:
    return {
        "tenant_id": "tenant_northwind",
        "anomaly_id": "anom_001",
        "evidence_version": "triage_evidence.v1",
        "runbook_refs": [
            {"runbook_id": "generic_asset_triage", "section": "temperature"},
        ],
    }


def _valid_diagnosis(**overrides: object) -> dict[str, object]:
    diag: dict[str, object] = {
        "tenant_id": "tenant_northwind",
        "anomaly_id": "anom_001",
        "evidence_version": "triage_evidence.v1",
        "summary": "Temperature anomaly observed in the selected window.",
        "findings": ["Observed value 95.0 vs expected 65.0."],
        "possible_causes": ["Hypothesis: sensor or process condition changed."],
        "recommended_checks": ["Check recent sensor readings.", "Review asset runbook."],
        "runbook_references": ["generic_asset_triage#temperature"],
        "confidence_label": "medium",
        "quality_caveats": [],
    }
    diag.update(overrides)
    return diag


class TestOutputValidator:
    def test_consistent_pass(self) -> None:
        result = validate_diagnosis(_valid_diagnosis(), _evidence())
        assert result["status"] == "valid"

    def test_inconsistent_fail_cross_tenant(self) -> None:
        diagnosis = _valid_diagnosis(tenant_id="tenant_southridge")
        result = validate_diagnosis(diagnosis, _evidence())
        assert result["status"] == "quarantined"
        assert "cross_tenant" in result["failure_reasons"]

    def test_inconsistent_fail_anomaly_mismatch(self) -> None:
        diagnosis = _valid_diagnosis(anomaly_id="anom_999")
        result = validate_diagnosis(diagnosis, _evidence())
        assert result["status"] == "quarantined"
        assert "anomaly_mismatch" in result["failure_reasons"]

    def test_inconsistent_fail_evidence_version_mismatch(self) -> None:
        diagnosis = _valid_diagnosis(evidence_version="wrong_version")
        result = validate_diagnosis(diagnosis, _evidence())
        assert result["status"] == "quarantined"
        assert "evidence_version_mismatch" in result["failure_reasons"]

    def test_empty_checks_fail(self) -> None:
        diagnosis = _valid_diagnosis(recommended_checks=[])
        result = validate_diagnosis(diagnosis, _evidence())
        assert result["status"] == "quarantined"
        assert "empty_recommended_checks" in result["failure_reasons"]

    def test_empty_findings_fail(self) -> None:
        diagnosis = _valid_diagnosis(findings=[], possible_causes=[])
        result = validate_diagnosis(diagnosis, _evidence())
        assert result["status"] == "quarantined"
        assert "empty_findings_or_causes" in result["failure_reasons"]

    def test_invalid_runbook_fail(self) -> None:
        diagnosis = _valid_diagnosis(runbook_references=["nonexistent_runbook#section"])
        result = validate_diagnosis(diagnosis, _evidence())
        assert result["status"] == "quarantined"
        assert any("unsupported_runbook_ref" in r for r in result["failure_reasons"])

    def test_banned_wording_fail(self) -> None:
        diagnosis = _valid_diagnosis(
            summary="This is an autonomous diagnosis that uses automatic remediation to shut down the equipment",
        )
        result = validate_diagnosis(diagnosis, _evidence())
        assert result["status"] == "quarantined"
        assert "banned_autonomous_claim" in result["failure_reasons"]

    def test_valid_output_pass(self) -> None:
        diagnosis = _valid_diagnosis(
            findings=["Finding 1", "Finding 2"],
            possible_causes=["Hypothesis: test 1", "Hypothesis: test 2"],
            recommended_checks=["Check 1", "Check 2", "Check 3"],
        )
        result = validate_diagnosis(diagnosis, _evidence())
        assert result["status"] == "valid"

    def test_quarantine_record_has_failure_reasons(self) -> None:
        diagnosis = _valid_diagnosis(tenant_id="tenant_other", recommended_checks=[])
        result = validate_diagnosis(diagnosis, _evidence())
        assert result["status"] == "quarantined"
        assert len(result["failure_reasons"]) > 1
        assert "quarantine_id" in result
        assert "quarantined_at" in result

    def test_banned_wording_root_cause(self) -> None:
        diagnosis = _valid_diagnosis(
            findings=["The root_cause is definitively identified as sensor failure."],
        )
        result = validate_diagnosis(diagnosis, _evidence())
        assert result["status"] == "quarantined"
        assert "banned_autonomous_claim" in result["failure_reasons"]

    def test_banned_wording_guaranteed_rul(self) -> None:
        diagnosis = _valid_diagnosis(
            summary="Guaranteed RUL estimate of 30 days.",
        )
        result = validate_diagnosis(diagnosis, _evidence())
        assert result["status"] == "quarantined"
        assert "banned_autonomous_claim" in result["failure_reasons"]
