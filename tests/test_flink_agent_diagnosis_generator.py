import os

import pytest

from src.flink_agents.diagnosis_generator import generate_diagnosis
from src.serving.triage_evidence import build_triage_evidence


def _evidence_payload(**overrides: object) -> dict[str, object]:
    return build_triage_evidence(
        tenant_id="tenant_northwind",
        anomaly={
            "tenant_id": "tenant_northwind",
            "anomaly_id": "anom_001",
            "asset_id": "asset_001",
            "tag_id": "tag_temp",
            "metric_name": "temperature",
            "window_start": "2026-01-01T00:00:00Z",
            "window_end": "2026-01-01T00:05:00Z",
            "observed_value": 95.0,
            "baseline_value": 60.0,
            "expected_value": 65.0,
            "quality_flags": [],
            "source_event_ids": ["evt_001"],
            **overrides,
        },
        sensor_window=[{"tenant_id": "tenant_northwind", "event_time": "2026-01-01T00:01:00Z", "value": 88.0}],
        runbook_refs=[{"runbook_id": "generic_asset_triage", "section": "temperature"}],
    )


class TestFallbackDiagnosis:
    def test_fallback_no_key_no_llm_call(self) -> None:
        env: dict[str, str] = {}
        evidence = _evidence_payload()
        diagnosis = generate_diagnosis(evidence, env=env)
        assert diagnosis["status"] == "fallback"
        assert diagnosis["model_name"] == "fallback-rule-based"
        assert diagnosis["token_cost"] == 0
        assert diagnosis["recommended_checks"]
        assert diagnosis["findings"]
        assert diagnosis["possible_causes"]

    def test_fallback_with_use_llm_false(self) -> None:
        evidence = _evidence_payload()
        diagnosis = generate_diagnosis(evidence, use_llm=False)
        assert diagnosis["status"] == "fallback"
        assert diagnosis["model_name"] == "fallback-rule-based"

    def test_fallback_disabled_provider_env(self) -> None:
        env = {"OPENAI_API_KEY": "", "OPENAI_MODEL": ""}
        evidence = _evidence_payload()
        diagnosis = generate_diagnosis(evidence, use_llm=True, env=env)
        assert diagnosis["status"] == "fallback"

    def test_empty_evidence_handling(self) -> None:
        evidence: dict[str, object] = {
            "evidence_id": "ev_empty",
            "evidence_version": "triage_evidence.v1",
            "tenant_id": "tenant_northwind",
            "anomaly_id": "anom_empty",
            "asset_id": None,
            "tag_id": None,
            "metric_name": None,
            "window_start": None,
            "window_end": None,
            "observed_value": None,
            "baseline_value": None,
            "expected_value": None,
            "quality_flags": [],
            "quality_events": [],
            "stream_health": [],
            "sensor_window": [],
            "source_event_ids": [],
            "quality_event_ids": [],
            "failure_mode_context": None,
            "work_order_context": None,
            "runbook_refs": [],
        }
        diagnosis = generate_diagnosis(evidence)
        assert diagnosis["status"] == "fallback"
        assert "summary" in diagnosis


class TestLLMDiagnosis:
    def test_llm_path_uses_real_llm_from_env(self) -> None:
        api_key = os.environ.get("OPENAI_API_KEY", "").strip()
        model = os.environ.get("OPENAI_MODEL", "").strip()
        if not api_key or not model:
            pytest.skip("OPENAI_API_KEY and OPENAI_MODEL required in .env for LLM test")

        evidence = _evidence_payload()
        diagnosis = generate_diagnosis(evidence, use_llm=True)
        assert diagnosis["status"] in ("llm_generated", "fallback")
        assert "model_name" in diagnosis
        assert "token_cost" in diagnosis
        assert "latency_ms" in diagnosis


class TestBannedWordingGuard:
    def test_banned_wording_interception(self) -> None:
        evidence = _evidence_payload()
        diagnosis = {
            "status": "llm_generated",
            "tenant_id": "tenant_northwind",
            "anomaly_id": "anom_001",
            "evidence_version": "triage_evidence.v1",
            "summary": "This is an autonomous diagnosis of the root_cause",
            "findings": ["Finding 1"],
            "possible_causes": ["Hypothesis: test"],
            "recommended_checks": ["Check 1"],
            "runbook_references": [],
            "confidence_label": "medium",
            "quality_caveats": [],
            "model_name": "test-model",
            "prompt_version": "triage_prompt.v1",
            "token_cost": 0,
            "latency_ms": 10,
            "created_at": "2026-01-01T00:00:00Z",
            "recommendation_id": "rec_001",
        }
        from src.flink_agents.diagnosis_generator import _assert_no_banned_wording

        with pytest.raises(ValueError, match="Banned wording"):
            _assert_no_banned_wording(diagnosis)

    def test_no_banned_wording_passes(self) -> None:
        diagnosis = {
            "summary": "This is an evidence-backed anomaly summary for operator review.",
            "findings": ["Finding 1"],
            "possible_causes": ["Hypothesis: test"],
            "recommended_checks": ["Check 1"],
        }
        from src.flink_agents.diagnosis_generator import _assert_no_banned_wording

        _assert_no_banned_wording(diagnosis)


class TestNoFailureModeHandling:
    def test_no_failure_mode_handling(self) -> None:
        evidence = _evidence_payload()
        evidence["failure_mode_context"] = None
        evidence["work_order_context"] = None
        diagnosis = generate_diagnosis(evidence)
        assert diagnosis["status"] == "fallback"
        assert diagnosis["recommended_checks"]
