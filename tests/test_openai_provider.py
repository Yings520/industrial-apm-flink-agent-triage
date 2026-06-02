import json
from typing import Any

from src.triage_service.openai_provider import generate_llm_recommendation


def _evidence() -> dict[str, Any]:
    return {
        "tenant_id": "tenant_northwind",
        "anomaly_id": "anom_1",
        "evidence_id": "ev_abc",
        "evidence_version": "triage_evidence.v1",
        "asset_id": "asset_1",
        "metric_name": "temperature",
        "window_start": "2026-01-01T00:00:00Z",
        "window_end": "2026-01-01T00:05:00Z",
        "observed_value": 90,
        "baseline_value": 60,
        "expected_value": 65,
        "quality_flags": [],
        "sensor_window": [],
        "stream_health": [],
        "runbook_refs": [{"runbook_id": "generic_asset_triage", "section": "temperature"}],
        "quality_events": [],
    }


def test_missing_api_key_returns_provider_disabled() -> None:
    env: dict[str, str] = {}
    result = generate_llm_recommendation(_evidence(), env=env)
    assert result["status"] == "provider_disabled"
    assert result["parsed_status"] == "provider_disabled"
    assert result["recommendation"] is None


def test_missing_model_returns_provider_disabled() -> None:
    env = {"OPENAI_API_KEY": "sk-test"}
    result = generate_llm_recommendation(_evidence(), env=env)
    assert result["status"] == "provider_disabled"


def test_fake_transport_success_returns_parsed_json() -> None:
    evidence = _evidence()
    fake_response = {
        "choices": [
            {
                "message": {
                    "content": json.dumps({
                        "recommendation_id": "rec_test",
                        "schema_version": "agent_explanation.v1",
                        "tenant_id": "tenant_northwind",
                        "anomaly_id": "anom_1",
                        "evidence_version": "triage_evidence.v1",
                        "summary": "Test summary",
                        "findings": ["Finding 1"],
                        "possible_causes": ["Hypothesis: test"],
                        "recommended_checks": ["Check A"],
                        "runbook_references": ["generic_asset_triage#temperature"],
                        "confidence_label": "medium",
                        "quality_caveats": [],
                        "model_name": "test-model",
                        "prompt_version": "triage_prompt.v1",
                        "token_cost": 0.01,
                        "latency_ms": 100,
                        "created_at": "2026-01-01T00:00:00Z",
                        "status": "llm_generated",
                    })
                }
            }
        ],
        "usage": {"prompt_tokens": 500, "completion_tokens": 200},
    }

    def fake_transport(*, url: str, headers: dict[str, str], body: bytes, timeout: int) -> tuple[int, bytes]:
        return 200, json.dumps(fake_response).encode("utf-8")

    env = {"OPENAI_API_KEY": "sk-test", "OPENAI_MODEL": "gpt-4o-mini"}
    result = generate_llm_recommendation(evidence, env=env, transport=fake_transport)
    assert result["status"] == "api_success"
    assert result["parsed_status"] == "parsed_json"
    assert result["recommendation"] is not None
    assert result["recommendation"]["tenant_id"] == "tenant_northwind"
    assert result["recommendation"]["model_name"] == "gpt-4o-mini"


def test_fake_transport_non_json_returns_non_json() -> None:
    fake_response = {
        "choices": [{"message": {"content": "not valid json {{{"}}],
        "usage": {"prompt_tokens": 10, "completion_tokens": 5},
    }

    def fake_transport(*, url: str, headers: dict[str, str], body: bytes, timeout: int) -> tuple[int, bytes]:
        return 200, json.dumps(fake_response).encode("utf-8")

    env = {"OPENAI_API_KEY": "sk-test", "OPENAI_MODEL": "gpt-4o-mini"}
    result = generate_llm_recommendation(_evidence(), env=env, transport=fake_transport)
    assert result["parsed_status"] == "non_json"
    assert result["recommendation"] is None
    assert len(result["raw_response"]) > 0


def test_fake_transport_api_failure_returns_failure() -> None:
    def fake_transport(*, url: str, headers: dict[str, str], body: bytes, timeout: int) -> tuple[int, bytes]:
        return 500, b'{"error": "internal"}'

    env = {"OPENAI_API_KEY": "sk-test", "OPENAI_MODEL": "gpt-4o-mini"}
    result = generate_llm_recommendation(_evidence(), env=env, transport=fake_transport)
    assert result["status"] == "api_failure"
    assert result["recommendation"] is None


def test_transport_exception_returns_failure() -> None:
    def fake_transport(*, url: str, headers: dict[str, str], body: bytes, timeout: int) -> tuple[int, bytes]:
        raise RuntimeError("connection refused")

    env = {"OPENAI_API_KEY": "sk-test", "OPENAI_MODEL": "gpt-4o-mini"}
    result = generate_llm_recommendation(_evidence(), env=env, transport=fake_transport)
    assert result["status"] == "api_failure"
    assert result["recommendation"] is None


def test_no_paid_credential_required_in_test() -> None:
    result = generate_llm_recommendation(_evidence(), env={})
    assert result["status"] == "provider_disabled"
