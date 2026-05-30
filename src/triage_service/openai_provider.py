from __future__ import annotations

import hashlib
import json
import os
import time
from datetime import UTC, datetime
from typing import Any, Protocol


class CompletionTransport(Protocol):
    def __call__(self, *, url: str, headers: dict[str, str], body: bytes, timeout: int) -> tuple[int, bytes]:
        ...


def _std_transport(*, url: str, headers: dict[str, str], body: bytes, timeout: int) -> tuple[int, bytes]:
    from urllib.request import Request, urlopen

    req = Request(url, data=body, headers=headers, method="POST")
    with urlopen(req, timeout=timeout) as resp:
        return resp.status, resp.read()


def generate_llm_recommendation(
    evidence: dict[str, Any],
    *,
    env: dict[str, str] | None = None,
    transport: CompletionTransport | None = None,
) -> dict[str, Any]:
    if env is None:
        env = dict(os.environ)
    api_key = env.get("OPENAI_API_KEY", "").strip()
    model = env.get("OPENAI_MODEL", "").strip()

    if not api_key or not model:
        return {
            "status": "provider_disabled",
            "parsed_status": "provider_disabled",
            "raw_response": "",
            "provider": "openai",
            "model_name": model or "unset",
            "token_cost": 0,
            "latency_ms": 0,
            "recommendation": None,
        }

    started = time.perf_counter()
    base_url = env.get("OPENAI_BASE_URL", "https://api.openai.com/v1").strip().rstrip("/")
    url = f"{base_url}/chat/completions"
    system_msg = (
        "You are an industrial asset triage assistant. "
        "Use ONLY the provided deterministic evidence to produce a recommendation. "
        "Do not invent evidence, cross tenant boundaries, diagnose faults, or prescribe remediation. "
        "Output MUST be strict JSON with exactly these fields:\n"
        '{\n'
        '  "schema_version": "agent_explanation.v1",\n'
        '  "tenant_id": "<from evidence>",\n'
        '  "anomaly_id": "<from evidence>",\n'
        '  "evidence_version": "<from evidence>",\n'
        '  "summary": "one-sentence summary of what was observed",\n'
        '  "findings": ["finding 1", "finding 2"],\n'
        '  "possible_causes": ["Hypothesis: cause 1", "Hypothesis: cause 2"],\n'
        '  "recommended_checks": ["check 1", "check 2"],\n'
        '  "runbook_references": ["ref1", "ref2"],\n'
        '  "confidence_label": "low|medium|high",\n'
        '  "quality_caveats": ["caveat 1 if applicable"]\n'
        '}\n'
        "Do NOT include any other fields. Do NOT wrap in markdown code blocks."
    )

    evidence_json = json.dumps(
        {
            "tenant_id": evidence.get("tenant_id"),
            "anomaly_id": evidence.get("anomaly_id"),
            "evidence_version": evidence.get("evidence_version"),
            "asset_id": evidence.get("asset_id"),
            "metric_name": evidence.get("metric_name"),
            "window_start": evidence.get("window_start"),
            "window_end": evidence.get("window_end"),
            "observed_value": evidence.get("observed_value"),
            "baseline_value": evidence.get("baseline_value"),
            "expected_value": evidence.get("expected_value"),
            "quality_flags": evidence.get("quality_flags"),
            "sensor_window": evidence.get("sensor_window"),
            "stream_health": evidence.get("stream_health"),
            "runbook_refs": evidence.get("runbook_refs"),
            "quality_events": evidence.get("quality_events"),
        },
        sort_keys=True,
    )

    body = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_msg},
            {"role": "user", "content": f"Evidence:\n```json\n{evidence_json}\n```\n\nReturn a JSON recommendation matching the agent_explanation schema."},
        ],
        "temperature": 0,
        "response_format": {"type": "json_object"},
    }
    body_bytes = json.dumps(body).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }

    t = transport or _std_transport
    raw_response = ""
    parsed_status = "unknown"
    recommendation: dict[str, Any] | None = None

    try:
        status, resp_bytes = t(url=url, headers=headers, body=body_bytes, timeout=30)
        raw_response = resp_bytes.decode("utf-8", errors="replace")
        latency_ms = max(0, int((time.perf_counter() - started) * 1000))

        if status != 200:
            return {
                "status": "api_failure",
                "parsed_status": "api_failure",
                "raw_response": raw_response,
                "provider": "openai",
                "model_name": model,
                "token_cost": 0,
                "latency_ms": latency_ms,
                "recommendation": None,
            }

        parsed = json.loads(resp_bytes)
        choice = parsed.get("choices", [{}])[0]
        content = choice.get("message", {}).get("content", "")
        usage = parsed.get("usage", {})
        prompt_tokens = usage.get("prompt_tokens", 0)
        completion_tokens = usage.get("completion_tokens", 0)
        token_cost = prompt_tokens * 0.001 / 1000 + completion_tokens * 0.002 / 1000

        try:
            # Strip markdown code fences if LLM wraps output in ```json...```
            cleaned = content.strip()
            if cleaned.startswith("```"):
                lines = cleaned.split("\n")
                if lines[0].startswith("```"):
                    lines = lines[1:]
                if lines and lines[-1].startswith("```"):
                    lines = lines[:-1]
                cleaned = "\n".join(lines).strip()
            recommendation = json.loads(cleaned)
            recommendation["model_name"] = model
            recommendation["token_cost"] = token_cost
            recommendation["latency_ms"] = latency_ms
            recommendation["created_at"] = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
            recommendation["status"] = "llm_generated"
            parsed_status = "parsed_json"
        except (json.JSONDecodeError, TypeError):
            parsed_status = "non_json"
            raw_response = content

    except Exception as exc:
        latency_ms = max(0, int((time.perf_counter() - started) * 1000))
        return {
            "status": "api_failure",
            "parsed_status": "api_failure",
            "raw_response": raw_response or str(exc),
            "provider": "openai",
            "model_name": model,
            "token_cost": 0,
            "latency_ms": latency_ms,
            "recommendation": None,
        }

    return {
        "status": "api_success" if recommendation else "non_json",
        "parsed_status": parsed_status,
        "raw_response": raw_response,
        "provider": "openai",
        "model_name": model,
        "token_cost": token_cost,
        "latency_ms": latency_ms,
        "recommendation": recommendation,
    }
