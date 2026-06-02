from __future__ import annotations

import json
import logging
import os
import time
from datetime import UTC, datetime
from typing import Any, Protocol

logger = logging.getLogger(__name__)


class CompletionTransport(Protocol):
    def __call__(self, *, url: str, headers: dict[str, str], body: bytes, timeout: int) -> tuple[int, bytes]: ...


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
        "{\n"
        '  "schema_version": "agent_explanation.v1",\n'
        '  "tenant_id": "<copy from evidence.tenant_id>",\n'
        '  "anomaly_id": "<copy from evidence.anomaly_id>",\n'
        '  "evidence_version": "<copy from evidence.evidence_version>",\n'
        '  "asset_id": "<copy from evidence.asset_id>",\n'
        '  "component_id": "<copy from evidence.component_id>",\n'
        '  "summary": "one-sentence summary grounded in observed_value vs expected_value",\n'
        '  "findings": ["finding 1", "finding 2"],\n'
        '  "possible_causes": ["Hypothesis: cause 1", "Hypothesis: cause 2"],\n'
        '  "recommended_checks": ["check 1", "check 2"],\n'
        '  "runbook_references": ["ref1", "ref2"],\n'
        '  "related_signals": ["tag_id1", "tag_id2"],\n'
        '  "related_failure_modes": ["failure_mode_id1"],\n'
        '  "suggested_action": "short actionable instruction",\n'
        '  "confidence_label": "low|medium|high",\n'
        '  "quality_caveats": ["caveat 1 if applicable"]\n'
        "}\n"
        "STRICT CONSTRAINTS — EVERY field below MUST be non-empty and derived from evidence:\n"
        "  summary: Must reference metric_name, observed_value, and window range from evidence. Never empty.\n"
        "  findings: Minimum 2 items. Each must cite specific values from evidence "
        "(observed_value, expected_value, quality_flags, sensor_window).\n"
        "  possible_causes: Minimum 2 items. Each must be a hypothesis grounded in evidence "
        "(metric deviation, quality_flags, failure_modes, stream_health). "
        "Prefix each with 'Hypothesis:'.\n"
        "  recommended_checks: Minimum 3 items. Concrete, actionable checks referencing "
        "evidence.asset_id, evidence.metric_name, and runbook_refs.\n"
        "  runbook_references: MUST be copied verbatim from evidence.runbook_refs as "
        "'runbook_id#section' strings. If evidence.runbook_refs is empty, use "
        "['generic_asset_triage#general']. Never empty.\n"
        "  related_signals: List ALL tag_ids from evidence.sensor_window. "
        "If sensor_window is empty, use [evidence.tag_id]. Never empty.\n"
        "  related_failure_modes: List failure_mode_id strings from evidence.failure_modes. "
        "May be empty [] only if evidence.failure_modes is empty.\n"
        "  suggested_action: One concise instruction for the operator. "
        "Must mention evidence.asset_id and evidence.metric_name. Never empty.\n"
        "  confidence_label: 'low' if evidence.quality_flags is non-empty OR "
        "quality_events has failures, 'medium' otherwise, 'high' only when "
        "quality_flags is empty AND sensor_window has >=3 readings.\n"
        "  quality_caveats: Describe each data quality concern found in "
        "evidence.quality_flags or evidence.quality_events. May be empty [] only when "
        "quality_flags is empty AND quality_events has no failures.\n"
        "Do NOT include any other fields. Do NOT wrap in markdown code blocks."
    )

    evidence_json = json.dumps(
        {
            "tenant_id": evidence.get("tenant_id"),
            "anomaly_id": evidence.get("anomaly_id"),
            "evidence_version": evidence.get("evidence_version"),
            "asset_id": evidence.get("asset_id"),
            "component_id": evidence.get("component_id"),
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
            "failure_modes": evidence.get("failure_modes"),
        },
        sort_keys=True,
    )

    body = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_msg},
            {
                "role": "user",
                "content": (
                    f"Evidence:\n```json\n{evidence_json}\n```\n\n"
                    "Return a JSON recommendation matching the agent_explanation schema."
                ),
            },
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
        status, resp_bytes = t(url=url, headers=headers, body=body_bytes, timeout=120)
        raw_response = resp_bytes.decode("utf-8", errors="replace")
        latency_ms = max(0, int((time.perf_counter() - started) * 1000))

        if status != 200:
            logger.error(
                "LLM API non-200: status=%d | body[:300]=%s",
                status,
                raw_response[:300],
            )
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
            logger.info("LLM raw content[:300]: %s", cleaned[:300])
            if cleaned.startswith("```"):
                lines = cleaned.split("\n")
                if lines[0].startswith("```"):
                    lines = lines[1:]
                if lines and lines[-1].startswith("```"):
                    lines = lines[:-1]
                cleaned = "\n".join(lines).strip()
                logger.info("LLM cleaned (after fence strip): %s", cleaned[:300])
            recommendation = json.loads(cleaned)
            if not isinstance(recommendation, dict):
                raise TypeError("LLM response is not a JSON object")
            logger.info("LLM JSON parsed OK, keys: %s", list(recommendation.keys())[:20])
            recommendation["model_name"] = model
            recommendation["token_cost"] = token_cost
            recommendation["latency_ms"] = latency_ms
            recommendation["created_at"] = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
            recommendation["status"] = "llm_generated"
            parsed_status = "parsed_json"
        except (json.JSONDecodeError, TypeError) as exc:
            logger.error(
                "LLM response is not valid JSON. content[:500]=%s | error=%s",
                content[:500] if isinstance(content, str) else str(content)[:500],
                exc,
            )
            parsed_status = "non_json"
            raw_response = content

    except Exception as exc:
        latency_ms = max(0, int((time.perf_counter() - started) * 1000))
        logger.error(
            "LLM API transport exception: %s | raw=%s",
            exc,
            raw_response[:200],
        )
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
