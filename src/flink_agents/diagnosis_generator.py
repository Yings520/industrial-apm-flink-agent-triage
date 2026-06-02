from __future__ import annotations

import hashlib
import logging
import os
import re
import time
from datetime import UTC, datetime
from typing import Any

from src.triage_service.openai_provider import generate_llm_recommendation
from src.triage_service.recommendations import generate_fallback_recommendation

logger = logging.getLogger(__name__)

BANNED_WORDS = re.compile(
    r"\b(root_cause|definitive root cause|autonomous diagnosis|automatic remediation|"
    + r"remediation executed|shut down the equipment|guaranteed RUL)\b",
    re.IGNORECASE,
)


def generate_diagnosis(
    evidence_payload: dict[str, Any],
    *,
    use_llm: bool = False,
    env: dict[str, str] | None = None,
) -> dict[str, Any]:
    started = time.perf_counter()

    if use_llm:
        diagnosis = _llm_diagnosis(evidence_payload, env=env)
    else:
        diagnosis = generate_fallback_recommendation(evidence_payload)

    _assert_no_banned_wording(diagnosis)

    latency_ms = max(0, int((time.perf_counter() - started) * 1000))
    if "latency_ms" not in diagnosis:
        diagnosis["latency_ms"] = latency_ms

    if "generated_at" not in diagnosis:
        diagnosis["generated_at"] = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    return diagnosis


def _llm_diagnosis(
    evidence_payload: dict[str, Any],
    *,
    env: dict[str, str] | None = None,
) -> dict[str, Any]:
    if env is None:
        env = dict(os.environ)

    api_key = env.get("OPENAI_API_KEY", "").strip()
    model = env.get("OPENAI_MODEL", "").strip()

    if not api_key or not model:
        logger.info("No LLM credentials available, falling back to rule-based diagnosis")
        return generate_fallback_recommendation(evidence_payload)

    result = generate_llm_recommendation(evidence_payload, env=env)
    recommendation = result.get("recommendation")

    if recommendation is not None and isinstance(recommendation, dict):
        diagnosis = dict(recommendation)
        diagnosis.setdefault(
            "recommendation_id",
            _recommendation_id(
                str(evidence_payload.get("tenant_id", "")),
                str(evidence_payload.get("anomaly_id", "")),
                str(evidence_payload.get("evidence_version", "")),
            ),
        )
        diagnosis.setdefault("prompt_version", "triage_prompt.v1")
        return diagnosis

    logger.warning("LLM returned no recommendation, falling back to rule-based diagnosis")
    return generate_fallback_recommendation(evidence_payload)


def _assert_no_banned_wording(diagnosis: dict[str, Any]) -> None:
    text_fields = []
    for key in (
        "summary",
        "findings",
        "possible_causes",
        "recommended_checks",
        "diagnosis_summary",
        "probable_causes",
        "suggested_action",
    ):
        value = diagnosis.get(key)
        if isinstance(value, str):
            text_fields.append(value)
        elif isinstance(value, list):
            text_fields.extend(str(v) for v in value)

    text = " ".join(text_fields)
    match = BANNED_WORDS.search(text)
    if match:
        raise ValueError(f"Banned wording detected in diagnosis output: '{match.group(0)}'")


def _recommendation_id(tenant_id: str, anomaly_id: str, evidence_version: str) -> str:
    digest = hashlib.sha1(f"{tenant_id}|{anomaly_id}|{evidence_version}".encode()).hexdigest()[:16]
    return f"rec_{digest}"
