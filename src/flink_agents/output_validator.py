from __future__ import annotations

import hashlib
import re
from datetime import UTC, datetime
from typing import Any

BANNED_WORDS = re.compile(
    r"\b(root_cause|definitive root cause|autonomous diagnosis|automatic remediation|"
    + r"remediation executed|shut down the equipment|guaranteed RUL)\b",
    re.IGNORECASE,
)


def validate_diagnosis(
    diagnosis: dict[str, Any],
    evidence_payload: dict[str, Any],
) -> dict[str, Any]:
    errors: list[str] = []

    if diagnosis.get("tenant_id") != evidence_payload.get("tenant_id"):
        errors.append("cross_tenant")
    if diagnosis.get("anomaly_id") != evidence_payload.get("anomaly_id"):
        errors.append("anomaly_mismatch")
    if diagnosis.get("evidence_version") != evidence_payload.get("evidence_version"):
        errors.append("evidence_version_mismatch")

    findings = diagnosis.get("findings")
    possible_causes = diagnosis.get("possible_causes")
    if not findings or not possible_causes:
        errors.append("empty_findings_or_causes")

    recommended_checks = diagnosis.get("recommended_checks")
    if not recommended_checks:
        errors.append("empty_recommended_checks")

    _runbook_references = diagnosis.get("runbook_references") or []
    # Runbook references are informational — do not quarantine on format mismatch

    text = _extract_text_fields(diagnosis)
    if BANNED_WORDS.search(text):
        errors.append("banned_autonomous_claim")

    if errors:
        return {
            "status": "quarantined",
            "diagnosis": diagnosis,
            "evidence": evidence_payload,
            "failure_reasons": sorted(set(errors)),
            "quarantine_id": _quarantine_id(
                str(evidence_payload.get("tenant_id", "")),
                str(evidence_payload.get("anomaly_id", "")),
            ),
            "quarantined_at": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        }

    return {
        "status": "valid",
        "diagnosis": diagnosis,
    }


def _extract_text_fields(diagnosis: dict[str, Any]) -> str:
    parts: list[str] = []
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
            parts.append(value)
        elif isinstance(value, list):
            parts.extend(str(v) for v in value)
    return " ".join(parts)


def _quarantine_id(tenant_id: str, anomaly_id: str) -> str:
    digest = hashlib.sha1(f"{tenant_id}|{anomaly_id}|quarantine".encode()).hexdigest()[:16]
    return f"qn_{digest}"
