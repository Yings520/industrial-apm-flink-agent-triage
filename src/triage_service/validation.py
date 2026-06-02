from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

from src.triage_service.models import ValidationResult

BANNED_LANGUAGE = re.compile(
    r"\b(definitive root cause|autonomous diagnosis|automatic remediation|remediation executed|shut down the equipment)\b",
    re.IGNORECASE,
)


def validate_recommendation(recommendation: dict[str, Any], evidence: dict[str, Any]) -> ValidationResult:
    errors: list[str] = []
    schema = json.loads(Path("contracts/agent_explanation.schema.json").read_text())
    schema_errors = sorted(Draft202012Validator(schema).iter_errors(recommendation), key=lambda err: list(err.path))
    errors.extend(error.message for error in schema_errors)

    if recommendation.get("tenant_id") != evidence.get("tenant_id"):
        errors.append("cross_tenant")
    if recommendation.get("anomaly_id") != evidence.get("anomaly_id"):
        errors.append("unsupported_anomaly")
    if recommendation.get("evidence_version") != evidence.get("evidence_version"):
        errors.append("unsupported_evidence_version")
    if not recommendation.get("findings") or not recommendation.get("recommended_checks"):
        errors.append("evidence_free")

    allowed_runbook_refs = {f"{ref.get('runbook_id')}#{ref.get('section')}" for ref in evidence.get("runbook_refs", [])}
    for ref in recommendation.get("runbook_references") or []:
        if allowed_runbook_refs and ref not in allowed_runbook_refs:
            errors.append(f"unsupported_runbook_ref:{ref}")

    text_parts: list[str] = []
    for key in ("summary", "findings", "possible_causes", "recommended_checks"):
        raw = recommendation.get(key)
        if raw is None:
            text_parts.append("")
        elif isinstance(raw, list):
            text_parts.extend(str(v) for v in raw)
        else:
            text_parts.append(str(raw))
    text = " ".join(text_parts)
    if BANNED_LANGUAGE.search(text):
        errors.append("banned_autonomous_claim")

    if errors:
        return ValidationResult(False, "quarantined", ";".join(sorted(set(errors))), sorted(set(errors)))
    return ValidationResult(True, "valid")
