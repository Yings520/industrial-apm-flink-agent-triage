from __future__ import annotations

import json
from typing import Any

from src.triage_service.models import ValidationResult


def recommendation_row(
    recommendation: dict[str, Any],
    validation: ValidationResult,
) -> dict[str, Any]:
    return {
        "recommendation_id": recommendation["recommendation_id"],
        "tenant_id": recommendation["tenant_id"],
        "anomaly_id": recommendation["anomaly_id"],
        "evidence_version": recommendation["evidence_version"],
        "summary": recommendation["summary"],
        "findings": json.dumps(recommendation.get("findings", []), sort_keys=True),
        "possible_causes": json.dumps(recommendation.get("possible_causes", []), sort_keys=True),
        "recommended_checks": json.dumps(recommendation.get("recommended_checks", []), sort_keys=True),
        "runbook_references": json.dumps(recommendation.get("runbook_references", []), sort_keys=True),
        "confidence_label": recommendation["confidence_label"],
        "quality_caveats": json.dumps(recommendation.get("quality_caveats", []), sort_keys=True),
        "model_name": recommendation["model_name"],
        "prompt_version": recommendation["prompt_version"],
        "token_cost": recommendation["token_cost"],
        "latency_ms": recommendation["latency_ms"],
        "status": recommendation["status"],
        "validation_status": validation.validation_status,
        "quarantine_reason": validation.quarantine_reason,
        "created_at": recommendation["created_at"],
    }
