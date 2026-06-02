from __future__ import annotations

from typing import Any

_VALID_SEVERITY_CORRECTIONS = {"none", "info", "warning", "critical"}


def feedback_row(feedback: dict[str, Any]) -> dict[str, Any]:
    required_keys = (
        "feedback_id",
        "tenant_id",
        "incident_id",
        "anomaly_id",
        "operator_id",
        "is_true_positive",
        "severity_correction",
        "explanation_usefulness",
        "resolution_note",
        "created_at",
    )
    for key in required_keys:
        if key not in feedback:
            raise ValueError(f"missing required feedback key: {key}")

    severity = str(feedback.get("severity_correction", "none"))
    if severity not in _VALID_SEVERITY_CORRECTIONS:
        raise ValueError(
            f"invalid severity_correction: {severity!r}; must be one of {sorted(_VALID_SEVERITY_CORRECTIONS)}"
        )

    return {
        "feedback_id": str(feedback["feedback_id"]),
        "tenant_id": str(feedback["tenant_id"]),
        "incident_id": str(feedback["incident_id"]),
        "anomaly_id": str(feedback["anomaly_id"]),
        "operator_id": str(feedback["operator_id"]),
        "is_true_positive": bool(feedback["is_true_positive"]),
        "severity_correction": severity,
        "explanation_usefulness": int(feedback["explanation_usefulness"]),
        "resolution_note": str(feedback["resolution_note"]),
        "created_at": str(feedback["created_at"]),
    }
