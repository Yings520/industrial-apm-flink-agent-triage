from __future__ import annotations

import hashlib
import time
from datetime import UTC, datetime
from typing import Any

from src.triage_service.models import Recommendation


def generate_fallback_recommendation(
    evidence: dict[str, Any],
    *,
    prompt_version: str = "triage_prompt.v1",
) -> dict[str, object]:
    started = time.perf_counter()
    quality_caveats = _quality_caveats(evidence)
    confidence_label = "low" if quality_caveats else "medium"
    anomaly_id = str(evidence["anomaly_id"])
    metric_name = evidence.get("metric_name") or "metric"
    asset_id = str(evidence.get("asset_id") or "asset")
    component_id = str(evidence.get("component_id") or "")
    summary = (
        f"{asset_id} has an evidence-backed {metric_name} anomaly in the selected window. "
        "Review the recommended checks before taking operational action."
    )
    findings = [
        f"Observed value {evidence.get('observed_value')} vs expected {evidence.get('expected_value')}.",
        f"Detection window {evidence.get('window_start')} to {evidence.get('window_end')}.",
    ]
    possible_causes = [
        "Hypothesis: sensor or process condition changed during the anomaly window.",
        "Hypothesis: data quality issues may affect confidence."
        if quality_caveats
        else "Hypothesis: physical operating condition breached configured expectations.",
    ]
    recommended_checks = [
        "Check recent sensor readings and source event IDs in Doris.",
        "Review asset runbook references before operator action.",
        "Confirm data quality flags before escalating severity.",
    ]
    runbook_references = [f"{ref.get('runbook_id')}#{ref.get('section')}" for ref in evidence.get("runbook_refs", [])]

    related_signals: list[str] = [
        str(sig.get("tag_id", "")) for sig in evidence.get("sensor_window", []) if sig.get("tag_id")
    ]
    related_signals = sorted(set(related_signals))

    related_failure_modes: list[str] = sorted(
        set(str(fm.get("failure_mode_id", "")) for fm in evidence.get("failure_modes", []) if fm.get("failure_mode_id"))
    )

    suggested_action = (
        f"Review {metric_name} readings on {asset_id}"
        f"{'/' + component_id if component_id else ''} "
        "in Doris and cross-check with asset runbook references before operator action."
    )

    latency_ms = max(0, int((time.perf_counter() - started) * 1000))
    recommendation = Recommendation(
        recommendation_id=_recommendation_id(str(evidence["tenant_id"]), anomaly_id, str(evidence["evidence_version"])),
        schema_version="agent_explanation.v1",
        tenant_id=str(evidence["tenant_id"]),
        anomaly_id=anomaly_id,
        evidence_version=str(evidence["evidence_version"]),
        asset_id=asset_id,
        component_id=component_id,
        summary=summary,
        findings=findings,
        possible_causes=possible_causes,
        recommended_checks=recommended_checks,
        runbook_references=runbook_references,
        related_signals=related_signals,
        related_failure_modes=related_failure_modes,
        suggested_action=suggested_action,
        confidence_label=confidence_label,
        quality_caveats=quality_caveats,
        model_name="fallback-rule-based",
        prompt_version=prompt_version,
        token_cost=0,
        latency_ms=latency_ms,
        created_at=datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        status="fallback",
    )
    return recommendation.as_dict()


def _quality_caveats(evidence: dict[str, Any]) -> list[str]:
    caveats: list[str] = []
    quality_events = evidence.get("quality_events") or []
    if not evidence.get("sensor_window"):
        caveats.append("Sensor window evidence is missing or empty.")
    for event in quality_events:
        severity = event.get("severity")
        status = event.get("check_status")
        if severity in {"critical", "warning"} or status == "fail":
            caveats.append(f"Quality check {event.get('check_name')} reported {status}/{severity}.")
    return sorted(set(caveats))


def _recommendation_id(tenant_id: str, anomaly_id: str, evidence_version: str) -> str:
    digest = hashlib.sha1(f"{tenant_id}|{anomaly_id}|{evidence_version}".encode()).hexdigest()[:16]
    return f"rec_{digest}"
