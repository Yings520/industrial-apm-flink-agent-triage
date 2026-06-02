from __future__ import annotations

import hashlib
import json
from typing import Any

FORBIDDEN_EVIDENCE_FIELDS = {"finding", "recommendation", "recommended_action", "root_cause"}


def build_triage_evidence(
    *,
    tenant_id: str,
    anomaly: dict[str, Any],
    sensor_window: list[dict[str, Any]] | None = None,
    quality_events: list[dict[str, Any]] | None = None,
    stream_health: list[dict[str, Any]] | None = None,
    failure_mode: dict[str, Any] | None = None,
    work_order: dict[str, Any] | None = None,
    runbook_refs: list[dict[str, str]] | None = None,
    evidence_version: str = "triage_evidence.v1",
    max_source_event_ids: int = 10,
) -> dict[str, Any]:
    _assert_tenant(tenant_id, anomaly, "anomaly")
    sensor_window = sensor_window or []
    quality_events = quality_events or []
    stream_health = stream_health or []
    runbook_refs = runbook_refs or []
    for index, row in enumerate(sensor_window):
        _assert_tenant(tenant_id, row, f"sensor_window[{index}]")
    for index, row in enumerate(quality_events):
        _assert_tenant(tenant_id, row, f"quality_events[{index}]")
    for index, row in enumerate(stream_health):
        _assert_tenant(tenant_id, row, f"stream_health[{index}]")

    source_event_ids = sorted(set(map(str, anomaly.get("source_event_ids") or [])))[:max_source_event_ids]
    payload = {
        "evidence_id": _evidence_id(tenant_id, str(anomaly["anomaly_id"]), evidence_version),
        "evidence_version": evidence_version,
        "tenant_id": tenant_id,
        "anomaly_id": anomaly["anomaly_id"],
        "asset_id": anomaly.get("asset_id"),
        "component_id": anomaly.get("component_id"),
        "tag_id": anomaly.get("tag_id"),
        "metric_name": anomaly.get("metric_name"),
        "window_start": anomaly.get("window_start"),
        "window_end": anomaly.get("window_end"),
        "observed_value": anomaly.get("observed_value"),
        "baseline_value": anomaly.get("baseline_value"),
        "expected_value": anomaly.get("expected_value"),
        "quality_flags": sorted(set(map(str, anomaly.get("quality_flags") or []))),
        "quality_events": sorted(quality_events, key=lambda row: str(row.get("quality_event_id", ""))),
        "stream_health": sorted(stream_health, key=lambda row: str(row.get("job_name", ""))),
        "sensor_window": sorted(sensor_window, key=lambda row: str(row.get("event_time", ""))),
        "source_event_ids": source_event_ids,
        "quality_event_ids": sorted(
            set(str(q.get("quality_event_id")) for q in quality_events if q.get("quality_event_id"))
        ),
        "failure_mode_context": failure_mode,
        "work_order_context": work_order,
        "runbook_refs": sorted(runbook_refs, key=lambda row: (row.get("runbook_id", ""), row.get("section", ""))),
    }
    forbidden = FORBIDDEN_EVIDENCE_FIELDS.intersection(payload)
    if forbidden:
        raise ValueError(f"Evidence payload contains forbidden LLM fields: {sorted(forbidden)}")
    return payload


def evidence_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _assert_tenant(tenant_id: str, row: dict[str, Any], label: str) -> None:
    if row.get("tenant_id") != tenant_id:
        raise ValueError(f"{label} tenant mismatch: expected {tenant_id}, got {row.get('tenant_id')}")


def _evidence_id(tenant_id: str, anomaly_id: str, evidence_version: str) -> str:
    digest = hashlib.sha1(f"{tenant_id}|{anomaly_id}|{evidence_version}".encode()).hexdigest()[:16]
    return f"ev_{digest}"
