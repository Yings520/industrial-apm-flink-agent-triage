from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Any

QUALITY_CHECKS = (
    "freshness",
    "completeness",
    "duplicate",
    "range",
    "schema_drift",
    "late_event",
    "tenant_leakage",
)


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _stable_id(parts: list[str]) -> str:
    return "qevt_" + hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()[:16]


def build_quality_event(
    *,
    tenant_id: str,
    check_name: str,
    check_status: str,
    severity: str,
    window_start: str,
    window_end: str,
    evidence: dict[str, Any],
    plant_id: str | None = None,
    asset_id: str | None = None,
    tag_id: str | None = None,
    component_id: str | None = None,
    metric_name: str | None = None,
    quality_type: str | None = None,
    detected_at: str | None = None,
    resolution_status: str = "open",
    source_event_ids: list[str] | None = None,
    observed_value: object | None = None,
    expected_value: object | None = None,
    created_at: str | None = None,
) -> dict[str, Any]:
    if check_name not in QUALITY_CHECKS:
        raise ValueError(f"Unknown quality check: {check_name}")
    if not tenant_id:
        raise ValueError("tenant_id is required for quality events")
    if check_name == "tenant_leakage" and evidence.get("tenant_id") not in (None, tenant_id):
        check_status = "fail"
        severity = "critical"

    if quality_type is None:
        quality_type = check_name

    quality_event_id = _stable_id(
        [
            tenant_id,
            check_name,
            asset_id or "",
            tag_id or "",
            window_start,
            window_end,
            json.dumps(evidence, sort_keys=True),
        ]
    )
    return {
        "quality_event_id": quality_event_id,
        "tenant_id": tenant_id,
        "plant_id": plant_id,
        "asset_id": asset_id,
        "component_id": component_id,
        "tag_id": tag_id,
        "metric_name": metric_name,
        "check_name": check_name,
        "quality_type": quality_type,
        "check_status": check_status,
        "severity": severity,
        "detected_at": detected_at or _now(),
        "resolution_status": resolution_status,
        "source_event_ids": source_event_ids or [],
        "observed_value": observed_value,
        "expected_value": expected_value,
        "window_start": window_start,
        "window_end": window_end,
        "evidence_json": json.dumps(evidence, sort_keys=True),
        "created_at": created_at or _now(),
    }


def events_from_quality_flags(record: dict[str, Any]) -> list[dict[str, Any]]:
    tenant_id = str(record.get("tenant_id") or "")
    window_start = str(record.get("window_start") or record.get("event_time") or "")
    window_end = str(record.get("window_end") or record.get("event_time") or "")
    events: list[dict[str, Any]] = []
    for flag in sorted(record.get("quality_flags") or []):
        check_name = _flag_to_check_name(str(flag))
        events.append(
            build_quality_event(
                tenant_id=tenant_id,
                plant_id=record.get("plant_id"),
                asset_id=record.get("asset_id"),
                tag_id=record.get("tag_id"),
                metric_name=record.get("metric_name"),
                check_name=check_name,
                check_status="fail",
                severity="warning",
                window_start=window_start,
                window_end=window_end,
                evidence={"quality_flag": flag, "source": "quality_flags"},
            )
        )
    return events


def event_from_late_record(record: dict[str, Any]) -> dict[str, Any]:
    return build_quality_event(
        tenant_id=str(record.get("tenant_id") or ""),
        plant_id=record.get("plant_id"),
        asset_id=record.get("asset_id"),
        tag_id=record.get("tag_id"),
        metric_name=record.get("metric_name"),
        check_name="late_event",
        check_status="fail",
        severity="warning",
        window_start=str(record.get("event_time") or ""),
        window_end=str(record.get("watermark_time") or record.get("event_time") or ""),
        observed_value=record.get("lateness_minutes"),
        expected_value="<= allowed_lateness_minutes",
        evidence={"reason": record.get("reason"), "event_id": record.get("event_id")},
    )


def tenant_leakage_event(expected_tenant_id: str, observed: dict[str, Any]) -> dict[str, Any]:
    observed_tenant_id = observed.get("tenant_id")
    return build_quality_event(
        tenant_id=expected_tenant_id,
        plant_id=observed.get("plant_id"),
        asset_id=observed.get("asset_id"),
        tag_id=observed.get("tag_id"),
        metric_name=observed.get("metric_name"),
        check_name="tenant_leakage",
        check_status="pass" if observed_tenant_id == expected_tenant_id else "fail",
        severity="info" if observed_tenant_id == expected_tenant_id else "critical",
        window_start=str(observed.get("window_start") or observed.get("event_time") or ""),
        window_end=str(observed.get("window_end") or observed.get("event_time") or ""),
        evidence={"tenant_id": observed_tenant_id, "expected_tenant_id": expected_tenant_id},
    )


def _flag_to_check_name(flag: str) -> str:
    if flag in {"late_arrival", "late_event"}:
        return "late_event"
    if flag in {"invalid_contract", "schema_drift"}:
        return "schema_drift"
    if flag in {"threshold_spike", "range"}:
        return "range"
    if flag in {"missing_heartbeat", "completeness"}:
        return "completeness"
    if flag in {"duplicate"}:
        return "duplicate"
    return "freshness"
