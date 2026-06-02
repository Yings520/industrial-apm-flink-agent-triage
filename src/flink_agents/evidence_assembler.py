from __future__ import annotations

import hashlib
import json
from typing import Any

from src.serving.triage_evidence import FORBIDDEN_EVIDENCE_FIELDS, build_triage_evidence


def assemble_evidence(
    anomaly_trigger: dict[str, Any],
    context: dict[str, Any],
    *,
    evidence_version: str = "triage_evidence.v1",
) -> dict[str, Any]:
    anomaly_tenant = anomaly_trigger.get("tenant_id")
    context_tenant = _extract_context_tenant(context)
    if context_tenant is not None and anomaly_tenant != context_tenant:
        raise ValueError(
            f"Cross-tenant evidence assembly denied: anomaly tenant={anomaly_tenant}, context tenant={context_tenant}"
        )

    tenant_id = str(anomaly_tenant)

    # Check anomaly trigger and context for forbidden fields before assembly
    trigger_forbidden = FORBIDDEN_EVIDENCE_FIELDS.intersection(anomaly_trigger)
    if trigger_forbidden:
        raise ValueError(f"Evidence payload contains forbidden LLM fields: {sorted(trigger_forbidden)}")

    signal_window = context.get("signal_window", [])
    evidence_block = context.get("evidence", {})
    asset_context = context.get("asset_context", {})
    _ = asset_context  # reserved for future enrichment fields

    failure_mode = _first_or_none(evidence_block.get("failure_modes", []))
    work_order = _first_or_none(evidence_block.get("work_orders", []))
    runbook_refs = _derive_runbook_refs(anomaly_trigger)

    quality_flags = list(anomaly_trigger.get("quality_flags", []))
    quality_caveats = _build_quality_caveats(quality_flags, signal_window)
    quality_events: list[dict[str, Any]] = []
    if quality_caveats:
        quality_events.append(
            {
                "tenant_id": tenant_id,
                "quality_event_id": _quality_event_id(tenant_id, str(anomaly_trigger.get("anomaly_id", ""))),
                "check_name": "agent_quality_flags",
                "check_status": "fail",
                "severity": "warning",
            }
        )

    stream_health: list[dict[str, Any]] = []

    evidence = build_triage_evidence(
        tenant_id=tenant_id,
        anomaly=anomaly_trigger,
        sensor_window=cast_sensor_window(signal_window),
        quality_events=quality_events,
        stream_health=stream_health,
        failure_mode=failure_mode,
        work_order=work_order,
        runbook_refs=runbook_refs,
        evidence_version=evidence_version,
    )

    if quality_caveats:
        evidence["quality_caveats"] = quality_caveats

    forbidden = FORBIDDEN_EVIDENCE_FIELDS.intersection(evidence)
    if forbidden:
        raise ValueError(f"Evidence payload contains forbidden LLM fields: {sorted(forbidden)}")

    evidence["failure_modes"] = evidence_block.get("failure_modes", [])
    evidence["failure_events"] = evidence_block.get("failure_events", [])
    evidence["maintenance_records"] = evidence_block.get("maintenance_records", [])
    evidence["signal_mapping"] = context.get("signal_mapping", {})

    return evidence


def cast_sensor_window(signal_window: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "tenant_id": sig.get("tenant_id", ""),
            "event_time": sig.get("event_time", ""),
            "tag_id": sig.get("tag_id"),
            "metric_name": sig.get("metric_name"),
            "value": sig.get("value"),
            "unit": sig.get("unit"),
            "quality_flags": sig.get("quality_flags", []),
        }
        for sig in signal_window
    ]


def _extract_context_tenant(context: dict[str, Any]) -> str | None:
    signal_window = context.get("signal_window", [])
    if signal_window:
        first = signal_window[0]
        if isinstance(first, dict) and "tenant_id" in first:
            return str(first["tenant_id"])

    evidence_block = context.get("evidence", {})
    for key in ("failure_modes", "failure_events", "work_orders", "maintenance_records"):
        records = evidence_block.get(key, [])
        if records:
            first = records[0]
            if isinstance(first, dict) and "tenant_id" in first:
                return str(first["tenant_id"])

    asset_context = context.get("asset_context", {})
    if isinstance(asset_context, dict) and "tenant_id" in asset_context:
        return str(asset_context["tenant_id"])

    signal_mapping = context.get("signal_mapping", {})
    if isinstance(signal_mapping, dict) and "tenant_id" in signal_mapping:
        return str(signal_mapping["tenant_id"])

    return None


def _first_or_none(items: list[dict[str, Any]]) -> dict[str, Any] | None:
    return items[0] if items else None


def _derive_runbook_refs(anomaly_trigger: dict[str, Any]) -> list[dict[str, str]]:
    asset_type = str(anomaly_trigger.get("asset_type", ""))
    metric_name = str(anomaly_trigger.get("metric_name", ""))

    default_refs: dict[str, str] = {
        "pump": "manufacturing_rotating_equipment",
        "compressor": "manufacturing_rotating_equipment",
        "motor": "manufacturing_rotating_equipment",
        "fan": "manufacturing_rotating_equipment",
        "reactor": "process_safety_sensor_checks",
        "boiler": "power_generation_vibration",
        "turbine": "power_generation_vibration",
        "furnace": "high_heat_line_checks",
        "conveyor": "industrial_asset_management_generic",
        "water_pump": "water_flow_and_pump_checks",
        "chiller": "campus_bms_checks",
        "cooling_tower": "campus_bms_checks",
        "air_handler": "campus_bms_checks",
        "transformer": "power_generation_vibration",
        "heat_exchanger": "high_heat_line_checks",
        "valve": "process_safety_sensor_checks",
        "centrifuge": "manufacturing_rotating_equipment",
        "mixer": "process_safety_sensor_checks",
    }

    runbook_id = default_refs.get(
        str(asset_type).lower() if asset_type else "",
        "generic_asset_triage",
    )

    if metric_name:
        return [{"runbook_id": runbook_id, "section": str(metric_name)}]
    return [{"runbook_id": runbook_id, "section": "general"}]


def _build_quality_caveats(
    quality_flags: list[str],
    signal_window: list[dict[str, Any]],
) -> list[str]:
    caveats: list[str] = []
    flag_map = {
        "late_arrival": "Data contains late-arriving events during the anomaly window.",
        "missing_heartbeat": "Sensor heartbeat missing during the anomaly window.",
        "flatline": "Flatline (frozen sensor) detected during the anomaly window.",
        "invalid_contract": "One or more events had an invalid schema contract.",
        "gap_detected": "Data gap detected in the anomaly window.",
        "sensor_fault_suspected": "Sensor fault suspected during the anomaly window.",
    }
    for flag in sorted(set(quality_flags)):
        if flag in flag_map:
            caveats.append(flag_map[flag])

    if not signal_window:
        caveats.append("Signal window evidence is missing or empty.")

    return caveats


def _quality_event_id(tenant_id: str, anomaly_id: str) -> str:
    digest = hashlib.sha1(f"{tenant_id}|{anomaly_id}|quality_caveats".encode()).hexdigest()[:16]
    return f"qe_{digest}"


def evidence_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))
