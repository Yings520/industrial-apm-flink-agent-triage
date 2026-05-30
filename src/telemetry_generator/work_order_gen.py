"""
Work Order & Inspection Generator — synthetic maintenance history.

Generates 30 days of work orders per asset/tenant:
- Corrective: linked to fault events in the fault timeline
- Preventive: periodic, no fault link
- Inspection: tied to work orders, with findings and measurements
"""
from __future__ import annotations

import hashlib
import json
import random
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from src.telemetry_generator.fault_injector import FaultEvent


@dataclass(frozen=True)
class WorkOrder:
    work_order_id: str
    tenant_id: str
    asset_id: str
    component_id: str
    failure_mode_id: str | None
    fault_id: str | None
    anomaly_id: str | None
    incident_id: str | None
    work_type: str      # corrective, preventive, inspection
    priority: str
    problem_code: str
    cause_code: str
    remedy_code: str
    technician_notes: str
    downtime_minutes: int
    parts_used: list[dict[str, object]]
    created_at: str
    completed_at: str
    status: str         # open, completed, cancelled
    source_system: str  # apm_triage, cmms, eam


@dataclass(frozen=True)
class InspectionRecord:
    inspection_id: str
    work_order_id: str
    tenant_id: str
    asset_id: str
    component_id: str | None
    inspection_type: str  # visual, thermal, vibration_analysis, oil_analysis
    findings: str
    severity: str         # normal, degraded, action_required, critical
    measurements: dict[str, float]
    created_at: str


def _stable_id(prefix: str, *parts: str) -> str:
    digest = hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()[:12]
    return f"{prefix}_{digest}"


def _format_utc(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


WEEKDAY_NAMES = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

PROBLEM_CODES: dict[str, list[str]] = {
    "bearing": ["BRG001", "BRG002", "BRG003"],
    "motor": ["MOT001", "MOT002"],
    "gearbox": ["GBX001", "GBX002"],
    "pump_impeller": ["PMP001", "PMP002"],
    "seal": ["SEAL001", "SEAL002"],
    "compressor_stage": ["CMP001"],
    "turbine_blade": ["TBN001"],
    "heat_exchanger_bundle": ["HEX001"],
    "chiller_compressor": ["CHL001"],
    "hydraulic_cylinder": ["HYD001"],
}

CAUSE_CODES: dict[str, list[str]] = {
    "BRG001": ["LUB001", "LUB002", "FAT001"],
    "BRG002": ["ALIGN001", "OVER001"],
    "MOT001": ["INS001", "OVER002"],
    "MOT002": ["VIB001"],
    "GBX001": ["LUB003", "FAT002"],
    "PMP001": ["CAV001", "FAT003"],
    "SEAL001": ["LUB004", "FAT004"],
}

REMEDY_CODES: dict[str, list[str]] = {
    "LUB001": ["RELUBE", "GREASE_REP"],
    "LUB002": ["OIL_CHANGE", "SYSTEM_FLUSH"],
    "FAT001": ["BRG_REPLACE"],
    "ALIGN001": ["ALIGNMENT"],
    "OVER001": ["COOLING_FIX", "LOAD_REDUCE"],
    "INS001": ["REWIND", "MOTOR_REPLACE"],
    "VIB001": ["BALANCING", "ALIGNMENT"],
    "CAV001": ["IMPELLER_REPLACE", "NPSH_UPGRADE"],
    "FAT003": ["IMPELLER_REPLACE"],
    "FAT004": ["SEAL_REPLACE"],
}

TECHNICIAN_NOTES: list[str] = [
    "Found excessive wear on {component}. Replaced {part} during scheduled maintenance window.",
    "Vibration analysis confirmed {pattern} at {freq}x RPM. Recommended {action} within next {hours}h.",
    "Oil analysis showed elevated metal particles ({element} > {ppm}ppm). Flushed system and replaced oil.",
    "Thermography confirmed hot spot near {location}. Ambient temp normal. Suspect {cause}.",
    "Visual inspection found {finding}. No immediate safety concern. Work order created for next scheduled downtime.",
    "Ultrasonic detection confirmed {issue}. Repaired seal and pressure tested OK at {pressure}bar.",
]


def generate_work_orders(
    tenant_id: str,
    plant_id: str,
    asset_id: str,
    asset_type: str,
    component_type: str,
    *,
    fault: FaultEvent | None = None,
    start_date: datetime,
    days: int = 30,
    seed: int = 42,
) -> list[WorkOrder]:
    rng = random.Random(f"{seed}|{tenant_id}|{asset_id}|wo")
    orders: list[WorkOrder] = []

    if fault is not None:
        # Corrective work order from fault
        fault_detect = datetime.fromisoformat(fault.detect_time.replace("Z", "+00:00"))
        fault_end = datetime.fromisoformat(fault.end_time.replace("Z", "+00:00"))

        pcodes = PROBLEM_CODES.get(component_type, ["GEN001"])
        problem_code = rng.choice(pcodes)

        ccodes = CAUSE_CODES.get(problem_code, ["GEN_CAUSE"])
        cause_code = rng.choice(ccodes)

        rcodes = REMEDY_CODES.get(cause_code, ["REPAIRED"])
        remedy_code = rng.choice(rcodes)

        orders.append(
            WorkOrder(
                work_order_id=_stable_id("wo", tenant_id, asset_id, "corrective", fault.fault_id),
                tenant_id=tenant_id,
                asset_id=asset_id,
                component_id=fault.component_id,
                failure_mode_id=fault.failure_mode_id,
                fault_id=fault.fault_id,
                anomaly_id=None,
                incident_id=None,
                work_type="corrective",
                priority=fault.severity,
                problem_code=problem_code,
                cause_code=cause_code,
                remedy_code=remedy_code,
                technician_notes=(
                    f"Triggered by APM anomaly on {fault.metric_name} ({fault.failure_mode_name}). "
                    f"Value drifted from {fault.base_value} to {fault.degraded_value}. "
                    f"Found {fault.component_type} failure. Applied {remedy_code}."
                ),
                downtime_minutes=rng.choice([30, 60, 90, 120, 240, 480]),
                parts_used=[
                    {"part_number": f"SP-{component_type.upper()}-{rng.randint(100,999)}",
                     "part_name": f"{component_type.replace('_',' ').title()} Assembly",
                     "quantity": 1}
                ],
                created_at=fault.detect_time,
                completed_at=fault.end_time,
                status="completed",
                source_system="apm_triage",
            )
        )

    # Preventive work orders: every ~7-14 days
    for day in range(0, days, rng.choice([7, 10, 14])):
        if rng.random() < 0.3:
            continue
        wo_time = start_date + timedelta(days=day, hours=rng.randint(6, 12))
        if wo_time > start_date + timedelta(days=days):
            break
        orders.append(
            WorkOrder(
                work_order_id=_stable_id(
                    "wo", tenant_id, asset_id, "preventive", str(day)
                ),
                tenant_id=tenant_id,
                asset_id=asset_id,
                component_id=_stable_id("comp", tenant_id, asset_id, component_type),
                failure_mode_id=None,
                fault_id=None,
                anomaly_id=None,
                incident_id=None,
                work_type="preventive",
                priority=rng.choice(["low", "low", "medium"]),
                problem_code="PM_ROUTINE",
                cause_code="SCHEDULED",
                remedy_code="INSPECTION",
                technician_notes=(
                    f"Scheduled preventive check on {asset_type}. "
                    f"Checked {component_type} condition. All parameters within normal range."
                ),
                downtime_minutes=rng.choice([15, 30, 45]),
                parts_used=[],
                created_at=_format_utc(wo_time),
                completed_at=_format_utc(wo_time + timedelta(minutes=rng.choice([15, 30, 45]))),
                status="completed",
                source_system="cmms",
            )
        )

    return orders


def generate_inspections(
    work_order: WorkOrder,
    component_type: str,
    *,
    seed: int = 42,
) -> InspectionRecord | None:
    """Generate inspection record for corrective work orders."""
    if work_order.work_type not in ("corrective", "inspection"):
        return None

    rng = random.Random(f"{seed}|{work_order.work_order_id}|inspection")
    created_dt = datetime.fromisoformat(work_order.created_at.replace("Z", "+00:00"))
    created_dt += timedelta(minutes=rng.randint(5, 30))

    inspection_types = {
        "bearing": "vibration_analysis",
        "motor": "thermal",
        "gearbox": "oil_analysis",
        "pump_impeller": "vibration_analysis",
        "seal": "visual",
        "compressor_stage": "vibration_analysis",
        "turbine_blade": "vibration_analysis",
        "heat_exchanger_bundle": "thermal",
        "chiller_compressor": "thermal",
        "hydraulic_cylinder": "visual",
    }

    findings_templates: dict[str, list[str]] = {
        "bearing": [
            "Inner race spalling confirmed. BPFI harmonics present.",
            "Outer race defect detected. BPFO at 4.3x RPM.",
            "Cage frequency sidebands visible. Early stage degradation.",
        ],
        "motor": [
            "Winding temperature 15C above baseline. Insulation degradation suspected.",
            "Phase imbalance detected (4.2%). Possible loose connection.",
        ],
        "seal": [
            "Visible leakage at mechanical seal. Seal face damaged.",
            "Flush system pressure low. Seal running dry.",
        ],
    }

    findings = rng.choice(
        findings_templates.get(component_type, ["Component inspected. Wear consistent with operating hours."])
    )

    measurements = {
        "vibration": round(rng.uniform(5.0, 12.0), 2),
        "temperature": round(rng.uniform(65.0, 95.0), 1),
    }

    return InspectionRecord(
        inspection_id=_stable_id("insp", work_order.work_order_id),
        work_order_id=work_order.work_order_id,
        tenant_id=work_order.tenant_id,
        asset_id=work_order.asset_id,
        component_id=work_order.component_id,
        inspection_type=inspection_types.get(component_type, "visual"),
        findings=findings,
        severity=rng.choice(["degraded", "degraded", "action_required", "critical"]),
        measurements=measurements,
        created_at=_format_utc(created_dt),
    )


def write_work_orders(orders: list[WorkOrder], output: Path) -> Path:
    output.parent.mkdir(parents=True, exist_ok=True)
    records = [
        {
            "work_order_id": o.work_order_id,
            "tenant_id": o.tenant_id,
            "asset_id": o.asset_id,
            "component_id": o.component_id,
            "failure_mode_id": o.failure_mode_id,
            "fault_id": o.fault_id,
            "anomaly_id": o.anomaly_id,
            "incident_id": o.incident_id,
            "work_type": o.work_type,
            "priority": o.priority,
            "problem_code": o.problem_code,
            "cause_code": o.cause_code,
            "remedy_code": o.remedy_code,
            "technician_notes": o.technician_notes,
            "downtime_minutes": o.downtime_minutes,
            "parts_used": o.parts_used,
            "created_at": o.created_at,
            "completed_at": o.completed_at,
            "status": o.status,
            "source_system": o.source_system,
        }
        for o in orders
    ]
    with output.open("w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, sort_keys=True))
            f.write("\n")
    return output


def write_inspections(inspections: list[InspectionRecord], output: Path) -> Path:
    output.parent.mkdir(parents=True, exist_ok=True)
    records = [
        {
            "inspection_id": i.inspection_id,
            "work_order_id": i.work_order_id,
            "tenant_id": i.tenant_id,
            "asset_id": i.asset_id,
            "component_id": i.component_id,
            "inspection_type": i.inspection_type,
            "findings": i.findings,
            "severity": i.severity,
            "measurements": i.measurements,
            "created_at": i.created_at,
        }
        for i in inspections
    ]
    with output.open("w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, sort_keys=True))
            f.write("\n")
    return output
