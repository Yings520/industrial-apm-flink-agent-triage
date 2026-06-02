"""
Fault Timeline Generator — synthetic fault events with causal links.

Generates a 30-day fault timeline per tenant. Each fault:
- References a component and failure_mode from the FMECA catalog
- Has a degradation window where telemetry values drift
- Later triggers work order + inspection records
"""

from __future__ import annotations

import hashlib
import json
import random
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class FaultEvent:
    fault_id: str
    tenant_id: str
    plant_id: str
    asset_id: str
    asset_type: str
    component_type: str
    component_id: str
    failure_mode_id: str
    failure_mode_name: str
    metric_name: str
    start_time: str  # when degradation begins
    detect_time: str  # when anomaly should be visible
    end_time: str  # when fault is resolved (work order completed)
    severity: str  # info/warning/critical
    base_value: float  # normal operating value
    degraded_value: float  # value at detect_time
    description: str


@dataclass
class FaultInjectionResult:
    events: list[dict[str, Any]] = field(default_factory=list)
    degradation_map: dict[str, list[dict[str, Any]]] = field(
        default_factory=dict
    )  # tenant_id|tag_id|metric_name -> [period, value]


def _stable_id(prefix: str, *parts: str) -> str:
    digest = hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()[:16]
    return f"{prefix}_{digest}"


def _format_utc(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def generate_fault_timeline(
    tenant_id: str,
    plant_id: str,
    asset_id: str,
    asset_type: str,
    component_type: str,
    failure_mode_id: str,
    failure_mode_name: str,
    metric_name: str,
    base_value: float,
    *,
    start_date: datetime,
    days: int = 30,
    seed: int = 42,
) -> list[FaultEvent]:
    rng = random.Random(f"{seed}|{tenant_id}|{asset_id}|{component_type}|{failure_mode_id}")
    faults: list[FaultEvent] = []

    # Distribute 1-3 faults per asset over the 30-day window
    fault_count = rng.choice([1, 1, 1, 2, 2, 3])
    available_days = list(range(5, days - 3))
    rng.shuffle(available_days)
    chosen_days = sorted(available_days[:fault_count])

    for _fault_index, fault_day in enumerate(chosen_days):
        fault_id = _stable_id(
            "fm",
            tenant_id,
            asset_id,
            component_type,
            failure_mode_id,
            str(fault_day),
        )

        # Fault start: random time on fault_day
        fault_start = start_date.replace(
            hour=rng.randint(0, 23),
            minute=rng.randint(0, 59),
            second=0,
            microsecond=0,
        ) + timedelta(days=fault_day)

        # Degradation window: 12-48 hours
        degrade_hours = rng.choice([12, 18, 24, 36, 48])
        detect_time = fault_start + timedelta(hours=degrade_hours)

        # Resolution: 4-48 hours after detection
        resolve_hours = rng.choice([4, 8, 12, 24, 48])
        end_time = detect_time + timedelta(hours=resolve_hours)

        # Degraded value depends on metric
        degradation_multipliers = {
            "temperature": rng.uniform(1.4, 2.2),
            "vibration": rng.uniform(2.5, 4.0),
            "pressure": rng.choice([rng.uniform(0.3, 0.6), rng.uniform(1.8, 2.5)]),
            "flow_rate": rng.uniform(0.3, 0.7),
            "power_draw": rng.uniform(1.5, 2.5),
        }
        multiplier = degradation_multipliers.get(metric_name, 2.0)
        degraded_value = round(base_value * multiplier, 3)

        severity_weights = ["info", "warning", "warning", "critical", "critical"]
        severity = rng.choice(severity_weights)

        faults.append(
            FaultEvent(
                fault_id=fault_id,
                tenant_id=tenant_id,
                plant_id=plant_id,
                asset_id=asset_id,
                asset_type=asset_type,
                component_type=component_type,
                component_id=_stable_id("comp", tenant_id, asset_id, component_type),
                failure_mode_id=failure_mode_id,
                failure_mode_name=failure_mode_name,
                metric_name=metric_name,
                start_time=_format_utc(fault_start),
                detect_time=_format_utc(detect_time),
                end_time=_format_utc(end_time),
                severity=severity,
                base_value=base_value,
                degraded_value=degraded_value,
                description=(
                    f"{failure_mode_name} on {asset_type} {component_type}. "
                    f"{metric_name} drifts from {base_value} to {degraded_value} "
                    f"over {degrade_hours}h. Detected via {'threshold' if severity == 'critical' else 'z-score'}."
                ),
            )
        )

    return faults


def build_degradation_map(
    faults: list[FaultEvent],
    periods_per_metric: int = 24,
) -> dict[str, list[dict[str, Any]]]:
    """
    Build a degradation map that generate_events can query.
    Key: {tenant_id}|{tag_id}|{metric_name}
    Value: list of {period_offset, degraded_value, scenario, quality_flags}
    """
    degradation_map: dict[str, list[dict[str, Any]]] = {}

    for fault in faults:
        tag_id = f"tag_{fault.plant_id}_{fault.asset_id[-4:]}_{fault.metric_name}"
        key = f"{fault.tenant_id}|{tag_id}|{fault.metric_name}"
        if key not in degradation_map:
            degradation_map[key] = []

        start_dt = datetime.fromisoformat(fault.start_time.replace("Z", "+00:00"))
        detect_dt = datetime.fromisoformat(fault.detect_time.replace("Z", "+00:00"))
        degrade_minutes = int((detect_dt - start_dt).total_seconds() / 60)

        base = fault.base_value
        target = fault.degraded_value
        steps = min(degrade_minutes, periods_per_metric)

        for step in range(steps):
            ratio = (step + 1) / steps
            degraded = round(base + (target - base) * ratio, 3)
            quality_flags = ["degraded_signal"]
            if ratio > 0.7:
                scenario = "spike" if abs(target / base) > 2.0 else "drift"
                quality_flags.append("threshold_breach")
            else:
                scenario = "normal"

            degradation_map[key].append(
                {
                    "period_offset": step,
                    "value": degraded,
                    "scenario": scenario,
                    "quality_flags": quality_flags,
                }
            )

    return degradation_map


def write_fault_timeline(faults: list[FaultEvent], output: Path) -> Path:
    output.parent.mkdir(parents=True, exist_ok=True)
    records = [
        {
            "fault_id": f.fault_id,
            "tenant_id": f.tenant_id,
            "plant_id": f.plant_id,
            "asset_id": f.asset_id,
            "asset_type": f.asset_type,
            "component_type": f.component_type,
            "component_id": f.component_id,
            "failure_mode_id": f.failure_mode_id,
            "failure_mode_name": f.failure_mode_name,
            "metric_name": f.metric_name,
            "start_time": f.start_time,
            "detect_time": f.detect_time,
            "end_time": f.end_time,
            "severity": f.severity,
            "base_value": f.base_value,
            "degraded_value": f.degraded_value,
            "description": f.description,
        }
        for f in faults
    ]
    with output.open("w", encoding="utf-8") as f:
        for record in records:
            _ = f.write(json.dumps(record, sort_keys=True))
            _ = f.write("\n")
    return output
