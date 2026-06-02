from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from src.telemetry_generator.config import build_tag_metadata


def _format_utc(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def generate_business_data(seed: int = 42) -> dict[str, list[dict[str, Any]]]:
    _ = seed
    metadata = build_tag_metadata()
    now = datetime(2026, 1, 30, tzinfo=UTC)
    assets_by_id: dict[str, dict[str, Any]] = {}
    components_by_id: dict[str, dict[str, Any]] = {}
    measurement_points_by_id: dict[str, dict[str, Any]] = {}
    sensor_tags: list[dict[str, Any]] = []

    for tag in metadata.values():
        _ = assets_by_id.setdefault(
            tag["asset_id"],
            {
                "asset_id": tag["asset_id"],
                "asset_name": tag["asset_name"],
                "asset_type": tag["asset_type"],
                "tenant_id": tag["tenant_id"],
                "plant_id": tag["plant_id"],
                "criticality": "critical" if tag["asset_type"] in {"reactor", "compressor"} else "high",
                "lifecycle_state": "operating",
            },
        )
        _ = components_by_id.setdefault(
            tag["component_id"],
            {
                "component_id": tag["component_id"],
                "asset_id": tag["asset_id"],
                "component_type": tag["component_type"],
                "criticality": "critical" if tag["component_type"] in {"seal", "compressor_stage"} else "high",
            },
        )
        _ = measurement_points_by_id.setdefault(
            tag["measurement_point_id"],
            {
                "measurement_point_id": tag["measurement_point_id"],
                "component_id": tag["component_id"],
                "measurement_point": tag["measurement_point"],
                "metric_name": tag["metric_name"],
                "unit": tag["unit"],
            },
        )
        sensor_tags.append(dict(tag))

    selected_components = list(components_by_id.values())[:8]
    work_orders = []
    maintenance_events = []
    rul_predictions = []
    triage_evidence = []
    for index, component in enumerate(selected_components, start=1):
        work_order_id = f"wo_demo_{index:04d}"
        failure_mode_id = "fm_pump_cavitation" if index % 2 else "fm_bearing_inner_race_wear"
        created_at = now - timedelta(days=3, hours=index)
        completed_at = created_at + timedelta(hours=8)
        work_orders.append(
            {
                "work_order_id": work_order_id,
                "asset_id": component["asset_id"],
                "component_id": component["component_id"],
                "failure_mode_id": failure_mode_id,
                "priority": "P1" if index % 3 == 0 else "P2",
                "work_type": "corrective" if index % 3 == 0 else "inspection",
                "problem_code": "abnormal_vibration",
                "cause_code": "suspected_degradation",
                "remedy_code": "inspect_and_service",
                "status": "completed",
                "created_at": _format_utc(created_at),
                "scheduled_start_at": _format_utc(created_at + timedelta(hours=2)),
                "completed_at": _format_utc(completed_at),
                "downtime_minutes": 120 if index % 3 == 0 else 0,
                "technician_notes": "Synthetic CMMS record for APM demo.",
            }
        )
        maintenance_events.append(
            {
                "maintenance_event_id": f"maint_demo_{index:04d}",
                "work_order_id": work_order_id,
                "asset_id": component["asset_id"],
                "component_id": component["component_id"],
                "event_type": "inspection_completed",
                "event_time": _format_utc(completed_at),
            }
        )
        for day in range(30):
            rul_predictions.append(
                {
                    "prediction_id": f"rul_demo_{index:04d}_{day:02d}",
                    "asset_id": component["asset_id"],
                    "component_id": component["component_id"],
                    "failure_mode_id": failure_mode_id,
                    "prediction_time": _format_utc(now - timedelta(days=30 - day)),
                    "horizon_minutes": 60 * 24 * 30,
                    "rul_hours": max(0, 720 - day * 24 - index),
                    "rul_lower_hours": max(0, 680 - day * 24 - index),
                    "rul_upper_hours": max(0, 760 - day * 24 - index),
                    "confidence_score": 0.72,
                    "model_name": "synthetic_rul_curve",
                    "model_version": "v1",
                    "evidence_window_start": _format_utc(now - timedelta(days=30 - day, minutes=15)),
                    "evidence_window_end": _format_utc(now - timedelta(days=30 - day)),
                }
            )
        triage_evidence.append(
            {
                "evidence_id": f"evidence_demo_{index:04d}",
                "anomaly_id": f"anom_demo_{index:04d}",
                "asset_id": component["asset_id"],
                "component_id": component["component_id"],
                "failure_mode_id": failure_mode_id,
                "supporting_evidence": ["vibration trend increased", "RUL curve declined"],
                "root_cause_hypotheses": ["bearing wear", "process load increase"],
                "recommended_inspection": ["inspect bearing housing", "check lubrication history"],
                "confidence_score": 0.76,
                "quality_caveats": [],
            }
        )

    return {
        "assets": list(assets_by_id.values()),
        "components": list(components_by_id.values()),
        "measurement_points": list(measurement_points_by_id.values()),
        "sensor_tags": sensor_tags,
        "work_orders": work_orders,
        "maintenance_events": maintenance_events,
        "rul_predictions": rul_predictions,
        "triage_evidence": triage_evidence,
    }
