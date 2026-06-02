from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

SECTION_HEADINGS = (
    "Doris Ingestion Counts",
    "Anomaly Trends",
    "Late Event Rate",
    "Quality Failures",
    "Tenant Asset Health",
    "Incident Queue",
    "Stream Health",
    "Triage Recommendations",
    "Local Demo Metrics",
)


def render_report(results: dict[str, Any]) -> str:
    generated_at = results.get("generated_at") or datetime.now(UTC).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )
    lines = [
        "# Phase 3 Serving Report",
        "",
        f"Generated at: {generated_at}",
        "",
        "Scope: local/synthetic demo metrics only. This report does not claim production SLA, real factory deployment, or real MTTR reduction.",
        "",
    ]
    for heading in SECTION_HEADINGS:
        lines.extend([f"## {heading}", ""])
        value = results.get(_key_for_heading(heading), [])
        lines.extend(_render_value(value))
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def write_report(results: dict[str, Any], output: Path) -> Path:
    output.parent.mkdir(parents=True, exist_ok=True)
    _ = output.write_text(render_report(results), encoding="utf-8")
    return output


def sample_results() -> dict[str, Any]:
    return {
        "doris_ingestion_counts": [
            {"table_name": "dwd_apm_sensor_readings_rt", "row_count": 12},
            {"table_name": "fact_apm_anomaly_events", "row_count": 8},
        ],
        "anomaly_trends": [{"tenant_id": "tenant_northwind", "rule_id": "threshold_spike", "anomaly_count": 8}],
        "late_event_rate": [{"tenant_id": "tenant_northwind", "late_event_count": 0}],
        "quality_failures": [{"tenant_id": "tenant_northwind", "check_name": "range", "failure_count": 8}],
        "tenant_asset_health": [
            {"tenant_id": "tenant_northwind", "asset_id": "asset_northwind_mfg_01_0001", "critical_anomaly_count": 8}
        ],
        "incident_queue": [
            {"tenant_id": "tenant_northwind", "anomaly_id": "anom_phase03_demo", "severity": "critical"}
        ],
        "stream_health": [
            {"tenant_id": "tenant_northwind", "job_name": "flink_anomalies", "checkpoint_status": "demo"}
        ],
        "triage_recommendations": [
            {"tenant_id": "tenant_northwind", "anomaly_id": "anom_phase03_demo", "status": "fallback"}
        ],
        "local_demo_metrics": {"event_count": 12, "anomaly_count": 8, "llm_provider": "fallback"},
    }


def _key_for_heading(heading: str) -> str:
    return heading.lower().replace(" ", "_")


def _render_value(value: Any) -> list[str]:
    if isinstance(value, dict):
        return [f"- `{key}`: {item}" for key, item in value.items()]
    if isinstance(value, list):
        if not value:
            return ["No rows returned."]
        return [f"- {row}" for row in value]
    return [str(value)]
