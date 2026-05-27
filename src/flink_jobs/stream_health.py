from __future__ import annotations

from datetime import datetime

from src.flink_jobs.event_time import format_event_time


def stream_health_record(
    job_name: str,
    tenant_id: str,
    watermark_lag_seconds: float,
    late_event_count: int,
    checkpoint_status: str,
    observed_at: datetime,
) -> dict[str, object]:
    return {
        "schema_version": "stream_health.v1",
        "job_name": job_name,
        "tenant_id": tenant_id,
        "watermark_lag_seconds": watermark_lag_seconds,
        "late_event_count": late_event_count,
        "checkpoint_status": checkpoint_status,
        "observed_at": format_event_time(observed_at),
    }

