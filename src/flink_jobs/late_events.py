from __future__ import annotations

from datetime import datetime

from src.flink_jobs.event_time import format_event_time, is_beyond_allowed_lateness, parse_event_time


def late_event_record(
    event: dict[str, object], watermark_time: datetime, reason: str = "beyond_allowed_lateness"
) -> dict[str, object]:
    event_time_raw = event["event_time"]
    if not isinstance(event_time_raw, str):
        raise ValueError("event_time must be a string")
    event_time = parse_event_time(event_time_raw)
    lateness_minutes = max(0.0, (watermark_time - event_time).total_seconds() / 60)
    return {
        "event_id": event["event_id"],
        "schema_version": "late_event.v1",
        "tenant_id": event["tenant_id"],
        "tag_id": event["tag_id"],
        "event_time": event["event_time"],
        "ingest_time": event["ingest_time"],
        "watermark_time": format_event_time(watermark_time),
        "lateness_minutes": round(lateness_minutes, 3),
        "reason": reason,
    }


def classify_late_event(event: dict[str, object], watermark_time: datetime) -> dict[str, object] | None:
    event_time_raw = event["event_time"]
    if not isinstance(event_time_raw, str):
        raise ValueError("event_time must be a string")
    if is_beyond_allowed_lateness(parse_event_time(event_time_raw), watermark_time):
        return late_event_record(event, watermark_time)
    return None
