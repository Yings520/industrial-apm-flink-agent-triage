from __future__ import annotations

from datetime import UTC, datetime, timedelta

from src.telemetry_generator.config import load_streaming_config


def parse_event_time(value: str) -> datetime:
    if not value.endswith("Z"):
        raise ValueError("event_time must end with Z")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("event_time must be timezone-aware")
    return parsed.astimezone(UTC)


def format_event_time(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def watermark_delay() -> timedelta:
    minutes = load_streaming_config()["flink"]["watermark_delay_minutes"]
    return timedelta(minutes=minutes)


def allowed_lateness() -> timedelta:
    minutes = load_streaming_config()["flink"]["allowed_lateness_minutes"]
    return timedelta(minutes=minutes)


def watermark_for(max_event_time: datetime) -> datetime:
    return max_event_time - watermark_delay()


def is_beyond_allowed_lateness(event_time: datetime, watermark_time: datetime) -> bool:
    return event_time < watermark_time - allowed_lateness()
