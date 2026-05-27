from datetime import UTC, datetime, timedelta

import pytest

from src.flink_jobs.event_time import allowed_lateness, is_beyond_allowed_lateness, parse_event_time, watermark_delay
from src.flink_jobs.late_events import classify_late_event
from src.telemetry_generator.generate_events import generate_events


def test_parse_event_time_requires_z_suffix() -> None:
    with pytest.raises(ValueError, match="must end with Z"):
        parse_event_time("2026-01-01T00:00:00")


def test_streaming_defaults_are_demo_friendly() -> None:
    assert watermark_delay() == timedelta(minutes=5)
    assert allowed_lateness() == timedelta(minutes=10)


def test_out_of_order_event_within_watermark_is_not_beyond_allowed_lateness() -> None:
    event_time = datetime(2026, 1, 1, 0, 5, tzinfo=UTC)
    watermark_time = datetime(2026, 1, 1, 0, 10, tzinfo=UTC)

    assert is_beyond_allowed_lateness(event_time, watermark_time) is False


def test_event_beyond_allowed_lateness_is_classified_for_late_topic() -> None:
    event = dict(generate_events(profile_name="smoke", seed=42).valid_events[0])
    event["event_time"] = "2026-01-01T00:00:00Z"
    event["ingest_time"] = "2026-01-01T00:20:00Z"
    watermark_time = datetime(2026, 1, 1, 0, 16, tzinfo=UTC)

    late = classify_late_event(event, watermark_time)

    assert late is not None
    assert late["tenant_id"] == event["tenant_id"]
    assert late["tag_id"] == event["tag_id"]
    assert late["event_time"] == event["event_time"]
    assert late["ingest_time"] == event["ingest_time"]
    assert late["reason"] == "beyond_allowed_lateness"
