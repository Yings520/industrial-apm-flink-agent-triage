from src.serving.quality_events import (
    QUALITY_CHECKS,
    event_from_late_record,
    events_from_quality_flags,
    tenant_leakage_event,
)


def test_quality_check_inventory_is_complete() -> None:
    assert set(QUALITY_CHECKS) == {
        "freshness",
        "completeness",
        "duplicate",
        "range",
        "schema_drift",
        "late_event",
        "tenant_leakage",
    }


def test_quality_flags_create_quality_events() -> None:
    events = events_from_quality_flags(
        {
            "tenant_id": "tenant_northwind",
            "asset_id": "asset_1",
            "tag_id": "tag_1",
            "metric_name": "temperature",
            "window_start": "2026-01-01T00:00:00Z",
            "window_end": "2026-01-01T00:05:00Z",
            "quality_flags": ["threshold_spike"],
        }
    )
    assert events[0]["tenant_id"] == "tenant_northwind"
    assert events[0]["check_name"] == "range"
    assert events[0]["check_status"] == "fail"
    assert "evidence_json" in events[0]


def test_late_record_creates_late_quality_event() -> None:
    event = event_from_late_record(
        {
            "event_id": "evt_1",
            "tenant_id": "tenant_northwind",
            "tag_id": "tag_1",
            "event_time": "2026-01-01T00:00:00Z",
            "watermark_time": "2026-01-01T00:15:00Z",
            "lateness_minutes": 15,
            "reason": "beyond_allowed_lateness",
        }
    )
    assert event["check_name"] == "late_event"
    assert event["observed_value"] == 15


def test_tenant_leakage_flags_cross_tenant_record() -> None:
    event = tenant_leakage_event("tenant_a", {"tenant_id": "tenant_b", "event_time": "2026-01-01T00:00:00Z"})
    assert event["check_name"] == "tenant_leakage"
    assert event["check_status"] == "fail"
    assert event["severity"] == "critical"
