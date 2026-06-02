from pathlib import Path
from typing import cast

from src.streaming.publish_raw_events import ProducerMode, PublishNamespace, apply_producer_mode, build_parser, plan_routes
from src.telemetry_generator.generate_events import generate_events


def test_publish_routes_valid_raw_records_to_tenant_topic() -> None:
    record = dict(generate_events(profile_name="smoke", seed=42).valid_events[0])

    routes = plan_routes([record])

    assert len(routes) == 1
    assert routes[0].valid is True
    assert routes[0].topic == "tenant_northwind.raw_apm__sensor_readings.v1"
    assert routes[0].key == f"{record['tenant_id']}|{record['tag_id']}"


def test_publish_parser_uses_generated_profile_input_by_default() -> None:
    args = build_parser().parse_args(["--profile", "smoke"], namespace=PublishNamespace())

    assert args.input is None


def test_publish_routes_invalid_raw_records_to_tenant_dlq() -> None:
    record = dict(generate_events(profile_name="smoke", seed=42).invalid_events[0])

    routes = plan_routes([record])

    assert len(routes) == 1
    assert routes[0].valid is False
    assert routes[0].topic == "tenant_northwind.raw_apm__dlq_events.v1"
    assert routes[0].record["schema_name"] == "raw_sensor_event"


def test_publish_routes_missing_tenant_to_fallback_artifact() -> None:
    record = dict(generate_events(profile_name="smoke", seed=42).invalid_events[0])
    _ = record.pop("tenant_id")

    routes = plan_routes([record], output_dir=Path("data"))

    assert len(routes) == 1
    assert routes[0].valid is False
    assert routes[0].topic is None
    assert routes[0].fallback_path == Path("data/rejected/raw_publish_rejected.jsonl")


def test_publish_routes_unknown_tenant_to_fallback_artifact() -> None:
    record = dict(generate_events(profile_name="smoke", seed=42).invalid_events[0])
    record["tenant_id"] = "tenant_not_configured"

    routes = plan_routes([record], output_dir=Path("data"))

    assert len(routes) == 1
    assert routes[0].valid is False
    assert routes[0].topic is None
    assert routes[0].fallback_path == Path("data/rejected/raw_publish_rejected.jsonl")


def test_backfill_replay_preserves_event_time_and_updates_ingest_time() -> None:
    record = {
        "event_id": "evt_1",
        "schema_version": "raw_sensor_event.v1",
        "tenant_id": "tenant_northwind",
        "tag_id": "tag_1",
        "tag_name": "pump_vibration",
        "event_time": "2026-01-01T00:00:00Z",
        "ingest_time": "2026-01-01T00:00:00Z",
        "value": 1.0,
        "unit": "mm_s",
        "source_system": "historian",
        "quality_flags": [],
    }

    updated = apply_producer_mode(cast(dict[str, object], record), ProducerMode.BACKFILL_REPLAY, now_iso="2026-02-01T00:00:00Z")

    assert str(updated["event_time"]) == "2026-01-01T00:00:00Z"
    assert str(updated["ingest_time"]) == "2026-02-01T00:00:00Z"
    sm_val = updated.get("synthetic_metadata", {})
    assert isinstance(sm_val, dict)
    assert sm_val.get("producer_mode") == "backfill_replay"


def test_realtime_live_sets_current_event_and_ingest_time() -> None:
    record = {
        "event_id": "evt_1",
        "schema_version": "raw_sensor_event.v1",
        "tenant_id": "tenant_northwind",
        "tag_id": "tag_1",
        "tag_name": "pump_vibration",
        "event_time": "2026-01-01T00:00:00Z",
        "ingest_time": "2026-01-01T00:00:00Z",
        "value": 1.0,
        "unit": "mm_s",
        "source_system": "historian",
        "quality_flags": [],
    }

    updated = apply_producer_mode(cast(dict[str, object], record), ProducerMode.REALTIME_LIVE, now_iso="2026-02-01T00:00:00Z")

    assert str(updated["event_time"]) == "2026-02-01T00:00:00Z"
    assert str(updated["ingest_time"]) == "2026-02-01T00:00:00Z"
    sm_val = updated.get("synthetic_metadata", {})
    assert isinstance(sm_val, dict)
    assert sm_val.get("producer_mode") == "realtime_live"
