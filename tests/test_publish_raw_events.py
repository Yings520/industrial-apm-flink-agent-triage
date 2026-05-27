from pathlib import Path

from src.streaming.publish_raw_events import plan_routes
from src.telemetry_generator.generate_events import generate_events


def test_publish_routes_valid_raw_records_to_tenant_topic() -> None:
    record = dict(generate_events(profile_name="smoke", seed=42).valid_events[0])

    routes = plan_routes([record])

    assert len(routes) == 1
    assert routes[0].valid is True
    assert routes[0].topic == "tenant_northwind.raw.sensor_events.v1"
    assert routes[0].key == f"{record['tenant_id']}|{record['tag_id']}"


def test_publish_routes_invalid_raw_records_to_tenant_dlq() -> None:
    record = dict(generate_events(profile_name="smoke", seed=42).invalid_events[0])

    routes = plan_routes([record])

    assert len(routes) == 1
    assert routes[0].valid is False
    assert routes[0].topic == "tenant_northwind.raw.dlq.v1"
    assert routes[0].record["schema_name"] == "raw_sensor_event"


def test_publish_routes_missing_tenant_to_fallback_artifact() -> None:
    record = dict(generate_events(profile_name="smoke", seed=42).invalid_events[0])
    record.pop("tenant_id")

    routes = plan_routes([record], output_dir=Path("data"))

    assert len(routes) == 1
    assert routes[0].valid is False
    assert routes[0].topic is None
    assert routes[0].fallback_path == Path("data/rejected/raw_publish_rejected.jsonl")
