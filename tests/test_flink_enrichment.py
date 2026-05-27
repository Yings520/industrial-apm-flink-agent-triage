from src.flink_jobs.metadata import enrich_raw_event
from src.telemetry_generator.generate_events import generate_events


def test_valid_raw_event_enriches_to_asset_and_metric() -> None:
    raw_event = generate_events(profile_name="smoke", seed=42).valid_events[0]

    enriched = enrich_raw_event(raw_event)

    assert enriched["schema_version"] == "enriched_telemetry.v1"
    assert enriched["tenant_id"] == raw_event["tenant_id"]
    assert enriched["asset_id"]
    assert enriched["asset_name"]
    assert enriched["plant_id"]
    assert enriched["metric_name"]
    assert enriched["threshold_profile_id"]
    assert enriched["tag_id"] == raw_event["tag_id"]


def test_unknown_tag_id_routes_to_staging_dlq() -> None:
    raw_event = dict(generate_events(profile_name="smoke", seed=42).valid_events[0])
    raw_event["tag_id"] = "tag_unknown_0001_temperature"

    result = enrich_raw_event(raw_event)  # type: ignore[arg-type]

    assert result["schema_name"] == "enriched_telemetry"
    assert result["error_code"] == "missing_tag_metadata"


def test_tenant_tag_mismatch_routes_to_staging_dlq() -> None:
    raw_event = dict(generate_events(profile_name="smoke", seed=42).valid_events[0])
    raw_event["tenant_id"] = "tenant_wrong"

    result = enrich_raw_event(raw_event)  # type: ignore[arg-type]

    assert result["schema_name"] == "enriched_telemetry"
    assert result["error_code"] == "invalid_asset_tag_mapping"


def test_unit_mismatch_routes_to_staging_dlq() -> None:
    raw_event = dict(generate_events(profile_name="smoke", seed=42).valid_events[0])
    raw_event["unit"] = "kw"

    result = enrich_raw_event(raw_event)  # type: ignore[arg-type]

    assert result["schema_name"] == "enriched_telemetry"
    assert result["error_code"] == "invalid_tag_mapping"
