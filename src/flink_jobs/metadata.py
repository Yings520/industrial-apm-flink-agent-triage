from __future__ import annotations

from src.telemetry_generator.config import build_tag_metadata
from src.telemetry_generator.models import RawSensorEvent, TagMetadata
from src.telemetry_generator.schema_validation import reject_entry


def resolve_tag_metadata(raw_event: RawSensorEvent, mappings: dict[str, TagMetadata] | None = None) -> TagMetadata | None:
    tag_id = raw_event["tag_id"]
    available = mappings if mappings is not None else build_tag_metadata()
    metadata = available.get(tag_id)
    if metadata is None:
        return None
    if metadata["tenant_id"] != raw_event["tenant_id"]:
        return None
    if metadata["unit"] != raw_event["unit"]:
        return None
    return metadata


def enrich_raw_event(raw_event: RawSensorEvent, mappings: dict[str, TagMetadata] | None = None) -> dict[str, object]:
    available = mappings if mappings is not None else build_tag_metadata()
    metadata = available.get(raw_event["tag_id"])
    if metadata is None:
        return to_staging_dlq(raw_event, "missing_tag_metadata", "/tag_id", "No valid tag metadata mapping")
    if metadata["tenant_id"] != raw_event["tenant_id"]:
        return to_staging_dlq(raw_event, "invalid_asset_tag_mapping", "/tenant_id", "Tag metadata tenant mismatch")
    if metadata["unit"] != raw_event["unit"]:
        return to_staging_dlq(raw_event, "invalid_tag_mapping", "/unit", "Tag metadata unit mismatch")
    return {
        "event_id": raw_event["event_id"],
        "schema_version": "enriched_telemetry.v1",
        "tenant_id": raw_event["tenant_id"],
        "plant_id": metadata["plant_id"],
        "asset_id": metadata["asset_id"],
        "asset_name": metadata["asset_name"],
        "tag_id": raw_event["tag_id"],
        "tag_name": raw_event["tag_name"],
        "metric_name": metadata["metric_name"],
        "threshold_profile_id": metadata["threshold_profile_id"],
        "event_time": raw_event["event_time"],
        "ingest_time": raw_event["ingest_time"],
        "value": raw_event["value"],
        "unit": raw_event["unit"],
        "source_system": raw_event["source_system"],
        "quality_flags": list(raw_event["quality_flags"]),
        "scenario": raw_event["scenario"],
    }


def to_staging_dlq(
    raw_event: dict[str, object],
    error_code: str,
    field_path: str,
    message: str,
) -> dict[str, object]:
    return reject_entry(
        original_record=raw_event,
        error_code=error_code,
        field_path=field_path,
        message=message,
        schema_version="enriched_telemetry.v1",
        schema_name="enriched_telemetry",
    )
