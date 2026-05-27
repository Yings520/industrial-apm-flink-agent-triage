from __future__ import annotations

from src.telemetry_generator.config import load_asset_config, load_streaming_config

TOPIC_KEYS = (
    "raw_sensor_events",
    "raw_dlq",
    "staging_telemetry_enriched",
    "staging_dlq",
    "mart_anomalies",
    "mart_late_events",
    "mart_stream_health",
)


def tenant_ids() -> list[str]:
    return [tenant["tenant_id"] for tenant in load_asset_config()["tenants"]]


def render_topic(topic_key: str, tenant_id: str) -> str:
    topics = load_streaming_config()["topics"]
    if topic_key not in topics:
        raise ValueError(f"Unknown topic key: {topic_key}")
    return topics[topic_key].format(tenant_id=tenant_id)


def topics_for_tenant(tenant_id: str) -> dict[str, str]:
    return {topic_key: render_topic(topic_key, tenant_id) for topic_key in TOPIC_KEYS}


def all_topics() -> list[str]:
    return [topic for tenant_id in tenant_ids() for topic in topics_for_tenant(tenant_id).values()]


def raw_message_key(record: dict[str, object]) -> str:
    tenant_id = record.get("tenant_id")
    tag_id = record.get("tag_id")
    if not isinstance(tenant_id, str) or not tenant_id:
        raise ValueError("tenant_id is required for raw message key")
    if not isinstance(tag_id, str) or not tag_id:
        raise ValueError("tag_id is required for raw message key")
    return f"{tenant_id}|{tag_id}"

