from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Any

from kafka import KafkaProducer  # pyright: ignore[reportMissingTypeStubs]

from src.streaming.topic_names import render_topic

logger = logging.getLogger(__name__)

DIAGNOSIS_EVENT_FIELDS = (
    "diagnosis_id",
    "tenant_id",
    "asset_id",
    "component_id",
    "anomaly_event_id",
    "diagnosis_summary",
    "probable_causes",
    "evidence",
    "related_signals",
    "related_failure_modes",
    "recommended_checks",
    "suggested_action",
    "risk_level",
    "confidence",
    "escalation_required",
    "quality_caveats",
    "generated_at",
    "agent_version",
    "prompt_version",
    "model_version",
)


def produce_diagnosis(
    diagnosis_event: dict[str, Any],
    tenant_id: str,
    *,
    broker: str = "localhost:19092",
) -> None:
    topic = render_topic("mart_realtime_diagnosis", tenant_id)
    producer = _build_producer(broker)
    try:
        serialized = _serialize_diagnosis(diagnosis_event, tenant_id)
        future = producer.send(topic, value=serialized)
        future.get(timeout=10)
        logger.info("Produced diagnosis event to topic=%s", topic)
    finally:
        producer.close()


def _serialize_diagnosis(diagnosis_event: dict[str, Any], tenant_id: str) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for field in DIAGNOSIS_EVENT_FIELDS:
        if field in diagnosis_event:
            payload[field] = diagnosis_event[field]
        else:
            payload[field] = _default_for_field(field, tenant_id, diagnosis_event)

    payload["generated_at"] = diagnosis_event.get("generated_at") or datetime.now(UTC).replace(
        microsecond=0
    ).isoformat().replace("+00:00", "Z")
    return payload


def _default_for_field(field: str, tenant_id: str, diagnosis_event: dict[str, Any]) -> Any:
    defaults: dict[str, Any] = {
        "diagnosis_id": diagnosis_event.get("recommendation_id", ""),
        "tenant_id": tenant_id,
        "asset_id": diagnosis_event.get("asset_id", ""),
        "component_id": diagnosis_event.get("component_id", ""),
        "anomaly_event_id": diagnosis_event.get("anomaly_id", ""),
        "diagnosis_summary": diagnosis_event.get("summary", ""),
        "probable_causes": diagnosis_event.get("possible_causes", []),
        "evidence": {},
        "related_signals": [],
        "related_failure_modes": [],
        "recommended_checks": diagnosis_event.get("recommended_checks", []),
        "suggested_action": diagnosis_event.get("suggested_action", ""),
        "risk_level": "medium",
        "confidence": 0.5,
        "escalation_required": False,
        "quality_caveats": diagnosis_event.get("quality_caveats", []),
        "agent_version": "agent.v1",
        "prompt_version": diagnosis_event.get("prompt_version", "triage_prompt.v1"),
        "model_version": diagnosis_event.get("model_name", "fallback-rule-based"),
    }
    return defaults.get(field, "")


def _build_producer(broker: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=broker,
        batch_size=16384,
        linger_ms=10,
        compression_type="gzip",
        value_serializer=lambda v: json.dumps(v, sort_keys=True, separators=(",", ":")).encode("utf-8"),
    )
