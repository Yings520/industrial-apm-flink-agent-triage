from __future__ import annotations

import json
import logging
import re
from typing import Any, final

from kafka import KafkaConsumer  # pyright: ignore[reportMissingTypeStubs]

from src.streaming.topic_names import render_topic

logger = logging.getLogger(__name__)


def _render_pattern() -> re.Pattern[str]:
    return re.compile(r".*\.mart_apm__fct_anomaly_events\.v1")


REQUIRED_EVENT_FIELDS = (
    "tenant_id",
    "anomaly_id",
    "asset_id",
    "component_id",
    "tag_id",
    "metric_name",
    "rule_id",
    "window_start",
    "window_end",
    "severity",
    "quality_flags",
    "source_event_ids",
)


@final
class AnomalyEventConsumer:
    def __init__(
        self,
        *,
        tenant_id: str,
        broker: str = "localhost:19092",
        consumer_group: str | None = None,
    ) -> None:
        topic = render_topic("mart_anomalies", tenant_id)
        self._topic = topic
        self._tenant_id = tenant_id
        self._consumer = KafkaConsumer(
            topic,
            bootstrap_servers=broker,
            group_id=consumer_group or f"flink-agent-{tenant_id}",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )

    def __iter__(self) -> AnomalyEventConsumer:
        return self

    def __next__(self) -> dict[str, Any]:
        while True:
            event = self._poll_raw()
            if event is None:
                continue
            return event

    def poll(self, timeout_ms: int = 5000) -> dict[str, Any] | None:
        records = self._consumer.poll(timeout_ms=timeout_ms, max_records=1)
        if not records:
            return None
        for _tp, msgs in records.items():
            for msg in msgs:
                return self._parse_and_validate(msg.value)
        return None

    def poll_blocking(self) -> dict[str, Any]:
        for msg in self._consumer:
            result = self._parse_and_validate(msg.value)
            if result is not None:
                return result
        raise RuntimeError("Kafka consumer closed unexpectedly")

    def close(self) -> None:
        self._consumer.close()

    @property
    def topic(self) -> str:
        return self._topic

    @property
    def tenant_id(self) -> str:
        return self._tenant_id

    @classmethod
    def for_all_tenants(
        cls,
        *,
        broker: str = "localhost:19092",
        consumer_group: str = "flink-agent-all-tenants",
    ) -> AnomalyEventConsumer:
        topic_pattern = _render_pattern()
        consumer = KafkaConsumer(
            bootstrap_servers=broker,
            group_id=consumer_group,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )
        # Discover all matching topics
        available = consumer.topics()
        matching = [t for t in available if topic_pattern.match(t)]
        if not matching:
            raise RuntimeError(f"No anomaly topics found matching pattern: {topic_pattern.pattern}")
        consumer.subscribe(topics=matching)
        obj = object.__new__(cls)
        obj._topic = ",".join(matching)
        obj._tenant_id = "*"
        obj._consumer = consumer
        return obj

    def _poll_raw(self) -> dict[str, Any] | None:
        return self.poll(timeout_ms=5000)

    def _parse_and_validate(self, raw_value: Any) -> dict[str, Any] | None:
        if not isinstance(raw_value, dict):
            logger.warning("Quarantine: non-dict JSON received on topic=%s", self._topic)
            return None

        missing = [f for f in REQUIRED_EVENT_FIELDS if f not in raw_value]
        if missing:
            logger.warning(
                "Quarantine: missing required fields %s on topic=%s",
                missing,
                self._topic,
            )
            return None

        tenant_in_record = raw_value.get("tenant_id")
        if self._tenant_id != "*" and tenant_in_record != self._tenant_id:
            logger.warning(
                "Quarantine: tenant mismatch expected=%s got=%s on topic=%s",
                self._tenant_id,
                tenant_in_record,
                self._topic,
            )
            return None

        return dict(raw_value)
