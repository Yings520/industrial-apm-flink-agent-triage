import json
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from src.flink_agents.anomaly_event_consumer import (
    REQUIRED_EVENT_FIELDS,
    AnomalyEventConsumer,
)


def _valid_anomaly_event(**overrides: Any) -> dict[str, Any]:
    event: dict[str, Any] = {
        "tenant_id": "tenant_northwind",
        "anomaly_id": "anom_test_001",
        "asset_id": "asset_001",
        "component_id": "comp_001",
        "tag_id": "tag_001_temperature",
        "metric_name": "temperature",
        "rule_id": "rule_threshold_spike_001",
        "window_start": "2026-01-01T00:00:00Z",
        "window_end": "2026-01-01T00:05:00Z",
        "severity": "warning",
        "quality_flags": ["threshold_spike"],
        "source_event_ids": ["evt_001", "evt_002"],
    }
    event.update(overrides)
    return event


def _mock_consumer(records: list[dict[str, Any]]) -> Any:
    mock = MagicMock()
    mock.__iter__.return_value = iter(records)
    mock_value = MagicMock()
    mock_value.value = records[0] if records else {}
    mock.__next__.return_value = mock_value
    return mock


class TestAnomalyEventConsumer:
    def test_normal_event_parsing(self) -> None:
        event = _valid_anomaly_event()
        consumer = AnomalyEventConsumer(tenant_id="tenant_northwind")
        result = consumer._parse_and_validate(event)
        assert result is not None
        assert result["tenant_id"] == "tenant_northwind"
        assert result["anomaly_id"] == "anom_test_001"
        assert result["asset_id"] == "asset_001"
        assert result["component_id"] == "comp_001"
        assert result["tag_id"] == "tag_001_temperature"
        assert result["metric_name"] == "temperature"
        assert result["rule_id"] == "rule_threshold_spike_001"
        assert result["window_start"] == "2026-01-01T00:00:00Z"
        assert result["window_end"] == "2026-01-01T00:05:00Z"
        assert result["severity"] == "warning"
        assert result["quality_flags"] == ["threshold_spike"]
        assert result["source_event_ids"] == ["evt_001", "evt_002"]

    def test_invalid_json_handling(self) -> None:
        consumer = AnomalyEventConsumer(tenant_id="tenant_northwind")
        result = consumer._parse_and_validate(None)
        assert result is None

    def test_missing_required_field_detection(self) -> None:
        for field in REQUIRED_EVENT_FIELDS:
            event = _valid_anomaly_event()
            del event[field]
            consumer = AnomalyEventConsumer(tenant_id="tenant_northwind")
            result = consumer._parse_and_validate(event)
            assert result is None, f"Field {field} should be required but was not caught"

    def test_tenant_mismatch_quarantines(self) -> None:
        event = _valid_anomaly_event(tenant_id="tenant_southridge")
        consumer = AnomalyEventConsumer(tenant_id="tenant_northwind")
        result = consumer._parse_and_validate(event)
        assert result is None

    def test_consumer_non_dict_value_returns_none(self) -> None:
        consumer = AnomalyEventConsumer(tenant_id="tenant_northwind")
        result = consumer._parse_and_validate("not a dict")
        assert result is None

    def test_consumer_iteration_yields_valid_events(self) -> None:
        valid_event = _valid_anomaly_event()

        class FakeConsumer:
            def __iter__(self):
                return self

            def __next__(self):
                return valid_event

        consumer = AnomalyEventConsumer(tenant_id="tenant_northwind")
        # Test __next__ path via _poll_raw → poll → _parse_and_validate
        # We mock the inner consumer to skip actual Kafka
        with patch.object(consumer, "_consumer") as mock_kafka:
            mock_kafka.poll.return_value = {MagicMock(): [MagicMock(value=valid_event)]}
            result = consumer.poll(timeout_ms=1000)
            assert result is not None
            assert result["anomaly_id"] == "anom_test_001"

    def test_empty_topic_poll_returns_none(self) -> None:
        consumer = AnomalyEventConsumer(tenant_id="tenant_northwind")
        with patch.object(consumer, "_consumer") as mock_kafka:
            mock_kafka.poll.return_value = {}
            result = consumer.poll(timeout_ms=1000)
            assert result is None
