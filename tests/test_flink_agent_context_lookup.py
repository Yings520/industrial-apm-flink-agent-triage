from datetime import UTC, datetime, timedelta
from typing import Any

from src.flink_agents.context_lookup import ContextLookup


def _make_signal_store(records: list[dict[str, Any]]):
    def store(tenant_id, tag_id, metric_name, window_start, window_end, lookback_minutes, lookforward_minutes):
        return [
            r for r in records
            if r.get("tag_id") == tag_id and r.get("metric_name") == metric_name
        ]
    return store


def _make_asset_store(data: dict[str, dict[str, Any]]):
    def store(tenant_id, asset_id, component_id):
        key = f"{tenant_id}|{asset_id}|{component_id}"
        return data.get(key)
    return store


def _make_signal_mapping_store(data: dict[str, dict[str, Any]]):
    def store(tenant_id, tag_id, source_system):
        key = f"{tenant_id}|{tag_id}|{source_system}"
        return data.get(key)
    return store


def _make_evidence_store(records: list[dict[str, Any]]):
    def store(tenant_id, asset_id, component_id, failure_mode_id, window_start, window_end):
        return [
            r for r in records
            if r.get("asset_id") == asset_id and r.get("component_id") == component_id
        ]
    return store


class TestContextLookupSignalWindow:
    def test_signal_window_time_range_correct(self) -> None:
        signal_records = [
            {"tenant_id": "tenant_northwind", "event_time": "2026-01-01T00:01:00Z", "tag_id": "tag_temp", "metric_name": "temperature", "value": 88.0, "unit": "celsius", "quality_flags": []},
            {"tenant_id": "tenant_northwind", "event_time": "2026-01-01T00:03:00Z", "tag_id": "tag_temp", "metric_name": "temperature", "value": 91.0, "unit": "celsius", "quality_flags": []},
        ]
        store = _make_signal_store(signal_records)
        lookup = ContextLookup(signal_window_store=store)

        result = lookup.lookup_signal_window(
            "tenant_northwind", "tag_temp", "temperature",
            "2026-01-01T00:00:00Z", "2026-01-01T00:05:00Z",
            lookback_minutes=15, lookforward_minutes=15,
        )
        assert len(result) == 2
        assert result[0]["value"] == 88.0
        assert result[1]["value"] == 91.0

    def test_signal_window_empty_when_no_matching_records(self) -> None:
        store = _make_signal_store([])
        lookup = ContextLookup(signal_window_store=store)
        result = lookup.lookup_signal_window(
            "tenant_northwind", "tag_missing", "pressure",
            "2026-01-01T00:00:00Z", "2026-01-01T00:05:00Z",
        )
        assert result == []


class TestContextLookupAssetContext:
    def test_asset_context_completeness(self) -> None:
        asset_data = {
            "tenant_northwind|asset_001|comp_001": {
                "asset_id": "asset_001",
                "component_id": "comp_001",
                "asset_name": "Primary Boiler",
                "asset_type": "boiler",
                "asset_class": "thermal",
                "site_id": "site_north",
                "area_id": "area_boiler_house",
                "line_id": "line_steam_a",
                "functional_location": "BLR-HSE-A01",
                "component_type": "temperature_sensor",
                "manufacturer": "Siemens",
                "model": "SITRANS-T500",
            }
        }
        store = _make_asset_store(asset_data)
        lookup = ContextLookup(asset_context_store=store)

        result = lookup.lookup_asset_context("tenant_northwind", "asset_001", "comp_001")
        assert result["asset_name"] == "Primary Boiler"
        assert result["asset_type"] == "boiler"
        assert result["manufacturer"] == "Siemens"
        assert result["model"] == "SITRANS-T500"

    def test_asset_context_returns_defaults_when_missing(self) -> None:
        lookup = ContextLookup(asset_context_store=_make_asset_store({}))
        result = lookup.lookup_asset_context("tenant_unknown", "asset_999", "comp_999")
        assert result["asset_id"] == "asset_999"
        assert result["asset_name"] == ""


class TestContextLookupSignalMapping:
    def test_signal_mapping_correctness(self) -> None:
        mapping_data = {
            "tenant_northwind|tag_temp|opc_ua": {
                "tag_id": "tag_temp",
                "source_system": "opc_ua",
                "measurement_point": "boiler_outlet_temp",
                "expected_unit": "celsius",
                "normal_range_min": 60.0,
                "normal_range_max": 100.0,
                "sampling_rate_seconds": 5,
            }
        }
        store = _make_signal_mapping_store(mapping_data)
        lookup = ContextLookup(signal_mapping_store=store)

        result = lookup.lookup_signal_mapping("tenant_northwind", "tag_temp", "opc_ua")
        assert result["measurement_point"] == "boiler_outlet_temp"
        assert result["expected_unit"] == "celsius"
        assert result["normal_range_min"] == 60.0
        assert result["normal_range_max"] == 100.0

    def test_signal_mapping_defaults_when_missing(self) -> None:
        lookup = ContextLookup(signal_mapping_store=_make_signal_mapping_store({}))
        result = lookup.lookup_signal_mapping("tenant_unknown", "tag_unknown", "unknown")
        assert result["tag_id"] == "tag_unknown"
        assert result["normal_range_min"] is None


class TestContextLookupEvidence:
    def test_evidence_time_window_constraint(self) -> None:
        evidence_records = [
            {"entity": "failure_mode", "tenant_id": "tenant_northwind", "asset_id": "asset_001", "component_id": "comp_001", "failure_mode_id": "fm_001"},
            {"entity": "failure_event", "tenant_id": "tenant_northwind", "asset_id": "asset_001", "component_id": "comp_001", "failure_event_id": "fe_001"},
            {"entity": "work_order", "tenant_id": "tenant_northwind", "asset_id": "asset_001", "component_id": "comp_001", "work_order_id": "wo_001"},
            {"entity": "maintenance_record", "tenant_id": "tenant_northwind", "asset_id": "asset_001", "component_id": "comp_001", "maintenance_id": "mt_001"},
        ]
        store = _make_evidence_store(evidence_records)
        lookup = ContextLookup(evidence_store=store)

        result = lookup.lookup_evidence(
            "tenant_northwind", "asset_001", "comp_001", "fm_001",
            "2026-01-01T00:00:00Z", "2026-01-01T00:05:00Z",
        )
        assert len(result["failure_modes"]) == 1
        assert len(result["failure_events"]) == 1
        assert len(result["work_orders"]) == 1
        assert len(result["maintenance_records"]) == 1

    def test_lookup_all_returns_all_context(self) -> None:
        signal_records = [
            {"tenant_id": "tenant_northwind", "event_time": "2026-01-01T00:01:00Z", "tag_id": "tag_temp", "metric_name": "temperature", "value": 90.0, "unit": "celsius", "quality_flags": []},
        ]
        evidence_records = [
            {"entity": "failure_mode", "tenant_id": "tenant_northwind", "asset_id": "asset_001", "component_id": "comp_001", "failure_mode_id": "fm_001"},
        ]

        lookup = ContextLookup(
            signal_window_store=_make_signal_store(signal_records),
            evidence_store=_make_evidence_store(evidence_records),
        )

        trigger = {
            "tenant_id": "tenant_northwind",
            "anomaly_id": "anom_001",
            "asset_id": "asset_001",
            "component_id": "comp_001",
            "tag_id": "tag_temp",
            "metric_name": "temperature",
            "window_start": "2026-01-01T00:00:00Z",
            "window_end": "2026-01-01T00:05:00Z",
            "failure_mode_id": "fm_001",
            "source_system": "opc_ua",
        }

        result = lookup.lookup_all("tenant_northwind", trigger)
        assert "signal_window" in result
        assert "asset_context" in result
        assert "signal_mapping" in result
        assert "evidence" in result
        assert len(result["signal_window"]) == 1
        assert len(result["evidence"]["failure_modes"]) == 1
