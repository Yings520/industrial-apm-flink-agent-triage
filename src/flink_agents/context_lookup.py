from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Any, final


@final
class ContextLookup:
    def __init__(
        self,
        *,
        signal_window_store: Callable[[str, str, str, str, str, int, int], list[dict[str, Any]]] | None = None,
        asset_context_store: Callable[[str, str, str], dict[str, Any] | None] | None = None,
        signal_mapping_store: Callable[[str, str, str], dict[str, Any] | None] | None = None,
        evidence_store: Callable[..., list[dict[str, Any]]] | None = None,
    ) -> None:
        self._signal_window_store = signal_window_store
        self._asset_context_store = asset_context_store
        self._signal_mapping_store = signal_mapping_store
        self._evidence_store = evidence_store

    def lookup_signal_window(
        self,
        tenant_id: str,
        tag_id: str,
        metric_name: str,
        window_start: str,
        window_end: str,
        *,
        lookback_minutes: int = 15,
        lookforward_minutes: int = 15,
    ) -> list[dict[str, Any]]:
        if self._signal_window_store is not None:
            return self._signal_window_store(
                tenant_id,
                tag_id,
                metric_name,
                window_start,
                window_end,
                lookback_minutes,
                lookforward_minutes,
            )

        return _default_signal_window(
            tenant_id,
            tag_id,
            metric_name,
            window_start,
            window_end,
            lookback_minutes,
            lookforward_minutes,
        )

    def lookup_asset_context(
        self,
        tenant_id: str,
        asset_id: str,
        component_id: str,
    ) -> dict[str, Any]:
        if self._asset_context_store is not None:
            result = self._asset_context_store(tenant_id, asset_id, component_id)
            if result is not None:
                return result

        return {
            "asset_id": asset_id,
            "component_id": component_id,
            "asset_name": "",
            "asset_type": "",
            "asset_class": "",
            "site_id": "",
            "area_id": "",
            "line_id": "",
            "functional_location": "",
            "component_type": "",
            "manufacturer": "",
            "model": "",
        }

    def lookup_signal_mapping(
        self,
        tenant_id: str,
        tag_id: str,
        source_system: str,
    ) -> dict[str, Any]:
        if self._signal_mapping_store is not None:
            result = self._signal_mapping_store(tenant_id, tag_id, source_system)
            if result is not None:
                return result

        return {
            "tag_id": tag_id,
            "source_system": source_system,
            "measurement_point": "",
            "expected_unit": "",
            "normal_range_min": None,
            "normal_range_max": None,
            "sampling_rate_seconds": None,
        }

    def lookup_evidence(
        self,
        tenant_id: str,
        asset_id: str,
        component_id: str,
        failure_mode_id: str,
        window_start: str,
        window_end: str,
    ) -> dict[str, Any]:
        if self._evidence_store is not None:
            records = self._evidence_store(
                tenant_id,
                asset_id,
                component_id,
                failure_mode_id,
                window_start,
                window_end,
            )
            return _partition_evidence_records(records)

        return {
            "failure_modes": [],
            "failure_events": [],
            "work_orders": [],
            "maintenance_records": [],
        }

    def lookup_all(
        self,
        tenant_id: str,
        anomaly_trigger: dict[str, Any],
    ) -> dict[str, Any]:
        tag_id = str(anomaly_trigger.get("tag_id", ""))
        metric_name = str(anomaly_trigger.get("metric_name", ""))
        window_start = str(anomaly_trigger.get("window_start", ""))
        window_end = str(anomaly_trigger.get("window_end", ""))
        asset_id = str(anomaly_trigger.get("asset_id", ""))
        component_id = str(anomaly_trigger.get("component_id", ""))
        failure_mode_id = str(anomaly_trigger.get("failure_mode_id", ""))
        source_system = str(anomaly_trigger.get("source_system", ""))

        return {
            "signal_window": self.lookup_signal_window(
                tenant_id,
                tag_id,
                metric_name,
                window_start,
                window_end,
            ),
            "asset_context": self.lookup_asset_context(tenant_id, asset_id, component_id),
            "signal_mapping": self.lookup_signal_mapping(tenant_id, tag_id, source_system),
            "evidence": self.lookup_evidence(
                tenant_id,
                asset_id,
                component_id,
                failure_mode_id,
                window_start,
                window_end,
            ),
        }


def _default_signal_window(
    tenant_id: str,
    tag_id: str,
    metric_name: str,
    window_start: str,
    window_end: str,
    lookback_minutes: int,
    lookforward_minutes: int,
) -> list[dict[str, Any]]:
    try:
        ws = datetime.fromisoformat(window_start.replace("Z", "+00:00"))
        we = datetime.fromisoformat(window_end.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return []

    extended_start = (ws - timedelta(minutes=lookback_minutes)).isoformat().replace("+00:00", "Z")
    _extended_end = (we + timedelta(minutes=lookforward_minutes)).isoformat().replace("+00:00", "Z")

    return [
        {
            "tenant_id": tenant_id,
            "event_time": extended_start,
            "tag_id": tag_id,
            "metric_name": metric_name,
            "value": 0.0,
            "unit": "",
            "quality_flags": [],
        }
    ]


def _partition_evidence_records(records: list[dict[str, Any]]) -> dict[str, Any]:
    failure_modes: list[dict[str, Any]] = []
    failure_events: list[dict[str, Any]] = []
    work_orders: list[dict[str, Any]] = []
    maintenance_records: list[dict[str, Any]] = []

    for rec in records:
        entity = rec.get("entity", rec.get("record_type", ""))
        if entity in {"failure_mode", "apm_failuremode"}:
            failure_modes.append(rec)
        elif entity in {"failure_event", "apm_failureevent"}:
            failure_events.append(rec)
        elif entity in {"work_order", "workorder"}:
            work_orders.append(rec)
        elif entity in {"maintenance", "maintenance_record"}:
            maintenance_records.append(rec)

    return {
        "failure_modes": failure_modes,
        "failure_events": failure_events,
        "work_orders": work_orders,
        "maintenance_records": maintenance_records,
    }
