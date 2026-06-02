from __future__ import annotations

import math
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from statistics import mean, pstdev
from typing import Any


def _parse_utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def _format_utc(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _window_start(value: datetime) -> datetime:
    minute = (value.minute // 15) * 15
    return value.replace(minute=minute, second=0, microsecond=0)


def _p95(values: list[float]) -> float:
    ordered = sorted(values)
    index = min(len(ordered) - 1, math.ceil(len(ordered) * 0.95) - 1)
    return ordered[index]


def aggregate_15m(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, datetime], list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        event_time = _parse_utc(str(record["event_time"]))
        grouped[(str(record["tenant_id"]), str(record["tag_id"]), _window_start(event_time))].append(record)

    windows: list[dict[str, Any]] = []
    for (_tenant_id, _tag_id, start), group in sorted(grouped.items(), key=lambda item: item[0]):
        values = [float(record["value"]) for record in group]
        first = group[0]
        metadata = first.get("synthetic_metadata", {})
        if not isinstance(metadata, dict):
            metadata = {}
        quality_flags = sorted(
            {flag for record in group for flag in record.get("quality_flags", []) if isinstance(flag, str)}
        )
        stddev_value = pstdev(values) if len(values) > 1 else 0.0
        windows.append(
            {
                "tenant_id": str(first["tenant_id"]),
                "asset_id": str(metadata.get("asset_id", "")),
                "component_id": str(metadata.get("component_id", "")),
                "measurement_point_id": str(metadata.get("measurement_point_id", "")),
                "tag_id": str(first["tag_id"]),
                "window_start": _format_utc(start),
                "window_end": _format_utc(start + timedelta(minutes=15)),
                "avg_value": round(mean(values), 3),
                "min_value": round(min(values), 3),
                "max_value": round(max(values), 3),
                "stddev_value": round(stddev_value, 6),
                "p95_value": round(_p95(values), 3),
                "slope": round((values[-1] - values[0]) / max(len(values) - 1, 1), 6),
                "z_score": 0.0 if stddev_value == 0 else round((values[-1] - mean(values)) / stddev_value, 6),
                "event_count": len(group),
                "expected_count": 15,
                "quality_flags": quality_flags,
                "operating_state": str(metadata.get("operating_state", "unknown")),
                "degradation_stage": str(metadata.get("degradation_stage", "none")),
            }
        )
    return windows
