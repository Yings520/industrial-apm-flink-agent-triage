from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from typing import Any

_SEVERITY_ORDER: dict[str, int] = {"critical": 3, "warning": 2, "info": 1}


def correlation_key(anomaly: dict[str, Any], bucket_minutes: int = 5) -> str:
    ts = _timestamp_from(anomaly)
    bucket = _time_bucket(ts, bucket_minutes)
    parts = [
        str(anomaly.get("tenant_id", "")),
        str(anomaly.get("asset_id", "")),
        str(anomaly.get("metric_name", "")),
        str(anomaly.get("rule_id", "")),
        bucket.isoformat().replace("+00:00", "Z"),
    ]
    return "|".join(parts)


def base_correlation_key(anomaly: dict[str, Any]) -> str:
    parts = [
        str(anomaly.get("tenant_id", "")),
        str(anomaly.get("asset_id", "")),
        str(anomaly.get("metric_name", "")),
        str(anomaly.get("rule_id", "")),
    ]
    return "|".join(parts)


def incident_id_for(correlation_key: str) -> str:
    digest = hashlib.sha1(correlation_key.encode("utf-8")).hexdigest()[:16]
    return f"inc_{digest}"


def severity_route(severity: str) -> str:
    mapping: dict[str, str] = {
        "critical": "teams_card",
        "warning": "ticket_simulated",
        "info": "dashboard_only",
    }
    return mapping.get(severity, "dashboard_only")


def group_anomalies(
    anomalies: list[dict[str, Any]],
    cooldown_minutes: int = 10,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    _ = now
    cooldown_delta = timedelta(minutes=cooldown_minutes)
    resolved: list[dict[str, Any]] = []
    active: dict[str, dict[str, Any]] = {}

    for anomaly in sorted(anomalies, key=_sort_key):
        key = correlation_key(anomaly)
        base_key = base_correlation_key(anomaly)
        anomaly_id = str(anomaly.get("anomaly_id", ""))
        severity_val = str(anomaly.get("severity", "info"))
        current_time = _timestamp_from(anomaly)

        if base_key in active:
            existing = active[base_key]
            last_ts = _parse_dt(existing["last_seen_at"])
            if current_time > last_ts + cooldown_delta:
                resolved.append(active.pop(base_key))
            else:
                _update_incident(existing, anomaly_id, severity_val, anomaly)
                continue

        incident = _new_incident(key, base_key, anomaly_id, severity_val, anomaly)
        active[base_key] = incident

    return resolved + list(active.values())


def _new_incident(
    correlation_key_val: str,
    base_key: str,
    anomaly_id: str,
    severity_val: str,
    anomaly: dict[str, Any],
) -> dict[str, Any]:
    _ = base_key
    ts = _timestamp_from(anomaly)
    incident_id = incident_id_for(correlation_key_val)
    routing = severity_route(severity_val)
    return {
        "incident_id": incident_id,
        "correlation_key": correlation_key_val,
        "tenant_id": str(anomaly.get("tenant_id", "")),
        "asset_id": str(anomaly.get("asset_id", "")),
        "metric_name": str(anomaly.get("metric_name", "")),
        "rule_id": str(anomaly.get("rule_id", "")),
        "first_seen_at": ts.isoformat().replace("+00:00", "Z"),
        "last_seen_at": ts.isoformat().replace("+00:00", "Z"),
        "anomaly_count": 1,
        "max_severity": severity_val,
        "source_anomaly_ids": [anomaly_id],
        "status": "open",
        "routing_channel": routing,
    }


def _update_incident(
    incident: dict[str, Any],
    anomaly_id: str,
    severity_val: str,
    anomaly: dict[str, Any],
) -> None:
    ts = _timestamp_from(anomaly)
    incident["last_seen_at"] = ts.isoformat().replace("+00:00", "Z")
    incident["anomaly_count"] = int(incident.get("anomaly_count", 0)) + 1
    current = str(incident.get("max_severity", "info"))
    if _severity_rank(severity_val) > _severity_rank(current):
        incident["max_severity"] = severity_val
        incident["routing_channel"] = severity_route(severity_val)
    ids: list[str] = list(incident.get("source_anomaly_ids", []))
    if anomaly_id not in ids:
        ids.append(anomaly_id)
    incident["source_anomaly_ids"] = ids


def _severity_rank(severity: str) -> int:
    return _SEVERITY_ORDER.get(severity, 0)


def _timestamp_from(anomaly: dict[str, Any]) -> datetime:
    for field in ("window_start", "window_end"):
        raw = anomaly.get(field)
        if raw is not None:
            text = str(raw).replace("Z", "+00:00")
            return datetime.fromisoformat(text)
    return datetime.now(UTC)


def _time_bucket(ts: datetime, bucket_minutes: int) -> datetime:
    epoch = ts.timestamp()
    bucket_seconds = bucket_minutes * 60
    floored = (int(epoch) // bucket_seconds) * bucket_seconds
    return datetime.fromtimestamp(floored, tz=UTC)


def _parse_dt(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    text = str(value).replace("Z", "+00:00")
    return datetime.fromisoformat(text)


def _sort_key(anomaly: dict[str, Any]) -> tuple[str, str, str]:
    ts = _timestamp_from(anomaly)
    return (
        str(anomaly.get("tenant_id", "")),
        ts.isoformat().replace("+00:00", "Z"),
        str(anomaly.get("anomaly_id", "")),
    )
