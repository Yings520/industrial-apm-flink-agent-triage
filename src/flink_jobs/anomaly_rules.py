from __future__ import annotations

import hashlib
import statistics
from collections.abc import Iterable

EnrichedEvent = dict[str, object]


def stable_anomaly_id(
    rule_id: str,
    tenant_id: str,
    asset_id: str,
    tag_id: str,
    metric_name: str,
    window_start: str,
    window_end: str,
) -> str:
    stable_key = "|".join([rule_id, tenant_id, asset_id, tag_id, metric_name, window_start, window_end])
    return "anom_" + hashlib.sha256(stable_key.encode("utf-8")).hexdigest()[:20]


def _sorted_events(events: Iterable[EnrichedEvent]) -> list[EnrichedEvent]:
    return sorted(events, key=lambda event: (str(event["event_time"]), str(event["event_id"])))


def _base_anomaly(rule_id: str, events: list[EnrichedEvent], severity: str, breach_count: int) -> dict[str, object]:
    ordered = _sorted_events(events)
    first = ordered[0]
    last = ordered[-1]
    values = [float(event["value"]) for event in ordered]
    observed = values[-1]
    baseline = round(statistics.mean(values[:-1]), 6) if len(values) > 1 else None
    window_start = str(first["event_time"])
    window_end = str(last["event_time"])
    source_event_ids = [str(event["event_id"]) for event in ordered[:10]]
    quality_flags = sorted({flag for event in ordered for flag in event.get("quality_flags", [])})
    return {
        "anomaly_id": stable_anomaly_id(
            rule_id=rule_id,
            tenant_id=str(first["tenant_id"]),
            asset_id=str(first["asset_id"]),
            tag_id=str(first["tag_id"]),
            metric_name=str(first["metric_name"]),
            window_start=window_start,
            window_end=window_end,
        ),
        "schema_version": "anomaly_event.v1",
        "rule_id": rule_id,
        "tenant_id": first["tenant_id"],
        "plant_id": first["plant_id"],
        "asset_id": first["asset_id"],
        "asset_name": first["asset_name"],
        "tag_id": first["tag_id"],
        "tag_name": first["tag_name"],
        "metric_name": first["metric_name"],
        "observed_value": observed,
        "baseline_value": baseline,
        "expected_value": baseline,
        "event_count": len(ordered),
        "breach_count": breach_count,
        "window_start": window_start,
        "window_end": window_end,
        "severity": severity,
        "quality_flags": quality_flags,
        "detection_confidence": 0.9,
        "source_event_ids": source_event_ids,
    }


def detect_missing_heartbeat(events: list[EnrichedEvent], max_gap_minutes: int = 10) -> dict[str, object] | None:
    ordered = _sorted_events(events)
    if len(ordered) < 2:
        return None
    from src.flink_jobs.event_time import parse_event_time

    for previous, current in zip(ordered, ordered[1:]):
        gap_minutes = (
            parse_event_time(str(current["event_time"])) - parse_event_time(str(previous["event_time"]))
        ).total_seconds() / 60
        if gap_minutes >= max_gap_minutes:
            anomaly = _base_anomaly("missing_heartbeat", [previous, current], "critical", 1)
            anomaly["expected_value"] = float(max_gap_minutes)
            anomaly["observed_value"] = gap_minutes
            anomaly["detection_confidence"] = 0.95
            return anomaly
    return None


def detect_flatline(events: list[EnrichedEvent], min_repeated_points: int = 6) -> dict[str, object] | None:
    ordered = _sorted_events(events)
    if len(ordered) < min_repeated_points:
        return None
    tail = ordered[-min_repeated_points:]
    values = {float(event["value"]) for event in tail}
    if len(values) == 1:
        anomaly = _base_anomaly("flatline", tail, "warning", min_repeated_points)
        anomaly["baseline_value"] = float(tail[0]["value"])
        anomaly["expected_value"] = None
        return anomaly
    return None


def detect_threshold_spike(events: list[EnrichedEvent], multiplier: float = 2.5) -> dict[str, object] | None:
    ordered = _sorted_events(events)
    if len(ordered) < 2:
        return None
    values = [float(event["value"]) for event in ordered]
    baseline = statistics.mean(values[:-1])
    observed = values[-1]
    if observed >= baseline * multiplier:
        anomaly = _base_anomaly("threshold_spike", ordered, "critical", 1)
        anomaly["baseline_value"] = round(baseline, 6)
        anomaly["expected_value"] = round(baseline * multiplier, 6)
        return anomaly
    return None


def detect_rolling_zscore(events: list[EnrichedEvent], zscore_threshold: float = 3.0) -> dict[str, object] | None:
    ordered = _sorted_events(events)
    if len(ordered) < 4:
        return None
    values = [float(event["value"]) for event in ordered]
    baseline_values = values[:-1]
    mean = statistics.mean(baseline_values)
    stdev = statistics.pstdev(baseline_values)
    if stdev == 0:
        return None
    zscore = abs((values[-1] - mean) / stdev)
    if zscore >= zscore_threshold:
        anomaly = _base_anomaly("rolling_zscore", ordered, "warning", 1)
        anomaly["baseline_value"] = round(mean, 6)
        anomaly["expected_value"] = round(mean + zscore_threshold * stdev, 6)
        anomaly["detection_confidence"] = min(0.99, round(zscore / (zscore_threshold * 2), 3))
        return anomaly
    return None


def detect_drift_rate_of_change(events: list[EnrichedEvent], max_slope_per_point: float = 0.8) -> dict[str, object] | None:
    ordered = _sorted_events(events)
    if len(ordered) < 3:
        return None
    values = [float(event["value"]) for event in ordered]
    slope = (values[-1] - values[0]) / (len(values) - 1)
    if abs(slope) >= max_slope_per_point:
        anomaly = _base_anomaly("drift_rate_of_change", ordered, "warning", len(values) - 1)
        anomaly["observed_value"] = round(slope, 6)
        anomaly["baseline_value"] = values[0]
        anomaly["expected_value"] = max_slope_per_point
        return anomaly
    return None


def detect_all(events: list[EnrichedEvent]) -> list[dict[str, object]]:
    detectors = [
        detect_missing_heartbeat,
        detect_flatline,
        detect_threshold_spike,
        detect_rolling_zscore,
        detect_drift_rate_of_change,
    ]
    anomalies: list[dict[str, object]] = []
    for detector in detectors:
        anomaly = detector(events)
        if anomaly is not None:
            anomalies.append(anomaly)
    return anomalies

