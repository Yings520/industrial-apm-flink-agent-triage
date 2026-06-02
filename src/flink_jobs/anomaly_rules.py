from __future__ import annotations

import hashlib
import json
import statistics
from collections.abc import Iterable
from typing import Any

EnrichedEvent = dict[str, Any]
WindowAggregate = dict[str, Any]
RuleParams = dict[str, Any]
ProfileThresholds = dict[str, float | None]
AnomalyResult = dict[str, object]


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
    return "anom_" + hashlib.sha256(stable_key.encode("utf-8")).hexdigest()[:16]


def parse_rule_parameters(parameters_raw: str | RuleParams | None) -> RuleParams:
    if parameters_raw is None:
        return {}
    if isinstance(parameters_raw, dict):
        return parameters_raw
    if parameters_raw.strip():
        parsed = json.loads(parameters_raw)
        if isinstance(parsed, dict):
            return parsed
    return {}


def resolve_severity(
    rule_params: RuleParams,
    profile_severity: str | None = None,
    rule_severity: str | None = None,
    default_severity: str = "warning",
) -> str:
    if profile_severity and profile_severity != "*":
        return profile_severity
    if rule_severity and rule_severity != "*":
        return rule_severity
    return rule_params.get("severity", default_severity)


def resolve_fault_type(quality_flags: list[str], parameters: RuleParams) -> str:
    sensor_flags = {"calibration_drift", "sensor_stuck", "dead_sensor", "noise_spike", "communication_loss"}
    has_sensor_fault = sensor_flags & {flag.lower() for flag in quality_flags}
    if has_sensor_fault:
        return "sensor_fault"
    if parameters.get("fault_type") == "sensor_fault":
        return "sensor_fault"
    if parameters.get("fault_type") == "operating_context_change":
        return "operating_context_change"
    return "asset_fault"


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
    quality_flags = sorted({str(flag) for event in ordered for flag in (event.get("quality_flags") or [])})
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
        "plant_id": first.get("plant_id"),
        "asset_id": first["asset_id"],
        "asset_name": first.get("asset_name"),
        "component_id": first.get("component_id"),
        "tag_id": first["tag_id"],
        "tag_name": first.get("tag_name"),
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
        "quality_event_ids": None,
        "fault_type": resolve_fault_type(quality_flags, {}),
        "failure_mode_id": None,
        "detection_confidence": 0.9,
        "source_event_ids": source_event_ids,
    }


def _base_window_anomaly(
    rule_id: str,
    window_data: WindowAggregate,
    severity: str,
    rule_params: RuleParams | None = None,
) -> AnomalyResult:
    params = rule_params or {}
    quality_flags_raw = window_data.get("quality_flags")
    if isinstance(quality_flags_raw, list) and quality_flags_raw:
        if isinstance(quality_flags_raw[0], list):
            quality_flags = quality_flags_raw[0]
        else:
            quality_flags = quality_flags_raw
    else:
        quality_flags = []
    return {
        "anomaly_id": stable_anomaly_id(
            rule_id=rule_id,
            tenant_id=str(window_data.get("tenant_id", "")),
            asset_id=str(window_data.get("asset_id", "")),
            tag_id=str(window_data.get("tag_id", "")),
            metric_name=str(window_data.get("metric_name", "")),
            window_start=str(window_data.get("window_start", "")),
            window_end=str(window_data.get("window_end", "")),
        ),
        "schema_version": "anomaly_event.v1",
        "rule_id": rule_id,
        "tenant_id": window_data.get("tenant_id"),
        "plant_id": window_data.get("plant_id"),
        "asset_id": window_data.get("asset_id"),
        "asset_name": window_data.get("asset_name"),
        "component_id": window_data.get("component_id"),
        "tag_id": window_data.get("tag_id"),
        "tag_name": window_data.get("tag_name"),
        "metric_name": window_data.get("metric_name"),
        "observed_value": window_data.get("observed_value"),
        "baseline_value": window_data.get("baseline_value"),
        "expected_value": window_data.get("expected_value"),
        "event_count": window_data.get("event_count"),
        "breach_count": window_data.get("breach_count"),
        "window_start": window_data.get("window_start"),
        "window_end": window_data.get("window_end"),
        "severity": severity,
        "quality_flags": quality_flags,
        "quality_event_ids": None,
        "fault_type": resolve_fault_type([str(f) for f in quality_flags], params),
        "failure_mode_id": window_data.get("failure_mode_id"),
        "detection_confidence": params.get("detection_confidence", 0.9),
        "source_event_ids": window_data.get("source_event_ids"),
    }


# =============================================================================
# D3.1: Threshold spike detection
# Checks observed_value against threshold_upper/lower from profile.
# Falls back to multiplier-based detection against baseline when no profile.
# =============================================================================
def detect_threshold_spike(
    events: list[EnrichedEvent],
    multiplier: float = 2.5,
    threshold_upper: float | None = None,
    threshold_lower: float | None = None,
) -> dict[str, object] | None:
    ordered = _sorted_events(events)
    if len(ordered) < 2:
        return None
    values = [float(event["value"]) for event in ordered]
    baseline = statistics.mean(values[:-1])
    observed = values[-1]
    breach_detected = False
    effective_expected: float | None = None

    if threshold_upper is not None and observed >= threshold_upper:
        breach_detected = True
        effective_expected = threshold_upper
    if threshold_lower is not None and observed <= threshold_lower:
        breach_detected = True
        effective_expected = threshold_lower

    if not breach_detected:
        if observed >= baseline * multiplier:
            breach_detected = True
            effective_expected = baseline * multiplier

    if not breach_detected:
        return None

    anomaly = _base_anomaly("threshold_spike", ordered, "critical", 1)
    anomaly["baseline_value"] = round(baseline, 6)
    anomaly["expected_value"] = round(effective_expected, 6) if effective_expected is not None else None
    return anomaly


def detect_threshold_spike_from_window(
    window: WindowAggregate,
    threshold_upper: float | None = None,
    threshold_lower: float | None = None,
    multiplier: float = 2.5,
) -> AnomalyResult | None:
    observed = float(window.get("observed_value", 0))
    baseline = window.get("rolling_baseline_mean") or window.get("baseline_value") or 0.0
    if isinstance(baseline, float) or isinstance(baseline, int):
        baseline = float(baseline)

    breach_detected = False
    effective_expected: float | None = None

    if threshold_upper is not None and observed >= threshold_upper:
        breach_detected = True
        effective_expected = threshold_upper
    if threshold_lower is not None and observed <= threshold_lower:
        breach_detected = True
        effective_expected = threshold_lower
    if observed >= baseline * multiplier:
        breach_detected = True
        effective_expected = baseline * multiplier

    if not breach_detected:
        return None

    result = _base_window_anomaly("threshold_spike", window, "critical")
    result["baseline_value"] = round(baseline, 6)
    result["expected_value"] = round(effective_expected, 6) if effective_expected is not None else None
    return result


# =============================================================================
# D3.2: Rolling z-score detection
# Z-score = (observed - rolling_mean) / rolling_stddev
# Anomaly when |z-score| >= threshold
# =============================================================================
def detect_rolling_zscore(
    events: list[EnrichedEvent],
    zscore_threshold: float = 3.0,
) -> dict[str, object] | None:
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


def detect_rolling_zscore_from_window(
    window: WindowAggregate,
    zscore_threshold: float = 3.0,
) -> AnomalyResult | None:
    rolling_mean = window.get("rolling_baseline_mean")
    rolling_stddev = window.get("rolling_baseline_stddev")
    observed = float(window.get("observed_value", 0))

    if rolling_mean is None or rolling_stddev is None:
        return None
    mean = float(rolling_mean)
    stdev = float(rolling_stddev)
    if stdev == 0:
        return None

    zscore = abs((observed - mean) / stdev)
    if zscore < zscore_threshold:
        return None

    result = _base_window_anomaly("rolling_zscore", window, "warning")
    result["baseline_value"] = round(mean, 6)
    result["expected_value"] = round(mean + zscore_threshold * stdev, 6)
    result["detection_confidence"] = min(0.99, round(zscore / (zscore_threshold * 2), 3))
    return result


# =============================================================================
# D3.3: Drift rate of change detection
# Rate of change between consecutive windows
# Anomaly when |rate| >= max_slope_per_point
# =============================================================================
def detect_drift_rate_of_change(
    events: list[EnrichedEvent],
    max_slope_per_point: float = 0.8,
) -> dict[str, object] | None:
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


def detect_drift_rate_of_change_from_window(
    window: WindowAggregate,
    max_slope_per_minute: float = 0.8,
) -> AnomalyResult | None:
    observed = float(window.get("observed_value", 0))
    prev_value = window.get("prev_window_value")

    if prev_value is None:
        return None

    prev_value_f = float(prev_value)
    time_diff_minutes = 5.0

    rate = abs(observed - prev_value_f) / max(time_diff_minutes, 1.0)
    if rate < max_slope_per_minute:
        return None

    result = _base_window_anomaly("drift_rate_of_change", window, "warning")
    result["observed_value"] = round(rate, 6)
    result["baseline_value"] = prev_value_f
    result["expected_value"] = max_slope_per_minute
    return result


# =============================================================================
# D3.4: Missing heartbeat detection
# Detects time gaps > max_gap_minutes between consecutive events
# =============================================================================
def detect_missing_heartbeat(
    events: list[EnrichedEvent],
    max_gap_minutes: int = 10,
) -> dict[str, object] | None:
    from src.flink_jobs.event_time import parse_event_time

    ordered = _sorted_events(events)
    if len(ordered) < 2:
        return None

    for previous, current in zip(ordered, ordered[1:], strict=False):
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


def detect_missing_heartbeat_from_window(
    window: WindowAggregate,
    max_gap_minutes: int = 10,
) -> AnomalyResult | None:
    gap_minutes = window.get("gap_minutes")
    if gap_minutes is None:
        return None

    gap = float(gap_minutes)
    if gap < max_gap_minutes:
        return None

    result = _base_window_anomaly("missing_heartbeat", window, "critical")
    result["observed_value"] = gap
    result["expected_value"] = float(max_gap_minutes)
    result["detection_confidence"] = 0.95
    return result


# =============================================================================
# D3.5: Flatline detection
# Detects consecutive readings with stddev=0 (frozen/stuck sensor)
# =============================================================================
def detect_flatline(
    events: list[EnrichedEvent],
    min_repeated_points: int = 6,
) -> dict[str, object] | None:
    ordered = _sorted_events(events)
    if len(ordered) < min_repeated_points:
        return None
    tail = ordered[-min_repeated_points:]
    values = {float(event["value"]) for event in tail}
    if len(values) == 1:
        anomaly = _base_anomaly("flatline", tail, "warning", min_repeated_points)
        anomaly["baseline_value"] = float(tail[0]["value"])
        anomaly["expected_value"] = None
        anomaly["fault_type"] = "sensor_fault"
        return anomaly
    return None


def detect_flatline_from_window(
    window: WindowAggregate,
    min_repeated_points: int = 3,
) -> AnomalyResult | None:
    window_stddev = window.get("window_stddev")
    event_count = window.get("event_count")

    stddev_val = float(window_stddev) if window_stddev is not None else 1.0
    count_val = int(event_count) if event_count is not None else 0

    if stddev_val != 0 or count_val < min_repeated_points:
        return None

    result = _base_window_anomaly("flatline", window, "warning")
    result["baseline_value"] = window.get("observed_value")
    result["expected_value"] = None
    result["fault_type"] = "sensor_fault"
    return result


# =============================================================================
# Unified detection API
# =============================================================================
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


def detect_all_from_window(
    window: WindowAggregate,
    rule_params: RuleParams | None = None,
) -> list[AnomalyResult]:
    params = rule_params or {}
    method = str(window.get("method", ""))

    detector_map = {
        "static_threshold": lambda w: detect_threshold_spike_from_window(
            w,
            threshold_upper=__safe_float(window.get("profile_critical_max")),
            threshold_lower=__safe_float(window.get("profile_critical_min")),
            multiplier=float(params.get("multiplier", 2.5)),
        ),
        "rolling_baseline_zscore": lambda w: detect_rolling_zscore_from_window(
            w,
            zscore_threshold=float(params.get("zscore_threshold", 3.0)),
        ),
        "slope_over_window": lambda w: detect_drift_rate_of_change_from_window(
            w,
            max_slope_per_minute=float(params.get("max_slope_per_hour", 0.8)),
        ),
        "repeated_value_window": lambda w: detect_flatline_from_window(
            w,
            min_repeated_points=int(params.get("min_repeated_points", 3)),
        ),
        "heartbeat_gap": lambda w: detect_missing_heartbeat_from_window(
            w,
            max_gap_minutes=int(params.get("max_gap_minutes", 10)),
        ),
    }

    detector = detector_map.get(method)
    if detector is None:
        return []

    result = detector(window)
    return [result] if result else []


def detect_from_window_with_rules(
    window: WindowAggregate,
    rules_config: list[dict[str, Any]],
) -> list[AnomalyResult]:
    method = str(window.get("method", ""))
    matching_rules = [r for r in rules_config if r.get("method") == method]

    anomalies: list[AnomalyResult] = []
    for rule in matching_rules:
        params = parse_rule_parameters(rule.get("parameters"))
        results = detect_all_from_window(window, params)
        anomalies.extend(results)
    return anomalies


def __safe_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(str(value))
    except (ValueError, TypeError):
        return None
