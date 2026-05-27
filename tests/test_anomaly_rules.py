from src.flink_jobs.anomaly_rules import (
    detect_drift_rate_of_change,
    detect_flatline,
    detect_missing_heartbeat,
    detect_rolling_zscore,
    detect_threshold_spike,
    stable_anomaly_id,
)


def _event(index: int, value: float, minute: int | None = None) -> dict[str, object]:
    event_minute = index if minute is None else minute
    return {
        "event_id": f"evt_{index}",
        "schema_version": "enriched_telemetry.v1",
        "tenant_id": "tenant_northwind",
        "plant_id": "plant_northwind_mfg_01",
        "asset_id": "asset_northwind_mfg_01_0001",
        "asset_name": "Cnc Machine 0001",
        "tag_id": "tag_northwind_mfg_01_0001_temperature",
        "tag_name": "cnc_machine_temperature_0001",
        "metric_name": "temperature",
        "threshold_profile_id": "threshold_manufacturing_rotating",
        "event_time": f"2026-01-01T00:{event_minute:02d}:00Z",
        "ingest_time": f"2026-01-01T00:{event_minute:02d}:05Z",
        "value": value,
        "unit": "celsius",
        "source_system": "opc_ua",
        "quality_flags": [],
        "scenario": "normal",
    }


def test_stable_anomaly_id_uses_window_not_observed_value() -> None:
    first = stable_anomaly_id("threshold_spike", "tenant", "asset", "tag", "temperature", "start", "end")
    same = stable_anomaly_id("threshold_spike", "tenant", "asset", "tag", "temperature", "start", "end")
    different_window = stable_anomaly_id("threshold_spike", "tenant", "asset", "tag", "temperature", "start2", "end")

    assert first == same
    assert first != different_window
    assert first.startswith("anom_")


def test_missing_heartbeat_detects_gap_with_evidence() -> None:
    anomaly = detect_missing_heartbeat([_event(1, 10.0, minute=0), _event(2, 10.1, minute=15)])

    assert anomaly is not None
    assert anomaly["rule_id"] == "missing_heartbeat"
    assert anomaly["severity"] == "critical"
    assert anomaly["observed_value"] == 15
    assert anomaly["event_count"] == 2
    assert len(anomaly["source_event_ids"]) <= 10


def test_flatline_detects_repeated_values() -> None:
    anomaly = detect_flatline([_event(index, 7.0) for index in range(6)])

    assert anomaly is not None
    assert anomaly["rule_id"] == "flatline"
    assert anomaly["breach_count"] == 6


def test_threshold_spike_detects_static_threshold_breach() -> None:
    anomaly = detect_threshold_spike([_event(0, 2.0), _event(1, 2.0), _event(2, 6.0)])

    assert anomaly is not None
    assert anomaly["rule_id"] == "threshold_spike"
    assert anomaly["observed_value"] == 6.0
    assert anomaly["expected_value"] == 5.0


def test_rolling_zscore_detects_baseline_outlier() -> None:
    anomaly = detect_rolling_zscore([_event(0, 1.0), _event(1, 1.1), _event(2, 0.9), _event(3, 10.0)])

    assert anomaly is not None
    assert anomaly["rule_id"] == "rolling_zscore"
    assert anomaly["baseline_value"] == 1.0


def test_drift_rate_of_change_detects_monotonic_drift() -> None:
    anomaly = detect_drift_rate_of_change([_event(0, 1.0), _event(1, 2.0), _event(2, 3.0)])

    assert anomaly is not None
    assert anomaly["rule_id"] == "drift_rate_of_change"
    assert anomaly["observed_value"] == 1.0

