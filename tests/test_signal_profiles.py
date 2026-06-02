from datetime import UTC, datetime, timedelta

from src.telemetry_generator.signal_profiles import OperatingState, SignalContext, signal_value


def test_degradation_increases_vibration_over_time() -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    context = SignalContext(
        metric_name="vibration_rms",
        normal_min=0.3,
        normal_max=4.5,
        engineering_min=0.0,
        engineering_max=18.0,
        failure_mode_id="fm_bearing_inner_race_wear",
        operating_state=OperatingState.DEGRADATION,
        scenario_start=start,
        scenario_end=start + timedelta(days=30),
        seed="asset_001|bearing",
    )

    early = signal_value(context, start + timedelta(days=2))
    late = signal_value(context, start + timedelta(days=24))

    assert late > early
    assert early >= context.engineering_min
    assert late <= context.engineering_max


def test_maintenance_suppresses_load_sensitive_signals() -> None:
    now = datetime(2026, 1, 15, tzinfo=UTC)
    context = SignalContext(
        metric_name="flow_rate",
        normal_min=80.0,
        normal_max=180.0,
        engineering_min=0.0,
        engineering_max=260.0,
        failure_mode_id="fm_pump_cavitation",
        operating_state=OperatingState.MAINTENANCE,
        scenario_start=now - timedelta(days=1),
        scenario_end=now + timedelta(days=1),
        seed="asset_001|impeller",
    )

    assert signal_value(context, now) < 20.0
