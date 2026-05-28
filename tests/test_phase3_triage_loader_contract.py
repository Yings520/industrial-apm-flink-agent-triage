from pathlib import Path


LOADER = Path("scripts/phase3-load-triage.py").read_text()


def test_phase3_triage_loader_writes_evidence_recommendations_and_quality_events() -> None:
    assert "serving_apm_triage_evidence" in LOADER
    assert "fact_apm_agent_recommendations" in LOADER
    assert "fact_apm_quality_events" in LOADER
    assert "build_triage_evidence" in LOADER
    assert "generate_fallback_recommendation" in LOADER
    assert "validate_recommendation" in LOADER


def test_phase3_triage_loader_reads_from_doris_serving_facts() -> None:
    assert "fact_apm_anomaly_events" in LOADER
    assert "dwd_apm_sensor_readings_rt" in LOADER
    assert "fact_apm_late_sensor_readings" in LOADER
    assert "fact_apm_stream_health_snapshots" in LOADER
