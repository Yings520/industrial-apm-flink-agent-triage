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
    assert "dwd_apm_anomaly_events" in LOADER
    assert "dwd_apm_realtime_signal_readings" in LOADER
    assert "dwd_apm_late_sensor_readings" in LOADER
    assert "dwd_apm_stream_health_snapshots" in LOADER


def test_phase3_triage_loader_has_clickhouse_text_casts() -> None:
    assert "USE_CLICKHOUSE" in LOADER
    assert "toString({column})" in LOADER
    assert "CAST({column} AS CHAR)" in LOADER


def test_phase3_triage_loader_normalizes_datetime_literals() -> None:
    assert "_datetime_sql" in LOADER
    assert ".replace(\"T\", \" \")" in LOADER
    assert ".removesuffix(\"Z\")" in LOADER
