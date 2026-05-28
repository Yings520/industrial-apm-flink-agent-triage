from pathlib import Path


def test_phase3_e2e_contains_visible_data_checks() -> None:
    script = Path("scripts/phase3-e2e.sh").read_text()
    assert "make phase3-reset" in script
    assert "make phase3-load-triage" in script
    assert "make serving-query" in script
    assert "make phase3-report" in script
    for table in (
        "dwd_apm_sensor_readings_rt",
        "fact_apm_anomaly_events",
        "fact_apm_late_sensor_readings",
        "fact_apm_stream_health_snapshots",
        "fact_apm_quality_events",
        "serving_apm_triage_evidence",
        "fact_apm_agent_recommendations",
    ):
        assert f"SELECT COUNT(*) > 0 FROM industrial_apm.{table}" in script
    assert "Redpanda Console: http://localhost:18080" in script
    assert "Flink UI: http://localhost:18081" in script
    assert "Doris FE: http://localhost:18030" in script


def test_makefile_exposes_phase3_targets() -> None:
    makefile = Path("Makefile").read_text()
    assert "phase3-e2e:" in makefile
    assert "phase3-reset:" in makefile
    assert "phase3-load-triage:" in makefile
    assert "phase3-report:" in makefile
    assert "serving-query:" in makefile


def test_phase3_contract_mentions_routine_load_and_fallback() -> None:
    routine_sql = Path("sql/doris_routine_load.sql").read_text()
    readme = Path("README.md").read_text()
    assert "CREATE ROUTINE LOAD" in routine_sql
    assert "OFFSET_BEGINNING" in routine_sql
    assert "Routine Load" in readme
    assert "fallback" in readme
    assert "local/synthetic" in readme


def test_phase3_reset_script_is_scoped_to_tenant_topics_and_doris_tables() -> None:
    script = Path("scripts/phase3-reset.sh").read_text()
    assert "tenant_northwind" in script
    assert "topic delete" in script
    assert "dwd_apm_sensor_readings_rt" in script
    assert "fact_apm_agent_recommendations" in script
    assert "TRUNCATE TABLE industrial_apm.${table}" in script
