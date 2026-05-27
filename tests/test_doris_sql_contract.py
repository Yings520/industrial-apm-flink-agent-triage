from pathlib import Path


SCHEMA_SQL = Path("sql/doris_schema.sql").read_text()
ROUTINE_SQL = Path("sql/doris_routine_load.sql").read_text()
VIEWS_SQL = Path("sql/doris_serving_views.sql").read_text()
INSPECTION_SQL = Path("sql/doris_inspection_queries.sql").read_text()


def test_locked_doris_tables_exist() -> None:
    for table in (
        "dwd_apm_sensor_readings_rt",
        "fact_apm_anomaly_events",
        "fact_apm_late_sensor_readings",
        "fact_apm_stream_health_snapshots",
        "fact_apm_quality_events",
        "serving_apm_triage_evidence",
        "fact_apm_agent_recommendations",
    ):
        assert table in SCHEMA_SQL
    assert "CREATE DATABASE IF NOT EXISTS industrial_apm" in SCHEMA_SQL
    assert "DUPLICATE KEY" in SCHEMA_SQL
    assert '"replication_num" = "1"' in SCHEMA_SQL


def test_routine_load_uses_only_staging_and_mart_topics() -> None:
    for topic in (
        "tenant_northwind.staging_apm__sensor_readings.v1",
        "tenant_northwind.mart_apm__fct_anomaly_events.v1",
        "tenant_northwind.mart_apm__fct_late_sensor_readings.v1",
        "tenant_northwind.mart_apm__fct_stream_health_snapshots.v1",
    ):
        assert topic in ROUTINE_SQL
    assert "CREATE ROUTINE LOAD" in ROUTINE_SQL
    assert "redpanda:19092" in ROUTINE_SQL
    assert "raw_apm__sensor_readings" not in ROUTINE_SQL


def test_serving_views_and_inspection_queries_exist() -> None:
    for view in (
        "vw_apm_incident_queue",
        "vw_apm_stream_health_current",
        "vw_apm_tenant_asset_health",
        "vw_apm_anomaly_trends",
        "vw_apm_late_event_rate",
        "serving_apm_triage_evidence",
    ):
        assert view in VIEWS_SQL
    assert "tenant_id" in VIEWS_SQL
    assert "COUNT(*) AS row_count FROM dwd_apm_sensor_readings_rt" in INSPECTION_SQL
    assert "COUNT(*) AS row_count FROM fact_apm_anomaly_events" in INSPECTION_SQL
