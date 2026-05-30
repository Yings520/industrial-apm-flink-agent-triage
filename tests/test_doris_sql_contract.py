from pathlib import Path
import re


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
        "fact_apm_incidents",
        "fact_apm_alert_routing",
        "fact_apm_operator_feedback",
        "fact_apm_llm_invocations",
    ):
        assert table in SCHEMA_SQL
    assert "CREATE DATABASE IF NOT EXISTS industrial_apm" in SCHEMA_SQL
    assert "DUPLICATE KEY" in SCHEMA_SQL
    assert '"replication_num" = "1"' in SCHEMA_SQL
    assert "CREATE TABLE IF NOT EXISTS fact_apm_llm_invocations" in SCHEMA_SQL


def test_phase4_doris_tables_exist() -> None:
    for table_columns in (
        ("fact_apm_incidents", "incident_id", "correlation_key", "source_anomaly_ids", "routing_channel"),
        ("fact_apm_alert_routing", "route_id", "routing_status", "notification_target", "error_message"),
        ("fact_apm_operator_feedback", "feedback_id", "incident_id", "severity_correction", "is_true_positive"),
        ("fact_apm_llm_invocations", "invocation_id", "raw_response", "parsed_status", "quarantine_reason"),
    ):
        table = table_columns[0]
        assert table in SCHEMA_SQL, f"{table} missing from schema"
        for col in table_columns[1:]:
            assert col in SCHEMA_SQL, f"{col} missing from {table}"


def test_doris_duplicate_keys_are_ordered_column_prefixes() -> None:
    table_pattern = re.compile(
        r"CREATE TABLE IF NOT EXISTS\s+(?P<table>\w+)\s+\((?P<columns>(?:(?!UNIQUE KEY).)*?)\)\s+"
        r"DUPLICATE KEY\((?P<keys>.*?)\)",
        re.DOTALL,
    )
    for match in table_pattern.finditer(SCHEMA_SQL):
        table = match.group("table")
        columns = []
        for raw_line in match.group("columns").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("--"):
                continue
            columns.append(line.split()[0])
        keys = [key.strip() for key in match.group("keys").split(",")]
        assert columns[: len(keys)] == keys, f"{table} duplicate key must be an ordered column prefix"


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
    assert "COUNT(*) AS row_count FROM serving_apm_triage_evidence" in INSPECTION_SQL
    assert "COUNT(*) AS row_count FROM fact_apm_agent_recommendations" in INSPECTION_SQL
    assert "COUNT(*) AS row_count FROM fact_apm_incidents" in INSPECTION_SQL
    assert "COUNT(*) AS row_count FROM fact_apm_alert_routing" in INSPECTION_SQL
    assert "COUNT(*) AS row_count FROM fact_apm_operator_feedback" in INSPECTION_SQL
    assert "COUNT(*) AS row_count FROM fact_apm_llm_invocations" in INSPECTION_SQL


def test_flink_sql_outputs_all_serving_mart_topics() -> None:
    flink_sql = Path("sql/flink_anomalies.sql").read_text()
    assert "tenant_northwind.mart_apm__fct_anomaly_events.v1" in flink_sql
    assert "tenant_northwind.mart_apm__fct_late_sensor_readings.v1" in flink_sql
    assert "tenant_northwind.mart_apm__fct_stream_health_snapshots.v1" in flink_sql
    assert "INSERT INTO mart_late_sensor_readings" in flink_sql
    assert "INSERT INTO mart_stream_health" in flink_sql
