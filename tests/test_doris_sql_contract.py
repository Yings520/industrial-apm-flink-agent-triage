from pathlib import Path
import re


SCHEMA_SQL = Path("sql/doris/ddl/schema.sql").read_text()
ROUTINE_SQL = Path("sql/doris/templates/routine_load.sql.tpl").read_text()
VIEWS_SQL = Path("sql/doris/templates/ads_refresh.sql.tpl").read_text()
VIEW_DDL_SQL = Path("sql/doris/views/serving_views.sql").read_text()
INSPECTION_SQL = Path("sql/doris/templates/inspection.sql.tpl").read_text()
GENERATED_DIR = Path("sql/doris/generated")
TENANTS = ("tenant_northwind", "tenant_apac_ops", "tenant_euro_core")


def test_locked_doris_tables_exist() -> None:
    for table in (
        "dwd_apm_realtime_signal_readings",
        "dwd_apm_anomaly_events",
        "dwd_apm_late_sensor_readings",
        "dwd_apm_stream_health_snapshots",
        "dwd_apm_realtime_diagnosis_events",
        "dwd_apm_dlq_events",
        "serving_apm_triage_evidence",
        "fact_apm_quality_events",
        "ads_apm_asset_condition_snapshot",
        "ads_apm_tag_signal_timeseries_1m",
        "ads_apm_anomaly_event_realtime",
        "ads_apm_stream_health_snapshot",
        "fact_apm_agent_recommendations",
        "fact_apm_incidents",
        "fact_apm_alert_routing",
        "fact_apm_operator_feedback",
        "fact_apm_llm_invocations",
        "dim_apm_measurement_points",
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
        "${tenant_id}.staging_apm__sensor_readings.v1",
        "${tenant_id}.mart_apm__fct_anomaly_events.v1",
        "${tenant_id}.mart_apm__fct_late_sensor_readings.v1",
        "${tenant_id}.mart_apm__fct_stream_health_snapshots.v1",
    ):
        assert topic in ROUTINE_SQL
    assert "CREATE ROUTINE LOAD" in ROUTINE_SQL
    assert "redpanda:19092" in ROUTINE_SQL
    assert "raw_apm__sensor_readings" not in ROUTINE_SQL
    assert "tenant_northwind" not in ROUTINE_SQL
    assert "rl_${tenant_id}_dwd_realtime_signal_readings" in ROUTINE_SQL
    assert '"property.group.id" = "doris_${tenant_id}_dwd_realtime_signal_readings"' in ROUTINE_SQL


def test_doris_template_sql_is_tenant_parameterized() -> None:
    runtime_files = [
        Path("sql/doris/templates/query.sql.tpl"),
        Path("sql/doris/templates/inspection.sql.tpl"),
        Path("sql/doris/templates/ads_refresh.sql.tpl"),
        Path("sql/doris/templates/legacy_routine.sql.tpl"),
        Path("sql/doris/templates/routine_load.sql.tpl"),
    ]

    for path in runtime_files:
        sql = path.read_text()
        assert "tenant_northwind" not in sql, path
        assert "tenant_apac_ops" not in sql, path
        assert "tenant_euro_core" not in sql, path
        assert "${tenant_id}" in sql, path


def test_doris_generated_runtime_sql_is_tenant_concrete() -> None:
    generated_files = []
    for tenant in TENANTS:
        tenant_dir = GENERATED_DIR / tenant
        for name in (
            "routine_load.sql",
            "legacy_routine.sql",
            "ads_refresh.sql",
            "inspection.sql",
            "query.sql",
        ):
            generated_files.append(tenant_dir / name)

    for path in generated_files:
        sql = path.read_text()
        tenant = path.parent.name
        assert "${tenant_id}" not in sql, path
        assert tenant in sql, path
        assert "tenant_northwind" not in sql or tenant == "tenant_northwind"
        assert "tenant_apac_ops" not in sql or tenant == "tenant_apac_ops"
        assert "tenant_euro_core" not in sql or tenant == "tenant_euro_core"

    northwind_routine = (GENERATED_DIR / "tenant_northwind" / "routine_load.sql").read_text()
    assert "rl_tenant_northwind_dwd_realtime_signal_readings" in northwind_routine
    assert '"kafka_topic" = "tenant_northwind.staging_apm__sensor_readings.v1"' in northwind_routine
    assert '"property.group.id" = "doris_tenant_northwind_dwd_realtime_signal_readings"' in northwind_routine


def test_core_doris_tables_use_tenant_id_for_isolation() -> None:
    core_tables = [
        "dwd_apm_realtime_signal_readings",
        "dwd_apm_anomaly_events",
        "dwd_apm_late_sensor_readings",
        "dwd_apm_stream_health_snapshots",
        "dwd_apm_realtime_diagnosis_events",
        "dwd_apm_dlq_events",
        "ads_apm_asset_condition_snapshot",
        "ads_apm_tag_signal_timeseries_1m",
        "ads_apm_anomaly_event_realtime",
        "ads_apm_stream_health_snapshot",
        "fact_apm_agent_recommendations",
        "fact_apm_incidents",
        "fact_apm_alert_routing",
        "fact_apm_operator_feedback",
        "fact_apm_llm_invocations",
    ]

    for table in core_tables:
        match = re.search(
            rf"CREATE TABLE IF NOT EXISTS\s+{table}\s+\((?P<body>.*?)\)\s+"
            rf"(?P<keys>(?:DUPLICATE|UNIQUE|AGGREGATE) KEY\(.*?\).*?)"
            rf"DISTRIBUTED BY HASH\((?P<bucket>.*?)\)",
            SCHEMA_SQL,
            re.DOTALL,
        )
        assert match is not None, table
        assert re.search(r"\btenant_id\s+VARCHAR\(128\)\s+NOT NULL", match.group("body")), table
        assert "tenant_id" in match.group("keys"), table
        assert "tenant_id" in match.group("bucket"), table


def test_serving_views_and_inspection_queries_exist() -> None:
    for view in (
        "vw_apm_incident_queue",
        "vw_apm_stream_health_current",
        "vw_apm_tenant_asset_health",
        "vw_apm_anomaly_trends",
        "vw_apm_late_event_rate",
        "vw_apm_fleet_health",
        "vw_apm_asset_health_latest",
        "vw_apm_anomaly_rul_detail",
        "serving_apm_triage_evidence",
    ):
        assert view in VIEW_DDL_SQL
    assert "tenant_id" in VIEW_DDL_SQL
    assert "${tenant_id}" in VIEWS_SQL
    assert "COUNT(*) AS row_count FROM dwd_apm_realtime_signal_readings" in INSPECTION_SQL
    assert "COUNT(*) AS row_count FROM dwd_apm_anomaly_events" in INSPECTION_SQL
    assert "COUNT(*) AS row_count FROM ads_apm_anomaly_event_realtime" in INSPECTION_SQL
    assert "COUNT(*) AS row_count FROM fact_apm_agent_recommendations" in INSPECTION_SQL
    assert "COUNT(*) AS row_count FROM fact_apm_incidents" in INSPECTION_SQL
    assert "COUNT(*) AS row_count FROM fact_apm_alert_routing" in INSPECTION_SQL
    assert "COUNT(*) AS row_count FROM fact_apm_operator_feedback" in INSPECTION_SQL
    assert "COUNT(*) AS row_count FROM fact_apm_llm_invocations" in INSPECTION_SQL


def test_ads_anomaly_refresh_uses_explicit_column_order() -> None:
    assert "INSERT INTO ads_apm_anomaly_event_realtime\n(" in VIEWS_SQL
    assert "asset_id,\n    asset_name,\n    component_type" in VIEWS_SQL
    assert "window_start,\n    window_end,\n    failure_mode_id" in VIEWS_SQL


def test_flink_sql_outputs_all_serving_mart_topics() -> None:
    anomaly_sql = Path("sql/flink/generated/tenant_northwind/anomaly.sql").read_text()
    late_sql = Path("sql/flink/generated/tenant_northwind/late_events.sql").read_text()
    health_sql = Path("sql/flink/generated/tenant_northwind/stream_health.sql").read_text()

    assert "tenant_northwind.mart_apm__fct_anomaly_events.v1" in anomaly_sql
    assert "tenant_northwind.mart_apm__fct_late_sensor_readings.v1" in late_sql
    assert "tenant_northwind.mart_apm__fct_stream_health_snapshots.v1" in health_sql
    assert "INSERT INTO tenant_northwind_late_events_sink" in late_sql
    assert "INSERT INTO tenant_northwind_stream_health_snapshots" in health_sql
