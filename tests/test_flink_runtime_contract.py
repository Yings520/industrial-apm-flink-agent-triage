from pathlib import Path
import re


REPO_ROOT = Path(__file__).resolve().parents[1]


def _make_target_body(makefile: str, target: str) -> str:
    match = re.search(rf"^{target}:\n(?P<body>(?:\t.*\n)+)", makefile, re.MULTILINE)
    assert match is not None
    return match.group("body")


def test_flink_make_targets_submit_runtime_sql_not_dry_run() -> None:
    makefile = (REPO_ROOT / "Makefile").read_text(encoding="utf-8")
    enrichment_body = _make_target_body(makefile, "flink-run-enrichment")
    anomalies_body = _make_target_body(makefile, "flink-run-anomalies")

    assert "flink-run-enrichment:" in makefile
    assert "flink-run-anomalies:" in makefile
    assert "--dry-run" not in enrichment_body
    assert "--dry-run" not in anomalies_body
    assert "$(FLINK_SQL) sql/flink/generated/tenant_northwind/enrich.sql" in enrichment_body
    assert "$(FLINK_SQL) sql/flink/generated/tenant_northwind/anomaly.sql" in anomalies_body
    assert "enrich_v2" not in makefile
    assert "$(FLINK_SQL) sql/anomaly_rules.sql" not in makefile


def test_flink_sql_client_preserves_sql_subpath_inside_container() -> None:
    script = (REPO_ROOT / "scripts/flink-sql-client.sh").read_text(encoding="utf-8")

    assert 'container_sql_file="/opt/flink/${sql_file}"' in script
    assert '-f "${container_sql_file}"' in script
    assert '$(basename "$sql_file")' not in script


def test_flink_sql_runtime_scripts_are_single_responsibility_jobs() -> None:
    runtime_jobs = [
        REPO_ROOT / "sql/flink/generated/tenant_northwind/enrich.sql",
        REPO_ROOT / "sql/flink/generated/tenant_northwind/anomaly.sql",
        REPO_ROOT / "sql/flink/generated/tenant_northwind/late_events.sql",
        REPO_ROOT / "sql/flink/generated/tenant_northwind/dlq_events.sql",
        REPO_ROOT / "sql/flink/generated/tenant_northwind/stream_health.sql",
    ]

    for path in runtime_jobs:
        sql = path.read_text(encoding="utf-8")
        assert "execution.runtime-mode' = 'streaming" in sql
        assert "pipeline.name' = 'tenant_northwind-" in sql
        assert sql.count("INSERT INTO") == 1, path
        assert "CREATE TABLE raw_sensor_events" not in sql
        assert "CREATE TABLE staging_sensor_readings" not in sql
        assert "CREATE TABLE anomaly_staging" not in sql
        assert "CREATE TABLE dlq_events" not in sql
        assert "CREATE TABLE stream_health_snapshots" not in sql

    assert "tenant_northwind.raw_apm__sensor_readings.v1" in runtime_jobs[0].read_text(encoding="utf-8")
    assert "tenant_northwind.staging_apm__sensor_readings.v1" in runtime_jobs[0].read_text(encoding="utf-8")
    assert "CREATE TABLE tenant_northwind_enrich_raw_sensor_events" in runtime_jobs[0].read_text(encoding="utf-8")
    assert "INSERT INTO tenant_northwind_enrich_staging_enriched" in runtime_jobs[0].read_text(encoding="utf-8")
    assert "ingest_time STRING" in runtime_jobs[0].read_text(encoding="utf-8")
    assert "tenant_northwind.mart_apm__fct_anomaly_events.v1" in runtime_jobs[1].read_text(encoding="utf-8")
    assert "CREATE TABLE tenant_northwind_anomaly_staging" in runtime_jobs[1].read_text(encoding="utf-8")
    assert "tenant_northwind.mart_apm__fct_late_sensor_readings.v1" in runtime_jobs[2].read_text(encoding="utf-8")
    assert "CREATE TABLE tenant_northwind_late_events_sensor_readings" in runtime_jobs[2].read_text(encoding="utf-8")
    assert "tenant_northwind.raw_apm__dlq_events.v1" in runtime_jobs[3].read_text(encoding="utf-8")
    assert "CREATE TABLE tenant_northwind_dlq_events_sink" in runtime_jobs[3].read_text(encoding="utf-8")
    assert "tenant_northwind.mart_apm__fct_stream_health_snapshots.v1" in runtime_jobs[4].read_text(encoding="utf-8")
    assert "CREATE TABLE tenant_northwind_stream_health_snapshots" in runtime_jobs[4].read_text(encoding="utf-8")


def test_flink_sql_templates_define_job_boundaries() -> None:
    template_dir = REPO_ROOT / "sql/flink/templates"
    expected_templates = {
        "enrich.sql.tpl",
        "anomaly.sql.tpl",
        "late_events.sql.tpl",
        "dlq_events.sql.tpl",
        "stream_health.sql.tpl",
    }

    assert expected_templates == {path.name for path in template_dir.glob("*.sql.tpl")}
