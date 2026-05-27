from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_flink_make_targets_submit_runtime_sql_not_dry_run() -> None:
    makefile = (REPO_ROOT / "Makefile").read_text(encoding="utf-8")

    assert "flink-run-enrichment:" in makefile
    assert "flink-run-anomalies:" in makefile
    assert "--dry-run" not in makefile
    assert "$(FLINK_SQL) sql/flink_enrichment.sql" in makefile
    assert "$(FLINK_SQL) sql/flink_anomalies.sql" in makefile


def test_flink_sql_runtime_scripts_use_bounded_redpanda_topics() -> None:
    enrichment_sql = (REPO_ROOT / "sql/flink_enrichment.sql").read_text(encoding="utf-8")
    anomaly_sql = (REPO_ROOT / "sql/flink_anomalies.sql").read_text(encoding="utf-8")

    assert "tenant_northwind.raw_apm__sensor_readings.v1" in enrichment_sql
    assert "tenant_northwind.staging_apm__sensor_readings.v1" in enrichment_sql
    assert "scan.bounded.mode" in enrichment_sql
    assert "tenant_northwind.staging_apm__sensor_readings.v1" in anomaly_sql
    assert "tenant_northwind.mart_apm__fct_anomaly_events.v1" in anomaly_sql
    assert "scan.bounded.mode" in anomaly_sql
