from pathlib import Path
import subprocess


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "full-e2e.sh"
MAKEFILE = REPO_ROOT / "Makefile"
COMPOSE = REPO_ROOT / "docker-compose.yml"


def test_docker_compose_defines_clickhouse_serving_stack() -> None:
    compose = COMPOSE.read_text(encoding="utf-8")

    assert "clickhouse:" in compose
    assert "clickhouse/clickhouse-server" in compose
    assert "./sql:/opt/clickhouse/sql:ro" in compose
    assert "clickhouse-client" in compose
    assert "SELECT 1" in compose
    assert "condition: service_healthy" in compose
    assert "doris:" not in compose

    config = subprocess.run(
        ["docker", "compose", "config", "--services"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    services = set(config.stdout.splitlines())
    assert "clickhouse" in services
    assert "grafana" in services
    assert "doris" not in services


def test_docker_compose_sizes_flink_taskmanager_for_full_e2e_jobs() -> None:
    compose = COMPOSE.read_text(encoding="utf-8")

    assert "taskmanager.memory.process.size: 4096m" in compose
    assert "taskmanager.memory.jvm-metaspace.size: 768m" in compose
    assert "taskmanager.memory.jvm-overhead.min: 512m" in compose
    assert "taskmanager.numberOfTaskSlots: 24" in compose
    assert "MaxMetaspaceSize=768m" in compose
    assert "-Xmx2048m" not in compose


def test_full_e2e_script_exists_and_runs_pipeline_in_order() -> None:
    script = SCRIPT.read_text(encoding="utf-8")
    main_body = script[script.index("main() {") :]

    expected_steps = [
        "check_dependencies",
        "start_base_services",
        "start_serving_stack",
        "reset_local_state",
        "load_postgres_seed_data",
        "register_debezium_connector",
        "create_kafka_topics",
        "submit_flink_jobs",
        "verify_flink_jobs",
        "load_serving_jobs",
        "start_agent_and_data_pump",
        "verify_kafka_outputs",
        "verify_flink_jobs",
        "verify_serving_rows",
    ]
    positions = []
    cursor = 0
    for step in expected_steps:
        position = main_body.index(step, cursor)
        positions.append(position)
        cursor = position + len(step)
    assert positions == sorted(positions)

    assert "docker compose up -d redpanda postgres kafka-connect" in script
    assert "scripts/phase1-load-postgres-source-data.py" in script
    assert "configs/debezium-pg-sensor-tags.json" in script
    assert 'run "${FLINK_SQL}" "sql/flink/generated/${tenant}/enrich.sql"' in script
    assert 'run "${FLINK_SQL}" "sql/flink/generated/${tenant}/anomaly.sql"' in script
    assert "docker compose up -d flink-agent-1 flink-agent-2 flink-agent-3 data-pump" in script
    assert "docker compose up -d flink-agent data-pump" not in script
    assert "topic_name()" in script
    assert "mart_apm__fct_anomaly_events" in script
    assert "src.telemetry_generator.generate_events" not in script
    assert "src.streaming.publish_raw_events" not in script


def test_full_e2e_recreates_and_verifies_flink_jobs() -> None:
    script = SCRIPT.read_text(encoding="utf-8")

    assert "docker compose up -d --force-recreate flink-jobmanager flink-taskmanager" in script
    assert "verify_flink_jobs()" in script
    assert "http://localhost:18081/jobs/overview" in script
    assert "flink-jobs-overview.json" in script
    assert 'job.get("state") != "RUNNING"' in script
    assert "No Flink jobs found" in script
    assert "expected_jobs" in script


def test_stream_health_jobs_are_real_streaming_jobs() -> None:
    template = (REPO_ROOT / "sql/flink/templates/stream_health.sql.tpl").read_text(encoding="utf-8")

    assert "SET 'execution.runtime-mode' = 'streaming'" in template
    assert "AND FALSE" not in template
    assert "WHERE tenant_id = '{{tenant_id}}';" in template

    for tenant in ("tenant_northwind", "tenant_apac_ops", "tenant_euro_core"):
        generated = (REPO_ROOT / f"sql/flink/generated/{tenant}/stream_health.sql").read_text(
            encoding="utf-8"
        )
        assert "AND FALSE" not in generated
        assert f"WHERE tenant_id = '{tenant}';" in generated


def test_full_e2e_script_is_reset_first_and_checks_serving_rows() -> None:
    script = SCRIPT.read_text(encoding="utf-8")

    assert "reset_local_state" in script
    assert "reset_clickhouse_tables" in script
    assert "verify_serving_rows" in script
    for table in (
        "dwd_apm_realtime_signal_readings",
        "dwd_apm_anomaly_events",
        "fact_apm_quality_events",
        "serving_apm_triage_evidence",
        "fact_apm_agent_recommendations",
    ):
        assert f"SELECT COUNT(*) > 0 FROM industrial_apm.{table}" in script


def test_full_e2e_script_prints_manual_inspection_urls() -> None:
    script = SCRIPT.read_text(encoding="utf-8")

    assert "print_manual_summary" in script
    assert "Redpanda Console: http://localhost:18080" in script
    assert "Flink UI: http://localhost:18081" in script
    assert "ClickHouse HTTP: http://localhost:18123/play" in script
    assert "Grafana: http://localhost:13000" in script


def test_full_e2e_waits_for_clickhouse_before_schema_init() -> None:
    script = SCRIPT.read_text(encoding="utf-8")

    assert "wait_for_clickhouse" in script
    assert "clickhouse-client" in script
    assert "sql/clickhouse/ddl/schema.sql" in script
    assert script.index("wait_for_clickhouse") < script.index("sql/clickhouse/ddl/schema.sql")


def test_full_e2e_defaults_to_three_tenant_pipeline() -> None:
    script = SCRIPT.read_text(encoding="utf-8")

    assert 'DEFAULT_TENANTS=(tenant_northwind tenant_apac_ops tenant_euro_core)' in script
    assert "E2E_TENANTS=" in script
    assert '"${E2E_TENANTS[@]}"' in script
    assert "TENANT=\"${TENANT:-tenant_northwind}\"" not in script
    assert 'load_clickhouse_tenant_ingest "${tenant}"' in script
    assert 'load_clickhouse_sql "sql/clickhouse/generated/${tenant}/query.sql"' in script
    for tenant in ("tenant_northwind", "tenant_apac_ops", "tenant_euro_core"):
        assert tenant in script


def test_full_e2e_loads_clickhouse_and_sets_loader_mode() -> None:
    script = SCRIPT.read_text(encoding="utf-8")

    assert "starting ClickHouse and Grafana" in script
    assert "docker compose up -d --force-recreate clickhouse" in script
    assert "sql/clickhouse/ingest/cdc_dim_ingest.sql" in script
    assert "sql/clickhouse/seeds/demo_anomaly_events.sql" in script
    assert "sql/clickhouse/templates/tenant_dwd_ingest.sql.tpl" in script
    assert 'env USE_CLICKHOUSE=1 "${PYTHON}" scripts/phase3-load-triage.py' in script
    assert 'env USE_CLICKHOUSE=1 "${PYTHON}" scripts/phase4-load-incidents.py' in script
    assert "doris-sql.sh" not in script


def test_makefile_exposes_full_e2e_target() -> None:
    makefile = MAKEFILE.read_text(encoding="utf-8")

    assert "full-e2e:" in makefile
    assert "./scripts/full-e2e.sh" in makefile
