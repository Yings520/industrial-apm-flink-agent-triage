from pathlib import Path

import pytest


def test_phase4_loader_queries_anomalies_and_writes_incidents() -> None:
    script = Path("scripts/phase4-load-incidents.py").read_text()
    assert "fact_apm_anomaly_events" in script
    assert "fact_apm_incidents" in script
    assert "fact_apm_alert_routing" in script
    assert "fact_apm_operator_feedback" in script
    assert "fact_apm_llm_invocations" in script
    assert "group_anomalies" in script
    assert "generate_llm_recommendation" in script
    assert "validate_recommendation" in script
    assert "TEAMS_WEBHOOK_URL" in script
    assert "send_teams_card" in script


def test_makefile_contains_phase4_targets() -> None:
    makefile = Path("Makefile").read_text()
    assert "phase4-load-incidents:" in makefile


def test_makefile_documents_industrial_apm_demo_flow() -> None:
    makefile = Path("Makefile").read_text()
    readme = Path("README.md").read_text()

    assert "industrial-apm-demo" in makefile
    assert "backfill_replay" in readme
    assert "realtime_live" in readme
    assert "APM Landing Dashboard" in readme


def test_feedback_contract_requires_incident_id() -> None:
    schema = Path("contracts/operator_feedback.schema.json").read_text()
    import json
    parsed = json.loads(schema)
    assert "incident_id" in parsed["required"]


def test_doris_schema_contains_phase4_tables() -> None:
    schema = Path("sql/doris/ddl/schema.sql").read_text()
    assert "CREATE TABLE IF NOT EXISTS fact_apm_incidents" in schema
    assert "CREATE TABLE IF NOT EXISTS fact_apm_alert_routing" in schema
    assert "CREATE TABLE IF NOT EXISTS fact_apm_operator_feedback" in schema
    assert "CREATE TABLE IF NOT EXISTS fact_apm_llm_invocations" in schema


def test_docs_mention_feedback_is_not_production_audit() -> None:
    docs = Path("docs/data-contracts.md").read_text()
    assert "not a production operator audit trail" in docs


def test_ci_workflow_exists_and_is_docker_free() -> None:
    ci = Path(".github/workflows/ci.yml").read_text()
    assert "python -m pytest" in ci
    assert "docker compose" not in ci
    lines = ci.split("\n")
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("#") or stripped.startswith("echo") or stripped.startswith('echo "'):
            continue
        assert "OPENAI_API_KEY" not in stripped, f"CI contains OPENAI_API_KEY reference: {stripped[:100]}"
        assert "TEAMS_WEBHOOK_URL" not in stripped, f"CI contains TEAMS_WEBHOOK_URL reference: {stripped[:100]}"


def test_phase4_e2e_script_contains_row_checks() -> None:
    script = Path("scripts/phase4-e2e.sh").read_text()
    assert "make phase3-load-triage" in script
    assert "make phase4-load-incidents" in script
    assert "make serving-query" in script
    assert "OPENAI_API_KEY" in script
    assert "TEAMS_WEBHOOK_URL" in script
    assert "fallback" in script.lower() or "simulated" in script.lower()
    for table in (
        "fact_apm_incidents",
        "fact_apm_alert_routing",
        "fact_apm_operator_feedback",
        "fact_apm_llm_invocations",
    ):
        assert f"SELECT COUNT(*) > 0 FROM industrial_apm.{table}" in script


def test_failure_mode_names_in_tests_or_docs() -> None:
    failure_modes: dict[str, bool] = {
        "schema_drift": False,
        "late_data_spike": False,
        "sensor_outage": False,
        "kafka_lag": False,
        "alert_storm": False,
        "flink_checkpoint_failure": False,
        "llm_outage": False,
        "llm_invalid_output": False,
        "tenant_leakage": False,
        "teams_webhook_failure": False,
        "cost_spike": False,
    }

    if Path("docs/failure-modes.md").exists():
        docs = Path("docs/failure-modes.md").read_text().lower()
        for mode in failure_modes:
            normalized = mode.lower().replace("_", " ")
            found = normalized in docs
            if not found and "invalid" in normalized:
                found = normalized.split(" ")[0] in docs
            assert found, f"failure mode '{mode}' not found in docs/failure-modes.md"
    else:
        pytest.skip("docs/failure-modes.md not yet created (Plan 04-04 will create it)")


def test_readme_mentions_phase4_and_safe_wording() -> None:
    readme = Path("README.md").read_text()
    assert "make phase4-e2e" in readme
    assert "phase4-e2e" in readme
    assert "OPENAI_API_KEY" in readme
    assert "OPENAI_BASE_URL" in readme
    assert "OPENAI_MODEL" in readme
    assert "TEAMS_WEBHOOK_URL" in readme
    assert "fallback" in readme.lower()
    assert "simulated" in readme.lower()
    assert "LLM-assisted alert triage" in readme or "LLM-assisted" in readme
    assert "operator support" in readme.lower()
    phase4_start = readme.find("Phase 4 Alert Workflow")
    phase4_section = readme[phase4_start:] if phase4_start >= 0 else readme
    for line in phase4_section.lower().split("\n"):
        if "does not claim" in line or "not claim" in line:
            continue
        assert "autonomous diagnosis" not in line, f"autonomous diagnosis found in README Phase 4: {line[:80]}"
        assert "automatic remediation" not in line, f"automatic remediation found in README Phase 4: {line[:80]}"


def test_readme_mentions_ci_no_paid_llm() -> None:
    readme = Path("README.md").read_text()
    assert "Phase 4" in readme
    assert "CI" in readme
    assert "pytest" in readme or "core tests" in readme


def test_readme_phase3_e2e_references() -> None:
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
