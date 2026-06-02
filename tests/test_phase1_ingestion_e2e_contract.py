import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

TABLES: list[str] = [
    "tenants",
    "assets",
    "asset_components",
    "asset_signal_tags_mapping",
    "workorder",
    "maintenance",
    "apm_failuredomain",
    "apm_failureclass",
    "apm_failuremode",
    "apm_failureevent",
    "apm_anomaly_rule",
    "apm_threshold_profile",
]

TENANTS: list[str] = [
    "tenant_northwind",
    "tenant_apac_ops",
    "tenant_euro_core",
]

CDC_TOPICS: list[str] = [
    "pg_apm.public.asset_signal_tags_mapping",
    "pg_apm.public.apm_anomaly_rule",
    "pg_apm.public.apm_threshold_profile",
    "pg_apm.public.workorder",
    "pg_apm.public.maintenance",
    "pg_apm.public.apm_failureevent",
]

METHODS: list[str] = [
    "threshold_spike",
    "rolling_zscore",
    "drift_rate_of_change",
    "missing_heartbeat",
    "flatline",
]


def run_psql(sql: str) -> str:
    result = subprocess.run(
        [
            "docker", "compose", "exec", "-T", "postgres",
            "psql", "-U", "apm", "-d", "apm_metadata", "-A", "-t", "-c", sql,
        ],
        capture_output=True,
        text=True,
        check=True,
        cwd=REPO_ROOT,
    )
    return result.stdout.strip()


def run_rpk_consume(topic: str, num: int = 1) -> str:
    result = subprocess.run(
        [
            "docker", "compose", "exec", "-T", "redpanda",
            "rpk", "-X", "brokers=localhost:19092",
            "topic", "consume", topic, "--num", str(num),
        ],
        capture_output=True,
        text=True,
        check=True,
        cwd=REPO_ROOT,
    )
    return result.stdout.strip()


def test_pg_tables_exist() -> None:
    for table in TABLES:
        sql = f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{table}');"
        out = run_psql(sql)
        assert out == "t", f"Table {table} does not exist"


def test_pg_data_not_empty() -> None:
    for table in TABLES:
        sql = f"SELECT COUNT(*) FROM {table};"
        out = run_psql(sql)
        count = int(out)
        assert count > 0, f"Table {table} is empty"


def test_raw_topic_consume() -> None:
    for tenant in TENANTS:
        topic = f"{tenant}.raw_apm__sensor_readings.v1"
        out = run_rpk_consume(topic, num=1)
        assert out, f"No messages consumed from {topic}"


def test_dlq_topic_consume() -> None:
    for tenant in TENANTS:
        topic = f"{tenant}.raw_apm__dlq_events.v1"
        out = run_rpk_consume(topic, num=1)
        assert out, f"No messages consumed from {topic}"


def test_cdc_topic_consume() -> None:
    for topic in CDC_TOPICS:
        out = run_rpk_consume(topic, num=1)
        assert out, f"No messages consumed from CDC topic {topic}"


def test_tag_mapping_join() -> None:
    sql = (
        "SELECT COUNT(*) FROM asset_signal_tags_mapping "
        "WHERE asset_id IS NOT NULL AND tag_id IS NOT NULL AND source_system IS NOT NULL;"
    )
    out = run_psql(sql)
    count = int(out)
    assert count > 0, "No tag mapping rows with asset_id, tag_id, and source_system"


def test_rule_profile_match() -> None:
    method_map = {
        "threshold_spike": "static_threshold",
        "rolling_zscore": "rolling_baseline_zscore",
        "drift_rate_of_change": "slope_over_window",
        "missing_heartbeat": "heartbeat_gap",
        "flatline": "repeated_value_window",
    }
    for rule_id_key, method in method_map.items():
        sql = f"SELECT COUNT(*) FROM apm_anomaly_rule WHERE method = '{method}' AND status = 'active';"
        out = run_psql(sql)
        count = int(out)
        assert count > 0, f"No active rule for method: {method} (rule_id: {rule_id_key})"


def test_business_evidence_boundary() -> None:
    sql = (
        "SELECT COUNT(*) FROM apm_failureevent fe "
        "JOIN workorder wo ON fe.tenant_id = wo.tenant_id AND fe.work_order_id = wo.work_order_id;"
    )
    out = run_psql(sql)
    count = int(out)
    assert count > 0, "No failure events joined with work orders"
