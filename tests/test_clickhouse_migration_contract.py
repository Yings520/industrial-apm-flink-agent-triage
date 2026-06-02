import json
import re
import pytest
from pathlib import Path


PROJECT_ROOT = Path(__file__).parent.parent
SCHEMA_SQL = (PROJECT_ROOT / "sql" / "clickhouse" / "ddl" / "schema.sql").read_text()
DASHBOARD_DIR = PROJECT_ROOT / "configs" / "grafana" / "dashboards"
TEMPLATES_DIR = PROJECT_ROOT / "sql" / "clickhouse" / "templates"
CDC_INGEST_SQL = (PROJECT_ROOT / "sql" / "clickhouse" / "ingest" / "cdc_dim_ingest.sql").read_text()

TENANTS = ("tenant_northwind", "tenant_apac_ops", "tenant_euro_core")

DORIS_KEYWORDS = (
    "UNIQUE KEY",
    "DUPLICATE KEY",
    "DISTRIBUTED BY",
    "AUTO_INCREMENT",
    "replication_num",
    "AGGREGATE KEY",
    "PROPERTIES",
)

STALE_TABLE_NAMES = (
    "fact_apm_anomaly_events",
    "agg_apm_sensor_windows_15m",
    "dim_failure_mode",
    "dwd_apm_sensor_readings_rt",
)

FORBIDDEN_TABLE_PATTERNS = re.compile(
    r"\b(fact_apm_rul_predictions|"
    r"ag_apm_|vw_apm_|"
    r"dim_asset\b|dim_component\b|dim_sensor_tag\b|dim_metric\b|"
    r"dim_threshold_profile\b|dim_site\b|dim_failure_mode\b|dim_work_order\b|"
    r"dim_measurement_point\b)",
    re.IGNORECASE,
)

CDC_SOURCE_COLUMNS = {
    "dim_apm_assets": {
        "tenant_id", "asset_id", "asset_name", "asset_type", "asset_class",
        "parent_asset_id", "site_id", "area_id", "line_id",
        "functional_location", "criticality", "lifecycle_state",
        "created_at", "updated_at",
    },
    "dim_apm_asset_components": {
        "tenant_id", "component_id", "asset_id", "parent_component_id",
        "component_name", "component_type", "lifecycle_state",
        "created_at", "updated_at",
    },
    "dim_apm_signal_tags_mapping": {
        "tenant_id", "mapping_id", "asset_id", "component_id", "tag_id",
        "tag_name", "source_system", "measurement_point", "metric_name",
        "signal_type", "unit", "expected_unit", "sampling_rate_seconds",
        "normal_range_min", "normal_range_max", "calibration_status",
        "mapping_status", "valid_from", "valid_to", "created_at", "updated_at",
    },
    "dim_apm_workorder": {
        "tenant_id", "work_order_id", "asset_id", "component_id", "source_system",
        "external_work_order_id", "work_order_type", "work_order_status", "priority",
        "title", "description", "problem_code", "cause_code", "remedy_code",
        "requested_at", "planned_start_at", "planned_end_at", "actual_start_at",
        "completed_at", "downtime_minutes", "technician_notes", "failure_mode_id",
        "related_anomaly_id", "related_tag_id", "created_at", "updated_at",
    },
    "dim_apm_maintenance": {
        "tenant_id", "maintenance_id", "asset_id", "component_id", "work_order_id",
        "maintenance_type", "maintenance_status", "maintenance_reason",
        "maintenance_result", "failure_mode_id", "suspected_failure_mode",
        "confirmed_fault_description", "performed_by", "performed_at",
        "completed_at", "parts_used", "labor_hours", "downtime_minutes",
        "related_tag_id", "related_anomaly_id", "sensor_anomaly_related",
        "evidence_notes", "created_at", "updated_at",
    },
    "dim_apm_failuredomain": {
        "tenant_id", "id", "yaml_id", "code", "label", "description", "active",
        "created_by_id", "modified_by_id", "created_date", "modified_date",
    },
    "dim_apm_failureclass": {
        "tenant_id", "id", "domain_id", "yaml_id", "code", "label", "description",
        "active", "created_by_id", "modified_by_id", "created_date", "modified_date",
    },
    "dim_apm_failuremode": {
        "tenant_id", "id", "failure_class_id", "yaml_id", "code", "label",
        "description", "asset_type", "component_type", "typical_effect",
        "related_metrics", "active", "created_by_id", "modified_by_id",
        "created_date", "modified_date",
    },
    "dim_apm_failureevent": {
        "tenant_id", "failure_event_id", "asset_id", "component_id", "failure_mode_id",
        "event_status", "severity", "occurred_at", "detected_at", "resolved_at",
        "source_system", "source_event_type", "source_event_id", "work_order_id",
        "maintenance_id", "related_anomaly_id", "related_tag_ids", "evidence_summary",
        "label_confidence", "created_at", "updated_at",
    },
    "dim_apm_anomaly_rule": {
        "tenant_id", "rule_id", "rule_name", "method", "asset_type", "component_type",
        "metric_name", "unit", "severity", "parameters", "failure_mode_id",
        "approval_status", "status", "owner", "version", "effective_from",
        "effective_to", "created_at", "updated_at", "created_by", "updated_by",
    },
    "dim_apm_threshold_profiles": {
        "tenant_id", "profile_id", "rule_id", "profile_name", "asset_type",
        "component_type", "metric_name", "unit", "normal_range_min", "normal_range_max",
        "warning_min", "warning_max", "critical_min", "critical_max", "parameters",
        "status", "version", "effective_from", "effective_to", "created_at", "updated_at",
    },
    "dim_apm_tenants": {
        "tenant_id", "tenant_name", "region", "tenant_status", "created_at", "updated_at",
    },
}


def _extract_table_names_from_ddl(ddl_text: str) -> set[str]:
    pattern = re.compile(r"CREATE TABLE IF NOT EXISTS\s+(\w+)")
    return set(pattern.findall(ddl_text))


def _extract_table_names_from_sql(raw_sql: str) -> set[str]:
    table_pattern = re.compile(
        r"\b(?:FROM|JOIN)\b\s+(?:industrial_apm\.)?(\w+)",
        re.IGNORECASE,
    )
    cte_pattern = re.compile(r"\bWITH\s+(\w+)\s+AS|\s*,\s*(\w+)\s+AS\s*\(", re.IGNORECASE)
    cte_names = {
        name
        for match in cte_pattern.findall(raw_sql)
        for name in match
        if name
    }
    return set(table_pattern.findall(raw_sql)) - cte_names


def _extract_create_table_columns(sql: str, table_name: str) -> list[str]:
    pattern = re.compile(
        rf"CREATE TABLE IF NOT EXISTS\s+{re.escape(table_name)}\s*\((.*?)\)\s*ENGINE",
        re.IGNORECASE | re.DOTALL,
    )
    match = pattern.search(sql)
    assert match, f"Missing CREATE TABLE for {table_name}"
    columns: list[str] = []
    for raw_line in match.group(1).splitlines():
        line = raw_line.strip().rstrip(",")
        if not line or line.startswith("--"):
            continue
        columns.append(line.split()[0])
    return columns


def _load_dashboard_panels(dashboard_path: Path):
    with open(dashboard_path) as f:
        dashboard = json.load(f)

    if "panels" not in dashboard:
        return

    for panel in dashboard["panels"]:
        if panel.get("type") == "row":
            continue
        targets = panel.get("targets", [])
        for target in targets:
            raw_sql = target.get("rawSql") or target.get("query", "")
            if raw_sql:
                yield panel.get("title", f"Panel {panel.get('id', '?')}"), panel.get("id", "?"), raw_sql


# ── Test 1: ClickHouse DDL exists with required content ─────────────────────

def test_clickhouse_ddl_exists() -> None:
    schema_path = PROJECT_ROOT / "sql" / "clickhouse" / "ddl" / "schema.sql"
    assert schema_path.exists(), "ClickHouse DDL file missing"

    assert "CREATE DATABASE IF NOT EXISTS industrial_apm" in SCHEMA_SQL, (
        "Missing CREATE DATABASE statement"
    )

    assert "MergeTree()" in SCHEMA_SQL or "ReplacingMergeTree" in SCHEMA_SQL, (
        "Must use MergeTree() or ReplacingMergeTree engine"
    )

    ddl_tables = _extract_table_names_from_ddl(SCHEMA_SQL)

    grafana_tables: set[str] = set()
    for dashboard_file in sorted(DASHBOARD_DIR.glob("*-ch.json")):
        for _title, _panel_id, raw_sql in _load_dashboard_panels(dashboard_file):
            grafana_tables |= _extract_table_names_from_sql(raw_sql)

    grafana_tables -= set(STALE_TABLE_NAMES)  # intentionally not migrated
    missing_in_ddl = grafana_tables - ddl_tables
    assert not missing_in_ddl, (
        f"Grafana-referenced tables missing from ClickHouse DDL: {sorted(missing_in_ddl)}"
    )


# ── Test 2: Every Grafana table reference exists in ClickHouse DDL ──────────

def test_clickhouse_all_grafana_tables_in_ddl() -> None:
    ddl_tables = _extract_table_names_from_ddl(SCHEMA_SQL)

    violations: list[str] = []
    for dashboard_file in sorted(DASHBOARD_DIR.glob("*-ch.json")):
        for title, panel_id, raw_sql in _load_dashboard_panels(dashboard_file):
            tables = _extract_table_names_from_sql(raw_sql)
            missing = tables - ddl_tables
            if missing:
                violations.append(
                    f"  {dashboard_file.name} Panel '{title}' (id={panel_id}): "
                    f"missing tables {sorted(missing)}"
                )

    assert not violations, (
        f"Found {len(violations)} dashboard panel(s) referencing tables "
        f"not defined in ClickHouse DDL:\n" + "\n".join(violations)
    )


# ── Test 3: ClickHouse DDL contains no stale/old table names ────────────────

def test_clickhouse_ddl_no_stale_table_names() -> None:
    for stale_table in STALE_TABLE_NAMES:
        assert stale_table not in SCHEMA_SQL, (
            f"Stale table name '{stale_table}' found in ClickHouse DDL"
        )


# ── Test 4: No Doris-specific keywords leak into ClickHouse DDL ─────────────

def test_clickhouse_ddl_no_doris_keywords() -> None:
    for keyword in DORIS_KEYWORDS:
        assert keyword not in SCHEMA_SQL, (
            f"Doris-specific keyword '{keyword}' found in ClickHouse DDL"
        )


# ── Test 5: ClickHouse template files exist ──────────────────────────────────

def test_clickhouse_templates_exist() -> None:
    required_templates = [
        TEMPLATES_DIR / "kafka_ingest.sql.tpl",
        TEMPLATES_DIR / "inspection.sql.tpl",
        TEMPLATES_DIR / "query.sql.tpl",
    ]
    for template_path in required_templates:
        assert template_path.exists(), f"Missing template file: {template_path}"

    tenant_dwd = TEMPLATES_DIR / "tenant_dwd_ingest.sql.tpl"
    if tenant_dwd.exists():
        content = tenant_dwd.read_text()
        assert "tenant_northwind" not in content
        assert "tenant_apac_ops" not in content
        assert "tenant_euro_core" not in content


# ── Test 6: Templates contain no hardcoded tenant literals ──────────────────

def test_clickhouse_templates_no_hardcoded_tenant() -> None:
    template_files = list(TEMPLATES_DIR.glob("*.sql.tpl"))
    if not template_files:
        pytest.skip("No ClickHouse template files found")

    for path in template_files:
        sql = path.read_text()
        for tenant in TENANTS:
            assert tenant not in sql, (
                f"Hardcoded tenant '{tenant}' found in template: {path.name}"
            )


def test_clickhouse_smoke_queries_do_not_use_final_on_mergetree() -> None:
    query_files = [TEMPLATES_DIR / "query.sql.tpl"]
    query_files.extend((PROJECT_ROOT / "sql" / "clickhouse" / "generated").glob("*/query.sql"))
    for path in query_files:
        assert " FINAL" not in path.read_text(encoding="utf-8"), (
            f"{path} uses FINAL, which is invalid for plain MergeTree tables"
        )


# ── Test 7: ClickHouse Grafana datasource config exists ─────────────────────

def test_clickhouse_datasource_exists() -> None:
    datasource_path = PROJECT_ROOT / "configs" / "grafana" / "datasources" / "clickhouse.yml"
    assert datasource_path.exists(), "Missing ClickHouse datasource config: configs/grafana/datasources/clickhouse.yml"


# ── Test 8: CDC raw landing tables match PostgreSQL source columns ──────────

def test_clickhouse_cdc_raw_tables_match_postgres_source_columns() -> None:
    for table_name, expected_columns in CDC_SOURCE_COLUMNS.items():
        ddl_columns = set(_extract_create_table_columns(SCHEMA_SQL, table_name))
        assert ddl_columns == expected_columns, (
            f"{table_name} must be raw PostgreSQL CDC landing only. "
            f"Extra={sorted(ddl_columns - expected_columns)} "
            f"Missing={sorted(expected_columns - ddl_columns)}"
        )


# ── Test 9: CDC materialized views do not fabricate raw-layer fields ────────

def test_clickhouse_cdc_ingest_has_no_null_field_fabrication() -> None:
    assert "NULL AS" not in CDC_INGEST_SQL
    assert "toBoolOrNull" not in CDC_INGEST_SQL
    for forbidden in (
        "company_id",
        "plant_id",
        "cdc_op",
        "cdc_updated_at",
        "is_current",
        "manufacturer",
        "serial_number",
        "engineering_limit_min",
        "last_calibrated_at",
        "parts_json",
        "inspection_results_json",
    ):
        assert f"AS {forbidden}" not in CDC_INGEST_SQL


# ── Test 8: No stale table references in dashboard SQL ──────────────────────

@pytest.mark.parametrize(
    "dashboard_name",
    [
        "apm-overview-ch.json",
        "apm-asset-health-ch.json",
    ],
)
def test_dashboard_sql_no_stale_table_names(dashboard_name: str) -> None:
    dashboard_path = DASHBOARD_DIR / dashboard_name
    violations: list[str] = []

    for title, panel_id, raw_sql in _load_dashboard_panels(dashboard_path):
        matches = FORBIDDEN_TABLE_PATTERNS.findall(raw_sql)
        if matches:
            violations.append(
                f"  Panel '{title}' (id={panel_id}): forbidden table patterns {matches}"
            )

    assert not violations, (
        f"Found {len(violations)} forbidden table reference(s) in {dashboard_name}:\n"
        + "\n".join(violations)
    )
