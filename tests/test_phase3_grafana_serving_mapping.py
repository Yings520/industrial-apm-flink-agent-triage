import json
import re
import pytest
from pathlib import Path

DASHBOARD_DIR = Path(__file__).parent.parent / "configs" / "grafana" / "dashboards"

VALID_TABLES = {
    "ads_apm_asset_condition_snapshot",
    "ads_apm_asset_condition_timeseries_1m",
    "ads_apm_tag_signal_timeseries_1m",
    "ads_apm_anomaly_event_realtime",
    "ads_apm_diagnosis_kpi_snapshot",
    "ads_apm_stream_health_snapshot",
    "ads_apm_failure_evidence_summary",
    "dwd_apm_realtime_signal_readings",
    "dwd_apm_anomaly_events",
    "dwd_apm_late_sensor_readings",
    "dwd_apm_stream_health_snapshots",
    "dwd_apm_realtime_diagnosis_events",
    "dwd_apm_dlq_events",
    "dim_apm_assets",
    "dim_apm_asset_components",
    "dim_apm_signal_tags_mapping",
    "dim_apm_failuremode",
    "dim_apm_failureevent",
    "dim_apm_workorder",
    "dim_apm_maintenance",
    "dim_apm_metrics",
    "dim_apm_sites",
    "dim_apm_threshold_profiles",
    "dim_apm_measurement_points",
    "dim_apm_tenants",
    "fact_apm_agent_recommendations",
    "fact_apm_incidents",
    "fact_apm_alert_routing",
    "fact_apm_operator_feedback",
    "fact_apm_llm_invocations",
}

FORBIDDEN_TABLE_PATTERNS = re.compile(
    r'\b(fact_apm_(quality_events|rul_predictions)|'
    r'ag_apm_|vw_apm_|serving_apm_|'
    r'dim_asset\b|dim_component\b|dim_sensor_tag\b|dim_metric\b|'
    r'dim_threshold_profile\b|dim_site\b|dim_failure_mode\b|dim_work_order\b|'
    r'dim_measurement_point\b)',
    re.IGNORECASE
)


def extract_table_names_from_sql(raw_sql: str) -> set:
    """Extract table names from FROM and JOIN clauses."""
    table_pattern = re.compile(
        r'\b(?:FROM|JOIN)\b\s+(?:industrial_apm\.)?(\w+)',
        re.IGNORECASE
    )
    cte_pattern = re.compile(r'\bWITH\s+(\w+)\s+AS|\s*,\s*(\w+)\s+AS\s*\(', re.IGNORECASE)
    cte_names = {
        name
        for match in cte_pattern.findall(raw_sql)
        for name in match
        if name
    }
    return set(table_pattern.findall(raw_sql)) - cte_names


def load_dashboard_panels(dashboard_path: Path):
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


def test_all_dashboard_files_exist():
    required = ["apm-overview-ch.json", "apm-asset-health-ch.json"]
    for name in required:
        path = DASHBOARD_DIR / name
        assert path.exists(), f"Missing dashboard: {name}"


@pytest.mark.parametrize("dashboard_name", ["apm-overview-ch.json", "apm-asset-health-ch.json"])
def test_panel_tables_are_valid(dashboard_name):
    dashboard_path = DASHBOARD_DIR / dashboard_name
    violations = []

    for title, panel_id, raw_sql in load_dashboard_panels(dashboard_path):
        tables = extract_table_names_from_sql(raw_sql)
        invalid = tables - VALID_TABLES
        if invalid:
            violations.append(f"  Panel '{title}' (id={panel_id}) uses invalid tables: {invalid}")

    assert len(violations) == 0, (
        f"Found {len(violations)} invalid table references in {dashboard_name}:\n"
        + "\n".join(violations)
    )


@pytest.mark.parametrize("dashboard_name", ["apm-overview-ch.json", "apm-asset-health-ch.json"])
def test_no_old_table_names_in_panels(dashboard_name):
    dashboard_path = DASHBOARD_DIR / dashboard_name
    violations = []

    for title, panel_id, raw_sql in load_dashboard_panels(dashboard_path):
        matches = FORBIDDEN_TABLE_PATTERNS.findall(raw_sql)
        if matches:
            violations.append(f"  Panel '{title}' (id={panel_id}): old table names {matches}")

    assert len(violations) == 0, (
        f"Found {len(violations)} old table names in {dashboard_name}:\n"
        + "\n".join(violations)
    )
