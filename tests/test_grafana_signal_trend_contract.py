import json
from pathlib import Path


PROJECT_ROOT = Path(__file__).parent.parent
DASHBOARD_PATH = PROJECT_ROOT / "configs" / "grafana" / "dashboards" / "apm-asset-health-ch.json"
VIEW_PATH = PROJECT_ROOT / "sql" / "clickhouse" / "views" / "grafana_signal_trend.sql"


def _load_dashboard() -> dict:
    return json.loads(DASHBOARD_PATH.read_text())


def _panel_by_title(dashboard: dict, title: str) -> dict:
    for panel in dashboard["panels"]:
        if panel.get("title") == title:
            return panel
    raise AssertionError(f"Missing panel: {title}")


def _variable_by_name(dashboard: dict, name: str) -> dict:
    for variable in dashboard["templating"]["list"]:
        if variable.get("name") == name:
            return variable
    raise AssertionError(f"Missing variable: {name}")


def test_signal_trend_clickhouse_view_contract() -> None:
    assert VIEW_PATH.exists(), "Missing Grafana signal trend ClickHouse view"
    view_sql = VIEW_PATH.read_text()

    assert "CREATE OR REPLACE VIEW industrial_apm.vw_grafana_signal_trend_by_tag" in view_sql
    assert "industrial_apm.dim_apm_assets" in view_sql
    assert "industrial_apm.ads_apm_tag_signal_timeseries_1m" in view_sql
    for column in (
        "tenant_id",
        "asset_id",
        "asset_name",
        "tag_id",
        "signal_time",
        "signal_value",
        "series_name",
    ):
        assert column in view_sql


def test_asset_signal_variables_are_cascaded() -> None:
    dashboard = _load_dashboard()

    variables = dashboard["templating"]["list"]
    assert [variable["name"] for variable in variables] == ["tenant_id", "asset_id", "tag_id"]

    tenant = _variable_by_name(dashboard, "tenant_id")
    asset = _variable_by_name(dashboard, "asset_id")
    tag = _variable_by_name(dashboard, "tag_id")

    assert tenant["includeAll"] is False
    assert tenant["multi"] is False

    assert asset["includeAll"] is False
    assert asset["multi"] is False
    assert "tenant_id = '${tenant_id}'" in asset["query"]

    assert tag["includeAll"] is True
    assert tag["multi"] is False
    assert "tenant_id = '${tenant_id}'" in tag["query"]
    assert "asset_id = '${asset_id}'" in tag["query"]
    assert "industrial_apm.dim_apm_signal_tags_mapping" in tag["query"]
    assert "industrial_apm.ads_apm_tag_signal_timeseries_1m" in tag["query"]
    assert "UNION DISTINCT" in tag["query"]


def test_signal_trend_panel_uses_explicit_tenant_and_asset_filters() -> None:
    dashboard = _load_dashboard()
    panel = _panel_by_title(dashboard, "Signal Trend by Tag")
    target = panel["targets"][0]
    query = target["query"]

    assert panel["type"] == "timeseries"
    assert target["refId"] == "A"
    assert target["format"] == "time_series"
    assert "industrial_apm.ads_apm_tag_signal_timeseries_1m" in query
    assert "tenant_id = '${tenant_id}'" in query
    assert "asset_id = '${asset_id}'" in query
    assert "component_id" not in query
    assert "('${tag_id}' = 'All' OR tag_id = '${tag_id}')" in query
    assert "bucket_minute >= toDateTime(intDiv(${__from}, 1000))" in query
    assert "bucket_minute <= toDateTime(intDiv(${__to}, 1000))" in query
    assert "bucket_minute AS time" in query
    assert "concat(tag_id, '::', metric_name) AS metric" in query
    assert "avg_value AS value" in query


def test_asset_details_table_normalizes_empty_component_criticality() -> None:
    dashboard = _load_dashboard()
    panel = _panel_by_title(dashboard, "Asset Details Table")
    query = panel["targets"][0]["query"]

    assert panel["type"] == "table"
    assert "industrial_apm.dim_apm_asset_components" in query
    assert "tenant_id = '${tenant_id}'" in query
    assert "asset_id = '${asset_id}'" in query
    assert "ifNull(nullIf(criticality, ''), 'unknown') AS criticality" in query
    assert panel["fieldConfig"]["overrides"] == []


def test_asset_profile_panels_are_tables_for_string_fields() -> None:
    dashboard = _load_dashboard()

    asset_info = _panel_by_title(dashboard, "Asset Info")
    assert asset_info["type"] == "table"
    assert asset_info["targets"][0]["format"] == "table"
    assert "industrial_apm.dim_apm_assets" in asset_info["targets"][0]["query"]

    current_condition = _panel_by_title(dashboard, "Current Condition")
    assert current_condition["type"] == "table"
    assert current_condition["targets"][0]["format"] == "table"
    assert "condition_status AS status" in current_condition["targets"][0]["query"]
    assert "formatDateTime(last_anomaly_time" in current_condition["targets"][0]["query"]


def test_asset_drilldown_layout_is_compact() -> None:
    dashboard = _load_dashboard()

    expected_positions = {
        "Asset Info": {"x": 0, "y": 1, "w": 5, "h": 4},
        "Current Condition": {"x": 5, "y": 1, "w": 4, "h": 4},
        "Anomaly Count (24h)": {"x": 9, "y": 1, "w": 3, "h": 4},
        "Diagnosis Count (24h)": {"x": 12, "y": 1, "w": 3, "h": 4},
        "Open Workorders": {"x": 15, "y": 1, "w": 3, "h": 4},
        "Asset Details Table": {"x": 18, "y": 1, "w": 6, "h": 4},
        "Signal Trend": {"x": 0, "y": 5, "w": 24, "h": 1},
        "Recent Anomalies": {"x": 0, "y": 14, "w": 24, "h": 1},
        "Diagnosis & Evidence": {"x": 0, "y": 22, "w": 24, "h": 1},
        "Diagnosis Count": {"x": 0, "y": 23, "w": 6, "h": 4},
        "High Risk Count": {"x": 6, "y": 23, "w": 6, "h": 4},
        "Avg Confidence": {"x": 12, "y": 23, "w": 6, "h": 4},
        "Escalation Required": {"x": 18, "y": 23, "w": 6, "h": 4},
        "Failure Evidence Table": {"x": 0, "y": 27, "w": 24, "h": 8},
    }

    for title, grid_pos in expected_positions.items():
        assert _panel_by_title(dashboard, title)["gridPos"] == grid_pos


def test_asset_drilldown_has_no_component_filter_contract() -> None:
    dashboard = _load_dashboard()

    dashboard_text = json.dumps(dashboard)
    assert '"name": "component_id"' not in dashboard_text
    assert "'${component_id}'" not in dashboard_text
    assert "${component_id}" not in dashboard_text


def test_asset_level_panels_do_not_depend_on_tag_filter() -> None:
    dashboard = _load_dashboard()

    tag_scoped_titles = {"Signal Trend by Tag", "Signal Stats Table"}
    for panel in dashboard["panels"]:
        title = panel.get("title")
        if panel.get("type") == "row" or title in tag_scoped_titles:
            continue
        for target in panel.get("targets", []):
            query = target.get("query", "")
            assert "${tag_id}" not in query, f"{title} unexpectedly depends on tag_id"


def test_asset_drilldown_defaults_to_non_empty_tenant_and_asset() -> None:
    dashboard = _load_dashboard()

    tenant = _variable_by_name(dashboard, "tenant_id")
    asset = _variable_by_name(dashboard, "asset_id")

    assert tenant["current"]["text"]
    assert tenant["current"]["value"]
    assert asset["current"]["text"]
    assert asset["current"]["value"]


def test_asset_dashboard_keeps_recent_anomaly_views_tenant_scoped() -> None:
    dashboard = _load_dashboard()

    trend_panel = _panel_by_title(dashboard, "Anomaly Trend")
    trend_query = trend_panel["targets"][0]["query"]
    assert trend_panel["type"] == "table"
    assert trend_panel["targets"][0]["format"] == "table"
    assert "industrial_apm.ads_apm_anomaly_event_realtime" in trend_query
    assert "tenant_id = '${tenant_id}'" in trend_query
    assert "asset_id = '${asset_id}'" in trend_query
    assert "AS anomaly_window_start" in trend_query
    assert "AS anomaly_count" in trend_query

    table_panel = _panel_by_title(dashboard, "Anomaly Events Table")
    table_query = table_panel["targets"][0]["query"]
    assert table_panel["targets"][0]["format"] == "table"
    assert "industrial_apm.ads_apm_anomaly_event_realtime" in table_query
    assert "tenant_id = '${tenant_id}'" in table_query
    assert "asset_id = '${asset_id}'" in table_query
    assert "has_related_workorder" in table_query
    assert "has_related_maintenance" in table_query
    assert "formatDateTime(window_start, '%F %T') AS anomaly_window_start" in table_query
    assert "metric_name AS metric" in table_query
    assert "failure_mode_label AS evidence" in table_query


def test_asset_dashboard_triage_and_failure_panels_stay_on_summary_tables() -> None:
    dashboard = _load_dashboard()

    for title in (
        "Diagnosis Count (24h)",
        "High Risk Count",
        "Avg Confidence",
        "Escalation Required",
    ):
        panel = _panel_by_title(dashboard, title)
        query = panel["targets"][0]["query"]
        assert "industrial_apm.ads_apm_diagnosis_kpi_snapshot" in query
        assert "tenant_id = '${tenant_id}'" in query
        assert "asset_id = '${asset_id}'" in query

    evidence_panel = _panel_by_title(dashboard, "Failure Evidence Table")
    evidence_query = evidence_panel["targets"][0]["query"]
    assert "industrial_apm.ads_apm_failure_evidence_summary" in evidence_query
    assert "tenant_id = '${tenant_id}'" in evidence_query
    assert "asset_id = '${asset_id}'" in evidence_query
