import json
import re
import pytest
from pathlib import Path
import yaml

PROJECT_ROOT = Path(__file__).parent.parent
DASHBOARD_DIR = PROJECT_ROOT / "configs" / "grafana" / "dashboards"
DATASOURCE_PATH = PROJECT_ROOT / "configs" / "grafana" / "datasources" / "clickhouse.yml"

REQUIRED_DASHBOARDS = [
    "apm-overview-ch.json",
    "apm-asset-health-ch.json",
]


def test_grafana_datasource_configured():
    assert DATASOURCE_PATH.exists(), "clickhouse.yml datasource config not found"

    with open(DATASOURCE_PATH) as f:
        config = yaml.safe_load(f)

    ds_list = config.get("datasources", [])
    assert len(ds_list) > 0, "No datasources configured"

    ds = ds_list[0]
    assert ds.get("type") == "vertamedia-clickhouse-datasource", (
        f"Expected ClickHouse plugin datasource, got {ds.get('type')}"
    )
    assert ds.get("database") == "industrial_apm", (
        f"Expected database=industrial_apm, got {ds.get('database')}"
    )
    assert ds.get("uid") == "apm-clickhouse", (
        f"Expected uid=apm-clickhouse, got {ds.get('uid')}"
    )
    assert "clickhouse" in str(ds.get("url", "")), "url should reference clickhouse host"


def test_dashboard_files_exist():
    for name in REQUIRED_DASHBOARDS:
        path = DASHBOARD_DIR / name
        assert path.exists(), f"Dashboard {name} not found"
        assert path.stat().st_size > 100, f"Dashboard {name} is too small"


def test_dashboard_variables_include_tenant_id():
    for name in REQUIRED_DASHBOARDS:
        path = DASHBOARD_DIR / name
        with open(path) as f:
            dashboard = json.load(f)

        templating = dashboard.get("templating", {}).get("list", [])
        var_names = [v.get("name") for v in templating]
        assert "tenant_id" in var_names, (
            f"Dashboard {name} missing 'tenant_id' variable"
        )


def test_no_kafka_direct_access():
    for name in REQUIRED_DASHBOARDS:
        path = DASHBOARD_DIR / name
        with open(path) as f:
            dashboard = json.load(f)

        violations = []
        for panel in dashboard.get("panels", []):
            if panel.get("type") == "row":
                continue
            for target in panel.get("targets", []):
                raw_sql = target.get("rawSql", "")
                if re.search(r'\bKAFKA\b|\bkafka_topic\b', raw_sql, re.IGNORECASE):
                    violations.append(
                        f"Panel '{panel.get('title', '?')}' (id={panel.get('id','?')})"
                    )

        assert len(violations) == 0, (
            f"Dashboard {name} has direct KAFKA access:\n" + "\n".join(violations)
        )


def test_drilldown_links_exist():
    path = DASHBOARD_DIR / "apm-overview-ch.json"
    if not path.exists():
        pytest.skip("apm-overview-ch.json not found")

    with open(path) as f:
        dashboard = json.load(f)

    link_count = 0
    for panel in dashboard.get("panels", []):
        links = panel.get("links", [])
        link_count += len(links)

    assert link_count >= 1, (
        f"Expected at least 1 drilldown link in apm-overview-ch.json, found {link_count}"
    )


def test_dashboard_json_valid():
    for name in REQUIRED_DASHBOARDS:
        path = DASHBOARD_DIR / name
        with open(path) as f:
            dashboard = json.load(f)

        assert "panels" in dashboard, f"{name}: missing 'panels'"
        assert "title" in dashboard, f"{name}: missing 'title'"
        assert "uid" in dashboard, f"{name}: missing 'uid'"

        data_panels = [p for p in dashboard["panels"] if p.get("type") != "row"]
        assert len(data_panels) > 0, f"{name}: no data panels found"


def test_apm_overview_dashboard_is_scoped_to_overview_panels_and_tenant_filter():
    expectations = {
        "apm-overview-ch.json": "${tenant_id:string}",
    }

    required_titles = {
        "Total Assets",
        "Healthy Assets",
        "Warning Assets",
        "Critical Assets",
        "Unknown Assets",
        "Asset Condition Distribution",
        "Priority Asset Worklist",
        "Asset Condition Trend",
    }
    forbidden_titles = {
        "Signal Trend by Tag",
        "Anomaly Events Table",
        "Total Diagnosis Count",
        "Failure Evidence Table",
    }

    for name, tenant_token in expectations.items():
        path = DASHBOARD_DIR / name
        with open(path) as f:
            dashboard = json.load(f)

        assert dashboard["title"] == "APM Overview", f"{name}: title must be APM Overview"
        assert dashboard["time"]["from"] == "now-48h", (
            f"{name}: overview dashboard should default to the last 48 hours"
        )

        templating = dashboard.get("templating", {}).get("list", [])
        assert [v.get("name") for v in templating] == ["tenant_id"], (
            f"{name}: expected only tenant_id variable in overview dashboard"
        )

        panel_titles = {
            panel.get("title")
            for panel in dashboard.get("panels", [])
            if panel.get("type") != "row"
        }
        assert required_titles.issubset(panel_titles), f"{name}: missing required overview panels"
        assert forbidden_titles.isdisjoint(panel_titles), (
            f"{name}: overview dashboard should not include non-overview sections"
        )

        for panel in dashboard.get("panels", []):
            if panel.get("type") == "row":
                continue
            for target in panel.get("targets", []):
                raw_sql = target.get("rawSql") or target.get("query") or ""
                assert tenant_token in raw_sql, (
                    f"{name}: panel '{panel.get('title', '?')}' is missing tenant filter"
                )
                if panel.get("title") in {
                    "Open Incidents",
                    "Avg Usefulness",
                    "Active Incident Worklist",
                }:
                    continue
                assert "__from" in raw_sql or "__timeFrom" in raw_sql, (
                    f"{name}: panel '{panel.get('title', '?')}' is missing time-range filtering"
                )


def test_apm_overview_layout_uses_full_24_column_grid():
    for name in ("apm-overview-ch.json",):
        with open(DASHBOARD_DIR / name) as f:
            dashboard = json.load(f)

        panels = {
            panel.get("title"): panel
            for panel in dashboard.get("panels", [])
            if panel.get("type") != "row"
        }

        top_row = [
            "Total Assets",
            "Healthy Assets",
            "Warning Assets",
            "Critical Assets",
            "Unknown Assets",
        ]
        expected_top = {
            "Total Assets": {"x": 0, "y": 1, "w": 5, "h": 4},
            "Healthy Assets": {"x": 5, "y": 1, "w": 5, "h": 4},
            "Warning Assets": {"x": 10, "y": 1, "w": 5, "h": 4},
            "Critical Assets": {"x": 15, "y": 1, "w": 5, "h": 4},
            "Unknown Assets": {"x": 20, "y": 1, "w": 4, "h": 4},
        }

        for title in top_row:
            assert panels[title]["gridPos"] == expected_top[title], (
                f"{name}: {title} gridPos is misaligned"
            )

        assert panels["Priority Asset Worklist"]["gridPos"] == {"x": 0, "y": 18, "w": 16, "h": 8}
        assert panels["Asset Condition Distribution"]["gridPos"] == {"x": 16, "y": 18, "w": 8, "h": 8}
        assert panels["Asset Condition Trend"]["gridPos"] == {"x": 0, "y": 26, "w": 24, "h": 8}
