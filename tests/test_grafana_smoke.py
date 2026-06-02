import json
from http.client import HTTPConnection, OK
from pathlib import Path
from typing import Any


GRAFANA_HOST = "localhost"
GRAFANA_PORT = 13000
GRAFANA_USER = "admin"
GRAFANA_PASSWORD = "apm_demo"

EXPECTED_DASHBOARDS = {
    "apm-overview": "APM Overview",
    "apm-asset-drilldown": "APM Asset Drilldown",
}


def _dashboard(path: str) -> dict[str, object]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise AssertionError("dashboard JSON must be an object")
    return payload


def _panel_titles(dashboard: dict[str, object]) -> set[str]:
    panels = dashboard.get("panels", [])
    if not isinstance(panels, list):
        raise AssertionError("dashboard panels must be a list")
    return {str(panel.get("title")) for panel in panels if isinstance(panel, dict)}


def test_apm_landing_dashboard_has_product_panels() -> None:
    dashboard = _dashboard("configs/grafana/dashboards/apm-overview-ch.json")
    titles = _panel_titles(dashboard)

    assert dashboard["title"] == "APM Overview"
    assert {
        "Total Assets",
        "Healthy Assets",
        "Critical Assets",
        "Priority Asset Worklist",
        "Asset Condition Trend",
    }.issubset(titles)


def test_asset_workbench_dashboard_has_operator_panels() -> None:
    dashboard = _dashboard("configs/grafana/dashboards/apm-asset-health-ch.json")
    titles = _panel_titles(dashboard)

    assert dashboard["title"] == "APM Asset Drilldown"
    assert {
        "Asset Info",
        "Current Condition",
        "Signal Trend by Tag",
        "Anomaly Events Table",
        "Failure Evidence Table",
    }.issubset(titles)


def _auth_bytes() -> str:
    import base64
    creds = f"{GRAFANA_USER}:{GRAFANA_PASSWORD}"
    return base64.b64encode(creds.encode()).decode()


def _grafana_get(path: str) -> tuple[int, dict[str, Any] | list[Any]]:
    conn = HTTPConnection(GRAFANA_HOST, GRAFANA_PORT, timeout=10)
    try:
        conn.request(
            "GET",
            path,
            headers={
                "Authorization": f"Basic {_auth_bytes()}",
                "Accept": "application/json",
            },
        )
        resp = conn.getresponse()
        body = resp.read()
        return resp.status, json.loads(body)
    finally:
        conn.close()


def _grafana_post(path: str, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
    conn = HTTPConnection(GRAFANA_HOST, GRAFANA_PORT, timeout=10)
    try:
        payload = json.dumps(body)
        conn.request(
            "POST",
            path,
            body=payload,
            headers={
                "Authorization": f"Basic {_auth_bytes()}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )
        resp = conn.getresponse()
        return resp.status, json.loads(resp.read())
    finally:
        conn.close()


def test_grafana_is_healthy() -> None:
    status, data = _grafana_get("/api/health")
    assert status == OK, f"Grafana health check failed: {status}"
    assert isinstance(data, dict), f"health response is not a dict: {type(data)}"
    assert data.get("database") == "ok"
    assert "version" in data


def test_grafana_clickhouse_datasource_exists() -> None:
    status, datasources = _grafana_get("/api/datasources")
    assert status == OK, f"Datasource API failed: {status}"
    assert isinstance(datasources, list)
    clickhouse = [d for d in datasources if d.get("uid") == "apm-clickhouse"]
    assert len(clickhouse) == 1, f"ClickHouse datasource not found in {[d.get('uid') for d in datasources]}"
    assert clickhouse[0]["type"] == "vertamedia-clickhouse-datasource"
    assert clickhouse[0]["database"] == "industrial_apm"


def test_grafana_datasource_can_query_clickhouse_variables() -> None:
    status, result = _grafana_post(
        "/api/ds/query",
        {
            "queries": [
                    {
                        "datasource": {"type": "vertamedia-clickhouse-datasource", "uid": "apm-clickhouse"},
                        "query": "SELECT DISTINCT tenant_id FROM industrial_apm.dim_apm_tenants ORDER BY tenant_id",
                        "queryType": "sql",
                        "editorMode": "sql",
                        "format": "table",
                        "refId": "A",
                    }
            ]
        },
    )
    assert status == OK, f"Query API failed: {status}"
    frames = result.get("results", {}).get("A", {}).get("frames", [])
    assert len(frames) > 0, "No result frames returned"
    values = frames[0].get("data", {}).get("values", [])
    assert values == [["tenant_apac_ops", "tenant_euro_core", "tenant_northwind"]], (
        f"Expected tenant variable options from ClickHouse, got {values}"
    )


def test_grafana_has_all_five_dashboards() -> None:
    status, dashboards = _grafana_get("/api/search?type=dash-db&limit=20")
    assert status == OK, f"Dashboard search API failed: {status}"
    assert isinstance(dashboards, list)

    loaded = {d["uid"]: d["title"] for d in dashboards if d.get("uid") in EXPECTED_DASHBOARDS}
    assert len(loaded) == len(EXPECTED_DASHBOARDS), (
        f"Expected {len(EXPECTED_DASHBOARDS)} dashboards, found {len(loaded)}: {loaded}"
    )

    for uid, expected_title in EXPECTED_DASHBOARDS.items():
        assert uid in loaded, f"Dashboard '{uid}' not found"
        assert loaded[uid] == expected_title, (
            f"Dashboard '{uid}' title mismatch: expected '{expected_title}', got '{loaded[uid]}'"
        )


def test_grafana_apm_folder_exists() -> None:
    status, dashboards = _grafana_get("/api/search?type=dash-db&limit=20")
    assert status == OK
    for d in dashboards:
        if d.get("folderTitle") == "APM":
            return
    assert False, "No dashboards found in 'APM' folder"


def test_grafana_anonymous_access_enabled() -> None:
    conn = HTTPConnection(GRAFANA_HOST, GRAFANA_PORT, timeout=10)
    try:
        conn.request("GET", "/api/health")
        resp = conn.getresponse()
        body = resp.read()
        data = json.loads(body)
        assert resp.status == OK, f"Anonymous health check failed with status {resp.status}"
        assert isinstance(data, dict), f"Expected dict response, got {type(data)}"
        assert data.get("database") == "ok"
    finally:
        conn.close()
