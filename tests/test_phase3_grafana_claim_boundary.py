import json
import re
import pytest
from pathlib import Path

DASHBOARD_DIR = Path(__file__).parent.parent / "configs" / "grafana" / "dashboards"

FORBIDDEN_PATTERNS = [
    re.compile(r'confirmed\s+failure', re.IGNORECASE),
    re.compile(r'Confirmed\s+root\s+cause', re.IGNORECASE),
    re.compile(r'Guaranteed\s+failure', re.IGNORECASE),
    re.compile(r'Automatically\s+diagnosed\s+fault', re.IGNORECASE),
    re.compile(r'Autonomous\s+diagnosis', re.IGNORECASE),
    re.compile(r'Autonomous\s+remediation', re.IGNORECASE),
    re.compile(r'Auto\-remediation', re.IGNORECASE),
]

ALLOWED_TERMS = [
    "Probable cause", "Recommended check", "Risk indication",
    "Evidence-based alert triage", "anomaly event", "Risk level",
    "diagnosis", "assisted", "operator support", "root-cause hypothesis",
    "evidence-grounded summary"
]


def scan_text(text: str, pattern: re.Pattern) -> list:
    return pattern.findall(text) if text else []


def collect_dashboard_texts(dashboard_path: Path) -> list:
    texts = []
    with open(dashboard_path) as f:
        dashboard = json.load(f)

    for field in ["title", "description"]:
        if dashboard.get(field):
            texts.append(("dashboard." + field, str(dashboard[field])))

    for panel in dashboard.get("panels", []):
        pid = panel.get("id", "?")
        for field in ["title", "description"]:
            if panel.get(field):
                texts.append((f"panel({pid}).{field}", str(panel[field])))
        for target in panel.get("targets", []):
            raw_sql = target.get("rawSql") or target.get("query", "")
            if raw_sql:
                texts.append((f"panel({pid}).rawSql", raw_sql))

    return texts


@pytest.mark.parametrize("dashboard_name", ["apm-overview-ch.json", "apm-asset-health-ch.json"])
def test_no_forbidden_text(dashboard_name):
    dashboard_path = DASHBOARD_DIR / dashboard_name
    texts = collect_dashboard_texts(dashboard_path)
    violations = []

    for location, text in texts:
        for pattern in FORBIDDEN_PATTERNS:
            matches = scan_text(text, pattern)
            for match in matches:
                violations.append(f"  {location}: found '{match}'")

    assert len(violations) == 0, (
        f"Found {len(violations)} forbidden text matches in {dashboard_name}:\n"
        + "\n".join(violations)
    )


LLM_TEXT_FIELD_TABLES = {
    "fact_apm_agent_recommendations",
    "fact_apm_llm_invocations",
}

_table_ref_pattern = re.compile(
    r'\b(?:FROM|JOIN)\b\s+(?:industrial_apm\.)?(\w+)',
    re.IGNORECASE
)


@pytest.mark.parametrize("dashboard_name", ["apm-overview-ch.json", "apm-asset-health-ch.json"])
def test_no_large_llm_text_in_sql(dashboard_name):
    dashboard_path = DASHBOARD_DIR / dashboard_name
    with open(dashboard_path) as f:
        dashboard = json.load(f)
    violations = []

    for panel in dashboard.get("panels", []):
        if panel.get("type") == "row":
            continue
        pid = panel.get("id", "?")
        title = panel.get("title", f"Panel {pid}")
        for target in panel.get("targets", []):
            raw_sql = target.get("rawSql") or target.get("query", "")
            referenced_tables = _table_ref_pattern.findall(raw_sql)
            llm_tables = set(referenced_tables) & LLM_TEXT_FIELD_TABLES
            if llm_tables:
                violations.append(
                    f"  Panel '{title}' (id={pid}): rawSql references LLM text tables: {llm_tables}"
                )

    assert len(violations) == 0, (
        f"Found {len(violations)} panels with LLM text tables in {dashboard_name}:\n"
        + "\n".join(violations)
    )
