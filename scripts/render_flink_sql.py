#!/usr/bin/env python3
"""Render tenant-scoped Flink SQL jobs from single-responsibility templates."""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
TEMPLATE_DIR = REPO_ROOT / "sql" / "flink" / "templates"
OUTPUT_DIR = REPO_ROOT / "sql" / "flink" / "generated"

TENANTS = ("tenant_northwind", "tenant_apac_ops", "tenant_euro_core")
JOBS = ("enrich", "anomaly", "late_events", "dlq_events", "stream_health")


def render_template(template: str, tenant_id: str) -> str:
    return template.replace("{{tenant_id}}", tenant_id)


def main() -> None:
    for tenant_id in TENANTS:
        tenant_dir = OUTPUT_DIR / tenant_id
        tenant_dir.mkdir(parents=True, exist_ok=True)

        for job in JOBS:
            template = (TEMPLATE_DIR / f"{job}.sql.tpl").read_text(encoding="utf-8")
            rendered = render_template(template, tenant_id)
            _ = (tenant_dir / f"{job}.sql").write_text(rendered, encoding="utf-8")


if __name__ == "__main__":
    main()
