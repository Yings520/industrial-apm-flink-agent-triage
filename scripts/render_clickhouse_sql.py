#!/usr/bin/env python3
"""Render concrete tenant-scoped ClickHouse runtime SQL from templates."""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SQL_DIR = REPO_ROOT / "sql"
TEMPLATE_DIR = SQL_DIR / "clickhouse" / "templates"
OUTPUT_DIR = SQL_DIR / "clickhouse" / "generated"

TENANTS = ("tenant_northwind", "tenant_apac_ops", "tenant_euro_core")
TEMPLATES = {
    "inspection.sql": TEMPLATE_DIR / "inspection.sql.tpl",
    "query.sql": TEMPLATE_DIR / "query.sql.tpl",
}


def render_template(template: str, tenant_id: str) -> str:
    rendered = template.replace("${tenant_id}", tenant_id)
    rendered = rendered.replace("${asset_id}", f"asset_{tenant_id.removeprefix('tenant_')}_mfg_01_0001")
    return rendered


def main() -> None:
    for tenant_id in TENANTS:
        tenant_dir = OUTPUT_DIR / tenant_id
        tenant_dir.mkdir(parents=True, exist_ok=True)
        for output_name, template_path in TEMPLATES.items():
            template = template_path.read_text(encoding="utf-8")
            _ = (tenant_dir / output_name).write_text(render_template(template, tenant_id), encoding="utf-8")


if __name__ == "__main__":
    main()
