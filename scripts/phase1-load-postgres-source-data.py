#!/usr/bin/env python3
"""Phase 1: PostgreSQL mock data loading script.

Loads all 12 opendataphy tables with mock data derived from YAML configs.
Self-contained -- no dependencies beyond pyyaml.

Usage: python scripts/phase1-load-postgres-source-data.py
"""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIG_DIR = REPO_ROOT / "configs"
REPORT_PATH = REPO_ROOT / "reports" / "phase1-postgres-load-summary.json"

sys.path.insert(0, str(REPO_ROOT / "src"))
sys.path.insert(0, str(REPO_ROOT))

NOW = datetime(2026, 5, 28, 10, 0, 0, tzinfo=UTC)
MAY_START = datetime(2026, 5, 1, 0, 0, 0, tzinfo=UTC)
TENANT_IDS = ["tenant_northwind", "tenant_apac_ops", "tenant_euro_core"]


# ---------------------------------------------------------------------------
# YAML helpers
# ---------------------------------------------------------------------------


def _load_yaml(name: str) -> dict[str, Any]:
    path = CONFIG_DIR / name
    if not path.exists():
        print(f"WARNING: config file not found: {path}")
        return {}
    with path.open("r", encoding="utf-8") as fh:
        result = yaml.safe_load(fh)
    if not isinstance(result, dict):
        return {}
    return result


# ---------------------------------------------------------------------------
# SQL execution helpers
# ---------------------------------------------------------------------------


def _run_psql(sql: str, timeout: int = 30) -> str:
    """Run a single psql -c command via docker compose, return stdout."""
    try:
        result = subprocess.run(
            [
                "docker",
                "compose",
                "exec",
                "-T",
                "postgres",
                "psql",
                "-U",
                "apm",
                "-d",
                "apm_metadata",
                "-c",
                sql,
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return result.stdout
    except subprocess.CalledProcessError as exc:
        print(f"  SQL error: {exc.stderr[:500]}")
        raise


def _run_psql_pipe(sql_content: str, timeout: int = 120) -> None:
    """Pipe SQL statements through psql stdin."""
    try:
        result = subprocess.run(
            [
                "docker",
                "compose",
                "exec",
                "-T",
                "postgres",
                "psql",
                "-U",
                "apm",
                "-d",
                "apm_metadata",
            ],
            input=sql_content,
            check=True,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.CalledProcessError as exc:
        print(f"  SQL pipe error: {exc.stderr[:2000]}")
        raise


def _query_count(table: str) -> int:
    """Return row count for a table."""
    out = _run_psql(f"SELECT COUNT(*) FROM {table};")
    for line in out.strip().splitlines():
        stripped = line.strip()
        if stripped.isdigit():
            return int(stripped)
    return 0


def _run_psql_raw(sql: str, timeout: int = 60) -> str:
    """Run psql with unaligned, tuples-only output for machine parsing."""
    try:
        result = subprocess.run(
            [
                "docker",
                "compose",
                "exec",
                "-T",
                "postgres",
                "psql",
                "-U",
                "apm",
                "-d",
                "apm_metadata",
                "-A",
                "-t",
                "-c",
                sql,
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return result.stdout
    except subprocess.CalledProcessError as exc:
        print(f"  SQL raw error: {exc.stderr[:500]}")
        raise


# ---------------------------------------------------------------------------
# SQL value escaping
# ---------------------------------------------------------------------------


def _esc(value: Any) -> str:
    """Escape a Python value for SQL."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, (dict, list)):
        return _esc(json.dumps(value))
    s = str(value).replace("'", "''")
    return f"'{s}'"


# ---------------------------------------------------------------------------
# ID generation (matching src/telemetry_generator/config.py)
# ---------------------------------------------------------------------------


def _plant_slug(plant_id: str) -> str:
    return plant_id.removeprefix("plant_")


def _build_asset_id(plant_id: str, asset_index: int) -> str:
    return f"asset_{_plant_slug(plant_id)}_{asset_index:04d}"


def _build_asset_name(asset_type: str, asset_index: int) -> str:
    return f"{asset_type.replace('_', ' ').title()} {asset_index:04d}"


def _build_component_id(tenant_id: str, asset_id: str, component_type: str, ordinal: int = 1) -> str:
    digest = hashlib.sha1(f"{tenant_id}|{asset_id}|{component_type}|{ordinal}".encode()).hexdigest()[:16]
    return f"comp_{digest}"


def _build_measurement_point_id(asset_type: str, component_type: str, measurement_point: str) -> str:
    digest = hashlib.sha1(f"{asset_type}|{component_type}|{measurement_point}".encode()).hexdigest()[:10]
    return f"mp_{asset_type}_{digest}"


def _build_mapping_id(tenant_id: str, tag_id: str) -> str:
    digest = hashlib.sha1(f"{tenant_id}|{tag_id}|opc_ua".encode()).hexdigest()[:20]
    return f"map_{digest}"


# ---------------------------------------------------------------------------
# DB helpers for FK-safe component references
# ---------------------------------------------------------------------------


def _get_component_ids(tenant_id: str) -> list[tuple[str, str, str]]:
    """Return list of (asset_id, component_id, component_type) from asset_components."""
    sql = f"SELECT asset_id, component_id, component_type FROM asset_components WHERE tenant_id='{tenant_id}';"
    out = _run_psql_raw(sql)
    results: list[tuple[str, str, str]] = []
    for line in out.strip().splitlines():
        if not line.strip():
            continue
        parts = line.split("|")
        if len(parts) >= 3:
            results.append((parts[0].strip(), parts[1].strip(), parts[2].strip()))
    return results


# ---------------------------------------------------------------------------
# Data generators
# ---------------------------------------------------------------------------


def _make_tenants_sql(tenants_cfg: dict[str, Any]) -> str:
    """INSERT tenants if not already seeded (postgres_init.sql already seeds them)."""
    stmts: list[str] = []
    tenants = tenants_cfg.get("tenants", [])
    for t in tenants:
        tid = t.get("tenant_id", "")
        tname = t.get("tenant_name", "")
        region = t.get("region", "")
        stmts.append(
            f"INSERT INTO tenants (tenant_id, tenant_name, region) VALUES "
            f"({_esc(tid)}, {_esc(tname)}, {_esc(region)}) "
            f"ON CONFLICT (tenant_id) DO NOTHING;"
        )
    return "\n".join(stmts)


def _make_assets_sql(assets_cfg: dict[str, Any]) -> str:
    """Generate asset rows for every tenant/plant/asset_index combination."""
    stmts: list[str] = []
    archetypes = assets_cfg.get("archetypes", {})
    tenants = assets_cfg.get("tenants", [])
    assets_per_plant = int(assets_cfg.get("assets_per_plant", 10))

    for tenant in tenants:
        tenant_id = str(tenant["tenant_id"])
        for plant in tenant.get("plants", []):
            plant_id = str(plant["plant_id"])
            site_id = str(plant["site_id"])
            archetype_name = str(plant["archetype"])
            arch = archetypes.get(archetype_name, {})
            asset_types = arch.get("asset_types", ["unknown"])

            for idx in range(1, assets_per_plant + 1):
                asset_type = asset_types[(idx - 1) % len(asset_types)]
                asset_id = _build_asset_id(plant_id, idx)
                asset_name = _build_asset_name(asset_type, idx)
                stmts.append(
                    f"INSERT INTO assets (tenant_id, asset_id, asset_name, asset_type, site_id, criticality) VALUES "
                    f"({_esc(tenant_id)}, {_esc(asset_id)}, {_esc(asset_name)}, {_esc(asset_type)}, {_esc(site_id)}, 'medium');"
                )
    return "\n".join(stmts)


def _make_components_from_tag_metadata_sql() -> str:
    """Generate asset_components from build_tag_metadata() component IDs.

    Uses build_tag_metadata() as the single source of truth so component_id
    values are consistent with the tag mappings inserted later, avoiding FK
    violations for asset types that fall back to generic_component.
    """
    from telemetry_generator.config import build_tag_metadata

    tag_meta = build_tag_metadata()
    seen: set[tuple[str, str]] = set()  # (tenant_id, component_id)
    stmts: list[str] = []

    for tag_id, metadata in tag_meta.items():
        key = (metadata["tenant_id"], metadata["component_id"])
        if key in seen:
            continue
        seen.add(key)

        stmts.append(
            f"INSERT INTO asset_components "
            f"(tenant_id, component_id, asset_id, component_name, component_type) VALUES "
            f"({_esc(metadata['tenant_id'])}, {_esc(metadata['component_id'])}, "
            f"{_esc(metadata['asset_id'])}, 'Component {metadata['component_type']}', "
            f"{_esc(metadata['component_type'])});"
        )
    return "\n".join(stmts)


def _make_tag_mappings_sql(
    assets_cfg: dict[str, Any],
    measurement_pts_cfg: dict[str, Any],
) -> str:
    """Generate asset_signal_tags_mapping rows using build_tag_metadata()."""
    from telemetry_generator.config import build_tag_metadata

    stmts: list[str] = []
    tag_metadata_by_id = build_tag_metadata()
    for tag_id, metadata in tag_metadata_by_id.items():
        mapping_id = _build_mapping_id(metadata["tenant_id"], tag_id)
        sampling_rate_hz = float(metadata["sampling_rate_hz"])
        sampling_rate_seconds = 1.0 / sampling_rate_hz if sampling_rate_hz > 0 else 1.0

        stmts.append(
            f"INSERT INTO asset_signal_tags_mapping "
            f"(tenant_id, mapping_id, asset_id, component_id, tag_id, tag_name, "
            f"source_system, measurement_point, metric_name, signal_type, unit, expected_unit, "
            f"sampling_rate_seconds, normal_range_min, normal_range_max, calibration_status) VALUES "
            f"({_esc(metadata['tenant_id'])}, {_esc(mapping_id)}, {_esc(metadata['asset_id'])}, "
            f"{_esc(metadata['component_id'])}, {_esc(tag_id)}, {_esc(metadata['tag_name'])}, "
            f"'opc_ua', {_esc(metadata['measurement_point'])}, "
            f"{_esc(metadata['metric_name'])}, 'analog', {_esc(metadata['unit'])}, "
            f"{_esc(metadata['unit'])}, {_esc(sampling_rate_seconds)}, "
            f"{_esc(metadata['normal_range_min'])}, {_esc(metadata['normal_range_max'])}, 'calibrated');"
        )
    return "\n".join(stmts)


def _load_failure_taxonomy(failure_modes_cfg: dict[str, Any]) -> None:
    """Insert apm_failuredomain, apm_failureclass, apm_failuremode sequentially.

    Uses INSERT...RETURNING via SELECT query to capture real generated IDs
    for FK references, avoiding fragile ID prediction.
    """
    # Categorize failure modes by component type
    _COMPONENT_DOMAIN: dict[str, str] = {
        "bearing": "mechanical",
        "motor": "electrical",
        "gearbox": "mechanical",
        "pump_impeller": "mechanical",
        "seal": "process",
        "heat_exchanger_bundle": "process",
        "compressor_stage": "mechanical",
        "turbine_blade": "mechanical",
        "chiller_compressor": "electrical",
        "generic_component": "instrumentation",
    }
    _COMPONENT_CLASS: dict[str, str] = {
        "bearing": "rotating_bearing",
        "motor": "motor_system",
        "gearbox": "rotating_gearbox",
        "pump_impeller": "rotating_pump",
        "seal": "fluid_containment",
        "heat_exchanger_bundle": "thermal_transfer",
        "compressor_stage": "rotating_compressor",
        "turbine_blade": "rotating_turbine",
        "chiller_compressor": "motor_compressor",
        "generic_component": "sensor_system",
    }

    # 1. Insert domains
    domain_sql_lines: list[str] = []
    for domain_code in ["mechanical", "electrical", "process", "instrumentation"]:
        for tenant_id in TENANT_IDS:
            domain_sql_lines.append(
                f"({_esc(tenant_id)}, {_esc(domain_code)}, {_esc(domain_code)}, "
                f"'{domain_code.title()} Failure Domain', 'Failure domain: {domain_code}')"
            )
    _run_psql_pipe(
        "INSERT INTO apm_failuredomain (tenant_id, yaml_id, code, label, description) VALUES\n"
        + ",\n".join(domain_sql_lines)
        + ";"
    )

    # Capture generated domain IDs
    domain_raw = _run_psql_raw("SELECT tenant_id, id, code FROM apm_failuredomain ORDER BY id")
    domain_ids: dict[tuple[str, str], int] = {}
    for line in domain_raw.strip().splitlines():
        if line:
            parts = line.split("|")
            if len(parts) >= 3:
                domain_ids[(parts[0], parts[2])] = int(parts[1])

    # 2. Insert classes using captured domain IDs
    class_sql_lines: list[str] = []
    seen_classes: set[str] = set()
    for cl_comp, class_code in _COMPONENT_CLASS.items():
        domain_code = _COMPONENT_DOMAIN.get(cl_comp, "mechanical")
        if class_code in seen_classes:
            continue
        seen_classes.add(class_code)
        for tenant_id in TENANT_IDS:
            domain_id = domain_ids.get((tenant_id, domain_code), 1)
            class_sql_lines.append(
                f"({_esc(tenant_id)}, {domain_id}, {_esc(class_code)}, {_esc(class_code)}, "
                f"'{class_code.replace('_', ' ').title()}', 'Class: {class_code}')"
            )
    _run_psql_pipe(
        "INSERT INTO apm_failureclass (tenant_id, domain_id, yaml_id, code, label, description) VALUES\n"
        + ",\n".join(class_sql_lines)
        + ";"
    )

    # Capture generated class IDs
    class_raw = _run_psql_raw("SELECT tenant_id, id, code FROM apm_failureclass ORDER BY id")
    class_ids: dict[tuple[str, str], int] = {}
    for line in class_raw.strip().splitlines():
        if line:
            parts = line.split("|")
            if len(parts) >= 3:
                class_ids[(parts[0], parts[2])] = int(parts[1])

    # 3. Insert failure modes using captured class IDs
    fm_sql_lines: list[str] = []
    failure_modes = failure_modes_cfg.get("failure_modes", [])
    _COMP_ASSET_MAP: dict[str, str] = {
        "bearing": "motor",
        "motor": "motor",
        "gearbox": "gearbox",
        "pump_impeller": "lift_pump",
        "seal": "reactor",
        "heat_exchanger_bundle": "heat_exchanger",
        "compressor_stage": "compressor",
        "turbine_blade": "turbine",
        "chiller_compressor": "chiller",
        "generic_component": "motor",
    }

    for fm in failure_modes:
        fm_id = fm.get("failure_mode_id", "")
        comp_type = fm.get("component_type", "generic_component")
        fm_name = fm.get("failure_mode_name", "")
        fm_effect = fm.get("failure_effect", "")
        related_metrics = json.dumps(fm.get("related_metrics", []))

        class_code = _COMPONENT_CLASS.get(comp_type, "rotating_bearing")
        asset_type = _COMP_ASSET_MAP.get(comp_type, comp_type)

        for tenant_id in TENANT_IDS:
            class_id = class_ids.get((tenant_id, class_code), 1)
            fm_sql_lines.append(
                f"({_esc(tenant_id)}, {class_id}, {_esc(fm_id)}, {_esc(fm_id)}, "
                f"{_esc(fm_name)}, {_esc(fm_effect)}, {_esc(asset_type)}, {_esc(comp_type)}, "
                f"{_esc(fm_effect)}, {_esc(related_metrics)}::jsonb)"
            )

    _run_psql_pipe(
        "INSERT INTO apm_failuremode (tenant_id, failure_class_id, yaml_id, code, label, "
        "description, asset_type, component_type, typical_effect, related_metrics) VALUES\n"
        + ",\n".join(fm_sql_lines)
        + ";"
    )


def _make_anomaly_rules_sql(anomaly_rules_cfg: dict[str, Any]) -> str:
    """Generate apm_anomaly_rule rows (one per rule per tenant)."""
    stmts: list[str] = []
    rules = anomaly_rules_cfg.get("rules", [])

    for rule in rules:
        profile_id = rule.get("profile_id", "")
        rule_name = rule.get("rule_name", "")
        method = rule.get("method", "")
        asset_type = rule.get("asset_type", None)
        component_type = rule.get("component_type", None)
        metric_name = rule.get("metric_name", "")
        unit = rule.get("unit", None)
        severity = rule.get("severity", "")
        params = json.dumps(rule.get("parameters", {}))
        approval = rule.get("approval_status", "draft")
        status = rule.get("status", "active")
        owner = rule.get("owner", None)
        version = rule.get("version", "1.0.0")
        effective_from = rule.get("effective_from", "2025-06-01T00:00:00Z")
        effective_to = rule.get("effective_to", None)
        created_by = rule.get("created_by", None)

        for tenant_id in TENANT_IDS:
            stmts.append(
                f"INSERT INTO apm_anomaly_rule "
                f"(tenant_id, rule_id, rule_name, method, asset_type, component_type, "
                f"metric_name, unit, severity, parameters, approval_status, status, "
                f"owner, version, effective_from, effective_to, created_by) VALUES "
                f"({_esc(tenant_id)}, {_esc(profile_id)}, {_esc(rule_name)}, {_esc(method)}, "
                f"{_esc(asset_type)}, {_esc(component_type)}, {_esc(metric_name)}, {_esc(unit)}, "
                f"{_esc(severity)}, {_esc(params)}::jsonb, {_esc(approval)}, {_esc(status)}, "
                f"{_esc(owner)}, {_esc(version)}, {_esc(effective_from)}::timestamptz, "
                f"{_esc(effective_to)}::timestamptz, {_esc(created_by)});"
            )
    return "\n".join(stmts)


def _make_threshold_profiles_sql(threshold_profiles_cfg: dict[str, Any]) -> str:
    """Generate apm_threshold_profile rows (one per profile per tenant).

    FK: apm_threshold_profile.rule_id -> apm_anomaly_rule.rule_id
    Both use the YAML profile_id as the join key, since PG apm_anomaly_rule
    uses YAML profile_id as its rule_id column.
    """
    stmts: list[str] = []
    profiles = threshold_profiles_cfg.get("profiles", [])

    for prof in profiles:
        profile_id = prof.get("profile_id", "")
        # Use profile_id as rule_id to match apm_anomaly_rule.rule_id
        rule_id_ref = profile_id
        profile_name = prof.get("rule_name", None)
        asset_type = prof.get("asset_type", None)
        component_type = prof.get("component_type", None)
        metric_name = prof.get("metric_name", "")
        unit = prof.get("unit", None)
        params = json.dumps(prof.get("parameters", {}))
        status = prof.get("status", "active")
        version = prof.get("version", "1.0.0")
        effective_from = prof.get("effective_from", "2025-06-01T00:00:00Z")
        effective_to = prof.get("effective_to", None)

        for tenant_id in TENANT_IDS:
            stmts.append(
                f"INSERT INTO apm_threshold_profile "
                f"(tenant_id, profile_id, rule_id, profile_name, asset_type, component_type, "
                f"metric_name, unit, parameters, status, version, "
                f"effective_from, effective_to) VALUES "
                f"({_esc(tenant_id)}, {_esc(profile_id)}, {_esc(rule_id_ref)}, {_esc(profile_name)}, "
                f"{_esc(asset_type)}, {_esc(component_type)}, {_esc(metric_name)}, {_esc(unit)}, "
                f"{_esc(params)}::jsonb, {_esc(status)}, {_esc(version)}, "
                f"{_esc(effective_from)}::timestamptz, {_esc(effective_to)}::timestamptz);"
            )
    return "\n".join(stmts)


def _make_failure_events_sql(assets_cfg: dict[str, Any]) -> str:
    """Generate apm_failureevent rows — 2-4 per asset, spread across May 2026.

    Uses existing component IDs from the database to avoid FK violations.
    """
    stmts: list[str] = []
    scenarios = ["spike", "drift", "flatline", "missing_heartbeat"]
    severities = ["critical", "warning", "warning", "critical"]
    event_statuses = ["suspected", "confirmed", "resolved"]
    may_days = 28

    event_id_counter = 0
    for tenant_id in TENANT_IDS:
        comps = _get_component_ids(tenant_id)
        if not comps:
            continue

        for comp_idx, (asset_id, comp_id, comp_type) in enumerate(comps):
            num_events = 3 if comp_type == "generic_component" else 2
            for ev_idx in range(num_events):
                si = (comp_idx + ev_idx) % len(scenarios)
                scenario = scenarios[si]
                sev = severities[si]
                e_status = event_statuses[(comp_idx + ev_idx) % len(event_statuses)]

                # Spread across May 2026, earlier events are more recent
                day_offset = (comp_idx * num_events + ev_idx) % may_days
                hour_offset = (comp_idx * 7 + ev_idx * 3) % 24
                minute_offset = (comp_idx * 13) % 60
                occurred_dt = MAY_START + timedelta(days=day_offset, hours=hour_offset, minutes=minute_offset)
                detected_dt = occurred_dt + timedelta(hours=1, minutes=15)

                event_id_counter += 1
                event_id = f"fe_{tenant_id}_{event_id_counter:04d}"

                failure_mode_id = (
                    _esc((comp_idx * num_events + ev_idx) % 12 + 1)
                    if (comp_idx * num_events + ev_idx) % 3 == 0
                    else "NULL"
                )
                label_confidence = (
                    f"{_esc(0.5 + ((comp_idx * num_events + ev_idx) % 5) * 0.1)}::numeric"
                    if e_status == "confirmed"
                    else "NULL"
                )

                stmts.append(
                    f"INSERT INTO apm_failureevent "
                    f"(tenant_id, failure_event_id, asset_id, component_id, failure_mode_id, "
                    f"event_status, severity, occurred_at, detected_at, "
                    f"source_system, source_event_type, label_confidence) VALUES "
                    f"({_esc(tenant_id)}, {_esc(event_id)}, {_esc(asset_id)}, {_esc(comp_id)}, "
                    f"{failure_mode_id}, {_esc(e_status)}, {_esc(sev)}, "
                    f"{_esc(occurred_dt.isoformat())}::timestamptz, "
                    f"{_esc(detected_dt.isoformat())}::timestamptz, "
                    f"'opc_ua_sim', {_esc(scenario)}, {label_confidence});"
                )
    return "\n".join(stmts)


def _make_workorder_sql(assets_cfg: dict[str, Any]) -> str:
    """Generate workorder rows linked to failure events — one per failure event."""
    stmts: list[str] = []
    wo_types = ["corrective", "preventive", "predictive", "corrective"]
    wo_statuses = ["open", "in_progress", "completed", "pending_review"]
    priorities = ["high", "medium", "low", "critical"]
    titles = [
        "Inspect and diagnose component degradation",
        "Replace worn bearing assembly",
        "Calibrate sensor and verify signal chain",
        "Lubricate rotating assembly and check clearances",
        "Perform vibration spectrum analysis",
        "Replace damaged seal and pressure test",
        "Clean heat exchanger and check thermal performance",
        "Inspect electrical connections and torque check",
    ]

    wo_counter = 0
    for tenant_id in TENANT_IDS:
        comps = _get_component_ids(tenant_id)
        if not comps:
            continue

        for comp_idx, (asset_id, comp_id, comp_type) in enumerate(comps):
            num_events = 3 if comp_type == "generic_component" else 2
            for ev_idx in range(num_events):
                wo_counter += 1
                wo_id = f"wo_{tenant_id}_{wo_counter:04d}"
                event_id = f"fe_{tenant_id}_{wo_counter:04d}"

                wo_type = wo_types[(comp_idx + ev_idx) % len(wo_types)]
                wo_status = wo_statuses[(comp_idx + ev_idx * 2) % len(wo_statuses)]
                priority = priorities[(comp_idx + ev_idx) % len(priorities)]
                title = titles[(comp_idx + ev_idx * 3) % len(titles)]

                # Requested a few hours before the failure event was detected
                may_day = (comp_idx * 5 + ev_idx * 3) % 28 + 1
                may_hour = (comp_idx * 3 + ev_idx) % 20 + 2
                requested_at = datetime(2026, 5, may_day, may_hour, 0, 0, tzinfo=UTC)
                planned_start = requested_at + timedelta(hours=4)
                completed_at_val = planned_start + timedelta(hours=8) if wo_status == "completed" else None
                actual_start_val = (
                    planned_start + timedelta(minutes=30) if wo_status in ("completed", "in_progress") else None
                )
                downtime_val = round(2.5 + (comp_idx + ev_idx) * 0.7, 2) if wo_status == "completed" else None
                completed_at_sql = (
                    f"{_esc(completed_at_val.isoformat())}::timestamptz" if completed_at_val is not None else "NULL"
                )
                actual_start_sql = (
                    f"{_esc(actual_start_val.isoformat())}::timestamptz" if actual_start_val is not None else "NULL"
                )
                downtime_sql = f"{_esc(downtime_val)}::numeric" if downtime_val is not None else "NULL"
                notes = (
                    f"'Inspected {comp_type}: found wear consistent with degradation pattern. Recommended further monitoring.'"
                    if wo_status == "completed"
                    else "NULL"
                )

                stmts.append(
                    f"INSERT INTO workorder "
                    f"(tenant_id, work_order_id, asset_id, component_id, source_system, "
                    f"work_order_type, work_order_status, priority, title, description, "
                    f"requested_at, planned_start_at, actual_start_at, completed_at, "
                    f"downtime_minutes, technician_notes, related_anomaly_id) VALUES "
                    f"({_esc(tenant_id)}, {_esc(wo_id)}, {_esc(asset_id)}, {_esc(comp_id)}, "
                    f"'opc_ua_sim', {_esc(wo_type)}, {_esc(wo_status)}, {_esc(priority)}, "
                    f"{_esc(title)}, 'Work order from anomaly detection', "
                    f"{_esc(requested_at.isoformat())}::timestamptz, "
                    f"{_esc(planned_start.isoformat())}::timestamptz, "
                    f"{actual_start_sql}, "
                    f"{completed_at_sql}, "
                    f"{downtime_sql}, {notes}, "
                    f"{_esc(event_id)});"
                )
    return "\n".join(stmts)


def _make_maintenance_sql(assets_cfg: dict[str, Any]) -> str:
    """Generate maintenance rows linked to work orders — one per work order."""
    stmts: list[str] = []
    maint_types = ["corrective", "preventive", "predictive", "condition_based"]
    maint_statuses = ["completed", "completed", "in_progress", "planned"]
    reasons = [
        "Anomaly detected: temperature threshold breach",
        "Anomaly detected: vibration pattern indicates bearing wear",
        "Anomaly detected: pressure drift suggesting seal degradation",
        "Anomaly detected: missing heartbeat — suspected sensor failure",
        "Anomaly detected: flatline signal — confirm sensor health",
    ]
    results = [
        "Replaced bearing assembly, vibration returned to normal range",
        "Recalibrated sensor, readings now within specification",
        "Tightened seal, pressure stabilized at nominal level",
        "Cleaned sensor contacts, signal restored",
        "No fault found — false alarm, sensor within calibration tolerance",
        "Replaced worn component, performance metrics restored",
    ]

    maint_counter = 0
    for tenant_id in TENANT_IDS:
        comps = _get_component_ids(tenant_id)
        if not comps:
            continue

        for comp_idx, (asset_id, comp_id, comp_type) in enumerate(comps):
            num_events = 3 if comp_type == "generic_component" else 2
            for ev_idx in range(num_events):
                maint_counter += 1
                maint_id = f"maint_{tenant_id}_{maint_counter:04d}"
                wo_id = f"wo_{tenant_id}_{maint_counter:04d}"
                ev_id = f"fe_{tenant_id}_{maint_counter:04d}"

                may_day = (comp_idx * 5 + ev_idx * 3) % 28 + 1
                may_hour = (comp_idx * 3 + ev_idx + 2) % 20 + 4
                performed_at = datetime(2026, 5, may_day, may_hour, 0, 0, tzinfo=UTC)
                completed_at = performed_at + timedelta(hours=3)

                maint_type = maint_types[(comp_idx + ev_idx) % len(maint_types)]
                maint_status = maint_statuses[(comp_idx + ev_idx) % len(maint_statuses)]
                reason = reasons[(comp_idx + ev_idx) % len(reasons)]
                result = results[(comp_idx + ev_idx * 2) % len(results)]
                labor_hours = f"{_esc(round(1.0 + (comp_idx + ev_idx) * 0.3, 2))}::numeric"
                downtime = f"{_esc(round(0.5 + (comp_idx + ev_idx) * 0.2, 2))}::numeric"
                parts_used = "'{}'::jsonb"

                stmts.append(
                    f"INSERT INTO maintenance "
                    f"(tenant_id, maintenance_id, asset_id, component_id, work_order_id, "
                    f"maintenance_type, maintenance_status, maintenance_reason, "
                    f"maintenance_result, performed_at, completed_at, "
                    f"parts_used, labor_hours, downtime_minutes, "
                    f"related_anomaly_id, sensor_anomaly_related) VALUES "
                    f"({_esc(tenant_id)}, {_esc(maint_id)}, {_esc(asset_id)}, {_esc(comp_id)}, "
                    f"{_esc(wo_id)}, {_esc(maint_type)}, {_esc(maint_status)}, {_esc(reason)}, "
                    f"{_esc(result)}, "
                    f"{_esc(performed_at.isoformat())}::timestamptz, "
                    f"{_esc(completed_at.isoformat())}::timestamptz, "
                    f"{parts_used}, {labor_hours}, {downtime}, "
                    f"{_esc(ev_id)}, {(comp_idx + ev_idx) % 2 == 1});"
                )
    return "\n".join(stmts)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    print("=== Phase 1: PostgreSQL Mock Data Loading ===")
    print(f"  Timestamp: {NOW.isoformat()}")
    print()

    # Load configs
    print("Loading YAML configs...")
    try:
        tenants_cfg = _load_yaml("tenants.yml")
        assets_cfg = _load_yaml("assets.yml")
        _components_cfg = _load_yaml("components.yml")
        anomaly_rules_cfg = _load_yaml("anomaly_rules.yml")
        threshold_profiles_cfg = _load_yaml("threshold_profiles.yml")
        failure_modes_cfg = _load_yaml("failure_modes.yml")
        measurement_pts_cfg = _load_yaml("measurement_points.yml")
    except FileNotFoundError as exc:
        print(f"ERROR: Config file missing: {exc}")
        sys.exit(1)

    row_counts: dict[str, int] = {}

    # -----------------------------------------------------------------------
    # Step 1: Tenants (verify, seed if missing)
    # -----------------------------------------------------------------------
    print("Step 1: Checking tenants...")
    tenant_count = _query_count("tenants")
    if tenant_count == 0:
        print("  Tenants table empty -- inserting from YAML...")
        _run_psql_pipe(_make_tenants_sql(tenants_cfg))
        tenant_count = _query_count("tenants")
    row_counts["tenants"] = tenant_count
    print(f"  tenants: {tenant_count} rows")

    # -----------------------------------------------------------------------
    # Step 2: Assets
    # -----------------------------------------------------------------------
    print("Step 2: Loading assets...")
    asset_count = _query_count("assets")
    if asset_count == 0:
        _run_psql_pipe(_make_assets_sql(assets_cfg))
        asset_count = _query_count("assets")
    row_counts["assets"] = asset_count
    print(f"  assets: {asset_count} rows")

    # -----------------------------------------------------------------------
    # Step 3: Asset Components (from tag metadata)
    # -----------------------------------------------------------------------
    print("Step 3: Loading asset_components (from tag metadata)...")
    comp_count = _query_count("asset_components")
    if comp_count == 0:
        _run_psql_pipe(_make_components_from_tag_metadata_sql())
        comp_count = _query_count("asset_components")
    row_counts["asset_components"] = comp_count
    print(f"  asset_components: {comp_count} rows")

    # -----------------------------------------------------------------------
    # Step 4: Asset Signal Tags Mapping
    # -----------------------------------------------------------------------
    print("Step 4: Loading asset_signal_tags_mapping...")
    tag_count = _query_count("asset_signal_tags_mapping")
    if tag_count == 0:
        print("  Generating tag mappings (this may take a moment)...")
        _run_psql_pipe(_make_tag_mappings_sql(assets_cfg, measurement_pts_cfg))
        tag_count = _query_count("asset_signal_tags_mapping")
    row_counts["asset_signal_tags_mapping"] = tag_count
    print(f"  asset_signal_tags_mapping: {tag_count} rows")

    # -----------------------------------------------------------------------
    # Step 5: Failure Taxonomy
    # -----------------------------------------------------------------------
    print("Step 5: Loading failure taxonomy...")

    domain_count = _query_count("apm_failuredomain")
    class_count = _query_count("apm_failureclass")
    mode_count = _query_count("apm_failuremode")
    if domain_count == 0 or class_count == 0 or mode_count == 0:
        _load_failure_taxonomy(failure_modes_cfg)
        domain_count = _query_count("apm_failuredomain")
        class_count = _query_count("apm_failureclass")
        mode_count = _query_count("apm_failuremode")
    row_counts["apm_failuredomain"] = domain_count
    print(f"  apm_failuredomain: {domain_count} rows")

    row_counts["apm_failureclass"] = class_count
    print(f"  apm_failureclass: {class_count} rows")

    row_counts["apm_failuremode"] = mode_count
    print(f"  apm_failuremode: {mode_count} rows")

    # -----------------------------------------------------------------------
    # Step 6: Anomaly Rules + Threshold Profiles
    # -----------------------------------------------------------------------
    print("Step 6: Loading anomaly rules and threshold profiles...")
    rule_count = _query_count("apm_anomaly_rule")
    if rule_count == 0:
        _run_psql_pipe(_make_anomaly_rules_sql(anomaly_rules_cfg))
        rule_count = _query_count("apm_anomaly_rule")
    row_counts["apm_anomaly_rule"] = rule_count
    print(f"  apm_anomaly_rule: {rule_count} rows")

    profile_count = _query_count("apm_threshold_profile")
    if profile_count == 0:
        _run_psql_pipe(_make_threshold_profiles_sql(threshold_profiles_cfg))
        profile_count = _query_count("apm_threshold_profile")
    row_counts["apm_threshold_profile"] = profile_count
    print(f"  apm_threshold_profile: {profile_count} rows")

    # -----------------------------------------------------------------------
    # Step 7: Failure Events
    # -----------------------------------------------------------------------
    print("Step 7: Loading failure events...")
    fe_count = _query_count("apm_failureevent")
    if fe_count == 0:
        _run_psql_pipe(_make_failure_events_sql(assets_cfg))
        fe_count = _query_count("apm_failureevent")
    row_counts["apm_failureevent"] = fe_count
    print(f"  apm_failureevent: {fe_count} rows")

    # -----------------------------------------------------------------------
    # Step 8: Work Orders + Maintenance
    # -----------------------------------------------------------------------
    print("Step 8: Loading work orders and maintenance...")
    wo_count = _query_count("workorder")
    if wo_count == 0:
        _run_psql_pipe(_make_workorder_sql(assets_cfg))
        wo_count = _query_count("workorder")
    row_counts["workorder"] = wo_count
    print(f"  workorder: {wo_count} rows")

    maint_count = _query_count("maintenance")
    if maint_count == 0:
        _run_psql_pipe(_make_maintenance_sql(assets_cfg))
        maint_count = _query_count("maintenance")
    row_counts["maintenance"] = maint_count
    print(f"  maintenance: {maint_count} rows")

    # Step 8b: Backfill FK references on failureevent (workorder/maintenance
    # must exist first due to FK constraints)
    print("Step 8b: Backfilling FK references on failureevent...")
    _run_psql_pipe("""
        UPDATE apm_failureevent fe SET
            work_order_id = wo.work_order_id,
            maintenance_id = m.maintenance_id
        FROM workorder wo
        JOIN maintenance m ON wo.tenant_id = m.tenant_id AND wo.work_order_id = m.work_order_id
        WHERE fe.tenant_id = wo.tenant_id
          AND fe.failure_event_id = wo.related_anomaly_id;
    """)
    print("  FK references updated")

    # -----------------------------------------------------------------------
    # Verification
    # -----------------------------------------------------------------------
    print()
    print("=== Verification ===")

    verification: dict[str, bool] = {}

    # Verify asset_signal_tags_mapping is joinable
    if tag_count > 0:
        try:
            verify_sql = (
                "SELECT COUNT(*) FROM asset_signal_tags_mapping "
                "WHERE tenant_id = 'tenant_northwind' AND source_system = 'opc_ua';"
            )
            _run_psql(verify_sql)
            verification["asset_signal_tags_mapping_joinable"] = True
            print("  [OK] asset_signal_tags_mapping joinable by (tenant_id, source_system)")
        except subprocess.CalledProcessError:
            verification["asset_signal_tags_mapping_joinable"] = False
            print("  [FAIL] asset_signal_tags_mapping not joinable")
    else:
        verification["asset_signal_tags_mapping_joinable"] = False

    # Verify tag_id uniqueness per tenant
    if tag_count > 0:
        dup_sql = (
            "SELECT COUNT(*) FROM ("
            "SELECT tenant_id, tag_id, COUNT(*) FROM asset_signal_tags_mapping "
            "GROUP BY tenant_id, tag_id HAVING COUNT(*) > 1"
            ") sub;"
        )
        try:
            out = _run_psql(dup_sql)
            dup_count = 0
            for line in out.strip().splitlines():
                stripped = line.strip()
                if stripped.isdigit():
                    dup_count = int(stripped)
                    break
            verification["tag_id_unique_per_tenant"] = dup_count == 0
        except subprocess.CalledProcessError:
            verification["tag_id_unique_per_tenant"] = False
    else:
        verification["tag_id_unique_per_tenant"] = False

    # Verify all 12 tables have data
    all_populated = all(v > 0 for v in row_counts.values())
    verification["all_tables_populated"] = all_populated
    print(f"  {'[OK]' if all_populated else '[FAIL]'} All 12 tables populated")

    # Verify FK chain: failureevent -> workorder -> maintenance
    if fe_count > 0 and wo_count > 0 and maint_count > 0:
        try:
            fk_sql = (
                "SELECT COUNT(*) FROM apm_failureevent fe "
                "JOIN workorder wo ON fe.tenant_id = wo.tenant_id AND fe.work_order_id = wo.work_order_id "
                "JOIN maintenance m ON wo.tenant_id = m.tenant_id AND wo.work_order_id = m.work_order_id;"
            )
            _run_psql(fk_sql)
            verification["fk_chain_failureevent_workorder_maintenance"] = True
            print("  [OK] FK chain failureevent -> workorder -> maintenance")
        except subprocess.CalledProcessError:
            verification["fk_chain_failureevent_workorder_maintenance"] = False
            print("  [FAIL] FK chain broken")
    else:
        verification["fk_chain_failureevent_workorder_maintenance"] = False

    # -----------------------------------------------------------------------
    # Summary JSON
    # -----------------------------------------------------------------------
    print()
    print("=== Row Counts ===")
    for table, count in sorted(row_counts.items()):
        print(f"  {table}: {count}")

    summary = {
        "tables": {k: {"rows": v} for k, v in row_counts.items()},
        "verification": verification,
        "generated_at": NOW.isoformat(),
    }

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    print(f"\nSummary written to {REPORT_PATH}")

    if all(v > 0 for v in row_counts.values()):
        print("\nPhase 1 data load complete.")
    else:
        print("\nPhase 1 data load completed with empty tables -- check errors above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
