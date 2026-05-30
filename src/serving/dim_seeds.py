from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def seed_dim_site(tenant_id: str, site_id: str) -> dict[str, Any]:
    site_names = {
        "site_northwind_chicago": ("Chicago", "US", "America/Chicago"),
        "site_northwind_baytown": ("Baytown", "US", "America/Chicago"),
        "site_northwind_phoenix": ("Phoenix", "US", "America/Phoenix"),
        "site_apac_pohang": ("Pohang", "KR", "Asia/Seoul"),
        "site_apac_singapore": ("Singapore", "SG", "Asia/Singapore"),
        "site_apac_university": ("Singapore", "SG", "Asia/Singapore"),
        "site_euro_rotterdam": ("Rotterdam", "NL", "Europe/Amsterdam"),
        "site_euro_stuttgart": ("Stuttgart", "DE", "Europe/Berlin"),
        "site_euro_oslo": ("Oslo", "NO", "Europe/Oslo"),
    }
    city, country, timezone = site_names.get(site_id, (site_id, "XX", "UTC"))
    return {
        "tenant_id": tenant_id,
        "site_id": site_id,
        "site_name": f"{site_id} - {city}",
        "city": city,
        "country": country,
        "timezone": timezone,
    }


def seed_dim_asset(
    tenant_id: str,
    site_id: str,
    plant_id: str,
    asset_id: str,
    asset_type: str,
    archetype: str,
    criticality: str = "medium",
    parent_asset_id: str | None = None,
) -> dict[str, Any]:
    now = _now()
    return {
        "tenant_id": tenant_id,
        "site_id": site_id,
        "plant_id": plant_id,
        "asset_id": asset_id,
        "asset_name": f"{asset_type.replace('_', ' ').title()} {asset_id[-4:]}",
        "asset_type": asset_type,
        "parent_asset_id": parent_asset_id,
        "functional_location": f"FL-{plant_id}-{asset_id[-8:]}",
        "manufacturer": "DemoCorp",
        "model": f"MDL-{asset_type[:4].upper()}-{asset_id[-4:]}",
        "serial_number": f"SN-{asset_id}",
        "commissioned_at": "2020-01-01T00:00:00Z",
        "lifecycle_state": "active",
        "operating_context_json": json.dumps(
            {"operating_mode": "continuous", "environmental_class": "indoor_industrial"}
        ),
        "criticality": criticality,
        "archetype": archetype,
        "threshold_profile_id": "threshold_manufacturing_rotating",
        "expected_metrics": json.dumps(["temperature", "vibration", "power_draw"]),
        "valid_from": "2020-01-01T00:00:00Z",
        "valid_to": None,
        "is_current": True,
    }


def seed_dim_component(
    tenant_id: str,
    asset_id: str,
    component_id: str,
    component_type: str,
    criticality: str = "medium",
) -> dict[str, Any]:
    return {
        "tenant_id": tenant_id,
        "asset_id": asset_id,
        "component_id": component_id,
        "component_type": component_type,
        "component_name": f"{component_type.replace('_', ' ').title()} {component_id[-4:]}",
        "functional_location": f"FL-CMP-{asset_id}-{component_id[-8:]}",
        "manufacturer": "DemoCorp",
        "model": f"MDL-CMP-{component_type[:4]}-{component_id[-4:]}",
        "serial_number": f"SN-{component_id}",
        "commissioned_at": "2020-01-01T00:00:00Z",
        "lifecycle_state": "active",
        "criticality": criticality,
        "expected_metrics": json.dumps(["temperature", "vibration"]),
        "valid_from": "2020-01-01T00:00:00Z",
        "valid_to": None,
        "is_current": True,
    }


def seed_dim_sensor_tag(
    tenant_id: str,
    tag_id: str,
    tag_name: str,
    asset_id: str,
    metric_name: str,
    unit: str,
    source_system: str = "opc_ua",
    component_id: str | None = None,
) -> dict[str, Any]:
    ranges = {
        ("temperature", "celsius"): (15.0, 95.0),
        ("vibration", "mm_s"): (0.0, 11.0),
        ("pressure", "bar"): (0.5, 16.0),
        ("flow_rate", "m3_h"): (0.0, 500.0),
        ("power_draw", "kw"): (0.0, 250.0),
    }
    normal_min, normal_max = ranges.get((metric_name, unit), (0.0, 100.0))
    return {
        "tenant_id": tenant_id,
        "tag_id": tag_id,
        "tag_name": tag_name,
        "plant_id": None,
        "asset_id": asset_id,
        "component_id": component_id,
        "metric_name": metric_name,
        "unit": unit,
        "source_system": source_system,
        "signal_type": "analog",
        "sampling_rate_hz": 1.0,
        "measurement_point": f"MP-{component_id or asset_id}-{metric_name}",
        "normal_range_min": normal_min,
        "normal_range_max": normal_max,
        "engineering_limit_min": normal_min * 0.8,
        "engineering_limit_max": normal_max * 1.2,
        "calibration_status": "calibrated",
        "last_calibrated_at": "2025-12-01T00:00:00Z",
        "valid_from": "2020-01-01T00:00:00Z",
        "valid_to": None,
        "is_current": True,
    }
