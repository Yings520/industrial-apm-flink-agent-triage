import json

import pytest

from src.serving.dim_seeds import (
    seed_dim_asset,
    seed_dim_component,
    seed_dim_sensor_tag,
    seed_dim_site,
)


def test_seed_dim_site_known():
    result = seed_dim_site("tenant_northwind", "site_northwind_chicago")
    assert result["tenant_id"] == "tenant_northwind"
    assert result["city"] == "Chicago"
    assert result["country"] == "US"
    assert result["timezone"] == "America/Chicago"


def test_seed_dim_site_unknown():
    result = seed_dim_site("tenant_x", "site_unknown")
    assert result["site_name"] == "site_unknown - site_unknown"
    assert result["country"] == "XX"


def test_seed_dim_asset_required_fields():
    result = seed_dim_asset(
        "tenant_northwind", "site_01", "plant_01",
        "asset_001", "cnc_machine", "manufacturing_factory"
    )
    assert result["tenant_id"] == "tenant_northwind"
    assert result["is_current"] is True
    assert result["lifecycle_state"] == "active"
    assert result["parent_asset_id"] is None
    operating = json.loads(result["operating_context_json"])
    assert operating["operating_mode"] == "continuous"


def test_seed_dim_asset_with_parent():
    result = seed_dim_asset(
        "tenant_01", "site_01", "plant_01",
        "asset_sub_001", "motor", "manufacturing_factory",
        parent_asset_id="asset_main_001"
    )
    assert result["parent_asset_id"] == "asset_main_001"


def test_seed_dim_component():
    result = seed_dim_component(
        "tenant_northwind", "asset_001", "comp_bearing_001", "bearing", "high"
    )
    assert result["component_type"] == "bearing"
    assert result["criticality"] == "high"
    assert result["is_current"] is True


def test_seed_dim_sensor_tag():
    result = seed_dim_sensor_tag(
        "tenant_northwind", "tag_001", "vibration_sensor",
        "asset_001", "vibration", "mm_s", component_id="comp_001"
    )
    assert result["signal_type"] == "analog"
    assert result["sampling_rate_hz"] == 1.0
    assert 0.0 <= result["normal_range_min"] <= 11.0
    assert result["calibration_status"] == "calibrated"
    assert result["component_id"] == "comp_001"


def test_seed_dim_sensor_tag_unknown_metric():
    result = seed_dim_sensor_tag(
        "t", "t1", "tn", "a1", "new_metric", "unknown_unit"
    )
    assert result["normal_range_min"] == 0.0
    assert result["normal_range_max"] == 100.0
