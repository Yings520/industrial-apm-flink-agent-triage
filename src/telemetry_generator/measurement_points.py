from __future__ import annotations

import hashlib

from src.telemetry_generator.config import load_measurement_point_config
from src.telemetry_generator.models import MeasurementPointRecord


def _stable_measurement_point_id(asset_type: str, component_type: str, measurement_point: str) -> str:
    digest = hashlib.sha1(f"{asset_type}|{component_type}|{measurement_point}".encode()).hexdigest()[:10]
    return f"mp_{asset_type}_{digest}"


def build_measurement_point_catalog(asset_type: str) -> list[MeasurementPointRecord]:
    config = load_measurement_point_config()
    if asset_type not in config["measurement_points"]:
        raise ValueError(f"Unknown asset_type for measurement points: {asset_type}")

    return [
        {
            **point,
            "asset_type": asset_type,
            "measurement_point_id": _stable_measurement_point_id(
                asset_type,
                point["component_type"],
                point["measurement_point"],
            ),
        }
        for point in config["measurement_points"][asset_type]
    ]
