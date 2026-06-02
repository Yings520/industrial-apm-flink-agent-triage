from src.telemetry_generator.config import build_tag_metadata, load_measurement_point_config
from src.telemetry_generator.measurement_points import build_measurement_point_catalog


def test_measurement_point_config_covers_target_asset_types() -> None:
    config = load_measurement_point_config()

    assert config["schema_version"] == "measurement_points.v1"
    assert {"pump", "motor", "compressor", "conveyor", "reactor"}.issubset(set(config["asset_types"]))


def test_measurement_points_expand_to_tags_with_component_context() -> None:
    catalog = build_measurement_point_catalog(asset_type="pump")

    vibration_points = [point for point in catalog if point["metric_name"] == "vibration_rms"]
    assert vibration_points
    assert all(point["component_type"] for point in vibration_points)
    assert all(point["measurement_point_id"] for point in vibration_points)
    assert all(point["tag_suffix"].endswith("_mm_s") for point in vibration_points)


def test_tag_metadata_contains_measurement_point_and_component_context() -> None:
    metadata = build_tag_metadata()
    sample = next(iter(metadata.values()))

    assert sample["component_id"].startswith("comp_")
    assert sample["component_type"]
    assert sample["measurement_point_id"].startswith("mp_")
    assert sample["sampling_rate_hz"] == 1.0
    assert sample["normal_range_min"] < sample["normal_range_max"]
    assert sample["engineering_limit_min"] < sample["engineering_limit_max"]
