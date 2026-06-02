from src.telemetry_generator.business_data import generate_business_data


def test_business_data_has_referential_integrity() -> None:
    data = generate_business_data(seed=42)

    asset_ids = {record["asset_id"] for record in data["assets"]}
    component_asset_ids = {record["asset_id"] for record in data["components"]}
    tag_component_ids = {record["component_id"] for record in data["sensor_tags"]}
    component_ids = {record["component_id"] for record in data["components"]}

    assert asset_ids
    assert component_asset_ids.issubset(asset_ids)
    assert tag_component_ids.issubset(component_ids)


def test_business_data_includes_rul_and_work_orders() -> None:
    data = generate_business_data(seed=42)

    assert data["work_orders"]
    assert data["maintenance_events"]
    assert data["rul_predictions"]
    assert all(record["rul_hours"] >= 0 for record in data["rul_predictions"])
