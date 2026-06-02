from src.telemetry_generator.window_aggregates import aggregate_15m


def test_aggregate_15m_calculates_summary_fields() -> None:
    records = [
        {
            "tenant_id": "tenant_northwind",
            "tag_id": "tag_1",
            "event_id": f"evt_{i}",
            "event_time": f"2026-01-01T00:{i:02d}:00Z",
            "value": float(i),
            "quality_flags": [],
            "synthetic_metadata": {
                "asset_id": "asset_1",
                "component_id": "comp_1",
                "measurement_point_id": "mp_1",
                "operating_state": "normal",
            },
        }
        for i in range(15)
    ]

    windows = aggregate_15m(records)

    assert len(windows) == 1
    window = windows[0]
    assert window["window_start"] == "2026-01-01T00:00:00Z"
    assert window["window_end"] == "2026-01-01T00:15:00Z"
    assert window["avg_value"] == 7.0
    assert window["min_value"] == 0.0
    assert window["max_value"] == 14.0
    assert window["event_count"] == 15
    assert window["expected_count"] == 15
