from src.streaming.topic_names import raw_message_key, topics_for_tenant


def test_tenant_scoped_topic_names_are_layered() -> None:
    topics = topics_for_tenant("tenant_northwind")

    assert topics["raw_sensor_events"] == "tenant_northwind.raw_apm__sensor_readings.v1"
    assert topics["raw_dlq"] == "tenant_northwind.raw_apm__dlq_events.v1"
    assert topics["staging_telemetry_enriched"] == "tenant_northwind.staging_apm__sensor_readings.v1"
    assert topics["staging_dlq"] == "tenant_northwind.staging_apm__dlq_events.v1"
    assert topics["mart_anomalies"] == "tenant_northwind.mart_apm__fct_anomaly_events.v1"
    assert topics["mart_late_events"] == "tenant_northwind.mart_apm__fct_late_sensor_readings.v1"
    assert topics["mart_stream_health"] == "tenant_northwind.mart_apm__fct_stream_health_snapshots.v1"


def test_raw_message_key_uses_tenant_and_tag() -> None:
    key = raw_message_key({"tenant_id": "tenant_northwind", "tag_id": "tag_a"})

    assert key == "tenant_northwind|tag_a"
