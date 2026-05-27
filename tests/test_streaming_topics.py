from src.streaming.topic_names import raw_message_key, topics_for_tenant


def test_tenant_scoped_topic_names_are_layered() -> None:
    topics = topics_for_tenant("tenant_northwind")

    assert topics["raw_sensor_events"] == "tenant_northwind.raw_sensor_events.v1"
    assert topics["raw_dlq"] == "tenant_northwind.raw_dlq.v1"
    assert topics["staging_telemetry_enriched"] == "tenant_northwind.staging_telemetry_enriched.v1"
    assert topics["staging_dlq"] == "tenant_northwind.staging_dlq.v1"
    assert topics["mart_anomalies"] == "tenant_northwind.mart_anomalies.v1"
    assert topics["mart_late_events"] == "tenant_northwind.mart_late_events.v1"
    assert topics["mart_stream_health"] == "tenant_northwind.mart_stream_health.v1"


def test_raw_message_key_uses_tenant_and_tag() -> None:
    key = raw_message_key({"tenant_id": "tenant_northwind", "tag_id": "tag_a"})

    assert key == "tenant_northwind|tag_a"
