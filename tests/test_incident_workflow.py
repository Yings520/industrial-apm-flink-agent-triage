from datetime import UTC, datetime, timedelta

import pytest

from src.alerts.incident_workflow import (
    base_correlation_key,
    correlation_key,
    group_anomalies,
    incident_id_for,
    severity_route,
)


def _anomaly(
    tenant_id: str = "tenant_northwind",
    asset_id: str = "asset_1",
    metric_name: str = "temperature",
    rule_id: str = "threshold_spike",
    window_start: str = "2026-01-01T00:00:00Z",
    anomaly_id: str | None = None,
    severity: str = "critical",
    window_end: str | None = None,
) -> dict[str, object]:
    return {
        "tenant_id": tenant_id,
        "asset_id": asset_id,
        "metric_name": metric_name,
        "rule_id": rule_id,
        "window_start": window_start,
        "window_end": window_end or window_start,
        "anomaly_id": anomaly_id or f"anom_{hash(tuple([tenant_id, asset_id, metric_name, rule_id, window_start, severity]))}",
        "severity": severity,
    }


def test_correlation_key_includes_tenant_asset_metric_rule_and_5min_bucket() -> None:
    a = _anomaly(tenant_id="t1", asset_id="a1", metric_name="m1", rule_id="r1", window_start="2026-01-01T00:03:00Z")
    key = correlation_key(a)
    assert "t1" in key
    assert "a1" in key
    assert "m1" in key
    assert "r1" in key
    assert "2026-01-01T00:00:00Z" in key


def test_correlation_key_different_buckets_produce_different_keys() -> None:
    a1 = _anomaly(window_start="2026-01-01T00:03:00Z")
    a2 = _anomaly(window_start="2026-01-01T00:06:00Z")
    assert correlation_key(a1) != correlation_key(a2)


def test_base_correlation_key_excludes_time_bucket() -> None:
    a1 = _anomaly(window_start="2026-01-01T00:03:00Z")
    a2 = _anomaly(window_start="2026-01-01T00:06:00Z")
    assert base_correlation_key(a1) == base_correlation_key(a2)


def test_different_tenants_have_different_base_keys() -> None:
    a1 = _anomaly(tenant_id="t1")
    a2 = _anomaly(tenant_id="t2")
    assert base_correlation_key(a1) != base_correlation_key(a2)


def test_incident_id_is_stable() -> None:
    a = _anomaly()
    ck = correlation_key(a)
    id1 = incident_id_for(ck)
    id2 = incident_id_for(ck)
    assert id1 == id2
    assert id1.startswith("inc_")


def test_severity_route() -> None:
    assert severity_route("critical") == "teams_card"
    assert severity_route("warning") == "ticket_simulated"
    assert severity_route("info") == "dashboard_only"
    assert severity_route("unknown") == "dashboard_only"


def test_single_anomaly_creates_one_incident() -> None:
    anomalies = [_anomaly()]
    incidents = group_anomalies(anomalies)
    assert len(incidents) == 1
    assert incidents[0]["anomaly_count"] == 1
    assert incidents[0]["max_severity"] == "critical"
    assert incidents[0]["routing_channel"] == "teams_card"
    assert "incident_id" in incidents[0]
    assert "correlation_key" in incidents[0]
    assert len(incidents[0]["source_anomaly_ids"]) == 1


def test_anomalies_in_same_cooldown_window_update_one_incident() -> None:
    base = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)
    anomalies = [
        _anomaly(
            window_start=_fmt(base),
            window_end=_fmt(base + timedelta(minutes=5)),
        ),
        _anomaly(
            window_start=_fmt(base + timedelta(minutes=7)),
            window_end=_fmt(base + timedelta(minutes=12)),
        ),
    ]
    incidents = group_anomalies(anomalies, cooldown_minutes=10, now=base + timedelta(minutes=20))
    assert len(incidents) == 1
    assert incidents[0]["anomaly_count"] == 2
    assert len(incidents[0]["source_anomaly_ids"]) == 2


def test_anomalies_outside_cooldown_create_new_incident() -> None:
    base = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)
    anomalies = [
        _anomaly(
            window_start=_fmt(base),
            window_end=_fmt(base + timedelta(minutes=5)),
        ),
        _anomaly(
            window_start=_fmt(base + timedelta(minutes=20)),
            window_end=_fmt(base + timedelta(minutes=25)),
        ),
    ]
    incidents = group_anomalies(anomalies, cooldown_minutes=10, now=base + timedelta(minutes=30))
    assert len(incidents) == 2
    for inc in incidents:
        assert inc["anomaly_count"] == 1


def test_different_tenants_never_share_incident() -> None:
    base = datetime(2026, 1, 1, tzinfo=UTC)
    anomalies = [
        _anomaly(tenant_id="t1", window_start=_fmt(base)),
        _anomaly(tenant_id="t2", window_start=_fmt(base)),
    ]
    incidents = group_anomalies(anomalies, cooldown_minutes=10, now=base + timedelta(minutes=5))
    assert len(incidents) == 2
    tenants = {inc["tenant_id"] for inc in incidents}
    assert tenants == {"t1", "t2"}


def test_max_severity_upgrades_within_cooldown() -> None:
    base = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)
    anomalies = [
        _anomaly(severity="info", window_start=_fmt(base)),
        _anomaly(severity="critical", window_start=_fmt(base + timedelta(minutes=3))),
    ]
    incidents = group_anomalies(anomalies, cooldown_minutes=10, now=base + timedelta(minutes=20))
    assert len(incidents) == 1
    assert incidents[0]["max_severity"] == "critical"
    assert incidents[0]["routing_channel"] == "teams_card"


def test_source_anomaly_ids_accumulate_and_remain_unique() -> None:
    base = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)
    a1 = _anomaly(anomaly_id="anom_a", window_start=_fmt(base))
    a2 = _anomaly(anomaly_id="anom_b", window_start=_fmt(base + timedelta(minutes=3)))
    a3 = _anomaly(anomaly_id="anom_a", window_start=_fmt(base + timedelta(minutes=5)))
    incidents = group_anomalies([a1, a2, a3], cooldown_minutes=10, now=base + timedelta(minutes=20))
    assert len(incidents) == 1
    ids = incidents[0]["source_anomaly_ids"]
    assert ids == ["anom_a", "anom_b"]


def test_incident_records_contain_required_fields() -> None:
    incidents = group_anomalies([_anomaly()])
    inc = incidents[0]
    for field in (
        "incident_id",
        "correlation_key",
        "tenant_id",
        "asset_id",
        "metric_name",
        "rule_id",
        "first_seen_at",
        "last_seen_at",
        "anomaly_count",
        "max_severity",
        "source_anomaly_ids",
        "status",
    ):
        assert field in inc, f"missing field {field}"
    assert inc["status"] == "open"


def _fmt(dt: datetime) -> str:
    return dt.isoformat().replace("+00:00", "Z")
