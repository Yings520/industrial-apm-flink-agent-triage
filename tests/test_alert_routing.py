import os
from typing import cast, final

from src.alerts.routing import route_decision, routing_row
from src.alerts.teams import send_teams_card, teams_card_payload


def _incident(**overrides: object) -> dict[str, object]:
    data = {
        "incident_id": "inc_001",
        "correlation_key": "t1|a1|temp|r1|2026-01-01T00:00:00Z",
        "tenant_id": "tenant_northwind",
        "asset_id": "asset_1",
        "metric_name": "temperature",
        "rule_id": "threshold_spike",
        "first_seen_at": "2026-01-01T00:00:00Z",
        "last_seen_at": "2026-01-01T00:05:00Z",
        "anomaly_count": 3,
        "max_severity": "critical",
        "source_anomaly_ids": ["anom_1", "anom_2"],
        "status": "open",
        "routing_channel": "teams_card",
    }
    data.update(overrides)  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]
    return cast(dict[str, object], data)


def _recommendation() -> dict[str, object]:
    return {
        "recommendation_id": "rec_001",
        "tenant_id": "tenant_northwind",
        "summary": "Temperature spike detected.",
        "recommended_checks": ["Inspect sensor"],
        "recommendation_id": "rec_001",
    }


def _evidence() -> dict[str, object]:
    return {
        "evidence_id": "ev_001",
        "tenant_id": "tenant_northwind",
        "anomaly_id": "anom_1",
        "evidence_version": "triage_evidence.v1",
    }


def test_route_decision_critical_is_teams_card() -> None:
    assert route_decision("critical")["routing_channel"] == "teams_card"


def test_route_decision_warning_is_ticket_simulated() -> None:
    assert route_decision("warning")["routing_channel"] == "ticket_simulated"


def test_route_decision_info_is_dashboard_only() -> None:
    assert route_decision("info")["routing_channel"] == "dashboard_only"


def test_route_decision_unknown_defaults_to_dashboard_only() -> None:
    assert route_decision("unknown")["routing_channel"] == "dashboard_only"


def test_routing_row_contains_required_fields() -> None:
    row = routing_row(
        route_id="route_001",
        incident_id="inc_001",
        tenant_id="t1",
        severity="critical",
        routing_channel="teams_card",
        routing_status="simulated",
        created_at="2026-01-01T00:00:00Z",
    )
    assert row["routing_status"] == "simulated"
    assert row["routing_channel"] == "teams_card"


def test_send_teams_card_missing_webhook_simulates() -> None:
    with _no_env("TEAMS_WEBHOOK_URL"):
        result = send_teams_card({"test": True}, webhook_url="")
        assert result["routing_status"] == "simulated"


def test_send_teams_card_configured_success() -> None:
    def fake_transport(*, url: str, headers: dict[str, str], body: bytes, timeout: int) -> tuple[int, bytes]:
        return 200, b"OK"

    result = send_teams_card(
        {"test": True}, webhook_url="https://example.com/hook", transport=fake_transport
    )
    assert result["routing_status"] == "delivered"


def test_send_teams_card_configured_failure_records_failed() -> None:
    def fake_transport(*, url: str, headers: dict[str, str], body: bytes, timeout: int) -> tuple[int, bytes]:
        return 500, b"error"

    result = send_teams_card(
        {"test": True}, webhook_url="https://example.com/hook", transport=fake_transport
    )
    assert result["routing_status"] == "failed"
    assert result["error"] is not None


def test_send_teams_card_transport_exception_records_failed() -> None:
    def fake_transport(*, url: str, headers: dict[str, str], body: bytes, timeout: int) -> tuple[int, bytes]:
        raise ConnectionError("refused")

    result = send_teams_card(
        {"test": True}, webhook_url="https://example.com/hook", transport=fake_transport
    )
    assert result["routing_status"] == "failed"


def test_teams_card_payload_contains_required_fields() -> None:
    payload = teams_card_payload(_incident(), _recommendation(), _evidence())
    text = str(payload.get("text", ""))
    assert "incident_id" in text.lower() or "inc_001" in text
    assert "tenant_northwind" in text
    assert "asset_1" in text
    assert "temperature" in text
    assert "threshold_spike" in text
    assert "ev_001" in text
    assert "rec_001" in text
    assert "anomaly count" in text.lower()


def test_teams_card_payload_avoids_banned_wording() -> None:
    payload = teams_card_payload(_incident(), _recommendation(), _evidence())
    text = str(payload.get("text", ""))
    text_lower = text.lower()
    for banned in ("autonomous diagnosis", "definitive root cause", "automatic remediation"):
        assert banned not in text_lower, f"banned phrase found: {banned}"


def _no_env(var: str) -> "_EnvGuard":
    return _EnvGuard(var)


@final
class _EnvGuard:
    def __init__(self, var: str) -> None:
        self._var = var
        self._saved: str | None = None

    def __enter__(self) -> None:
        self._saved = os.environ.get(self._var)
        _ = os.environ.pop(self._var, None)

    def __exit__(self, *args: object) -> None:
        if self._saved is not None:
            os.environ[self._var] = self._saved
