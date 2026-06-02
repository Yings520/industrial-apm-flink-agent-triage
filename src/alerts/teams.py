from __future__ import annotations

import json
import os
from typing import Any, Protocol

BANNED_WORDS = ("autonomous diagnosis", "definitive root cause", "automatic remediation")


class TeamsTransport(Protocol):
    def __call__(self, *, url: str, headers: dict[str, str], body: bytes, timeout: int) -> tuple[int, bytes]: ...


def _std_transport(*, url: str, headers: dict[str, str], body: bytes, timeout: int) -> tuple[int, bytes]:
    from urllib.request import Request, urlopen

    req = Request(url, data=body, headers=headers, method="POST")
    with urlopen(req, timeout=timeout) as resp:
        return resp.status, resp.read()


def teams_card_payload(
    incident: dict[str, Any],
    recommendation: dict[str, Any] | None,
    evidence: dict[str, Any],
) -> dict[str, Any]:
    rec = recommendation or {}
    return {
        "@type": "MessageCard",
        "@context": "https://schema.org/extensions",
        "themeColor": "FF0000" if incident.get("max_severity") == "critical" else "FFA500",
        "summary": f"Alert: {incident.get('tenant_id')} / {incident.get('asset_id')} / {incident.get('metric_name')}",
        "title": "Industrial APM Alert - Operator Support",
        "text": (
            f"Tenant: {incident.get('tenant_id')}\n\n"
            f"Asset: {incident.get('asset_id')}\n\n"
            f"Metric: {incident.get('metric_name')}\n\n"
            f"Rule: {incident.get('rule_id')}\n\n"
            f"Incident ID: {incident.get('incident_id')}\n\n"
            f"Severity: {incident.get('max_severity')}\n\n"
            f"Anomaly count: {incident.get('anomaly_count')}\n\n"
            f"First seen: {incident.get('first_seen_at')}\n\n"
            f"Last seen: {incident.get('last_seen_at')}\n\n"
            f"Cooldown: 10 min\n\n"
            f"Triage finding: {rec.get('summary', 'N/A')}\n\n"
            f"Recommended checks: {', '.join(rec.get('recommended_checks', [])) or 'N/A'}\n\n"
            f"Evidence ID: {evidence.get('evidence_id', 'N/A')}\n\n"
            f"Recommendation ID: {rec.get('recommendation_id', 'N/A')}\n\n"
            "This is an LLM-assisted triage summary for operator review. "
            "Review the recommended checks before taking operational action."
        ),
    }


def send_teams_card(
    payload: dict[str, Any],
    *,
    webhook_url: str | None = None,
    transport: TeamsTransport | None = None,
) -> dict[str, Any]:
    url = webhook_url or os.environ.get("TEAMS_WEBHOOK_URL", "").strip()

    if not url:
        return {"routing_status": "simulated", "routing_channel": "teams_card", "error": None}

    t = transport or _std_transport
    body = json.dumps(payload).encode("utf-8")
    headers = {"Content-Type": "application/json"}

    try:
        status, resp_bytes = t(url=url, headers=headers, body=body, timeout=10)
        if 200 <= status < 300:
            return {"routing_status": "delivered", "routing_channel": "teams_card", "error": None}
        else:
            return {
                "routing_status": "failed",
                "routing_channel": "teams_card",
                "error": f"HTTP {status}: {resp_bytes.decode('utf-8', errors='replace')[:512]}",
            }
    except Exception as exc:
        return {"routing_status": "failed", "routing_channel": "teams_card", "error": str(exc)[:512]}
