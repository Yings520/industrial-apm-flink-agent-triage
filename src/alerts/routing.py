from __future__ import annotations

from typing import Any


def route_decision(severity: str) -> dict[str, Any]:
    mapping = {
        "critical": "teams_card",
        "warning": "ticket_simulated",
        "info": "dashboard_only",
    }
    channel = mapping.get(severity, "dashboard_only")
    return {
        "routing_channel": channel,
        "routing_status": "pending",
    }


def routing_row(
    *,
    route_id: str,
    incident_id: str,
    tenant_id: str,
    severity: str,
    routing_channel: str,
    routing_status: str,
    notification_target: str | None = None,
    error_message: str | None = None,
    created_at: str | None = None,
) -> dict[str, Any]:
    return {
        "route_id": route_id,
        "incident_id": incident_id,
        "tenant_id": tenant_id,
        "severity": severity,
        "routing_channel": routing_channel,
        "routing_status": routing_status,
        "notification_target": notification_target,
        "error_message": error_message,
        "created_at": created_at,
    }
