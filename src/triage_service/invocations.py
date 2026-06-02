from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from typing import Any

from src.serving.triage_evidence import evidence_json


def request_hash_for(evidence: dict[str, Any]) -> str:
    payload = evidence_json(evidence)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:32]


def invocation_id_for(
    tenant_id: str,
    incident_id: str,
    anomaly_id: str,
    evidence_id: str,
    request_hash: str,
) -> str:
    digest = hashlib.sha1(f"{tenant_id}|{incident_id}|{anomaly_id}|{evidence_id}|{request_hash}".encode()).hexdigest()[
        :16
    ]
    return f"inv_{digest}"


def invocation_row(
    *,
    tenant_id: str,
    incident_id: str,
    anomaly_id: str,
    evidence_id: str,
    provider: str,
    model_name: str,
    prompt_version: str,
    raw_response: str,
    parsed_status: str,
    validation_status: str,
    quarantine_reason: str,
    token_cost: float,
    latency_ms: int,
    created_at: str | None = None,
) -> dict[str, Any]:
    request_hash = request_hash_for({"evidence_id": evidence_id})
    invocation_id = invocation_id_for(tenant_id, incident_id, anomaly_id, evidence_id, request_hash)
    return {
        "invocation_id": invocation_id,
        "tenant_id": tenant_id,
        "incident_id": incident_id,
        "anomaly_id": anomaly_id,
        "evidence_id": evidence_id,
        "provider": provider,
        "model_name": model_name,
        "prompt_version": prompt_version,
        "request_hash": request_hash,
        "raw_response": raw_response,
        "parsed_status": parsed_status,
        "validation_status": validation_status,
        "quarantine_reason": quarantine_reason,
        "token_cost": token_cost,
        "latency_ms": latency_ms,
        "created_at": created_at or datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
    }
