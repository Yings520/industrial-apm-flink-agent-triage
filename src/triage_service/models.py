from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Recommendation:
    recommendation_id: str
    schema_version: str
    tenant_id: str
    anomaly_id: str
    evidence_version: str
    summary: str
    findings: list[str]
    possible_causes: list[str]
    recommended_checks: list[str]
    runbook_references: list[str]
    confidence_label: str
    quality_caveats: list[str]
    model_name: str
    prompt_version: str
    token_cost: float
    latency_ms: int
    created_at: str
    status: str = "fallback"

    def as_dict(self) -> dict[str, object]:
        return {
            "recommendation_id": self.recommendation_id,
            "schema_version": self.schema_version,
            "tenant_id": self.tenant_id,
            "anomaly_id": self.anomaly_id,
            "evidence_version": self.evidence_version,
            "summary": self.summary,
            "findings": list(self.findings),
            "possible_causes": list(self.possible_causes),
            "recommended_checks": list(self.recommended_checks),
            "runbook_references": list(self.runbook_references),
            "confidence_label": self.confidence_label,
            "quality_caveats": list(self.quality_caveats),
            "model_name": self.model_name,
            "prompt_version": self.prompt_version,
            "token_cost": self.token_cost,
            "latency_ms": self.latency_ms,
            "created_at": self.created_at,
            "status": self.status,
        }


@dataclass(frozen=True)
class ValidationResult:
    valid: bool
    validation_status: str
    quarantine_reason: str | None = None
    errors: list[str] = field(default_factory=list)
