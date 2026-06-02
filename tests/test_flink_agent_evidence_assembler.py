import pytest

from src.flink_agents.evidence_assembler import assemble_evidence


def _anomaly_trigger(**overrides: object) -> dict[str, object]:
    event: dict[str, object] = {
        "tenant_id": "tenant_northwind",
        "anomaly_id": "anom_001",
        "asset_id": "asset_001",
        "component_id": "comp_001",
        "tag_id": "tag_temp",
        "metric_name": "temperature",
        "rule_id": "rule_threshold_spike_001",
        "window_start": "2026-01-01T00:00:00Z",
        "window_end": "2026-01-01T00:05:00Z",
        "observed_value": 95.0,
        "baseline_value": 60.0,
        "expected_value": 65.0,
        "severity": "warning",
        "quality_flags": ["threshold_spike"],
        "source_event_ids": ["evt_001", "evt_002"],
    }
    event.update(overrides)
    return event


def _context(**overrides: object) -> dict[str, object]:
    ctx: dict[str, object] = {
        "signal_window": [
            {
                "tenant_id": "tenant_northwind",
                "event_time": "2026-01-01T00:01:00Z",
                "tag_id": "tag_temp",
                "metric_name": "temperature",
                "value": 88.0,
                "unit": "celsius",
                "quality_flags": [],
            }
        ],
        "asset_context": {
            "asset_id": "asset_001",
            "asset_name": "Primary Boiler",
            "asset_type": "boiler",
        },
        "signal_mapping": {
            "tag_id": "tag_temp",
            "measurement_point": "boiler_outlet_temp",
            "expected_unit": "celsius",
        },
        "evidence": {
            "failure_modes": [
                {"tenant_id": "tenant_northwind", "failure_mode_id": "fm_001", "entity": "failure_mode"}
            ],
            "failure_events": [],
            "work_orders": [],
            "maintenance_records": [],
        },
    }
    ctx.update(overrides)
    return ctx


class TestEvidenceAssembler:
    def test_normal_assembly(self) -> None:
        evidence = assemble_evidence(_anomaly_trigger(), _context())
        assert evidence["tenant_id"] == "tenant_northwind"
        assert evidence["anomaly_id"] == "anom_001"
        assert evidence["evidence_id"].startswith("ev_")
        assert evidence["evidence_version"] == "triage_evidence.v1"
        assert "finding" not in evidence
        assert "recommendation" not in evidence
        assert "recommended_action" not in evidence
        assert "root_cause" not in evidence

    def test_missing_context_handling(self) -> None:
        evidence = assemble_evidence(_anomaly_trigger(), {})
        assert evidence["tenant_id"] == "tenant_northwind"
        assert evidence["sensor_window"] == []
        assert evidence["failure_modes"] == []

    def test_cross_tenant_failure(self) -> None:
        trigger = _anomaly_trigger(tenant_id="tenant_northwind")
        cross_ctx = _context()
        cross_ctx["signal_window"] = [
            {"tenant_id": "tenant_southridge", "event_time": "2026-01-01T00:01:00Z", "value": 88.0}
        ]
        with pytest.raises(ValueError, match="Cross-tenant evidence assembly denied"):
            assemble_evidence(trigger, cross_ctx)

    def test_forbidden_field_rejection(self) -> None:
        trigger = _anomaly_trigger()
        trigger["finding"] = "some finding"
        with pytest.raises(ValueError, match="forbidden LLM fields"):
            assemble_evidence(trigger, _context())

    def test_quality_caveat_inclusion(self) -> None:
        trigger = _anomaly_trigger(
            quality_flags=["late_arrival", "missing_heartbeat", "threshold_spike"],
        )
        evidence = assemble_evidence(trigger, _context())
        assert "quality_caveats" in evidence
        caveats = evidence["quality_caveats"]
        assert any("late-arriving" in c for c in caveats)
        assert any("heartbeat" in c for c in caveats)

    def test_quality_caveat_for_empty_signal_window(self) -> None:
        ctx = _context()
        ctx["signal_window"] = []
        evidence = assemble_evidence(_anomaly_trigger(), ctx)
        assert "quality_caveats" in evidence
        assert any("missing or empty" in c for c in evidence["quality_caveats"])

    def test_flatline_quality_caveat(self) -> None:
        trigger = _anomaly_trigger(quality_flags=["flatline"])
        evidence = assemble_evidence(trigger, _context())
        assert any("Flatline" in c for c in evidence.get("quality_caveats", []))

    def test_invalid_contract_quality_caveat(self) -> None:
        trigger = _anomaly_trigger(quality_flags=["invalid_contract"])
        evidence = assemble_evidence(trigger, _context())
        assert any("schema contract" in c for c in evidence.get("quality_caveats", []))

    def test_derives_runbook_refs_from_asset_type(self) -> None:
        trigger = _anomaly_trigger(asset_type="boiler", metric_name="steam_temperature")
        evidence = assemble_evidence(trigger, _context())
        assert len(evidence["runbook_refs"]) > 0
        assert evidence["runbook_refs"][0]["runbook_id"] == "power_generation_vibration"
        assert evidence["runbook_refs"][0]["section"] == "steam_temperature"
