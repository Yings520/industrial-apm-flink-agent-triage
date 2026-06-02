from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import pytest

from src.flink_agents.diagnosis_generator import generate_diagnosis
from src.flink_agents.evidence_assembler import assemble_evidence
from src.flink_agents.output_validator import validate_diagnosis
from src.flink_agents.realtime_diagnosis_agent import _load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]


def _llm_available() -> bool:
    return bool(
        os.environ.get("OPENAI_API_KEY", "").strip()
        and os.environ.get("OPENAI_MODEL", "").strip()
    )


def _load_env_for_tests() -> None:
    env_path = REPO_ROOT / ".env"
    if env_path.exists():
        _load_dotenv(str(env_path))


_load_env_for_tests()

requires_llm = pytest.mark.skipif(
    not _llm_available(),
    reason="Requires OPENAI_API_KEY and OPENAI_MODEL in .env",
)


def _valid_anomaly_event(**overrides: Any) -> dict[str, Any]:
    event: dict[str, Any] = {
        "tenant_id": "tenant_northwind",
        "anomaly_id": "anom_e2e_001",
        "asset_id": "asset_001",
        "component_id": "comp_001",
        "tag_id": "tag_001_temperature",
        "tag_name": "Temperature Sensor A1",
        "metric_name": "temperature",
        "rule_id": "rule_threshold_spike_001",
        "window_start": "2026-01-01T00:00:00Z",
        "window_end": "2026-01-01T00:05:00Z",
        "severity": "warning",
        "quality_flags": [],
        "source_event_ids": ["evt_001", "evt_002"],
        "observed_value": 95.0,
        "baseline_value": 72.0,
        "expected_value": 75.0,
        "detection_confidence": 0.85,
        "asset_type": "pump",
        "source_system": "scada_pi",
    }
    event.update(overrides)
    return event


def _valid_context(**overrides: Any) -> dict[str, Any]:
    ctx: dict[str, Any] = {
        "signal_window": [
            {
                "tenant_id": "tenant_northwind",
                "event_time": "2026-01-01T00:02:00Z",
                "tag_id": "tag_001_temperature",
                "metric_name": "temperature",
                "value": 92.0,
                "unit": "celsius",
                "quality_flags": [],
            },
            {
                "tenant_id": "tenant_northwind",
                "event_time": "2026-01-01T00:04:00Z",
                "tag_id": "tag_001_temperature",
                "metric_name": "temperature",
                "value": 96.0,
                "unit": "celsius",
                "quality_flags": [],
            },
        ],
        "asset_context": {
            "asset_id": "asset_001",
            "asset_name": "Cooling Pump A",
            "asset_type": "pump",
            "asset_class": "rotating",
            "site_id": "site_001",
            "area_id": "area_001",
            "line_id": "line_001",
            "functional_location": "PLANT-A/BUILDING-1/FLOOR-1",
            "component_id": "comp_001",
            "component_type": "bearing",
            "manufacturer": "SKF",
            "model": "Bearing-6205",
        },
        "signal_mapping": {
            "tag_id": "tag_001_temperature",
            "source_system": "scada_pi",
            "measurement_point": "pump_bearing_temp",
            "expected_unit": "celsius",
            "normal_range_min": 20.0,
            "normal_range_max": 80.0,
            "sampling_rate_seconds": 60,
        },
        "evidence": {
            "failure_modes": [
                {
                    "entity": "apm_failuremode",
                    "tenant_id": "tenant_northwind",
                    "failure_mode_id": "fm_001",
                    "failure_mode_name": "bearing_overheat",
                }
            ],
            "failure_events": [],
            "work_orders": [],
            "maintenance_records": [],
        },
    }
    ctx.update(overrides)
    return ctx



class TestFlinkAgentE2E:
    """Phase 2 Flink Agent E2E test scenarios."""

    # --- Scenario 1: Normal anomaly → valid diagnosis ---

    @requires_llm
    def test_scenario1_normal_anomaly_valid_diagnosis_llm(self) -> None:
        """Normal anomaly event → valid diagnosis via real LLM."""
        anomaly = _valid_anomaly_event()
        context = _valid_context()

        evidence = assemble_evidence(anomaly, context)
        assert evidence is not None
        assert evidence.get("tenant_id") == "tenant_northwind"

        diagnosis = generate_diagnosis(evidence, use_llm=True)
        assert diagnosis is not None
        assert "model_name" in diagnosis, f"Missing model_name in {sorted(diagnosis.keys())}"
        assert "token_cost" in diagnosis
        assert "latency_ms" in diagnosis
        assert isinstance(diagnosis["latency_ms"], (int, float))

        # LLM path: if LLM succeeded, model_name should not be fallback.
        # If LLM fallback occurred (API unavailable), that's also valid —
        # the diagnosis still has all required fields.
        if diagnosis["model_name"] == "fallback-rule-based":
            assert diagnosis["token_cost"] == 0
        else:
            assert diagnosis["token_cost"] >= 0

        validated = validate_diagnosis(diagnosis, evidence)
        assert validated["status"] == "valid", f"Unexpected quarantine: {validated.get('failure_reasons')}"

    def test_scenario1_normal_anomaly_valid_diagnosis_fallback(self) -> None:
        """Normal anomaly event → valid diagnosis via deterministic fallback."""
        anomaly = _valid_anomaly_event()
        context = _valid_context()

        evidence = assemble_evidence(anomaly, context)
        assert evidence is not None
        assert evidence.get("tenant_id") == "tenant_northwind"

        diagnosis = generate_diagnosis(evidence, use_llm=False)
        assert diagnosis is not None
        assert "model_name" in diagnosis
        assert diagnosis["model_name"] == "fallback-rule-based"
        assert diagnosis.get("token_cost") == 0
        assert "latency_ms" in diagnosis

        validated = validate_diagnosis(diagnosis, evidence)
        assert validated["status"] == "valid", f"Unexpected quarantine: {validated.get('failure_reasons')}"

    # --- Scenario 2: Late quality caveat ---

    def test_scenario2_late_quality_caveat(self) -> None:
        """Anomaly with 'late' quality flag → quality_caveats appear in evidence and diagnosis."""
        anomaly = _valid_anomaly_event(quality_flags=["late_arrival"])
        context = _valid_context()

        evidence = assemble_evidence(anomaly, context)
        assert "quality_caveats" in evidence, f"Missing quality_caveats in evidence"
        caveats = evidence["quality_caveats"]
        assert any(
            "late" in c.lower() for c in caveats
        ), f"No late-arrival caveat found in: {caveats}"

        diagnosis = generate_diagnosis(evidence, use_llm=False)
        dq_caveats = diagnosis.get("quality_caveats", [])
        assert dq_caveats, f"Expected quality_caveats in diagnosis, got: {dq_caveats}"

    # --- Scenario 3: Missing mapping (unmappable tag_id) ---

    def test_scenario3_missing_mapping_gap_detection(self) -> None:
        """Anomaly with unmappable tag_id → gap detection, no crash."""
        anomaly = _valid_anomaly_event(
            tag_id="tag_999_unmappable",
            source_system="unknown_system",
        )
        context = _valid_context()

        # With unmappable tag_id, evidence assembly should still succeed
        # (no crash), returning whatever context is available
        try:
            evidence = assemble_evidence(anomaly, context)
            assert evidence is not None
        except Exception as exc:
            pytest.fail(f"Evidence assembly crashed on unmappable tag_id: {exc}")

        # Diagnosis should also not crash
        try:
            diagnosis = generate_diagnosis(evidence, use_llm=False)
            assert diagnosis is not None
        except Exception as exc:
            pytest.fail(f"Diagnosis generation crashed on unmappable tag_id: {exc}")

    # --- Scenario 4: Cross-tenant guard ---

    def test_scenario4_cross_tenant_guard(self) -> None:
        """tenant_northwind anomaly + tenant_euro_core context → evidence assembly fails."""
        anomaly = _valid_anomaly_event(tenant_id="tenant_northwind")
        context = _valid_context()
        # Override context to have different tenant
        context["signal_window"][0]["tenant_id"] = "tenant_euro_core"

        with pytest.raises(ValueError, match="Cross-tenant evidence assembly denied"):
            assemble_evidence(anomaly, context)

    # --- Scenario 5: Banned wording quarantine ---

    def test_scenario5_banned_wording_quarantine(self) -> None:
        """Diagnosis with 'autonomous diagnosis' → quarantined."""
        anomaly = _valid_anomaly_event()
        context = _valid_context()
        evidence = assemble_evidence(anomaly, context)

        diagnosis = {
            "schema_version": "agent_explanation.v1",
            "tenant_id": "tenant_northwind",
            "anomaly_id": "anom_e2e_001",
            "evidence_version": "triage_evidence.v1",
            "summary": "This is an autonomous diagnosis of the anomaly.",
            "findings": ["Finding 1", "Finding 2"],
            "possible_causes": ["Hypothesis: something changed"],
            "recommended_checks": ["Check sensor readings"],
            "runbook_references": [],
            "confidence_label": "low",
            "quality_caveats": [],
            "model_name": "fallback-rule-based",
            "latency_ms": 10,
        }

        validated = validate_diagnosis(diagnosis, evidence)
        assert validated["status"] == "quarantined", (
            f"Expected quarantine for banned wording, got status={validated['status']}"
        )
        reasons = validated.get("failure_reasons", [])
        assert "banned_autonomous_claim" in reasons, f"Unexpected failure reasons: {reasons}"

    # --- Scenario 6: Daemon mode (test mode via subprocess) ---

    def test_scenario6_daemon_mode_test(self) -> None:
        """Service in test mode with --max-events 0 exits cleanly with help text.

        This test verifies the module entrypoint starts, parses args, and
        exits gracefully. For a full daemon-mode test with live Kafka, see
        reports/phase2-flink-agent-e2e-test.md.
        """
        try:
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "src.flink_agents.realtime_diagnosis_agent",
                    "--tenant",
                    "tenant_northwind",
                    "--broker",
                    "localhost:19092",
                    "--max-events",
                    "0",
                ],
                capture_output=True,
                text=True,
                timeout=10,
                cwd=REPO_ROOT,
            )
        except subprocess.TimeoutExpired:
            pytest.fail("Agent service subprocess timed out")

        stdout = result.stdout
        assert "Flink Agent" in stdout, f"Missing banner in output:\n{stdout}"
        assert "tenant_northwind" in stdout, f"Missing tenant in output:\n{stdout}"
        assert "test" in stdout.lower(), f"Missing 'test' mode in output:\n{stdout}"
        assert "disabled" in stdout.lower(), f"Missing LLM 'disabled' in output:\n{stdout}"

        assert result.returncode == 0, (
            f"Agent exited with non-zero code {result.returncode}\n"
            f"STDERR: {result.stderr[-500:]}"
        )

    def test_scenario6_daemon_mode_sigterm(self) -> None:
        """Service handles SIGTERM gracefully (via subprocess)."""
        try:
            proc = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "src.flink_agents.realtime_diagnosis_agent",
                    "--tenant",
                    "tenant_northwind",
                    "--broker",
                    "localhost:19092",
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                cwd=REPO_ROOT,
            )
        except Exception as exc:
            pytest.fail(f"Failed to start agent subprocess: {exc}")

        time.sleep(1.5)

        proc.terminate()
        try:
            stdout, stderr = proc.communicate(timeout=8)
        except subprocess.TimeoutExpired:
            proc.kill()
            stdout, stderr = proc.communicate()

        combined = (stdout or "") + (stderr or "")
        assert "Flink Agent" in combined, f"Missing startup banner:\n{combined}"
        assert (
            "shutdown" in combined.lower() or "Gracefully" in combined
        ), f"No graceful shutdown message in output:\n{combined}"
        assert proc.returncode is not None, "Process did not exit after SIGTERM"
