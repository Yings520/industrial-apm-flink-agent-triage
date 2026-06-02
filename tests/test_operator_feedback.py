import pytest

from src.alerts.feedback import feedback_row


def _valid_feedback(**overrides: object) -> dict[str, object]:
    row = {
        "feedback_id": "fb_001",
        "tenant_id": "tenant_northwind",
        "incident_id": "inc_abc123",
        "anomaly_id": "anom_001",
        "operator_id": "op_7",
        "is_true_positive": True,
        "severity_correction": "warning",
        "explanation_usefulness": 4,
        "resolution_note": "Pump seal replaced",
        "created_at": "2026-01-01T02:00:00Z",
    }
    for key, value in overrides.items():
        row[key] = value
    return row


def test_valid_feedback_row_contains_incident_id() -> None:
    row = feedback_row(_valid_feedback())
    assert row["incident_id"] == "inc_abc123"


def test_missing_incident_id_raises() -> None:
    payload = _valid_feedback()
    del payload["incident_id"]
    with pytest.raises(ValueError, match="incident_id"):
        feedback_row(payload)


def test_missing_feedback_id_raises() -> None:
    payload = _valid_feedback()
    del payload["feedback_id"]
    with pytest.raises(ValueError, match="feedback_id"):
        feedback_row(payload)


def test_valid_severity_correction_values() -> None:
    for value in ("none", "info", "warning", "critical"):
        row = feedback_row(_valid_feedback(severity_correction=value))
        assert row["severity_correction"] == value


def test_invalid_severity_correction_raises() -> None:
    with pytest.raises(ValueError, match="severity_correction"):
        feedback_row(_valid_feedback(severity_correction="bad_value"))


def test_is_true_positive_is_boolean() -> None:
    row = feedback_row(_valid_feedback(is_true_positive=False))
    assert row["is_true_positive"] is False


def test_feedback_row_preserves_all_keys() -> None:
    row = feedback_row(_valid_feedback())
    for key in (
        "feedback_id",
        "tenant_id",
        "incident_id",
        "anomaly_id",
        "operator_id",
        "is_true_positive",
        "severity_correction",
        "explanation_usefulness",
        "resolution_note",
        "created_at",
    ):
        assert key in row
