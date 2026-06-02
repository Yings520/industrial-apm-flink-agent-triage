from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

USE_CLICKHOUSE = os.environ.get("USE_CLICKHOUSE", "").strip().lower() in ("1", "true", "yes")

if USE_CLICKHOUSE:
    from scripts._clickhouse_client import clickhouse_query_rows as _ch_query_rows
    from scripts._clickhouse_client import clickhouse_execute_sql as _ch_execute_sql

_SOURCE_TOPIC = "clickhouse.dwd_apm_anomaly_events" if USE_CLICKHOUSE else "doris.dwd_apm_anomaly_events"

from src.alerts.feedback import feedback_row
from src.alerts.incident_workflow import group_anomalies
from src.alerts.teams import send_teams_card, teams_card_payload
from src.serving.triage_evidence import build_triage_evidence
from src.triage_service.invocations import invocation_row
from src.triage_service.openai_provider import generate_llm_recommendation
from src.triage_service.recommendations import generate_fallback_recommendation
from src.triage_service.validation import validate_recommendation


def main() -> int:
    parser = argparse.ArgumentParser(description="Load Phase 4 incidents, routing, feedback and triage.")
    parser.add_argument("--limit", type=int, default=10)
    args = parser.parse_args()

    anomalies = _query_rows(
        f"""
        SELECT tenant_id, anomaly_id, asset_id, tag_id, metric_name,
               {_cast_text("window_start")}, {_cast_text("window_end")},
               rule_id, severity, observed_value, baseline_value, expected_value,
               quality_flags, source_event_ids, plant_id
        FROM dwd_apm_anomaly_events
        ORDER BY window_start DESC, anomaly_id
        LIMIT {args.limit}
        """,
        (
            "tenant_id", "anomaly_id", "asset_id", "tag_id", "metric_name",
            "window_start", "window_end", "rule_id", "severity",
            "observed_value", "baseline_value", "expected_value",
            "quality_flags", "source_event_ids", "plant_id",
        ),
    )
    if not anomalies:
        raise SystemExit("No anomaly rows found in serving store; run Flink and serving ingest first.")

    for anomaly in anomalies:
        anomaly["quality_flags"] = _parse_jsonish(anomaly.get("quality_flags"))
        anomaly["source_event_ids"] = _parse_jsonish(anomaly.get("source_event_ids"))

    incidents = group_anomalies(anomalies)
    sql_statements: list[str] = []
    now = _now()

    teams_webhook = os.environ.get("TEAMS_WEBHOOK_URL", "").strip()
    evidence_by_anomaly: dict[str, dict[str, Any]] = {}
    recommendation_by_anomaly: dict[str, dict[str, Any]] = {}

    for anomaly in anomalies:
        anomaly_id = str(anomaly["anomaly_id"])
        sensor_window = _sensor_window(str(anomaly["tenant_id"]), str(anomaly["tag_id"]))
        stream_health = _stream_health(str(anomaly["tenant_id"]))

        evidence = build_triage_evidence(
            tenant_id=str(anomaly["tenant_id"]),
            anomaly=anomaly,
            sensor_window=sensor_window,
            stream_health=stream_health,
            runbook_refs=[
                {"runbook_id": "generic_asset_triage", "section": str(anomaly["metric_name"] or "asset")},
            ],
        )
        evidence_by_anomaly[anomaly_id] = evidence

        sql_statements.append(_insert_evidence(evidence, now))

        llm_result = generate_llm_recommendation(evidence)
        recommendation: dict[str, Any]

        if llm_result["recommendation"] is not None and llm_result["parsed_status"] == "parsed_json":
            raw_rec = llm_result["recommendation"]
            raw_rec["recommendation_id"] = _recommendation_id(
                evidence["tenant_id"], anomaly_id, evidence["evidence_version"]
            )
            validation = validate_recommendation(raw_rec, evidence)
        else:
            validation = None

        if validation is not None and validation.valid:
            recommendation = raw_rec
            inv_status = "valid"
            quarantine_reason = ""
        else:
            recommendation = generate_fallback_recommendation(evidence)
            inv_status = "quarantined" if validation and not validation.valid else "fallback"
            quarantine_reason = validation.quarantine_reason if validation and not validation.valid else (
                llm_result["parsed_status"] if llm_result["parsed_status"] != "parsed_json" else ""
            )

        recommendation_by_anomaly[anomaly_id] = recommendation

        inv = invocation_row(
            tenant_id=str(anomaly["tenant_id"]),
            incident_id="pending",
            anomaly_id=anomaly_id,
            evidence_id=evidence["evidence_id"],
            provider=llm_result.get("provider", "openai"),
            model_name=llm_result.get("model_name", "fallback"),
            prompt_version="triage_prompt.v1",
            raw_response=llm_result.get("raw_response", ""),
            parsed_status=llm_result.get("parsed_status", "fallback"),
            validation_status=inv_status,
            quarantine_reason=quarantine_reason,
            token_cost=llm_result.get("token_cost", 0),
            latency_ms=llm_result.get("latency_ms", 0),
        )
        sql_statements.append(_insert_invocation(inv))

        rec_row = _recommendation_row(recommendation, inv_status, quarantine_reason)
        sql_statements.append(_insert_recommendation(rec_row))

    for incident in incidents:
        incident_id = incident["incident_id"]
        anomaly_ids = incident.get("source_anomaly_ids", [])
        primary_anomaly_id = anomaly_ids[0] if anomaly_ids else ""
        evidence = evidence_by_anomaly.get(primary_anomaly_id, {})
        recommendation = recommendation_by_anomaly.get(primary_anomaly_id)

        sql_statements.append(_insert_incident(incident, now))

        if incident.get("routing_channel") == "teams_card":
            payload = teams_card_payload(incident, recommendation, evidence)
            teams_result = send_teams_card(payload, webhook_url=teams_webhook or None)
            routing_status = teams_result["routing_status"]
            routing_error = teams_result.get("error")
        else:
            routing_status = "simulated"
            routing_error = None

        sql_statements.append(_insert_routing(incident, now, routing_status, routing_error))

    demo_feedback = {
        "feedback_id": "fb_phase4_demo_001",
        "tenant_id": str(incidents[0]["tenant_id"]) if incidents else "tenant_northwind",
        "incident_id": str(incidents[0]["incident_id"]) if incidents else "inc_demo",
        "anomaly_id": str(incidents[0]["source_anomaly_ids"][0]) if incidents else "anom_demo",
        "operator_id": "operator_demo",
        "is_true_positive": True,
        "severity_correction": "none",
        "explanation_usefulness": 4,
        "resolution_note": "Phase 4 demo feedback row for local E2E validation.",
        "created_at": now,
    }
    row = feedback_row(demo_feedback)
    sql_statements.append(_insert_feedback(row))

    _execute_sql("\n".join(sql_statements))
    print(
        f"loaded {len(incidents)} incidents, {len(incidents)} routing rows, "
        f"{len(anomalies)} recommendations, {len(anomalies)} invocation audits, "
        f"1 demo feedback row"
    )
    return 0


def _query_rows(sql: str, columns: tuple[str, ...]) -> list[dict[str, Any]]:
    result = subprocess.run(
        [
            "docker", "exec", "industrial-apm-flink-agent-triage-doris-1", "mysql",
            "-N", "-B", "-uroot", "-h127.0.0.1", "-P9030", "industrial_apm",
            "-e", " ".join(line.strip() for line in sql.strip().splitlines()),
        ],
        check=True, capture_output=True, text=True,
    )
    rows: list[dict[str, Any]] = []
    for line in result.stdout.splitlines():
        if not line.strip():
            continue
        values = line.split("\t")
        rows.append(
            {column: _null_if_needed(values[index]) if index < len(values) else None
             for index, column in enumerate(columns)}
        )
    return rows


def _execute_sql(sql: str) -> None:
    _ = subprocess.run(
        ["docker", "exec", "-i", "industrial-apm-flink-agent-triage-doris-1", "mysql",
         "-uroot", "-h127.0.0.1", "-P9030", "industrial_apm"],
        input=sql, check=True, text=True,
    )


def _sensor_window(tenant_id: str, tag_id: str) -> list[dict[str, Any]]:
    rows = _query_rows(
        f"""
        SELECT tenant_id, event_id, tag_id, metric_name, {_cast_text("event_time")}, value, quality_flags
        FROM dwd_apm_realtime_signal_readings
        WHERE tenant_id = {_sql(tenant_id)} AND tag_id = {_sql(tag_id)}
        ORDER BY event_time DESC LIMIT 5
        """,
        ("tenant_id", "event_id", "tag_id", "metric_name", "event_time", "value", "quality_flags"),
    )
    for row in rows:
        row["quality_flags"] = _parse_jsonish(row.get("quality_flags"))
    return rows


def _stream_health(tenant_id: str) -> list[dict[str, Any]]:
    return _query_rows(
        f"""
        SELECT tenant_id, job_name, {_cast_text("observed_at")}, watermark_lag_seconds, late_event_count, checkpoint_status
        FROM dwd_apm_stream_health_snapshots
        WHERE tenant_id = {_sql(tenant_id)}
        ORDER BY observed_at DESC LIMIT 1
        """,
        ("tenant_id", "job_name", "observed_at", "watermark_lag_seconds", "late_event_count", "checkpoint_status"),
    )


def _insert_evidence(evidence: dict[str, Any], created_at: str) -> str:
    from src.serving.triage_evidence import evidence_json
    return (
        "INSERT INTO serving_apm_triage_evidence "
        "(evidence_id, evidence_version, tenant_id, anomaly_id, asset_id, tag_id, metric_name, "
        "window_start, window_end, evidence_json, created_at, source_topic) VALUES "
        f"({_sql(evidence['evidence_id'])}, {_sql(evidence['evidence_version'])}, {_sql(evidence['tenant_id'])}, "
        f"{_sql(evidence['anomaly_id'])}, {_sql(evidence.get('asset_id'))}, {_sql(evidence.get('tag_id'))}, "
        f"{_sql(evidence.get('metric_name'))}, {_datetime_sql(evidence.get('window_start'))}, {_datetime_sql(evidence.get('window_end'))}, "
        f"{_sql(evidence_json(evidence))}, {_datetime_sql(created_at)}, '{_SOURCE_TOPIC}');"
    )


def _insert_incident(incident: dict[str, Any], created_at: str) -> str:
    columns = (
        "incident_id", "correlation_key", "tenant_id", "asset_id", "metric_name", "rule_id",
        "first_seen_at", "last_seen_at", "anomaly_count", "max_severity",
        "source_anomaly_ids", "status", "routing_channel",
    )
    values = [
        _sql(incident["incident_id"]), _sql(incident["correlation_key"]),
        _sql(incident["tenant_id"]), _sql(incident.get("asset_id")),
        _sql(incident.get("metric_name")), _sql(incident.get("rule_id")),
        _datetime_sql(incident["first_seen_at"]), _datetime_sql(incident["last_seen_at"]),
        str(incident.get("anomaly_count", 1)), _sql(incident.get("max_severity")),
        _sql(",".join(incident.get("source_anomaly_ids", []))),
        _sql(incident.get("status")), _sql(incident.get("routing_channel")),
    ]
    return f"INSERT INTO fact_apm_incidents ({', '.join(columns)}) VALUES ({', '.join(values)});"


def _insert_routing(
    incident: dict[str, Any], created_at: str, routing_status: str, error_message: str | None
) -> str:
    columns = (
        "route_id", "incident_id", "tenant_id", "severity", "routing_channel",
        "routing_status", "notification_target", "error_message", "created_at",
    )
    route_id = f"route_{incident['incident_id']}"
    values = [
        _sql(route_id), _sql(incident["incident_id"]), _sql(incident["tenant_id"]),
        _sql(incident.get("max_severity")), _sql(incident.get("routing_channel")),
        _sql(routing_status), _sql(incident.get("routing_channel")),
        _sql(error_message), _datetime_sql(created_at),
    ]
    return f"INSERT INTO fact_apm_alert_routing ({', '.join(columns)}) VALUES ({', '.join(values)});"


def _insert_feedback(row: dict[str, Any]) -> str:
    columns = (
        "feedback_id", "tenant_id", "incident_id", "anomaly_id", "operator_id",
        "is_true_positive", "severity_correction", "explanation_usefulness",
        "resolution_note", "created_at",
    )
    _bool_true = "1" if USE_CLICKHOUSE else "TRUE"
    _bool_false = "0" if USE_CLICKHOUSE else "FALSE"
    values = [
        _sql(row["feedback_id"]), _sql(row["tenant_id"]), _sql(row["incident_id"]),
        _sql(row["anomaly_id"]), _sql(row["operator_id"]),
        _bool_true if row["is_true_positive"] else _bool_false,
        _sql(row["severity_correction"]), str(row["explanation_usefulness"]),
        _sql(row["resolution_note"]), _datetime_sql(row["created_at"]),
    ]
    return f"INSERT INTO fact_apm_operator_feedback ({', '.join(columns)}) VALUES ({', '.join(values)});"


def _insert_invocation(row: dict[str, Any]) -> str:
    columns = (
        "invocation_id", "tenant_id", "incident_id", "anomaly_id", "evidence_id",
        "provider", "model_name", "prompt_version", "request_hash",
        "raw_response", "parsed_status", "validation_status", "quarantine_reason",
        "token_cost", "latency_ms", "created_at",
    )
    values = [
        _sql(row["invocation_id"]), _sql(row["tenant_id"]), _sql(row["incident_id"]),
        _sql(row["anomaly_id"]), _sql(row["evidence_id"]), _sql(row["provider"]),
        _sql(row["model_name"]), _sql(row["prompt_version"]), _sql(row["request_hash"]),
        _sql(row["raw_response"]), _sql(row["parsed_status"]),
        _sql(row["validation_status"]), _sql(row["quarantine_reason"]),
        str(row["token_cost"]), str(row["latency_ms"]), _datetime_sql(row["created_at"]),
    ]
    return f"INSERT INTO fact_apm_llm_invocations ({', '.join(columns)}) VALUES ({', '.join(values)});"


def _insert_recommendation(row: dict[str, Any]) -> str:
    columns = (
        "recommendation_id", "tenant_id", "anomaly_id", "evidence_version",
        "summary", "findings", "possible_causes", "recommended_checks",
        "runbook_references", "confidence_label", "quality_caveats",
        "model_name", "prompt_version", "token_cost", "latency_ms",
        "status", "validation_status", "quarantine_reason", "created_at",
    )
    values = [
        _sql(row["recommendation_id"]), _sql(row["tenant_id"]),
        _sql(row["anomaly_id"]), _sql(row["evidence_version"]),
        _sql(row["summary"]), _sql(row["findings"]), _sql(row["possible_causes"]),
        _sql(row["recommended_checks"]), _sql(row["runbook_references"]),
        _sql(row["confidence_label"]), _sql(row["quality_caveats"]),
        _sql(row["model_name"]), _sql(row["prompt_version"]),
        str(row["token_cost"]), str(row["latency_ms"]),
        _sql(row["status"]), _sql(row["validation_status"]),
        _sql(row["quarantine_reason"]), _datetime_sql(row["created_at"]),
    ]
    return f"INSERT INTO fact_apm_agent_recommendations ({', '.join(columns)}) VALUES ({', '.join(values)});"


def _parse_jsonish(value: Any) -> list[Any]:
    if value in (None, "NULL", ""):
        return []
    if isinstance(value, list):
        return value
    text = str(value)
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return [text]
    return parsed if isinstance(parsed, list) else [parsed]


def _null_if_needed(value: str) -> Any:
    return None if value == "NULL" else value


def _sql(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value).replace("\\", "\\\\").replace("'", "''")
    return f"'{text}'"


def _now() -> str:
    return _normalize_datetime(datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"))


def _datetime_sql(value: Any) -> str:
    if value is None:
        return "NULL"
    return _sql(_normalize_datetime(str(value)))


def _normalize_datetime(value: str) -> str:
    return value.replace("T", " ").removesuffix("Z").removesuffix("+00:00")


def _cast_text(column: str) -> str:
    return f"toString({column})" if USE_CLICKHOUSE else f"CAST({column} AS CHAR)"


if USE_CLICKHOUSE:
    _query_rows = _ch_query_rows
    _execute_sql = _ch_execute_sql


def _recommendation_row(
    recommendation: dict[str, Any],
    validation_status: str,
    quarantine_reason: str,
) -> dict[str, Any]:
    import json as _json
    return {
        "recommendation_id": recommendation["recommendation_id"],
        "tenant_id": recommendation["tenant_id"],
        "anomaly_id": recommendation["anomaly_id"],
        "evidence_version": recommendation["evidence_version"],
        "summary": recommendation["summary"],
        "findings": _json.dumps(recommendation.get("findings", []), sort_keys=True),
        "possible_causes": _json.dumps(recommendation.get("possible_causes", []), sort_keys=True),
        "recommended_checks": _json.dumps(recommendation.get("recommended_checks", []), sort_keys=True),
        "runbook_references": _json.dumps(recommendation.get("runbook_references", []), sort_keys=True),
        "confidence_label": recommendation["confidence_label"],
        "quality_caveats": _json.dumps(recommendation.get("quality_caveats", []), sort_keys=True),
        "model_name": recommendation["model_name"],
        "prompt_version": recommendation["prompt_version"],
        "token_cost": recommendation["token_cost"],
        "latency_ms": recommendation["latency_ms"],
        "status": recommendation["status"],
        "validation_status": validation_status,
        "quarantine_reason": quarantine_reason,
        "created_at": recommendation["created_at"],
    }


def _recommendation_id(tenant_id: str, anomaly_id: str, evidence_version: str) -> str:
    import hashlib
    digest = hashlib.sha1(f"{tenant_id}|{anomaly_id}|{evidence_version}".encode("utf-8")).hexdigest()[:16]
    return f"rec_{digest}"


if __name__ == "__main__":
    raise SystemExit(main())
