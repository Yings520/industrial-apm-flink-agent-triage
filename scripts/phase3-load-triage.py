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
    from scripts._clickhouse_client import clickhouse_execute_sql as _ch_execute_sql
    from scripts._clickhouse_client import clickhouse_query_rows as _ch_query_rows

_SOURCE_TOPIC = "clickhouse.dwd_apm_anomaly_events" if USE_CLICKHOUSE else "doris.dwd_apm_anomaly_events"

from src.serving.quality_events import event_from_late_record, events_from_quality_flags
from src.serving.triage_evidence import build_triage_evidence, evidence_json
from src.triage_service.persist import recommendation_row
from src.triage_service.recommendations import generate_fallback_recommendation
from src.triage_service.validation import validate_recommendation


def main() -> int:
    parser = argparse.ArgumentParser(description="Load Phase 3 triage evidence and fallback recommendations.")
    parser.add_argument("--limit", type=int, default=5)
    args = parser.parse_args()

    anomalies = _query_rows(
        f"""
        SELECT tenant_id, anomaly_id, asset_id, tag_id, metric_name,
               {_cast_text("window_start")}, {_cast_text("window_end")},
               observed_value, baseline_value, expected_value,
               quality_flags, source_event_ids, plant_id, rule_id, severity
        FROM dwd_apm_anomaly_events
        ORDER BY window_start DESC, anomaly_id
        LIMIT {args.limit}
        """,
        (
            "tenant_id",
            "anomaly_id",
            "asset_id",
            "tag_id",
            "metric_name",
            "window_start",
            "window_end",
            "observed_value",
            "baseline_value",
            "expected_value",
            "quality_flags",
            "source_event_ids",
            "plant_id",
            "rule_id",
            "severity",
        ),
    )
    if not anomalies:
        raise SystemExit("No anomaly rows found in serving store; run Flink and serving ingest first.")

    sql_statements: list[str] = []
    all_quality_events: dict[str, dict[str, Any]] = {}
    evidence_count = 0
    recommendation_count = 0

    for anomaly in anomalies:
        quality_events_by_id: dict[str, dict[str, Any]] = {}
        anomaly["quality_flags"] = _parse_jsonish(anomaly.get("quality_flags"))
        anomaly["source_event_ids"] = _parse_jsonish(anomaly.get("source_event_ids"))
        sensor_window = _sensor_window(str(anomaly["tenant_id"]), str(anomaly["tag_id"]))
        late_records = _late_records(str(anomaly["tenant_id"]), str(anomaly["tag_id"]))
        stream_health = _stream_health(str(anomaly["tenant_id"]))

        for event in events_from_quality_flags(anomaly):
            quality_events_by_id[str(event["quality_event_id"])] = event
        for late_record in late_records:
            quality_events_by_id[str(late_record["quality_event_id"])] = event_from_late_record(late_record)

        all_quality_events.update(quality_events_by_id)

        evidence = build_triage_evidence(
            tenant_id=str(anomaly["tenant_id"]),
            anomaly=anomaly,
            sensor_window=sensor_window,
            quality_events=list(quality_events_by_id.values()),
            stream_health=stream_health,
            runbook_refs=[{"runbook_id": "generic_asset_triage", "section": str(anomaly["metric_name"] or "asset")}],
        )
        recommendation = generate_fallback_recommendation(evidence)
        validation = validate_recommendation(recommendation, evidence)
        row = recommendation_row(recommendation, validation)
        created_at = _now()

        sql_statements.append(_insert_evidence(evidence, created_at))
        sql_statements.append(_insert_recommendation(row))
        evidence_count += 1
        recommendation_count += 1

    for quality_event in all_quality_events.values():
        sql_statements.append(_insert_quality_event(quality_event))

    _execute_sql("\n".join(sql_statements))
    print(
        "loaded "
        f"{evidence_count} evidence rows, "
        f"{recommendation_count} recommendation rows, "
        f"{len(all_quality_events)} quality event rows"
    )
    return 0


def _query_rows(sql: str, columns: tuple[str, ...]) -> list[dict[str, Any]]:
    result = subprocess.run(
        [
            "docker",
            "exec",
            "industrial-apm-flink-agent-triage-doris-1",
            "mysql",
            "-N",
            "-B",
            "-uroot",
            "-h127.0.0.1",
            "-P9030",
            "industrial_apm",
            "-e",
            " ".join(line.strip() for line in sql.strip().splitlines()),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    rows: list[dict[str, Any]] = []
    for line in result.stdout.splitlines():
        if not line.strip():
            continue
        values = line.split("\t")
        rows.append(
            {
                column: _null_if_needed(values[index]) if index < len(values) else None
                for index, column in enumerate(columns)
            }
        )
    return rows


def _execute_sql(sql: str) -> None:
    _ = subprocess.run(
        [
            "docker",
            "exec",
            "-i",
            "industrial-apm-flink-agent-triage-doris-1",
            "mysql",
            "-uroot",
            "-h127.0.0.1",
            "-P9030",
            "industrial_apm",
        ],
        input=sql,
        check=True,
        text=True,
    )


def _sensor_window(tenant_id: str, tag_id: str) -> list[dict[str, Any]]:
    rows = _query_rows(
        f"""
        SELECT tenant_id, event_id, tag_id, metric_name, {_cast_text("event_time")}, value, quality_flags
        FROM dwd_apm_realtime_signal_readings
        WHERE tenant_id = {_sql(tenant_id)} AND tag_id = {_sql(tag_id)}
        ORDER BY event_time DESC
        LIMIT 5
        """,
        ("tenant_id", "event_id", "tag_id", "metric_name", "event_time", "value", "quality_flags"),
    )
    for row in rows:
        row["quality_flags"] = _parse_jsonish(row.get("quality_flags"))
    return rows


def _late_records(tenant_id: str, tag_id: str) -> list[dict[str, Any]]:
    rows = _query_rows(
        f"""
        SELECT tenant_id, event_id, plant_id, asset_id, tag_id, metric_name,
               {_cast_text("event_time")}, {_cast_text("watermark_time")}, lateness_minutes, reason
        FROM dwd_apm_late_sensor_readings
        WHERE tenant_id = {_sql(tenant_id)} AND tag_id = {_sql(tag_id)}
        ORDER BY event_time DESC
        LIMIT 5
        """,
        (
            "tenant_id",
            "event_id",
            "plant_id",
            "asset_id",
            "tag_id",
            "metric_name",
            "event_time",
            "watermark_time",
            "lateness_minutes",
            "reason",
        ),
    )
    return rows


def _stream_health(tenant_id: str) -> list[dict[str, Any]]:
    return _query_rows(
        f"""
        SELECT tenant_id, job_name, {_cast_text("observed_at")}, watermark_lag_seconds, late_event_count, checkpoint_status
        FROM dwd_apm_stream_health_snapshots
        WHERE tenant_id = {_sql(tenant_id)}
        ORDER BY observed_at DESC
        LIMIT 1
        """,
        ("tenant_id", "job_name", "observed_at", "watermark_lag_seconds", "late_event_count", "checkpoint_status"),
    )


def _insert_evidence(evidence: dict[str, Any], created_at: str) -> str:
    return (
        "INSERT INTO serving_apm_triage_evidence "
        "(evidence_id, evidence_version, tenant_id, anomaly_id, asset_id, tag_id, metric_name, "
        "window_start, window_end, evidence_json, created_at, source_topic) VALUES "
        f"({_sql(evidence['evidence_id'])}, {_sql(evidence['evidence_version'])}, {_sql(evidence['tenant_id'])}, "
        f"{_sql(evidence['anomaly_id'])}, {_sql(evidence.get('asset_id'))}, {_sql(evidence.get('tag_id'))}, "
        f"{_sql(evidence.get('metric_name'))}, {_datetime_sql(evidence.get('window_start'))}, {_datetime_sql(evidence.get('window_end'))}, "
        f"{_sql(evidence_json(evidence))}, {_datetime_sql(created_at)}, '{_SOURCE_TOPIC}');"
    )


def _insert_recommendation(row: dict[str, Any]) -> str:
    columns = (
        "recommendation_id",
        "tenant_id",
        "anomaly_id",
        "evidence_version",
        "summary",
        "findings",
        "possible_causes",
        "recommended_checks",
        "runbook_references",
        "confidence_label",
        "quality_caveats",
        "model_name",
        "prompt_version",
        "token_cost",
        "latency_ms",
        "status",
        "validation_status",
        "quarantine_reason",
        "created_at",
    )
    values = ", ".join(
        _datetime_sql(row.get(column)) if column == "created_at" else _sql(row.get(column))
        for column in columns
    )
    return f"INSERT INTO fact_apm_agent_recommendations ({', '.join(columns)}) VALUES ({values});"


def _insert_quality_event(row: dict[str, Any]) -> str:
    columns = (
        "quality_event_id",
        "tenant_id",
        "plant_id",
        "asset_id",
        "tag_id",
        "metric_name",
        "check_name",
        "check_status",
        "severity",
        "observed_value",
        "expected_value",
        "window_start",
        "window_end",
        "evidence_json",
        "created_at",
        "source_topic",
    )
    values = ", ".join(
        _datetime_sql(row.get(column)) if column in ("window_start", "window_end", "created_at") else _sql(row.get(column))
        for column in columns[:-1]
    )
    return f"INSERT INTO fact_apm_quality_events ({', '.join(columns)}) VALUES ({values}, 'phase3-triage-loader');"


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


if __name__ == "__main__":
    raise SystemExit(main())
