from __future__ import annotations

import subprocess
from typing import Any

_CLICKHOUSE_CONTAINER = "industrial-apm-flink-agent-triage-clickhouse-1"


def clickhouse_query_rows(sql: str, columns: tuple[str, ...]) -> list[dict[str, Any]]:
    result = subprocess.run(
        [
            "docker",
            "exec",
            _CLICKHOUSE_CONTAINER,
            "clickhouse-client",
            "--host",
            "127.0.0.1",
            "--port",
            "9000",
            "--database",
            "industrial_apm",
            "--user",
            "default",
            "--password",
            "clickhouse_secret",
            "--format",
            "TabSeparatedWithNames",
            "--query",
            " ".join(line.strip() for line in sql.strip().splitlines()),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    rows: list[dict[str, Any]] = []
    lines = result.stdout.splitlines()
    if len(lines) < 2:
        return rows
    for line in lines[1:]:
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


def clickhouse_execute_sql(sql: str) -> None:
    _ = subprocess.run(
        [
            "docker",
            "exec",
            "-i",
            _CLICKHOUSE_CONTAINER,
            "clickhouse-client",
            "--host",
            "127.0.0.1",
            "--port",
            "9000",
            "--database",
            "industrial_apm",
            "--user",
            "default",
            "--password",
            "clickhouse_secret",
        ],
        input=sql,
        check=True,
        text=True,
    )


def _null_if_needed(value: str) -> Any:
    if value in ("NULL", "\\N"):
        return None
    return value
