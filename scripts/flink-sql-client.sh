#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 sql/<job>.sql" >&2
  exit 2
fi

sql_file="$1"
if [[ ! -f "$sql_file" ]]; then
  echo "SQL file not found: $sql_file" >&2
  exit 2
fi

connector_version="3.3.0-1.19"
connector_name="flink-sql-connector-kafka-${connector_version}.jar"
connector_dir="flink/lib"
connector_path="${connector_dir}/${connector_name}"
connector_url="https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${connector_version}/${connector_name}"

mkdir -p "$connector_dir"
if [[ ! -f "$connector_path" ]]; then
  curl -fsSL "$connector_url" -o "$connector_path"
fi

docker compose exec -T flink-jobmanager \
  /opt/flink/bin/sql-client.sh \
  -l /opt/flink/usrlib \
  -f "/opt/flink/sql/$(basename "$sql_file")"
