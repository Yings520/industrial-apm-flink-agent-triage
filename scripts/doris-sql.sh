#!/usr/bin/env bash
set -euo pipefail

DORIS_SERVICE="${DORIS_SERVICE:-doris}"
DORIS_HOST="${DORIS_HOST:-127.0.0.1}"
DORIS_PORT="${DORIS_PORT:-9030}"
DORIS_USER="${DORIS_USER:-root}"

MYSQL_ARGS=()
while [[ "${1:-}" == -* && "${1:-}" != "-e" ]]; do
  MYSQL_ARGS+=("$1")
  shift
done

if [[ "${1:-}" == "-e" ]]; then
  shift
  if [[ ${#MYSQL_ARGS[@]} -gt 0 ]]; then
    exec docker compose exec -T "${DORIS_SERVICE}" mysql "${MYSQL_ARGS[@]}" -u"${DORIS_USER}" -h"${DORIS_HOST}" -P"${DORIS_PORT}" -e "$*"
  fi
  exec docker compose exec -T "${DORIS_SERVICE}" mysql -u"${DORIS_USER}" -h"${DORIS_HOST}" -P"${DORIS_PORT}" -e "$*"
fi

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 path/to/query.sql | -e 'SQL'" >&2
  exit 2
fi

if [[ ${#MYSQL_ARGS[@]} -gt 0 ]]; then
  exec docker compose exec -T "${DORIS_SERVICE}" mysql "${MYSQL_ARGS[@]}" -u"${DORIS_USER}" -h"${DORIS_HOST}" -P"${DORIS_PORT}" < "$1"
fi
exec docker compose exec -T "${DORIS_SERVICE}" mysql -u"${DORIS_USER}" -h"${DORIS_HOST}" -P"${DORIS_PORT}" < "$1"
