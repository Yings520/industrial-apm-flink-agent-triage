#!/usr/bin/env bash
set -euo pipefail

# Full local E2E launcher for manual testing:
# PostgreSQL -> Debezium -> Redpanda -> Flink SQL -> realtime agent -> ClickHouse/Grafana.
#
# Usage:
#   ./scripts/full-e2e.sh
#   make full-e2e
#
# Useful knobs:
#   TENANTS="tenant_northwind tenant_apac_ops tenant_euro_core" SERVING=1 ./scripts/full-e2e.sh
#   TENANTS=tenant_northwind SERVING=1 ./scripts/full-e2e.sh
#   SERVING=0 ./scripts/full-e2e.sh

DEFAULT_TENANTS=(tenant_northwind tenant_apac_ops tenant_euro_core)
if [[ -n "${TENANTS:-}" ]]; then
  read -r -a E2E_TENANTS <<< "${TENANTS}"
elif [[ -n "${TENANT:-}" ]]; then
  E2E_TENANTS=("${TENANT}")
else
  E2E_TENANTS=("${DEFAULT_TENANTS[@]}")
fi

PYTHON="${PYTHON:-./.venv/bin/python}"
FLINK_SQL="${FLINK_SQL:-./scripts/flink-sql-client.sh}"
BROKER="${BROKER:-localhost:19092}"
SERVING="${SERVING:-1}"
PUMP_WARMUP_SECONDS="${PUMP_WARMUP_SECONDS:-45}"
SERVING_WARMUP_SECONDS="${SERVING_WARMUP_SECONDS:-65}"
REPORT_DIR="${REPORT_DIR:-reports}"
LOG_FILE="${LOG_FILE:-${REPORT_DIR}/full-e2e.log}"
CLICKHOUSE_CONTAINER="${CLICKHOUSE_CONTAINER:-clickhouse}"

mkdir -p "${REPORT_DIR}" flink/lib
: > "${LOG_FILE}"

log() {
  printf '[full-e2e] %s\n' "$*" | tee -a "${LOG_FILE}"
}

run() {
  log "$*"
  "$@" 2>&1 | tee -a "${LOG_FILE}"
}

wait_for_http() {
  local name="$1"
  local url="$2"
  local attempts="${3:-30}"

  for attempt in $(seq 1 "${attempts}"); do
    if curl -fsS "${url}" >/dev/null 2>&1; then
      log "${name} is ready"
      return 0
    fi
    log "waiting for ${name} (${attempt}/${attempts})"
    sleep 5
  done

  log "${name} did not become ready: ${url}"
  return 1
}

wait_for_postgres() {
  for attempt in $(seq 1 30); do
    if docker compose exec -T postgres pg_isready -U apm -d apm_metadata >/dev/null 2>&1; then
      log "postgres is ready"
      return 0
    fi
    log "waiting for postgres (${attempt}/30)"
    sleep 3
  done

  log "postgres did not become ready"
  return 1
}

wait_for_redpanda() {
  for attempt in $(seq 1 30); do
    if docker compose exec -T redpanda rpk -X brokers=localhost:19092 cluster health >/dev/null 2>&1; then
      log "redpanda is ready"
      return 0
    fi
    log "waiting for redpanda (${attempt}/30)"
    sleep 3
  done

  log "redpanda did not become ready"
  return 1
}

clickhouse_client() {
  docker compose exec -T "${CLICKHOUSE_CONTAINER}" clickhouse-client \
    --host 127.0.0.1 \
    --port 9000 \
    --database industrial_apm \
    --user default \
    --password clickhouse_secret \
    "$@"
}

load_clickhouse_sql() {
  local sql_file="$1"
  log "loading ClickHouse SQL: ${sql_file}"
  clickhouse_client --multiquery < "${sql_file}" 2>&1 | tee -a "${LOG_FILE}"
}

load_clickhouse_tenant_ingest() {
  local tenant="$1"
  log "loading ClickHouse tenant Kafka ingest for ${tenant}"
  sed "s/\${tenant_id}/${tenant}/g" sql/clickhouse/templates/tenant_dwd_ingest.sql.tpl \
    | clickhouse_client --multiquery 2>&1 | tee -a "${LOG_FILE}"
}

wait_for_clickhouse() {
  for attempt in $(seq 1 30); do
    if clickhouse_client --query "SELECT 1" >/dev/null 2>&1; then
      log "ClickHouse SQL endpoint is ready"
      return 0
    fi
    log "waiting for ClickHouse (${attempt}/30)"
    sleep 5
  done

  log "ClickHouse did not become ready"
  return 1
}

topic_has_data() {
  local topic="$1"
  local sample_file="${REPORT_DIR}/topic-sample-${topic//[^A-Za-z0-9_.-]/_}.jsonl"
  docker compose exec -T redpanda rpk -X brokers=localhost:19092 topic consume "${topic}" --num 1 >"${sample_file}" 2>>"${LOG_FILE}" &
  local pid="$!"

  for _ in $(seq 1 30); do
    if ! kill -0 "${pid}" >/dev/null 2>&1; then
      wait "${pid}"
      return $?
    fi
    sleep 1
  done

  kill "${pid}" >/dev/null 2>&1 || true
  wait "${pid}" >/dev/null 2>&1 || true
  return 1
}

require_topic_data() {
  local topic="$1"
  log "checking topic has data: ${topic}"
  if topic_has_data "${topic}"; then
    log "OK: ${topic}"
    return 0
  fi

  log "FAIL: no sample consumed from ${topic}; check ${LOG_FILE}"
  return 1
}

topic_name() {
  local tenant="$1"
  local suffix="$2"
  printf '%s.%s.v1' "${tenant}" "${suffix}"
}

check_dependencies() {
  log "checking local commands and generated SQL"
  command -v docker >/dev/null
  command -v curl >/dev/null
  command -v make >/dev/null
  test -x "${PYTHON}"
  test -x "${FLINK_SQL}"
  test -f configs/debezium-pg-sensor-tags.json
  for tenant in "${E2E_TENANTS[@]}"; do
    test -f "sql/flink/generated/${tenant}/enrich.sql"
    test -f "sql/flink/generated/${tenant}/anomaly.sql"
    test -f "sql/flink/generated/${tenant}/late_events.sql"
    test -f "sql/flink/generated/${tenant}/dlq_events.sql"
    test -f "sql/flink/generated/${tenant}/stream_health.sql"
    test -f "sql/clickhouse/generated/${tenant}/query.sql"
    test -f "sql/clickhouse/templates/tenant_dwd_ingest.sql.tpl"
  done
  test -f sql/clickhouse/ddl/schema.sql
  test -f sql/clickhouse/ingest/cdc_dim_ingest.sql
  test -f sql/clickhouse/views/grafana_signal_trend.sql
}

start_base_services() {
  log "starting base services: redpanda + postgres + kafka-connect"
  run docker compose up -d redpanda postgres kafka-connect
  wait_for_redpanda
  wait_for_postgres
  wait_for_http "kafka-connect" "http://localhost:18083/"
}

load_postgres_seed_data() {
  log "loading PostgreSQL seed/mock source data"
  run "${PYTHON}" scripts/phase1-load-postgres-source-data.py
}

register_debezium_connector() {
  log "registering Debezium PostgreSQL CDC connector"
  curl -fsS -X DELETE http://localhost:18083/connectors/pg-sensor-tags-connector >/dev/null 2>&1 || true
  sleep 2
  run curl -fsS -X POST -H "Content-Type: application/json" -d @configs/debezium-pg-sensor-tags.json http://localhost:18083/connectors

  for attempt in $(seq 1 24); do
    state="$(curl -fsS http://localhost:18083/connectors/pg-sensor-tags-connector/status 2>/dev/null \
      | python3 -c 'import json,sys; print(json.load(sys.stdin).get("connector",{}).get("state","UNKNOWN"))' 2>/dev/null || true)"
    if [[ "${state}" == "RUNNING" ]]; then
      log "debezium connector is RUNNING"
      return 0
    fi
    log "waiting for debezium connector (${attempt}/24), state=${state:-UNKNOWN}"
    sleep 5
  done

  log "debezium connector did not reach RUNNING"
  curl -fsS http://localhost:18083/connectors/pg-sensor-tags-connector/status | tee -a "${LOG_FILE}" || true
  return 1
}

create_kafka_topics() {
  log "creating Kafka/Redpanda topics"
  run "${PYTHON}" -m src.streaming.topic_admin --create --broker "${BROKER}"
}

submit_flink_jobs() {
  log "starting Flink"
  run docker compose up -d --force-recreate flink-jobmanager flink-taskmanager
  wait_for_http "flink-jobmanager" "http://localhost:18081/overview"

  for tenant in "${E2E_TENANTS[@]}"; do
    log "submitting Flink SQL for ${tenant}"
    run "${FLINK_SQL}" "sql/flink/generated/${tenant}/enrich.sql"
    run "${FLINK_SQL}" "sql/flink/generated/${tenant}/late_events.sql"
    run "${FLINK_SQL}" "sql/flink/generated/${tenant}/dlq_events.sql"
    run "${FLINK_SQL}" "sql/flink/generated/${tenant}/anomaly.sql"
    run "${FLINK_SQL}" "sql/flink/generated/${tenant}/stream_health.sql"
  done
}

verify_flink_jobs() {
  log "verifying Flink jobs are RUNNING"
  local overview="${REPORT_DIR}/flink-jobs-overview.json"
  local expected_jobs
  expected_jobs=$((${#E2E_TENANTS[@]} * 5))

  for attempt in $(seq 1 24); do
    curl -fsS "http://localhost:18081/jobs/overview" > "${overview}"
    if "${PYTHON}" - "${overview}" "${expected_jobs}" <<'PY'
import json
import sys

path = sys.argv[1]
expected_jobs = int(sys.argv[2])
with open(path, encoding="utf-8") as fh:
    jobs = json.load(fh).get("jobs", [])

if not jobs:
    print("No Flink jobs found", file=sys.stderr)
    sys.exit(1)

if len(jobs) < expected_jobs:
    print(f"Only {len(jobs)} Flink jobs found; expected {expected_jobs}", file=sys.stderr)
    sys.exit(1)

bad = [job for job in jobs if job.get("state") != "RUNNING"]

if bad:
    print("Flink jobs not RUNNING:", file=sys.stderr)
    for job in bad:
        print(
            f"{job.get('jid', '<unknown>')} "
            f"{job.get('name', '<unnamed>')} "
            f"{job.get('state', '<unknown>')}",
            file=sys.stderr,
        )
    sys.exit(1)

print(f"{len(jobs)} Flink jobs are RUNNING")
PY
    then
      log "Flink jobs are healthy"
      return 0
    fi
    log "waiting for healthy Flink jobs (${attempt}/24)"
    sleep 5
  done

  log "Flink jobs did not become healthy; overview saved to ${overview}"
  "${PYTHON}" -m json.tool "${overview}" 2>/dev/null | tee -a "${LOG_FILE}" || true
  return 1
}

start_agent_and_data_pump() {
  log "starting realtime diagnosis agents and data pump"
  log "data-pump is the raw telemetry source for this compose E2E"
  run docker compose up -d flink-agent-1 flink-agent-2 flink-agent-3 data-pump
  log "letting pump produce realtime data for ${PUMP_WARMUP_SECONDS}s"
  sleep "${PUMP_WARMUP_SECONDS}"
}

verify_kafka_outputs() {
  log "verifying Kafka topics"
  run docker compose ps

  for tenant in "${E2E_TENANTS[@]}"; do
    local raw_topic
    local staging_topic
    local anomaly_topic
    local diagnosis_topic
    raw_topic="$(topic_name "${tenant}" "raw_apm__sensor_readings")"
    staging_topic="$(topic_name "${tenant}" "staging_apm__sensor_readings")"
    anomaly_topic="$(topic_name "${tenant}" "mart_apm__fct_anomaly_events")"
    diagnosis_topic="$(topic_name "${tenant}" "mart_apm__fct_realtime_diagnosis_events")"

    require_topic_data "${raw_topic}"
    require_topic_data "${staging_topic}"
    require_topic_data "${anomaly_topic}"

    log "diagnosis topic is allowed to be empty if no anomaly was consumed yet: ${diagnosis_topic}"
    topic_has_data "${diagnosis_topic}" && log "OK: ${diagnosis_topic}" || log "INFO: ${diagnosis_topic} empty so far"
  done
}

start_serving_stack() {
  if [[ "${SERVING}" != "1" ]]; then
    log "SERVING=0, skipping ClickHouse/Grafana"
    return 0
  fi

  log "starting ClickHouse and Grafana"
  run docker compose up -d --force-recreate clickhouse
  wait_for_clickhouse
  run docker compose up -d grafana
  wait_for_http "grafana" "http://localhost:13000/api/health" 20

  log "initializing ClickHouse schema and Kafka ingest"
  load_clickhouse_sql sql/clickhouse/ddl/schema.sql
  load_clickhouse_sql sql/clickhouse/ingest/cdc_dim_ingest.sql
  load_clickhouse_sql sql/clickhouse/views/grafana_signal_trend.sql
  for tenant in "${E2E_TENANTS[@]}"; do
    load_clickhouse_tenant_ingest "${tenant}"
  done
}

reset_local_state() {
  log "resetting tenant Kafka topics and ClickHouse serving tables"
  if [[ "${SERVING}" == "1" ]]; then
    reset_clickhouse_tables
    for tenant in "${E2E_TENANTS[@]}"; do
      delete_tenant_topics "${tenant}"
    done
    return 0
  fi

  for tenant in "${E2E_TENANTS[@]}"; do
    delete_tenant_topics "${tenant}"
  done
}

delete_tenant_topics() {
  local tenant="$1"
  local topics=(
    "$(topic_name "${tenant}" "raw_apm__sensor_readings")"
    "$(topic_name "${tenant}" "raw_apm__dlq_events")"
    "$(topic_name "${tenant}" "staging_apm__sensor_readings")"
    "$(topic_name "${tenant}" "staging_apm__dlq_events")"
    "$(topic_name "${tenant}" "mart_apm__fct_anomaly_events")"
    "$(topic_name "${tenant}" "mart_apm__fct_late_sensor_readings")"
    "$(topic_name "${tenant}" "mart_apm__fct_stream_health_snapshots")"
    "$(topic_name "${tenant}" "mart_apm__fct_realtime_diagnosis_events")"
  )

  for topic in "${topics[@]}"; do
    docker compose exec -T redpanda rpk -X "brokers=${BROKER}" topic delete "${topic}" >/dev/null 2>&1 || true
  done
}

reset_clickhouse_tables() {
  local tables=(
    fact_apm_alert_routing
    fact_apm_incidents
    fact_apm_agent_recommendations
    serving_apm_triage_evidence
    fact_apm_quality_events
    dwd_apm_stream_health_snapshots
    dwd_apm_late_sensor_readings
    dwd_apm_anomaly_events
    dwd_apm_realtime_signal_readings
    dwd_apm_realtime_diagnosis_events
    dwd_apm_dlq_events
  )

  for table in "${tables[@]}"; do
    clickhouse_client --query "TRUNCATE TABLE IF EXISTS industrial_apm.${table};" >/dev/null 2>&1 || true
  done
}

load_serving_jobs() {
  if [[ "${SERVING}" != "1" ]]; then
    return 0
  fi

  log "ClickHouse Kafka engine ingest is initialized with the serving stack"
  for tenant in "${E2E_TENANTS[@]}"; do
    load_clickhouse_sql "sql/clickhouse/generated/${tenant}/query.sql"
  done
  log "letting ClickHouse Kafka tables consume data for ${SERVING_WARMUP_SECONDS}s"
  sleep "${SERVING_WARMUP_SECONDS}"
}

verify_serving_rows() {
  if [[ "${SERVING}" != "1" ]]; then
    return 0
  fi

  log "loading triage evidence/recommendations and checking ClickHouse rows"
  run env USE_CLICKHOUSE=1 "${PYTHON}" scripts/phase3-load-triage.py
  run env USE_CLICKHOUSE=1 "${PYTHON}" scripts/phase4-load-incidents.py
  load_clickhouse_sql sql/clickhouse/seeds/demo_anomaly_events.sql
  run make phase3-report
  for tenant in "${E2E_TENANTS[@]}"; do
    load_clickhouse_sql "sql/clickhouse/generated/${tenant}/query.sql"
  done

  clickhouse_client --query "SELECT COUNT(*) > 0 FROM industrial_apm.dwd_apm_realtime_signal_readings;" | grep -q 1
  clickhouse_client --query "SELECT COUNT(*) > 0 FROM industrial_apm.dwd_apm_anomaly_events;" | grep -q 1
  clickhouse_client --query "SELECT COUNT(*) > 0 FROM industrial_apm.fact_apm_quality_events;" | grep -q 1
  clickhouse_client --query "SELECT COUNT(*) > 0 FROM industrial_apm.serving_apm_triage_evidence;" | grep -q 1
  clickhouse_client --query "SELECT COUNT(*) > 0 FROM industrial_apm.fact_apm_agent_recommendations;" | grep -q 1
  clickhouse_client --query "SELECT COUNT(*) > 0 FROM industrial_apm.fact_apm_incidents;" | grep -q 1
  clickhouse_client --query "SELECT COUNT(*) > 0 FROM industrial_apm.fact_apm_alert_routing;" | grep -q 1
  log "ClickHouse row checks passed"
}

print_manual_summary() {
  log "FULL E2E COMPLETE"
  log "Manual inspection URLs:"
  log "  Redpanda Console: http://localhost:18080"
  log "  Flink UI: http://localhost:18081"
  log "  ClickHouse HTTP: http://localhost:18123/play"
  log "  Grafana: http://localhost:13000"
  log "Reports:"
  log "  Log file: ${LOG_FILE}"
  log "  Serving report: reports/phase3-serving-report.md"
}

main() {
  log "FULL E2E START tenants=${E2E_TENANTS[*]} serving=${SERVING}"
  check_dependencies
  start_base_services
  start_serving_stack
  reset_local_state
  load_postgres_seed_data
  register_debezium_connector
  create_kafka_topics
  submit_flink_jobs
  verify_flink_jobs
  load_serving_jobs
  start_agent_and_data_pump
  verify_kafka_outputs
  verify_flink_jobs
  verify_serving_rows
  print_manual_summary
}

main "$@"
