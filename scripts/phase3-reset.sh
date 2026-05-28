#!/usr/bin/env bash
set -euo pipefail

TENANT="${TENANT:-tenant_northwind}"
BROKER="${BROKER:-localhost:19092}"

topics=(
  "${TENANT}.raw_apm__sensor_readings.v1"
  "${TENANT}.raw_apm__dlq_events.v1"
  "${TENANT}.staging_apm__sensor_readings.v1"
  "${TENANT}.staging_apm__dlq_events.v1"
  "${TENANT}.mart_apm__fct_anomaly_events.v1"
  "${TENANT}.mart_apm__fct_late_sensor_readings.v1"
  "${TENANT}.mart_apm__fct_stream_health_snapshots.v1"
)

jobs=(
  "rl_staging_apm_sensor_readings"
  "rl_mart_apm_anomaly_events"
  "rl_mart_apm_late_sensor_readings"
  "rl_mart_apm_stream_health_snapshots"
)

tables=(
  "fact_apm_agent_recommendations"
  "serving_apm_triage_evidence"
  "fact_apm_quality_events"
  "fact_apm_stream_health_snapshots"
  "fact_apm_late_sensor_readings"
  "fact_apm_anomaly_events"
  "dwd_apm_sensor_readings_rt"
)

for job in "${jobs[@]}"; do
  ./scripts/doris-sql.sh -e "STOP ROUTINE LOAD FOR industrial_apm.${job};" >/dev/null 2>&1 || true
done

for table in "${tables[@]}"; do
  ./scripts/doris-sql.sh -e "TRUNCATE TABLE industrial_apm.${table};" >/dev/null 2>&1 || true
done

for topic in "${topics[@]}"; do
  docker compose exec -T redpanda rpk -X "brokers=${BROKER}" topic delete "${topic}" >/dev/null 2>&1 || true
done

echo "Phase 3 reset complete for ${TENANT}"
