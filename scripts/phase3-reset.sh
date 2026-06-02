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

cdc_jobs=(
  "rl_cdc_dim_apm_assets"
  "rl_cdc_dim_apm_asset_components"
  "rl_cdc_dim_apm_signal_tags_mapping"
  "rl_cdc_dim_apm_failuremode"
  "rl_cdc_dim_apm_failureevent"
  "rl_cdc_dim_apm_workorder"
  "rl_cdc_dim_apm_maintenance"
  "rl_cdc_dim_apm_tenants"
)

jobs=(
  "rl_${TENANT}_dwd_realtime_signal_readings"
  "rl_${TENANT}_dwd_anomaly_events"
  "rl_${TENANT}_dwd_late_sensor_readings"
  "rl_${TENANT}_dwd_stream_health_snapshots"
  "rl_${TENANT}_dwd_realtime_diagnosis_events"
  "rl_${TENANT}_dwd_dlq_events"
)

tables=(
  "fact_apm_agent_recommendations"
  "serving_apm_triage_evidence"
  "fact_apm_quality_events"
  "dwd_apm_stream_health_snapshots"
  "dwd_apm_late_sensor_readings"
  "dwd_apm_anomaly_events"
  "dwd_apm_realtime_signal_readings"
  "dwd_apm_realtime_diagnosis_events"
  "dwd_apm_dlq_events"
)

for job in "${cdc_jobs[@]}"; do
  ./scripts/doris-sql.sh -e "STOP ROUTINE LOAD FOR industrial_apm.${job};" >/dev/null 2>&1 || true
done

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
