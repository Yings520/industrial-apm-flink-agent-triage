#!/usr/bin/env bash
set -euo pipefail

echo "Phase 3 local E2E: raw -> Flink -> Redpanda mart -> Doris -> report"
echo "Redpanda Console: http://localhost:18080"
echo "Flink UI: http://localhost:18081"
echo "Doris FE: http://localhost:18030"
echo "Doris MySQL: localhost:19030"

make serving-start
for attempt in {1..18}; do
  if make serving-health; then
    break
  fi
  if [[ "${attempt}" == "18" ]]; then
    echo "Doris did not become healthy in time" >&2
    exit 1
  fi
  sleep 10
done
make serving-init
make phase3-reset
make stream-create-topics
make stream-publish-raw PROFILE=smoke
make flink-run-enrichment
make flink-run-anomalies
make serving-load-jobs
sleep 65
make phase3-load-triage

echo "Doris inspection queries: dwd_apm_sensor_readings_rt, fact_apm_anomaly_events, fact_apm_quality_events"
make serving-query
./scripts/doris-sql.sh -N -e "SELECT COUNT(*) > 0 FROM industrial_apm.dwd_apm_sensor_readings_rt;" | grep -q 1
./scripts/doris-sql.sh -N -e "SELECT COUNT(*) > 0 FROM industrial_apm.fact_apm_anomaly_events;" | grep -q 1
./scripts/doris-sql.sh -N -e "SELECT COUNT(*) > 0 FROM industrial_apm.fact_apm_late_sensor_readings;" | grep -q 1
./scripts/doris-sql.sh -N -e "SELECT COUNT(*) > 0 FROM industrial_apm.fact_apm_stream_health_snapshots;" | grep -q 1
./scripts/doris-sql.sh -N -e "SELECT COUNT(*) > 0 FROM industrial_apm.fact_apm_quality_events;" | grep -q 1
./scripts/doris-sql.sh -N -e "SELECT COUNT(*) > 0 FROM industrial_apm.serving_apm_triage_evidence;" | grep -q 1
./scripts/doris-sql.sh -N -e "SELECT COUNT(*) > 0 FROM industrial_apm.fact_apm_agent_recommendations;" | grep -q 1
make phase3-report

echo "Report: reports/phase3-serving-report.md"
