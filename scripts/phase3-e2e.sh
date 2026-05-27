#!/usr/bin/env bash
set -euo pipefail

echo "Phase 3 local E2E: raw -> Flink -> Redpanda mart -> Doris -> report"
echo "Redpanda Console: http://localhost:18080"
echo "Flink UI: http://localhost:18081"
echo "Doris FE: http://localhost:18030"
echo "Doris MySQL: localhost:19030"

make serving-start
make stream-create-topics
make stream-publish-raw PROFILE=smoke
make flink-run-enrichment
make flink-run-anomalies
make serving-init
make serving-load-jobs
sleep 65

echo "Doris inspection queries: dwd_apm_sensor_readings_rt, fact_apm_anomaly_events, fact_apm_quality_events"
make serving-query
./scripts/doris-sql.sh -N -e "SELECT COUNT(*) > 0 FROM industrial_apm.dwd_apm_sensor_readings_rt;" | grep -q 1
./scripts/doris-sql.sh -N -e "SELECT COUNT(*) > 0 FROM industrial_apm.fact_apm_anomaly_events;" | grep -q 1
make phase3-report

echo "Report: reports/phase3-serving-report.md"
