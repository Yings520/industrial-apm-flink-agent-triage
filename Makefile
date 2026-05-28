.PHONY: setup generate-demo-data validate-schemas stream-start stream-health stream-stop stream-logs stream-create-topics stream-publish-raw flink-run-enrichment flink-run-anomalies stream-verify-replay stream-read-staging serving-start serving-health serving-stop serving-init serving-load-jobs serving-query phase3-reset phase3-load-triage phase3-report phase3-e2e test

PYTHON ?= ./.venv/bin/python
OUTPUT_DIR ?= data
SEED ?= 42
PROFILE ?= demo
FLINK_SQL ?= ./scripts/flink-sql-client.sh

setup:
	python3 -m venv .venv
	$(PYTHON) -m pip install -e .
	$(PYTHON) -m pip install 'pytest>=8.0'

generate-demo-data:
	$(PYTHON) -m src.telemetry_generator.generate_events --profile $(PROFILE) --seed $(SEED) --output-dir $(OUTPUT_DIR)

validate-schemas:
	$(PYTHON) -m src.telemetry_generator.schema_validation --input $(OUTPUT_DIR)/generated --output-dir $(OUTPUT_DIR) --profile $(PROFILE) --schema raw_sensor_event

stream-start:
	mkdir -p flink/lib
	docker compose up -d redpanda redpanda-console flink-jobmanager flink-taskmanager

stream-health:
	docker compose ps
	docker compose exec -T redpanda rpk -X brokers=localhost:19092 cluster health

stream-stop:
	docker compose down

stream-logs:
	docker compose logs --tail=100 redpanda redpanda-console flink-jobmanager flink-taskmanager

stream-create-topics:
	$(PYTHON) -m src.streaming.topic_admin --create --broker localhost:19092

stream-publish-raw:
	$(PYTHON) -m src.telemetry_generator.generate_events --profile $(PROFILE) --seed $(SEED) --output-dir $(OUTPUT_DIR)
	$(PYTHON) -m src.streaming.publish_raw_events --profile $(PROFILE) --broker localhost:19092 --output-dir $(OUTPUT_DIR)

flink-run-enrichment:
	$(FLINK_SQL) sql/flink_enrichment.sql

flink-run-anomalies:
	$(FLINK_SQL) sql/flink_anomalies.sql

stream-verify-replay:
	$(PYTHON) -m src.flink_jobs.replay_verification --actual tests/golden/phase02_anomalies.jsonl --expected tests/golden/phase02_anomalies.jsonl

stream-read-staging:
	docker compose exec -T redpanda rpk -X brokers=localhost:19092 topic consume tenant_northwind.staging_apm__sensor_readings.v1 --num 5

serving-start:
	mkdir -p flink/lib reports
	docker compose up -d redpanda redpanda-console flink-jobmanager flink-taskmanager
	docker compose up -d --force-recreate doris

serving-health:
	docker compose ps
	./scripts/doris-sql.sh -e "SHOW FRONTENDS; SHOW BACKENDS;"
	./scripts/doris-sql.sh -N -B -e "SHOW BACKENDS;" | awk -F '\t' '{ if ($$10 == "true") found = 1 } END { exit !found }'

serving-stop:
	docker compose down

serving-init:
	./scripts/doris-sql.sh sql/doris_schema.sql
	./scripts/doris-sql.sh sql/doris_serving_views.sql

serving-load-jobs:
	-./scripts/doris-sql.sh -e "STOP ROUTINE LOAD FOR industrial_apm.rl_staging_apm_sensor_readings;"
	-./scripts/doris-sql.sh -e "STOP ROUTINE LOAD FOR industrial_apm.rl_mart_apm_anomaly_events;"
	-./scripts/doris-sql.sh -e "STOP ROUTINE LOAD FOR industrial_apm.rl_mart_apm_late_sensor_readings;"
	-./scripts/doris-sql.sh -e "STOP ROUTINE LOAD FOR industrial_apm.rl_mart_apm_stream_health_snapshots;"
	./scripts/doris-sql.sh sql/doris_routine_load.sql

serving-query:
	./scripts/doris-sql.sh sql/doris_inspection_queries.sql

phase3-reset:
	./scripts/phase3-reset.sh

phase3-load-triage:
	$(PYTHON) scripts/phase3-load-triage.py

phase3-report:
	$(PYTHON) -m src.dashboards.apm_report --output reports/phase3-serving-report.md

phase3-e2e:
	./scripts/phase3-e2e.sh

test:
	$(PYTHON) -m pytest
