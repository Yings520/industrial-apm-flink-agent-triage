.PHONY: setup generate-demo-data validate-schemas stream-start stream-health stream-stop stream-logs stream-create-topics stream-publish-raw flink-run-enrichment flink-run-anomalies stream-verify-replay stream-read-staging test

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
	docker compose exec -T redpanda rpk -X brokers=localhost:19092 topic consume tenant_northwind.staging_telemetry_enriched.v1 --num 5

test:
	$(PYTHON) -m pytest
