.PHONY: setup generate-demo-data validate-schemas stream-start stream-health stream-stop stream-logs stream-create-topics stream-publish-raw test

PYTHON ?= ./.venv/bin/python
OUTPUT_DIR ?= data
SEED ?= 42
PROFILE ?= demo

setup:
	python3 -m venv .venv
	$(PYTHON) -m pip install -e .
	$(PYTHON) -m pip install 'pytest>=8.0'

generate-demo-data:
	$(PYTHON) -m src.telemetry_generator.generate_events --profile $(PROFILE) --seed $(SEED) --output-dir $(OUTPUT_DIR)

validate-schemas:
	$(PYTHON) -m src.telemetry_generator.schema_validation --input $(OUTPUT_DIR)/generated --output-dir $(OUTPUT_DIR) --profile $(PROFILE) --schema raw_sensor_event

stream-start:
	docker compose up -d redpanda flink-jobmanager flink-taskmanager

stream-health:
	docker compose ps
	docker compose exec -T redpanda rpk cluster health --brokers localhost:19092

stream-stop:
	docker compose down

stream-logs:
	docker compose logs --tail=100 redpanda flink-jobmanager flink-taskmanager

stream-create-topics:
	$(PYTHON) -m src.streaming.topic_admin --create --broker localhost:19092

stream-publish-raw:
	$(PYTHON) -m src.telemetry_generator.generate_events --profile $(PROFILE) --seed $(SEED) --output-dir $(OUTPUT_DIR)
	$(PYTHON) -m src.streaming.publish_raw_events --profile $(PROFILE) --broker localhost:19092 --output-dir $(OUTPUT_DIR)

test:
	$(PYTHON) -m pytest
