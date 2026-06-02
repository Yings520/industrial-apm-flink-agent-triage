.PHONY: setup generate-demo-data validate-schemas stream-start stream-health stream-stop stream-logs stream-create-topics stream-publish-raw flink-run-enrichment flink-run-anomalies flink-run-late-events flink-run-dlq-events flink-run-stream-health stream-verify-replay stream-read-staging serving-start serving-health serving-stop serving-init serving-load-jobs serving-query phase3-reset phase3-load-triage phase3-report phase3-e2e phase3-serving-visualization-e2e phase4-load-incidents phase4-e2e phase1-ingestion-e2e phase2-e2e full-e2e industrial-apm-demo test

PYTHON ?= ./.venv/bin/python
OUTPUT_DIR ?= data
SEED ?= 42
PROFILE ?= demo
FAULT_DAYS ?= 30
FLINK_SQL ?= ./scripts/flink-sql-client.sh

setup:
	python3 -m venv .venv
	$(PYTHON) -m pip install -e .
	$(PYTHON) -m pip install 'pytest>=8.0'

generate-demo-data:
	$(PYTHON) -m src.telemetry_generator.generate_events --profile $(PROFILE) --seed $(SEED) --output-dir $(OUTPUT_DIR)

generate-fault-data:
	$(PYTHON) -m src.telemetry_generator.generate_fault_data --seed $(SEED) --days $(FAULT_DAYS)

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
	$(FLINK_SQL) sql/flink/generated/tenant_northwind/enrich.sql

flink-run-anomalies:
	$(FLINK_SQL) sql/flink/generated/tenant_northwind/anomaly.sql

flink-run-late-events:
	$(FLINK_SQL) sql/flink/generated/tenant_northwind/late_events.sql

flink-run-dlq-events:
	$(FLINK_SQL) sql/flink/generated/tenant_northwind/dlq_events.sql

flink-run-stream-health:
	$(FLINK_SQL) sql/flink/generated/tenant_northwind/stream_health.sql

stream-verify-replay:
	$(PYTHON) -m src.flink_jobs.replay_verification --actual tests/golden/phase02_anomalies.jsonl --expected tests/golden/phase02_anomalies.jsonl

stream-read-staging:
	docker compose exec -T redpanda rpk -X brokers=localhost:19092 topic consume tenant_northwind.staging_apm__sensor_readings.v1 --num 5

serving-start:
	mkdir -p flink/lib reports
	docker compose up -d redpanda redpanda-console flink-jobmanager flink-taskmanager
	docker compose up -d postgres
	docker compose up -d --force-recreate doris
	docker compose up -d kafka-connect
	docker compose up -d grafana

serving-health:
	docker compose ps
	./scripts/doris-sql.sh -e "SHOW FRONTENDS; SHOW BACKENDS;"
	./scripts/doris-sql.sh -N -B -e "SHOW BACKENDS;" | awk -F '\t' '{ if ($$10 == "true") found = 1 } END { exit !found }'
	@echo "Kafka Connect: $$(curl -s http://localhost:18083/ | head -c 50 2>/dev/null || echo 'not ready')"
	@echo "PostgreSQL: $$(docker compose exec -T postgres pg_isready -U apm -d apm_metadata 2>/dev/null || echo 'not ready')"

debezium-register:
	@echo "Waiting for Kafka Connect..."
	@for i in 1 2 3 4 5 6 7 8 9 10; do \
		curl -s -o /dev/null http://localhost:18083/ && break; \
		echo "  attempt $$i/10..."; sleep 10; \
	done
	@echo "Deleting old connector if exists..."
	curl -s -X DELETE http://localhost:18083/connectors/pg-sensor-tags-connector || true
	sleep 2
	@echo "Registering Debezium PostgreSQL connector..."
	curl -s -X POST -H "Content-Type: application/json" \
		-d @configs/debezium-pg-sensor-tags.json \
		http://localhost:18083/connectors
	@echo ""
	@echo "Waiting for CDC snapshot..."
	@for i in 1 2 3 4 5 6; do \
		state=$$(curl -s http://localhost:18083/connectors/pg-sensor-tags-connector/status | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('connector',{}).get('state','UNKNOWN'))" 2>/dev/null); \
		[ "$$state" = "RUNNING" ] && break; \
		echo "  connector state: $$state, attempt $$i/6..."; sleep 5; \
	done
	@echo "CDC connector ready."

serving-stop:
	docker compose down

serving-init:
	./scripts/doris-sql.sh sql/doris/ddl/schema.sql
	./scripts/doris-sql.sh sql/doris/generated/tenant_northwind/ads_refresh.sql

serving-load-jobs:
	-./scripts/doris-sql.sh -e "STOP ROUTINE LOAD FOR industrial_apm.rl_staging_apm_sensor_readings;" 2>/dev/null
	-./scripts/doris-sql.sh -e "STOP ROUTINE LOAD FOR industrial_apm.rl_mart_apm_anomaly_events;" 2>/dev/null
	-./scripts/doris-sql.sh -e "STOP ROUTINE LOAD FOR industrial_apm.rl_mart_apm_late_sensor_readings;" 2>/dev/null
	-./scripts/doris-sql.sh -e "STOP ROUTINE LOAD FOR industrial_apm.rl_mart_apm_stream_health_snapshots;" 2>/dev/null
	./scripts/doris-sql.sh sql/doris/generated/tenant_northwind/routine_load.sql

serving-query:
	./scripts/doris-sql.sh sql/doris/generated/tenant_northwind/inspection.sql

phase3-reset:
	./scripts/phase3-reset.sh

phase3-load-triage:
	$(PYTHON) scripts/phase3-load-triage.py

phase3-report:
	$(PYTHON) -m src.dashboards.apm_report --output reports/phase3-serving-report.md

phase3-e2e:
	./scripts/phase3-e2e.sh

phase4-load-incidents:
	$(PYTHON) scripts/phase4-load-incidents.py

phase4-e2e:
	./scripts/phase4-e2e.sh

industrial-apm-demo:
	$(PYTHON) -m src.telemetry_generator.generate_events --profile industrial_demo --seed $(SEED) --output-dir $(OUTPUT_DIR)
	$(PYTHON) -m src.streaming.publish_raw_events --profile industrial_demo --mode backfill_replay --dry-run --output-dir $(OUTPUT_DIR)
	@echo "Backfill dry-run complete. Start realtime_live without --dry-run when Kafka/Redpanda is running."

test:
	$(PYTHON) -m pytest

phase1-ingestion-e2e:
	@echo "=== Phase 1: Data Source Ingestion E2E ==="
	@echo "Step 1: Starting services (postgres + redpanda + kafka-connect)..."
	docker compose up -d postgres redpanda kafka-connect
	@sleep 8
	@echo "Step 2: Loading PostgreSQL mock data..."
	$(PYTHON) scripts/phase1-load-postgres-source-data.py
	@echo "Step 3: Registering Debezium CDC..."
	curl -s -X DELETE http://localhost:18083/connectors/pg-sensor-tags-connector 2>/dev/null || true
	@sleep 2
	curl -s -X POST -H "Content-Type: application/json" -d @configs/debezium-pg-sensor-tags.json http://localhost:18083/connectors >/dev/null
	@sleep 10
	@echo "Step 4: Creating Kafka topics..."
	$(PYTHON) -m src.streaming.topic_admin --create --broker localhost:19092
	@echo "Step 5: Generating and publishing Historian telemetry..."
	$(PYTHON) -m src.telemetry_generator.generate_events --profile $(PROFILE) --seed $(SEED) --output-dir $(OUTPUT_DIR)
	$(PYTHON) -m src.streaming.publish_raw_events --profile $(PROFILE) --broker localhost:19092 --output-dir $(OUTPUT_DIR)
	$(PYTHON) -m src.streaming.publish_raw_events --input $(OUTPUT_DIR)/generated/raw_sensor_events_invalid_$(PROFILE).jsonl --profile $(PROFILE) --broker localhost:19092 --output-dir $(OUTPUT_DIR)
	@echo "Step 6: Running schema validation..."
	-$(PYTHON) -m src.telemetry_generator.schema_validation --input $(OUTPUT_DIR)/generated --output-dir $(OUTPUT_DIR) --profile $(PROFILE) --schema raw_sensor_event
	@echo "Step 7: Running E2E tests..."
	$(PYTHON) -m pytest tests/test_phase1_ingestion_e2e_contract.py -q -v
	@echo "=== Phase 1 E2E Complete ==="

phase2-e2e:
	./scripts/phase2-e2e.sh

full-e2e:
	./scripts/full-e2e.sh

e2e-test:
	./scripts/e2e_manual_test.sh $(CMD)

e2e-smoke:
	./scripts/e2e_manual_test.sh smoke

e2e-full:
	./scripts/e2e_manual_test.sh full

# ============================================================
# Phase 3: Serving + Visualization E2E (T3.31 / S8.1)
# 一键执行入口: Doris → Grafana 全链路验证
# ============================================================
phase3-serving-visualization-e2e:
	@echo "=== Phase 3: Serving + Visualization E2E ==="
	@echo "Step 1: Starting Doris + Grafana..."
	docker compose up -d doris grafana
	@sleep 15
	@echo "Step 2: Doris health check..."
	./scripts/doris-sql.sh -e "SHOW FRONTENDS; SHOW BACKENDS;"
	./scripts/doris-sql.sh -e "CREATE DATABASE IF NOT EXISTS industrial_apm;"
	@echo "Step 3: Initialize Doris schema..."
	./scripts/doris-sql.sh sql/doris/ddl/schema.sql
	@echo "Step 4: Initialize ADS Serving Views..."
	./scripts/doris-sql.sh sql/doris/generated/tenant_northwind/ads_refresh.sql
	@echo "Step 5: Start Routine Load jobs..."
	-./scripts/doris-sql.sh -e "STOP ROUTINE LOAD FOR industrial_apm.rl_cdc_dim_apm_assets;" 2>/dev/null
	-./scripts/doris-sql.sh -e "STOP ROUTINE LOAD FOR industrial_apm.rl_cdc_dim_apm_asset_components;" 2>/dev/null
	-./scripts/doris-sql.sh -e "STOP ROUTINE LOAD FOR industrial_apm.rl_cdc_dim_apm_signal_tags_mapping;" 2>/dev/null
	-./scripts/doris-sql.sh -e "STOP ROUTINE LOAD FOR industrial_apm.rl_cdc_dim_apm_failuremode;" 2>/dev/null
	-./scripts/doris-sql.sh -e "STOP ROUTINE LOAD FOR industrial_apm.rl_cdc_dim_apm_failureevent;" 2>/dev/null
	-./scripts/doris-sql.sh -e "STOP ROUTINE LOAD FOR industrial_apm.rl_cdc_dim_apm_workorder;" 2>/dev/null
	-./scripts/doris-sql.sh -e "STOP ROUTINE LOAD FOR industrial_apm.rl_cdc_dim_apm_maintenance;" 2>/dev/null
	-./scripts/doris-sql.sh -e "STOP ROUTINE LOAD FOR industrial_apm.rl_cdc_dim_apm_tenants;" 2>/dev/null
	-./scripts/doris-sql.sh -e "STOP ROUTINE LOAD FOR industrial_apm.rl_tenant_northwind_dwd_realtime_signal_readings;" 2>/dev/null
	-./scripts/doris-sql.sh -e "STOP ROUTINE LOAD FOR industrial_apm.rl_tenant_northwind_dwd_anomaly_events;" 2>/dev/null
	-./scripts/doris-sql.sh -e "STOP ROUTINE LOAD FOR industrial_apm.rl_tenant_northwind_dwd_late_sensor_readings;" 2>/dev/null
	-./scripts/doris-sql.sh -e "STOP ROUTINE LOAD FOR industrial_apm.rl_tenant_northwind_dwd_stream_health_snapshots;" 2>/dev/null
	-./scripts/doris-sql.sh -e "STOP ROUTINE LOAD FOR industrial_apm.rl_tenant_northwind_dwd_realtime_diagnosis_events;" 2>/dev/null
	-./scripts/doris-sql.sh -e "STOP ROUTINE LOAD FOR industrial_apm.rl_tenant_northwind_dwd_dlq_events;" 2>/dev/null
	./scripts/doris-sql.sh sql/doris/generated/tenant_northwind/routine_load.sql
	@sleep 5
	@echo "Step 6: Run inspection queries..."
	./scripts/doris-sql.sh sql/doris/generated/tenant_northwind/inspection.sql
	@echo "Step 7: Routine Load status..."
	./scripts/doris-sql.sh -e "SHOW ROUTINE LOAD FOR industrial_apm.*;"
	@echo "Step 8: Run Phase 3 tests..."
	$(PYTHON) -m pytest tests/test_phase3_doris_serving_e2e.py tests/test_phase3_grafana_e2e.py tests/test_phase3_grafana_serving_mapping.py tests/test_phase3_grafana_claim_boundary.py -q -v 2>&1 | tee reports/phase3-test-results.log
	@echo ""
	@echo "=== Phase 3 E2E Complete ==="
	@echo "Reports: reports/phase3-*"
	@echo "Grafana: http://localhost:13000 (anonymous access enabled)"
