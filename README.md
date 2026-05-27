# industrial-apm-flink-agent-triage

Real-time industrial asset performance management (APM) pipeline with Kafka/Flink and LLM-assisted alert triage.

## Phase 1 Quickstart

Phase 1 is local-only: generate synthetic telemetry, validate JSON Schema contracts, inspect accepted/rejected artifacts, and run tests. Kafka/Flink/Docker start in Phase 2; serving stores, dashboards, and LLM triage also start in later phases.

Requires Python `>=3.13,<3.16`.

```bash
make setup
make generate-demo-data
make validate-schemas
make test
```

The demo profile intentionally includes invalid candidate events. `make validate-schemas` returns non-zero when invalid records exist, while still writing accepted, rejected, and summary artifacts.
`PROFILE` defaults to `demo`; use `PROFILE=smoke` for a smaller run. Validation filters to the selected profile so old JSONL files from previous runs are not mixed into the current summary.

Generated artifacts:

- `data/generated/raw_sensor_events_*.jsonl` - generated tag-centric telemetry candidates.
- `data/generated/validated_raw_sensor_events.jsonl` - accepted validated events for Phase 2 replay into Kafka.
- `data/rejected/rejected_raw_sensor_records.jsonl` - DLQ-shaped rejected records from validation.
- `data/validation-summary.json` - machine-readable validation summary.

Useful inspection commands:

```bash
head -n 3 data/generated/raw_sensor_events_demo.jsonl
head -n 3 data/generated/validated_raw_sensor_events.jsonl
head -n 3 data/rejected/rejected_raw_sensor_records.jsonl
python -m json.tool data/validation-summary.json
```

## Phase 2 Streaming Quickstart

Phase 2 proves local Kafka-compatible ingestion and PyFlink event-time anomaly processing for a portfolio/current-project demo. It does not claim production deployment, production SLA, autonomous diagnosis, automatic remediation, real factory operation, or real downtime/MTTR reduction.

Local ports:

- Redpanda Kafka listener: `localhost:19092`
- Redpanda admin API: `localhost:19644`
- Flink JobManager UI: `http://localhost:18081`

```bash
make stream-start
make stream-health
make stream-create-topics
make generate-demo-data PROFILE=smoke
make stream-publish-raw PROFILE=smoke
```

Dry-run helpers are available without Docker:

```bash
python -m src.streaming.topic_admin --create --dry-run
python -m src.streaming.publish_raw_events --profile smoke --dry-run
```

Tenant-scoped topics use raw/staging/mart layers, for example `tenant_northwind.raw.sensor_events.v1`, `tenant_northwind.staging.telemetry_enriched.v1`, `tenant_northwind.mart.anomalies.v1`, and `tenant_northwind.raw.dlq.v1`.
