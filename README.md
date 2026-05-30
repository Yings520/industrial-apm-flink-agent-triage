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

Phase 2 proves local Kafka-compatible ingestion and Docker-backed Flink SQL processing for a portfolio/current-project demo. It does not claim production deployment, production SLA, autonomous diagnosis, automatic remediation, real factory operation, or real downtime/MTTR reduction.

Local ports:

- Redpanda Kafka listener: `localhost:19092`
- Redpanda admin API: `localhost:19644`
- Redpanda Console UI: `http://localhost:18080`
- Flink JobManager UI: `http://localhost:18081`

```bash
make stream-start
make stream-health
make stream-create-topics
make generate-demo-data PROFILE=smoke
make stream-publish-raw PROFILE=smoke
make flink-run-enrichment
make flink-run-anomalies
make stream-verify-replay
```

Dry-run helpers are available without Docker:

```bash
python -m src.streaming.topic_admin --create --dry-run
python -m src.streaming.publish_raw_events --profile smoke --dry-run
```

Tenant-scoped topics use `{tenant_id}.{layer}_{domain}__{model_name}.v1`, for example `tenant_northwind.raw_apm__sensor_readings.v1`, `tenant_northwind.staging_apm__sensor_readings.v1`, `tenant_northwind.mart_apm__fct_anomaly_events.v1`, and `tenant_northwind.raw_apm__dlq_events.v1`.

The local demo defaults are `watermark_delay_minutes: 5` and `allowed_lateness_minutes: 10` in `configs/streaming.yml`. These are reviewer-friendly industrial APM defaults for the synthetic demo, not production tuning advice. Staging sensor readings are written to `*.staging_apm__sensor_readings.v1`, mapping failures go to `*.staging_apm__dlq_events.v1`, late readings go to `*.mart_apm__fct_late_sensor_readings.v1`, and stream health records expose watermark lag and checkpoint status on `*.mart_apm__fct_stream_health_snapshots.v1`.

PyFlink package compatibility may lag the repo's Python `>=3.13,<3.16` range. The executable Python job modules still expose `--help` and local pipeline descriptions, while the reviewer-facing `make flink-run-*` targets submit Flink SQL through the Docker Flink runtime and the Kafka SQL connector.

Flink runtime output order is not a contract. Replay verification reads anomaly JSONL, sorts by deterministic keys including `anomaly_id`, `rule_id`, `tenant_id`, `asset_id`, `tag_id`, `metric_name`, `window_start`, and `window_end`, then compares normalized records.

## Phase 3 Serving Quickstart

Phase 3 adds Apache Doris as the local realtime serving store. Doris consumes Redpanda staging/mart topics through Routine Load and exposes SQL/report surfaces for inspection. Docker Doris is local/demo only; it is not a production HA deployment.

Additional local ports:

- Doris FE HTTP: `http://localhost:18030`
- Doris MySQL protocol: `localhost:19030`

```bash
make serving-start
make serving-health
make serving-init
make phase3-reset
make stream-create-topics
make stream-publish-raw PROFILE=smoke
make flink-run-enrichment
make flink-run-anomalies
make serving-load-jobs
make phase3-load-triage
make serving-query
make phase3-report
```

One-command local path:

```bash
make phase3-e2e
```

Inspection surfaces:

- Redpanda Console: `http://localhost:18080` for raw/staging/mart topics.
- Flink UI: `http://localhost:18081` for local job/runtime status.
- Doris SQL: `make serving-query` for table counts and sample rows.
- Report: `reports/phase3-serving-report.md` for local/synthetic dashboard metrics.

Phase 3 is not considered passed unless Doris query output or the generated report shows data from the serving path. Routine Load jobs consume `tenant_northwind.staging_apm__sensor_readings.v1`, `tenant_northwind.mart_apm__fct_anomaly_events.v1`, `tenant_northwind.mart_apm__fct_late_sensor_readings.v1`, and `tenant_northwind.mart_apm__fct_stream_health_snapshots.v1`; raw topics stay in Redpanda for replay/debug.

`make phase3-e2e` is reset-first for repeatability: it deletes/recreates the local `tenant_northwind` Redpanda topics, truncates Phase 3 Doris serving tables, reloads Routine Load jobs from the beginning, and fails unless Doris has nonzero sensor, anomaly, late-event, stream-health, quality-event, triage-evidence, and recommendation rows.

LLM-assisted triage remains provider-optional. The default fallback path generates evidence-grounded findings and recommended checks without paid LLM credentials. The LLM/fallback writes recommendation output; it does not generate evidence or detect anomalies.

## Phase 4 Alert Workflow Quickstart

Phase 4 converts anomalies into incident-level alerts, adds LLM/fallback triage with invocation audit, Teams card routing with simulated notification fallback, operator feedback storage, and reliability documentation.

```bash
make phase4-e2e
```

This reset-first E2E path runs the Phase 3 sequence, then loads Phase 4 incidents, routing records, LLM invocation audit, and demo feedback into Doris.

Optional environment variables:

- `OPENAI_API_KEY` — OpenAI-compatible API key for real LLM triage.
- `OPENAI_BASE_URL` — Custom API base URL for compatible providers (defaults to `https://api.openai.com/v1`).
- `OPENAI_MODEL` — Model name (e.g. `gpt-4o-mini`).
- `TEAMS_WEBHOOK_URL` — Teams incoming webhook URL for card notifications.

When `OPENAI_API_KEY` or `OPENAI_MODEL` is missing, fallback recommendations are used. When `TEAMS_WEBHOOK_URL` is missing, Teams notification is recorded as simulated and the workflow continues.

Phase 4 Doris tables:

- `fact_apm_incidents` — Incident-level alert records with correlation key and cooldown.
- `fact_apm_alert_routing` — Routing status (pending/simulated/delivered/failed).
- `fact_apm_operator_feedback` — Demo/evaluation feedback rows (not a production audit trail).
- `fact_apm_llm_invocations` — LLM invocation audit with raw response and quarantine reason.

Inspection surfaces:

```bash
make serving-query           # All Phase 3 + Phase 4 table counts and samples
make phase4-load-incidents   # Load Phase 4 incidents/routing/feedback/audit
```

### CI

Core tests run without paid LLM API calls or Teams webhook in CI (`.github/workflows/ci.yml`). Docker E2E (`make phase3-e2e`, `make phase4-e2e`) is local-only. CI uses `python -m pytest` with mocked transports.

### Failure Mode Documentation

`docs/failure-modes.md` covers 11 system failure modes with detection signals, mitigation, test evidence, and portfolio claim boundaries. Includes: schema drift, late data spike, sensor outage, Kafka lag, Flink checkpoint failure, alert storm, LLM outage, LLM invalid/hallucinated output, tenant leakage, Teams webhook failure, and cost spike.

Phase 4 triage is LLM-assisted alert triage for operator support. It provides root-cause hypotheses and evidence-grounded summaries. It does not claim autonomous diagnosis, automatic remediation, production SLA, real factory deployment, or real downtime/MTTR reduction.

## Model Maturity

This project is a **production-aware portfolio MVP**. The table below maps current and planned data model capabilities.

| Data Domain | MVP (Phases 1-4) | Production Target (Phase 5+) |
|-------------|-------------------|------------------------------|
| Asset Registry | tenant/site/plant/asset (flat) | Full hierarchy + SCD Type 2 + component |
| Sensor Tags | tag_id/name only | Measurement point context + engineering limits |
| Telemetry | event_time/ingest_time, 5 metrics | + sampling_rate, operating_context |
| Anomaly Detection | 5 rule-based scenarios | + failure_mode correlation, fault_type |
| Quality | 7 check types, string flags | Structured quality_event_id references |
| Failure Mode | Not modeled | FMECA catalog per component type |
| Work Order | Not modeled | WO/inspection with cause/remedy codes |
| Triage Evidence | Deterministic, tenant-safe | + failure_mode, work_order, quality_event context |
| Rule Governance | rule_id/method/severity only | Version, scope, owner, approval |
| Flink SQL | Hardcoded per tenant | Parameterized, multi-tenant, metadata-driven |

**Intended audience:** Data engineering / streaming platform reviewers evaluating contract design, event-time processing, and LLM-assisted triage architecture.

**Not claimed:** Production deployment, autonomous diagnosis, RUL prediction, certified safety actions, or real MTTR reduction.
