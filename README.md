# Industrial APM — Real-Time Pipeline with Flink Event-Time Processing & LLM-Assisted Alert Triage

Real-time industrial Asset Performance Management data pipeline supporting synthetic telemetry generation, schema-contract validation, Kafka/Redpanda streaming, Flink SQL event-time processing, ClickHouse-based serving with Grafana dashboards, and LLM-assisted operator alert triage with audit trails.

> **Portfolio/current-project scope.** This is a local demonstration pipeline using synthetic data, Docker-based infrastructure, and an OpenAI-compatible LLM provider. See [Claim Boundary](#claim-boundary).

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│  PHASE 1 — Data Source Ingestion                                    │
│                                                                     │
│  Synthetic Telemetry Generator  ──►  JSON Schema Validation         │
│  (11 operating states,               (raw_sensor_event schema,       │
│   6 scenario types,                  split accepted / DLQ)           │
│   3 archetypes × 180 assets)                                        │
│                                                                     │
│  PostgreSQL (CDC source) ──► Debezium ──► Kafka (CDC topics)        │
└─────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│  PHASE 2 — Streaming (Redpanda + Flink SQL)                         │
│                                                                     │
│  raw_apm__sensor_readings                                           │
│       │                                                             │
│       ├── Flink: enrichment ──► staging_apm__sensor_readings        │
│       │   (joins tag metadata, validates tenant/unit)                │
│       │                                                             │
│       ├── Flink: anomaly detection ──► mart_apm__fct_anomaly_events │
│       │   (5 detectors: threshold_spike, rolling_zscore,             │
│       │    drift_rate_of_change, missing_heartbeat, flatline)        │
│       │                                                             │
│       ├── Flink: late event handling ──► mart_apm__fct_late_...     │
│       ├── Flink: DLQ routing ──► staging_apm__dlq_events            │
│       └── Flink: stream health ──► mart_apm__fct_stream_health_...  │
│                                                                     │
│  Event-time: Watermark delay 5 min, allowed lateness 10 min         │
└─────────────────────────────────────────────────────────────────────┘
                                   │
                    ┌──────────────┼──────────────┐
                    ▼              ▼              ▼
┌─────────────────────────────────────────────────────────────────────┐
│  LLM Agents (3 × Python daemons, multi-threaded)                    │
│                                                                     │
│  consume anomaly → context_lookup → assemble_evidence →              │
│  generate_diagnosis (LLM or rule-based fallback) →                   │
│  validate_diagnosis → produce to mart_apm__fct_realtime_diagnosis_...│
└─────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│  PHASE 3 — Serving (ClickHouse + Grafana)                           │
│                                                                     │
│  ClickHouse Kafka Engine Materialized Views + Routine Load           │
│  Grafana dashboards (APM Overview, APM Asset Health)                 │
│  Triage evidence assembly, quality event tracking                    │
└─────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│  PHASE 4 — Alert Workflow & Operator Triage                          │
│                                                                     │
│  Incident correlation (time-bucketed dedup + cooldown)               │
│  Alert routing (critical → Teams card; warning → simulated ticket)   │
│  Operator feedback collection & storage                              │
│  LLM invocation audit trail (request hashing, token costs, latency)  │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Prerequisites

- **Python** `>=3.13,<3.16`
- **uv** (Python package manager — or pip + venv)
- **Docker** (for Kafka/Redpanda, Flink, PostgreSQL, ClickHouse, Grafana, agents)
- **No external API keys required** — LLM integration uses an OpenAI-compatible endpoint (DeepSeek by default). The pipeline falls back to rule-based recommendations when no API key is available.

---

## Quick Start

```bash
# Clone and set up
git clone <repo-url>
cd industrial-apm-flink-agent-triage
make setup

# Run core tests (no Docker, no LLM required)
make test

# Phase 1: Generate synthetic data and validate schemas
make generate-demo-data
make validate-schemas

# Phase 1 E2E: PostgreSQL → Debezium CDC → Redpanda → Validation
make phase1-ingestion-e2e
```

---

## Project Structure

```
├── configs/                       # YAML/JSON configuration
│   ├── tenants.yml                # 3 tenants: northwind, apac_ops, euro_core
│   ├── assets.yml                 # Asset hierarchy (plants, archetypes, assets)
│   ├── streaming.yml              # Kafka topics, watermark/lateness settings
│   ├── anomaly_rules.yml          # 6 anomaly detection rules with governance metadata
│   ├── failure_modes.yml          # 11 FMECA failure modes with detection signals
│   ├── scenarios.yml              # 7 scenario types, 2 profiles (smoke, demo)
│   ├── triage_prompt.yml          # LLM triage prompt config
│   ├── threshold_profiles.yml     # Sensor threshold profiles
│   └── grafana/                   # Grafana dashboards & datasource provisioning
│       ├── dashboard.yml
│       ├── dashboards/            # apm-overview-ch.json, apm-asset-health-ch.json
│       └── datasources/           # clickhouse.yml
│
├── contracts/                     # JSON Schema contracts (Draft 2020-12)
│   ├── raw_sensor_event.schema.json
│   ├── enriched_telemetry.schema.json
│   ├── anomaly_event.schema.json
│   ├── sensor_tag.schema.json
│   ├── agent_explanation.schema.json
│   ├── realtime_diagnosis_event.schema.json
│   ├── operator_feedback.schema.json
│   └── ... (17 schemas total)
│
├── sql/                           # SQL definitions
│   ├── postgres_init.sql          # PostgreSQL CDC source schema
│   ├── schema_postgres.sql
│   ├── cdc_sources.sql
│   ├── flink/
│   │   ├── templates/             # Jinja2-style Flink SQL templates (*.sql.tpl)
│   │   │   ├── enrich.sql.tpl
│   │   │   ├── anomaly.sql.tpl
│   │   │   ├── late_events.sql.tpl
│   │   │   ├── dlq_events.sql.tpl
│   │   │   └── stream_health.sql.tpl
│   │   └── generated/             # Per-tenant rendered SQL
│   │       ├── tenant_northwind/
│   │       ├── tenant_apac_ops/
│   │       └── tenant_euro_core/
│   └── clickhouse/
│       ├── ddl/schema.sql         # ClickHouse DWH schema
│       ├── ingest/cdc_dim_ingest.sql
│       ├── seeds/demo_anomaly_events.sql
│       ├── views/grafana_signal_trend.sql
│       ├── templates/             # ClickHouse SQL templates
│       │   ├── tenant_dwd_ingest.sql.tpl
│       │   ├── kafka_ingest.sql.tpl
│       │   ├── ads_refresh.sql.tpl
│       │   ├── inspection.sql.tpl
│       │   └── query.sql.tpl
│       └── generated/             # Per-tenant rendered SQL
│
├── src/                           # Python source
│   ├── telemetry_generator/       # Synthetic telemetry generation
│   │   ├── generate_events.py     # Main entry point (CLI + API)
│   │   ├── schema_validation.py   # JSON Schema validation with DLQ routing
│   │   ├── models.py              # TypedDict / frozen dataclass definitions
│   │   ├── config.py              # YAML config loading & tag metadata builder
│   │   ├── scenarios.py           # Profile resolution & validation
│   │   ├── signal_profiles.py     # Deterministic signal value generation (11 states)
│   │   ├── fault_injector.py      # Fault timeline generation (30-day degradation)
│   │   ├── business_data.py       # Dimension seed data (assets, components, tags)
│   │   ├── measurement_points.py  # Measurement point catalog builder
│   │   └── window_aggregates.py   # 15-min tumbling window aggregates
│   │
│   ├── streaming/                 # Kafka/Redpanda topic management & publishing
│   │   ├── topic_admin.py         # Topic CRUD via rpk CLI
│   │   ├── topic_names.py         # Tenant-scoped topic name resolution
│   │   └── publish_raw_events.py  # Schema-validated raw event producer
│   │
│   ├── flink_jobs/                # PyFlink pipeline descriptions & utilities
│   │   ├── enrichment_job.py      # Raw → staging enrichment pipeline
│   │   ├── anomaly_job.py         # Anomaly detection pipeline
│   │   ├── anomaly_rules.py       # 5-method anomaly detection rule engine
│   │   ├── event_time.py          # Event-time watermark/lateness utilities
│   │   ├── late_events.py         # Late event classification
│   │   ├── metadata.py            # Tag metadata enrichment logic
│   │   ├── stream_health.py       # Stream health snapshot generation
│   │   └── replay_verification.py # Deterministic output comparison
│   │
│   ├── flink_agents/              # LLM-assisted realtime diagnosis agents
│   │   ├── realtime_diagnosis_agent.py    # Main daemon (multi-threaded)
│   │   ├── anomaly_event_consumer.py      # Kafka anomaly consumer (pattern sub)
│   │   ├── context_lookup.py              # Signal/asset/evidence context lookup
│   │   ├── evidence_assembler.py          # Triage evidence assembly
│   │   ├── diagnosis_generator.py         # LLM or fallback diagnosis dispatch
│   │   ├── diagnosis_producer.py          # Kafka diagnosis event producer
│   │   └── output_validator.py            # Cross-tenant + content validation
│   │
│   ├── serving/                   # ClickHouse serving & reporting
│   │   ├── dim_seeds.py           # Dimensional seed data generation
│   │   ├── quality_events.py      # Data quality event tracking
│   │   ├── triage_evidence.py     # Triage evidence payload builder
│   │   └── report.py              # Markdown serving report generation
│   │
│   ├── alerts/                    # Alert workflow & operator interaction
│   │   ├── incident_workflow.py   # Anomaly-to-incident correlation + dedup
│   │   ├── routing.py             # Alert routing decisions
│   │   ├── teams.py               # Microsoft Teams MessageCard integration
│   │   └── feedback.py            # Operator feedback record validation
│   │
│   └── triage_service/            # LLM integration & recommendation service
│       ├── models.py              # Recommendation & ValidationResult dataclasses
│       ├── openai_provider.py     # OpenAI-compatible chat/completions client
│       ├── invocations.py         # LLM invocation audit (request hash, cost tracking)
│       ├── recommendations.py     # Rule-based fallback recommendation generator
│       ├── persist.py             # Recommendation persistence layer
│       └── validation.py          # Recommendation schema & content validation
│
├── scripts/                       # Shell & Python helper scripts
│   ├── full-e2e.sh                # Full local E2E launcher (all phases)
│   ├── flink-sql-client.sh        # Flink SQL CLI wrapper
│   ├── doris-sql.sh               # ClickHouse MySQL protocol wrapper (legacy name)
│   ├── e2e_data_pump.py           # Continuous data pump for E2E testing
│   ├── render_flink_sql.py        # Jinja2 Flink SQL template renderer
│   ├── render_clickhouse_sql.py   # Jinja2 ClickHouse SQL template renderer
│   ├── phase1-load-postgres-source-data.py
│   ├── phase3-load-triage.py
│   ├── phase3-reset.sh
│   └── phase4-load-incidents.py
│
├── tests/                         # 47 test files (~300+ tests)
│   ├── golden/                    # Golden reference data for replay verification
│   ├── test_generate_events.py    # Generator determinism, coverage, field validation
│   ├── test_schema_validation.py  # JSON Schema validation & DLQ routing
│   ├── test_anomaly_rules.py      # 5-detector anomaly rule engine tests
│   ├── test_replay_verification.py
│   ├── test_flink_enrichment.py
│   ├── test_flink_runtime_contract.py
│   ├── test_flink_agent_*.py      # 5 agent sub-component test files
│   ├── test_phase2_flink_agent_e2e.py
│   ├── test_phase3_doris_serving_e2e.py
│   ├── test_phase3_grafana_*.py   # 3 Grafana contract tests
│   ├── test_phase4_e2e_contract.py
│   ├── test_triage_recommendations.py
│   ├── test_llm_invocations.py
│   ├── test_openai_provider.py
│   └── ... (47 files total, 48 entries with __pycache__)
│
├── docker/grafana.Dockerfile      # Custom Grafana image
├── docker-compose.yml             # 11 services (Redpanda, Flink, PG, Connect, ClickHouse, Grafana, 3 agents, data-pump)
├── Makefile                       # ~50 build/run targets
├── pyproject.toml                 # Python project config (uv/pip)
├── pyrightconfig.json             # Type checker config
├── .github/workflows/ci.yml       # CI on push/PR to main
├── AGENTS.md                      # Agent guidance & claim boundary
└── LICENSE                        # Apache 2.0
```

---

## Pipeline Phases

### Phase 1 — Data Source Ingestion

Deterministic synthetic telemetry generation with schema contract validation.

**Generator profiles:**
- `smoke` — 1 tenant, 1 plant, 2 assets, minimal event coverage
- `demo` — 3 tenants, 3 plants, 10 assets each (180 tags)
- `industrial_demo` — 30-day operating state lifecycle (normal → degradation → warning → critical → maintenance → recovery)

**6 scenario types:** normal, late, missing_heartbeat, flatline, spike, drift, invalid_event

**11 operating states:** NORMAL, DEGRADATION, WARNING, CRITICAL, FAILURE, MAINTENANCE, RECOVERY, SHUTDOWN, COMMISSIONING, IDLE, OFFLINE

**Key commands:**
```bash
make generate-demo-data          # PROFILE=demo (default)
make generate-fault-data         # 30-day fault timelines
make validate-schemas            # Validate against raw_sensor_event schema
make phase1-ingestion-e2e        # Full E2E: PG → Debezium → Redpanda → Validate
```

### Phase 2 — Streaming (Kafka + Flink)

Docker-based Redpanda (Kafka-compatible) + Flink SQL event-time processing.

**Services & Ports:**

| Service | Port | UI |
|---------|------|----|
| Redpanda (Kafka) | `19092` | — |
| Redpanda Console | — | `http://localhost:18080` |
| Flink JobManager | — | `http://localhost:18081` |
| PostgreSQL | `15432` | — |
| Kafka Connect (Debezium) | — | `http://localhost:18083` |

**Topic naming:** `{tenant_id}.{layer}_{domain}__{model_name}.v1`
- `raw_apm__sensor_readings`, `raw_apm__dlq_events`
- `staging_apm__sensor_readings`, `staging_apm__dlq_events`
- `mart_apm__fct_anomaly_events`, `mart_apm__fct_late_sensor_readings`
- `mart_apm__fct_stream_health_snapshots`, `mart_apm__fct_realtime_diagnosis_events`

**Key commands:**
```bash
make stream-start                # Start Redpanda + Flink
make stream-health               # Health checks
make stream-create-topics        # Create all Kafka topics
make stream-publish-raw          # Generate + publish raw events
make flink-run-enrichment        # Submit enrichment SQL
make flink-run-anomalies         # Submit anomaly detection SQL
make flink-run-late-events       # Submit late event SQL
make flink-run-dlq-events        # Submit DLQ SQL
make flink-run-stream-health     # Submit stream health SQL
make stream-read-staging         # Consume staging topic (sample)
make phase2-e2e                  # Full Phase 2 E2E
```

### LLM Agents

Three containerized Python daemons (`flink-agent-1/2/3`) that:
1. Consume anomaly events from Kafka
2. Look up signal window, asset context, and failure mode evidence
3. Assemble triage evidence payload
4. Generate diagnosis via LLM (OpenAI-compatible) or fallback to rule-based recommendations
5. Validate output (cross-tenant isolation, banned wording, content completeness)
6. Produce validated `RealtimeDiagnosisEvent` back to Kafka

**LLM configuration** (in `.env`):
```
OPENAI_API_KEY=sk-...
OPENAI_MODEL=deepseek-v4-pro
OPENAI_BASE_URL=https://api.deepseek.com
```

When no API key is configured, agents fall back to rule-based recommendations (no LLM call).

### Phase 3 — Serving (ClickHouse + Grafana)

ClickHouse serves as the real-time analytics store with Kafka Engine materialized views and Routine Load for continuous ingestion.

**Services & Ports:**

| Service | Port | UI |
|---------|------|----|
| ClickHouse HTTP | `18123` | `http://localhost:18123/play` |
| ClickHouse Native | `19000` | — |
| Grafana | — | `http://localhost:13000` |

**Grafana credentials:** `admin` / `apm_demo` (anonymous access enabled)

**Key commands:**
```bash
make serving-start               # Start ClickHouse + Grafana + dependency services
make serving-init                 # Initialize ClickHouse schema & ADS views
make serving-load-jobs            # Start Kafka ingest materialized views
make serving-query                # Run inspection queries
make phase3-e2e                   # Full Phase 3 E2E
make phase3-load-triage           # Load triage evidence/recommendations
make phase3-report                # Generate serving report
make phase3-serving-visualization-e2e  # Doris (ClickHouse) → Grafana E2E
```

### Phase 4 — Alert Workflow & Operator Triage

Converts anomaly events into incident-level alerts with LLM-assisted triage and operator feedback loops.

**Key commands:**
```bash
make phase4-load-incidents        # Load incident & routing data
make phase4-e2e                   # Full Phase 4 E2E
```

**Alert routing channels:**
- **critical** → Teams card (simulated fallback when no webhook URL configured)
- **warning** → simulated ticket
- **info** → dashboard only

### Full E2E

```bash
# Run the full E2E pipeline (PostgreSQL → Debezium → Redpanda → Flink → Agents → ClickHouse → Grafana)
make full-e2e

# Or with environment knobs:
TENANTS=tenant_northwind SERVING=1 ./scripts/full-e2e.sh
TENANTS="tenant_northwind tenant_apac_ops tenant_euro_core" SERVING=0 ./scripts/full-e2e.sh
```

---

## Configuration

All pipeline behavior is driven by YAML configs under `configs/`:

| File | Purpose |
|------|---------|
| `tenants.yml` | Tenant registry (3 tenants) |
| `assets.yml` | Asset hierarchy: sites, plants, archetypes, assets, components |
| `scenarios.yml` | Scenario types (7) and generation profiles (smoke, demo) |
| `streaming.yml` | Kafka broker, Flink UI, topic templates, watermark/lateness settings |
| `anomaly_rules.yml` | 6 anomaly detection rules with full governance metadata |
| `failure_modes.yml` | 11 failure modes (FMECA) with detection signals, recommended checks, runbook references |
| `threshold_profiles.yml` | Sensor threshold profiles per archetype |
| `triage_prompt.yml` | LLM triage prompt configuration |
| `tag_mapping.yml` | Tag-to-asset measurement point mappings |
| `measurement_points.yml` | Measurement point definitions by asset type |
| `synthetic_scenarios.yml` | Synthetic scenario definitions for industrial demo |

---

## Testing

```bash
# Run all core tests (no Docker, no LLM required)
make test

# Run specific test files
.venv/bin/python -m pytest tests/test_generate_events.py -v
.venv/bin/python -m pytest tests/test_anomaly_rules.py -v
.venv/bin/python -m pytest tests/test_flink_agent_diagnosis_generator.py -v

# Run LLM-required tests (needs OPENAI_API_KEY in env)
.venv/bin/python -m pytest -m llm -v
```

**Test categories (47 files):**
- Generator: determinism, coverage, field validation, scenario contract
- Schema: JSON Schema validation, DLQ routing, error classification
- Anomaly rules: 5 detectors (threshold, z-score, drift, heartbeat, flatline)
- Flink enrichment: metadata join, DLQ routing
- Flink agents: consumer, context, evidence, diagnosis, output validation, E2E
- ClickHouse: migration contract, Serving E2E, Grafana contract, claim boundary
- Triage: recommendations, invocations, prompt guardrails, evidence, phase 4 E2E
- E2E: Phase 1 ingestion, Phase 2 Flink agent, Phase 3 serving, Phase 4 incident workflow

---

## CI/CD

GitHub Actions CI (`.github/workflows/ci.yml`):
- Runs on push/PR to `main`
- Python 3.13, installs with pip, runs `pytest`
- No Docker, no LLM API keys, no Teams webhook — runs only core deterministic tests

---

## Claim Boundary

This is a portfolio/current-project demonstration with synthetic data. When referencing this work:

**Supported claims:**
- Flink event-time processing with watermark semantics
- LLM-assisted alert triage for operator support
- Evidence-grounded root-cause hypotheses
- Data quality contract enforcement (JSON Schema)
- Multi-tenant pipeline with tenant isolation

**NOT supported claims:**
- Autonomous diagnosis or automatic remediation
- Real factory deployment or production SLA
- Remaining useful life (RUL) prediction
- Certified safety actions
- Real downtime reduction or MTTR improvement

---

## License

Apache 2.0 — see [LICENSE](LICENSE).
