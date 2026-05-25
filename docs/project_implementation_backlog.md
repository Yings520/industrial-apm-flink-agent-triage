# Project Implementation Backlog for AI Data Engineering Portfolio

Generated: 2026-05-24

Purpose: convert `interview_preparation_roadmap.md` into an implementation backlog that can be executed week by week. This backlog covers repo structure, milestones, tickets, acceptance criteria, GitHub artifacts, and resume-ready checkpoints for the Top 3 projects.

## Inputs

| Input | How it is used |
|---|---|
| `interview_preparation_roadmap.md` | Primary source for weekly execution focus, interview deliverables, GitHub artifacts, resume checkpoints, and claim boundaries. |
| `project_priority_scoring.md` | Confirms Top 1 / Top 2 / Top 3 priority and project scope order. |
| `top_project_designs_index.md` | Confirms Step 6 design completion, cross-project reuse, and resume-safe boundaries. |
| `project_design_realtime_industrial_apm_flink_agent.md` | Defines Top 1 architecture, MVP scope, metrics, failure modes, GitHub artifacts, and resume packaging. |
| `project_design_llm_response_evaluation_observability_pipeline.md` | Defines Top 2 architecture, MVP scope, metrics, failure modes, GitHub artifacts, and resume packaging. |
| `project_design_multi_tenant_ai_saas_data_platform.md` | Defines Top 3 architecture, MVP scope, metrics, failure modes, GitHub artifacts, and resume packaging. |
| `resume_bullets_ai_data_engineering.md` | Supplies resume-safe wording, role variants, and do-not-claim checklist. |
| `sc_workexperience.md` | Fact boundary for verified work, current work, portfolio evidence, and unsupported claims. |

## Findings

### 1. Implementation Strategy

Default GitHub packaging:

| Repo | Purpose | Why separate |
|---|---|---|
| `industrial-apm-flink-agent-triage` | Top 1: real-time industrial APM pipeline with Kafka/Flink and LLM-assisted alert triage. | Best standalone demo for streaming, Flink semantics, anomaly detection, LLM guardrails, and operator workflow. |
| `llm-response-eval-observability-pipeline` | Top 2: LLM / AI-agent response evaluation and observability pipeline. | Best standalone demo for AI data quality, golden datasets, quality gates, evaluation reports, and CI regression workflow. |
| `multi-tenant-ai-saas-data-platform` | Top 3: multi-tenant AI SaaS platform foundation with dbt/Dagster/governance/AI-ready marts. | Best work-experience anchor and platform story; integrates outputs from Top 1 and Top 2 as AI-ready marts. |
| `ai-data-engineering-portfolio-index` | Portfolio landing repo linking the three projects. | Makes GitHub scanning easy while preserving the integrated story. |

Execution principle:

1. Each project must be runnable or reviewable on its own.
2. Each project must include explicit synthetic/demo data boundaries.
3. Each project must expose architecture, data contracts, tests, demo flow, limitations, and resume-safe wording.
4. Top 1 and Top 2 should publish output schemas that Top 3 can consume as AI-ready platform marts.
5. Do not write portfolio implementation as verified employer production work unless new evidence is supplied.

### 2. Shared Portfolio Standards

| Standard | Required implementation |
|---|---|
| README | Problem, target roles, architecture, quickstart, demo flow, what is synthetic, measured metrics, limitations, resume-safe wording. |
| Architecture | Mermaid diagram plus static PNG export if convenient. Must show data flow, quality gates, storage layers, and serving/reporting surface. |
| Data contracts | `docs/data-contracts.md` plus schema files under `contracts/` or `schemas/`. |
| Tests | At least schema/data tests, deterministic metric tests, and one failure-mode test per project. |
| Demo | One reproducible 3-5 minute scenario with command list and expected outputs. |
| Metrics | Store local/demo metrics in `reports/metrics-summary.md`. No business impact claims unless measured and labeled as demo. |
| Resume checkpoint | `docs/resume-checkpoint.md` with allowed bullets, placement, evidence status, and do-not-claim notes. |
| Interview notes | `docs/interview-deep-dive.md` with architecture story, trade-offs, failure modes, metrics, and likely questions. |

### 3. Cross-Project Dependency Map

| Producer | Output | Consumer |
|---|---|---|
| Top 1 APM pipeline | `mart_apm_incident_summary`, `mart_stream_health`, `fact_operator_feedback` | Top 3 AI-ready marts and portfolio integration story. |
| Top 2 LLM eval pipeline | `fact_eval_run`, `fact_metric_result`, `fact_gate_decision`, `mart_agent_eval_summary` | Top 3 AI-ready marts and current Tamira AI-agent evaluation story. |
| Top 3 platform | `dim_tenant`, `dim_data_source`, `dim_data_contract`, `fact_quality_result` | Shared governance vocabulary for Top 1 and Top 2. |
| Portfolio index | Project links, screenshots, demo scripts, resume-safe summaries | Recruiters and interviewers. |

## Project 1: Real-time Industrial APM Data Pipeline With Flink And LLM-assisted Alert Triage

### Repo Structure

Recommended repo: `industrial-apm-flink-agent-triage`

```text
industrial-apm-flink-agent-triage/
  README.md
  docker-compose.yml
  Makefile
  .github/workflows/ci.yml
  configs/
    tenants.yml
    assets.yml
    anomaly_rules.yml
    triage_prompt.yml
  contracts/
    sensor_event.schema.json
    asset_metadata.schema.json
    anomaly_event.schema.json
    agent_explanation.schema.json
    operator_feedback.schema.json
  src/
    telemetry_generator/
      generate_events.py
      scenarios.py
      schema_validation.py
    flink_jobs/
      anomaly_detection.py
      event_time_utils.py
    serving/
      load_to_postgres.py
      db.py
    triage_service/
      context_builder.py
      prompt_builder.py
      output_validator.py
      fallback.py
    dashboards/
      streamlit_app.py
  dbt/
    dbt_project.yml
    models/
      staging/
      marts/
    tests/
  tests/
    test_schema_validation.py
    test_anomaly_rules.py
    test_out_of_order_events.py
    test_prompt_guardrails.py
    test_tenant_isolation.py
  docs/
    architecture.md
    data-contracts.md
    demo-script.md
    failure-modes.md
    production-mapping.md
    interview-deep-dive.md
    resume-checkpoint.md
  reports/
    metrics-summary.md
    sample-incident-report.md
```

### Milestones And Tickets

| Milestone | Ticket | Implementation task | Acceptance criteria | GitHub artifact | Resume-ready checkpoint |
|---|---|---|---|---|---|
| P1-M1: Synthetic data and contracts | P1-T1 | Create tenant, asset, sensor, APM, incident-label, and runbook configs. | At least 3 tenants, 20 assets, 5 metric types, 5 anomaly scenarios, and deterministic seed support. | `configs/*.yml`, `docs/data-contracts.md` | Can say "modeled tenant-aware industrial telemetry and APM event contracts" only under Projects. |
| P1-M1 | P1-T2 | Implement telemetry generator and schema validation. | Generator emits valid normal, late, missing heartbeat, flatline, spike, and drift events; invalid records are rejected with reasons. | `src/telemetry_generator/`, `tests/test_schema_validation.py` | Can mention synthetic telemetry only after generator and validation exist. |
| P1-M2: Kafka/Flink MVP | P1-T3 | Add Docker Compose for Kafka/Redpanda, Postgres, and local services. | `make up` starts required services; README lists ports and health checks. | `docker-compose.yml`, `Makefile`, README quickstart | No resume bullet yet; this is infrastructure setup. |
| P1-M2 | P1-T4 | Implement Flink event-time anomaly job. | Job handles watermarks, allowed lateness, keyed windows, late-event side output, and stable anomaly IDs. | `src/flink_jobs/`, `docs/architecture.md` | After passing tests, can mention "Flink event-time processing" as portfolio evidence. |
| P1-M2 | P1-T5 | Add rule/statistical anomaly detectors. | Missing heartbeat, flatline, threshold, rolling baseline, and rate-of-change rules produce labeled anomaly events. | `configs/anomaly_rules.yml`, `tests/test_anomaly_rules.py` | Can mention "rule-based anomaly detection on synthetic industrial streams." |
| P1-M3: Serving marts and quality | P1-T6 | Write anomaly and stream-health outputs to Postgres/DuckDB. | Curated anomaly, telemetry, late-event, and quality tables populate from a demo run. | `src/serving/`, sample SQL in docs | Can mention "serving marts for anomaly and stream-health review." |
| P1-M3 | P1-T7 | Add dbt staging and mart models. | dbt builds `mart_apm_incident_summary`, `mart_stream_health`, `mart_quality_flags`, and `mart_tenant_asset_health`. | `dbt/models/`, dbt docs screenshot | Can mention dbt marts once models and tests pass. |
| P1-M3 | P1-T8 | Add freshness, completeness, duplicate, range, schema, and tenant leakage checks. | Tests fail on injected bad data and pass on valid demo data. | `dbt/tests/`, `tests/test_tenant_isolation.py` | Can mention data quality checks and tenant leakage tests as portfolio evidence. |
| P1-M4: Observability and dashboard | P1-T9 | Build stream-health and incident dashboard. | Dashboard shows anomaly trend, late-event rate, quality failures, tenant health, and incident queue. | `src/dashboards/`, screenshots in README | Can mention "dashboards for stream health and incident review." |
| P1-M4 | P1-T10 | Produce local metrics summary. | Report includes event count, p95 ingest-to-alert latency, late-event rate, data quality failure rate, and anomaly counts, labeled demo/synthetic. | `reports/metrics-summary.md` | Metrics can be used only as local/demo metrics. |
| P1-M5: LLM-assisted triage | P1-T11 | Build context builder for anomaly triage. | Context includes anomaly evidence, metric windows, asset metadata, quality flags, and runbook snippets without cross-tenant data. | `src/triage_service/context_builder.py` | Can mention "evidence-grounded context construction." |
| P1-M5 | P1-T12 | Implement structured prompt, output validation, and fallback behavior. | Malformed or unsupported LLM output is rejected or quarantined; fallback summary is generated on LLM failure. | `src/triage_service/`, `tests/test_prompt_guardrails.py` | Use "LLM-assisted alert triage / operator support"; avoid autonomous diagnosis. |
| P1-M6: Alert workflow and feedback | P1-T13 | Add alert dedup, cooldown, severity routing, and operator feedback labels. | Demo shows duplicate anomalies grouped into one incident and feedback captured as structured data. | `reports/sample-incident-report.md` | Can mention feedback labels and alert-fatigue controls. |
| P1-M7: Failure modes and CI | P1-T14 | Add replay, out-of-order, schema drift, tenant leakage, LLM failure, and prompt guardrail tests. | CI runs tests and includes at least one intentional failure scenario documented in `docs/failure-modes.md`. | `.github/workflows/ci.yml`, `docs/failure-modes.md` | Can discuss failure-mode testing in interviews. |
| P1-M8: Portfolio polish | P1-T15 | Finalize README, architecture, demo script, production mapping, interview deep dive, and resume checkpoint. | New reader can run or review the project in under 15 minutes; demo path is explicit. | README, `docs/demo-script.md`, `docs/interview-deep-dive.md` | Project bullet becomes safe after implementation evidence exists. |

### Project 1 Definition Of Done

| Area | Done means |
|---|---|
| Runnable demo | One command sequence injects an anomaly, processes it through Kafka/Flink, writes marts, shows dashboard output, creates LLM-assisted triage, and captures feedback. |
| Technical depth | Watermarks, allowed lateness, keyed state, side outputs, idempotent sink or dedupe logic, quality flags, and prompt guardrails are visible in code or docs. |
| Tests | Schema, anomaly rules, out-of-order events, tenant isolation, prompt validation, and at least one replay/failure scenario pass in CI. |
| GitHub polish | README, architecture, data dictionary, screenshots, demo script, metrics report, limitations, and production mapping are complete. |
| Resume checkpoint | Bullet stays under Projects unless production Flink/LLM triage evidence is later confirmed. |

## Project 2: LLM Response Evaluation And Observability Pipeline

### Repo Structure

Recommended repo: `llm-response-eval-observability-pipeline`

```text
llm-response-eval-observability-pipeline/
  README.md
  Makefile
  docker-compose.yml
  .github/workflows/ci.yml
  configs/
    eval_config.yml
    gate_thresholds.yml
    judge_rubrics.yml
  data/
    source_snapshots/
    eval_cases/
    response_fixtures/
    feedback_events/
  contracts/
    dim_eval_case.schema.json
    dim_source_snapshot.schema.json
    fact_eval_run.schema.json
    fact_answer_attempt.schema.json
    fact_metric_result.schema.json
    fact_judge_result.schema.json
    fact_feedback.schema.json
    fact_gate_decision.schema.json
  src/
    eval_runner/
      run_eval.py
      target_adapter.py
      run_store.py
    metrics/
      retrieval_metrics.py
      grounding_metrics.py
      freshness_metrics.py
      access_safety_metrics.py
      cost_latency_metrics.py
    judge/
      judge_runner.py
      rubric_loader.py
      judge_validator.py
    gates/
      gate_decision.py
      regression_compare.py
    feedback/
      ingest_feedback.py
      promote_case.py
    reports/
      render_report.py
  dbt/
    dbt_project.yml
    models/
      staging/
      marts/
    tests/
  dagster_project/
    assets.py
    jobs.py
    definitions.py
  tests/
    test_dataset_integrity.py
    test_metrics_grounding.py
    test_access_leakage.py
    test_gate_decision.py
    test_judge_validator.py
  docs/
    architecture.md
    data-contracts.md
    metric-taxonomy.md
    demo-script.md
    failure-modes.md
    interview-deep-dive.md
    resume-checkpoint.md
  reports/
    baseline_eval_report.md
    current_eval_report.md
    regression_gate_report.md
```

### Milestones And Tickets

| Milestone | Ticket | Implementation task | Acceptance criteria | GitHub artifact | Resume-ready checkpoint |
|---|---|---|---|---|---|
| P2-M1: Dataset contracts | P2-T1 | Define eval case, source snapshot, response attempt, metric result, judge result, feedback, and gate schemas. | Schemas include dataset version, source snapshot ID, tenant/access tags, evidence IDs, rubric ID, run ID, and prompt/model metadata. | `contracts/`, `docs/data-contracts.md` | Can discuss evaluation data modeling once contracts exist. |
| P2-M1 | P2-T2 | Create 100 seed eval cases across 8 failure categories. | Cases cover factual lookup, multi-hop, stale source, missing evidence, access-restricted query, retrieval miss, ambiguous query, fallback/refusal. | `data/eval_cases/`, dataset summary in README | Can mention "100-case demo eval set" only as project metric. |
| P2-M1 | P2-T3 | Create source snapshots and expected evidence mapping. | Each critical case has accepted evidence, negative evidence when relevant, source hash, freshness timestamp, and access tag. | `data/source_snapshots/` | Can discuss source attribution and freshness evaluation. |
| P2-M2: Response capture | P2-T4 | Implement target adapter using stored fixtures first. | Eval run can execute without real LLM API cost; captures response text, citations/context IDs, latency, token/cost placeholders, and errors. | `src/eval_runner/target_adapter.py` | Keeps project reproducible and interview-safe. |
| P2-M2 | P2-T5 | Implement eval run store. | Every run records run type, dataset version, source snapshot, model/prompt/retriever config, code commit placeholder, status, start/end time. | `src/eval_runner/run_store.py`, sample run outputs | Can explain immutable eval run metadata. |
| P2-M3: Deterministic metrics | P2-T6 | Implement grounding, citation, freshness, access leakage, latency, cost, and dataset integrity metrics. | Metrics produce `fact_metric_result` rows and fail on seeded bad examples. | `src/metrics/`, metric tests | Can mention deterministic metrics after tests pass. |
| P2-M3 | P2-T7 | Add retrieval metrics if retrieved context IDs exist. | recall@k, source hit rate, and context precision work on fixture context IDs; docs explain boundary if no full retriever exists. | `src/metrics/retrieval_metrics.py` | Avoid claiming full RAG/vector ownership. |
| P2-M4: dbt and Dagster | P2-T8 | Build raw/staging/mart dbt models for eval runs, metric results, feedback, and gate summaries. | dbt tests check uniqueness, required fields, relationships, access leakage counts, and freshness flags. | `dbt/models/`, dbt docs | Can mention evaluation marts and dbt quality checks. |
| P2-M4 | P2-T9 | Add Dagster assets/jobs for eval workflow. | Dagster job runs dataset load, target response capture, metric computation, dbt build, report generation, and gate decision. | `dagster_project/`, Dagster screenshot | Strong evidence for observable evaluation workflow. |
| P2-M5: Gates and reports | P2-T10 | Implement pass/warn/fail gate service and baseline/current comparison. | Gate report blocks access leakage, critical grounding failures, stale critical cases, excessive cost, and latency threshold failures. | `src/gates/`, `reports/regression_gate_report.md` | Can discuss CI/release gate story. |
| P2-M5 | P2-T11 | Render Markdown/HTML eval report. | Report includes dataset version, source snapshot, run config, pass/fail summary, top regressions, failed cases, quality metrics, cost, latency. | `src/reports/`, sample reports | Best GitHub artifact for recruiters and interviewers. |
| P2-M6: Bounded LLM-as-judge | P2-T12 | Add optional judge runner, rubric loader, metadata capture, and output validator. | Judge output stores model/prompt/rubric/temperature; judge failure is handled; deterministic gates remain primary. | `src/judge/`, `tests/test_judge_validator.py` | Use "bounded LLM-as-judge as auxiliary signal." |
| P2-M7: Feedback loop | P2-T13 | Implement feedback ingestion and case promotion workflow. | Feedback labels include incorrect, missing source, stale, unsafe, wrong tenant, helpful; promoted cases require review status. | `src/feedback/`, sample feedback events | Can discuss feedback-to-regression loop. |
| P2-M7 | P2-T14 | Add access leakage, dataset contamination, judge drift, and metric gaming examples to failure docs. | Each failure mode has detection, mitigation, and interview explanation. | `docs/failure-modes.md` | Can answer hard AI reliability questions. |
| P2-M8: Portfolio polish | P2-T15 | Finalize README, architecture, metric taxonomy, demo script, reports, interview deep dive, and resume checkpoint. | New reader can run baseline vs current eval and see one gate failure. | README, `docs/demo-script.md`, `docs/interview-deep-dive.md` | Current-work wording depends on actual Tamira implementation truth; portfolio bullets safe after demo exists. |

### Project 2 Definition Of Done

| Area | Done means |
|---|---|
| Runnable demo | A baseline/current eval run produces metric rows, dbt marts, gate decision, and a Markdown/HTML report. |
| Technical depth | Versioned eval cases, source snapshots, deterministic metrics, optional bounded judge, feedback promotion, quality gates, and run metadata are implemented or explicitly documented. |
| Tests | Dataset integrity, grounding, access leakage, gate decision, and judge output validation tests pass in CI. |
| GitHub polish | README, architecture, data contracts, metric taxonomy, sample reports, dashboard/report screenshots, demo script, limitations, and failure modes are complete. |
| Resume checkpoint | Tamira current-work bullet may use `building/designed/built` only according to implementation truth; full RAG/vector/hallucination reduction claims remain out. |

## Project 3: Multi-tenant AI SaaS Data Platform For Industrial And Agentic Workloads

### Repo Structure

Recommended repo: `multi-tenant-ai-saas-data-platform`

```text
multi-tenant-ai-saas-data-platform/
  README.md
  Makefile
  docker-compose.yml
  .github/workflows/ci.yml
  configs/
    tenants.yml
    data_sources.yml
    data_contracts.yml
    policies.yml
    quotas.yml
  contracts/
    dim_tenant.schema.json
    dim_data_source.schema.json
    dim_data_contract.schema.json
    fact_ingestion_run.schema.json
    fact_quality_result.schema.json
    fact_cost_usage.schema.json
    mart_ai_agent_eval_summary.schema.json
    mart_apm_incident_summary.schema.json
  src/
    ingestion/
      file_ingest.py
      api_ingest.py
      kafka_ready_events.py
      contract_validator.py
    governance/
      policy_engine.py
      tenant_filter.py
      audit_logger.py
      leakage_tests.py
    observability/
      cost_usage.py
      freshness.py
      quality_summary.py
    dashboards/
      streamlit_app.py
  dbt/
    dbt_project.yml
    models/
      staging/
      intermediate/
      marts/
      ai_ready/
    tests/
  dagster_project/
    assets.py
    schedules.py
    sensors.py
    definitions.py
  tests/
    test_contract_validator.py
    test_policy_engine.py
    test_tenant_leakage.py
    test_quality_gates.py
    test_backfill_scenario.py
  docs/
    architecture.md
    data-contracts.md
    governance-model.md
    dbt-modeling-standards.md
    dagster-operations.md
    demo-script.md
    failure-modes.md
    production-mapping.md
    interview-deep-dive.md
    resume-checkpoint.md
  reports/
    platform-health-report.md
    quality-gate-report.md
    cost-usage-summary.md
```

### Milestones And Tickets

| Milestone | Ticket | Implementation task | Acceptance criteria | GitHub artifact | Resume-ready checkpoint |
|---|---|---|---|---|---|
| P3-M1: Tenant control plane | P3-T1 | Define tenant, source, contract, policy, quota, retention, and ownership configs. | At least 3 tenants, 4 source types, retention tiers, quotas, access tiers, and owner metadata. | `configs/*.yml`, `docs/governance-model.md` | Can discuss tenant control plane as project/platform design evidence. |
| P3-M1 | P3-T2 | Implement contract validator for required fields, tenant keys, schema versions, and freshness SLOs. | Validator catches missing tenant key, schema drift, invalid source, and stale source examples. | `src/ingestion/contract_validator.py`, tests | Can mention data contracts after tests exist. |
| P3-M2: Ingestion and raw/staging | P3-T3 | Build file/API ingestion and Kafka-ready event examples. | Demo ingests synthetic tenant metadata, source records, APM-like events, and agent-eval summaries into raw/staging tables. | `src/ingestion/`, sample data | Can mention multi-source ingestion in project docs. |
| P3-M2 | P3-T4 | Add raw/staging tables and source metadata. | Every tenant-scoped table includes `tenant_id`, `source_id`, load timestamp, and quality status. | dbt staging models | Can explain lake-to-warehouse layering. |
| P3-M3: dbt core marts | P3-T5 | Build tenant health, source freshness, ingestion summary, quality result, and usage marts. | At least 10 dbt models; tests cover uniqueness, non-null tenant keys, relationships, freshness, and row-count sanity. | `dbt/models/`, dbt docs | Strong Data Platform Engineer evidence. |
| P3-M3 | P3-T6 | Write dbt modeling standards. | Docs define staging/intermediate/mart/AI-ready layers, naming, tests, metrics, and exposures. | `docs/dbt-modeling-standards.md` | Can discuss standardization and metrics-as-code. |
| P3-M4: Dagster orchestration | P3-T7 | Add Dagster assets, schedules, sensors, retries, freshness metadata, and backfill examples. | At least 10 assets; one backfill scenario and one failed-asset recovery path documented. | `dagster_project/`, Dagster screenshots | Can mention observable orchestration in portfolio; verified Tamira Dagster already supports work-experience story. |
| P3-M5: Governance and quality gates | P3-T8 | Implement simulated RBAC/RLS, policy-as-data, audit logs, and tenant filtering. | Queries/tests prove tenant A cannot access tenant B data; policy decisions are logged. | `src/governance/`, `docs/governance-model.md` | Use "simulated RBAC/RLS and audit logs" unless verified as real work. |
| P3-M5 | P3-T9 | Add quality gate service. | Critical quality failures block affected AI-ready marts; reports show pass/warn/fail outcomes. | `reports/quality-gate-report.md` | Can mention quality gates in project bullets. |
| P3-M6: AI-ready marts | P3-T10 | Add `mart_ai_agent_eval_summary` compatible with Top 2 outputs. | Mart includes eval run ID, query count, grounding pass rate, freshness violation rate, p95 latency, cost, and gate status. | `dbt/models/ai_ready/` | Connects current Tamira AI-agent evaluation story to platform foundation. |
| P3-M6 | P3-T11 | Add `mart_apm_incident_summary` compatible with Top 1 outputs. | Mart includes tenant, asset, incident count, severity, quality flags, stream health, feedback count, and triage status. | `dbt/models/ai_ready/` | Connects APM/Flink portfolio story to platform foundation. |
| P3-M7: Cost/resource observability | P3-T12 | Implement cost/resource proxy metrics by tenant/source/run. | Dashboard/report shows bytes processed, run duration, estimated compute group, quota group, and cost proxy. | `reports/cost-usage-summary.md` | Only verified work metric remains Tamira 60%; project metrics labeled demo. |
| P3-M7 | P3-T13 | Build platform dashboard. | Dashboard shows tenant health, source freshness, quality failures, cost/resource usage, AI-ready mart status, and asset health. | `src/dashboards/`, screenshots | Strong GitHub scanning artifact. |
| P3-M8: Portfolio polish | P3-T14 | Finalize README, architecture, dbt docs, Dagster docs, demo script, production mapping, interview deep dive, and resume checkpoint. | New reader can run tenant/source ingestion, dbt build, quality gates, and view platform reports. | README, docs, screenshots | Top 3 can be the strongest work-experience anchor where facts are verified. |

### Project 3 Definition Of Done

| Area | Done means |
|---|---|
| Runnable demo | A demo adds tenant/source configs, ingests synthetic data, validates contracts, runs Dagster/dbt, applies policy tests, builds AI-ready marts, and generates platform health/cost/quality reports. |
| Technical depth | Tenant control plane, contract validation, dbt layers, Dagster asset graph, simulated governance, quality gates, AI-ready marts, and cost/resource observability are present. |
| Tests | Contract validator, policy engine, tenant leakage, quality gates, and backfill scenario tests pass in CI. |
| GitHub polish | README, architecture, data contracts, governance model, dbt standards, Dagster operations, demo script, screenshots, reports, and production mapping are complete. |
| Resume checkpoint | Verified Tamira platform bullets can stay in Work Experience; simulated governance and AI-ready extensions stay under Projects unless verified. |

## Portfolio Index Repo

### Repo Structure

Recommended repo: `ai-data-engineering-portfolio-index`

```text
ai-data-engineering-portfolio-index/
  README.md
  docs/
    portfolio-map.md
    claim-boundary.md
    target-role-positioning.md
    interview-script.md
    resume-project-section.md
  assets/
    architecture-overview.png
    apm-demo-screenshot.png
    eval-report-screenshot.png
    platform-dashboard-screenshot.png
```

### Tickets

| Ticket | Implementation task | Acceptance criteria | GitHub artifact | Resume-ready checkpoint |
|---|---|---|---|---|
| IDX-T1 | Create portfolio map linking the three repos. | README explains why the three projects form one AI-ready data platform narrative. | README, `docs/portfolio-map.md` | Gives recruiter-friendly project ordering. |
| IDX-T2 | Add claim-boundary page. | Clearly separates verified work, current work, portfolio projects, planned extensions, and do-not-claim items. | `docs/claim-boundary.md` | Prevents overclaiming during interviews. |
| IDX-T3 | Add target-role positioning page. | Includes variants for Senior Data Engineer, Data Platform Engineer, AI Data Engineer, and ML Data Engineer. | `docs/target-role-positioning.md` | Helps choose resume/project emphasis by JD. |
| IDX-T4 | Add interview script. | Includes 90-second pitch, 2-minute project summaries, and hard-question answers. | `docs/interview-script.md` | Turns implementation into interview performance. |
| IDX-T5 | Add resume project section. | Contains final safe project bullets with placement and evidence status. | `docs/resume-project-section.md` | Ready to paste after implementation truth is checked. |

## 8-Week Execution Schedule

| Week | Primary build focus | Project 1 | Project 2 | Project 3 | Portfolio / resume output |
|---:|---|---|---|---|---|
| 1 | Contracts and repo skeletons | P1-T1, P1-T2 | P2-T1, P2-T2, P2-T3 | P3-T1, P3-T2 | IDX-T1, initial claim boundary, master resume section draft. |
| 2 | Data generation and ingestion | P1-T3, P1-T4 | P2-T4, P2-T5 | P3-T3, P3-T4 | First architecture diagrams and README quickstarts. |
| 3 | Core logic | P1-T5, P1-T6 | P2-T6, P2-T7 | P3-T5, P3-T6 | Draft project bullets only after each core feature is demonstrable. |
| 4 | dbt/Dagster and quality | P1-T7, P1-T8 | P2-T8, P2-T9 | P3-T7 | dbt docs, Dagster screenshots, quality gate examples. |
| 5 | AI guardrails and gates | P1-T11, P1-T12 | P2-T10, P2-T11, P2-T12 | P3-T8, P3-T9 | Hard-question notes: hallucination, access leakage, judge drift. |
| 6 | Integration marts | P1-T9, P1-T13 | P2-T13 | P3-T10, P3-T11 | IDX-T2, cross-project platform story. |
| 7 | Failure modes and observability | P1-T10, P1-T14 | P2-T14 | P3-T12, P3-T13 | `docs/interview-deep-dive.md` in all repos, STAR story bank. |
| 8 | Polish and application readiness | P1-T15 | P2-T15 | P3-T14 | IDX-T3, IDX-T4, IDX-T5, final resume/project section. |

## Acceptance Criteria Summary

| Project | Minimum shippable GitHub demo | Minimum interview proof | Minimum resume checkpoint |
|---|---|---|---|
| Top 1 APM/Flink | Inject synthetic incident -> Kafka/Flink -> anomaly mart -> dashboard -> LLM-assisted triage -> feedback. | Can explain Flink event time, watermarks, checkpointing, late events, idempotent sink boundary, alert fatigue, and LLM guardrails. | Project bullet only unless real production Flink/LLM evidence is confirmed. |
| Top 2 LLM Eval | Run baseline/current eval -> deterministic metrics -> dbt marts -> gate report -> feedback promotion. | Can explain golden dataset, source snapshots, grounding, freshness, access safety, judge boundary, CI smoke/full eval, and regression gates. | Current Tamira work wording depends on actual implementation truth; portfolio metrics labeled demo. |
| Top 3 Platform | Add tenant/source -> ingest -> contract validation -> dbt/Dagster -> policy tests -> AI-ready marts -> dashboard. | Can explain tenant isolation, data contracts, lake-to-warehouse-to-AI layers, dbt/Dagster, governance, quality gates, and cost controls. | Verified Tamira platform bullets safe; simulated extensions separated under Projects. |
| Portfolio index | Recruiter can understand the three-project narrative in 3 minutes. | Can answer "why these projects" and "what is real vs portfolio." | Resume project section is traceable and does not overclaim. |

## Resume-Ready Checkpoints

Use these only after the matching acceptance criteria are met.

| Checkpoint | Safe bullet direction | Evidence status |
|---|---|---|
| P1 core streaming complete | Built a portfolio-grade real-time industrial APM pipeline using Kafka and Flink event-time processing to detect late, missing, and anomalous sensor/APM events across synthetic tenant/device streams. | Portfolio project |
| P1 LLM triage complete | Integrated rule-based anomaly detection with LLM-assisted alert triage, generating evidence-grounded summaries, root-cause hypotheses, and recommended next checks for operator review. | Portfolio project |
| P1 tests and observability complete | Added replay tests, out-of-order event tests, schema contract tests, prompt guardrail tests, and dashboards for stream health, anomaly trends, and LLM triage reliability. | Portfolio project |
| P2 eval workflow complete | Designed an LLM Response Evaluation and Observability Pipeline with versioned evaluation datasets, verified query libraries, response traces, feedback ingestion, regression gates, and response-quality metrics. | Current work or portfolio, depending on implementation truth |
| P2 dbt/Dagster complete | Built a Dagster-orchestrated evaluation workflow with dbt/SQL checks, immutable eval runs, baseline/current comparison, and dashboards for quality, traceability, stability, freshness, latency, and cost. | Current work or portfolio, depending on implementation truth |
| P2 gates complete | Implemented deterministic relevance, grounding, freshness, traceability, access-safety, latency, and cost metrics, using bounded LLM-as-judge scoring only as an auxiliary signal. | Current work or portfolio, depending on implementation truth |
| P3 platform core complete | Designed a multi-tenant AI SaaS data platform blueprint with tenant control plane, data contracts, dbt marts, Dagster orchestration, quality gates, and cost/resource dashboards. | Verified work anchor plus portfolio extension |
| P3 governance complete | Implemented synthetic tenant/source configs, simulated RBAC/RLS, audit tables, and tenant leakage tests to demonstrate governed AI-ready data products. | Portfolio extension unless verified at Tamira |
| P3 AI-ready marts complete | Published AI-ready marts for industrial APM, agent evaluation, feedback, tenant health, and source freshness, with lineage back to raw and curated assets. | Portfolio extension or current work if verified |

## Do-Not-Claim Checklist

| Unsupported claim | Safer replacement |
|---|---|
| Owned production Flink platform | Built portfolio/current-project Flink event-time pipeline with synthetic industrial APM data. |
| Autonomous diagnosis or root cause identification | LLM-assisted alert triage, operator support, and root-cause hypotheses with evidence. |
| Reduced hallucinations in production | Detected unsupported claims or grounding failures in a demo evaluation suite. |
| Owned full RAG/vector infrastructure | Built evaluation, source snapshot, metadata, and retrieval-quality project evidence; keep vector/RAG as extension unless implemented. |
| Delivered formal GDPR/APRA/OAIC/SOC2 compliance | Simulated RBAC/RLS, audit logs, policy-as-data, retention metadata, and tenant leakage tests. |
| Built full MLOps/model serving/feature store | Built AI/ML-oriented data platform foundations, evaluation workflows, and AI-ready marts. |
| New business KPI or cost savings from portfolio | Report local/demo metrics only; keep verified work metrics separate. |

## Open Questions

| Question | Impact on execution |
|---|---|
| Should the three project repos be implemented separately or as one mono-repo? | This backlog defaults to separate repos plus portfolio index for GitHub scanning; one mono-repo would reduce setup overhead but may be harder for recruiters to scan. |
| Should Top 1 use PyFlink, Java Flink, or Flink SQL? | PyFlink is faster for portfolio delivery; Java Flink may improve Flink credibility; Flink SQL is simplest but may show less stateful depth. |
| Will Top 2 call a real LLM API or use response fixtures only? | Fixtures make CI reproducible; API calls make the demo more vivid but add cost/secrets/failure handling. |
| Is the Tamira AI-agent evaluation pipeline already integrated with the same multi-tenant platform? | If yes, Top 2 and Top 3 can be packaged as one stronger current-work story; if no, present them as related but separate workflows. |
| Are RBAC/RLS, audit logs, retention, data classification, or PII masking verified work? | Verified evidence would allow stronger governance wording in Work Experience; otherwise keep it as simulated portfolio controls. |

## Downstream Inputs

1. The next execution artifact can be a concrete `week_1_execution_plan.md` that starts with repo skeletons, contracts, synthetic configs, and portfolio index setup.
2. For implementation, start with schema/data-contract tickets before dashboards or LLM features; this keeps the projects data-engineering-led.
3. For GitHub, every repo should be readable before it is fully runnable: README, architecture diagram, data contracts, and limitations should land early.
4. For resume, do not promote a project bullet from planned to completed until its acceptance criteria and GitHub artifact exist.
5. For interviews, every completed milestone should produce one hard-question answer and one failure-mode story.
6. Final reporting should present the portfolio as one coherent AI-ready data platform: Top 3 is the foundation, Top 1 is the streaming/industrial data product, and Top 2 is the AI response evaluation data product.
