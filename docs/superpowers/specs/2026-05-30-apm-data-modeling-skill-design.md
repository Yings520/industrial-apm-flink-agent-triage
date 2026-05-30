# APM Data Modeling Skill Design

Date: 2026-05-30

## Purpose

Create a reusable personal Codex skill named `apm-data-modeling`.

Use this skill when designing, reviewing, or extending data models for Asset Performance Management (APM), Industrial IoT, condition monitoring, predictive maintenance evidence, anomaly events, asset health data products, and LLM-assisted operator triage.

The skill should help Codex produce industrially meaningful data models instead of generic sensor tables. It should guide Codex to reason from asset context, reliability semantics, telemetry quality, time-series behavior, operational workflows, and evidence boundaries before generating artifacts.

## Core Positioning

The skill follows four guiding principles:

- Design first, artifacts second.
- Standards-aware, not compliance-claiming.
- Evidence-grounded, not diagnosis-claiming.
- Asset-context-first, not sensor-table-first.

The skill is reusable across future APM, IIoT, condition monitoring, and predictive maintenance projects. It must not depend on this repository's file names, schemas, or implementation details.

## In Scope

The skill should be used for:

- APM data model design.
- Industrial asset hierarchy modeling.
- Sensor tag contextualization.
- Telemetry schema and event contract design.
- Anomaly event, quality event, and asset health modeling.
- Failure mode, reliability, and FMECA-oriented data structures.
- Predictive-maintenance evidence modeling.
- Work order, CMMS, and EAM integration modeling.
- LLM-assisted operator triage evidence modeling.
- Review of APM schemas, tables, fields, APIs, quality checks, and terminology.

## Out Of Scope

The skill must not default to claims of:

- Production-grade APM deployment.
- Safety-certified system behavior.
- Autonomous root-cause diagnosis.
- Automatic remediation.
- Guaranteed RUL prediction.
- Guaranteed downtime or MTTR reduction.
- ISO, IEC, or ISA standard compliance.
- Accurate failure prediction without failure labels, maintenance history, and validation evidence.

## Design-First Workflow

### 1. Classify The Modeling Task

First classify what the user is asking for. Common task types include:

- Asset registry or asset hierarchy.
- Sensor tag contextualization.
- Raw telemetry contract.
- Enriched telemetry or condition signal model.
- Condition monitoring model.
- Anomaly event model.
- Asset health score model.
- Failure mode or FMECA model.
- Maintenance or work order integration.
- Predictive-maintenance evidence model.
- LLM-assisted triage evidence model.
- Operator feedback or closed-loop learning model.
- Dashboard, API, or serving model.

If the requested system spans many independent domains, decompose it before designing artifacts.

### 2. Ask For Missing Context

Do not generate schemas or DDL immediately when critical context is missing. Ask about:

- Asset objects: equipment, lines, plants, components, sensors, software services, or facilities.
- Hierarchy: site, area, plant, line, cell, asset, component, sensor tag, functional location.
- Data sources: PLC, SCADA, historian, PI System, MQTT, Kafka, EAM, CMMS, manual inspection, lab system, or operator entry.
- Time semantics: event time, ingest time, detected time, window time, sampling rate, lateness, out-of-order arrival, and watermarking.
- Business action: alerting, inspection, maintenance planning, reliability analysis, operator triage, dashboarding, or model training.
- Data quality concerns: missing, late, duplicate, flatline, out of range, unit mismatch, stale metadata, schema drift, and tenant leakage.
- Labels and evidence: failure labels, work orders, inspection records, runbooks, operator feedback, and historical incidents.
- Boundaries: multitenancy, OT cybersecurity, safety-instrumented systems, and compliance goals.

### 3. Produce Design Before Artifacts

Before writing artifacts, present a design that includes:

- Core entities.
- Entity relationships.
- Grain for each fact/event/model.
- Required fields.
- Time semantics.
- Quality signals.
- Downstream consumers.
- Assumptions.
- Out-of-scope claims.
- Validation strategy.

Example conceptual chain:

```text
Asset -> Component -> Sensor Tag -> Telemetry Reading
Telemetry Reading -> Quality Event
Telemetry Window -> Anomaly Event
Anomaly Event -> Triage Evidence -> Operator Feedback
Asset -> Failure Mode -> Recommended Inspection
```

### 4. Generate Artifacts After Confirmation

After design confirmation, generate the requested artifacts:

- JSON Schema.
- SQL DDL.
- dbt models and tests.
- OpenAPI contracts.
- Data dictionary.
- Quality checks.
- Unit or integration tests.
- ERD or lineage description.
- Sample records.

### 5. Review The Output

After producing any design or artifact, review for:

- Explicit asset, component, tag, metric, and unit context.
- Clear distinction between event time, ingest time, detected time, and window time.
- Quality signal propagation.
- Separation of anomaly, degradation, suspected failure mode, confirmed fault, diagnosis, and remediation.
- LLM evidence boundaries.
- Standards wording that avoids compliance claims.
- Source traceability to tags, events, rules, models, work orders, inspections, or runbooks.

## Standards-Aware Modeling

Use industrial standards as modeling lenses, not automatic compliance claims. Select the relevant lens based on the modeling problem.

| Modeling Problem | Useful Lens |
| --- | --- |
| Asset management goals, lifecycle, value, risk, and governance | ISO 55000 / ISO 55001 |
| Reliability and maintenance data, failure modes, equipment taxonomy | ISO 14224 |
| Enterprise, MES, SCADA, PLC, and equipment boundaries | ISA-95 / IEC 62264 |
| Batch/process hierarchy, equipment modules, control modules | ISA-88 / IEC 61512 |
| Reference designation, functional location, structured asset identifiers | IEC 81346 |
| Condition monitoring program design | ISO 17359 |
| Condition monitoring data processing, health assessment, diagnosis/advisory pipeline | ISO 13374 |
| Condition-based maintenance architecture | MIMOSA OSA-CBM |
| Semantic models, digital twins, submodels, industrial interoperability | Asset Administration Shell |
| SIS, SIF, SIL, and process safety boundaries | ISA-84 / IEC 61511 |
| OT/ICS cybersecurity, zones, conduits, and access boundaries | ISA/IEC 62443 |
| Large process-plant lifecycle data integration | ISO 15926 |

Recommended phrasing:

```text
inspired by ISO 14224 reliability-data concepts
aligned with ISA-95-style enterprise/control boundaries
uses AAS-style semantic submodel thinking
```

Avoid phrasing such as:

```text
ISO 14224 compliant
IEC 61511 certified
ISA-95 certified architecture
```

Never claim standard compliance unless the user explicitly asks for compliance-oriented modeling and provides enough domain evidence.

Treat ISA-84 / IEC 61511 safety-instrumented-system data as safety-context data, not ordinary APM control authority. Do not suggest that APM anomaly models can trigger safety actions or satisfy SIS requirements unless the user explicitly scopes a safety-certified engineering workflow.

## Core APM Data Domains

### Asset Registry / Asset Hierarchy

Purpose: describe what the asset is, where it is, who owns it, and how critical it is.

Typical fields:

- `asset_id`
- `asset_name`
- `asset_type`
- `asset_class`
- `site_id`
- `area_id`
- `plant_id`
- `line_id`
- `component_id`
- `parent_asset_id`
- `functional_location`
- `criticality`
- `manufacturer`
- `model`
- `commissioned_at`
- `operating_context`
- `lifecycle_state`

Principle: Asset hierarchy should support both physical structure and functional location. Do not assume one fixed hierarchy for every industry.

### Sensor Tag / Measurement Point

Purpose: map raw tags to assets, components, measurement points, metrics, units, and source systems.

Typical fields:

- `tag_id`
- `tag_name`
- `asset_id`
- `component_id`
- `measurement_point`
- `metric_name`
- `unit`
- `sampling_rate`
- `source_system`
- `signal_type`
- `normal_range_min`
- `normal_range_max`
- `engineering_limits`
- `calibration_status`

Principle: A raw tag is not an asset signal until it has asset, metric, unit, and measurement-point context.

### Raw Telemetry Event

Purpose: preserve source time-series data without erasing lineage too early.

Typical fields:

- `event_id`
- `tag_id`
- `event_time`
- `ingest_time`
- `value`
- `unit`
- `source_system`
- `quality_flags`
- `schema_version`
- `raw_payload_ref`

Principle: Raw telemetry must preserve source timing and source identity. Normalize later, but do not erase lineage.

### Enriched Telemetry / Condition Signal

Purpose: contextualize raw telemetry into asset-level signals for monitoring and analytics.

Typical fields:

- `event_id`
- `asset_id`
- `component_id`
- `tag_id`
- `metric_name`
- `event_time`
- `window_start`
- `window_end`
- `value`
- `normalized_value`
- `unit`
- `operating_mode`
- `quality_flags`
- `threshold_profile_id`

Principle: Enriched telemetry should carry quality context forward into analytics and triage.

### Quality Event

Purpose: make telemetry quality issues queryable, alertable, and available to downstream confidence decisions.

Typical fields:

- `quality_event_id`
- `event_time`
- `detected_at`
- `asset_id`
- `tag_id`
- `quality_type`
- `severity`
- `affected_window_start`
- `affected_window_end`
- `description`
- `source_event_ids`
- `resolution_status`

Typical quality types:

- `missing`
- `late`
- `duplicate`
- `flatline`
- `out_of_range`
- `unit_mismatch`
- `schema_drift`
- `stale_metadata`
- `tenant_leakage`
- `sensor_fault_suspected`

### Anomaly Event

Purpose: describe abnormal behavior without treating it as confirmed failure.

Typical fields:

- `anomaly_id`
- `asset_id`
- `component_id`
- `tag_id`
- `metric_name`
- `rule_id`
- `model_id`
- `window_start`
- `window_end`
- `observed_value`
- `baseline_value`
- `expected_value`
- `severity`
- `detection_confidence`
- `quality_flags`
- `source_event_ids`
- `status`

Principle: An anomaly event is evidence of abnormal behavior, not proof of failure or root cause.

### Failure Mode / Reliability Context

Purpose: provide interpretation context for condition monitoring and maintenance decisions.

Typical fields:

- `failure_mode_id`
- `asset_type`
- `component_type`
- `failure_mode_name`
- `failure_effect`
- `criticality`
- `detectability`
- `recommended_inspections`
- `related_metrics`
- `pf_interval_hint`
- `maintenance_strategy`

Principle: Failure modes provide interpretation context. They should not turn unconfirmed telemetry anomalies into confirmed faults.

### Maintenance / Work Order Integration

Purpose: connect APM signals to operational maintenance actions and feedback.

Typical fields:

- `work_order_id`
- `asset_id`
- `failure_mode_id`
- `created_at`
- `completed_at`
- `work_type`
- `priority`
- `problem_code`
- `cause_code`
- `remedy_code`
- `technician_notes`
- `downtime_minutes`
- `parts_used`
- `source_system`

Principle: Work orders and inspections are often noisy labels. Treat them as operational evidence, not perfect ground truth.

### Triage Evidence

Purpose: provide deterministic, traceable evidence for LLM-assisted or operator-support workflows.

Typical fields:

- `evidence_id`
- `anomaly_id`
- `asset_id`
- `evidence_version`
- `metric_window_summary`
- `quality_caveats`
- `asset_context`
- `recent_anomalies`
- `related_work_orders`
- `runbook_refs`
- `source_refs`

Principle: Triage evidence should be deterministic. LLM output should not create new evidence.

### Operator Feedback

Purpose: close the learning loop for anomaly, triage, and maintenance workflows.

Typical fields:

- `feedback_id`
- `anomaly_id`
- `asset_id`
- `operator_id`
- `is_actionable`
- `is_true_positive`
- `severity_correction`
- `suspected_failure_mode`
- `resolution_note`
- `explanation_usefulness`
- `created_at`

Principle: Feedback closes the learning loop, but it must preserve who said what, when, and based on which evidence.

## Artifact Generation Rules

### JSON Schema / Event Contracts

Use for Kafka events, MQTT payloads, API payloads, LLM structured output, telemetry events, anomaly events, quality events, and operator feedback events.

Rules:

- Include `schema_version`.
- Include stable identifiers such as `event_id`, `anomaly_id`, or `feedback_id`.
- Include `event_time` for event-like data and `ingest_time` where relevant.
- Include lineage fields such as `source_system`, `source_event_ids`, or `raw_payload_ref`.
- Include `unit` for telemetry.
- Include `quality_flags` or `quality_caveats` for anomaly and triage models.
- Avoid `root_cause` for LLM output; prefer `possible_causes` or `root_cause_hypotheses`.

### SQL DDL / Warehouse Models

Use for asset dimensions, tag dimensions, telemetry facts, anomaly facts, quality facts, work order facts, triage evidence facts, and operator feedback facts.

Rules:

- State the grain, such as one row per asset-metric-window.
- Separate `event_time`, `ingest_time`, `window_start`, and `window_end`.
- Include `tenant_id` or `site_id` for multi-tenant or multi-site systems.
- Consider slowly changing metadata through fields such as `valid_from` and `valid_to`.
- Do not combine raw telemetry, enriched telemetry, anomaly events, and triage output into one universal table.
- For high-frequency telemetry, consider partitioning, sort keys, TTL, rollups, and hot/cold storage separation.

### dbt Models

Use for raw/staging/mart layers, source freshness, tests, documentation, and dashboard marts.

Rules:

- `stg_` models should handle type cleanup, unit normalization, and tag mapping.
- `int_` models should handle windows, quality signals, and asset-context enrichment.
- `fct_` models should represent business events such as anomalies, quality events, work orders, and asset health.
- `dim_` models should represent assets, tags, components, and failure modes.
- Add tests for quality flags, unit values, event time, `asset_id`, and `tag_id`.
- For high-frequency telemetry, avoid default full-table tests when partition, window, or sample tests are more appropriate.

### OpenAPI / API Contracts

Use for asset health, anomaly detail, triage evidence, operator feedback, and work order context endpoints.

Rules:

- Distinguish evidence, summary, recommendation, and feedback.
- Support filters by asset, time range, severity, and status.
- Return source references for anomaly detail.
- Do not expose triage endpoints as root-cause diagnosis APIs.
- Prefer endpoint names such as `/assets/{asset_id}/health`, `/anomalies/{anomaly_id}`, `/anomalies/{anomaly_id}/triage-evidence`, and `/operator-feedback`.

### Quality Checks / Tests

Generated artifacts should include validation suggestions for:

- Schema compatibility.
- Required fields.
- Timestamp format and timezone.
- Event-time and ingest-time expectations, with caveats.
- Duplicate event IDs.
- Missing sensor heartbeat.
- Flatline detection.
- Unit mismatch.
- Late arrival.
- Out-of-range values.
- Stale metadata.
- Tenant boundary leakage.
- Source event traceability.
- LLM output schema validation.
- Overclaiming language.

### Sample Records

Sample records should:

- Use realistic but fictional industrial semantics.
- Be marked as synthetic or demo data.
- Avoid real company names, equipment serial numbers, and factory locations unless provided by the user.
- Use suspected, possible, or hypothesis language for anomaly examples.
- Make LLM examples cite evidence fields instead of inventing facts.

## Terminology Guardrails

Recommended terms:

- anomaly
- condition indicator
- degradation signal
- suspected failure mode
- root-cause hypothesis
- recommended check
- evidence-grounded summary
- operator support
- maintenance decision support

Terms to avoid by default:

- confirmed root cause
- autonomous diagnosis
- automatic remediation
- guaranteed RUL
- certified safety action
- production-proven MTTR reduction
- standard-compliant system

## Review Checklist

When reviewing APM models, check:

- Does the model include asset, component, tag, metric, and unit context?
- Does it support physical hierarchy and functional location?
- Does it avoid assuming one universal industrial hierarchy?
- Does it distinguish `event_time`, `ingest_time`, `detected_at`, `window_start`, and `window_end`?
- Does it describe sampling rate, lateness, out-of-order handling, watermarking, or batch latency where relevant?
- Does it model missing, late, duplicate, flatline, unit mismatch, schema drift, and stale metadata?
- Do quality flags propagate into anomaly, health, and triage layers?
- Does it distinguish sensor faults from asset faults?
- Does it distinguish anomaly, condition indicator, degradation signal, failure mode, and confirmed fault?
- Are work orders and inspections treated as evidence instead of perfect ground truth?
- If RUL or predictive maintenance appears, does the design address failure labels, censored data, maintenance history, and validation?
- Does LLM output consume deterministic evidence and return summaries, hypotheses, and recommended checks?
- Does standards wording avoid compliance claims?
- If safety systems are in scope, does the design preserve SIS and APM responsibility boundaries?
- Can the model answer who uses the output, when they use it, and what next action it supports?
- Can output be traced to source tags, events, rules, models, work orders, inspections, or runbooks?

## Anti-Patterns

Avoid or flag:

- Sensor table first: only modeling `sensor_id`, `timestamp`, and `value`.
- One giant APM table: mixing asset, telemetry, anomaly, work order, and triage grains.
- Anomaly equals failure: treating threshold breaches or model scores as confirmed failures.
- LLM creates facts: allowing the model to invent failure causes, maintenance conclusions, or evidence.
- No quality propagation: dropping quality flags before anomaly or triage layers.
- Standard name dropping: mentioning standards without reflecting their concepts in the model.
- Ignoring operating context: missing operating mode, load, shift, recipe, process stage, or environmental context where relevant.
- Predictive maintenance overclaiming: claiming RUL or failure prediction without labels, maintenance history, censoring assumptions, and validation.
- Ignoring OT or safety boundaries: turning APM alerts into automatic control or safety actions.
- No feedback loop: omitting operator feedback, inspection results, or work order closure.

## Review Output Style

When reviewing an APM model, lead with findings and then recommend fixes.

Example:

```text
Findings:
- Missing asset/component/tag context.
- Time semantics are unclear.
- Quality flags do not propagate to the anomaly layer.
- The `root_cause` field overclaims; rename it to `possible_causes`.
- No operator feedback loop is modeled.

Recommended fixes:
- Add an asset hierarchy model.
- Split telemetry facts and anomaly event facts.
- Add a quality event fact.
- Add evidence references to triage output.
- Add tests for timestamps, units, duplicates, and late data.
```

## Implementation Planning Decisions

The implementation plan should use these defaults:

- Keep `SKILL.md` operational and concise. Put the trigger rules, workflow, standards lens table, core domains, artifact rules, terminology guardrails, and review checklist in the main skill file.
- Add short reusable examples in `SKILL.md` for design output and review output.
- If implementation needs longer examples or templates, place them in a `references/` file instead of making `SKILL.md` too large.
- Include a compact glossary for key terms such as asset, component, tag, condition indicator, failure mode, anomaly, evidence, and triage.
