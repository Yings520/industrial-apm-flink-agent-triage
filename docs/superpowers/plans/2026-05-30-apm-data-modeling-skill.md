# APM Data Modeling Skill Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a reusable personal Codex skill named `apm-data-modeling` for APM, IIoT, condition monitoring, PdM evidence, and LLM-assisted operator triage data modeling.

**Architecture:** Create one focused skill directory under the personal Codex skills library. Keep `SKILL.md` operational and concise, with trigger rules, workflow, standards-aware modeling, core domains, artifact rules, terminology guardrails, review checklist, anti-patterns, and a compact glossary. Put longer reusable artifact examples in `references/artifact-templates.md` so the main skill stays readable.

**Tech Stack:** Markdown skill files, Codex personal skills directory, shell validation with `rg`, `sed`, and `wc`.

---

## Source Spec

Use this approved design as the source of truth:

- `/Users/ysc/Documents/Data_Engineering/projects/industrial-apm-flink-agent-triage/docs/superpowers/specs/2026-05-30-apm-data-modeling-skill-design.md`

## File Structure

Create these files during implementation:

- Create: `/Users/ysc/.codex/skills/apm-data-modeling/SKILL.md`
  - Responsibility: Main skill instructions. Keep it operational, action-oriented, and usable without opening extra files.
- Create: `/Users/ysc/.codex/skills/apm-data-modeling/references/artifact-templates.md`
  - Responsibility: Reusable examples for design output, JSON Schema, SQL DDL, dbt model naming, OpenAPI shape, quality checks, and review findings.

No repository source files need to change during skill implementation.

## Permission Note

The target skill directory is outside this repository. If the implementation environment blocks writes to `/Users/ysc/.codex/skills`, request approval for the specific command that creates or edits the skill files.

### Task 1: Create Skill Directory Skeleton

**Files:**
- Create: `/Users/ysc/.codex/skills/apm-data-modeling/`
- Create: `/Users/ysc/.codex/skills/apm-data-modeling/references/`

- [ ] **Step 1: Verify the target parent exists**

Run:

```bash
rtk ls /Users/ysc/.codex/skills
```

Expected: command succeeds and lists existing skills.

- [ ] **Step 2: Create the skill and references directories**

Run:

```bash
mkdir -p /Users/ysc/.codex/skills/apm-data-modeling/references
```

Expected: command succeeds without output.

- [ ] **Step 3: Verify the directories exist**

Run:

```bash
rtk ls /Users/ysc/.codex/skills/apm-data-modeling
```

Expected: output includes `references`.

### Task 2: Write Main Skill File

**Files:**
- Create: `/Users/ysc/.codex/skills/apm-data-modeling/SKILL.md`

- [ ] **Step 1: Create `SKILL.md` with frontmatter and complete operating instructions**

Write this exact content to `/Users/ysc/.codex/skills/apm-data-modeling/SKILL.md`:

````markdown
---
name: apm-data-modeling
description: Use when designing, reviewing, or extending data models for Asset Performance Management (APM), Industrial IoT telemetry, condition monitoring, predictive-maintenance evidence, anomaly events, asset health products, work order context, or LLM-assisted operator triage.
---

# Asset Performance Management Data Modeling

Use this skill to design, review, or extend data models for APM, IIoT, condition monitoring, predictive maintenance evidence, anomaly events, asset health data products, and LLM-assisted operator triage.

Core posture:

- Design first, artifacts second.
- Standards-aware, not compliance-claiming.
- Evidence-grounded, not diagnosis-claiming.
- Asset-context-first, not sensor-table-first.

## First Response

When this skill triggers:

1. Classify the modeling task.
2. Ask for missing industrial context before generating schemas or DDL.
3. Produce a design for confirmation before artifacts unless the user explicitly asks for review only.
4. Generate artifacts only after the model grain, time semantics, asset context, quality signals, and downstream consumers are clear.
5. Review the output for terminology, traceability, and claim boundaries.

Common task types:

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

## Context Questions

Ask only for context that affects the design. Prefer one or a small set of high-impact questions.

Important context:

- Asset objects: equipment, lines, plants, components, sensors, software services, or facilities.
- Hierarchy: site, area, plant, line, cell, asset, component, sensor tag, functional location.
- Data sources: PLC, SCADA, historian, PI System, MQTT, Kafka, EAM, CMMS, manual inspection, lab system, or operator entry.
- Time semantics: event time, ingest time, detected time, window time, sampling rate, lateness, out-of-order arrival, and watermarking.
- Business action: alerting, inspection, maintenance planning, reliability analysis, operator triage, dashboarding, or model training.
- Quality concerns: missing, late, duplicate, flatline, out of range, unit mismatch, stale metadata, schema drift, and tenant leakage.
- Labels and evidence: failure labels, work orders, inspection records, runbooks, operator feedback, and historical incidents.
- Boundaries: multitenancy, OT cybersecurity, safety-instrumented systems, and compliance goals.

## Design Output

Before artifacts, present a design with:

- Core entities.
- Entity relationships.
- Grain for each fact, event, or model.
- Required fields.
- Time semantics.
- Quality signals.
- Downstream consumers.
- Assumptions.
- Out-of-scope claims.
- Validation strategy.

Useful conceptual chain:

```text
Asset -> Component -> Sensor Tag -> Telemetry Reading
Telemetry Reading -> Quality Event
Telemetry Window -> Anomaly Event
Anomaly Event -> Triage Evidence -> Operator Feedback
Asset -> Failure Mode -> Recommended Inspection
```

## Standards-Aware Modeling

Use industrial standards as modeling lenses, not automatic compliance claims.

| Modeling Problem | Useful Lens |
| --- | --- |
| Asset management goals, lifecycle, value, risk, governance | ISO 55000 / ISO 55001 |
| Reliability and maintenance data, failure modes, equipment taxonomy | ISO 14224 |
| Enterprise, MES, SCADA, PLC, equipment boundaries | ISA-95 / IEC 62264 |
| Batch/process hierarchy, equipment modules, control modules | ISA-88 / IEC 61512 |
| Reference designation, functional location, structured asset identifiers | IEC 81346 |
| Condition monitoring program design | ISO 17359 |
| Condition monitoring data processing, health assessment, advisory pipeline | ISO 13374 |
| Condition-based maintenance architecture | MIMOSA OSA-CBM |
| Semantic models, digital twins, submodels, interoperability | Asset Administration Shell |
| SIS, SIF, SIL, process safety boundaries | ISA-84 / IEC 61511 |
| OT/ICS cybersecurity, zones, conduits, access boundaries | ISA/IEC 62443 |
| Large process-plant lifecycle data integration | ISO 15926 |

Recommended phrasing:

- `inspired by ISO 14224 reliability-data concepts`
- `aligned with ISA-95-style enterprise/control boundaries`
- `uses AAS-style semantic submodel thinking`

Avoid phrasing:

- `ISO 14224 compliant`
- `IEC 61511 certified`
- `ISA-95 certified architecture`

Never claim standard compliance unless the user explicitly asks for compliance-oriented modeling and provides enough domain evidence.

Treat ISA-84 / IEC 61511 safety-instrumented-system data as safety-context data, not ordinary APM control authority. Do not suggest that APM anomaly models can trigger safety actions or satisfy SIS requirements unless the user explicitly scopes a safety-certified engineering workflow.

## Core APM Data Domains

### Asset Registry / Asset Hierarchy

Purpose: describe what the asset is, where it is, who owns it, and how critical it is.

Typical fields: `asset_id`, `asset_name`, `asset_type`, `asset_class`, `site_id`, `area_id`, `plant_id`, `line_id`, `component_id`, `parent_asset_id`, `functional_location`, `criticality`, `manufacturer`, `model`, `commissioned_at`, `operating_context`, `lifecycle_state`.

Principle: support both physical structure and functional location. Do not assume one fixed hierarchy for every industry.

### Sensor Tag / Measurement Point

Purpose: map raw tags to assets, components, measurement points, metrics, units, and source systems.

Typical fields: `tag_id`, `tag_name`, `asset_id`, `component_id`, `measurement_point`, `metric_name`, `unit`, `sampling_rate`, `source_system`, `signal_type`, `normal_range_min`, `normal_range_max`, `engineering_limits`, `calibration_status`.

Principle: a raw tag is not an asset signal until it has asset, metric, unit, and measurement-point context.

### Raw Telemetry Event

Purpose: preserve source time-series data without erasing lineage too early.

Typical fields: `event_id`, `tag_id`, `event_time`, `ingest_time`, `value`, `unit`, `source_system`, `quality_flags`, `schema_version`, `raw_payload_ref`.

Principle: preserve source timing and source identity. Normalize later, but do not erase lineage.

### Enriched Telemetry / Condition Signal

Purpose: contextualize raw telemetry into asset-level signals for monitoring and analytics.

Typical fields: `event_id`, `asset_id`, `component_id`, `tag_id`, `metric_name`, `event_time`, `window_start`, `window_end`, `value`, `normalized_value`, `unit`, `operating_mode`, `quality_flags`, `threshold_profile_id`.

Principle: enriched telemetry carries quality context forward into analytics and triage.

### Quality Event

Purpose: make telemetry quality issues queryable, alertable, and available to downstream confidence decisions.

Typical fields: `quality_event_id`, `event_time`, `detected_at`, `asset_id`, `tag_id`, `quality_type`, `severity`, `affected_window_start`, `affected_window_end`, `description`, `source_event_ids`, `resolution_status`.

Quality types: `missing`, `late`, `duplicate`, `flatline`, `out_of_range`, `unit_mismatch`, `schema_drift`, `stale_metadata`, `tenant_leakage`, `sensor_fault_suspected`.

### Anomaly Event

Purpose: describe abnormal behavior without treating it as confirmed failure.

Typical fields: `anomaly_id`, `asset_id`, `component_id`, `tag_id`, `metric_name`, `rule_id`, `model_id`, `window_start`, `window_end`, `observed_value`, `baseline_value`, `expected_value`, `severity`, `detection_confidence`, `quality_flags`, `source_event_ids`, `status`.

Principle: an anomaly event is evidence of abnormal behavior, not proof of failure or root cause.

### Failure Mode / Reliability Context

Purpose: provide interpretation context for condition monitoring and maintenance decisions.

Typical fields: `failure_mode_id`, `asset_type`, `component_type`, `failure_mode_name`, `failure_effect`, `criticality`, `detectability`, `recommended_inspections`, `related_metrics`, `pf_interval_hint`, `maintenance_strategy`.

Principle: failure modes provide interpretation context. They should not turn unconfirmed telemetry anomalies into confirmed faults.

### Maintenance / Work Order Integration

Purpose: connect APM signals to operational maintenance actions and feedback.

Typical fields: `work_order_id`, `asset_id`, `failure_mode_id`, `created_at`, `completed_at`, `work_type`, `priority`, `problem_code`, `cause_code`, `remedy_code`, `technician_notes`, `downtime_minutes`, `parts_used`, `source_system`.

Principle: work orders and inspections are often noisy labels. Treat them as operational evidence, not perfect ground truth.

### Triage Evidence

Purpose: provide deterministic, traceable evidence for LLM-assisted or operator-support workflows.

Typical fields: `evidence_id`, `anomaly_id`, `asset_id`, `evidence_version`, `metric_window_summary`, `quality_caveats`, `asset_context`, `recent_anomalies`, `related_work_orders`, `runbook_refs`, `source_refs`.

Principle: triage evidence should be deterministic. LLM output should not create new evidence.

### Operator Feedback

Purpose: close the learning loop for anomaly, triage, and maintenance workflows.

Typical fields: `feedback_id`, `anomaly_id`, `asset_id`, `operator_id`, `is_actionable`, `is_true_positive`, `severity_correction`, `suspected_failure_mode`, `resolution_note`, `explanation_usefulness`, `created_at`.

Principle: feedback must preserve who said what, when, and based on which evidence.

## Artifact Rules

### JSON Schema / Event Contracts

- Include `schema_version`.
- Include stable identifiers such as `event_id`, `anomaly_id`, or `feedback_id`.
- Include `event_time` for event-like data and `ingest_time` where relevant.
- Include lineage fields such as `source_system`, `source_event_ids`, or `raw_payload_ref`.
- Include `unit` for telemetry.
- Include `quality_flags` or `quality_caveats` for anomaly and triage models.
- Avoid `root_cause` for LLM output; prefer `possible_causes` or `root_cause_hypotheses`.

### SQL / Warehouse Models

- State the grain, such as one row per asset-metric-window.
- Separate `event_time`, `ingest_time`, `window_start`, and `window_end`.
- Include `tenant_id` or `site_id` for multi-tenant or multi-site systems.
- Consider slowly changing metadata through fields such as `valid_from` and `valid_to`.
- Do not combine raw telemetry, enriched telemetry, anomaly events, and triage output into one universal table.
- For high-frequency telemetry, consider partitioning, sort keys, TTL, rollups, and hot/cold storage separation.

### dbt Models

- `stg_` models handle type cleanup, unit normalization, and tag mapping.
- `int_` models handle windows, quality signals, and asset-context enrichment.
- `fct_` models represent business events such as anomalies, quality events, work orders, and asset health.
- `dim_` models represent assets, tags, components, and failure modes.
- Add tests for quality flags, unit values, event time, `asset_id`, and `tag_id`.
- For high-frequency telemetry, prefer partition, window, or sample tests over expensive full-table tests.

### OpenAPI / API Contracts

- Distinguish evidence, summary, recommendation, and feedback.
- Support filters by asset, time range, severity, and status.
- Return source references for anomaly detail.
- Do not expose triage endpoints as root-cause diagnosis APIs.
- Prefer endpoint names such as `/assets/{asset_id}/health`, `/anomalies/{anomaly_id}`, `/anomalies/{anomaly_id}/triage-evidence`, and `/operator-feedback`.

### Quality Checks / Tests

Suggest checks for schema compatibility, required fields, timestamp format, timezone, event-time and ingest-time expectations, duplicate event IDs, missing heartbeat, flatline detection, unit mismatch, late arrival, out-of-range values, stale metadata, tenant boundary leakage, source event traceability, LLM output schema validation, and overclaiming language.

### Sample Records

- Use realistic but fictional industrial semantics.
- Mark examples as synthetic or demo data.
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

Avoid by default:

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

## Compact Glossary

- Asset: physical, logical, or functional item whose performance, condition, or lifecycle matters.
- Component: maintainable or monitorable part of an asset.
- Sensor tag: source-system identifier for a measured signal.
- Measurement point: physical or logical location where a signal is measured.
- Condition indicator: signal or derived feature that describes current asset condition.
- Anomaly: abnormal behavior detected by a rule, statistic, or model.
- Failure mode: way an asset or component can fail.
- Confirmed fault: verified failure or defect supported by inspection, work order, or domain confirmation.
- Triage evidence: deterministic context used by humans or LLMs to summarize and recommend checks.
- Operator feedback: human assessment of alert usefulness, severity, actionability, or resolution.

## Review Output Style

When reviewing an APM model, lead with findings and then recommend fixes:

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

## Additional References

Open `references/artifact-templates.md` when the user asks for concrete examples, reusable templates, or artifact shapes.
````

- [ ] **Step 2: Verify `SKILL.md` metadata and required sections**

Run:

```bash
rtk rg -n "^---$|^name: apm-data-modeling|^description:|^# Asset Performance Management Data Modeling|^## Standards-Aware Modeling|^## Core APM Data Domains|^## Artifact Rules|^## Review Checklist|^## Anti-Patterns" /Users/ysc/.codex/skills/apm-data-modeling/SKILL.md
```

Expected: output includes the frontmatter markers, `name`, `description`, title, and all listed sections.

### Task 3: Write Artifact Templates Reference

**Files:**
- Create: `/Users/ysc/.codex/skills/apm-data-modeling/references/artifact-templates.md`

- [ ] **Step 1: Create the reference file with reusable examples**

Write this exact content to `/Users/ysc/.codex/skills/apm-data-modeling/references/artifact-templates.md`:

````markdown
# APM Data Modeling Artifact Templates

Use these examples when the user asks for concrete artifacts. Adapt names and fields to the user's domain instead of copying them blindly.

## Design Output Template

```markdown
## Model Design

**Use Case:** Asset condition monitoring for synthetic/demo industrial telemetry.

**Core Entities:**
- Asset: one row per maintainable asset.
- Sensor Tag: one row per source-system measurement tag.
- Telemetry Reading: one row per tag reading at source event time.
- Quality Event: one row per detected data quality issue.
- Anomaly Event: one row per detected abnormal asset-metric window.
- Triage Evidence: one row per anomaly evidence packet for operator support.

**Relationships:**
- Asset 1-to-many Sensor Tag.
- Sensor Tag 1-to-many Telemetry Reading.
- Telemetry Reading many-to-many Anomaly Event through bounded source event references.
- Anomaly Event 1-to-many Triage Evidence versions.

**Time Semantics:**
- `event_time` is source time.
- `ingest_time` is collector or platform receipt time.
- `window_start` and `window_end` define analytic windows.
- `detected_at` records when a rule, job, or model emitted an event.

**Quality Signals:**
- Preserve `quality_flags` from raw telemetry into enriched telemetry.
- Create queryable quality events for missing, late, duplicate, flatline, unit mismatch, and schema drift issues.

**Boundaries:**
- Anomalies are not confirmed failures.
- LLM outputs are summaries, hypotheses, and recommended checks based on deterministic evidence.
- Standard references are modeling lenses, not compliance claims.
```

## JSON Schema Skeleton

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "anomaly_event",
  "type": "object",
  "additionalProperties": false,
  "required": [
    "anomaly_id",
    "schema_version",
    "asset_id",
    "tag_id",
    "metric_name",
    "window_start",
    "window_end",
    "severity",
    "detection_confidence",
    "quality_flags",
    "source_event_ids"
  ],
  "properties": {
    "anomaly_id": { "type": "string", "minLength": 1 },
    "schema_version": { "type": "string", "const": "anomaly_event.v1" },
    "asset_id": { "type": "string", "minLength": 1 },
    "tag_id": { "type": "string", "minLength": 1 },
    "metric_name": { "type": "string", "minLength": 1 },
    "window_start": { "type": "string", "format": "date-time" },
    "window_end": { "type": "string", "format": "date-time" },
    "observed_value": { "type": "number" },
    "baseline_value": { "type": ["number", "null"] },
    "severity": { "type": "string", "enum": ["info", "warning", "critical"] },
    "detection_confidence": { "type": "number", "minimum": 0, "maximum": 1 },
    "quality_flags": {
      "type": "array",
      "items": { "type": "string" },
      "uniqueItems": true
    },
    "source_event_ids": {
      "type": "array",
      "items": { "type": "string" },
      "maxItems": 20
    }
  }
}
```

## SQL DDL Skeleton

```sql
create table fact_apm_anomaly_events (
    anomaly_id varchar(128) not null,
    schema_version varchar(64) not null,
    tenant_id varchar(128),
    site_id varchar(128),
    asset_id varchar(128) not null,
    component_id varchar(128),
    tag_id varchar(128) not null,
    metric_name varchar(128) not null,
    rule_id varchar(128),
    model_id varchar(128),
    window_start timestamp not null,
    window_end timestamp not null,
    detected_at timestamp not null,
    observed_value double precision,
    baseline_value double precision,
    expected_value double precision,
    severity varchar(32) not null,
    detection_confidence double precision,
    quality_flags json,
    source_event_ids json,
    status varchar(32) not null,
    created_at timestamp not null,
    primary key (anomaly_id)
);
```

Grain: one row per detected asset-tag-metric anomaly window.

## dbt Model Naming

```text
stg_apm__sensor_tags
stg_apm__telemetry_readings
int_apm__telemetry_with_asset_context
int_apm__quality_events
fct_apm__anomaly_events
fct_apm__asset_health_windows
dim_apm__assets
dim_apm__failure_modes
fct_apm__operator_feedback
```

## OpenAPI Shape

```yaml
paths:
  /anomalies/{anomaly_id}/triage-evidence:
    get:
      summary: Get deterministic triage evidence for an anomaly
      parameters:
        - name: anomaly_id
          in: path
          required: true
          schema:
            type: string
      responses:
        "200":
          description: Evidence packet for operator support
          content:
            application/json:
              schema:
                type: object
                required:
                  - evidence_id
                  - anomaly_id
                  - asset_context
                  - metric_window_summary
                  - quality_caveats
                  - source_refs
                properties:
                  evidence_id:
                    type: string
                  anomaly_id:
                    type: string
                  asset_context:
                    type: object
                  metric_window_summary:
                    type: object
                  quality_caveats:
                    type: array
                    items:
                      type: string
                  runbook_refs:
                    type: array
                    items:
                      type: string
                  source_refs:
                    type: array
                    items:
                      type: string
```

## Quality Check Examples

```text
- Validate every telemetry record has event_time, ingest_time, tag_id, value, unit, and source_system.
- Validate event_time is timezone-aware and not parsed as local time.
- Detect duplicate event_id within the same source system.
- Detect missing heartbeat by asset-tag-sampling-rate window.
- Detect flatline when repeated values exceed the configured duration for a tag.
- Detect unit mismatch between tag metadata and telemetry payload.
- Detect schema drift by rejecting fields not present in the contract.
- Detect tenant leakage by verifying tenant_id aligns across topic, asset, tag, and event.
- Validate anomaly source_event_ids resolve to telemetry events.
- Validate LLM triage output contains evidence references and avoids confirmed-root-cause language.
```

## Review Output Template

```text
Findings:
- Missing asset/component/tag context.
- Time semantics are unclear.
- Quality flags do not propagate to the anomaly layer.
- The `root_cause` field overclaims; rename it to `possible_causes`.
- No operator feedback loop is modeled.

Recommended fixes:
- Add an asset hierarchy model with physical and functional-location fields.
- Split telemetry facts and anomaly event facts.
- Add a quality event fact.
- Add evidence references to triage output.
- Add tests for timestamps, units, duplicates, and late data.
```
````

- [ ] **Step 2: Verify reference sections**

Run:

```bash
rtk rg -n "^# APM Data Modeling Artifact Templates|^## Design Output Template|^## JSON Schema Skeleton|^## SQL DDL Skeleton|^## dbt Model Naming|^## OpenAPI Shape|^## Quality Check Examples|^## Review Output Template" /Users/ysc/.codex/skills/apm-data-modeling/references/artifact-templates.md
```

Expected: output includes all listed headings.

### Task 4: Validate Skill Content

**Files:**
- Validate: `/Users/ysc/.codex/skills/apm-data-modeling/SKILL.md`
- Validate: `/Users/ysc/.codex/skills/apm-data-modeling/references/artifact-templates.md`

- [ ] **Step 1: Check for unresolved draft markers**

Run:

```bash
rtk rg -n "T[B]D|TO[D]O|FIX[M]E|\\?\\?|place[h]older|fill[ ]in|implement[ ]later" /Users/ysc/.codex/skills/apm-data-modeling
```

Expected: no matches and exit code 1.

- [ ] **Step 2: Check claim-boundary guardrails exist**

Run:

```bash
rtk rg -n "autonomous diagnosis|automatic remediation|guaranteed RUL|standard-compliant|compliance claims|LLM output should not create new evidence|anomaly event is evidence" /Users/ysc/.codex/skills/apm-data-modeling
```

Expected: output includes matches in `SKILL.md`.

- [ ] **Step 3: Check the main skill stays reasonably compact**

Run:

```bash
rtk wc -l /Users/ysc/.codex/skills/apm-data-modeling/SKILL.md /Users/ysc/.codex/skills/apm-data-modeling/references/artifact-templates.md
```

Expected: `SKILL.md` is under 360 lines and `artifact-templates.md` is under 260 lines.

- [ ] **Step 4: Read the skill as a final sanity check**

Run:

```bash
rtk sed -n '1,260p' /Users/ysc/.codex/skills/apm-data-modeling/SKILL.md
```

Expected: the file is readable, operational, and does not require opening the reference file for ordinary design/review work.

### Task 5: Commit Skill Files If The Skills Directory Is Git-Tracked

**Files:**
- Check: `/Users/ysc/.codex/skills/apm-data-modeling/SKILL.md`
- Check: `/Users/ysc/.codex/skills/apm-data-modeling/references/artifact-templates.md`

- [ ] **Step 1: Check whether `/Users/ysc/.codex/skills` is inside a git repository**

Run:

```bash
git -C /Users/ysc/.codex/skills rev-parse --show-toplevel
```

Expected: either prints a git root path or fails with `not a git repository`.

- [ ] **Step 2: If it is not a git repository, skip commit**

Run only if Step 1 fails:

```bash
rtk ls /Users/ysc/.codex/skills/apm-data-modeling
```

Expected: output includes `SKILL.md` and `references`.

- [ ] **Step 3: If it is a git repository, review status**

Run only if Step 1 prints a git root:

```bash
git -C /Users/ysc/.codex/skills status --short
```

Expected: output includes only the new `apm-data-modeling` skill files or clearly unrelated pre-existing changes.

- [ ] **Step 4: If it is a git repository, commit only the new skill files**

Run only if Step 1 prints a git root:

```bash
git -C /Users/ysc/.codex/skills add apm-data-modeling/SKILL.md apm-data-modeling/references/artifact-templates.md
git -C /Users/ysc/.codex/skills commit -m "feat: add APM data modeling skill"
```

Expected: commit succeeds and includes only the two skill files.

## Self-Review Checklist

- Spec coverage: The plan covers skill positioning, triggers, workflow, standards-aware modeling, core data domains, artifact rules, terminology guardrails, review checklist, anti-patterns, examples, and glossary.
- Draft-marker scan: The plan contains no unresolved draft markers, and the validation command avoids literal draft-marker words so it does not flag itself.
- Scope check: The plan creates one reusable personal skill and one reference file. It does not modify the current APM project implementation.
- Permission check: The plan explicitly notes that writing to `/Users/ysc/.codex/skills` may require approval.
