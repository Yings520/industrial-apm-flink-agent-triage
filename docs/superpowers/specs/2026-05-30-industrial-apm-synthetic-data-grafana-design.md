# Industrial APM Synthetic Data And Grafana Design

Date: 2026-05-30

## Purpose

Design an end-to-end industrial Asset Performance Management demo dataset and Grafana dashboard suite for chemical and metallurgy scenarios.

The design supports Kafka mock telemetry, Flink event-time processing, Doris serving tables, and Grafana product-style dashboards. It is intended for realistic portfolio and interview demonstration of condition monitoring, degradation, anomaly detection, RUL prediction, maintenance context, and LLM-assisted alert triage.

## Scope

In scope:

- Review and refine the current APM data model.
- Generate realistic synthetic industrial sensor streams at 1 Hz.
- Generate supporting business data for assets, components, measurement points, tags, failure modes, rules, work orders, maintenance events, RUL predictions, anomaly events, and triage evidence.
- Preserve compatibility with the existing Kafka -> Flink -> Doris -> Grafana architecture.
- Redesign Grafana around three dashboards:
  - `APM Landing Dashboard`
  - `Asset Operations Workbench`
  - `Anomaly / RUL Detail Dashboard`

Out of scope:

- Claims of real factory deployment.
- Autonomous diagnosis or automatic remediation.
- Safety-certified control actions.
- Production SLA, MTTR reduction, or downtime-reduction claims.
- Claiming RUL predictions are validated against real failure labels.

## Current Model Review

The current repository already has a strong APM skeleton:

- Contracts exist for raw sensor events, enriched telemetry, anomaly events, failure modes, work orders, operator feedback, and agent explanation.
- Doris serving tables cover realtime readings, anomaly events, late events, stream health, quality events, triage evidence, incidents, routing, feedback, and LLM invocation audit.
- Config files already describe assets, components, failure modes, anomaly rules, scenarios, and Grafana dashboards.
- The data flow already matches a credible realtime architecture: Kafka mock producer -> Flink enrichment/anomaly jobs -> Doris routine load -> Grafana dashboards -> triage service.

The main design gaps are:

- The current generator is closer to a contract smoke generator than a realistic industrial time-series simulator.
- `tag_mapping.yml` currently derives one tag per asset and metric. This is too coarse for industrial APM, where tags belong to physical measurement points on components.
- Current scenarios such as `spike`, `drift`, `flatline`, and `late` are useful test cases but should be nested under richer operational states and failure-mode timelines.
- The current demo profiles produce too few periods for 30-day RUL and degradation storytelling.
- Grafana dashboards are functional but still read like technical monitoring dashboards rather than APM product dashboards.

## Conceptual Model

Use this hierarchy:

```text
tenant -> site -> area -> plant -> production_line -> asset -> component -> measurement_point -> sensor_tag
```

The important modeling distinction is:

```text
component -> measurement_point -> sensor_tag
```

A `measurement_point` is the physical location or logical sensing point, for example `pump_de_bearing_vertical`. A `sensor_tag` is the actual signal emitted from that point, for example `vibration_rms_mm_s`.

One measurement point may have one or many tags:

- Temperature, pressure, and flow measurement points often map to one tag.
- Vibration measurement points may map to multiple tags such as RMS, peak, crest factor, kurtosis, and envelope.
- A sensor tag should not replace component context. Tags need asset, component, measurement point, metric, unit, and source-system context.

## Target Industrial Scenario

Industries:

- chemical processing
- metallurgy / steel production

Asset types:

- `pump`
- `motor`
- `compressor`
- `conveyor`
- `reactor`

Representative asset/component examples:

| Asset Type | Components |
| --- | --- |
| `pump` | `motor`, `bearing_de`, `bearing_nde`, `impeller`, `seal`, `coupling`, `pump_casing` |
| `motor` | `stator`, `rotor`, `bearing_de`, `bearing_nde`, `cooling_fan`, `terminal_box` |
| `compressor` | `motor`, `bearing`, `compressor_stage`, `inlet_filter`, `seal`, `lubrication_system` |
| `conveyor` | `drive_motor`, `gearbox`, `head_pulley`, `tail_pulley`, `idler_roller`, `belt` |
| `reactor` | `agitator_motor`, `agitator_shaft`, `jacket`, `seal`, `vessel`, `inlet_valve`, `outlet_valve` |

## Sensor Tags

Use simple English field names and industrially plausible metric names:

| Metric | Unit | Typical Use |
| --- | --- | --- |
| `temperature_c` | `celsius` | bearing, motor winding, reactor jacket, compressor stage |
| `vibration_rms_mm_s` | `mm_s` | rotating components, bearings, motor, gearbox |
| `pressure_bar` | `bar` | reactor, pump discharge, compressor stage, hydraulic system |
| `flow_rate_m3_h` | `m3_h` | pump, seal flush, reactor feed, compressor inlet |
| `motor_current_a` | `ampere` | motor load and electrical stress |
| `power_kw` | `kw` | energy consumption and energy penalty |
| `rpm` | `rpm` | rotating asset speed |
| `acoustic_db` | `db` | cavitation, compressor surge, bearing defects |

Sampling strategy:

- Raw telemetry emits at `1 Hz` for each tag.
- The Kafka mock producer supports two modes:
  - `backfill_replay`: accelerated replay of 30 days of historical data for degradation, RUL, maintenance, and dashboard context.
  - `realtime_live`: continuous realtime generation at 1 Hz after the historical replay has completed.
- 15-minute aggregates are the main analytics and dashboard layer.
- The generator should support deterministic seeds for reproducible tests.
- Kafka remains the primary delivery target for raw events.

Window aggregate fields:

- `tenant_id`
- `asset_id`
- `component_id`
- `measurement_point_id`
- `tag_id`
- `metric_name`
- `window_start`
- `window_end`
- `avg_value`
- `min_value`
- `max_value`
- `stddev_value`
- `p95_value`
- `slope`
- `z_score`
- `event_count`
- `expected_count`
- `quality_flags`
- `operating_state`
- `degradation_stage`

## Synthetic Time-Series Strategy

Do not generate independent random values. Generate values from operating context and failure-mode timelines.

Base signal pattern:

```text
value = baseline
      + daily_cycle
      + load_factor_effect
      + process_noise
      + degradation_effect
      + event_effect
      + sensor_quality_effect
```

Operating states:

- `startup`
- `normal`
- `steady_load`
- `high_load`
- `idle`
- `warning`
- `critical`
- `degradation`
- `failure`
- `maintenance`
- `shutdown`

State behavior:

| State | Behavior |
| --- | --- |
| `startup` | ramp speed, current, pressure, and flow upward over several minutes |
| `normal` | stable range with small noise and weak daily/load cycles |
| `steady_load` | low variance, high data completeness |
| `high_load` | current, power, temperature, vibration, and pressure move upward |
| `idle` | flow and load drop; temperature decays slowly |
| `warning` | early threshold breaches or sustained drift |
| `critical` | repeated breaches, higher confidence anomalies, operator attention required |
| `degradation` | monotonic or stepped deterioration over days |
| `failure` | multi-signal abnormal behavior and potential downtime |
| `maintenance` | asset stopped, readings missing or low, work order active |
| `shutdown` | controlled ramp down |

## Failure Mode Timelines

Use 30-day scenarios for RUL storytelling. Each scenario should have:

- a normal baseline period
- a degradation period
- a warning stage
- a critical stage
- a failure or intervention event
- a maintenance or recovery period

Recommended failure modes:

| Asset | Failure Mode | Signal Pattern |
| --- | --- | --- |
| `pump` | `bearing_wear` | vibration and bearing temperature trend upward |
| `pump` | `cavitation` | vibration and acoustic level rise, flow becomes unstable, suction/discharge pressure fluctuates |
| `pump` | `seal_leak` | pressure and flow degrade, alarms increase, maintenance follows |
| `motor` | `overheating` | winding temperature, current, and power rise under load |
| `motor` | `phase_imbalance` | current imbalance, temperature rise, power penalty |
| `compressor` | `surge_risk` | flow drops, pressure oscillates, vibration spikes |
| `compressor` | `filter_clogging` | flow decreases, pressure differential increases, power penalty rises |
| `conveyor` | `belt_misalignment` | power draw and vibration rise, intermittent warning events |
| `reactor` | `agitator_bearing_wear` | vibration and motor current rise; temperature response may lag |
| `reactor` | `jacket_fouling` | temperature control deviation increases, energy penalty rises |

## Supporting Business Data

Generate these datasets:

| Dataset | Grain |
| --- | --- |
| `assets` | one row per asset |
| `components` | one row per component |
| `measurement_points` | one row per physical measurement point |
| `sensor_tags` | one row per sensor tag |
| `failure_modes` | one row per failure mode |
| `detection_rules` | one row per rule version |
| `telemetry_raw` | one row per tag event |
| `telemetry_window_15m` | one row per tag per 15-minute window |
| `anomaly_events` | one row per rule/tag/window anomaly |
| `alarm_events` | one row per operator-facing alarm |
| `maintenance_events` | one row per maintenance action |
| `work_orders` | one row per CMMS-style work order |
| `rul_predictions` | one row per asset/component prediction timestamp |
| `triage_evidence` | one row per anomaly evidence packet |

CMMS-style `work_orders` fields:

- `work_order_id`
- `asset_id`
- `component_id`
- `failure_mode_id`
- `priority`
- `work_type`
- `problem_code`
- `cause_code`
- `remedy_code`
- `status`
- `created_at`
- `scheduled_start_at`
- `completed_at`
- `downtime_minutes`
- `technician_notes`

RUL prediction fields:

- `prediction_id`
- `asset_id`
- `component_id`
- `failure_mode_id`
- `prediction_time`
- `horizon_minutes`
- `rul_hours`
- `rul_lower_hours`
- `rul_upper_hours`
- `confidence_score`
- `model_name`
- `model_version`
- `evidence_window_start`
- `evidence_window_end`

Triage evidence fields:

- `evidence_id`
- `anomaly_id`
- `asset_id`
- `component_id`
- `measurement_point_id`
- `tag_id`
- `failure_mode_id`
- `metric_window_summary`
- `supporting_evidence`
- `root_cause_hypotheses`
- `recommended_inspection`
- `confidence_score`
- `quality_caveats`
- `related_failure_modes`
- `source_event_ids`

Use `root_cause_hypotheses`, not `root_cause`, because the project is LLM-assisted operator support rather than autonomous diagnosis.

## Kafka And Doris Data Flow

Kafka raw event topic:

```text
tenant_id.raw_apm__sensor_readings.v1
```

Telemetry producer behavior:

- Start with `backfill_replay` to publish 30 days of historical synthetic telemetry through Kafka at an accelerated rate.
- Preserve original `event_time` values from the historical timeline while using current `ingest_time` during replay, so Flink event-time, watermark, and lateness behavior remain testable.
- After backfill completes, switch to `realtime_live` and emit one event per sensor tag per second.
- Continue using the same scenario state machine so the live stream extends the historical degradation, maintenance, or recovery story instead of becoming unrelated random data.

Recommended raw event extensions:

- `component_id`
- `measurement_point_id`
- `operating_state`
- `synthetic_scenario_id`
- `failure_mode_id`
- `degradation_stage`

Flink jobs:

- enrich raw events using tag metadata
- compute 15-minute window aggregates
- detect anomaly events
- create quality events for missing, late, duplicate, flatline, out-of-range, and stale metadata cases
- emit mart topics for anomaly events, late events, stream health, and optional 15-minute aggregates

Doris serving model:

- keep `dwd_apm_sensor_readings_rt` for raw/enriched realtime readings
- add or expose `agg_apm_sensor_windows_15m`
- add or expose `fact_apm_rul_predictions`
- add or expose `dim_measurement_point`
- expand dashboard views so Grafana queries tables and views instead of raw JSON fields where practical

## Grafana Dashboard Architecture

Use three dashboards.

### APM Landing Dashboard

Purpose: product/demo first page for interview storytelling and executive fleet health.

Panels:

- Fleet Health Score
- Critical Assets
- Warning Assets
- 24h Anomaly Count
- RUL Risk Assets
- Energy Penalty
- Asset Health by Asset Type
- Asset Health Heatmap by Site / Area
- Anomaly Rate Trend
- Top Risk Assets
- Recent Critical Events
- Stream Health and Data Quality

Primary tables/views:

- `vw_apm_fleet_health`
- `vw_apm_asset_health_latest`
- `fact_apm_anomaly_events`
- `fact_apm_quality_events`
- `fact_apm_stream_health_snapshots`
- `fact_apm_rul_predictions`

### Asset Operations Workbench

Purpose: main operator-facing APM workspace.

Panels:

- Asset selector variables: `site_id`, `area_id`, `asset_type`, `asset_id`, `component_id`
- Selected Asset Summary
- Component Health Matrix
- Operating State Timeline
- Sensor Trend by Component
- 15-Minute Window Aggregates
- Current Alarms
- Related Work Orders
- Failure Mode Suspects
- Data Quality Caveats

Primary tables/views:

- `dim_asset`
- `dim_component`
- `dim_measurement_point`
- `dim_sensor_tag`
- `agg_apm_sensor_windows_15m`
- `fact_apm_anomaly_events`
- `dim_work_order`
- `fact_apm_quality_events`

### Anomaly / RUL Detail Dashboard

Purpose: show degradation, prediction, evidence, and LLM-assisted triage.

Panels:

- Anomaly Header
- 30-Day Degradation Curve
- RUL Prediction Curve
- Failure Mode Evidence
- Rule Trigger Details
- Source Signal Window
- Related Work Orders and Maintenance Events
- LLM-Assisted Triage Summary
- Recommended Inspection Checks
- Quality Caveats

Primary tables/views:

- `fact_apm_anomaly_events`
- `agg_apm_sensor_windows_15m`
- `fact_apm_rul_predictions`
- `serving_apm_triage_evidence`
- `fact_apm_agent_recommendations`
- `dim_failure_mode`
- `dim_work_order`

## Visual Style

Dashboard style should be polished but operational:

- dark Grafana theme
- restrained color palette
- health colors:
  - `normal`: green
  - `warning`: amber
  - `critical`: red
  - `maintenance`: blue
  - `offline`: gray
- avoid decorative visuals that obscure operational scanning
- prefer grouped rows by workflow: fleet -> asset -> component -> signal -> evidence -> action
- use drill-down links between dashboards

## Data-To-Panel Mapping

| Panel | Source |
| --- | --- |
| Fleet Health Score | latest asset health view or aggregate of component scores |
| Critical Assets | anomaly events and latest health view |
| RUL Risk Assets | latest RUL predictions where `rul_hours` is below threshold |
| Energy Penalty | `power_kw` windows compared with normal baseline |
| Component Health Matrix | latest component health and severity by asset |
| Sensor Trend | 15-minute window aggregate values by tag |
| Degradation Curve | long-range aggregate slope / health score / z-score |
| RUL Curve | `fact_apm_rul_predictions` |
| Failure Mode Evidence | anomaly event + failure mode + related metrics |
| LLM Triage Summary | `fact_apm_agent_recommendations` joined to deterministic evidence |
| Quality Caveats | quality events and triage evidence caveats |
| Work Orders | CMMS-style work order data |

## Implementation Work Split

Data Team:

- Extend asset/component/measurement-point/tag configuration.
- Build realistic 30-day synthetic scenario definitions.
- Implement deterministic 1 Hz raw telemetry generation.
- Generate supporting business datasets.
- Add 15-minute aggregate output and Doris serving tables/views.
- Validate schema contracts and event-time behavior.
- Add tests for scenario realism, referential integrity, and data volume.

Platform / Frontend Team:

- Add Kafka mock producer mode for long-running demo streams.
- Wire Flink aggregate and anomaly outputs to Doris.
- Create or update Grafana dashboard JSON.
- Add dashboard variables and drill-down links.
- Tune panel queries for Doris.
- Add Grafana smoke tests for dashboard JSON and expected panels.
- Create demo runbook explaining how to start the pipeline and what story to show.

## Validation Strategy

Validate at four levels:

- Contract validation: generated JSON matches schema contracts.
- Referential integrity: every tag maps to a measurement point, component, asset, plant, and tenant.
- Industrial realism: failure modes create multi-signal patterns that match expected directions.
- Dashboard readiness: Doris views return non-empty rows for all critical panels.

Minimum demo acceptance criteria:

- At least 30 days of 1 Hz data can be generated or replayed for a scoped demo subset.
- At least one pump, motor, compressor, conveyor, and reactor scenario exists.
- At least one RUL curve visibly declines before intervention.
- At least one maintenance event improves post-maintenance signal behavior.
- Grafana landing page shows fleet health, anomalies, RUL risk, and energy penalty.
- Asset workbench can drill into component and tag-level trends.
- Anomaly detail page shows evidence-grounded LLM-assisted triage without diagnosis claims.

## Recommended Implementation Order

1. Add `measurement_points` as an explicit model and config.
2. Upgrade tag generation from asset-metric tags to measurement-point tags.
3. Add 30-day scenario definitions for chemical and metallurgy assets.
4. Generate raw 1 Hz telemetry with operating-state and failure-mode context.
5. Generate 15-minute aggregates.
6. Generate supporting business data.
7. Add Doris tables/views for aggregates, RUL, and measurement points.
8. Update Grafana dashboards in the A/B/C structure.
9. Add end-to-end smoke tests and demo documentation.

## Implementation Defaults

- Generate a smaller replayable 30-day demo subset first instead of fully materializing every asset/tag combination. This keeps local Kafka, Flink, Doris, and Grafana runs practical while still proving the end-to-end pattern.
- Run the Kafka mock producer in two phases: accelerated `backfill_replay` for the 30-day historical timeline, then `realtime_live` at 1 Hz for live dashboard and streaming verification.
- Grafana should query curated Doris serving views by default. Raw tables remain available for inspection and debugging.
- Keep the raw Kafka contract tag-centric where possible. Introduce `measurement_point_id`, component context, and failure-mode context during enrichment, while allowing synthetic metadata on raw events for deterministic replay.
- Generate RUL predictions with a deterministic synthetic model first. Add a model interface later only if the project needs to demonstrate ML integration beyond APM data plumbing and dashboard storytelling.
