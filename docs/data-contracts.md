# Data Contracts

JSON Schema files in `contracts/` are the only machine-readable contract source. The YAML files in `configs/` define the portfolio, tag metadata, scenario inventory, anomaly rules, streaming topic defaults, and prompt placeholders that drive synthetic telemetry.

## Schema Versioning

Every event or output contract includes `schema_version`. Producers must emit the exact version expected by the schema, such as `sensor_event.v1`, so downstream Kafka topics, Flink jobs, validation summaries, and triage services can reject incompatible records before stateful processing.

## Timestamp Semantics

All timestamps are timezone-aware UTC ISO-8601 strings ending in `Z`. `event_time` means the industrial source time. `ingest_time` means the collector time. Window fields such as `window_start` and `window_end` use the same UTC representation for Flink event-time processing and watermark planning.

## Streaming Layers

Phase 2 models Kafka topics with raw/staging/mart layers. Raw topics contain original tag-level sensor readings. Staging topics contain Flink-enriched telemetry after tag-to-asset and tag-to-metric mapping. Mart topics contain downstream products such as anomaly events, late events, stream health, and quality summaries.

Default tenant-scoped topic templates live in `configs/streaming.yml`: `{tenant_id}.raw_apm__sensor_readings.v1`, `{tenant_id}.raw_apm__dlq_events.v1`, `{tenant_id}.staging_apm__sensor_readings.v1`, `{tenant_id}.staging_apm__dlq_events.v1`, `{tenant_id}.mart_apm__fct_anomaly_events.v1`, `{tenant_id}.mart_apm__fct_late_sensor_readings.v1`, and `{tenant_id}.mart_apm__fct_stream_health_snapshots.v1`.

Tenant-scoped topics are an MVP default for clarity and tenant isolation. A larger production system may prefer shared topics with tenant-aware partitioning or dedicated topics only for high-isolation tenants.

## raw_sensor_event

Machine contract: `raw_sensor_event.schema.json`

Raw sensor events are tag-centric. They intentionally do not require `asset_id`, `plant_id`, `metric_name`, or `threshold_profile_id`; Flink derives those fields from static tag metadata.

| Field | Type | Required | Example | Validation Rule | Downstream Use |
| --- | --- | --- | --- | --- | --- |
| event_id | string | yes | `evt_abc123` | non-empty | Idempotent replay and traceability |
| schema_version | string | yes | `raw_sensor_event.v1` | fixed version | Contract compatibility |
| tenant_id | string | yes | `tenant_northwind` | tenant pattern | Tenant isolation and topic routing |
| tag_id | string | yes | `tag_northwind_mfg_01_0001_temperature` | tag pattern | Flink metadata enrichment key |
| tag_name | string | yes | `cnc_machine_temperature_0001` | non-empty | Operator context |
| event_time | date-time | yes | `2026-01-01T00:00:00Z` | UTC ISO-8601 | Flink event time |
| ingest_time | date-time | yes | `2026-01-01T00:00:05Z` | UTC ISO-8601 | Lateness checks |
| value | number | yes | `74.2` | numeric | Aggregations and anomaly scoring |
| unit | string | yes | `celsius` | known unit | Metadata consistency checks |
| source_system | string | yes | `opc_ua` | known source | Lineage |
| quality_flags | string array | yes | `["late_arrival"]` | unique strings | Data quality and DLQ routing |
| scenario | string | yes | `late` | valid generated scenario | Replay and validation evidence |

## sensor_event

Machine contract: `sensor_event.schema.json`

| Field | Type | Required | Example | Validation Rule | Downstream Use |
| --- | --- | --- | --- | --- | --- |
| event_id | string | yes | `evt_demo_000001` | non-empty | Idempotent replay and traceability |
| schema_version | string | yes | `sensor_event.v1` | fixed version | Contract compatibility |
| tenant_id | string | yes | `tenant_northwind` | tenant pattern | Tenant isolation |
| plant_id | string | yes | `plant_northwind_mfg_01` | plant pattern | Site partitioning |
| asset_id | string | yes | `asset_northwind_mfg_01_0001` | asset pattern | Asset state and joins |
| metric_name | string | yes | `temperature` | one of 5 metric types | Rule routing |
| event_time | date-time | yes | `2026-01-01T00:00:00Z` | UTC ISO-8601 | Flink event time |
| ingest_time | date-time | yes | `2026-01-01T00:00:05Z` | UTC ISO-8601 | Lateness checks |
| value | number | yes | `74.2` | numeric | Aggregations and anomaly scoring |
| unit | string | yes | `celsius` | known unit | Display and validation |
| source_system | string | yes | `opc_ua` | known source | Lineage |
| quality_flags | string array | yes | `["late_arrival"]` | unique strings | Data quality and DLQ routing |
| scenario | string | yes | `late` | valid generated scenario | Replay and validation evidence |

## asset_metadata

Machine contract: `asset_metadata.schema.json`

| Field | Type | Required | Example | Validation Rule | Downstream Use |
| --- | --- | --- | --- | --- | --- |
| schema_version | string | yes | `asset_metadata.v1` | fixed version | Contract compatibility |
| tenant_id | string | yes | `tenant_apac_ops` | string | Tenant isolation |
| site_id | string | yes | `site_apac_singapore` | string | Facility grouping |
| plant_id | string | yes | `plant_apac_water_01` | string | Plant joins |
| asset_id | string | yes | `asset_apac_water_01_0001` | string | Event joins |
| asset_type | string | yes | `lift_pump` | string | Rule tuning |
| criticality | string | yes | `critical` | low/medium/high/critical | Alert priority |
| expected_metrics | array | yes | `["flow_rate", "pressure"]` | non-empty | Completeness checks |
| threshold_profile_id | string | yes | `threshold_water_flow_quality` | string | Rule lookup |
| archetype | string | yes | `water_treatment_utilities` | locked archetype | Scenario realism |

## anomaly_event

Machine contract: `anomaly_event.schema.json`

| Field | Type | Required | Example | Validation Rule | Downstream Use |
| --- | --- | --- | --- | --- | --- |
| anomaly_id | string | yes | `anom_000001` | string | Alert identity |
| schema_version | string | yes | `anomaly_event.v1` | fixed version | Contract compatibility |
| rule_id | string | yes | `threshold_spike` | string | Rule attribution |
| tenant_id | string | yes | `tenant_euro_core` | string | Tenant isolation |
| plant_id | string | yes | `plant_euro_generic_01` | string | Plant routing |
| asset_id | string | yes | `asset_euro_generic_01_0001` | string | Asset context |
| asset_name | string | yes | `Motor 0001` | string | Operator display |
| tag_id | string | yes | `tag_euro_generic_01_0001_vibration` | string | Signal-level dedupe |
| tag_name | string | yes | `motor_vibration_0001` | string | Signal context |
| metric_name | string | yes | `vibration` | string | Metric context |
| observed_value | number | yes | `8.7` | numeric | Evidence |
| baseline_value | number/null | yes | `2.4` | numeric or null | Baseline evidence |
| expected_value | number/null | yes | `3.0` | numeric or null | Expected-value evidence |
| event_count | integer | yes | `24` | non-negative | Window evidence |
| breach_count | integer | yes | `6` | non-negative | Window evidence |
| window_start | date-time | yes | `2026-01-01T01:00:00Z` | UTC ISO-8601 | Windowed detection |
| window_end | date-time | yes | `2026-01-01T01:05:00Z` | UTC ISO-8601 | Windowed detection |
| severity | string | yes | `critical` | info/warning/critical | Operator prioritization |
| quality_flags | array | yes | `["threshold_spike"]` | strings | Data quality context |
| detection_confidence | number | yes | `0.91` | 0 to 1 | Ranking and triage |
| source_event_ids | array | yes | `["evt_abc123"]` | max 10 unique strings | Bounded traceability without embedding full event lists |

## enriched_telemetry

Machine contract: `enriched_telemetry.schema.json`

Staging telemetry is asset-enriched. Flink maps raw `tag_id` values to `plant_id`, `asset_id`, `asset_name`, `metric_name`, and `threshold_profile_id` by reading static metadata from `configs/tag_mapping.yml` and `configs/assets.yml`.

## DLQ, Late Events, And Stream Health

Machine contracts: `dlq_record.schema.json`, `late_event.schema.json`, and `stream_health.schema.json`

DLQ records preserve `original_record`, `error_code`, `field_path`, `message`, `schema_name`, `schema_version`, `validation_time`, and optional `all_errors`. Late events preserve the source event timing, watermark time, lateness minutes, and reason. Stream-health records expose job name, tenant ID, watermark lag, late-event count, checkpoint status/config, and observation time.

The local event-time defaults are `watermark_delay_minutes: 5` and `allowed_lateness_minutes: 10`. They are demo defaults for deterministic replay and inspection, not production best practices. Production tuning depends on sampling frequency, network delay, edge buffering, and alert latency goals.

Flink runtime output order is not a contract. Replay verification sorts anomaly records by deterministic keys: `anomaly_id`, `rule_id`, `tenant_id`, `asset_id`, `tag_id`, `metric_name`, `window_start`, and `window_end`.

## agent_explanation

Machine contract: `agent_explanation.schema.json`

| Field | Type | Required | Example | Validation Rule | Downstream Use |
| --- | --- | --- | --- | --- | --- |
| explanation_id | string | yes | `expl_000001` | string | Explanation identity |
| schema_version | string | yes | `agent_explanation.v1` | fixed version | Contract compatibility |
| tenant_id | string | yes | `tenant_northwind` | string | Tenant isolation |
| anomaly_id | string | yes | `anom_000001` | string | Alert join |
| evidence | array | yes | `["vibration exceeded baseline"]` | strings | Operator context |
| possible_causes | array | yes | `["bearing wear"]` | strings | Hypothesis support |
| recommended_checks | array | yes | `["Inspect bearing temperature"]` | strings | Next actions |
| runbook_refs | array | yes | `["runbooks/..."]` | strings | Documentation links |
| confidence_label | string | yes | `medium` | low/medium/high | Human trust calibration |
| model_name | string | yes | `placeholder` | string | Model lineage |
| prompt_version | string | yes | `phase1-placeholder` | string | Prompt lineage |
| token_cost | number | yes | `0.0` | non-negative | Cost tracking |
| latency_ms | integer | yes | `0` | non-negative | Performance tracking |

## operator_feedback

Machine contract: `operator_feedback.schema.json`

| Field | Type | Required | Example | Validation Rule | Downstream Use |
| --- | --- | --- | --- | --- | --- |
| feedback_id | string | yes | `fb_000001` | string | Feedback identity |
| schema_version | string | yes | `operator_feedback.v1` | fixed version | Contract compatibility |
| tenant_id | string | yes | `tenant_apac_ops` | string | Tenant isolation |
| anomaly_id | string | yes | `anom_000001` | string | Alert join |
| operator_id | string | yes | `operator_17` | string | Audit trail |
| is_true_positive | boolean | yes | `true` | boolean | Model/rule quality labels |
| severity_correction | string | yes | `warning` | none/info/warning/critical | Alert quality tuning |
| explanation_usefulness | integer | yes | `4` | 1 to 5 | Agent evaluation |
| resolution_note | string | yes | `Pump seal replaced` | string | Case closure |
| created_at | date-time | yes | `2026-01-01T02:00:00Z` | UTC ISO-8601 | Feedback timeline |

## Scenario Profiles

`configs/scenarios.yml` defines `smoke` and `demo` profiles. The `demo` baseline is 3 tenants, 3 plants per tenant, and 20 assets per plant, for 180 assets total. It covers 5 metric types and 5 anomaly scenarios: late arrival, missing heartbeat, flatline, spike, and drift. `invalid_event` is a deliberate contract failure scenario for validation and rejected-record planning.

## Local Artifacts

The README quickstart writes generated raw telemetry candidates to `data/generated/raw_sensor_events_*.jsonl`, accepted validated records to `data/generated/validated_raw_sensor_events.jsonl`, rejected DLQ-shaped records to `data/rejected/rejected_raw_sensor_records.jsonl`, and the JSON-only validation report to `data/validation-summary.json`.

## Industrial Archetypes

The locked plant/site archetypes are `manufacturing_factory`, `chemical_plant`, `power_plant`, `metallurgy_steel`, `water_treatment_utilities`, `campus_facilities`, and `industrial_asset_management_generic`.

Each archetype shapes asset types, expected metrics, units, threshold profiles, and plausible scenarios. For example, `water_treatment_utilities` emphasizes flow, pressure, pumps, and missing-heartbeat handling, while `metallurgy_steel` emphasizes high heat, rolling equipment, pressure, vibration, spike, and drift behavior.
