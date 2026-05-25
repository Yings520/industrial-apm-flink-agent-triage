# Data Contracts

Phase 1 uses JSON Schema files in `contracts/` as the only machine-readable contract source. The YAML files in `configs/` define the portfolio, scenario inventory, anomaly rules, and prompt placeholders that drive synthetic telemetry.

## Schema Versioning

Every event or output contract includes `schema_version`. Producers must emit the exact version expected by the schema, such as `sensor_event.v1`, so downstream Kafka topics, Flink jobs, validation summaries, and triage services can reject incompatible records before stateful processing.

## Timestamp Semantics

All timestamps are timezone-aware UTC ISO-8601 strings ending in `Z`. `event_time` means the industrial source time. `ingest_time` means the collector time. Window fields such as `window_start` and `window_end` use the same UTC representation for Flink event-time processing and watermark planning.

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
| metric_name | string | yes | `vibration` | string | Metric context |
| window_start | date-time | yes | `2026-01-01T01:00:00Z` | UTC ISO-8601 | Windowed detection |
| window_end | date-time | yes | `2026-01-01T01:05:00Z` | UTC ISO-8601 | Windowed detection |
| severity | string | yes | `critical` | info/warning/critical | Operator prioritization |
| quality_flags | array | yes | `["threshold_spike"]` | strings | Data quality context |
| detection_confidence | number | yes | `0.91` | 0 to 1 | Ranking and triage |

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

The README quickstart writes generated telemetry candidates to `data/generated/sensor_events_*.jsonl`, accepted validated records to `data/generated/validated_sensor_events.jsonl`, rejected DLQ-shaped records to `data/rejected/rejected_records.jsonl`, and the JSON-only validation report to `data/validation-summary.json`.

## Industrial Archetypes

The locked plant/site archetypes are `manufacturing_factory`, `chemical_plant`, `power_plant`, `metallurgy_steel`, `water_treatment_utilities`, `campus_facilities`, and `industrial_asset_management_generic`.

Each archetype shapes asset types, expected metrics, units, threshold profiles, and plausible scenarios. For example, `water_treatment_utilities` emphasizes flow, pressure, pumps, and missing-heartbeat handling, while `metallurgy_steel` emphasizes high heat, rolling equipment, pressure, vibration, spike, and drift behavior.
