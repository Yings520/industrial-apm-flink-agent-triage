# Industrial APM Synthetic Data And Grafana Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a realistic end-to-end industrial APM demo data system with measurement-point-aware synthetic telemetry, accelerated 30-day Kafka backfill replay, realtime 1 Hz live generation, Doris serving views, and three Grafana dashboards.

**Architecture:** Keep Kafka raw events tag-centric, enrich them with asset/component/measurement-point context, and serve curated Doris views to Grafana. Synthetic data uses deterministic scenario state machines so historical backfill and live 1 Hz telemetry tell one continuous degradation, maintenance, and recovery story.

**Tech Stack:** Python 3.14, pytest, JSON Schema, YAML configs, Kafka/Redpanda via `rpk`, Flink SQL/Python job helpers, Apache Doris SQL, Grafana dashboard JSON.

---

## File Structure

Create:

- `configs/measurement_points.yml` - physical measurement-point catalog by asset type and component type.
- `configs/synthetic_scenarios.yml` - 30-day industrial operating-state and failure-mode timelines.
- `src/telemetry_generator/measurement_points.py` - builds deterministic components, measurement points, and sensor tags.
- `src/telemetry_generator/signal_profiles.py` - signal formulas for baseline, load, noise, degradation, failure, maintenance, and recovery.
- `src/telemetry_generator/window_aggregates.py` - computes 15-minute aggregates from generated raw events.
- `src/telemetry_generator/business_data.py` - generates supporting datasets: assets, components, measurement points, tags, work orders, maintenance events, RUL predictions, and triage evidence.
- `tests/test_measurement_points.py` - mapping and referential-integrity tests.
- `tests/test_signal_profiles.py` - industrial signal behavior tests.
- `tests/test_window_aggregates.py` - 15-minute aggregation tests.
- `tests/test_business_data_generation.py` - supporting data tests.

Modify:

- `src/telemetry_generator/models.py` - add typed configs and records for measurement points, sensor tags, scenario timelines, aggregates, business records, and producer modes.
- `src/telemetry_generator/config.py` - load measurement-point and synthetic-scenario config; build enriched tag metadata.
- `src/telemetry_generator/generate_events.py` - add realistic industrial generation while preserving existing smoke/demo behavior until migration tests pass.
- `src/streaming/publish_raw_events.py` - add `backfill_replay` and `realtime_live` producer modes.
- `contracts/raw_sensor_event.schema.json` - allow optional synthetic metadata fields needed for deterministic replay.
- `contracts/enriched_telemetry.schema.json` - ensure component and measurement-point context exists after enrichment.
- `sql/doris_schema.sql` - add `dim_measurement_point`, `agg_apm_sensor_windows_15m`, and `fact_apm_rul_predictions`.
- `sql/doris_serving_views.sql` - add curated Grafana views.
- `configs/grafana/dashboards/apm-overview.json` - convert to APM Landing Dashboard.
- `configs/grafana/dashboards/apm-asset-health.json` - convert to Asset Operations Workbench.
- `configs/grafana/dashboards/apm-anomaly-deep-dive.json` - convert to Anomaly / RUL Detail Dashboard.
- `tests/test_generate_events.py` - update data-volume and scenario assertions.
- `tests/test_publish_raw_events.py` - cover replay/live routing behavior.
- `tests/test_doris_sql_contract.py` - cover new Doris objects and views.
- `tests/test_grafana_smoke.py` - cover A/B/C dashboard panels and data-source queries.
- `README.md` or `docs/project_implementation_backlog.md` - document demo run command and story.

---

## Task 1: Add Measurement-Point Configuration And Typed Models

**Files:**

- Create: `configs/measurement_points.yml`
- Create: `src/telemetry_generator/measurement_points.py`
- Modify: `src/telemetry_generator/models.py`
- Modify: `src/telemetry_generator/config.py`
- Test: `tests/test_measurement_points.py`

- [ ] **Step 1: Write failing tests for measurement-point catalog loading**

Add `tests/test_measurement_points.py`:

```python
from src.telemetry_generator.config import load_measurement_point_config
from src.telemetry_generator.measurement_points import build_measurement_point_catalog


def test_measurement_point_config_covers_target_asset_types() -> None:
    config = load_measurement_point_config()

    assert config["schema_version"] == "measurement_points.v1"
    assert {"pump", "motor", "compressor", "conveyor", "reactor"}.issubset(
        set(config["asset_types"])
    )


def test_measurement_points_expand_to_tags_with_component_context() -> None:
    catalog = build_measurement_point_catalog(asset_type="pump")

    vibration_points = [
        point for point in catalog if point["metric_name"] == "vibration_rms"
    ]
    assert vibration_points
    assert all(point["component_type"] for point in vibration_points)
    assert all(point["measurement_point_id"] for point in vibration_points)
    assert all(point["tag_suffix"].endswith("_mm_s") for point in vibration_points)
```

- [ ] **Step 2: Run test to verify failure**

Run:

```bash
rtk ./.venv/bin/pytest tests/test_measurement_points.py -q
```

Expected: FAIL because `load_measurement_point_config` and `measurement_points.py` do not exist.

- [ ] **Step 3: Add `configs/measurement_points.yml`**

Create a first usable catalog:

```yaml
schema_version: measurement_points.v1
asset_types: [pump, motor, compressor, conveyor, reactor]
measurement_points:
  pump:
    - component_type: bearing_de
      measurement_point: drive_end_bearing_vertical
      metric_name: vibration_rms
      tag_suffix: vibration_rms_mm_s
      unit: mm_s
      sampling_rate_hz: 1.0
      normal_range_min: 0.3
      normal_range_max: 4.5
      engineering_limit_min: 0.0
      engineering_limit_max: 18.0
    - component_type: bearing_de
      measurement_point: drive_end_bearing_temperature
      metric_name: temperature
      tag_suffix: temperature_c
      unit: celsius
      sampling_rate_hz: 1.0
      normal_range_min: 35.0
      normal_range_max: 82.0
      engineering_limit_min: -20.0
      engineering_limit_max: 140.0
    - component_type: seal
      measurement_point: seal_chamber_pressure
      metric_name: pressure
      tag_suffix: pressure_bar
      unit: bar
      sampling_rate_hz: 1.0
      normal_range_min: 2.0
      normal_range_max: 9.5
      engineering_limit_min: 0.0
      engineering_limit_max: 20.0
    - component_type: impeller
      measurement_point: discharge_flow
      metric_name: flow_rate
      tag_suffix: flow_rate_m3_h
      unit: m3_h
      sampling_rate_hz: 1.0
      normal_range_min: 80.0
      normal_range_max: 180.0
      engineering_limit_min: 0.0
      engineering_limit_max: 260.0
  motor:
    - component_type: stator
      measurement_point: winding_temperature
      metric_name: temperature
      tag_suffix: temperature_c
      unit: celsius
      sampling_rate_hz: 1.0
      normal_range_min: 45.0
      normal_range_max: 95.0
      engineering_limit_min: -20.0
      engineering_limit_max: 155.0
    - component_type: terminal_box
      measurement_point: phase_current
      metric_name: motor_current
      tag_suffix: motor_current_a
      unit: ampere
      sampling_rate_hz: 1.0
      normal_range_min: 35.0
      normal_range_max: 180.0
      engineering_limit_min: 0.0
      engineering_limit_max: 280.0
  compressor:
    - component_type: compressor_stage
      measurement_point: stage_vibration
      metric_name: vibration_rms
      tag_suffix: vibration_rms_mm_s
      unit: mm_s
      sampling_rate_hz: 1.0
      normal_range_min: 0.5
      normal_range_max: 5.0
      engineering_limit_min: 0.0
      engineering_limit_max: 22.0
    - component_type: compressor_stage
      measurement_point: discharge_pressure
      metric_name: pressure
      tag_suffix: pressure_bar
      unit: bar
      sampling_rate_hz: 1.0
      normal_range_min: 5.0
      normal_range_max: 16.0
      engineering_limit_min: 0.0
      engineering_limit_max: 35.0
  conveyor:
    - component_type: drive_motor
      measurement_point: motor_power
      metric_name: power
      tag_suffix: power_kw
      unit: kw
      sampling_rate_hz: 1.0
      normal_range_min: 8.0
      normal_range_max: 40.0
      engineering_limit_min: 0.0
      engineering_limit_max: 80.0
    - component_type: gearbox
      measurement_point: gearbox_vibration
      metric_name: vibration_rms
      tag_suffix: vibration_rms_mm_s
      unit: mm_s
      sampling_rate_hz: 1.0
      normal_range_min: 0.3
      normal_range_max: 4.0
      engineering_limit_min: 0.0
      engineering_limit_max: 18.0
  reactor:
    - component_type: jacket
      measurement_point: jacket_temperature
      metric_name: temperature
      tag_suffix: temperature_c
      unit: celsius
      sampling_rate_hz: 1.0
      normal_range_min: 65.0
      normal_range_max: 115.0
      engineering_limit_min: -20.0
      engineering_limit_max: 220.0
    - component_type: vessel
      measurement_point: vessel_pressure
      metric_name: pressure
      tag_suffix: pressure_bar
      unit: bar
      sampling_rate_hz: 1.0
      normal_range_min: 1.5
      normal_range_max: 8.0
      engineering_limit_min: 0.0
      engineering_limit_max: 18.0
```

- [ ] **Step 4: Add typed models**

In `src/telemetry_generator/models.py`, add:

```python
class MeasurementPointConfig(TypedDict):
    component_type: str
    measurement_point: str
    metric_name: str
    tag_suffix: str
    unit: str
    sampling_rate_hz: float
    normal_range_min: float
    normal_range_max: float
    engineering_limit_min: float
    engineering_limit_max: float


class MeasurementPointCatalogConfig(TypedDict):
    schema_version: str
    asset_types: list[str]
    measurement_points: dict[str, list[MeasurementPointConfig]]


class MeasurementPointRecord(MeasurementPointConfig):
    asset_type: str
    measurement_point_id: str
```

- [ ] **Step 5: Load config and build catalog**

In `src/telemetry_generator/config.py`, import the new types and add float parsing:

```python
def _as_float(value: object, context: str) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError(f"Expected number for {context}")
    return float(value)
```

Add:

```python
def load_measurement_point_config() -> MeasurementPointCatalogConfig:
    raw = load_yaml_config(CONFIG_DIR / "measurement_points.yml")
    points_raw = _as_mapping(raw["measurement_points"], "measurement_points.measurement_points")
    points: dict[str, list[MeasurementPointConfig]] = {}
    for asset_type, value in points_raw.items():
        records = []
        for index, item in enumerate(_as_sequence(value, f"measurement_points.{asset_type}")):
            record = _as_mapping(item, f"measurement_points.{asset_type}[{index}]")
            records.append(
                {
                    "component_type": _as_str(record["component_type"], "component_type"),
                    "measurement_point": _as_str(record["measurement_point"], "measurement_point"),
                    "metric_name": _as_str(record["metric_name"], "metric_name"),
                    "tag_suffix": _as_str(record["tag_suffix"], "tag_suffix"),
                    "unit": _as_str(record["unit"], "unit"),
                    "sampling_rate_hz": _as_float(record["sampling_rate_hz"], "sampling_rate_hz"),
                    "normal_range_min": _as_float(record["normal_range_min"], "normal_range_min"),
                    "normal_range_max": _as_float(record["normal_range_max"], "normal_range_max"),
                    "engineering_limit_min": _as_float(record["engineering_limit_min"], "engineering_limit_min"),
                    "engineering_limit_max": _as_float(record["engineering_limit_max"], "engineering_limit_max"),
                }
            )
        points[asset_type] = records
    return {
        "schema_version": _as_str(raw["schema_version"], "measurement_points.schema_version"),
        "asset_types": _as_str_list(raw["asset_types"], "measurement_points.asset_types"),
        "measurement_points": points,
    }
```

Create `src/telemetry_generator/measurement_points.py`:

```python
from __future__ import annotations

import hashlib

from src.telemetry_generator.config import load_measurement_point_config
from src.telemetry_generator.models import MeasurementPointRecord


def _stable_measurement_point_id(asset_type: str, component_type: str, measurement_point: str) -> str:
    digest = hashlib.sha1(f"{asset_type}|{component_type}|{measurement_point}".encode("utf-8")).hexdigest()[:10]
    return f"mp_{asset_type}_{digest}"


def build_measurement_point_catalog(asset_type: str) -> list[MeasurementPointRecord]:
    config = load_measurement_point_config()
    if asset_type not in config["measurement_points"]:
        raise ValueError(f"Unknown asset_type for measurement points: {asset_type}")
    return [
        {
            **point,
            "asset_type": asset_type,
            "measurement_point_id": _stable_measurement_point_id(
                asset_type,
                point["component_type"],
                point["measurement_point"],
            ),
        }
        for point in config["measurement_points"][asset_type]
    ]
```

- [ ] **Step 6: Run tests**

Run:

```bash
rtk ./.venv/bin/pytest tests/test_measurement_points.py -q
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
rtk git add configs/measurement_points.yml src/telemetry_generator/models.py src/telemetry_generator/config.py src/telemetry_generator/measurement_points.py tests/test_measurement_points.py
rtk git commit -m "Add measurement point catalog"
```

---

## Task 2: Upgrade Tag Metadata To Measurement-Point Tags

**Files:**

- Modify: `src/telemetry_generator/models.py`
- Modify: `src/telemetry_generator/config.py`
- Modify: `configs/tag_mapping.yml`
- Test: `tests/test_measurement_points.py`
- Test: `tests/test_generate_events.py`

- [ ] **Step 1: Add failing test for measurement-point-aware tag metadata**

Append to `tests/test_measurement_points.py`:

```python
from src.telemetry_generator.config import build_tag_metadata


def test_tag_metadata_contains_measurement_point_and_component_context() -> None:
    metadata = build_tag_metadata()
    sample = next(iter(metadata.values()))

    assert sample["component_id"].startswith("comp_")
    assert sample["component_type"]
    assert sample["measurement_point_id"].startswith("mp_")
    assert sample["sampling_rate_hz"] == 1.0
    assert sample["normal_range_min"] < sample["normal_range_max"]
    assert sample["engineering_limit_min"] < sample["engineering_limit_max"]
```

- [ ] **Step 2: Run test to verify failure**

Run:

```bash
rtk ./.venv/bin/pytest tests/test_measurement_points.py::test_tag_metadata_contains_measurement_point_and_component_context -q
```

Expected: FAIL because `TagMetadata` lacks these keys.

- [ ] **Step 3: Extend `TagMetadata` type**

In `src/telemetry_generator/models.py`, replace or extend `TagMetadata` with:

```python
class TagMetadata(TypedDict):
    tenant_id: str
    plant_id: str
    asset_id: str
    asset_name: str
    asset_type: str
    component_id: str
    component_type: str
    measurement_point_id: str
    measurement_point: str
    tag_id: str
    tag_name: str
    metric_name: str
    unit: str
    sampling_rate_hz: float
    normal_range_min: float
    normal_range_max: float
    engineering_limit_min: float
    engineering_limit_max: float
    threshold_profile_id: str
```

- [ ] **Step 4: Update tag naming and metadata generation**

In `src/telemetry_generator/config.py`, import `build_measurement_point_catalog`. Add:

```python
def build_component_id(tenant_id: str, asset_id: str, component_type: str, ordinal: int = 1) -> str:
    digest = f"{tenant_id}|{asset_id}|{component_type}|{ordinal}"
    return "comp_" + __import__("hashlib").sha1(digest.encode("utf-8")).hexdigest()[:16]


def build_measurement_tag_id(plant_id: str, asset_index: int, measurement_point_id: str, tag_suffix: str) -> str:
    return f"tag_{_plant_slug(plant_id)}_{asset_index:04d}_{measurement_point_id}_{tag_suffix}"
```

Update `build_tag_metadata()` so each asset expands measurement points for the asset type:

```python
for point in build_measurement_point_catalog(asset_type):
    component_id = build_component_id(tenant_id, asset_id, point["component_type"])
    tag_id = build_measurement_tag_id(
        plant_id,
        asset_index,
        point["measurement_point_id"],
        point["tag_suffix"],
    )
    mappings[tag_id] = {
        "tenant_id": tenant_id,
        "plant_id": plant_id,
        "asset_id": asset_id,
        "asset_name": asset_name,
        "asset_type": asset_type,
        "component_id": component_id,
        "component_type": point["component_type"],
        "measurement_point_id": point["measurement_point_id"],
        "measurement_point": point["measurement_point"],
        "tag_id": tag_id,
        "tag_name": f"{asset_type}_{point['measurement_point']}_{point['tag_suffix']}_{asset_index:04d}",
        "metric_name": point["metric_name"],
        "unit": point["unit"],
        "sampling_rate_hz": point["sampling_rate_hz"],
        "normal_range_min": point["normal_range_min"],
        "normal_range_max": point["normal_range_max"],
        "engineering_limit_min": point["engineering_limit_min"],
        "engineering_limit_max": point["engineering_limit_max"],
        "threshold_profile_id": threshold_profile_id,
    }
```

Keep `build_tag_id()` temporarily for old tests and backward-compatible smoke generation until Task 3 migrates generation.

- [ ] **Step 5: Update tag mapping config description**

Modify `configs/tag_mapping.yml`:

```yaml
schema_version: tag_mapping.v2
description: Static metadata mapping derived from asset, component, measurement-point, and tag definitions.
tag_id_pattern: "tag_{plant_slug}_{asset_index:04d}_{measurement_point_id}_{tag_suffix}"
tag_name_pattern: "{asset_type}_{measurement_point}_{tag_suffix}_{asset_index:04d}"
required_fields:
  - tenant_id
  - plant_id
  - asset_id
  - asset_name
  - asset_type
  - component_id
  - component_type
  - measurement_point_id
  - measurement_point
  - tag_id
  - tag_name
  - metric_name
  - unit
  - sampling_rate_hz
  - normal_range_min
  - normal_range_max
  - engineering_limit_min
  - engineering_limit_max
  - threshold_profile_id
```

- [ ] **Step 6: Run focused tests**

Run:

```bash
rtk ./.venv/bin/pytest tests/test_measurement_points.py tests/test_generate_events.py::test_generated_raw_tags_have_unique_tenant_mapping -q
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
rtk git add configs/tag_mapping.yml src/telemetry_generator/models.py src/telemetry_generator/config.py tests/test_measurement_points.py
rtk git commit -m "Map tags to measurement points"
```

---

## Task 3: Add Industrial Signal Profiles And 30-Day Scenarios

**Files:**

- Create: `configs/synthetic_scenarios.yml`
- Create: `src/telemetry_generator/signal_profiles.py`
- Modify: `src/telemetry_generator/models.py`
- Modify: `src/telemetry_generator/config.py`
- Test: `tests/test_signal_profiles.py`

- [ ] **Step 1: Write failing signal behavior tests**

Create `tests/test_signal_profiles.py`:

```python
from datetime import UTC, datetime, timedelta

from src.telemetry_generator.signal_profiles import (
    OperatingState,
    SignalContext,
    signal_value,
)


def test_degradation_increases_vibration_over_time() -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    context = SignalContext(
        metric_name="vibration_rms",
        normal_min=0.3,
        normal_max=4.5,
        engineering_min=0.0,
        engineering_max=18.0,
        failure_mode_id="fm_bearing_inner_race_wear",
        operating_state=OperatingState.DEGRADATION,
        scenario_start=start,
        scenario_end=start + timedelta(days=30),
        seed="asset_001|bearing",
    )

    early = signal_value(context, start + timedelta(days=2))
    late = signal_value(context, start + timedelta(days=24))

    assert late > early
    assert early >= context.engineering_min
    assert late <= context.engineering_max


def test_maintenance_suppresses_load_sensitive_signals() -> None:
    now = datetime(2026, 1, 15, tzinfo=UTC)
    context = SignalContext(
        metric_name="flow_rate",
        normal_min=80.0,
        normal_max=180.0,
        engineering_min=0.0,
        engineering_max=260.0,
        failure_mode_id="fm_pump_cavitation",
        operating_state=OperatingState.MAINTENANCE,
        scenario_start=now - timedelta(days=1),
        scenario_end=now + timedelta(days=1),
        seed="asset_001|impeller",
    )

    assert signal_value(context, now) < 20.0
```

- [ ] **Step 2: Run test to verify failure**

Run:

```bash
rtk ./.venv/bin/pytest tests/test_signal_profiles.py -q
```

Expected: FAIL because `signal_profiles.py` does not exist.

- [ ] **Step 3: Add `configs/synthetic_scenarios.yml`**

Create:

```yaml
schema_version: synthetic_scenarios.v1
default_start_time: "2026-01-01T00:00:00Z"
default_days: 30
operating_states:
  - startup
  - normal
  - steady_load
  - high_load
  - idle
  - warning
  - critical
  - degradation
  - failure
  - maintenance
  - shutdown
failure_timelines:
  pump_cavitation_30d:
    asset_type: pump
    failure_mode_id: fm_pump_cavitation
    component_type: impeller
    phases:
      - state: normal
        start_day: 0
        end_day: 10
      - state: degradation
        start_day: 10
        end_day: 22
      - state: warning
        start_day: 22
        end_day: 26
      - state: critical
        start_day: 26
        end_day: 28
      - state: maintenance
        start_day: 28
        end_day: 29
      - state: normal
        start_day: 29
        end_day: 30
  motor_overheating_30d:
    asset_type: motor
    failure_mode_id: fm_motor_winding_overheat
    component_type: stator
    phases:
      - state: normal
        start_day: 0
        end_day: 14
      - state: high_load
        start_day: 14
        end_day: 18
      - state: degradation
        start_day: 18
        end_day: 25
      - state: critical
        start_day: 25
        end_day: 28
      - state: maintenance
        start_day: 28
        end_day: 29
      - state: normal
        start_day: 29
        end_day: 30
```

- [ ] **Step 4: Implement signal profile functions**

Create `src/telemetry_generator/signal_profiles.py`:

```python
from __future__ import annotations

import hashlib
import math
import random
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum


class OperatingState(StrEnum):
    STARTUP = "startup"
    NORMAL = "normal"
    STEADY_LOAD = "steady_load"
    HIGH_LOAD = "high_load"
    IDLE = "idle"
    WARNING = "warning"
    CRITICAL = "critical"
    DEGRADATION = "degradation"
    FAILURE = "failure"
    MAINTENANCE = "maintenance"
    SHUTDOWN = "shutdown"


@dataclass(frozen=True)
class SignalContext:
    metric_name: str
    normal_min: float
    normal_max: float
    engineering_min: float
    engineering_max: float
    failure_mode_id: str | None
    operating_state: OperatingState
    scenario_start: datetime
    scenario_end: datetime
    seed: str


def _rng(seed: str, timestamp: datetime) -> random.Random:
    minute_bucket = int(timestamp.timestamp() // 60)
    digest = hashlib.sha1(f"{seed}|{minute_bucket}".encode("utf-8")).hexdigest()
    return random.Random(int(digest[:12], 16))


def _progress(context: SignalContext, timestamp: datetime) -> float:
    total = max((context.scenario_end - context.scenario_start).total_seconds(), 1.0)
    elapsed = max((timestamp - context.scenario_start).total_seconds(), 0.0)
    return min(elapsed / total, 1.0)


def signal_value(context: SignalContext, timestamp: datetime) -> float:
    rng = _rng(context.seed, timestamp)
    midpoint = (context.normal_min + context.normal_max) / 2.0
    span = context.normal_max - context.normal_min
    hour = timestamp.hour + timestamp.minute / 60.0
    daily_cycle = math.sin((hour / 24.0) * 2.0 * math.pi) * span * 0.04
    noise = rng.uniform(-span * 0.025, span * 0.025)
    value = midpoint + daily_cycle + noise

    if context.operating_state == OperatingState.HIGH_LOAD:
        value += span * 0.18
    elif context.operating_state == OperatingState.IDLE:
        value -= span * 0.35
    elif context.operating_state == OperatingState.MAINTENANCE:
        value = context.engineering_min + span * 0.03
    elif context.operating_state in {
        OperatingState.DEGRADATION,
        OperatingState.WARNING,
        OperatingState.CRITICAL,
        OperatingState.FAILURE,
    }:
        multiplier = {
            OperatingState.DEGRADATION: 0.45,
            OperatingState.WARNING: 0.75,
            OperatingState.CRITICAL: 1.05,
            OperatingState.FAILURE: 1.25,
        }[context.operating_state]
        direction = -1.0 if context.metric_name == "flow_rate" else 1.0
        value += direction * span * multiplier * _progress(context, timestamp)

    return round(max(context.engineering_min, min(context.engineering_max, value)), 3)
```

- [ ] **Step 5: Add config loader types**

In `src/telemetry_generator/models.py`, add:

```python
class ScenarioPhaseConfig(TypedDict):
    state: str
    start_day: int
    end_day: int


class FailureTimelineConfig(TypedDict):
    asset_type: str
    failure_mode_id: str
    component_type: str
    phases: list[ScenarioPhaseConfig]


class SyntheticScenarioConfig(TypedDict):
    schema_version: str
    default_start_time: str
    default_days: int
    operating_states: list[str]
    failure_timelines: dict[str, FailureTimelineConfig]
```

In `src/telemetry_generator/config.py`, add `load_synthetic_scenario_config()` using the same `_as_mapping`, `_as_sequence`, `_as_str`, and `_as_int` helpers.

- [ ] **Step 6: Run tests**

Run:

```bash
rtk ./.venv/bin/pytest tests/test_signal_profiles.py -q
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
rtk git add configs/synthetic_scenarios.yml src/telemetry_generator/models.py src/telemetry_generator/config.py src/telemetry_generator/signal_profiles.py tests/test_signal_profiles.py
rtk git commit -m "Add industrial signal profiles"
```

---

## Task 4: Generate 30-Day Raw Telemetry With Replay Metadata

**Files:**

- Modify: `src/telemetry_generator/models.py`
- Modify: `src/telemetry_generator/generate_events.py`
- Modify: `contracts/raw_sensor_event.schema.json`
- Test: `tests/test_generate_events.py`
- Test: `tests/test_schema_validation.py`

- [ ] **Step 1: Add failing test for industrial profile**

Append to `tests/test_generate_events.py`:

```python
def test_industrial_demo_generates_measurement_point_raw_events() -> None:
    result = generate_events(profile_name="industrial_demo", seed=42)

    first = result.valid_events[0]
    assert first["schema_version"] == "raw_sensor_event.v1"
    assert "asset_id" not in first
    assert first["synthetic_metadata"]["producer_mode"] == "backfill_replay"
    assert first["synthetic_metadata"]["measurement_point_id"].startswith("mp_")
    assert first["synthetic_metadata"]["operating_state"] in {
        "normal",
        "degradation",
        "warning",
        "critical",
        "maintenance",
    }


def test_industrial_demo_event_time_spans_30_days() -> None:
    result = generate_events(profile_name="industrial_demo", seed=42)
    event_times = [
        datetime.fromisoformat(event["event_time"].replace("Z", "+00:00"))
        for event in result.valid_events
    ]

    assert max(event_times) - min(event_times) >= timedelta(days=29)
```

Also import `timedelta` at the top.

- [ ] **Step 2: Run test to verify failure**

Run:

```bash
rtk ./.venv/bin/pytest tests/test_generate_events.py::test_industrial_demo_generates_measurement_point_raw_events -q
```

Expected: FAIL because `industrial_demo` is not an allowed profile.

- [ ] **Step 3: Extend profile parser**

In `src/telemetry_generator/generate_events.py`, update parser choices:

```python
_ = parser.add_argument("--profile", choices=["smoke", "demo", "industrial_demo"], default="smoke")
```

Update `GenerateCliNamespace.profile` comments or defaults only if needed.

- [ ] **Step 4: Add industrial generation path**

In `generate_events()`, branch early:

```python
if profile_name == "industrial_demo":
    return generate_industrial_events(seed=seed)
```

Add `generate_industrial_events(seed: int) -> GenerationResult` in the same file or a new focused file if the function grows beyond 150 lines. It should:

- load `build_tag_metadata()`
- choose a small subset of tags for local practicality
- generate 30 days with one point per tag per minute for file-based tests
- keep `sampling_rate_hz: 1.0` in metadata so the realtime producer can emit per-second later
- set raw event `synthetic_metadata.producer_mode` to `backfill_replay`
- set raw event `synthetic_metadata.original_sampling_rate_hz` to `1.0`
- set raw event `synthetic_metadata.replay_acceleration` to an integer such as `900`

Use this event shape:

```python
RawSensorEvent(
    event_id=event_id,
    schema_version="raw_sensor_event.v1",
    tenant_id=metadata["tenant_id"],
    tag_id=metadata["tag_id"],
    tag_name=metadata["tag_name"],
    event_time=event_time,
    ingest_time=event_time,
    value=value,
    unit=metadata["unit"],
    source_system="historian",
    quality_flags=quality_flags,
    scenario=operating_state,
    synthetic_metadata={
        "generator_version": "2.0",
        "producer_mode": "backfill_replay",
        "asset_id": metadata["asset_id"],
        "component_id": metadata["component_id"],
        "component_type": metadata["component_type"],
        "measurement_point_id": metadata["measurement_point_id"],
        "measurement_point": metadata["measurement_point"],
        "operating_state": operating_state,
        "failure_mode_id": failure_mode_id,
        "degradation_stage": degradation_stage,
        "original_sampling_rate_hz": 1.0,
        "replay_acceleration": 900,
    },
)
```

- [ ] **Step 5: Relax schema if needed**

Inspect `contracts/raw_sensor_event.schema.json`. If `synthetic_metadata` already allows arbitrary object values, do not change it. If it blocks nested metadata keys, add explicit properties for:

```json
"synthetic_metadata": {
  "type": "object",
  "additionalProperties": true
}
```

- [ ] **Step 6: Run generation and schema tests**

Run:

```bash
rtk ./.venv/bin/pytest tests/test_generate_events.py tests/test_schema_validation.py -q
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
rtk git add src/telemetry_generator/generate_events.py src/telemetry_generator/models.py contracts/raw_sensor_event.schema.json tests/test_generate_events.py tests/test_schema_validation.py
rtk git commit -m "Generate industrial replay telemetry"
```

---

## Task 5: Add Backfill Replay And Realtime Live Producer Modes

**Files:**

- Modify: `src/streaming/publish_raw_events.py`
- Test: `tests/test_publish_raw_events.py`

- [ ] **Step 1: Add failing producer-mode tests**

Append to `tests/test_publish_raw_events.py`:

```python
from src.streaming.publish_raw_events import ProducerMode, apply_producer_mode


def test_backfill_replay_preserves_event_time_and_updates_ingest_time() -> None:
    record = {
        "event_id": "evt_1",
        "schema_version": "raw_sensor_event.v1",
        "tenant_id": "tenant_northwind",
        "tag_id": "tag_1",
        "tag_name": "pump_vibration",
        "event_time": "2026-01-01T00:00:00Z",
        "ingest_time": "2026-01-01T00:00:00Z",
        "value": 1.0,
        "unit": "mm_s",
        "source_system": "historian",
        "quality_flags": [],
    }

    updated = apply_producer_mode(record, ProducerMode.BACKFILL_REPLAY, now_iso="2026-02-01T00:00:00Z")

    assert updated["event_time"] == "2026-01-01T00:00:00Z"
    assert updated["ingest_time"] == "2026-02-01T00:00:00Z"
    assert updated["synthetic_metadata"]["producer_mode"] == "backfill_replay"


def test_realtime_live_sets_current_event_and_ingest_time() -> None:
    record = {
        "event_id": "evt_1",
        "schema_version": "raw_sensor_event.v1",
        "tenant_id": "tenant_northwind",
        "tag_id": "tag_1",
        "tag_name": "pump_vibration",
        "event_time": "2026-01-01T00:00:00Z",
        "ingest_time": "2026-01-01T00:00:00Z",
        "value": 1.0,
        "unit": "mm_s",
        "source_system": "historian",
        "quality_flags": [],
    }

    updated = apply_producer_mode(record, ProducerMode.REALTIME_LIVE, now_iso="2026-02-01T00:00:00Z")

    assert updated["event_time"] == "2026-02-01T00:00:00Z"
    assert updated["ingest_time"] == "2026-02-01T00:00:00Z"
    assert updated["synthetic_metadata"]["producer_mode"] == "realtime_live"
```

- [ ] **Step 2: Run test to verify failure**

Run:

```bash
rtk ./.venv/bin/pytest tests/test_publish_raw_events.py::test_backfill_replay_preserves_event_time_and_updates_ingest_time -q
```

Expected: FAIL because `ProducerMode` does not exist.

- [ ] **Step 3: Implement producer modes**

In `src/streaming/publish_raw_events.py`, add:

```python
from enum import StrEnum
from datetime import UTC, datetime


class ProducerMode(StrEnum):
    BACKFILL_REPLAY = "backfill_replay"
    REALTIME_LIVE = "realtime_live"


def _now_iso() -> str:
    return datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")


def apply_producer_mode(
    record: dict[str, object],
    mode: ProducerMode,
    *,
    now_iso: str | None = None,
) -> dict[str, object]:
    timestamp = now_iso or _now_iso()
    updated = dict(record)
    metadata = dict(updated.get("synthetic_metadata", {}))
    metadata["producer_mode"] = mode.value
    if mode == ProducerMode.BACKFILL_REPLAY:
        updated["ingest_time"] = timestamp
    else:
        updated["event_time"] = timestamp
        updated["ingest_time"] = timestamp
    updated["synthetic_metadata"] = metadata
    return updated
```

- [ ] **Step 4: Add CLI flags**

In `PublishNamespace`, add:

```python
mode: str = "backfill_replay"
sleep_seconds: float = 0.0
```

In `build_parser()`:

```python
_ = parser.add_argument("--mode", choices=[mode.value for mode in ProducerMode], default=ProducerMode.BACKFILL_REPLAY.value)
_ = parser.add_argument("--sleep-seconds", type=float, default=0.0)
```

In `main()`, before planning routes:

```python
mode = ProducerMode(args.mode)
records = [apply_producer_mode(record, mode) for record in records]
```

If `sleep_seconds` is provided and not dry-run, add a sleep between route publications inside `publish_routes`.

- [ ] **Step 5: Run tests**

Run:

```bash
rtk ./.venv/bin/pytest tests/test_publish_raw_events.py -q
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
rtk git add src/streaming/publish_raw_events.py tests/test_publish_raw_events.py
rtk git commit -m "Add replay and live producer modes"
```

---

## Task 6: Compute 15-Minute Window Aggregates

**Files:**

- Create: `src/telemetry_generator/window_aggregates.py`
- Modify: `src/telemetry_generator/models.py`
- Test: `tests/test_window_aggregates.py`

- [ ] **Step 1: Write failing aggregate tests**

Create `tests/test_window_aggregates.py`:

```python
from src.telemetry_generator.window_aggregates import aggregate_15m


def test_aggregate_15m_calculates_summary_fields() -> None:
    records = [
        {
            "tenant_id": "tenant_northwind",
            "tag_id": "tag_1",
            "event_id": f"evt_{i}",
            "event_time": f"2026-01-01T00:{i:02d}:00Z",
            "value": float(i),
            "quality_flags": [],
            "synthetic_metadata": {
                "asset_id": "asset_1",
                "component_id": "comp_1",
                "measurement_point_id": "mp_1",
                "operating_state": "normal",
            },
        }
        for i in range(15)
    ]

    windows = aggregate_15m(records)

    assert len(windows) == 1
    window = windows[0]
    assert window["window_start"] == "2026-01-01T00:00:00Z"
    assert window["window_end"] == "2026-01-01T00:15:00Z"
    assert window["avg_value"] == 7.0
    assert window["min_value"] == 0.0
    assert window["max_value"] == 14.0
    assert window["event_count"] == 15
    assert window["expected_count"] == 15
```

- [ ] **Step 2: Run test to verify failure**

Run:

```bash
rtk ./.venv/bin/pytest tests/test_window_aggregates.py -q
```

Expected: FAIL because `window_aggregates.py` does not exist.

- [ ] **Step 3: Add aggregate type**

In `src/telemetry_generator/models.py`, add:

```python
class TelemetryWindow15m(TypedDict):
    tenant_id: str
    asset_id: str
    component_id: str
    measurement_point_id: str
    tag_id: str
    window_start: str
    window_end: str
    avg_value: float
    min_value: float
    max_value: float
    stddev_value: float
    p95_value: float
    slope: float
    z_score: float
    event_count: int
    expected_count: int
    quality_flags: list[str]
    operating_state: str
    degradation_stage: str
```

- [ ] **Step 4: Implement aggregate function**

Create `src/telemetry_generator/window_aggregates.py`:

```python
from __future__ import annotations

import math
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from statistics import mean, pstdev
from typing import Any


def _parse_utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def _format_utc(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _window_start(value: datetime) -> datetime:
    minute = (value.minute // 15) * 15
    return value.replace(minute=minute, second=0, microsecond=0)


def _p95(values: list[float]) -> float:
    ordered = sorted(values)
    index = min(len(ordered) - 1, math.ceil(len(ordered) * 0.95) - 1)
    return ordered[index]


def aggregate_15m(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, datetime], list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        event_time = _parse_utc(str(record["event_time"]))
        grouped[(str(record["tenant_id"]), str(record["tag_id"]), _window_start(event_time))].append(record)

    windows: list[dict[str, Any]] = []
    for (_tenant_id, _tag_id, start), group in sorted(grouped.items(), key=lambda item: item[0]):
        values = [float(record["value"]) for record in group]
        first = group[0]
        metadata = first.get("synthetic_metadata", {})
        if not isinstance(metadata, dict):
            metadata = {}
        quality_flags = sorted(
            {
                flag
                for record in group
                for flag in record.get("quality_flags", [])
                if isinstance(flag, str)
            }
        )
        slope = round((values[-1] - values[0]) / max(len(values) - 1, 1), 6)
        stddev_value = pstdev(values) if len(values) > 1 else 0.0
        windows.append(
            {
                "tenant_id": str(first["tenant_id"]),
                "asset_id": str(metadata.get("asset_id", "")),
                "component_id": str(metadata.get("component_id", "")),
                "measurement_point_id": str(metadata.get("measurement_point_id", "")),
                "tag_id": str(first["tag_id"]),
                "window_start": _format_utc(start),
                "window_end": _format_utc(start + timedelta(minutes=15)),
                "avg_value": round(mean(values), 3),
                "min_value": round(min(values), 3),
                "max_value": round(max(values), 3),
                "stddev_value": round(stddev_value, 6),
                "p95_value": round(_p95(values), 3),
                "slope": slope,
                "z_score": 0.0 if stddev_value == 0 else round((values[-1] - mean(values)) / stddev_value, 6),
                "event_count": len(group),
                "expected_count": 15,
                "quality_flags": quality_flags,
                "operating_state": str(metadata.get("operating_state", "unknown")),
                "degradation_stage": str(metadata.get("degradation_stage", "none")),
            }
        )
    return windows
```

- [ ] **Step 5: Run tests**

Run:

```bash
rtk ./.venv/bin/pytest tests/test_window_aggregates.py -q
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
rtk git add src/telemetry_generator/models.py src/telemetry_generator/window_aggregates.py tests/test_window_aggregates.py
rtk git commit -m "Add telemetry window aggregates"
```

---

## Task 7: Generate Supporting Business Data

**Files:**

- Create: `src/telemetry_generator/business_data.py`
- Modify: `src/telemetry_generator/models.py`
- Test: `tests/test_business_data_generation.py`

- [ ] **Step 1: Write failing business-data tests**

Create `tests/test_business_data_generation.py`:

```python
from src.telemetry_generator.business_data import generate_business_data


def test_business_data_has_referential_integrity() -> None:
    data = generate_business_data(seed=42)

    asset_ids = {record["asset_id"] for record in data["assets"]}
    component_asset_ids = {record["asset_id"] for record in data["components"]}
    tag_component_ids = {record["component_id"] for record in data["sensor_tags"]}
    component_ids = {record["component_id"] for record in data["components"]}

    assert asset_ids
    assert component_asset_ids.issubset(asset_ids)
    assert tag_component_ids.issubset(component_ids)


def test_business_data_includes_rul_and_work_orders() -> None:
    data = generate_business_data(seed=42)

    assert data["work_orders"]
    assert data["maintenance_events"]
    assert data["rul_predictions"]
    assert all(record["rul_hours"] >= 0 for record in data["rul_predictions"])
```

- [ ] **Step 2: Run test to verify failure**

Run:

```bash
rtk ./.venv/bin/pytest tests/test_business_data_generation.py -q
```

Expected: FAIL because `business_data.py` does not exist.

- [ ] **Step 3: Implement supporting data generator**

Create `src/telemetry_generator/business_data.py`:

```python
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from src.telemetry_generator.config import build_tag_metadata


def _format_utc(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def generate_business_data(seed: int = 42) -> dict[str, list[dict[str, Any]]]:
    metadata = build_tag_metadata()
    now = datetime(2026, 1, 30, tzinfo=UTC)
    assets_by_id: dict[str, dict[str, Any]] = {}
    components_by_id: dict[str, dict[str, Any]] = {}
    measurement_points_by_id: dict[str, dict[str, Any]] = {}
    sensor_tags: list[dict[str, Any]] = []

    for tag in metadata.values():
        assets_by_id.setdefault(
            tag["asset_id"],
            {
                "asset_id": tag["asset_id"],
                "asset_name": tag["asset_name"],
                "asset_type": tag["asset_type"],
                "tenant_id": tag["tenant_id"],
                "plant_id": tag["plant_id"],
                "criticality": "critical" if tag["asset_type"] in {"reactor", "compressor"} else "high",
                "lifecycle_state": "operating",
            },
        )
        components_by_id.setdefault(
            tag["component_id"],
            {
                "component_id": tag["component_id"],
                "asset_id": tag["asset_id"],
                "component_type": tag["component_type"],
                "criticality": "critical" if tag["component_type"] in {"seal", "compressor_stage"} else "high",
            },
        )
        measurement_points_by_id.setdefault(
            tag["measurement_point_id"],
            {
                "measurement_point_id": tag["measurement_point_id"],
                "component_id": tag["component_id"],
                "measurement_point": tag["measurement_point"],
                "metric_name": tag["metric_name"],
                "unit": tag["unit"],
            },
        )
        sensor_tags.append(dict(tag))

    selected_components = list(components_by_id.values())[:8]
    work_orders = []
    maintenance_events = []
    rul_predictions = []
    triage_evidence = []
    for index, component in enumerate(selected_components, start=1):
        work_order_id = f"wo_demo_{index:04d}"
        failure_mode_id = "fm_pump_cavitation" if index % 2 else "fm_bearing_inner_race_wear"
        created_at = now - timedelta(days=3, hours=index)
        completed_at = created_at + timedelta(hours=8)
        work_orders.append(
            {
                "work_order_id": work_order_id,
                "asset_id": component["asset_id"],
                "component_id": component["component_id"],
                "failure_mode_id": failure_mode_id,
                "priority": "P1" if index % 3 == 0 else "P2",
                "work_type": "corrective" if index % 3 == 0 else "inspection",
                "problem_code": "abnormal_vibration",
                "cause_code": "suspected_degradation",
                "remedy_code": "inspect_and_service",
                "status": "completed",
                "created_at": _format_utc(created_at),
                "scheduled_start_at": _format_utc(created_at + timedelta(hours=2)),
                "completed_at": _format_utc(completed_at),
                "downtime_minutes": 120 if index % 3 == 0 else 0,
                "technician_notes": "Synthetic CMMS record for APM demo.",
            }
        )
        maintenance_events.append(
            {
                "maintenance_event_id": f"maint_demo_{index:04d}",
                "work_order_id": work_order_id,
                "asset_id": component["asset_id"],
                "component_id": component["component_id"],
                "event_type": "inspection_completed",
                "event_time": _format_utc(completed_at),
            }
        )
        for day in range(30):
            rul_predictions.append(
                {
                    "prediction_id": f"rul_demo_{index:04d}_{day:02d}",
                    "asset_id": component["asset_id"],
                    "component_id": component["component_id"],
                    "failure_mode_id": failure_mode_id,
                    "prediction_time": _format_utc(now - timedelta(days=30 - day)),
                    "horizon_minutes": 60 * 24 * 30,
                    "rul_hours": max(0, 720 - day * 24 - index),
                    "rul_lower_hours": max(0, 680 - day * 24 - index),
                    "rul_upper_hours": max(0, 760 - day * 24 - index),
                    "confidence_score": 0.72,
                    "model_name": "synthetic_rul_curve",
                    "model_version": "v1",
                    "evidence_window_start": _format_utc(now - timedelta(days=30 - day, minutes=15)),
                    "evidence_window_end": _format_utc(now - timedelta(days=30 - day)),
                }
            )
        triage_evidence.append(
            {
                "evidence_id": f"evidence_demo_{index:04d}",
                "anomaly_id": f"anom_demo_{index:04d}",
                "asset_id": component["asset_id"],
                "component_id": component["component_id"],
                "failure_mode_id": failure_mode_id,
                "supporting_evidence": ["vibration trend increased", "RUL curve declined"],
                "root_cause_hypotheses": ["bearing wear", "process load increase"],
                "recommended_inspection": ["inspect bearing housing", "check lubrication history"],
                "confidence_score": 0.76,
                "quality_caveats": [],
            }
        )

    return {
        "assets": list(assets_by_id.values()),
        "components": list(components_by_id.values()),
        "measurement_points": list(measurement_points_by_id.values()),
        "sensor_tags": sensor_tags,
        "work_orders": work_orders,
        "maintenance_events": maintenance_events,
        "rul_predictions": rul_predictions,
        "triage_evidence": triage_evidence,
    }
```

- [ ] **Step 4: Run tests**

Run:

```bash
rtk ./.venv/bin/pytest tests/test_business_data_generation.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
rtk git add src/telemetry_generator/business_data.py tests/test_business_data_generation.py
rtk git commit -m "Generate APM business context data"
```

---

## Task 8: Add Doris Tables And Serving Views

**Files:**

- Modify: `sql/doris_schema.sql`
- Modify: `sql/doris_serving_views.sql`
- Test: `tests/test_doris_sql_contract.py`

- [ ] **Step 1: Add failing SQL contract tests**

Add to `tests/test_doris_sql_contract.py`:

```python
from pathlib import Path


def test_doris_schema_contains_measurement_points_windows_and_rul() -> None:
    sql = Path("sql/doris_schema.sql").read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS dim_measurement_point" in sql
    assert "CREATE TABLE IF NOT EXISTS agg_apm_sensor_windows_15m" in sql
    assert "CREATE TABLE IF NOT EXISTS fact_apm_rul_predictions" in sql


def test_doris_serving_views_support_grafana_apm_dashboards() -> None:
    sql = Path("sql/doris_serving_views.sql").read_text(encoding="utf-8")

    assert "CREATE VIEW IF NOT EXISTS vw_apm_fleet_health" in sql
    assert "CREATE VIEW IF NOT EXISTS vw_apm_asset_health_latest" in sql
    assert "CREATE VIEW IF NOT EXISTS vw_apm_anomaly_rul_detail" in sql
```

- [ ] **Step 2: Run test to verify failure**

Run:

```bash
rtk ./.venv/bin/pytest tests/test_doris_sql_contract.py -q
```

Expected: FAIL until SQL objects exist.

- [ ] **Step 3: Add Doris tables**

Append to `sql/doris_schema.sql`:

```sql
CREATE TABLE IF NOT EXISTS dim_measurement_point (
  measurement_point_id VARCHAR(128) NOT NULL,
  tenant_id VARCHAR(128) NOT NULL,
  asset_id VARCHAR(128),
  component_id VARCHAR(128),
  component_type VARCHAR(128),
  measurement_point VARCHAR(256),
  metric_name VARCHAR(128),
  unit VARCHAR(32),
  sampling_rate_hz DOUBLE,
  normal_range_min DOUBLE,
  normal_range_max DOUBLE,
  engineering_limit_min DOUBLE,
  engineering_limit_max DOUBLE,
  valid_from DATETIME,
  valid_to DATETIME,
  is_current BOOLEAN,
  load_time DATETIME DEFAULT CURRENT_TIMESTAMP
)
DUPLICATE KEY(measurement_point_id, tenant_id)
DISTRIBUTED BY HASH(tenant_id, measurement_point_id) BUCKETS 8
PROPERTIES ("replication_num" = "1");

CREATE TABLE IF NOT EXISTS agg_apm_sensor_windows_15m (
  tenant_id VARCHAR(128) NOT NULL,
  tag_id VARCHAR(256) NOT NULL,
  window_start DATETIME NOT NULL,
  window_end DATETIME NOT NULL,
  asset_id VARCHAR(128),
  component_id VARCHAR(128),
  measurement_point_id VARCHAR(128),
  metric_name VARCHAR(128),
  avg_value DOUBLE,
  min_value DOUBLE,
  max_value DOUBLE,
  stddev_value DOUBLE,
  p95_value DOUBLE,
  slope DOUBLE,
  z_score DOUBLE,
  event_count INT,
  expected_count INT,
  quality_flags VARCHAR(1024),
  operating_state VARCHAR(64),
  degradation_stage VARCHAR(64),
  load_time DATETIME DEFAULT CURRENT_TIMESTAMP
)
DUPLICATE KEY(tenant_id, tag_id, window_start)
DISTRIBUTED BY HASH(tenant_id, tag_id) BUCKETS 8
PROPERTIES ("replication_num" = "1");

CREATE TABLE IF NOT EXISTS fact_apm_rul_predictions (
  prediction_id VARCHAR(256) NOT NULL,
  tenant_id VARCHAR(128) NOT NULL,
  asset_id VARCHAR(128),
  component_id VARCHAR(128),
  failure_mode_id VARCHAR(256),
  prediction_time DATETIME NOT NULL,
  horizon_minutes INT,
  rul_hours DOUBLE,
  rul_lower_hours DOUBLE,
  rul_upper_hours DOUBLE,
  confidence_score DOUBLE,
  model_name VARCHAR(128),
  model_version VARCHAR(64),
  evidence_window_start DATETIME,
  evidence_window_end DATETIME,
  load_time DATETIME DEFAULT CURRENT_TIMESTAMP
)
DUPLICATE KEY(prediction_id, tenant_id, prediction_time)
DISTRIBUTED BY HASH(tenant_id, asset_id) BUCKETS 8
PROPERTIES ("replication_num" = "1");
```

- [ ] **Step 4: Add serving views**

Append to `sql/doris_serving_views.sql`:

```sql
CREATE VIEW IF NOT EXISTS vw_apm_fleet_health AS
SELECT
  tenant_id,
  COUNT(DISTINCT asset_id) AS asset_count,
  SUM(CASE WHEN severity = 'critical' THEN 1 ELSE 0 END) AS critical_anomaly_count,
  SUM(CASE WHEN severity = 'warning' THEN 1 ELSE 0 END) AS warning_anomaly_count,
  100 - LEAST(100, SUM(CASE WHEN severity = 'critical' THEN 10 WHEN severity = 'warning' THEN 4 ELSE 1 END)) AS fleet_health_score
FROM fact_apm_anomaly_events
GROUP BY tenant_id;

CREATE VIEW IF NOT EXISTS vw_apm_asset_health_latest AS
SELECT
  tenant_id,
  asset_id,
  MAX(window_end) AS last_seen_at,
  AVG(CASE WHEN z_score > 3 THEN 40 WHEN z_score > 2 THEN 70 ELSE 92 END) AS health_score,
  MAX(operating_state) AS operating_state
FROM agg_apm_sensor_windows_15m
GROUP BY tenant_id, asset_id;

CREATE VIEW IF NOT EXISTS vw_apm_anomaly_rul_detail AS
SELECT
  a.tenant_id,
  a.anomaly_id,
  a.asset_id,
  a.component_id,
  a.tag_id,
  a.metric_name,
  a.window_start,
  a.window_end,
  a.severity,
  r.rul_hours,
  r.rul_lower_hours,
  r.rul_upper_hours,
  r.confidence_score
FROM fact_apm_anomaly_events a
LEFT JOIN fact_apm_rul_predictions r
  ON a.tenant_id = r.tenant_id
 AND a.asset_id = r.asset_id
 AND a.component_id = r.component_id;
```

- [ ] **Step 5: Run SQL contract tests**

Run:

```bash
rtk ./.venv/bin/pytest tests/test_doris_sql_contract.py -q
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
rtk git add sql/doris_schema.sql sql/doris_serving_views.sql tests/test_doris_sql_contract.py
rtk git commit -m "Add Doris APM serving objects"
```

---

## Task 9: Redesign Grafana A/B/C Dashboards

**Files:**

- Modify: `configs/grafana/dashboards/apm-overview.json`
- Modify: `configs/grafana/dashboards/apm-asset-health.json`
- Modify: `configs/grafana/dashboards/apm-anomaly-deep-dive.json`
- Test: `tests/test_grafana_smoke.py`

- [ ] **Step 1: Add failing Grafana smoke expectations**

Update `tests/test_grafana_smoke.py` with assertions:

```python
import json
from pathlib import Path


def _dashboard(path: str) -> dict[str, object]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _panel_titles(dashboard: dict[str, object]) -> set[str]:
    return {
        str(panel.get("title"))
        for panel in dashboard.get("panels", [])
        if isinstance(panel, dict)
    }


def test_apm_landing_dashboard_has_product_panels() -> None:
    dashboard = _dashboard("configs/grafana/dashboards/apm-overview.json")
    titles = _panel_titles(dashboard)

    assert dashboard["title"] == "APM Landing Dashboard"
    assert {
        "Fleet Health Score",
        "Critical Assets",
        "RUL Risk Assets",
        "Energy Penalty",
        "Top Risk Assets",
        "Recent Critical Events",
    }.issubset(titles)


def test_asset_workbench_dashboard_has_operator_panels() -> None:
    dashboard = _dashboard("configs/grafana/dashboards/apm-asset-health.json")
    titles = _panel_titles(dashboard)

    assert dashboard["title"] == "Asset Operations Workbench"
    assert {
        "Selected Asset Summary",
        "Component Health Matrix",
        "Operating State Timeline",
        "Sensor Trend by Component",
        "Related Work Orders",
    }.issubset(titles)


def test_anomaly_rul_dashboard_has_triage_panels() -> None:
    dashboard = _dashboard("configs/grafana/dashboards/apm-anomaly-deep-dive.json")
    titles = _panel_titles(dashboard)

    assert dashboard["title"] == "Anomaly / RUL Detail Dashboard"
    assert {
        "30-Day Degradation Curve",
        "RUL Prediction Curve",
        "Failure Mode Evidence",
        "LLM-Assisted Triage Summary",
        "Quality Caveats",
    }.issubset(titles)
```

- [ ] **Step 2: Run test to verify failure**

Run:

```bash
rtk ./.venv/bin/pytest tests/test_grafana_smoke.py -q
```

Expected: FAIL until dashboards are renamed and panels exist.

- [ ] **Step 3: Update dashboard titles and panels**

Edit the three dashboard JSON files. Preserve datasource references and existing schema metadata. Update panel titles and SQL targets to reference:

- `vw_apm_fleet_health`
- `vw_apm_asset_health_latest`
- `agg_apm_sensor_windows_15m`
- `fact_apm_rul_predictions`
- `vw_apm_anomaly_rul_detail`
- `serving_apm_triage_evidence`
- `fact_apm_agent_recommendations`

Minimum panel type choices:

- Landing: stat, timeseries, table, heatmap.
- Workbench: stat, state-timeline, timeseries, table.
- Detail: timeseries, table, text/stat panels.

- [ ] **Step 4: Validate dashboard JSON**

Run:

```bash
rtk jq empty configs/grafana/dashboards/apm-overview.json
rtk jq empty configs/grafana/dashboards/apm-asset-health.json
rtk jq empty configs/grafana/dashboards/apm-anomaly-deep-dive.json
```

Expected: no output and exit code 0.

- [ ] **Step 5: Run Grafana smoke tests**

Run:

```bash
rtk ./.venv/bin/pytest tests/test_grafana_smoke.py -q
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
rtk git add configs/grafana/dashboards/apm-overview.json configs/grafana/dashboards/apm-asset-health.json configs/grafana/dashboards/apm-anomaly-deep-dive.json tests/test_grafana_smoke.py
rtk git commit -m "Redesign Grafana APM dashboards"
```

---

## Task 10: Add End-To-End Demo Command And Documentation

**Files:**

- Modify: `Makefile`
- Modify: `README.md`
- Modify: `docs/project_implementation_backlog.md`
- Test: `tests/test_phase4_e2e_contract.py` or new focused test if phase numbering changes.

- [ ] **Step 1: Add failing command/documentation test**

Add to `tests/test_phase4_e2e_contract.py`:

```python
from pathlib import Path


def test_makefile_documents_industrial_apm_demo_flow() -> None:
    makefile = Path("Makefile").read_text(encoding="utf-8")
    readme = Path("README.md").read_text(encoding="utf-8")

    assert "industrial-apm-demo" in makefile
    assert "backfill_replay" in readme
    assert "realtime_live" in readme
    assert "APM Landing Dashboard" in readme
```

- [ ] **Step 2: Run test to verify failure**

Run:

```bash
rtk ./.venv/bin/pytest tests/test_phase4_e2e_contract.py::test_makefile_documents_industrial_apm_demo_flow -q
```

Expected: FAIL until command and docs are added.

- [ ] **Step 3: Add Makefile target**

Add:

```make
.PHONY: industrial-apm-demo
industrial-apm-demo:
	./.venv/bin/python -m src.telemetry_generator.generate_events --profile industrial_demo --output-dir data
	./.venv/bin/python -m src.streaming.publish_raw_events --profile industrial_demo --mode backfill_replay --dry-run
	@echo "Backfill dry-run complete. Start realtime_live without --dry-run when Kafka/Redpanda is running."
```

- [ ] **Step 4: Document demo story in README**

Add a concise section:

```markdown
## Industrial APM Demo Flow

The demo uses two producer phases:

1. `backfill_replay` publishes an accelerated 30-day historical timeline through Kafka so Doris and Grafana have degradation, RUL, anomaly, and maintenance context.
2. `realtime_live` continues the same scenario at 1 Hz per sensor tag so the live Kafka -> Flink -> Doris -> Grafana pipeline can be observed.

Grafana uses three dashboards:

- `APM Landing Dashboard`
- `Asset Operations Workbench`
- `Anomaly / RUL Detail Dashboard`
```

- [ ] **Step 5: Run docs test**

Run:

```bash
rtk ./.venv/bin/pytest tests/test_phase4_e2e_contract.py::test_makefile_documents_industrial_apm_demo_flow -q
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
rtk git add Makefile README.md docs/project_implementation_backlog.md tests/test_phase4_e2e_contract.py
rtk git commit -m "Document industrial APM demo flow"
```

---

## Final Verification

- [ ] **Run focused telemetry generator suite**

```bash
rtk ./.venv/bin/pytest tests/test_measurement_points.py tests/test_signal_profiles.py tests/test_generate_events.py tests/test_window_aggregates.py tests/test_business_data_generation.py -q
```

Expected: PASS.

- [ ] **Run streaming and contract suite**

```bash
rtk ./.venv/bin/pytest tests/test_publish_raw_events.py tests/test_schema_validation.py tests/test_doris_sql_contract.py -q
```

Expected: PASS.

- [ ] **Run Grafana and E2E contract suite**

```bash
rtk ./.venv/bin/pytest tests/test_grafana_smoke.py tests/test_phase4_e2e_contract.py -q
```

Expected: PASS.

- [ ] **Run full test suite if local runtime allows**

```bash
rtk ./.venv/bin/pytest -q
```

Expected: PASS, or document any existing unrelated failures with exact test names.

---

## Spec Coverage Check

- Measurement point model: Tasks 1 and 2.
- 1 Hz synthetic generator: Tasks 3, 4, and 5.
- 30-day backfill then realtime live: Tasks 4, 5, and 10.
- 15-minute aggregate: Task 6.
- Supporting business data: Task 7.
- Doris serving views: Task 8.
- Grafana A/B/C dashboards: Task 9.
- Tests and demo docs: Tasks 1 through 10 plus Final Verification.
