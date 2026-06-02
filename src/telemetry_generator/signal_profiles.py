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
    digest = hashlib.sha1(f"{seed}|{minute_bucket}".encode()).hexdigest()
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
