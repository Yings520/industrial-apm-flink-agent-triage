#!/usr/bin/env python3
"""Real-time data pump (Docker internal): generate and publish signal data via kafka-python.

Usage (inside Docker): python scripts/e2e_data_pump.py --broker redpanda:19092 --interval 1.0
"""

from __future__ import annotations

import argparse
import hashlib
import json
import random
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from kafka import KafkaProducer  # type: ignore[import-untyped]

from src.streaming.topic_names import render_topic
from src.telemetry_generator.config import build_tag_metadata
from src.telemetry_generator.models import TagMetadata
from src.telemetry_generator.signal_profiles import OperatingState, SignalContext, signal_value


def _stable_event_id(parts: list[str]) -> str:
    digest = hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:20]
    return f"evt_{digest}"


def _format_utc(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _build_event(tag: TagMetadata, timestamp: datetime, seq: int) -> dict[str, Any]:
    context = SignalContext(
        metric_name=tag["metric_name"],
        normal_min=tag["normal_range_min"],
        normal_max=tag["normal_range_max"],
        engineering_min=tag["engineering_limit_min"],
        engineering_max=tag["engineering_limit_max"],
        failure_mode_id=None,
        operating_state=OperatingState.NORMAL,
        scenario_start=datetime(2026, 1, 1, tzinfo=UTC),
        scenario_end=datetime(2099, 12, 31, tzinfo=UTC),
        seed=f"{tag['tenant_id']}|{tag['tag_id']}",
    )
    value = round(signal_value(context, timestamp), 6)
    seed_key = f"{tag['tenant_id']}|{tag['tag_id']}|{seq}|anomaly"
    rng = random.Random(int(hashlib.sha1(seed_key.encode()).hexdigest()[:12], 16))
    if rng.random() < 0.20:
        value = round(value * rng.uniform(2.5, 4.0), 6)
        scenario, quality_flags = "spike", ["threshold_spike"]
    else:
        scenario, quality_flags = "normal", []

    event_id = _stable_event_id(["realtime_pump", tag["tenant_id"], tag["tag_id"], str(seq)])
    return {
        "event_id": event_id,
        "schema_version": "raw_sensor_event.v1",
        "tenant_id": tag["tenant_id"],
        "tag_id": tag["tag_id"],
        "tag_name": tag["tag_name"],
        "event_time": _format_utc(timestamp),
        "ingest_time": _format_utc(timestamp),
        "value": value,
        "unit": tag["unit"],
        "source_system": "opc_ua",
        "quality_flags": quality_flags,
        "scenario": scenario,
        "synthetic_metadata": {"asset_id": tag["asset_id"], "metric_name": tag["metric_name"]},
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="E2E realtime data pump (Docker mode)")
    _ = parser.add_argument("--broker", default="redpanda:19092")
    _ = parser.add_argument("--interval", type=float, default=1.0)
    args = parser.parse_args()

    tags = list(build_tag_metadata().values())
    if not tags:
        print("ERROR: zero tags", file=sys.stderr)
        sys.exit(1)

    print(f"[PUMP] {len(tags)} tags, broker={args.broker}, interval={args.interval}s")
    for t in sorted(set(t["tenant_id"] for t in tags)):
        print(f"  {t}: {sum(1 for x in tags if x['tenant_id'] == t)} tags")

    producer = KafkaProducer(
        bootstrap_servers=args.broker,
        batch_size=65536,
        linger_ms=5,
        compression_type="gzip",
        value_serializer=lambda v: json.dumps(v, sort_keys=True, separators=(",", ":")).encode("utf-8"),
    )

    seq = 0
    total_sent = 0
    start = time.time()
    try:
        while True:
            seq += 1
            ts = datetime.now(tz=UTC)
            for tag in tags:
                event = _build_event(tag, ts, seq)
                topic = render_topic("raw_sensor_events", tag["tenant_id"])
                producer.send(topic, value=event)
            producer.flush(timeout=10)
            total_sent += len(tags)
            elapsed = time.time() - start
            print(f"[PUMP] seq={seq} sent={total_sent} elapsed={elapsed:.1f}s rate={total_sent / elapsed:.0f}/s")
            time.sleep(max(0, start + seq * args.interval - time.time()))
    except KeyboardInterrupt:
        print(f"\n[DONE] total_sent={total_sent}")
    finally:
        producer.close(timeout=10)


if __name__ == "__main__":
    main()
