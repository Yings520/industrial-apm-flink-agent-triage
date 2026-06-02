from __future__ import annotations

import argparse
import json
import subprocess
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import cast

from jsonschema import Draft202012Validator, FormatChecker

from src.streaming.topic_names import raw_message_key, render_topic, tenant_ids
from src.telemetry_generator.config import REPO_ROOT
from src.telemetry_generator.schema_validation import reject_entry

RAW_SCHEMA_PATH = REPO_ROOT / "contracts" / "raw_sensor_event.schema.json"


@dataclass(frozen=True)
class PublishRoute:
    topic: str | None
    key: str | None
    record: dict[str, object]
    valid: bool
    fallback_path: Path | None = None


class ProducerMode(StrEnum):
    BACKFILL_REPLAY = "backfill_replay"
    REALTIME_LIVE = "realtime_live"


class PublishNamespace(argparse.Namespace):
    input: Path | None = None
    broker: str = "localhost:19092"
    tenant: str | None = None
    profile: str = "demo"
    mode: str = "backfill_replay"
    dry_run: bool = False
    output_dir: Path = Path("data")


def _load_schema() -> dict[str, object]:
    payload = cast(object, json.loads(RAW_SCHEMA_PATH.read_text(encoding="utf-8")))
    if not isinstance(payload, dict):
        raise ValueError("raw sensor event schema must be a JSON object")
    return cast(dict[str, object], payload)


def _read_jsonl(path: Path) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = cast(object, json.loads(line))
        if not isinstance(payload, dict):
            raise ValueError(f"JSONL record must be an object: {path}")
        records.append(cast(dict[str, object], payload))
    return records


def _schema_version(schema: dict[str, object]) -> str:
    version = cast(dict[str, object], cast(dict[str, object], schema["properties"])["schema_version"]).get("const")
    if not isinstance(version, str):
        raise ValueError("raw schema_version const is required")
    return version


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
    metadata_raw = updated.get("synthetic_metadata", {})
    metadata = dict(metadata_raw) if isinstance(metadata_raw, dict) else {}
    metadata["producer_mode"] = mode.value
    if mode == ProducerMode.BACKFILL_REPLAY:
        updated["ingest_time"] = timestamp
    else:
        updated["event_time"] = timestamp
        updated["ingest_time"] = timestamp
    updated["synthetic_metadata"] = metadata
    return updated


def plan_routes(records: list[dict[str, object]], output_dir: Path | None = None) -> list[PublishRoute]:
    if output_dir is None:
        output_dir = Path("data")
    schema = _load_schema()
    schema_version = _schema_version(schema)
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    routes: list[PublishRoute] = []
    fallback_path = output_dir / "rejected" / "raw_publish_rejected.jsonl"
    known_tenants = set(tenant_ids())

    for record in records:
        errors = sorted(validator.iter_errors(cast(object, record)), key=lambda error: list(error.absolute_path))  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]
        tenant_id = record.get("tenant_id")
        if not errors:
            key = raw_message_key(record)
            routes.append(
                PublishRoute(
                    topic=render_topic("raw_sensor_events", cast(str, tenant_id)),
                    key=key,
                    record=record,
                    valid=True,
                )
            )
            continue

        message = errors[0].message
        field_path = "/" + "/".join(str(part) for part in errors[0].absolute_path)
        dlq_record = reject_entry(
            original_record=record,
            error_code="schema_validation_error",
            field_path=field_path if field_path != "/" else "/",
            message=message,
            schema_version=schema_version,
            schema_name="raw_sensor_event",
        )
        if isinstance(tenant_id, str) and tenant_id in known_tenants:
            routes.append(
                PublishRoute(
                    topic=render_topic("raw_dlq", tenant_id),
                    key=f"{tenant_id}|dlq",
                    record=dlq_record,
                    valid=False,
                )
            )
        else:
            routes.append(
                PublishRoute(topic=None, key=None, record=dlq_record, valid=False, fallback_path=fallback_path)
            )
    return routes


def _append_fallback(path: Path, record: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        _ = handle.write(json.dumps(record, sort_keys=True, separators=(",", ":")))
        _ = handle.write("\n")


def _build_producer(broker: str):  # pyright: ignore[reportUnusedFunction]
    from kafka import KafkaProducer  # pyright: ignore[reportMissingTypeStubs]

    return KafkaProducer(
        bootstrap_servers=broker,
        batch_size=16384,
        linger_ms=10,
        compression_type="gzip",
        value_serializer=lambda v: json.dumps(v, sort_keys=True, separators=(",", ":")).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )


def publish_routes(routes: list[PublishRoute], _broker: str, dry_run: bool) -> None:
    if dry_run:
        for route in routes:
            if route.topic is None:
                print(f"fallback {route.fallback_path}")
            else:
                print(f"produce topic={route.topic} key={route.key} valid={route.valid}")
        return

    import sys
    from tempfile import NamedTemporaryFile

    topic_batches: dict[str, list[dict[str, object]]] = {}
    for route in routes:
        if route.topic is None:
            assert route.fallback_path is not None
            _append_fallback(route.fallback_path, route.record)
            continue
        topic_batches.setdefault(route.topic, []).append(route.record)

    total = sum(len(v) for v in topic_batches.values())
    sent = 0
    for topic, records in topic_batches.items():
        with NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as tmp:
            for rec in records:
                _ = tmp.write(json.dumps(rec, sort_keys=True, separators=(",", ":")).replace("\n", "") + "\n")
            tmp_path = tmp.name

        _ = subprocess.run(
            ["docker", "compose", "exec", "-T", "redpanda", "rpk", "topic", "produce", topic],
            input=Path(tmp_path).read_text(),
            text=True,
            check=True,
            capture_output=True,
        )
        Path(tmp_path).unlink()
        sent += len(records)
        print(f"  {topic}: {len(records)} events ({sent}/{total})", file=sys.stderr)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Publish raw sensor events to tenant raw topics.")
    _ = parser.add_argument("--input", type=Path)
    _ = parser.add_argument("--broker", default="localhost:19092")
    _ = parser.add_argument("--tenant")
    _ = parser.add_argument("--profile", default="demo", choices=["smoke", "demo", "industrial_demo"])
    _ = parser.add_argument(
        "--mode", choices=[mode.value for mode in ProducerMode], default=ProducerMode.BACKFILL_REPLAY.value
    )
    _ = parser.add_argument("--dry-run", action="store_true")
    _ = parser.add_argument("--output-dir", type=Path, default=Path("data"))
    return parser


def main(argv: list[str] | None = None) -> int:
    namespace = PublishNamespace()
    args = build_parser().parse_args(argv, namespace=namespace)
    input_path = args.input or args.output_dir / "generated" / f"raw_sensor_events_{args.profile}.jsonl"
    records = _read_jsonl(input_path)
    if args.tenant:
        records = [record for record in records if record.get("tenant_id") == args.tenant]
    records = [apply_producer_mode(record, ProducerMode(args.mode)) for record in records]
    routes = plan_routes(records, args.output_dir)
    publish_routes(routes, args.broker, args.dry_run)
    valid_count = sum(1 for route in routes if route.valid)
    print(f"planned {len(routes)} records: {valid_count} valid, {len(routes) - valid_count} dlq")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
