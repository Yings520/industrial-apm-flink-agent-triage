from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import cast

SORT_KEYS = ("anomaly_id", "rule_id", "tenant_id", "asset_id", "tag_id", "metric_name", "window_start", "window_end")


class ReplayNamespace(argparse.Namespace):
    actual: Path = Path()
    expected: Path = Path()


def read_jsonl(path: Path) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = cast(object, json.loads(line))
        if not isinstance(payload, dict):
            raise ValueError(f"Replay record must be an object: {path}")
        records.append(cast(dict[str, object], payload))
    return records


def normalize(records: list[dict[str, object]]) -> list[dict[str, object]]:
    return sorted(records, key=lambda record: tuple(str(record.get(key, "")) for key in SORT_KEYS))


def compare_jsonl(actual_path: Path, expected_path: Path) -> bool:
    return normalize(read_jsonl(actual_path)) == normalize(read_jsonl(expected_path))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Verify deterministic anomaly replay output.")
    _ = parser.add_argument("--actual", type=Path, required=True)
    _ = parser.add_argument("--expected", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    namespace = ReplayNamespace()
    args = build_parser().parse_args(argv, namespace=namespace)
    if compare_jsonl(args.actual, args.expected):
        print("replay verification passed")
        return 0
    print("replay verification failed")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

