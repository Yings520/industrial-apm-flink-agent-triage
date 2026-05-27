from __future__ import annotations

import argparse
import subprocess

from src.streaming.topic_names import all_topics


class TopicAdminNamespace(argparse.Namespace):
    broker: str = "localhost:19092"
    create: bool = False
    list: bool = False
    dry_run: bool = False


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Create or list local Redpanda topics.")
    _ = parser.add_argument("--broker", default="localhost:19092")
    _ = parser.add_argument("--create", action="store_true")
    _ = parser.add_argument("--list", action="store_true")
    _ = parser.add_argument("--dry-run", action="store_true")
    return parser


def _run_rpk(args: list[str]) -> None:
    _ = subprocess.run(args, check=True)


def create_topics(broker: str, dry_run: bool = False) -> list[str]:
    topics = all_topics()
    for topic in topics:
        command = [
            "docker",
            "compose",
            "exec",
            "-T",
            "redpanda",
            "rpk",
            "-X",
            f"brokers={broker}",
            "topic",
            "create",
            topic,
        ]
        if dry_run:
            print(" ".join(command))
        else:
            _run_rpk(command)
    return topics


def list_topics(broker: str, dry_run: bool = False) -> None:
    command = ["docker", "compose", "exec", "-T", "redpanda", "rpk", "-X", f"brokers={broker}", "topic", "list"]
    if dry_run:
        print(" ".join(command))
    else:
        _run_rpk(command)


def main(argv: list[str] | None = None) -> int:
    namespace = TopicAdminNamespace()
    args = build_parser().parse_args(argv, namespace=namespace)
    if not args.create and not args.list:
        raise SystemExit("Select --create or --list")
    if args.create:
        topics = create_topics(args.broker, args.dry_run)
        print(f"topics: {len(topics)}")
    if args.list:
        list_topics(args.broker, args.dry_run)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
