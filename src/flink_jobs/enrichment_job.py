from __future__ import annotations

import argparse

from src.flink_jobs.metadata import enrich_raw_event
from src.streaming.topic_names import render_topic


class EnrichmentNamespace(argparse.Namespace):
    broker: str = "localhost:19092"
    tenant: str = "tenant_northwind"
    dry_run: bool = False


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run PyFlink raw-to-staging enrichment.")
    _ = parser.add_argument("--broker", default="localhost:19092")
    _ = parser.add_argument("--tenant", default="tenant_northwind")
    _ = parser.add_argument("--dry-run", action="store_true")
    return parser


def describe_pipeline(tenant_id: str) -> dict[str, str]:
    return {
        "source": render_topic("raw_sensor_events", tenant_id),
        "sink": render_topic("staging_telemetry_enriched", tenant_id),
        "dlq": render_topic("staging_dlq", tenant_id),
        "late": render_topic("mart_late_events", tenant_id),
        "health": render_topic("mart_stream_health", tenant_id),
        "event_time": "event_time",
        "watermark": "WatermarkStrategy.for_bounded_out_of_orderness",
        "key_by": "tenant_id,asset_id,tag_id",
    }


def run_pyflink_pipeline(tenant_id: str, broker: str) -> None:
    try:
        from pyflink.datastream import StreamExecutionEnvironment  # type: ignore[import-not-found]
        from pyflink.common.watermark_strategy import WatermarkStrategy  # type: ignore[import-not-found]
    except ImportError as exc:
        raise RuntimeError(
            "PyFlink is not installed in this Python environment. Use the Docker Flink runtime or install PyFlink "
            "compatible with the active Python version."
        ) from exc

    _ = StreamExecutionEnvironment.get_execution_environment()
    _ = WatermarkStrategy
    _ = broker
    _ = enrich_raw_event
    raise NotImplementedError("Submit the packaged enrichment job through the Docker Flink runtime for local execution.")


def main(argv: list[str] | None = None) -> int:
    namespace = EnrichmentNamespace()
    args = build_parser().parse_args(argv, namespace=namespace)
    if args.dry_run:
        for key, value in describe_pipeline(args.tenant).items():
            print(f"{key}: {value}")
        return 0
    run_pyflink_pipeline(args.tenant, args.broker)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

