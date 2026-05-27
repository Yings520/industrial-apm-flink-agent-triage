from __future__ import annotations

import argparse

from src.streaming.topic_names import render_topic


class AnomalyNamespace(argparse.Namespace):
    broker: str = "localhost:19092"
    tenant: str = "tenant_northwind"
    dry_run: bool = False


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run PyFlink staging-to-mart anomaly detection.")
    _ = parser.add_argument("--broker", default="localhost:19092")
    _ = parser.add_argument("--tenant", default="tenant_northwind")
    _ = parser.add_argument("--dry-run", action="store_true")
    return parser


def describe_pipeline(tenant_id: str) -> dict[str, str]:
    return {
        "source": render_topic("staging_telemetry_enriched", tenant_id),
        "anomalies": render_topic("mart_anomalies", tenant_id),
        "late": render_topic("mart_late_events", tenant_id),
        "health": render_topic("mart_stream_health", tenant_id),
        "topic_templates": "{tenant_id}.mart_anomalies.v1,{tenant_id}.mart_late_events.v1,{tenant_id}.mart_stream_health.v1",
        "event_time": "event_time",
        "watermark": "WatermarkStrategy.for_bounded_out_of_orderness",
        "keyBy": "tenant_id,asset_id,tag_id",
        "anomaly_id": "stable hash of rule_id, tenant_id, asset_id, tag_id, metric_name, window_start, window_end",
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
    _ = tenant_id
    _ = broker
    raise NotImplementedError("Submit the packaged anomaly job through the Docker Flink runtime for local execution.")


def main(argv: list[str] | None = None) -> int:
    namespace = AnomalyNamespace()
    args = build_parser().parse_args(argv, namespace=namespace)
    if args.dry_run:
        for key, value in describe_pipeline(args.tenant).items():
            print(f"{key}: {value}")
        return 0
    run_pyflink_pipeline(args.tenant, args.broker)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
