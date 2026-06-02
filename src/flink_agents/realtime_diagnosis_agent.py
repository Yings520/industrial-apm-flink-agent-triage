from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
import threading
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, as_completed, wait
from typing import Any

from src.flink_agents.anomaly_event_consumer import AnomalyEventConsumer
from src.flink_agents.context_lookup import ContextLookup
from src.flink_agents.diagnosis_generator import generate_diagnosis
from src.flink_agents.diagnosis_producer import produce_diagnosis
from src.flink_agents.evidence_assembler import assemble_evidence
from src.flink_agents.output_validator import validate_diagnosis

logger = logging.getLogger(__name__)

_shutdown_flag = False


def _load_dotenv(env_file: str) -> None:
    try:
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    key, _, value = line.partition("=")
                    key = key.strip()
                    value = value.strip().strip('"').strip("'")
                    if key and key not in os.environ:
                        os.environ[key] = value
    except FileNotFoundError:
        logger.debug("No .env file found at %s", env_file)


def _signal_handler(signum: int, frame: object) -> None:
    del signum, frame
    global _shutdown_flag
    _shutdown_flag = True
    print("\n[INFO] Received shutdown signal (SIGTERM/SIGINT). Gracefully exiting...")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m src.flink_agents.realtime_diagnosis_agent",
        description="Flink Agent realtime diagnosis daemon — event-driven service for LLM-assisted alert triage.",
    )
    _ = parser.add_argument(
        "--tenant",
        required=True,
        help="Tenant ID (e.g. tenant_northwind) or 'all' for multi-tenant mode",
    )
    _ = parser.add_argument(
        "--broker",
        default="localhost:19092",
        help="Kafka bootstrap broker address (default: localhost:19092)",
    )
    _ = parser.add_argument(
        "--max-events",
        type=int,
        default=None,
        help="Optional: test mode — process at most N anomaly events then exit",
    )
    _ = parser.add_argument(
        "--use-llm",
        action="store_true",
        default=False,
        help="Enable real LLM-based diagnosis (reads API key/model from .env)",
    )
    _ = parser.add_argument(
        "--env-file",
        default=".env",
        help="Path to .env file containing OPENAI_API_KEY etc. (default: .env)",
    )
    _ = parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of concurrent worker threads for event processing (default: 4)",
    )
    return parser.parse_args(argv)


def _print_config_summary(
    tenant: str,
    broker: str,
    max_events: int | None,
    use_llm: bool,
    workers: int,
) -> None:
    mode = f"test (max {max_events} events)" if max_events is not None else "daemon (continuous)"
    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    model = os.environ.get("OPENAI_MODEL", "").strip()
    llm_enabled = use_llm and bool(api_key) and bool(model)
    llm_info = f"enabled (model={model})" if llm_enabled else "disabled"

    print("=" * 60)
    print("Flink Agent — Realtime Diagnosis Service")
    print(f"  Tenant:      {tenant}")
    print(f"  Broker:      {broker}")
    print(f"  Workers:     {workers}")
    print(f"  Mode:        {mode}")
    print(f"  LLM:         {llm_info}")
    print("=" * 60)


def _process_event(
    anomaly: dict[str, Any],
    tenant_id: str,
    *,
    use_llm: bool = False,
    broker: str = "localhost:19092",
) -> tuple[str, str, int, str]:
    anomaly_id = anomaly.get("anomaly_id", "unknown")
    started = time.perf_counter()

    try:
        lookup = ContextLookup()
        context = lookup.lookup_all(tenant_id, anomaly)
        evidence = assemble_evidence(anomaly, context)
        diagnosis = generate_diagnosis(evidence, use_llm=use_llm)
        validated = validate_diagnosis(diagnosis, evidence)

        if validated["status"] == "valid":
            produce_diagnosis(validated["diagnosis"], tenant_id, broker=broker)
            latency_ms = int((time.perf_counter() - started) * 1000)
            diagnosis_id = validated["diagnosis"].get("diagnosis_id", "unknown")
            return ("pass", anomaly_id, latency_ms, diagnosis_id)
        else:
            reasons = "; ".join(validated.get("failure_reasons", []))
            return ("quarantine", anomaly_id, 0, reasons)
    except Exception as exc:
        logger.error(
            "Agent pipeline error for anomaly_id=%s: %s",
            anomaly_id,
            exc,
            exc_info=True,
        )
        return ("error", anomaly_id, 0, str(exc))


def _handle_result(
    result: tuple[str, str, int, str],
    stats: dict[str, int],
    lock: threading.Lock,
) -> None:
    status, anomaly_id, latency_ms, detail = result
    with lock:
        if status == "pass":
            stats["produced"] += 1
            print(f"[PASS] {anomaly_id} -> {detail} (latency={latency_ms}ms)")
        elif status == "quarantine":
            stats["quarantined"] += 1
            print(f"[QUARANTINE] {anomaly_id} | reasons: {detail}")
        elif status == "error":
            stats["errors"] += 1
            print(f"[ERROR] {anomaly_id} | {detail}")


def _collect_completed(
    pending: dict[Future[tuple[str, str, int, str]], str],
    stats: dict[str, int],
    lock: threading.Lock,
) -> None:
    done = [f for f in pending if f.done()]
    for f in done:
        anomaly_id = pending.pop(f)
        try:
            result = f.result()
        except Exception as exc:
            with lock:
                stats["errors"] += 1
            print(f"[ERROR] {anomaly_id} | {exc}")
        else:
            _handle_result(result, stats, lock)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stderr,
    )

    _load_dotenv(args.env_file)

    _print_config_summary(
        tenant=args.tenant,
        broker=args.broker,
        max_events=args.max_events,
        use_llm=args.use_llm,
        workers=args.workers,
    )

    _ = signal.signal(signal.SIGTERM, _signal_handler)
    _ = signal.signal(signal.SIGINT, _signal_handler)

    multi_tenant = args.tenant == "all"
    if multi_tenant:
        consumer = AnomalyEventConsumer.for_all_tenants(broker=args.broker)
    else:
        consumer = AnomalyEventConsumer(tenant_id=args.tenant, broker=args.broker)

    max_events = args.max_events
    workers = args.workers
    stats: dict[str, int] = {"processed": 0, "produced": 0, "quarantined": 0, "errors": 0}
    stats_lock = threading.Lock()
    pending: dict[Future[tuple[str, str, int, str]], str] = {}
    executor = ThreadPoolExecutor(max_workers=workers)

    try:
        while not _shutdown_flag:
            if max_events is not None and stats["processed"] >= max_events:
                print(f"\n[INFO] Test mode complete: processed {stats['processed']} events.")
                break

            if len(pending) >= workers * 2:
                done_set, _ = wait(pending.keys(), timeout=1.0, return_when=FIRST_COMPLETED)
                if done_set:
                    for f in done_set:
                        anomaly_id = pending.pop(f)
                        try:
                            result = f.result()
                        except Exception as exc:
                            with stats_lock:
                                stats["errors"] += 1
                            print(f"[ERROR] {anomaly_id} | {exc}")
                        else:
                            _handle_result(result, stats, stats_lock)
                continue

            anomaly = consumer.poll(timeout_ms=500)
            if anomaly is None:
                _collect_completed(pending, stats, stats_lock)
                continue

            anomaly_id = anomaly.get("anomaly_id", "unknown")
            tenant_id = anomaly.get("tenant_id", args.tenant)

            future = executor.submit(
                _process_event,
                anomaly,
                tenant_id,
                use_llm=args.use_llm,
                broker=args.broker,
            )
            pending[future] = anomaly_id
            stats["processed"] += 1

        if pending:
            print(f"\n[INFO] Waiting for {len(pending)} in-flight tasks to complete...")
            for future in as_completed(list(pending.keys())):
                anomaly_id = pending.pop(future, "unknown")
                try:
                    result = future.result()
                except Exception as exc:
                    with stats_lock:
                        stats["errors"] += 1
                    print(f"[ERROR] {anomaly_id} | {exc}")
                else:
                    _handle_result(result, stats, stats_lock)

    except KeyboardInterrupt:
        print("\n[INFO] KeyboardInterrupt received. Gracefully exiting...")
    finally:
        executor.shutdown(wait=True, cancel_futures=False)
        consumer.close()
        print(
            "\n[INFO] Service shutdown complete. "
            + f"processed={stats['processed']} produced={stats['produced']} "
            + f"quarantined={stats['quarantined']} errors={stats['errors']}"
        )


if __name__ == "__main__":
    main()
