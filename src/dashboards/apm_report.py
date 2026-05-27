from __future__ import annotations

import argparse
from pathlib import Path

from src.serving.report import sample_results, write_report


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate the Phase 3 Doris-backed APM report.")
    parser.add_argument("--output", default="reports/phase3-serving-report.md")
    args = parser.parse_args()
    path = write_report(sample_results(), Path(args.output))
    print(f"Wrote {path}")


if __name__ == "__main__":
    main()
