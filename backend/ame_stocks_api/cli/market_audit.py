"""CLI for the resumable minute-to-day Flat File reconciliation."""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

from ame_stocks_api.artifacts import write_json_atomic
from ame_stocks_api.audit.market import (
    MarketAuditError,
    MarketAuditTolerance,
    MarketCrossAuditor,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ame-audit-market",
        description=(
            "Reconcile every Massive minute Flat File with its day aggregate using "
            "manifest-bound per-session caches."
        ),
    )
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--cache-dir", type=Path)
    parser.add_argument("--no-cache", action="store_true")
    parser.add_argument("--max-examples", type=int, default=20)
    parser.add_argument("--price-absolute-tolerance", type=float, default=1e-8)
    parser.add_argument("--price-relative-tolerance", type=float, default=1e-9)
    parser.add_argument("--volume-absolute-tolerance", type=float, default=1e-6)
    parser.add_argument("--volume-relative-tolerance", type=float, default=1e-9)
    parser.add_argument(
        "--output",
        type=Path,
        help="optional JSON report path; relative paths are resolved under data-root",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    arguments = parser.parse_args(argv)
    try:
        tolerance = MarketAuditTolerance(
            price_absolute=arguments.price_absolute_tolerance,
            price_relative=arguments.price_relative_tolerance,
            volume_absolute=arguments.volume_absolute_tolerance,
            volume_relative=arguments.volume_relative_tolerance,
        )
        report = MarketCrossAuditor(
            arguments.data_root,
            start=arguments.start,
            end=arguments.end,
            workers=arguments.workers,
            cache_dir=arguments.cache_dir,
            use_cache=not arguments.no_cache,
            tolerance=tolerance,
            max_examples=arguments.max_examples,
        ).run()
        if arguments.output:
            output = arguments.output
            if not output.is_absolute():
                output = arguments.data_root / output
            write_json_atomic(output.resolve(), report)
            report["report_path"] = str(output.resolve())
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0 if report["status"] == "passed" else 1
    except (MarketAuditError, OSError, ValueError) as exc:
        parser.exit(2, f"ame-audit-market: {exc}\n")


if __name__ == "__main__":
    raise SystemExit(main())
