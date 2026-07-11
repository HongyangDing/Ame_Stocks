"""Offline command for daily point-in-time universe materialization."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from ame_stocks_api.artifacts import ArtifactError
from ame_stocks_api.cli.date_range import add_history_range_arguments, resolve_history_range
from ame_stocks_api.downloads import BronzeStorageError
from ame_stocks_api.transforms import (
    MaterializationError,
    materialize_ticker_overview_lifecycles,
    materialize_ticker_overview_safe,
    materialize_universe,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ame-materialize",
        description="Offline only: verify Bronze and build reviewable Parquet outputs.",
    )
    parser.add_argument(
        "action",
        choices=("ticker-overview-lifecycles", "ticker-overview-safe", "universe"),
    )
    parser.add_argument("--data-root", type=Path, required=True)
    add_history_range_arguments(parser)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    arguments = parser.parse_args(argv)
    try:
        start, end = resolve_history_range(
            start=arguments.start,
            end=arguments.end,
            years=arguments.years,
        )
        if arguments.action == "universe":
            result = materialize_universe(
                arguments.data_root,
                start=start,
                end=end,
            )
            print(
                json.dumps(
                    {
                        "manifest": str(result.manifest_path),
                        "daily_files": result.daily_file_count,
                        "snapshot_rows": result.snapshot_rows,
                        "status": result.status,
                        "ticker_count": result.ticker_count,
                        "ticker_file": str(result.ticker_file),
                    },
                    sort_keys=True,
                )
            )
            return 0
        if arguments.action == "ticker-overview-lifecycles":
            result = materialize_ticker_overview_lifecycles(
                arguments.data_root,
                start=start,
                end=end,
            )
            print(
                json.dumps(
                    {
                        "lifecycle_count": result.lifecycle_count,
                        "lifecycle_file": str(result.lifecycle_path),
                        "manifest": str(result.manifest_path),
                        "request_count": result.request_count,
                        "request_file": str(result.request_file),
                        "status": result.status,
                    },
                    sort_keys=True,
                )
            )
            return 0
        if arguments.action == "ticker-overview-safe":
            result = materialize_ticker_overview_safe(
                arguments.data_root,
                start=start,
                end=end,
            )
            print(
                json.dumps(
                    {
                        "failed_request_count": result.failed_request_count,
                        "lifecycle_count": result.lifecycle_count,
                        "manifest": str(result.manifest_path),
                        "output_file": str(result.output_path),
                        "row_count": result.row_count,
                        "status": result.status,
                    },
                    sort_keys=True,
                )
            )
            return 0
    except (ArtifactError, BronzeStorageError, MaterializationError, OSError, ValueError) as exc:
        parser.exit(2, f"ame-materialize: {exc}\n")


if __name__ == "__main__":
    raise SystemExit(main())
