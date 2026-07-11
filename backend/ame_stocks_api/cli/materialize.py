"""Offline command for daily point-in-time universe materialization."""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

from ame_stocks_api.artifacts import ArtifactError
from ame_stocks_api.downloads import BronzeStorageError
from ame_stocks_api.transforms import MaterializationError, materialize_universe


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ame-materialize",
        description="Offline only: verify Bronze and build reviewable Parquet outputs.",
    )
    parser.add_argument(
        "action",
        choices=("universe",),
    )
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--start", type=_parse_date, required=True)
    parser.add_argument("--end", type=_parse_date, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    arguments = parser.parse_args(argv)
    try:
        if arguments.action == "universe":
            result = materialize_universe(
                arguments.data_root,
                start=arguments.start,
                end=arguments.end,
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
    except (ArtifactError, BronzeStorageError, MaterializationError, OSError, ValueError) as exc:
        parser.exit(2, f"ame-materialize: {exc}\n")


def _parse_date(raw: str) -> date:
    try:
        return date.fromisoformat(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("dates must use YYYY-MM-DD") from exc


if __name__ == "__main__":
    raise SystemExit(main())
