"""Offline commands for universe and minute Parquet materialization."""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

from ame_stocks_api.downloads import BronzeStorageError, build_download_plan
from ame_stocks_api.transforms import (
    MaterializationError,
    compact_minute_days,
    materialize_universe,
    partition_minute_request,
)
from ame_stocks_core import ProviderDataset


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ame-materialize",
        description="Offline only: verify Bronze and build reviewable Parquet outputs.",
    )
    parser.add_argument(
        "action",
        choices=("universe", "partition-minute", "compact-minute"),
    )
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--start", type=_parse_date, required=True)
    parser.add_argument("--end", type=_parse_date, required=True)
    parser.add_argument("--ticker-file", type=Path)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    arguments = parser.parse_args(argv)
    try:
        if arguments.action == "universe":
            if arguments.ticker_file is not None:
                parser.error("universe does not accept --ticker-file")
            result = materialize_universe(
                arguments.data_root,
                start=arguments.start,
                end=arguments.end,
            )
            print(
                json.dumps(
                    {
                        "manifest": str(result.manifest_path),
                        "snapshot_rows": result.snapshot_rows,
                        "status": result.status,
                        "ticker_count": result.ticker_count,
                        "ticker_file": str(result.ticker_file),
                    },
                    sort_keys=True,
                )
            )
            return 0

        if arguments.ticker_file is None:
            parser.error(f"{arguments.action} requires --ticker-file")
        tickers = _load_tickers(arguments.ticker_file)
        plan = build_download_plan(
            dataset=ProviderDataset.MINUTE_BARS,
            start=arguments.start,
            end=arguments.end,
            tickers=tickers,
        )
        if arguments.action == "partition-minute":
            results = [
                partition_minute_request(arguments.data_root, request) for request in plan.requests
            ]
            print(
                json.dumps(
                    {
                        "requests": len(results),
                        "rows": sum(result.row_count for result in results),
                        "status": "complete",
                        "ticker_files": sum(result.fragment_count for result in results),
                    },
                    sort_keys=True,
                )
            )
            return 0

        results = compact_minute_days(
            arguments.data_root,
            start=arguments.start,
            end=arguments.end,
            requests=plan.requests,
        )
        print(
            json.dumps(
                {
                    "days": len(results),
                    "duplicates_preserved": sum(result.duplicate_count for result in results),
                    "rows": sum(result.row_count for result in results),
                    "status": "complete",
                },
                sort_keys=True,
            )
        )
        return 0
    except (BronzeStorageError, MaterializationError, OSError, ValueError) as exc:
        parser.exit(2, f"ame-materialize: {exc}\n")


def _load_tickers(path: Path) -> tuple[str, ...]:
    tickers = []
    for line in path.read_text(encoding="utf-8").splitlines():
        ticker = line.partition("#")[0].strip()
        if ticker:
            tickers.append(ticker)
    return tuple(tickers)


def _parse_date(raw: str) -> date:
    try:
        return date.fromisoformat(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("dates must use YYYY-MM-DD") from exc


if __name__ == "__main__":
    raise SystemExit(main())
