"""Print a deterministic, read-only S4 assets source profile as JSON."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pyarrow.parquet as pq

from ame_stocks_api.silver.asset_source_profile import (
    AssetSourceProfileError,
    profile_asset_source,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ame-silver-assets-source-profile",
        description=(
            "Stream manifest-bound Massive assets pages and print a deterministic read-only "
            "source profile. The command never writes to the data root."
        ),
    )
    parser.add_argument("--data-root", required=True, type=Path)
    parser.add_argument("--manifest", action="append", type=Path, default=None)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--exchange-dim", type=Path)
    parser.add_argument("--ticker-type-dim", type=Path)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    arguments = parser.parse_args(argv)
    try:
        exchange_mics = _parquet_values(arguments.exchange_dim, "mic")
        ticker_types = _parquet_values(arguments.ticker_type_dim, "type_code")
        report = profile_asset_source(
            arguments.data_root,
            manifest_paths=arguments.manifest,
            workers=arguments.workers,
            current_exchange_mics=exchange_mics,
            current_ticker_types=ticker_types,
        )
        print(json.dumps(report, allow_nan=False, indent=2, sort_keys=True))
        return 0
    except (AssetSourceProfileError, OSError, ValueError) as exc:
        parser.exit(2, f"ame-silver-assets-source-profile: {exc}\n")


def _parquet_values(path: Path | None, column: str) -> set[str] | None:
    if path is None:
        return None
    table = pq.read_table(path.expanduser().resolve(), columns=[column])
    return {value for value in table.column(column).to_pylist() if isinstance(value, str)}


if __name__ == "__main__":
    raise SystemExit(main())
