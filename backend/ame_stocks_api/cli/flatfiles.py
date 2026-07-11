"""Explicit plan, download, conversion, and coverage commands for Flat Files."""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

from ame_stocks_api.artifacts import ArtifactError
from ame_stocks_api.flatfiles import (
    FlatFileDataset,
    FlatFileDownloadError,
    MassiveFlatFileDownloader,
    build_daily_coverage,
    build_flat_file_plan,
    convert_flat_file,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ame-flatfiles",
        description="Plan is offline; only the explicit download action contacts Massive S3.",
    )
    subparsers = parser.add_subparsers(dest="action", required=True)
    plan = subparsers.add_parser("plan", help="print deterministic S3 object keys offline")
    _add_dataset_range(plan)
    plan.add_argument("--show-all", action="store_true")

    download = subparsers.add_parser("download", help="download immutable gzip CSV Bronze files")
    _add_dataset_range(download)
    download.add_argument("--data-root", type=Path, required=True)

    convert = subparsers.add_parser("convert", help="convert downloaded CSV files to Parquet")
    _add_dataset_range(convert)
    convert.add_argument("--data-root", type=Path, required=True)

    coverage = subparsers.add_parser(
        "coverage",
        help="join minute activity to the daily REST security master",
    )
    _add_range(coverage)
    coverage.add_argument("--data-root", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    arguments = parser.parse_args(argv)
    try:
        if arguments.action == "coverage":
            return _coverage(arguments)
        dataset = FlatFileDataset(arguments.dataset)
        plan = build_flat_file_plan(
            dataset=dataset,
            start=arguments.start,
            end=arguments.end,
        )
        if arguments.action == "plan":
            print(json.dumps(plan.summary(show_all=arguments.show_all), indent=2, sort_keys=True))
            return 0
        if arguments.action == "download":
            downloader = MassiveFlatFileDownloader.from_env(arguments.data_root)
            downloaded = resumed = skipped = compressed_bytes = 0
            for index, item in enumerate(plan.objects, start=1):
                result = downloader.download(item)
                downloaded += result.status == "downloaded"
                resumed += result.status == "resumed"
                skipped += result.status == "skipped"
                compressed_bytes += result.compressed_bytes
                print(
                    json.dumps(
                        {
                            "index": index,
                            "object_count": len(plan.objects),
                            "object_key": item.object_key,
                            "status": result.status,
                        },
                        sort_keys=True,
                    )
                )
            print(
                json.dumps(
                    {
                        "compressed_bytes": compressed_bytes,
                        "downloaded": downloaded,
                        "resumed": resumed,
                        "skipped": skipped,
                        "status": "complete",
                    },
                    sort_keys=True,
                )
            )
            return 0

        results = [convert_flat_file(arguments.data_root, item) for item in plan.objects]
        print(
            json.dumps(
                {
                    "converted": sum(result.status == "converted" for result in results),
                    "days": len(results),
                    "duplicates_preserved": sum(result.duplicate_count for result in results),
                    "rows": sum(result.row_count for result in results),
                    "skipped": sum(result.status == "skipped" for result in results),
                    "status": "complete",
                },
                sort_keys=True,
            )
        )
        return 0
    except (ArtifactError, FlatFileDownloadError, OSError, ValueError) as exc:
        parser.exit(2, f"ame-flatfiles: {exc}\n")


def _coverage(arguments: argparse.Namespace) -> int:
    sessions = build_flat_file_plan(
        dataset=FlatFileDataset.MINUTE_AGGREGATES,
        start=arguments.start,
        end=arguments.end,
    ).objects
    results = [
        build_daily_coverage(arguments.data_root, session_date=item.session_date)
        for item in sessions
    ]
    print(
        json.dumps(
            {
                "active_without_bars": sum(result.active_without_bars for result in results),
                "bars_without_reference": sum(result.bars_without_reference for result in results),
                "days": len(results),
                "inactive_with_bars": sum(result.inactive_with_bars for result in results),
                "status": "complete",
            },
            sort_keys=True,
        )
    )
    return 0


def _add_dataset_range(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--dataset",
        choices=[dataset.value for dataset in FlatFileDataset],
        required=True,
    )
    _add_range(parser)


def _add_range(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--start", type=_parse_date, required=True)
    parser.add_argument("--end", type=_parse_date, required=True)


def _parse_date(raw: str) -> date:
    try:
        return date.fromisoformat(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("dates must use YYYY-MM-DD") from exc


if __name__ == "__main__":
    raise SystemExit(main())
