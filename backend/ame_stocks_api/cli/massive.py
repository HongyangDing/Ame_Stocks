"""Plan or explicitly execute Massive Bronze downloads."""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
from datetime import date
from pathlib import Path

from ame_stocks_api.cli.date_range import add_history_range_arguments, resolve_history_range
from ame_stocks_api.downloads import BronzeDownloader, BronzeStorageError, build_download_plan
from ame_stocks_api.providers import MassiveProvider, MassiveProviderError
from ame_stocks_core import ProviderDataset, ProviderRequest


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ame-massive",
        description="Plan by default; only the explicit download command contacts Massive.",
    )
    parser.add_argument("action", choices=("plan", "download"))
    parser.add_argument(
        "--dataset",
        choices=[dataset.value for dataset in ProviderDataset],
        required=True,
    )
    add_history_range_arguments(parser)
    parser.add_argument("--ticker", action="append", default=[])
    parser.add_argument("--ticker-file", type=Path)
    parser.add_argument(
        "--ticker-date-file",
        type=Path,
        help="ticker_overview only: CSV with exact ticker,query_date columns",
    )
    parser.add_argument(
        "--active",
        choices=("history", "true", "false", "both"),
        default="both",
        help=(
            "assets only: both saves active and inactive on every session; history saves "
            "daily active plus one final inactive snapshot"
        ),
    )
    parser.add_argument("--requests-per-minute", type=float, default=600.0)
    parser.add_argument(
        "--concurrency",
        type=int,
        default=10,
        help="maximum concurrent request streams for paid-plan downloads",
    )
    parser.add_argument("--timeout-seconds", type=float, default=30.0)
    parser.add_argument("--max-attempts", type=int, default=5)
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help=(
            "finish independent request streams after expected per-identifier failures; "
            "failed manifests remain retryable"
        ),
    )
    parser.add_argument(
        "--allow-partial-success",
        action="store_true",
        help="return exit code 0 despite failed requests; requires --continue-on-error",
    )
    parser.add_argument("--data-root", type=Path)
    parser.add_argument("--show-all", action="store_true")
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
        tickers = _load_tickers(arguments.ticker, arguments.ticker_file)
        ticker_dates = _load_ticker_dates(arguments.ticker_date_file)
        dataset = ProviderDataset(arguments.dataset)
        plan = build_download_plan(
            dataset=dataset,
            start=start,
            end=end,
            tickers=tickers,
            ticker_dates=ticker_dates,
            active=arguments.active,
            requests_per_minute=arguments.requests_per_minute,
        )
        if arguments.concurrency < 1:
            raise ValueError("concurrency must be positive")
        if arguments.allow_partial_success and not arguments.continue_on_error:
            raise ValueError("--allow-partial-success requires --continue-on-error")
        if arguments.action == "plan":
            print(json.dumps(plan.summary(show_all=arguments.show_all), indent=2, sort_keys=True))
            return 0
        if arguments.data_root is None:
            parser.error("download requires --data-root; no implicit storage path is allowed")
        return asyncio.run(_execute_downloads(arguments, plan.requests))
    except (BronzeStorageError, MassiveProviderError, OSError, ValueError) as exc:
        parser.exit(2, f"ame-massive: {exc}\n")


async def _execute_downloads(
    arguments: argparse.Namespace,
    requests: tuple[ProviderRequest, ...],
) -> int:
    downloader = BronzeDownloader(arguments.data_root)
    downloaded = resumed = skipped = failed = pages = records = compressed_bytes = 0

    async with MassiveProvider.from_env(
        requests_per_minute=arguments.requests_per_minute,
        timeout_seconds=arguments.timeout_seconds,
        max_attempts=arguments.max_attempts,
    ) as provider:
        semaphore = asyncio.Semaphore(arguments.concurrency)

        async def download_one(index: int, request: ProviderRequest):
            async with semaphore:
                try:
                    result = await downloader.download(provider, request)
                except (BronzeStorageError, MassiveProviderError, OSError) as exc:
                    if not arguments.continue_on_error:
                        raise
                    return index, request, None, type(exc).__name__
                return index, request, result, None

        tasks = [
            asyncio.create_task(download_one(index, request))
            for index, request in enumerate(requests, start=1)
        ]
        try:
            for completed in asyncio.as_completed(tasks):
                index, request, result, error_type = await completed
                if result is None:
                    failed += 1
                    print(
                        json.dumps(
                            {
                                "error_type": error_type,
                                "index": index,
                                "request_count": len(requests),
                                "request_id": request.request_id,
                                "status": "failed",
                            },
                            sort_keys=True,
                        )
                    )
                    continue
                downloaded += result.status == "downloaded"
                resumed += result.status == "resumed"
                skipped += result.status == "skipped"
                pages += result.page_count
                records += result.record_count
                compressed_bytes += result.compressed_bytes
                print(
                    json.dumps(
                        {
                            "index": index,
                            "manifest": str(result.manifest_path),
                            "request_count": len(requests),
                            "request_id": request.request_id,
                            "status": result.status,
                        },
                        sort_keys=True,
                    )
                )
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    print(
        json.dumps(
            {
                "compressed_bytes": compressed_bytes,
                "downloaded_requests": downloaded,
                "failed_requests": failed,
                "pages": pages,
                "records": records,
                "resumed_requests": resumed,
                "skipped_requests": skipped,
                "status": "complete_with_failures" if failed else "complete",
            },
            sort_keys=True,
        )
    )
    return 0 if not failed or getattr(arguments, "allow_partial_success", False) else 1


def _load_tickers(cli_tickers: list[str], ticker_file: Path | None) -> tuple[str, ...]:
    tickers = list(cli_tickers)
    if ticker_file:
        for line in ticker_file.read_text(encoding="utf-8").splitlines():
            value = line.partition("#")[0].strip()
            if value:
                tickers.append(value)
    return tuple(tickers)


def _load_ticker_dates(ticker_date_file: Path | None) -> tuple[tuple[str, date], ...]:
    if ticker_date_file is None:
        return ()
    with ticker_date_file.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != ["ticker", "query_date"]:
            raise ValueError("ticker-date CSV must have exact ticker,query_date columns")
        rows: list[tuple[str, date]] = []
        for line_number, row in enumerate(reader, start=2):
            ticker = str(row.get("ticker", "")).strip()
            raw_date = str(row.get("query_date", "")).strip()
            if not ticker or not raw_date:
                raise ValueError(f"ticker-date CSV line {line_number} is incomplete")
            try:
                query_date = date.fromisoformat(raw_date)
            except ValueError as exc:
                raise ValueError(f"ticker-date CSV line {line_number} has an invalid date") from exc
            rows.append((ticker, query_date))
    if not rows:
        raise ValueError("ticker-date CSV cannot be empty")
    return tuple(rows)


if __name__ == "__main__":
    raise SystemExit(main())
