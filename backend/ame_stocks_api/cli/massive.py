"""Plan or explicitly execute Massive Bronze downloads."""

from __future__ import annotations

import argparse
import asyncio
import json
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
        dataset = ProviderDataset(arguments.dataset)
        plan = build_download_plan(
            dataset=dataset,
            start=start,
            end=end,
            tickers=tickers,
            active=arguments.active,
            requests_per_minute=arguments.requests_per_minute,
        )
        if arguments.concurrency < 1:
            raise ValueError("concurrency must be positive")
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
    downloaded = resumed = skipped = pages = records = compressed_bytes = 0

    async with MassiveProvider.from_env(
        requests_per_minute=arguments.requests_per_minute,
        timeout_seconds=arguments.timeout_seconds,
        max_attempts=arguments.max_attempts,
    ) as provider:
        semaphore = asyncio.Semaphore(arguments.concurrency)

        async def download_one(index: int, request: ProviderRequest):
            async with semaphore:
                return index, request, await downloader.download(provider, request)

        tasks = [
            asyncio.create_task(download_one(index, request))
            for index, request in enumerate(requests, start=1)
        ]
        try:
            for completed in asyncio.as_completed(tasks):
                index, request, result = await completed
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
                "pages": pages,
                "records": records,
                "resumed_requests": resumed,
                "skipped_requests": skipped,
                "status": "complete",
            },
            sort_keys=True,
        )
    )
    return 0


def _load_tickers(cli_tickers: list[str], ticker_file: Path | None) -> tuple[str, ...]:
    tickers = list(cli_tickers)
    if ticker_file:
        for line in ticker_file.read_text(encoding="utf-8").splitlines():
            value = line.partition("#")[0].strip()
            if value:
                tickers.append(value)
    return tuple(tickers)


if __name__ == "__main__":
    raise SystemExit(main())
