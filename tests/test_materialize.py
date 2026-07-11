import asyncio
import json
from collections.abc import AsyncIterator
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd
import polars as pl

from ame_stocks_api.downloads import BronzeDownloader, build_download_plan
from ame_stocks_api.transforms import (
    compact_minute_days,
    materialize_universe,
    partition_minute_request,
)
from ame_stocks_core import ProviderBatch, ProviderDataset, ProviderRequest


class StaticMassiveProvider:
    name = "massive"
    version = "test-fixture"

    def __init__(self, document: dict[str, object]) -> None:
        self.payload = json.dumps(document, sort_keys=True).encode("utf-8")

    async def fetch(
        self,
        request: ProviderRequest,
        *,
        checkpoint=None,
    ) -> AsyncIterator[ProviderBatch]:
        yield ProviderBatch(
            provider=self.name,
            provider_version=self.version,
            dataset=request.dataset,
            request_id=request.request_id,
            sequence=checkpoint.next_sequence if checkpoint else 0,
            payload=self.payload,
        )


def _write_bronze(
    root: Path,
    request: ProviderRequest,
    results: list[dict[str, object]],
) -> None:
    provider = StaticMassiveProvider({"results": results, "status": "OK"})
    asyncio.run(BronzeDownloader(root).download(provider, request))


def _milliseconds(value: datetime) -> int:
    return int(value.timestamp() * 1000)


def test_materialize_universe_keeps_daily_active_and_recently_delisted(tmp_path: Path) -> None:
    plan = build_download_plan(
        dataset=ProviderDataset.ASSETS,
        start=date(2026, 6, 30),
        end=date(2026, 6, 30),
    )
    active_request, inactive_request = plan.requests
    _write_bronze(
        tmp_path,
        active_request,
        [
            {
                "active": True,
                "name": "Apple Inc.",
                "primary_exchange": "XNAS",
                "ticker": "AAPL",
                "type": "CS",
            }
        ],
    )
    _write_bronze(
        tmp_path,
        inactive_request,
        [
            {
                "active": False,
                "delisted_utc": "2026-06-30T00:00:00Z",
                "name": "Recent Delisting",
                "ticker": "OLD",
                "type": "CS",
            },
            {
                "active": False,
                "delisted_utc": "2020-01-02T00:00:00Z",
                "name": "Ancient Delisting",
                "ticker": "ANCIENT",
                "type": "CS",
            },
        ],
    )

    first = materialize_universe(
        tmp_path,
        start=date(2026, 6, 30),
        end=date(2026, 6, 30),
    )
    second = materialize_universe(
        tmp_path,
        start=date(2026, 6, 30),
        end=date(2026, 6, 30),
    )

    assert first.status == "materialized"
    assert second.status == "skipped"
    assert first.snapshot_rows == 3
    assert first.ticker_count == 2
    assert first.ticker_file.read_text().splitlines() == ["AAPL", "OLD"]
    historical = pl.read_parquet(first.ticker_file.with_suffix(".parquet"))
    assert historical["ticker"].to_list() == ["AAPL", "OLD"]
    assert (
        tmp_path / "staging/universe/window=2026-06-30_2026-06-30/snapshots/"
        "date=2026-06-30/status=inactive/tickers.parquet"
    ).is_file()


def test_partition_and_compact_minute_data_use_new_york_date_and_preserve_duplicates(
    tmp_path: Path,
) -> None:
    plan = build_download_plan(
        dataset=ProviderDataset.MINUTE_BARS,
        start=date(2026, 6, 30),
        end=date(2026, 7, 1),
        tickers=("AAPL", "MSFT"),
    )
    requests = {request.asset_ids[0]: request for request in plan.requests}
    june_30_open = _milliseconds(datetime(2026, 6, 30, 13, 30, tzinfo=UTC))
    june_30_after_hours_in_utc_july = _milliseconds(datetime(2026, 7, 1, 0, 30, tzinfo=UTC))
    july_1_open = _milliseconds(datetime(2026, 7, 1, 13, 30, tzinfo=UTC))
    _write_bronze(
        tmp_path,
        requests["AAPL"],
        [
            {
                "c": 10.5,
                "h": 11.0,
                "l": 9.5,
                "n": 4,
                "o": 10.0,
                "t": june_30_open,
                "v": 100,
                "vw": 10.2,
            },
            {
                "c": 10.5,
                "h": 11.0,
                "l": 9.5,
                "n": 4,
                "o": 10.0,
                "t": june_30_open,
                "v": 100,
                "vw": 10.2,
            },
            {
                "c": 10.8,
                "h": 10.9,
                "l": 10.7,
                "n": 2,
                "o": 10.7,
                "t": june_30_after_hours_in_utc_july,
                "v": 20,
                "vw": 10.8,
            },
            {
                "c": 11.5,
                "h": 12.0,
                "l": 11.0,
                "n": 5,
                "o": 11.0,
                "t": july_1_open,
                "v": 120,
                "vw": 11.4,
            },
        ],
    )
    _write_bronze(
        tmp_path,
        requests["MSFT"],
        [
            {
                "c": 20.5,
                "h": 21.0,
                "l": 19.5,
                "n": 3,
                "o": 20.0,
                "t": june_30_open,
                "v": 80,
                "vw": 20.2,
            }
        ],
    )

    aapl = partition_minute_request(tmp_path, requests["AAPL"])
    msft = partition_minute_request(tmp_path, requests["MSFT"])
    repeated = partition_minute_request(tmp_path, requests["AAPL"])

    assert aapl.status == "materialized"
    assert repeated.status == "skipped"
    assert aapl.session_count == 2
    assert aapl.fragment_count == 1
    assert msft.session_count == 1
    staged_aapl = pl.read_parquet(
        next(
            (tmp_path / "staging/minute_unadjusted/by_ticker/ticker=AAPL").glob(
                "request_id=*/bars.parquet"
            )
        )
    )
    june_30_aapl = staged_aapl.filter(pl.col("session_date") == date(2026, 6, 30))
    assert june_30_aapl.height == 3

    daily = compact_minute_days(
        tmp_path,
        start=date(2026, 6, 30),
        end=date(2026, 7, 1),
        requests=plan.requests,
    )

    assert len(daily) == 2
    assert daily[0].row_count == 4
    assert daily[0].ticker_count == 2
    assert daily[0].duplicate_count == 1
    assert daily[1].row_count == 1
    june_30 = pl.read_parquet(daily[0].output_path)
    june_30_pandas = pd.read_parquet(daily[0].output_path)
    assert june_30.columns == [
        "session_date",
        "timestamp_utc",
        "ticker",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "vwap",
        "transactions",
        "otc",
    ]
    assert len(june_30_pandas) == 4
    assert june_30["ticker"].to_list()[:3] == ["AAPL", "AAPL", "MSFT"]

    rerun = compact_minute_days(
        tmp_path,
        start=date(2026, 6, 30),
        end=date(2026, 7, 1),
        requests=plan.requests,
    )
    assert all(result.status == "skipped" for result in rerun)
