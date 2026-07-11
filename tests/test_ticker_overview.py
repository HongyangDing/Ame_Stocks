import asyncio
import json
from collections.abc import AsyncIterator
from datetime import date
from pathlib import Path

import polars as pl

from ame_stocks_api.downloads import BronzeDownloader, BronzeReader, build_download_plan
from ame_stocks_api.transforms import (
    QUARANTINED_TICKER_OVERVIEW_FIELDS,
    materialize_ticker_overview_lifecycles,
    materialize_ticker_overview_safe,
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
    results: object,
) -> None:
    provider = StaticMassiveProvider({"results": results, "status": "OK"})
    asyncio.run(BronzeDownloader(root, minimum_free_bytes=0).download(provider, request))


def test_lifecycle_overview_allowlist_quarantines_market_cap_and_shares(
    tmp_path: Path,
) -> None:
    start = date(2026, 6, 29)
    middle = date(2026, 6, 30)
    end = date(2026, 7, 1)
    asset_plan = build_download_plan(
        dataset=ProviderDataset.ASSETS,
        start=start,
        end=end,
        active="true",
    )
    _write_bronze(
        tmp_path,
        asset_plan.requests[0],
        [
            {
                "active": True,
                "ticker": "AAPL",
                "share_class_figi": "FIGI-AAPL",
                "cik": "0000320193",
            },
            {
                "active": True,
                "ticker": "REUSE",
                "share_class_figi": "FIGI-OLD",
                "cik": "0000000001",
            },
        ],
    )
    _write_bronze(
        tmp_path,
        asset_plan.requests[1],
        [
            {
                "active": True,
                "ticker": "AAPL",
                "share_class_figi": "FIGI-AAPL",
                "cik": "0000320193",
            },
            {
                "active": True,
                "ticker": "REUSE",
                "share_class_figi": "FIGI-NEW",
                "cik": "0000000002",
            },
        ],
    )
    _write_bronze(
        tmp_path,
        asset_plan.requests[2],
        [
            {
                "active": True,
                "ticker": "AAPL",
                "share_class_figi": "FIGI-AAPL",
                "cik": "0000320193",
            },
            {
                "active": True,
                "ticker": "REUSE",
                "share_class_figi": "FIGI-OLD",
                "cik": "0000000001",
            },
        ],
    )

    lifecycle = materialize_ticker_overview_lifecycles(tmp_path, start=start, end=end)
    lifecycle_rerun = materialize_ticker_overview_lifecycles(tmp_path, start=start, end=end)
    lifecycles = pl.read_parquet(lifecycle.lifecycle_path)

    assert lifecycle.status == "materialized"
    assert lifecycle_rerun.status == "skipped"
    assert lifecycle.lifecycle_count == 3
    assert lifecycle.request_count == 3
    assert lifecycle.request_file.read_text(encoding="utf-8").splitlines() == [
        "ticker,query_date",
        "REUSE,2026-06-30",
        "AAPL,2026-07-01",
        "REUSE,2026-07-01",
    ]
    assert lifecycles.filter(pl.col("ticker") == "AAPL")["first_active_date"].item() == start

    ticker_dates = tuple(
        (row["ticker"], row["query_date"])
        for row in lifecycles.select("ticker", "query_date").iter_rows(named=True)
    )
    overview_plan = build_download_plan(
        dataset=ProviderDataset.TICKER_OVERVIEW,
        start=start,
        end=end,
        ticker_dates=ticker_dates,
    )
    overview_results = {
        ("REUSE", end): {
            "ticker": "REUSE",
            "active": True,
            "share_class_figi": "FIGI-OLD",
            "cik": "0000000001",
            "sic_code": "1000",
            "sic_description": "Old industry",
            "list_date": "2010-01-02",
            "market_cap": 999,
            "weighted_shares_outstanding": 111,
            "share_class_shares_outstanding": 110,
        },
        ("AAPL", end): {
            "ticker": "AAPL",
            "name": "Apple Inc.",
            "active": True,
            "cik": "0000320193",
            "sic_code": "3571",
            "sic_description": "Electronic Computers",
            "list_date": "1980-12-12",
            "market_cap": 3_000_000,
            "weighted_shares_outstanding": 15_000,
            "share_class_shares_outstanding": 14_900,
            "total_employees": 999_999,
        },
        ("REUSE", middle): {
            "ticker": "REUSE",
            "active": True,
            "share_class_figi": "FIGI-NEW",
            "cik": "0000000002",
            "sic_code": "2000",
            "list_date": "2026-06-30",
            "market_cap": 123,
            "weighted_shares_outstanding": 45,
        },
    }
    for request in overview_plan.requests:
        _write_bronze(
            tmp_path,
            request,
            overview_results[(request.asset_ids[0], request.start)],
        )

    safe = materialize_ticker_overview_safe(tmp_path, start=start, end=end)
    safe_rerun = materialize_ticker_overview_safe(tmp_path, start=start, end=end)
    safe_frame = pl.read_parquet(safe.output_path)

    assert safe.status == "materialized"
    assert safe_rerun.status == "skipped"
    assert safe.row_count == 3
    assert safe.failed_request_count == 0
    assert safe_frame["identity_match"].to_list() == [True, True, True]
    assert safe_frame["identity_match_basis"].to_list() == [
        "share_class_figi",
        "cik",
        "share_class_figi",
    ]
    assert not QUARANTINED_TICKER_OVERVIEW_FIELDS.intersection(safe_frame.columns)
    assert "total_employees" not in safe_frame.columns
    assert safe_frame.filter(pl.col("ticker") == "AAPL")["sic_code"].item() == "3571"

    apple_request = next(
        request
        for request in overview_plan.requests
        if request.asset_ids == ("AAPL",)
    )
    raw_result = BronzeReader(tmp_path).pages(apple_request)[0].document["results"]
    assert raw_result["market_cap"] == 3_000_000
    assert raw_result["weighted_shares_outstanding"] == 15_000
