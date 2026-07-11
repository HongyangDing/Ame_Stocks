import asyncio
import gzip
import json
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd
import polars as pl
import pytest

from ame_stocks_api.cli.flatfiles import main as flatfiles_main
from ame_stocks_api.downloads import BronzeDownloader, build_download_plan
from ame_stocks_api.flatfiles import (
    FlatFileDataset,
    FlatFileDownloadError,
    FlatFileObject,
    MassiveFlatFileDownloader,
    build_daily_coverage,
    build_flat_file_plan,
    convert_flat_file,
)
from ame_stocks_api.transforms import materialize_universe
from ame_stocks_core import ProviderBatch, ProviderDataset, ProviderRequest


class FakeBody:
    def __init__(self, content: bytes, *, fail_after: int | None = None) -> None:
        self.content = content
        self.fail_after = fail_after
        self.closed = False

    def iter_chunks(self, chunk_size: int = 1024 * 1024) -> Iterator[bytes]:
        if self.fail_after is not None:
            yield self.content[: self.fail_after]
            raise RuntimeError("simulated S3 interruption")
        for offset in range(0, len(self.content), chunk_size):
            yield self.content[offset : offset + chunk_size]

    def close(self) -> None:
        self.closed = True


class FakeS3Client:
    def __init__(self, content: bytes, *, interrupt_once: bool = False) -> None:
        self.content = content
        self.interrupt_once = interrupt_once
        self.head_calls = 0
        self.ranges: list[str | None] = []

    def head_object(self, *, Bucket: str, Key: str):
        self.head_calls += 1
        assert Bucket == "flatfiles"
        assert Key.startswith("us_stocks_sip/")
        return {
            "ContentLength": len(self.content),
            "ETag": '"fixture-etag"',
            "LastModified": datetime(2026, 7, 1, tzinfo=UTC),
        }

    def get_object(self, **kwargs: str):
        range_value = kwargs.get("Range")
        self.ranges.append(range_value)
        offset = int(range_value.removeprefix("bytes=").removesuffix("-")) if range_value else 0
        fail_after = None
        if self.interrupt_once:
            self.interrupt_once = False
            fail_after = max(1, len(self.content) // 3)
        response = {"Body": FakeBody(self.content[offset:], fail_after=fail_after)}
        if range_value:
            response["ContentRange"] = f"bytes {offset}-{len(self.content) - 1}/{len(self.content)}"
        return response


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


def _gzip_csv(rows: list[dict[str, object]]) -> bytes:
    header = "ticker,volume,open,close,high,low,window_start,transactions\n"
    lines = [header]
    for row in rows:
        lines.append(
            ",".join(
                str(row[column])
                for column in (
                    "ticker",
                    "volume",
                    "open",
                    "close",
                    "high",
                    "low",
                    "window_start",
                    "transactions",
                )
            )
            + "\n"
        )
    return gzip.compress("".join(lines).encode("utf-8"), compresslevel=9, mtime=0)


def _nanoseconds(value: datetime) -> int:
    return int(value.timestamp() * 1_000_000_000)


def _write_reference_bronze(
    root: Path,
    request: ProviderRequest,
    results: list[dict[str, object]],
) -> None:
    provider = StaticMassiveProvider({"results": results, "status": "OK"})
    asyncio.run(BronzeDownloader(root).download(provider, request))


def _download_fixture(
    root: Path,
    item: FlatFileObject,
    rows: list[dict[str, object]],
) -> None:
    content = _gzip_csv(rows)
    downloader = MassiveFlatFileDownloader(
        root,
        "fixture-access",
        "fixture-secret",
        client=FakeS3Client(content),
        minimum_free_bytes=0,
    )
    downloader.download(item)


def test_flat_file_plan_uses_one_daily_s3_object_and_skips_market_holiday() -> None:
    plan = build_flat_file_plan(
        dataset=FlatFileDataset.MINUTE_AGGREGATES,
        start=date(2026, 6, 30),
        end=date(2026, 7, 3),
    )

    assert [item.session_date for item in plan.objects] == [
        date(2026, 6, 30),
        date(2026, 7, 1),
        date(2026, 7, 2),
    ]
    assert plan.objects[0].object_key == ("us_stocks_sip/minute_aggs_v1/2026/06/2026-06-30.csv.gz")


def test_flat_file_plan_cli_needs_no_s3_credentials(monkeypatch, capsys) -> None:
    monkeypatch.delenv("MASSIVE_S3_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("MASSIVE_S3_SECRET_ACCESS_KEY", raising=False)

    result = flatfiles_main(
        [
            "plan",
            "--dataset",
            "minute_aggregates",
            "--start",
            "2026-06-30",
            "--end",
            "2026-06-30",
        ]
    )

    output = json.loads(capsys.readouterr().out)
    assert result == 0
    assert output["object_count"] == 1
    assert output["note"].startswith("Plan output is offline")


def test_flat_file_plan_cli_defaults_to_five_years(monkeypatch, capsys) -> None:
    monkeypatch.delenv("MASSIVE_S3_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("MASSIVE_S3_SECRET_ACCESS_KEY", raising=False)

    result = flatfiles_main(
        [
            "plan",
            "--dataset",
            "minute_aggregates",
            "--end",
            "2026-06-30",
        ]
    )

    output = json.loads(capsys.readouterr().out)
    assert result == 0
    assert output["start"] == "2021-06-30"
    assert output["end"] == "2026-06-30"
    assert output["objects"][0]["session_date"] == "2021-06-30"
    assert output["objects"][-1]["session_date"] == "2021-07-14"
    assert output["object_count"] > 1_200


def test_flat_file_download_is_checksummed_idempotent_and_resumable(tmp_path: Path) -> None:
    timestamp = _nanoseconds(datetime(2026, 6, 30, 13, 30, tzinfo=UTC))
    content = _gzip_csv(
        [
            {
                "ticker": "AAPL",
                "volume": 100,
                "open": 10,
                "close": 10.5,
                "high": 11,
                "low": 9.5,
                "window_start": timestamp,
                "transactions": 4,
            }
        ]
    )
    item = FlatFileObject(FlatFileDataset.MINUTE_AGGREGATES, date(2026, 6, 30))
    client = FakeS3Client(content, interrupt_once=True)
    downloader = MassiveFlatFileDownloader(
        tmp_path,
        "fixture-access",
        "fixture-secret",
        client=client,
        minimum_free_bytes=0,
    )

    with pytest.raises(FlatFileDownloadError, match="interrupted"):
        downloader.download(item)
    resumed = downloader.download(item)
    head_calls_before_skip = client.head_calls
    skipped = downloader.download(item)

    assert resumed.status == "resumed"
    assert skipped.status == "skipped"
    assert client.head_calls == head_calls_before_skip
    assert client.ranges[0] is None
    assert client.ranges[1] is not None
    assert "fixture-secret" not in repr(downloader)
    manifest_text = resumed.manifest_path.read_text()
    assert "fixture-secret" not in manifest_text
    assert resumed.file_path.read_bytes() == content


def test_flat_file_conversion_preserves_rows_and_new_york_session(tmp_path: Path) -> None:
    open_time = _nanoseconds(datetime(2026, 6, 30, 13, 30, tzinfo=UTC))
    utc_next_day_after_hours = _nanoseconds(datetime(2026, 7, 1, 0, 30, tzinfo=UTC))
    rows = [
        {
            "ticker": "AAPL",
            "volume": 100,
            "open": 10,
            "close": 10.5,
            "high": 11,
            "low": 9.5,
            "window_start": open_time,
            "transactions": 4,
        },
        {
            "ticker": "AAPL",
            "volume": 100,
            "open": 10,
            "close": 10.5,
            "high": 11,
            "low": 9.5,
            "window_start": open_time,
            "transactions": 4,
        },
        {
            "ticker": "OLD",
            "volume": 20,
            "open": 8,
            "close": 8.1,
            "high": 8.2,
            "low": 7.9,
            "window_start": utc_next_day_after_hours,
            "transactions": 2,
        },
    ]
    item = FlatFileObject(FlatFileDataset.MINUTE_AGGREGATES, date(2026, 6, 30))
    _download_fixture(tmp_path, item, rows)

    first = convert_flat_file(tmp_path, item, minimum_free_bytes=0)
    second = convert_flat_file(tmp_path, item, minimum_free_bytes=0)

    assert first.status == "converted"
    assert second.status == "skipped"
    assert first.row_count == 3
    assert first.ticker_count == 2
    assert first.duplicate_count == 1
    frame = pl.read_parquet(first.output_path)
    pandas_frame = pd.read_parquet(first.output_path)
    assert frame["session_date"].unique().to_list() == [date(2026, 6, 30)]
    assert "vwap" not in frame.columns
    assert len(pandas_frame) == 3


def test_daily_universe_and_flat_file_coverage_keep_status_separate(tmp_path: Path) -> None:
    session = date(2026, 6, 30)
    plan = build_download_plan(
        dataset=ProviderDataset.ASSETS,
        start=session,
        end=session,
    )
    requests = {dict(request.parameters)["active"]: request for request in plan.requests}
    _write_reference_bronze(
        tmp_path,
        requests["true"],
        [
            {"active": True, "ticker": "AAPL", "type": "CS"},
            {"active": True, "ticker": "HALT", "type": "CS"},
        ],
    )
    _write_reference_bronze(
        tmp_path,
        requests["false"],
        [
            {
                "active": False,
                "delisted_utc": "2026-06-30T00:00:00Z",
                "ticker": "OLD",
                "type": "CS",
            }
        ],
    )
    universe_result = materialize_universe(tmp_path, start=session, end=session)
    universe_rerun = materialize_universe(tmp_path, start=session, end=session)
    universe = pl.read_parquet(
        tmp_path / "silver_unadjusted/universe/date=2026-06-30/tickers.parquet"
    )

    assert universe_result.status == "materialized"
    assert universe_rerun.status == "skipped"
    assert universe_result.daily_file_count == 1
    assert dict(zip(universe["ticker"], universe["active_on_date"], strict=True)) == {
        "AAPL": True,
        "HALT": True,
        "OLD": False,
    }

    minute_item = FlatFileObject(FlatFileDataset.MINUTE_AGGREGATES, session)
    open_time = _nanoseconds(datetime(2026, 6, 30, 13, 30, tzinfo=UTC))
    activity_rows = [
        {
            "ticker": ticker,
            "volume": 100,
            "open": 10,
            "close": 10.5,
            "high": 11,
            "low": 9.5,
            "window_start": open_time,
            "transactions": 4,
        }
        for ticker in ("AAPL", "OLD", "MISSING")
    ]
    _download_fixture(tmp_path, minute_item, activity_rows)
    convert_flat_file(tmp_path, minute_item, minimum_free_bytes=0)

    coverage = build_daily_coverage(tmp_path, session_date=session)
    coverage_frame = pl.read_parquet(coverage.output_path)

    assert coverage.ticker_count == 4
    assert coverage.active_without_bars == 1
    assert coverage.inactive_with_bars == 1
    assert coverage.bars_without_reference == 1
    halt = coverage_frame.filter(pl.col("ticker") == "HALT").row(0, named=True)
    assert halt["active_on_date"] is True
    assert halt["has_minute_bar"] is False
