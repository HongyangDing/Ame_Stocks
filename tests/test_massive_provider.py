import asyncio
from collections.abc import Callable
from datetime import date

import httpx2
import pytest

from ame_stocks_api.providers import (
    MassiveConfigurationError,
    MassiveProvider,
    MassiveRequestError,
    MassiveResponseError,
)
from ame_stocks_core import FetchCheckpoint, ProviderDataset, ProviderRequest

BASE_URL = "https://api.massive.test"
TEST_KEY = "unit-test-key-not-a-real-secret"


async def _collect(provider: MassiveProvider, request: ProviderRequest, checkpoint=None):
    async with provider:
        return [batch async for batch in provider.fetch(request, checkpoint=checkpoint)]


def _provider(handler: Callable, **kwargs) -> MassiveProvider:
    return MassiveProvider(
        TEST_KEY,
        base_url=BASE_URL,
        requests_per_minute=None,
        transport=httpx2.MockTransport(handler),
        **kwargs,
    )


def test_minute_bars_use_bearer_auth_max_limit_and_exact_pagination() -> None:
    requests: list[httpx2.Request] = []

    def handler(request: httpx2.Request) -> httpx2.Response:
        requests.append(request)
        assert request.headers["authorization"] == f"Bearer {TEST_KEY}"
        assert "apiKey" not in request.url.params
        if len(requests) == 1:
            assert request.url.path == "/v2/aggs/ticker/BRK.B/range/1/minute/2024-07-01/2026-06-30"
            assert request.url.params["adjusted"] == "false"
            assert request.url.params["limit"] == "50000"
            assert request.url.params["sort"] == "asc"
            return httpx2.Response(
                200,
                json={
                    "next_url": f"{BASE_URL}/v2/aggs/ticker/BRK.B/range/1/minute/"
                    "1719792000000/2026-06-30?cursor=page-two",
                    "results": [{"t": 1}],
                    "status": "OK",
                },
            )
        assert request.url.params["cursor"] == "page-two"
        return httpx2.Response(200, json={"results": [{"t": 2}], "status": "OK"})

    provider = _provider(handler)
    request = ProviderRequest(
        dataset=ProviderDataset.MINUTE_BARS,
        start=date(2024, 7, 1),
        end=date(2026, 6, 30),
        asset_ids=("BRK.B",),
        adjusted=False,
    )

    batches = asyncio.run(_collect(provider, request))

    assert [batch.sequence for batch in batches] == [0, 1]
    assert batches[0].is_last is False
    assert batches[0].next_cursor is not None
    assert batches[0].next_cursor.startswith("/v2/aggs/")
    assert batches[1].is_last is True


@pytest.mark.parametrize(
    ("dataset", "start", "end", "asset_ids", "parameters", "expected_path", "expected_query"),
    [
        (
            ProviderDataset.ASSETS,
            date(2026, 6, 30),
            date(2026, 6, 30),
            (),
            (("active", "false"),),
            "/v3/reference/tickers",
            {"active": "false", "date": "2026-06-30", "limit": "1000", "market": "stocks"},
        ),
        (
            ProviderDataset.DAILY_BARS,
            date(2024, 7, 1),
            date(2026, 6, 30),
            ("AAPL",),
            (),
            "/v2/aggs/ticker/AAPL/range/1/day/2024-07-01/2026-06-30",
            {"adjusted": "false", "limit": "50000", "sort": "asc"},
        ),
        (
            ProviderDataset.SPLITS,
            date(2024, 7, 1),
            date(2026, 6, 30),
            (),
            (),
            "/stocks/v1/splits",
            {
                "execution_date.gte": "2024-07-01",
                "execution_date.lte": "2026-06-30",
                "limit": "5000",
            },
        ),
        (
            ProviderDataset.DIVIDENDS,
            date(2024, 7, 1),
            date(2026, 6, 30),
            (),
            (),
            "/stocks/v1/dividends",
            {
                "ex_dividend_date.gte": "2024-07-01",
                "ex_dividend_date.lte": "2026-06-30",
                "limit": "5000",
            },
        ),
    ],
)
def test_dataset_endpoint_mapping(
    dataset: ProviderDataset,
    start: date,
    end: date,
    asset_ids: tuple[str, ...],
    parameters: tuple[tuple[str, str], ...],
    expected_path: str,
    expected_query: dict[str, str],
) -> None:
    def handler(request: httpx2.Request) -> httpx2.Response:
        assert request.url.path == expected_path
        for key, value in expected_query.items():
            assert request.url.params[key] == value
        return httpx2.Response(200, json={"results": [], "status": "OK"})

    request = ProviderRequest(
        dataset=dataset,
        start=start,
        end=end,
        asset_ids=asset_ids,
        adjusted=False,
        parameters=parameters,
    )

    batches = asyncio.run(_collect(_provider(handler), request))

    assert len(batches) == 1


def test_checkpoint_resumes_at_exact_relative_next_url() -> None:
    def handler(request: httpx2.Request) -> httpx2.Response:
        assert request.url.path.endswith("/1719792000000/2026-06-30")
        assert request.url.params["cursor"] == "resume-here"
        return httpx2.Response(200, json={"results": [], "status": "OK"})

    request = ProviderRequest(
        dataset=ProviderDataset.MINUTE_BARS,
        start=date(2024, 7, 1),
        end=date(2026, 6, 30),
        asset_ids=("AAPL",),
    )
    checkpoint = FetchCheckpoint(
        continuation=(
            "/v2/aggs/ticker/AAPL/range/1/minute/1719792000000/2026-06-30"
            "?cursor=resume-here"
        ),
        next_sequence=4,
    )

    batches = asyncio.run(_collect(_provider(handler), request, checkpoint))

    assert batches[0].sequence == 4


def test_retry_uses_retry_after_without_real_sleep() -> None:
    calls = 0
    delays: list[float] = []

    def handler(request: httpx2.Request) -> httpx2.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx2.Response(429, headers={"Retry-After": "0"})
        return httpx2.Response(200, json={"results": [], "status": "OK"})

    async def fake_sleep(delay: float) -> None:
        delays.append(delay)

    provider = _provider(handler, sleep=fake_sleep, random_float=lambda: 0.0)
    request = ProviderRequest(
        dataset=ProviderDataset.DAILY_BARS,
        start=date(2026, 1, 1),
        end=date(2026, 1, 2),
        asset_ids=("AAPL",),
    )

    batches = asyncio.run(_collect(provider, request))

    assert len(batches) == 1
    assert calls == 2
    assert delays == [0.0]


def test_free_tier_rate_limiter_spaces_paginated_calls_twelve_seconds() -> None:
    calls = 0
    now = 0.0
    delays: list[float] = []

    def handler(request: httpx2.Request) -> httpx2.Response:
        nonlocal calls
        calls += 1
        if calls < 3:
            return httpx2.Response(
                200,
                json={
                    "next_url": f"{BASE_URL}/v2/page?cursor={calls + 1}",
                    "results": [],
                    "status": "OK",
                },
            )
        return httpx2.Response(200, json={"results": [], "status": "OK"})

    async def fake_sleep(delay: float) -> None:
        nonlocal now
        delays.append(delay)
        now += delay

    provider = MassiveProvider(
        TEST_KEY,
        base_url=BASE_URL,
        requests_per_minute=5,
        transport=httpx2.MockTransport(handler),
        sleep=fake_sleep,
        clock=lambda: now,
    )
    request = ProviderRequest(
        dataset=ProviderDataset.MINUTE_BARS,
        start=date(2024, 7, 1),
        end=date(2026, 6, 30),
        asset_ids=("AAPL",),
    )

    batches = asyncio.run(_collect(provider, request))

    assert len(batches) == 3
    assert delays == [12.0, 12.0]


def test_rejects_cross_origin_pagination() -> None:
    def handler(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(
            200,
            json={
                "next_url": "https://example.com/steal?cursor=x",
                "results": [],
                "status": "OK",
            },
        )

    provider = _provider(handler)
    request = ProviderRequest(
        dataset=ProviderDataset.DAILY_BARS,
        start=date(2026, 1, 1),
        end=date(2026, 1, 2),
        asset_ids=("AAPL",),
    )

    with pytest.raises(MassiveResponseError, match="origin"):
        asyncio.run(_collect(provider, request))


def test_configuration_and_errors_never_display_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
    with pytest.raises(MassiveConfigurationError):
        MassiveProvider.from_env(base_url=BASE_URL)

    provider = _provider(lambda request: httpx2.Response(401))
    assert TEST_KEY not in repr(provider)
    request = ProviderRequest(
        dataset=ProviderDataset.DAILY_BARS,
        start=date(2026, 1, 1),
        end=date(2026, 1, 2),
        asset_ids=("AAPL",),
    )

    with pytest.raises(MassiveRequestError) as exc_info:
        asyncio.run(_collect(provider, request))
    assert TEST_KEY not in str(exc_info.value)
