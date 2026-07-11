import asyncio
import json
from datetime import date

import pytest

from ame_stocks_api.providers import MockProvider
from ame_stocks_core import DataProvider, FetchCheckpoint, ProviderDataset, ProviderRequest


async def _fetch_once(provider: MockProvider, request: ProviderRequest):
    return [batch async for batch in provider.fetch(request)]


def test_mock_provider_implements_contract_and_is_deterministic() -> None:
    provider = MockProvider()
    request = ProviderRequest(
        dataset=ProviderDataset.MINUTE_BARS,
        start=date(2026, 1, 2),
        end=date(2026, 1, 2),
        asset_ids=("AAPL",),
        adjusted=False,
    )

    first = asyncio.run(_fetch_once(provider, request))
    second = asyncio.run(_fetch_once(provider, request))

    assert isinstance(provider, DataProvider)
    assert first == second
    assert len(first) == 1
    assert first[0].request_id == request.request_id
    assert first[0].dataset is ProviderDataset.MINUTE_BARS
    assert first[0].sha256 == second[0].sha256
    assert json.loads(first[0].payload)["results"] == []


def test_request_id_is_order_independent_for_assets_and_parameters() -> None:
    left = ProviderRequest(
        dataset=ProviderDataset.ASSETS,
        start=date(2026, 1, 2),
        end=date(2026, 1, 2),
        asset_ids=("MSFT", "AAPL"),
        parameters=(("limit", "100"), ("active", "true")),
    )
    right = ProviderRequest(
        dataset=ProviderDataset.ASSETS,
        start=date(2026, 1, 2),
        end=date(2026, 1, 2),
        asset_ids=("AAPL", "MSFT"),
        parameters=(("active", "true"), ("limit", "100")),
    )

    assert left.request_id == right.request_id


def test_provider_request_rejects_invalid_ranges() -> None:
    with pytest.raises(ValueError, match="start"):
        ProviderRequest(
            dataset=ProviderDataset.SPLITS,
            start=date(2026, 1, 3),
            end=date(2026, 1, 2),
        )


def test_mock_provider_accepts_resume_checkpoint() -> None:
    provider = MockProvider()
    request = ProviderRequest(
        dataset=ProviderDataset.DAILY_BARS,
        start=date(2026, 1, 2),
        end=date(2026, 1, 3),
        asset_ids=("AAPL",),
    )

    batches = asyncio.run(
        _fetch_once_with_checkpoint(
            provider,
            request,
            FetchCheckpoint(continuation="/mock?cursor=next", next_sequence=3),
        )
    )

    assert batches[0].sequence == 3


async def _fetch_once_with_checkpoint(
    provider: MockProvider,
    request: ProviderRequest,
    checkpoint: FetchCheckpoint,
):
    return [batch async for batch in provider.fetch(request, checkpoint=checkpoint)]
