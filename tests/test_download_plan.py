from datetime import date

import pytest

from ame_stocks_api.downloads import build_download_plan
from ame_stocks_core import ProviderDataset


def test_bar_plan_uses_one_full_range_request_per_ticker() -> None:
    plan = build_download_plan(
        dataset=ProviderDataset.MINUTE_BARS,
        start=date(2024, 7, 1),
        end=date(2026, 6, 30),
        tickers=("msft", "AAPL", "AAPL"),
    )

    assert len(plan.requests) == 2
    assert [request.asset_ids for request in plan.requests] == [("AAPL",), ("MSFT",)]
    assert all(request.start == date(2024, 7, 1) for request in plan.requests)
    assert all(request.end == date(2026, 6, 30) for request in plan.requests)
    assert all(request.adjusted is False for request in plan.requests)


def test_full_market_corporate_actions_use_one_request_stream() -> None:
    for dataset in (ProviderDataset.SPLITS, ProviderDataset.DIVIDENDS):
        plan = build_download_plan(
            dataset=dataset,
            start=date(2024, 7, 1),
            end=date(2026, 6, 30),
        )
        assert len(plan.requests) == 1
        assert plan.requests[0].asset_ids == ()


def test_assets_plan_captures_active_and_inactive_point_in_time() -> None:
    plan = build_download_plan(
        dataset=ProviderDataset.ASSETS,
        start=date(2026, 6, 30),
        end=date(2026, 6, 30),
    )

    assert [dict(request.parameters)["active"] for request in plan.requests] == [
        "true",
        "false",
    ]


def test_minute_plan_requires_tickers() -> None:
    with pytest.raises(ValueError, match="ticker"):
        build_download_plan(
            dataset=ProviderDataset.MINUTE_BARS,
            start=date(2024, 7, 1),
            end=date(2026, 6, 30),
        )
