from datetime import date

import pytest

from ame_stocks_api.downloads import build_download_plan
from ame_stocks_core import ProviderDataset


def test_minute_plan_uses_one_full_range_request_per_ticker() -> None:
    plan = build_download_plan(
        dataset=ProviderDataset.MINUTE_BARS,
        start=date(2024, 7, 1),
        end=date(2026, 6, 30),
        tickers=("BCpC", "AAPL", "AAPL"),
    )

    assert len(plan.requests) == 2
    assert [request.asset_ids for request in plan.requests] == [("AAPL",), ("BCpC",)]
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


def test_history_assets_plan_uses_daily_active_and_one_final_inactive_snapshot() -> None:
    plan = build_download_plan(
        dataset=ProviderDataset.ASSETS,
        start=date(2026, 6, 29),
        end=date(2026, 6, 30),
        active="history",
    )

    assert [(request.start, dict(request.parameters)["active"]) for request in plan.requests] == [
        (date(2026, 6, 29), "true"),
        (date(2026, 6, 30), "true"),
        (date(2026, 6, 30), "false"),
    ]


def test_default_assets_plan_captures_active_and_inactive_every_session() -> None:
    plan = build_download_plan(
        dataset=ProviderDataset.ASSETS,
        start=date(2026, 6, 29),
        end=date(2026, 6, 30),
    )

    assert [(request.start, dict(request.parameters)["active"]) for request in plan.requests] == [
        (date(2026, 6, 29), "true"),
        (date(2026, 6, 29), "false"),
        (date(2026, 6, 30), "true"),
        (date(2026, 6, 30), "false"),
    ]


def test_daily_plan_uses_one_full_market_request_per_exchange_session() -> None:
    plan = build_download_plan(
        dataset=ProviderDataset.DAILY_BARS,
        start=date(2026, 6, 29),
        end=date(2026, 7, 3),
    )

    # July 3, 2026 is the observed Independence Day market holiday.
    assert [request.start for request in plan.requests] == [
        date(2026, 6, 29),
        date(2026, 6, 30),
        date(2026, 7, 1),
        date(2026, 7, 2),
    ]
    assert all(request.start == request.end for request in plan.requests)
    assert all(request.asset_ids == () for request in plan.requests)


def test_daily_plan_rejects_ticker_filters() -> None:
    with pytest.raises(ValueError, match="full-market"):
        build_download_plan(
            dataset=ProviderDataset.DAILY_BARS,
            start=date(2026, 6, 30),
            end=date(2026, 6, 30),
            tickers=("AAPL",),
        )


def test_minute_plan_requires_tickers() -> None:
    with pytest.raises(ValueError, match="ticker"):
        build_download_plan(
            dataset=ProviderDataset.MINUTE_BARS,
            start=date(2024, 7, 1),
            end=date(2026, 6, 30),
        )


@pytest.mark.parametrize(
    "dataset",
    [
        ProviderDataset.SHORT_INTEREST,
        ProviderDataset.SHORT_VOLUME,
        ProviderDataset.IPOS,
        ProviderDataset.EDGAR_INDEX,
        ProviderDataset.FORM_3,
        ProviderDataset.FORM_4,
        ProviderDataset.LEGACY_FINANCIALS,
        ProviderDataset.INCOME_STATEMENTS,
        ProviderDataset.BALANCE_SHEETS,
        ProviderDataset.CASH_FLOW_STATEMENTS,
        ProviderDataset.RISK_FACTORS,
        ProviderDataset.TEN_K_SECTIONS,
        ProviderDataset.EIGHT_K_TEXT,
        ProviderDataset.EIGHT_K_DISCLOSURES,
        ProviderDataset.NEWS,
    ],
)
def test_bulk_research_plans_use_chronological_calendar_year_chunks(dataset) -> None:
    plan = build_download_plan(
        dataset=dataset,
        start=date(2016, 7, 11),
        end=date(2018, 2, 3),
    )

    assert [(request.start, request.end) for request in plan.requests] == [
        (date(2016, 7, 11), date(2016, 12, 31)),
        (date(2017, 1, 1), date(2017, 12, 31)),
        (date(2018, 1, 1), date(2018, 2, 3)),
    ]


@pytest.mark.parametrize(
    "dataset",
    [
        ProviderDataset.FLOAT,
        ProviderDataset.TICKER_TYPES,
        ProviderDataset.EXCHANGES,
        ProviderDataset.CONDITION_CODES,
        ProviderDataset.RATIOS,
        ProviderDataset.RISK_TAXONOMY,
        ProviderDataset.DISCLOSURE_TAXONOMY,
    ],
)
def test_latest_snapshots_require_one_capture_date(dataset) -> None:
    with pytest.raises(ValueError, match="latest-only"):
        build_download_plan(
            dataset=dataset,
            start=date(2026, 7, 8),
            end=date(2026, 7, 9),
        )

    plan = build_download_plan(
        dataset=dataset,
        start=date(2026, 7, 9),
        end=date(2026, 7, 9),
    )
    assert len(plan.requests) == 1


def test_condition_codes_plan_rejects_ticker_filters() -> None:
    with pytest.raises(ValueError, match="does not accept ticker filters"):
        build_download_plan(
            dataset=ProviderDataset.CONDITION_CODES,
            start=date(2026, 7, 12),
            end=date(2026, 7, 12),
            tickers=("AAPL",),
        )


@pytest.mark.parametrize(
    "dataset",
    [
        ProviderDataset.INCOME_STATEMENTS,
        ProviderDataset.BALANCE_SHEETS,
        ProviderDataset.CASH_FLOW_STATEMENTS,
        ProviderDataset.LEGACY_FINANCIALS,
        ProviderDataset.RATIOS,
    ],
)
def test_fundamentals_plans_reject_ticker_filters(dataset) -> None:
    with pytest.raises(ValueError, match=r"ticker|full-market"):
        build_download_plan(
            dataset=dataset,
            start=date(2026, 7, 12),
            end=date(2026, 7, 12),
            tickers=("AAPL",),
        )


def test_ratios_plan_rejects_history_and_uses_one_latest_full_market_snapshot() -> None:
    with pytest.raises(ValueError, match="latest-only"):
        build_download_plan(
            dataset=ProviderDataset.RATIOS,
            start=date(2026, 7, 11),
            end=date(2026, 7, 12),
        )

    plan = build_download_plan(
        dataset=ProviderDataset.RATIOS,
        start=date(2026, 7, 12),
        end=date(2026, 7, 12),
    )

    assert len(plan.requests) == 1
    assert plan.requests[0].asset_ids == ()


def test_legacy_financials_plan_uses_full_market_filing_year_chunks() -> None:
    plan = build_download_plan(
        dataset=ProviderDataset.LEGACY_FINANCIALS,
        start=date(2009, 3, 29),
        end=date(2010, 2, 1),
    )

    assert [(request.start, request.end, request.asset_ids) for request in plan.requests] == [
        (date(2009, 3, 29), date(2009, 12, 31), ()),
        (date(2010, 1, 1), date(2010, 2, 1), ()),
    ]


def test_ticker_events_require_identifiers_and_preserve_exact_case() -> None:
    with pytest.raises(ValueError, match="CUSIP"):
        build_download_plan(
            dataset=ProviderDataset.TICKER_EVENTS,
            start=date(2003, 9, 10),
            end=date(2026, 7, 9),
        )

    plan = build_download_plan(
        dataset=ProviderDataset.TICKER_EVENTS,
        start=date(2003, 9, 10),
        end=date(2026, 7, 9),
        tickers=("BCpC", "BBG000B9XRY4", "BCpC"),
    )
    assert [request.asset_ids[0] for request in plan.requests] == ["BBG000B9XRY4", "BCpC"]
    assert all(dict(request.parameters) == {"types": "ticker_change"} for request in plan.requests)


def test_ticker_overview_uses_one_request_per_lifecycle_date() -> None:
    plan = build_download_plan(
        dataset=ProviderDataset.TICKER_OVERVIEW,
        start=date(2016, 7, 11),
        end=date(2026, 7, 9),
        ticker_dates=(
            ("BCpC", date(2018, 2, 1)),
            ("AAPL", date(2026, 7, 9)),
            ("BCpC", date(2018, 2, 1)),
        ),
    )

    assert [(request.asset_ids[0], request.start) for request in plan.requests] == [
        ("BCpC", date(2018, 2, 1)),
        ("AAPL", date(2026, 7, 9)),
    ]
    assert all(request.start == request.end for request in plan.requests)


def test_ticker_overview_rejects_ambiguous_or_out_of_window_inputs() -> None:
    with pytest.raises(ValueError, match="cannot combine"):
        build_download_plan(
            dataset=ProviderDataset.TICKER_OVERVIEW,
            start=date(2026, 7, 9),
            end=date(2026, 7, 9),
            tickers=("AAPL",),
            ticker_dates=(("AAPL", date(2026, 7, 9)),),
        )
    with pytest.raises(ValueError, match="outside"):
        build_download_plan(
            dataset=ProviderDataset.TICKER_OVERVIEW,
            start=date(2026, 7, 9),
            end=date(2026, 7, 9),
            ticker_dates=(("AAPL", date(2026, 7, 8)),),
        )
    with pytest.raises(ValueError, match="only supported"):
        build_download_plan(
            dataset=ProviderDataset.ASSETS,
            start=date(2026, 7, 9),
            end=date(2026, 7, 9),
            ticker_dates=(("AAPL", date(2026, 7, 9)),),
        )


def test_13f_rejects_ticker_filter() -> None:
    with pytest.raises(ValueError, match="full-market"):
        build_download_plan(
            dataset=ProviderDataset.FORM_13F,
            start=date(2026, 1, 1),
            end=date(2026, 7, 9),
            tickers=("AAPL",),
        )


def test_13f_uses_chronological_calendar_quarter_chunks() -> None:
    plan = build_download_plan(
        dataset=ProviderDataset.FORM_13F,
        start=date(2016, 7, 11),
        end=date(2017, 2, 3),
    )

    assert [(request.start, request.end) for request in plan.requests] == [
        (date(2016, 7, 11), date(2016, 9, 30)),
        (date(2016, 10, 1), date(2016, 12, 31)),
        (date(2017, 1, 1), date(2017, 2, 3)),
    ]


@pytest.mark.parametrize(
    "dataset",
    [
        ProviderDataset.TREASURY_YIELDS,
        ProviderDataset.INFLATION,
        ProviderDataset.INFLATION_EXPECTATIONS,
        ProviderDataset.LABOR_MARKET,
    ],
)
def test_macro_history_uses_one_full_range_stream(dataset) -> None:
    plan = build_download_plan(
        dataset=dataset,
        start=date(1947, 1, 1),
        end=date(2026, 7, 9),
    )

    assert len(plan.requests) == 1
    assert plan.requests[0].start == date(1947, 1, 1)
    assert plan.requests[0].end == date(2026, 7, 9)
    assert plan.requests[0].asset_ids == ()
