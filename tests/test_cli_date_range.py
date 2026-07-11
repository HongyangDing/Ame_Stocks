from datetime import date

import pytest

from ame_stocks_api.cli.date_range import DEFAULT_HISTORY_YEARS, resolve_history_range


def test_default_history_range_is_ten_calendar_years() -> None:
    start, end = resolve_history_range(
        start=None,
        end=date(2026, 6, 30),
        years=None,
    )

    assert DEFAULT_HISTORY_YEARS == 10
    assert start == date(2016, 6, 30)
    assert end == date(2026, 6, 30)


def test_explicit_start_keeps_short_review_window() -> None:
    start, end = resolve_history_range(
        start=date(2026, 6, 30),
        end=date(2026, 6, 30),
        years=None,
    )

    assert (start, end) == (date(2026, 6, 30), date(2026, 6, 30))


def test_february_29_lookback_uses_february_28() -> None:
    start, _ = resolve_history_range(
        start=None,
        end=date(2024, 2, 29),
        years=5,
    )

    assert start == date(2019, 2, 28)


def test_explicit_start_and_years_are_mutually_exclusive() -> None:
    with pytest.raises(ValueError, match="mutually exclusive"):
        resolve_history_range(
            start=date(2026, 6, 30),
            end=date(2026, 6, 30),
            years=5,
        )


def test_history_years_must_be_positive() -> None:
    with pytest.raises(ValueError, match="positive"):
        resolve_history_range(
            start=None,
            end=date(2026, 6, 30),
            years=0,
        )
