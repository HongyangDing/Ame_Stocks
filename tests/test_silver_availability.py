from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from ame_stocks_api.silver.availability import (
    SilverAvailabilityError,
    first_xnys_open_after,
    require_first_xnys_open_session,
    require_timestamp_at_or_after_xnys_open,
    require_xnys_session_after,
)


def test_first_open_handles_premarket_after_close_weekend_and_holiday() -> None:
    assert first_xnys_open_after(datetime(2024, 1, 12, 14, 0, tzinfo=UTC))[0] == date(2024, 1, 12)
    assert first_xnys_open_after(datetime(2024, 1, 12, 22, 0, tzinfo=UTC))[0] == date(2024, 1, 16)
    assert first_xnys_open_after(datetime(2024, 1, 15, 14, 0, tzinfo=UTC))[0] == date(2024, 1, 16)


def test_exact_and_conservative_session_gates() -> None:
    captured = datetime(2024, 1, 5, 14, 2, tzinfo=UTC)
    assert require_first_xnys_open_session(
        captured,
        date(2024, 1, 5),
        label="approval availability",
    ) == date(2024, 1, 5)
    assert require_xnys_session_after(
        captured,
        date(2024, 1, 8),
        label="evidence availability",
    ) == date(2024, 1, 8)
    with pytest.raises(SilverAvailabilityError, match="first XNYS open"):
        require_first_xnys_open_session(
            captured,
            date(2024, 1, 8),
            label="approval availability",
        )
    with pytest.raises(SilverAvailabilityError, match="XNYS session"):
        require_xnys_session_after(
            captured,
            date(2024, 1, 6),
            label="evidence availability",
        )


def test_timestamp_must_not_precede_controlling_session_open() -> None:
    assert require_timestamp_at_or_after_xnys_open(
        datetime(2024, 1, 8, 15, 0, tzinfo=UTC),
        date(2024, 1, 8),
        label="decision plan evidence availability",
    ) == datetime(2024, 1, 8, 15, 0, tzinfo=UTC)
    with pytest.raises(SilverAvailabilityError, match="cannot precede"):
        require_timestamp_at_or_after_xnys_open(
            datetime(2024, 1, 8, 14, 0, tzinfo=UTC),
            date(2024, 1, 8),
            label="decision plan evidence availability",
        )
