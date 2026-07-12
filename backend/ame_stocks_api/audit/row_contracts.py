"""Shared semantic contracts for REST Bronze rows."""

from __future__ import annotations

import math
from datetime import UTC, date, datetime
from typing import Any


def epoch_millisecond_date(value: object) -> str | None:
    """Return the UTC date for a non-negative integer Unix-millisecond timestamp."""

    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    try:
        return datetime.fromtimestamp(value / 1000, tz=UTC).date().isoformat()
    except (OSError, OverflowError, ValueError):
        return None


def valid_daily_bar(row: dict[str, Any]) -> bool:
    """Validate Massive's raw grouped-daily compact-key response contract."""

    ticker = row.get("T")
    if not isinstance(ticker, str) or not ticker.strip() or ticker != ticker.strip():
        return False
    if epoch_millisecond_date(row.get("t")) is None:
        return False
    values = {field: row.get(field) for field in ("o", "h", "l", "c", "v", "vw")}
    if any(not _finite_nonnegative_number(value) for value in values.values()):
        return False
    open_price = float(values["o"])
    high_price = float(values["h"])
    low_price = float(values["l"])
    close_price = float(values["c"])
    return high_price >= max(open_price, close_price, low_price) and low_price <= min(
        open_price, close_price, high_price
    )


def valid_legacy_financials(row: dict[str, Any]) -> bool:
    """Validate fields needed for point-in-time use of the legacy financials endpoint."""

    filing_date = _iso_date(row.get("filing_date"))
    end_date = _iso_date(row.get("end_date"))
    if filing_date is None or end_date is None or end_date > filing_date:
        return False
    cik = row.get("cik")
    if (
        not isinstance(cik, str)
        or cik != cik.strip()
        or not cik.isdigit()
        or not 1 <= len(cik) <= 10
    ):
        return False
    timeframe = row.get("timeframe")
    if (
        not isinstance(timeframe, str)
        or not timeframe.strip()
        or timeframe != timeframe.strip()
    ):
        return False
    financials = row.get("financials")
    if not isinstance(financials, dict) or not financials:
        return False
    acceptance_datetime = row.get("acceptance_datetime")
    if acceptance_datetime is not None and not _aware_iso_datetime(acceptance_datetime):
        return False
    for field_name in ("source_filing_file_url", "source_filing_url"):
        value = row.get(field_name)
        if value is not None and (
            not isinstance(value, str) or not value.strip() or value != value.strip()
        ):
            return False
    return True


def _finite_nonnegative_number(value: object) -> bool:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    if isinstance(value, int):
        return value >= 0
    return math.isfinite(value) and value >= 0


def _iso_date(value: object) -> str | None:
    if not isinstance(value, str) or len(value) != 10:
        return None
    try:
        return date.fromisoformat(value).isoformat()
    except ValueError:
        return None


def _aware_iso_datetime(value: object) -> bool:
    if not isinstance(value, str) or len(value) < 12 or value[10] not in {"T", " "}:
        return False
    candidate = f"{value[:-1]}+00:00" if value.endswith(("Z", "z")) else value
    try:
        observed = datetime.fromisoformat(candidate)
    except ValueError:
        return False
    return observed.tzinfo is not None and observed.utcoffset() is not None


__all__ = ["epoch_millisecond_date", "valid_daily_bar", "valid_legacy_financials"]
