"""Shared, explicit history-window rules for command-line tools."""

from __future__ import annotations

import argparse
from datetime import date

DEFAULT_HISTORY_YEARS = 10


def add_history_range_arguments(parser: argparse.ArgumentParser) -> None:
    """Add a required end date and either an explicit or derived start date."""

    parser.add_argument(
        "--start",
        type=parse_date,
        help="inclusive start date; overrides the default ten-year lookback",
    )
    parser.add_argument(
        "--end",
        type=parse_date,
        required=True,
        help="inclusive end date",
    )
    parser.add_argument(
        "--years",
        type=int,
        help=(
            "calendar years to look back from --end when --start is omitted "
            f"(default: {DEFAULT_HISTORY_YEARS})"
        ),
    )


def resolve_history_range(
    *,
    start: date | None,
    end: date,
    years: int | None,
) -> tuple[date, date]:
    """Resolve an inclusive range without hiding an explicitly supplied start."""

    if start is not None and years is not None:
        raise ValueError("--start and --years are mutually exclusive")
    if start is not None:
        if start > end:
            raise ValueError("start must be on or before end")
        return start, end

    lookback_years = DEFAULT_HISTORY_YEARS if years is None else years
    if lookback_years <= 0:
        raise ValueError("--years must be positive")

    target_year = end.year - lookback_years
    try:
        resolved_start = end.replace(year=target_year)
    except ValueError:
        # February 29 has no same-day counterpart in a non-leap target year.
        resolved_start = end.replace(year=target_year, day=28)
    return resolved_start, end


def parse_date(raw: str) -> date:
    try:
        return date.fromisoformat(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("dates must use YYYY-MM-DD") from exc
