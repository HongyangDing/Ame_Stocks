"""Shared point-in-time availability rules for reviewed Silver controls."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import exchange_calendars as xcals
import pandas as pd

from ame_stocks_api.silver.contracts import SilverContractError

_NEW_YORK = ZoneInfo("America/New_York")


class SilverAvailabilityError(SilverContractError):
    """Raised when a claimed availability session is not calendar-reproducible."""


def first_xnys_open_after(value: datetime) -> tuple[date, datetime]:
    """Return the first official XNYS open strictly after a UTC-aware timestamp."""

    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise SilverAvailabilityError("availability timestamp must be timezone-aware")
    instant = value.astimezone(UTC)
    local_date = instant.astimezone(_NEW_YORK).date()
    try:
        calendar = xcals.get_calendar("XNYS")
        sessions = calendar.sessions_in_range(
            (local_date - timedelta(days=1)).isoformat(),
            (local_date + timedelta(days=14)).isoformat(),
        )
    except Exception as exc:  # exchange-calendars exposes multiple exception types
        raise SilverAvailabilityError("cannot load the XNYS exchange calendar") from exc
    for session in sessions:
        opening = calendar.session_open(session)
        if opening > pd.Timestamp(instant):
            return session.date(), opening.to_pydatetime().astimezone(UTC)
    raise SilverAvailabilityError("cannot find an XNYS open after the timestamp")


def require_first_xnys_open_session(value: datetime, claimed: date, *, label: str) -> date:
    """Require a claimed date to equal the first XNYS open after ``value``."""

    if type(claimed) is not date:
        raise SilverAvailabilityError(f"{label} must be a native date")
    expected, _ = first_xnys_open_after(value)
    if claimed != expected:
        raise SilverAvailabilityError(
            f"{label} must equal the first XNYS open after its controlling timestamp"
        )
    return claimed


def require_xnys_session_after(
    value: datetime,
    claimed: date,
    *,
    label: str,
) -> date:
    """Require a claimed XNYS session whose open is strictly after ``value``.

    This permits conservative later evidence availability while rejecting weekends,
    holidays, same-session look-ahead and non-session dates.
    """

    if type(claimed) is not date:
        raise SilverAvailabilityError(f"{label} must be a native date")
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise SilverAvailabilityError("availability timestamp must be timezone-aware")
    instant = value.astimezone(UTC)
    try:
        calendar = xcals.get_calendar("XNYS")
        session = pd.Timestamp(claimed)
        if not calendar.is_session(session):
            raise SilverAvailabilityError(f"{label} must be an XNYS session")
        opening = calendar.session_open(session)
    except SilverAvailabilityError:
        raise
    except Exception as exc:
        raise SilverAvailabilityError(f"cannot validate {label} on XNYS") from exc
    if opening <= pd.Timestamp(instant):
        raise SilverAvailabilityError(
            f"{label} open must be strictly after its controlling timestamp"
        )
    return claimed


def require_timestamp_at_or_after_xnys_open(
    value: datetime,
    session: date,
    *,
    label: str,
) -> datetime:
    """Require a timestamp to be no earlier than one exact XNYS session open."""

    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise SilverAvailabilityError(f"{label} timestamp must be timezone-aware")
    if type(session) is not date:
        raise SilverAvailabilityError(f"{label} session must be a native date")
    instant = value.astimezone(UTC)
    try:
        calendar = xcals.get_calendar("XNYS")
        session_label = pd.Timestamp(session)
        if not calendar.is_session(session_label):
            raise SilverAvailabilityError(f"{label} session must be an XNYS session")
        opening = calendar.session_open(session_label)
    except SilverAvailabilityError:
        raise
    except Exception as exc:
        raise SilverAvailabilityError(f"cannot validate {label} on XNYS") from exc
    if pd.Timestamp(instant) < opening:
        raise SilverAvailabilityError(
            f"{label} timestamp cannot precede the controlling XNYS session open"
        )
    return instant


__all__ = [
    "SilverAvailabilityError",
    "first_xnys_open_after",
    "require_first_xnys_open_session",
    "require_timestamp_at_or_after_xnys_open",
    "require_xnys_session_after",
]
