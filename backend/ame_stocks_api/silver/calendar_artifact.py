"""Immutable, content-addressed XNYS session-open calendar artifacts.

The artifact is intentionally self-contained and has no ``latest`` pointer.  Callers
must bind both the exact artifact ID and the SHA-256 of its canonical JSON bytes.
"""

from __future__ import annotations

import hashlib
import json
from bisect import bisect_right
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Final
from zoneinfo import ZoneInfo

import exchange_calendars as xcals
import pandas as pd

from ame_stocks_api.artifacts import (
    safe_relative_path,
    sha256_file,
    stable_digest,
    write_bytes_immutable,
)
from ame_stocks_api.silver.contracts import SilverContractError

CALENDAR_ARTIFACT_SCHEMA_VERSION: Final = 1
CALENDAR_ARTIFACT_RULE_VERSION: Final = "ame-stocks-xnys-market-open-v1"
CALENDAR_NAME: Final = "XNYS"
CALENDAR_TIMEZONE: Final = "UTC"

_NEW_YORK = ZoneInfo("America/New_York")
_DOCUMENT_KEYS = frozenset(
    {
        "artifact_kind",
        "calendar_artifact_id",
        "calendar_name",
        "market_open_timezone",
        "rule_version",
        "schema_version",
        "session_count",
        "sessions",
        "start_session",
        "end_session",
    }
)
_SESSION_KEYS = frozenset({"market_open_utc", "session_date"})


class XNYSCalendarArtifactError(SilverContractError):
    """Raised when a frozen calendar artifact cannot be trusted or reproduced."""


@dataclass(frozen=True, slots=True, order=True)
class XNYSCalendarSession:
    """One official XNYS session and its exact UTC market open."""

    session_date: date
    market_open_utc: datetime

    def __post_init__(self) -> None:
        _native_date(self.session_date, "session_date")
        instant = _aware_utc(self.market_open_utc, "market_open_utc")
        object.__setattr__(self, "market_open_utc", instant)

    def to_dict(self) -> dict[str, str]:
        return {
            "market_open_utc": _format_utc(self.market_open_utc),
            "session_date": self.session_date.isoformat(),
        }


@dataclass(frozen=True, slots=True)
class XNYSCalendarArtifact:
    """Validated immutable bytes plus convenient cutoff-resolution methods."""

    calendar_artifact_id: str
    sha256: str
    content: bytes
    start_session: date
    end_session: date
    sessions: tuple[XNYSCalendarSession, ...]

    def __post_init__(self) -> None:
        _digest(self.calendar_artifact_id, "calendar_artifact_id")
        _digest(self.sha256, "calendar artifact SHA-256")
        if not isinstance(self.content, bytes) or not self.content:
            raise XNYSCalendarArtifactError("calendar artifact content must be non-empty bytes")
        if hashlib.sha256(self.content).hexdigest() != self.sha256:
            raise XNYSCalendarArtifactError("calendar artifact SHA-256 does not match its bytes")
        document = _decode_and_validate_document(self.content)
        if document["calendar_artifact_id"] != self.calendar_artifact_id:
            raise XNYSCalendarArtifactError("calendar artifact object ID differs from its bytes")
        parsed = _sessions_from_document(document)
        if (
            self.start_session != _parse_date(document["start_session"], "start_session")
            or self.end_session != _parse_date(document["end_session"], "end_session")
            or self.sessions != parsed
        ):
            raise XNYSCalendarArtifactError("calendar artifact object differs from its bytes")

    @property
    def relative_path(self) -> str:
        return xnys_calendar_artifact_path(self.calendar_artifact_id)

    @property
    def document(self) -> dict[str, Any]:
        """Return a detached JSON document; mutation cannot alter the frozen artifact."""

        loaded = json.loads(self.content)
        assert isinstance(loaded, dict)  # guaranteed in ``__post_init__``
        return loaded

    def market_open(self, session: date) -> datetime:
        """Return the exact UTC open for one session present in this artifact."""

        _native_date(session, "session")
        index = bisect_right(tuple(item.session_date for item in self.sessions), session) - 1
        if index < 0 or self.sessions[index].session_date != session:
            raise XNYSCalendarArtifactError(
                f"session {session.isoformat()} is absent from the bound XNYS calendar artifact"
            )
        return self.sessions[index].market_open_utc

    def validate_session_open(self, session: date, market_open_utc: datetime) -> datetime:
        """Validate an explicit session/open pair against the frozen artifact."""

        expected = self.market_open(session)
        claimed = _aware_utc(market_open_utc, "market_open_utc")
        if claimed != expected:
            raise XNYSCalendarArtifactError(
                "claimed market open differs from the bound XNYS calendar artifact"
            )
        return expected

    def first_open_after(self, value: datetime) -> tuple[date, datetime]:
        """Return the first bound XNYS open strictly after an aware timestamp.

        Resolution fails closed when the controlling timestamp falls outside the
        artifact's explicit local-date coverage or when the next open would lie past
        ``end_session``.
        """

        instant = _aware_utc(value, "availability timestamp")
        local_date = instant.astimezone(_NEW_YORK).date()
        if local_date < self.start_session or local_date > self.end_session:
            raise XNYSCalendarArtifactError(
                "availability timestamp is outside the bound calendar coverage"
            )
        openings = tuple(item.market_open_utc for item in self.sessions)
        index = bisect_right(openings, instant)
        if index >= len(self.sessions):
            raise XNYSCalendarArtifactError(
                "bound calendar does not contain an XNYS open after the timestamp"
            )
        item = self.sessions[index]
        return item.session_date, item.market_open_utc

    def require_first_open_session(self, value: datetime, claimed: date, *, label: str) -> date:
        """Require ``claimed`` to be the first frozen XNYS open after ``value``."""

        _native_date(claimed, label)
        expected, _ = self.first_open_after(value)
        if claimed != expected:
            raise XNYSCalendarArtifactError(
                f"{label} must equal the first XNYS open after its controlling timestamp"
            )
        return claimed

    def require_session_open_after(self, value: datetime, claimed: date, *, label: str) -> date:
        """Require a bound (possibly conservative later) session open after ``value``."""

        instant = _aware_utc(value, "availability timestamp")
        local_date = instant.astimezone(_NEW_YORK).date()
        if local_date < self.start_session or local_date > self.end_session:
            raise XNYSCalendarArtifactError(
                "availability timestamp is outside the bound calendar coverage"
            )
        opening = self.market_open(claimed)
        if opening <= instant:
            raise XNYSCalendarArtifactError(
                f"{label} open must be strictly after its controlling timestamp"
            )
        return claimed

    def require_timestamp_at_or_after_open(
        self,
        value: datetime,
        session: date,
        *,
        label: str,
    ) -> datetime:
        """Require an aware timestamp to be no earlier than a bound session open."""

        instant = _aware_utc(value, f"{label} timestamp")
        opening = self.market_open(session)
        if instant < opening:
            raise XNYSCalendarArtifactError(
                f"{label} timestamp cannot precede the controlling XNYS session open"
            )
        return instant


def build_xnys_calendar_artifact(
    start_session: date,
    end_session: date,
) -> XNYSCalendarArtifact:
    """Build deterministic canonical bytes for an inclusive XNYS session range."""

    _native_date(start_session, "start_session")
    _native_date(end_session, "end_session")
    if start_session > end_session:
        raise XNYSCalendarArtifactError("start_session cannot follow end_session")
    sessions = _official_sessions(start_session, end_session)
    logical_document: dict[str, Any] = {
        "artifact_kind": "xnys_market_open_calendar",
        "calendar_name": CALENDAR_NAME,
        "end_session": end_session.isoformat(),
        "market_open_timezone": CALENDAR_TIMEZONE,
        "rule_version": CALENDAR_ARTIFACT_RULE_VERSION,
        "schema_version": CALENDAR_ARTIFACT_SCHEMA_VERSION,
        "session_count": len(sessions),
        "sessions": [item.to_dict() for item in sessions],
        "start_session": start_session.isoformat(),
    }
    calendar_artifact_id = stable_digest(logical_document)
    document = {**logical_document, "calendar_artifact_id": calendar_artifact_id}
    content = _canonical_json_bytes(document)
    return XNYSCalendarArtifact(
        calendar_artifact_id=calendar_artifact_id,
        sha256=hashlib.sha256(content).hexdigest(),
        content=content,
        start_session=start_session,
        end_session=end_session,
        sessions=sessions,
    )


def xnys_calendar_artifact_path(calendar_artifact_id: str) -> str:
    """Return the sole canonical path for an exact artifact ID (never ``latest``)."""

    _digest(calendar_artifact_id, "calendar_artifact_id")
    return f"manifests/silver/xnys-calendars/calendar_artifact_id={calendar_artifact_id}.json"


def write_xnys_calendar_artifact(
    root: Path,
    artifact: XNYSCalendarArtifact,
) -> dict[str, object]:
    """Write one artifact idempotently at its immutable content-addressed path."""

    target = root.expanduser().resolve() / artifact.relative_path
    stored = write_bytes_immutable(root, target, artifact.content)
    return {
        **stored,
        "calendar_artifact_id": artifact.calendar_artifact_id,
        "end_session": artifact.end_session.isoformat(),
        "media_type": "application/json",
        "session_count": len(artifact.sessions),
        "start_session": artifact.start_session.isoformat(),
    }


def load_xnys_calendar_artifact(
    root: Path,
    *,
    calendar_artifact_id: str,
    expected_sha256: str,
) -> XNYSCalendarArtifact:
    """Load one explicitly bound artifact and validate bytes, schema, and schedule.

    Both identifiers are mandatory by design.  This loader never scans a directory and
    never resolves a ``latest`` pointer.
    """

    _digest(calendar_artifact_id, "calendar_artifact_id")
    _digest(expected_sha256, "expected calendar artifact SHA-256")
    relative = xnys_calendar_artifact_path(calendar_artifact_id)
    path = safe_relative_path(root, relative)
    if not path.is_file() or path.is_symlink():
        raise XNYSCalendarArtifactError(f"bound XNYS calendar artifact is missing: {relative}")
    actual_sha256 = sha256_file(path)
    if actual_sha256 != expected_sha256:
        raise XNYSCalendarArtifactError("bound XNYS calendar artifact SHA-256 mismatch")
    try:
        content = path.read_bytes()
    except OSError as exc:
        raise XNYSCalendarArtifactError("cannot read bound XNYS calendar artifact") from exc
    document = _decode_and_validate_document(content)
    if document["calendar_artifact_id"] != calendar_artifact_id:
        raise XNYSCalendarArtifactError("calendar artifact path ID differs from its bytes")
    sessions = _sessions_from_document(document)
    return XNYSCalendarArtifact(
        calendar_artifact_id=calendar_artifact_id,
        sha256=actual_sha256,
        content=content,
        start_session=_parse_date(document["start_session"], "start_session"),
        end_session=_parse_date(document["end_session"], "end_session"),
        sessions=sessions,
    )


def _decode_and_validate_document(content: bytes) -> dict[str, Any]:
    try:
        document = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise XNYSCalendarArtifactError("calendar artifact is not valid UTF-8 JSON") from exc
    if not isinstance(document, dict):
        raise XNYSCalendarArtifactError("calendar artifact root must be an object")
    if frozenset(document) != _DOCUMENT_KEYS:
        raise XNYSCalendarArtifactError("calendar artifact document schema is not exact")
    if (
        type(document["schema_version"]) is not int
        or document["schema_version"] != CALENDAR_ARTIFACT_SCHEMA_VERSION
    ):
        raise XNYSCalendarArtifactError("calendar artifact schema version is unsupported")
    if document["rule_version"] != CALENDAR_ARTIFACT_RULE_VERSION:
        raise XNYSCalendarArtifactError("calendar artifact rule version is unsupported")
    if document["artifact_kind"] != "xnys_market_open_calendar":
        raise XNYSCalendarArtifactError("calendar artifact kind is invalid")
    if document["calendar_name"] != CALENDAR_NAME:
        raise XNYSCalendarArtifactError("calendar artifact must bind XNYS")
    if document["market_open_timezone"] != CALENDAR_TIMEZONE:
        raise XNYSCalendarArtifactError("calendar market opens must be encoded in UTC")
    if _canonical_json_bytes(document) != content:
        raise XNYSCalendarArtifactError("calendar artifact JSON bytes are not canonical")
    _digest(document["calendar_artifact_id"], "calendar_artifact_id")
    logical_document = dict(document)
    claimed_id = logical_document.pop("calendar_artifact_id")
    if stable_digest(logical_document) != claimed_id:
        raise XNYSCalendarArtifactError("calendar_artifact_id does not reproduce from its payload")

    start = _parse_date(document["start_session"], "start_session")
    end = _parse_date(document["end_session"], "end_session")
    if start > end:
        raise XNYSCalendarArtifactError("calendar artifact start follows its end")
    rows = document["sessions"]
    if not isinstance(rows, list) or not rows:
        raise XNYSCalendarArtifactError("calendar artifact sessions must be a non-empty array")
    count = document["session_count"]
    if type(count) is not int or count != len(rows):
        raise XNYSCalendarArtifactError("calendar artifact session_count does not reconcile")
    sessions = _sessions_from_rows(rows)
    if sessions[0].session_date != start or sessions[-1].session_date != end:
        raise XNYSCalendarArtifactError("calendar artifact range endpoints do not reconcile")
    expected = _official_sessions(start, end)
    if sessions != expected:
        raise XNYSCalendarArtifactError(
            "calendar artifact sessions/opens differ from the official XNYS schedule"
        )
    return document


def _sessions_from_document(document: Mapping[str, Any]) -> tuple[XNYSCalendarSession, ...]:
    rows = document["sessions"]
    assert isinstance(rows, list)  # validated by ``_decode_and_validate_document``
    return _sessions_from_rows(rows)


def _sessions_from_rows(rows: list[object]) -> tuple[XNYSCalendarSession, ...]:
    parsed: list[XNYSCalendarSession] = []
    for index, row in enumerate(rows):
        if not isinstance(row, dict) or frozenset(row) != _SESSION_KEYS:
            raise XNYSCalendarArtifactError(
                f"calendar session row {index} does not have the exact schema"
            )
        parsed.append(
            XNYSCalendarSession(
                session_date=_parse_date(row["session_date"], f"sessions[{index}].session_date"),
                market_open_utc=_parse_utc(
                    row["market_open_utc"],
                    f"sessions[{index}].market_open_utc",
                ),
            )
        )
    sessions = tuple(parsed)
    if tuple(item.session_date for item in sessions) != tuple(
        sorted(item.session_date for item in sessions)
    ):
        raise XNYSCalendarArtifactError("calendar sessions must be strictly date-ordered")
    if len({item.session_date for item in sessions}) != len(sessions):
        raise XNYSCalendarArtifactError("calendar sessions must be unique")
    return sessions


def _official_sessions(start: date, end: date) -> tuple[XNYSCalendarSession, ...]:
    try:
        calendar = xcals.get_calendar(CALENDAR_NAME)
        start_label = pd.Timestamp(start)
        end_label = pd.Timestamp(end)
        if not calendar.is_session(start_label) or not calendar.is_session(end_label):
            raise XNYSCalendarArtifactError(
                "calendar artifact start_session and end_session must both be XNYS sessions"
            )
        labels = calendar.sessions_in_range(start_label, end_label)
        return tuple(
            XNYSCalendarSession(
                session_date=label.date(),
                market_open_utc=calendar.session_open(label).to_pydatetime().astimezone(UTC),
            )
            for label in labels
        )
    except XNYSCalendarArtifactError:
        raise
    except Exception as exc:  # exchange-calendars exposes multiple exception types
        raise XNYSCalendarArtifactError("cannot reproduce the official XNYS schedule") from exc


def _canonical_json_bytes(document: Mapping[str, Any]) -> bytes:
    try:
        return (
            json.dumps(
                document,
                allow_nan=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
            + b"\n"
        )
    except (TypeError, ValueError) as exc:
        raise XNYSCalendarArtifactError("calendar artifact cannot be encoded canonically") from exc


def _native_date(value: object, label: str) -> date:
    if type(value) is not date:
        raise XNYSCalendarArtifactError(f"{label} must be a native date")
    return value


def _parse_date(value: object, label: str) -> date:
    if not isinstance(value, str):
        raise XNYSCalendarArtifactError(f"{label} must be an ISO date string")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise XNYSCalendarArtifactError(f"{label} must be an ISO date string") from exc
    if parsed.isoformat() != value:
        raise XNYSCalendarArtifactError(f"{label} must be a canonical ISO date")
    return parsed


def _aware_utc(value: object, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise XNYSCalendarArtifactError(f"{label} must be timezone-aware")
    return value.astimezone(UTC)


def _parse_utc(value: object, label: str) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise XNYSCalendarArtifactError(f"{label} must be a canonical UTC timestamp")
    try:
        parsed = datetime.fromisoformat(f"{value[:-1]}+00:00")
    except ValueError as exc:
        raise XNYSCalendarArtifactError(f"{label} must be a canonical UTC timestamp") from exc
    if _format_utc(parsed) != value:
        raise XNYSCalendarArtifactError(f"{label} must be a canonical UTC timestamp")
    return parsed


def _format_utc(value: datetime) -> str:
    return _aware_utc(value, "UTC timestamp").isoformat().replace("+00:00", "Z")


def _digest(value: object, label: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise XNYSCalendarArtifactError(f"{label} must be a lowercase SHA-256 digest")
    return value


__all__ = [
    "CALENDAR_ARTIFACT_RULE_VERSION",
    "CALENDAR_ARTIFACT_SCHEMA_VERSION",
    "CALENDAR_NAME",
    "CALENDAR_TIMEZONE",
    "XNYSCalendarArtifact",
    "XNYSCalendarArtifactError",
    "XNYSCalendarSession",
    "build_xnys_calendar_artifact",
    "load_xnys_calendar_artifact",
    "write_xnys_calendar_artifact",
    "xnys_calendar_artifact_path",
]
