"""Bounded-memory primitives for an S7 identity-bounce preview.

The pure engine intentionally does **not** open the official six-release source bundle and
does not write the canonical identity-case candidate manifest.  Callers feed one already-
scoped source session at a time, while each ticker retains only a boundary plus at most 20
source-record IDs.

The production-shaped entry point delegates to a source-bound runner that accepts only exact
plan/approval IDs and checksums.  It cannot accept a caller-provided bundle, observations, or
corroboration, and its terminal output remains review-only.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field, replace
from datetime import date
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Final

from ame_stocks_api.artifacts import (
    ArtifactError,
    safe_relative_path,
    sha256_file,
    stable_digest,
    write_bytes_immutable,
)
from ame_stocks_api.silver.identity_bounce import (
    CASE_ID_RULE_VERSION,
    DETECTOR_RULE_VERSION,
    MAX_MIDDLE_SESSIONS,
    BounceCase,
    IdentityBounceError,
    IdentityObservation,
    SourceSession,
)

if TYPE_CHECKING:
    from ame_stocks_api.silver.identity_preview_runner import S7DetectorPreviewCompletion

BOUNDED_PREVIEW_RULE_VERSION: Final = "s7_identity_bounce_bounded_preview_v1"
BOUNDED_PREVIEW_ARTIFACT_RULE_VERSION: Final = "s7_identity_bounce_bounded_preview_artifact_v1"

HARD_MAX_PREVIEW_SESSIONS: Final = 25
HARD_MAX_PREVIEW_TICKERS: Final = 250
HARD_MAX_PREVIEW_SELECTED_ROWS: Final = 6_250
HARD_MAX_PREVIEW_SCANNED_ROWS: Final = 2_100_000
HARD_MAX_PREVIEW_ARTIFACTS: Final = 80
HARD_MAX_PREVIEW_BYTES: Final = 512 * 1024 * 1024
HARD_MAX_PREVIEW_CASES: Final = 2_500

_FIGI = re.compile(r"^BBG[0-9A-Z]{9}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SESSION_BANDS = ("1", "2-5", "6-20")
_NOT_EVALUATED_REASON_CODES = tuple(
    sorted(
        {
            "hierarchy_support_not_evaluated",
            "s5_and_s6_support_not_evaluated",
            "s5_ticker_change_event_support_not_evaluated",
            "s6_overview_identity_support_not_evaluated",
        }
    )
)


class IdentityStreamingPreviewError(IdentityBounceError):
    """Raised when a bounded preview is unsafe, over limit, or not source-authorized."""


@dataclass(frozen=True, slots=True)
class BoundedIdentityPreviewLimits:
    """Per-run limits that may only tighten the repository hard ceilings."""

    max_sessions: int = HARD_MAX_PREVIEW_SESSIONS
    max_tickers: int = HARD_MAX_PREVIEW_TICKERS
    max_selected_rows: int = HARD_MAX_PREVIEW_SELECTED_ROWS
    max_scanned_rows: int = HARD_MAX_PREVIEW_SCANNED_ROWS
    max_artifacts: int = HARD_MAX_PREVIEW_ARTIFACTS
    max_bytes: int = HARD_MAX_PREVIEW_BYTES
    max_cases: int = HARD_MAX_PREVIEW_CASES

    def __post_init__(self) -> None:
        for label, value, ceiling in (
            ("max_sessions", self.max_sessions, HARD_MAX_PREVIEW_SESSIONS),
            ("max_tickers", self.max_tickers, HARD_MAX_PREVIEW_TICKERS),
            ("max_selected_rows", self.max_selected_rows, HARD_MAX_PREVIEW_SELECTED_ROWS),
            ("max_scanned_rows", self.max_scanned_rows, HARD_MAX_PREVIEW_SCANNED_ROWS),
            ("max_artifacts", self.max_artifacts, HARD_MAX_PREVIEW_ARTIFACTS),
            ("max_bytes", self.max_bytes, HARD_MAX_PREVIEW_BYTES),
            ("max_cases", self.max_cases, HARD_MAX_PREVIEW_CASES),
        ):
            _positive_int(value, label)
            if value > ceiling:
                raise IdentityStreamingPreviewError(
                    f"bounded preview {label} exceeds its repository hard ceiling"
                )

    def to_dict(self) -> dict[str, int]:
        return {
            "max_artifacts": self.max_artifacts,
            "max_bytes": self.max_bytes,
            "max_cases": self.max_cases,
            "max_scanned_rows": self.max_scanned_rows,
            "max_selected_rows": self.max_selected_rows,
            "max_sessions": self.max_sessions,
            "max_tickers": self.max_tickers,
        }


@dataclass(frozen=True, slots=True)
class BoundedIdentityPreviewResult:
    """Final bounded result; it is review-only and never adjudication eligible."""

    six_release_binding_id: str
    preview_manifest_available_session: date
    scoped_tickers: tuple[str, ...]
    limits: BoundedIdentityPreviewLimits
    source_session_count: int
    incomplete_source_session_count: int
    selected_observation_count: int
    valid_active_observation_count: int
    scanned_row_count: int
    scanned_artifact_count: int
    scanned_bytes: int
    cases: tuple[BounceCase, ...]
    session_band_counts: Mapping[str, int]
    support_reason_counts: Mapping[str, int]
    scope_kind: str = field(default="bounded_preview", init=False)
    status: str = field(default="awaiting_review", init=False)
    adjudication_eligible: bool = field(default=False, init=False)
    source_attested: bool = field(default=False, init=False)
    canonical_candidate_eligible: bool = field(default=False, init=False)
    corroboration_evaluation_state: str = field(default="not_evaluated", init=False)
    support_absence_verified: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        _digest(self.six_release_binding_id, "six_release_binding_id")
        _native_date(
            self.preview_manifest_available_session,
            "preview_manifest_available_session",
        )
        _validate_scoped_tickers(self.scoped_tickers, self.limits)
        for label, value in (
            ("source_session_count", self.source_session_count),
            ("incomplete_source_session_count", self.incomplete_source_session_count),
            ("selected_observation_count", self.selected_observation_count),
            ("valid_active_observation_count", self.valid_active_observation_count),
            ("scanned_row_count", self.scanned_row_count),
            ("scanned_artifact_count", self.scanned_artifact_count),
            ("scanned_bytes", self.scanned_bytes),
        ):
            _nonnegative_int(value, label)
        if (
            self.source_session_count == 0
            or self.source_session_count > self.limits.max_sessions
            or self.incomplete_source_session_count > self.source_session_count
            or self.selected_observation_count > self.limits.max_selected_rows
            or self.valid_active_observation_count > self.selected_observation_count
            or self.scanned_row_count > self.limits.max_scanned_rows
            or self.selected_observation_count > self.scanned_row_count
            or self.scanned_artifact_count > self.limits.max_artifacts
            or self.scanned_bytes > self.limits.max_bytes
            or len(self.cases) > self.limits.max_cases
        ):
            raise IdentityStreamingPreviewError("bounded preview result exceeds its frozen limits")
        if tuple(case.identity_case_id for case in self.cases) != tuple(
            sorted(case.identity_case_id for case in self.cases)
        ):
            raise IdentityStreamingPreviewError(
                "bounded preview cases are not deterministically sorted"
            )
        expected_bands = Counter(case.session_band for case in self.cases)
        if dict(self.session_band_counts) != {
            band: expected_bands[band] for band in _SESSION_BANDS
        }:
            raise IdentityStreamingPreviewError("bounded preview session-band counts drifted")
        expected_reasons = Counter(reason for case in self.cases for reason in case.reason_codes)
        if dict(self.support_reason_counts) != dict(sorted(expected_reasons.items())):
            raise IdentityStreamingPreviewError("bounded preview support-reason counts drifted")
        if (
            self.scope_kind != "bounded_preview"
            or self.status != "awaiting_review"
            or self.adjudication_eligible is not False
            or self.source_attested is not False
            or self.canonical_candidate_eligible is not False
            or self.corroboration_evaluation_state != "not_evaluated"
            or self.support_absence_verified is not False
        ):
            raise IdentityStreamingPreviewError("bounded preview fail-closed markers changed")
        if any(
            case.s5_source_record_ids
            or case.s6_source_record_ids
            or case.hierarchy_source_record_ids
            or case.reason_codes != _NOT_EVALUATED_REASON_CODES
            for case in self.cases
        ):
            raise IdentityStreamingPreviewError(
                "bounded preview cannot claim evaluated corroboration"
            )

    @property
    def suspected_provider_figi_bounce_rows(self) -> int:
        return sum(case.middle_session_count for case in self.cases)

    def to_dict(self) -> dict[str, object]:
        return {
            "adjudication_eligible": False,
            "canonical_candidate_eligible": False,
            "cases": [case.to_manifest_dict() for case in self.cases],
            "corroboration_evaluation_state": "not_evaluated",
            "input_summary": {
                "incomplete_source_session_count": self.incomplete_source_session_count,
                "scanned_artifact_count": self.scanned_artifact_count,
                "scanned_bytes": self.scanned_bytes,
                "scanned_row_count": self.scanned_row_count,
                "selected_observation_count": self.selected_observation_count,
                "source_session_count": self.source_session_count,
                "valid_active_observation_count": self.valid_active_observation_count,
            },
            "limits": self.limits.to_dict(),
            "preview_manifest_available_session": (
                self.preview_manifest_available_session.isoformat()
            ),
            "rule_version": BOUNDED_PREVIEW_RULE_VERSION,
            "scope_kind": "bounded_preview",
            "scoped_tickers": list(self.scoped_tickers),
            "session_band_counts": dict(self.session_band_counts),
            "six_release_binding_id": self.six_release_binding_id,
            "source_attested": False,
            "status": "awaiting_review",
            "support_absence_verified": False,
            "support_reason_counts": dict(self.support_reason_counts),
            "suspected_provider_figi_bounce_rows": (self.suspected_provider_figi_bounce_rows),
        }


@dataclass(frozen=True, slots=True)
class BoundedIdentityPreviewArtifact:
    """Content-addressed review artifact distinct from the canonical candidate manifest."""

    preview_artifact_id: str
    sha256: str
    content: bytes
    document: Mapping[str, Any]

    def __post_init__(self) -> None:
        _digest(self.preview_artifact_id, "preview_artifact_id")
        _digest(self.sha256, "bounded preview artifact SHA-256")
        if hashlib.sha256(self.content).hexdigest() != self.sha256:
            raise IdentityStreamingPreviewError("bounded preview artifact checksum drifted")
        if _preview_artifact_bytes(self.document) != self.content:
            raise IdentityStreamingPreviewError("bounded preview artifact is not canonical JSON")
        if set(self.document) != {
            "adjudication_eligible",
            "artifact_type",
            "artifact_rule_version",
            "preview_artifact_id",
            "result",
            "scope_kind",
            "status",
        }:
            raise IdentityStreamingPreviewError("bounded preview artifact fields changed")
        if (
            self.document["artifact_type"] != "bounded_identity_bounce_preview"
            or self.document["artifact_rule_version"] != BOUNDED_PREVIEW_ARTIFACT_RULE_VERSION
            or self.document["scope_kind"] != "bounded_preview"
            or self.document["status"] != "awaiting_review"
            or self.document["adjudication_eligible"] is not False
        ):
            raise IdentityStreamingPreviewError("bounded preview artifact markers changed")
        logical = dict(self.document)
        del logical["preview_artifact_id"]
        if stable_digest(logical) != self.preview_artifact_id:
            raise IdentityStreamingPreviewError("bounded preview artifact ID does not reproduce")

    @property
    def relative_path(self) -> str:
        return (
            "manifests/silver/identity-bounce-bounded-previews/"
            f"preview_artifact_id={self.preview_artifact_id}/manifest.json"
        )


@dataclass(slots=True)
class _RunBoundary:
    figi: str
    last_source_record_id: str


@dataclass(slots=True)
class _CurrentRun:
    figi: str
    first_session: date
    last_session: date
    last_source_record_id: str
    source_record_ids: list[str]
    count: int = 1
    overflow: bool = False

    @classmethod
    def start(cls, row: IdentityObservation) -> _CurrentRun:
        assert row.observed_composite_figi is not None
        return cls(
            figi=row.observed_composite_figi,
            first_session=row.session_date,
            last_session=row.session_date,
            last_source_record_id=row.source_record_id,
            source_record_ids=[row.source_record_id],
        )

    def append(self, row: IdentityObservation) -> None:
        self.count += 1
        self.last_session = row.session_date
        self.last_source_record_id = row.source_record_id
        if len(self.source_record_ids) < MAX_MIDDLE_SESSIONS:
            self.source_record_ids.append(row.source_record_id)
        else:
            self.overflow = True

    def boundary(self) -> _RunBoundary:
        return _RunBoundary(
            figi=self.figi,
            last_source_record_id=self.last_source_record_id,
        )


@dataclass(slots=True)
class _TickerState:
    previous_run: _RunBoundary | None
    current_run: _CurrentRun


class BoundedIdentityPreviewEngine:
    """Consume scoped sessions with O(20) retained source IDs per ticker.

    This is a pure rule engine.  Its output carries ``source_attested=false`` and cannot be
    promoted to the production adjudication chain.
    """

    def __init__(
        self,
        *,
        six_release_binding_id: str,
        preview_manifest_available_session: date,
        scoped_tickers: Iterable[str],
        limits: BoundedIdentityPreviewLimits | None = None,
    ) -> None:
        self._six_release_binding_id = _digest(
            six_release_binding_id,
            "six_release_binding_id",
        )
        self._preview_available = _native_date(
            preview_manifest_available_session,
            "preview_manifest_available_session",
        )
        self._limits = limits or BoundedIdentityPreviewLimits()
        self._scoped_tickers = _normalized_tickers(scoped_tickers, self._limits)
        self._scope = frozenset(self._scoped_tickers)
        self._states: dict[str, _TickerState] = {}
        self._cases: list[BounceCase] = []
        self._last_session: date | None = None
        self._source_session_count = 0
        self._incomplete_source_session_count = 0
        self._selected_observation_count = 0
        self._valid_active_observation_count = 0
        self._scanned_row_count = 0
        self._scanned_artifact_count = 0
        self._scanned_bytes = 0
        self._failed = False
        self._finalized = False

    @property
    def buffered_source_record_id_count(self) -> int:
        """Expose the retained-state bound for tests and runtime monitoring."""

        return sum(
            len(state.current_run.source_record_ids) + (1 if state.previous_run is not None else 0)
            for state in self._states.values()
        )

    def consume_session(
        self,
        source_session: SourceSession,
        observations: Iterable[IdentityObservation],
        *,
        scanned_row_count: int,
        scanned_artifact_count: int,
        scanned_bytes: int,
    ) -> None:
        """Consume one strictly increasing source session without materializing its rows."""

        if self._failed:
            raise IdentityStreamingPreviewError("bounded preview engine is poisoned")
        if self._finalized:
            raise IdentityStreamingPreviewError("bounded preview engine is already finalized")
        try:
            self._consume_session(
                source_session,
                observations,
                scanned_row_count=scanned_row_count,
                scanned_artifact_count=scanned_artifact_count,
                scanned_bytes=scanned_bytes,
            )
        except Exception:
            self._failed = True
            raise

    def _consume_session(
        self,
        source_session: SourceSession,
        observations: Iterable[IdentityObservation],
        *,
        scanned_row_count: int,
        scanned_artifact_count: int,
        scanned_bytes: int,
    ) -> None:
        if not isinstance(source_session, SourceSession):
            raise IdentityStreamingPreviewError("source_session must be a SourceSession")
        for label, value in (
            ("scanned_row_count", scanned_row_count),
            ("scanned_artifact_count", scanned_artifact_count),
            ("scanned_bytes", scanned_bytes),
        ):
            _nonnegative_int(value, label)
        if self._source_session_count + 1 > self._limits.max_sessions:
            raise IdentityStreamingPreviewError("bounded preview session cap exceeded")
        if self._last_session is not None and source_session.session_date <= self._last_session:
            raise IdentityStreamingPreviewError(
                "bounded preview sessions must be strictly increasing"
            )
        self._precheck_total(
            self._scanned_row_count + scanned_row_count,
            self._scanned_artifact_count + scanned_artifact_count,
            self._scanned_bytes + scanned_bytes,
        )

        self._source_session_count += 1
        self._scanned_row_count += scanned_row_count
        self._scanned_artifact_count += scanned_artifact_count
        self._scanned_bytes += scanned_bytes
        self._last_session = source_session.session_date
        if not source_session.source_complete:
            self._incomplete_source_session_count += 1
            self._states.clear()

        seen: set[str] = set()
        session_selected_rows = 0
        for row in observations:
            if not isinstance(row, IdentityObservation):
                raise IdentityStreamingPreviewError(
                    "session observations must contain IdentityObservation values"
                )
            if row.session_date != source_session.session_date:
                raise IdentityStreamingPreviewError(
                    "observation session differs from the streamed source session"
                )
            if row.ticker not in self._scope:
                raise IdentityStreamingPreviewError(
                    "observation ticker is outside the exact preview scope"
                )
            if row.ticker in seen:
                raise IdentityStreamingPreviewError(
                    "duplicate ticker/session observation in streaming input"
                )
            seen.add(row.ticker)
            session_selected_rows += 1
            if self._selected_observation_count + 1 > self._limits.max_selected_rows:
                raise IdentityStreamingPreviewError("bounded preview selected-row cap exceeded")
            self._selected_observation_count += 1

            if not source_session.source_complete:
                continue
            figi = row.observed_composite_figi
            if not row.active_on_date or figi is None or not _FIGI.fullmatch(figi):
                self._states.pop(row.ticker, None)
                continue
            self._valid_active_observation_count += 1
            self._consume_valid_row(row)

        if session_selected_rows > scanned_row_count:
            raise IdentityStreamingPreviewError(
                "selected session rows exceed physically scanned rows"
            )
        if source_session.source_complete:
            for missing_ticker in set(self._states).difference(seen):
                self._states.pop(missing_ticker, None)
        if self.buffered_source_record_id_count > len(self._scope) * (MAX_MIDDLE_SESSIONS + 1):
            raise IdentityStreamingPreviewError(
                "streaming detector state exceeded O(20) per ticker"
            )

    def _consume_valid_row(self, row: IdentityObservation) -> None:
        figi = row.observed_composite_figi
        assert figi is not None
        state = self._states.get(row.ticker)
        if state is None:
            self._states[row.ticker] = _TickerState(
                previous_run=None,
                current_run=_CurrentRun.start(row),
            )
            return
        if state.current_run.figi == figi:
            state.current_run.append(row)
            return

        left = state.previous_run
        middle = state.current_run
        if (
            left is not None
            and left.figi == figi
            and left.figi != middle.figi
            and not middle.overflow
            and middle.count <= MAX_MIDDLE_SESSIONS
        ):
            self._append_case(row, left=left, middle=middle)
        state.previous_run = middle.boundary()
        state.current_run = _CurrentRun.start(row)

    def _append_case(
        self,
        right: IdentityObservation,
        *,
        left: _RunBoundary,
        middle: _CurrentRun,
    ) -> None:
        if len(self._cases) + 1 > self._limits.max_cases:
            raise IdentityStreamingPreviewError("bounded preview case cap exceeded")
        episode_ids = tuple(middle.source_record_ids)
        if len(episode_ids) != middle.count:
            raise IdentityStreamingPreviewError("bounded preview middle-run buffer is incomplete")
        if len(set(episode_ids)) != len(episode_ids):
            raise IdentityStreamingPreviewError(
                "duplicate source_record_id inside a bounce episode"
            )
        episode_digest = stable_digest(sorted(set(episode_ids)))
        case_payload = {
            "namespace": "ame_stocks.identity.provider_figi_bounce_case",
            "rule_version": CASE_ID_RULE_VERSION,
            "six_release_binding_id": self._six_release_binding_id,
            "detector_rule_version": DETECTOR_RULE_VERSION,
            "ticker": right.ticker,
            "left_outer_composite_figi": left.figi,
            "middle_observed_composite_figi": middle.figi,
            "right_outer_composite_figi": right.observed_composite_figi,
            "left_outer_source_record_id": left.last_source_record_id,
            "right_outer_source_record_id": right.source_record_id,
            "episode_valid_from_session": middle.first_session.isoformat(),
            "episode_valid_through_session": middle.last_session.isoformat(),
            "episode_source_record_set_digest": episode_digest,
        }
        self._cases.append(
            BounceCase(
                identity_case_id=stable_digest(case_payload),
                six_release_binding_id=self._six_release_binding_id,
                ticker=right.ticker,
                left_outer_composite_figi=left.figi,
                middle_observed_composite_figi=middle.figi,
                right_outer_composite_figi=right.observed_composite_figi or "",
                left_outer_source_record_id=left.last_source_record_id,
                right_outer_source_record_id=right.source_record_id,
                episode_valid_from_session=middle.first_session,
                episode_valid_through_session=middle.last_session,
                episode_source_record_ids=episode_ids,
                episode_source_record_set_digest=episode_digest,
                middle_session_count=middle.count,
                session_band=_session_band(middle.count),
                right_evidence_available_session=right.source_available_session,
                identity_case_available_session=max(
                    right.source_available_session,
                    self._preview_available,
                ),
                s5_source_record_ids=(),
                s6_source_record_ids=(),
                hierarchy_source_record_ids=(),
                reason_codes=_NOT_EVALUATED_REASON_CODES,
            )
        )

    def _precheck_total(self, rows: int, artifacts: int, scanned_bytes: int) -> None:
        if rows > self._limits.max_scanned_rows:
            raise IdentityStreamingPreviewError("bounded preview scanned-row cap exceeded")
        if artifacts > self._limits.max_artifacts:
            raise IdentityStreamingPreviewError("bounded preview artifact cap exceeded")
        if scanned_bytes > self._limits.max_bytes:
            raise IdentityStreamingPreviewError("bounded preview byte cap exceeded")

    def finalize(
        self,
        *,
        preview_manifest_available_session: date | None = None,
    ) -> BoundedIdentityPreviewResult:
        """Freeze a review-only result after all cap checks have succeeded."""

        if self._failed:
            raise IdentityStreamingPreviewError("bounded preview engine is poisoned")
        if self._finalized:
            raise IdentityStreamingPreviewError("bounded preview engine is already finalized")
        if self._source_session_count == 0:
            self._failed = True
            raise IdentityStreamingPreviewError("bounded preview cannot finalize an empty scope")
        self._precheck_total(
            self._scanned_row_count,
            self._scanned_artifact_count,
            self._scanned_bytes,
        )
        preview_available = (
            self._preview_available
            if preview_manifest_available_session is None
            else _native_date(
                preview_manifest_available_session,
                "preview_manifest_available_session",
            )
        )
        cases = tuple(
            sorted(
                (
                    replace(
                        case,
                        identity_case_available_session=max(
                            case.right_evidence_available_session,
                            preview_available,
                        ),
                    )
                    for case in self._cases
                ),
                key=lambda item: item.identity_case_id,
            )
        )
        band_counts = Counter(case.session_band for case in cases)
        reason_counts = Counter(reason for case in cases for reason in case.reason_codes)
        result = BoundedIdentityPreviewResult(
            six_release_binding_id=self._six_release_binding_id,
            preview_manifest_available_session=preview_available,
            scoped_tickers=self._scoped_tickers,
            limits=self._limits,
            source_session_count=self._source_session_count,
            incomplete_source_session_count=self._incomplete_source_session_count,
            selected_observation_count=self._selected_observation_count,
            valid_active_observation_count=self._valid_active_observation_count,
            scanned_row_count=self._scanned_row_count,
            scanned_artifact_count=self._scanned_artifact_count,
            scanned_bytes=self._scanned_bytes,
            cases=cases,
            session_band_counts=MappingProxyType(
                {band: band_counts[band] for band in _SESSION_BANDS}
            ),
            support_reason_counts=MappingProxyType(dict(sorted(reason_counts.items()))),
        )
        self._finalized = True
        return result


def build_bounded_identity_preview_artifact(
    result: BoundedIdentityPreviewResult,
) -> BoundedIdentityPreviewArtifact:
    """Build an immutable-byte model without writing it or creating a candidate manifest."""

    if not isinstance(result, BoundedIdentityPreviewResult):
        raise IdentityStreamingPreviewError("bounded preview artifact requires a finalized result")
    logical: dict[str, object] = {
        "adjudication_eligible": False,
        "artifact_rule_version": BOUNDED_PREVIEW_ARTIFACT_RULE_VERSION,
        "artifact_type": "bounded_identity_bounce_preview",
        "result": result.to_dict(),
        "scope_kind": "bounded_preview",
        "status": "awaiting_review",
    }
    artifact_id = stable_digest(logical)
    document = {**logical, "preview_artifact_id": artifact_id}
    content = _preview_artifact_bytes(document)
    return BoundedIdentityPreviewArtifact(
        preview_artifact_id=artifact_id,
        sha256=hashlib.sha256(content).hexdigest(),
        content=content,
        document=MappingProxyType(document),
    )


def write_bounded_identity_preview_artifact(
    root: Path,
    artifact: BoundedIdentityPreviewArtifact,
) -> dict[str, object]:
    """Write one review-only preview idempotently at its sole canonical path."""

    if not isinstance(root, Path) or not isinstance(artifact, BoundedIdentityPreviewArtifact):
        raise IdentityStreamingPreviewError("bounded preview write arguments are invalid")
    try:
        target = safe_relative_path(root, artifact.relative_path)
        stored = write_bytes_immutable(root, target, artifact.content)
    except ArtifactError as exc:
        raise IdentityStreamingPreviewError(str(exc)) from exc
    return {
        **stored,
        "preview_artifact_id": artifact.preview_artifact_id,
        "media_type": "application/json",
    }


def read_bounded_identity_preview_artifact(
    root: Path,
    *,
    preview_artifact_id: str,
    expected_sha256: str,
) -> BoundedIdentityPreviewArtifact:
    """Load exact canonical bytes by ID/SHA; latest discovery is impossible."""

    if not isinstance(root, Path):
        raise IdentityStreamingPreviewError("bounded preview root must be a Path")
    _digest(preview_artifact_id, "preview_artifact_id")
    _digest(expected_sha256, "expected bounded preview SHA-256")
    relative = (
        "manifests/silver/identity-bounce-bounded-previews/"
        f"preview_artifact_id={preview_artifact_id}/manifest.json"
    )
    try:
        path = safe_relative_path(root, relative)
    except ArtifactError as exc:
        raise IdentityStreamingPreviewError(str(exc)) from exc
    if not path.is_file() or path.is_symlink() or sha256_file(path) != expected_sha256:
        raise IdentityStreamingPreviewError("exact bounded preview artifact is unavailable")
    content = path.read_bytes()
    try:
        document = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise IdentityStreamingPreviewError("bounded preview artifact is not valid JSON") from exc
    if not isinstance(document, dict):
        raise IdentityStreamingPreviewError("bounded preview artifact must be an object")
    artifact = BoundedIdentityPreviewArtifact(
        preview_artifact_id=preview_artifact_id,
        sha256=expected_sha256,
        content=content,
        document=MappingProxyType(document),
    )
    if artifact.relative_path != relative:
        raise IdentityStreamingPreviewError("bounded preview artifact path binding differs")
    return artifact


def run_source_bound_identity_streaming_preview(
    data_root: Path,
    *,
    plan_id: str,
    expected_plan_sha256: str,
    approval_id: str,
    expected_approval_sha256: str,
) -> S7DetectorPreviewCompletion:
    """Delegate to the source-bound runner without exposing injectable source inputs."""

    from ame_stocks_api.silver.identity_preview_runner import (
        IdentityPreviewRunnerError,
    )
    from ame_stocks_api.silver.identity_preview_runner import (
        run_source_bound_identity_streaming_preview as run,
    )

    try:
        return run(
            data_root,
            plan_id=plan_id,
            expected_plan_sha256=expected_plan_sha256,
            approval_id=approval_id,
            expected_approval_sha256=expected_approval_sha256,
        )
    except IdentityPreviewRunnerError as exc:
        raise IdentityStreamingPreviewError(str(exc)) from exc


def _normalized_tickers(
    tickers: Iterable[str],
    limits: BoundedIdentityPreviewLimits,
) -> tuple[str, ...]:
    try:
        values = tuple(tickers)
    except TypeError as exc:
        raise IdentityStreamingPreviewError("scoped_tickers must be iterable") from exc
    normalized = tuple(sorted(values))
    _validate_scoped_tickers(normalized, limits)
    if values != normalized:
        raise IdentityStreamingPreviewError("scoped_tickers must be supplied in exact sorted order")
    return normalized


def _validate_scoped_tickers(
    tickers: tuple[str, ...],
    limits: BoundedIdentityPreviewLimits,
) -> None:
    if not tickers:
        raise IdentityStreamingPreviewError("bounded preview requires at least one ticker")
    if len(tickers) > limits.max_tickers:
        raise IdentityStreamingPreviewError("bounded preview ticker cap exceeded")
    if len(set(tickers)) != len(tickers):
        raise IdentityStreamingPreviewError("scoped_tickers must be unique")
    for ticker in tickers:
        if (
            not isinstance(ticker, str)
            or not ticker
            or ticker != ticker.strip()
            or ticker in {"*", ".*"}
        ):
            raise IdentityStreamingPreviewError(
                "scoped_tickers must be exact nonempty case-sensitive values"
            )


def _preview_artifact_bytes(document: Mapping[str, object]) -> bytes:
    return (
        json.dumps(
            dict(document),
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        + b"\n"
    )


def _session_band(count: int) -> str:
    _positive_int(count, "middle session count")
    if count == 1:
        return "1"
    if count <= 5:
        return "2-5"
    if count <= MAX_MIDDLE_SESSIONS:
        return "6-20"
    raise IdentityStreamingPreviewError("middle session count exceeds detector bound")


def _digest(value: object, label: str) -> str:
    if not isinstance(value, str) or not _SHA256.fullmatch(value):
        raise IdentityStreamingPreviewError(f"{label} must be a lowercase SHA-256")
    return value


def _native_date(value: object, label: str) -> date:
    if type(value) is not date:
        raise IdentityStreamingPreviewError(f"{label} must be a native date")
    return value


def _positive_int(value: object, label: str) -> int:
    if type(value) is not int or value <= 0:
        raise IdentityStreamingPreviewError(f"{label} must be a positive native int")
    return value


def _nonnegative_int(value: object, label: str) -> int:
    if type(value) is not int or value < 0:
        raise IdentityStreamingPreviewError(f"{label} must be a nonnegative native int")
    return value


__all__ = [
    "BOUNDED_PREVIEW_ARTIFACT_RULE_VERSION",
    "BOUNDED_PREVIEW_RULE_VERSION",
    "HARD_MAX_PREVIEW_ARTIFACTS",
    "HARD_MAX_PREVIEW_BYTES",
    "HARD_MAX_PREVIEW_CASES",
    "HARD_MAX_PREVIEW_SCANNED_ROWS",
    "HARD_MAX_PREVIEW_SELECTED_ROWS",
    "HARD_MAX_PREVIEW_SESSIONS",
    "HARD_MAX_PREVIEW_TICKERS",
    "BoundedIdentityPreviewArtifact",
    "BoundedIdentityPreviewEngine",
    "BoundedIdentityPreviewLimits",
    "BoundedIdentityPreviewResult",
    "IdentityStreamingPreviewError",
    "build_bounded_identity_preview_artifact",
    "read_bounded_identity_preview_artifact",
    "run_source_bound_identity_streaming_preview",
    "write_bounded_identity_preview_artifact",
]
