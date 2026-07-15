"""Deterministic S7 provider-FIGI bounce discovery and candidate manifests.

The detector is deliberately discovery-only.  It recognizes bounded, maximal-run
``A -> B -> A`` episodes on the exact S4 source-session spine and preserves the
middle observations for review.  It never selects a canonical identity or an
adjudication disposition.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from types import MappingProxyType
from typing import Any, Final

from ame_stocks_api.artifacts import (
    ArtifactError,
    safe_relative_path,
    sha256_file,
    stable_digest,
    write_bytes_immutable,
)
from ame_stocks_api.silver.availability import (
    SilverAvailabilityError,
    require_first_xnys_open_session,
)
from ame_stocks_api.silver.identity_source import S7_SIX_RELEASE_BINDING_ID

DETECTOR_RULE_VERSION: Final = "s7_provider_figi_bounce_detector_v1"
CASE_ID_RULE_VERSION: Final = "s7_provider_figi_bounce_case_id_v1"
CANDIDATE_MANIFEST_RULE_VERSION: Final = "s7_identity_case_candidate_manifest_v1"
CANDIDATE_MANIFEST_SCHEMA_VERSION: Final = 1
MAX_MIDDLE_SESSIONS: Final = 20
DEFAULT_BOUNDED_EXAMPLE_LIMIT: Final = 25
MAX_BOUNDED_OBSERVATIONS: Final = 250_000
MAX_BOUNDED_SOURCE_SESSIONS: Final = 5_000

_FIGI = re.compile(r"^BBG[0-9A-Z]{9}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SESSION_BANDS = ("1", "2-5", "6-20")


class IdentityBounceError(ArtifactError):
    """Raised when detector inputs or a candidate manifest are unsafe."""


@dataclass(frozen=True, slots=True)
class SourceSession:
    """One expected global S4 source session.

    ``source_complete=False`` represents a known global source-session gap and
    breaks every ticker run.  Callers must provide the complete expected XNYS
    spine; omitted calendar sessions cannot be inferred by this pure detector.
    """

    session_date: date
    source_complete: bool = True

    def __post_init__(self) -> None:
        _native_date(self.session_date, "source session date")
        _native_bool(self.source_complete, "source_complete")


@dataclass(frozen=True, slots=True)
class IdentityObservation:
    """The exact selected S4 membership observation for one ticker/session."""

    session_date: date
    ticker: str
    observed_composite_figi: str | None
    source_record_id: str
    source_available_session: date
    active_on_date: bool = True

    def __post_init__(self) -> None:
        _native_date(self.session_date, "observation session_date")
        _exact_text(self.ticker, "ticker")
        if self.observed_composite_figi is not None:
            _exact_text(self.observed_composite_figi, "observed_composite_figi")
        _exact_text(self.source_record_id, "source_record_id")
        _native_date(self.source_available_session, "source_available_session")
        _native_bool(self.active_on_date, "active_on_date")
        if self.source_available_session < self.session_date:
            raise IdentityBounceError(
                "source_available_session cannot precede the observed session"
            )


@dataclass(frozen=True, slots=True)
class BounceCorroboration:
    """Exact optional S5/S6/hierarchy evidence associated with one episode.

    These references only classify review reasons.  Presence, absence, or count
    never changes detection and never produces an automatic decision.
    """

    ticker: str
    middle_observed_composite_figi: str
    episode_valid_from_session: date
    episode_valid_through_session: date
    s5_source_record_ids: tuple[str, ...] = ()
    s6_source_record_ids: tuple[str, ...] = ()
    hierarchy_source_record_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _exact_text(self.ticker, "corroboration ticker")
        if not _FIGI.fullmatch(self.middle_observed_composite_figi):
            raise IdentityBounceError("corroboration middle Composite FIGI is malformed")
        start = _native_date(
            self.episode_valid_from_session,
            "corroboration episode_valid_from_session",
        )
        end = _native_date(
            self.episode_valid_through_session,
            "corroboration episode_valid_through_session",
        )
        if start > end:
            raise IdentityBounceError("corroboration episode bounds are reversed")
        for label, values in (
            ("S5 source-record IDs", self.s5_source_record_ids),
            ("S6 source-record IDs", self.s6_source_record_ids),
            ("hierarchy source-record IDs", self.hierarchy_source_record_ids),
        ):
            _sorted_unique_text_tuple(values, label)

    @property
    def key(self) -> tuple[str, str, date, date]:
        return (
            self.ticker,
            self.middle_observed_composite_figi,
            self.episode_valid_from_session,
            self.episode_valid_through_session,
        )


@dataclass(frozen=True, slots=True)
class BounceCase:
    """One deterministic, review-only provider FIGI bounce case."""

    identity_case_id: str
    six_release_binding_id: str
    ticker: str
    left_outer_composite_figi: str
    middle_observed_composite_figi: str
    right_outer_composite_figi: str
    left_outer_source_record_id: str
    right_outer_source_record_id: str
    episode_valid_from_session: date
    episode_valid_through_session: date
    episode_source_record_ids: tuple[str, ...]
    episode_source_record_set_digest: str
    middle_session_count: int
    session_band: str
    right_evidence_available_session: date
    identity_case_available_session: date
    s5_source_record_ids: tuple[str, ...]
    s6_source_record_ids: tuple[str, ...]
    hierarchy_source_record_ids: tuple[str, ...]
    reason_codes: tuple[str, ...]

    def __post_init__(self) -> None:
        _digest(self.identity_case_id, "identity_case_id")
        _digest(self.six_release_binding_id, "six_release_binding_id")
        _exact_text(self.ticker, "case ticker")
        for label, figi in (
            ("left outer Composite FIGI", self.left_outer_composite_figi),
            ("middle observed Composite FIGI", self.middle_observed_composite_figi),
            ("right outer Composite FIGI", self.right_outer_composite_figi),
        ):
            if not _FIGI.fullmatch(figi):
                raise IdentityBounceError(f"{label} is malformed")
        if self.left_outer_composite_figi != self.right_outer_composite_figi:
            raise IdentityBounceError("bounce outer Composite FIGIs must be equal")
        if self.left_outer_composite_figi == self.middle_observed_composite_figi:
            raise IdentityBounceError(
                "bounce middle Composite FIGI must differ from the outer FIGI"
            )
        _exact_text(self.left_outer_source_record_id, "left outer source_record_id")
        _exact_text(self.right_outer_source_record_id, "right outer source_record_id")
        start = _native_date(self.episode_valid_from_session, "episode_valid_from_session")
        end = _native_date(self.episode_valid_through_session, "episode_valid_through_session")
        if start > end:
            raise IdentityBounceError("bounce episode bounds are reversed")
        _sorted_unique_text_tuple(
            tuple(sorted(self.episode_source_record_ids)),
            "episode source-record IDs",
        )
        if len(self.episode_source_record_ids) != self.middle_session_count:
            raise IdentityBounceError(
                "episode source-record count differs from middle_session_count"
            )
        _native_positive_int(self.middle_session_count, "middle_session_count")
        if self.middle_session_count > MAX_MIDDLE_SESSIONS:
            raise IdentityBounceError("bounce middle run exceeds the v1 bound")
        if self.session_band != _session_band(self.middle_session_count):
            raise IdentityBounceError("bounce session band does not match middle_session_count")
        _digest(self.episode_source_record_set_digest, "episode_source_record_set_digest")
        expected_set_digest = stable_digest(sorted(set(self.episode_source_record_ids)))
        if self.episode_source_record_set_digest != expected_set_digest:
            raise IdentityBounceError("episode source-record-set digest does not reproduce")
        right_available = _native_date(
            self.right_evidence_available_session,
            "right_evidence_available_session",
        )
        case_available = _native_date(
            self.identity_case_available_session,
            "identity_case_available_session",
        )
        if case_available < right_available:
            raise IdentityBounceError("case availability precedes right-side A evidence")
        for label, values in (
            ("S5 source-record IDs", self.s5_source_record_ids),
            ("S6 source-record IDs", self.s6_source_record_ids),
            ("hierarchy source-record IDs", self.hierarchy_source_record_ids),
            ("reason codes", self.reason_codes),
        ):
            _sorted_unique_text_tuple(values, label)
        if tuple(sorted(self.reason_codes)) != self.reason_codes:
            raise IdentityBounceError("reason codes must be lexicographically sorted")
        expected_case_id = stable_digest(self.case_id_payload())
        if self.identity_case_id != expected_case_id:
            raise IdentityBounceError("identity_case_id does not reproduce from its frozen payload")

    @property
    def corroboration_key(self) -> tuple[str, str, date, date]:
        return (
            self.ticker,
            self.middle_observed_composite_figi,
            self.episode_valid_from_session,
            self.episode_valid_through_session,
        )

    @property
    def s5_support_count(self) -> int:
        return len(self.s5_source_record_ids)

    @property
    def s6_support_count(self) -> int:
        return len(self.s6_source_record_ids)

    @property
    def hierarchy_support_count(self) -> int:
        return len(self.hierarchy_source_record_ids)

    def case_id_payload(self) -> dict[str, object]:
        """Return the exact frozen preimage used by the reviewed S7 fixed vector."""

        return {
            "namespace": "ame_stocks.identity.provider_figi_bounce_case",
            "rule_version": CASE_ID_RULE_VERSION,
            "six_release_binding_id": self.six_release_binding_id,
            "detector_rule_version": DETECTOR_RULE_VERSION,
            "ticker": self.ticker,
            "left_outer_composite_figi": self.left_outer_composite_figi,
            "middle_observed_composite_figi": self.middle_observed_composite_figi,
            "right_outer_composite_figi": self.right_outer_composite_figi,
            "left_outer_source_record_id": self.left_outer_source_record_id,
            "right_outer_source_record_id": self.right_outer_source_record_id,
            "episode_valid_from_session": self.episode_valid_from_session.isoformat(),
            "episode_valid_through_session": self.episode_valid_through_session.isoformat(),
            "episode_source_record_set_digest": self.episode_source_record_set_digest,
        }

    def to_manifest_dict(self) -> dict[str, object]:
        """Return a detached JSON-safe manifest row."""

        return {
            "detector_disposition": "review_required_no_auto_decision",
            "detector_rule_version": DETECTOR_RULE_VERSION,
            "episode_source_record_ids": list(self.episode_source_record_ids),
            "episode_source_record_set_digest": self.episode_source_record_set_digest,
            "episode_valid_from_session": self.episode_valid_from_session.isoformat(),
            "episode_valid_through_session": self.episode_valid_through_session.isoformat(),
            "hierarchy_source_record_ids": list(self.hierarchy_source_record_ids),
            "hierarchy_support_count": self.hierarchy_support_count,
            "identity_case_available_session": self.identity_case_available_session.isoformat(),
            "identity_case_id": self.identity_case_id,
            "left_outer_composite_figi": self.left_outer_composite_figi,
            "left_outer_source_record_id": self.left_outer_source_record_id,
            "middle_observed_composite_figi": self.middle_observed_composite_figi,
            "middle_session_count": self.middle_session_count,
            "reason_codes": list(self.reason_codes),
            "right_evidence_available_session": (self.right_evidence_available_session.isoformat()),
            "right_outer_composite_figi": self.right_outer_composite_figi,
            "right_outer_source_record_id": self.right_outer_source_record_id,
            "s5_source_record_ids": list(self.s5_source_record_ids),
            "s5_support_count": self.s5_support_count,
            "s6_source_record_ids": list(self.s6_source_record_ids),
            "s6_support_count": self.s6_support_count,
            "session_band": self.session_band,
            "six_release_binding_id": self.six_release_binding_id,
            "ticker": self.ticker,
        }


@dataclass(frozen=True, slots=True)
class BounceDetection:
    """Complete deterministic detector result before manifest serialization."""

    six_release_binding_id: str
    candidate_manifest_available_session: date
    source_session_count: int
    incomplete_source_session_count: int
    observation_count: int
    valid_active_observation_count: int
    cases: tuple[BounceCase, ...]
    session_band_counts: Mapping[str, int]
    support_reason_counts: Mapping[str, int]

    def __post_init__(self) -> None:
        _digest(self.six_release_binding_id, "six_release_binding_id")
        _native_date(
            self.candidate_manifest_available_session,
            "candidate_manifest_available_session",
        )
        for label, value in (
            ("source_session_count", self.source_session_count),
            ("incomplete_source_session_count", self.incomplete_source_session_count),
            ("observation_count", self.observation_count),
            ("valid_active_observation_count", self.valid_active_observation_count),
        ):
            _native_nonnegative_int(value, label)
        if self.incomplete_source_session_count > self.source_session_count:
            raise IdentityBounceError("incomplete source-session count exceeds the spine")
        if self.valid_active_observation_count > self.observation_count:
            raise IdentityBounceError("valid active observation count exceeds input rows")
        if tuple(sorted(case.identity_case_id for case in self.cases)) != tuple(
            case.identity_case_id for case in self.cases
        ):
            raise IdentityBounceError("bounce cases must be sorted by identity_case_id")
        expected_bands = Counter(case.session_band for case in self.cases)
        if dict(self.session_band_counts) != {
            band: expected_bands[band] for band in _SESSION_BANDS
        }:
            raise IdentityBounceError("session-band counts do not reconcile to cases")
        expected_reasons = Counter(reason for case in self.cases for reason in case.reason_codes)
        if dict(self.support_reason_counts) != dict(sorted(expected_reasons.items())):
            raise IdentityBounceError("support-reason counts do not reconcile to cases")

    @property
    def suspected_provider_figi_bounce_rows(self) -> int:
        return sum(case.middle_session_count for case in self.cases)


@dataclass(frozen=True, slots=True)
class IdentityCaseCandidateManifest:
    """Validated content-addressed manifest bytes and canonical relative path."""

    candidate_manifest_id: str
    sha256: str
    content: bytes
    document: Mapping[str, Any]

    def __post_init__(self) -> None:
        _digest(self.candidate_manifest_id, "candidate_manifest_id")
        _digest(self.sha256, "candidate manifest SHA-256")
        if hashlib.sha256(self.content).hexdigest() != self.sha256:
            raise IdentityBounceError("candidate manifest SHA-256 does not match its bytes")
        _validate_candidate_manifest_document(dict(self.document), self.content)
        if self.document.get("candidate_manifest_id") != self.candidate_manifest_id:
            raise IdentityBounceError("candidate manifest object ID does not match its document")

    @property
    def relative_path(self) -> str:
        return identity_case_candidate_manifest_path(self.candidate_manifest_id)

    @property
    def candidate_manifest_available_session(self) -> date:
        """Return the exact physical availability bound validated from the manifest."""

        return date.fromisoformat(str(self.document["candidate_manifest_available_session"]))

    @property
    def six_release_binding_id(self) -> str:
        return str(self.document["six_release_binding_id"])

    @property
    def cases(self) -> tuple[BounceCase, ...]:
        """Rehydrate the validated immutable case rows for a cutoff resolver."""

        rows = self.document["cases"]
        assert isinstance(rows, list)  # enforced by manifest validation
        return tuple(_bounce_case_from_manifest_row(row) for row in rows)


def detect_provider_figi_bounces(
    session_spine: Iterable[SourceSession],
    observations: Iterable[IdentityObservation],
    *,
    six_release_binding_id: str,
    candidate_manifest_available_session: date,
    corroboration: Iterable[BounceCorroboration] = (),
) -> BounceDetection:
    """Detect bounded in-memory A/B/A episodes without adjudicating them.

    This primitive is intentionally capped for fixtures and explicitly scoped previews.  It
    is not the production all-history runner and carries no exact-release source attestation.
    """

    _digest(six_release_binding_id, "six_release_binding_id")
    manifest_available = _native_date(
        candidate_manifest_available_session,
        "candidate_manifest_available_session",
    )
    sessions = tuple(session_spine)
    if len(sessions) > MAX_BOUNDED_SOURCE_SESSIONS:
        raise IdentityBounceError("bounded detector source-session limit exceeded")
    if any(not isinstance(item, SourceSession) for item in sessions):
        raise IdentityBounceError("session_spine must contain SourceSession values")
    _validate_session_spine(sessions)
    rows = tuple(observations)
    if len(rows) > MAX_BOUNDED_OBSERVATIONS:
        raise IdentityBounceError("bounded detector observation limit exceeded")
    if any(not isinstance(item, IdentityObservation) for item in rows):
        raise IdentityBounceError("observations must contain IdentityObservation values")
    session_dates = {item.session_date for item in sessions}
    by_key: dict[tuple[str, date], IdentityObservation] = {}
    for row in rows:
        if row.session_date not in session_dates:
            raise IdentityBounceError("observation session is outside the supplied global spine")
        key = (row.ticker, row.session_date)
        if key in by_key:
            raise IdentityBounceError("duplicate ticker/session observation in detector input")
        by_key[key] = row

    corroboration_by_key: dict[tuple[str, str, date, date], BounceCorroboration] = {}
    for support in corroboration:
        if not isinstance(support, BounceCorroboration):
            raise IdentityBounceError("corroboration must contain BounceCorroboration values")
        if support.key in corroboration_by_key:
            raise IdentityBounceError("duplicate corroboration record for one bounce episode")
        corroboration_by_key[support.key] = support

    complete_by_date = {item.session_date: item.source_complete for item in sessions}
    tickers = sorted({row.ticker for row in rows})
    cases: list[BounceCase] = []
    matched_support_keys: set[tuple[str, str, date, date]] = set()
    valid_active_observations = 0
    for ticker in tickers:
        runs: list[list[IdentityObservation]] = []
        current: list[IdentityObservation] = []
        for source_session in sessions:
            row = by_key.get((ticker, source_session.session_date))
            if (
                not source_session.source_complete
                or row is None
                or not row.active_on_date
                or row.observed_composite_figi is None
                or not _FIGI.fullmatch(row.observed_composite_figi)
            ):
                if current:
                    runs.append(current)
                    current = []
                # Insert an explicit separator so adjacent valid runs on either side
                # of a broken session cannot be treated as adjacent maximal runs.
                if runs and runs[-1]:
                    runs.append([])
                continue
            valid_active_observations += 1
            if current and (current[-1].observed_composite_figi != row.observed_composite_figi):
                runs.append(current)
                current = []
            current.append(row)
        if current:
            runs.append(current)

        for index in range(1, len(runs) - 1):
            left = runs[index - 1]
            middle = runs[index]
            right = runs[index + 1]
            if not left or not middle or not right:
                continue
            left_figi = left[-1].observed_composite_figi
            middle_figi = middle[0].observed_composite_figi
            right_figi = right[0].observed_composite_figi
            if (
                left_figi is None
                or middle_figi is None
                or right_figi is None
                or left_figi != right_figi
                or left_figi == middle_figi
                or len(middle) > MAX_MIDDLE_SESSIONS
            ):
                continue
            episode_ids = tuple(row.source_record_id for row in middle)
            if len(set(episode_ids)) != len(episode_ids):
                raise IdentityBounceError("duplicate source_record_id inside a bounce episode")
            episode_digest = stable_digest(sorted(set(episode_ids)))
            support_key = (
                ticker,
                middle_figi,
                middle[0].session_date,
                middle[-1].session_date,
            )
            support = corroboration_by_key.get(support_key)
            if support is None:
                support = BounceCorroboration(
                    ticker=ticker,
                    middle_observed_composite_figi=middle_figi,
                    episode_valid_from_session=middle[0].session_date,
                    episode_valid_through_session=middle[-1].session_date,
                )
            else:
                matched_support_keys.add(support_key)
            case_payload = {
                "namespace": "ame_stocks.identity.provider_figi_bounce_case",
                "rule_version": CASE_ID_RULE_VERSION,
                "six_release_binding_id": six_release_binding_id,
                "detector_rule_version": DETECTOR_RULE_VERSION,
                "ticker": ticker,
                "left_outer_composite_figi": left_figi,
                "middle_observed_composite_figi": middle_figi,
                "right_outer_composite_figi": right_figi,
                "left_outer_source_record_id": left[-1].source_record_id,
                "right_outer_source_record_id": right[0].source_record_id,
                "episode_valid_from_session": middle[0].session_date.isoformat(),
                "episode_valid_through_session": middle[-1].session_date.isoformat(),
                "episode_source_record_set_digest": episode_digest,
            }
            right_available = right[0].source_available_session
            cases.append(
                BounceCase(
                    identity_case_id=stable_digest(case_payload),
                    six_release_binding_id=six_release_binding_id,
                    ticker=ticker,
                    left_outer_composite_figi=left_figi,
                    middle_observed_composite_figi=middle_figi,
                    right_outer_composite_figi=right_figi,
                    left_outer_source_record_id=left[-1].source_record_id,
                    right_outer_source_record_id=right[0].source_record_id,
                    episode_valid_from_session=middle[0].session_date,
                    episode_valid_through_session=middle[-1].session_date,
                    episode_source_record_ids=episode_ids,
                    episode_source_record_set_digest=episode_digest,
                    middle_session_count=len(middle),
                    session_band=_session_band(len(middle)),
                    right_evidence_available_session=right_available,
                    identity_case_available_session=max(right_available, manifest_available),
                    s5_source_record_ids=support.s5_source_record_ids,
                    s6_source_record_ids=support.s6_source_record_ids,
                    hierarchy_source_record_ids=support.hierarchy_source_record_ids,
                    reason_codes=_reason_codes(support),
                )
            )

    unused_support = set(corroboration_by_key).difference(matched_support_keys)
    if unused_support:
        raise IdentityBounceError(
            "corroboration does not match any exact detected bounce episode: "
            + repr(sorted(unused_support))
        )
    cases.sort(key=lambda item: item.identity_case_id)
    band_counts = Counter(case.session_band for case in cases)
    reason_counts = Counter(reason for case in cases for reason in case.reason_codes)
    return BounceDetection(
        six_release_binding_id=six_release_binding_id,
        candidate_manifest_available_session=manifest_available,
        source_session_count=len(sessions),
        incomplete_source_session_count=sum(
            not complete_by_date[item.session_date] for item in sessions
        ),
        observation_count=len(rows),
        valid_active_observation_count=valid_active_observations,
        cases=tuple(cases),
        session_band_counts=MappingProxyType({band: band_counts[band] for band in _SESSION_BANDS}),
        support_reason_counts=MappingProxyType(dict(sorted(reason_counts.items()))),
    )


def build_identity_case_candidate_manifest(
    detection: BounceDetection,
    *,
    created_at_utc: datetime,
    bounded_example_limit: int = DEFAULT_BOUNDED_EXAMPLE_LIMIT,
) -> IdentityCaseCandidateManifest:
    """Build deterministic content-addressed bytes for one complete detector result."""

    created_at = _utc_datetime(created_at_utc, "created_at_utc")
    try:
        require_first_xnys_open_session(
            created_at,
            detection.candidate_manifest_available_session,
            label="candidate manifest availability",
        )
    except SilverAvailabilityError as exc:
        raise IdentityBounceError(str(exc)) from exc
    _native_positive_int(bounded_example_limit, "bounded_example_limit")
    if bounded_example_limit > 100:
        raise IdentityBounceError("bounded_example_limit exceeds the hard limit of 100")
    case_rows = [case.to_manifest_dict() for case in detection.cases]
    logical_payload: dict[str, object] = {
        "artifact_type": "identity_case_candidate_manifest",
        "bounded_example_limit": bounded_example_limit,
        "bounded_examples": case_rows[:bounded_example_limit],
        "candidate_manifest_available_session": (
            detection.candidate_manifest_available_session.isoformat()
        ),
        "candidate_manifest_rule_version": CANDIDATE_MANIFEST_RULE_VERSION,
        "case_count": len(case_rows),
        "cases": case_rows,
        "created_at_utc": created_at.isoformat(),
        "detector_disposition": "discovery_only_no_auto_adjudication",
        "detector_rule_version": DETECTOR_RULE_VERSION,
        "input_summary": {
            "incomplete_source_session_count": detection.incomplete_source_session_count,
            "observation_count": detection.observation_count,
            "source_session_count": detection.source_session_count,
            "valid_active_observation_count": detection.valid_active_observation_count,
        },
        "schema_version": CANDIDATE_MANIFEST_SCHEMA_VERSION,
        "session_band_counts": dict(detection.session_band_counts),
        "six_release_binding_id": detection.six_release_binding_id,
        "support_reason_counts": dict(detection.support_reason_counts),
        "suspected_provider_figi_bounce_rows": (detection.suspected_provider_figi_bounce_rows),
    }
    manifest_id = stable_digest(logical_payload)
    document = {**logical_payload, "candidate_manifest_id": manifest_id}
    content = _manifest_bytes(document)
    return IdentityCaseCandidateManifest(
        candidate_manifest_id=manifest_id,
        sha256=hashlib.sha256(content).hexdigest(),
        content=content,
        document=MappingProxyType(document),
    )


def identity_case_candidate_manifest_path(candidate_manifest_id: str) -> str:
    """Return the only accepted path for an S7 identity-case candidate manifest."""

    _digest(candidate_manifest_id, "candidate_manifest_id")
    return (
        "manifests/silver/identity-case-candidates/"
        f"candidate_manifest_id={candidate_manifest_id}.json"
    )


def write_identity_case_candidate_manifest(
    data_root: Path,
    manifest: IdentityCaseCandidateManifest,
) -> dict[str, object]:
    """Publish only non-production fixtures until promotion provenance is implemented."""

    if manifest.six_release_binding_id == S7_SIX_RELEASE_BINDING_ID:
        raise IdentityBounceError(
            "production candidate writing remains hard-gated pending separately approved "
            "corroboration and promotion provenance"
        )

    root = data_root.expanduser().resolve()
    relative = identity_case_candidate_manifest_path(manifest.candidate_manifest_id)
    stored = write_bytes_immutable(root, root / relative, manifest.content)
    if stored["sha256"] != manifest.sha256:
        raise IdentityBounceError("stored candidate manifest SHA-256 drifted")
    return {
        **stored,
        "candidate_manifest_id": manifest.candidate_manifest_id,
        "media_type": "application/json",
    }


def read_identity_case_candidate_manifest(
    data_root: Path,
    *,
    candidate_manifest_id: str,
    expected_sha256: str,
) -> IdentityCaseCandidateManifest:
    """Read one exact manifest by ID/SHA without latest-by-time discovery."""

    _digest(candidate_manifest_id, "candidate_manifest_id")
    _digest(expected_sha256, "expected candidate manifest SHA-256")
    root = data_root.expanduser().resolve()
    relative = identity_case_candidate_manifest_path(candidate_manifest_id)
    path = safe_relative_path(root, relative)
    if not path.is_file() or path.is_symlink():
        raise IdentityBounceError("exact identity-case candidate manifest is unavailable")
    if sha256_file(path) != expected_sha256:
        raise IdentityBounceError("identity-case candidate manifest checksum mismatch")
    content = path.read_bytes()
    try:
        raw = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise IdentityBounceError("identity-case candidate manifest is not valid JSON") from exc
    if not isinstance(raw, dict):
        raise IdentityBounceError("identity-case candidate manifest must be a JSON object")
    _validate_candidate_manifest_document(raw, content)
    if raw.get("six_release_binding_id") == S7_SIX_RELEASE_BINDING_ID:
        raise IdentityBounceError(
            "production candidate reading remains hard-gated pending separately approved "
            "corroboration and promotion provenance"
        )
    return IdentityCaseCandidateManifest(
        candidate_manifest_id=candidate_manifest_id,
        sha256=expected_sha256,
        content=content,
        document=MappingProxyType(raw),
    )


def _validate_session_spine(sessions: tuple[SourceSession, ...]) -> None:
    prior: date | None = None
    for source_session in sessions:
        if prior is not None and source_session.session_date <= prior:
            raise IdentityBounceError("global source-session spine must be strictly increasing")
        prior = source_session.session_date


def _reason_codes(support: BounceCorroboration) -> tuple[str, ...]:
    reasons = {
        (
            "s5_ticker_change_event_support_present"
            if support.s5_source_record_ids
            else "s5_ticker_change_event_support_absent"
        ),
        (
            "s6_overview_identity_support_present"
            if support.s6_source_record_ids
            else "s6_overview_identity_support_absent"
        ),
        (
            "hierarchy_support_present"
            if support.hierarchy_source_record_ids
            else "hierarchy_support_absent"
        ),
    }
    if not support.s5_source_record_ids and not support.s6_source_record_ids:
        reasons.add("s5_and_s6_support_absent")
    elif support.s5_source_record_ids and support.s6_source_record_ids:
        reasons.add("s5_and_s6_support_present")
    return tuple(sorted(reasons))


def _session_band(count: int) -> str:
    _native_positive_int(count, "middle session count")
    if count == 1:
        return "1"
    if count <= 5:
        return "2-5"
    if count <= MAX_MIDDLE_SESSIONS:
        return "6-20"
    raise IdentityBounceError("middle session count exceeds the v1 detector bound")


def _manifest_bytes(document: Mapping[str, object]) -> bytes:
    return (
        json.dumps(
            document,
            allow_nan=False,
            indent=2,
            sort_keys=True,
        ).encode("utf-8")
        + b"\n"
    )


def _validate_candidate_manifest_document(document: dict[str, Any], content: bytes) -> None:
    if _manifest_bytes(document) != content:
        raise IdentityBounceError("candidate manifest bytes are not canonical JSON")
    expected_keys = {
        "artifact_type",
        "bounded_example_limit",
        "bounded_examples",
        "candidate_manifest_available_session",
        "candidate_manifest_id",
        "candidate_manifest_rule_version",
        "case_count",
        "cases",
        "created_at_utc",
        "detector_disposition",
        "detector_rule_version",
        "input_summary",
        "schema_version",
        "session_band_counts",
        "six_release_binding_id",
        "support_reason_counts",
        "suspected_provider_figi_bounce_rows",
    }
    if set(document) != expected_keys:
        raise IdentityBounceError("candidate manifest has unexpected or missing top-level fields")
    if (
        document["artifact_type"] != "identity_case_candidate_manifest"
        or document["schema_version"] != CANDIDATE_MANIFEST_SCHEMA_VERSION
        or document["candidate_manifest_rule_version"] != CANDIDATE_MANIFEST_RULE_VERSION
        or document["detector_rule_version"] != DETECTOR_RULE_VERSION
        or document["detector_disposition"] != "discovery_only_no_auto_adjudication"
    ):
        raise IdentityBounceError("candidate manifest fixed contract fields are invalid")
    manifest_id = _digest(document["candidate_manifest_id"], "candidate_manifest_id")
    six_release_binding_id = _digest(
        document["six_release_binding_id"],
        "six_release_binding_id",
    )
    logical_payload = dict(document)
    del logical_payload["candidate_manifest_id"]
    if stable_digest(logical_payload) != manifest_id:
        raise IdentityBounceError("candidate_manifest_id does not reproduce")
    cases = document["cases"]
    examples = document["bounded_examples"]
    if not isinstance(cases, list) or not all(isinstance(item, dict) for item in cases):
        raise IdentityBounceError("candidate manifest cases must be an object array")
    if not isinstance(examples, list) or not all(isinstance(item, dict) for item in examples):
        raise IdentityBounceError("candidate manifest examples must be an object array")
    case_count = _native_nonnegative_int(document["case_count"], "case_count")
    limit = _native_positive_int(document["bounded_example_limit"], "bounded_example_limit")
    if limit > 100 or len(cases) != case_count or examples != cases[:limit]:
        raise IdentityBounceError("candidate manifest case/example counts do not reconcile")
    manifest_available = _manifest_date(
        document["candidate_manifest_available_session"],
        "candidate_manifest_available_session",
    )
    case_ids: list[str] = []
    bounce_rows = 0
    band_counts: Counter[str] = Counter()
    reason_counts: Counter[str] = Counter()
    for row in cases:
        _validate_candidate_case_row(
            row,
            six_release_binding_id=six_release_binding_id,
            candidate_manifest_available_session=manifest_available,
        )
        case_ids.append(_digest(row["identity_case_id"], "case identity_case_id"))
        middle_count = _native_positive_int(
            row.get("middle_session_count"),
            "case middle_session_count",
        )
        bounce_rows += middle_count
        band = row.get("session_band")
        if band not in _SESSION_BANDS or band != _session_band(middle_count):
            raise IdentityBounceError("candidate case session band is invalid")
        band_counts[band] += 1
        reasons = row.get("reason_codes")
        if (
            not isinstance(reasons, list)
            or any(not isinstance(item, str) or not item for item in reasons)
            or reasons != sorted(set(reasons))
        ):
            raise IdentityBounceError("candidate case reason codes are not sorted unique text")
        reason_counts.update(reasons)
    if case_ids != sorted(case_ids) or len(set(case_ids)) != len(case_ids):
        raise IdentityBounceError("candidate cases must have sorted unique identity_case_ids")
    if document["session_band_counts"] != {band: band_counts[band] for band in _SESSION_BANDS}:
        raise IdentityBounceError("candidate manifest session-band counts do not reconcile")
    if document["support_reason_counts"] != dict(sorted(reason_counts.items())):
        raise IdentityBounceError("candidate manifest support-reason counts do not reconcile")
    if document["suspected_provider_figi_bounce_rows"] != bounce_rows:
        raise IdentityBounceError("candidate manifest bounce-row numerator does not reconcile")
    input_summary = document["input_summary"]
    if not isinstance(input_summary, dict) or set(input_summary) != {
        "incomplete_source_session_count",
        "observation_count",
        "source_session_count",
        "valid_active_observation_count",
    }:
        raise IdentityBounceError("candidate manifest input summary is malformed")
    source_sessions = _native_nonnegative_int(
        input_summary["source_session_count"],
        "input source_session_count",
    )
    incomplete_sessions = _native_nonnegative_int(
        input_summary["incomplete_source_session_count"],
        "input incomplete_source_session_count",
    )
    observations = _native_nonnegative_int(
        input_summary["observation_count"],
        "input observation_count",
    )
    valid_observations = _native_nonnegative_int(
        input_summary["valid_active_observation_count"],
        "input valid_active_observation_count",
    )
    if incomplete_sessions > source_sessions or valid_observations > observations:
        raise IdentityBounceError("candidate manifest input summary counts are inconsistent")
    created_at_text = _exact_text(document["created_at_utc"], "created_at_utc")
    try:
        created_at = datetime.fromisoformat(created_at_text)
    except ValueError as exc:
        raise IdentityBounceError("created_at_utc is not an ISO datetime") from exc
    if _utc_datetime(created_at, "created_at_utc").isoformat() != created_at_text:
        raise IdentityBounceError("created_at_utc is not in canonical UTC ISO format")
    try:
        require_first_xnys_open_session(
            created_at,
            manifest_available,
            label="candidate manifest availability",
        )
    except SilverAvailabilityError as exc:
        raise IdentityBounceError(str(exc)) from exc


def _validate_candidate_case_row(
    row: dict[str, Any],
    *,
    six_release_binding_id: str,
    candidate_manifest_available_session: date,
) -> None:
    expected_keys = {
        "detector_disposition",
        "detector_rule_version",
        "episode_source_record_ids",
        "episode_source_record_set_digest",
        "episode_valid_from_session",
        "episode_valid_through_session",
        "hierarchy_source_record_ids",
        "hierarchy_support_count",
        "identity_case_available_session",
        "identity_case_id",
        "left_outer_composite_figi",
        "left_outer_source_record_id",
        "middle_observed_composite_figi",
        "middle_session_count",
        "reason_codes",
        "right_evidence_available_session",
        "right_outer_composite_figi",
        "right_outer_source_record_id",
        "s5_source_record_ids",
        "s5_support_count",
        "s6_source_record_ids",
        "s6_support_count",
        "session_band",
        "six_release_binding_id",
        "ticker",
    }
    if set(row) != expected_keys:
        raise IdentityBounceError("candidate case has unexpected or missing fields")
    if (
        row["detector_disposition"] != "review_required_no_auto_decision"
        or row["detector_rule_version"] != DETECTOR_RULE_VERSION
        or row["six_release_binding_id"] != six_release_binding_id
    ):
        raise IdentityBounceError("candidate case fixed detector fields are invalid")
    ticker = _exact_text(row["ticker"], "case ticker")
    left_figi = _exact_text(
        row["left_outer_composite_figi"],
        "case left outer Composite FIGI",
    )
    middle_figi = _exact_text(
        row["middle_observed_composite_figi"],
        "case middle observed Composite FIGI",
    )
    right_figi = _exact_text(
        row["right_outer_composite_figi"],
        "case right outer Composite FIGI",
    )
    if (
        not all(_FIGI.fullmatch(item) for item in (left_figi, middle_figi, right_figi))
        or left_figi != right_figi
        or left_figi == middle_figi
    ):
        raise IdentityBounceError("candidate case does not contain a valid A/B/A FIGI pattern")
    left_source_id = _exact_text(
        row["left_outer_source_record_id"],
        "case left outer source_record_id",
    )
    right_source_id = _exact_text(
        row["right_outer_source_record_id"],
        "case right outer source_record_id",
    )
    start = _manifest_date(
        row["episode_valid_from_session"],
        "case episode_valid_from_session",
    )
    end = _manifest_date(
        row["episode_valid_through_session"],
        "case episode_valid_through_session",
    )
    if start > end:
        raise IdentityBounceError("candidate case episode bounds are reversed")
    middle_count = _native_positive_int(
        row["middle_session_count"],
        "case middle_session_count",
    )
    if middle_count > MAX_MIDDLE_SESSIONS or row["session_band"] != _session_band(middle_count):
        raise IdentityBounceError("candidate case middle-run band is invalid")
    episode_ids = _manifest_text_tuple(
        row["episode_source_record_ids"],
        "case episode source-record IDs",
        require_sorted=False,
    )
    if len(episode_ids) != middle_count:
        raise IdentityBounceError("candidate case episode source-record count is invalid")
    episode_digest = _digest(
        row["episode_source_record_set_digest"],
        "case episode source-record-set digest",
    )
    if episode_digest != stable_digest(sorted(set(episode_ids))):
        raise IdentityBounceError("candidate case episode source-record-set digest drifted")
    right_available = _manifest_date(
        row["right_evidence_available_session"],
        "case right_evidence_available_session",
    )
    case_available = _manifest_date(
        row["identity_case_available_session"],
        "case identity_case_available_session",
    )
    if case_available != max(right_available, candidate_manifest_available_session):
        raise IdentityBounceError("candidate case availability does not reproduce")
    s5_ids = _manifest_text_tuple(
        row["s5_source_record_ids"],
        "case S5 source-record IDs",
    )
    s6_ids = _manifest_text_tuple(
        row["s6_source_record_ids"],
        "case S6 source-record IDs",
    )
    hierarchy_ids = _manifest_text_tuple(
        row["hierarchy_source_record_ids"],
        "case hierarchy source-record IDs",
    )
    if (
        row["s5_support_count"] != len(s5_ids)
        or row["s6_support_count"] != len(s6_ids)
        or row["hierarchy_support_count"] != len(hierarchy_ids)
    ):
        raise IdentityBounceError("candidate case support counts do not reconcile")
    reasons = _manifest_text_tuple(row["reason_codes"], "case reason codes")
    support = BounceCorroboration(
        ticker=ticker,
        middle_observed_composite_figi=middle_figi,
        episode_valid_from_session=start,
        episode_valid_through_session=end,
        s5_source_record_ids=s5_ids,
        s6_source_record_ids=s6_ids,
        hierarchy_source_record_ids=hierarchy_ids,
    )
    if reasons != _reason_codes(support):
        raise IdentityBounceError("candidate case reason codes do not reproduce")
    case_payload = {
        "namespace": "ame_stocks.identity.provider_figi_bounce_case",
        "rule_version": CASE_ID_RULE_VERSION,
        "six_release_binding_id": six_release_binding_id,
        "detector_rule_version": DETECTOR_RULE_VERSION,
        "ticker": ticker,
        "left_outer_composite_figi": left_figi,
        "middle_observed_composite_figi": middle_figi,
        "right_outer_composite_figi": right_figi,
        "left_outer_source_record_id": left_source_id,
        "right_outer_source_record_id": right_source_id,
        "episode_valid_from_session": start.isoformat(),
        "episode_valid_through_session": end.isoformat(),
        "episode_source_record_set_digest": episode_digest,
    }
    if row["identity_case_id"] != stable_digest(case_payload):
        raise IdentityBounceError("candidate case identity_case_id does not reproduce")


def _bounce_case_from_manifest_row(value: object) -> BounceCase:
    """Rehydrate one row only after the enclosing manifest passed strict validation."""

    if not isinstance(value, Mapping):
        raise IdentityBounceError("candidate case row must be an object")
    row = dict(value)

    def text(name: str) -> str:
        return _exact_text(row.get(name), f"candidate case {name}")

    def text_tuple(name: str, *, require_sorted: bool = True) -> tuple[str, ...]:
        return _manifest_text_tuple(
            row.get(name),
            f"candidate case {name}",
            require_sorted=require_sorted,
        )

    return BounceCase(
        identity_case_id=text("identity_case_id"),
        six_release_binding_id=text("six_release_binding_id"),
        ticker=text("ticker"),
        left_outer_composite_figi=text("left_outer_composite_figi"),
        middle_observed_composite_figi=text("middle_observed_composite_figi"),
        right_outer_composite_figi=text("right_outer_composite_figi"),
        left_outer_source_record_id=text("left_outer_source_record_id"),
        right_outer_source_record_id=text("right_outer_source_record_id"),
        episode_valid_from_session=_manifest_date(
            row.get("episode_valid_from_session"),
            "candidate case episode_valid_from_session",
        ),
        episode_valid_through_session=_manifest_date(
            row.get("episode_valid_through_session"),
            "candidate case episode_valid_through_session",
        ),
        episode_source_record_ids=text_tuple(
            "episode_source_record_ids",
            require_sorted=False,
        ),
        episode_source_record_set_digest=text("episode_source_record_set_digest"),
        middle_session_count=_native_positive_int(
            row.get("middle_session_count"),
            "candidate case middle_session_count",
        ),
        session_band=text("session_band"),
        right_evidence_available_session=_manifest_date(
            row.get("right_evidence_available_session"),
            "candidate case right_evidence_available_session",
        ),
        identity_case_available_session=_manifest_date(
            row.get("identity_case_available_session"),
            "candidate case identity_case_available_session",
        ),
        s5_source_record_ids=text_tuple("s5_source_record_ids"),
        s6_source_record_ids=text_tuple("s6_source_record_ids"),
        hierarchy_source_record_ids=text_tuple("hierarchy_source_record_ids"),
        reason_codes=text_tuple("reason_codes"),
    )


def _manifest_date(value: object, label: str) -> date:
    text = _exact_text(value, label)
    try:
        parsed = date.fromisoformat(text)
    except ValueError as exc:
        raise IdentityBounceError(f"{label} is not an ISO date") from exc
    if parsed.isoformat() != text:
        raise IdentityBounceError(f"{label} is not in canonical ISO format")
    return parsed


def _manifest_text_tuple(
    value: object,
    label: str,
    *,
    require_sorted: bool = True,
) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise IdentityBounceError(f"{label} must be an array")
    values = tuple(value)
    if any(not isinstance(item, str) or not item or item != item.strip() for item in values):
        raise IdentityBounceError(f"{label} must contain non-empty exact text")
    if len(set(values)) != len(values):
        raise IdentityBounceError(f"{label} must be unique")
    if require_sorted and values != tuple(sorted(values)):
        raise IdentityBounceError(f"{label} must be lexicographically sorted")
    return values


def _digest(value: object, label: str) -> str:
    if not isinstance(value, str) or not _SHA256.fullmatch(value):
        raise IdentityBounceError(f"{label} must be a lowercase SHA-256 digest")
    return value


def _exact_text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise IdentityBounceError(f"{label} must be non-empty exact text without edge whitespace")
    return value


def _native_bool(value: object, label: str) -> bool:
    if type(value) is not bool:
        raise IdentityBounceError(f"{label} must be a native bool")
    return value


def _native_date(value: object, label: str) -> date:
    if type(value) is not date:
        raise IdentityBounceError(f"{label} must be a native date")
    return value


def _native_nonnegative_int(value: object, label: str) -> int:
    if type(value) is not int or value < 0:
        raise IdentityBounceError(f"{label} must be a nonnegative native int")
    return value


def _native_positive_int(value: object, label: str) -> int:
    if type(value) is not int or value <= 0:
        raise IdentityBounceError(f"{label} must be a positive native int")
    return value


def _sorted_unique_text_tuple(values: object, label: str) -> tuple[str, ...]:
    if not isinstance(values, tuple):
        raise IdentityBounceError(f"{label} must be a tuple")
    if any(not isinstance(item, str) or not item or item != item.strip() for item in values):
        raise IdentityBounceError(f"{label} must contain non-empty exact text")
    if values != tuple(sorted(set(values))):
        raise IdentityBounceError(f"{label} must be lexicographically sorted and unique")
    return values


def _utc_datetime(value: object, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise IdentityBounceError(f"{label} must be timezone-aware")
    normalized = value.astimezone(UTC)
    if value.utcoffset() != normalized.utcoffset():
        raise IdentityBounceError(f"{label} must be expressed in UTC")
    return normalized
