"""Pure S7 cross-market identity controls and fixture resolver.

The bounce detector answers whether one provider series contains ``A -> B -> A``.  It
cannot answer whether A or B belongs to the provider locale.  This module adds that
orthogonal control without rewriting either the provider observation or the existing
bounce-case lineage.

It is deliberately source-independent and has no production ingress.  A future S7
materializer must load observations, the market-consistency candidate manifest, and an
exact published cross-market adjudication release before calling equivalent logic.
"""

from __future__ import annotations

import re
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import StrEnum
from types import MappingProxyType

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver.contracts import SilverContractError

_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_FIGI = re.compile(r"^BBG[0-9A-Z]{9}$")
_CODE = re.compile(r"^[A-Z0-9]{2,8}$")

POSITION_CONTINUITY_RESOLVED = "resolved_identity"
POSITION_CONTINUITY_UNCERTAIN = (
    "identity_uncertain_no_new_trade_no_forced_exit_run_incomplete"
)


class CrossMarketIdentityError(SilverContractError):
    """Raised when a cross-market control or fixture view is unsafe."""


class CompositeMarketClass(StrEnum):
    US = "us"
    NON_US = "non_us"


class IdentityCaseResolutionRole(StrEnum):
    CONTAMINATED_MIDDLE_EPISODE = "contaminated_middle_episode"
    INVERSE_MIDDLE_IS_CANONICAL_US = "inverse_middle_is_canonical_us"


class CrossMarketAdjudicationDisposition(StrEnum):
    CONFIRMED_PROVIDER_CONTAMINATION = "confirmed_provider_contamination"
    ADJUDICATED_UNRESOLVED = "cross_market_adjudicated_unresolved"


@dataclass(frozen=True, slots=True)
class CompositeMarketReference:
    """One immutable identifier-authority assertion for a Composite FIGI."""

    composite_figi: str
    share_class_figi: str
    composite_market_code: str
    market_class: CompositeMarketClass
    external_evidence_manifest_id: str
    external_evidence_manifest_sha256: str
    evidence_available_session: date

    def __post_init__(self) -> None:
        _figi(self.composite_figi, "reference Composite FIGI")
        _figi(self.share_class_figi, "reference Share Class FIGI")
        if not _CODE.fullmatch(self.composite_market_code):
            raise CrossMarketIdentityError("reference market code is invalid")
        if not isinstance(self.market_class, CompositeMarketClass):
            raise CrossMarketIdentityError("reference market class is invalid")
        if (self.market_class is CompositeMarketClass.US) != (
            self.composite_market_code == "US"
        ):
            raise CrossMarketIdentityError(
                "US market classification must be represented by exact code US"
            )
        _digest(self.external_evidence_manifest_id, "external evidence manifest ID")
        _digest(
            self.external_evidence_manifest_sha256,
            "external evidence manifest SHA-256",
        )
        _date(self.evidence_available_session, "evidence available session")


@dataclass(frozen=True, slots=True)
class LinkedIdentityCase:
    identity_case_id: str
    role: IdentityCaseResolutionRole

    def __post_init__(self) -> None:
        _digest(self.identity_case_id, "identity case ID")
        if not isinstance(self.role, IdentityCaseResolutionRole):
            raise CrossMarketIdentityError("identity case role is invalid")


@dataclass(frozen=True, slots=True)
class CrossMarketIdentityObservation:
    """Minimum exact S4 lineage needed by the cross-market fixture engine."""

    session_date: date
    provider_id: str
    provider_market: str
    provider_locale: str
    ticker: str
    active_on_date: bool
    primary_exchange_mic: str | None
    observed_composite_figi: str
    observed_share_class_figi: str
    source_record_id: str
    source_s4_release_set_id: str
    relationship_conflict: bool = False
    identity_case_id: str | None = None
    identity_case_resolution_role: IdentityCaseResolutionRole | None = None

    def __post_init__(self) -> None:
        _date(self.session_date, "observation session")
        for value, label in (
            (self.provider_id, "provider ID"),
            (self.provider_market, "provider market"),
            (self.provider_locale, "provider locale"),
            (self.ticker, "ticker"),
        ):
            _text(value, label)
        if type(self.active_on_date) is not bool:
            raise CrossMarketIdentityError("active_on_date must be a native bool")
        if self.primary_exchange_mic is not None and not re.fullmatch(
            r"[A-Z0-9]{4}", self.primary_exchange_mic
        ):
            raise CrossMarketIdentityError("primary exchange MIC is invalid")
        _figi(self.observed_composite_figi, "observed Composite FIGI")
        _figi(self.observed_share_class_figi, "observed Share Class FIGI")
        _digest(self.source_record_id, "source record ID")
        _digest(self.source_s4_release_set_id, "source S4 release-set ID")
        if type(self.relationship_conflict) is not bool:
            raise CrossMarketIdentityError("relationship_conflict must be a native bool")
        if (self.identity_case_id is None) != (
            self.identity_case_resolution_role is None
        ):
            raise CrossMarketIdentityError(
                "identity case ID and cross-market case role must be jointly null"
            )
        if self.identity_case_id is not None:
            _digest(self.identity_case_id, "identity case ID")
        if self.identity_case_resolution_role is not None and not isinstance(
            self.identity_case_resolution_role, IdentityCaseResolutionRole
        ):
            raise CrossMarketIdentityError("identity case role is invalid")


@dataclass(frozen=True, slots=True)
class ApprovedCrossMarketAdjudication:
    """One approved, versioned, exact-scope provider-locale override."""

    provider_id: str
    provider_market: str
    provider_locale: str
    ticker: str
    share_class_figi: str
    observed_foreign_composite_figi: str
    disposition: CrossMarketAdjudicationDisposition
    canonical_us_composite_figi: str | None
    observed_composite_market_code: str
    canonical_composite_market_code: str | None
    valid_from_session: date
    valid_through_session: date
    scoped_source_record_ids: tuple[str, ...]
    source_s4_release_set_id: str
    source_six_release_binding_id: str
    source_market_consistency_candidate_manifest_id: str
    source_market_consistency_candidate_manifest_sha256: str
    candidate_available_session: date
    source_external_evidence_manifest_id: str
    source_external_evidence_manifest_sha256: str
    external_evidence_available_session: date
    approval_receipt_id: str
    approval_receipt_sha256: str
    approved_by: str
    approved_at_utc: datetime
    approval_available_session: date
    decision_version: int
    supersedes_cross_market_adjudication_id: str | None
    linked_identity_cases: tuple[LinkedIdentityCase, ...]
    reason_code: str
    reason_detail: str

    def __post_init__(self) -> None:
        for value, label in (
            (self.provider_id, "provider ID"),
            (self.provider_market, "provider market"),
            (self.provider_locale, "provider locale"),
            (self.ticker, "ticker"),
            (self.approved_by, "approval actor"),
            (self.reason_code, "reason code"),
            (self.reason_detail, "reason detail"),
        ):
            _text(value, label)
        if self.provider_locale != "us":
            raise CrossMarketIdentityError("cross-market override locale must be exact us")
        _figi(self.share_class_figi, "scope Share Class FIGI")
        _figi(self.observed_foreign_composite_figi, "observed foreign Composite FIGI")
        if not isinstance(self.disposition, CrossMarketAdjudicationDisposition):
            raise CrossMarketIdentityError("cross-market disposition is invalid")
        if (
            not _CODE.fullmatch(self.observed_composite_market_code)
            or self.observed_composite_market_code == "US"
        ):
            raise CrossMarketIdentityError("observed market code must be non-US")
        if (
            self.disposition
            is CrossMarketAdjudicationDisposition.CONFIRMED_PROVIDER_CONTAMINATION
        ):
            _figi(self.canonical_us_composite_figi, "canonical US Composite FIGI")
            if self.observed_foreign_composite_figi == self.canonical_us_composite_figi:
                raise CrossMarketIdentityError(
                    "cross-market override must change the canonical FIGI"
                )
            if self.canonical_composite_market_code != "US":
                raise CrossMarketIdentityError("canonical market code must be US")
        elif (
            self.canonical_us_composite_figi is not None
            or self.canonical_composite_market_code is not None
        ):
            raise CrossMarketIdentityError(
                "cross-market adjudicated unresolved cannot carry a canonical target"
            )
        _date(self.valid_from_session, "valid-from session")
        _date(self.valid_through_session, "valid-through session")
        if self.valid_from_session > self.valid_through_session:
            raise CrossMarketIdentityError("cross-market validity interval is reversed")
        source_ids = tuple(sorted(self.scoped_source_record_ids))
        if not source_ids or len(source_ids) != len(set(source_ids)):
            raise CrossMarketIdentityError("scoped source records must be nonempty and unique")
        for source_id in source_ids:
            _digest(source_id, "scoped source record ID")
        object.__setattr__(self, "scoped_source_record_ids", source_ids)
        for value, label in (
            (self.source_s4_release_set_id, "source S4 release-set ID"),
            (self.source_six_release_binding_id, "six-release binding ID"),
            (
                self.source_market_consistency_candidate_manifest_id,
                "market-consistency candidate manifest ID",
            ),
            (
                self.source_market_consistency_candidate_manifest_sha256,
                "market-consistency candidate manifest SHA-256",
            ),
            (
                self.source_external_evidence_manifest_id,
                "external evidence manifest ID",
            ),
            (
                self.source_external_evidence_manifest_sha256,
                "external evidence manifest SHA-256",
            ),
            (self.approval_receipt_id, "approval receipt ID"),
            (self.approval_receipt_sha256, "approval receipt SHA-256"),
        ):
            _digest(value, label)
        for value, label in (
            (self.candidate_available_session, "candidate available session"),
            (
                self.external_evidence_available_session,
                "external evidence available session",
            ),
            (self.approval_available_session, "approval available session"),
        ):
            _date(value, label)
        approved_at = self.approved_at_utc
        if not isinstance(approved_at, datetime) or approved_at.tzinfo is None:
            raise CrossMarketIdentityError("approved_at_utc must be timezone-aware")
        approved_at = approved_at.astimezone(UTC)
        object.__setattr__(self, "approved_at_utc", approved_at)
        if self.approval_available_session < approved_at.date():
            raise CrossMarketIdentityError("approval availability is backdated")
        if type(self.decision_version) is not int or self.decision_version <= 0:
            raise CrossMarketIdentityError("decision version must be positive")
        if (self.decision_version == 1) != (
            self.supersedes_cross_market_adjudication_id is None
        ):
            raise CrossMarketIdentityError(
                "only decision version 1 may have a null predecessor"
            )
        if self.supersedes_cross_market_adjudication_id is not None:
            _digest(
                self.supersedes_cross_market_adjudication_id,
                "superseded cross-market adjudication ID",
            )
        cases = tuple(sorted(self.linked_identity_cases, key=lambda item: item.identity_case_id))
        if len({item.identity_case_id for item in cases}) != len(cases):
            raise CrossMarketIdentityError("linked identity case IDs are repeated")
        object.__setattr__(self, "linked_identity_cases", cases)

    @property
    def scoped_source_record_set_digest(self) -> str:
        return stable_digest(list(self.scoped_source_record_ids))

    @property
    def cross_market_subject_id(self) -> str:
        return stable_digest(
            {
                "namespace": "ame_stocks.identity.cross_market_subject",
                "observed_foreign_composite_figi": self.observed_foreign_composite_figi,
                "provider_id": self.provider_id,
                "provider_locale": self.provider_locale,
                "provider_market": self.provider_market,
                "rule_version": "s7_cross_market_subject_id_v1",
                "share_class_figi": self.share_class_figi,
                "ticker": self.ticker,
            }
        )

    @property
    def cross_market_scope_id(self) -> str:
        return stable_digest(
            {
                "canonical_us_composite_figi": self.canonical_us_composite_figi,
                "canonical_composite_market_code": self.canonical_composite_market_code,
                "cross_market_subject_id": self.cross_market_subject_id,
                "disposition": self.disposition.value,
                "namespace": "ame_stocks.identity.cross_market_scope",
                "observed_composite_market_code": self.observed_composite_market_code,
                "rule_version": "s7_cross_market_scope_id_v2",
                "scoped_source_record_set_digest": self.scoped_source_record_set_digest,
                "source_identity_market_consistency_candidate_manifest_id": (
                    self.source_market_consistency_candidate_manifest_id
                ),
                "source_s4_release_set_id": self.source_s4_release_set_id,
                "valid_from_session": self.valid_from_session.isoformat(),
                "valid_through_session": self.valid_through_session.isoformat(),
            }
        )

    @property
    def cross_market_series_id(self) -> str:
        return stable_digest(
            {
                "cross_market_subject_id": self.cross_market_subject_id,
                "namespace": "ame_stocks.identity.cross_market_adjudication_series",
                "rule_version": "s7_cross_market_adjudication_series_id_v1",
            }
        )

    @property
    def cross_market_adjudication_id(self) -> str:
        return stable_digest(
            {
                "canonical_us_composite_figi": self.canonical_us_composite_figi,
                "cross_market_scope_id": self.cross_market_scope_id,
                "cross_market_series_id": self.cross_market_series_id,
                "decision_version": self.decision_version,
                "disposition": self.disposition.value,
                "namespace": "ame_stocks.identity.cross_market_adjudication",
                "reason_code": self.reason_code,
                "reason_detail": self.reason_detail,
                "rule_version": "s7_cross_market_adjudication_id_v1",
                "source_external_evidence_manifest_id": (
                    self.source_external_evidence_manifest_id
                ),
                "supersedes_cross_market_adjudication_id": (
                    self.supersedes_cross_market_adjudication_id
                ),
            }
        )

    @property
    def adjudication_available_session(self) -> date:
        return max(
            self.candidate_available_session,
            self.external_evidence_available_session,
            self.approval_available_session,
        )

    def matches_foreign_observation(self, row: CrossMarketIdentityObservation) -> bool:
        """Exact provider-locale, identifier, date, release, and source-row match."""

        return (
            row.provider_id == self.provider_id
            and row.provider_market == self.provider_market
            and row.provider_locale == self.provider_locale
            and row.ticker == self.ticker
            and row.observed_share_class_figi == self.share_class_figi
            and row.observed_composite_figi == self.observed_foreign_composite_figi
            and self.valid_from_session <= row.session_date <= self.valid_through_session
            and row.source_s4_release_set_id == self.source_s4_release_set_id
            and row.source_record_id in self.scoped_source_record_ids
        )

    def linked_inverse_case(self, row: CrossMarketIdentityObservation) -> bool:
        if (
            row.identity_case_id is None
            or row.identity_case_resolution_role
            is not IdentityCaseResolutionRole.INVERSE_MIDDLE_IS_CANONICAL_US
        ):
            return False
        return (
            row.provider_id == self.provider_id
            and row.provider_market == self.provider_market
            and row.provider_locale == self.provider_locale
            and row.ticker == self.ticker
            and row.observed_share_class_figi == self.share_class_figi
            and self.canonical_us_composite_figi is not None
            and row.observed_composite_figi == self.canonical_us_composite_figi
            and row.source_s4_release_set_id == self.source_s4_release_set_id
            and any(
                item.identity_case_id == row.identity_case_id
                and item.role
                is IdentityCaseResolutionRole.INVERSE_MIDDLE_IS_CANONICAL_US
                for item in self.linked_identity_cases
            )
        )


@dataclass(frozen=True, slots=True)
class CrossMarketResolutionDecision:
    session_date: date
    provider_id: str
    provider_market: str
    provider_locale: str
    ticker: str
    active_on_date: bool
    source_record_id: str
    observed_composite_figi: str
    observed_share_class_figi: str
    observed_composite_market_code: str | None
    canonical_composite_figi: str | None
    canonical_composite_market_code: str | None
    canonical_override: bool
    cross_market_classification_status: str
    identity_resolution_status: str
    identity_resolution_method: str
    identity_disposition: str
    identity_case_id: str | None
    identity_case_resolution_role: str | None
    cross_market_scope_id: str | None
    cross_market_adjudication_id: str | None
    backtest_identity_eligible: bool
    alias_emitted: bool
    position_continuity_status: str
    identity_quality_membership_mutated: bool = False
    identity_quality_liquidation_signal: bool = False


@dataclass(frozen=True, slots=True)
class CrossMarketAudit:
    observation_rows: int
    us_locale_non_us_composite_figi_rows: int
    us_locale_non_us_reason_counts: Mapping[str, int]
    us_locale_non_us_bounded_examples: tuple[Mapping[str, object], ...]
    unapproved_cross_market_composite_eligible_rows: int
    inverse_bounce_misclassified_as_genuine_transition_rows: int
    cross_market_override_foreign_locale_leak_rows: int
    correct_us_observation_overridden_rows: int
    correct_us_observation_ineligible_due_only_to_inverse_bounce_rows: int
    figi_market_classification_uncovered_rows: int
    identity_quality_membership_mutation_rows: int
    identity_quality_liquidation_signal_rows: int


@dataclass(frozen=True, slots=True)
class CrossMarketResolutionResult:
    decisions: tuple[CrossMarketResolutionDecision, ...]
    audit: CrossMarketAudit
    effective_adjudication_ids_by_scope: Mapping[str, str]


def resolve_cross_market_identity(
    observations: Sequence[CrossMarketIdentityObservation],
    market_references: Sequence[CompositeMarketReference],
    adjudications: Sequence[ApprovedCrossMarketAdjudication],
    *,
    cutoff_session: date,
    bounded_example_limit: int = 20,
) -> CrossMarketResolutionResult:
    """Resolve exact cross-market scopes while preserving every observed value."""

    _date(cutoff_session, "resolution cutoff session")
    if type(bounded_example_limit) is not int or not 1 <= bounded_example_limit <= 100:
        raise CrossMarketIdentityError("bounded example limit must be in 1..100")

    rows = tuple(observations)
    if not rows:
        raise CrossMarketIdentityError("cross-market resolver requires observations")
    row_keys = [
        (
            row.provider_id,
            row.provider_market,
            row.provider_locale,
            row.session_date,
            row.ticker,
        )
        for row in rows
    ]
    if len(row_keys) != len(set(row_keys)):
        raise CrossMarketIdentityError("observation key is not unique")
    if any(row.session_date > cutoff_session for row in rows):
        raise CrossMarketIdentityError("observation is later than the physical cutoff")

    references = tuple(market_references)
    by_composite = {item.composite_figi: item for item in references}
    if len(by_composite) != len(references):
        raise CrossMarketIdentityError("Composite market reference is duplicated")

    effective = _effective_adjudications(tuple(adjudications), cutoff_session=cutoff_session)
    _validate_adjudication_references(effective, by_composite, cutoff_session=cutoff_session)

    decisions: list[CrossMarketResolutionDecision] = []
    reason_counts: Counter[str] = Counter()
    bounded_examples: list[Mapping[str, object]] = []
    for row in sorted(
        rows,
        key=lambda item: (
            item.session_date,
            item.provider_id,
            item.provider_locale,
            item.ticker,
        ),
    ):
        reference = by_composite.get(row.observed_composite_figi)
        if reference is not None and reference.evidence_available_session > cutoff_session:
            reference = None
        matching_overrides = [
            item for item in effective.values() if item.matches_foreign_observation(row)
        ]
        if len(matching_overrides) > 1:
            raise CrossMarketIdentityError("one observation matches overlapping overrides")

        if (
            row.provider_locale == "us"
            and reference is not None
            and reference.market_class is CompositeMarketClass.NON_US
        ):
            reason = "known_non_us_composite_in_us_provider_locale"
            if row.primary_exchange_mic in {"XNAS", "XNYS"}:
                reason = f"{reason}_with_us_primary_exchange"
            reason_counts[reason] += 1
            if len(bounded_examples) < bounded_example_limit:
                bounded_examples.append(
                    MappingProxyType(
                        {
                            "composite_market_code": reference.composite_market_code,
                            "observed_composite_figi": row.observed_composite_figi,
                            "session_date": row.session_date.isoformat(),
                            "source_record_id": row.source_record_id,
                            "ticker": row.ticker,
                        }
                    )
                )
            if reference.share_class_figi != row.observed_share_class_figi:
                decisions.append(_unresolved_foreign_decision(row, reference))
            elif matching_overrides:
                adjudication = matching_overrides[0]
                if (
                    adjudication.disposition
                    is CrossMarketAdjudicationDisposition.CONFIRMED_PROVIDER_CONTAMINATION
                ):
                    decisions.append(
                        _approved_foreign_decision(row, reference, adjudication)
                    )
                else:
                    decisions.append(
                        _unresolved_foreign_decision(
                            row, reference, adjudication=adjudication
                        )
                    )
            else:
                decisions.append(_unresolved_foreign_decision(row, reference))
            continue

        inverse_links = [
            item for item in effective.values() if item.linked_inverse_case(row)
        ]
        if len(inverse_links) > 1:
            raise CrossMarketIdentityError("one inverse case links to multiple overrides")
        if (
            row.provider_locale == "us"
            and reference is not None
            and reference.market_class is CompositeMarketClass.US
            and row.identity_case_resolution_role
            is IdentityCaseResolutionRole.INVERSE_MIDDLE_IS_CANONICAL_US
        ):
            decisions.append(
                _direct_decision(
                    row,
                    reference,
                    method="source_composite_figi_exact_with_inverse_bounce_resolution",
                    classification_status="inverse_us_observation",
                    linked_override=inverse_links[0] if inverse_links else None,
                )
            )
            continue

        classification_status = (
            "known_us"
            if reference is not None
            and reference.market_class is CompositeMarketClass.US
            and row.provider_locale == "us"
            else "known_non_us_foreign_locale"
            if reference is not None
            and reference.market_class is CompositeMarketClass.NON_US
            and row.provider_locale != "us"
            else "not_classified"
        )
        decisions.append(
            _direct_decision(
                row,
                reference,
                method="source_composite_figi_exact",
                classification_status=classification_status,
                linked_override=None,
            )
        )

    output = tuple(decisions)
    audit = _audit(
        output,
        reason_counts=reason_counts,
        bounded_examples=tuple(bounded_examples),
    )
    if any(
        (
            audit.unapproved_cross_market_composite_eligible_rows,
            audit.inverse_bounce_misclassified_as_genuine_transition_rows,
            audit.cross_market_override_foreign_locale_leak_rows,
            audit.correct_us_observation_overridden_rows,
            audit.identity_quality_membership_mutation_rows,
            audit.identity_quality_liquidation_signal_rows,
        )
    ):
        raise CrossMarketIdentityError("critical cross-market identity QA failed")
    return CrossMarketResolutionResult(
        decisions=output,
        audit=audit,
        effective_adjudication_ids_by_scope=MappingProxyType(
            {
                scope_id: item.cross_market_adjudication_id
                for scope_id, item in sorted(effective.items())
            }
        ),
    )


def _effective_adjudications(
    rows: tuple[ApprovedCrossMarketAdjudication, ...], *, cutoff_session: date
) -> dict[str, ApprovedCrossMarketAdjudication]:
    by_series: defaultdict[str, list[ApprovedCrossMarketAdjudication]] = defaultdict(list)
    ids = [item.cross_market_adjudication_id for item in rows]
    if len(ids) != len(set(ids)):
        raise CrossMarketIdentityError("cross-market adjudication ID is duplicated")
    for item in rows:
        by_series[item.cross_market_series_id].append(item)
    effective: dict[str, ApprovedCrossMarketAdjudication] = {}
    for versions in by_series.values():
        ordered = sorted(versions, key=lambda item: item.decision_version)
        if [item.decision_version for item in ordered] != list(
            range(1, len(ordered) + 1)
        ):
            raise CrossMarketIdentityError("cross-market versions are not contiguous")
        for index, item in enumerate(ordered):
            predecessor = (
                None if index == 0 else ordered[index - 1].cross_market_adjudication_id
            )
            if item.supersedes_cross_market_adjudication_id != predecessor:
                raise CrossMarketIdentityError("cross-market predecessor chain is invalid")
        available = [
            item
            for item in ordered
            if item.adjudication_available_session <= cutoff_session
        ]
        if not available:
            continue
        head = available[-1]
        if head.cross_market_scope_id in effective:
            raise CrossMarketIdentityError("cross-market scope has multiple effective series")
        effective[head.cross_market_scope_id] = head
    return effective


def _validate_adjudication_references(
    effective: Mapping[str, ApprovedCrossMarketAdjudication],
    references: Mapping[str, CompositeMarketReference],
    *,
    cutoff_session: date,
) -> None:
    for item in effective.values():
        try:
            foreign = references[item.observed_foreign_composite_figi]
        except KeyError as exc:
            raise CrossMarketIdentityError(
                "approved cross-market observation lacks frozen identifier evidence"
            ) from exc
        if (
            foreign.market_class is not CompositeMarketClass.NON_US
            or foreign.composite_market_code != item.observed_composite_market_code
            or foreign.share_class_figi != item.share_class_figi
        ):
            raise CrossMarketIdentityError(
                "approved foreign scope conflicts with identifier evidence"
            )
        if (
            foreign.evidence_available_session > cutoff_session
            or foreign.external_evidence_manifest_id
            != item.source_external_evidence_manifest_id
            or foreign.external_evidence_manifest_sha256
            != item.source_external_evidence_manifest_sha256
        ):
            raise CrossMarketIdentityError(
                "cross-market identifier evidence is unavailable or differently bound"
            )
        if (
            item.disposition
            is CrossMarketAdjudicationDisposition.CONFIRMED_PROVIDER_CONTAMINATION
        ):
            assert item.canonical_us_composite_figi is not None
            try:
                canonical = references[item.canonical_us_composite_figi]
            except KeyError as exc:
                raise CrossMarketIdentityError(
                    "approved cross-market target lacks frozen identifier evidence"
                ) from exc
            if (
                canonical.market_class is not CompositeMarketClass.US
                or canonical.composite_market_code
                != item.canonical_composite_market_code
                or canonical.share_class_figi != item.share_class_figi
                or canonical.evidence_available_session > cutoff_session
                or canonical.external_evidence_manifest_id
                != item.source_external_evidence_manifest_id
                or canonical.external_evidence_manifest_sha256
                != item.source_external_evidence_manifest_sha256
            ):
                raise CrossMarketIdentityError(
                    "approved canonical target conflicts with identifier evidence"
                )


def _approved_foreign_decision(
    row: CrossMarketIdentityObservation,
    reference: CompositeMarketReference,
    adjudication: ApprovedCrossMarketAdjudication,
) -> CrossMarketResolutionDecision:
    eligible = row.active_on_date and not row.relationship_conflict
    assert adjudication.canonical_us_composite_figi is not None
    assert adjudication.canonical_composite_market_code is not None
    return CrossMarketResolutionDecision(
        session_date=row.session_date,
        provider_id=row.provider_id,
        provider_market=row.provider_market,
        provider_locale=row.provider_locale,
        ticker=row.ticker,
        active_on_date=row.active_on_date,
        source_record_id=row.source_record_id,
        observed_composite_figi=row.observed_composite_figi,
        observed_share_class_figi=row.observed_share_class_figi,
        observed_composite_market_code=reference.composite_market_code,
        canonical_composite_figi=adjudication.canonical_us_composite_figi,
        canonical_composite_market_code=adjudication.canonical_composite_market_code,
        canonical_override=True,
        cross_market_classification_status="known_non_us_overridden",
        identity_resolution_status=(
            "resolved_approved_override" if eligible else "resolved_conflicted"
        ),
        identity_resolution_method="approved_cross_market_provider_contamination_override",
        identity_disposition="confirmed_provider_contamination",
        identity_case_id=row.identity_case_id,
        identity_case_resolution_role=(
            None
            if row.identity_case_resolution_role is None
            else row.identity_case_resolution_role.value
        ),
        cross_market_scope_id=adjudication.cross_market_scope_id,
        cross_market_adjudication_id=adjudication.cross_market_adjudication_id,
        backtest_identity_eligible=eligible,
        alias_emitted=eligible,
        position_continuity_status=(
            POSITION_CONTINUITY_RESOLVED if eligible else POSITION_CONTINUITY_UNCERTAIN
        ),
    )


def _unresolved_foreign_decision(
    row: CrossMarketIdentityObservation,
    reference: CompositeMarketReference,
    *,
    adjudication: ApprovedCrossMarketAdjudication | None = None,
) -> CrossMarketResolutionDecision:
    withdrawn = adjudication is not None
    return CrossMarketResolutionDecision(
        session_date=row.session_date,
        provider_id=row.provider_id,
        provider_market=row.provider_market,
        provider_locale=row.provider_locale,
        ticker=row.ticker,
        active_on_date=row.active_on_date,
        source_record_id=row.source_record_id,
        observed_composite_figi=row.observed_composite_figi,
        observed_share_class_figi=row.observed_share_class_figi,
        observed_composite_market_code=reference.composite_market_code,
        canonical_composite_figi=None,
        canonical_composite_market_code=None,
        canonical_override=False,
        cross_market_classification_status=(
            "known_non_us_adjudicated_unresolved"
            if withdrawn
            else "known_non_us_pending"
        ),
        identity_resolution_status="unresolved",
        identity_resolution_method=(
            "cross_market_composite_adjudicated_unresolved"
            if withdrawn
            else "cross_market_composite_pending_unresolved"
        ),
        identity_disposition=(
            CrossMarketAdjudicationDisposition.ADJUDICATED_UNRESOLVED.value
            if withdrawn
            else "pending_cross_market_review"
        ),
        identity_case_id=row.identity_case_id,
        identity_case_resolution_role=(
            None
            if row.identity_case_resolution_role is None
            else row.identity_case_resolution_role.value
        ),
        cross_market_scope_id=(
            None if adjudication is None else adjudication.cross_market_scope_id
        ),
        cross_market_adjudication_id=(
            None
            if adjudication is None
            else adjudication.cross_market_adjudication_id
        ),
        backtest_identity_eligible=False,
        alias_emitted=False,
        position_continuity_status=POSITION_CONTINUITY_UNCERTAIN,
    )


def _direct_decision(
    row: CrossMarketIdentityObservation,
    reference: CompositeMarketReference | None,
    *,
    method: str,
    classification_status: str,
    linked_override: ApprovedCrossMarketAdjudication | None,
) -> CrossMarketResolutionDecision:
    eligible = row.active_on_date and not row.relationship_conflict
    return CrossMarketResolutionDecision(
        session_date=row.session_date,
        provider_id=row.provider_id,
        provider_market=row.provider_market,
        provider_locale=row.provider_locale,
        ticker=row.ticker,
        active_on_date=row.active_on_date,
        source_record_id=row.source_record_id,
        observed_composite_figi=row.observed_composite_figi,
        observed_share_class_figi=row.observed_share_class_figi,
        observed_composite_market_code=(
            None if reference is None else reference.composite_market_code
        ),
        canonical_composite_figi=row.observed_composite_figi,
        canonical_composite_market_code=(
            None if reference is None else reference.composite_market_code
        ),
        canonical_override=False,
        cross_market_classification_status=classification_status,
        identity_resolution_status="resolved_strong" if eligible else "resolved_conflicted",
        identity_resolution_method=method,
        identity_disposition="observed_consistent",
        identity_case_id=row.identity_case_id,
        identity_case_resolution_role=(
            None
            if row.identity_case_resolution_role is None
            else row.identity_case_resolution_role.value
        ),
        cross_market_scope_id=(
            None if linked_override is None else linked_override.cross_market_scope_id
        ),
        cross_market_adjudication_id=(
            None
            if linked_override is None
            else linked_override.cross_market_adjudication_id
        ),
        backtest_identity_eligible=eligible,
        alias_emitted=eligible,
        position_continuity_status=(
            POSITION_CONTINUITY_RESOLVED if eligible else POSITION_CONTINUITY_UNCERTAIN
        ),
    )


def _audit(
    decisions: tuple[CrossMarketResolutionDecision, ...],
    *,
    reason_counts: Counter[str],
    bounded_examples: tuple[Mapping[str, object], ...],
) -> CrossMarketAudit:
    known_non_us = sum(
        item.cross_market_classification_status
        in {
            "known_non_us_pending",
            "known_non_us_overridden",
            "known_non_us_adjudicated_unresolved",
        }
        for item in decisions
    )
    return CrossMarketAudit(
        observation_rows=len(decisions),
        us_locale_non_us_composite_figi_rows=known_non_us,
        us_locale_non_us_reason_counts=MappingProxyType(dict(sorted(reason_counts.items()))),
        us_locale_non_us_bounded_examples=bounded_examples,
        unapproved_cross_market_composite_eligible_rows=sum(
            item.cross_market_classification_status == "known_non_us_pending"
            and item.backtest_identity_eligible
            for item in decisions
        ),
        inverse_bounce_misclassified_as_genuine_transition_rows=sum(
            item.identity_case_resolution_role
            == IdentityCaseResolutionRole.INVERSE_MIDDLE_IS_CANONICAL_US.value
            and item.identity_disposition == "confirmed_genuine_transition"
            for item in decisions
        ),
        cross_market_override_foreign_locale_leak_rows=sum(
            item.provider_locale != "us" and item.canonical_override for item in decisions
        ),
        correct_us_observation_overridden_rows=sum(
            item.cross_market_classification_status == "inverse_us_observation"
            and item.canonical_override
            for item in decisions
        ),
        correct_us_observation_ineligible_due_only_to_inverse_bounce_rows=sum(
            item.cross_market_classification_status == "inverse_us_observation"
            and item.active_on_date
            and item.identity_resolution_status != "resolved_conflicted"
            and not item.backtest_identity_eligible
            for item in decisions
        ),
        figi_market_classification_uncovered_rows=sum(
            item.cross_market_classification_status == "not_classified"
            for item in decisions
        ),
        identity_quality_membership_mutation_rows=sum(
            item.identity_quality_membership_mutated for item in decisions
        ),
        identity_quality_liquidation_signal_rows=sum(
            item.identity_quality_liquidation_signal for item in decisions
        ),
    )


def _digest(value: object, label: str) -> str:
    if not isinstance(value, str) or not _DIGEST.fullmatch(value):
        raise CrossMarketIdentityError(f"{label} must be a lowercase SHA-256")
    return value


def _figi(value: object, label: str) -> str:
    if not isinstance(value, str) or not _FIGI.fullmatch(value):
        raise CrossMarketIdentityError(f"{label} must be a valid FIGI")
    return value


def _date(value: object, label: str) -> date:
    if type(value) is not date:
        raise CrossMarketIdentityError(f"{label} must be a date")
    return value


def _text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise CrossMarketIdentityError(f"{label} must be trimmed nonempty text")
    return value


__all__ = [
    "ApprovedCrossMarketAdjudication",
    "CompositeMarketClass",
    "CompositeMarketReference",
    "CrossMarketAdjudicationDisposition",
    "CrossMarketAudit",
    "CrossMarketIdentityError",
    "CrossMarketIdentityObservation",
    "CrossMarketResolutionDecision",
    "CrossMarketResolutionResult",
    "IdentityCaseResolutionRole",
    "LinkedIdentityCase",
    "resolve_cross_market_identity",
]
