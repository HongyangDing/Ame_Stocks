"""Cutoff-bound S7 identity resolution over immutable cases and approvals.

This module is intentionally a pure in-memory fixture engine.  It cannot discover releases,
write Silver data, or turn a detector finding into a decision.  The production entry point is
deliberately withheld until memberships can be loaded from an exact verified S4 source rather
than supplied as an arbitrary Python sequence.
"""

from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from enum import StrEnum
from types import MappingProxyType
from typing import Protocol

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver.contracts import SilverContractError
from ame_stocks_api.silver.identity_adjudication import (
    ApprovedIdentityDecision,
    IdentityAdjudicationRegistryRelease,
    LoadedIdentityAdjudicationRegistryRelease,
)
from ame_stocks_api.silver.identity_bounce import IdentityCaseCandidateManifest
from ame_stocks_api.silver.identity_source import S7_SIX_RELEASE_BINDING_ID

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_FIGI = re.compile(r"^BBG[0-9A-Z]{9}$")

ASSET_ID_RULE_VERSION = "ame_stocks_asset_id_from_composite_figi_v1"
ASSET_ID_NAMESPACE = "ame_stocks.identity.asset"
POSITION_CONTINUITY_RESOLVED = "resolved_identity"
POSITION_CONTINUITY_UNCERTAIN = "identity_uncertain_no_new_trade_no_forced_exit_run_incomplete"


class IdentityResolutionError(SilverContractError):
    """Raised when a cutoff view cannot be reproduced without guessing."""


class AdjudicationDisposition(StrEnum):
    CONFIRMED_GENUINE_TRANSITION = "confirmed_genuine_transition"
    CONFIRMED_PROVIDER_CONTAMINATION = "confirmed_provider_contamination"
    ADJUDICATED_UNRESOLVED = "adjudicated_unresolved"


class BounceCaseLike(Protocol):
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
    identity_case_available_session: date


class AdjudicationLike(Protocol):
    identity_adjudication_id: str
    adjudication_series_id: str
    decision_version: int
    supersedes_identity_adjudication_id: str | None
    identity_case_id: str
    identity_case_available_session: date
    observed_ticker: str
    observed_composite_figi: str
    episode_valid_from_session: date
    episode_valid_through_session: date
    episode_source_record_set_digest: str
    disposition: str
    canonical_composite_figi: str | None
    canonical_asset_id: str | None
    canonical_override: bool
    approval_status: str
    adjudication_available_session: date
    outcome_or_backtest_evidence_used: bool
    source_identity_case_candidate_manifest_id: str
    source_identity_case_candidate_manifest_sha256: str


@dataclass(frozen=True, slots=True)
class ObservedIdentityMembership:
    """The minimum S4 row needed by the pure identity decision engine."""

    session_date: date
    ticker: str
    active_on_date: bool
    observed_composite_figi: str | None
    source_record_id: str
    relationship_conflict: bool = False

    def __post_init__(self) -> None:
        if type(self.session_date) is not date:
            raise IdentityResolutionError("membership session_date must be a date")
        if (
            not isinstance(self.ticker, str)
            or not self.ticker
            or self.ticker != self.ticker.strip()
        ):
            raise IdentityResolutionError("membership ticker must be trimmed nonempty text")
        if type(self.active_on_date) is not bool or self.active_on_date is not True:
            raise IdentityResolutionError("S7 resolver accepts active membership rows only")
        if self.observed_composite_figi is not None and not isinstance(
            self.observed_composite_figi, str
        ):
            raise IdentityResolutionError("observed Composite FIGI must be text or null")
        _digest(self.source_record_id, "source_record_id")
        if type(self.relationship_conflict) is not bool:
            raise IdentityResolutionError("relationship_conflict must be a native bool")


@dataclass(frozen=True, slots=True)
class IdentityResolutionBinding:
    """Exact non-discoverable control artifacts for one physical cutoff view."""

    cutoff_session: date
    six_release_binding_id: str
    candidate_manifest_id: str
    candidate_manifest_sha256: str
    candidate_manifest_available_session: date
    adjudication_release_id: str
    adjudication_release_available_session: date

    def __post_init__(self) -> None:
        for label in (
            "cutoff_session",
            "candidate_manifest_available_session",
            "adjudication_release_available_session",
        ):
            if type(getattr(self, label)) is not date:
                raise IdentityResolutionError(f"{label} must be a date")
        for label in (
            "six_release_binding_id",
            "candidate_manifest_id",
            "candidate_manifest_sha256",
            "adjudication_release_id",
        ):
            _digest(getattr(self, label), label)
        if self.six_release_binding_id != S7_SIX_RELEASE_BINDING_ID:
            raise IdentityResolutionError("S7 resolver source binding differs from approval")
        if self.candidate_manifest_available_session > self.cutoff_session:
            raise IdentityResolutionError("candidate manifest was unavailable at the cutoff")
        if self.adjudication_release_available_session > self.cutoff_session:
            raise IdentityResolutionError("adjudication release was unavailable at the cutoff")


@dataclass(frozen=True, slots=True)
class IdentityResolutionDecision:
    session_date: date
    ticker: str
    source_record_id: str
    observed_composite_figi: str | None
    canonical_composite_figi: str | None
    canonical_asset_id: str | None
    identity_resolution_status: str
    identity_resolution_method: str
    identity_disposition: str
    identity_case_id: str | None
    identity_adjudication_id: str | None
    backtest_identity_eligible: bool
    alias_emitted: bool
    position_continuity_status: str
    identity_quality_liquidation_signal: bool = False


@dataclass(frozen=True, slots=True)
class IdentityResolutionAudit:
    active_membership_rows: int
    suspected_provider_figi_bounce_rows: int
    pending_or_adjudicated_unresolved_rows: int
    approved_provider_contamination_override_rows: int
    unapproved_canonical_identity_override_rows: int
    suspected_provider_contamination_eligible_rows: int
    identity_quality_liquidation_signal_rows: int


@dataclass(frozen=True, slots=True)
class IdentityResolutionResult:
    binding: IdentityResolutionBinding
    decisions: tuple[IdentityResolutionDecision, ...]
    audit: IdentityResolutionAudit
    effective_adjudication_ids_by_case: Mapping[str, str]


def canonical_asset_id(composite_figi: str) -> str:
    """Reproduce the frozen S7 canonical asset-ID rule."""

    _figi(composite_figi, "canonical Composite FIGI")
    return stable_digest(
        {
            "anchor_type": "composite_figi",
            "anchor_value": composite_figi,
            "namespace": ASSET_ID_NAMESPACE,
            "rule_version": ASSET_ID_RULE_VERSION,
        }
    )


def resolve_identity_at_cutoff(
    observations: Sequence[ObservedIdentityMembership],
    cases: Sequence[BounceCaseLike],
    adjudications: Sequence[AdjudicationLike],
    *,
    binding: IdentityResolutionBinding,
) -> IdentityResolutionResult:
    """Low-level fixture engine over already trusted controls.

    Production callers must use :func:`resolve_registry_release_at_cutoff`, which reloads
    and verifies the exact immutable registry chain before calling this pure function.
    """

    rows = tuple(observations)
    case_rows = tuple(cases)
    decision_rows = tuple(adjudications)
    _validate_memberships(rows)
    if any(item.session_date > binding.cutoff_session for item in rows):
        raise IdentityResolutionError("membership row is later than the physical cutoff")
    case_by_source_record = _validate_cases(rows, case_rows, binding=binding)
    effective = _effective_adjudications(case_rows, decision_rows, binding=binding)

    direct_anchor_figis = {
        row.observed_composite_figi
        for row in rows
        if row.source_record_id not in case_by_source_record
        and _is_figi(row.observed_composite_figi)
        and not row.relationship_conflict
    }
    output: list[IdentityResolutionDecision] = []
    for row in sorted(rows, key=lambda item: (item.session_date, item.ticker)):
        case = case_by_source_record.get(row.source_record_id)
        if case is None:
            output.append(_direct_decision(row))
            continue
        adjudication = effective.get(case.identity_case_id)
        output.append(
            _case_decision(
                row,
                case,
                adjudication,
                direct_anchor_figis=direct_anchor_figis,
            )
        )

    decisions = tuple(output)
    suspected_rows = sum(item.source_record_id in case_by_source_record for item in rows)
    unresolved_rows = sum(
        item.identity_disposition in {"pending_unresolved", "adjudicated_unresolved"}
        for item in decisions
    )
    override_rows = sum(
        item.identity_disposition == "confirmed_provider_contamination" for item in decisions
    )
    unapproved_overrides = sum(
        item.observed_composite_figi != item.canonical_composite_figi
        and item.canonical_composite_figi is not None
        and item.identity_disposition != "confirmed_provider_contamination"
        for item in decisions
    )
    unresolved_eligible = sum(
        item.identity_disposition in {"pending_unresolved", "adjudicated_unresolved"}
        and item.backtest_identity_eligible
        for item in decisions
    )
    liquidation_rows = sum(item.identity_quality_liquidation_signal for item in decisions)
    if unapproved_overrides or unresolved_eligible or liquidation_rows:
        raise IdentityResolutionError("S7 critical identity safety gate failed")
    return IdentityResolutionResult(
        binding=binding,
        decisions=decisions,
        audit=IdentityResolutionAudit(
            active_membership_rows=len(rows),
            suspected_provider_figi_bounce_rows=suspected_rows,
            pending_or_adjudicated_unresolved_rows=unresolved_rows,
            approved_provider_contamination_override_rows=override_rows,
            unapproved_canonical_identity_override_rows=unapproved_overrides,
            suspected_provider_contamination_eligible_rows=unresolved_eligible,
            identity_quality_liquidation_signal_rows=liquidation_rows,
        ),
        effective_adjudication_ids_by_case=MappingProxyType(
            {case_id: item.identity_adjudication_id for case_id, item in sorted(effective.items())}
        ),
    )


def resolve_loaded_registry_at_cutoff(
    observations: Sequence[ObservedIdentityMembership],
    registry: LoadedIdentityAdjudicationRegistryRelease,
    *,
    cutoff_session: date,
) -> IdentityResolutionResult:
    """Fixture helper with exact control objects but caller-supplied membership rows.

    This is not a production ingress: it deliberately rejects Protocol lookalikes but cannot
    prove the provenance of ``observations``.  A source-bound production wrapper must be added
    before any real S7 transform or publish run.
    """

    if type(registry) is not LoadedIdentityAdjudicationRegistryRelease:
        raise IdentityResolutionError(
            "registry input must be an exact IdentityAdjudicationStore loader result"
        )

    release = registry.release
    manifest = registry.candidate_manifest
    if (
        type(release) is not IdentityAdjudicationRegistryRelease
        or type(manifest) is not IdentityCaseCandidateManifest
        or any(type(item) is not ApprovedIdentityDecision for item in registry.decisions)
    ):
        raise IdentityResolutionError("loaded registry contains non-concrete control objects")
    if (
        release.candidate_manifest_id != manifest.candidate_manifest_id
        or release.candidate_manifest_sha256 != manifest.sha256
        or release.six_release_binding_id != manifest.six_release_binding_id
    ):
        raise IdentityResolutionError("loaded registry/candidate binding is inconsistent")
    binding = IdentityResolutionBinding(
        cutoff_session=cutoff_session,
        six_release_binding_id=release.six_release_binding_id,
        candidate_manifest_id=manifest.candidate_manifest_id,
        candidate_manifest_sha256=manifest.sha256,
        candidate_manifest_available_session=(manifest.candidate_manifest_available_session),
        adjudication_release_id=release.release_id,
        adjudication_release_available_session=release.release_available_session,
    )
    return resolve_identity_at_cutoff(
        observations,
        manifest.cases,
        registry.decisions,
        binding=binding,
    )


def _validate_memberships(rows: tuple[ObservedIdentityMembership, ...]) -> None:
    if not rows:
        raise IdentityResolutionError("S7 resolver requires active membership rows")
    keys = [(item.session_date, item.ticker) for item in rows]
    if len(keys) != len(set(keys)):
        raise IdentityResolutionError("S7 active membership key is not unique")
    source_ids = [item.source_record_id for item in rows]
    if len(source_ids) != len(set(source_ids)):
        raise IdentityResolutionError("S7 source_record_id is not unique")


def _validate_cases(
    rows: tuple[ObservedIdentityMembership, ...],
    cases: tuple[BounceCaseLike, ...],
    *,
    binding: IdentityResolutionBinding,
) -> dict[str, BounceCaseLike]:
    by_source_id = {item.source_record_id: item for item in rows}
    seen_case_ids: set[str] = set()
    result: dict[str, BounceCaseLike] = {}
    for case in cases:
        _digest(case.identity_case_id, "identity_case_id")
        _digest(case.episode_source_record_set_digest, "episode source-record-set digest")
        if case.six_release_binding_id != binding.six_release_binding_id:
            raise IdentityResolutionError("identity case uses a different six-release binding")
        if case.identity_case_id in seen_case_ids:
            raise IdentityResolutionError("candidate manifest repeats an identity case")
        seen_case_ids.add(case.identity_case_id)
        if case.identity_case_available_session > binding.candidate_manifest_available_session:
            raise IdentityResolutionError("identity case predates neither its manifest nor cutoff")
        if (
            not _is_figi(case.left_outer_composite_figi)
            or case.left_outer_composite_figi != case.right_outer_composite_figi
            or case.left_outer_composite_figi == case.middle_observed_composite_figi
        ):
            raise IdentityResolutionError("identity case is not an exact A-to-B-to-A bounce")
        _figi(case.middle_observed_composite_figi, "case middle Composite FIGI")
        source_ids = tuple(case.episode_source_record_ids)
        if not source_ids or len(source_ids) != len(set(source_ids)):
            raise IdentityResolutionError("identity case source-record scope is empty or repeated")
        if stable_digest(sorted(source_ids)) != case.episode_source_record_set_digest:
            raise IdentityResolutionError("identity case source-record-set digest mismatch")
        try:
            left = by_source_id[case.left_outer_source_record_id]
            right = by_source_id[case.right_outer_source_record_id]
        except KeyError as exc:
            raise IdentityResolutionError("identity case outer boundary row is absent") from exc
        if (
            left.ticker != case.ticker
            or right.ticker != case.ticker
            or left.observed_composite_figi != case.left_outer_composite_figi
            or right.observed_composite_figi != case.right_outer_composite_figi
            or left.session_date >= case.episode_valid_from_session
            or right.session_date <= case.episode_valid_through_session
        ):
            raise IdentityResolutionError("identity case outer boundary does not match source rows")
        for source_id in source_ids:
            _digest(source_id, "case source_record_id")
            try:
                row = by_source_id[source_id]
            except KeyError as exc:
                raise IdentityResolutionError("identity case source row is absent") from exc
            if source_id in result:
                raise IdentityResolutionError(
                    "one source row belongs to overlapping identity cases"
                )
            if (
                row.ticker != case.ticker
                or row.observed_composite_figi != case.middle_observed_composite_figi
                or not (
                    case.episode_valid_from_session
                    <= row.session_date
                    <= case.episode_valid_through_session
                )
            ):
                raise IdentityResolutionError("identity case episode does not match source rows")
            result[source_id] = case
    return result


def _effective_adjudications(
    cases: tuple[BounceCaseLike, ...],
    rows: tuple[AdjudicationLike, ...],
    *,
    binding: IdentityResolutionBinding,
) -> dict[str, AdjudicationLike]:
    case_by_id = {item.identity_case_id: item for item in cases}
    decision_ids = [item.identity_adjudication_id for item in rows]
    if len(decision_ids) != len(set(decision_ids)):
        raise IdentityResolutionError("adjudication release repeats a decision ID")
    series: defaultdict[str, list[AdjudicationLike]] = defaultdict(list)
    series_by_case: dict[str, str] = {}
    for row in rows:
        _digest(row.identity_adjudication_id, "identity_adjudication_id")
        _digest(row.adjudication_series_id, "adjudication_series_id")
        if row.approval_status != "approved":
            raise IdentityResolutionError("unapproved adjudication entered the registry input")
        if row.outcome_or_backtest_evidence_used is not False:
            raise IdentityResolutionError("outcome/backtest evidence cannot adjudicate identity")
        if (
            row.source_identity_case_candidate_manifest_id != binding.candidate_manifest_id
            or row.source_identity_case_candidate_manifest_sha256
            != binding.candidate_manifest_sha256
        ):
            raise IdentityResolutionError("adjudication uses a different candidate manifest")
        try:
            case = case_by_id[row.identity_case_id]
        except KeyError as exc:
            raise IdentityResolutionError(
                "adjudication refers to an absent candidate case"
            ) from exc
        _validate_adjudication_scope(row, case, binding=binding)
        prior_series = series_by_case.setdefault(row.identity_case_id, row.adjudication_series_id)
        if prior_series != row.adjudication_series_id:
            raise IdentityResolutionError("one case has multiple adjudication series")
        series[row.adjudication_series_id].append(row)

    effective: dict[str, AdjudicationLike] = {}
    for versions in series.values():
        ordered = sorted(versions, key=lambda item: item.decision_version)
        if [item.decision_version for item in ordered] != list(range(1, len(ordered) + 1)):
            raise IdentityResolutionError("adjudication decision versions are not contiguous")
        for index, item in enumerate(ordered):
            expected_predecessor = (
                None if index == 0 else ordered[index - 1].identity_adjudication_id
            )
            if item.supersedes_identity_adjudication_id != expected_predecessor:
                raise IdentityResolutionError("adjudication predecessor chain is invalid")
            if (
                index > 0
                and item.adjudication_available_session
                < ordered[index - 1].adjudication_available_session
            ):
                raise IdentityResolutionError("adjudication availability moves backward")
        available = [
            item
            for item in ordered
            if item.adjudication_available_session <= binding.cutoff_session
        ]
        if not available:
            continue
        head = available[-1]
        effective[head.identity_case_id] = head
    return effective


def _validate_adjudication_scope(
    row: AdjudicationLike,
    case: BounceCaseLike,
    *,
    binding: IdentityResolutionBinding,
) -> None:
    if (
        row.identity_case_available_session != case.identity_case_available_session
        or row.observed_ticker != case.ticker
        or row.observed_composite_figi != case.middle_observed_composite_figi
        or row.episode_valid_from_session != case.episode_valid_from_session
        or row.episode_valid_through_session != case.episode_valid_through_session
        or row.episode_source_record_set_digest != case.episode_source_record_set_digest
    ):
        raise IdentityResolutionError("adjudication scope differs from its candidate case")
    if row.adjudication_available_session < case.identity_case_available_session:
        raise IdentityResolutionError("adjudication availability precedes case availability")
    if row.adjudication_available_session > binding.adjudication_release_available_session:
        raise IdentityResolutionError("registry publication precedes an included adjudication")
    try:
        disposition = AdjudicationDisposition(row.disposition)
    except ValueError as exc:
        raise IdentityResolutionError("adjudication disposition is invalid") from exc
    canonical = row.canonical_composite_figi
    expected_asset_id = None if canonical is None else canonical_asset_id(canonical)
    if row.canonical_asset_id != expected_asset_id:
        raise IdentityResolutionError("adjudication canonical asset ID does not reproduce")
    if disposition is AdjudicationDisposition.CONFIRMED_GENUINE_TRANSITION:
        if canonical != case.middle_observed_composite_figi or row.canonical_override:
            raise IdentityResolutionError("genuine-transition canonical matrix is invalid")
    elif disposition is AdjudicationDisposition.CONFIRMED_PROVIDER_CONTAMINATION:
        if (
            not row.canonical_override
            or not _is_figi(canonical)
            or canonical == case.middle_observed_composite_figi
            or canonical != case.left_outer_composite_figi
        ):
            raise IdentityResolutionError("provider-contamination canonical matrix is invalid")
    elif canonical is not None or row.canonical_override:
        raise IdentityResolutionError("adjudicated-unresolved cannot carry a canonical override")


def _direct_decision(row: ObservedIdentityMembership) -> IdentityResolutionDecision:
    observed = row.observed_composite_figi
    if not _is_figi(observed):
        return _unresolved_decision(
            row,
            method="ticker_only",
            disposition="not_applicable_no_observed_composite",
            case_id=None,
            adjudication_id=None,
        )
    if row.relationship_conflict:
        return IdentityResolutionDecision(
            session_date=row.session_date,
            ticker=row.ticker,
            source_record_id=row.source_record_id,
            observed_composite_figi=observed,
            canonical_composite_figi=observed,
            canonical_asset_id=canonical_asset_id(observed),
            identity_resolution_status="resolved_conflicted",
            identity_resolution_method="source_composite_figi_exact_with_relationship_conflict",
            identity_disposition="observed_consistent",
            identity_case_id=None,
            identity_adjudication_id=None,
            backtest_identity_eligible=False,
            alias_emitted=False,
            position_continuity_status=POSITION_CONTINUITY_UNCERTAIN,
        )
    return _resolved_decision(
        row,
        canonical=observed,
        status="resolved_strong",
        method="source_composite_figi_exact",
        disposition="observed_consistent",
        case_id=None,
        adjudication_id=None,
    )


def _case_decision(
    row: ObservedIdentityMembership,
    case: BounceCaseLike,
    adjudication: AdjudicationLike | None,
    *,
    direct_anchor_figis: set[str | None],
) -> IdentityResolutionDecision:
    if adjudication is None:
        return _unresolved_decision(
            row,
            method="provider_figi_bounce_pending_unresolved",
            disposition="pending_unresolved",
            case_id=case.identity_case_id,
            adjudication_id=None,
        )
    disposition = AdjudicationDisposition(adjudication.disposition)
    if disposition is AdjudicationDisposition.ADJUDICATED_UNRESOLVED:
        return _unresolved_decision(
            row,
            method="provider_figi_bounce_adjudicated_unresolved",
            disposition=disposition.value,
            case_id=case.identity_case_id,
            adjudication_id=adjudication.identity_adjudication_id,
        )
    canonical = adjudication.canonical_composite_figi
    assert canonical is not None  # validated by the decision matrix
    if (
        disposition is AdjudicationDisposition.CONFIRMED_PROVIDER_CONTAMINATION
        and canonical not in direct_anchor_figis
    ):
        raise IdentityResolutionError(
            "provider-contamination target lacks independent direct S4 evidence"
        )
    method = (
        "approved_genuine_transition"
        if disposition is AdjudicationDisposition.CONFIRMED_GENUINE_TRANSITION
        else "approved_provider_contamination_override"
    )
    status = (
        "resolved_strong"
        if disposition is AdjudicationDisposition.CONFIRMED_GENUINE_TRANSITION
        else "resolved_approved_override"
    )
    if row.relationship_conflict:
        return IdentityResolutionDecision(
            session_date=row.session_date,
            ticker=row.ticker,
            source_record_id=row.source_record_id,
            observed_composite_figi=row.observed_composite_figi,
            canonical_composite_figi=canonical,
            canonical_asset_id=canonical_asset_id(canonical),
            identity_resolution_status="resolved_conflicted",
            identity_resolution_method=method,
            identity_disposition=disposition.value,
            identity_case_id=case.identity_case_id,
            identity_adjudication_id=adjudication.identity_adjudication_id,
            backtest_identity_eligible=False,
            alias_emitted=False,
            position_continuity_status=POSITION_CONTINUITY_UNCERTAIN,
        )
    return _resolved_decision(
        row,
        canonical=canonical,
        status=status,
        method=method,
        disposition=disposition.value,
        case_id=case.identity_case_id,
        adjudication_id=adjudication.identity_adjudication_id,
    )


def _resolved_decision(
    row: ObservedIdentityMembership,
    *,
    canonical: str,
    status: str,
    method: str,
    disposition: str,
    case_id: str | None,
    adjudication_id: str | None,
) -> IdentityResolutionDecision:
    return IdentityResolutionDecision(
        session_date=row.session_date,
        ticker=row.ticker,
        source_record_id=row.source_record_id,
        observed_composite_figi=row.observed_composite_figi,
        canonical_composite_figi=canonical,
        canonical_asset_id=canonical_asset_id(canonical),
        identity_resolution_status=status,
        identity_resolution_method=method,
        identity_disposition=disposition,
        identity_case_id=case_id,
        identity_adjudication_id=adjudication_id,
        backtest_identity_eligible=True,
        alias_emitted=True,
        position_continuity_status=POSITION_CONTINUITY_RESOLVED,
    )


def _unresolved_decision(
    row: ObservedIdentityMembership,
    *,
    method: str,
    disposition: str,
    case_id: str | None,
    adjudication_id: str | None,
) -> IdentityResolutionDecision:
    return IdentityResolutionDecision(
        session_date=row.session_date,
        ticker=row.ticker,
        source_record_id=row.source_record_id,
        observed_composite_figi=row.observed_composite_figi,
        canonical_composite_figi=None,
        canonical_asset_id=None,
        identity_resolution_status="unresolved",
        identity_resolution_method=method,
        identity_disposition=disposition,
        identity_case_id=case_id,
        identity_adjudication_id=adjudication_id,
        backtest_identity_eligible=False,
        alias_emitted=False,
        position_continuity_status=POSITION_CONTINUITY_UNCERTAIN,
    )


def _digest(value: object, label: str) -> str:
    if not isinstance(value, str) or not _SHA256.fullmatch(value):
        raise IdentityResolutionError(f"{label} must be a lowercase SHA-256")
    return value


def _figi(value: object, label: str) -> str:
    if not isinstance(value, str) or not _FIGI.fullmatch(value):
        raise IdentityResolutionError(f"{label} must be a valid Composite FIGI")
    return value


def _is_figi(value: object) -> bool:
    return isinstance(value, str) and _FIGI.fullmatch(value) is not None


__all__ = [
    "ASSET_ID_NAMESPACE",
    "ASSET_ID_RULE_VERSION",
    "POSITION_CONTINUITY_RESOLVED",
    "POSITION_CONTINUITY_UNCERTAIN",
    "AdjudicationDisposition",
    "IdentityResolutionAudit",
    "IdentityResolutionBinding",
    "IdentityResolutionDecision",
    "IdentityResolutionError",
    "IdentityResolutionResult",
    "ObservedIdentityMembership",
    "canonical_asset_id",
]
