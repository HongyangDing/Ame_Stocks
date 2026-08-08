"""Closed, local-only identity dispatcher for the S7.5 I3 staging boundary.

The public Gate A release validator intentionally remains unable to attest
row-bearing releases.  This module adds a narrower capability for I3 local
staging only: it binds an exact three-session calendar/source window, applies
one fixed five-registry policy, runs module-owned identity checks, and mints a
sealed proof.  The proof is not publication, correction, replacement,
registry-mutation, or base-cutover authority.

No validator or callback is accepted from a caller.  Raw provider membership
is always preserved; identity uncertainty can only suppress a research alias
and ``backtest_identity_eligible``.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from datetime import date, datetime
from types import MappingProxyType
from typing import Final

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver.incremental_contract import ArtifactPin
from ame_stocks_api.silver.incremental_gate import QaSeverity
from ame_stocks_api.silver.incremental_i3_checkpoint import (
    IDENTITY_REGISTRY_ORDER,
    I3CheckpointError,
    IdentityPolicyBundle,
    IdentityRegistryKind,
)
from ame_stocks_api.silver.incremental_identity import canonical_asset_id

I3_DISPATCH_RULE_VERSION: Final = "s7_5_i3_closed_identity_dispatch_v2"
I3_CALENDAR_RULE_VERSION: Final = "s7_5_i3_exact_calendar_v1"
I3_COVERAGE_RULE_VERSION: Final = "s7_5_i3_alias_source_coverage_v1"
I3_ROW_PROOF_RULE_VERSION: Final = "s7_5_i3_row_semantic_proof_v2"
I3_LOCAL_ATTESTATION_RULE_VERSION: Final = "s7_5_i3_local_staging_attestation_v2"
I3_FIXTURE_REGISTRY_RELEASE_NAMESPACE: Final = "ame_stocks.silver.s7_5.i3_fixture_registry_release"
I3_FIXTURE_REGISTRY_RELEASE_RULE_VERSION: Final = "s7_5_i3_fixture_registry_release_v2"
I3_FIXTURE_REGISTRY_RELEASE_SCHEMA_VERSION: Final = 2
I3_FIXTURE_REGISTRY_DECISION_NAMESPACE: Final = (
    "ame_stocks.silver.s7_5.i3_fixture_registry_decision_v2"
)
I3_POLICY_SNAPSHOT_VERIFICATION_RULE_VERSION: Final = (
    "s7_5_i3_policy_snapshot_structural_reproduction_v2"
)
I3_FIXTURE_POLICY_SOURCE: Final = "fixture_registry_release_bytes"
I3_PRODUCTION_POLICY_SOURCE: Final = "production_loaded_registry_release_set"
I3_REGISTRY_DISPOSITION_MATRIX_RULE_VERSION: Final = "s7_5_i3_frozen_registry_disposition_matrix_v2"
I3_INVERSE_BOUNCE_RULE_VERSION: Final = "s7_5_i3_inverse_bounce_middle_decision_check_v2"
I3_RAW_REVIEW_RESULT_RULE_VERSION: Final = "s7_5_i3_typed_raw_review_results_v2"
I3_FIXED_BOUNDARY_LOOKBACK_PREVIOUS_SESSIONS: Final = 2

_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_FIGI = re.compile(r"^BBG[0-9A-Z]{9}$")
_TOKEN = re.compile(r"^[a-z][a-z0-9_]*$")
_TICKER = re.compile(r"^[A-Z0-9][A-Z0-9.\-/]{0,31}$")
_COUNTRY = re.compile(r"^[A-Z]{2}$")
_MIC = re.compile(r"^[A-Z0-9]{4}$")
_CALENDAR_SEAL = object()
_COVERAGE_SEAL = object()
_ROW_PROOF_SEAL = object()
_LOCAL_ATTESTATION_SEAL = object()
_POLICY_SNAPSHOT_SEAL = object()
_VERIFIED_POLICY_BATCH_SEAL = object()

_COMPOSITE_OVERRIDE_REGISTRIES: Final = frozenset(
    {
        IdentityRegistryKind.IDENTITY_ADJUDICATION,
        IdentityRegistryKind.IDENTITY_CROSS_MARKET_ADJUDICATION,
        IdentityRegistryKind.PROVIDER_COMPOSITE_OVERRIDE,
    }
)

_IDENTITY_ADJUDICATION_DISPOSITIONS: Final = frozenset(
    {
        "adjudicated_unresolved",
        "confirmed_genuine_transition",
        "confirmed_provider_contamination",
    }
)
_CROSS_MARKET_DISPOSITIONS: Final = frozenset(
    {
        "confirmed_provider_contamination",
        "cross_market_adjudicated_unresolved",
    }
)
_PROVIDER_COMPOSITE_OVERRIDE_DISPOSITIONS: Final = frozenset(
    {
        "confirmed_provider_composite_stale_after_transition",
        "provider_composite_override_adjudicated_unresolved",
    }
)
_SHARE_CLASS_ADJUDICATION_DISPOSITIONS: Final = frozenset(
    {
        "confirmed_share_class_correction",
        "share_class_adjudicated_unresolved",
    }
)
_ASSET_TRANSITION_DISPOSITIONS: Final = frozenset(
    {
        "asset_transition_adjudicated_unresolved",
        "confirmed_genuine_transition",
    }
)


class I3DispatchError(ValueError):
    """Raised when I3 staging inputs or derived proofs cross a frozen boundary."""


@dataclass(frozen=True, slots=True)
class I3QaRule:
    """One immutable member of the module-owned I3 QA catalog."""

    check_id: str
    severity: QaSeverity
    owner: str
    semantics_digest: str

    def __post_init__(self) -> None:
        if not isinstance(self.check_id, str) or _TOKEN.fullmatch(self.check_id) is None:
            raise I3DispatchError("QA check ID is invalid")
        if not isinstance(self.severity, QaSeverity):
            raise I3DispatchError("I3 QA severity is invalid")
        if self.owner not in {"dispatcher", "materialization"}:
            raise I3DispatchError("I3 QA owner is invalid")
        if (
            not isinstance(self.semantics_digest, str)
            or _DIGEST.fullmatch(self.semantics_digest) is None
        ):
            raise I3DispatchError("QA semantics digest must be lowercase SHA-256")

    def to_dict(self) -> dict[str, str]:
        return {
            "check_id": self.check_id,
            "owner": self.owner,
            "semantics_digest": self.semantics_digest,
            "severity": self.severity.value,
        }


def _qa_rule(
    check_id: str,
    severity: QaSeverity,
    statement: str,
    *,
    owner: str = "dispatcher",
) -> I3QaRule:
    return I3QaRule(
        check_id=check_id,
        severity=severity,
        semantics_digest=stable_digest(
            {
                "check_id": check_id,
                "dispatch_rule_version": I3_DISPATCH_RULE_VERSION,
                "owner": owner,
                "statement": statement,
            }
        ),
        owner=owner,
    )


I3_QA_CATALOG: Final = tuple(
    sorted(
        (
            _qa_rule(
                "availability_mismatch_rows",
                QaSeverity.CRITICAL,
                "Selected evidence and decisions cannot be consumed before availability.",
                owner="materialization",
            ),
            _qa_rule(
                "boundary_coverage_mismatch_rows",
                QaSeverity.CRITICAL,
                "Coverage is exactly the authoritative previous-two-plus-target window.",
            ),
            _qa_rule(
                "eligible_membership_missing_alias_rows",
                QaSeverity.CRITICAL,
                "Every identity-eligible membership has one materialized alias.",
                owner="materialization",
            ),
            _qa_rule(
                "identity_quality_changed_active_rows",
                QaSeverity.CRITICAL,
                "Identity quality never changes provider active_on_date membership.",
            ),
            _qa_rule(
                "identity_quality_forced_liquidation_rows",
                QaSeverity.CRITICAL,
                "Identity quality never emits a forced-liquidation signal.",
                owner="materialization",
            ),
            _qa_rule(
                "inactive_or_delisted_inferred_from_identity_quality_rows",
                QaSeverity.CRITICAL,
                "Identity uncertainty is never interpreted as inactivity or delisting.",
                owner="materialization",
            ),
            _qa_rule(
                "ineligible_membership_with_alias_rows",
                QaSeverity.CRITICAL,
                "Identity-ineligible membership cannot have an alias.",
                owner="materialization",
            ),
            _qa_rule(
                "ineligible_membership_with_master_version_rows",
                QaSeverity.CRITICAL,
                "Identity-ineligible membership cannot reference canonical master versions.",
                owner="materialization",
            ),
            _qa_rule(
                "inverse_bounce_misclassified_as_genuine_transition_rows",
                QaSeverity.CRITICAL,
                "A foreign-US-foreign inverse bounce is never inferred to be genuine.",
            ),
            _qa_rule(
                "multi_registry_composite_override_collision_alias_rows",
                QaSeverity.CRITICAL,
                "A Composite override collision cannot emit a ticker alias.",
            ),
            _qa_rule(
                "multi_registry_composite_override_collision_eligible_rows",
                QaSeverity.CRITICAL,
                "A Composite override collision cannot be identity eligible.",
            ),
            _qa_rule(
                "multi_registry_composite_override_collision_resolved_rows",
                QaSeverity.CRITICAL,
                "A Composite override collision cannot be resolved automatically.",
            ),
            _qa_rule(
                "multi_registry_composite_override_collision_rows",
                QaSeverity.INFO,
                "Raw collisions are bounded review observations, not publish failures.",
            ),
            _qa_rule(
                "row_semantic_proof_mismatch_rows",
                QaSeverity.CRITICAL,
                "Every target decision has the module-owned, content-bound proof.",
            ),
            _qa_rule(
                "row_version_fk_mismatch_rows",
                QaSeverity.CRITICAL,
                "Every emitted membership foreign key resolves to the same snapshot.",
                owner="materialization",
            ),
            _qa_rule(
                "share_class_applied_before_unique_composite_rows",
                QaSeverity.CRITICAL,
                "Share Class adjudication is applied only after one Composite is selected.",
            ),
            _qa_rule(
                "source_membership_omission_or_duplication_rows",
                QaSeverity.CRITICAL,
                "Materialization preserves exactly one output membership per source membership.",
                owner="materialization",
            ),
            _qa_rule(
                "suspected_provider_figi_bounce_rows",
                QaSeverity.HIGH,
                "A-B-A episodes are reported and unresolved episodes remain reviewable.",
            ),
            _qa_rule(
                "suspected_provider_contamination_eligible_rows",
                QaSeverity.CRITICAL,
                "A retrospectively suspected unapproved bounce middle cannot remain eligible.",
            ),
            _qa_rule(
                "target_market_consistency_unchecked_rows",
                QaSeverity.CRITICAL,
                "Every target row receives the locale-to-Composite market check.",
            ),
            _qa_rule(
                "unapproved_canonical_identity_override_rows",
                QaSeverity.CRITICAL,
                "Canonical Composite changes require one decision in the pinned policy.",
            ),
            _qa_rule(
                "unapproved_cross_market_composite_eligible_rows",
                QaSeverity.CRITICAL,
                "A US-locale foreign Composite is ineligible without approved correction.",
            ),
            _qa_rule(
                "us_locale_non_us_composite_figi_rows",
                QaSeverity.HIGH,
                "Every target US-locale foreign Composite is counted; unapproved rows fail.",
            ),
        ),
        key=lambda item: item.check_id,
    )
)
I3_RAW_REVIEW_CHECK_IDS: Final = (
    "multi_registry_composite_override_collision_rows",
    "suspected_provider_figi_bounce_rows",
    "us_locale_non_us_composite_figi_rows",
)
I3_QA_CATALOG_DIGEST: Final = stable_digest(
    {
        "qa_catalog": [item.to_dict() for item in I3_QA_CATALOG],
        "raw_review_check_ids": list(I3_RAW_REVIEW_CHECK_IDS),
    }
)
I3_ROW_VALIDATOR_SEMANTICS_DIGEST: Final = stable_digest(
    {
        "dispatch_rule_version": I3_DISPATCH_RULE_VERSION,
        "fixed_boundary_lookback_previous_sessions": (I3_FIXED_BOUNDARY_LOOKBACK_PREVIOUS_SESSIONS),
        "qa_catalog_digest": I3_QA_CATALOG_DIGEST,
        "inverse_bounce_rule_version": I3_INVERSE_BOUNCE_RULE_VERSION,
        "policy_snapshot_verification_rule_version": (I3_POLICY_SNAPSHOT_VERIFICATION_RULE_VERSION),
        "raw_review_result_rule_version": I3_RAW_REVIEW_RESULT_RULE_VERSION,
        "registry_disposition_matrix_rule_version": (I3_REGISTRY_DISPOSITION_MATRIX_RULE_VERSION),
        "registry_order": [item.value for item in IDENTITY_REGISTRY_ORDER],
        "row_proof_rule_version": I3_ROW_PROOF_RULE_VERSION,
    }
)


@dataclass(frozen=True, slots=True)
class ExactCalendarPartition:
    """One authenticated calendar session and its source partition receipt."""

    session_date: date
    partition_receipt_id: str

    def __post_init__(self) -> None:
        _date(self.session_date, "calendar session")
        _digest(self.partition_receipt_id, "calendar partition receipt ID")

    def to_dict(self) -> dict[str, str]:
        return {
            "partition_receipt_id": self.partition_receipt_id,
            "session_date": self.session_date.isoformat(),
        }


@dataclass(frozen=True, slots=True, init=False)
class ExactTradingCalendar:
    """Sealed local projection of one exact, content-addressed calendar."""

    sessions: tuple[date, ...]
    artifact: ArtifactPin
    calendar_digest: str
    _seal: object = field(init=False, repr=False, compare=False)

    def __init__(
        self,
        *,
        sessions: tuple[date, ...],
        artifact: ArtifactPin,
        calendar_digest: str,
        _seal: object,
    ) -> None:
        if _seal is not _CALENDAR_SEAL:
            raise I3DispatchError("exact calendars can only be minted by the fixed freezer")
        object.__setattr__(self, "sessions", sessions)
        object.__setattr__(self, "artifact", artifact)
        object.__setattr__(self, "calendar_digest", calendar_digest)
        object.__setattr__(self, "_seal", _seal)

    def to_dict(self) -> dict[str, object]:
        return {
            "artifact": self.artifact.to_dict(),
            "calendar_digest": self.calendar_digest,
            "rule_version": I3_CALENDAR_RULE_VERSION,
            "sessions": [item.isoformat() for item in self.sessions],
        }


def freeze_exact_trading_calendar(
    sessions: tuple[date, ...],
    *,
    artifact_path: str,
) -> ExactTradingCalendar:
    """Canonicalize and locally attest an ordered trading-calendar artifact."""

    if type(sessions) is not tuple or len(sessions) < 3:
        raise I3DispatchError("exact calendar requires at least three sessions")
    if any(type(item) is not date for item in sessions):
        raise I3DispatchError("exact calendar contains an invalid session")
    if tuple(sorted(set(sessions))) != sessions:
        raise I3DispatchError("exact calendar sessions must be sorted and unique")
    document = {
        "rule_version": I3_CALENDAR_RULE_VERSION,
        "sessions": [item.isoformat() for item in sessions],
    }
    content = _canonical_json_bytes(document)
    artifact = ArtifactPin(
        path=artifact_path,
        sha256=hashlib.sha256(content).hexdigest(),
        bytes=len(content),
    )
    return ExactTradingCalendar(
        sessions=sessions,
        artifact=artifact,
        calendar_digest=stable_digest(document),
        _seal=_CALENDAR_SEAL,
    )


@dataclass(frozen=True, slots=True)
class IdentityObservation:
    """Exact provider-observed membership row used by the dispatcher."""

    provider_id: str
    provider_market: str
    provider_locale: str
    ticker: str
    session_date: date
    observed_composite_figi: str | None
    observed_composite_country: str | None
    observed_share_class_figi: str | None
    primary_exchange: str | None
    source_record_id: str
    active_on_date: bool

    def __post_init__(self) -> None:
        _token(self.provider_id, "provider ID")
        _token(self.provider_market, "provider market")
        _token(self.provider_locale, "provider locale")
        if self.provider_locale != "us":
            raise I3DispatchError("I3 identity dispatch is closed to provider locale=us")
        if not isinstance(self.ticker, str) or _TICKER.fullmatch(self.ticker) is None:
            raise I3DispatchError("ticker is invalid")
        _date(self.session_date, "observation session")
        _optional_figi(self.observed_composite_figi, "observed Composite FIGI")
        _optional_country(self.observed_composite_country)
        if (self.observed_composite_figi is None) != (self.observed_composite_country is None):
            raise I3DispatchError(
                "Composite FIGI and its independently attested country must coexist"
            )
        _optional_figi(self.observed_share_class_figi, "observed Share Class FIGI")
        if self.primary_exchange is not None:
            _token(self.primary_exchange.lower(), "primary exchange")
        _digest(self.source_record_id, "source record ID")
        if type(self.active_on_date) is not bool:
            raise I3DispatchError("active_on_date must be Boolean")

    def to_dict(self) -> dict[str, object]:
        return {
            "active_on_date": self.active_on_date,
            "observed_composite_country": self.observed_composite_country,
            "observed_composite_figi": self.observed_composite_figi,
            "observed_share_class_figi": self.observed_share_class_figi,
            "primary_exchange": self.primary_exchange,
            "provider_id": self.provider_id,
            "provider_locale": self.provider_locale,
            "provider_market": self.provider_market,
            "session_date": self.session_date.isoformat(),
            "source_record_id": self.source_record_id,
            "ticker": self.ticker,
        }


@dataclass(frozen=True, slots=True, order=True)
class RegistrySourceScopeRow:
    """One exact S4 source row retained by a production registry decision.

    Fixture releases deliberately carry only one source-record digest.  The
    production registry workflow, by contrast, authenticates the complete
    :class:`ExactSourceScope`.  Keeping the full row here lets snapshot
    reverification reproduce membership-based lookup without trusting a
    caller-authored expansion of one real registry decision into many fake
    decisions.
    """

    session_date: date
    source_record_id: str
    source_dataset: str
    source_s4_release_set_id: str
    provider_id: str
    provider_market: str
    provider_locale: str
    ticker: str
    observed_composite_figi: str
    observed_share_class_figi: str | None
    primary_exchange_mic: str | None

    def __post_init__(self) -> None:
        _date(self.session_date, "registry source-scope session")
        _digest(self.source_record_id, "registry source-scope record ID")
        if not isinstance(self.source_dataset, str) or not self.source_dataset:
            raise I3DispatchError("registry source-scope dataset is invalid")
        _digest(self.source_s4_release_set_id, "registry source-scope S4 release-set ID")
        _token(self.provider_id, "registry source-scope provider ID")
        _token(self.provider_market, "registry source-scope provider market")
        _token(self.provider_locale, "registry source-scope provider locale")
        if self.provider_locale != "us":
            raise I3DispatchError("registry source scope is outside locale=us")
        if not isinstance(self.ticker, str) or _TICKER.fullmatch(self.ticker) is None:
            raise I3DispatchError("registry source-scope ticker is invalid")
        _optional_figi(self.observed_composite_figi, "registry source-scope Composite FIGI")
        _optional_figi(
            self.observed_share_class_figi,
            "registry source-scope Share Class FIGI",
        )
        if (
            self.primary_exchange_mic is not None
            and _MIC.fullmatch(self.primary_exchange_mic) is None
        ):
            raise I3DispatchError("registry source-scope primary exchange MIC is invalid")

    def to_dict(self) -> dict[str, object]:
        return {
            "observed_composite_figi": self.observed_composite_figi,
            "observed_share_class_figi": self.observed_share_class_figi,
            "primary_exchange_mic": self.primary_exchange_mic,
            "provider_id": self.provider_id,
            "provider_locale": self.provider_locale,
            "provider_market": self.provider_market,
            "session_date": self.session_date.isoformat(),
            "source_dataset": self.source_dataset,
            "source_record_id": self.source_record_id,
            "source_s4_release_set_id": self.source_s4_release_set_id,
            "ticker": self.ticker,
        }


@dataclass(frozen=True, slots=True)
class SourceCoverageSlot:
    """Exact statement of all selected source records for one covered session."""

    session_date: date
    partition_receipt_id: str
    source_record_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        _date(self.session_date, "coverage session")
        _digest(self.partition_receipt_id, "coverage partition receipt ID")
        if type(self.source_record_ids) is not tuple:
            raise I3DispatchError("coverage source-record IDs must be a tuple")
        if tuple(sorted(set(self.source_record_ids))) != self.source_record_ids:
            raise I3DispatchError("coverage source-record IDs must be sorted and unique")
        if len(self.source_record_ids) > 1:
            raise I3DispatchError("one ticker/session cannot select multiple source rows")
        for item in self.source_record_ids:
            _digest(item, "coverage source-record ID")

    def to_dict(self) -> dict[str, object]:
        return {
            "partition_receipt_id": self.partition_receipt_id,
            "session_date": self.session_date.isoformat(),
            "source_record_ids": list(self.source_record_ids),
        }


@dataclass(frozen=True, slots=True, init=False)
class AliasSourceCoverageReceipt:
    """Sealed exact previous-two-plus-target source coverage."""

    provider_id: str
    provider_market: str
    provider_locale: str
    ticker: str
    target_session: date
    calendar_digest: str
    calendar_artifact: ArtifactPin
    slots: tuple[SourceCoverageSlot, ...]
    coverage_available_session: date
    coverage_receipt_id: str
    _seal: object = field(init=False, repr=False, compare=False)

    def __init__(
        self,
        *,
        provider_id: str,
        provider_market: str,
        provider_locale: str,
        ticker: str,
        target_session: date,
        calendar_digest: str,
        calendar_artifact: ArtifactPin,
        slots: tuple[SourceCoverageSlot, ...],
        coverage_available_session: date,
        coverage_receipt_id: str,
        _seal: object,
    ) -> None:
        if _seal is not _COVERAGE_SEAL:
            raise I3DispatchError("source coverage can only be minted by the fixed binder")
        for name, value in (
            ("provider_id", provider_id),
            ("provider_market", provider_market),
            ("provider_locale", provider_locale),
            ("ticker", ticker),
            ("target_session", target_session),
            ("calendar_digest", calendar_digest),
            ("calendar_artifact", calendar_artifact),
            ("slots", slots),
            ("coverage_available_session", coverage_available_session),
            ("coverage_receipt_id", coverage_receipt_id),
        ):
            object.__setattr__(self, name, value)
        object.__setattr__(self, "_seal", _seal)

    def logical_payload(self) -> dict[str, object]:
        return {
            "calendar_artifact": self.calendar_artifact.to_dict(),
            "calendar_digest": self.calendar_digest,
            "coverage_available_session": self.coverage_available_session.isoformat(),
            "provider_id": self.provider_id,
            "provider_locale": self.provider_locale,
            "provider_market": self.provider_market,
            "rule_version": I3_COVERAGE_RULE_VERSION,
            "slots": [item.to_dict() for item in self.slots],
            "target_session": self.target_session.isoformat(),
            "ticker": self.ticker,
        }

    def to_dict(self) -> dict[str, object]:
        return {"coverage_receipt_id": self.coverage_receipt_id, **self.logical_payload()}


def bind_alias_source_coverage(
    calendar: ExactTradingCalendar,
    *,
    provider_id: str,
    provider_market: str,
    provider_locale: str,
    ticker: str,
    target_session: date,
    slots: tuple[SourceCoverageSlot, ...],
    coverage_available_session: date,
) -> AliasSourceCoverageReceipt:
    """Bind exactly two prior authoritative sessions plus the target."""

    verified = _verify_calendar(calendar)
    try:
        target_index = verified.sessions.index(target_session)
    except ValueError as exc:
        raise I3DispatchError("target session is absent from the exact calendar") from exc
    if target_index < I3_FIXED_BOUNDARY_LOOKBACK_PREVIOUS_SESSIONS:
        raise I3DispatchError("target lacks the fixed two-session calendar lookback")
    expected_sessions = verified.sessions[target_index - 2 : target_index + 1]
    if type(slots) is not tuple or len(slots) != 3:
        raise I3DispatchError("coverage requires exactly three session slots")
    if tuple(item.session_date for item in slots) != expected_sessions:
        raise I3DispatchError("coverage slots are missing or gapped against the exact calendar")
    if any(type(item) is not SourceCoverageSlot for item in slots):
        raise I3DispatchError("coverage contains an invalid slot")
    _token(provider_id, "coverage provider ID")
    _token(provider_market, "coverage provider market")
    _token(provider_locale, "coverage provider locale")
    if provider_locale != "us":
        raise I3DispatchError("coverage is outside locale=us")
    if not isinstance(ticker, str) or _TICKER.fullmatch(ticker) is None:
        raise I3DispatchError("coverage ticker is invalid")
    _date(coverage_available_session, "coverage availability")
    if coverage_available_session < target_session:
        raise I3DispatchError("coverage availability precedes target session")
    payload = {
        "calendar_artifact": verified.artifact.to_dict(),
        "calendar_digest": verified.calendar_digest,
        "coverage_available_session": coverage_available_session.isoformat(),
        "provider_id": provider_id,
        "provider_locale": provider_locale,
        "provider_market": provider_market,
        "rule_version": I3_COVERAGE_RULE_VERSION,
        "slots": [item.to_dict() for item in slots],
        "target_session": target_session.isoformat(),
        "ticker": ticker,
    }
    return AliasSourceCoverageReceipt(
        provider_id=provider_id,
        provider_market=provider_market,
        provider_locale=provider_locale,
        ticker=ticker,
        target_session=target_session,
        calendar_digest=verified.calendar_digest,
        calendar_artifact=verified.artifact,
        slots=slots,
        coverage_available_session=coverage_available_session,
        coverage_receipt_id=stable_digest(payload),
        _seal=_COVERAGE_SEAL,
    )


@dataclass(frozen=True, slots=True)
class RegistryDecision:
    """One fixed-policy registry record with kind-specific closed semantics."""

    registry_kind: IdentityRegistryKind
    registry_release_id: str
    decision_id: str
    provider_id: str
    provider_market: str
    provider_locale: str
    ticker: str
    source_record_id: str
    identity_disposition: str | None
    decision_available_session: date
    effective_from_session: date
    effective_to_session: date | None
    observed_composite_figi: str | None = None
    canonical_composite_figi: str | None = None
    observed_composite_market_code: str | None = None
    canonical_composite_market_code: str | None = None
    composite_scope_figi: str | None = None
    observed_share_class_figi: str | None = None
    canonical_share_class_figi: str | None = None
    transition_relation_id: str | None = None
    predecessor_asset_id: str | None = None
    successor_asset_id: str | None = None
    source_scope: tuple[RegistrySourceScopeRow, ...] = ()
    production_registry_row: Mapping[str, object] | None = field(
        default=None,
        repr=False,
        compare=False,
    )

    def __post_init__(self) -> None:
        if not isinstance(self.registry_kind, IdentityRegistryKind):
            raise I3DispatchError("registry decision kind is invalid")
        _digest(self.registry_release_id, "registry release ID")
        _digest(self.decision_id, "registry decision ID")
        _token(self.provider_id, "registry provider ID")
        _token(self.provider_market, "registry provider market")
        _token(self.provider_locale, "registry provider locale")
        if self.provider_locale != "us":
            raise I3DispatchError("registry decision is outside locale=us")
        if not isinstance(self.ticker, str) or _TICKER.fullmatch(self.ticker) is None:
            raise I3DispatchError("registry decision ticker is invalid")
        _digest(self.source_record_id, "registry scoped source-record ID")
        if self.identity_disposition is not None:
            _token(self.identity_disposition, "registry identity disposition")
        _date(self.decision_available_session, "registry decision availability")
        _date(self.effective_from_session, "registry effective-from session")
        if self.effective_to_session is not None:
            _date(self.effective_to_session, "registry effective-to session")
            if self.effective_to_session < self.effective_from_session:
                raise I3DispatchError("registry effective interval is reversed")
        for value, label in (
            (self.observed_composite_figi, "registry observed Composite FIGI"),
            (self.canonical_composite_figi, "registry canonical Composite FIGI"),
            (self.composite_scope_figi, "registry Composite scope FIGI"),
            (self.observed_share_class_figi, "registry observed Share Class FIGI"),
            (self.canonical_share_class_figi, "registry canonical Share Class FIGI"),
        ):
            _optional_figi(value, label)
        for value in (
            self.observed_composite_market_code,
            self.canonical_composite_market_code,
        ):
            _optional_country(value)
        for value, label in (
            (self.transition_relation_id, "transition relation ID"),
            (self.predecessor_asset_id, "transition predecessor asset ID"),
            (self.successor_asset_id, "transition successor asset ID"),
        ):
            _optional_digest(value, label)
        self._validate_source_scope()
        self._validate_responsibility()

    def _validate_source_scope(self) -> None:
        if type(self.source_scope) is not tuple or any(
            type(item) is not RegistrySourceScopeRow for item in self.source_scope
        ):
            raise I3DispatchError("registry decision source scope is invalid")
        if not self.source_scope:
            if self.production_registry_row is not None:
                raise I3DispatchError(
                    "fixture registry decision cannot carry a production registry row"
                )
            return
        if tuple(sorted(self.source_scope)) != self.source_scope:
            raise I3DispatchError("production registry source scope must be sorted")
        source_ids = tuple(item.source_record_id for item in self.source_scope)
        if len(source_ids) != len(set(source_ids)):
            raise I3DispatchError("production registry source scope repeats a source record")
        if self.source_record_id != min(source_ids):
            raise I3DispatchError(
                "production registry representative source record is not canonical"
            )
        if self.production_registry_row is None:
            raise I3DispatchError("production registry decision lacks its complete registry row")
        normalized = _snapshot_json_value(self.production_registry_row)
        if not isinstance(normalized, dict):  # pragma: no cover - Mapping input proves
            raise I3DispatchError("production registry row is invalid")
        object.__setattr__(self, "production_registry_row", MappingProxyType(normalized))
        for item in self.source_scope:
            if (
                item.provider_id != self.provider_id
                or item.provider_market != self.provider_market
                or item.provider_locale != self.provider_locale
                or item.ticker != self.ticker
                or item.session_date < self.effective_from_session
                or (
                    self.effective_to_session is not None
                    and item.session_date > self.effective_to_session
                )
            ):
                raise I3DispatchError("production registry decision crossed its exact source scope")

    @property
    def source_record_ids(self) -> tuple[str, ...]:
        """All exact member source IDs; fixtures remain one-record scopes."""

        if not self.source_scope:
            return (self.source_record_id,)
        return tuple(sorted(item.source_record_id for item in self.source_scope))

    @property
    def is_production_registry_decision(self) -> bool:
        return bool(self.source_scope)

    def _validate_responsibility(self) -> None:
        if self.registry_kind is IdentityRegistryKind.IDENTITY_CROSS_MARKET_ADJUDICATION:
            if self.identity_disposition not in _CROSS_MARKET_DISPOSITIONS:
                raise I3DispatchError("cross-market adjudication has the wrong closed disposition")
            if self.observed_composite_figi is None:
                raise I3DispatchError("cross-market adjudication requires observed Composite FIGI")
            if self.observed_composite_market_code in {None, "US"}:
                raise I3DispatchError(
                    "cross-market adjudication requires one non-US observed market"
                )
            if self.observed_share_class_figi is None:
                raise I3DispatchError(
                    "cross-market adjudication requires observed Share Class scope"
                )
            if self.identity_disposition == "confirmed_provider_contamination":
                if (
                    self.canonical_composite_figi is None
                    or self.canonical_composite_figi == self.observed_composite_figi
                    or self.canonical_composite_market_code != "US"
                ):
                    raise I3DispatchError(
                        "confirmed cross-market adjudication requires a distinct US target"
                    )
            elif (
                self.canonical_composite_figi is not None
                or self.canonical_composite_market_code is not None
            ):
                raise I3DispatchError(
                    "unresolved cross-market adjudication must have a null target"
                )
            if any(
                value is not None
                for value in (
                    self.composite_scope_figi,
                    self.canonical_share_class_figi,
                    self.transition_relation_id,
                    self.predecessor_asset_id,
                    self.successor_asset_id,
                )
            ):
                raise I3DispatchError("cross-market adjudication crossed its responsibility")
            return
        if self.registry_kind is IdentityRegistryKind.IDENTITY_ADJUDICATION:
            if self.identity_disposition not in _IDENTITY_ADJUDICATION_DISPOSITIONS:
                raise I3DispatchError("identity adjudication has an invalid disposition")
            if self.observed_composite_figi is None:
                raise I3DispatchError("identity adjudication requires observed Composite FIGI")
            if self.identity_disposition == "confirmed_genuine_transition":
                if self.canonical_composite_figi != self.observed_composite_figi:
                    raise I3DispatchError(
                        "genuine identity adjudication must preserve the observed Composite"
                    )
            elif self.identity_disposition == "confirmed_provider_contamination":
                if (
                    self.canonical_composite_figi is None
                    or self.canonical_composite_figi == self.observed_composite_figi
                ):
                    raise I3DispatchError(
                        "contamination identity adjudication requires a distinct target"
                    )
            elif self.canonical_composite_figi is not None:
                raise I3DispatchError("unresolved identity adjudication must have a null target")
            if any(
                value is not None
                for value in (
                    self.composite_scope_figi,
                    self.observed_composite_market_code,
                    self.canonical_composite_market_code,
                    self.observed_share_class_figi,
                    self.canonical_share_class_figi,
                    self.transition_relation_id,
                    self.predecessor_asset_id,
                    self.successor_asset_id,
                )
            ):
                raise I3DispatchError("identity adjudication crossed its responsibility")
            return
        if self.registry_kind is IdentityRegistryKind.PROVIDER_COMPOSITE_OVERRIDE:
            if self.identity_disposition not in _PROVIDER_COMPOSITE_OVERRIDE_DISPOSITIONS:
                raise I3DispatchError("provider Composite override has an invalid disposition")
            if self.observed_composite_figi is None:
                raise I3DispatchError("provider Composite override requires observed Composite")
            if self.observed_composite_market_code != "US":
                raise I3DispatchError("provider Composite override must be same-market US")
            if self.transition_relation_id is None:
                raise I3DispatchError(
                    "provider Composite override requires exactly one asset-transition relation"
                )
            if self.identity_disposition == "confirmed_provider_composite_stale_after_transition":
                if (
                    self.canonical_composite_figi is None
                    or self.canonical_composite_figi == self.observed_composite_figi
                    or self.canonical_composite_market_code != self.observed_composite_market_code
                ):
                    raise I3DispatchError(
                        "confirmed provider Composite override requires a distinct "
                        "same-market target"
                    )
            elif (
                self.canonical_composite_figi is not None
                or self.canonical_composite_market_code is not None
            ):
                raise I3DispatchError(
                    "unresolved provider Composite override must have a null target"
                )
            if any(
                value is not None
                for value in (
                    self.composite_scope_figi,
                    self.observed_share_class_figi,
                    self.canonical_share_class_figi,
                    self.predecessor_asset_id,
                    self.successor_asset_id,
                )
            ):
                raise I3DispatchError("provider Composite override crossed its responsibility")
            return
        if self.registry_kind is IdentityRegistryKind.SHARE_CLASS_ADJUDICATION:
            if self.identity_disposition not in _SHARE_CLASS_ADJUDICATION_DISPOSITIONS:
                raise I3DispatchError("Share Class adjudication has an invalid disposition")
            required = (
                self.observed_composite_figi,
                self.composite_scope_figi,
                self.observed_share_class_figi,
            )
            if any(value is None for value in required):
                raise I3DispatchError("Share Class adjudication requires its narrow scope")
            if self.identity_disposition == "confirmed_share_class_correction":
                if (
                    self.canonical_share_class_figi is None
                    or self.canonical_share_class_figi == self.observed_share_class_figi
                ):
                    raise I3DispatchError(
                        "confirmed Share Class adjudication requires a distinct target"
                    )
            elif self.canonical_share_class_figi is not None:
                raise I3DispatchError("unresolved Share Class adjudication must have a null target")
            if any(
                value is not None
                for value in (
                    self.canonical_composite_figi,
                    self.observed_composite_market_code,
                    self.canonical_composite_market_code,
                    self.transition_relation_id,
                    self.predecessor_asset_id,
                    self.successor_asset_id,
                )
            ):
                raise I3DispatchError("Share Class adjudication crossed its responsibility")
            return
        if self.registry_kind is IdentityRegistryKind.ASSET_TRANSITION:
            if self.identity_disposition not in _ASSET_TRANSITION_DISPOSITIONS:
                raise I3DispatchError("asset transition has an invalid disposition")
            if self.predecessor_asset_id is None:
                raise I3DispatchError("asset transition requires a predecessor")
            if self.identity_disposition == "confirmed_genuine_transition":
                if (
                    self.successor_asset_id is None
                    or self.predecessor_asset_id == self.successor_asset_id
                ):
                    raise I3DispatchError("confirmed asset transition requires distinct endpoints")
            elif self.successor_asset_id is not None:
                raise I3DispatchError("unresolved asset transition must have a null successor")
            if any(
                value is not None
                for value in (
                    self.observed_composite_figi,
                    self.canonical_composite_figi,
                    self.observed_composite_market_code,
                    self.canonical_composite_market_code,
                    self.composite_scope_figi,
                    self.observed_share_class_figi,
                    self.canonical_share_class_figi,
                    self.transition_relation_id,
                )
            ):
                raise I3DispatchError("asset transition cannot execute an identity override")
            return
        raise I3DispatchError("registry responsibility is not implemented")

    def applies_to(self, observation: IdentityObservation) -> bool:
        if (
            observation.provider_id != self.provider_id
            or observation.provider_market != self.provider_market
            or observation.provider_locale != self.provider_locale
            or observation.ticker != self.ticker
            or observation.source_record_id not in self.source_record_ids
            or observation.session_date < self.effective_from_session
            or (
                self.effective_to_session is not None
                and observation.session_date > self.effective_to_session
            )
        ):
            return False
        if self.registry_kind in _COMPOSITE_OVERRIDE_REGISTRIES:
            if observation.observed_composite_figi != self.observed_composite_figi:
                return False
            if self.registry_kind is IdentityRegistryKind.IDENTITY_CROSS_MARKET_ADJUDICATION:
                return (
                    observation.observed_composite_country == self.observed_composite_market_code
                    and observation.observed_share_class_figi == self.observed_share_class_figi
                )
            if self.registry_kind is IdentityRegistryKind.PROVIDER_COMPOSITE_OVERRIDE:
                return observation.observed_composite_country == (
                    self.observed_composite_market_code
                )
            return True
        if self.registry_kind is IdentityRegistryKind.SHARE_CLASS_ADJUDICATION:
            return (
                observation.observed_composite_figi == self.observed_composite_figi
                and observation.observed_share_class_figi == self.observed_share_class_figi
            )
        return self.registry_kind is IdentityRegistryKind.ASSET_TRANSITION

    def to_dict(self) -> dict[str, object]:
        result: dict[str, object] = {
            "canonical_composite_figi": self.canonical_composite_figi,
            "canonical_composite_market_code": self.canonical_composite_market_code,
            "canonical_share_class_figi": self.canonical_share_class_figi,
            "composite_scope_figi": self.composite_scope_figi,
            "decision_available_session": self.decision_available_session.isoformat(),
            "decision_id": self.decision_id,
            "effective_from_session": self.effective_from_session.isoformat(),
            "effective_to_session": (
                self.effective_to_session.isoformat()
                if self.effective_to_session is not None
                else None
            ),
            "identity_disposition": self.identity_disposition,
            "observed_composite_figi": self.observed_composite_figi,
            "observed_composite_market_code": self.observed_composite_market_code,
            "observed_share_class_figi": self.observed_share_class_figi,
            "predecessor_asset_id": self.predecessor_asset_id,
            "provider_id": self.provider_id,
            "provider_locale": self.provider_locale,
            "provider_market": self.provider_market,
            "registry_kind": self.registry_kind.value,
            "registry_release_id": self.registry_release_id,
            "source_record_id": self.source_record_id,
            "successor_asset_id": self.successor_asset_id,
            "ticker": self.ticker,
            "transition_relation_id": self.transition_relation_id,
        }
        if self.is_production_registry_decision:
            result.update(
                {
                    "production_registry_row": dict(self.production_registry_row or {}),
                    "source_record_ids": list(self.source_record_ids),
                    "source_scope": [item.to_dict() for item in self.source_scope],
                }
            )
        return result


@dataclass(frozen=True, slots=True, init=False)
class IdentityPolicySnapshot:
    """Sealed policy parsed from one authenticated five-registry trust boundary.

    Fixture and production loaders have distinct sources and ID rules.  Both
    construct the subject/source index and snapshot ID exactly once, so a row
    dispatch considers only decisions whose source scope contains that exact
    provider membership.
    """

    policy_bundle: IdentityPolicyBundle
    decisions: tuple[RegistryDecision, ...]
    policy_source: str
    _decision_index: Mapping[tuple[str, str, str, str, str], tuple[RegistryDecision, ...]] = field(
        repr=False, compare=False
    )
    _decision_by_id: Mapping[str, RegistryDecision] = field(repr=False, compare=False)
    _policy_snapshot_id: str = field(repr=False, compare=False)
    _production_release_set_binding_digest: str | None = field(repr=False, compare=False)
    _seal: object = field(repr=False, compare=False)

    def __init__(
        self,
        *,
        policy_bundle: IdentityPolicyBundle,
        decisions: tuple[RegistryDecision, ...],
        decision_index: Mapping[tuple[str, str, str, str, str], tuple[RegistryDecision, ...]],
        decision_by_id: Mapping[str, RegistryDecision],
        policy_snapshot_id: str,
        policy_source: str,
        production_release_set_binding_digest: str | None,
        _seal: object,
    ) -> None:
        if _seal is not _POLICY_SNAPSHOT_SEAL:
            raise I3DispatchError(
                "identity policy snapshots require an authenticated registry trust boundary"
            )
        object.__setattr__(self, "policy_bundle", policy_bundle)
        object.__setattr__(self, "decisions", decisions)
        object.__setattr__(self, "policy_source", policy_source)
        object.__setattr__(self, "_decision_index", MappingProxyType(dict(decision_index)))
        object.__setattr__(self, "_decision_by_id", MappingProxyType(dict(decision_by_id)))
        object.__setattr__(self, "_policy_snapshot_id", policy_snapshot_id)
        object.__setattr__(
            self,
            "_production_release_set_binding_digest",
            production_release_set_binding_digest,
        )
        object.__setattr__(self, "_seal", _seal)

    @property
    def policy_snapshot_id(self) -> str:
        return self._policy_snapshot_id

    @property
    def production_release_set_binding_digest(self) -> str | None:
        """Exact loaded-release binding for production snapshots; null for fixtures."""

        return self._production_release_set_binding_digest

    def matching_decisions(self, observation: IdentityObservation) -> tuple[RegistryDecision, ...]:
        """Return exact-row candidates from the immutable subject/source index."""

        if type(observation) is not IdentityObservation:
            raise I3DispatchError("policy lookup observation is invalid")
        key = (
            observation.provider_id,
            observation.provider_market,
            observation.provider_locale,
            observation.ticker,
            observation.source_record_id,
        )
        return tuple(
            item for item in self._decision_index.get(key, ()) if item.applies_to(observation)
        )

    def decision_by_id(self, decision_id: str) -> RegistryDecision:
        """Return one exact decision without scanning the registry snapshot."""

        _digest(decision_id, "registry decision lookup ID")
        try:
            return self._decision_by_id[decision_id]
        except KeyError as exc:
            raise I3DispatchError(
                "registry decision ID is absent from the policy snapshot"
            ) from exc

    def to_dict(self) -> dict[str, object]:
        result: dict[str, object] = {
            "decisions": [item.to_dict() for item in self.decisions],
            "identity_policy_bundle_id": self.policy_bundle.identity_policy_bundle_id,
            "policy_snapshot_id": self.policy_snapshot_id,
        }
        if self.policy_source == I3_PRODUCTION_POLICY_SOURCE:
            result.update(
                {
                    "policy_source": self.policy_source,
                    "production_release_set_binding_digest": (
                        self.production_release_set_binding_digest
                    ),
                }
            )
        return result


@dataclass(frozen=True, slots=True, init=False)
class _VerifiedIdentityPolicyBatch:
    """One structurally verified snapshot handle reusable across a row batch."""

    _policy_snapshot: IdentityPolicySnapshot = field(repr=False, compare=False)
    policy_snapshot_id: str
    _bundle_object_id: int = field(repr=False, compare=False)
    _decisions_object_id: int = field(repr=False, compare=False)
    _decision_index_object_id: int = field(repr=False, compare=False)
    _decision_by_id_object_id: int = field(repr=False, compare=False)
    _policy_bundle_content_sha256: str = field(repr=False, compare=False)
    _decision_payload_digests: Mapping[str, str] = field(repr=False, compare=False)
    _snapshot_source_binding_digest: str = field(repr=False, compare=False)
    _seal: object = field(repr=False, compare=False)

    def __init__(
        self,
        *,
        policy_snapshot: IdentityPolicySnapshot,
        policy_snapshot_id: str,
        _seal: object,
    ) -> None:
        if _seal is not _VERIFIED_POLICY_BATCH_SEAL:
            raise I3DispatchError("verified policy batches require structural reproduction")
        object.__setattr__(self, "_policy_snapshot", policy_snapshot)
        object.__setattr__(self, "policy_snapshot_id", policy_snapshot_id)
        object.__setattr__(self, "_bundle_object_id", id(policy_snapshot.policy_bundle))
        object.__setattr__(self, "_decisions_object_id", id(policy_snapshot.decisions))
        object.__setattr__(self, "_decision_index_object_id", id(policy_snapshot._decision_index))
        object.__setattr__(self, "_decision_by_id_object_id", id(policy_snapshot._decision_by_id))
        object.__setattr__(
            self,
            "_policy_bundle_content_sha256",
            hashlib.sha256(policy_snapshot.policy_bundle.canonical_bytes()).hexdigest(),
        )
        object.__setattr__(
            self,
            "_decision_payload_digests",
            MappingProxyType(
                {
                    item.decision_id: stable_digest(item.to_dict())
                    for item in policy_snapshot.decisions
                }
            ),
        )
        object.__setattr__(
            self,
            "_snapshot_source_binding_digest",
            stable_digest(
                {
                    "policy_source": policy_snapshot.policy_source,
                    "production_release_set_binding_digest": (
                        policy_snapshot._production_release_set_binding_digest
                    ),
                }
            ),
        )
        object.__setattr__(self, "_seal", _seal)

    @property
    def policy_bundle(self) -> IdentityPolicyBundle:
        return self._policy_snapshot.policy_bundle

    def matching_decisions(self, observation: IdentityObservation) -> tuple[RegistryDecision, ...]:
        _require_verified_policy_batch(self)
        matches = self._policy_snapshot.matching_decisions(observation)
        for item in matches:
            if self._policy_snapshot._decision_by_id.get(
                item.decision_id
            ) is not item or self._decision_payload_digests.get(item.decision_id) != stable_digest(
                item.to_dict()
            ):
                raise I3DispatchError("verified policy decision payload changed after mint")
        return matches

    def decision_by_id(self, decision_id: str) -> RegistryDecision:
        _require_verified_policy_batch(self)
        item = self._policy_snapshot.decision_by_id(decision_id)
        if self._decision_payload_digests.get(decision_id) != stable_digest(item.to_dict()):
            raise I3DispatchError("verified policy decision payload changed after mint")
        return item


def load_fixture_identity_policy_snapshot(
    policy_bundle: IdentityPolicyBundle,
    *,
    registry_release_contents: tuple[bytes, ...],
) -> IdentityPolicySnapshot:
    """Parse the exact five canonical local-fixture registry releases.

    This intentionally accepts bytes rather than paths or a discovery root.
    Every byte string must reproduce its corresponding bundle ``ArtifactPin``
    and its release ID.  The format is a fixture-only trust boundary; it is not
    a loader for published S7 registry tables or remote execution authority.
    """

    if type(policy_bundle) is not IdentityPolicyBundle:
        raise I3DispatchError("fixture policy loader requires a typed policy bundle")
    if tuple(item.registry_kind for item in policy_bundle.registry_releases) != (
        IDENTITY_REGISTRY_ORDER
    ):
        raise I3DispatchError("fixture policy loader crossed the fixed registry order")
    if type(registry_release_contents) is not tuple or len(registry_release_contents) != len(
        IDENTITY_REGISTRY_ORDER
    ):
        raise I3DispatchError("fixture policy loader requires exactly five registry byte strings")

    decisions: list[RegistryDecision] = []
    for pin, content in zip(
        policy_bundle.registry_releases, registry_release_contents, strict=True
    ):
        if type(content) is not bytes or not content:
            raise I3DispatchError("fixture registry release content must be nonempty bytes")
        if len(content) != pin.artifact.bytes:
            raise I3DispatchError("fixture registry release byte count differs from exact pin")
        if hashlib.sha256(content).hexdigest() != pin.artifact.sha256:
            raise I3DispatchError("fixture registry release SHA differs from exact pin")
        document = _canonical_json_document(content, "fixture registry release")
        release = _closed_mapping(
            document,
            {
                "decision_cutoff_session",
                "decisions",
                "namespace",
                "registry_kind",
                "release_available_session",
                "release_id",
                "rule_version",
                "schema_version",
                "scope",
            },
            "fixture registry release",
        )
        _literal(
            release["namespace"],
            I3_FIXTURE_REGISTRY_RELEASE_NAMESPACE,
            "fixture registry namespace",
        )
        _literal(
            release["rule_version"],
            I3_FIXTURE_REGISTRY_RELEASE_RULE_VERSION,
            "fixture registry rule version",
        )
        _literal(
            release["schema_version"],
            I3_FIXTURE_REGISTRY_RELEASE_SCHEMA_VERSION,
            "fixture registry schema version",
        )
        _literal(release["scope"], "local_fixture_only", "fixture registry scope")
        _literal(release["registry_kind"], pin.registry_kind.value, "fixture registry kind")
        _literal(
            release["decision_cutoff_session"],
            pin.decision_cutoff_session.isoformat(),
            "fixture registry decision cutoff",
        )
        _literal(
            release["release_available_session"],
            pin.release_available_session.isoformat(),
            "fixture registry release availability",
        )
        release_id = _text(release["release_id"], "fixture registry release ID")
        _digest(release_id, "fixture registry release ID")
        logical_release = {key: value for key, value in release.items() if key != "release_id"}
        if release_id != stable_digest(logical_release):
            raise I3DispatchError("fixture registry release ID does not reproduce")
        if release_id != pin.release_id:
            raise I3DispatchError("fixture registry release differs from the pinned release")
        records = release["decisions"]
        if type(records) is not list:
            raise I3DispatchError("fixture registry decisions must be an array")
        parsed = tuple(
            _registry_decision_from_fixture_record(
                record,
                registry_kind=pin.registry_kind,
                registry_release_id=pin.release_id,
                decision_cutoff_session=pin.decision_cutoff_session,
                release_available_session=pin.release_available_session,
            )
            for record in records
        )
        if tuple(item.decision_id for item in parsed) != tuple(
            sorted({item.decision_id for item in parsed})
        ):
            raise I3DispatchError("fixture registry decisions must be sorted and unique")
        decisions.extend(parsed)
    return _mint_identity_policy_snapshot(
        policy_bundle,
        tuple(sorted(decisions, key=lambda item: item.decision_id)),
    )


def _registry_decision_from_fixture_record(
    value: object,
    *,
    registry_kind: IdentityRegistryKind,
    registry_release_id: str,
    decision_cutoff_session: date,
    release_available_session: date,
) -> RegistryDecision:
    record = _closed_mapping(
        value,
        {
            "canonical_composite_figi",
            "canonical_composite_market_code",
            "canonical_share_class_figi",
            "composite_scope_figi",
            "decision_available_session",
            "decision_id",
            "effective_from_session",
            "effective_to_session",
            "identity_disposition",
            "observed_composite_figi",
            "observed_composite_market_code",
            "observed_share_class_figi",
            "predecessor_asset_id",
            "provider_id",
            "provider_locale",
            "provider_market",
            "source_record_id",
            "successor_asset_id",
            "ticker",
            "transition_relation_id",
        },
        "fixture registry decision",
    )
    decision_id = _text(record["decision_id"], "fixture registry decision ID")
    _digest(decision_id, "fixture registry decision ID")
    logical_decision = {
        "namespace": I3_FIXTURE_REGISTRY_DECISION_NAMESPACE,
        "registry_kind": registry_kind.value,
        **{key: item for key, item in record.items() if key != "decision_id"},
    }
    if decision_id != stable_digest(logical_decision):
        raise I3DispatchError("fixture registry decision ID does not reproduce")
    decision_available_session = _date_from_json(
        record["decision_available_session"], "fixture decision availability"
    )
    effective_from_session = _date_from_json(
        record["effective_from_session"], "fixture decision effective-from"
    )
    effective_to_session = _optional_date_from_json(
        record["effective_to_session"], "fixture decision effective-to"
    )
    if decision_available_session > release_available_session:
        raise I3DispatchError("fixture decision availability exceeds its release availability")
    if effective_from_session > decision_cutoff_session:
        raise I3DispatchError("fixture decision begins after the registry decision cutoff")
    return RegistryDecision(
        registry_kind=registry_kind,
        registry_release_id=registry_release_id,
        decision_id=decision_id,
        provider_id=_text(record["provider_id"], "fixture decision provider ID"),
        provider_market=_text(record["provider_market"], "fixture decision provider market"),
        provider_locale=_text(record["provider_locale"], "fixture decision provider locale"),
        ticker=_text(record["ticker"], "fixture decision ticker"),
        source_record_id=_text(record["source_record_id"], "fixture decision source-record ID"),
        identity_disposition=_optional_text(
            record["identity_disposition"], "fixture decision identity disposition"
        ),
        decision_available_session=decision_available_session,
        effective_from_session=effective_from_session,
        effective_to_session=effective_to_session,
        observed_composite_figi=_optional_text(
            record["observed_composite_figi"], "fixture observed Composite FIGI"
        ),
        canonical_composite_figi=_optional_text(
            record["canonical_composite_figi"], "fixture canonical Composite FIGI"
        ),
        observed_composite_market_code=_optional_text(
            record["observed_composite_market_code"],
            "fixture observed Composite market code",
        ),
        canonical_composite_market_code=_optional_text(
            record["canonical_composite_market_code"],
            "fixture canonical Composite market code",
        ),
        composite_scope_figi=_optional_text(
            record["composite_scope_figi"], "fixture Composite scope FIGI"
        ),
        observed_share_class_figi=_optional_text(
            record["observed_share_class_figi"], "fixture observed Share Class FIGI"
        ),
        canonical_share_class_figi=_optional_text(
            record["canonical_share_class_figi"], "fixture canonical Share Class FIGI"
        ),
        transition_relation_id=_optional_text(
            record["transition_relation_id"], "fixture transition relation ID"
        ),
        predecessor_asset_id=_optional_text(
            record["predecessor_asset_id"], "fixture predecessor asset ID"
        ),
        successor_asset_id=_optional_text(
            record["successor_asset_id"], "fixture successor asset ID"
        ),
    )


def _mint_identity_policy_snapshot(
    policy_bundle: IdentityPolicyBundle,
    decisions: tuple[RegistryDecision, ...],
) -> IdentityPolicySnapshot:
    """Mint the fixture-only policy form without changing its ID semantics."""

    return _mint_policy_snapshot(
        policy_bundle,
        decisions,
        policy_source=I3_FIXTURE_POLICY_SOURCE,
        production_release_set_binding_digest=None,
    )


def _mint_production_identity_policy_snapshot(
    policy_bundle: IdentityPolicyBundle,
    decisions: tuple[RegistryDecision, ...],
    *,
    production_release_set_binding_digest: str,
) -> IdentityPolicySnapshot:
    """Internal mint used only after the production adapter replays releases."""

    _digest(
        production_release_set_binding_digest,
        "production registry release-set binding digest",
    )
    return _mint_policy_snapshot(
        policy_bundle,
        decisions,
        policy_source=I3_PRODUCTION_POLICY_SOURCE,
        production_release_set_binding_digest=production_release_set_binding_digest,
    )


def _mint_policy_snapshot(
    policy_bundle: IdentityPolicyBundle,
    decisions: tuple[RegistryDecision, ...],
    *,
    policy_source: str,
    production_release_set_binding_digest: str | None,
) -> IdentityPolicySnapshot:
    (
        canonical_decisions,
        decision_index,
        decision_by_id,
        policy_snapshot_id,
    ) = _canonical_policy_snapshot_components(
        policy_bundle,
        decisions,
        policy_source=policy_source,
        production_release_set_binding_digest=production_release_set_binding_digest,
    )
    return IdentityPolicySnapshot(
        policy_bundle=policy_bundle,
        decisions=canonical_decisions,
        decision_index=decision_index,
        decision_by_id=decision_by_id,
        policy_snapshot_id=policy_snapshot_id,
        policy_source=policy_source,
        production_release_set_binding_digest=production_release_set_binding_digest,
        _seal=_POLICY_SNAPSHOT_SEAL,
    )


def _canonical_policy_snapshot_components(
    policy_bundle: IdentityPolicyBundle,
    decisions: tuple[RegistryDecision, ...],
    *,
    policy_source: str,
    production_release_set_binding_digest: str | None,
) -> tuple[
    tuple[RegistryDecision, ...],
    dict[tuple[str, str, str, str, str], tuple[RegistryDecision, ...]],
    dict[str, RegistryDecision],
    str,
]:
    """Rebuild every derived snapshot structure from canonical decision records."""

    if type(policy_bundle) is not IdentityPolicyBundle:
        raise I3DispatchError("policy snapshot requires a typed five-registry bundle")
    if policy_source not in {I3_FIXTURE_POLICY_SOURCE, I3_PRODUCTION_POLICY_SOURCE}:
        raise I3DispatchError("policy snapshot source is invalid")
    if policy_source == I3_FIXTURE_POLICY_SOURCE:
        if production_release_set_binding_digest is not None:
            raise I3DispatchError("fixture policy cannot carry a production release-set binding")
    else:
        _digest(
            production_release_set_binding_digest,
            "production registry release-set binding digest",
        )
    try:
        reproduced_bundle = IdentityPolicyBundle.from_dict(policy_bundle.to_dict())
    except (AttributeError, I3CheckpointError, TypeError, ValueError) as exc:
        raise I3DispatchError("policy snapshot bundle does not reproduce") from exc
    if reproduced_bundle.to_dict() != policy_bundle.to_dict():  # pragma: no cover - defensive
        raise I3DispatchError("policy snapshot bundle does not reproduce")
    if type(decisions) is not tuple or any(
        type(item) is not RegistryDecision for item in decisions
    ):
        raise I3DispatchError("policy snapshot decisions are invalid")
    canonical_decisions = tuple(replace(item) for item in decisions)
    if tuple(item.decision_id for item in canonical_decisions) != tuple(
        sorted({item.decision_id for item in canonical_decisions})
    ):
        raise I3DispatchError("policy snapshot decisions must be sorted and unique")
    release_pins = {item.registry_kind: item for item in reproduced_bundle.registry_releases}
    for item in canonical_decisions:
        if policy_source == I3_FIXTURE_POLICY_SOURCE:
            if item.is_production_registry_decision:
                raise I3DispatchError("fixture policy contains a production registry decision")
            if item.decision_id != stable_digest(_registry_decision_identity_payload(item)):
                raise I3DispatchError("registry decision ID does not reproduce")
        elif not item.is_production_registry_decision:
            raise I3DispatchError("production policy contains a fixture registry decision")
        pin = release_pins[item.registry_kind]
        if item.registry_release_id != pin.release_id:
            raise I3DispatchError("registry decision differs from the pinned policy release")
        if item.decision_available_session > pin.release_available_session:
            raise I3DispatchError("registry decision was unavailable in its pinned release")
        if item.effective_from_session > pin.decision_cutoff_session:
            raise I3DispatchError("registry decision begins after its pinned decision cutoff")
    transitions = {
        item.decision_id: item
        for item in canonical_decisions
        if item.registry_kind is IdentityRegistryKind.ASSET_TRANSITION
    }
    for item in canonical_decisions:
        if item.registry_kind is not IdentityRegistryKind.PROVIDER_COMPOSITE_OVERRIDE:
            continue
        relation = transitions.get(item.transition_relation_id)
        if relation is None:
            raise I3DispatchError("provider Composite override lacks its pinned transition")
        common_relation_differs = (
            relation.provider_id != item.provider_id
            or relation.provider_market != item.provider_market
            or relation.provider_locale != item.provider_locale
            or relation.ticker != item.ticker
            or relation.identity_disposition != "confirmed_genuine_transition"
            or relation.decision_available_session > item.decision_available_session
            or relation.predecessor_asset_id != canonical_asset_id(item.observed_composite_figi)
        )
        if policy_source == I3_FIXTURE_POLICY_SOURCE:
            interval_relation_differs = (
                relation.effective_from_session > item.effective_from_session
                or (
                    relation.effective_to_session is not None
                    and (
                        item.effective_to_session is None
                        or relation.effective_to_session < item.effective_to_session
                    )
                )
            )
        else:
            # A production asset_transition is a bounded predecessor/successor
            # event.  It establishes the stale-provider correction; it is not
            # an override interval and therefore need not cover the later
            # provider_composite_override scope.
            interval_relation_differs = (
                relation.effective_from_session > item.effective_from_session
                or relation.effective_to_session is None
                or relation.effective_to_session > item.effective_from_session
            )
        confirmed_override_differs = (
            item.identity_disposition == "confirmed_provider_composite_stale_after_transition"
            and (relation.successor_asset_id != canonical_asset_id(item.canonical_composite_figi))
        )
        if common_relation_differs or interval_relation_differs or confirmed_override_differs:
            raise I3DispatchError("provider Composite override crossed its exact asset transition")
    mutable_index: dict[tuple[str, str, str, str, str], list[RegistryDecision]] = {}
    for item in canonical_decisions:
        for source_record_id in item.source_record_ids:
            key = (
                item.provider_id,
                item.provider_market,
                item.provider_locale,
                item.ticker,
                source_record_id,
            )
            mutable_index.setdefault(key, []).append(item)
    decision_index = {
        key: tuple(sorted(items, key=lambda item: item.decision_id))
        for key, items in mutable_index.items()
    }
    payload = {
        "decisions": [item.to_dict() for item in canonical_decisions],
        "identity_policy_bundle_id": reproduced_bundle.identity_policy_bundle_id,
    }
    if policy_source == I3_PRODUCTION_POLICY_SOURCE:
        payload.update(
            {
                "policy_source": policy_source,
                "production_release_set_binding_digest": (production_release_set_binding_digest),
            }
        )
    return (
        canonical_decisions,
        decision_index,
        {item.decision_id: item for item in canonical_decisions},
        stable_digest(payload),
    )


def _registry_decision_identity_payload(decision: RegistryDecision) -> dict[str, object]:
    record = decision.to_dict()
    record.pop("decision_id")
    record.pop("registry_kind")
    record.pop("registry_release_id")
    return {
        "namespace": I3_FIXTURE_REGISTRY_DECISION_NAMESPACE,
        "registry_kind": decision.registry_kind.value,
        **record,
    }


@dataclass(frozen=True, slots=True)
class RegistryDecisionLineage:
    """Typed availability projection for one decision actually applied to a row."""

    registry_kind: IdentityRegistryKind
    decision_id: str
    decision_available_session: date

    def __post_init__(self) -> None:
        if not isinstance(self.registry_kind, IdentityRegistryKind):
            raise I3DispatchError("decision-lineage registry kind is invalid")
        _digest(self.decision_id, "decision-lineage ID")
        _date(self.decision_available_session, "decision-lineage availability")

    @property
    def sort_key(self) -> tuple[int, str]:
        return (IDENTITY_REGISTRY_ORDER.index(self.registry_kind), self.decision_id)

    def to_dict(self) -> dict[str, str]:
        return {
            "decision_available_session": self.decision_available_session.isoformat(),
            "decision_id": self.decision_id,
            "registry_kind": self.registry_kind.value,
        }


@dataclass(frozen=True, slots=True)
class IdentityDispatchDecision:
    """Identity-only projection for the target membership row."""

    source_record_id: str
    session_date: date
    active_on_date: bool
    membership_preserved: bool
    market_consistency_checked: bool
    observed_composite_figi: str | None
    canonical_composite_figi: str | None
    canonical_share_class_figi: str | None
    composite_registry_decision_ids: tuple[str, ...]
    share_class_decision_ids: tuple[str, ...]
    asset_transition_decision_ids: tuple[str, ...]
    decision_lineage: tuple[RegistryDecisionLineage, ...]
    selected_decision_available_session: date | None
    composite_registry_collision: bool
    identity_resolution_status: str
    identity_resolution_method: str
    identity_disposition: str
    backtest_identity_eligible: bool
    alias_permitted: bool
    reason_codes: tuple[str, ...]

    def __post_init__(self) -> None:
        _digest(self.source_record_id, "dispatch source record ID")
        _date(self.session_date, "dispatch session")
        if any(
            type(item) is not bool
            for item in (
                self.active_on_date,
                self.membership_preserved,
                self.market_consistency_checked,
                self.composite_registry_collision,
                self.backtest_identity_eligible,
                self.alias_permitted,
            )
        ):
            raise I3DispatchError("dispatch Boolean field is invalid")
        _optional_figi(self.observed_composite_figi, "dispatch observed Composite FIGI")
        _optional_figi(self.canonical_composite_figi, "dispatch canonical Composite FIGI")
        _optional_figi(self.canonical_share_class_figi, "dispatch canonical Share Class FIGI")
        _sorted_digests(self.composite_registry_decision_ids, "Composite decision IDs")
        _sorted_digests(self.share_class_decision_ids, "Share Class decision IDs")
        _sorted_digests(self.asset_transition_decision_ids, "asset transition decision IDs")
        if type(self.decision_lineage) is not tuple or any(
            type(item) is not RegistryDecisionLineage for item in self.decision_lineage
        ):
            raise I3DispatchError("decision lineage must be a typed tuple")
        if tuple(item.sort_key for item in self.decision_lineage) != tuple(
            sorted({item.sort_key for item in self.decision_lineage})
        ):
            raise I3DispatchError("decision lineage must be sorted and unique")
        if {item.decision_id for item in self.decision_lineage} != set(
            (
                *self.composite_registry_decision_ids,
                *self.share_class_decision_ids,
                *self.asset_transition_decision_ids,
            )
        ):
            raise I3DispatchError("decision lineage differs from selected decision IDs")
        if self.selected_decision_available_session is not None:
            _date(self.selected_decision_available_session, "selected decision availability")
        has_selected_decision = bool(
            self.composite_registry_decision_ids
            or self.share_class_decision_ids
            or self.asset_transition_decision_ids
        )
        if has_selected_decision != (self.selected_decision_available_session is not None):
            raise I3DispatchError("selected decision availability does not match decision lineage")
        if self.decision_lineage and self.selected_decision_available_session != max(
            item.decision_available_session for item in self.decision_lineage
        ):
            raise I3DispatchError("selected decision availability differs from typed lineage")
        _token(self.identity_resolution_status, "identity resolution status")
        _token(self.identity_resolution_method, "identity resolution method")
        _token(self.identity_disposition, "identity disposition")
        _sorted_tokens(self.reason_codes, "identity reason codes")
        if not self.membership_preserved:
            raise I3DispatchError("identity dispatcher cannot remove membership")
        if self.composite_registry_collision and (
            self.canonical_composite_figi is not None
            or self.backtest_identity_eligible
            or self.alias_permitted
            or self.identity_resolution_status != "unresolved_registry_collision"
        ):
            raise I3DispatchError("Composite collision did not fail closed")
        if self.backtest_identity_eligible != self.alias_permitted:
            raise I3DispatchError("identity eligibility and alias permission diverged")
        if self.backtest_identity_eligible and self.canonical_composite_figi is None:
            raise I3DispatchError("identity-eligible row lacks one canonical Composite")

    def to_dict(self) -> dict[str, object]:
        return {
            "active_on_date": self.active_on_date,
            "alias_permitted": self.alias_permitted,
            "asset_transition_decision_ids": list(self.asset_transition_decision_ids),
            "backtest_identity_eligible": self.backtest_identity_eligible,
            "canonical_composite_figi": self.canonical_composite_figi,
            "canonical_share_class_figi": self.canonical_share_class_figi,
            "composite_registry_collision": self.composite_registry_collision,
            "composite_registry_decision_ids": list(self.composite_registry_decision_ids),
            "decision_lineage": [item.to_dict() for item in self.decision_lineage],
            "identity_resolution_status": self.identity_resolution_status,
            "identity_resolution_method": self.identity_resolution_method,
            "identity_disposition": self.identity_disposition,
            "market_consistency_checked": self.market_consistency_checked,
            "membership_preserved": self.membership_preserved,
            "observed_composite_figi": self.observed_composite_figi,
            "reason_codes": list(self.reason_codes),
            "session_date": self.session_date.isoformat(),
            "selected_decision_available_session": (
                self.selected_decision_available_session.isoformat()
                if self.selected_decision_available_session is not None
                else None
            ),
            "share_class_decision_ids": list(self.share_class_decision_ids),
            "source_record_id": self.source_record_id,
        }


@dataclass(frozen=True, slots=True)
class I3QaReasonCount:
    """One deterministic reason bucket for a raw-review QA metric."""

    reason_code: str
    count: int

    def __post_init__(self) -> None:
        _token(self.reason_code, "QA reason code")
        if type(self.count) is not int or self.count <= 0:
            raise I3DispatchError("QA reason count must be positive")

    def to_dict(self) -> dict[str, object]:
        return {"count": self.count, "reason_code": self.reason_code}


@dataclass(frozen=True, slots=True)
class I3QaBoundedExample:
    """One content-addressed, deterministically ordered raw-review example."""

    source_record_id: str
    reason_codes: tuple[str, ...]

    def __post_init__(self) -> None:
        _digest(self.source_record_id, "QA example source-record ID")
        if not self.reason_codes:
            raise I3DispatchError("QA bounded example requires at least one reason")
        _sorted_tokens(self.reason_codes, "QA bounded example reasons")

    @property
    def sort_key(self) -> tuple[str, tuple[str, ...]]:
        return (self.source_record_id, self.reason_codes)

    def to_dict(self) -> dict[str, object]:
        return {
            "reason_codes": list(self.reason_codes),
            "source_record_id": self.source_record_id,
        }


@dataclass(frozen=True, slots=True)
class I3QaResult:
    """One result under the exact closed catalog."""

    check_id: str
    semantics_digest: str
    observed_count: int
    failure_count: int
    evaluation_status: str
    bounded_example_ids: tuple[str, ...] = ()
    reason_counts: tuple[I3QaReasonCount, ...] = ()
    bounded_examples: tuple[I3QaBoundedExample, ...] = ()

    def __post_init__(self) -> None:
        _token(self.check_id, "QA result check ID")
        _digest(self.semantics_digest, "QA result semantics digest")
        _nonnegative_int(self.observed_count, "QA observed count")
        _nonnegative_int(self.failure_count, "QA failure count")
        if self.evaluation_status not in {"evaluated", "deferred_to_materialization"}:
            raise I3DispatchError("QA evaluation status is invalid")
        if self.evaluation_status == "deferred_to_materialization" and (
            self.observed_count
            or self.failure_count
            or self.bounded_example_ids
            or self.reason_counts
            or self.bounded_examples
        ):
            raise I3DispatchError("deferred QA cannot report evaluated counts")
        if self.failure_count > self.observed_count:
            raise I3DispatchError("QA failure count exceeds observed count")
        if len(self.bounded_example_ids) > 20:
            raise I3DispatchError("QA bounded examples exceed the fixed limit")
        _sorted_digests(self.bounded_example_ids, "QA bounded example IDs")
        if type(self.reason_counts) is not tuple or any(
            type(item) is not I3QaReasonCount for item in self.reason_counts
        ):
            raise I3DispatchError("QA reason counts must be a typed tuple")
        if tuple(item.reason_code for item in self.reason_counts) != tuple(
            sorted({item.reason_code for item in self.reason_counts})
        ):
            raise I3DispatchError("QA reason counts must be sorted and unique")
        if type(self.bounded_examples) is not tuple or any(
            type(item) is not I3QaBoundedExample for item in self.bounded_examples
        ):
            raise I3DispatchError("QA bounded examples must be a typed tuple")
        if len(self.bounded_examples) > 20:
            raise I3DispatchError("QA typed bounded examples exceed the fixed limit")
        if tuple(item.sort_key for item in self.bounded_examples) != tuple(
            sorted({item.sort_key for item in self.bounded_examples})
        ):
            raise I3DispatchError("QA bounded examples must be sorted and unique")

    def to_dict(self) -> dict[str, object]:
        return {
            "bounded_example_ids": list(self.bounded_example_ids),
            "bounded_examples": [item.to_dict() for item in self.bounded_examples],
            "check_id": self.check_id,
            "failure_count": self.failure_count,
            "evaluation_status": self.evaluation_status,
            "observed_count": self.observed_count,
            "reason_counts": [item.to_dict() for item in self.reason_counts],
            "semantics_digest": self.semantics_digest,
        }


@dataclass(frozen=True, slots=True)
class I3QaReceipt:
    """Complete exact-catalog QA projection for one target dispatch."""

    qa_catalog_digest: str
    results: tuple[I3QaResult, ...]

    def __post_init__(self) -> None:
        if self.qa_catalog_digest != I3_QA_CATALOG_DIGEST:
            raise I3DispatchError("QA receipt uses an unknown catalog")
        expected = tuple(item.check_id for item in I3_QA_CATALOG)
        if tuple(item.check_id for item in self.results) != expected:
            raise I3DispatchError("QA receipt does not cover the exact closed catalog")
        by_id = {item.check_id: item for item in I3_QA_CATALOG}
        if any(
            result.semantics_digest != by_id[result.check_id].semantics_digest
            for result in self.results
        ):
            raise I3DispatchError("QA result semantics differ from the closed catalog")
        if any(
            result.evaluation_status
            != (
                "evaluated"
                if by_id[result.check_id].owner == "dispatcher"
                else "deferred_to_materialization"
            )
            for result in self.results
        ):
            raise I3DispatchError("QA evaluation status differs from catalog ownership")
        raw_review = set(I3_RAW_REVIEW_CHECK_IDS)
        for result in self.results:
            if result.check_id in raw_review:
                if sum(item.count for item in result.reason_counts) != result.observed_count:
                    raise I3DispatchError("raw-review QA reason counts differ from observed count")
                if result.observed_count and (
                    not result.reason_counts or not result.bounded_examples
                ):
                    raise I3DispatchError(
                        "observed raw-review QA requires reasons and bounded examples"
                    )
                example_ids = tuple(
                    sorted({item.source_record_id for item in result.bounded_examples})
                )
                if result.bounded_example_ids != example_ids:
                    raise I3DispatchError("raw-review QA example IDs differ from typed examples")
                known_reasons = {item.reason_code for item in result.reason_counts}
                if any(
                    not set(item.reason_codes).issubset(known_reasons)
                    for item in result.bounded_examples
                ):
                    raise I3DispatchError("raw-review QA example names an unknown reason")
            elif result.reason_counts or result.bounded_examples:
                raise I3DispatchError("non-review QA cannot emit raw-review details")

    @property
    def qa_receipt_id(self) -> str:
        return stable_digest(self.to_dict(include_id=False))

    @property
    def critical_failure_count(self) -> int:
        severity = {item.check_id: item.severity for item in I3_QA_CATALOG}
        return sum(
            item.failure_count
            for item in self.results
            if severity[item.check_id] is QaSeverity.CRITICAL
        )

    def to_dict(self, *, include_id: bool = True) -> dict[str, object]:
        result: dict[str, object] = {
            "qa_catalog_digest": self.qa_catalog_digest,
            "results": [item.to_dict() for item in self.results],
        }
        if include_id:
            result["qa_receipt_id"] = self.qa_receipt_id
        return result


@dataclass(frozen=True, slots=True, init=False)
class I3RowSemanticProof:
    """Sealed content proof for the one target identity projection."""

    source_window_digest: str
    coverage_receipt_id: str
    policy_snapshot_id: str
    decision_digest: str
    validator_semantics_digest: str
    proof_id: str
    _seal: object = field(init=False, repr=False, compare=False)

    def __init__(
        self,
        *,
        source_window_digest: str,
        coverage_receipt_id: str,
        policy_snapshot_id: str,
        decision_digest: str,
        validator_semantics_digest: str,
        proof_id: str,
        _seal: object,
    ) -> None:
        if _seal is not _ROW_PROOF_SEAL:
            raise I3DispatchError("I3 row proofs can only be minted by the closed dispatcher")
        for name, value in (
            ("source_window_digest", source_window_digest),
            ("coverage_receipt_id", coverage_receipt_id),
            ("policy_snapshot_id", policy_snapshot_id),
            ("decision_digest", decision_digest),
            ("validator_semantics_digest", validator_semantics_digest),
            ("proof_id", proof_id),
        ):
            object.__setattr__(self, name, value)
        object.__setattr__(self, "_seal", _seal)

    def logical_payload(self) -> dict[str, object]:
        return {
            "coverage_receipt_id": self.coverage_receipt_id,
            "decision_digest": self.decision_digest,
            "policy_snapshot_id": self.policy_snapshot_id,
            "rule_version": I3_ROW_PROOF_RULE_VERSION,
            "source_window_digest": self.source_window_digest,
            "validator_semantics_digest": self.validator_semantics_digest,
        }

    def to_dict(self) -> dict[str, object]:
        return {"proof_id": self.proof_id, **self.logical_payload()}


@dataclass(frozen=True, slots=True, init=False)
class I3LocalStagingAttestation:
    """Runtime-only local staging proof; all mutation/publication powers are false."""

    decision: IdentityDispatchDecision
    row_proof: I3RowSemanticProof
    qa_receipt: I3QaReceipt
    attestation_id: str
    _seal: object = field(init=False, repr=False, compare=False)

    def __init__(
        self,
        *,
        decision: IdentityDispatchDecision,
        row_proof: I3RowSemanticProof,
        qa_receipt: I3QaReceipt,
        attestation_id: str,
        _seal: object,
    ) -> None:
        if _seal is not _LOCAL_ATTESTATION_SEAL:
            raise I3DispatchError("local staging attestations require the closed dispatcher")
        object.__setattr__(self, "decision", decision)
        object.__setattr__(self, "row_proof", row_proof)
        object.__setattr__(self, "qa_receipt", qa_receipt)
        object.__setattr__(self, "attestation_id", attestation_id)
        object.__setattr__(self, "_seal", _seal)

    @property
    def allows_publish(self) -> bool:
        return False

    @property
    def allows_correction(self) -> bool:
        return False

    @property
    def allows_partition_replacement(self) -> bool:
        return False

    @property
    def allows_registry_mutation(self) -> bool:
        return False

    @property
    def allows_base_cutover(self) -> bool:
        return False

    def logical_payload(self) -> dict[str, object]:
        return {
            "decision": self.decision.to_dict(),
            "qa_receipt": self.qa_receipt.to_dict(),
            "row_proof": self.row_proof.to_dict(),
            "rule_version": I3_LOCAL_ATTESTATION_RULE_VERSION,
            "scope": "local_staging_only",
        }

    def to_dict(self) -> dict[str, object]:
        return {"attestation_id": self.attestation_id, **self.logical_payload()}


@dataclass(frozen=True, slots=True)
class _ObservationEvaluation:
    canonical_composite_figi: str | None
    canonical_share_class_figi: str | None
    composite_decision_ids: tuple[str, ...]
    share_class_decision_ids: tuple[str, ...]
    asset_transition_decision_ids: tuple[str, ...]
    decision_lineage: tuple[RegistryDecisionLineage, ...]
    selected_decision_available_session: date | None
    composite_collision: bool
    approved_cross_market: bool
    foreign_composite: bool
    market_consistent: bool
    status: str
    resolution_method: str
    disposition: str
    eligible: bool
    reason_codes: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class _BounceFacts:
    detected: bool
    inverse: bool
    inverse_misclassified_as_genuine_transition: bool
    middle_approved: bool
    middle_unapproved_eligible: bool
    middle_source_record_id: str | None
    reason_code: str | None


def dispatch_i3_identity_window(
    *,
    policy_snapshot: IdentityPolicySnapshot,
    coverage: AliasSourceCoverageReceipt,
    observations: tuple[IdentityObservation, ...],
) -> I3LocalStagingAttestation:
    """Run the only I3 row dispatcher and mint a local-only staging proof."""

    verified = _verify_i3_identity_policy_snapshot_for_batch(policy_snapshot)
    return _dispatch_i3_identity_window(
        verified_policy=verified,
        coverage=coverage,
        observations=observations,
    )


def _verify_i3_identity_policy_snapshot_for_batch(
    policy_snapshot: IdentityPolicySnapshot,
) -> _VerifiedIdentityPolicyBatch:
    """Rebuild one snapshot once before O(bucket) row dispatches."""

    verified = _verify_policy_snapshot(policy_snapshot)
    return _VerifiedIdentityPolicyBatch(
        policy_snapshot=verified,
        policy_snapshot_id=verified.policy_snapshot_id,
        _seal=_VERIFIED_POLICY_BATCH_SEAL,
    )


def _dispatch_i3_identity_window_from_verified_batch(
    *,
    verified_policy: _VerifiedIdentityPolicyBatch,
    coverage: AliasSourceCoverageReceipt,
    observations: tuple[IdentityObservation, ...],
) -> I3LocalStagingAttestation:
    """Dispatch one row window after one batch-level structural verification."""

    verified = _require_verified_policy_batch(verified_policy)
    return _dispatch_i3_identity_window(
        verified_policy=verified,
        coverage=coverage,
        observations=observations,
    )


def verify_i3_local_staging_attestation(
    value: object,
    *,
    policy_snapshot: IdentityPolicySnapshot,
    coverage: AliasSourceCoverageReceipt,
    observations: tuple[IdentityObservation, ...],
) -> I3LocalStagingAttestation:
    """Re-run fixed semantics and compare every byte-derived logical field."""

    if type(value) is not I3LocalStagingAttestation or (value._seal is not _LOCAL_ATTESTATION_SEAL):
        raise I3DispatchError("value is not a sealed I3 local staging attestation")
    verified = _verify_i3_identity_policy_snapshot_for_batch(policy_snapshot)
    reproduced = _dispatch_i3_identity_window(
        verified_policy=verified,
        coverage=coverage,
        observations=observations,
    )
    if value.to_dict() != reproduced.to_dict():
        raise I3DispatchError("I3 local staging attestation does not reproduce")
    return value


def _dispatch_i3_identity_window(
    *,
    verified_policy: _VerifiedIdentityPolicyBatch,
    coverage: AliasSourceCoverageReceipt,
    observations: tuple[IdentityObservation, ...],
) -> I3LocalStagingAttestation:
    policy_snapshot = _require_verified_policy_batch(verified_policy)
    _verify_coverage(coverage)
    if policy_snapshot.policy_bundle.policy_available_session > coverage.coverage_available_session:
        raise I3DispatchError(
            "identity policy wrapper was unavailable at the source-coverage cutoff"
        )
    ordered = _bind_observations_to_coverage(coverage, observations)
    target = ordered[-1]
    if target is None:
        raise I3DispatchError("target coverage contains no membership row")
    evaluations = tuple(
        None if item is None else _evaluate_observation(policy_snapshot, item) for item in ordered
    )
    target_evaluation = evaluations[-1]
    if target_evaluation is None:  # pragma: no cover - target checked above
        raise I3DispatchError("target evaluation is absent")

    bounce = _bounce_facts(ordered, evaluations)
    reasons = set(target_evaluation.reason_codes)
    if bounce.inverse:
        reasons.add("inverse_bounce_detected_not_transition")
    decision = IdentityDispatchDecision(
        source_record_id=target.source_record_id,
        session_date=target.session_date,
        active_on_date=target.active_on_date,
        membership_preserved=True,
        market_consistency_checked=True,
        observed_composite_figi=target.observed_composite_figi,
        canonical_composite_figi=target_evaluation.canonical_composite_figi,
        canonical_share_class_figi=target_evaluation.canonical_share_class_figi,
        composite_registry_decision_ids=target_evaluation.composite_decision_ids,
        share_class_decision_ids=target_evaluation.share_class_decision_ids,
        asset_transition_decision_ids=(target_evaluation.asset_transition_decision_ids),
        decision_lineage=target_evaluation.decision_lineage,
        selected_decision_available_session=(target_evaluation.selected_decision_available_session),
        composite_registry_collision=target_evaluation.composite_collision,
        identity_resolution_status=target_evaluation.status,
        identity_resolution_method=target_evaluation.resolution_method,
        identity_disposition=target_evaluation.disposition,
        backtest_identity_eligible=target_evaluation.eligible,
        alias_permitted=target_evaluation.eligible,
        reason_codes=tuple(sorted(reasons)),
    )
    source_window_digest = stable_digest(
        {
            "coverage_receipt_id": coverage.coverage_receipt_id,
            "observations": [item.to_dict() for item in observations],
        }
    )
    proof_payload = {
        "coverage_receipt_id": coverage.coverage_receipt_id,
        "decision_digest": stable_digest(decision.to_dict()),
        "policy_snapshot_id": policy_snapshot.policy_snapshot_id,
        "rule_version": I3_ROW_PROOF_RULE_VERSION,
        "source_window_digest": source_window_digest,
        "validator_semantics_digest": I3_ROW_VALIDATOR_SEMANTICS_DIGEST,
    }
    proof = I3RowSemanticProof(
        source_window_digest=source_window_digest,
        coverage_receipt_id=coverage.coverage_receipt_id,
        policy_snapshot_id=policy_snapshot.policy_snapshot_id,
        decision_digest=stable_digest(decision.to_dict()),
        validator_semantics_digest=I3_ROW_VALIDATOR_SEMANTICS_DIGEST,
        proof_id=stable_digest(proof_payload),
        _seal=_ROW_PROOF_SEAL,
    )
    qa = _build_qa(
        target,
        target_evaluation,
        decision,
        bounce,
        proof,
        coverage_available_session=coverage.coverage_available_session,
    )
    if qa.critical_failure_count:
        raise I3DispatchError("closed dispatcher produced a Critical QA failure")
    logical = {
        "decision": decision.to_dict(),
        "qa_receipt": qa.to_dict(),
        "row_proof": proof.to_dict(),
        "rule_version": I3_LOCAL_ATTESTATION_RULE_VERSION,
        "scope": "local_staging_only",
    }
    return I3LocalStagingAttestation(
        decision=decision,
        row_proof=proof,
        qa_receipt=qa,
        attestation_id=stable_digest(logical),
        _seal=_LOCAL_ATTESTATION_SEAL,
    )


def _evaluate_observation(
    policy: _VerifiedIdentityPolicyBatch,
    observation: IdentityObservation,
) -> _ObservationEvaluation:
    matches = policy.matching_decisions(observation)
    composite = tuple(
        item for item in matches if item.registry_kind in _COMPOSITE_OVERRIDE_REGISTRIES
    )
    share_class = tuple(
        item
        for item in matches
        if item.registry_kind is IdentityRegistryKind.SHARE_CLASS_ADJUDICATION
    )
    direct_transitions = tuple(
        item for item in matches if item.registry_kind is IdentityRegistryKind.ASSET_TRANSITION
    )
    related_transitions = tuple(
        policy.decision_by_id(item.transition_relation_id)
        for item in composite
        if item.registry_kind is IdentityRegistryKind.PROVIDER_COMPOSITE_OVERRIDE
        and item.transition_relation_id is not None
    )
    transitions = tuple(
        sorted(
            {
                item.decision_id: item for item in (*direct_transitions, *related_transitions)
            }.values(),
            key=lambda item: item.decision_id,
        )
    )
    market_consistent = observation.observed_composite_country == "US"
    foreign_composite = (
        observation.observed_composite_country is not None
        and observation.observed_composite_country != "US"
    )
    if any(
        item.registry_kind is IdentityRegistryKind.IDENTITY_CROSS_MARKET_ADJUDICATION
        and market_consistent
        for item in composite
    ):
        raise I3DispatchError("cross-market registry matched a US Composite observation")
    collision = len(composite) > 1
    if collision:
        return _ObservationEvaluation(
            canonical_composite_figi=None,
            canonical_share_class_figi=None,
            composite_decision_ids=tuple(sorted(item.decision_id for item in composite)),
            share_class_decision_ids=(),
            asset_transition_decision_ids=tuple(sorted(item.decision_id for item in transitions)),
            decision_lineage=tuple(
                RegistryDecisionLineage(
                    registry_kind=item.registry_kind,
                    decision_id=item.decision_id,
                    decision_available_session=item.decision_available_session,
                )
                for item in sorted(
                    (*composite, *transitions),
                    key=lambda item: (
                        IDENTITY_REGISTRY_ORDER.index(item.registry_kind),
                        item.decision_id,
                    ),
                )
            ),
            selected_decision_available_session=max(
                item.decision_available_session for item in (*composite, *transitions)
            ),
            composite_collision=True,
            approved_cross_market=False,
            foreign_composite=foreign_composite,
            market_consistent=market_consistent,
            status="unresolved_registry_collision",
            resolution_method="registry_collision_unresolved",
            disposition="pending_registry_collision_review",
            eligible=False,
            reason_codes=("multi_registry_composite_override_collision",),
        )

    approved_cross_market = False
    if composite:
        selected = composite[0]
        registry_disposition = selected.identity_disposition
        if registry_disposition is None:  # pragma: no cover - decision contract proves
            raise I3DispatchError("Composite registry decision lacks a disposition")
        unresolved = registry_disposition in {
            "adjudicated_unresolved",
            "cross_market_adjudicated_unresolved",
            "provider_composite_override_adjudicated_unresolved",
        }
        if unresolved:
            canonical_composite = None
            status = "unresolved"
            if selected.registry_kind is IdentityRegistryKind.IDENTITY_ADJUDICATION:
                resolution_method = "provider_figi_bounce_adjudicated_unresolved"
                disposition = "adjudicated_unresolved"
            elif selected.registry_kind is IdentityRegistryKind.IDENTITY_CROSS_MARKET_ADJUDICATION:
                resolution_method = "cross_market_composite_adjudicated_unresolved"
                disposition = "cross_market_adjudicated_unresolved"
            else:
                resolution_method = "adjudicated_unresolved"
                disposition = "adjudicated_unresolved"
        else:
            canonical_composite = selected.canonical_composite_figi
            approved_cross_market = (
                selected.registry_kind is IdentityRegistryKind.IDENTITY_CROSS_MARKET_ADJUDICATION
            )
            status = "resolved_approved_override"
            if selected.registry_kind is IdentityRegistryKind.PROVIDER_COMPOSITE_OVERRIDE:
                resolution_method = "approved_provider_composite_override"
                disposition = "provider_composite_stale_after_transition"
            elif selected.registry_kind is IdentityRegistryKind.IDENTITY_CROSS_MARKET_ADJUDICATION:
                resolution_method = "approved_cross_market_provider_contamination_override"
                disposition = "confirmed_provider_contamination"
            elif registry_disposition == "confirmed_genuine_transition":
                resolution_method = "approved_genuine_transition"
                disposition = "confirmed_genuine_transition"
            else:
                resolution_method = "approved_provider_contamination_override"
                disposition = "confirmed_provider_contamination"
        reasons: set[str] = {selected.registry_kind.value}
    elif market_consistent:
        canonical_composite = observation.observed_composite_figi
        status = "resolved_direct_observed"
        resolution_method = "source_composite_figi_exact"
        disposition = "observed_consistent"
        reasons = set()
    elif foreign_composite:
        canonical_composite = None
        status = "unresolved_cross_market"
        resolution_method = "cross_market_composite_pending_unresolved"
        disposition = "pending_cross_market_review"
        reasons = {"us_locale_non_us_composite_figi"}
    else:
        canonical_composite = None
        status = "unresolved_missing_composite"
        resolution_method = "missing_composite_pending_unresolved"
        disposition = "pending_identity_review"
        reasons = {"missing_observed_composite_figi"}

    applicable_share = tuple(
        item for item in share_class if item.composite_scope_figi == canonical_composite
    )
    if canonical_composite is not None and len(applicable_share) > 1:
        canonical_composite = None
        status = "unresolved_registry_collision"
        resolution_method = "registry_collision_unresolved"
        disposition = "registry_collision_unresolved"
        reasons.add("share_class_registry_collision")
    if canonical_composite is not None and len(applicable_share) == 1:
        selected_share = applicable_share[0]
        if selected_share.identity_disposition == "share_class_adjudicated_unresolved":
            canonical_composite = None
            canonical_share = None
            status = "unresolved"
            resolution_method = "adjudicated_unresolved"
            disposition = "adjudicated_unresolved"
            reasons.add("share_class_adjudicated_unresolved")
        else:
            canonical_share = selected_share.canonical_share_class_figi
    elif canonical_composite is not None:
        canonical_share = observation.observed_share_class_figi
    else:
        canonical_share = None
    eligible = canonical_composite is not None
    selected_decisions = (*composite, *applicable_share, *transitions)
    decision_lineage = tuple(
        RegistryDecisionLineage(
            registry_kind=item.registry_kind,
            decision_id=item.decision_id,
            decision_available_session=item.decision_available_session,
        )
        for item in sorted(
            selected_decisions,
            key=lambda item: (
                IDENTITY_REGISTRY_ORDER.index(item.registry_kind),
                item.decision_id,
            ),
        )
    )
    return _ObservationEvaluation(
        canonical_composite_figi=canonical_composite,
        canonical_share_class_figi=canonical_share,
        composite_decision_ids=tuple(sorted(item.decision_id for item in composite)),
        share_class_decision_ids=tuple(sorted(item.decision_id for item in applicable_share)),
        asset_transition_decision_ids=tuple(sorted(item.decision_id for item in transitions)),
        decision_lineage=decision_lineage,
        selected_decision_available_session=(
            max(item.decision_available_session for item in selected_decisions)
            if selected_decisions
            else None
        ),
        composite_collision=False,
        approved_cross_market=approved_cross_market,
        foreign_composite=foreign_composite,
        market_consistent=market_consistent,
        status=status,
        resolution_method=resolution_method,
        disposition=disposition,
        eligible=eligible,
        reason_codes=tuple(sorted(reasons)),
    )


def _bounce_facts(
    observations: tuple[IdentityObservation | None, ...],
    evaluations: tuple[_ObservationEvaluation | None, ...],
) -> _BounceFacts:
    if any(item is None for item in observations):
        return _BounceFacts(False, False, False, False, False, None, None)
    first, middle, target = observations
    first_figi = first.observed_composite_figi  # type: ignore[union-attr]
    middle_figi = middle.observed_composite_figi  # type: ignore[union-attr]
    target_figi = target.observed_composite_figi  # type: ignore[union-attr]
    detected = first_figi is not None and first_figi == target_figi and first_figi != middle_figi
    if not detected:
        return _BounceFacts(False, False, False, False, False, None, None)
    first_country = first.observed_composite_country  # type: ignore[union-attr]
    middle_country = middle.observed_composite_country  # type: ignore[union-attr]
    inverse = first_country != "US" and middle_country == "US"
    reason_code = (
        "foreign_us_foreign_inverse_bounce"
        if inverse
        else "us_foreign_us_provider_bounce"
        if first_country == "US" and middle_country != "US"
        else "same_market_or_unknown_composite_bounce"
    )
    middle_evaluation = evaluations[1]
    middle_approved = bool(
        middle_evaluation is not None
        and middle_evaluation.composite_decision_ids
        and middle_evaluation.eligible
        and not middle_evaluation.composite_collision
    )
    middle_unapproved_eligible = bool(
        not inverse
        and middle_evaluation is not None
        and middle_evaluation.eligible
        and not middle_evaluation.composite_decision_ids
    )
    inverse_misclassified = bool(
        inverse
        and middle_evaluation is not None
        and middle_evaluation.disposition == "confirmed_genuine_transition"
    )
    return _BounceFacts(
        detected=True,
        inverse=inverse,
        inverse_misclassified_as_genuine_transition=inverse_misclassified,
        middle_approved=middle_approved,
        middle_unapproved_eligible=middle_unapproved_eligible,
        middle_source_record_id=middle.source_record_id,  # type: ignore[union-attr]
        reason_code=reason_code,
    )


def _build_qa(
    target: IdentityObservation,
    evaluation: _ObservationEvaluation,
    decision: IdentityDispatchDecision,
    bounce: _BounceFacts,
    proof: I3RowSemanticProof,
    *,
    coverage_available_session: date,
) -> I3QaReceipt:
    if (
        decision.selected_decision_available_session is not None
        and decision.selected_decision_available_session > coverage_available_session
    ):
        raise I3DispatchError("selected registry decision was unavailable at coverage cutoff")
    collision = evaluation.composite_collision
    foreign = evaluation.foreign_composite
    example = (target.source_record_id,)
    bounce_example = (
        (bounce.middle_source_record_id,) if bounce.middle_source_record_id is not None else ()
    )
    counts: dict[str, tuple[int, int, tuple[str, ...]]] = {
        "availability_mismatch_rows": (0, 0, ()),
        "boundary_coverage_mismatch_rows": (1, 0, ()),
        "eligible_membership_missing_alias_rows": (0, 0, ()),
        "identity_quality_changed_active_rows": (
            1,
            int(decision.active_on_date != target.active_on_date),
            (),
        ),
        "identity_quality_forced_liquidation_rows": (0, 0, ()),
        "inactive_or_delisted_inferred_from_identity_quality_rows": (0, 0, ()),
        "ineligible_membership_with_alias_rows": (0, 0, ()),
        "ineligible_membership_with_master_version_rows": (0, 0, ()),
        "inverse_bounce_misclassified_as_genuine_transition_rows": (
            int(bounce.inverse),
            int(bounce.inverse_misclassified_as_genuine_transition),
            bounce_example if bounce.inverse_misclassified_as_genuine_transition else (),
        ),
        "multi_registry_composite_override_collision_alias_rows": (
            int(collision),
            int(collision and decision.alias_permitted),
            example if collision else (),
        ),
        "multi_registry_composite_override_collision_eligible_rows": (
            int(collision),
            int(collision and decision.backtest_identity_eligible),
            example if collision else (),
        ),
        "multi_registry_composite_override_collision_resolved_rows": (
            int(collision),
            int(collision and decision.canonical_composite_figi is not None),
            example if collision else (),
        ),
        "multi_registry_composite_override_collision_rows": (
            int(collision),
            0,
            example if collision else (),
        ),
        "row_semantic_proof_mismatch_rows": (
            1,
            int(proof.validator_semantics_digest != I3_ROW_VALIDATOR_SEMANTICS_DIGEST),
            (),
        ),
        "row_version_fk_mismatch_rows": (0, 0, ()),
        "share_class_applied_before_unique_composite_rows": (1, 0, ()),
        "source_membership_omission_or_duplication_rows": (0, 0, ()),
        "suspected_provider_figi_bounce_rows": (
            int(bounce.detected),
            int(bounce.detected and not bounce.middle_approved),
            bounce_example,
        ),
        "suspected_provider_contamination_eligible_rows": (
            int(bounce.detected),
            int(bounce.middle_unapproved_eligible),
            bounce_example if bounce.middle_unapproved_eligible else (),
        ),
        "target_market_consistency_unchecked_rows": (
            1,
            int(not decision.market_consistency_checked),
            (),
        ),
        "unapproved_canonical_identity_override_rows": (1, 0, ()),
        "unapproved_cross_market_composite_eligible_rows": (
            int(foreign),
            int(
                foreign
                and decision.backtest_identity_eligible
                and not evaluation.approved_cross_market
            ),
            example if foreign else (),
        ),
        "us_locale_non_us_composite_figi_rows": (
            int(foreign),
            int(foreign and not evaluation.approved_cross_market),
            example if foreign else (),
        ),
    }
    raw_reason_codes = {
        "multi_registry_composite_override_collision_rows": (
            "multiple_composite_registry_matches" if collision else None
        ),
        "suspected_provider_figi_bounce_rows": bounce.reason_code,
        "us_locale_non_us_composite_figi_rows": (
            f"observed_non_us_composite_market_{target.observed_composite_country.lower()}"
            if foreign and target.observed_composite_country is not None
            else None
        ),
    }
    rules = {item.check_id: item for item in I3_QA_CATALOG}
    return I3QaReceipt(
        qa_catalog_digest=I3_QA_CATALOG_DIGEST,
        results=tuple(
            I3QaResult(
                check_id=check_id,
                semantics_digest=rules[check_id].semantics_digest,
                observed_count=(observed if rules[check_id].owner == "dispatcher" else 0),
                failure_count=(failures if rules[check_id].owner == "dispatcher" else 0),
                evaluation_status=(
                    "evaluated"
                    if rules[check_id].owner == "dispatcher"
                    else "deferred_to_materialization"
                ),
                bounded_example_ids=(examples if rules[check_id].owner == "dispatcher" else ()),
                reason_counts=(
                    (
                        I3QaReasonCount(
                            reason_code=raw_reason_codes[check_id],
                            count=observed,
                        ),
                    )
                    if rules[check_id].owner == "dispatcher"
                    and check_id in raw_reason_codes
                    and raw_reason_codes[check_id] is not None
                    and observed
                    else ()
                ),
                bounded_examples=(
                    tuple(
                        I3QaBoundedExample(
                            source_record_id=source_record_id,
                            reason_codes=(raw_reason_codes[check_id],),
                        )
                        for source_record_id in examples
                    )
                    if rules[check_id].owner == "dispatcher"
                    and check_id in raw_reason_codes
                    and raw_reason_codes[check_id] is not None
                    else ()
                ),
            )
            for check_id, (observed, failures, examples) in sorted(counts.items())
        ),
    )


def _verify_calendar(value: object) -> ExactTradingCalendar:
    if type(value) is not ExactTradingCalendar or value._seal is not _CALENDAR_SEAL:
        raise I3DispatchError("calendar is not a sealed exact calendar")
    document = {
        "rule_version": I3_CALENDAR_RULE_VERSION,
        "sessions": [item.isoformat() for item in value.sessions],
    }
    content = _canonical_json_bytes(document)
    if value.artifact.sha256 != hashlib.sha256(content).hexdigest():
        raise I3DispatchError("calendar artifact SHA does not reproduce")
    if value.artifact.bytes != len(content):
        raise I3DispatchError("calendar artifact byte count does not reproduce")
    if value.calendar_digest != stable_digest(document):
        raise I3DispatchError("calendar digest does not reproduce")
    return value


def _verify_coverage(value: object) -> AliasSourceCoverageReceipt:
    if type(value) is not AliasSourceCoverageReceipt or value._seal is not _COVERAGE_SEAL:
        raise I3DispatchError("coverage is not a sealed exact receipt")
    if value.coverage_receipt_id != stable_digest(value.logical_payload()):
        raise I3DispatchError("coverage receipt ID does not reproduce")
    if len(value.slots) != 3 or value.slots[-1].session_date != value.target_session:
        raise I3DispatchError("coverage no longer has previous-two-plus-target shape")
    return value


def _verify_policy_snapshot(value: object) -> IdentityPolicySnapshot:
    if type(value) is not IdentityPolicySnapshot or value._seal is not _POLICY_SNAPSHOT_SEAL:
        raise I3DispatchError("identity policy snapshot is invalid")
    canonical_decisions, decision_index, decision_by_id, snapshot_id = (
        _canonical_policy_snapshot_components(
            value.policy_bundle,
            value.decisions,
            policy_source=value.policy_source,
            production_release_set_binding_digest=(value._production_release_set_binding_digest),
        )
    )
    if value.decisions != canonical_decisions:
        raise I3DispatchError("identity policy snapshot decisions do not reproduce")
    if dict(value._decision_index) != decision_index:
        raise I3DispatchError("identity policy snapshot decision index does not reproduce")
    if dict(value._decision_by_id) != decision_by_id:
        raise I3DispatchError("identity policy snapshot decision-ID index does not reproduce")
    if value._policy_snapshot_id != snapshot_id:
        raise I3DispatchError("identity policy snapshot ID does not reproduce")
    return IdentityPolicySnapshot(
        policy_bundle=value.policy_bundle,
        decisions=canonical_decisions,
        decision_index=decision_index,
        decision_by_id=decision_by_id,
        policy_snapshot_id=snapshot_id,
        policy_source=value.policy_source,
        production_release_set_binding_digest=(value._production_release_set_binding_digest),
        _seal=_POLICY_SNAPSHOT_SEAL,
    )


def _require_verified_policy_batch(value: object) -> _VerifiedIdentityPolicyBatch:
    if (
        type(value) is not _VerifiedIdentityPolicyBatch
        or value._seal is not _VERIFIED_POLICY_BATCH_SEAL
    ):
        raise I3DispatchError("identity dispatch requires a verified policy batch")
    if (
        value._policy_snapshot._seal is not _POLICY_SNAPSHOT_SEAL
        or value._policy_snapshot._policy_snapshot_id != value.policy_snapshot_id
        or id(value._policy_snapshot.policy_bundle) != value._bundle_object_id
        or id(value._policy_snapshot.decisions) != value._decisions_object_id
        or id(value._policy_snapshot._decision_index) != value._decision_index_object_id
        or id(value._policy_snapshot._decision_by_id) != value._decision_by_id_object_id
    ):
        raise I3DispatchError("verified policy batch snapshot identity changed")
    try:
        bundle_content_sha256 = hashlib.sha256(
            value._policy_snapshot.policy_bundle.canonical_bytes()
        ).hexdigest()
    except (AttributeError, I3CheckpointError, TypeError, ValueError) as exc:
        raise I3DispatchError("verified policy batch bundle no longer reproduces") from exc
    if bundle_content_sha256 != value._policy_bundle_content_sha256:
        raise I3DispatchError("verified policy batch bundle content changed after mint")
    source_binding_digest = stable_digest(
        {
            "policy_source": value._policy_snapshot.policy_source,
            "production_release_set_binding_digest": (
                value._policy_snapshot._production_release_set_binding_digest
            ),
        }
    )
    if source_binding_digest != value._snapshot_source_binding_digest:
        raise I3DispatchError("verified policy batch source binding changed after mint")
    return value


def _bind_observations_to_coverage(
    coverage: AliasSourceCoverageReceipt,
    observations: tuple[IdentityObservation, ...],
) -> tuple[IdentityObservation | None, ...]:
    if type(observations) is not tuple or any(
        type(item) is not IdentityObservation for item in observations
    ):
        raise I3DispatchError("observations must be a typed tuple")
    expected_order = tuple(
        sorted(observations, key=lambda item: (item.session_date, item.source_record_id))
    )
    if observations != expected_order:
        raise I3DispatchError("observations must be sorted by session and source record")
    for item in observations:
        if (
            item.provider_id != coverage.provider_id
            or item.provider_market != coverage.provider_market
            or item.provider_locale != coverage.provider_locale
            or item.ticker != coverage.ticker
        ):
            raise I3DispatchError("observation crossed the exact coverage subject")
    by_session = {item.session_date: item for item in observations}
    if len(by_session) != len(observations):
        raise I3DispatchError("multiple observations occupy one ticker/session")
    for slot in coverage.slots:
        actual = (
            ()
            if slot.session_date not in by_session
            else (by_session[slot.session_date].source_record_id,)
        )
        if actual != slot.source_record_ids:
            raise I3DispatchError("observation bytes differ from exact source coverage")
    if set(by_session) - {item.session_date for item in coverage.slots}:
        raise I3DispatchError("observation lies outside the fixed coverage window")
    return tuple(by_session.get(item.session_date) for item in coverage.slots)


def _canonical_json_document(content: bytes, label: str) -> dict[str, object]:
    try:
        value = json.loads(
            content,
            object_pairs_hook=_unique_json_pairs,
            parse_constant=_reject_json_constant,
            parse_float=_reject_json_float,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, I3DispatchError) as exc:
        raise I3DispatchError(f"{label} is not strict canonical JSON") from exc
    if type(value) is not dict or _canonical_json_bytes(value) != content:
        raise I3DispatchError(f"{label} is not strict canonical JSON")
    return value


def _unique_json_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise I3DispatchError("JSON object contains a duplicate key")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> object:
    raise I3DispatchError(f"JSON constant {value!r} is forbidden")


def _reject_json_float(value: str) -> object:
    raise I3DispatchError(f"JSON float {value!r} is forbidden")


def _closed_mapping(
    value: object,
    expected_keys: set[str],
    label: str,
) -> dict[str, object]:
    if type(value) is not dict or set(value) != expected_keys:
        raise I3DispatchError(f"{label} fields are not the exact closed schema")
    return value


def _text(value: object, label: str) -> str:
    if not isinstance(value, str):
        raise I3DispatchError(f"{label} must be text")
    return value


def _optional_text(value: object, label: str) -> str | None:
    if value is None:
        return None
    return _text(value, label)


def _date_from_json(value: object, label: str) -> date:
    text = _text(value, label)
    try:
        result = date.fromisoformat(text)
    except ValueError as exc:
        raise I3DispatchError(f"{label} is invalid") from exc
    if result.isoformat() != text:
        raise I3DispatchError(f"{label} is not canonical ISO date")
    return result


def _optional_date_from_json(value: object, label: str) -> date | None:
    if value is None:
        return None
    return _date_from_json(value, label)


def _literal(value: object, expected: object, label: str) -> None:
    if type(value) is not type(expected) or value != expected:
        raise I3DispatchError(f"{label} differs from the fixed value")


def _canonical_json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _snapshot_json_value(value: object) -> object:
    """Freeze registry-row values into the canonical snapshot JSON domain."""

    if value is None or type(value) in {bool, int, str}:
        return value
    if type(value) is date:
        return value.isoformat()
    # Parquet replay may surface timezone-aware timestamps as a
    # ``pandas.Timestamp`` (a ``datetime`` subclass).  This remains the same
    # canonical temporal domain and must not make an authenticated production
    # registry row depend on the reader's concrete scalar implementation.
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise I3DispatchError("production registry timestamp must be timezone-aware")
        return value.isoformat()
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise I3DispatchError("production registry row keys must be text")
        return {
            key: _snapshot_json_value(item)
            for key, item in sorted(value.items(), key=lambda pair: pair[0])
        }
    if type(value) in {list, tuple}:
        return [_snapshot_json_value(item) for item in value]
    raise I3DispatchError("production registry row contains a non-canonical value")


def _date(value: object, label: str) -> date:
    if type(value) is not date:
        raise I3DispatchError(f"{label} is invalid")
    return value


def _digest(value: object, label: str) -> str:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise I3DispatchError(f"{label} must be lowercase SHA-256")
    return value


def _optional_digest(value: object, label: str) -> str | None:
    if value is None:
        return None
    return _digest(value, label)


def _optional_figi(value: object, label: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or _FIGI.fullmatch(value) is None:
        raise I3DispatchError(f"{label} is invalid")
    return value


def _optional_country(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or _COUNTRY.fullmatch(value) is None:
        raise I3DispatchError("Composite country must be an ISO-like uppercase pair")
    return value


def _token(value: object, label: str) -> str:
    if not isinstance(value, str) or _TOKEN.fullmatch(value) is None:
        raise I3DispatchError(f"{label} is invalid")
    return value


def _nonnegative_int(value: object, label: str) -> int:
    if type(value) is not int or value < 0:
        raise I3DispatchError(f"{label} must be a nonnegative integer")
    return value


def _sorted_digests(values: tuple[str, ...], label: str) -> None:
    if type(values) is not tuple or tuple(sorted(set(values))) != values:
        raise I3DispatchError(f"{label} must be sorted and unique")
    for item in values:
        _digest(item, label)


def _sorted_tokens(values: tuple[str, ...], label: str) -> None:
    if type(values) is not tuple or tuple(sorted(set(values))) != values:
        raise I3DispatchError(f"{label} must be sorted and unique")
    for item in values:
        _token(item, label)
