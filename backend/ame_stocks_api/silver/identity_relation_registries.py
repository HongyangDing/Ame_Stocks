"""Fail-closed S7 identity relation registries.

These controls deliberately separate three responsibilities:

* ``provider_composite_override`` corrects an exact same-market Composite FIGI
  observation after an independently approved genuine asset transition;
* ``share_class_adjudication`` corrects only the Share Class FIGI hierarchy after
  the canonical Composite identity is already unique; and
* ``asset_transition`` records a predecessor/successor relationship without
  applying an identity override, changing membership, or stitching returns.

The module is source independent.  It contains immutable decision/release models
and deterministic collision semantics, but no production data reader or writer.
"""

from __future__ import annotations

import json
import re
from collections import defaultdict
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver.contracts import SilverContractError
from ame_stocks_api.silver.identity_resolution import canonical_asset_id

_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_FIGI = re.compile(r"^BBG[0-9A-Z]{9}$")
_REGISTRY_NAMES = frozenset(
    {
        "identity_adjudication",
        "identity_cross_market_adjudication",
        "provider_composite_override",
    }
)


class IdentityRelationRegistryError(SilverContractError):
    """Raised when a relation decision or registry release is unsafe."""


class ProviderCompositeOverrideDisposition(StrEnum):
    CONFIRMED_STALE_AFTER_TRANSITION = "confirmed_provider_composite_stale_after_transition"
    ADJUDICATED_UNRESOLVED = "provider_composite_override_adjudicated_unresolved"


class ShareClassAdjudicationDisposition(StrEnum):
    CONFIRMED_CORRECTION = "confirmed_share_class_correction"
    ADJUDICATED_UNRESOLVED = "share_class_adjudicated_unresolved"


class AssetTransitionDisposition(StrEnum):
    CONFIRMED_GENUINE_TRANSITION = "confirmed_genuine_transition"
    ADJUDICATED_UNRESOLVED = "asset_transition_adjudicated_unresolved"


class AssetTransitionType(StrEnum):
    CORPORATE_REORGANIZATION_SUCCESSOR_SECURITY = "corporate_reorganization_successor_security"


class RelationRegistryKind(StrEnum):
    PROVIDER_COMPOSITE_OVERRIDE = "provider_composite_override"
    SHARE_CLASS_ADJUDICATION = "share_class_adjudication"
    ASSET_TRANSITION = "asset_transition"


def canonical_share_class_id(share_class_figi: str) -> str:
    """Reproduce the frozen canonical Share Class ID rule."""

    _figi(share_class_figi, "canonical Share Class FIGI")
    return stable_digest(
        {
            "anchor_type": "share_class_figi",
            "anchor_value": share_class_figi,
            "namespace": "ame_stocks.identity.share_class",
            "rule_version": "ame_stocks_share_class_id_from_share_class_figi_v1",
        }
    )


@dataclass(frozen=True, slots=True)
class ProviderCompositeOverrideDecision:
    """One approved version of one exact same-market stale-Composite correction."""

    provider_id: str
    provider_market: str
    provider_locale: str
    observed_ticker: str
    observed_composite_figi: str
    canonical_composite_figi: str | None
    observed_composite_market_code: str
    canonical_composite_market_code: str | None
    valid_from_session: date
    valid_through_session: date
    scoped_source_record_ids: tuple[str, ...]
    source_s4_release_set_id: str
    source_exact_group_candidate_manifest_id: str
    source_exact_group_candidate_manifest_sha256: str
    candidate_available_session: date
    asset_transition_series_id: str
    asset_transition_id: str
    asset_transition_available_session: date
    source_external_evidence_manifest_id: str
    source_external_evidence_manifest_sha256: str
    external_evidence_available_session: date
    disposition: ProviderCompositeOverrideDisposition
    decision_version: int
    supersedes_provider_composite_override_id: str | None
    source_decision_plan_id: str
    source_decision_plan_path: str
    source_decision_plan_sha256: str
    approval_request_event_id: str
    approval_request_event_sha256: str
    approval_receipt_id: str
    approval_receipt_sha256: str
    approved_by: str
    approved_at_utc: datetime
    approval_available_session: date
    availability_calendar_id: str
    availability_calendar_sha256: str
    reason_code: str
    reason_detail: str

    def __post_init__(self) -> None:
        _provider_scope(self.provider_id, self.provider_market, self.provider_locale)
        _text(self.observed_ticker, "observed ticker")
        _figi(self.observed_composite_figi, "observed Composite FIGI")
        _market_code(self.observed_composite_market_code, "observed market code")
        if self.observed_composite_market_code != "US":
            raise IdentityRelationRegistryError(
                "provider Composite override cannot consume a cross-market observation"
            )
        _ordered_interval(self.valid_from_session, self.valid_through_session)
        _record_ids(self.scoped_source_record_ids, "scoped source record IDs")
        for value, label in (
            (self.source_s4_release_set_id, "source S4 release-set ID"),
            (self.source_exact_group_candidate_manifest_id, "exact-group candidate ID"),
            (self.source_exact_group_candidate_manifest_sha256, "exact-group candidate SHA"),
            (self.asset_transition_series_id, "asset transition series ID"),
            (self.asset_transition_id, "asset transition ID"),
            (self.source_external_evidence_manifest_id, "external evidence manifest ID"),
            (self.source_external_evidence_manifest_sha256, "external evidence manifest SHA"),
            (self.source_decision_plan_id, "decision plan ID"),
            (self.source_decision_plan_sha256, "decision plan SHA"),
            (self.approval_request_event_id, "approval request event ID"),
            (self.approval_request_event_sha256, "approval request event SHA"),
            (self.approval_receipt_id, "approval receipt ID"),
            (self.approval_receipt_sha256, "approval receipt SHA"),
            (self.availability_calendar_id, "availability calendar ID"),
            (self.availability_calendar_sha256, "availability calendar SHA"),
        ):
            _digest(value, label)
        _relative_path(self.source_decision_plan_path, "decision plan path")
        for value, label in (
            (self.candidate_available_session, "candidate availability"),
            (self.asset_transition_available_session, "asset transition availability"),
            (self.external_evidence_available_session, "external evidence availability"),
            (self.approval_available_session, "approval availability"),
        ):
            _date(value, label)
        _approval(self.approved_by, self.approved_at_utc)
        _version(
            self.decision_version,
            self.supersedes_provider_composite_override_id,
            "provider Composite override",
        )
        _text(self.reason_code, "reason code")
        _text(self.reason_detail, "reason detail")
        if not isinstance(self.disposition, ProviderCompositeOverrideDisposition):
            raise IdentityRelationRegistryError("provider override disposition is invalid")
        if (
            self.disposition
            is ProviderCompositeOverrideDisposition.CONFIRMED_STALE_AFTER_TRANSITION
        ):
            _figi(self.canonical_composite_figi, "canonical Composite FIGI")
            _market_code(self.canonical_composite_market_code, "canonical market code")
            if self.canonical_composite_figi == self.observed_composite_figi:
                raise IdentityRelationRegistryError(
                    "confirmed provider Composite override must change the Composite FIGI"
                )
            if self.canonical_composite_market_code != self.observed_composite_market_code:
                raise IdentityRelationRegistryError(
                    "provider Composite override is same-market only"
                )
        elif (
            self.canonical_composite_figi is not None
            or self.canonical_composite_market_code is not None
        ):
            raise IdentityRelationRegistryError(
                "unresolved provider Composite override must have a null target"
            )

    @property
    def source_record_set_digest(self) -> str:
        return stable_digest(list(self.scoped_source_record_ids))

    @property
    def provider_composite_override_subject_id(self) -> str:
        return stable_digest(
            {
                "asset_transition_series_id": self.asset_transition_series_id,
                "namespace": "ame_stocks.identity.provider_composite_override_subject",
                "observed_composite_figi": self.observed_composite_figi,
                "observed_ticker": self.observed_ticker,
                "provider_id": self.provider_id,
                "provider_locale": self.provider_locale,
                "provider_market": self.provider_market,
                "rule_version": "s7_provider_composite_override_subject_id_v1",
            }
        )

    @property
    def provider_composite_override_series_id(self) -> str:
        return stable_digest(
            {
                "namespace": "ame_stocks.identity.provider_composite_override_series",
                "provider_composite_override_subject_id": (
                    self.provider_composite_override_subject_id
                ),
                "rule_version": "s7_provider_composite_override_series_id_v1",
            }
        )

    @property
    def canonical_asset_id(self) -> str | None:
        if self.canonical_composite_figi is None:
            return None
        return canonical_asset_id(self.canonical_composite_figi)

    @property
    def override_available_session(self) -> date:
        return max(
            self.candidate_available_session,
            self.asset_transition_available_session,
            self.external_evidence_available_session,
            self.approval_available_session,
        )

    @property
    def provider_composite_override_id(self) -> str:
        return stable_digest(
            {
                "asset_transition_id": self.asset_transition_id,
                "canonical_composite_figi": self.canonical_composite_figi,
                "decision_version": self.decision_version,
                "disposition": self.disposition.value,
                "namespace": "ame_stocks.identity.provider_composite_override",
                "provider_composite_override_series_id": (
                    self.provider_composite_override_series_id
                ),
                "reason_code": self.reason_code,
                "reason_detail": self.reason_detail,
                "rule_version": "s7_provider_composite_override_id_v1",
                "source_exact_group_candidate_manifest_id": (
                    self.source_exact_group_candidate_manifest_id
                ),
                "source_record_set_digest": self.source_record_set_digest,
                "source_s4_release_set_id": self.source_s4_release_set_id,
                "supersedes_provider_composite_override_id": (
                    self.supersedes_provider_composite_override_id
                ),
                "valid_from_session": self.valid_from_session.isoformat(),
                "valid_through_session": self.valid_through_session.isoformat(),
            }
        )

    def matches(
        self,
        *,
        provider_id: str,
        provider_market: str,
        provider_locale: str,
        ticker: str,
        observed_composite_figi: str,
        session_date: date,
        source_record_id: str,
        source_s4_release_set_id: str,
    ) -> bool:
        return (
            self.disposition
            is ProviderCompositeOverrideDisposition.CONFIRMED_STALE_AFTER_TRANSITION
            and provider_id == self.provider_id
            and provider_market == self.provider_market
            and provider_locale == self.provider_locale
            and ticker == self.observed_ticker
            and observed_composite_figi == self.observed_composite_figi
            and self.valid_from_session <= session_date <= self.valid_through_session
            and source_record_id in self.scoped_source_record_ids
            and source_s4_release_set_id == self.source_s4_release_set_id
        )

    def to_registry_row(self) -> dict[str, object]:
        confirmed = (
            self.disposition
            is ProviderCompositeOverrideDisposition.CONFIRMED_STALE_AFTER_TRANSITION
        )
        return {
            "provider_composite_override_id": self.provider_composite_override_id,
            "provider_composite_override_series_id": (self.provider_composite_override_series_id),
            "provider_composite_override_subject_id": (self.provider_composite_override_subject_id),
            "decision_version": self.decision_version,
            "supersedes_provider_composite_override_id": (
                self.supersedes_provider_composite_override_id
            ),
            "provider_id": self.provider_id,
            "provider_market": self.provider_market,
            "provider_locale": self.provider_locale,
            "observed_ticker": self.observed_ticker,
            "observed_composite_figi": self.observed_composite_figi,
            "canonical_composite_figi": self.canonical_composite_figi,
            "observed_composite_market_code": self.observed_composite_market_code,
            "canonical_composite_market_code": self.canonical_composite_market_code,
            "canonical_asset_id": self.canonical_asset_id,
            "valid_from_session": self.valid_from_session,
            "valid_through_session": self.valid_through_session,
            "scoped_source_record_count": len(self.scoped_source_record_ids),
            "scoped_source_record_set_digest": self.source_record_set_digest,
            "scoped_source_record_ids_json": _canonical_json(list(self.scoped_source_record_ids)),
            "asset_transition_series_id": self.asset_transition_series_id,
            "asset_transition_id": self.asset_transition_id,
            "asset_transition_available_session": self.asset_transition_available_session,
            "disposition": self.disposition.value,
            "canonical_override": confirmed,
            "identity_effect": (
                "canonical_research_identity_only" if confirmed else "none_unresolved"
            ),
            "membership_effect": "none",
            "active_status_effect": "none",
            "identity_quality_liquidation_signal": False,
            "reason_code": self.reason_code,
            "reason_detail": self.reason_detail,
            **self._common_control_row(),
            "override_available_session": self.override_available_session,
            "outcome_or_backtest_evidence_used": False,
            "rule_version": "s7_provider_composite_override_v1",
        }

    def _common_control_row(self) -> dict[str, object]:
        return {
            "source_decision_plan_id": self.source_decision_plan_id,
            "source_decision_plan_path": self.source_decision_plan_path,
            "source_decision_plan_sha256": self.source_decision_plan_sha256,
            "approval_request_event_id": self.approval_request_event_id,
            "approval_request_event_sha256": self.approval_request_event_sha256,
            "approval_receipt_id": self.approval_receipt_id,
            "approval_receipt_sha256": self.approval_receipt_sha256,
            "approved_by": self.approved_by,
            "approved_at_utc": self.approved_at_utc,
            "approval_available_session": self.approval_available_session,
            "source_s4_release_set_id": self.source_s4_release_set_id,
            "source_exact_group_candidate_manifest_id": (
                self.source_exact_group_candidate_manifest_id
            ),
            "source_exact_group_candidate_manifest_sha256": (
                self.source_exact_group_candidate_manifest_sha256
            ),
            "candidate_available_session": self.candidate_available_session,
            "source_external_evidence_manifest_id": (self.source_external_evidence_manifest_id),
            "source_external_evidence_manifest_sha256": (
                self.source_external_evidence_manifest_sha256
            ),
            "external_evidence_available_session": (self.external_evidence_available_session),
            "availability_calendar_id": self.availability_calendar_id,
            "availability_calendar_sha256": self.availability_calendar_sha256,
        }


@dataclass(frozen=True, slots=True)
class ShareClassAdjudicationDecision:
    """One approved Share Class-only correction under a unique Composite identity."""

    provider_id: str
    provider_market: str
    provider_locale: str
    observed_ticker: str
    observed_composite_figi: str
    required_unique_canonical_composite_figi: str
    observed_share_class_figi: str
    canonical_share_class_figi: str | None
    valid_from_session: date
    valid_through_session: date
    scoped_source_record_ids: tuple[str, ...]
    source_s4_release_set_id: str
    source_exact_group_candidate_manifest_id: str
    source_exact_group_candidate_manifest_sha256: str
    candidate_available_session: date
    source_external_evidence_manifest_id: str
    source_external_evidence_manifest_sha256: str
    external_evidence_available_session: date
    disposition: ShareClassAdjudicationDisposition
    decision_version: int
    supersedes_share_class_adjudication_id: str | None
    source_decision_plan_id: str
    source_decision_plan_path: str
    source_decision_plan_sha256: str
    approval_request_event_id: str
    approval_request_event_sha256: str
    approval_receipt_id: str
    approval_receipt_sha256: str
    approved_by: str
    approved_at_utc: datetime
    approval_available_session: date
    availability_calendar_id: str
    availability_calendar_sha256: str
    reason_code: str
    reason_detail: str

    def __post_init__(self) -> None:
        _provider_scope(self.provider_id, self.provider_market, self.provider_locale)
        _text(self.observed_ticker, "observed ticker")
        for value, label in (
            (self.observed_composite_figi, "observed Composite FIGI"),
            (
                self.required_unique_canonical_composite_figi,
                "required canonical Composite FIGI",
            ),
            (self.observed_share_class_figi, "observed Share Class FIGI"),
        ):
            _figi(value, label)
        _ordered_interval(self.valid_from_session, self.valid_through_session)
        _record_ids(self.scoped_source_record_ids, "scoped source record IDs")
        for value, label in (
            (self.source_s4_release_set_id, "source S4 release-set ID"),
            (self.source_exact_group_candidate_manifest_id, "exact-group candidate ID"),
            (self.source_exact_group_candidate_manifest_sha256, "exact-group candidate SHA"),
            (self.source_external_evidence_manifest_id, "external evidence manifest ID"),
            (self.source_external_evidence_manifest_sha256, "external evidence manifest SHA"),
            (self.source_decision_plan_id, "decision plan ID"),
            (self.source_decision_plan_sha256, "decision plan SHA"),
            (self.approval_request_event_id, "approval request event ID"),
            (self.approval_request_event_sha256, "approval request event SHA"),
            (self.approval_receipt_id, "approval receipt ID"),
            (self.approval_receipt_sha256, "approval receipt SHA"),
            (self.availability_calendar_id, "availability calendar ID"),
            (self.availability_calendar_sha256, "availability calendar SHA"),
        ):
            _digest(value, label)
        _relative_path(self.source_decision_plan_path, "decision plan path")
        for value, label in (
            (self.candidate_available_session, "candidate availability"),
            (self.external_evidence_available_session, "external evidence availability"),
            (self.approval_available_session, "approval availability"),
        ):
            _date(value, label)
        _approval(self.approved_by, self.approved_at_utc)
        _version(
            self.decision_version,
            self.supersedes_share_class_adjudication_id,
            "Share Class adjudication",
        )
        _text(self.reason_code, "reason code")
        _text(self.reason_detail, "reason detail")
        if not isinstance(self.disposition, ShareClassAdjudicationDisposition):
            raise IdentityRelationRegistryError("Share Class disposition is invalid")
        if self.disposition is ShareClassAdjudicationDisposition.CONFIRMED_CORRECTION:
            _figi(self.canonical_share_class_figi, "canonical Share Class FIGI")
            if self.canonical_share_class_figi == self.observed_share_class_figi:
                raise IdentityRelationRegistryError(
                    "confirmed Share Class adjudication must change the Share Class FIGI"
                )
        elif self.canonical_share_class_figi is not None:
            raise IdentityRelationRegistryError(
                "unresolved Share Class adjudication must have a null target"
            )

    @property
    def source_record_set_digest(self) -> str:
        return stable_digest(list(self.scoped_source_record_ids))

    @property
    def share_class_adjudication_subject_id(self) -> str:
        return stable_digest(
            {
                "namespace": "ame_stocks.identity.share_class_adjudication_subject",
                "observed_composite_figi": self.observed_composite_figi,
                "observed_share_class_figi": self.observed_share_class_figi,
                "observed_ticker": self.observed_ticker,
                "provider_id": self.provider_id,
                "provider_locale": self.provider_locale,
                "provider_market": self.provider_market,
                "rule_version": "s7_share_class_adjudication_subject_id_v1",
            }
        )

    @property
    def share_class_adjudication_series_id(self) -> str:
        return stable_digest(
            {
                "namespace": "ame_stocks.identity.share_class_adjudication_series",
                "rule_version": "s7_share_class_adjudication_series_id_v1",
                "share_class_adjudication_subject_id": (self.share_class_adjudication_subject_id),
            }
        )

    @property
    def canonical_share_class_id(self) -> str | None:
        if self.canonical_share_class_figi is None:
            return None
        return canonical_share_class_id(self.canonical_share_class_figi)

    @property
    def adjudication_available_session(self) -> date:
        return max(
            self.candidate_available_session,
            self.external_evidence_available_session,
            self.approval_available_session,
        )

    @property
    def share_class_adjudication_id(self) -> str:
        return stable_digest(
            {
                "canonical_share_class_figi": self.canonical_share_class_figi,
                "decision_version": self.decision_version,
                "disposition": self.disposition.value,
                "namespace": "ame_stocks.identity.share_class_adjudication",
                "reason_code": self.reason_code,
                "reason_detail": self.reason_detail,
                "required_unique_canonical_composite_figi": (
                    self.required_unique_canonical_composite_figi
                ),
                "rule_version": "s7_share_class_adjudication_id_v1",
                "share_class_adjudication_series_id": (self.share_class_adjudication_series_id),
                "source_exact_group_candidate_manifest_id": (
                    self.source_exact_group_candidate_manifest_id
                ),
                "source_record_set_digest": self.source_record_set_digest,
                "source_s4_release_set_id": self.source_s4_release_set_id,
                "supersedes_share_class_adjudication_id": (
                    self.supersedes_share_class_adjudication_id
                ),
                "valid_from_session": self.valid_from_session.isoformat(),
                "valid_through_session": self.valid_through_session.isoformat(),
            }
        )

    def matches(
        self,
        *,
        provider_id: str,
        provider_market: str,
        provider_locale: str,
        ticker: str,
        observed_composite_figi: str,
        unique_canonical_composite_figi: str | None,
        observed_share_class_figi: str,
        session_date: date,
        source_record_id: str,
        source_s4_release_set_id: str,
    ) -> bool:
        return (
            self.disposition is ShareClassAdjudicationDisposition.CONFIRMED_CORRECTION
            and unique_canonical_composite_figi is not None
            and unique_canonical_composite_figi == self.required_unique_canonical_composite_figi
            and provider_id == self.provider_id
            and provider_market == self.provider_market
            and provider_locale == self.provider_locale
            and ticker == self.observed_ticker
            and observed_composite_figi == self.observed_composite_figi
            and observed_share_class_figi == self.observed_share_class_figi
            and self.valid_from_session <= session_date <= self.valid_through_session
            and source_record_id in self.scoped_source_record_ids
            and source_s4_release_set_id == self.source_s4_release_set_id
        )

    def to_registry_row(self) -> dict[str, object]:
        confirmed = self.disposition is ShareClassAdjudicationDisposition.CONFIRMED_CORRECTION
        return {
            "share_class_adjudication_id": self.share_class_adjudication_id,
            "share_class_adjudication_series_id": (self.share_class_adjudication_series_id),
            "share_class_adjudication_subject_id": (self.share_class_adjudication_subject_id),
            "decision_version": self.decision_version,
            "supersedes_share_class_adjudication_id": (self.supersedes_share_class_adjudication_id),
            "provider_id": self.provider_id,
            "provider_market": self.provider_market,
            "provider_locale": self.provider_locale,
            "observed_ticker": self.observed_ticker,
            "observed_composite_figi": self.observed_composite_figi,
            "required_unique_canonical_composite_figi": (
                self.required_unique_canonical_composite_figi
            ),
            "observed_share_class_figi": self.observed_share_class_figi,
            "canonical_share_class_figi": self.canonical_share_class_figi,
            "canonical_share_class_id": self.canonical_share_class_id,
            "valid_from_session": self.valid_from_session,
            "valid_through_session": self.valid_through_session,
            "scoped_source_record_count": len(self.scoped_source_record_ids),
            "scoped_source_record_set_digest": self.source_record_set_digest,
            "scoped_source_record_ids_json": _canonical_json(list(self.scoped_source_record_ids)),
            "disposition": self.disposition.value,
            "share_class_override": confirmed,
            "composite_identity_effect": "none",
            "asset_id_effect": "none",
            "issuer_identity_effect": "none",
            "membership_effect": "none",
            "tradability_effect": "none",
            "identity_quality_liquidation_signal": False,
            "reason_code": self.reason_code,
            "reason_detail": self.reason_detail,
            **self._common_control_row(),
            "adjudication_available_session": self.adjudication_available_session,
            "outcome_or_backtest_evidence_used": False,
            "rule_version": "s7_share_class_adjudication_v1",
        }

    def _common_control_row(self) -> dict[str, object]:
        return {
            "source_decision_plan_id": self.source_decision_plan_id,
            "source_decision_plan_path": self.source_decision_plan_path,
            "source_decision_plan_sha256": self.source_decision_plan_sha256,
            "approval_request_event_id": self.approval_request_event_id,
            "approval_request_event_sha256": self.approval_request_event_sha256,
            "approval_receipt_id": self.approval_receipt_id,
            "approval_receipt_sha256": self.approval_receipt_sha256,
            "approved_by": self.approved_by,
            "approved_at_utc": self.approved_at_utc,
            "approval_available_session": self.approval_available_session,
            "source_s4_release_set_id": self.source_s4_release_set_id,
            "source_exact_group_candidate_manifest_id": (
                self.source_exact_group_candidate_manifest_id
            ),
            "source_exact_group_candidate_manifest_sha256": (
                self.source_exact_group_candidate_manifest_sha256
            ),
            "candidate_available_session": self.candidate_available_session,
            "source_external_evidence_manifest_id": (self.source_external_evidence_manifest_id),
            "source_external_evidence_manifest_sha256": (
                self.source_external_evidence_manifest_sha256
            ),
            "external_evidence_available_session": (self.external_evidence_available_session),
            "availability_calendar_id": self.availability_calendar_id,
            "availability_calendar_sha256": self.availability_calendar_sha256,
        }


@dataclass(frozen=True, slots=True)
class AssetTransitionDecision:
    """One approved predecessor/successor relation with no correction side effect."""

    provider_id: str
    provider_market: str
    provider_locale: str
    observed_ticker: str
    transition_type: AssetTransitionType
    legal_effective_date: date
    predecessor_last_session: date
    successor_first_session: date
    predecessor_composite_figi: str
    successor_composite_figi: str | None
    boundary_source_record_ids: tuple[str, ...]
    source_s4_release_set_id: str
    source_exact_group_candidate_manifest_id: str
    source_exact_group_candidate_manifest_sha256: str
    candidate_available_session: date
    source_external_evidence_manifest_id: str
    source_external_evidence_manifest_sha256: str
    external_evidence_available_session: date
    disposition: AssetTransitionDisposition
    decision_version: int
    supersedes_asset_transition_id: str | None
    source_decision_plan_id: str
    source_decision_plan_path: str
    source_decision_plan_sha256: str
    approval_request_event_id: str
    approval_request_event_sha256: str
    approval_receipt_id: str
    approval_receipt_sha256: str
    approved_by: str
    approved_at_utc: datetime
    approval_available_session: date
    availability_calendar_id: str
    availability_calendar_sha256: str
    reason_code: str
    reason_detail: str

    def __post_init__(self) -> None:
        _provider_scope(self.provider_id, self.provider_market, self.provider_locale)
        _text(self.observed_ticker, "observed ticker")
        if not isinstance(self.transition_type, AssetTransitionType):
            raise IdentityRelationRegistryError("asset transition type is invalid")
        for value, label in (
            (self.legal_effective_date, "legal effective date"),
            (self.predecessor_last_session, "predecessor last session"),
            (self.successor_first_session, "successor first session"),
        ):
            _date(value, label)
        if not (
            self.predecessor_last_session
            <= self.legal_effective_date
            < self.successor_first_session
        ):
            raise IdentityRelationRegistryError(
                "asset transition legal/session boundary is invalid"
            )
        _figi(self.predecessor_composite_figi, "predecessor Composite FIGI")
        _record_ids(self.boundary_source_record_ids, "boundary source record IDs")
        for value, label in (
            (self.source_s4_release_set_id, "source S4 release-set ID"),
            (self.source_exact_group_candidate_manifest_id, "exact-group candidate ID"),
            (self.source_exact_group_candidate_manifest_sha256, "exact-group candidate SHA"),
            (self.source_external_evidence_manifest_id, "external evidence manifest ID"),
            (self.source_external_evidence_manifest_sha256, "external evidence manifest SHA"),
            (self.source_decision_plan_id, "decision plan ID"),
            (self.source_decision_plan_sha256, "decision plan SHA"),
            (self.approval_request_event_id, "approval request event ID"),
            (self.approval_request_event_sha256, "approval request event SHA"),
            (self.approval_receipt_id, "approval receipt ID"),
            (self.approval_receipt_sha256, "approval receipt SHA"),
            (self.availability_calendar_id, "availability calendar ID"),
            (self.availability_calendar_sha256, "availability calendar SHA"),
        ):
            _digest(value, label)
        _relative_path(self.source_decision_plan_path, "decision plan path")
        for value, label in (
            (self.candidate_available_session, "candidate availability"),
            (self.external_evidence_available_session, "external evidence availability"),
            (self.approval_available_session, "approval availability"),
        ):
            _date(value, label)
        _approval(self.approved_by, self.approved_at_utc)
        _version(
            self.decision_version,
            self.supersedes_asset_transition_id,
            "asset transition",
        )
        _text(self.reason_code, "reason code")
        _text(self.reason_detail, "reason detail")
        if not isinstance(self.disposition, AssetTransitionDisposition):
            raise IdentityRelationRegistryError("asset transition disposition is invalid")
        if self.disposition is AssetTransitionDisposition.CONFIRMED_GENUINE_TRANSITION:
            _figi(self.successor_composite_figi, "successor Composite FIGI")
            if self.successor_composite_figi == self.predecessor_composite_figi:
                raise IdentityRelationRegistryError(
                    "confirmed asset transition requires distinct assets"
                )
        elif self.successor_composite_figi is not None:
            raise IdentityRelationRegistryError(
                "unresolved asset transition must have a null successor"
            )

    @property
    def boundary_source_record_set_digest(self) -> str:
        return stable_digest(list(self.boundary_source_record_ids))

    @property
    def asset_transition_subject_id(self) -> str:
        return stable_digest(
            {
                "legal_effective_date": self.legal_effective_date.isoformat(),
                "namespace": "ame_stocks.identity.asset_transition_subject",
                "observed_ticker": self.observed_ticker,
                "provider_id": self.provider_id,
                "provider_locale": self.provider_locale,
                "provider_market": self.provider_market,
                "rule_version": "s7_asset_transition_subject_id_v1",
                "transition_type": self.transition_type.value,
            }
        )

    @property
    def asset_transition_series_id(self) -> str:
        return stable_digest(
            {
                "asset_transition_subject_id": self.asset_transition_subject_id,
                "namespace": "ame_stocks.identity.asset_transition_series",
                "rule_version": "s7_asset_transition_series_id_v1",
            }
        )

    @property
    def predecessor_asset_id(self) -> str:
        return canonical_asset_id(self.predecessor_composite_figi)

    @property
    def successor_asset_id(self) -> str | None:
        if self.successor_composite_figi is None:
            return None
        return canonical_asset_id(self.successor_composite_figi)

    @property
    def transition_available_session(self) -> date:
        return max(
            self.candidate_available_session,
            self.external_evidence_available_session,
            self.approval_available_session,
        )

    @property
    def asset_transition_id(self) -> str:
        return stable_digest(
            {
                "asset_transition_series_id": self.asset_transition_series_id,
                "boundary_source_record_set_digest": (self.boundary_source_record_set_digest),
                "decision_version": self.decision_version,
                "disposition": self.disposition.value,
                "namespace": "ame_stocks.identity.asset_transition",
                "predecessor_composite_figi": self.predecessor_composite_figi,
                "predecessor_last_session": self.predecessor_last_session.isoformat(),
                "reason_code": self.reason_code,
                "reason_detail": self.reason_detail,
                "rule_version": "s7_asset_transition_id_v1",
                "source_exact_group_candidate_manifest_id": (
                    self.source_exact_group_candidate_manifest_id
                ),
                "source_s4_release_set_id": self.source_s4_release_set_id,
                "successor_composite_figi": self.successor_composite_figi,
                "successor_first_session": self.successor_first_session.isoformat(),
                "supersedes_asset_transition_id": self.supersedes_asset_transition_id,
            }
        )

    def to_registry_row(self) -> dict[str, object]:
        return {
            "asset_transition_id": self.asset_transition_id,
            "asset_transition_series_id": self.asset_transition_series_id,
            "asset_transition_subject_id": self.asset_transition_subject_id,
            "decision_version": self.decision_version,
            "supersedes_asset_transition_id": self.supersedes_asset_transition_id,
            "provider_id": self.provider_id,
            "provider_market": self.provider_market,
            "provider_locale": self.provider_locale,
            "observed_ticker": self.observed_ticker,
            "transition_type": self.transition_type.value,
            "legal_effective_date": self.legal_effective_date,
            "predecessor_last_session": self.predecessor_last_session,
            "successor_first_session": self.successor_first_session,
            "predecessor_composite_figi": self.predecessor_composite_figi,
            "predecessor_asset_id": self.predecessor_asset_id,
            "successor_composite_figi": self.successor_composite_figi,
            "successor_asset_id": self.successor_asset_id,
            "boundary_source_record_count": len(self.boundary_source_record_ids),
            "boundary_source_record_set_digest": (self.boundary_source_record_set_digest),
            "boundary_source_record_ids_json": _canonical_json(
                list(self.boundary_source_record_ids)
            ),
            "disposition": self.disposition.value,
            "relationship_effect": "lineage_only_no_override_no_return_stitching",
            "identity_override_effect": "none",
            "membership_effect": "none",
            "tradability_effect": "none",
            "return_stitching_effect": ("none_requires_future_entitlement_accounting"),
            "identity_quality_liquidation_signal": False,
            "reason_code": self.reason_code,
            "reason_detail": self.reason_detail,
            "source_decision_plan_id": self.source_decision_plan_id,
            "source_decision_plan_path": self.source_decision_plan_path,
            "source_decision_plan_sha256": self.source_decision_plan_sha256,
            "approval_request_event_id": self.approval_request_event_id,
            "approval_request_event_sha256": self.approval_request_event_sha256,
            "approval_receipt_id": self.approval_receipt_id,
            "approval_receipt_sha256": self.approval_receipt_sha256,
            "approved_by": self.approved_by,
            "approved_at_utc": self.approved_at_utc,
            "approval_available_session": self.approval_available_session,
            "transition_available_session": self.transition_available_session,
            "source_s4_release_set_id": self.source_s4_release_set_id,
            "source_exact_group_candidate_manifest_id": (
                self.source_exact_group_candidate_manifest_id
            ),
            "source_exact_group_candidate_manifest_sha256": (
                self.source_exact_group_candidate_manifest_sha256
            ),
            "candidate_available_session": self.candidate_available_session,
            "source_external_evidence_manifest_id": (self.source_external_evidence_manifest_id),
            "source_external_evidence_manifest_sha256": (
                self.source_external_evidence_manifest_sha256
            ),
            "external_evidence_available_session": (self.external_evidence_available_session),
            "availability_calendar_id": self.availability_calendar_id,
            "availability_calendar_sha256": self.availability_calendar_sha256,
            "outcome_or_backtest_evidence_used": False,
            "rule_version": "s7_asset_transition_v1",
        }


@dataclass(frozen=True, slots=True)
class RegistryDecisionArtifactRef:
    decision_id: str
    decision_path: str
    decision_sha256: str
    decision_available_session: date

    def __post_init__(self) -> None:
        _digest(self.decision_id, "registry decision ID")
        _relative_path(self.decision_path, "registry decision path")
        _digest(self.decision_sha256, "registry decision SHA")
        _date(self.decision_available_session, "registry decision availability")


@dataclass(frozen=True, slots=True)
class IdentityRelationRegistryRelease:
    """One immutable release envelope for exactly one relation-registry kind."""

    registry_kind: RelationRegistryKind
    source_candidate_manifest_id: str
    source_candidate_manifest_sha256: str
    source_external_evidence_manifest_id: str
    source_external_evidence_manifest_sha256: str
    availability_calendar_id: str
    availability_calendar_sha256: str
    published_at_utc: datetime
    release_available_session: date
    decisions: tuple[RegistryDecisionArtifactRef, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.registry_kind, RelationRegistryKind):
            raise IdentityRelationRegistryError("registry kind is invalid")
        for value, label in (
            (self.source_candidate_manifest_id, "candidate manifest ID"),
            (self.source_candidate_manifest_sha256, "candidate manifest SHA"),
            (self.source_external_evidence_manifest_id, "external evidence manifest ID"),
            (self.source_external_evidence_manifest_sha256, "external evidence manifest SHA"),
            (self.availability_calendar_id, "availability calendar ID"),
            (self.availability_calendar_sha256, "availability calendar SHA"),
        ):
            _digest(value, label)
        _utc(self.published_at_utc, "registry publication time")
        _date(self.release_available_session, "registry release availability")
        if self.release_available_session < self.published_at_utc.date():
            raise IdentityRelationRegistryError("registry release availability is backdated")
        decisions = tuple(sorted(self.decisions, key=lambda item: item.decision_id))
        if decisions != self.decisions:
            raise IdentityRelationRegistryError("registry decision refs must be sorted")
        if len({item.decision_id for item in decisions}) != len(decisions):
            raise IdentityRelationRegistryError("registry decision refs are not unique")
        if decisions and self.release_available_session < max(
            item.decision_available_session for item in decisions
        ):
            raise IdentityRelationRegistryError("registry release availability precedes a decision")

    @property
    def release_id(self) -> str:
        return stable_digest(
            {
                "availability_calendar_id": self.availability_calendar_id,
                "availability_calendar_sha256": self.availability_calendar_sha256,
                "decisions": [
                    {
                        "decision_available_session": (item.decision_available_session.isoformat()),
                        "decision_id": item.decision_id,
                        "decision_path": item.decision_path,
                        "decision_sha256": item.decision_sha256,
                    }
                    for item in self.decisions
                ],
                "namespace": "ame_stocks.identity.relation_registry_release",
                "published_at_utc": self.published_at_utc.isoformat(),
                "registry_kind": self.registry_kind.value,
                "release_available_session": self.release_available_session.isoformat(),
                "rule_version": "s7_identity_relation_registry_release_id_v1",
                "source_candidate_manifest_id": self.source_candidate_manifest_id,
                "source_candidate_manifest_sha256": (self.source_candidate_manifest_sha256),
                "source_external_evidence_manifest_id": (self.source_external_evidence_manifest_id),
                "source_external_evidence_manifest_sha256": (
                    self.source_external_evidence_manifest_sha256
                ),
            }
        )


@dataclass(frozen=True, slots=True)
class CompositeRegistryMatch:
    registry_name: str
    decision_id: str
    source_record_id: str
    observed_composite_figi: str
    canonical_composite_figi: str

    def __post_init__(self) -> None:
        if self.registry_name not in _REGISTRY_NAMES:
            raise IdentityRelationRegistryError(
                "only Composite correction registries may enter collision evaluation"
            )
        _digest(self.decision_id, "Composite decision ID")
        _digest(self.source_record_id, "source record ID")
        _figi(self.observed_composite_figi, "observed Composite FIGI")
        _figi(self.canonical_composite_figi, "canonical Composite FIGI")


@dataclass(frozen=True, slots=True)
class CompositeRegistryCollisionEvaluation:
    source_record_id: str | None
    raw_match_count: int
    matching_registry_names: tuple[str, ...]
    matching_decision_ids: tuple[str, ...]
    unique_decision_id: str | None
    collision: bool
    backtest_identity_eligible: bool
    identity_resolved: bool
    alias_allowed: bool


def evaluate_composite_registry_collisions(
    matches: Sequence[CompositeRegistryMatch],
) -> CompositeRegistryCollisionEvaluation:
    """Apply the frozen no-priority/no-majority Composite registry rule.

    ``backtest_identity_eligible`` is only the registry-layer necessary condition;
    downstream security type, hierarchy, price, liquidity and entitlement gates still
    determine final tradability.
    """

    ordered = tuple(sorted(matches, key=lambda item: (item.registry_name, item.decision_id)))
    source_ids = {item.source_record_id for item in ordered}
    if len(source_ids) > 1:
        raise IdentityRelationRegistryError("one collision evaluation cannot mix source records")
    source_record_id = next(iter(source_ids), None)
    raw_match_count = len(ordered)
    collision = raw_match_count > 1
    unique = ordered[0].decision_id if raw_match_count == 1 else None
    # Zero corrections is the normal direct-identity pass-through.  This helper is
    # only the Composite-registry collision gate; independent quality gates may
    # still make the row ineligible downstream.
    resolved = raw_match_count <= 1
    return CompositeRegistryCollisionEvaluation(
        source_record_id=source_record_id,
        raw_match_count=raw_match_count,
        matching_registry_names=tuple(item.registry_name for item in ordered),
        matching_decision_ids=tuple(item.decision_id for item in ordered),
        unique_decision_id=unique,
        collision=collision,
        backtest_identity_eligible=resolved,
        identity_resolved=resolved,
        alias_allowed=resolved,
    )


def select_effective_terminal_decisions(
    decisions: Sequence[object],
    *,
    cutoff_session: date,
    series_id: Callable[[object], str],
    decision_id: Callable[[object], str],
    decision_version: Callable[[object], int],
    predecessor_id: Callable[[object], str | None],
    available_session: Callable[[object], date],
) -> tuple[object, ...]:
    """Select the highest complete append-only chain version available at cutoff."""

    _date(cutoff_session, "resolution cutoff")
    grouped: dict[str, list[object]] = defaultdict(list)
    for item in decisions:
        grouped[series_id(item)].append(item)
    selected: list[object] = []
    for group_id, rows in sorted(grouped.items()):
        _digest(group_id, "decision series ID")
        ordered = sorted(rows, key=decision_version)
        expected_predecessor: str | None = None
        expected_version = 1
        available: list[object] = []
        seen_ids: set[str] = set()
        previous_available_session: date | None = None
        for item in ordered:
            item_id = decision_id(item)
            _digest(item_id, "decision ID")
            if item_id in seen_ids:
                raise IdentityRelationRegistryError("decision ID is duplicated")
            seen_ids.add(item_id)
            if decision_version(item) != expected_version:
                raise IdentityRelationRegistryError("decision versions are not contiguous")
            if predecessor_id(item) != expected_predecessor:
                raise IdentityRelationRegistryError("decision predecessor chain is invalid")
            item_available_session = available_session(item)
            _date(item_available_session, "decision availability")
            if (
                previous_available_session is not None
                and item_available_session < previous_available_session
            ):
                raise IdentityRelationRegistryError(
                    "decision availability moves backward within a series"
                )
            if item_available_session <= cutoff_session:
                available.append(item)
            expected_predecessor = item_id
            expected_version += 1
            previous_available_session = item_available_session
        if available:
            selected.append(available[-1])
    return tuple(selected)


def _provider_scope(provider_id: str, provider_market: str, provider_locale: str) -> None:
    for value, label in (
        (provider_id, "provider ID"),
        (provider_market, "provider market"),
        (provider_locale, "provider locale"),
    ):
        _text(value, label)
    if provider_id != "massive" or provider_market != "stocks" or provider_locale != "us":
        raise IdentityRelationRegistryError(
            "S7 relation registry scope must be exact massive/stocks/us"
        )


def _record_ids(values: tuple[str, ...], label: str) -> None:
    if not values:
        raise IdentityRelationRegistryError(f"{label} cannot be empty")
    for value in values:
        _digest(value, label)
    if values != tuple(sorted(set(values))):
        raise IdentityRelationRegistryError(f"{label} must be sorted and unique")


def _version(version: int, predecessor: str | None, label: str) -> None:
    if type(version) is not int or version < 1:
        raise IdentityRelationRegistryError(f"{label} version is invalid")
    if (version == 1) != (predecessor is None):
        raise IdentityRelationRegistryError(f"{label} predecessor/version matrix is invalid")
    if predecessor is not None:
        _digest(predecessor, f"{label} predecessor")


def _approval(actor: str, approved_at_utc: datetime) -> None:
    _text(actor, "approval actor")
    _utc(approved_at_utc, "approval time")


def _ordered_interval(start: date, end: date) -> None:
    _date(start, "interval start")
    _date(end, "interval end")
    if end < start:
        raise IdentityRelationRegistryError("decision interval is reversed")


def _utc(value: object, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise IdentityRelationRegistryError(f"{label} must be timezone-aware")
    if value.utcoffset() != timedelta(0):
        raise IdentityRelationRegistryError(f"{label} must use exact UTC")
    return value.astimezone(UTC)


def _date(value: object, label: str) -> date:
    if not isinstance(value, date) or isinstance(value, datetime):
        raise IdentityRelationRegistryError(f"{label} must be a date")
    return value


def _figi(value: object, label: str) -> str:
    if not isinstance(value, str) or not _FIGI.fullmatch(value):
        raise IdentityRelationRegistryError(f"{label} is invalid")
    return value


def _market_code(value: object, label: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[A-Z0-9]{2,8}", value):
        raise IdentityRelationRegistryError(f"{label} is invalid")
    return value


def _digest(value: object, label: str) -> str:
    if not isinstance(value, str) or not _DIGEST.fullmatch(value):
        raise IdentityRelationRegistryError(f"{label} is not a lowercase SHA-256")
    return value


def _text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > 1_000:
        raise IdentityRelationRegistryError(f"{label} is invalid")
    return value


def _relative_path(value: object, label: str) -> str:
    text = _text(value, label)
    if text.startswith("/") or ".." in text.split("/"):
        raise IdentityRelationRegistryError(f"{label} must be a normalized relative path")
    return text


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )


__all__ = [
    "AssetTransitionDecision",
    "AssetTransitionDisposition",
    "AssetTransitionType",
    "CompositeRegistryCollisionEvaluation",
    "CompositeRegistryMatch",
    "IdentityRelationRegistryError",
    "IdentityRelationRegistryRelease",
    "ProviderCompositeOverrideDecision",
    "ProviderCompositeOverrideDisposition",
    "RegistryDecisionArtifactRef",
    "RelationRegistryKind",
    "ShareClassAdjudicationDecision",
    "ShareClassAdjudicationDisposition",
    "canonical_share_class_id",
    "evaluate_composite_registry_collisions",
    "select_effective_terminal_decisions",
]
