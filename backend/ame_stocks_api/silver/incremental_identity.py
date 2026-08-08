"""Stable S7.5 ticker-alias segment and resolution-version identities.

The stable segment key describes only the provider observation that opened an
alias interval.  Canonical research results and their changing knowledge state
live in a separate, append-only resolution version.  Neither identity admits a
release identifier, process metadata, nor wall-clock capture time.

This module is deliberately pure: it validates and hashes logical values but
does not read data, resolve a release chain, or write an artifact.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import InitVar, dataclass, fields, replace
from datetime import date
from enum import StrEnum
from typing import Protocol, Self

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver.identity_resolution import canonical_asset_id

_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_FIGI = re.compile(r"^BBG[0-9A-Z]{9}$")
_CIK = re.compile(r"^[0-9]{10}$")
_TOKEN = re.compile(r"^[a-z][a-z0-9_]*$")

ALIAS_SEGMENT_ID_NAMESPACE = "ame_stocks.identity.alias_segment"
ALIAS_SEGMENT_ID_RULE_VERSION = "ame_stocks_alias_segment_id_v1"
ALIAS_RESOLUTION_VERSION_ID_NAMESPACE = "ame_stocks.identity.alias_resolution_version"
ALIAS_RESOLUTION_VERSION_ID_RULE_VERSION = "ame_stocks_alias_resolution_version_id_v1"
SHARE_CLASS_ID_NAMESPACE = "ame_stocks.identity.share_class"
SHARE_CLASS_ID_RULE_VERSION = "ame_stocks_share_class_id_from_share_class_figi_v1"
ISSUER_ID_NAMESPACE = "ame_stocks.identity.issuer"
ISSUER_ID_RULE_VERSION = "ame_stocks_issuer_id_from_normalized_cik_v1"

ALIAS_SEGMENT_ID_SUBJECT_FIELD_ORDER = (
    "observed_cik_normalized",
    "observed_composite_figi",
    "observed_share_class_figi",
    "provider_id",
    "provider_locale",
    "provider_market",
    "segment_origin_source_record_id",
    "ticker",
    "valid_from_session",
)
ALIAS_SEGMENT_ID_SUBJECT_FIELDS = frozenset(ALIAS_SEGMENT_ID_SUBJECT_FIELD_ORDER)
ALIAS_SEGMENT_ID_FIELDS = ALIAS_SEGMENT_ID_SUBJECT_FIELDS | frozenset({"namespace", "rule_version"})

ALIAS_RESOLUTION_VERSION_ID_SUBJECT_FIELD_ORDER = (
    "alias_segment_id",
    "canonical_asset_id",
    "canonical_cik_normalized",
    "canonical_composite_figi",
    "canonical_issuer_id",
    "canonical_share_class_id",
    "canonical_share_class_figi",
    "decision_lineage_ids",
    "disposition",
    "evidence_available_session",
    "evidence_cutoff_session",
    "identity_cutoff_session",
    "identity_policy_bundle_id",
    "is_tombstone",
    "predecessor_alias_resolution_version_id",
    "resolution_available_session",
    "resolution_method",
    "resolution_status",
    "share_class_decision_lineage_ids",
    "share_class_resolution_method",
    "source_record_set_digest",
    "tombstone_reason_code",
    "valid_through_session",
)
ALIAS_RESOLUTION_VERSION_ID_SUBJECT_FIELDS = frozenset(
    ALIAS_RESOLUTION_VERSION_ID_SUBJECT_FIELD_ORDER
)
ALIAS_RESOLUTION_VERSION_ID_FIELDS = ALIAS_RESOLUTION_VERSION_ID_SUBJECT_FIELDS | frozenset(
    {"namespace", "rule_version"}
)


class IncrementalIdentityError(ValueError):
    """Raised when an S7.5 alias identity is ambiguous or unsafe."""


class AliasResolutionStatus(StrEnum):
    """Closed resolution states for one immutable alias row version."""

    RESOLVED = "resolved"
    UNRESOLVED = "unresolved"
    TOMBSTONED = "tombstoned"


class AliasResolutionMethod(StrEnum):
    """Closed provenance domain for asset/Composite resolution."""

    DIRECT_OBSERVED = "direct_observed"
    APPROVED_GENUINE_TRANSITION = "approved_genuine_transition"
    APPROVED_PROVIDER_CONTAMINATION_OVERRIDE = "approved_provider_contamination_override"
    APPROVED_CROSS_MARKET_OVERRIDE = "approved_cross_market_provider_contamination_override"
    APPROVED_PROVIDER_COMPOSITE_OVERRIDE = "approved_provider_composite_override"
    PENDING_REVIEW = "pending_review"
    ADJUDICATED_UNRESOLVED = "adjudicated_unresolved"
    APPROVED_WITHDRAWAL = "approved_withdrawal"


class ShareClassResolutionMethod(StrEnum):
    """Independent Share Class resolution applied after Composite uniqueness."""

    NOT_APPLICABLE = "not_applicable"
    DIRECT_OBSERVED = "direct_observed"
    APPROVED_SHARE_CLASS_ADJUDICATION = "approved_share_class_adjudication"


class AliasResolutionDisposition(StrEnum):
    """Closed research interpretation domain for a resolution version."""

    OBSERVED_CONSISTENT = "observed_consistent"
    CONFIRMED_GENUINE_TRANSITION = "confirmed_genuine_transition"
    CONFIRMED_PROVIDER_CONTAMINATION = "confirmed_provider_contamination"
    PROVIDER_COMPOSITE_STALE_AFTER_TRANSITION = "provider_composite_stale_after_transition"
    TRANSIENT_DUPLICATE_SHARE_CLASS = "transient_duplicate_share_class"
    PENDING_UNRESOLVED = "pending_unresolved"
    PENDING_CROSS_MARKET_REVIEW = "pending_cross_market_review"
    REGISTRY_COLLISION_UNRESOLVED = "registry_collision_unresolved"
    ADJUDICATED_UNRESOLVED = "adjudicated_unresolved"
    CROSS_MARKET_ADJUDICATED_UNRESOLVED = "cross_market_adjudicated_unresolved"
    WITHDRAWN_SOURCE_CORRECTION = "withdrawn_source_correction"


_ALLOWED_RESOLUTION_SHAPES = frozenset(
    {
        (
            AliasResolutionMethod.DIRECT_OBSERVED,
            AliasResolutionStatus.RESOLVED,
            AliasResolutionDisposition.OBSERVED_CONSISTENT,
        ),
        (
            AliasResolutionMethod.APPROVED_GENUINE_TRANSITION,
            AliasResolutionStatus.RESOLVED,
            AliasResolutionDisposition.CONFIRMED_GENUINE_TRANSITION,
        ),
        (
            AliasResolutionMethod.APPROVED_PROVIDER_CONTAMINATION_OVERRIDE,
            AliasResolutionStatus.RESOLVED,
            AliasResolutionDisposition.CONFIRMED_PROVIDER_CONTAMINATION,
        ),
        (
            AliasResolutionMethod.APPROVED_CROSS_MARKET_OVERRIDE,
            AliasResolutionStatus.RESOLVED,
            AliasResolutionDisposition.CONFIRMED_PROVIDER_CONTAMINATION,
        ),
        (
            AliasResolutionMethod.APPROVED_PROVIDER_COMPOSITE_OVERRIDE,
            AliasResolutionStatus.RESOLVED,
            AliasResolutionDisposition.PROVIDER_COMPOSITE_STALE_AFTER_TRANSITION,
        ),
        (
            AliasResolutionMethod.DIRECT_OBSERVED,
            AliasResolutionStatus.RESOLVED,
            AliasResolutionDisposition.TRANSIENT_DUPLICATE_SHARE_CLASS,
        ),
        *(
            (
                AliasResolutionMethod.PENDING_REVIEW,
                AliasResolutionStatus.UNRESOLVED,
                disposition,
            )
            for disposition in (
                AliasResolutionDisposition.PENDING_UNRESOLVED,
                AliasResolutionDisposition.PENDING_CROSS_MARKET_REVIEW,
                AliasResolutionDisposition.REGISTRY_COLLISION_UNRESOLVED,
            )
        ),
        *(
            (
                AliasResolutionMethod.ADJUDICATED_UNRESOLVED,
                AliasResolutionStatus.UNRESOLVED,
                disposition,
            )
            for disposition in (
                AliasResolutionDisposition.ADJUDICATED_UNRESOLVED,
                AliasResolutionDisposition.CROSS_MARKET_ADJUDICATED_UNRESOLVED,
            )
        ),
        (
            AliasResolutionMethod.APPROVED_WITHDRAWAL,
            AliasResolutionStatus.TOMBSTONED,
            AliasResolutionDisposition.WITHDRAWN_SOURCE_CORRECTION,
        ),
    }
)

_DECISION_REQUIRED_METHODS = frozenset(
    set(AliasResolutionMethod)
    - {
        AliasResolutionMethod.DIRECT_OBSERVED,
        AliasResolutionMethod.PENDING_REVIEW,
    }
)

_MECHANICAL_SUCCESSOR_ALLOWED_CHANGE_FIELDS = frozenset(
    {
        "evidence_available_session",
        "evidence_cutoff_session",
        "identity_cutoff_session",
        "resolution_available_session",
        "source_record_set_digest",
        "valid_through_session",
    }
)
_MECHANICAL_SUCCESSOR_PROTECTED_FIELDS = frozenset(
    {
        "canonical_asset_id",
        "canonical_cik_normalized",
        "canonical_composite_figi",
        "canonical_issuer_id",
        "canonical_share_class_id",
        "canonical_share_class_figi",
        "decision_lineage_ids",
        "disposition",
        "identity_policy_bundle_id",
        "is_tombstone",
        "resolution_method",
        "resolution_status",
        "share_class_decision_lineage_ids",
        "share_class_resolution_method",
        "tombstone_reason_code",
    }
)


class TickerAliasRowReceiptLike(Protocol):
    """Minimal receipt projection needed to prove a ticker-alias row operation."""

    table_name: str
    stable_row_key: str
    row_version_id: str
    predecessor_row_version_id: str | None
    operation: object
    row_payload_digest: str


@dataclass(frozen=True, slots=True)
class AliasSegmentIdentity:
    """Immutable provider-observed identity that opens one alias segment."""

    provider_id: str
    provider_market: str
    provider_locale: str
    ticker: str
    observed_composite_figi: str | None
    observed_share_class_figi: str | None
    observed_cik_normalized: str | None
    valid_from_session: date
    segment_origin_source_record_id: str

    def __post_init__(self) -> None:
        _lower_token(self.provider_id, "provider ID")
        _lower_token(self.provider_market, "provider market")
        _lower_token(self.provider_locale, "provider locale")
        _ticker(self.ticker)
        _optional_figi(self.observed_composite_figi, "observed Composite FIGI")
        if self.observed_composite_figi is None:
            raise IncrementalIdentityError("alias segments require an observed Composite FIGI")
        _optional_figi(self.observed_share_class_figi, "observed Share Class FIGI")
        _optional_cik(self.observed_cik_normalized)
        _session(self.valid_from_session, "valid-from session")
        _digest(self.segment_origin_source_record_id, "segment-origin source-record ID")

    def logical_payload(self) -> dict[str, object]:
        """Return the exact, domain-separated payload used for ``alias_segment_id``."""

        payload: dict[str, object] = {
            "namespace": ALIAS_SEGMENT_ID_NAMESPACE,
            "observed_cik_normalized": self.observed_cik_normalized,
            "observed_composite_figi": self.observed_composite_figi,
            "observed_share_class_figi": self.observed_share_class_figi,
            "provider_id": self.provider_id,
            "provider_locale": self.provider_locale,
            "provider_market": self.provider_market,
            "segment_origin_source_record_id": self.segment_origin_source_record_id,
            "rule_version": ALIAS_SEGMENT_ID_RULE_VERSION,
            "ticker": self.ticker,
            "valid_from_session": self.valid_from_session.isoformat(),
        }
        if set(payload) != ALIAS_SEGMENT_ID_FIELDS:  # pragma: no cover - source invariant
            raise IncrementalIdentityError("alias segment payload fields differ")
        return payload

    @property
    def alias_segment_id(self) -> str:
        return stable_digest(self.logical_payload())

    def to_dict(self) -> dict[str, object]:
        return self.logical_payload()

    @classmethod
    def from_dict(cls, value: object) -> Self:
        item = _closed_mapping(value, ALIAS_SEGMENT_ID_FIELDS, "alias segment")
        _fixed_literal(
            item["namespace"],
            ALIAS_SEGMENT_ID_NAMESPACE,
            "alias segment ID namespace",
        )
        _fixed_literal(
            item["rule_version"],
            ALIAS_SEGMENT_ID_RULE_VERSION,
            "alias segment ID rule version",
        )
        return cls(
            provider_id=_text(item["provider_id"], "provider ID"),
            provider_market=_text(item["provider_market"], "provider market"),
            provider_locale=_text(item["provider_locale"], "provider locale"),
            ticker=_text(item["ticker"], "ticker"),
            observed_composite_figi=_optional_text(
                item["observed_composite_figi"], "observed Composite FIGI"
            ),
            observed_share_class_figi=_optional_text(
                item["observed_share_class_figi"], "observed Share Class FIGI"
            ),
            observed_cik_normalized=_optional_text(
                item["observed_cik_normalized"], "observed normalized CIK"
            ),
            valid_from_session=_session_from_json(item["valid_from_session"], "valid-from session"),
            segment_origin_source_record_id=_text(
                item["segment_origin_source_record_id"],
                "segment-origin source-record ID",
            ),
        )


@dataclass(frozen=True, slots=True)
class AliasResolutionVersion:
    """One immutable canonical-resolution version for a stable alias segment."""

    segment: InitVar[AliasSegmentIdentity]
    alias_segment_id: str
    canonical_asset_id: str | None
    canonical_composite_figi: str | None
    canonical_share_class_id: str | None
    canonical_share_class_figi: str | None
    canonical_issuer_id: str | None
    canonical_cik_normalized: str | None
    resolution_method: AliasResolutionMethod
    resolution_status: AliasResolutionStatus
    disposition: AliasResolutionDisposition
    decision_lineage_ids: tuple[str, ...]
    share_class_resolution_method: ShareClassResolutionMethod
    share_class_decision_lineage_ids: tuple[str, ...]
    identity_policy_bundle_id: str
    identity_cutoff_session: date
    resolution_available_session: date
    evidence_cutoff_session: date
    evidence_available_session: date
    valid_through_session: date | None
    source_record_set_digest: str
    predecessor_alias_resolution_version_id: str | None
    is_tombstone: bool
    tombstone_reason_code: str | None

    def __post_init__(self, segment: AliasSegmentIdentity) -> None:
        if not isinstance(segment, AliasSegmentIdentity):
            raise IncrementalIdentityError(
                "alias resolution construction requires AliasSegmentIdentity"
            )
        _digest(self.alias_segment_id, "alias segment ID")
        if self.alias_segment_id != segment.alias_segment_id:
            raise IncrementalIdentityError(
                "alias resolution segment ID does not reproduce from supplied segment"
            )
        _optional_digest(self.canonical_asset_id, "canonical asset ID")
        _optional_figi(self.canonical_composite_figi, "canonical Composite FIGI")
        _optional_digest(self.canonical_share_class_id, "canonical Share Class ID")
        _optional_figi(self.canonical_share_class_figi, "canonical Share Class FIGI")
        _optional_digest(self.canonical_issuer_id, "canonical issuer ID")
        _optional_cik_value(self.canonical_cik_normalized, "canonical normalized CIK")
        if not isinstance(self.resolution_method, AliasResolutionMethod):
            raise IncrementalIdentityError("resolution method is invalid")
        if not isinstance(self.resolution_status, AliasResolutionStatus):
            raise IncrementalIdentityError("resolution status is invalid")
        if not isinstance(self.disposition, AliasResolutionDisposition):
            raise IncrementalIdentityError("resolution disposition is invalid")
        _digest_tuple(self.decision_lineage_ids, "decision lineage IDs")
        if not isinstance(self.share_class_resolution_method, ShareClassResolutionMethod):
            raise IncrementalIdentityError("Share Class resolution method is invalid")
        _digest_tuple(
            self.share_class_decision_lineage_ids,
            "Share Class decision lineage IDs",
        )
        _digest(self.identity_policy_bundle_id, "identity policy bundle ID")
        _session(self.identity_cutoff_session, "identity cutoff session")
        _session(self.resolution_available_session, "resolution availability session")
        _session(self.evidence_cutoff_session, "evidence cutoff session")
        _session(self.evidence_available_session, "evidence availability session")
        _optional_session(self.valid_through_session, "valid-through session")
        _digest(self.source_record_set_digest, "source-record-set digest")
        _optional_digest(
            self.predecessor_alias_resolution_version_id,
            "predecessor alias resolution version ID",
        )
        if type(self.is_tombstone) is not bool:
            raise IncrementalIdentityError("is_tombstone must be a boolean")
        _optional_token(self.tombstone_reason_code, "tombstone reason code")

        resolution_shape = (
            self.resolution_method,
            self.resolution_status,
            self.disposition,
        )
        if resolution_shape not in _ALLOWED_RESOLUTION_SHAPES:
            raise IncrementalIdentityError("resolution method/status/disposition shape is invalid")
        if self.resolution_method in _DECISION_REQUIRED_METHODS:
            if not self.decision_lineage_ids:
                raise IncrementalIdentityError(
                    "approved override, correction, transition, withdrawal, or adjudication "
                    "requires decision lineage"
                )
        elif (
            self.resolution_method
            in {AliasResolutionMethod.DIRECT_OBSERVED, AliasResolutionMethod.PENDING_REVIEW}
            and self.decision_lineage_ids
        ):
            raise IncrementalIdentityError(
                "direct observation or pending review cannot claim approved decision lineage"
            )
        if (
            self.share_class_resolution_method
            is ShareClassResolutionMethod.APPROVED_SHARE_CLASS_ADJUDICATION
        ):
            if not self.share_class_decision_lineage_ids:
                raise IncrementalIdentityError(
                    "Share Class adjudication requires its own decision lineage"
                )
        elif self.share_class_decision_lineage_ids:
            raise IncrementalIdentityError(
                "non-adjudicated Share Class resolution cannot claim decision lineage"
            )

        if self.resolution_available_session > self.identity_cutoff_session:
            raise IncrementalIdentityError("resolution availability exceeds identity cutoff")
        if self.evidence_available_session > self.evidence_cutoff_session:
            raise IncrementalIdentityError("evidence availability exceeds its cutoff")
        if self.evidence_cutoff_session > self.identity_cutoff_session:
            raise IncrementalIdentityError("evidence cutoff exceeds identity cutoff")
        if self.evidence_available_session > self.resolution_available_session:
            raise IncrementalIdentityError(
                "resolution availability cannot precede evidence availability"
            )
        for value, label in (
            (self.identity_cutoff_session, "identity cutoff"),
            (self.resolution_available_session, "resolution availability"),
            (self.evidence_cutoff_session, "evidence cutoff"),
            (self.evidence_available_session, "evidence availability"),
        ):
            if value < segment.valid_from_session:
                raise IncrementalIdentityError(f"{label} precedes alias segment start")
        if self.valid_through_session is not None:
            if self.valid_through_session < segment.valid_from_session:
                raise IncrementalIdentityError("valid-through session precedes alias segment start")
            if self.valid_through_session > self.identity_cutoff_session:
                raise IncrementalIdentityError("valid-through session exceeds identity cutoff")
        if self.predecessor_alias_resolution_version_id == self.alias_segment_id:
            raise IncrementalIdentityError("predecessor version cannot be an alias segment ID")

        canonical_ids = (
            self.canonical_asset_id,
            self.canonical_composite_figi,
            self.canonical_share_class_id,
            self.canonical_share_class_figi,
            self.canonical_issuer_id,
            self.canonical_cik_normalized,
        )
        if self.resolution_status is AliasResolutionStatus.RESOLVED:
            if (
                self.is_tombstone
                or self.canonical_asset_id is None
                or self.canonical_composite_figi is None
            ):
                raise IncrementalIdentityError(
                    "resolved versions require a canonical asset/Composite and cannot be tombstones"
                )
            if (self.canonical_share_class_id is None) != (self.canonical_share_class_figi is None):
                raise IncrementalIdentityError(
                    "canonical Share Class ID and FIGI must be present together"
                )
            if (self.canonical_issuer_id is None) != (self.canonical_cik_normalized is None):
                raise IncrementalIdentityError(
                    "canonical issuer ID and CIK must be present together"
                )
            if self.canonical_asset_id != canonical_asset_id(self.canonical_composite_figi):
                raise IncrementalIdentityError(
                    "canonical asset ID does not reproduce from canonical Composite FIGI"
                )
            if (
                self.canonical_share_class_figi is not None
                and self.canonical_share_class_id
                != canonical_share_class_id(self.canonical_share_class_figi)
            ):
                raise IncrementalIdentityError(
                    "canonical Share Class ID does not reproduce from canonical Share Class FIGI"
                )
            if (
                self.canonical_cik_normalized is not None
                and self.canonical_issuer_id != canonical_issuer_id(self.canonical_cik_normalized)
            ):
                raise IncrementalIdentityError(
                    "canonical issuer ID does not reproduce from canonical normalized CIK"
                )
            if (
                self.resolution_method
                in {
                    AliasResolutionMethod.DIRECT_OBSERVED,
                    AliasResolutionMethod.APPROVED_GENUINE_TRANSITION,
                }
                and self.canonical_composite_figi != segment.observed_composite_figi
            ):
                raise IncrementalIdentityError(
                    "direct or genuine-transition canonical Composite must equal observed"
                )
            if self.canonical_cik_normalized != segment.observed_cik_normalized:
                raise IncrementalIdentityError(
                    "alias resolution cannot change canonical issuer identity"
                )
            if (
                self.resolution_method
                in {
                    AliasResolutionMethod.APPROVED_PROVIDER_CONTAMINATION_OVERRIDE,
                    AliasResolutionMethod.APPROVED_CROSS_MARKET_OVERRIDE,
                    AliasResolutionMethod.APPROVED_PROVIDER_COMPOSITE_OVERRIDE,
                }
                and self.canonical_composite_figi == segment.observed_composite_figi
            ):
                raise IncrementalIdentityError(
                    "canonical override must differ from the observed Composite"
                )
            if (
                self.resolution_method
                in {
                    AliasResolutionMethod.APPROVED_PROVIDER_CONTAMINATION_OVERRIDE,
                    AliasResolutionMethod.APPROVED_CROSS_MARKET_OVERRIDE,
                    AliasResolutionMethod.APPROVED_PROVIDER_COMPOSITE_OVERRIDE,
                }
                and self.canonical_cik_normalized != segment.observed_cik_normalized
            ):
                raise IncrementalIdentityError(
                    "Composite adjudication cannot change issuer identity"
                )
            if self.share_class_resolution_method is ShareClassResolutionMethod.NOT_APPLICABLE:
                if (
                    segment.observed_share_class_figi is not None
                    or self.canonical_share_class_figi is not None
                ):
                    raise IncrementalIdentityError(
                        "resolved Share Class is not-applicable only when observed and "
                        "canonical Share Class FIGIs are absent"
                    )
            elif self.share_class_resolution_method is ShareClassResolutionMethod.DIRECT_OBSERVED:
                if segment.observed_share_class_figi is None:
                    raise IncrementalIdentityError(
                        "direct Share Class resolution requires an observed Share Class FIGI"
                    )
                if self.canonical_share_class_figi != segment.observed_share_class_figi:
                    raise IncrementalIdentityError(
                        "direct Share Class resolution must equal observed"
                    )
            else:
                if self.canonical_share_class_figi is None:
                    raise IncrementalIdentityError(
                        "Share Class adjudication requires a canonical Share Class FIGI"
                    )
                if self.canonical_share_class_figi == segment.observed_share_class_figi:
                    raise IncrementalIdentityError(
                        "Share Class adjudication must change the observed Share Class FIGI"
                    )
                if (
                    self.resolution_method is AliasResolutionMethod.DIRECT_OBSERVED
                    and self.disposition
                    is not AliasResolutionDisposition.TRANSIENT_DUPLICATE_SHARE_CLASS
                ):
                    raise IncrementalIdentityError(
                        "standalone Share Class adjudication requires transient duplicate "
                        "disposition"
                    )
            if (
                self.disposition is AliasResolutionDisposition.TRANSIENT_DUPLICATE_SHARE_CLASS
                and self.share_class_resolution_method
                is not ShareClassResolutionMethod.APPROVED_SHARE_CLASS_ADJUDICATION
            ):
                raise IncrementalIdentityError(
                    "transient duplicate Share Class disposition requires adjudication"
                )
        elif self.resolution_status is AliasResolutionStatus.UNRESOLVED:
            if self.is_tombstone or any(value is not None for value in canonical_ids):
                raise IncrementalIdentityError(
                    "unresolved versions cannot carry canonical IDs or tombstone state"
                )
            if self.share_class_resolution_method is not ShareClassResolutionMethod.NOT_APPLICABLE:
                raise IncrementalIdentityError(
                    "unresolved versions require not-applicable Share Class resolution"
                )
        elif not self.is_tombstone or any(value is not None for value in canonical_ids):
            raise IncrementalIdentityError(
                "tombstoned versions require null canonical IDs and is_tombstone=true"
            )
        elif self.share_class_resolution_method is not ShareClassResolutionMethod.NOT_APPLICABLE:
            raise IncrementalIdentityError(
                "tombstoned versions require not-applicable Share Class resolution"
            )

        if self.is_tombstone:
            if self.predecessor_alias_resolution_version_id is None:
                raise IncrementalIdentityError("a tombstone must supersede an existing version")
            if self.tombstone_reason_code is None:
                raise IncrementalIdentityError("a tombstone requires a reason")
            if not self.decision_lineage_ids:
                raise IncrementalIdentityError("a tombstone requires decision lineage")
        elif self.tombstone_reason_code is not None:
            raise IncrementalIdentityError(
                "non-tombstone versions cannot carry tombstone-only fields"
            )

    @classmethod
    def for_segment(cls, segment: AliasSegmentIdentity, **values: object) -> Self:
        """Build a version while deriving and validating its stable segment edge."""

        if "segment" in values or "alias_segment_id" in values:
            raise IncrementalIdentityError("for_segment derives both segment and alias segment ID")
        return cls(  # type: ignore[arg-type]
            segment=segment,
            alias_segment_id=segment.alias_segment_id,
            **values,
        )

    def logical_payload(self) -> dict[str, object]:
        """Return the exact, closed version-ID payload.

        In particular, a materializing release ID and wall-clock/runtime fields
        are absent: the same logical version may be referenced by more than one
        immutable release without changing its identity.
        """

        payload: dict[str, object] = {
            "alias_segment_id": self.alias_segment_id,
            "canonical_asset_id": self.canonical_asset_id,
            "canonical_cik_normalized": self.canonical_cik_normalized,
            "canonical_composite_figi": self.canonical_composite_figi,
            "canonical_issuer_id": self.canonical_issuer_id,
            "canonical_share_class_id": self.canonical_share_class_id,
            "canonical_share_class_figi": self.canonical_share_class_figi,
            "decision_lineage_ids": list(self.decision_lineage_ids),
            "disposition": self.disposition.value,
            "evidence_available_session": self.evidence_available_session.isoformat(),
            "evidence_cutoff_session": self.evidence_cutoff_session.isoformat(),
            "identity_cutoff_session": self.identity_cutoff_session.isoformat(),
            "identity_policy_bundle_id": self.identity_policy_bundle_id,
            "is_tombstone": self.is_tombstone,
            "namespace": ALIAS_RESOLUTION_VERSION_ID_NAMESPACE,
            "predecessor_alias_resolution_version_id": (
                self.predecessor_alias_resolution_version_id
            ),
            "resolution_available_session": self.resolution_available_session.isoformat(),
            "resolution_method": self.resolution_method.value,
            "resolution_status": self.resolution_status.value,
            "rule_version": ALIAS_RESOLUTION_VERSION_ID_RULE_VERSION,
            "share_class_decision_lineage_ids": list(self.share_class_decision_lineage_ids),
            "share_class_resolution_method": self.share_class_resolution_method.value,
            "source_record_set_digest": self.source_record_set_digest,
            "tombstone_reason_code": self.tombstone_reason_code,
            "valid_through_session": (
                self.valid_through_session.isoformat()
                if self.valid_through_session is not None
                else None
            ),
        }
        if set(payload) != ALIAS_RESOLUTION_VERSION_ID_FIELDS:  # pragma: no cover
            raise IncrementalIdentityError("alias resolution payload fields differ")
        return payload

    @property
    def alias_resolution_version_id(self) -> str:
        return stable_digest(self.logical_payload())

    def to_dict(self) -> dict[str, object]:
        return self.logical_payload()

    @classmethod
    def from_dict(cls, value: object, *, segment: AliasSegmentIdentity) -> Self:
        item = _closed_mapping(
            value,
            ALIAS_RESOLUTION_VERSION_ID_FIELDS,
            "alias resolution version",
        )
        _fixed_literal(
            item["namespace"],
            ALIAS_RESOLUTION_VERSION_ID_NAMESPACE,
            "alias resolution version ID namespace",
        )
        _fixed_literal(
            item["rule_version"],
            ALIAS_RESOLUTION_VERSION_ID_RULE_VERSION,
            "alias resolution version ID rule version",
        )
        try:
            status = AliasResolutionStatus(_text(item["resolution_status"], "resolution status"))
        except ValueError as exc:
            raise IncrementalIdentityError("resolution status is invalid") from exc
        try:
            method = AliasResolutionMethod(_text(item["resolution_method"], "resolution method"))
        except ValueError as exc:
            raise IncrementalIdentityError("resolution method is invalid") from exc
        try:
            share_class_method = ShareClassResolutionMethod(
                _text(
                    item["share_class_resolution_method"],
                    "Share Class resolution method",
                )
            )
        except ValueError as exc:
            raise IncrementalIdentityError("Share Class resolution method is invalid") from exc
        try:
            disposition = AliasResolutionDisposition(
                _text(item["disposition"], "resolution disposition")
            )
        except ValueError as exc:
            raise IncrementalIdentityError("resolution disposition is invalid") from exc
        lineage = item["decision_lineage_ids"]
        if not isinstance(lineage, list):
            raise IncrementalIdentityError("decision lineage IDs must be an array")
        share_class_lineage = item["share_class_decision_lineage_ids"]
        if not isinstance(share_class_lineage, list):
            raise IncrementalIdentityError("Share Class decision lineage IDs must be an array")
        return cls(
            segment=segment,
            alias_segment_id=_text(item["alias_segment_id"], "alias segment ID"),
            canonical_asset_id=_optional_text(item["canonical_asset_id"], "canonical asset ID"),
            canonical_composite_figi=_optional_text(
                item["canonical_composite_figi"], "canonical Composite FIGI"
            ),
            canonical_share_class_id=_optional_text(
                item["canonical_share_class_id"], "canonical Share Class ID"
            ),
            canonical_share_class_figi=_optional_text(
                item["canonical_share_class_figi"], "canonical Share Class FIGI"
            ),
            canonical_issuer_id=_optional_text(item["canonical_issuer_id"], "canonical issuer ID"),
            canonical_cik_normalized=_optional_text(
                item["canonical_cik_normalized"], "canonical normalized CIK"
            ),
            resolution_method=method,
            resolution_status=status,
            disposition=disposition,
            decision_lineage_ids=tuple(_text(value, "decision lineage ID") for value in lineage),
            share_class_resolution_method=share_class_method,
            share_class_decision_lineage_ids=tuple(
                _text(value, "Share Class decision lineage ID") for value in share_class_lineage
            ),
            identity_policy_bundle_id=_text(
                item["identity_policy_bundle_id"], "identity policy bundle ID"
            ),
            identity_cutoff_session=_session_from_json(
                item["identity_cutoff_session"], "identity cutoff session"
            ),
            resolution_available_session=_session_from_json(
                item["resolution_available_session"], "resolution availability session"
            ),
            evidence_cutoff_session=_session_from_json(
                item["evidence_cutoff_session"], "evidence cutoff session"
            ),
            evidence_available_session=_session_from_json(
                item["evidence_available_session"], "evidence availability session"
            ),
            valid_through_session=_optional_session_from_json(
                item["valid_through_session"], "valid-through session"
            ),
            source_record_set_digest=_text(
                item["source_record_set_digest"], "source-record-set digest"
            ),
            predecessor_alias_resolution_version_id=_optional_text(
                item["predecessor_alias_resolution_version_id"],
                "predecessor alias resolution version ID",
            ),
            is_tombstone=_boolean(item["is_tombstone"], "is_tombstone"),
            tombstone_reason_code=_optional_text(
                item["tombstone_reason_code"], "tombstone reason code"
            ),
        )


def alias_segment_id(identity: AliasSegmentIdentity) -> str:
    """Hash exactly the stable provider-observation payload."""

    if not isinstance(identity, AliasSegmentIdentity):
        raise IncrementalIdentityError("alias_segment_id requires AliasSegmentIdentity")
    return identity.alias_segment_id


def alias_resolution_version_id(version: AliasResolutionVersion) -> str:
    """Hash exactly the immutable canonical-resolution payload."""

    if not isinstance(version, AliasResolutionVersion):
        raise IncrementalIdentityError(
            "alias_resolution_version_id requires AliasResolutionVersion"
        )
    return version.alias_resolution_version_id


def canonical_share_class_id(share_class_figi: str) -> str:
    """Reproduce the frozen S7 Share Class ID rule."""

    validated = _optional_figi(share_class_figi, "canonical Share Class FIGI")
    if validated is None:  # pragma: no cover - non-optional public signature
        raise IncrementalIdentityError("canonical Share Class FIGI is required")
    return stable_digest(
        {
            "anchor_type": "share_class_figi",
            "anchor_value": validated,
            "namespace": SHARE_CLASS_ID_NAMESPACE,
            "rule_version": SHARE_CLASS_ID_RULE_VERSION,
        }
    )


def canonical_issuer_id(cik_normalized: str) -> str:
    """Reproduce the frozen S7 issuer ID rule for an already-normalized CIK."""

    validated = _optional_cik_value(cik_normalized, "canonical normalized CIK")
    if validated is None:  # pragma: no cover - non-optional public signature
        raise IncrementalIdentityError("canonical normalized CIK is required")
    return stable_digest(
        {
            "anchor_type": "cik_normalized",
            "anchor_value": validated,
            "namespace": ISSUER_ID_NAMESPACE,
            "rule_version": ISSUER_ID_RULE_VERSION,
        }
    )


def successor_alias_resolution_version(
    previous: AliasResolutionVersion,
    *,
    segment: AliasSegmentIdentity,
    **changes: object,
) -> AliasResolutionVersion:
    """Create a same-segment successor with an exact predecessor edge.

    Only logical resolution fields may change.  The stable segment and the
    predecessor edge are derived from ``previous``; release/runtime metadata is
    therefore rejected as an unknown field rather than leaking into the ID.
    """

    if not isinstance(previous, AliasResolutionVersion):
        raise IncrementalIdentityError("previous must be an AliasResolutionVersion")
    if not isinstance(segment, AliasSegmentIdentity):
        raise IncrementalIdentityError("successor requires AliasSegmentIdentity")
    if previous.alias_segment_id != segment.alias_segment_id:
        raise IncrementalIdentityError("successor segment does not match previous version")
    if previous.is_tombstone:
        raise IncrementalIdentityError("a tombstoned resolution version cannot have a successor")
    forbidden = {"alias_segment_id", "predecessor_alias_resolution_version_id"}
    known = {item.name for item in fields(AliasResolutionVersion)}
    unknown = set(changes) - known
    if unknown:
        raise IncrementalIdentityError(
            f"alias resolution successor fields differ: {sorted(unknown)}"
        )
    if forbidden & set(changes):
        raise IncrementalIdentityError(
            "successor cannot replace its stable segment or predecessor edge"
        )
    if not changes or all(getattr(previous, key) == value for key, value in changes.items()):
        raise IncrementalIdentityError("successor must change a logical resolution field")
    successor = replace(
        previous,
        segment=segment,
        predecessor_alias_resolution_version_id=(previous.alias_resolution_version_id),
        **changes,
    )
    for field_name, label in (
        ("identity_cutoff_session", "identity cutoff session"),
        ("resolution_available_session", "resolution availability session"),
        ("evidence_cutoff_session", "evidence cutoff session"),
        ("evidence_available_session", "evidence availability session"),
    ):
        if getattr(successor, field_name) < getattr(previous, field_name):
            raise IncrementalIdentityError(f"successor {label} cannot move backward")
    if successor.alias_segment_id != previous.alias_segment_id:  # pragma: no cover
        raise IncrementalIdentityError("successor changed its stable alias segment")
    if successor.alias_resolution_version_id == previous.alias_resolution_version_id:
        raise IncrementalIdentityError("successor did not create a new resolution version")
    return successor


def validate_ticker_alias_mechanical_successor(
    previous: AliasResolutionVersion,
    successor: AliasResolutionVersion,
    segment: AliasSegmentIdentity,
    receipt: TickerAliasRowReceiptLike | None = None,
) -> None:
    """Validate the structural part of a mechanical ticker-alias successor.

    This helper freezes every
    research-identity decision and admits only interval/source-range progress
    plus monotone knowledge-time progress.  An optional receipt projection is
    verified against the proven row so the ``mechanical_successor`` label
    cannot be attached to a different payload.  It does not prove that every
    market session in an extended interval has source coverage; Gate A keeps
    row publication disabled until I3 verifies a calendar-aware exact coverage
    receipt in the module-owned dispatcher.
    """

    if not isinstance(previous, AliasResolutionVersion) or not isinstance(
        successor, AliasResolutionVersion
    ):
        raise IncrementalIdentityError(
            "mechanical successor proof requires alias resolution versions"
        )
    if not isinstance(segment, AliasSegmentIdentity):
        raise IncrementalIdentityError("mechanical successor proof requires AliasSegmentIdentity")

    structural_fields = frozenset({"alias_segment_id", "predecessor_alias_resolution_version_id"})
    covered_fields = (
        structural_fields
        | _MECHANICAL_SUCCESSOR_ALLOWED_CHANGE_FIELDS
        | _MECHANICAL_SUCCESSOR_PROTECTED_FIELDS
    )
    if {item.name for item in fields(AliasResolutionVersion)} != covered_fields:
        raise IncrementalIdentityError(
            "mechanical successor proof field coverage differs from the alias model"
        )

    expected_segment_id = segment.alias_segment_id
    if (
        previous.alias_segment_id != expected_segment_id
        or successor.alias_segment_id != expected_segment_id
    ):
        raise IncrementalIdentityError("mechanical successor must preserve the exact segment")
    if previous.is_tombstone:
        raise IncrementalIdentityError("a tombstoned version cannot have a mechanical successor")
    if successor.predecessor_alias_resolution_version_id != previous.alias_resolution_version_id:
        raise IncrementalIdentityError(
            "mechanical successor must bind the exact predecessor version"
        )
    if successor.alias_resolution_version_id == previous.alias_resolution_version_id:
        raise IncrementalIdentityError("mechanical successor must create a new row version")

    for field_name in sorted(_MECHANICAL_SUCCESSOR_PROTECTED_FIELDS):
        if getattr(successor, field_name) != getattr(previous, field_name):
            raise IncrementalIdentityError(
                f"mechanical successor changed protected field {field_name}"
            )

    for field_name in (
        "identity_cutoff_session",
        "resolution_available_session",
        "evidence_cutoff_session",
        "evidence_available_session",
    ):
        if getattr(successor, field_name) < getattr(previous, field_name):
            raise IncrementalIdentityError(
                f"mechanical successor {field_name} cannot move backward"
            )

    if (
        successor.valid_through_session is not None
        and successor.valid_through_session < segment.valid_from_session
    ):
        raise IncrementalIdentityError(
            "mechanical successor valid-through session precedes its segment"
        )
    if previous.valid_through_session is not None and (
        successor.valid_through_session is None
        or successor.valid_through_session < previous.valid_through_session
    ):
        raise IncrementalIdentityError(
            "mechanical successor valid-through session cannot move backward"
        )
    if all(
        getattr(successor, field_name) == getattr(previous, field_name)
        for field_name in _MECHANICAL_SUCCESSOR_ALLOWED_CHANGE_FIELDS
    ):
        raise IncrementalIdentityError("mechanical successor must advance an allowed logical field")

    if receipt is None:
        return
    try:
        operation = receipt.operation
        operation_value = getattr(operation, "value", operation)
        receipt_projection = {
            "operation": operation_value,
            "predecessor_row_version_id": receipt.predecessor_row_version_id,
            "row_payload_digest": receipt.row_payload_digest,
            "row_version_id": receipt.row_version_id,
            "stable_row_key": receipt.stable_row_key,
            "table_name": receipt.table_name,
        }
    except AttributeError as exc:
        raise IncrementalIdentityError(
            "mechanical successor receipt projection is incomplete"
        ) from exc

    expected_projection = {
        "operation": "mechanical_successor",
        "predecessor_row_version_id": previous.alias_resolution_version_id,
        "row_payload_digest": stable_digest(successor.to_dict()),
        "row_version_id": successor.alias_resolution_version_id,
        "stable_row_key": expected_segment_id,
        "table_name": "ticker_alias",
    }
    for field_name, expected in expected_projection.items():
        if receipt_projection[field_name] != expected:
            raise IncrementalIdentityError(
                f"mechanical successor receipt {field_name} does not match proven row"
            )


def validate_ticker_alias_clean_delta_root(
    segment: AliasSegmentIdentity,
    version: AliasResolutionVersion,
    receipt: TickerAliasRowReceiptLike,
) -> None:
    """Prove that a clean delta adds only a safe ticker-alias root."""

    if not isinstance(segment, AliasSegmentIdentity) or not isinstance(
        version, AliasResolutionVersion
    ):
        raise IncrementalIdentityError(
            "clean delta root proof requires a segment and alias resolution version"
        )
    if version.alias_segment_id != segment.alias_segment_id:
        raise IncrementalIdentityError("clean delta root must preserve the exact segment")
    if version.predecessor_alias_resolution_version_id is not None:
        raise IncrementalIdentityError("clean delta root cannot have a predecessor")
    if version.is_tombstone:
        raise IncrementalIdentityError("clean delta root cannot be a tombstone")

    allowed_method_status = {
        (
            AliasResolutionMethod.DIRECT_OBSERVED,
            AliasResolutionStatus.RESOLVED,
        ),
        (
            AliasResolutionMethod.PENDING_REVIEW,
            AliasResolutionStatus.UNRESOLVED,
        ),
    }
    if (version.resolution_method, version.resolution_status) not in allowed_method_status:
        raise IncrementalIdentityError(
            "clean delta root must be direct-observed resolved or pending-review unresolved"
        )
    if version.decision_lineage_ids or version.share_class_decision_lineage_ids:
        raise IncrementalIdentityError(
            "clean delta root cannot carry an approved decision or override"
        )
    if version.share_class_resolution_method not in {
        ShareClassResolutionMethod.NOT_APPLICABLE,
        ShareClassResolutionMethod.DIRECT_OBSERVED,
    }:
        raise IncrementalIdentityError("clean delta root cannot carry Share Class adjudication")

    try:
        operation = receipt.operation
        operation_value = getattr(operation, "value", operation)
        receipt_projection = {
            "operation": operation_value,
            "predecessor_row_version_id": receipt.predecessor_row_version_id,
            "row_payload_digest": receipt.row_payload_digest,
            "row_version_id": receipt.row_version_id,
            "stable_row_key": receipt.stable_row_key,
            "table_name": receipt.table_name,
        }
    except AttributeError as exc:
        raise IncrementalIdentityError("clean delta root receipt projection is incomplete") from exc

    expected_projection = {
        "operation": "new_root",
        "predecessor_row_version_id": None,
        "row_payload_digest": stable_digest(version.to_dict()),
        "row_version_id": version.alias_resolution_version_id,
        "stable_row_key": segment.alias_segment_id,
        "table_name": "ticker_alias",
    }
    for field_name, expected in expected_projection.items():
        if receipt_projection[field_name] != expected:
            raise IncrementalIdentityError(
                f"clean delta root receipt {field_name} does not match proven row"
            )


def _closed_mapping(
    value: object,
    expected: frozenset[str],
    label: str,
) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise IncrementalIdentityError(f"{label} must be an object")
    item = dict(value)
    if set(item) != expected:
        raise IncrementalIdentityError(f"{label} fields differ")
    return item


def _fixed_literal(value: object, expected: str, label: str) -> str:
    if value != expected:
        raise IncrementalIdentityError(f"{label} must equal {expected}")
    return expected


def _text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise IncrementalIdentityError(f"{label} must be trimmed nonempty text")
    return value


def _optional_text(value: object, label: str) -> str | None:
    if value is None:
        return None
    return _text(value, label)


def _lower_token(value: object, label: str) -> str:
    text = _text(value, label)
    if _TOKEN.fullmatch(text) is None:
        raise IncrementalIdentityError(f"{label} must be a lowercase token")
    return text


def _optional_token(value: object, label: str) -> str | None:
    if value is None:
        return None
    return _lower_token(value, label)


def _ticker(value: object) -> str:
    # Massive ticker values are exact, case-sensitive provider identifiers.
    # In particular, suffixes such as ``w``/``r``/``p`` are intentionally
    # lower-case in historical S4/S7 rows.  Normalising or rejecting those
    # spellings would change the provider identity grain.
    return _text(value, "ticker")


def _digest(value: object, label: str) -> str:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise IncrementalIdentityError(f"{label} must be a lowercase SHA-256")
    return value


def _optional_digest(value: object, label: str) -> str | None:
    if value is None:
        return None
    return _digest(value, label)


def _digest_tuple(value: object, label: str) -> tuple[str, ...]:
    if not isinstance(value, tuple):
        raise IncrementalIdentityError(f"{label} must be a tuple")
    for item in value:
        _digest(item, label)
    if tuple(sorted(set(value))) != value:
        raise IncrementalIdentityError(f"{label} must be sorted and unique")
    return value


def _optional_figi(value: object, label: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or _FIGI.fullmatch(value) is None:
        raise IncrementalIdentityError(f"{label} is not a valid FIGI")
    return value


def _optional_cik(value: object) -> str | None:
    return _optional_cik_value(value, "observed normalized CIK")


def _optional_cik_value(value: object, label: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or _CIK.fullmatch(value) is None:
        raise IncrementalIdentityError(f"{label} must be ten digits")
    return value


def _session(value: object, label: str) -> date:
    if type(value) is not date:
        raise IncrementalIdentityError(f"{label} must be a date")
    return value


def _optional_session(value: object, label: str) -> date | None:
    if value is None:
        return None
    return _session(value, label)


def _session_from_json(value: object, label: str) -> date:
    text = _text(value, label)
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise IncrementalIdentityError(f"{label} must be an ISO date") from exc


def _optional_session_from_json(value: object, label: str) -> date | None:
    if value is None:
        return None
    return _session_from_json(value, label)


def _boolean(value: object, label: str) -> bool:
    if type(value) is not bool:
        raise IncrementalIdentityError(f"{label} must be a boolean")
    return value
