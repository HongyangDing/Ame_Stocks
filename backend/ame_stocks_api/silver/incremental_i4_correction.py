"""Bounded correction validation for the S7.5 native-v2 identity tables.

I4 retains a pure fixture planner and adds one sealed production factory.  The
production factory reads through an exact-artifact callback, but performs no
discovery, writes, or publication.  It authenticates the parent manifest,
RunReceipt, checkpoint, native-v2 partition bytes, production policy snapshots,
registry change ledger, replacement receipts, approval event, and approval
ledger before deriving the affected scope itself.  Every affected
``universe_daily`` session is replaced as a whole partition; unrelated
canonical projections and all issuer projections must remain byte-for-byte
equivalent at the logical level.

The module has no discovery path and no permissive fallback.  When the alias
frontier cannot be shown to converge to the parent frontier, it raises an
``ExactGroupExpansionRequired`` carrying only the same provider/ticker group.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date
from enum import StrEnum

import pyarrow as pa
import pyarrow.parquet as pq

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver.incremental_contract import (
    ArtifactPin,
    IncrementalReleaseManifest,
    ManifestPin,
    PartitionReceipt,
    PartitionReplacement,
    ReleaseType,
    RowVersionOperation,
    RowVersionReceipt,
    RowVersionReference,
    RunReceipt,
    correction_scope_digest,
    logical_change_set_digest,
)
from ame_stocks_api.silver.incremental_gate import (
    CorrectionAuthorizedAction,
    GateArtifactPin,
    IncrementalGateError,
    PinnedCorrectionAuthorization,
    validate_correction_authorization,
)
from ame_stocks_api.silver.incremental_i3_checkpoint import (
    NATIVE_V2_RELEASE_FAMILY,
    I3CheckpointState,
    IdentityRegistryKind,
    NativeV2ParentReleasePin,
    NativeV2ReleaseManifest,
    OpenAliasState,
    ResolvedPartitionState,
)
from ame_stocks_api.silver.incremental_i3_contract import I3_V2_CONTRACTS
from ame_stocks_api.silver.incremental_i3_dispatch import (
    I3_PRODUCTION_POLICY_SOURCE,
    IdentityObservation,
    IdentityPolicySnapshot,
    RegistryDecision,
    RegistrySourceScopeRow,
    _verify_policy_snapshot,
)
from ame_stocks_api.silver.incremental_identity import (
    canonical_asset_id,
    canonical_issuer_id,
    canonical_share_class_id,
)

I4_CORRECTION_RULE_VERSION = "s7_5_i4_bounded_correction_v1"
I4_ALIAS_BOUNDARY_RULE_VERSION = "s7_5_i4_alias_stable_boundary_v1"
I4_APPROVAL_ATTESTATION_RULE_VERSION = "s7_5_i4_approval_event_attestation_v2"
I4_APPROVAL_EVENT_RULE_VERSION = "s7_5_i4_exact_approval_event_v1"
I4_APPROVAL_LEDGER_RULE_VERSION = "s7_5_i4_approval_ledger_release_v1"
I4_REGISTRY_LEDGER_RULE_VERSION = "s7_5_i4_registry_change_ledger_v1"
I4_WITHDRAWAL_DECISION_RULE_VERSION = "s7_5_i4_registry_withdrawal_decision_v1"
I4_ALIAS_STATE_LEDGER_RULE_VERSION = "s7_5_i4_alias_state_ledger_v1"
I4_LATE_SOURCE_SNAPSHOT_RULE_VERSION = "s7_5_i4_late_source_snapshot_v1"
I4_LATE_SOURCE_LEDGER_RULE_VERSION = "s7_5_i4_late_source_change_ledger_v1"
I4_PRODUCTION_DERIVATION_RULE_VERSION = "s7_5_i4_production_exact_derivation_v2"
I4_PRODUCTION_SOURCE_BINDING_RULE_VERSION = "s7_5_i4_production_source_binding_v2"
I4_TICKER_ALIAS_CORRECTION_VALIDATOR_RULE_VERSION = "s7_5_i4_ticker_alias_reviewed_correction_v1"
_ROW_SEMANTIC_PROOF_ARTIFACT_TYPE = "s7_5_i3_production_row_semantic_proof"
_ROW_SEMANTIC_PROOF_RULE_VERSION = "s7_5_i3_production_row_semantic_proof_v1"

ExactArtifactReader = Callable[[str], bytes]

_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_FIGI = re.compile(r"^BBG[0-9A-Z]{9}$")
_CIK = re.compile(r"^[0-9]{10}$")
_TOKEN = re.compile(r"^[a-z][a-z0-9_]*$")
_TICKER = re.compile(r"^[A-Z0-9][A-Z0-9.\-/]{0,31}$")
_COUNTRY = re.compile(r"^[A-Z]{2}$")

_COMPOSITE_CORRECTION_REGISTRIES = frozenset(
    {
        IdentityRegistryKind.IDENTITY_ADJUDICATION,
        IdentityRegistryKind.IDENTITY_CROSS_MARKET_ADJUDICATION,
        IdentityRegistryKind.PROVIDER_COMPOSITE_OVERRIDE,
    }
)


class I4CorrectionError(ValueError):
    """Raised when an I4 correction is unbounded, ambiguous, or unauthorized."""


class ExactGroupExpansionRequired(I4CorrectionError):
    """Fail-closed result when only more history for the same group may help."""

    def __init__(
        self,
        group: ExactIdentityGroup,
        *,
        from_session: date,
        reason: str,
    ) -> None:
        self.group = group
        self.from_session = from_session
        self.reason = reason
        super().__init__(
            "alias stable boundary is unproved; expand only exact group "
            f"{group.provider_id}/{group.provider_locale}/{group.ticker} "
            f"from {from_session.isoformat()}: {reason}"
        )


class RegistryChangeOperation(StrEnum):
    """Closed registry-change domain for a reviewed correction."""

    SUCCESSOR = "successor"
    WITHDRAWAL = "withdrawal"


class I4ProductionCorrectionCause(StrEnum):
    """Closed production correction evidence branch."""

    REGISTRY_CHANGE = "registry_change"
    LATE_SOURCE = "late_source"


@dataclass(frozen=True, slots=True)
class ExactIdentityGroup:
    """Narrow provider identity group; I4 corrections are US-locale only."""

    provider_id: str
    provider_market: str
    provider_locale: str
    ticker: str

    def __post_init__(self) -> None:
        _token(self.provider_id, "provider ID")
        _token(self.provider_market, "provider market")
        _token(self.provider_locale, "provider locale")
        if self.provider_locale != "us":
            raise I4CorrectionError("I4 correction scope is closed to provider locale=us")
        _ticker(self.ticker)

    def matches(self, row: SourceIdentityKey) -> bool:
        return (
            row.provider_id == self.provider_id
            and row.provider_market == self.provider_market
            and row.provider_locale == self.provider_locale
            and row.ticker == self.ticker
        )

    def to_dict(self) -> dict[str, str]:
        return {
            "provider_id": self.provider_id,
            "provider_locale": self.provider_locale,
            "provider_market": self.provider_market,
            "ticker": self.ticker,
        }


@dataclass(frozen=True, slots=True)
class SourceIdentityKey:
    """Immutable observed-lineage projection for a partition row.

    Unlike the I3 dispatcher row, this projection can represent a real foreign
    locale so I4 can prove that a US-scoped correction did not rewrite it.
    ``from_i3_observation`` is the normal construction path for US rows.
    """

    provider_id: str
    provider_market: str
    provider_locale: str
    ticker: str
    session_date: date
    source_record_id: str
    observed_composite_figi: str | None
    observed_composite_country: str | None
    observed_share_class_figi: str | None
    active_on_date: bool

    def __post_init__(self) -> None:
        _token(self.provider_id, "source provider ID")
        _token(self.provider_market, "source provider market")
        _token(self.provider_locale, "source provider locale")
        _ticker(self.ticker)
        _session(self.session_date, "source session")
        _digest(self.source_record_id, "source record ID")
        _optional_figi(self.observed_composite_figi, "observed Composite FIGI")
        _optional_country(self.observed_composite_country)
        if (self.observed_composite_figi is None) != (self.observed_composite_country is None):
            raise I4CorrectionError("observed Composite FIGI and country must be jointly present")
        _optional_figi(self.observed_share_class_figi, "observed Share Class FIGI")
        if type(self.active_on_date) is not bool:
            raise I4CorrectionError("active_on_date must be Boolean")

    @classmethod
    def from_i3_observation(cls, observation: IdentityObservation) -> SourceIdentityKey:
        if not isinstance(observation, IdentityObservation):
            raise I4CorrectionError("source row is not an I3 IdentityObservation")
        return cls(
            provider_id=observation.provider_id,
            provider_market=observation.provider_market,
            provider_locale=observation.provider_locale,
            ticker=observation.ticker,
            session_date=observation.session_date,
            source_record_id=observation.source_record_id,
            observed_composite_figi=observation.observed_composite_figi,
            observed_composite_country=observation.observed_composite_country,
            observed_share_class_figi=observation.observed_share_class_figi,
            active_on_date=observation.active_on_date,
        )

    @property
    def row_key(self) -> tuple[date, str]:
        return (self.session_date, self.source_record_id)

    def to_dict(self) -> dict[str, object]:
        return {
            "active_on_date": self.active_on_date,
            "observed_composite_country": self.observed_composite_country,
            "observed_composite_figi": self.observed_composite_figi,
            "observed_share_class_figi": self.observed_share_class_figi,
            "provider_id": self.provider_id,
            "provider_locale": self.provider_locale,
            "provider_market": self.provider_market,
            "session_date": self.session_date.isoformat(),
            "source_record_id": self.source_record_id,
            "ticker": self.ticker,
        }

    @classmethod
    def from_dict(cls, value: object) -> SourceIdentityKey:
        if not isinstance(value, Mapping):
            raise I4CorrectionError("source identity row must be an object")
        expected = {
            "active_on_date",
            "observed_composite_country",
            "observed_composite_figi",
            "observed_share_class_figi",
            "provider_id",
            "provider_locale",
            "provider_market",
            "session_date",
            "source_record_id",
            "ticker",
        }
        if set(value) != expected:
            raise I4CorrectionError("source identity row fields differ from the closed contract")
        try:
            session = date.fromisoformat(str(value["session_date"]))
        except ValueError as exc:
            raise I4CorrectionError("source identity session is invalid") from exc
        return cls(
            provider_id=str(value["provider_id"]),
            provider_market=str(value["provider_market"]),
            provider_locale=str(value["provider_locale"]),
            ticker=str(value["ticker"]),
            session_date=session,
            source_record_id=str(value["source_record_id"]),
            observed_composite_figi=_none_or_text(value["observed_composite_figi"]),
            observed_composite_country=_none_or_text(value["observed_composite_country"]),
            observed_share_class_figi=_none_or_text(value["observed_share_class_figi"]),
            active_on_date=value["active_on_date"],
        )


@dataclass(frozen=True, slots=True)
class CanonicalIdentityProjection:
    """Canonical fields whose invariance/authorized change I4 can prove."""

    source: SourceIdentityKey
    canonical_composite_figi: str | None
    canonical_asset_id: str | None
    canonical_share_class_figi: str | None
    canonical_share_class_id: str | None
    canonical_issuer_id: str | None
    canonical_cik_normalized: str | None
    backtest_identity_eligible: bool
    resolution_method: str
    resolution_status: str
    disposition: str
    share_class_resolution_method: str
    decision_lineage_ids: tuple[str, ...]
    share_class_decision_lineage_ids: tuple[str, ...]
    alias_segment_id: str | None
    alias_resolution_version_id: str | None

    def __post_init__(self) -> None:
        if not isinstance(self.source, SourceIdentityKey):
            raise I4CorrectionError("canonical projection source is invalid")
        _optional_figi(self.canonical_composite_figi, "canonical Composite FIGI")
        _optional_digest(self.canonical_asset_id, "canonical asset ID")
        if (self.canonical_composite_figi is None) != (self.canonical_asset_id is None):
            raise I4CorrectionError("canonical Composite and asset ID must coexist")
        if self.canonical_composite_figi is not None and self.canonical_asset_id != (
            canonical_asset_id(self.canonical_composite_figi)
        ):
            raise I4CorrectionError("canonical asset ID does not reproduce")
        _optional_figi(self.canonical_share_class_figi, "canonical Share Class FIGI")
        _optional_digest(self.canonical_share_class_id, "canonical Share Class ID")
        if (self.canonical_share_class_figi is None) != (self.canonical_share_class_id is None):
            raise I4CorrectionError("canonical Share Class FIGI and ID must coexist")
        if self.canonical_share_class_figi is not None and self.canonical_share_class_id != (
            canonical_share_class_id(self.canonical_share_class_figi)
        ):
            raise I4CorrectionError("canonical Share Class ID does not reproduce")
        _optional_digest(self.canonical_issuer_id, "canonical issuer ID")
        if self.canonical_cik_normalized is not None:
            if _CIK.fullmatch(self.canonical_cik_normalized) is None:
                raise I4CorrectionError("canonical CIK must be ten digits")
            if self.canonical_issuer_id != canonical_issuer_id(self.canonical_cik_normalized):
                raise I4CorrectionError("canonical issuer ID does not reproduce from CIK")
        elif self.canonical_issuer_id is not None:
            raise I4CorrectionError("canonical issuer ID requires a canonical CIK")
        if type(self.backtest_identity_eligible) is not bool:
            raise I4CorrectionError("identity eligibility must be Boolean")
        for value, label in (
            (self.resolution_method, "resolution method"),
            (self.resolution_status, "resolution status"),
            (self.disposition, "resolution disposition"),
            (self.share_class_resolution_method, "Share Class resolution method"),
        ):
            _token(value, label)
        _digest_tuple(self.decision_lineage_ids, "decision lineage IDs")
        _digest_tuple(
            self.share_class_decision_lineage_ids,
            "Share Class decision lineage IDs",
        )
        _optional_digest(self.alias_segment_id, "alias segment ID")
        _optional_digest(
            self.alias_resolution_version_id,
            "alias resolution-version ID",
        )
        if (self.alias_segment_id is None) != (self.alias_resolution_version_id is None):
            raise I4CorrectionError("alias segment and resolution-version IDs must coexist")

    @property
    def row_key(self) -> tuple[date, str]:
        return self.source.row_key

    def issuer_payload(self) -> dict[str, object]:
        return {
            "canonical_cik_normalized": self.canonical_cik_normalized,
            "canonical_issuer_id": self.canonical_issuer_id,
        }

    def canonical_payload(self, *, include_alias: bool = True) -> dict[str, object]:
        payload: dict[str, object] = {
            "backtest_identity_eligible": self.backtest_identity_eligible,
            "canonical_asset_id": self.canonical_asset_id,
            "canonical_cik_normalized": self.canonical_cik_normalized,
            "canonical_composite_figi": self.canonical_composite_figi,
            "canonical_issuer_id": self.canonical_issuer_id,
            "canonical_share_class_figi": self.canonical_share_class_figi,
            "canonical_share_class_id": self.canonical_share_class_id,
            "decision_lineage_ids": list(self.decision_lineage_ids),
            "disposition": self.disposition,
            "resolution_method": self.resolution_method,
            "resolution_status": self.resolution_status,
            "share_class_resolution_method": self.share_class_resolution_method,
            "share_class_decision_lineage_ids": list(self.share_class_decision_lineage_ids),
        }
        if include_alias:
            payload.update(
                {
                    "alias_resolution_version_id": self.alias_resolution_version_id,
                    "alias_segment_id": self.alias_segment_id,
                }
            )
        return payload

    def to_dict(self) -> dict[str, object]:
        return {"source": self.source.to_dict(), **self.canonical_payload()}


@dataclass(frozen=True, slots=True)
class SessionPartitionImage:
    """Complete logical rows plus the exact Gate-A partition receipt."""

    receipt: PartitionReceipt
    rows: tuple[CanonicalIdentityProjection, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.receipt, PartitionReceipt):
            raise I4CorrectionError("session image receipt is invalid")
        if type(self.rows) is not tuple or not all(
            isinstance(row, CanonicalIdentityProjection) for row in self.rows
        ):
            raise I4CorrectionError("session image rows are invalid")
        if len(self.rows) != self.receipt.row_count:
            raise I4CorrectionError("session image row count differs from receipt")
        session = date.fromisoformat(self.receipt.partition_key)
        if any(row.source.session_date != session for row in self.rows):
            raise I4CorrectionError("session image contains a row from another partition")
        keys = [row.row_key for row in self.rows]
        if keys != sorted(set(keys)):
            raise I4CorrectionError("session image rows must be sorted and unique")

    @property
    def session_date(self) -> date:
        return date.fromisoformat(self.receipt.partition_key)


@dataclass(frozen=True, slots=True)
class ExactGroupSessionSlot:
    """Complete presence/absence statement for one group on one calendar session."""

    group: ExactIdentityGroup
    session_date: date
    source_rows: tuple[SourceIdentityKey, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.group, ExactIdentityGroup):
            raise I4CorrectionError("exact-group slot group is invalid")
        _session(self.session_date, "exact-group slot session")
        if type(self.source_rows) is not tuple or not all(
            isinstance(row, SourceIdentityKey) for row in self.source_rows
        ):
            raise I4CorrectionError("exact-group slot source rows are invalid")
        if any(
            row.session_date != self.session_date or not self.group.matches(row)
            for row in self.source_rows
        ):
            raise I4CorrectionError("exact-group slot crossed its group or session")
        keys = [row.source_record_id for row in self.source_rows]
        if keys != sorted(set(keys)):
            raise I4CorrectionError("exact-group slot rows must be sorted and unique")

    @property
    def slot_digest(self) -> str:
        return stable_digest(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            "group": self.group.to_dict(),
            "session_date": self.session_date.isoformat(),
            "source_rows": [row.to_dict() for row in self.source_rows],
        }


@dataclass(frozen=True, slots=True)
class AliasBoundaryProof:
    """Full-state convergence proof after one exact-group session."""

    group: ExactIdentityGroup
    session_date: date
    source_slot_digest: str
    parent_open_alias: OpenAliasState | None
    corrected_open_alias: OpenAliasState | None
    parent_fixed_lookback_digest: str
    corrected_fixed_lookback_digest: str
    parent_future_registry_effect_digest: str
    corrected_future_registry_effect_digest: str
    exact_group_history_complete: bool
    correction_effect_exhausted: bool

    def __post_init__(self) -> None:
        if not isinstance(self.group, ExactIdentityGroup):
            raise I4CorrectionError("alias-boundary group is invalid")
        _session(self.session_date, "alias-boundary session")
        for value, label in (
            (self.source_slot_digest, "source-slot digest"),
            (self.parent_fixed_lookback_digest, "parent lookback digest"),
            (self.corrected_fixed_lookback_digest, "corrected lookback digest"),
            (
                self.parent_future_registry_effect_digest,
                "parent future-registry digest",
            ),
            (
                self.corrected_future_registry_effect_digest,
                "corrected future-registry digest",
            ),
        ):
            _digest(value, label)
        for value in (self.parent_open_alias, self.corrected_open_alias):
            if value is not None and not isinstance(value, OpenAliasState):
                raise I4CorrectionError("alias-boundary open state is invalid")
            if value is not None and (
                value.segment.provider_id != self.group.provider_id
                or value.segment.provider_market != self.group.provider_market
                or value.segment.provider_locale != self.group.provider_locale
                or value.segment.ticker != self.group.ticker
            ):
                raise I4CorrectionError("alias-boundary open state crossed exact group")
            if value is not None and (
                value.segment.valid_from_session > self.session_date
                or (
                    value.resolution.valid_through_session is not None
                    and value.resolution.valid_through_session < self.session_date
                )
            ):
                raise I4CorrectionError("alias-boundary open state does not cover session")
        if type(self.exact_group_history_complete) is not bool:
            raise I4CorrectionError("exact-group completeness must be Boolean")
        if type(self.correction_effect_exhausted) is not bool:
            raise I4CorrectionError("correction-effect exhaustion must be Boolean")

    @property
    def is_stable(self) -> bool:
        return (
            self.exact_group_history_complete
            and self.correction_effect_exhausted
            and _alias_state_digest(self.parent_open_alias)
            == _alias_state_digest(self.corrected_open_alias)
            and self.parent_fixed_lookback_digest == self.corrected_fixed_lookback_digest
            and self.parent_future_registry_effect_digest
            == self.corrected_future_registry_effect_digest
        )


@dataclass(frozen=True, slots=True)
class I4AliasStateLedgerEntry:
    """Exact parent/corrected alias states for one reviewed session.

    Production never accepts a caller supplied ``complete`` or ``exhausted``
    flag.  The factory reads this entry from an exact ledger, binds both full
    states to the old/new partition row-version IDs, and derives convergence.
    """

    entry_sequence: int
    group: ExactIdentityGroup
    session_date: date
    parent_open_alias: OpenAliasState
    corrected_open_alias: OpenAliasState

    def __post_init__(self) -> None:
        if type(self.entry_sequence) is not int or self.entry_sequence <= 0:
            raise I4CorrectionError("alias-state ledger entry sequence must be positive")
        if not isinstance(self.group, ExactIdentityGroup):
            raise I4CorrectionError("alias-state ledger group is invalid")
        _session(self.session_date, "alias-state ledger session")
        for state, label in (
            (self.parent_open_alias, "parent"),
            (self.corrected_open_alias, "corrected"),
        ):
            if not isinstance(state, OpenAliasState):
                raise I4CorrectionError(f"alias-state ledger {label} state is invalid")
            segment = state.segment
            if (
                segment.provider_id != self.group.provider_id
                or segment.provider_market != self.group.provider_market
                or segment.provider_locale != self.group.provider_locale
                or segment.ticker != self.group.ticker
            ):
                raise I4CorrectionError("alias-state ledger crossed its exact group")
            if segment.valid_from_session > self.session_date or (
                state.resolution.valid_through_session is not None
                and state.resolution.valid_through_session < self.session_date
            ):
                raise I4CorrectionError("alias-state ledger state does not cover its session")

    def to_dict(self) -> dict[str, object]:
        return {
            "corrected_open_alias": self.corrected_open_alias.to_dict(),
            "entry_sequence": self.entry_sequence,
            "group": self.group.to_dict(),
            "parent_open_alias": self.parent_open_alias.to_dict(),
            "session_date": self.session_date.isoformat(),
        }


@dataclass(frozen=True, slots=True)
class I4AliasStateLedgerRelease:
    """One genesis-only alias-frontier evidence package for S7.5 I4.

    S7.5 deliberately has no multi-release alias-ledger chain.  The retained
    sequence/previous fields are frozen to their genesis values so a caller
    cannot imply append-only history that this module does not verify.
    """

    release_sequence: int
    previous_ledger_release_id: str | None
    release_available_session: date
    entries: tuple[I4AliasStateLedgerEntry, ...]

    def __post_init__(self) -> None:
        if (
            type(self.release_sequence) is not int
            or self.release_sequence != 1
            or self.previous_ledger_release_id is not None
        ):
            raise I4CorrectionError("alias-state evidence must be a genesis-only package")
        _session(self.release_available_session, "alias-state ledger release availability")
        if (
            type(self.entries) is not tuple
            or not self.entries
            or any(not isinstance(item, I4AliasStateLedgerEntry) for item in self.entries)
        ):
            raise I4CorrectionError("alias-state ledger release entries are invalid")
        sequences = tuple(item.entry_sequence for item in self.entries)
        sessions = tuple(item.session_date for item in self.entries)
        groups = {item.group for item in self.entries}
        if sequences != tuple(sorted(set(sequences))):
            raise I4CorrectionError("alias-state ledger entry sequences must be sorted and unique")
        if sessions != tuple(sorted(set(sessions))):
            raise I4CorrectionError("alias-state ledger sessions must be sorted and unique")
        if len(groups) != 1:
            raise I4CorrectionError("alias-state ledger crossed exact groups")

    @property
    def ledger_release_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "entries": [item.to_dict() for item in self.entries],
            "previous_ledger_release_id": self.previous_ledger_release_id,
            "release_available_session": self.release_available_session.isoformat(),
            "release_sequence": self.release_sequence,
            "rule_version": I4_ALIAS_STATE_LEDGER_RULE_VERSION,
        }

    def to_dict(self) -> dict[str, object]:
        return {"ledger_release_id": self.ledger_release_id, **self.logical_payload()}

    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.to_dict())

    def exact_pin(self, *, path: str) -> ArtifactPin:
        content = self.canonical_bytes()
        return ArtifactPin(
            path=path,
            sha256=hashlib.sha256(content).hexdigest(),
            bytes=len(content),
        )


@dataclass(frozen=True, slots=True)
class I4LateSourceSnapshot:
    """One immutable, full selected-source projection for a source session."""

    source_release_id: str
    session_date: date
    source_available_session: date
    rows: tuple[SourceIdentityKey, ...]

    def __post_init__(self) -> None:
        _digest(self.source_release_id, "late-source release ID")
        _session(self.session_date, "late-source snapshot session")
        _session(self.source_available_session, "late-source snapshot availability")
        if self.source_available_session < self.session_date:
            raise I4CorrectionError("late-source snapshot predates its source session")
        if type(self.rows) is not tuple or any(
            not isinstance(item, SourceIdentityKey) for item in self.rows
        ):
            raise I4CorrectionError("late-source snapshot rows are invalid")
        if any(item.session_date != self.session_date for item in self.rows):
            raise I4CorrectionError("late-source snapshot crossed its session")
        keys = tuple(
            (
                item.provider_id,
                item.provider_market,
                item.provider_locale,
                item.ticker,
                item.source_record_id,
            )
            for item in self.rows
        )
        if keys != tuple(sorted(set(keys))):
            raise I4CorrectionError("late-source snapshot rows must be sorted and unique")

    @property
    def snapshot_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "rows": [item.to_dict() for item in self.rows],
            "rule_version": I4_LATE_SOURCE_SNAPSHOT_RULE_VERSION,
            "session_date": self.session_date.isoformat(),
            "source_available_session": self.source_available_session.isoformat(),
            "source_release_id": self.source_release_id,
        }

    def to_dict(self) -> dict[str, object]:
        return {"snapshot_id": self.snapshot_id, **self.logical_payload()}

    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.to_dict())

    def exact_pin(self, *, path: str) -> ArtifactPin:
        content = self.canonical_bytes()
        return ArtifactPin(
            path=path,
            sha256=hashlib.sha256(content).hexdigest(),
            bytes=len(content),
        )

    @classmethod
    def from_dict(cls, value: object) -> I4LateSourceSnapshot:
        if not isinstance(value, Mapping):
            raise I4CorrectionError("late-source snapshot must be an object")
        expected = {
            "rows",
            "rule_version",
            "session_date",
            "snapshot_id",
            "source_available_session",
            "source_release_id",
        }
        if set(value) != expected or value["rule_version"] != I4_LATE_SOURCE_SNAPSHOT_RULE_VERSION:
            raise I4CorrectionError("late-source snapshot fields or rule version differ")
        rows_value = value["rows"]
        if not isinstance(rows_value, list):
            raise I4CorrectionError("late-source snapshot rows must be an array")
        try:
            snapshot = cls(
                source_release_id=str(value["source_release_id"]),
                session_date=date.fromisoformat(str(value["session_date"])),
                source_available_session=date.fromisoformat(str(value["source_available_session"])),
                rows=tuple(SourceIdentityKey.from_dict(item) for item in rows_value),
            )
        except ValueError as exc:
            raise I4CorrectionError("late-source snapshot dates are invalid") from exc
        if value["snapshot_id"] != snapshot.snapshot_id:
            raise I4CorrectionError("late-source snapshot ID does not reproduce")
        return snapshot


@dataclass(frozen=True, slots=True)
class I4LateSourceLedgerEntry:
    """Exact old/new source snapshot pair for one late-source session."""

    entry_sequence: int
    session_date: date
    parent_snapshot_artifact: ArtifactPin
    corrected_snapshot_artifact: ArtifactPin
    change_available_session: date

    def __post_init__(self) -> None:
        if type(self.entry_sequence) is not int or self.entry_sequence <= 0:
            raise I4CorrectionError("late-source ledger entry sequence must be positive")
        _session(self.session_date, "late-source ledger session")
        _session(self.change_available_session, "late-source change availability")
        if any(
            not isinstance(item, ArtifactPin)
            for item in (self.parent_snapshot_artifact, self.corrected_snapshot_artifact)
        ):
            raise I4CorrectionError("late-source snapshot pins are invalid")
        if self.parent_snapshot_artifact == self.corrected_snapshot_artifact:
            raise I4CorrectionError("late-source change requires distinct old/new receipts")

    def to_dict(self) -> dict[str, object]:
        return {
            "change_available_session": self.change_available_session.isoformat(),
            "corrected_snapshot_artifact": self.corrected_snapshot_artifact.to_dict(),
            "entry_sequence": self.entry_sequence,
            "parent_snapshot_artifact": self.parent_snapshot_artifact.to_dict(),
            "session_date": self.session_date.isoformat(),
        }


@dataclass(frozen=True, slots=True)
class I4LateSourceChangeLedgerRelease:
    """Non-authoritative genesis-only caller snapshot package.

    These bytes exist only so red-team tests can prove they never grant a
    production capability.  A future late-source runner requires a real
    ``S4HistoricalSourceCorrectionReceipt`` owned by S4.
    """

    release_sequence: int
    previous_ledger_release_id: str | None
    release_available_session: date
    entries: tuple[I4LateSourceLedgerEntry, ...]

    def __post_init__(self) -> None:
        if (
            type(self.release_sequence) is not int
            or self.release_sequence != 1
            or self.previous_ledger_release_id is not None
        ):
            raise I4CorrectionError("late-source evidence must be a genesis-only package")
        _session(self.release_available_session, "late-source ledger release availability")
        if (
            type(self.entries) is not tuple
            or not self.entries
            or any(not isinstance(item, I4LateSourceLedgerEntry) for item in self.entries)
        ):
            raise I4CorrectionError("late-source ledger release entries are invalid")
        sequences = tuple(item.entry_sequence for item in self.entries)
        sessions = tuple(item.session_date for item in self.entries)
        if sequences != tuple(sorted(set(sequences))):
            raise I4CorrectionError("late-source ledger sequences must be sorted and unique")
        if sessions != tuple(sorted(set(sessions))):
            raise I4CorrectionError("late-source ledger sessions must be sorted and unique")
        if any(
            item.change_available_session > self.release_available_session for item in self.entries
        ):
            raise I4CorrectionError("late-source change availability exceeds ledger release")

    @property
    def ledger_release_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "entries": [item.to_dict() for item in self.entries],
            "previous_ledger_release_id": self.previous_ledger_release_id,
            "release_available_session": self.release_available_session.isoformat(),
            "release_sequence": self.release_sequence,
            "rule_version": I4_LATE_SOURCE_LEDGER_RULE_VERSION,
        }

    def to_dict(self) -> dict[str, object]:
        return {"ledger_release_id": self.ledger_release_id, **self.logical_payload()}

    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.to_dict())

    def exact_pin(self, *, path: str) -> ArtifactPin:
        content = self.canonical_bytes()
        return ArtifactPin(
            path=path,
            sha256=hashlib.sha256(content).hexdigest(),
            bytes=len(content),
        )


@dataclass(frozen=True, slots=True)
class RegistryChange:
    """One externally authenticated registry successor or explicit withdrawal."""

    operation: RegistryChangeOperation
    predecessor: RegistryDecision
    successor: RegistryDecision | None
    change_available_session: date

    def __post_init__(self) -> None:
        if not isinstance(self.operation, RegistryChangeOperation):
            raise I4CorrectionError("registry change operation is invalid")
        if not isinstance(self.predecessor, RegistryDecision):
            raise I4CorrectionError("registry predecessor is invalid")
        _session(self.change_available_session, "registry-change availability")
        if self.change_available_session < self.predecessor.decision_available_session:
            raise I4CorrectionError("registry change predates its predecessor")
        if self.operation is RegistryChangeOperation.WITHDRAWAL:
            if self.successor is not None:
                raise I4CorrectionError("registry withdrawal cannot carry a successor")
            return
        if not isinstance(self.successor, RegistryDecision):
            raise I4CorrectionError("registry successor operation requires a successor")
        successor = self.successor
        if successor.registry_kind is not self.predecessor.registry_kind:
            raise I4CorrectionError("registry successor changed responsibility")
        stable_scope = (
            "provider_id",
            "provider_market",
            "provider_locale",
            "ticker",
            "source_record_id",
            "observed_composite_figi",
            "observed_composite_market_code",
            "composite_scope_figi",
            "observed_share_class_figi",
        )
        if any(
            getattr(successor, field) != getattr(self.predecessor, field) for field in stable_scope
        ):
            raise I4CorrectionError("registry successor widened or changed exact source scope")
        if successor.source_record_ids != self.predecessor.source_record_ids or tuple(
            item.to_dict() for item in successor.source_scope
        ) != tuple(item.to_dict() for item in self.predecessor.source_scope):
            raise I4CorrectionError("registry successor changed complete exact source scope")
        if successor.decision_id == self.predecessor.decision_id:
            raise I4CorrectionError("registry successor must create a new decision ID")
        if successor.registry_release_id == self.predecessor.registry_release_id:
            raise I4CorrectionError("registry successor must be in a successor release")
        if successor.decision_available_session < self.predecessor.decision_available_session:
            raise I4CorrectionError("registry successor availability regressed")
        if self.change_available_session < successor.decision_available_session:
            raise I4CorrectionError("registry-change availability precedes successor decision")

    @property
    def registry_kind(self) -> IdentityRegistryKind:
        return self.predecessor.registry_kind

    @property
    def decision_after(self) -> RegistryDecision | None:
        return self.successor

    def group(self) -> ExactIdentityGroup:
        return ExactIdentityGroup(
            provider_id=self.predecessor.provider_id,
            provider_market=self.predecessor.provider_market,
            provider_locale=self.predecessor.provider_locale,
            ticker=self.predecessor.ticker,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "change_available_session": self.change_available_session.isoformat(),
            "operation": self.operation.value,
            "predecessor": self.predecessor.to_dict(),
            "successor": self.successor.to_dict() if self.successor is not None else None,
        }


@dataclass(frozen=True, slots=True)
class I4RegistryLedgerEntry:
    """One exact externally authenticated successor or withdrawal decision.

    A withdrawal is a positive ledger fact.  It has its own decision ID,
    decision artifact, target registry release, and immutable reason artifact;
    absence of the predecessor from a target snapshot is never sufficient.
    """

    entry_sequence: int
    registry_kind: IdentityRegistryKind
    operation: RegistryChangeOperation
    predecessor_decision_id: str
    predecessor_decision_artifact: ArtifactPin
    predecessor_registry_release_id: str
    predecessor_registry_release_artifact: ArtifactPin
    change_decision_id: str
    change_decision_artifact: ArtifactPin
    change_registry_release_id: str
    change_registry_release_artifact: ArtifactPin
    change_available_session: date
    withdrawal_reason_code: str | None = None
    withdrawal_reason_artifact: ArtifactPin | None = None

    def __post_init__(self) -> None:
        if type(self.entry_sequence) is not int or self.entry_sequence <= 0:
            raise I4CorrectionError("registry ledger entry sequence must be positive")
        if not isinstance(self.registry_kind, IdentityRegistryKind):
            raise I4CorrectionError("registry ledger responsibility is invalid")
        if not isinstance(self.operation, RegistryChangeOperation):
            raise I4CorrectionError("registry ledger operation is invalid")
        for value, label in (
            (self.predecessor_decision_id, "ledger predecessor decision ID"),
            (self.predecessor_registry_release_id, "ledger predecessor release ID"),
            (self.change_decision_id, "ledger change decision ID"),
            (self.change_registry_release_id, "ledger change release ID"),
        ):
            _digest(value, label)
        for value in (
            self.predecessor_decision_artifact,
            self.predecessor_registry_release_artifact,
            self.change_decision_artifact,
            self.change_registry_release_artifact,
        ):
            if not isinstance(value, ArtifactPin):
                raise I4CorrectionError("registry ledger exact artifact pin is invalid")
        if self.predecessor_decision_id == self.change_decision_id:
            raise I4CorrectionError("registry change decision must append a distinct decision")
        if self.predecessor_registry_release_id == self.change_registry_release_id:
            raise I4CorrectionError("registry change must append a successor release")
        _session(self.change_available_session, "registry ledger change availability")
        if self.operation is RegistryChangeOperation.WITHDRAWAL:
            if self.withdrawal_reason_code is None or self.withdrawal_reason_artifact is None:
                raise I4CorrectionError(
                    "registry withdrawal requires an exact reviewed reason artifact"
                )
            _token(self.withdrawal_reason_code, "registry withdrawal reason code")
            if not isinstance(self.withdrawal_reason_artifact, ArtifactPin):
                raise I4CorrectionError("registry withdrawal reason artifact is invalid")
        elif self.withdrawal_reason_code is not None or self.withdrawal_reason_artifact is not None:
            raise I4CorrectionError("registry successor cannot carry withdrawal reason fields")

    @property
    def entry_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "change_available_session": self.change_available_session.isoformat(),
            "change_decision_artifact": self.change_decision_artifact.to_dict(),
            "change_decision_id": self.change_decision_id,
            "change_registry_release_artifact": (self.change_registry_release_artifact.to_dict()),
            "change_registry_release_id": self.change_registry_release_id,
            "entry_sequence": self.entry_sequence,
            "operation": self.operation.value,
            "predecessor_decision_artifact": self.predecessor_decision_artifact.to_dict(),
            "predecessor_decision_id": self.predecessor_decision_id,
            "predecessor_registry_release_artifact": (
                self.predecessor_registry_release_artifact.to_dict()
            ),
            "predecessor_registry_release_id": self.predecessor_registry_release_id,
            "registry_kind": self.registry_kind.value,
            "withdrawal_reason_artifact": (
                self.withdrawal_reason_artifact.to_dict()
                if self.withdrawal_reason_artifact is not None
                else None
            ),
            "withdrawal_reason_code": self.withdrawal_reason_code,
        }

    def to_dict(self) -> dict[str, object]:
        return {"entry_id": self.entry_id, **self.logical_payload()}

    def withdrawal_decision_bytes(self) -> bytes:
        if self.operation is not RegistryChangeOperation.WITHDRAWAL:
            raise I4CorrectionError("successor entry has no withdrawal decision body")
        return _canonical_json_bytes(
            {
                "change_available_session": self.change_available_session.isoformat(),
                "decision_id": self.change_decision_id,
                "operation": self.operation.value,
                "predecessor_decision_id": self.predecessor_decision_id,
                "reason_artifact": self.withdrawal_reason_artifact.to_dict(),
                "reason_code": self.withdrawal_reason_code,
                "registry_kind": self.registry_kind.value,
                "registry_release_id": self.change_registry_release_id,
                "rule_version": I4_WITHDRAWAL_DECISION_RULE_VERSION,
            }
        )


@dataclass(frozen=True, slots=True)
class I4RegistryChangeLedgerRelease:
    """One genesis-only package of externally authenticated registry decisions."""

    release_sequence: int
    previous_ledger_release_id: str | None
    release_available_session: date
    entries: tuple[I4RegistryLedgerEntry, ...]

    def __post_init__(self) -> None:
        if (
            type(self.release_sequence) is not int
            or self.release_sequence != 1
            or self.previous_ledger_release_id is not None
        ):
            raise I4CorrectionError("registry evidence must be a genesis-only package")
        _session(self.release_available_session, "registry ledger release availability")
        if (
            type(self.entries) is not tuple
            or not self.entries
            or any(not isinstance(item, I4RegistryLedgerEntry) for item in self.entries)
        ):
            raise I4CorrectionError("registry ledger release entries are invalid")
        sequences = tuple(item.entry_sequence for item in self.entries)
        if sequences != tuple(sorted(set(sequences))):
            raise I4CorrectionError("registry ledger entries must be sorted and unique")
        if any(
            item.change_available_session > self.release_available_session for item in self.entries
        ):
            raise I4CorrectionError("registry change availability exceeds ledger release")

    @property
    def ledger_release_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "entries": [item.to_dict() for item in self.entries],
            "previous_ledger_release_id": self.previous_ledger_release_id,
            "release_available_session": self.release_available_session.isoformat(),
            "release_sequence": self.release_sequence,
            "rule_version": I4_REGISTRY_LEDGER_RULE_VERSION,
        }

    def to_dict(self) -> dict[str, object]:
        return {"ledger_release_id": self.ledger_release_id, **self.logical_payload()}

    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.to_dict())

    def exact_pin(self, *, path: str) -> ArtifactPin:
        content = self.canonical_bytes()
        return ArtifactPin(
            path=path,
            sha256=hashlib.sha256(content).hexdigest(),
            bytes=len(content),
        )


@dataclass(frozen=True, slots=True)
class I4ApprovalEvent:
    """Canonical approval event whose bytes are named by Gate A.

    The event deliberately excludes ``authorization_id``: Gate A includes the
    event ID and SHA-256 in the authorization body, so including the resulting
    authorization ID here would create a hash cycle.  Every substantive Gate-A
    field is repeated and compared by the exact reader instead.
    """

    authorized_action: CorrectionAuthorizedAction
    parent_release_id: str
    expected_change_set_digest: str
    source_binding_digest: str
    schema_digest: str
    transform_semantics_digest: str
    calendar_digest: str
    identity_policy_before_id: str
    identity_policy_after_id: str
    scope_digest: str
    approver_id: str
    event_available_session: date

    def __post_init__(self) -> None:
        if self.authorized_action is not CorrectionAuthorizedAction.PUBLISH_EXACT_CORRECTION:
            raise I4CorrectionError("approval event grants an unsupported action")
        for value, label in (
            (self.parent_release_id, "approval parent release ID"),
            (self.expected_change_set_digest, "approval change-set digest"),
            (self.source_binding_digest, "approval source-binding digest"),
            (self.schema_digest, "approval schema digest"),
            (self.transform_semantics_digest, "approval transform digest"),
            (self.calendar_digest, "approval calendar digest"),
            (self.identity_policy_before_id, "approval prior policy ID"),
            (self.identity_policy_after_id, "approval target policy ID"),
            (self.scope_digest, "approval scope digest"),
        ):
            _digest(value, label)
        _token(self.approver_id, "approval event approver")
        _session(self.event_available_session, "approval event availability")

    @property
    def approval_event_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "authorized_action": self.authorized_action.value,
            "calendar_digest": self.calendar_digest,
            "event_available_session": self.event_available_session.isoformat(),
            "expected_change_set_digest": self.expected_change_set_digest,
            "identity_policy_after_id": self.identity_policy_after_id,
            "identity_policy_before_id": self.identity_policy_before_id,
            "parent_release_id": self.parent_release_id,
            "approver_id": self.approver_id,
            "rule_version": I4_APPROVAL_EVENT_RULE_VERSION,
            "schema_digest": self.schema_digest,
            "scope_digest": self.scope_digest,
            "source_binding_digest": self.source_binding_digest,
            "transform_semantics_digest": self.transform_semantics_digest,
        }

    def to_dict(self) -> dict[str, object]:
        return {"approval_event_id": self.approval_event_id, **self.logical_payload()}

    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.to_dict())

    def exact_pin(self, *, path: str) -> GateArtifactPin:
        content = self.canonical_bytes()
        return GateArtifactPin(
            path=path,
            sha256=hashlib.sha256(content).hexdigest(),
            bytes=len(content),
        )


@dataclass(frozen=True, slots=True)
class I4ApprovalLedgerEntry:
    """One exact evidence row binding authorization and event bytes."""

    ledger_index: int
    authorization_id: str
    authorization_artifact: GateArtifactPin
    approval_event_id: str
    event_artifact: GateArtifactPin
    recorded_available_session: date

    def __post_init__(self) -> None:
        if type(self.ledger_index) is not int or self.ledger_index <= 0:
            raise I4CorrectionError("approval ledger index must be positive")
        _digest(self.authorization_id, "approval ledger authorization ID")
        _digest(self.approval_event_id, "approval ledger event ID")
        if not isinstance(self.authorization_artifact, GateArtifactPin) or not isinstance(
            self.event_artifact, GateArtifactPin
        ):
            raise I4CorrectionError("approval ledger exact pins are invalid")
        _session(self.recorded_available_session, "approval ledger row availability")

    def to_dict(self) -> dict[str, object]:
        return {
            "approval_event_id": self.approval_event_id,
            "authorization_artifact": self.authorization_artifact.to_dict(),
            "authorization_id": self.authorization_id,
            "event_artifact": self.event_artifact.to_dict(),
            "ledger_index": self.ledger_index,
            "recorded_available_session": self.recorded_available_session.isoformat(),
        }


@dataclass(frozen=True, slots=True)
class I4ApprovalLedgerRelease:
    """One genesis-only exact approval evidence package for S7.5 I4."""

    release_sequence: int
    previous_ledger_release_id: str | None
    release_available_session: date
    entries: tuple[I4ApprovalLedgerEntry, ...]

    def __post_init__(self) -> None:
        if (
            type(self.release_sequence) is not int
            or self.release_sequence != 1
            or self.previous_ledger_release_id is not None
        ):
            raise I4CorrectionError("approval evidence must be a genesis-only package")
        _session(self.release_available_session, "approval ledger release availability")
        if (
            type(self.entries) is not tuple
            or not self.entries
            or any(not isinstance(item, I4ApprovalLedgerEntry) for item in self.entries)
        ):
            raise I4CorrectionError("approval ledger release entries are invalid")
        indexes = tuple(item.ledger_index for item in self.entries)
        if indexes != tuple(sorted(set(indexes))):
            raise I4CorrectionError("approval ledger indexes must be sorted and unique")
        if any(
            item.recorded_available_session > self.release_available_session
            for item in self.entries
        ):
            raise I4CorrectionError("approval ledger row availability exceeds its release")

    @property
    def ledger_release_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "entries": [item.to_dict() for item in self.entries],
            "previous_ledger_release_id": self.previous_ledger_release_id,
            "release_available_session": self.release_available_session.isoformat(),
            "release_sequence": self.release_sequence,
            "rule_version": I4_APPROVAL_LEDGER_RULE_VERSION,
        }

    def to_dict(self) -> dict[str, object]:
        return {"ledger_release_id": self.ledger_release_id, **self.logical_payload()}

    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.to_dict())

    def exact_pin(self, *, path: str) -> GateArtifactPin:
        content = self.canonical_bytes()
        return GateArtifactPin(
            path=path,
            sha256=hashlib.sha256(content).hexdigest(),
            bytes=len(content),
        )


@dataclass(frozen=True, slots=True, init=False)
class I4ApprovalEventAttestation:
    """Sealed proof that exact event and genesis evidence-package bytes were read."""

    authorization_id: str
    approval_event_id: str
    event_artifact: GateArtifactPin
    ledger_release_id: str
    ledger_artifact: GateArtifactPin
    attestation_available_session: date
    _seal: object

    def __init__(
        self,
        *,
        authorization_id: str,
        approval_event_id: str,
        event_artifact: GateArtifactPin,
        ledger_release_id: str,
        ledger_artifact: GateArtifactPin,
        attestation_available_session: date,
        _seal: object,
    ) -> None:
        if _seal is not _APPROVAL_ATTESTATION_SEAL:
            raise I4CorrectionError(
                "approval attestations require the production exact-reader factory"
            )
        for name, value in locals().copy().items():
            if name not in {"self", "_seal"}:
                object.__setattr__(self, name, value)
        object.__setattr__(self, "_seal", _seal)
        _digest(self.authorization_id, "attested authorization ID")
        _digest(self.approval_event_id, "attested approval event ID")
        if not isinstance(self.event_artifact, GateArtifactPin):
            raise I4CorrectionError("approval event artifact is invalid")
        _digest(self.ledger_release_id, "approval ledger release ID")
        if not isinstance(self.ledger_artifact, GateArtifactPin):
            raise I4CorrectionError("approval ledger artifact is invalid")
        _session(self.attestation_available_session, "approval attestation availability")

    def validate(
        self,
        pinned: PinnedCorrectionAuthorization,
        *,
        availability_cutoff_session: date,
    ) -> None:
        if self._seal is not _APPROVAL_ATTESTATION_SEAL:
            raise I4CorrectionError("approval attestation lost its exact-reader seal")
        if not isinstance(pinned, PinnedCorrectionAuthorization):
            raise I4CorrectionError("correction authorization pin is invalid")
        authorization = pinned.authorization
        if self.authorization_id != authorization.authorization_id:
            raise I4CorrectionError("approval attestation binds another authorization")
        if self.approval_event_id != authorization.approval_event_id:
            raise I4CorrectionError("approval attestation binds another event")
        if self.event_artifact.sha256 != authorization.approval_event_sha256:
            raise I4CorrectionError("approval event bytes differ from authorization")
        cutoff = _session(availability_cutoff_session, "approval availability cutoff")
        if self.attestation_available_session > cutoff:
            raise I4CorrectionError("approval attestation was unavailable at cutoff")
        if self.attestation_available_session < authorization.approval_available_session:
            raise I4CorrectionError("approval attestation predates approval availability")
        if authorization.approval_available_session > cutoff:
            raise I4CorrectionError("correction approval was unavailable at cutoff")

    def to_dict(self) -> dict[str, object]:
        return {
            "approval_event_id": self.approval_event_id,
            "attestation_available_session": self.attestation_available_session.isoformat(),
            "authorization_id": self.authorization_id,
            "event_artifact": self.event_artifact.to_dict(),
            "ledger_artifact": self.ledger_artifact.to_dict(),
            "ledger_release_id": self.ledger_release_id,
            "rule_version": I4_APPROVAL_ATTESTATION_RULE_VERSION,
        }


@dataclass(frozen=True, slots=True, init=False)
class BoundedCorrectionScope:
    """Content-addressed exact-group range selected by a stable-boundary proof."""

    group: ExactIdentityGroup
    direct_source_rows: tuple[SourceIdentityKey, ...]
    exact_group_slots: tuple[ExactGroupSessionSlot, ...]
    alias_recompute_from_session: date
    alias_stable_boundary_session: date
    authorization_id: str
    authorization_available_session: date
    availability_cutoff_session: date
    _seal: object

    def __init__(
        self,
        *,
        group: ExactIdentityGroup,
        direct_source_rows: tuple[SourceIdentityKey, ...],
        exact_group_slots: tuple[ExactGroupSessionSlot, ...],
        alias_recompute_from_session: date,
        alias_stable_boundary_session: date,
        authorization_id: str,
        authorization_available_session: date,
        availability_cutoff_session: date,
        _seal: object,
    ) -> None:
        if _seal is not _SCOPE_SEAL:
            raise I4CorrectionError(
                "bounded correction scope requires stable-boundary factory output"
            )
        for name, value in locals().copy().items():
            if name not in {"self", "_seal"}:
                object.__setattr__(self, name, value)
        object.__setattr__(self, "_seal", _seal)
        self.__post_init__()

    def __post_init__(self) -> None:
        if not isinstance(self.group, ExactIdentityGroup):
            raise I4CorrectionError("correction scope group is invalid")
        _digest(self.authorization_id, "correction scope authorization ID")
        _session(self.authorization_available_session, "authorization availability")
        _session(self.availability_cutoff_session, "scope availability cutoff")
        if self.authorization_available_session > self.availability_cutoff_session:
            raise I4CorrectionError("authorization availability exceeds correction cutoff")
        if type(self.direct_source_rows) is not tuple or not self.direct_source_rows:
            raise I4CorrectionError("correction scope requires directly affected rows")
        if any(
            not isinstance(row, SourceIdentityKey) or not self.group.matches(row)
            for row in self.direct_source_rows
        ):
            raise I4CorrectionError("directly affected row crossed exact group")
        direct_keys = [row.row_key for row in self.direct_source_rows]
        if direct_keys != sorted(set(direct_keys)):
            raise I4CorrectionError("directly affected rows must be sorted and unique")
        if type(self.exact_group_slots) is not tuple or not self.exact_group_slots:
            raise I4CorrectionError("correction scope requires exact-group session slots")
        if any(
            not isinstance(slot, ExactGroupSessionSlot) or slot.group != self.group
            for slot in self.exact_group_slots
        ):
            raise I4CorrectionError("correction scope slot crossed exact group")
        slot_sessions = tuple(slot.session_date for slot in self.exact_group_slots)
        if slot_sessions != tuple(sorted(set(slot_sessions))):
            raise I4CorrectionError("correction scope slots must be sorted and unique")
        _session(self.alias_recompute_from_session, "alias recompute start")
        _session(self.alias_stable_boundary_session, "alias stable boundary")
        if self.alias_recompute_from_session != min(
            row.session_date for row in self.direct_source_rows
        ):
            raise I4CorrectionError("alias recompute must start at earliest affected session")
        if slot_sessions[0] != self.alias_recompute_from_session:
            raise I4CorrectionError("exact-group slots do not start at alias recompute session")
        if slot_sessions[-1] != self.alias_stable_boundary_session:
            raise I4CorrectionError("exact-group slots do not end at stable boundary")
        all_slot_keys = {row.row_key for slot in self.exact_group_slots for row in slot.source_rows}
        if not set(direct_keys).issubset(all_slot_keys):
            raise I4CorrectionError("direct affected rows are absent from exact-group history")

    @property
    def recompute_sessions(self) -> tuple[date, ...]:
        return tuple(slot.session_date for slot in self.exact_group_slots)

    @property
    def direct_row_keys(self) -> frozenset[tuple[date, str]]:
        return frozenset(row.row_key for row in self.direct_source_rows)

    @property
    def exact_scope_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "alias_recompute_from_session": self.alias_recompute_from_session.isoformat(),
            "alias_stable_boundary_session": self.alias_stable_boundary_session.isoformat(),
            "authorization_available_session": self.authorization_available_session.isoformat(),
            "authorization_id": self.authorization_id,
            "availability_cutoff_session": self.availability_cutoff_session.isoformat(),
            "direct_source_rows": [row.to_dict() for row in self.direct_source_rows],
            "exact_group_slots": [slot.to_dict() for slot in self.exact_group_slots],
            "group": self.group.to_dict(),
            "rule_version": I4_CORRECTION_RULE_VERSION,
        }

    def to_dict(self) -> dict[str, object]:
        return {"exact_scope_id": self.exact_scope_id, **self.logical_payload()}


@dataclass(frozen=True, slots=True)
class I4AuthorizationExpectations:
    """All Gate-A values supplied independently by the correction candidate."""

    change_set_digest: str
    source_binding_digest: str
    schema_digest: str
    transform_semantics_digest: str
    calendar_digest: str
    identity_policy_before_id: str
    identity_policy_after_id: str

    def __post_init__(self) -> None:
        for value, label in (
            (self.change_set_digest, "expected change-set digest"),
            (self.source_binding_digest, "expected source-binding digest"),
            (self.schema_digest, "expected schema digest"),
            (self.transform_semantics_digest, "expected transform digest"),
            (self.calendar_digest, "expected calendar digest"),
            (self.identity_policy_before_id, "prior policy ID"),
            (self.identity_policy_after_id, "target policy ID"),
        ):
            _digest(value, label)


@dataclass(frozen=True, slots=True, init=False)
class I4CorrectionPlan:
    """Validated pure correction plan; still not publication authority."""

    parent_checkpoint_id: str
    parent_release_id: str
    exact_scope: BoundedCorrectionScope
    partition_replacements: tuple[PartitionReplacement, ...]
    added_row_version_receipts: tuple[RowVersionReceipt, ...]
    superseded_row_version_ids: tuple[str, ...]
    registry_changes: tuple[RegistryChange, ...]
    authorization_id: str
    approval_attestation: I4ApprovalEventAttestation
    change_set_digest: str
    identity_policy_before_id: str
    identity_policy_after_id: str
    _seal: object

    def __init__(
        self,
        *,
        parent_checkpoint_id: str,
        parent_release_id: str,
        exact_scope: BoundedCorrectionScope,
        partition_replacements: tuple[PartitionReplacement, ...],
        added_row_version_receipts: tuple[RowVersionReceipt, ...],
        superseded_row_version_ids: tuple[str, ...],
        registry_changes: tuple[RegistryChange, ...],
        authorization_id: str,
        approval_attestation: I4ApprovalEventAttestation,
        change_set_digest: str,
        identity_policy_before_id: str,
        identity_policy_after_id: str,
        _seal: object,
    ) -> None:
        if _seal is not _PLAN_SEAL:
            raise I4CorrectionError("I4 correction plans require validated factory output")
        for name, value in locals().copy().items():
            if name not in {"self", "_seal"}:
                object.__setattr__(self, name, value)
        object.__setattr__(self, "_seal", _seal)

    @property
    def plan_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "added_row_version_receipts": [
                item.to_dict() for item in self.added_row_version_receipts
            ],
            "approval_attestation": self.approval_attestation.to_dict(),
            "authorization_id": self.authorization_id,
            "change_set_digest": self.change_set_digest,
            "exact_scope": self.exact_scope.to_dict(),
            "identity_policy_after_id": self.identity_policy_after_id,
            "identity_policy_before_id": self.identity_policy_before_id,
            "parent_checkpoint_id": self.parent_checkpoint_id,
            "parent_release_id": self.parent_release_id,
            "partition_replacements": [item.to_dict() for item in self.partition_replacements],
            "registry_changes": [item.to_dict() for item in self.registry_changes],
            "rule_version": I4_CORRECTION_RULE_VERSION,
            "superseded_row_version_ids": list(self.superseded_row_version_ids),
        }

    def to_dict(self) -> dict[str, object]:
        return {"plan_id": self.plan_id, **self.logical_payload()}


@dataclass(frozen=True, slots=True, init=False)
class ProductionI4CorrectionCapability:
    """Runtime-only authority produced by exact bytes and module-owned derivation.

    This is still not publication or filesystem-write authority.  It proves
    that a candidate correction has one authenticated parent, one complete
    exact-group scope, one first reproducible stable boundary, exact old/new
    partition receipts, explicit registry ledger changes, and an approval event
    present in one exact genesis-only approval evidence package.
    """

    parent_manifest_pin: ManifestPin
    parent_checkpoint_pin: ArtifactPin
    parent_checkpoint_id: str
    exact_scope: BoundedCorrectionScope
    partition_replacements: tuple[PartitionReplacement, ...]
    added_row_version_receipts: tuple[RowVersionReceipt, ...]
    superseded_row_version_ids: tuple[str, ...]
    registry_changes: tuple[RegistryChange, ...]
    correction_cause: I4ProductionCorrectionCause
    registry_ledger_release_id: str | None
    late_source_ledger_release_id: str | None
    alias_state_ledger_release_id: str
    unaffected_partition_receipts_digest: str
    authorization_id: str
    approval_attestation: I4ApprovalEventAttestation
    source_binding_digest: str
    change_set_digest: str
    identity_policy_before_id: str
    identity_policy_after_id: str
    _seal: object

    def __init__(
        self,
        *,
        parent_manifest_pin: ManifestPin,
        parent_checkpoint_pin: ArtifactPin,
        parent_checkpoint_id: str,
        exact_scope: BoundedCorrectionScope,
        partition_replacements: tuple[PartitionReplacement, ...],
        added_row_version_receipts: tuple[RowVersionReceipt, ...],
        superseded_row_version_ids: tuple[str, ...],
        registry_changes: tuple[RegistryChange, ...],
        correction_cause: I4ProductionCorrectionCause,
        registry_ledger_release_id: str | None,
        late_source_ledger_release_id: str | None,
        alias_state_ledger_release_id: str,
        unaffected_partition_receipts_digest: str,
        authorization_id: str,
        approval_attestation: I4ApprovalEventAttestation,
        source_binding_digest: str,
        change_set_digest: str,
        identity_policy_before_id: str,
        identity_policy_after_id: str,
        _seal: object,
    ) -> None:
        if _seal is not _PRODUCTION_CAPABILITY_SEAL:
            raise I4CorrectionError(
                "production I4 capability requires the sealed exact-derivation factory"
            )
        for name, value in locals().copy().items():
            if name not in {"self", "_seal"}:
                object.__setattr__(self, name, value)
        object.__setattr__(self, "_seal", _seal)

    @property
    def capability_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "added_row_version_receipts": [
                item.to_dict() for item in self.added_row_version_receipts
            ],
            "approval_attestation": self.approval_attestation.to_dict(),
            "authorization_id": self.authorization_id,
            "alias_state_ledger_release_id": self.alias_state_ledger_release_id,
            "change_set_digest": self.change_set_digest,
            "correction_cause": self.correction_cause.value,
            "exact_scope": self.exact_scope.to_dict(),
            "identity_policy_after_id": self.identity_policy_after_id,
            "identity_policy_before_id": self.identity_policy_before_id,
            "parent_checkpoint_id": self.parent_checkpoint_id,
            "parent_checkpoint_pin": self.parent_checkpoint_pin.to_dict(),
            "parent_manifest_pin": self.parent_manifest_pin.to_dict(),
            "partition_replacements": [item.to_dict() for item in self.partition_replacements],
            "registry_changes": [item.to_dict() for item in self.registry_changes],
            "registry_ledger_release_id": self.registry_ledger_release_id,
            "late_source_ledger_release_id": self.late_source_ledger_release_id,
            "rule_version": I4_PRODUCTION_DERIVATION_RULE_VERSION,
            "source_binding_digest": self.source_binding_digest,
            "superseded_row_version_ids": list(self.superseded_row_version_ids),
            "unaffected_partition_receipts_digest": (self.unaffected_partition_receipts_digest),
        }

    def to_dict(self) -> dict[str, object]:
        return {"capability_id": self.capability_id, **self.logical_payload()}


_PLAN_SEAL = object()
_SCOPE_SEAL = object()
_APPROVAL_ATTESTATION_SEAL = object()
_PRODUCTION_CAPABILITY_SEAL = object()


def select_first_stable_alias_boundary(
    *,
    group: ExactIdentityGroup,
    earliest_affected_session: date,
    exact_group_slots: tuple[ExactGroupSessionSlot, ...],
    boundary_proofs: tuple[AliasBoundaryProof, ...],
) -> tuple[date, tuple[ExactGroupSessionSlot, ...]]:
    """Return the first proven convergence point or request exact-group history."""

    if not isinstance(group, ExactIdentityGroup):
        raise I4CorrectionError("alias-boundary group is invalid")
    _session(earliest_affected_session, "earliest affected session")
    if type(exact_group_slots) is not tuple or not exact_group_slots:
        raise ExactGroupExpansionRequired(
            group,
            from_session=earliest_affected_session,
            reason="no exact-group session slots were supplied",
        )
    sessions = tuple(slot.session_date for slot in exact_group_slots)
    if sessions != tuple(sorted(set(sessions))) or sessions[0] != earliest_affected_session:
        raise I4CorrectionError("alias review slots must be sorted from earliest affected session")
    if any(slot.group != group for slot in exact_group_slots):
        raise I4CorrectionError("alias review slots crossed exact group")
    if type(boundary_proofs) is not tuple:
        raise I4CorrectionError("alias boundary proofs must be a tuple")
    proof_by_session: dict[date, AliasBoundaryProof] = {}
    for proof in boundary_proofs:
        if not isinstance(proof, AliasBoundaryProof) or proof.group != group:
            raise I4CorrectionError("alias boundary proof crossed exact group")
        if proof.session_date in proof_by_session:
            raise I4CorrectionError("duplicate alias boundary proof session")
        proof_by_session[proof.session_date] = proof
    if not set(proof_by_session).issubset(set(sessions)):
        raise I4CorrectionError("alias boundary proof is outside supplied exact-group history")
    for index, slot in enumerate(exact_group_slots):
        proof = proof_by_session.get(slot.session_date)
        if proof is None:
            raise ExactGroupExpansionRequired(
                group,
                from_session=earliest_affected_session,
                reason=f"missing boundary proof for {slot.session_date.isoformat()}",
            )
        if proof.source_slot_digest != slot.slot_digest:
            raise I4CorrectionError("alias boundary proof binds another source slot")
        if not proof.exact_group_history_complete:
            raise ExactGroupExpansionRequired(
                group,
                from_session=earliest_affected_session,
                reason=f"incomplete exact-group history on {slot.session_date.isoformat()}",
            )
        if proof.is_stable:
            return slot.session_date, exact_group_slots[: index + 1]
    raise ExactGroupExpansionRequired(
        group,
        from_session=earliest_affected_session,
        reason="no supplied session reproduces the parent alias/lookback/future-policy frontier",
    )


def freeze_bounded_correction_scope(
    *,
    group: ExactIdentityGroup,
    direct_source_rows: tuple[SourceIdentityKey, ...],
    exact_group_slots: tuple[ExactGroupSessionSlot, ...],
    boundary_proofs: tuple[AliasBoundaryProof, ...],
    ordered_calendar_sessions: tuple[date, ...],
    authorization: PinnedCorrectionAuthorization,
    availability_cutoff_session: date,
) -> BoundedCorrectionScope:
    """Freeze exact source rows and the smallest proven alias recompute range."""

    if type(direct_source_rows) is not tuple or not direct_source_rows:
        raise I4CorrectionError("bounded correction requires directly affected source rows")
    if any(not isinstance(row, SourceIdentityKey) for row in direct_source_rows):
        raise I4CorrectionError("directly affected source rows are invalid")
    earliest = min(row.session_date for row in direct_source_rows)
    boundary, selected_slots = select_first_stable_alias_boundary(
        group=group,
        earliest_affected_session=earliest,
        exact_group_slots=exact_group_slots,
        boundary_proofs=boundary_proofs,
    )
    calendar = _calendar(ordered_calendar_sessions)
    try:
        first_index = calendar.index(earliest)
        last_index = calendar.index(boundary)
    except ValueError as exc:
        raise I4CorrectionError("alias range is absent from the exact calendar") from exc
    if (
        tuple(slot.session_date for slot in selected_slots)
        != calendar[first_index : last_index + 1]
    ):
        raise I4CorrectionError("exact-group slots are gapped against the exact calendar")
    if not isinstance(authorization, PinnedCorrectionAuthorization):
        raise I4CorrectionError("bounded scope requires a pinned correction authorization")
    return BoundedCorrectionScope(
        group=group,
        direct_source_rows=direct_source_rows,
        exact_group_slots=selected_slots,
        alias_recompute_from_session=earliest,
        alias_stable_boundary_session=boundary,
        authorization_id=authorization.authorization.authorization_id,
        authorization_available_session=(authorization.authorization.approval_available_session),
        availability_cutoff_session=availability_cutoff_session,
        _seal=_SCOPE_SEAL,
    )


def production_i4_source_binding_digest(
    *,
    parent_manifest_pin: ManifestPin,
    parent_run_receipt_artifact: ArtifactPin,
    parent_checkpoint_artifact: ArtifactPin,
    parent_partition_artifacts: tuple[ArtifactPin, ...],
    replacement_partition_receipts: tuple[PartitionReceipt, ...],
    prior_policy_bundle_artifact: ArtifactPin,
    target_policy_bundle_artifact: ArtifactPin,
    alias_state_ledger_artifact: ArtifactPin,
    registry_ledger_artifact: ArtifactPin | None = None,
    late_source_ledger_artifact: ArtifactPin | None = None,
    added_row_version_receipts: tuple[RowVersionReceipt, ...] = (),
) -> str:
    """Reproduce the fixed production-I4 input binding from exact pins.

    The factory always calls this itself.  Exposing the deterministic function
    lets an approval producer calculate the value before the event is written;
    supplying the resulting digest back to the factory grants no capability.
    """

    if not isinstance(parent_manifest_pin, ManifestPin):
        raise I4CorrectionError("production source binding requires a parent manifest pin")
    fixed_required = (
        parent_run_receipt_artifact,
        parent_checkpoint_artifact,
        prior_policy_bundle_artifact,
        target_policy_bundle_artifact,
        alias_state_ledger_artifact,
    )
    if any(not isinstance(item, ArtifactPin) for item in fixed_required):
        raise I4CorrectionError("production source binding contains an invalid exact pin")
    if not isinstance(registry_ledger_artifact, ArtifactPin):
        raise I4CorrectionError(
            "production source binding requires exact registry correction evidence"
        )
    if late_source_ledger_artifact is not None:
        raise I4CorrectionError(
            "late-source production correction requires a future "
            "S4HistoricalSourceCorrectionReceipt"
        )
    optional_evidence = (registry_ledger_artifact,)
    if type(parent_partition_artifacts) is not tuple or any(
        not isinstance(item, ArtifactPin) for item in parent_partition_artifacts
    ):
        raise I4CorrectionError("parent partition pins are invalid")
    if type(replacement_partition_receipts) is not tuple or any(
        not isinstance(item, PartitionReceipt) for item in replacement_partition_receipts
    ):
        raise I4CorrectionError("replacement partition receipts are invalid")
    if type(added_row_version_receipts) is not tuple or any(
        not isinstance(item, RowVersionReceipt) for item in added_row_version_receipts
    ):
        raise I4CorrectionError("correction row-version receipts are invalid")
    manifest_artifact = ArtifactPin(
        path=parent_manifest_pin.manifest_path,
        sha256=parent_manifest_pin.manifest_sha256,
        bytes=parent_manifest_pin.manifest_bytes,
    )
    row_pins = tuple(
        pin
        for item in added_row_version_receipts
        for pin in (item.index_artifact, item.semantic_proof.artifact)
    )
    pins = (
        manifest_artifact,
        *fixed_required,
        *optional_evidence,
        *parent_partition_artifacts,
        *(item.receipt for item in replacement_partition_receipts),
        *row_pins,
    )
    by_path: dict[str, ArtifactPin] = {}
    for pin in pins:
        prior = by_path.get(pin.path)
        if prior is not None and prior != pin:
            raise I4CorrectionError("production I4 source pins conflict on an artifact path")
        by_path[pin.path] = pin
    ordered = tuple(by_path[path] for path in sorted(by_path))
    return stable_digest(
        {
            "input_pins": [item.to_dict() for item in ordered],
            "rule_version": I4_PRODUCTION_SOURCE_BINDING_RULE_VERSION,
        }
    )


def attest_i4_approval_event_exact(
    *,
    authorization: PinnedCorrectionAuthorization,
    event: I4ApprovalEvent,
    event_artifact: GateArtifactPin,
    ledger: I4ApprovalLedgerRelease,
    ledger_artifact: GateArtifactPin,
    availability_cutoff_session: date,
    artifact_reader: ExactArtifactReader,
    _cache: _ExactReadCache | None = None,
) -> I4ApprovalEventAttestation:
    """Read and verify authorization, event, and ledger release exact bytes."""

    if not isinstance(authorization, PinnedCorrectionAuthorization):
        raise I4CorrectionError("approval exact reader requires a pinned authorization")
    if not isinstance(event, I4ApprovalEvent) or not isinstance(ledger, I4ApprovalLedgerRelease):
        raise I4CorrectionError("approval exact reader inputs are invalid")
    if not isinstance(event_artifact, GateArtifactPin) or not isinstance(
        ledger_artifact, GateArtifactPin
    ):
        raise I4CorrectionError("approval exact-reader pins are invalid")
    cutoff = _session(availability_cutoff_session, "approval availability cutoff")
    cache = _cache or _ExactReadCache()
    authorization_bytes = cache.read(_artifact_from_gate(authorization.artifact), artifact_reader)
    if authorization_bytes != _canonical_json_bytes(authorization.authorization.to_dict()):
        raise I4CorrectionError("stored correction authorization differs from canonical bytes")
    event_bytes = cache.read(_artifact_from_gate(event_artifact), artifact_reader)
    if event_bytes != event.canonical_bytes():
        raise I4CorrectionError("stored approval event differs from canonical event bytes")
    ledger_bytes = cache.read(_artifact_from_gate(ledger_artifact), artifact_reader)
    if ledger_bytes != ledger.canonical_bytes():
        raise I4CorrectionError("stored approval ledger differs from canonical release bytes")

    body = authorization.authorization
    expected = (
        (event.approval_event_id, body.approval_event_id, "event ID"),
        (event_artifact.sha256, body.approval_event_sha256, "event SHA-256"),
        (event.authorized_action, body.authorized_action, "authorized action"),
        (event.parent_release_id, body.parent_release_id, "parent release"),
        (
            event.expected_change_set_digest,
            body.expected_change_set_digest,
            "change set",
        ),
        (event.source_binding_digest, body.source_binding_digest, "source binding"),
        (event.schema_digest, body.schema_digest, "schema"),
        (
            event.transform_semantics_digest,
            body.transform_semantics_digest,
            "transform semantics",
        ),
        (event.calendar_digest, body.calendar_digest, "calendar"),
        (
            event.identity_policy_before_id,
            body.identity_policy_before_id,
            "prior policy",
        ),
        (
            event.identity_policy_after_id,
            body.identity_policy_after_id,
            "target policy",
        ),
        (event.scope_digest, body.scope_digest, "scope"),
        (event.approver_id, body.approver_id, "approver"),
        (
            event.event_available_session,
            body.approval_available_session,
            "availability",
        ),
    )
    for observed, required, label in expected:
        if observed != required:
            raise I4CorrectionError(f"approval event {label} differs from authorization")
    matching = tuple(
        item
        for item in ledger.entries
        if item.authorization_id == body.authorization_id
        and item.approval_event_id == event.approval_event_id
    )
    if len(matching) != 1:
        raise I4CorrectionError("approval ledger lacks one exact authorization/event row")
    entry = matching[0]
    if (
        entry.authorization_artifact != authorization.artifact
        or entry.event_artifact != event_artifact
    ):
        raise I4CorrectionError("approval ledger row exact pins differ")
    if (
        body.approval_available_session > cutoff
        or entry.recorded_available_session > cutoff
        or ledger.release_available_session > cutoff
    ):
        raise I4CorrectionError("approval or ledger release was unavailable at cutoff")
    if entry.recorded_available_session < body.approval_available_session:
        raise I4CorrectionError("approval ledger row predates the approved event")
    return I4ApprovalEventAttestation(
        authorization_id=body.authorization_id,
        approval_event_id=event.approval_event_id,
        event_artifact=event_artifact,
        ledger_release_id=ledger.ledger_release_id,
        ledger_artifact=ledger_artifact,
        attestation_available_session=ledger.release_available_session,
        _seal=_APPROVAL_ATTESTATION_SEAL,
    )


def mint_production_i4_correction_capability(
    *,
    parent_manifest: IncrementalReleaseManifest,
    parent_manifest_pin: ManifestPin,
    parent_run_receipt: RunReceipt,
    checkpoint: I3CheckpointState,
    parent_checkpoint_artifact: ArtifactPin,
    replacement_partition_receipts: tuple[PartitionReceipt, ...],
    prior_policy_snapshot: IdentityPolicySnapshot,
    target_policy_snapshot: IdentityPolicySnapshot,
    target_policy_bundle_artifact: ArtifactPin,
    registry_ledger: I4RegistryChangeLedgerRelease | None,
    registry_ledger_artifact: ArtifactPin | None,
    late_source_ledger: I4LateSourceChangeLedgerRelease | None,
    late_source_ledger_artifact: ArtifactPin | None,
    alias_state_ledger: I4AliasStateLedgerRelease,
    alias_state_ledger_artifact: ArtifactPin,
    added_row_version_receipts: tuple[RowVersionReceipt, ...],
    superseded_row_version_ids: tuple[str, ...],
    authorization: PinnedCorrectionAuthorization,
    approval_event: I4ApprovalEvent,
    approval_event_artifact: GateArtifactPin,
    approval_ledger: I4ApprovalLedgerRelease,
    approval_ledger_artifact: GateArtifactPin,
    availability_cutoff_session: date,
    artifact_reader: ExactArtifactReader,
    parent_correction_authorization: PinnedCorrectionAuthorization | None = None,
    parent_correction_approval_attestation: I4ApprovalEventAttestation | None = None,
) -> ProductionI4CorrectionCapability:
    """Mint the only production I4 capability from authenticated exact bytes.

    There are intentionally no scope, direct-row, slot, boundary-proof,
    registry-change, expected-digest, or replacement-pair parameters.  Those
    facts are all derived below from the parent bytes and exact ledgers.
    """

    cutoff = _session(availability_cutoff_session, "production correction cutoff")
    cache = _ExactReadCache()
    _authenticate_production_parent(
        parent_manifest=parent_manifest,
        parent_manifest_pin=parent_manifest_pin,
        parent_run_receipt=parent_run_receipt,
        checkpoint=checkpoint,
        parent_checkpoint_artifact=parent_checkpoint_artifact,
        parent_correction_authorization=parent_correction_authorization,
        parent_correction_approval_attestation=parent_correction_approval_attestation,
        artifact_reader=artifact_reader,
        cache=cache,
    )
    late_source_branch = late_source_ledger is not None or late_source_ledger_artifact is not None
    if late_source_branch:
        raise I4CorrectionError(
            "caller source snapshots are not production facts; late-source correction "
            "requires a future S4HistoricalSourceCorrectionReceipt"
        )
    if not isinstance(registry_ledger, I4RegistryChangeLedgerRelease) or not isinstance(
        registry_ledger_artifact, ArtifactPin
    ):
        raise I4CorrectionError("registry correction evidence pair is incomplete")
    prior_policy, target_policy = _authenticate_production_policies(
        checkpoint=checkpoint,
        prior_policy_snapshot=prior_policy_snapshot,
        target_policy_snapshot=target_policy_snapshot,
        target_policy_bundle_artifact=target_policy_bundle_artifact,
        availability_cutoff_session=cutoff,
        require_distinct_target=True,
        artifact_reader=artifact_reader,
        cache=cache,
    )
    alias_ledger = _read_alias_state_ledger_exact(
        ledger=alias_state_ledger,
        artifact=alias_state_ledger_artifact,
        availability_cutoff_session=cutoff,
        artifact_reader=artifact_reader,
        cache=cache,
    )
    changes = _derive_registry_changes_exact(
        registry_ledger=registry_ledger,
        registry_ledger_artifact=registry_ledger_artifact,
        prior_policy_snapshot=prior_policy,
        target_policy_snapshot=target_policy,
        availability_cutoff_session=cutoff,
        artifact_reader=artifact_reader,
        cache=cache,
    )
    impact = _derive_registry_impact(changes)
    replacement_sessions = _bounded_replacement_sessions(
        checkpoint=checkpoint,
        replacement_partition_receipts=replacement_partition_receipts,
        impact=impact,
    )
    parent_decoded = _read_parent_partitions_exact(
        checkpoint,
        sessions=replacement_sessions,
        artifact_reader=artifact_reader,
        cache=cache,
    )
    _validate_parent_manifest_partition_receipts(parent_manifest, parent_decoded)
    replacement_decoded = _read_replacement_partitions_exact(
        replacement_partition_receipts,
        artifact_reader=artifact_reader,
        cache=cache,
    )
    scope = _derive_production_scope(
        checkpoint=checkpoint,
        parent_partitions=parent_decoded,
        replacement_partitions=replacement_decoded,
        impact=impact,
        registry_changes=changes,
        prior_policy_snapshot=prior_policy,
        target_policy_snapshot=target_policy,
        alias_state_ledger=alias_ledger,
        authorization=authorization,
        availability_cutoff_session=cutoff,
    )
    validated_changes = _validate_registry_changes(
        scope,
        changes,
        prior_policy.decisions,
        target_policy.decisions,
    )
    _validate_exact_unaffected_projection(
        scope=scope,
        parent_partitions=parent_decoded,
        replacement_partitions=replacement_decoded,
        allow_scoped_source_change=False,
    )
    replacements = _validate_partition_images(
        checkpoint,
        scope,
        tuple(
            item.image for item in parent_decoded if item.session_date in scope.recompute_sessions
        ),
        tuple(
            item.image
            for item in replacement_decoded
            if item.session_date in scope.recompute_sessions
        ),
        validated_changes,
        target_policy,
        late_source_changes=(),
    )
    row_receipts, superseded = _validate_production_alias_row_version(
        checkpoint=checkpoint,
        scope=scope,
        parent_partitions=parent_decoded,
        replacement_partitions=replacement_decoded,
        alias_state_ledger=alias_ledger,
        registry_changes=validated_changes,
        target_policy_snapshot=target_policy,
        receipts=added_row_version_receipts,
        superseded_ids=superseded_row_version_ids,
        availability_cutoff_session=cutoff,
        authorization_available_session=(authorization.authorization.approval_available_session),
        artifact_reader=artifact_reader,
        cache=cache,
    )
    change_set = logical_change_set_digest(
        added_partition_receipts=(),
        partition_replacements=replacements,
        added_row_version_receipts=row_receipts,
        superseded_row_version_ids=superseded,
    )
    source_binding = production_i4_source_binding_digest(
        parent_manifest_pin=parent_manifest_pin,
        parent_run_receipt_artifact=parent_manifest.run_receipt_pin.artifact,
        parent_checkpoint_artifact=parent_checkpoint_artifact,
        parent_partition_artifacts=tuple(
            item.artifact
            for item in checkpoint.resolved_partition_map
            if item.session_date in replacement_sessions
        ),
        replacement_partition_receipts=replacement_partition_receipts,
        prior_policy_bundle_artifact=checkpoint.identity_policy_bundle_artifact,
        target_policy_bundle_artifact=target_policy_bundle_artifact,
        alias_state_ledger_artifact=alias_state_ledger_artifact,
        registry_ledger_artifact=registry_ledger_artifact,
        late_source_ledger_artifact=late_source_ledger_artifact,
        added_row_version_receipts=row_receipts,
    )
    gate_scope = correction_scope_digest(
        parent_release_id=parent_manifest.release_id,
        change_set_digest=change_set,
    )
    try:
        validate_correction_authorization(
            authorization,
            parent_release_id=parent_manifest.release_id,
            change_set_digest=change_set,
            source_binding_digest=source_binding,
            schema_digest=parent_manifest.schema_digest,
            transform_semantics_digest=parent_manifest.transform_semantics_digest,
            calendar_digest=parent_manifest.calendar_digest,
            identity_policy_before_id=prior_policy.policy_bundle.identity_policy_bundle_id,
            identity_policy_after_id=target_policy.policy_bundle.identity_policy_bundle_id,
            scope_digest=gate_scope,
            availability_cutoff_session=cutoff,
        )
    except (IncrementalGateError, ValueError) as exc:
        raise I4CorrectionError(
            "production Gate-A correction authorization does not reproduce"
        ) from exc
    attestation = attest_i4_approval_event_exact(
        authorization=authorization,
        event=approval_event,
        event_artifact=approval_event_artifact,
        ledger=approval_ledger,
        ledger_artifact=approval_ledger_artifact,
        availability_cutoff_session=cutoff,
        artifact_reader=artifact_reader,
        _cache=cache,
    )
    return ProductionI4CorrectionCapability(
        parent_manifest_pin=parent_manifest_pin,
        parent_checkpoint_pin=parent_checkpoint_artifact,
        parent_checkpoint_id=checkpoint.checkpoint_id,
        exact_scope=scope,
        partition_replacements=replacements,
        added_row_version_receipts=row_receipts,
        superseded_row_version_ids=superseded,
        registry_changes=validated_changes,
        correction_cause=impact.cause,
        registry_ledger_release_id=(
            registry_ledger.ledger_release_id if registry_ledger is not None else None
        ),
        late_source_ledger_release_id=None,
        alias_state_ledger_release_id=alias_ledger.ledger_release_id,
        unaffected_partition_receipts_digest=_unaffected_partition_receipts_digest(
            checkpoint,
            replacement_sessions,
        ),
        authorization_id=authorization.authorization.authorization_id,
        approval_attestation=attestation,
        source_binding_digest=source_binding,
        change_set_digest=change_set,
        identity_policy_before_id=prior_policy.policy_bundle.identity_policy_bundle_id,
        identity_policy_after_id=target_policy.policy_bundle.identity_policy_bundle_id,
        _seal=_PRODUCTION_CAPABILITY_SEAL,
    )


def validate_canonical_row_correction(
    old: CanonicalIdentityProjection,
    new: CanonicalIdentityProjection,
    *,
    registry_changes: tuple[RegistryChange, ...],
    target_policy_snapshot: IdentityPolicySnapshot,
) -> None:
    """Validate one exact source-row projection without granting a release capability."""

    if not isinstance(old, CanonicalIdentityProjection) or not isinstance(
        new, CanonicalIdentityProjection
    ):
        raise I4CorrectionError("canonical row correction inputs are invalid")
    if old.source != new.source:
        raise I4CorrectionError("canonical row correction rewrote observed lineage")
    if old.alias_segment_id != new.alias_segment_id:
        raise I4CorrectionError("canonical correction rewrote immutable alias segment")
    if old.issuer_payload() != new.issuer_payload():
        raise I4CorrectionError("identity correction exceeded issuer authority")
    if old.source.provider_locale != "us":
        if old.canonical_payload() != new.canonical_payload():
            raise I4CorrectionError("US correction rewrote a legal foreign-locale identity")
        return
    if type(registry_changes) is not tuple or any(
        not isinstance(change, RegistryChange) for change in registry_changes
    ):
        raise I4CorrectionError("canonical row registry changes are invalid")
    kinds = frozenset(
        change.registry_kind
        for change in registry_changes
        if _change_affects_source(change, old.source)
    )
    _validate_direct_row_change(
        old,
        new,
        registry_kinds=kinds,
        target_policy_snapshot=target_policy_snapshot,
    )


def build_bounded_correction_plan(
    *,
    checkpoint: I3CheckpointState,
    scope: BoundedCorrectionScope,
    parent_partition_images: tuple[SessionPartitionImage, ...],
    replacement_partition_images: tuple[SessionPartitionImage, ...],
    registry_changes: tuple[RegistryChange, ...],
    prior_policy_snapshot: IdentityPolicySnapshot,
    target_policy_snapshot: IdentityPolicySnapshot,
    added_row_version_receipts: tuple[RowVersionReceipt, ...],
    superseded_row_version_ids: tuple[str, ...],
    authorization: PinnedCorrectionAuthorization,
    approval_attestation: I4ApprovalEventAttestation,
    expectations: I4AuthorizationExpectations,
) -> I4CorrectionPlan:
    """Validate and freeze one pure correction plan.

    The returned object is fixture-safe evidence only.  It performs no write
    and intentionally exposes no publication or registry-mutation capability.
    """

    if not isinstance(checkpoint, I3CheckpointState):
        raise I4CorrectionError("correction parent must be an I3 checkpoint")
    if not isinstance(scope, BoundedCorrectionScope):
        raise I4CorrectionError("bounded correction scope is invalid")
    if not isinstance(expectations, I4AuthorizationExpectations):
        raise I4CorrectionError("correction authorization expectations are invalid")
    if not isinstance(prior_policy_snapshot, IdentityPolicySnapshot) or not isinstance(
        target_policy_snapshot, IdentityPolicySnapshot
    ):
        raise I4CorrectionError("correction policies must be sealed I3 snapshots")
    if prior_policy_snapshot.policy_bundle.identity_policy_bundle_id != (
        expectations.identity_policy_before_id
    ):
        raise I4CorrectionError("prior policy snapshot differs from correction parent")
    if target_policy_snapshot.policy_bundle.identity_policy_bundle_id != (
        expectations.identity_policy_after_id
    ):
        raise I4CorrectionError("target policy snapshot differs from correction target")
    if any(
        snapshot.policy_bundle.bundle_available_session > scope.availability_cutoff_session
        for snapshot in (prior_policy_snapshot, target_policy_snapshot)
    ):
        raise I4CorrectionError("identity policy snapshot was unavailable at correction cutoff")
    if scope.authorization_id != authorization.authorization.authorization_id:
        raise I4CorrectionError("scope and correction authorization differ")
    if checkpoint.parent_release.release_id != authorization.authorization.parent_release_id:
        raise I4CorrectionError("authorization does not bind the checkpoint parent")
    if checkpoint.identity_policy_bundle.identity_policy_bundle_id != (
        expectations.identity_policy_before_id
    ):
        raise I4CorrectionError("checkpoint identity policy differs from correction prior")
    if expectations.identity_policy_before_id == expectations.identity_policy_after_id:
        raise I4CorrectionError("registry correction requires a distinct target policy")
    if checkpoint.schema_digest != expectations.schema_digest:
        raise I4CorrectionError("correction changed the native-v2 schema")
    if checkpoint.transform_semantics_digest != expectations.transform_semantics_digest:
        raise I4CorrectionError("correction changed transform semantics")
    if checkpoint.calendar_digest != expectations.calendar_digest:
        raise I4CorrectionError("correction changed the exact calendar")
    if checkpoint.availability_cutoff_session > scope.availability_cutoff_session:
        raise I4CorrectionError("correction availability precedes parent checkpoint")

    changes = _validate_registry_changes(
        scope,
        registry_changes,
        prior_policy_snapshot.decisions,
        target_policy_snapshot.decisions,
    )
    replacements = _validate_partition_images(
        checkpoint,
        scope,
        parent_partition_images,
        replacement_partition_images,
        changes,
        target_policy_snapshot,
    )
    row_receipts, superseded = _validate_row_versions(
        added_row_version_receipts,
        superseded_row_version_ids,
        availability_cutoff_session=scope.availability_cutoff_session,
        authorization_available_session=scope.authorization_available_session,
    )
    change_set = logical_change_set_digest(
        added_partition_receipts=(),
        partition_replacements=replacements,
        added_row_version_receipts=row_receipts,
        superseded_row_version_ids=superseded,
    )
    if change_set != expectations.change_set_digest:
        raise I4CorrectionError("exact logical correction change set differs from expectation")
    gate_scope = correction_scope_digest(
        parent_release_id=checkpoint.parent_release.release_id,
        change_set_digest=change_set,
    )
    try:
        validate_correction_authorization(
            authorization,
            parent_release_id=checkpoint.parent_release.release_id,
            change_set_digest=change_set,
            source_binding_digest=expectations.source_binding_digest,
            schema_digest=expectations.schema_digest,
            transform_semantics_digest=expectations.transform_semantics_digest,
            calendar_digest=expectations.calendar_digest,
            identity_policy_before_id=expectations.identity_policy_before_id,
            identity_policy_after_id=expectations.identity_policy_after_id,
            scope_digest=gate_scope,
            availability_cutoff_session=scope.availability_cutoff_session,
        )
    except (IncrementalGateError, ValueError) as exc:
        raise I4CorrectionError("Gate-A correction authorization validation failed") from exc
    approval_attestation.validate(
        authorization,
        availability_cutoff_session=scope.availability_cutoff_session,
    )
    return I4CorrectionPlan(
        parent_checkpoint_id=checkpoint.checkpoint_id,
        parent_release_id=checkpoint.parent_release.release_id,
        exact_scope=scope,
        partition_replacements=replacements,
        added_row_version_receipts=row_receipts,
        superseded_row_version_ids=superseded,
        registry_changes=changes,
        authorization_id=authorization.authorization.authorization_id,
        approval_attestation=approval_attestation,
        change_set_digest=change_set,
        identity_policy_before_id=expectations.identity_policy_before_id,
        identity_policy_after_id=expectations.identity_policy_after_id,
        _seal=_PLAN_SEAL,
    )


@dataclass(frozen=True, slots=True)
class _DecodedPartition:
    image: SessionPartitionImage
    raw_rows: tuple[Mapping[str, object], ...]

    @property
    def session_date(self) -> date:
        return self.image.session_date

    @property
    def raw_by_key(self) -> dict[tuple[date, str], Mapping[str, object]]:
        return {
            projection.row_key: raw
            for projection, raw in zip(self.image.rows, self.raw_rows, strict=True)
        }


@dataclass(frozen=True, slots=True)
class _LateSourceSessionChange:
    session_date: date
    parent_snapshot: I4LateSourceSnapshot
    corrected_snapshot: I4LateSourceSnapshot


@dataclass(frozen=True, slots=True)
class _ProductionImpact:
    cause: I4ProductionCorrectionCause
    group: ExactIdentityGroup
    affected_sessions: tuple[date, ...]
    registry_source_scope: tuple[RegistrySourceScopeRow, ...] = ()
    late_source_changes: tuple[_LateSourceSessionChange, ...] = ()


class _ExactReadCache:
    """One-call exact reader that rejects path aliasing across different pins."""

    def __init__(self) -> None:
        self._by_path: dict[str, tuple[ArtifactPin, bytes]] = {}

    def read(self, pin: ArtifactPin, reader: ExactArtifactReader) -> bytes:
        if not isinstance(pin, ArtifactPin):
            raise I4CorrectionError("exact reader pin is invalid")
        if not callable(reader):
            raise I4CorrectionError("exact artifact reader must be callable")
        prior = self._by_path.get(pin.path)
        if prior is not None:
            prior_pin, content = prior
            if prior_pin != pin:
                raise I4CorrectionError(
                    "one exact artifact path was presented with conflicting pins"
                )
            return content
        content = reader(pin.path)
        if type(content) is not bytes:
            raise I4CorrectionError("exact artifact reader must return bytes")
        if len(content) != pin.bytes or hashlib.sha256(content).hexdigest() != pin.sha256:
            raise I4CorrectionError("stored artifact bytes differ from their exact pin")
        self._by_path[pin.path] = (pin, content)
        return content


def _authenticate_production_parent(
    *,
    parent_manifest: IncrementalReleaseManifest,
    parent_manifest_pin: ManifestPin,
    parent_run_receipt: RunReceipt,
    checkpoint: I3CheckpointState,
    parent_checkpoint_artifact: ArtifactPin,
    parent_correction_authorization: PinnedCorrectionAuthorization | None,
    parent_correction_approval_attestation: I4ApprovalEventAttestation | None,
    artifact_reader: ExactArtifactReader,
    cache: _ExactReadCache,
) -> None:
    if not isinstance(parent_manifest, IncrementalReleaseManifest) or not isinstance(
        parent_manifest_pin, ManifestPin
    ):
        raise I4CorrectionError("production correction parent manifest is invalid")
    if parent_manifest.release_type not in {
        ReleaseType.BASE,
        ReleaseType.DELTA,
        ReleaseType.CORRECTION,
    }:
        raise I4CorrectionError("production I4 parent release type is invalid")
    is_correction_parent = parent_manifest.release_type is ReleaseType.CORRECTION
    if is_correction_parent != (
        parent_correction_authorization is not None
        and parent_correction_approval_attestation is not None
    ):
        raise I4CorrectionError(
            "correction parent requires its exact authorization and approval attestation"
        )
    if not is_correction_parent and (
        parent_correction_authorization is not None
        or parent_correction_approval_attestation is not None
    ):
        raise I4CorrectionError("clean parent cannot carry correction-parent approval evidence")
    manifest_pin = ArtifactPin(
        path=parent_manifest_pin.manifest_path,
        sha256=parent_manifest_pin.manifest_sha256,
        bytes=parent_manifest_pin.manifest_bytes,
    )
    manifest_content = cache.read(manifest_pin, artifact_reader)
    if (
        manifest_content != parent_manifest.canonical_bytes()
        or parent_manifest.exact_pin(manifest_path=parent_manifest_pin.manifest_path)
        != parent_manifest_pin
    ):
        raise I4CorrectionError("parent manifest exact bytes do not reproduce")
    if not isinstance(parent_run_receipt, RunReceipt) or not parent_run_receipt.succeeded:
        raise I4CorrectionError("production parent requires a successful exact RunReceipt")
    receipt_content = cache.read(parent_manifest.run_receipt_pin.artifact, artifact_reader)
    if receipt_content != _canonical_json_bytes(parent_run_receipt.to_dict()):
        raise I4CorrectionError("parent RunReceipt stored bytes do not reproduce")
    if (
        parent_manifest.run_receipt_pin.object_id != parent_run_receipt.run_receipt_id
        or parent_manifest.run_spec_pin.object_id != parent_run_receipt.run_spec_id
    ):
        raise I4CorrectionError("parent manifest control pins differ from its RunReceipt")
    parent_change_set = logical_change_set_digest(
        added_partition_receipts=parent_manifest.added_partition_receipts,
        partition_replacements=parent_manifest.partition_replacements,
        added_row_version_receipts=parent_manifest.added_row_version_receipts,
        superseded_row_version_ids=parent_manifest.superseded_row_version_ids,
    )
    if is_correction_parent:
        assert parent_correction_authorization is not None
        assert parent_correction_approval_attestation is not None
        if parent_manifest.parent_release_pin is None:  # constructor proves; defensive
            raise I4CorrectionError("correction parent lost its exact predecessor pin")
        parent_authorization_artifact = ArtifactPin(
            path=parent_correction_authorization.artifact.path,
            sha256=parent_correction_authorization.artifact.sha256,
            bytes=parent_correction_authorization.artifact.bytes,
        )
        authorization_bytes = cache.read(parent_authorization_artifact, artifact_reader)
        if authorization_bytes != _canonical_json_bytes(
            parent_correction_authorization.authorization.to_dict()
        ):
            raise I4CorrectionError("correction-parent authorization bytes do not reproduce")
        if parent_manifest.correction_authorization_id != (
            parent_correction_authorization.authorization.authorization_id
        ):
            raise I4CorrectionError("correction parent manifest binds another authorization")
        parent_gate_scope = correction_scope_digest(
            parent_release_id=parent_manifest.parent_release_pin.release_id,
            change_set_digest=parent_change_set,
        )
        try:
            validate_correction_authorization(
                parent_correction_authorization,
                parent_release_id=parent_manifest.parent_release_pin.release_id,
                change_set_digest=parent_change_set,
                source_binding_digest=parent_manifest.source_binding_digest,
                schema_digest=parent_manifest.schema_digest,
                transform_semantics_digest=parent_manifest.transform_semantics_digest,
                calendar_digest=parent_manifest.calendar_digest,
                identity_policy_before_id=(
                    parent_correction_authorization.authorization.identity_policy_before_id
                ),
                identity_policy_after_id=parent_manifest.identity_policy_bundle_id,
                scope_digest=parent_gate_scope,
                availability_cutoff_session=parent_manifest.availability_cutoff_session,
            )
            parent_correction_approval_attestation.validate(
                parent_correction_authorization,
                availability_cutoff_session=parent_manifest.availability_cutoff_session,
            )
        except (IncrementalGateError, ValueError) as exc:
            raise I4CorrectionError(
                "correction parent approval evidence does not reproduce"
            ) from exc
    qa_receipt = parent_run_receipt.qa_receipt
    if (
        parent_run_receipt.actual_input_set_digest != parent_manifest.source_binding_digest
        or parent_run_receipt.output_set_digest != parent_change_set
        or qa_receipt is None
        or qa_receipt.qa_receipt_id != parent_manifest.qa_receipt_id
        or qa_receipt.source_binding_digest != parent_manifest.source_binding_digest
        or qa_receipt.change_set_digest != parent_change_set
    ):
        raise I4CorrectionError("parent manifest and RunReceipt change projection differ")
    if not isinstance(checkpoint, I3CheckpointState) or not isinstance(
        parent_checkpoint_artifact, ArtifactPin
    ):
        raise I4CorrectionError("production parent checkpoint is invalid")
    checkpoint_content = cache.read(parent_checkpoint_artifact, artifact_reader)
    try:
        parsed_checkpoint = I3CheckpointState.from_dict(
            json.loads(checkpoint_content.decode("utf-8"))
        )
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
        raise I4CorrectionError(
            "parent checkpoint bytes are not a canonical I3 checkpoint"
        ) from exc
    if (
        checkpoint_content != checkpoint.canonical_bytes()
        or parsed_checkpoint.to_dict() != checkpoint.to_dict()
    ):
        raise I4CorrectionError("parent checkpoint stored bytes do not reproduce")
    if checkpoint.parent_release.release_family != NATIVE_V2_RELEASE_FAMILY:
        raise I4CorrectionError("fixture native-v2 checkpoint cannot mint production I4 authority")
    native_content = cache.read(checkpoint.parent_release.manifest, artifact_reader)
    try:
        native_manifest = NativeV2ReleaseManifest.from_dict(
            json.loads(native_content.decode("utf-8"))
        )
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
        raise I4CorrectionError("native-v2 parent manifest bytes are invalid") from exc
    if (
        native_content != native_manifest.canonical_bytes()
        or NativeV2ParentReleasePin.from_manifest(
            native_manifest,
            path=checkpoint.parent_release.manifest.path,
        )
        != checkpoint.parent_release
    ):
        raise I4CorrectionError("checkpoint native-v2 parent bytes do not reproduce")
    receipt_checkpoint = parent_run_receipt.checkpoint
    if receipt_checkpoint is None:
        raise I4CorrectionError("successful parent RunReceipt lost its checkpoint receipt")
    if (
        receipt_checkpoint.artifact != parent_checkpoint_artifact
        or receipt_checkpoint.parent_release_id
        != (
            parent_manifest.parent_release_pin.release_id
            if parent_manifest.parent_release_pin is not None
            else None
        )
        or receipt_checkpoint.last_session != checkpoint.last_session
        or receipt_checkpoint.resolved_content_digest != checkpoint.resolved_state_digest
        or receipt_checkpoint.rebuild_basis_digest != checkpoint.rebuild_basis_digest
    ):
        raise I4CorrectionError("parent RunReceipt does not bind the exact I3 checkpoint")
    expected = (
        (parent_manifest.schema_digest, checkpoint.schema_digest, "schema"),
        (
            parent_manifest.transform_semantics_digest,
            checkpoint.transform_semantics_digest,
            "transform semantics",
        ),
        (parent_manifest.calendar_digest, checkpoint.calendar_digest, "calendar"),
        (
            parent_manifest.identity_policy_bundle_id,
            checkpoint.identity_policy_bundle.identity_policy_bundle_id,
            "identity policy",
        ),
        (
            parent_manifest.source_cutoff_session,
            checkpoint.source_cutoff_session,
            "source cutoff",
        ),
        (
            parent_manifest.availability_cutoff_session,
            checkpoint.availability_cutoff_session,
            "availability cutoff",
        ),
        (
            parent_manifest.resolved_content_digest,
            checkpoint.resolved_state_digest,
            "resolved state",
        ),
    )
    for observed, required, label in expected:
        if observed != required:
            raise I4CorrectionError(f"parent manifest and checkpoint {label} differ")
    frontier_by_session = {item.session_date: item for item in checkpoint.resolved_partition_map}
    for receipt in parent_manifest.added_partition_receipts:
        frontier = frontier_by_session.get(date.fromisoformat(receipt.partition_key))
        if frontier is None or (
            receipt.receipt != frontier.artifact
            or receipt.row_count != frontier.row_count
            or receipt.availability_session != frontier.availability_session
        ):
            raise I4CorrectionError(
                "parent manifest partition change differs from checkpoint frontier"
            )


def _authenticate_production_policies(
    *,
    checkpoint: I3CheckpointState,
    prior_policy_snapshot: IdentityPolicySnapshot,
    target_policy_snapshot: IdentityPolicySnapshot,
    target_policy_bundle_artifact: ArtifactPin,
    availability_cutoff_session: date,
    require_distinct_target: bool,
    artifact_reader: ExactArtifactReader,
    cache: _ExactReadCache,
) -> tuple[IdentityPolicySnapshot, IdentityPolicySnapshot]:
    try:
        prior = _verify_policy_snapshot(prior_policy_snapshot)
        target = _verify_policy_snapshot(target_policy_snapshot)
    except ValueError as exc:
        raise I4CorrectionError("production identity policy snapshot does not reproduce") from exc
    if (
        prior.policy_source != I3_PRODUCTION_POLICY_SOURCE
        or target.policy_source != I3_PRODUCTION_POLICY_SOURCE
        or prior.production_release_set_binding_digest is None
        or target.production_release_set_binding_digest is None
    ):
        raise I4CorrectionError("production I4 requires production-loaded policy snapshots")
    if prior.policy_bundle != checkpoint.identity_policy_bundle:
        raise I4CorrectionError("prior production policy differs from the parent checkpoint")
    if require_distinct_target and prior.policy_bundle.identity_policy_bundle_id == (
        target.policy_bundle.identity_policy_bundle_id
    ):
        raise I4CorrectionError("registry correction requires a distinct target policy")
    if not require_distinct_target and prior.to_dict() != target.to_dict():
        raise I4CorrectionError("late-source-only correction cannot change identity policy")
    prior_content = cache.read(checkpoint.identity_policy_bundle_artifact, artifact_reader)
    if prior_content != prior.policy_bundle.canonical_bytes():
        raise I4CorrectionError("prior policy bundle exact bytes differ")
    target_content = cache.read(target_policy_bundle_artifact, artifact_reader)
    if target_content != target.policy_bundle.canonical_bytes():
        raise I4CorrectionError("target policy bundle exact bytes differ")
    for bundle in (prior.policy_bundle, target.policy_bundle):
        if bundle.bundle_available_session > availability_cutoff_session:
            raise I4CorrectionError("identity policy bundle was unavailable at correction cutoff")
        for release in bundle.registry_releases:
            cache.read(release.artifact, artifact_reader)
            if release.release_available_session > availability_cutoff_session:
                raise I4CorrectionError(
                    "identity registry release was unavailable at correction cutoff"
                )
    return prior, target


def _validate_parent_manifest_partition_receipts(
    parent_manifest: IncrementalReleaseManifest,
    parent_partitions: tuple[_DecodedPartition, ...],
) -> None:
    """Reproduce parent-manifest receipts only for the bounded read window."""

    decoded_by_session = {item.session_date: item.image.receipt for item in parent_partitions}
    manifest_receipts = parent_manifest.added_partition_receipts + tuple(
        item.replacement_receipt for item in parent_manifest.partition_replacements
    )
    for receipt in manifest_receipts:
        session = date.fromisoformat(receipt.partition_key)
        decoded = decoded_by_session.get(session)
        if decoded is not None and decoded != receipt:
            raise I4CorrectionError(
                "parent manifest partition receipt differs from actual native-v2 bytes"
            )


def _read_parent_partitions_exact(
    checkpoint: I3CheckpointState,
    *,
    sessions: tuple[date, ...],
    artifact_reader: ExactArtifactReader,
    cache: _ExactReadCache,
) -> tuple[_DecodedPartition, ...]:
    if sessions != tuple(sorted(set(sessions))) or not sessions:
        raise I4CorrectionError("bounded parent partition sessions must be sorted and unique")
    frontier_by_session = {item.session_date: item for item in checkpoint.resolved_partition_map}
    decoded: list[_DecodedPartition] = []
    for session in sessions:
        frontier = frontier_by_session.get(session)
        if frontier is None:
            raise I4CorrectionError("bounded correction session is absent from parent checkpoint")
        content = cache.read(frontier.artifact, artifact_reader)
        decoded.append(
            _decode_native_v2_partition(
                content,
                session_date=frontier.session_date,
                artifact=frontier.artifact,
                row_count=frontier.row_count,
                availability_session=frontier.availability_session,
                supplied_receipt=None,
            )
        )
    return tuple(decoded)


def _read_replacement_partitions_exact(
    receipts: tuple[PartitionReceipt, ...],
    *,
    artifact_reader: ExactArtifactReader,
    cache: _ExactReadCache,
) -> tuple[_DecodedPartition, ...]:
    if (
        type(receipts) is not tuple
        or not receipts
        or any(not isinstance(item, PartitionReceipt) for item in receipts)
    ):
        raise I4CorrectionError("production correction requires replacement receipts")
    sessions = tuple(date.fromisoformat(item.partition_key) for item in receipts)
    if sessions != tuple(sorted(set(sessions))):
        raise I4CorrectionError("replacement receipts must be sorted and unique")
    result = []
    for receipt in receipts:
        content = cache.read(receipt.receipt, artifact_reader)
        result.append(
            _decode_native_v2_partition(
                content,
                session_date=date.fromisoformat(receipt.partition_key),
                artifact=receipt.receipt,
                row_count=receipt.row_count,
                availability_session=receipt.availability_session,
                supplied_receipt=receipt,
            )
        )
    return tuple(result)


def _derive_registry_impact(
    changes: tuple[RegistryChange, ...],
) -> _ProductionImpact:
    if not changes:
        raise I4CorrectionError("production registry correction has no authenticated change")
    groups = {change.group() for change in changes}
    if len(groups) != 1:
        raise I4CorrectionError("registry ledger changes crossed exact provider/ticker groups")
    source_scope = _expected_registry_source_scope(changes)
    sessions = tuple(sorted({item.session_date for item in source_scope}))
    if not sessions:
        raise I4CorrectionError("registry decisions have no authenticated source-scope sessions")
    return _ProductionImpact(
        cause=I4ProductionCorrectionCause.REGISTRY_CHANGE,
        group=next(iter(groups)),
        affected_sessions=sessions,
        registry_source_scope=source_scope,
    )


def _read_alias_state_ledger_exact(
    *,
    ledger: I4AliasStateLedgerRelease,
    artifact: ArtifactPin,
    availability_cutoff_session: date,
    artifact_reader: ExactArtifactReader,
    cache: _ExactReadCache,
) -> I4AliasStateLedgerRelease:
    if not isinstance(ledger, I4AliasStateLedgerRelease) or not isinstance(artifact, ArtifactPin):
        raise I4CorrectionError("alias-state ledger exact inputs are invalid")
    content = cache.read(artifact, artifact_reader)
    if content != ledger.canonical_bytes():
        raise I4CorrectionError("alias-state ledger stored bytes do not reproduce")
    if ledger.release_available_session > availability_cutoff_session:
        raise I4CorrectionError("alias-state ledger was unavailable at correction cutoff")
    return ledger


def _read_late_source_change_exact(
    *,
    ledger: I4LateSourceChangeLedgerRelease,
    artifact: ArtifactPin,
    availability_cutoff_session: date,
    artifact_reader: ExactArtifactReader,
    cache: _ExactReadCache,
) -> tuple[I4LateSourceChangeLedgerRelease, _ProductionImpact]:
    if not isinstance(ledger, I4LateSourceChangeLedgerRelease) or not isinstance(
        artifact, ArtifactPin
    ):
        raise I4CorrectionError("late-source ledger exact inputs are invalid")
    content = cache.read(artifact, artifact_reader)
    if content != ledger.canonical_bytes():
        raise I4CorrectionError("late-source ledger stored bytes do not reproduce")
    if ledger.release_available_session > availability_cutoff_session:
        raise I4CorrectionError("late-source ledger was unavailable at correction cutoff")

    changes: list[_LateSourceSessionChange] = []
    changed_groups: set[tuple[str, str, str, str]] = set()
    for entry in ledger.entries:
        if entry.change_available_session > availability_cutoff_session:
            raise I4CorrectionError("late-source change was unavailable at correction cutoff")
        snapshots: list[I4LateSourceSnapshot] = []
        for pin in (entry.parent_snapshot_artifact, entry.corrected_snapshot_artifact):
            snapshot_content = cache.read(pin, artifact_reader)
            try:
                parsed = I4LateSourceSnapshot.from_dict(
                    json.loads(snapshot_content.decode("utf-8"))
                )
            except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
                raise I4CorrectionError("late-source snapshot bytes are invalid") from exc
            if snapshot_content != parsed.canonical_bytes():
                raise I4CorrectionError("late-source snapshot is not canonical exact bytes")
            if parsed.session_date != entry.session_date:
                raise I4CorrectionError("late-source snapshot crossed its ledger session")
            if parsed.source_available_session > entry.change_available_session:
                raise I4CorrectionError("late-source snapshot availability exceeds its change")
            snapshots.append(parsed)
        parent, corrected = snapshots
        if parent.source_release_id == corrected.source_release_id:
            raise I4CorrectionError("late-source change reused one source release ID")
        parent_by_group = _source_rows_by_group(parent.rows)
        corrected_by_group = _source_rows_by_group(corrected.rows)
        entry_changed = {
            key
            for key in set(parent_by_group) | set(corrected_by_group)
            if tuple(item.to_dict() for item in parent_by_group.get(key, ()))
            != tuple(item.to_dict() for item in corrected_by_group.get(key, ()))
        }
        if not entry_changed:
            raise I4CorrectionError("late-source ledger entry has no exact source diff")
        changed_groups.update(entry_changed)
        changes.append(
            _LateSourceSessionChange(
                session_date=entry.session_date,
                parent_snapshot=parent,
                corrected_snapshot=corrected,
            )
        )
    if len(changed_groups) != 1:
        raise I4CorrectionError("late-source evidence crossed exact provider/ticker groups")
    provider_id, provider_market, provider_locale, ticker = next(iter(changed_groups))
    group = ExactIdentityGroup(
        provider_id=provider_id,
        provider_market=provider_market,
        provider_locale=provider_locale,
        ticker=ticker,
    )
    sessions = tuple(item.session_date for item in changes)
    return ledger, _ProductionImpact(
        cause=I4ProductionCorrectionCause.LATE_SOURCE,
        group=group,
        affected_sessions=sessions,
        late_source_changes=tuple(changes),
    )


def _source_rows_by_group(
    rows: tuple[SourceIdentityKey, ...],
) -> dict[tuple[str, str, str, str], tuple[SourceIdentityKey, ...]]:
    grouped: dict[tuple[str, str, str, str], list[SourceIdentityKey]] = {}
    for row in rows:
        key = (row.provider_id, row.provider_market, row.provider_locale, row.ticker)
        grouped.setdefault(key, []).append(row)
    return {key: tuple(value) for key, value in grouped.items()}


def _bounded_replacement_sessions(
    *,
    checkpoint: I3CheckpointState,
    replacement_partition_receipts: tuple[PartitionReceipt, ...],
    impact: _ProductionImpact,
) -> tuple[date, ...]:
    if (
        type(replacement_partition_receipts) is not tuple
        or not replacement_partition_receipts
        or any(not isinstance(item, PartitionReceipt) for item in replacement_partition_receipts)
    ):
        raise I4CorrectionError("production correction requires replacement receipts")
    sessions = tuple(
        date.fromisoformat(item.partition_key) for item in replacement_partition_receipts
    )
    if sessions != tuple(sorted(set(sessions))):
        raise I4CorrectionError("replacement receipts must be sorted and unique")
    calendar = tuple(item.session_date for item in checkpoint.resolved_partition_map)
    earliest = min(impact.affected_sessions)
    try:
        earliest_index = calendar.index(earliest)
    except ValueError as exc:
        raise I4CorrectionError("earliest affected session is absent from checkpoint") from exc
    if sessions != calendar[earliest_index : earliest_index + len(sessions)]:
        raise I4CorrectionError(
            "replacement receipts must be contiguous from authenticated earliest session"
        )
    if not set(impact.affected_sessions).issubset(sessions):
        raise I4CorrectionError("replacement receipts omit an authenticated affected session")
    return sessions


def _unaffected_partition_receipts_digest(
    checkpoint: I3CheckpointState,
    replaced_sessions: tuple[date, ...],
) -> str:
    replaced = set(replaced_sessions)
    return stable_digest(
        {
            "parent_checkpoint_id": checkpoint.checkpoint_id,
            "unchanged_partition_frontier": [
                item.to_dict()
                for item in checkpoint.resolved_partition_map
                if item.session_date not in replaced
            ],
        }
    )


def _decode_native_v2_partition(
    content: bytes,
    *,
    session_date: date,
    artifact: ArtifactPin,
    row_count: int,
    availability_session: date,
    supplied_receipt: PartitionReceipt | None,
) -> _DecodedPartition:
    contract = I3_V2_CONTRACTS["universe_daily"]
    try:
        parquet = pq.ParquetFile(pa.BufferReader(content))
        table = parquet.read()
    except (OSError, pa.ArrowException) as exc:
        raise I4CorrectionError("I4 exact partition bytes are not readable Parquet") from exc
    if parquet.metadata.num_rows != row_count or not table.schema.equals(contract.arrow_schema):
        raise I4CorrectionError("I4 partition row count or native-v2 schema differs")
    rows = tuple(table.to_pylist())
    physical_keys = tuple((row.get("session_date"), row.get("ticker")) for row in rows)
    if physical_keys != tuple(sorted(set(physical_keys))):
        raise I4CorrectionError("I4 partition physical rows are not sorted and unique")
    if any(row.get("session_date") != session_date for row in rows):
        raise I4CorrectionError("I4 partition contains rows from another session")
    projections = tuple(
        sorted(
            (_projection_from_native_row(row) for row in rows),
            key=lambda item: item.row_key,
        )
    )
    raw_by_source = {str(row["selected_source_record_id"]): row for row in rows}
    if len(raw_by_source) != len(rows):
        raise I4CorrectionError("I4 partition repeats a selected source record")
    ordered_raw = tuple(raw_by_source[item.source.source_record_id] for item in projections)
    references = _partition_row_version_references(rows)
    receipt = supplied_receipt or PartitionReceipt(
        table_name="universe_daily",
        partition_key=session_date.isoformat(),
        receipt=artifact,
        row_count=row_count,
        schema_digest=contract.schema_digest,
        availability_session=availability_session,
        row_version_references=references,
    )
    if (
        receipt.receipt != artifact
        or receipt.schema_digest != contract.schema_digest
        or receipt.row_version_references != references
    ):
        raise I4CorrectionError(
            "partition receipt does not reproduce from actual native-v2 row bytes"
        )
    return _DecodedPartition(
        image=SessionPartitionImage(receipt=receipt, rows=projections),
        raw_rows=ordered_raw,
    )


def _projection_from_native_row(row: Mapping[str, object]) -> CanonicalIdentityProjection:
    session = row.get("session_date")
    if type(session) is not date:
        raise I4CorrectionError("native-v2 membership session is invalid")
    source = SourceIdentityKey(
        provider_id="massive",
        provider_market="stocks",
        provider_locale="us",
        ticker=str(row.get("ticker")),
        session_date=session,
        source_record_id=str(row.get("selected_source_record_id")),
        observed_composite_figi=_none_or_text(row.get("observed_composite_figi")),
        observed_composite_country=_none_or_text(row.get("observed_composite_market_code")),
        observed_share_class_figi=_none_or_text(row.get("observed_share_class_figi")),
        active_on_date=row.get("active_on_date"),
    )
    composite_lineage = tuple(
        sorted(
            {
                value
                for key in (
                    "identity_adjudication_id",
                    "cross_market_adjudication_id",
                    "provider_composite_override_id",
                )
                if (value := row.get(key)) is not None
            }
        )
    )
    share_lineage = tuple(
        sorted({value for value in (row.get("share_class_adjudication_id"),) if value is not None})
    )
    if any(not isinstance(item, str) for item in (*composite_lineage, *share_lineage)):
        raise I4CorrectionError("native-v2 membership decision lineage is invalid")
    return CanonicalIdentityProjection(
        source=source,
        canonical_composite_figi=_none_or_text(row.get("canonical_composite_figi")),
        canonical_asset_id=_none_or_text(row.get("asset_id")),
        canonical_share_class_figi=_none_or_text(row.get("canonical_share_class_figi")),
        canonical_share_class_id=_none_or_text(row.get("share_class_id")),
        canonical_issuer_id=_none_or_text(row.get("issuer_id")),
        canonical_cik_normalized=_none_or_text(row.get("canonical_cik_normalized")),
        backtest_identity_eligible=row.get("backtest_identity_eligible"),
        resolution_method=str(row.get("identity_resolution_method")),
        resolution_status=str(row.get("identity_resolution_status")),
        disposition=str(row.get("identity_disposition")),
        share_class_resolution_method=(
            "approved_share_class_adjudication" if share_lineage else "direct_observed"
        ),
        decision_lineage_ids=composite_lineage,
        share_class_decision_lineage_ids=share_lineage,
        alias_segment_id=_none_or_text(row.get("alias_segment_id")),
        alias_resolution_version_id=_none_or_text(row.get("alias_resolution_version_id")),
    )


def _partition_row_version_references(
    rows: tuple[Mapping[str, object], ...],
) -> tuple[RowVersionReference, ...]:
    columns = (
        ("asset_master", "asset_master_version_id"),
        ("issuer_master", "issuer_master_version_id"),
        ("ticker_alias", "alias_resolution_version_id"),
    )
    references = {
        (table_name, str(value))
        for row in rows
        for table_name, column in columns
        if (value := row.get(column)) is not None
    }
    return tuple(
        RowVersionReference(table_name=table_name, row_version_id=row_version_id)
        for table_name, row_version_id in sorted(references)
    )


def _derive_registry_changes_exact(
    *,
    registry_ledger: I4RegistryChangeLedgerRelease,
    registry_ledger_artifact: ArtifactPin,
    prior_policy_snapshot: IdentityPolicySnapshot,
    target_policy_snapshot: IdentityPolicySnapshot,
    availability_cutoff_session: date,
    artifact_reader: ExactArtifactReader,
    cache: _ExactReadCache,
) -> tuple[RegistryChange, ...]:
    if not isinstance(registry_ledger, I4RegistryChangeLedgerRelease) or not isinstance(
        registry_ledger_artifact, ArtifactPin
    ):
        raise I4CorrectionError("registry change ledger exact inputs are invalid")
    ledger_content = cache.read(registry_ledger_artifact, artifact_reader)
    if ledger_content != registry_ledger.canonical_bytes():
        raise I4CorrectionError("registry change ledger stored bytes do not reproduce")
    if registry_ledger.release_available_session > availability_cutoff_session:
        raise I4CorrectionError("registry change ledger was unavailable at correction cutoff")
    prior_by_id = {item.decision_id: item for item in prior_policy_snapshot.decisions}
    target_by_id = {item.decision_id: item for item in target_policy_snapshot.decisions}
    prior_release_by_kind = {
        item.registry_kind: item for item in prior_policy_snapshot.policy_bundle.registry_releases
    }
    target_release_by_kind = {
        item.registry_kind: item for item in target_policy_snapshot.policy_bundle.registry_releases
    }
    changed_release_kinds = {
        kind
        for kind, prior_release in prior_release_by_kind.items()
        if target_release_by_kind[kind] != prior_release
    }
    ledger_release_kinds = {item.registry_kind for item in registry_ledger.entries}
    if changed_release_kinds != ledger_release_kinds:
        raise I4CorrectionError(
            "target policy registry-release changes differ from the exact change ledger"
        )
    result: list[RegistryChange] = []
    for entry in registry_ledger.entries:
        if entry.change_available_session > availability_cutoff_session:
            raise I4CorrectionError("registry ledger entry was unavailable at correction cutoff")
        predecessor = prior_by_id.get(entry.predecessor_decision_id)
        if predecessor is None or predecessor.registry_kind is not entry.registry_kind:
            raise I4CorrectionError("registry ledger predecessor is absent from prior policy")
        prior_release = prior_release_by_kind[entry.registry_kind]
        target_release = target_release_by_kind[entry.registry_kind]
        if (
            entry.predecessor_registry_release_id != prior_release.release_id
            or entry.predecessor_registry_release_artifact != prior_release.artifact
            or predecessor.registry_release_id != prior_release.release_id
        ):
            raise I4CorrectionError("registry ledger predecessor release exact pin differs")
        if (
            entry.change_registry_release_id != target_release.release_id
            or entry.change_registry_release_artifact != target_release.artifact
        ):
            raise I4CorrectionError("registry ledger target release exact pin differs")
        predecessor_bytes = cache.read(entry.predecessor_decision_artifact, artifact_reader)
        if predecessor_bytes != _canonical_json_bytes(predecessor.to_dict()):
            raise I4CorrectionError("registry predecessor decision artifact differs")
        cache.read(entry.predecessor_registry_release_artifact, artifact_reader)
        cache.read(entry.change_registry_release_artifact, artifact_reader)

        successor: RegistryDecision | None
        if entry.operation is RegistryChangeOperation.SUCCESSOR:
            successor = target_by_id.get(entry.change_decision_id)
            if successor is None or successor.registry_kind is not entry.registry_kind:
                raise I4CorrectionError("registry successor decision is absent from target policy")
            if successor.registry_release_id != entry.change_registry_release_id:
                raise I4CorrectionError("registry successor belongs to another exact release")
            successor_bytes = cache.read(entry.change_decision_artifact, artifact_reader)
            if successor_bytes != _canonical_json_bytes(successor.to_dict()):
                raise I4CorrectionError("registry successor decision artifact differs")
        else:
            successor = None
            if entry.predecessor_decision_id in target_by_id:
                raise I4CorrectionError("withdrawn registry predecessor remains in target policy")
            if entry.change_decision_id in target_by_id:
                raise I4CorrectionError(
                    "withdrawal control decision cannot become an effective policy decision"
                )
            withdrawal_bytes = cache.read(entry.change_decision_artifact, artifact_reader)
            if withdrawal_bytes != entry.withdrawal_decision_bytes():
                raise I4CorrectionError("registry withdrawal decision artifact differs")
            reason_artifact = entry.withdrawal_reason_artifact
            if reason_artifact is None:  # constructor already proves; defensive
                raise I4CorrectionError("registry withdrawal lost its reason artifact")
            reason_bytes = cache.read(reason_artifact, artifact_reader)
            if not reason_bytes.strip():
                raise I4CorrectionError("registry withdrawal reason artifact is empty")
        result.append(
            RegistryChange(
                operation=entry.operation,
                predecessor=predecessor,
                successor=successor,
                change_available_session=entry.change_available_session,
            )
        )
    return tuple(
        sorted(
            result,
            key=lambda item: (
                item.predecessor.registry_kind.value,
                item.predecessor.decision_id,
            ),
        )
    )


def _derive_production_scope(
    *,
    checkpoint: I3CheckpointState,
    parent_partitions: tuple[_DecodedPartition, ...],
    replacement_partitions: tuple[_DecodedPartition, ...],
    impact: _ProductionImpact,
    registry_changes: tuple[RegistryChange, ...],
    prior_policy_snapshot: IdentityPolicySnapshot,
    target_policy_snapshot: IdentityPolicySnapshot,
    alias_state_ledger: I4AliasStateLedgerRelease,
    authorization: PinnedCorrectionAuthorization,
    availability_cutoff_session: date,
) -> BoundedCorrectionScope:
    if not isinstance(impact, _ProductionImpact):
        raise I4CorrectionError("production correction impact is invalid")
    group = impact.group
    parent_rows = {
        row.row_key: row for partition in parent_partitions for row in partition.image.rows
    }
    parent_raw_rows = {
        (partition.session_date, str(row["selected_source_record_id"])): row
        for partition in parent_partitions
        for row in partition.raw_rows
    }
    parent_by_session = {item.session_date: item for item in parent_partitions}
    replacement_by_session = {item.session_date: item for item in replacement_partitions}
    if impact.cause is I4ProductionCorrectionCause.REGISTRY_CHANGE:
        expected_scope_rows = impact.registry_source_scope
        expected_keys = {(item.session_date, item.source_record_id) for item in expected_scope_rows}
        if len(expected_keys) != len(expected_scope_rows):
            raise I4CorrectionError("registry ledger repeats an exact source-scope row")
        missing = expected_keys - set(parent_rows)
        if missing:
            raise I4CorrectionError(
                "authenticated parent partitions omitted a registry source-scope row"
            )
        for expected in expected_scope_rows:
            observed = parent_rows[(expected.session_date, expected.source_record_id)].source
            raw = parent_raw_rows[(expected.session_date, expected.source_record_id)]
            if (
                observed.provider_id != expected.provider_id
                or observed.provider_market != expected.provider_market
                or observed.provider_locale != expected.provider_locale
                or observed.ticker != expected.ticker
                or observed.observed_composite_figi != expected.observed_composite_figi
                or observed.observed_share_class_figi != expected.observed_share_class_figi
                or expected.source_dataset != "universe_source_daily"
                or raw.get("source_s4_release_set_id") != expected.source_s4_release_set_id
                or raw.get("primary_exchange_mic") != expected.primary_exchange_mic
            ):
                raise I4CorrectionError(
                    "registry exact source scope differs from actual parent partition bytes"
                )
        direct_rows = tuple(
            sorted(
                (parent_rows[key].source for key in expected_keys),
                key=lambda row: row.row_key,
            )
        )
    else:
        direct: list[SourceIdentityKey] = []
        for change in impact.late_source_changes:
            parent = parent_by_session.get(change.session_date)
            corrected = replacement_by_session.get(change.session_date)
            if parent is None or corrected is None:
                raise I4CorrectionError("late-source affected session is absent from bounded bytes")
            parent_snapshot_rows = tuple(
                row for row in change.parent_snapshot.rows if group.matches(row)
            )
            corrected_snapshot_rows = tuple(
                row for row in change.corrected_snapshot.rows if group.matches(row)
            )
            parent_partition_rows = tuple(
                row.source for row in parent.image.rows if group.matches(row.source)
            )
            corrected_partition_rows = tuple(
                row.source for row in corrected.image.rows if group.matches(row.source)
            )
            if parent_snapshot_rows != parent_partition_rows or (
                corrected_snapshot_rows != corrected_partition_rows
            ):
                raise I4CorrectionError(
                    "late-source exact snapshots differ from old/new partition lineage"
                )
            if len(corrected_snapshot_rows) != 1:
                raise I4CorrectionError(
                    "bounded late-source correction requires one corrected selected source row"
                )
            direct.extend(corrected_snapshot_rows)
        direct_rows = tuple(sorted(direct, key=lambda row: row.row_key))
    if any(not group.matches(row) for row in direct_rows):
        raise I4CorrectionError("derived directly affected rows crossed exact group")
    earliest = min(row.session_date for row in direct_rows)
    calendar = tuple(item.session_date for item in checkpoint.resolved_partition_map)
    replacement_sessions = tuple(replacement_by_session)
    try:
        earliest_index = calendar.index(earliest)
    except ValueError as exc:  # expected source rows already bind one partition
        raise I4CorrectionError("earliest affected row is absent from checkpoint calendar") from exc
    expected_prefix = calendar[earliest_index : earliest_index + len(replacement_sessions)]
    if replacement_sessions != expected_prefix:
        raise I4CorrectionError(
            "replacement receipts must be contiguous from earliest affected session"
        )
    slots = tuple(
        ExactGroupSessionSlot(
            group=group,
            session_date=session,
            source_rows=tuple(
                row.source
                for row in (
                    replacement_by_session[session].image.rows
                    if impact.cause is I4ProductionCorrectionCause.LATE_SOURCE
                    else parent_by_session[session].image.rows
                )
                if group.matches(row.source)
            ),
        )
        for session in replacement_sessions
    )
    proofs = _derive_alias_boundary_proofs(
        checkpoint=checkpoint,
        group=group,
        earliest_index=earliest_index,
        calendar=calendar,
        slots=slots,
        parent_by_session=parent_by_session,
        replacement_by_session=replacement_by_session,
        registry_changes=registry_changes,
        alias_state_ledger=alias_state_ledger,
    )
    boundary, selected_slots = select_first_stable_alias_boundary(
        group=group,
        earliest_affected_session=earliest,
        exact_group_slots=slots,
        boundary_proofs=proofs,
    )
    selected_sessions = tuple(item.session_date for item in selected_slots)
    if replacement_sessions != selected_sessions:
        raise I4CorrectionError(
            "replacement receipts exceed the first module-derived stable boundary"
        )
    if not isinstance(authorization, PinnedCorrectionAuthorization):
        raise I4CorrectionError("production scope requires a pinned authorization")
    return BoundedCorrectionScope(
        group=group,
        direct_source_rows=direct_rows,
        exact_group_slots=selected_slots,
        alias_recompute_from_session=earliest,
        alias_stable_boundary_session=boundary,
        authorization_id=authorization.authorization.authorization_id,
        authorization_available_session=(authorization.authorization.approval_available_session),
        availability_cutoff_session=availability_cutoff_session,
        _seal=_SCOPE_SEAL,
    )


def _expected_registry_source_scope(
    changes: tuple[RegistryChange, ...],
) -> tuple[RegistrySourceScopeRow, ...]:
    rows: dict[tuple[date, str], RegistrySourceScopeRow] = {}
    for change in changes:
        decisions = (
            (change.predecessor,)
            if change.successor is None
            else (change.predecessor, change.successor)
        )
        if any(not decision.source_scope for decision in decisions):
            raise I4CorrectionError(
                "production registry change lacks its authenticated complete source scope"
            )
        predecessor_scope = tuple(item.to_dict() for item in change.predecessor.source_scope)
        if change.successor is not None and predecessor_scope != tuple(
            item.to_dict() for item in change.successor.source_scope
        ):
            raise I4CorrectionError("registry successor changed exact source-scope rows")
        for item in change.predecessor.source_scope:
            key = (item.session_date, item.source_record_id)
            prior = rows.get(key)
            if prior is not None and prior != item:
                raise I4CorrectionError("registry changes conflict on one exact source row")
            rows[key] = item
    return tuple(rows[key] for key in sorted(rows))


def _derive_alias_boundary_proofs(
    *,
    checkpoint: I3CheckpointState,
    group: ExactIdentityGroup,
    earliest_index: int,
    calendar: tuple[date, ...],
    slots: tuple[ExactGroupSessionSlot, ...],
    parent_by_session: Mapping[date, _DecodedPartition],
    replacement_by_session: Mapping[date, _DecodedPartition],
    registry_changes: tuple[RegistryChange, ...],
    alias_state_ledger: I4AliasStateLedgerRelease,
) -> tuple[AliasBoundaryProof, ...]:
    entries = {item.session_date: item for item in alias_state_ledger.entries}
    slot_sessions = tuple(item.session_date for item in slots)
    history_complete = tuple(entries) == slot_sessions and all(
        item.group == group for item in alias_state_ledger.entries
    )
    if not history_complete:
        raise I4CorrectionError(
            "alias-state ledger must cover every and only bounded replacement session"
        )
    if slot_sessions[-1] == checkpoint.last_session:
        checkpoint_states = tuple(
            item
            for item in checkpoint.open_aliases
            if (
                item.segment.provider_id == group.provider_id
                and item.segment.provider_market == group.provider_market
                and item.segment.provider_locale == group.provider_locale
                and item.segment.ticker == group.ticker
            )
        )
        if len(checkpoint_states) != 1 or (
            entries[slot_sessions[-1]].parent_open_alias != checkpoint_states[0]
        ):
            raise I4CorrectionError(
                "terminal parent alias-state evidence differs from exact checkpoint frontier"
            )
    proofs: list[AliasBoundaryProof] = []
    for offset, slot in enumerate(slots):
        calendar_index = earliest_index + offset
        session = slot.session_date
        entry = entries[session]
        _bind_alias_state_to_partition(
            entry.parent_open_alias,
            parent_by_session[session],
            group,
            label="parent",
        )
        _bind_alias_state_to_partition(
            entry.corrected_open_alias,
            replacement_by_session[session],
            group,
            label="corrected",
        )
        lookback_sessions = calendar[max(earliest_index, calendar_index - 2) : calendar_index + 1]
        parent_lookback = stable_digest(
            {
                "group": group.to_dict(),
                "sessions": [
                    _group_session_projection_payload(parent_by_session[item], group)
                    for item in lookback_sessions
                ],
            }
        )
        corrected_lookback = stable_digest(
            {
                "group": group.to_dict(),
                "sessions": [
                    _group_session_projection_payload(
                        replacement_by_session.get(item, parent_by_session[item]),
                        group,
                    )
                    for item in lookback_sessions
                ],
            }
        )
        future_sessions = calendar[calendar_index + 1 :]
        parent_future = _future_policy_effect_digest(
            group,
            future_sessions,
            registry_changes,
            corrected=False,
        )
        corrected_future = _future_policy_effect_digest(
            group,
            future_sessions,
            registry_changes,
            corrected=True,
        )
        state_equal = _alias_state_digest(entry.parent_open_alias) == _alias_state_digest(
            entry.corrected_open_alias
        )
        effect_exhausted = (
            state_equal
            and parent_lookback == corrected_lookback
            and parent_future == corrected_future
        )
        proofs.append(
            AliasBoundaryProof(
                group=group,
                session_date=session,
                source_slot_digest=slot.slot_digest,
                parent_open_alias=entry.parent_open_alias,
                corrected_open_alias=entry.corrected_open_alias,
                parent_fixed_lookback_digest=parent_lookback,
                corrected_fixed_lookback_digest=corrected_lookback,
                parent_future_registry_effect_digest=parent_future,
                corrected_future_registry_effect_digest=corrected_future,
                exact_group_history_complete=history_complete,
                correction_effect_exhausted=effect_exhausted,
            )
        )
    return tuple(proofs)


def _bind_alias_state_to_partition(
    state: OpenAliasState,
    partition: _DecodedPartition,
    group: ExactIdentityGroup,
    *,
    label: str,
) -> None:
    projections = tuple(item for item in partition.image.rows if group.matches(item.source))
    if not projections:
        raise I4CorrectionError(f"{label} alias-state evidence has no exact-group partition row")
    expected = (
        state.segment.alias_segment_id,
        state.resolution.alias_resolution_version_id,
    )
    observed = {(item.alias_segment_id, item.alias_resolution_version_id) for item in projections}
    if observed != {expected}:
        raise I4CorrectionError(
            f"{label} alias-state evidence differs from partition row-version IDs"
        )


def _group_session_projection_payload(
    partition: _DecodedPartition,
    group: ExactIdentityGroup,
) -> dict[str, object]:
    rows = []
    for projection in partition.image.rows:
        if not group.matches(projection.source):
            continue
        rows.append(
            {
                "alias_segment_id": projection.alias_segment_id,
                "canonical": projection.canonical_payload(include_alias=False),
                "source": projection.source.to_dict(),
            }
        )
    return {
        "rows": rows,
        "session_date": partition.session_date.isoformat(),
    }


def _group_session_projection_digest(
    partition: _DecodedPartition,
    group: ExactIdentityGroup,
) -> str:
    return stable_digest(_group_session_projection_payload(partition, group))


def _future_policy_effect_digest(
    group: ExactIdentityGroup,
    sessions: tuple[date, ...],
    changes: tuple[RegistryChange, ...],
    *,
    corrected: bool,
) -> str:
    effects = []
    future = set(sessions)
    for change in changes:
        decision = change.successor if corrected else change.predecessor
        if decision is None:
            continue
        for row in decision.source_scope:
            if row.session_date in future:
                effects.append(
                    {
                        "decision": decision.to_dict(),
                        "source_scope_row": row.to_dict(),
                    }
                )
    return stable_digest(
        {
            "effects": effects,
            "group": group.to_dict(),
            "rule_version": I4_ALIAS_BOUNDARY_RULE_VERSION,
        }
    )


def _validate_exact_unaffected_projection(
    *,
    scope: BoundedCorrectionScope,
    parent_partitions: tuple[_DecodedPartition, ...],
    replacement_partitions: tuple[_DecodedPartition, ...],
    allow_scoped_source_change: bool = False,
) -> None:
    parent_by_session = {item.session_date: item for item in parent_partitions}
    for replacement in replacement_partitions:
        parent = parent_by_session.get(replacement.session_date)
        if parent is None:
            raise I4CorrectionError("replacement session is absent from authenticated parent")
        old_by_key = {item.row_key: item for item in parent.image.rows}
        new_by_key = {item.row_key: item for item in replacement.image.rows}
        old_unaffected = {
            key: item for key, item in old_by_key.items() if not scope.group.matches(item.source)
        }
        new_unaffected = {
            key: item for key, item in new_by_key.items() if not scope.group.matches(item.source)
        }
        if old_unaffected != new_unaffected:
            raise I4CorrectionError(
                "replacement changed an exact unaffected canonical row projection"
            )
        if not allow_scoped_source_change and set(old_by_key) != set(new_by_key):
            raise I4CorrectionError("replacement changed exact source-row membership")
        for key, old in old_by_key.items():
            if scope.group.matches(old.source):
                continue
            new = new_by_key[key]
            if old.source != new.source or old.canonical_payload() != new.canonical_payload():
                raise I4CorrectionError(
                    "replacement changed an exact unaffected canonical row projection"
                )


def _validate_registry_changes(
    scope: BoundedCorrectionScope,
    registry_changes: tuple[RegistryChange, ...],
    effective_before: tuple[RegistryDecision, ...],
    effective_after: tuple[RegistryDecision, ...],
) -> tuple[RegistryChange, ...]:
    if type(registry_changes) is not tuple or not registry_changes:
        raise I4CorrectionError("correction requires at least one registry change")
    if any(not isinstance(change, RegistryChange) for change in registry_changes):
        raise I4CorrectionError("correction registry changes are invalid")
    keys = [
        (
            change.predecessor.registry_kind.value,
            change.predecessor.decision_id,
        )
        for change in registry_changes
    ]
    if keys != sorted(set(keys)):
        raise I4CorrectionError("registry changes must be sorted and unique")
    if any(change.group() != scope.group for change in registry_changes):
        raise I4CorrectionError("registry change crossed exact provider/ticker scope")
    direct_source_ids = {row.source_record_id for row in scope.direct_source_rows}
    if any(
        change.registry_kind is not IdentityRegistryKind.ASSET_TRANSITION
        and not set(change.predecessor.source_record_ids).issubset(direct_source_ids)
        for change in registry_changes
    ):
        raise I4CorrectionError("registry correction source is outside directly affected rows")
    if any(
        change.change_available_session > scope.availability_cutoff_session
        for change in registry_changes
    ):
        raise I4CorrectionError("registry change was unavailable at correction cutoff")

    for decisions, label in (
        (effective_before, "prior registry decisions"),
        (effective_after, "post-correction registry decisions"),
    ):
        if type(decisions) is not tuple or any(
            not isinstance(item, RegistryDecision) for item in decisions
        ):
            raise I4CorrectionError(f"{label} are invalid")
        decision_ids = [item.decision_id for item in decisions]
        if decision_ids != sorted(set(decision_ids)):
            raise I4CorrectionError(f"{label} must be sorted and unique")
    before_by_id = {item.decision_id: item for item in effective_before}
    after_by_id = {item.decision_id: item for item in effective_after}
    before_set = set(before_by_id)
    after_ids = [item.decision_id for item in effective_after]
    after_set = set(after_ids)
    for change in registry_changes:
        if change.predecessor.decision_id not in before_set:
            raise I4CorrectionError("registry predecessor is absent from prior policy")
        if before_by_id[change.predecessor.decision_id].to_dict() != (change.predecessor.to_dict()):
            raise I4CorrectionError("registry predecessor differs from prior policy bytes")
        predecessor_present = change.predecessor.decision_id in after_set
        if predecessor_present:
            raise I4CorrectionError("superseded or withdrawn registry decision remains effective")
        if change.successor is not None:
            if change.successor.decision_id not in after_set:
                raise I4CorrectionError("registry successor is absent from target policy")
            if after_by_id[change.successor.decision_id].to_dict() != (change.successor.to_dict()):
                raise I4CorrectionError("registry successor differs from target policy bytes")
    predecessor_ids = {item.predecessor.decision_id for item in registry_changes}
    successor_ids = {
        item.successor.decision_id for item in registry_changes if item.successor is not None
    }
    if before_set - predecessor_ids != after_set - successor_ids:
        raise I4CorrectionError("target policy changed unrelated registry decisions")
    return registry_changes


def _validate_partition_images(
    checkpoint: I3CheckpointState,
    scope: BoundedCorrectionScope,
    parent_images: tuple[SessionPartitionImage, ...],
    replacement_images: tuple[SessionPartitionImage, ...],
    registry_changes: tuple[RegistryChange, ...],
    target_policy_snapshot: IdentityPolicySnapshot,
    late_source_changes: tuple[_LateSourceSessionChange, ...] = (),
) -> tuple[PartitionReplacement, ...]:
    for value, label in (
        (parent_images, "parent partition images"),
        (replacement_images, "replacement partition images"),
    ):
        if type(value) is not tuple or any(
            not isinstance(item, SessionPartitionImage) for item in value
        ):
            raise I4CorrectionError(f"{label} are invalid")
    parent_sessions = tuple(item.session_date for item in parent_images)
    replacement_sessions = tuple(item.session_date for item in replacement_images)
    if parent_sessions != scope.recompute_sessions or replacement_sessions != (
        scope.recompute_sessions
    ):
        raise I4CorrectionError(
            "correction must replace every and only whole sessions in alias recompute range"
        )
    if parent_sessions != tuple(sorted(set(parent_sessions))):
        raise I4CorrectionError("correction partition sessions are not sorted and unique")
    checkpoint_by_session = {item.session_date: item for item in checkpoint.resolved_partition_map}
    replacements: list[PartitionReplacement] = []
    direct_keys = scope.direct_row_keys
    changed_registry_kinds_by_row = _registry_kinds_by_direct_row(
        scope,
        registry_changes,
    )
    late_by_session = {item.session_date: item for item in late_source_changes}
    for parent, replacement in zip(parent_images, replacement_images, strict=True):
        frontier = checkpoint_by_session.get(parent.session_date)
        if frontier is None:
            raise I4CorrectionError("correction tried to add a historical partition")
        _bind_parent_image_to_checkpoint(parent, frontier)
        if parent.receipt.schema_digest != replacement.receipt.schema_digest:
            raise I4CorrectionError("correction partition changed schema")
        if replacement.receipt.availability_session > scope.availability_cutoff_session:
            raise I4CorrectionError("replacement partition was unavailable at cutoff")
        if replacement.receipt.availability_session < scope.authorization_available_session:
            raise I4CorrectionError("replacement partition predates correction authorization")
        parent_by_key = {row.row_key: row for row in parent.rows}
        replacement_by_key = {row.row_key: row for row in replacement.rows}
        if parent.session_date in late_by_session:
            old_group = tuple(row for row in parent.rows if scope.group.matches(row.source))
            new_group = tuple(row for row in replacement.rows if scope.group.matches(row.source))
            if len(old_group) != 1 or len(new_group) != 1:
                raise I4CorrectionError(
                    "bounded late-source replacement requires one old/new exact-group row"
                )
            old, new = old_group[0], new_group[0]
            if old.issuer_payload() != new.issuer_payload():
                raise I4CorrectionError("late-source correction exceeded issuer authority")
            if old.source.provider_locale != "us" or new.source.provider_locale != "us":
                raise I4CorrectionError("late-source correction crossed into a foreign locale")
            old_unaffected = {
                key: row
                for key, row in parent_by_key.items()
                if not scope.group.matches(row.source)
            }
            new_unaffected = {
                key: row
                for key, row in replacement_by_key.items()
                if not scope.group.matches(row.source)
            }
            if old_unaffected != new_unaffected:
                raise I4CorrectionError(
                    "late-source replacement changed an unrelated canonical projection"
                )
            try:
                replacements.append(
                    PartitionReplacement(
                        replaced_receipt=parent.receipt,
                        replacement_receipt=replacement.receipt,
                    )
                )
            except ValueError as exc:
                raise I4CorrectionError("whole-session partition replacement is invalid") from exc
            continue
        if set(parent_by_key) != set(replacement_by_key):
            raise I4CorrectionError("whole-session replacement changed source membership")
        for key, old in parent_by_key.items():
            new = replacement_by_key[key]
            if old.source != new.source:
                raise I4CorrectionError("whole-session replacement rewrote observed lineage")
            if old.alias_segment_id != new.alias_segment_id:
                raise I4CorrectionError("canonical correction rewrote immutable alias segment")
            if old.issuer_payload() != new.issuer_payload():
                raise I4CorrectionError("identity correction exceeded issuer authority")
            if old.source.provider_locale != "us":
                if old.canonical_payload() != new.canonical_payload():
                    raise I4CorrectionError("US correction rewrote a legal foreign-locale identity")
                continue
            if not scope.group.matches(old.source):
                if old.canonical_payload() != new.canonical_payload():
                    raise I4CorrectionError("unrelated canonical projection changed")
                continue
            if key not in direct_keys:
                if old.canonical_payload(include_alias=False) != new.canonical_payload(
                    include_alias=False
                ):
                    raise I4CorrectionError(
                        "alias ripple changed a non-direct canonical projection"
                    )
                continue
            row_changes = tuple(
                change for change in registry_changes if _change_affects_source(change, old.source)
            )
            _validate_direct_row_change(
                old,
                new,
                registry_kinds=changed_registry_kinds_by_row.get(key, frozenset()),
                target_policy_snapshot=target_policy_snapshot,
            )
            _validate_direct_row_ledger_projection(old, new, row_changes)
        try:
            replacements.append(
                PartitionReplacement(
                    replaced_receipt=parent.receipt,
                    replacement_receipt=replacement.receipt,
                )
            )
        except ValueError as exc:
            raise I4CorrectionError("whole-session partition replacement is invalid") from exc
    return tuple(replacements)


def _registry_kinds_by_direct_row(
    scope: BoundedCorrectionScope,
    changes: tuple[RegistryChange, ...],
) -> dict[tuple[date, str], frozenset[IdentityRegistryKind]]:
    result: dict[tuple[date, str], set[IdentityRegistryKind]] = {
        row.row_key: set() for row in scope.direct_source_rows
    }
    for change in changes:
        for row in scope.direct_source_rows:
            if _change_affects_source(change, row):
                result[row.row_key].add(change.registry_kind)
    return {key: frozenset(value) for key, value in result.items()}


def _validate_direct_row_change(
    old: CanonicalIdentityProjection,
    new: CanonicalIdentityProjection,
    *,
    registry_kinds: frozenset[IdentityRegistryKind],
    target_policy_snapshot: IdentityPolicySnapshot,
) -> None:
    observation = _i3_observation_from_source(new.source)
    matching_composite = tuple(
        decision
        for decision in target_policy_snapshot.matching_decisions(observation)
        if decision.registry_kind in _COMPOSITE_CORRECTION_REGISTRIES
    )
    if len(matching_composite) > 1:
        raise I4CorrectionError("post-correction cross-registry Composite collision")
    if old.canonical_payload() == new.canonical_payload():
        return
    if not registry_kinds:
        raise I4CorrectionError("direct row changed without a scoped registry successor/withdrawal")
    if registry_kinds == frozenset({IdentityRegistryKind.ASSET_TRANSITION}):
        raise I4CorrectionError("asset_transition may change lineage only, not canonical rows")

    changed = {
        key
        for key in old.canonical_payload()
        if old.canonical_payload()[key] != new.canonical_payload()[key]
    }
    allowed = {"alias_resolution_version_id"}
    composite_kinds = registry_kinds & _COMPOSITE_CORRECTION_REGISTRIES
    if composite_kinds:
        if len(composite_kinds) > 1:
            raise I4CorrectionError("one source row changed through multiple Composite registries")
        allowed.update(
            {
                "backtest_identity_eligible",
                "canonical_asset_id",
                "canonical_composite_figi",
                "decision_lineage_ids",
                "disposition",
                "resolution_method",
                "resolution_status",
            }
        )
    if IdentityRegistryKind.SHARE_CLASS_ADJUDICATION in registry_kinds:
        allowed.update(
            {
                "canonical_share_class_figi",
                "canonical_share_class_id",
                "disposition",
                "share_class_decision_lineage_ids",
                "share_class_resolution_method",
            }
        )
    if not composite_kinds and IdentityRegistryKind.SHARE_CLASS_ADJUDICATION not in registry_kinds:
        raise I4CorrectionError("registry change has no canonical correction authority")
    if not changed <= allowed:
        if IdentityRegistryKind.SHARE_CLASS_ADJUDICATION in registry_kinds and not composite_kinds:
            raise I4CorrectionError("Share Class correction crossed Composite or asset authority")
        raise I4CorrectionError("Composite correction crossed Share Class or alias authority")


def _validate_direct_row_ledger_projection(
    old: CanonicalIdentityProjection,
    new: CanonicalIdentityProjection,
    changes: tuple[RegistryChange, ...],
) -> None:
    for change in changes:
        if change.registry_kind is IdentityRegistryKind.ASSET_TRANSITION:
            continue
        composite = change.registry_kind in _COMPOSITE_CORRECTION_REGISTRIES
        old_lineage = (
            old.decision_lineage_ids if composite else old.share_class_decision_lineage_ids
        )
        new_lineage = (
            new.decision_lineage_ids if composite else new.share_class_decision_lineage_ids
        )
        if change.predecessor.decision_id in new_lineage:
            raise I4CorrectionError("replacement retained a superseded registry decision")
        if change.successor is None:
            if change.predecessor.decision_id not in old_lineage:
                raise I4CorrectionError(
                    "withdrawal predecessor is absent from the exact parent row lineage"
                )
            continue
        successor = change.successor
        if successor.decision_id not in new_lineage:
            raise I4CorrectionError("replacement omitted the exact registry successor lineage")
        if composite:
            if successor.canonical_composite_figi is None:
                if new.backtest_identity_eligible or new.canonical_composite_figi is not None:
                    raise I4CorrectionError(
                        "unresolved Composite successor did not suppress canonical eligibility"
                    )
            elif new.canonical_composite_figi != successor.canonical_composite_figi:
                raise I4CorrectionError("replacement Composite differs from registry successor")
        elif successor.canonical_share_class_figi is None:
            if new.canonical_share_class_figi is not None:
                raise I4CorrectionError(
                    "unresolved Share Class successor retained a canonical Share Class"
                )
        elif new.canonical_share_class_figi != successor.canonical_share_class_figi:
            raise I4CorrectionError("replacement Share Class differs from registry successor")


def _validate_production_alias_row_version(
    *,
    checkpoint: I3CheckpointState,
    scope: BoundedCorrectionScope,
    parent_partitions: tuple[_DecodedPartition, ...],
    replacement_partitions: tuple[_DecodedPartition, ...],
    alias_state_ledger: I4AliasStateLedgerRelease,
    registry_changes: tuple[RegistryChange, ...],
    target_policy_snapshot: IdentityPolicySnapshot,
    receipts: tuple[RowVersionReceipt, ...],
    superseded_ids: tuple[str, ...],
    availability_cutoff_session: date,
    authorization_available_session: date,
    artifact_reader: ExactArtifactReader,
    cache: _ExactReadCache,
) -> tuple[tuple[RowVersionReceipt, ...], tuple[str, ...]]:
    """Replay the one S7.5 reviewed alias correction from exact physical bytes.

    This is deliberately narrower than the I3 production row dispatcher.  I4
    accepts exactly one reviewed ``ticker_alias`` successor, rooted in the
    authenticated parent terminal.  It does not grant NEW_ROOT, tombstone,
    other-table, or multi-version-chain authority.
    """

    if type(receipts) is tuple and any(
        isinstance(item, RowVersionReceipt) and item.operation is RowVersionOperation.NEW_ROOT
        for item in receipts
    ):
        raise I4CorrectionError("I4 alias correction rejects NEW_ROOT")
    checked, superseded = _validate_row_versions(
        receipts,
        superseded_ids,
        availability_cutoff_session=availability_cutoff_session,
        authorization_available_session=authorization_available_session,
    )
    changed_entries = tuple(
        item
        for item in alias_state_ledger.entries
        if item.parent_open_alias != item.corrected_open_alias
    )
    if len(changed_entries) != 1:
        raise I4CorrectionError(
            "registry correction requires exactly one changed alias-state entry"
        )
    if len(checked) != 1:
        raise I4CorrectionError("changed alias requires exactly one ticker_alias row receipt")
    receipt = checked[0]
    if receipt.table_name != "ticker_alias":
        raise I4CorrectionError("I4 production correction permits only ticker_alias receipt")
    if receipt.operation is not RowVersionOperation.REVIEWED_CORRECTION:
        raise I4CorrectionError("I4 alias correction requires REVIEWED_CORRECTION")

    entry = changed_entries[0]
    parent_state = entry.parent_open_alias
    corrected_state = entry.corrected_open_alias
    if parent_state.segment != corrected_state.segment:
        raise I4CorrectionError("reviewed alias correction rewrote immutable alias segment")
    if corrected_state.resolution.is_tombstone:
        raise I4CorrectionError("reviewed alias correction cannot mint a tombstone")
    if corrected_state.resolution.predecessor_alias_resolution_version_id is None:
        raise I4CorrectionError("reviewed alias correction lacks an exact predecessor")

    parent_terminal = tuple(
        item
        for item in checkpoint.terminal_row_versions
        if item.table_name == "ticker_alias"
        and item.stable_row_key == corrected_state.segment.alias_segment_id
    )
    parent_frontier = tuple(
        item
        for item in checkpoint.open_aliases
        if item.segment.alias_segment_id == corrected_state.segment.alias_segment_id
    )
    if len(parent_terminal) != 1 or len(parent_frontier) != 1:
        raise I4CorrectionError("changed alias lacks one authenticated parent terminal/frontier")
    terminal = parent_terminal[0]
    frontier = parent_frontier[0]
    if terminal.row_version_id != frontier.resolution.alias_resolution_version_id:
        raise I4CorrectionError(
            "authenticated parent alias terminal differs from its open frontier"
        )
    if (
        receipt.stable_row_key != corrected_state.segment.alias_segment_id
        or receipt.row_version_id != corrected_state.resolution.alias_resolution_version_id
        or receipt.predecessor_row_version_id != terminal.row_version_id
        or corrected_state.resolution.predecessor_alias_resolution_version_id
        != terminal.row_version_id
        or receipt.semantic_proof.predecessor_payload_digest != terminal.row_payload_digest
    ):
        raise I4CorrectionError(
            "alias receipt predecessor differs from authenticated parent terminal"
        )

    replacement = next(
        (item for item in replacement_partitions if item.session_date == entry.session_date),
        None,
    )
    if replacement is None:
        raise I4CorrectionError("changed alias session lacks a replacement partition")
    matching = tuple(
        (projection, raw)
        for projection, raw in zip(
            replacement.image.rows,
            replacement.raw_rows,
            strict=True,
        )
        if scope.group.matches(projection.source)
    )
    if len(matching) != 1:
        raise I4CorrectionError("changed alias session lacks one exact replacement projection")
    projection, replacement_raw = matching[0]
    _validate_corrected_alias_projection(corrected_state, projection)
    _validate_corrected_alias_evidence(
        entry=entry,
        state=corrected_state,
        scope=scope,
        parent_partitions=parent_partitions,
        replacement_partitions=replacement_partitions,
        registry_changes=registry_changes,
        target_policy_snapshot=target_policy_snapshot,
    )

    proof_body = {
        "artifact_type": _ROW_SEMANTIC_PROOF_ARTIFACT_TYPE,
        "operation": receipt.operation.value,
        "predecessor_payload_digest": receipt.semantic_proof.predecessor_payload_digest,
        "predecessor_row_version_id": receipt.predecessor_row_version_id,
        "row_payload_digest": receipt.row_payload_digest,
        "row_version_id": receipt.row_version_id,
        "rule_version": _ROW_SEMANTIC_PROOF_RULE_VERSION,
        "stable_row_key": receipt.stable_row_key,
        "table_name": receipt.table_name,
        "validator_semantics_digest": receipt.semantic_proof.validator_semantics_digest,
    }
    expected_proof = {"proof_id": stable_digest(proof_body), **proof_body}
    if cache.read(receipt.semantic_proof.artifact, artifact_reader) != _canonical_json_bytes(
        expected_proof
    ):
        raise I4CorrectionError("alias row semantic-proof bytes do not reproduce")
    expected_validator = stable_digest(
        {
            "operation": RowVersionOperation.REVIEWED_CORRECTION.value,
            "rule_version": I4_TICKER_ALIAS_CORRECTION_VALIDATOR_RULE_VERSION,
            "schema_digest": I3_V2_CONTRACTS["ticker_alias"].schema_digest,
            "table_name": "ticker_alias",
        }
    )
    if receipt.semantic_proof.validator_semantics_digest != expected_validator:
        raise I4CorrectionError("alias proof names an unrecognized validator semantics")

    content = cache.read(receipt.index_artifact, artifact_reader)
    try:
        parquet = pq.ParquetFile(pa.BufferReader(content))
        table = parquet.read()
    except (OSError, pa.ArrowException) as exc:
        raise I4CorrectionError("alias receipt artifact is not readable Parquet") from exc
    if not table.schema.equals(I3_V2_CONTRACTS["ticker_alias"].arrow_schema):
        raise I4CorrectionError("alias receipt Parquet schema differs from native-v2")
    row_index = _canonical_row_locator_index(receipt.row_locator)
    if table.num_rows != 1 or row_index != 0:
        raise I4CorrectionError("single-receipt alias artifact must contain exactly row_index=0")
    row = table.slice(row_index, 1).to_pylist()[0]
    if (
        str(row.get("alias_segment_id")) != receipt.stable_row_key
        or row.get("alias_resolution_version_id") != receipt.row_version_id
        or row.get("predecessor_alias_resolution_version_id") != receipt.predecessor_row_version_id
        or row.get("alias_version_available_session") != receipt.availability_session
        or stable_digest(_jsonable(row)) != receipt.row_payload_digest
    ):
        raise I4CorrectionError("alias receipt differs from its exact physical Parquet row")
    _validate_alias_physical_row(
        row,
        state=corrected_state,
        projection=projection,
        replacement_raw=replacement_raw,
        receipt=receipt,
    )

    parent_refs = {
        (reference.table_name, reference.row_version_id)
        for partition in parent_partitions
        for reference in partition.image.receipt.row_version_references
    }
    replacement_refs = {
        (reference.table_name, reference.row_version_id)
        for partition in replacement_partitions
        for reference in partition.image.receipt.row_version_references
    }
    receipt_refs = {(item.table_name, item.row_version_id) for item in checked}
    if replacement_refs - parent_refs != receipt_refs:
        raise I4CorrectionError("replacement new row-version references differ from exact receipts")
    return checked, superseded


def _validate_corrected_alias_projection(
    state: OpenAliasState,
    projection: CanonicalIdentityProjection,
) -> None:
    resolution = state.resolution
    segment = state.segment
    legacy_method = {
        "direct_observed": "source_composite_figi_exact",
        "approved_genuine_transition": "approved_identity_adjudication",
        "approved_provider_contamination_override": "approved_identity_adjudication",
        "approved_cross_market_provider_contamination_override": (
            "approved_cross_market_adjudication"
        ),
        "approved_provider_composite_override": "approved_provider_composite_override",
    }.get(resolution.resolution_method.value)
    legacy_status = {
        "resolved": "resolved_strong",
        "unresolved": "unresolved",
        "tombstoned": "tombstoned",
    }[resolution.resolution_status.value]
    expected = (
        (projection.source.provider_id, segment.provider_id, "provider ID"),
        (projection.source.provider_market, segment.provider_market, "provider market"),
        (projection.source.provider_locale, segment.provider_locale, "provider locale"),
        (projection.source.ticker, segment.ticker, "ticker"),
        (
            projection.source.observed_composite_figi,
            segment.observed_composite_figi,
            "observed Composite",
        ),
        (
            projection.source.observed_share_class_figi,
            segment.observed_share_class_figi,
            "observed Share Class",
        ),
        (projection.canonical_asset_id, resolution.canonical_asset_id, "canonical asset"),
        (
            projection.canonical_composite_figi,
            resolution.canonical_composite_figi,
            "canonical Composite",
        ),
        (
            projection.canonical_share_class_id,
            resolution.canonical_share_class_id,
            "canonical Share Class ID",
        ),
        (
            projection.canonical_share_class_figi,
            resolution.canonical_share_class_figi,
            "canonical Share Class",
        ),
        (
            projection.canonical_issuer_id,
            resolution.canonical_issuer_id,
            "canonical issuer",
        ),
        (
            projection.canonical_cik_normalized,
            resolution.canonical_cik_normalized,
            "canonical CIK",
        ),
        (projection.resolution_method, legacy_method, "resolution method"),
        (projection.resolution_status, legacy_status, "resolution status"),
        (projection.disposition, resolution.disposition.value, "disposition"),
        (
            projection.share_class_resolution_method,
            resolution.share_class_resolution_method.value,
            "Share Class method",
        ),
        (
            projection.decision_lineage_ids,
            resolution.decision_lineage_ids,
            "decision lineage",
        ),
        (
            projection.share_class_decision_lineage_ids,
            resolution.share_class_decision_lineage_ids,
            "Share Class lineage",
        ),
        (projection.alias_segment_id, segment.alias_segment_id, "alias segment ID"),
        (
            projection.alias_resolution_version_id,
            resolution.alias_resolution_version_id,
            "alias resolution version ID",
        ),
    )
    if legacy_method is None:
        raise I4CorrectionError("corrected alias uses an unsupported production method")
    for observed, required, label in expected:
        if observed != required:
            raise I4CorrectionError(f"corrected OpenAliasState differs from replacement {label}")
    if projection.backtest_identity_eligible is not (
        resolution.resolution_status.value == "resolved" and not resolution.is_tombstone
    ):
        raise I4CorrectionError("corrected OpenAliasState differs from replacement eligibility")


def _validate_corrected_alias_evidence(
    *,
    entry: I4AliasStateLedgerEntry,
    state: OpenAliasState,
    scope: BoundedCorrectionScope,
    parent_partitions: tuple[_DecodedPartition, ...],
    replacement_partitions: tuple[_DecodedPartition, ...],
    registry_changes: tuple[RegistryChange, ...],
    target_policy_snapshot: IdentityPolicySnapshot,
) -> None:
    """Resolve non-projection alias facts from already authenticated inputs.

    The alias-state ledger is only a locator.  Policy identity/cutoffs come
    from the sealed target snapshot, while source-set and the old evidence
    timeline come from authenticated bounded parent rows plus the decision
    scope.  Replacement policy/evidence fields are outputs to verify, never
    facts to trust.
    """

    if not isinstance(target_policy_snapshot, IdentityPolicySnapshot):
        raise I4CorrectionError("alias evidence resolver lacks authenticated target policy")
    expected_scope = _expected_registry_source_scope(registry_changes)
    direct_keys = tuple(item.row_key for item in scope.direct_source_rows)
    expected_keys = tuple((item.session_date, item.source_record_id) for item in expected_scope)
    if direct_keys != expected_keys:
        raise I4CorrectionError(
            "alias evidence resolver differs from authenticated decision source scope"
        )
    parent_by_session = {item.session_date: item for item in parent_partitions}
    replacement_by_session = {item.session_date: item for item in replacement_partitions}
    selected_parent_raw: list[Mapping[str, object]] = []
    selected_replacement_raw: list[Mapping[str, object]] = []
    for source in scope.direct_source_rows:
        parent_partition = parent_by_session.get(source.session_date)
        partition = replacement_by_session.get(source.session_date)
        parent_raw = (
            None if parent_partition is None else parent_partition.raw_by_key.get(source.row_key)
        )
        replacement_raw = None if partition is None else partition.raw_by_key.get(source.row_key)
        if parent_raw is None or replacement_raw is None:
            raise I4CorrectionError("alias evidence resolver lacks an exact bounded source record")
        if (
            parent_raw.get("selected_source_record_id") != source.source_record_id
            or replacement_raw.get("selected_source_record_id") != source.source_record_id
            or parent_raw.get("ticker") != scope.group.ticker
            or replacement_raw.get("ticker") != scope.group.ticker
        ):
            raise I4CorrectionError(
                "alias evidence resolver source differs from authenticated scope"
            )
        selected_parent_raw.append(parent_raw)
        selected_replacement_raw.append(replacement_raw)
    source_record_ids = tuple(
        sorted({str(item["selected_source_record_id"]) for item in selected_parent_raw})
    )
    if len(source_record_ids) != len(selected_parent_raw):
        raise I4CorrectionError("alias evidence resolver repeated a bounded source record")
    expected_source_digest = stable_digest(
        {
            "source_record_ids": list(source_record_ids),
        }
    )
    parent_raw_evidence = tuple(
        _session(
            item.get("identity_evidence_available_session"),
            "authenticated parent source evidence availability",
        )
        for item in selected_parent_raw
    )
    lineage_ids = (
        state.resolution.decision_lineage_ids + state.resolution.share_class_decision_lineage_ids
    )
    target_by_id = {item.decision_id: item for item in target_policy_snapshot.decisions}
    try:
        lineage_decisions = tuple(target_by_id[item] for item in lineage_ids)
    except KeyError as exc:
        raise I4CorrectionError(
            "corrected alias lineage is absent from authenticated target policy"
        ) from exc
    scoped_source_ids = set(source_record_ids)
    if any(
        not set(decision.source_record_ids).issubset(scoped_source_ids)
        for decision in lineage_decisions
    ):
        raise I4CorrectionError(
            "corrected alias lineage crossed authenticated decision source scope"
        )
    release_by_id = {
        item.release_id: item for item in target_policy_snapshot.policy_bundle.registry_releases
    }
    release_by_kind = {
        item.registry_kind: item for item in target_policy_snapshot.policy_bundle.registry_releases
    }
    target_release_ids = {decision.registry_release_id for decision in lineage_decisions} | {
        change.successor.registry_release_id
        for change in registry_changes
        if change.successor is not None
    }
    try:
        target_release_availability = tuple(
            release_by_id[item].release_available_session for item in sorted(target_release_ids)
        )
        changed_registry_release_availability = tuple(
            release_by_kind[change.predecessor.registry_kind].release_available_session
            for change in registry_changes
        )
    except KeyError as exc:
        raise I4CorrectionError(
            "required registry release is absent from authenticated target bundle"
        ) from exc
    evidence_candidates = (
        *parent_raw_evidence,
        *(item.decision_available_session for item in lineage_decisions),
        *(item.change_available_session for item in registry_changes),
        *target_release_availability,
        *changed_registry_release_availability,
    )
    if not evidence_candidates:  # pragma: no cover - registry correction proves rows
        raise I4CorrectionError("alias evidence resolver has no exact evidence")
    expected_evidence_available = max(evidence_candidates)
    bundle = target_policy_snapshot.policy_bundle
    expected_resolution_available = max(
        bundle.bundle_available_session,
        expected_evidence_available,
    )
    if expected_resolution_available > bundle.policy_cutoff_session:
        raise I4CorrectionError(
            "authenticated target policy cutoff cannot admit corrected alias evidence"
        )
    for raw in selected_replacement_raw:
        if raw.get("identity_policy_bundle_id") != bundle.identity_policy_bundle_id:
            raise I4CorrectionError(
                "replacement identity policy bundle differs from authenticated target"
            )
        if raw.get("identity_evidence_available_session") != expected_evidence_available:
            raise I4CorrectionError("replacement evidence availability was not module-derived")
    resolution = state.resolution
    expected = (
        (
            resolution.identity_policy_bundle_id,
            bundle.identity_policy_bundle_id,
            "policy bundle",
        ),
        (
            resolution.identity_cutoff_session,
            bundle.policy_cutoff_session,
            "identity cutoff",
        ),
        (
            resolution.evidence_cutoff_session,
            bundle.policy_cutoff_session,
            "evidence cutoff",
        ),
        (
            resolution.resolution_available_session,
            expected_resolution_available,
            "resolution availability",
        ),
        (
            resolution.evidence_available_session,
            expected_evidence_available,
            "evidence availability",
        ),
        (
            resolution.source_record_set_digest,
            expected_source_digest,
            "source-record-set digest",
        ),
        (
            resolution.valid_through_session,
            entry.session_date,
            "valid-through session",
        ),
    )
    for observed, required, label in expected:
        if observed != required:
            raise I4CorrectionError(
                f"corrected alias {label} was not derived from authenticated evidence"
            )


def _validate_alias_physical_row(
    row: Mapping[str, object],
    *,
    state: OpenAliasState,
    projection: CanonicalIdentityProjection,
    replacement_raw: Mapping[str, object],
    receipt: RowVersionReceipt,
) -> None:
    segment = state.segment
    resolution = state.resolution
    expected = {
        "alias_is_tombstone": resolution.is_tombstone,
        "alias_resolution_method": projection.resolution_method,
        "alias_resolution_status": projection.resolution_status,
        "alias_resolution_version_id": resolution.alias_resolution_version_id,
        "alias_segment_id": segment.alias_segment_id,
        "alias_tombstone_reason_code": resolution.tombstone_reason_code,
        "alias_version_available_session": receipt.availability_session,
        "asset_id": resolution.canonical_asset_id,
        "backtest_identity_eligible": projection.backtest_identity_eligible,
        "canonical_cik_normalized": resolution.canonical_cik_normalized,
        "canonical_composite_figi": resolution.canonical_composite_figi,
        "canonical_share_class_figi": resolution.canonical_share_class_figi,
        "decision_lineage_ids": list(resolution.decision_lineage_ids),
        "evidence_cutoff_session": resolution.evidence_cutoff_session,
        "first_source_record_id": segment.segment_origin_source_record_id,
        "identity_disposition": resolution.disposition.value,
        "identity_policy_bundle_id": resolution.identity_policy_bundle_id,
        "issuer_id": resolution.canonical_issuer_id,
        "last_source_record_id": replacement_raw.get("selected_source_record_id"),
        "observed_cik_normalized": segment.observed_cik_normalized,
        "observed_composite_figi": segment.observed_composite_figi,
        "observed_share_class_figi": segment.observed_share_class_figi,
        "predecessor_alias_resolution_version_id": (
            resolution.predecessor_alias_resolution_version_id
        ),
        "provider_id": segment.provider_id,
        "provider_locale": segment.provider_locale,
        "provider_market": segment.provider_market,
        "resolution_available_session": resolution.resolution_available_session,
        "segment_origin_source_record_id": segment.segment_origin_source_record_id,
        "share_class_decision_lineage_ids": list(resolution.share_class_decision_lineage_ids),
        "share_class_id": resolution.canonical_share_class_id,
        "source_record_set_digest": resolution.source_record_set_digest,
        "ticker": segment.ticker,
        "valid_from_session": segment.valid_from_session,
        "valid_through_session": resolution.valid_through_session,
    }
    for field, required in expected.items():
        if row.get(field) != required:
            raise I4CorrectionError(
                f"physical ticker_alias row differs from OpenAliasState field {field}"
            )


def _canonical_row_locator_index(value: str) -> int:
    prefix = "row_index="
    if not value.startswith(prefix):
        raise I4CorrectionError("alias row locator is not canonical")
    raw = value[len(prefix) :]
    if not raw.isdigit() or (raw != "0" and raw.startswith("0")):
        raise I4CorrectionError("alias row locator is not canonical")
    return int(raw)


def _jsonable(value: object) -> object:
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in sorted(value.items())}
    if isinstance(value, (tuple, list)):
        return [_jsonable(item) for item in value]
    return value


def _validate_row_versions(
    receipts: tuple[RowVersionReceipt, ...],
    superseded_ids: tuple[str, ...],
    *,
    availability_cutoff_session: date,
    authorization_available_session: date,
) -> tuple[tuple[RowVersionReceipt, ...], tuple[str, ...]]:
    if type(receipts) is not tuple or any(
        not isinstance(item, RowVersionReceipt) for item in receipts
    ):
        raise I4CorrectionError("correction row-version receipts are invalid")
    keys = [(item.table_name, item.stable_row_key) for item in receipts]
    if keys != sorted(set(keys)):
        raise I4CorrectionError("correction row versions must be sorted and unique")
    if any(item.table_name == "issuer_master" for item in receipts):
        raise I4CorrectionError("identity correction cannot publish issuer_master versions")
    if any(
        item.operation
        not in {RowVersionOperation.REVIEWED_CORRECTION, RowVersionOperation.TOMBSTONE}
        for item in receipts
    ):
        raise I4CorrectionError("correction row versions require reviewed correction/tombstone")
    if any(
        item.availability_session > availability_cutoff_session
        or item.availability_session < authorization_available_session
        for item in receipts
    ):
        raise I4CorrectionError("correction row-version availability is invalid")
    _digest_tuple(superseded_ids, "superseded row-version IDs")
    if superseded_ids != tuple(sorted(set(superseded_ids))):
        raise I4CorrectionError("superseded row-version IDs must be sorted and unique")
    expected = tuple(sorted(item.predecessor_row_version_id for item in receipts))
    if superseded_ids != expected:
        raise I4CorrectionError("superseded IDs differ from exact correction predecessors")
    return receipts, superseded_ids


def _bind_parent_image_to_checkpoint(
    image: SessionPartitionImage,
    frontier: ResolvedPartitionState,
) -> None:
    if not isinstance(frontier, ResolvedPartitionState):
        raise I4CorrectionError("checkpoint partition frontier is invalid")
    if (
        image.session_date != frontier.session_date
        or image.receipt.receipt != frontier.artifact
        or image.receipt.row_count != frontier.row_count
        or image.receipt.availability_session != frontier.availability_session
    ):
        raise I4CorrectionError("parent partition image differs from checkpoint frontier")


def _i3_observation_from_source(source: SourceIdentityKey) -> IdentityObservation:
    if source.provider_locale != "us":
        raise I4CorrectionError("foreign-locale source cannot enter target-policy lookup")
    return IdentityObservation(
        provider_id=source.provider_id,
        provider_market=source.provider_market,
        provider_locale=source.provider_locale,
        ticker=source.ticker,
        session_date=source.session_date,
        observed_composite_figi=source.observed_composite_figi,
        observed_composite_country=source.observed_composite_country,
        observed_share_class_figi=source.observed_share_class_figi,
        primary_exchange=None,
        source_record_id=source.source_record_id,
        active_on_date=source.active_on_date,
    )


def _change_affects_source(change: RegistryChange, source: SourceIdentityKey) -> bool:
    decisions = (
        (change.predecessor,)
        if change.successor is None
        else (change.predecessor, change.successor)
    )
    return any(
        source.source_record_id in decision.source_record_ids
        and decision.effective_from_session <= source.session_date
        and (
            decision.effective_to_session is None
            or source.session_date <= decision.effective_to_session
        )
        for decision in decisions
    )


def _alias_state_digest(value: OpenAliasState | None) -> str:
    return stable_digest(
        {
            "alias_state": value.to_dict() if value is not None else None,
            "rule_version": I4_ALIAS_BOUNDARY_RULE_VERSION,
        }
    )


def _calendar(values: tuple[date, ...]) -> tuple[date, ...]:
    if type(values) is not tuple or not values:
        raise I4CorrectionError("exact calendar must be a nonempty tuple")
    if any(type(value) is not date for value in values):
        raise I4CorrectionError("exact calendar contains an invalid session")
    if values != tuple(sorted(set(values))):
        raise I4CorrectionError("exact calendar must be sorted and unique")
    return values


def _artifact_from_gate(pin: GateArtifactPin) -> ArtifactPin:
    if not isinstance(pin, GateArtifactPin):
        raise I4CorrectionError("gate exact pin is invalid")
    return ArtifactPin(path=pin.path, sha256=pin.sha256, bytes=pin.bytes)


def _canonical_json_bytes(value: object) -> bytes:
    return (
        json.dumps(
            value,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        + b"\n"
    )


def _none_or_text(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise I4CorrectionError("native-v2 membership text field is invalid")
    return value


def _digest(value: object, label: str) -> str:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise I4CorrectionError(f"{label} must be a lowercase SHA-256")
    return value


def _optional_digest(value: object, label: str) -> None:
    if value is not None:
        _digest(value, label)


def _digest_tuple(values: object, label: str) -> None:
    if type(values) is not tuple:
        raise I4CorrectionError(f"{label} must be a tuple")
    for value in values:
        _digest(value, label)
    if values != tuple(sorted(set(values))):
        raise I4CorrectionError(f"{label} must be sorted and unique")


def _token(value: object, label: str) -> str:
    if not isinstance(value, str) or _TOKEN.fullmatch(value) is None:
        raise I4CorrectionError(f"{label} must be a lowercase token")
    return value


def _ticker(value: object) -> str:
    if not isinstance(value, str) or _TICKER.fullmatch(value) is None:
        raise I4CorrectionError("ticker is invalid")
    return value


def _session(value: object, label: str) -> date:
    if type(value) is not date:
        raise I4CorrectionError(f"{label} must be a date")
    return value


def _optional_figi(value: object, label: str) -> None:
    if value is not None and (not isinstance(value, str) or _FIGI.fullmatch(value) is None):
        raise I4CorrectionError(f"{label} must be a FIGI")


def _optional_country(value: object) -> None:
    if value is not None and (not isinstance(value, str) or _COUNTRY.fullmatch(value) is None):
        raise I4CorrectionError("observed Composite country must be ISO alpha-2")


__all__ = [
    "AliasBoundaryProof",
    "BoundedCorrectionScope",
    "CanonicalIdentityProjection",
    "ExactGroupExpansionRequired",
    "ExactGroupSessionSlot",
    "ExactIdentityGroup",
    "I4ApprovalEvent",
    "I4ApprovalEventAttestation",
    "I4ApprovalLedgerEntry",
    "I4ApprovalLedgerRelease",
    "I4AuthorizationExpectations",
    "I4CorrectionError",
    "I4CorrectionPlan",
    "I4RegistryChangeLedgerRelease",
    "I4RegistryLedgerEntry",
    "ProductionI4CorrectionCapability",
    "RegistryChange",
    "RegistryChangeOperation",
    "SessionPartitionImage",
    "SourceIdentityKey",
    "attest_i4_approval_event_exact",
    "build_bounded_correction_plan",
    "freeze_bounded_correction_scope",
    "mint_production_i4_correction_capability",
    "production_i4_source_binding_digest",
    "select_first_stable_alias_boundary",
    "validate_canonical_row_correction",
]
