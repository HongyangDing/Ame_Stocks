"""Content-addressed, case-bound provider evidence primitives for S7.

This module deliberately does not authorize production S7 ingress.  It models and
verifies evidence that a caller has already read through the exact six-release source
bundle.  No latest lookup, arbitrary path reader, adjudication mutation, or production
gate bypass lives here.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from types import MappingProxyType
from typing import Final
from weakref import WeakSet

import pyarrow as pa
import pyarrow.parquet as pq

from ame_stocks_api.artifacts import (
    ArtifactError,
    safe_relative_path,
    sha256_file,
    stable_digest,
    write_bytes_immutable,
)
from ame_stocks_api.silver.asset_contract import ASSET_CONTRACTS
from ame_stocks_api.silver.calendar_artifact import (
    XNYSCalendarArtifact,
    XNYSCalendarArtifactError,
)
from ame_stocks_api.silver.contracts import (
    SilverContractError,
    TableContract,
    arrow_schema_digest,
)
from ame_stocks_api.silver.identity_bounce import BounceCase
from ame_stocks_api.silver.identity_preview_plan import (
    IdentityPreviewPlanStore,
    S7DetectorPreviewPlan,
    S7DetectorPreviewPlanApproval,
)
from ame_stocks_api.silver.identity_source import (
    S7_SIX_RELEASE_BINDING_ID,
    S7_SOURCE_PINS,
    IdentitySourceArtifact,
    IdentitySourceBatch,
    IdentitySourceBundle,
    IdentitySourceError,
    IdentitySourcePin,
    open_identity_source_bundle,
)
from ame_stocks_api.silver.identity_streaming_preview import BoundedIdentityPreviewArtifact
from ame_stocks_api.silver.ticker_event_contract import TICKER_EVENT_CONTRACTS
from ame_stocks_api.silver.ticker_overview_contract import TICKER_OVERVIEW_SAFE_CONTRACT

PROVIDER_ROW_ATTESTATION_SCHEMA_VERSION: Final = 2
PROVIDER_EVIDENCE_USAGE_SCHEMA_VERSION: Final = 1
PROVIDER_EVIDENCE_MANIFEST_SCHEMA_VERSION: Final = 1
PROVIDER_ROW_ATTESTATION_RULE_VERSION: Final = "s7_provider_physical_row_attestation_v2"
S4_BOUNCE_USAGE_SCHEMA_VERSION: Final = 1
S4_BOUNCE_MANIFEST_SCHEMA_VERSION: Final = 1
S4_BOUNCE_USAGE_RULE_VERSION: Final = "s7_s4_bounce_case_physical_usage_v1"
S4_BOUNCE_MANIFEST_RULE_VERSION: Final = "s7_s4_bounce_source_attested_manifest_v1"
PROVIDER_EVIDENCE_USAGE_RULE_VERSION: Final = "s7_provider_case_bound_usage_v1"
PROVIDER_EVIDENCE_MANIFEST_RULE_VERSION: Final = "s7_provider_evidence_manifest_v1"
PHYSICAL_REPLAY_BATCH_SIZE: Final = 8_192
RUNNER_WRITE_BOUNDARY_SAFETY_MARGIN: Final = timedelta(minutes=1)

_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_FIELD = re.compile(r"^[a-z][a-z0-9_]*$")
_LEAKAGE_TOKENS = frozenset(
    {
        "backtest",
        "factor",
        "label",
        "outcome",
        "performance",
        "pnl",
        "portfolio",
        "profit",
        "return",
        "returns",
        "sharpe",
        "target",
    }
)


class ProviderEvidenceError(SilverContractError):
    """Raised when provider evidence is not exactly reproducible or is unsafe."""


@dataclass(frozen=True, slots=True, eq=False, weakref_slot=True)
class _RunnerEvidenceAuthority:
    """Ephemeral authority for one freshly started source-bound preview run."""

    data_root: Path
    bundle: IdentitySourceBundle
    plan_id: str
    plan_sha256: str
    approval_id: str
    approval_sha256: str
    calendar_artifact_id: str
    calendar_sha256: str
    created_at_utc: datetime
    manifest_available_session: date

    def require(
        self,
        *,
        data_root: Path,
        bundle: IdentitySourceBundle,
        plan: S7DetectorPreviewPlan,
        approval: S7DetectorPreviewPlanApproval,
        calendar: XNYSCalendarArtifact,
    ) -> None:
        if self not in _RUNNER_EVIDENCE_AUTHORITIES:
            raise ProviderEvidenceError("runner evidence authority was not factory-issued")
        if (
            self.data_root != data_root.expanduser().resolve()
            or self.bundle is not bundle
            or self.plan_id != plan.plan_id
            or self.plan_sha256 != plan.sha256
            or self.approval_id != approval.approval_id
            or self.approval_sha256 != approval.sha256
            or self.calendar_artifact_id != calendar.calendar_artifact_id
            or self.calendar_sha256 != calendar.sha256
        ):
            raise ProviderEvidenceError("runner evidence authority crosses run controls")

    def require_live_write(self, *, calendar: XNYSCalendarArtifact) -> None:
        if self not in _RUNNER_EVIDENCE_AUTHORITIES:
            raise ProviderEvidenceError("runner evidence authority was not factory-issued")
        written_at = _utc_now()
        if written_at < self.created_at_utc:
            raise ProviderEvidenceError("runner evidence write predates its authority")
        try:
            available_session, _ = calendar.first_open_after(written_at)
        except XNYSCalendarArtifactError as exc:
            raise ProviderEvidenceError(str(exc)) from exc
        if available_session != self.manifest_available_session:
            raise ProviderEvidenceError(
                "runner evidence authority expired across an availability boundary"
            )
        market_open = calendar.market_open(available_session)
        if market_open - written_at < RUNNER_WRITE_BOUNDARY_SAFETY_MARGIN:
            raise ProviderEvidenceError(
                "runner evidence write is too close to its availability boundary"
            )


_RUNNER_EVIDENCE_AUTHORITIES: WeakSet[_RunnerEvidenceAuthority] = WeakSet()


def _issue_runner_evidence_authority(
    *,
    data_root: Path,
    bundle: IdentitySourceBundle,
    plan: S7DetectorPreviewPlan,
    approval: S7DetectorPreviewPlanApproval,
    calendar: XNYSCalendarArtifact,
    created_at_utc: datetime,
) -> _RunnerEvidenceAuthority:
    """Issue a live-only authority; historical timestamps fail before any evidence write."""

    if type(bundle) is not IdentitySourceBundle:
        raise ProviderEvidenceError("runner authority requires the exact source bundle")
    try:
        bundle.require_official()
    except IdentitySourceError as exc:
        raise ProviderEvidenceError("runner authority requires an official source bundle") from exc
    root = data_root.expanduser().resolve()
    if bundle.data_root != root:
        raise ProviderEvidenceError("runner authority source root differs")
    if (
        type(plan) is not S7DetectorPreviewPlan
        or type(approval) is not S7DetectorPreviewPlanApproval
    ):
        raise ProviderEvidenceError("runner authority requires exact plan controls")
    if type(calendar) is not XNYSCalendarArtifact:
        raise ProviderEvidenceError("runner authority requires the exact calendar")
    sessions = tuple(
        item.session_date
        for item in calendar.sessions
        if plan.start_session <= item.session_date <= plan.end_session
    )
    try:
        bundle.require_approved_preview_scope(
            plan_id=plan.plan_id,
            plan_sha256=plan.sha256,
            approval_id=approval.approval_id,
            approval_sha256=approval.sha256,
            sessions=sessions,
        )
    except IdentitySourceError as exc:
        raise ProviderEvidenceError(
            "runner authority source capability crosses its approved preview"
        ) from exc
    created = _utc_datetime(created_at_utc, "runner authority created_at_utc")
    issued_at = _utc_now()
    if created > issued_at or issued_at - created > timedelta(minutes=5):
        raise ProviderEvidenceError("runner evidence authority cannot be historically backfilled")
    if (
        approval.plan_id != plan.plan_id
        or approval.plan_sha256 != plan.sha256
        or created < approval.approved_at_utc
        or plan.calendar_artifact_id != calendar.calendar_artifact_id
        or plan.calendar_artifact_sha256 != calendar.sha256
    ):
        raise ProviderEvidenceError("runner authority crosses approval or calendar controls")
    try:
        available_session, _ = calendar.first_open_after(created)
    except XNYSCalendarArtifactError as exc:
        raise ProviderEvidenceError(str(exc)) from exc
    authority = _RunnerEvidenceAuthority(
        data_root=root,
        bundle=bundle,
        plan_id=plan.plan_id,
        plan_sha256=plan.sha256,
        approval_id=approval.approval_id,
        approval_sha256=approval.sha256,
        calendar_artifact_id=calendar.calendar_artifact_id,
        calendar_sha256=calendar.sha256,
        created_at_utc=created,
        manifest_available_session=available_session,
    )
    _RUNNER_EVIDENCE_AUTHORITIES.add(authority)
    return authority


class ProviderEvidenceRole(StrEnum):
    S4_MEMBERSHIP_OBSERVATION = "s4_membership_observation"
    S4_PROVIDER_IDENTITY = "s4_provider_identity"
    S4_VERSION_SELECTION_CONTROL = "s4_version_selection_control"
    S5_REQUEST_COVERAGE_CONTEXT = "s5_request_coverage_context"
    S5_TICKER_CHANGE_CORROBORATION = "s5_ticker_change_corroboration"
    S6_OVERVIEW_CORROBORATION = "s6_overview_corroboration"


@dataclass(frozen=True, slots=True)
class _DatasetRule:
    record_id_field: str
    availability_basis_field: str
    availability_rule: str
    allowed_asserted_fields: frozenset[str]


_CONTRACTS: Final[Mapping[str, TableContract]] = MappingProxyType(
    {
        **ASSET_CONTRACTS,
        **TICKER_EVENT_CONTRACTS,
        TICKER_OVERVIEW_SAFE_CONTRACT.table: TICKER_OVERVIEW_SAFE_CONTRACT,
    }
)

_DATASET_RULES: Final[Mapping[str, _DatasetRule]] = MappingProxyType(
    {
        "asset_observation_daily": _DatasetRule(
            record_id_field="source_record_id",
            availability_basis_field="source_capture_at_utc",
            availability_rule="first_xnys_open_after_source_capture_v1",
            allowed_asserted_fields=frozenset(
                {
                    "cik",
                    "composite_figi",
                    "currency_name",
                    "delisted_at_utc",
                    "locale",
                    "market",
                    "name",
                    "primary_exchange_mic",
                    "provider_active",
                    "requested_active",
                    "session_date",
                    "share_class_figi",
                    "ticker",
                    "type_code",
                }
            ),
        ),
        "asset_observation_version": _DatasetRule(
            record_id_field="source_record_id",
            availability_basis_field="source_capture_at_utc",
            availability_rule="first_xnys_open_after_source_capture_v1",
            allowed_asserted_fields=frozenset(
                {
                    "difference_fields_json",
                    "is_selected",
                    "selected_source_record_id",
                    "selection_reason",
                    "selection_status",
                    "session_date",
                    "ticker",
                    "version_count",
                    "version_group_id",
                }
            ),
        ),
        "universe_source_daily": _DatasetRule(
            record_id_field="selected_source_record_id",
            availability_basis_field="universe_capture_completed_at_utc",
            availability_rule="first_xnys_open_after_complete_active_inactive_pair_v1",
            allowed_asserted_fields=frozenset(
                {
                    "active_on_date",
                    "cik",
                    "composite_figi",
                    "identity_link_status",
                    "primary_exchange_mic",
                    "session_date",
                    "share_class_figi",
                    "ticker",
                    "type_code",
                }
            ),
        ),
        "ticker_event_request_status": _DatasetRule(
            record_id_field="source_request_id",
            availability_basis_field="source_status_observed_at_utc",
            availability_rule="first_xnys_open_after_source_observation_v1",
            allowed_asserted_fields=frozenset(
                {
                    "accepted_event_count",
                    "coverage_interpretation",
                    "quarantined_event_count",
                    "raw_event_count",
                    "request_outcome",
                    "requested_identifier",
                    "response_composite_figi",
                }
            ),
        ),
        "ticker_change_event": _DatasetRule(
            record_id_field="source_record_id",
            availability_basis_field="source_capture_at_utc",
            availability_rule="first_xnys_open_after_source_capture_v1",
            allowed_asserted_fields=frozenset(
                {
                    "effective_ticker",
                    "event_date",
                    "event_date_quality",
                    "event_type",
                    "requested_identifier",
                    "response_cik",
                    "response_composite_figi",
                    "same_figi_date_multiple_tickers",
                }
            ),
        ),
        "ticker_overview_safe": _DatasetRule(
            record_id_field="source_record_id",
            availability_basis_field="source_capture_at_utc",
            availability_rule="first_xnys_open_after_source_capture_v1",
            allowed_asserted_fields=frozenset(
                {
                    "active",
                    "cik",
                    "composite_figi",
                    "currency_name",
                    "delisted_utc",
                    "first_active_date",
                    "identity_match",
                    "identity_match_basis",
                    "identity_type",
                    "identity_value",
                    "last_active_date",
                    "list_date",
                    "locale",
                    "market",
                    "primary_exchange",
                    "query_date",
                    "query_ticker",
                    "share_class_figi",
                    "ticker",
                    "ticker_root",
                    "ticker_suffix",
                    "type",
                }
            ),
        ),
    }
)

_ROLE_DATASETS: Final[Mapping[ProviderEvidenceRole, Counter[str]]] = MappingProxyType(
    {
        ProviderEvidenceRole.S4_MEMBERSHIP_OBSERVATION: Counter(
            {"asset_observation_daily": 1, "universe_source_daily": 1}
        ),
        ProviderEvidenceRole.S4_PROVIDER_IDENTITY: Counter({"asset_observation_daily": 1}),
        ProviderEvidenceRole.S4_VERSION_SELECTION_CONTROL: Counter(
            {"asset_observation_daily": 1, "asset_observation_version": 1}
        ),
        ProviderEvidenceRole.S5_REQUEST_COVERAGE_CONTEXT: Counter(
            {"ticker_event_request_status": 1}
        ),
        ProviderEvidenceRole.S5_TICKER_CHANGE_CORROBORATION: Counter(
            {"ticker_change_event": 1, "ticker_event_request_status": 1}
        ),
        ProviderEvidenceRole.S6_OVERVIEW_CORROBORATION: Counter({"ticker_overview_safe": 1}),
    }
)


@dataclass(frozen=True, slots=True)
class ProviderRowAttestation:
    six_release_binding_id: str
    dataset: str
    release_id: str
    release_manifest_path: str
    release_manifest_sha256: str
    contract_id: str
    arrow_schema_digest: str
    silver_artifact_path: str
    silver_artifact_sha256: str
    parquet_row_group: int
    row_index_in_row_group: int
    primary_key: Mapping[str, object]
    source_record_id_field: str
    source_record_id: str
    source_request_id: str
    full_row_digest: str
    full_row_snapshot: Mapping[str, object]
    availability_basis_field: str
    availability_basis_at_utc: datetime
    source_available_session: date
    source_available_at_utc: datetime
    source_availability_rule: str
    availability_calendar_id: str
    availability_calendar_sha256: str
    attestation_rule_version: str = PROVIDER_ROW_ATTESTATION_RULE_VERSION

    def __post_init__(self) -> None:
        if self.six_release_binding_id != S7_SIX_RELEASE_BINDING_ID:
            raise ProviderEvidenceError("row attestation is outside the exact six-release binding")
        try:
            pin = S7_SOURCE_PINS[self.dataset]
            rule = _DATASET_RULES[self.dataset]
            contract = _CONTRACTS[self.dataset]
        except KeyError as exc:
            raise ProviderEvidenceError(
                "row attestation dataset is outside the exact profile"
            ) from exc
        if self.release_id != pin.release_id:
            raise ProviderEvidenceError("row attestation release differs from the dataset pin")
        expected_manifest = f"manifests/silver/releases/release_id={self.release_id}.json"
        if self.release_manifest_path != expected_manifest:
            raise ProviderEvidenceError("row attestation release manifest path is not canonical")
        if self.release_manifest_sha256 != pin.release_manifest_sha256:
            raise ProviderEvidenceError("row attestation release manifest SHA differs from its pin")
        if self.contract_id != contract.contract_id:
            raise ProviderEvidenceError("row attestation contract differs from its dataset")
        if self.arrow_schema_digest != arrow_schema_digest(contract.arrow_schema):
            raise ProviderEvidenceError("row attestation Arrow schema digest differs")
        _relative_path(self.silver_artifact_path, "Silver artifact path")
        if not self.silver_artifact_path.endswith(".parquet"):
            raise ProviderEvidenceError("provider evidence Silver artifact must be Parquet")
        _digest(self.silver_artifact_sha256, "Silver artifact SHA-256")
        _native_nonnegative_int(self.parquet_row_group, "Parquet row group")
        _native_nonnegative_int(self.row_index_in_row_group, "row index in row group")
        normalized_pk = _normalize_mapping(self.primary_key, "primary key")
        if set(normalized_pk) != set(contract.primary_key):
            raise ProviderEvidenceError("row attestation primary key fields differ from contract")
        object.__setattr__(self, "primary_key", MappingProxyType(normalized_pk))
        if self.source_record_id_field != rule.record_id_field:
            raise ProviderEvidenceError("record-ID field differs from the dataset rule")
        _digest(self.source_record_id, "source record ID")
        _digest(self.source_request_id, "source request ID")
        _digest(self.full_row_digest, "full-row digest")
        snapshot = _normalize_mapping(self.full_row_snapshot, "full row snapshot")
        if set(snapshot) != set(contract.arrow_schema.names):
            raise ProviderEvidenceError("full row snapshot fields differ from contract")
        if (
            stable_digest(
                {
                    "arrow_schema_digest": self.arrow_schema_digest,
                    "namespace": "ame_stocks.identity.provider_full_row",
                    "row": snapshot,
                    "rule_version": "s7_provider_full_row_digest_v1",
                }
            )
            != self.full_row_digest
        ):
            raise ProviderEvidenceError("full row snapshot digest does not reproduce")
        object.__setattr__(self, "full_row_snapshot", MappingProxyType(snapshot))
        if self.availability_basis_field != rule.availability_basis_field:
            raise ProviderEvidenceError("availability basis field differs from dataset rule")
        object.__setattr__(
            self,
            "availability_basis_at_utc",
            _utc_datetime(self.availability_basis_at_utc, "availability basis"),
        )
        _native_date(self.source_available_session, "source available session")
        object.__setattr__(
            self,
            "source_available_at_utc",
            _utc_datetime(self.source_available_at_utc, "source available timestamp"),
        )
        if self.source_availability_rule != rule.availability_rule:
            raise ProviderEvidenceError("source availability rule differs from dataset rule")
        _digest(self.availability_calendar_id, "availability calendar ID")
        _digest(self.availability_calendar_sha256, "availability calendar SHA-256")
        if self.attestation_rule_version != PROVIDER_ROW_ATTESTATION_RULE_VERSION:
            raise ProviderEvidenceError("unsupported provider row attestation rule")

    @property
    def locator(self) -> tuple[str, str, str, int, int]:
        return (
            self.dataset,
            self.release_id,
            self.silver_artifact_path,
            self.parquet_row_group,
            self.row_index_in_row_group,
        )

    @property
    def row_attestation_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "arrow_schema_digest": self.arrow_schema_digest,
            "attestation_rule_version": self.attestation_rule_version,
            "availability_basis_at_utc": _format_utc(self.availability_basis_at_utc),
            "availability_basis_field": self.availability_basis_field,
            "availability_calendar_id": self.availability_calendar_id,
            "availability_calendar_sha256": self.availability_calendar_sha256,
            "contract_id": self.contract_id,
            "dataset": self.dataset,
            "full_row_digest": self.full_row_digest,
            "full_row_snapshot": dict(self.full_row_snapshot),
            "parquet_row_group": self.parquet_row_group,
            "primary_key": dict(self.primary_key),
            "release_id": self.release_id,
            "release_manifest_path": self.release_manifest_path,
            "release_manifest_sha256": self.release_manifest_sha256,
            "row_index_in_row_group": self.row_index_in_row_group,
            "silver_artifact_path": self.silver_artifact_path,
            "silver_artifact_sha256": self.silver_artifact_sha256,
            "six_release_binding_id": self.six_release_binding_id,
            "source_availability_rule": self.source_availability_rule,
            "source_available_at_utc": _format_utc(self.source_available_at_utc),
            "source_available_session": self.source_available_session.isoformat(),
            "source_record_id": self.source_record_id,
            "source_record_id_field": self.source_record_id_field,
            "source_request_id": self.source_request_id,
        }

    def to_dict(self) -> dict[str, object]:
        return {
            "provider_row_attestation_schema_version": (PROVIDER_ROW_ATTESTATION_SCHEMA_VERSION),
            "row_attestation_id": self.row_attestation_id,
            **self.logical_payload(),
        }

    @classmethod
    def from_dict(cls, value: object) -> ProviderRowAttestation:
        item = _exact_mapping(
            value,
            {
                "arrow_schema_digest",
                "attestation_rule_version",
                "availability_basis_at_utc",
                "availability_basis_field",
                "availability_calendar_id",
                "availability_calendar_sha256",
                "contract_id",
                "dataset",
                "full_row_digest",
                "full_row_snapshot",
                "parquet_row_group",
                "primary_key",
                "provider_row_attestation_schema_version",
                "release_id",
                "release_manifest_path",
                "release_manifest_sha256",
                "row_attestation_id",
                "row_index_in_row_group",
                "silver_artifact_path",
                "silver_artifact_sha256",
                "six_release_binding_id",
                "source_availability_rule",
                "source_available_at_utc",
                "source_available_session",
                "source_record_id",
                "source_record_id_field",
                "source_request_id",
            },
            "provider row attestation",
        )
        if item["provider_row_attestation_schema_version"] != 2:
            raise ProviderEvidenceError("unsupported provider row attestation schema")
        attestation = cls(
            six_release_binding_id=_string(item, "six_release_binding_id"),
            dataset=_string(item, "dataset"),
            release_id=_string(item, "release_id"),
            release_manifest_path=_string(item, "release_manifest_path"),
            release_manifest_sha256=_string(item, "release_manifest_sha256"),
            contract_id=_string(item, "contract_id"),
            arrow_schema_digest=_string(item, "arrow_schema_digest"),
            silver_artifact_path=_string(item, "silver_artifact_path"),
            silver_artifact_sha256=_string(item, "silver_artifact_sha256"),
            parquet_row_group=_native_nonnegative_int(
                item["parquet_row_group"], "Parquet row group"
            ),
            row_index_in_row_group=_native_nonnegative_int(
                item["row_index_in_row_group"], "row index in row group"
            ),
            primary_key=_mapping(item["primary_key"], "primary key"),
            source_record_id_field=_string(item, "source_record_id_field"),
            source_record_id=_string(item, "source_record_id"),
            source_request_id=_string(item, "source_request_id"),
            full_row_digest=_string(item, "full_row_digest"),
            full_row_snapshot=_mapping(item["full_row_snapshot"], "full row snapshot"),
            availability_basis_field=_string(item, "availability_basis_field"),
            availability_basis_at_utc=_parse_utc(_string(item, "availability_basis_at_utc")),
            source_available_session=_parse_date(
                _string(item, "source_available_session"), "source_available_session"
            ),
            source_available_at_utc=_parse_utc(_string(item, "source_available_at_utc")),
            source_availability_rule=_string(item, "source_availability_rule"),
            availability_calendar_id=_string(item, "availability_calendar_id"),
            availability_calendar_sha256=_string(item, "availability_calendar_sha256"),
            attestation_rule_version=_string(item, "attestation_rule_version"),
        )
        if item["row_attestation_id"] != attestation.row_attestation_id:
            raise ProviderEvidenceError("provider row attestation ID does not reproduce")
        return attestation


@dataclass(frozen=True, slots=True)
class ProviderEvidenceUsage:
    identity_case_id: str
    evidence_role: ProviderEvidenceRole
    row_attestation_ids: tuple[str, ...]
    asserted_fields: tuple[str, ...]
    evidence_available_session: date
    usage_rule_version: str = PROVIDER_EVIDENCE_USAGE_RULE_VERSION

    def __post_init__(self) -> None:
        _digest(self.identity_case_id, "identity case ID")
        if not isinstance(self.evidence_role, ProviderEvidenceRole):
            raise ProviderEvidenceError("provider evidence role is invalid")
        raw_ids = tuple(self.row_attestation_ids)
        if not raw_ids or len(set(raw_ids)) != len(raw_ids):
            raise ProviderEvidenceError("usage attestation IDs must be nonempty and unique")
        for value in raw_ids:
            _digest(value, "row attestation ID")
        object.__setattr__(self, "row_attestation_ids", tuple(sorted(raw_ids)))
        raw_fields = tuple(self.asserted_fields)
        if not raw_fields or len(set(raw_fields)) != len(raw_fields):
            raise ProviderEvidenceError("asserted fields must be nonempty and unique")
        for field in raw_fields:
            if not isinstance(field, str) or not _FIELD.fullmatch(field):
                raise ProviderEvidenceError("asserted field name is invalid")
            _reject_leakage_field(field, self.evidence_role)
        object.__setattr__(self, "asserted_fields", tuple(sorted(raw_fields)))
        _native_date(self.evidence_available_session, "evidence available session")
        if self.usage_rule_version != PROVIDER_EVIDENCE_USAGE_RULE_VERSION:
            raise ProviderEvidenceError("unsupported provider evidence usage rule")

    @property
    def usage_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "asserted_fields": list(self.asserted_fields),
            "evidence_available_session": self.evidence_available_session.isoformat(),
            "evidence_role": self.evidence_role.value,
            "identity_case_id": self.identity_case_id,
            "row_attestation_ids": list(self.row_attestation_ids),
            "usage_rule_version": self.usage_rule_version,
        }

    def to_dict(self) -> dict[str, object]:
        return {
            "provider_evidence_usage_schema_version": PROVIDER_EVIDENCE_USAGE_SCHEMA_VERSION,
            "usage_id": self.usage_id,
            **self.logical_payload(),
        }

    @classmethod
    def from_dict(cls, value: object) -> ProviderEvidenceUsage:
        item = _exact_mapping(
            value,
            {
                "asserted_fields",
                "evidence_available_session",
                "evidence_role",
                "identity_case_id",
                "provider_evidence_usage_schema_version",
                "row_attestation_ids",
                "usage_id",
                "usage_rule_version",
            },
            "provider evidence usage",
        )
        if item["provider_evidence_usage_schema_version"] != 1:
            raise ProviderEvidenceError("unsupported provider evidence usage schema")
        try:
            evidence_role = ProviderEvidenceRole(_string(item, "evidence_role"))
        except ValueError as exc:
            raise ProviderEvidenceError("provider evidence role is invalid") from exc
        usage = cls(
            identity_case_id=_string(item, "identity_case_id"),
            evidence_role=evidence_role,
            row_attestation_ids=_string_tuple(item["row_attestation_ids"], "attestation IDs"),
            asserted_fields=_string_tuple(item["asserted_fields"], "asserted fields"),
            evidence_available_session=_parse_date(
                _string(item, "evidence_available_session"),
                "evidence_available_session",
            ),
            usage_rule_version=_string(item, "usage_rule_version"),
        )
        if item["usage_id"] != usage.usage_id:
            raise ProviderEvidenceError("provider evidence usage ID does not reproduce")
        return usage


@dataclass(frozen=True, slots=True)
class S4BounceCaseEvidenceUsage:
    """One exact S4 observation/parent pair occupying one reviewed bounce role."""

    plan_id: str
    plan_sha256: str
    preview_artifact_id: str
    preview_artifact_sha256: str
    identity_case_id: str
    case_snapshot_digest: str
    case_role: str
    role_ordinal: int
    session_date: date
    ticker: str
    observed_composite_figi: str
    source_record_id: str
    asset_observation_attestation_id: str
    universe_membership_attestation_id: str
    evidence_available_session: date
    usage_rule_version: str = S4_BOUNCE_USAGE_RULE_VERSION

    def __post_init__(self) -> None:
        for label, value in (
            ("plan ID", self.plan_id),
            ("plan SHA-256", self.plan_sha256),
            ("preview artifact ID", self.preview_artifact_id),
            ("preview artifact SHA-256", self.preview_artifact_sha256),
            ("identity case ID", self.identity_case_id),
            ("case snapshot digest", self.case_snapshot_digest),
            ("asset observation attestation ID", self.asset_observation_attestation_id),
            ("universe membership attestation ID", self.universe_membership_attestation_id),
        ):
            _digest(value, label)
        if self.case_role not in {"left_outer", "middle", "right_outer"}:
            raise ProviderEvidenceError("S4 bounce usage case role is invalid")
        _native_nonnegative_int(self.role_ordinal, "S4 bounce role ordinal")
        if self.case_role != "middle" and self.role_ordinal != 0:
            raise ProviderEvidenceError("outer S4 bounce role ordinal must be zero")
        _native_date(self.session_date, "S4 bounce usage session")
        if not isinstance(self.ticker, str) or not self.ticker:
            raise ProviderEvidenceError("S4 bounce usage ticker must be nonempty")
        if not re.fullmatch(r"BBG[0-9A-Z]{9}", self.observed_composite_figi):
            raise ProviderEvidenceError("S4 bounce usage Composite FIGI is malformed")
        _digest(self.source_record_id, "S4 bounce source record ID")
        _native_date(self.evidence_available_session, "S4 bounce evidence availability")
        if self.usage_rule_version != S4_BOUNCE_USAGE_RULE_VERSION:
            raise ProviderEvidenceError("unsupported S4 bounce usage rule")

    @property
    def usage_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "asset_observation_attestation_id": self.asset_observation_attestation_id,
            "case_role": self.case_role,
            "case_snapshot_digest": self.case_snapshot_digest,
            "evidence_available_session": self.evidence_available_session.isoformat(),
            "identity_case_id": self.identity_case_id,
            "observed_composite_figi": self.observed_composite_figi,
            "plan_id": self.plan_id,
            "plan_sha256": self.plan_sha256,
            "preview_artifact_id": self.preview_artifact_id,
            "preview_artifact_sha256": self.preview_artifact_sha256,
            "role_ordinal": self.role_ordinal,
            "session_date": self.session_date.isoformat(),
            "source_record_id": self.source_record_id,
            "ticker": self.ticker,
            "universe_membership_attestation_id": self.universe_membership_attestation_id,
            "usage_rule_version": self.usage_rule_version,
        }

    def to_dict(self) -> dict[str, object]:
        return {
            "s4_bounce_usage_schema_version": S4_BOUNCE_USAGE_SCHEMA_VERSION,
            "usage_id": self.usage_id,
            **self.logical_payload(),
        }

    @classmethod
    def from_dict(cls, value: object) -> S4BounceCaseEvidenceUsage:
        item = _exact_mapping(
            value,
            {
                "asset_observation_attestation_id",
                "case_role",
                "case_snapshot_digest",
                "evidence_available_session",
                "identity_case_id",
                "observed_composite_figi",
                "plan_id",
                "plan_sha256",
                "preview_artifact_id",
                "preview_artifact_sha256",
                "role_ordinal",
                "s4_bounce_usage_schema_version",
                "session_date",
                "source_record_id",
                "ticker",
                "universe_membership_attestation_id",
                "usage_id",
                "usage_rule_version",
            },
            "S4 bounce case evidence usage",
        )
        if item["s4_bounce_usage_schema_version"] != S4_BOUNCE_USAGE_SCHEMA_VERSION:
            raise ProviderEvidenceError("unsupported S4 bounce usage schema")
        usage = cls(
            plan_id=_string(item, "plan_id"),
            plan_sha256=_string(item, "plan_sha256"),
            preview_artifact_id=_string(item, "preview_artifact_id"),
            preview_artifact_sha256=_string(item, "preview_artifact_sha256"),
            identity_case_id=_string(item, "identity_case_id"),
            case_snapshot_digest=_string(item, "case_snapshot_digest"),
            case_role=_string(item, "case_role"),
            role_ordinal=_native_nonnegative_int(item["role_ordinal"], "role ordinal"),
            session_date=_parse_date(_string(item, "session_date"), "usage session"),
            ticker=_string(item, "ticker"),
            observed_composite_figi=_string(item, "observed_composite_figi"),
            source_record_id=_string(item, "source_record_id"),
            asset_observation_attestation_id=_string(item, "asset_observation_attestation_id"),
            universe_membership_attestation_id=_string(item, "universe_membership_attestation_id"),
            evidence_available_session=_parse_date(
                _string(item, "evidence_available_session"), "evidence availability"
            ),
            usage_rule_version=_string(item, "usage_rule_version"),
        )
        if item["usage_id"] != usage.usage_id:
            raise ProviderEvidenceError("S4 bounce usage ID does not reproduce")
        return usage


@dataclass(frozen=True, slots=True)
class S4BounceProviderEvidenceManifest:
    """Physically replayed, exact-plan evidence for exactly one bounce case."""

    plan_id: str
    plan_sha256: str
    approval_id: str
    approval_sha256: str
    preview_artifact_id: str
    preview_artifact_sha256: str
    case_snapshot: Mapping[str, object]
    row_attestations: tuple[ProviderRowAttestation, ...]
    usages: tuple[S4BounceCaseEvidenceUsage, ...]
    created_at_utc: datetime
    manifest_available_session: date
    availability_calendar_id: str
    availability_calendar_sha256: str
    source_attested_bounce: bool = True
    manifest_rule_version: str = S4_BOUNCE_MANIFEST_RULE_VERSION

    def __post_init__(self) -> None:
        for label, value in (
            ("plan ID", self.plan_id),
            ("plan SHA-256", self.plan_sha256),
            ("approval ID", self.approval_id),
            ("approval SHA-256", self.approval_sha256),
            ("preview artifact ID", self.preview_artifact_id),
            ("preview artifact SHA-256", self.preview_artifact_sha256),
            ("availability calendar ID", self.availability_calendar_id),
            ("availability calendar SHA-256", self.availability_calendar_sha256),
        ):
            _digest(value, label)
        snapshot = _normalize_mapping(self.case_snapshot, "bounce case snapshot")
        case = _bounce_case_from_snapshot(snapshot)
        object.__setattr__(self, "case_snapshot", MappingProxyType(snapshot))
        object.__setattr__(self, "created_at_utc", _utc_datetime(self.created_at_utc, "created_at"))
        _native_date(self.manifest_available_session, "manifest available session")
        attestations = tuple(self.row_attestations)
        usages = tuple(self.usages)
        if not attestations or not usages:
            raise ProviderEvidenceError("source-attested bounce manifest cannot be empty")
        if len({item.row_attestation_id for item in attestations}) != len(attestations):
            raise ProviderEvidenceError("source-attested bounce repeats an attestation")
        if len({item.usage_id for item in usages}) != len(usages):
            raise ProviderEvidenceError("source-attested bounce repeats a usage")
        object.__setattr__(
            self,
            "row_attestations",
            tuple(sorted(attestations, key=lambda item: item.row_attestation_id)),
        )
        object.__setattr__(self, "usages", tuple(sorted(usages, key=lambda item: item.usage_id)))
        snapshot_digest = stable_digest(snapshot)
        for usage in usages:
            if (
                usage.plan_id != self.plan_id
                or usage.plan_sha256 != self.plan_sha256
                or usage.preview_artifact_id != self.preview_artifact_id
                or usage.preview_artifact_sha256 != self.preview_artifact_sha256
                or usage.identity_case_id != case.identity_case_id
                or usage.case_snapshot_digest != snapshot_digest
            ):
                raise ProviderEvidenceError("S4 bounce usage crosses plan, preview, or case")
        by_id = {item.row_attestation_id: item for item in attestations}
        used: set[str] = set()
        right_universe: ProviderRowAttestation | None = None
        for usage in usages:
            try:
                asset = by_id[usage.asset_observation_attestation_id]
                universe = by_id[usage.universe_membership_attestation_id]
            except KeyError as exc:
                raise ProviderEvidenceError(
                    "S4 bounce usage references absent attestations"
                ) from exc
            _validate_s4_pair(asset, universe)
            _validate_s4_usage_against_pair(usage, asset, universe)
            if usage.case_role == "right_outer":
                if right_universe is not None:
                    raise ProviderEvidenceError("source-attested bounce repeats right boundary")
                right_universe = universe
            used.update((asset.row_attestation_id, universe.row_attestation_id))
        if used != set(by_id):
            raise ProviderEvidenceError("source-attested bounce contains orphan attestations")
        if len(usages) != case.middle_session_count + 2:
            raise ProviderEvidenceError("source-attested bounce has incomplete boundary evidence")
        expected_records = {
            case.left_outer_source_record_id,
            *case.episode_source_record_ids,
            case.right_outer_source_record_id,
        }
        if {item.source_record_id for item in usages} != expected_records:
            raise ProviderEvidenceError("source-attested bounce evidence set differs from case")
        if (
            right_universe is None
            or case.right_evidence_available_session != right_universe.source_available_session
        ):
            raise ProviderEvidenceError(
                "bounce right-side availability differs from physical membership evidence"
            )
        if case.identity_case_available_session != self.manifest_available_session:
            raise ProviderEvidenceError(
                "source-attested case availability must equal manifest availability"
            )
        if self.source_attested_bounce is not True:
            raise ProviderEvidenceError("S4 bounce manifest must be physically source-attested")
        if self.manifest_rule_version != S4_BOUNCE_MANIFEST_RULE_VERSION:
            raise ProviderEvidenceError("unsupported S4 bounce manifest rule")

    @property
    def manifest_id(self) -> str:
        return stable_digest(self.logical_payload())

    @property
    def content(self) -> bytes:
        return _canonical_json_bytes(self.to_dict())

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.content).hexdigest()

    @property
    def relative_path(self) -> str:
        return s4_bounce_provider_evidence_manifest_path(self.manifest_id)

    def logical_payload(self) -> dict[str, object]:
        return {
            "approval_id": self.approval_id,
            "approval_sha256": self.approval_sha256,
            "availability_calendar_id": self.availability_calendar_id,
            "availability_calendar_sha256": self.availability_calendar_sha256,
            "case_snapshot": dict(self.case_snapshot),
            "created_at_utc": _format_utc(self.created_at_utc),
            "manifest_available_session": self.manifest_available_session.isoformat(),
            "manifest_rule_version": self.manifest_rule_version,
            "plan_id": self.plan_id,
            "plan_sha256": self.plan_sha256,
            "preview_artifact_id": self.preview_artifact_id,
            "preview_artifact_sha256": self.preview_artifact_sha256,
            "row_attestations": [item.to_dict() for item in self.row_attestations],
            "source_attested_bounce": True,
            "usages": [item.to_dict() for item in self.usages],
        }

    def to_dict(self) -> dict[str, object]:
        return {
            "artifact_kind": "s7_s4_bounce_source_attested_manifest",
            "manifest_id": self.manifest_id,
            "s4_bounce_manifest_schema_version": S4_BOUNCE_MANIFEST_SCHEMA_VERSION,
            **self.logical_payload(),
        }

    @classmethod
    def from_dict(cls, value: object) -> S4BounceProviderEvidenceManifest:
        item = _exact_mapping(
            value,
            {
                "approval_id",
                "approval_sha256",
                "artifact_kind",
                "availability_calendar_id",
                "availability_calendar_sha256",
                "case_snapshot",
                "created_at_utc",
                "manifest_available_session",
                "manifest_id",
                "manifest_rule_version",
                "plan_id",
                "plan_sha256",
                "preview_artifact_id",
                "preview_artifact_sha256",
                "row_attestations",
                "s4_bounce_manifest_schema_version",
                "source_attested_bounce",
                "usages",
            },
            "S4 bounce provider evidence manifest",
        )
        if (
            item["artifact_kind"] != "s7_s4_bounce_source_attested_manifest"
            or item["s4_bounce_manifest_schema_version"] != S4_BOUNCE_MANIFEST_SCHEMA_VERSION
        ):
            raise ProviderEvidenceError("unsupported S4 bounce manifest schema")
        manifest = cls(
            plan_id=_string(item, "plan_id"),
            plan_sha256=_string(item, "plan_sha256"),
            approval_id=_string(item, "approval_id"),
            approval_sha256=_string(item, "approval_sha256"),
            preview_artifact_id=_string(item, "preview_artifact_id"),
            preview_artifact_sha256=_string(item, "preview_artifact_sha256"),
            case_snapshot=_mapping(item["case_snapshot"], "case snapshot"),
            row_attestations=tuple(
                ProviderRowAttestation.from_dict(row)
                for row in _array(item["row_attestations"], "row attestations")
            ),
            usages=tuple(
                S4BounceCaseEvidenceUsage.from_dict(row)
                for row in _array(item["usages"], "S4 bounce usages")
            ),
            created_at_utc=_parse_utc(_string(item, "created_at_utc")),
            manifest_available_session=_parse_date(
                _string(item, "manifest_available_session"), "manifest availability"
            ),
            availability_calendar_id=_string(item, "availability_calendar_id"),
            availability_calendar_sha256=_string(item, "availability_calendar_sha256"),
            source_attested_bounce=item["source_attested_bounce"],
            manifest_rule_version=_string(item, "manifest_rule_version"),
        )
        if item["manifest_id"] != manifest.manifest_id:
            raise ProviderEvidenceError("S4 bounce manifest ID does not reproduce")
        return manifest


@dataclass(frozen=True, slots=True)
class ProviderEvidenceVerificationManifest:
    six_release_binding_id: str
    availability_calendar_id: str
    availability_calendar_sha256: str
    created_at_utc: datetime
    manifest_available_session: date
    row_attestations: tuple[ProviderRowAttestation, ...]
    usages: tuple[ProviderEvidenceUsage, ...]
    manifest_rule_version: str = PROVIDER_EVIDENCE_MANIFEST_RULE_VERSION

    def __post_init__(self) -> None:
        if self.six_release_binding_id != S7_SIX_RELEASE_BINDING_ID:
            raise ProviderEvidenceError("provider evidence manifest has the wrong source binding")
        _digest(self.availability_calendar_id, "availability calendar ID")
        _digest(self.availability_calendar_sha256, "availability calendar SHA-256")
        object.__setattr__(self, "created_at_utc", _utc_datetime(self.created_at_utc, "created_at"))
        _native_date(self.manifest_available_session, "manifest available session")
        attestations = tuple(self.row_attestations)
        usages = tuple(self.usages)
        if not attestations or not usages:
            raise ProviderEvidenceError("provider evidence manifest cannot be empty")
        if len({item.row_attestation_id for item in attestations}) != len(attestations):
            raise ProviderEvidenceError("provider evidence manifest repeats an attestation")
        if len({item.locator for item in attestations}) != len(attestations):
            raise ProviderEvidenceError("provider evidence manifest repeats a physical row locator")
        if len({item.usage_id for item in usages}) != len(usages):
            raise ProviderEvidenceError("provider evidence manifest repeats a usage")
        object.__setattr__(
            self,
            "row_attestations",
            tuple(sorted(attestations, key=lambda item: item.row_attestation_id)),
        )
        object.__setattr__(self, "usages", tuple(sorted(usages, key=lambda item: item.usage_id)))
        if any(
            item.six_release_binding_id != self.six_release_binding_id
            or item.availability_calendar_id != self.availability_calendar_id
            or item.availability_calendar_sha256 != self.availability_calendar_sha256
            for item in attestations
        ):
            raise ProviderEvidenceError("row attestation release/calendar binding drifted")
        by_id = {item.row_attestation_id: item for item in attestations}
        used: set[str] = set()
        case_by_attestation: dict[str, str] = {}
        for usage in usages:
            selected = []
            for attestation_id in usage.row_attestation_ids:
                try:
                    selected.append(by_id[attestation_id])
                except KeyError as exc:
                    raise ProviderEvidenceError(
                        "usage references an absent row attestation"
                    ) from exc
                previous_case = case_by_attestation.setdefault(
                    attestation_id, usage.identity_case_id
                )
                if previous_case != usage.identity_case_id:
                    raise ProviderEvidenceError(
                        "physical provider evidence was replayed across cases"
                    )
                used.add(attestation_id)
            _validate_usage(usage, tuple(selected))
        if used != set(by_id):
            raise ProviderEvidenceError("provider evidence manifest contains orphan attestations")
        if self.manifest_rule_version != PROVIDER_EVIDENCE_MANIFEST_RULE_VERSION:
            raise ProviderEvidenceError("unsupported provider evidence manifest rule")

    @property
    def row_attestation_set_digest(self) -> str:
        return stable_digest([item.row_attestation_id for item in self.row_attestations])

    @property
    def usage_set_digest(self) -> str:
        return stable_digest([item.usage_id for item in self.usages])

    @property
    def case_set_digest(self) -> str:
        return stable_digest(sorted({item.identity_case_id for item in self.usages}))

    @property
    def manifest_id(self) -> str:
        return stable_digest(self.logical_payload())

    @property
    def content(self) -> bytes:
        return _canonical_json_bytes(self.to_dict())

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.content).hexdigest()

    @property
    def relative_path(self) -> str:
        return provider_evidence_manifest_path(self.manifest_id)

    def logical_payload(self) -> dict[str, object]:
        return {
            "availability_calendar_id": self.availability_calendar_id,
            "availability_calendar_sha256": self.availability_calendar_sha256,
            "case_set_digest": self.case_set_digest,
            "created_at_utc": _format_utc(self.created_at_utc),
            "manifest_available_session": self.manifest_available_session.isoformat(),
            "manifest_rule_version": self.manifest_rule_version,
            "row_attestation_set_digest": self.row_attestation_set_digest,
            "row_attestations": [item.to_dict() for item in self.row_attestations],
            "six_release_binding_id": self.six_release_binding_id,
            "source_attested_bounce": False,
            "usage_set_digest": self.usage_set_digest,
            "usages": [item.to_dict() for item in self.usages],
        }

    def to_dict(self) -> dict[str, object]:
        return {
            "artifact_kind": "s7_provider_evidence_verification_manifest",
            "manifest_id": self.manifest_id,
            "provider_evidence_manifest_schema_version": (
                PROVIDER_EVIDENCE_MANIFEST_SCHEMA_VERSION
            ),
            **self.logical_payload(),
        }

    @classmethod
    def from_dict(cls, value: object) -> ProviderEvidenceVerificationManifest:
        item = _exact_mapping(
            value,
            {
                "artifact_kind",
                "availability_calendar_id",
                "availability_calendar_sha256",
                "case_set_digest",
                "created_at_utc",
                "manifest_available_session",
                "manifest_id",
                "manifest_rule_version",
                "provider_evidence_manifest_schema_version",
                "row_attestation_set_digest",
                "row_attestations",
                "six_release_binding_id",
                "source_attested_bounce",
                "usage_set_digest",
                "usages",
            },
            "provider evidence verification manifest",
        )
        if (
            item["artifact_kind"] != "s7_provider_evidence_verification_manifest"
            or item["provider_evidence_manifest_schema_version"] != 1
            or item["source_attested_bounce"] is not False
        ):
            raise ProviderEvidenceError("unsupported provider evidence manifest schema")
        raw_attestations = _array(item["row_attestations"], "row attestations")
        raw_usages = _array(item["usages"], "usages")
        manifest = cls(
            six_release_binding_id=_string(item, "six_release_binding_id"),
            availability_calendar_id=_string(item, "availability_calendar_id"),
            availability_calendar_sha256=_string(item, "availability_calendar_sha256"),
            created_at_utc=_parse_utc(_string(item, "created_at_utc")),
            manifest_available_session=_parse_date(
                _string(item, "manifest_available_session"),
                "manifest_available_session",
            ),
            row_attestations=tuple(
                ProviderRowAttestation.from_dict(row) for row in raw_attestations
            ),
            usages=tuple(ProviderEvidenceUsage.from_dict(row) for row in raw_usages),
            manifest_rule_version=_string(item, "manifest_rule_version"),
        )
        expected = manifest.to_dict()
        for field in (
            "case_set_digest",
            "manifest_id",
            "row_attestation_set_digest",
            "usage_set_digest",
        ):
            if item[field] != expected[field]:
                raise ProviderEvidenceError(f"provider evidence manifest {field} drifted")
        return manifest


def attest_provider_row(
    source_batch: IdentitySourceBatch,
    *,
    row_index_in_batch: int,
    calendar: XNYSCalendarArtifact,
) -> ProviderRowAttestation:
    """Attest one row from a concrete, full-schema physical S7 source batch."""

    return attest_provider_rows(
        source_batch,
        row_indices_in_batch=(row_index_in_batch,),
        calendar=calendar,
    )[0]


def attest_provider_rows(
    source_batch: IdentitySourceBatch,
    *,
    row_indices_in_batch: Sequence[int],
    calendar: XNYSCalendarArtifact,
) -> tuple[ProviderRowAttestation, ...]:
    """Attest selected rows through a bounded physical Parquet iterator.

    No path, checksum, release, locator, row, or dataset claim is accepted from the
    caller.  All such values are derived from the exact ``IdentitySourceBatch`` and
    are checked against its physical Parquet bytes.  A later source-attested bounce
    manifest must additionally replay this locator through an independently opened
    exact six-release bundle.
    """

    if type(source_batch) is not IdentitySourceBatch:
        raise ProviderEvidenceError("provider attestation requires an official source batch")
    try:
        source_batch.require_official()
    except IdentitySourceError as exc:
        raise ProviderEvidenceError(
            "provider attestation requires an official source batch"
        ) from exc
    indices = tuple(row_indices_in_batch)
    if not indices or len(set(indices)) != len(indices):
        raise ProviderEvidenceError("provider batch row indices must be nonempty and unique")
    for row_index in indices:
        _native_nonnegative_int(row_index, "row index in batch")
        if row_index >= source_batch.batch.num_rows:
            raise ProviderEvidenceError("row index is outside the official source batch")
    artifact = source_batch.artifact
    dataset = artifact.table
    try:
        contract = _CONTRACTS[dataset]
        pin = S7_SOURCE_PINS[dataset]
    except KeyError as exc:
        raise ProviderEvidenceError("provider row dataset is outside the exact profile") from exc
    if not source_batch.batch.schema.equals(contract.arrow_schema):
        raise ProviderEvidenceError("provider attestation requires the full exact table schema")
    _validate_official_artifact(artifact, pin=pin, contract=contract)
    physical_indices = tuple(source_batch.row_index_in_group + item for item in indices)
    physical_rows = _read_physical_rows_bounded(
        artifact.path,
        row_group=source_batch.row_group,
        row_indices=physical_indices,
    )
    attestations: list[ProviderRowAttestation] = []
    for row_index, physical_index, physical_row in zip(
        indices,
        physical_indices,
        physical_rows,
        strict=True,
    ):
        batch_row = source_batch.batch.slice(row_index, 1).to_pylist()[0]
        if batch_row != physical_row:
            raise ProviderEvidenceError(
                "source batch row differs from its physical Parquet locator"
            )
        attestations.append(
            _attest_provider_physical_row(
                artifact=artifact,
                parquet_row_group=source_batch.row_group,
                row_index_in_row_group=physical_index,
                row=physical_row,
                calendar=calendar,
            )
        )
    return tuple(attestations)


def _attest_provider_physical_row(
    *,
    artifact: IdentitySourceArtifact,
    parquet_row_group: int,
    row_index_in_row_group: int,
    row: Mapping[str, object],
    calendar: XNYSCalendarArtifact,
) -> ProviderRowAttestation:
    dataset = artifact.table
    contract = _CONTRACTS[dataset]
    rule = _DATASET_RULES[dataset]
    normalized_row = _normalize_contracted_row(row, contract)
    basis = _utc_datetime(row[rule.availability_basis_field], rule.availability_basis_field)
    try:
        available_session, available_at = calendar.first_open_after(basis)
    except XNYSCalendarArtifactError as exc:
        raise ProviderEvidenceError(str(exc)) from exc
    if "source_available_session" in row and row["source_available_session"] != available_session:
        raise ProviderEvidenceError("source available session does not reproduce from the row")
    if (
        "source_available_at_utc" in row
        and _utc_datetime(row["source_available_at_utc"], "source_available_at_utc") != available_at
    ):
        raise ProviderEvidenceError("source available timestamp differs from the calendar")
    if (
        "source_availability_rule" in row
        and row["source_availability_rule"] != rule.availability_rule
    ):
        raise ProviderEvidenceError("source row availability rule differs from dataset rule")
    record_id = row[rule.record_id_field]
    request_id = row["source_request_id"]
    _digest(record_id, rule.record_id_field)
    _digest(request_id, "source_request_id")
    primary_key = {field: normalized_row[field] for field in contract.primary_key}
    full_row_digest = stable_digest(
        {
            "arrow_schema_digest": contract.schema_digest,
            "namespace": "ame_stocks.identity.provider_full_row",
            "row": normalized_row,
            "rule_version": "s7_provider_full_row_digest_v1",
        }
    )
    return ProviderRowAttestation(
        six_release_binding_id=S7_SIX_RELEASE_BINDING_ID,
        dataset=dataset,
        release_id=artifact.release_id,
        release_manifest_path=artifact.release_manifest_path,
        release_manifest_sha256=artifact.release_manifest_sha256,
        contract_id=contract.contract_id,
        arrow_schema_digest=contract.schema_digest,
        silver_artifact_path=artifact.ref.path,
        silver_artifact_sha256=artifact.ref.sha256,
        parquet_row_group=parquet_row_group,
        row_index_in_row_group=row_index_in_row_group,
        primary_key=primary_key,
        source_record_id_field=rule.record_id_field,
        source_record_id=str(record_id),
        source_request_id=str(request_id),
        full_row_digest=full_row_digest,
        full_row_snapshot=normalized_row,
        availability_basis_field=rule.availability_basis_field,
        availability_basis_at_utc=basis,
        source_available_session=available_session,
        source_available_at_utc=available_at,
        source_availability_rule=rule.availability_rule,
        availability_calendar_id=calendar.calendar_artifact_id,
        availability_calendar_sha256=calendar.sha256,
    )


def verify_provider_row_attestation(
    attestation: ProviderRowAttestation,
    *,
    data_root: Path,
    calendar: XNYSCalendarArtifact,
) -> ProviderRowAttestation:
    """Replay one attestation from an independently opened exact source bundle."""

    if not isinstance(data_root, Path):
        raise ProviderEvidenceError("provider evidence data_root must be a Path")
    bundle = open_identity_source_bundle(data_root)
    return _replay_provider_row_attestation(attestation, bundle=bundle, calendar=calendar)


def replay_provider_row_attestations_from_official_bundle(
    attestations: Sequence[ProviderRowAttestation],
    *,
    bundle: IdentitySourceBundle,
    calendar: XNYSCalendarArtifact,
) -> tuple[ProviderRowAttestation, ...]:
    """Replay persisted attestations through one already-opened official bundle.

    This is the idempotent-read counterpart to batch attestation. It avoids reopening and
    rehashing all six releases once per bounce case while preserving exact physical row replay.
    """

    return _ProviderReplaySession(bundle=bundle, calendar=calendar).replay(attestations)


def _validate_official_artifact(
    artifact: IdentitySourceArtifact,
    *,
    pin: IdentitySourcePin,
    contract: TableContract,
) -> None:
    if type(artifact) is not IdentitySourceArtifact:
        raise ProviderEvidenceError("provider attestation artifact is not an official artifact")
    try:
        artifact.require_official()
    except IdentitySourceError as exc:
        raise ProviderEvidenceError("provider attestation artifact is not official") from exc
    expected_manifest = f"manifests/silver/releases/release_id={artifact.release_id}.json"
    if (
        artifact.release_id != pin.release_id
        or artifact.release_manifest_path != expected_manifest
        or artifact.release_manifest_sha256 != pin.release_manifest_sha256
        or artifact.ref.table != contract.table
        or artifact.ref.schema_digest != contract.schema_digest
        or artifact.ref.sha256 != sha256_file(artifact.path)
        or artifact.ref.bytes != artifact.path.stat().st_size
    ):
        raise ProviderEvidenceError("source batch artifact is outside the exact release pin")


def _read_physical_rows_bounded(
    path: Path,
    *,
    row_group: int,
    row_indices: Sequence[int],
) -> tuple[dict[str, object], ...]:
    """Read exact row-group locators without ever materializing the whole row group."""

    _native_nonnegative_int(row_group, "physical replay row group")
    indices = tuple(row_indices)
    if not indices or len(set(indices)) != len(indices):
        raise ProviderEvidenceError("physical replay row indices must be nonempty and unique")
    for item in indices:
        _native_nonnegative_int(item, "physical replay row index")
    try:
        parquet = pq.ParquetFile(path)
        if row_group >= parquet.num_row_groups:
            raise ProviderEvidenceError("physical replay row group is outside Parquet")
        row_count = parquet.metadata.row_group(row_group).num_rows
        if any(item >= row_count for item in indices):
            raise ProviderEvidenceError("physical replay row index is outside Parquet")
        wanted = set(indices)
        found: dict[int, dict[str, object]] = {}
        offset = 0
        for batch in parquet.iter_batches(
            batch_size=PHYSICAL_REPLAY_BATCH_SIZE,
            row_groups=(row_group,),
            use_threads=False,
        ):
            end = offset + batch.num_rows
            selected = sorted(item for item in wanted if offset <= item < end)
            for item in selected:
                found[item] = batch.slice(item - offset, 1).to_pylist()[0]
            wanted.difference_update(selected)
            if not wanted:
                break
            offset = end
    except (OSError, pa.ArrowException) as exc:
        raise ProviderEvidenceError("cannot replay bounded physical Parquet rows") from exc
    if wanted:
        raise ProviderEvidenceError("physical replay did not reach every row locator")
    return tuple(found[item] for item in indices)


class _ProviderReplaySession:
    """Batch and memoize exact physical replays within one trusted operation.

    A detector case can share outer rows with adjacent cases.  Reopening the release,
    hashing the same Parquet artifact, and scanning from the start of its row group for
    every occurrence would turn a bounded preview into an avoidable multiplicative
    workload.  This session verifies each artifact once and reads all requested rows in
    a row group in one bounded batch walk.  The cache never substitutes for verification
    across processes or independently opened operations.
    """

    __slots__ = (
        "_artifact_maps",
        "_bundle",
        "_calendar",
        "_verified_artifacts",
        "_verified_attestations",
    )

    def __init__(
        self,
        *,
        bundle: IdentitySourceBundle,
        calendar: XNYSCalendarArtifact,
    ) -> None:
        if type(bundle) is not IdentitySourceBundle:
            raise ProviderEvidenceError("provider replay requires the exact source bundle")
        try:
            bundle.require_official()
        except IdentitySourceError as exc:
            raise ProviderEvidenceError(
                "provider replay requires an official source bundle"
            ) from exc
        if type(calendar) is not XNYSCalendarArtifact:
            raise ProviderEvidenceError("provider replay requires the exact calendar")
        self._bundle = bundle
        self._calendar = calendar
        self._artifact_maps: dict[str, dict[str, IdentitySourceArtifact]] = {}
        self._verified_artifacts: set[tuple[str, str, str]] = set()
        self._verified_attestations: dict[str, ProviderRowAttestation] = {}

    def require_context(
        self,
        *,
        bundle: IdentitySourceBundle,
        calendar: XNYSCalendarArtifact,
    ) -> None:
        if bundle is not self._bundle or type(calendar) is not XNYSCalendarArtifact:
            raise ProviderEvidenceError("provider replay session crosses source context")
        if (
            calendar.calendar_artifact_id != self._calendar.calendar_artifact_id
            or calendar.sha256 != self._calendar.sha256
        ):
            raise ProviderEvidenceError("provider replay session crosses calendar context")

    def replay(
        self,
        attestations: Sequence[ProviderRowAttestation],
    ) -> tuple[ProviderRowAttestation, ...]:
        selected = tuple(attestations)
        if not selected or any(type(item) is not ProviderRowAttestation for item in selected):
            raise ProviderEvidenceError("provider replay requires concrete row attestations")
        if len({item.row_attestation_id for item in selected}) != len(selected):
            raise ProviderEvidenceError("provider replay attestations must be unique")

        pending: list[ProviderRowAttestation] = []
        for attestation in selected:
            if (
                attestation.availability_calendar_id != self._calendar.calendar_artifact_id
                or attestation.availability_calendar_sha256 != self._calendar.sha256
            ):
                raise ProviderEvidenceError("row attestation calendar binding differs")
            verified = self._verified_attestations.get(attestation.row_attestation_id)
            if verified is None:
                pending.append(attestation)
            elif verified != attestation:
                raise ProviderEvidenceError("row attestation ID resolves to different content")

        grouped: dict[tuple[str, str, int], list[ProviderRowAttestation]] = {}
        for attestation in pending:
            key = (
                attestation.dataset,
                attestation.silver_artifact_path,
                attestation.parquet_row_group,
            )
            grouped.setdefault(key, []).append(attestation)

        for (dataset, artifact_path, row_group), group in grouped.items():
            artifact = self._artifact(dataset, artifact_path)
            self._verify_artifact(artifact, dataset=dataset)
            for attestation in group:
                if (
                    artifact.release_id != attestation.release_id
                    or artifact.release_manifest_path != attestation.release_manifest_path
                    or artifact.release_manifest_sha256 != attestation.release_manifest_sha256
                    or artifact.ref.sha256 != attestation.silver_artifact_sha256
                ):
                    raise ProviderEvidenceError("attestation release or artifact binding changed")
            row_indices = tuple(item.row_index_in_row_group for item in group)
            if len(set(row_indices)) != len(row_indices):
                raise ProviderEvidenceError(
                    "different attestations reuse one physical provider row"
                )
            physical_rows = _read_physical_rows_bounded(
                artifact.path,
                row_group=row_group,
                row_indices=row_indices,
            )
            for attestation, physical_row in zip(group, physical_rows, strict=True):
                rebuilt = _attest_provider_physical_row(
                    artifact=artifact,
                    parquet_row_group=row_group,
                    row_index_in_row_group=attestation.row_index_in_row_group,
                    row=physical_row,
                    calendar=self._calendar,
                )
                if rebuilt != attestation:
                    raise ProviderEvidenceError(
                        "physical provider row differs from its attestation"
                    )
                self._verified_attestations[attestation.row_attestation_id] = attestation
        return selected

    def _artifact(self, dataset: str, path: str) -> IdentitySourceArtifact:
        artifact_map = self._artifact_maps.get(dataset)
        if artifact_map is None:
            try:
                artifacts = self._bundle.artifacts(dataset)
            except IdentitySourceError as exc:
                raise ProviderEvidenceError(
                    "attested dataset is absent from the exact source bundle"
                ) from exc
            artifact_map = {item.ref.path: item for item in artifacts}
            if len(artifact_map) != len(artifacts):
                raise ProviderEvidenceError("exact source bundle has duplicate artifact paths")
            self._artifact_maps[dataset] = artifact_map
        artifact = artifact_map.get(path)
        if artifact is None:
            raise ProviderEvidenceError("attested artifact is absent from the exact source bundle")
        return artifact

    def _verify_artifact(self, artifact: IdentitySourceArtifact, *, dataset: str) -> None:
        key = (dataset, artifact.ref.path, artifact.ref.sha256)
        if key in self._verified_artifacts:
            return
        try:
            pin = S7_SOURCE_PINS[dataset]
            contract = _CONTRACTS[dataset]
        except KeyError as exc:
            raise ProviderEvidenceError("attested dataset is outside the S7 source pins") from exc
        _validate_official_artifact(artifact, pin=pin, contract=contract)
        self._verified_artifacts.add(key)


def _replay_provider_row_attestation(
    attestation: ProviderRowAttestation,
    *,
    bundle: IdentitySourceBundle,
    calendar: XNYSCalendarArtifact,
) -> ProviderRowAttestation:
    return _ProviderReplaySession(bundle=bundle, calendar=calendar).replay((attestation,))[0]


def build_s4_bounce_case_evidence_usage(
    case: BounceCase,
    *,
    plan: S7DetectorPreviewPlan,
    preview: BoundedIdentityPreviewArtifact,
    asset_observation: ProviderRowAttestation,
    universe_membership: ProviderRowAttestation,
    calendar: XNYSCalendarArtifact,
) -> S4BounceCaseEvidenceUsage:
    """Bind one exact parent/member pair to a role derived from the plan spine."""

    if type(case) is not BounceCase:
        raise ProviderEvidenceError("S4 bounce usage requires a concrete BounceCase")
    if type(plan) is not S7DetectorPreviewPlan:
        raise ProviderEvidenceError("S4 bounce usage requires a concrete preview plan")
    if type(preview) is not BoundedIdentityPreviewArtifact:
        raise ProviderEvidenceError("S4 bounce usage requires a concrete preview artifact")
    _validate_plan_preview_case(plan, preview, case, calendar)
    _validate_s4_pair(asset_observation, universe_membership)
    role, ordinal, expected_session, expected_figi = _derive_case_role(
        case,
        source_record_id=universe_membership.source_record_id,
        session_spine=_plan_session_spine(plan, calendar),
    )
    usage = S4BounceCaseEvidenceUsage(
        plan_id=plan.plan_id,
        plan_sha256=plan.sha256,
        preview_artifact_id=preview.preview_artifact_id,
        preview_artifact_sha256=preview.sha256,
        identity_case_id=case.identity_case_id,
        case_snapshot_digest=stable_digest(case.to_manifest_dict()),
        case_role=role,
        role_ordinal=ordinal,
        session_date=expected_session,
        ticker=case.ticker,
        observed_composite_figi=expected_figi,
        source_record_id=universe_membership.source_record_id,
        asset_observation_attestation_id=asset_observation.row_attestation_id,
        universe_membership_attestation_id=universe_membership.row_attestation_id,
        evidence_available_session=max(
            asset_observation.source_available_session,
            universe_membership.source_available_session,
        ),
    )
    _validate_s4_usage_against_pair(usage, asset_observation, universe_membership)
    return usage


def build_s4_bounce_provider_evidence_manifest(
    *,
    data_root: Path,
    plan: S7DetectorPreviewPlan,
    approval: S7DetectorPreviewPlanApproval,
    preview: BoundedIdentityPreviewArtifact,
    case: BounceCase,
    attestations: Sequence[ProviderRowAttestation],
    usages: Sequence[S4BounceCaseEvidenceUsage],
    created_at_utc: datetime,
    calendar: XNYSCalendarArtifact,
) -> S4BounceProviderEvidenceManifest:
    """Reject standalone production evidence; only the source-bound runner may mint it."""

    raise ProviderEvidenceError(
        "standalone source-attested evidence building is disabled; use the source-bound runner"
    )


def _build_s4_bounce_provider_evidence_manifest_for_runner(
    *,
    data_root: Path,
    bundle: IdentitySourceBundle,
    plan: S7DetectorPreviewPlan,
    approval: S7DetectorPreviewPlanApproval,
    preview: BoundedIdentityPreviewArtifact,
    case: BounceCase,
    attestations: Sequence[ProviderRowAttestation],
    usages: Sequence[S4BounceCaseEvidenceUsage],
    calendar: XNYSCalendarArtifact,
    _authority: _RunnerEvidenceAuthority,
    _replay_session: _ProviderReplaySession | None = None,
) -> S4BounceProviderEvidenceManifest:
    if type(_authority) is not _RunnerEvidenceAuthority:
        raise ProviderEvidenceError("source-attested evidence lacks runner authority")
    _authority.require(
        data_root=data_root,
        bundle=bundle,
        plan=plan,
        approval=approval,
        calendar=calendar,
    )
    _authority.require_live_write(calendar=calendar)
    manifest = _rebuild_s4_bounce_provider_evidence_manifest_for_completion(
        data_root=data_root,
        bundle=bundle,
        plan=plan,
        approval=approval,
        preview=preview,
        case=case,
        attestations=attestations,
        usages=usages,
        created_at_utc=_authority.created_at_utc,
        calendar=calendar,
        _replay_session=_replay_session,
    )
    if manifest.manifest_available_session != _authority.manifest_available_session:
        raise ProviderEvidenceError("runner evidence availability crosses its live authority")
    return manifest


def _rebuild_s4_bounce_provider_evidence_manifest_for_completion(
    *,
    data_root: Path,
    bundle: IdentitySourceBundle,
    plan: S7DetectorPreviewPlan,
    approval: S7DetectorPreviewPlanApproval,
    preview: BoundedIdentityPreviewArtifact,
    case: BounceCase,
    attestations: Sequence[ProviderRowAttestation],
    usages: Sequence[S4BounceCaseEvidenceUsage],
    created_at_utc: datetime,
    calendar: XNYSCalendarArtifact,
    _replay_session: _ProviderReplaySession | None = None,
) -> S4BounceProviderEvidenceManifest:
    """Reproduce existing completion bytes; this path has no write authority."""

    if not isinstance(data_root, Path):
        raise ProviderEvidenceError("source-attested bounce data_root must be a Path")
    if type(bundle) is not IdentitySourceBundle:
        raise ProviderEvidenceError("source-attested bounce requires the exact source bundle")
    try:
        bundle.require_official()
    except IdentitySourceError as exc:
        raise ProviderEvidenceError("source-attested bounce requires an official bundle") from exc
    if bundle.data_root != data_root.expanduser().resolve():
        raise ProviderEvidenceError("source-attested bounce bundle root differs from data_root")
    if type(approval) is not S7DetectorPreviewPlanApproval:
        raise ProviderEvidenceError("source-attested bounce requires exact plan approval")
    control_store = IdentityPreviewPlanStore(data_root)
    try:
        persisted_plan, _ = control_store.load_plan(
            plan.plan_id,
            expected_sha256=plan.sha256,
        )
        persisted_approval, _ = control_store.load_approval(
            approval.approval_id,
            expected_sha256=approval.sha256,
        )
    except Exception as exc:
        raise ProviderEvidenceError("exact persisted preview controls cannot be verified") from exc
    if persisted_plan != plan or persisted_approval != approval:
        raise ProviderEvidenceError("caller preview controls differ from persisted exact controls")
    if (
        approval.plan_id != plan.plan_id
        or approval.plan_path != plan.relative_path
        or approval.plan_sha256 != plan.sha256
    ):
        raise ProviderEvidenceError("bounce approval does not bind the exact plan")
    try:
        preview_path = safe_relative_path(data_root, preview.relative_path)
    except ArtifactError as exc:
        raise ProviderEvidenceError(str(exc)) from exc
    if (
        not preview_path.is_file()
        or preview_path.is_symlink()
        or sha256_file(preview_path) != preview.sha256
        or preview_path.read_bytes() != preview.content
    ):
        raise ProviderEvidenceError("exact persisted bounded preview cannot be verified")
    _validate_plan_preview_case(plan, preview, case, calendar)
    selected_attestations = tuple(attestations)
    selected_usages = tuple(usages)
    replay_session = _replay_session or _ProviderReplaySession(
        bundle=bundle,
        calendar=calendar,
    )
    if type(replay_session) is not _ProviderReplaySession:
        raise ProviderEvidenceError("source-attested bounce replay session is invalid")
    replay_session.require_context(bundle=bundle, calendar=calendar)
    replay_session.replay(selected_attestations)
    created = _utc_datetime(created_at_utc, "created_at_utc")
    if created < approval.approved_at_utc:
        raise ProviderEvidenceError("bounce manifest predates its exact preview approval")
    latest_evidence_open = max(
        calendar.market_open(item.evidence_available_session) for item in selected_usages
    )
    if created < latest_evidence_open:
        raise ProviderEvidenceError("bounce manifest predates physically replayed evidence")
    try:
        available_session, _ = calendar.first_open_after(created)
    except XNYSCalendarArtifactError as exc:
        raise ProviderEvidenceError(str(exc)) from exc
    return S4BounceProviderEvidenceManifest(
        plan_id=plan.plan_id,
        plan_sha256=plan.sha256,
        approval_id=approval.approval_id,
        approval_sha256=approval.sha256,
        preview_artifact_id=preview.preview_artifact_id,
        preview_artifact_sha256=preview.sha256,
        case_snapshot=case.to_manifest_dict(),
        row_attestations=selected_attestations,
        usages=selected_usages,
        created_at_utc=created,
        manifest_available_session=available_session,
        availability_calendar_id=calendar.calendar_artifact_id,
        availability_calendar_sha256=calendar.sha256,
    )


def s4_bounce_provider_evidence_manifest_path(manifest_id: str) -> str:
    _digest(manifest_id, "S4 bounce provider evidence manifest ID")
    return (
        "manifests/silver/identity/detector-preview-provider-evidence/"
        f"manifest_id={manifest_id}.json"
    )


def write_s4_bounce_provider_evidence_manifest(
    root: Path,
    manifest: S4BounceProviderEvidenceManifest,
) -> dict[str, object]:
    """Reject standalone writes that are not owned by the source-bound runner."""

    raise ProviderEvidenceError(
        "standalone source-attested evidence writing is disabled; use the source-bound runner"
    )


def _write_s4_bounce_provider_evidence_manifest_from_official_bundle(
    root: Path,
    manifest: S4BounceProviderEvidenceManifest,
    *,
    bundle: IdentitySourceBundle,
    calendar: XNYSCalendarArtifact,
    _authority: _RunnerEvidenceAuthority,
    _replay_session: _ProviderReplaySession | None = None,
) -> dict[str, object]:
    """Validate against one already-opened official bundle, then write immutably."""

    if type(manifest) is not S4BounceProviderEvidenceManifest:
        raise ProviderEvidenceError("S4 bounce write requires its exact manifest type")
    if type(_authority) is not _RunnerEvidenceAuthority:
        raise ProviderEvidenceError("S4 bounce write lacks runner authority")
    _verify_s4_manifest_write_inputs(
        root,
        manifest,
        bundle=bundle,
        calendar=calendar,
        _authority=_authority,
        _replay_session=_replay_session,
    )
    try:
        target = safe_relative_path(root, manifest.relative_path)
        stored = write_bytes_immutable(root, target, manifest.content)
    except ArtifactError as exc:
        raise ProviderEvidenceError(str(exc)) from exc
    return {
        **stored,
        "manifest_id": manifest.manifest_id,
        "media_type": "application/json",
    }


def _verify_s4_manifest_write_inputs(
    root: Path,
    manifest: S4BounceProviderEvidenceManifest,
    *,
    bundle: IdentitySourceBundle,
    calendar: XNYSCalendarArtifact,
    _authority: _RunnerEvidenceAuthority,
    _replay_session: _ProviderReplaySession | None = None,
) -> None:
    if type(bundle) is not IdentitySourceBundle:
        raise ProviderEvidenceError("S4 bounce write requires the exact source bundle")
    try:
        bundle.require_official()
    except IdentitySourceError as exc:
        raise ProviderEvidenceError("S4 bounce write requires an official source bundle") from exc
    if bundle.data_root != root.expanduser().resolve():
        raise ProviderEvidenceError("S4 bounce write bundle root differs")
    store = IdentityPreviewPlanStore(root)
    try:
        plan, _ = store.load_plan(manifest.plan_id, expected_sha256=manifest.plan_sha256)
        approval, _ = store.load_approval(
            manifest.approval_id,
            expected_sha256=manifest.approval_sha256,
        )
        preview = _read_exact_bounded_preview(
            root,
            preview_artifact_id=manifest.preview_artifact_id,
            expected_sha256=manifest.preview_artifact_sha256,
        )
    except Exception as exc:
        raise ProviderEvidenceError("S4 bounce write controls cannot be reproduced") from exc
    _authority.require(
        data_root=root,
        bundle=bundle,
        plan=plan,
        approval=approval,
        calendar=calendar,
    )
    _authority.require_live_write(calendar=calendar)
    if (
        approval.plan_id != plan.plan_id
        or approval.plan_sha256 != plan.sha256
        or manifest.created_at_utc < approval.approved_at_utc
        or manifest.created_at_utc != _authority.created_at_utc
        or manifest.manifest_available_session != _authority.manifest_available_session
        or manifest.availability_calendar_id != calendar.calendar_artifact_id
        or manifest.availability_calendar_sha256 != calendar.sha256
    ):
        raise ProviderEvidenceError("S4 bounce write crosses approval or calendar controls")
    case = _bounce_case_from_snapshot(manifest.case_snapshot)
    _validate_plan_preview_case(plan, preview, case, calendar)
    try:
        calendar.require_first_open_session(
            manifest.created_at_utc,
            manifest.manifest_available_session,
            label="S4 bounce provider evidence availability",
        )
    except XNYSCalendarArtifactError as exc:
        raise ProviderEvidenceError(str(exc)) from exc
    latest_evidence_open = max(
        calendar.market_open(item.evidence_available_session) for item in manifest.usages
    )
    if manifest.created_at_utc < latest_evidence_open:
        raise ProviderEvidenceError("S4 bounce write predates its physical evidence")
    replay_session = _replay_session or _ProviderReplaySession(
        bundle=bundle,
        calendar=calendar,
    )
    if type(replay_session) is not _ProviderReplaySession:
        raise ProviderEvidenceError("S4 bounce write replay session is invalid")
    replay_session.require_context(bundle=bundle, calendar=calendar)
    replay_session.replay(manifest.row_attestations)


def read_s4_bounce_provider_evidence_manifest(
    root: Path,
    *,
    manifest_id: str,
    expected_sha256: str,
    plan_id: str,
    expected_plan_sha256: str,
    approval_id: str,
    expected_approval_sha256: str,
    preview_artifact_id: str,
    expected_preview_sha256: str,
    calendar: XNYSCalendarArtifact,
) -> S4BounceProviderEvidenceManifest:
    """Reject evidence reads until an exact revalidated completion is required."""

    raise ProviderEvidenceError(
        "standalone source-attested evidence reading is disabled; use the source-bound "
        "completion revalidator"
    )


def _read_exact_bounded_preview(
    root: Path,
    *,
    preview_artifact_id: str,
    expected_sha256: str,
) -> BoundedIdentityPreviewArtifact:
    relative = (
        "manifests/silver/identity-bounce-bounded-previews/"
        f"preview_artifact_id={preview_artifact_id}/manifest.json"
    )
    try:
        path = safe_relative_path(root, relative)
    except ArtifactError as exc:
        raise ProviderEvidenceError(str(exc)) from exc
    if not path.is_file() or path.is_symlink() or sha256_file(path) != expected_sha256:
        raise ProviderEvidenceError("exact bounded preview artifact is unavailable")
    content = path.read_bytes()
    try:
        raw = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProviderEvidenceError("bounded preview artifact is not valid JSON") from exc
    preview = BoundedIdentityPreviewArtifact(
        preview_artifact_id=preview_artifact_id,
        sha256=expected_sha256,
        content=content,
        document=MappingProxyType(raw),
    )
    if preview.relative_path != relative:
        raise ProviderEvidenceError("bounded preview artifact path is not canonical")
    return preview


def build_provider_evidence_usage(
    *,
    identity_case_id: str,
    evidence_role: ProviderEvidenceRole,
    attestations: Sequence[ProviderRowAttestation],
    asserted_fields: Sequence[str],
) -> ProviderEvidenceUsage:
    """Build one typed, case-bound usage from exact row attestations."""

    selected = tuple(attestations)
    if not selected:
        raise ProviderEvidenceError("provider evidence usage has no physical rows")
    if len({item.row_attestation_id for item in selected}) != len(selected):
        raise ProviderEvidenceError("provider evidence usage repeats an attestation")
    usage = ProviderEvidenceUsage(
        identity_case_id=identity_case_id,
        evidence_role=evidence_role,
        row_attestation_ids=tuple(item.row_attestation_id for item in selected),
        asserted_fields=tuple(asserted_fields),
        evidence_available_session=max(item.source_available_session for item in selected),
    )
    _validate_usage(usage, selected)
    return usage


def build_provider_evidence_manifest(
    *,
    attestations: Sequence[ProviderRowAttestation],
    usages: Sequence[ProviderEvidenceUsage],
    created_at_utc: datetime,
    calendar: XNYSCalendarArtifact,
) -> ProviderEvidenceVerificationManifest:
    """Build deterministic manifest bytes without authorizing production ingestion."""

    created = _utc_datetime(created_at_utc, "created_at_utc")
    selected = tuple(attestations)
    if selected:
        try:
            latest_source_open = max(
                calendar.market_open(item.source_available_session) for item in selected
            )
        except XNYSCalendarArtifactError as exc:
            raise ProviderEvidenceError(str(exc)) from exc
        if created < latest_source_open:
            raise ProviderEvidenceError("provider evidence manifest predates its source evidence")
    try:
        available_session, _ = calendar.first_open_after(created)
    except XNYSCalendarArtifactError as exc:
        raise ProviderEvidenceError(str(exc)) from exc
    return ProviderEvidenceVerificationManifest(
        six_release_binding_id=S7_SIX_RELEASE_BINDING_ID,
        availability_calendar_id=calendar.calendar_artifact_id,
        availability_calendar_sha256=calendar.sha256,
        created_at_utc=created,
        manifest_available_session=available_session,
        row_attestations=selected,
        usages=tuple(usages),
    )


def provider_evidence_manifest_path(manifest_id: str) -> str:
    _digest(manifest_id, "provider evidence manifest ID")
    return (
        "manifests/silver/identity/provider-evidence/"
        f"provider_evidence_manifest_id={manifest_id}.json"
    )


def write_provider_evidence_manifest(
    root: Path,
    manifest: ProviderEvidenceVerificationManifest,
) -> dict[str, object]:
    """Write exact canonical bytes idempotently; no latest pointer is created."""

    target = root.expanduser().resolve() / manifest.relative_path
    try:
        stored = write_bytes_immutable(root, target, manifest.content)
    except ArtifactError as exc:
        raise ProviderEvidenceError(str(exc)) from exc
    return {
        **stored,
        "manifest_id": manifest.manifest_id,
        "media_type": "application/json",
    }


def read_provider_evidence_manifest(
    root: Path,
    *,
    manifest_id: str,
    expected_sha256: str,
    calendar: XNYSCalendarArtifact,
) -> ProviderEvidenceVerificationManifest:
    """Read one exact ID/SHA artifact and revalidate canonical bytes and availability."""

    _digest(manifest_id, "provider evidence manifest ID")
    _digest(expected_sha256, "expected provider evidence manifest SHA-256")
    relative = provider_evidence_manifest_path(manifest_id)
    try:
        path = safe_relative_path(root, relative)
    except ArtifactError as exc:
        raise ProviderEvidenceError(str(exc)) from exc
    if not path.is_file() or path.is_symlink():
        raise ProviderEvidenceError("exact provider evidence manifest is unavailable")
    if sha256_file(path) != expected_sha256:
        raise ProviderEvidenceError("provider evidence manifest checksum mismatch")
    content = path.read_bytes()
    try:
        raw = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProviderEvidenceError("provider evidence manifest is not valid JSON") from exc
    if _canonical_json_bytes(raw) != content:
        raise ProviderEvidenceError("provider evidence manifest bytes are not canonical")
    manifest = ProviderEvidenceVerificationManifest.from_dict(raw)
    if manifest.manifest_id != manifest_id or manifest.sha256 != expected_sha256:
        raise ProviderEvidenceError("provider evidence manifest ID/SHA trust chain failed")
    if (
        manifest.availability_calendar_id != calendar.calendar_artifact_id
        or manifest.availability_calendar_sha256 != calendar.sha256
    ):
        raise ProviderEvidenceError("provider evidence manifest calendar binding differs")
    try:
        calendar.require_first_open_session(
            manifest.created_at_utc,
            manifest.manifest_available_session,
            label="provider evidence manifest availability",
        )
    except XNYSCalendarArtifactError as exc:
        raise ProviderEvidenceError(str(exc)) from exc
    return manifest


def _plan_session_spine(
    plan: S7DetectorPreviewPlan,
    calendar: XNYSCalendarArtifact,
) -> tuple[date, ...]:
    if (
        plan.calendar_artifact_id != calendar.calendar_artifact_id
        or plan.calendar_artifact_sha256 != calendar.sha256
    ):
        raise ProviderEvidenceError("preview plan calendar binding differs")
    spine = tuple(
        item.session_date
        for item in calendar.sessions
        if plan.start_session <= item.session_date <= plan.end_session
    )
    if (
        len(spine) != plan.session_count
        or not spine
        or spine[0] != plan.start_session
        or spine[-1] != plan.end_session
    ):
        raise ProviderEvidenceError("preview plan session spine does not reproduce")
    return spine


def _validate_plan_preview_case(
    plan: S7DetectorPreviewPlan,
    preview: BoundedIdentityPreviewArtifact,
    case: BounceCase,
    calendar: XNYSCalendarArtifact,
) -> None:
    _plan_session_spine(plan, calendar)
    result = _mapping(preview.document.get("result"), "bounded preview result")
    if result.get("six_release_binding_id") != plan.six_release_binding_id:
        raise ProviderEvidenceError("preview source binding differs from plan")
    if case.ticker not in set(result.get("scoped_tickers", [])):
        raise ProviderEvidenceError("bounce case ticker is outside preview scope")
    raw_cases = result.get("cases")
    if not isinstance(raw_cases, list) or raw_cases.count(case.to_manifest_dict()) != 1:
        raise ProviderEvidenceError("BounceCase snapshot is absent or duplicated in preview")
    preview_available = _parse_date(
        _string(result, "preview_manifest_available_session"),
        "preview manifest availability",
    )
    if case.identity_case_available_session != max(
        case.right_evidence_available_session,
        preview_available,
    ):
        raise ProviderEvidenceError("bounce case availability does not reproduce")


def _derive_case_role(
    case: BounceCase,
    *,
    source_record_id: str,
    session_spine: tuple[date, ...],
) -> tuple[str, int, date, str]:
    try:
        start_index = session_spine.index(case.episode_valid_from_session)
        end_index = session_spine.index(case.episode_valid_through_session)
    except ValueError as exc:
        raise ProviderEvidenceError("bounce episode is outside the plan session spine") from exc
    middle_sessions = session_spine[start_index : end_index + 1]
    if (
        len(middle_sessions) != case.middle_session_count
        or start_index == 0
        or end_index + 1 >= len(session_spine)
    ):
        raise ProviderEvidenceError("bounce boundaries do not derive from the plan spine")
    record_ids = (
        case.left_outer_source_record_id,
        *case.episode_source_record_ids,
        case.right_outer_source_record_id,
    )
    if len(set(record_ids)) != len(record_ids):
        raise ProviderEvidenceError("bounce case reuses a source record across roles")
    if source_record_id == case.left_outer_source_record_id:
        return "left_outer", 0, session_spine[start_index - 1], case.left_outer_composite_figi
    if source_record_id == case.right_outer_source_record_id:
        return "right_outer", 0, session_spine[end_index + 1], case.right_outer_composite_figi
    try:
        ordinal = case.episode_source_record_ids.index(source_record_id)
    except ValueError as exc:
        raise ProviderEvidenceError("source record is outside the exact BounceCase") from exc
    return "middle", ordinal, middle_sessions[ordinal], case.middle_observed_composite_figi


def _validate_s4_observation_parent_pair(
    asset: ProviderRowAttestation,
    universe: ProviderRowAttestation,
) -> None:
    if asset.dataset != "asset_observation_daily" or universe.dataset != "universe_source_daily":
        raise ProviderEvidenceError("S4 parent validation requires asset plus universe rows")
    left = asset.full_row_snapshot
    right = universe.full_row_snapshot
    lineage_pairs = (
        ("source_record_id", "selected_source_record_id"),
        ("source_capture_at_utc", "selected_source_capture_at_utc"),
        ("source_request_id", "source_request_id"),
        ("source_provider_request_id", "source_provider_request_id"),
        ("source_artifact_sha256", "source_artifact_sha256"),
        ("source_page_sequence", "source_page_sequence"),
        ("source_row_ordinal", "source_row_ordinal"),
        ("source_row_hash", "source_row_hash"),
    )
    common_fields = (
        "session_date",
        "ticker",
        "type_code",
        "name",
        "market",
        "locale",
        "primary_exchange_mic",
        "currency_name",
        "cik",
        "composite_figi",
        "share_class_figi",
        "delisted_at_utc",
        "last_updated_at_utc",
    )
    activity = left["provider_active"]
    request_route_field = (
        "active_source_request_id" if activity is True else "inactive_source_request_id"
    )
    if (
        any(left[a] != right[b] for a, b in lineage_pairs)
        or any(left[field] != right[field] for field in common_fields)
        or type(activity) is not bool
        or left["requested_active"] is not activity
        or right["active_on_date"] is not activity
        or right[request_route_field] != left["source_request_id"]
        or asset.source_record_id != universe.source_record_id
        or asset.source_request_id != universe.source_request_id
    ):
        raise ProviderEvidenceError("S4 universe row does not exactly pair to its asset parent")


def _validate_s4_pair(
    asset: ProviderRowAttestation,
    universe: ProviderRowAttestation,
) -> None:
    """Require one exact active pair before it can support bounce-case evidence."""

    _validate_s4_observation_parent_pair(asset, universe)
    if (
        asset.full_row_snapshot["provider_active"] is not True
        or universe.full_row_snapshot["active_on_date"] is not True
    ):
        raise ProviderEvidenceError("S4 bounce evidence requires active membership")


def validate_s4_observation_parent_pair(
    asset: ProviderRowAttestation,
    universe: ProviderRowAttestation,
) -> None:
    """Accept one exact active or inactive S4 observation/parent pair."""

    _validate_s4_observation_parent_pair(asset, universe)


def validate_s4_membership_parent_pair(
    asset: ProviderRowAttestation,
    universe: ProviderRowAttestation,
) -> None:
    """Fail closed unless an active S4 membership matches its selected parent."""

    _validate_s4_pair(asset, universe)


def _validate_s4_usage_against_pair(
    usage: S4BounceCaseEvidenceUsage,
    asset: ProviderRowAttestation,
    universe: ProviderRowAttestation,
) -> None:
    row = universe.full_row_snapshot
    if (
        usage.asset_observation_attestation_id != asset.row_attestation_id
        or usage.universe_membership_attestation_id != universe.row_attestation_id
        or usage.source_record_id != universe.source_record_id
        or usage.session_date.isoformat() != row["session_date"]
        or usage.ticker != row["ticker"]
        or usage.observed_composite_figi != row["composite_figi"]
        or usage.evidence_available_session
        != max(asset.source_available_session, universe.source_available_session)
    ):
        raise ProviderEvidenceError("S4 usage role fields differ from its full physical rows")


def _bounce_case_from_snapshot(snapshot: Mapping[str, object]) -> BounceCase:
    expected = {
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
    item = _exact_mapping(snapshot, expected, "BounceCase snapshot")
    if item["detector_disposition"] != "review_required_no_auto_decision":
        raise ProviderEvidenceError("BounceCase snapshot disposition changed")
    case = BounceCase(
        identity_case_id=_string(item, "identity_case_id"),
        six_release_binding_id=_string(item, "six_release_binding_id"),
        ticker=_string(item, "ticker"),
        left_outer_composite_figi=_string(item, "left_outer_composite_figi"),
        middle_observed_composite_figi=_string(item, "middle_observed_composite_figi"),
        right_outer_composite_figi=_string(item, "right_outer_composite_figi"),
        left_outer_source_record_id=_string(item, "left_outer_source_record_id"),
        right_outer_source_record_id=_string(item, "right_outer_source_record_id"),
        episode_valid_from_session=_parse_date(
            _string(item, "episode_valid_from_session"), "episode_valid_from_session"
        ),
        episode_valid_through_session=_parse_date(
            _string(item, "episode_valid_through_session"), "episode_valid_through_session"
        ),
        episode_source_record_ids=_string_tuple(
            item["episode_source_record_ids"], "episode source records"
        ),
        episode_source_record_set_digest=_string(item, "episode_source_record_set_digest"),
        middle_session_count=_native_nonnegative_int(
            item["middle_session_count"], "middle session count"
        ),
        session_band=_string(item, "session_band"),
        right_evidence_available_session=_parse_date(
            _string(item, "right_evidence_available_session"), "right evidence availability"
        ),
        identity_case_available_session=_parse_date(
            _string(item, "identity_case_available_session"), "case availability"
        ),
        s5_source_record_ids=_string_tuple(item["s5_source_record_ids"], "S5 source records"),
        s6_source_record_ids=_string_tuple(item["s6_source_record_ids"], "S6 source records"),
        hierarchy_source_record_ids=_string_tuple(
            item["hierarchy_source_record_ids"], "hierarchy source records"
        ),
        reason_codes=_string_tuple(item["reason_codes"], "reason codes"),
    )
    if case.to_manifest_dict() != dict(snapshot):
        raise ProviderEvidenceError("BounceCase snapshot does not reproduce exactly")
    return case


def _validate_usage(
    usage: ProviderEvidenceUsage,
    attestations: tuple[ProviderRowAttestation, ...],
) -> None:
    if not attestations:
        raise ProviderEvidenceError("provider evidence usage has no physical rows")
    actual = Counter(item.dataset for item in attestations)
    expected = _ROLE_DATASETS[usage.evidence_role]
    if actual != expected:
        raise ProviderEvidenceError("provider evidence role uses the wrong typed datasets")
    if set(usage.row_attestation_ids) != {item.row_attestation_id for item in attestations}:
        raise ProviderEvidenceError("provider evidence usage row binding differs")
    if usage.evidence_available_session != max(
        item.source_available_session for item in attestations
    ):
        raise ProviderEvidenceError("provider evidence usage availability does not reproduce")
    allowed = set().union(
        *(_DATASET_RULES[item.dataset].allowed_asserted_fields for item in attestations)
    )
    unknown = sorted(set(usage.asserted_fields) - allowed)
    if unknown:
        raise ProviderEvidenceError(
            f"asserted fields are outside the identity whitelist: {unknown}"
        )
    by_dataset = {item.dataset: item for item in attestations}
    if usage.evidence_role in {
        ProviderEvidenceRole.S4_MEMBERSHIP_OBSERVATION,
        ProviderEvidenceRole.S4_VERSION_SELECTION_CONTROL,
    }:
        left = by_dataset["asset_observation_daily"]
        other_name = (
            "universe_source_daily"
            if usage.evidence_role is ProviderEvidenceRole.S4_MEMBERSHIP_OBSERVATION
            else "asset_observation_version"
        )
        other = by_dataset[other_name]
        if (
            left.source_record_id != other.source_record_id
            or left.source_request_id != other.source_request_id
            or left.primary_key["session_date"] != other.primary_key["session_date"]
        ):
            raise ProviderEvidenceError("S4 evidence rows do not share exact parent lineage")
    if usage.evidence_role is ProviderEvidenceRole.S5_TICKER_CHANGE_CORROBORATION:
        event = by_dataset["ticker_change_event"]
        status = by_dataset["ticker_event_request_status"]
        if event.source_request_id != status.source_request_id:
            raise ProviderEvidenceError("S5 event/status evidence has different requests")


def _normalize_contracted_row(
    row: Mapping[str, object],
    contract: TableContract,
) -> dict[str, object]:
    if not isinstance(row, Mapping) or set(row) != set(contract.arrow_schema.names):
        raise ProviderEvidenceError("provider row fields differ from the exact table contract")
    result: dict[str, object] = {}
    for field in contract.arrow_schema:
        value = row[field.name]
        if value is None:
            if not field.nullable:
                raise ProviderEvidenceError(f"non-nullable provider field is null: {field.name}")
            result[field.name] = None
            continue
        data_type = field.type
        if pa.types.is_string(data_type):
            if not isinstance(value, str):
                raise ProviderEvidenceError(f"provider field is not exact string: {field.name}")
            result[field.name] = value
        elif pa.types.is_boolean(data_type):
            if type(value) is not bool:
                raise ProviderEvidenceError(f"provider field is not native bool: {field.name}")
            result[field.name] = value
        elif pa.types.is_int64(data_type):
            if type(value) is not int:
                raise ProviderEvidenceError(f"provider field is not native int: {field.name}")
            result[field.name] = value
        elif pa.types.is_float64(data_type):
            if type(value) not in {int, float} or not math.isfinite(float(value)):
                raise ProviderEvidenceError(f"provider field is not finite float: {field.name}")
            result[field.name] = float(value)
        elif pa.types.is_date32(data_type):
            _native_date(value, field.name)
            result[field.name] = value.isoformat()
        elif pa.types.is_timestamp(data_type):
            result[field.name] = _format_utc(_utc_datetime(value, field.name))
        else:  # pragma: no cover - the frozen six contracts use only these scalar types
            raise ProviderEvidenceError(f"unsupported provider field type: {field.name}")
    return result


def _reject_leakage_field(field: str, role: ProviderEvidenceRole) -> None:
    if field == "request_outcome" and role is ProviderEvidenceRole.S5_REQUEST_COVERAGE_CONTEXT:
        return
    tokens = set(field.split("_"))
    if tokens.intersection(_LEAKAGE_TOKENS):
        raise ProviderEvidenceError("outcome or backtest field cannot be provider evidence")


def _canonical_json_bytes(value: object) -> bytes:
    try:
        return json.dumps(value, allow_nan=False, indent=2, sort_keys=True).encode("utf-8") + b"\n"
    except (TypeError, ValueError) as exc:
        raise ProviderEvidenceError("provider evidence is not canonical JSON") from exc


def _normalize_mapping(value: object, label: str) -> dict[str, object]:
    item = _mapping(value, label)
    result: dict[str, object] = {}
    for key, raw in item.items():
        if not isinstance(key, str) or not _FIELD.fullmatch(key):
            raise ProviderEvidenceError(f"{label} contains an invalid key")
        result[key] = _normalize_json_value(raw, label)
    return dict(sorted(result.items()))


def _normalize_json_value(value: object, label: str) -> object:
    if value is None or type(value) in {str, bool, int}:
        return value
    if type(value) is float:
        if not math.isfinite(value):
            raise ProviderEvidenceError(f"{label} contains non-finite JSON")
        return value
    if isinstance(value, datetime):
        return _format_utc(_utc_datetime(value, label))
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Mapping):
        return _normalize_mapping(value, label)
    if isinstance(value, (list, tuple)):
        return [_normalize_json_value(item, label) for item in value]
    raise ProviderEvidenceError(f"{label} contains a non-JSON value")


def _relative_path(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise ProviderEvidenceError(f"{label} must be nonempty text")
    path = Path(value)
    if (
        path.is_absolute()
        or path.as_posix() != value
        or any(part in {"", ".", ".."} for part in path.parts)
    ):
        raise ProviderEvidenceError(f"{label} must be a normalized relative path")
    return value


def _digest(value: object, label: str) -> str:
    if not isinstance(value, str) or not _DIGEST.fullmatch(value):
        raise ProviderEvidenceError(f"{label} must be lowercase SHA-256")
    return value


def _native_nonnegative_int(value: object, label: str) -> int:
    if type(value) is not int or value < 0:
        raise ProviderEvidenceError(f"{label} must be a nonnegative native int")
    return value


def _native_date(value: object, label: str) -> date:
    if not isinstance(value, date) or isinstance(value, datetime):
        raise ProviderEvidenceError(f"{label} must be a date")
    return value


def _utc_datetime(value: object, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ProviderEvidenceError(f"{label} must be timezone-aware")
    return value.astimezone(UTC)


def _format_utc(value: datetime) -> str:
    return _utc_datetime(value, "UTC timestamp").isoformat()


def _parse_utc(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ProviderEvidenceError("timestamp is not ISO-8601") from exc
    result = _utc_datetime(parsed, "timestamp")
    if result.isoformat() != value:
        raise ProviderEvidenceError("timestamp is not canonical UTC ISO-8601")
    return result


def _parse_date(value: str, label: str) -> date:
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise ProviderEvidenceError(f"{label} is not an ISO date") from exc
    if parsed.isoformat() != value:
        raise ProviderEvidenceError(f"{label} is not a canonical ISO date")
    return parsed


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _mapping(value: object, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ProviderEvidenceError(f"{label} must be an object")
    return value


def _exact_mapping(
    value: object,
    expected_keys: set[str],
    label: str,
) -> Mapping[str, object]:
    item = _mapping(value, label)
    if set(item) != expected_keys:
        raise ProviderEvidenceError(f"{label} has unexpected or missing fields")
    return item


def _array(value: object, label: str) -> list[object]:
    if not isinstance(value, list):
        raise ProviderEvidenceError(f"{label} must be an array")
    return value


def _string(item: Mapping[str, object], field: str) -> str:
    value = item.get(field)
    if not isinstance(value, str) or not value:
        raise ProviderEvidenceError(f"{field} must be nonempty text")
    return value


def _string_tuple(value: object, label: str) -> tuple[str, ...]:
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise ProviderEvidenceError(f"{label} must be a string array")
    return tuple(value)


__all__ = [
    "PROVIDER_EVIDENCE_MANIFEST_RULE_VERSION",
    "PROVIDER_EVIDENCE_MANIFEST_SCHEMA_VERSION",
    "PROVIDER_EVIDENCE_USAGE_RULE_VERSION",
    "PROVIDER_EVIDENCE_USAGE_SCHEMA_VERSION",
    "PROVIDER_ROW_ATTESTATION_RULE_VERSION",
    "PROVIDER_ROW_ATTESTATION_SCHEMA_VERSION",
    "S4_BOUNCE_MANIFEST_RULE_VERSION",
    "S4_BOUNCE_MANIFEST_SCHEMA_VERSION",
    "S4_BOUNCE_USAGE_RULE_VERSION",
    "S4_BOUNCE_USAGE_SCHEMA_VERSION",
    "ProviderEvidenceError",
    "ProviderEvidenceRole",
    "ProviderEvidenceUsage",
    "ProviderEvidenceVerificationManifest",
    "ProviderRowAttestation",
    "S4BounceCaseEvidenceUsage",
    "S4BounceProviderEvidenceManifest",
    "attest_provider_row",
    "attest_provider_rows",
    "build_provider_evidence_manifest",
    "build_provider_evidence_usage",
    "build_s4_bounce_case_evidence_usage",
    "build_s4_bounce_provider_evidence_manifest",
    "provider_evidence_manifest_path",
    "read_provider_evidence_manifest",
    "read_s4_bounce_provider_evidence_manifest",
    "replay_provider_row_attestations_from_official_bundle",
    "s4_bounce_provider_evidence_manifest_path",
    "validate_s4_membership_parent_pair",
    "validate_s4_observation_parent_pair",
    "verify_provider_row_attestation",
    "write_provider_evidence_manifest",
    "write_s4_bounce_provider_evidence_manifest",
]
