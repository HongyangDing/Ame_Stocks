"""Pinned S7 contracts for exact identity relation registries."""

from __future__ import annotations

import hashlib
import json
from importlib.resources import files
from types import MappingProxyType
from typing import Final

from ame_stocks_api.silver.contracts import (
    ArrowType,
    ColumnSpec,
    QAMetric,
    QAOperator,
    QARule,
    QASeverity,
    QAStatus,
    TableContract,
)


def _column(
    name: str, arrow_type: ArrowType, description: str, *, nullable: bool = False
) -> ColumnSpec:
    return ColumnSpec(
        name=name,
        arrow_type=arrow_type,
        nullable=nullable,
        description=description,
    )


def _critical(check_id: str, description: str) -> QARule:
    return QARule(
        check_id=check_id,
        severity=QASeverity.CRITICAL,
        metric=QAMetric.NUMERATOR,
        operator=QAOperator.EQUAL,
        limit=0.0,
        failure_status=QAStatus.FAILED,
        description=description,
    )


def _high(check_id: str, description: str) -> QARule:
    return QARule(
        check_id=check_id,
        severity=QASeverity.HIGH,
        metric=QAMetric.NUMERATOR,
        operator=QAOperator.EQUAL,
        limit=0.0,
        failure_status=QAStatus.WARNING,
        description=description,
    )


_COMMON_PROVIDER_COLUMNS = (
    _column("provider_id", ArrowType.STRING, "Exact provider namespace; fixed massive."),
    _column("provider_market", ArrowType.STRING, "Exact provider market; fixed stocks."),
    _column("provider_locale", ArrowType.STRING, "Exact provider locale; fixed us."),
    _column("observed_ticker", ArrowType.STRING, "Exact case-sensitive provider ticker."),
)

_COMMON_SCOPE_COLUMNS = (
    _column("valid_from_session", ArrowType.DATE32, "First inclusive exact S4 scope session."),
    _column("valid_through_session", ArrowType.DATE32, "Last inclusive exact S4 scope session."),
    _column("scoped_source_record_count", ArrowType.INT64, "Positive exact source-record count."),
    _column(
        "scoped_source_record_set_digest",
        ArrowType.STRING,
        "Digest of the sorted unique exact source-record ID array.",
    ),
    _column(
        "scoped_source_record_ids_json",
        ArrowType.JSON_STRING,
        "Canonical JSON array of sorted unique exact source-record IDs.",
    ),
)

_COMMON_APPROVAL_COLUMNS = (
    _column("source_decision_plan_id", ArrowType.STRING, "Immutable decision-plan ID."),
    _column(
        "source_decision_plan_path", ArrowType.STRING, "Normalized relative decision-plan path."
    ),
    _column(
        "source_decision_plan_sha256", ArrowType.STRING, "SHA-256 of exact decision-plan bytes."
    ),
    _column(
        "approval_request_event_id", ArrowType.STRING, "Exact literal approval request-event ID."
    ),
    _column(
        "approval_request_event_sha256", ArrowType.STRING, "SHA-256 of exact request-event bytes."
    ),
    _column("approval_receipt_id", ArrowType.STRING, "Immutable row-specific approval receipt ID."),
    _column(
        "approval_receipt_sha256", ArrowType.STRING, "SHA-256 of exact approval receipt bytes."
    ),
    _column("approved_by", ArrowType.STRING, "Stable explicit human reviewer identity."),
    _column(
        "approved_at_utc", ArrowType.TIMESTAMP_NS_UTC, "UTC timestamp from the approval receipt."
    ),
    _column(
        "approval_available_session",
        ArrowType.DATE32,
        "First bound-calendar session after approval.",
    ),
)

_COMMON_SOURCE_COLUMNS = (
    _column("source_s4_release_set_id", ArrowType.STRING, "Exact frozen S4 release-set ID."),
    _column(
        "source_exact_group_candidate_manifest_id",
        ArrowType.STRING,
        "Exact completed three-group full-history candidate manifest ID.",
    ),
    _column(
        "source_exact_group_candidate_manifest_sha256",
        ArrowType.STRING,
        "SHA-256 of the exact three-group candidate manifest bytes.",
    ),
    _column(
        "candidate_available_session",
        ArrowType.DATE32,
        "Operational candidate availability session.",
    ),
    _column(
        "source_external_evidence_manifest_id",
        ArrowType.STRING,
        "Immutable external-evidence manifest ID binding raw SEC, issuer, and OpenFIGI bytes.",
    ),
    _column(
        "source_external_evidence_manifest_sha256",
        ArrowType.STRING,
        "SHA-256 of exact external-evidence manifest bytes.",
    ),
    _column(
        "external_evidence_available_session",
        ArrowType.DATE32,
        "First session when all frozen evidence was available to this project; "
        "never the historical event date.",
    ),
    _column(
        "availability_calendar_id", ArrowType.STRING, "Exact immutable XNYS calendar artifact ID."
    ),
    _column(
        "availability_calendar_sha256",
        ArrowType.STRING,
        "SHA-256 of exact calendar artifact bytes.",
    ),
)


PROVIDER_COMPOSITE_OVERRIDE_CONTRACT: Final = TableContract(
    domain="identity",
    table="provider_composite_override",
    schema_version=1,
    description=(
        "Append-only, exact-source-row same-market Composite corrections that are valid only after "
        "an independently approved genuine asset transition. Observed lineage is immutable."
    ),
    grain=(
        "One immutable approved decision version for one provider/market/locale/ticker/observed "
        "Composite and asset-transition series over one exact S4 source-record scope."
    ),
    columns=(
        _column(
            "provider_composite_override_id",
            ArrowType.STRING,
            "Content-addressed immutable decision ID.",
        ),
        _column(
            "provider_composite_override_series_id",
            ArrowType.STRING,
            "Stable append-only decision series ID.",
        ),
        _column(
            "provider_composite_override_subject_id", ArrowType.STRING, "Stable exact subject ID."
        ),
        _column(
            "decision_version", ArrowType.INT64, "Positive contiguous version within the series."
        ),
        _column(
            "supersedes_provider_composite_override_id",
            ArrowType.STRING,
            "Immediate predecessor; null exactly for version one.",
            nullable=True,
        ),
        *_COMMON_PROVIDER_COLUMNS,
        _column(
            "observed_composite_figi",
            ArrowType.STRING,
            "Exact provider-observed Composite FIGI, never rewritten.",
        ),
        _column(
            "canonical_composite_figi",
            ArrowType.STRING,
            "Independent same-market canonical target; null for an unresolved successor.",
            nullable=True,
        ),
        _column(
            "observed_composite_market_code", ArrowType.STRING, "Observed Composite market code."
        ),
        _column(
            "canonical_composite_market_code",
            ArrowType.STRING,
            "Canonical market code, equal to observed market code when resolved.",
            nullable=True,
        ),
        _column(
            "canonical_asset_id",
            ArrowType.STRING,
            "Deterministic asset ID for the canonical Composite; null when unresolved.",
            nullable=True,
        ),
        *_COMMON_SCOPE_COLUMNS,
        _column(
            "asset_transition_series_id",
            ArrowType.STRING,
            "Stable bound genuine-transition series ID.",
        ),
        _column(
            "asset_transition_id",
            ArrowType.STRING,
            "Exact approved genuine-transition decision ID.",
        ),
        _column(
            "asset_transition_available_session",
            ArrowType.DATE32,
            "Availability of the bound transition decision.",
        ),
        _column(
            "disposition",
            ArrowType.STRING,
            "Confirmed stale-after-transition or adjudicated unresolved.",
        ),
        _column(
            "canonical_override", ArrowType.BOOLEAN, "True exactly for a confirmed correction."
        ),
        _column(
            "identity_effect",
            ArrowType.STRING,
            "Canonical research identity only, or none when unresolved.",
        ),
        _column("membership_effect", ArrowType.STRING, "Fixed none."),
        _column("active_status_effect", ArrowType.STRING, "Fixed none."),
        _column("identity_quality_liquidation_signal", ArrowType.BOOLEAN, "Always false."),
        _column("reason_code", ArrowType.STRING, "Reviewed controlled reason code."),
        _column(
            "reason_detail", ArrowType.STRING, "Immutable human rationale without outcome evidence."
        ),
        *_COMMON_APPROVAL_COLUMNS,
        _column(
            "override_available_session",
            ArrowType.DATE32,
            "Maximum of candidate, transition, evidence, and approval availability.",
        ),
        *_COMMON_SOURCE_COLUMNS,
        _column("outcome_or_backtest_evidence_used", ArrowType.BOOLEAN, "Always false."),
        _column("rule_version", ArrowType.STRING, "Fixed s7_provider_composite_override_v1."),
    ),
    primary_key=("provider_composite_override_id",),
    partition_by=(),
    sort_by=("observed_ticker", "valid_from_session", "decision_version"),
    source_datasets=(
        "identity_exact_group_history_review_slot",
        "asset_transition",
        "identity_external_evidence",
        "identity_approval_receipt",
    ),
    qa_rules=(
        _critical("schema_exact", "Fields, order, Arrow types and nullability must exactly match."),
        _critical(
            "primary_key_null_or_duplicate_rows", "Decision IDs must be non-null and unique."
        ),
        _critical(
            "append_only_version_chain_invalid_rows",
            "Versions must be contiguous and reference the immediate predecessor.",
        ),
        _critical(
            "decision_id_recomputation_mismatch_rows",
            "Subject, series, source-set and decision IDs must reproduce.",
        ),
        _critical(
            "provider_scope_invalid_rows",
            "Scope must remain exact massive/stocks/us and case-sensitive ticker.",
        ),
        _critical(
            "same_market_target_invalid_rows",
            "Confirmed observed/canonical Composite market codes must match.",
        ),
        _critical(
            "exact_source_scope_invalid_rows",
            "Interval, release and every exact source record must reconcile.",
        ),
        _critical(
            "asset_transition_binding_invalid_rows",
            "Every confirmed correction must bind one approved genuine transition "
            "available by cutoff.",
        ),
        _critical(
            "canonical_target_evidence_invalid_rows",
            "The successor Composite must have an immutable independent authority assertion.",
        ),
        _critical(
            "approval_chain_invalid_rows",
            "Decision plan, request event and row receipt must bind the exact decision.",
        ),
        _critical(
            "availability_recomputation_mismatch_rows",
            "Availability must be max(candidate, transition, evidence, approval).",
        ),
        _critical(
            "overlapping_provider_override_rows",
            "One source row may match at most one effective provider override subject.",
        ),
        _critical(
            "unapproved_provider_composite_override_rows",
            "Only approved decision rows may enter a release.",
        ),
        _critical(
            "observed_identity_lineage_mutated_rows",
            "Observed Composite and source lineage must remain byte-for-byte attributable to S4.",
        ),
        _critical(
            "identity_quality_membership_mutation_rows",
            "A correction cannot alter membership or active status.",
        ),
        _critical(
            "identity_quality_forced_liquidation_signal_rows",
            "A correction cannot trigger liquidation.",
        ),
        _critical(
            "outcome_or_backtest_evidence_rows",
            "Returns, factors and backtests are prohibited evidence.",
        ),
        _high(
            "multi_registry_composite_override_collision_rows",
            "Report every source row matched by multiple Composite correction "
            "registries with reason counts and examples.",
        ),
        _critical(
            "multi_registry_composite_override_collision_eligible_rows",
            "A collision row must never remain identity eligible.",
        ),
        _critical(
            "multi_registry_composite_override_collision_resolved_rows",
            "A collision row must never be resolved automatically.",
        ),
        _critical(
            "multi_registry_composite_override_collision_alias_rows",
            "A collision row must never emit an alias.",
        ),
    ),
)


SHARE_CLASS_ADJUDICATION_CONTRACT: Final = TableContract(
    domain="identity",
    table="share_class_adjudication",
    schema_version=1,
    description=(
        "Append-only exact-source-row Share Class corrections. The registry is applied only after "
        "a unique canonical Composite is known and cannot create or change asset or "
        "issuer identity."
    ),
    grain=(
        "One immutable approved decision version for one provider/market/locale/ticker/observed "
        "Composite/observed Share Class subject over one exact S4 source-record scope."
    ),
    columns=(
        _column(
            "share_class_adjudication_id",
            ArrowType.STRING,
            "Content-addressed immutable decision ID.",
        ),
        _column(
            "share_class_adjudication_series_id",
            ArrowType.STRING,
            "Stable append-only decision series ID.",
        ),
        _column(
            "share_class_adjudication_subject_id", ArrowType.STRING, "Stable exact subject ID."
        ),
        _column(
            "decision_version", ArrowType.INT64, "Positive contiguous version within the series."
        ),
        _column(
            "supersedes_share_class_adjudication_id",
            ArrowType.STRING,
            "Immediate predecessor; null exactly for version one.",
            nullable=True,
        ),
        *_COMMON_PROVIDER_COLUMNS,
        _column(
            "observed_composite_figi",
            ArrowType.STRING,
            "Exact provider-observed Composite scope guard.",
        ),
        _column(
            "required_unique_canonical_composite_figi",
            ArrowType.STRING,
            "Unique canonical Composite precondition; this registry cannot create or alter it.",
        ),
        _column(
            "observed_share_class_figi",
            ArrowType.STRING,
            "Exact provider-observed Share Class FIGI, never rewritten.",
        ),
        _column(
            "canonical_share_class_figi",
            ArrowType.STRING,
            "Independently evidenced canonical Share Class; null when unresolved.",
            nullable=True,
        ),
        _column(
            "canonical_share_class_id",
            ArrowType.STRING,
            "Deterministic Share Class identity; null when unresolved.",
            nullable=True,
        ),
        *_COMMON_SCOPE_COLUMNS,
        _column(
            "disposition",
            ArrowType.STRING,
            "Confirmed Share Class correction or adjudicated unresolved.",
        ),
        _column(
            "share_class_override", ArrowType.BOOLEAN, "True exactly for a confirmed correction."
        ),
        _column("composite_identity_effect", ArrowType.STRING, "Fixed none."),
        _column("asset_id_effect", ArrowType.STRING, "Fixed none."),
        _column("issuer_identity_effect", ArrowType.STRING, "Fixed none."),
        _column("membership_effect", ArrowType.STRING, "Fixed none."),
        _column(
            "tradability_effect",
            ArrowType.STRING,
            "Fixed none; final tradability remains downstream.",
        ),
        _column("identity_quality_liquidation_signal", ArrowType.BOOLEAN, "Always false."),
        _column("reason_code", ArrowType.STRING, "Reviewed controlled reason code."),
        _column(
            "reason_detail", ArrowType.STRING, "Immutable human rationale without outcome evidence."
        ),
        *_COMMON_APPROVAL_COLUMNS,
        _column(
            "adjudication_available_session",
            ArrowType.DATE32,
            "Maximum of candidate, evidence, and approval availability.",
        ),
        *_COMMON_SOURCE_COLUMNS,
        _column("outcome_or_backtest_evidence_used", ArrowType.BOOLEAN, "Always false."),
        _column("rule_version", ArrowType.STRING, "Fixed s7_share_class_adjudication_v1."),
    ),
    primary_key=("share_class_adjudication_id",),
    partition_by=(),
    sort_by=("observed_ticker", "valid_from_session", "decision_version"),
    source_datasets=(
        "identity_exact_group_history_review_slot",
        "identity_external_evidence",
        "identity_approval_receipt",
    ),
    qa_rules=(
        _critical("schema_exact", "Fields, order, Arrow types and nullability must exactly match."),
        _critical(
            "primary_key_null_or_duplicate_rows", "Decision IDs must be non-null and unique."
        ),
        _critical(
            "append_only_version_chain_invalid_rows",
            "Versions must be contiguous and reference the immediate predecessor.",
        ),
        _critical(
            "decision_id_recomputation_mismatch_rows",
            "Subject, series, source-set and decision IDs must reproduce.",
        ),
        _critical(
            "provider_scope_invalid_rows",
            "Scope must remain exact massive/stocks/us and case-sensitive ticker.",
        ),
        _critical(
            "exact_source_scope_invalid_rows",
            "Composite, Share Class, interval, release and every source record must reconcile.",
        ),
        _critical(
            "unique_canonical_composite_precondition_invalid_rows",
            "A Share Class decision may apply only after one canonical Composite is "
            "uniquely resolved.",
        ),
        _critical(
            "canonical_share_class_target_evidence_invalid_rows",
            "The canonical Share Class target must have independent immutable evidence.",
        ),
        _critical(
            "approval_chain_invalid_rows",
            "Decision plan, request event and row receipt must bind the exact decision.",
        ),
        _critical(
            "availability_recomputation_mismatch_rows",
            "Availability must be max(candidate, evidence, approval).",
        ),
        _critical(
            "overlapping_share_class_adjudication_rows",
            "One source row may match at most one effective Share Class decision.",
        ),
        _critical(
            "unapproved_share_class_override_rows",
            "Only approved decision rows may enter a release.",
        ),
        _critical(
            "share_class_adjudication_changed_composite_rows",
            "A Share Class decision cannot change canonical Composite identity.",
        ),
        _critical(
            "share_class_adjudication_changed_asset_id_rows",
            "A Share Class decision cannot create or change asset_id.",
        ),
        _critical(
            "share_class_adjudication_changed_issuer_rows",
            "A Share Class decision cannot create, merge or change issuer identity.",
        ),
        _critical(
            "share_class_adjudication_membership_mutation_rows",
            "A Share Class decision cannot alter membership or active status.",
        ),
        _critical(
            "share_class_adjudication_tradability_rows",
            "A Share Class decision alone cannot set final tradability.",
        ),
        _critical(
            "observed_hierarchy_lineage_mutated_rows",
            "Observed Share Class and source lineage must remain unchanged.",
        ),
        _critical(
            "identity_quality_forced_liquidation_signal_rows",
            "A hierarchy correction cannot trigger liquidation.",
        ),
        _critical(
            "outcome_or_backtest_evidence_rows",
            "Returns, factors and backtests are prohibited evidence.",
        ),
        _high(
            "multi_share_class_adjudication_collision_rows",
            "Report raw multi-decision matches with reason counts and examples.",
        ),
        _critical(
            "multi_share_class_adjudication_collision_eligible_rows",
            "A hierarchy collision must not remain identity eligible.",
        ),
        _critical(
            "multi_share_class_adjudication_collision_resolved_rows",
            "A hierarchy collision must not auto-resolve.",
        ),
        _critical(
            "multi_share_class_adjudication_collision_alias_rows",
            "A hierarchy collision must not emit an alias.",
        ),
    ),
)


ASSET_TRANSITION_CONTRACT: Final = TableContract(
    domain="identity",
    table="asset_transition",
    schema_version=1,
    description=(
        "Append-only predecessor/successor security relations supported by immutable "
        "corporate-action evidence. A transition relation performs no identity override, "
        "return stitching, membership "
        "change, tradability decision or forced liquidation."
    ),
    grain=(
        "One immutable approved decision version for one exact provider/ticker/legal-event "
        "transition series."
    ),
    columns=(
        _column(
            "asset_transition_id",
            ArrowType.STRING,
            "Content-addressed immutable transition decision ID.",
        ),
        _column(
            "asset_transition_series_id", ArrowType.STRING, "Stable append-only event series ID."
        ),
        _column(
            "asset_transition_subject_id",
            ArrowType.STRING,
            "Stable provider/ticker/type/legal-date subject ID.",
        ),
        _column(
            "decision_version", ArrowType.INT64, "Positive contiguous version within the series."
        ),
        _column(
            "supersedes_asset_transition_id",
            ArrowType.STRING,
            "Immediate predecessor; null exactly for version one.",
            nullable=True,
        ),
        *_COMMON_PROVIDER_COLUMNS,
        _column(
            "transition_type", ArrowType.STRING, "Controlled genuine security-transition type."
        ),
        _column(
            "legal_effective_date",
            ArrowType.DATE32,
            "Historical legal effective date; not evidence availability.",
        ),
        _column(
            "predecessor_last_session",
            ArrowType.DATE32,
            "Last accepted predecessor trading session boundary.",
        ),
        _column(
            "successor_first_session",
            ArrowType.DATE32,
            "First accepted successor trading session boundary.",
        ),
        _column(
            "predecessor_composite_figi", ArrowType.STRING, "Canonical predecessor Composite FIGI."
        ),
        _column("predecessor_asset_id", ArrowType.STRING, "Deterministic predecessor asset ID."),
        _column(
            "successor_composite_figi",
            ArrowType.STRING,
            "Canonical successor Composite FIGI; null for an unresolved successor version.",
            nullable=True,
        ),
        _column(
            "successor_asset_id",
            ArrowType.STRING,
            "Deterministic successor asset ID; null when unresolved.",
            nullable=True,
        ),
        _column(
            "boundary_source_record_count",
            ArrowType.INT64,
            "Positive exact S4 boundary source-record count.",
        ),
        _column(
            "boundary_source_record_set_digest",
            ArrowType.STRING,
            "Digest of sorted unique boundary source-record IDs.",
        ),
        _column(
            "boundary_source_record_ids_json",
            ArrowType.JSON_STRING,
            "Canonical sorted boundary source-record ID array.",
        ),
        _column(
            "disposition",
            ArrowType.STRING,
            "Confirmed genuine transition or adjudicated unresolved.",
        ),
        _column(
            "relationship_effect",
            ArrowType.STRING,
            "Fixed lineage_only_no_override_no_return_stitching.",
        ),
        _column("identity_override_effect", ArrowType.STRING, "Fixed none."),
        _column("membership_effect", ArrowType.STRING, "Fixed none."),
        _column("tradability_effect", ArrowType.STRING, "Fixed none."),
        _column(
            "return_stitching_effect",
            ArrowType.STRING,
            "Fixed none_requires_future_entitlement_accounting.",
        ),
        _column("identity_quality_liquidation_signal", ArrowType.BOOLEAN, "Always false."),
        _column("reason_code", ArrowType.STRING, "Reviewed controlled reason code."),
        _column(
            "reason_detail", ArrowType.STRING, "Immutable human rationale without outcome evidence."
        ),
        *_COMMON_APPROVAL_COLUMNS,
        _column(
            "transition_available_session",
            ArrowType.DATE32,
            "Maximum of candidate, evidence, and approval availability; never backdated "
            "to the event.",
        ),
        *_COMMON_SOURCE_COLUMNS,
        _column("outcome_or_backtest_evidence_used", ArrowType.BOOLEAN, "Always false."),
        _column("rule_version", ArrowType.STRING, "Fixed s7_asset_transition_v1."),
    ),
    primary_key=("asset_transition_id",),
    partition_by=(),
    sort_by=("observed_ticker", "legal_effective_date", "decision_version"),
    source_datasets=(
        "identity_exact_group_history_review_slot",
        "identity_external_evidence",
        "identity_approval_receipt",
    ),
    qa_rules=(
        _critical("schema_exact", "Fields, order, Arrow types and nullability must exactly match."),
        _critical(
            "primary_key_null_or_duplicate_rows", "Transition IDs must be non-null and unique."
        ),
        _critical(
            "append_only_version_chain_invalid_rows",
            "Versions must be contiguous and reference the immediate predecessor.",
        ),
        _critical(
            "decision_id_recomputation_mismatch_rows",
            "Subject, series, source-set, asset and transition IDs must reproduce.",
        ),
        _critical(
            "provider_scope_invalid_rows",
            "Scope must remain exact massive/stocks/us and case-sensitive ticker.",
        ),
        _critical(
            "transition_boundary_invalid_rows",
            "Legal and predecessor/successor session boundaries must be ordered and "
            "evidence-backed.",
        ),
        _critical("self_transition_rows", "A confirmed transition cannot map an asset to itself."),
        _critical(
            "transition_cycle_rows",
            "Effective confirmed transition relations must form an acyclic graph.",
        ),
        _critical(
            "boundary_source_scope_invalid_rows",
            "Every exact boundary source record and release binding must reconcile.",
        ),
        _critical(
            "external_evidence_manifest_binding_invalid_rows",
            "SEC, issuer and OpenFIGI bytes, claims, timestamps and SHAs must replay.",
        ),
        _critical(
            "approval_chain_invalid_rows",
            "Decision plan, request event and row receipt must bind the exact transition.",
        ),
        _critical(
            "availability_recomputation_mismatch_rows",
            "Availability must be max(candidate, evidence, approval), never event date.",
        ),
        _critical(
            "unapproved_asset_transition_rows",
            "Only approved transition decisions may enter a release.",
        ),
        _critical(
            "asset_transition_used_as_identity_override_rows",
            "A relation cannot execute a Composite or Share Class override.",
        ),
        _critical(
            "asset_transition_return_stitching_rows",
            "A relation cannot stitch returns without future entitlement-aware accounting.",
        ),
        _critical(
            "asset_transition_membership_mutation_rows",
            "A relation cannot alter active/inactive membership.",
        ),
        _critical(
            "asset_transition_tradability_rows", "A relation alone cannot set final tradability."
        ),
        _critical(
            "identity_quality_forced_liquidation_signal_rows",
            "A transition relation cannot trigger liquidation.",
        ),
        _critical(
            "temporary_security_merged_into_ordinary_asset_rows",
            "A temporary ex-distribution security may not be merged into its ordinary "
            "ticker asset.",
        ),
        _critical(
            "outcome_or_backtest_evidence_rows",
            "Returns, factors and backtests are prohibited evidence.",
        ),
    ),
)


RELATION_REGISTRY_CONTRACTS: Final = MappingProxyType(
    {
        PROVIDER_COMPOSITE_OVERRIDE_CONTRACT.table: PROVIDER_COMPOSITE_OVERRIDE_CONTRACT,
        SHARE_CLASS_ADJUDICATION_CONTRACT.table: SHARE_CLASS_ADJUDICATION_CONTRACT,
        ASSET_TRANSITION_CONTRACT.table: ASSET_TRANSITION_CONTRACT,
    }
)


def contract_bytes(contract: TableContract) -> bytes:
    return (
        json.dumps(contract.to_dict(), allow_nan=False, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")


RELATION_REGISTRY_CONTRACT_IDS: Final = MappingProxyType(
    {table: contract.contract_id for table, contract in RELATION_REGISTRY_CONTRACTS.items()}
)
RELATION_REGISTRY_SCHEMA_DIGESTS: Final = MappingProxyType(
    {table: contract.schema_digest for table, contract in RELATION_REGISTRY_CONTRACTS.items()}
)
RELATION_REGISTRY_RESOURCE_SHA256: Final = MappingProxyType(
    {
        table: hashlib.sha256(contract_bytes(contract)).hexdigest()
        for table, contract in RELATION_REGISTRY_CONTRACTS.items()
    }
)


def load_pinned_relation_registry_contracts() -> MappingProxyType[str, TableContract]:
    """Load packaged JSON only when it is byte-identical to the code-pinned contract."""

    loaded: dict[str, TableContract] = {}
    root = files("ame_stocks_api.silver").joinpath("schema_resources")
    for table, expected in RELATION_REGISTRY_CONTRACTS.items():
        payload = root.joinpath(f"{table}.schema-v1.json").read_bytes()
        if payload != contract_bytes(expected):
            raise RuntimeError(f"packaged {table} contract differs from its pinned definition")
        loaded[table] = TableContract.from_dict(json.loads(payload))
    return MappingProxyType(loaded)


__all__ = [
    "ASSET_TRANSITION_CONTRACT",
    "PROVIDER_COMPOSITE_OVERRIDE_CONTRACT",
    "RELATION_REGISTRY_CONTRACTS",
    "RELATION_REGISTRY_CONTRACT_IDS",
    "RELATION_REGISTRY_RESOURCE_SHA256",
    "RELATION_REGISTRY_SCHEMA_DIGESTS",
    "SHARE_CLASS_ADJUDICATION_CONTRACT",
    "contract_bytes",
    "load_pinned_relation_registry_contracts",
]
