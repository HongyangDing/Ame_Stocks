"""Production S7 registry-release adapter for the S7.5 I3 dispatcher.

The existing fixture loader authenticates a compact, one-source-row JSON format.
Production decisions instead come from :mod:`identity_registry_workflow`, whose
loader has already replayed the Parquet row, canonical JSON decision artifact,
approval controls, release manifest, and complete :class:`ExactSourceScope`.

This module is the narrow bridge between those trust boundaries.  It accepts no
paths, readers, callbacks, or caller-authored decision rows.  It reconciles the
loaded five-release set against one exact :class:`IdentityPolicyBundle`, retains
the real registry decision IDs and full source scopes, rejects Composite registry
scope collisions, and asks the dispatcher to mint a production-source snapshot.
"""

from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Mapping
from datetime import date
from types import MappingProxyType
from typing import Final

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver.identity_registry_workflow import (
    COMPOSITE_CORRECTION_REGISTRIES,
    REGISTRY_ORDER,
    ExactSourceScope,
    LoadedRegistryRelease,
    LoadedRegistryReleaseSet,
)
from ame_stocks_api.silver.incremental_i3_checkpoint import (
    IDENTITY_REGISTRY_ORDER,
    IdentityPolicyBundle,
    IdentityRegistryKind,
)
from ame_stocks_api.silver.incremental_i3_dispatch import (
    I3DispatchError,
    IdentityPolicySnapshot,
    RegistryDecision,
    RegistrySourceScopeRow,
    _mint_production_identity_policy_snapshot,
)

I3_PRODUCTION_POLICY_ADAPTER_RULE_VERSION: Final = "s7_5_i3_loaded_registry_release_set_adapter_v1"

_DECISION_ID_COLUMN: Final[Mapping[IdentityRegistryKind, str]] = MappingProxyType(
    {
        IdentityRegistryKind.IDENTITY_ADJUDICATION: "identity_adjudication_id",
        IdentityRegistryKind.IDENTITY_CROSS_MARKET_ADJUDICATION: ("cross_market_adjudication_id"),
        IdentityRegistryKind.PROVIDER_COMPOSITE_OVERRIDE: ("provider_composite_override_id"),
        IdentityRegistryKind.SHARE_CLASS_ADJUDICATION: "share_class_adjudication_id",
        IdentityRegistryKind.ASSET_TRANSITION: "asset_transition_id",
    }
)
_SERIES_ID_COLUMN: Final[Mapping[IdentityRegistryKind, str]] = MappingProxyType(
    {
        IdentityRegistryKind.IDENTITY_ADJUDICATION: "adjudication_series_id",
        IdentityRegistryKind.IDENTITY_CROSS_MARKET_ADJUDICATION: "cross_market_series_id",
        IdentityRegistryKind.PROVIDER_COMPOSITE_OVERRIDE: ("provider_composite_override_series_id"),
        IdentityRegistryKind.SHARE_CLASS_ADJUDICATION: ("share_class_adjudication_series_id"),
        IdentityRegistryKind.ASSET_TRANSITION: "asset_transition_series_id",
    }
)
_AVAILABLE_SESSION_COLUMN: Final[Mapping[IdentityRegistryKind, str]] = MappingProxyType(
    {
        IdentityRegistryKind.IDENTITY_ADJUDICATION: "adjudication_available_session",
        IdentityRegistryKind.IDENTITY_CROSS_MARKET_ADJUDICATION: ("adjudication_available_session"),
        IdentityRegistryKind.PROVIDER_COMPOSITE_OVERRIDE: "override_available_session",
        IdentityRegistryKind.SHARE_CLASS_ADJUDICATION: "adjudication_available_session",
        IdentityRegistryKind.ASSET_TRANSITION: "transition_available_session",
    }
)
_SCOPE_COLUMNS: Final[Mapping[IdentityRegistryKind, tuple[str, str, str | None]]] = (
    MappingProxyType(
        {
            IdentityRegistryKind.IDENTITY_ADJUDICATION: (
                "episode_source_record_count",
                "episode_source_record_set_digest",
                None,
            ),
            IdentityRegistryKind.IDENTITY_CROSS_MARKET_ADJUDICATION: (
                "scoped_source_record_count",
                "scoped_source_record_set_digest",
                "scoped_source_record_ids_json",
            ),
            IdentityRegistryKind.PROVIDER_COMPOSITE_OVERRIDE: (
                "scoped_source_record_count",
                "scoped_source_record_set_digest",
                "scoped_source_record_ids_json",
            ),
            IdentityRegistryKind.SHARE_CLASS_ADJUDICATION: (
                "scoped_source_record_count",
                "scoped_source_record_set_digest",
                "scoped_source_record_ids_json",
            ),
            IdentityRegistryKind.ASSET_TRANSITION: (
                "boundary_source_record_count",
                "boundary_source_record_set_digest",
                "boundary_source_record_ids_json",
            ),
        }
    )
)


class I3ProductionPolicyError(I3DispatchError):
    """Raised when production registry releases cannot mint one exact policy."""


def load_production_identity_policy_snapshot(
    loaded: LoadedRegistryReleaseSet,
    bundle: IdentityPolicyBundle,
) -> IdentityPolicySnapshot:
    """Adapt one replayed production release set into a sealed I3 policy snapshot.

    The input ``loaded`` must be the direct result of the production registry
    workflow loader (or an object that has already passed the same typed
    constructors).  Only terminal decisions available by each bundle member's
    decision cutoff are admitted.  Release publication availability remains a
    separate, later boundary and is reconciled against the exact bundle pin.
    """

    if type(loaded) is not LoadedRegistryReleaseSet:
        raise I3ProductionPolicyError(
            "production policy adapter requires a LoadedRegistryReleaseSet"
        )
    if type(bundle) is not IdentityPolicyBundle:
        raise I3ProductionPolicyError(
            "production policy adapter requires a typed IdentityPolicyBundle"
        )
    if tuple(item.registry_kind for item in bundle.registry_releases) != (IDENTITY_REGISTRY_ORDER):
        raise I3ProductionPolicyError("production policy bundle changed registry order")
    if tuple(item.registry_name for item in loaded.releases) != REGISTRY_ORDER:
        raise I3ProductionPolicyError("production release set changed registry order")

    decisions: list[RegistryDecision] = []
    binding_releases: list[dict[str, object]] = []
    composite_scope_owner: dict[str, tuple[str, str]] = {}
    all_decision_ids: set[str] = set()

    for kind, pin, release in zip(
        IDENTITY_REGISTRY_ORDER,
        bundle.registry_releases,
        loaded.releases,
        strict=True,
    ):
        _reconcile_release_pin(kind, release, pin)
        selected_ids = _effective_decision_ids(
            release,
            kind=kind,
            decision_cutoff_session=pin.decision_cutoff_session,
        )
        release_bindings: list[dict[str, object]] = []
        for decision_id in selected_ids:
            if decision_id in all_decision_ids:
                raise I3ProductionPolicyError(
                    "production registry decision ID appears in multiple responsibilities"
                )
            all_decision_ids.add(decision_id)
            try:
                row = release.decision_rows[decision_id]
                scope = release.source_scopes[decision_id]
            except KeyError as exc:  # pragma: no cover - Loaded release constructor proves
                raise I3ProductionPolicyError(
                    "production release omitted a selected decision or source scope"
                ) from exc
            if type(scope) is not ExactSourceScope:
                raise I3ProductionPolicyError("production decision source scope is not exact")
            if kind.value in COMPOSITE_CORRECTION_REGISTRIES:
                _claim_composite_scope(
                    composite_scope_owner,
                    registry_name=kind.value,
                    decision_id=decision_id,
                    scope=scope,
                )
            decision = _adapt_decision(
                kind=kind,
                release=release,
                decision_id=decision_id,
                row=row,
                scope=scope,
            )
            decisions.append(decision)
            release_bindings.append(
                {
                    "decision_id": decision_id,
                    "registry_row": dict(decision.production_registry_row or {}),
                    "source_scope": [item.to_dict() for item in decision.source_scope],
                }
            )
        binding_releases.append(
            {
                "decision_cutoff_session": pin.decision_cutoff_session.isoformat(),
                "effective_decisions": release_bindings,
                "manifest_pin": release.manifest_pin.to_dict(),
                "registry_kind": kind.value,
                "release_id": release.release_id,
            }
        )

    ordered_decisions = tuple(sorted(decisions, key=lambda item: item.decision_id))
    release_set_binding_digest = stable_digest(
        {
            "identity_policy_bundle_id": bundle.identity_policy_bundle_id,
            "registry_releases": binding_releases,
            "rule_version": I3_PRODUCTION_POLICY_ADAPTER_RULE_VERSION,
        }
    )
    try:
        return _mint_production_identity_policy_snapshot(
            bundle,
            ordered_decisions,
            production_release_set_binding_digest=release_set_binding_digest,
        )
    except I3DispatchError as exc:
        raise I3ProductionPolicyError(
            "production registry release set cannot reproduce a closed policy snapshot"
        ) from exc


def _reconcile_release_pin(
    kind: IdentityRegistryKind,
    release: LoadedRegistryRelease,
    pin: object,
) -> None:
    if type(release) is not LoadedRegistryRelease:
        raise I3ProductionPolicyError("production release set contains an invalid member")
    if release.registry_name != kind.value:
        raise I3ProductionPolicyError("production release responsibility differs from bundle")
    expected = (
        kind,
        release.release_id,
        release.manifest_pin.manifest_path,
        release.manifest_pin.manifest_sha256,
        release.manifest_pin.manifest_bytes,
        release.release_available_session,
    )
    actual = (
        pin.registry_kind,
        pin.release_id,
        pin.artifact.path,
        pin.artifact.sha256,
        pin.artifact.bytes,
        pin.release_available_session,
    )
    if actual != expected:
        raise I3ProductionPolicyError("production registry release differs from its exact pin")
    if (
        release.manifest_pin.registry_name != kind.value
        or release.manifest_pin.release_id != release.release_id
        or release.manifest_pin.release_available_session != release.release_available_session
        or release.manifest.registry_name != kind.value
        or release.manifest.release_id != release.release_id
        or release.manifest.release_available_session != release.release_available_session
    ):
        raise I3ProductionPolicyError("production registry manifest and loaded release differ")


def _effective_decision_ids(
    release: LoadedRegistryRelease,
    *,
    kind: IdentityRegistryKind,
    decision_cutoff_session: date,
) -> tuple[str, ...]:
    """Select terminal revisions at the decision cutoff, not publication time."""

    if type(decision_cutoff_session) is not date:
        raise I3ProductionPolicyError("production registry decision cutoff is invalid")
    series_column = _SERIES_ID_COLUMN[kind]
    available_column = _AVAILABLE_SESSION_COLUMN[kind]
    decision_id_column = _DECISION_ID_COLUMN[kind]
    grouped: defaultdict[str, list[tuple[int, str, date]]] = defaultdict(list)
    for decision_id, row in release.decision_rows.items():
        if row.get(decision_id_column) != decision_id:
            raise I3ProductionPolicyError("production registry row decision ID changed")
        available = _native_date(row.get(available_column), "registry decision availability")
        version = row.get("decision_version")
        if type(version) is not int or version <= 0:
            raise I3ProductionPolicyError("production registry decision version is invalid")
        series_id = row.get(series_column)
        if not isinstance(series_id, str):
            raise I3ProductionPolicyError("production registry series ID is invalid")
        grouped[series_id].append((version, decision_id, available))

    selected: list[str] = []
    for revisions in grouped.values():
        admitted = [item for item in revisions if item[2] <= decision_cutoff_session]
        if admitted:
            selected.append(max(admitted, key=lambda item: item[0])[1])
    return tuple(sorted(selected))


def _claim_composite_scope(
    owners: dict[str, tuple[str, str]],
    *,
    registry_name: str,
    decision_id: str,
    scope: ExactSourceScope,
) -> None:
    for source_record_id in scope.source_record_ids:
        prior = owners.get(source_record_id)
        if prior is not None:
            raise I3ProductionPolicyError(
                "production Composite registry source scope collision; "
                "no priority or majority fallback is permitted"
            )
        owners[source_record_id] = (registry_name, decision_id)


def _adapt_decision(
    *,
    kind: IdentityRegistryKind,
    release: LoadedRegistryRelease,
    decision_id: str,
    row: Mapping[str, object],
    scope: ExactSourceScope,
) -> RegistryDecision:
    source_scope = tuple(
        RegistrySourceScopeRow(
            session_date=item.session_date,
            source_record_id=item.source_record_id,
            source_dataset=item.source_dataset,
            source_s4_release_set_id=item.source_s4_release_set_id,
            provider_id=item.provider_id,
            provider_market=item.provider_market,
            provider_locale=item.provider_locale,
            ticker=item.ticker,
            observed_composite_figi=item.observed_composite_figi,
            observed_share_class_figi=item.observed_share_class_figi,
            primary_exchange_mic=item.primary_exchange_mic,
        )
        for item in scope.rows
    )
    representative_source_record_id = min(item.source_record_id for item in source_scope)
    common = _common_scope_values(scope)
    _validate_row_scope_projection(kind=kind, row=row, scope=scope, common=common)

    values: dict[str, object] = {
        "registry_kind": kind,
        "registry_release_id": release.release_id,
        "decision_id": decision_id,
        "provider_id": common["provider_id"],
        "provider_market": common["provider_market"],
        "provider_locale": common["provider_locale"],
        "ticker": common["ticker"],
        "source_record_id": representative_source_record_id,
        "source_scope": source_scope,
        "production_registry_row": row,
    }
    if kind is IdentityRegistryKind.IDENTITY_ADJUDICATION:
        values.update(
            identity_disposition=row.get("disposition"),
            decision_available_session=row.get("adjudication_available_session"),
            effective_from_session=row.get("episode_valid_from_session"),
            effective_to_session=row.get("episode_valid_through_session"),
            observed_composite_figi=row.get("observed_composite_figi"),
            canonical_composite_figi=row.get("canonical_composite_figi"),
        )
    elif kind is IdentityRegistryKind.IDENTITY_CROSS_MARKET_ADJUDICATION:
        values.update(
            identity_disposition=row.get("identity_disposition"),
            decision_available_session=row.get("adjudication_available_session"),
            effective_from_session=row.get("valid_from_session"),
            effective_to_session=row.get("valid_through_session"),
            observed_composite_figi=row.get("observed_foreign_composite_figi"),
            canonical_composite_figi=row.get("canonical_us_composite_figi"),
            observed_composite_market_code=row.get("observed_composite_market_code"),
            canonical_composite_market_code=row.get("canonical_composite_market_code"),
            observed_share_class_figi=row.get("share_class_figi"),
        )
    elif kind is IdentityRegistryKind.PROVIDER_COMPOSITE_OVERRIDE:
        values.update(
            identity_disposition=row.get("disposition"),
            decision_available_session=row.get("override_available_session"),
            effective_from_session=row.get("valid_from_session"),
            effective_to_session=row.get("valid_through_session"),
            observed_composite_figi=row.get("observed_composite_figi"),
            canonical_composite_figi=row.get("canonical_composite_figi"),
            observed_composite_market_code=row.get("observed_composite_market_code"),
            canonical_composite_market_code=row.get("canonical_composite_market_code"),
            transition_relation_id=row.get("asset_transition_id"),
        )
    elif kind is IdentityRegistryKind.SHARE_CLASS_ADJUDICATION:
        values.update(
            identity_disposition=row.get("disposition"),
            decision_available_session=row.get("adjudication_available_session"),
            effective_from_session=row.get("valid_from_session"),
            effective_to_session=row.get("valid_through_session"),
            observed_composite_figi=row.get("observed_composite_figi"),
            composite_scope_figi=row.get("required_unique_canonical_composite_figi"),
            observed_share_class_figi=row.get("observed_share_class_figi"),
            canonical_share_class_figi=row.get("canonical_share_class_figi"),
        )
    else:
        values.update(
            identity_disposition=row.get("disposition"),
            decision_available_session=row.get("transition_available_session"),
            effective_from_session=row.get("predecessor_last_session"),
            effective_to_session=row.get("successor_first_session"),
            predecessor_asset_id=row.get("predecessor_asset_id"),
            successor_asset_id=row.get("successor_asset_id"),
        )

    try:
        return RegistryDecision(**values)  # type: ignore[arg-type]
    except (I3DispatchError, TypeError, ValueError) as exc:
        raise I3ProductionPolicyError(
            f"production {kind.value} row crossed its closed responsibility"
        ) from exc


def _common_scope_values(scope: ExactSourceScope) -> dict[str, str]:
    fields = {
        "provider_id": {item.provider_id for item in scope.rows},
        "provider_market": {item.provider_market for item in scope.rows},
        "provider_locale": {item.provider_locale for item in scope.rows},
        "ticker": {item.ticker for item in scope.rows},
    }
    if any(len(values) != 1 for values in fields.values()):
        raise I3ProductionPolicyError("production registry source scope mixes subjects")
    return {name: next(iter(values)) for name, values in fields.items()}


def _validate_row_scope_projection(
    *,
    kind: IdentityRegistryKind,
    row: Mapping[str, object],
    scope: ExactSourceScope,
    common: Mapping[str, str],
) -> None:
    if row.get("observed_ticker") != common["ticker"]:
        raise I3ProductionPolicyError("production registry ticker differs from source scope")
    if row.get("source_s4_release_set_id") != scope.rows[0].source_s4_release_set_id:
        raise I3ProductionPolicyError("production registry row binds another S4 release")
    if kind is not IdentityRegistryKind.IDENTITY_ADJUDICATION and any(
        row.get(name) != common[name]
        for name in ("provider_id", "provider_market", "provider_locale")
    ):
        raise I3ProductionPolicyError("production registry provider scope changed")

    count_column, digest_column, ids_column = _SCOPE_COLUMNS[kind]
    if (
        row.get(count_column) != len(scope.rows)
        or row.get(digest_column) != scope.source_record_set_digest
    ):
        raise I3ProductionPolicyError("production registry source scope projection changed")
    if ids_column is not None:
        expected_ids = json.dumps(
            list(scope.source_record_ids),
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        if row.get(ids_column) != expected_ids:
            raise I3ProductionPolicyError("production registry source-record membership changed")

    if kind is IdentityRegistryKind.IDENTITY_ADJUDICATION:
        if any(
            item.observed_composite_figi != row.get("observed_composite_figi")
            for item in scope.rows
        ):
            raise I3ProductionPolicyError("identity adjudication source Composite changed")
    elif kind is IdentityRegistryKind.IDENTITY_CROSS_MARKET_ADJUDICATION:
        if any(
            item.observed_composite_figi != row.get("observed_foreign_composite_figi")
            or item.observed_share_class_figi != row.get("share_class_figi")
            for item in scope.rows
        ):
            raise I3ProductionPolicyError("cross-market source hierarchy changed")
    elif kind is IdentityRegistryKind.PROVIDER_COMPOSITE_OVERRIDE:
        if any(
            item.observed_composite_figi != row.get("observed_composite_figi")
            for item in scope.rows
        ):
            raise I3ProductionPolicyError("provider override source Composite changed")
    elif kind is IdentityRegistryKind.SHARE_CLASS_ADJUDICATION:
        if any(
            item.observed_composite_figi != row.get("observed_composite_figi")
            or item.observed_share_class_figi != row.get("observed_share_class_figi")
            for item in scope.rows
        ):
            raise I3ProductionPolicyError("Share Class source hierarchy changed")
    else:
        boundary_sessions = {item.session_date for item in scope.rows}
        expected_sessions = {
            row.get("predecessor_last_session"),
            row.get("successor_first_session"),
        }
        if boundary_sessions != expected_sessions:
            raise I3ProductionPolicyError("asset transition boundary source scope changed")


def _native_date(value: object, label: str) -> date:
    if type(value) is not date:
        raise I3ProductionPolicyError(f"production {label} is invalid")
    return value


__all__ = [
    "I3_PRODUCTION_POLICY_ADAPTER_RULE_VERSION",
    "I3ProductionPolicyError",
    "load_production_identity_policy_snapshot",
]
