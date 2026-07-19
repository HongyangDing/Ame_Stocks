"""Source-bound, visibility-atomic S7 four-table candidate materialization.

This module deliberately stops at an immutable ``awaiting_review`` candidate.  It has no
workflow transition, FullRunPlan approval, release, or publication capability.  The caller must
first bind the resolved graph to one exact six-release source bundle and five exact registry
releases.  The final four Parquet tables become visible together through one directory rename.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import stat
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from types import MappingProxyType
from typing import Final, Protocol

import pyarrow as pa
import pyarrow.parquet as pq

from ame_stocks_api.artifacts import safe_relative_path, stable_digest, write_bytes_immutable
from ame_stocks_api.silver.contracts import TableContract
from ame_stocks_api.silver.identity_resolution_contract import (
    S7_DERIVED_CONTRACTS,
    S7_RESOURCE_SHA256_BY_TABLE,
)
from ame_stocks_api.silver.identity_source import (
    S7_S4_RELEASE_SET_ID,
    S7_SIX_RELEASE_BINDING_ID,
    S7_SOURCE_PINS,
)

S7_MATERIALIZATION_POLICY_VERSION: Final = "s7-four-table-atomic-candidate-v1"
S7_MATERIALIZATION_SOURCE_BINDING_VERSION: Final = 1
S7_MATERIALIZATION_PLAN_VERSION: Final = 1
S7_MATERIALIZATION_CANDIDATE_VERSION: Final = 1
S7_MATERIALIZATION_STATE: Final = "awaiting_review"

S7_MATERIALIZATION_TABLE_ORDER: Final = (
    "asset_master",
    "ticker_alias",
    "issuer_master",
    "universe_daily",
)
S7_MATERIALIZATION_REGISTRY_ORDER: Final = (
    "identity_adjudication",
    "identity_cross_market_adjudication",
    "provider_composite_override",
    "share_class_adjudication",
    "asset_transition",
)
_REGISTRY_COLUMN_PREFIX: Final[Mapping[str, str]] = MappingProxyType(
    {
        "identity_adjudication": "source_identity_adjudication_release",
        "identity_cross_market_adjudication": ("source_identity_cross_market_adjudication_release"),
        "provider_composite_override": "source_provider_composite_override_release",
        "share_class_adjudication": "source_share_class_adjudication_release",
        "asset_transition": "source_asset_transition_release",
    }
)
_SOURCE_RELEASE_COLUMN: Final[Mapping[str, str]] = MappingProxyType(
    {
        "ticker_event_request_status": "source_s5_status_release_id",
        "ticker_change_event": "source_s5_event_release_id",
        "ticker_overview_safe": "source_s6_overview_release_id",
    }
)
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_SEAL = object()


class S7IdentityMaterializationError(RuntimeError):
    """Raised before a partial S7 candidate can become visible."""


class CompositeRegistryCollisionEvaluationLike(Protocol):
    """Narrow adapter for ``identity_relation_registries`` collision output."""

    raw_match_count: int
    matching_registry_names: tuple[str, ...]
    matching_decision_ids: tuple[str, ...]
    unique_decision_id: str | None
    collision: bool
    backtest_identity_eligible: bool
    identity_resolved: bool
    alias_allowed: bool


@dataclass(frozen=True, slots=True)
class S7MaterializationRegistryPin:
    registry_name: str
    release_id: str
    manifest_path: str
    manifest_sha256: str
    manifest_bytes: int
    release_available_session: date

    def __post_init__(self) -> None:
        if self.registry_name not in S7_MATERIALIZATION_REGISTRY_ORDER:
            raise S7IdentityMaterializationError("unsupported S7 materialization registry")
        _digest(self.release_id, "registry release ID")
        _relative(self.manifest_path, "registry manifest path")
        _digest(self.manifest_sha256, "registry manifest SHA-256")
        _nonnegative(self.manifest_bytes, "registry manifest bytes")
        _date(self.release_available_session, "registry release availability")

    def to_dict(self) -> dict[str, object]:
        return {
            "manifest_bytes": self.manifest_bytes,
            "manifest_path": self.manifest_path,
            "manifest_sha256": self.manifest_sha256,
            "registry_name": self.registry_name,
            "release_available_session": self.release_available_session.isoformat(),
            "release_id": self.release_id,
        }

    @classmethod
    def from_dict(cls, value: object) -> S7MaterializationRegistryPin:
        item = _mapping(value, "registry pin")
        _expect_keys(
            item,
            {
                "manifest_bytes",
                "manifest_path",
                "manifest_sha256",
                "registry_name",
                "release_available_session",
                "release_id",
            },
            "registry pin",
        )
        return cls(
            registry_name=_text(item["registry_name"], "registry name"),
            release_id=_digest(item["release_id"], "registry release ID"),
            manifest_path=_relative(item["manifest_path"], "registry manifest path"),
            manifest_sha256=_digest(item["manifest_sha256"], "registry manifest SHA-256"),
            manifest_bytes=_nonnegative(item["manifest_bytes"], "registry manifest bytes"),
            release_available_session=date.fromisoformat(
                _text(item["release_available_session"], "registry release availability")
            ),
        )


@dataclass(frozen=True, slots=True)
class S7MaterializationSourceBinding:
    identity_resolution_cutoff_session: date
    resolved_graph_digest: str
    table_row_counts: Mapping[str, int]
    registry_pins: tuple[S7MaterializationRegistryPin, ...]
    six_release_binding_id: str = S7_SIX_RELEASE_BINDING_ID
    s4_release_set_id: str = S7_S4_RELEASE_SET_ID

    def __post_init__(self) -> None:
        _date(self.identity_resolution_cutoff_session, "identity resolution cutoff")
        _digest(self.resolved_graph_digest, "resolved graph digest")
        _digest(self.six_release_binding_id, "six-release binding ID")
        _digest(self.s4_release_set_id, "S4 release-set ID")
        if self.six_release_binding_id != S7_SIX_RELEASE_BINDING_ID:
            raise S7IdentityMaterializationError("six-release source binding is not approved")
        if self.s4_release_set_id != S7_S4_RELEASE_SET_ID:
            raise S7IdentityMaterializationError("S4 release-set binding is not approved")
        counts = dict(self.table_row_counts)
        if tuple(counts) != S7_MATERIALIZATION_TABLE_ORDER:
            raise S7IdentityMaterializationError("source binding table order changed")
        for table, count in counts.items():
            _nonnegative(count, f"{table} row count")
        object.__setattr__(self, "table_row_counts", MappingProxyType(counts))
        pins = tuple(self.registry_pins)
        if tuple(item.registry_name for item in pins) != S7_MATERIALIZATION_REGISTRY_ORDER:
            raise S7IdentityMaterializationError("source binding registry order changed")
        if any(
            item.release_available_session > self.identity_resolution_cutoff_session
            for item in pins
        ):
            raise S7IdentityMaterializationError("registry release is unavailable at the cutoff")
        object.__setattr__(self, "registry_pins", pins)

    @property
    def source_binding_id(self) -> str:
        return stable_digest(self.logical_payload())

    @property
    def relative_path(self) -> str:
        return (
            "manifests/silver/identity/s7-materialization-source-bindings/"
            f"source_binding_id={self.source_binding_id}/manifest.json"
        )

    def logical_payload(self) -> dict[str, object]:
        return {
            "identity_resolution_cutoff_session": (
                self.identity_resolution_cutoff_session.isoformat()
            ),
            "policy_version": S7_MATERIALIZATION_POLICY_VERSION,
            "registry_pins": [item.to_dict() for item in self.registry_pins],
            "resolved_graph_digest": self.resolved_graph_digest,
            "s4_release_set_id": self.s4_release_set_id,
            "six_release_binding_id": self.six_release_binding_id,
            "table_row_counts": dict(self.table_row_counts),
        }

    def to_dict(self) -> dict[str, object]:
        return {
            "source_binding_id": self.source_binding_id,
            "source_binding_version": S7_MATERIALIZATION_SOURCE_BINDING_VERSION,
            **self.logical_payload(),
        }

    @classmethod
    def from_dict(cls, value: object) -> S7MaterializationSourceBinding:
        item = _mapping(value, "S7 materialization source binding")
        expected = {
            "identity_resolution_cutoff_session",
            "policy_version",
            "registry_pins",
            "resolved_graph_digest",
            "s4_release_set_id",
            "six_release_binding_id",
            "source_binding_id",
            "source_binding_version",
            "table_row_counts",
        }
        _expect_keys(item, expected, "S7 materialization source binding")
        if item["source_binding_version"] != S7_MATERIALIZATION_SOURCE_BINDING_VERSION:
            raise S7IdentityMaterializationError("unsupported source binding version")
        if item["policy_version"] != S7_MATERIALIZATION_POLICY_VERSION:
            raise S7IdentityMaterializationError("source binding policy changed")
        raw_counts = _mapping(item["table_row_counts"], "table row counts")
        _expect_keys(raw_counts, set(S7_MATERIALIZATION_TABLE_ORDER), "table row counts")
        binding = cls(
            identity_resolution_cutoff_session=date.fromisoformat(
                _text(item["identity_resolution_cutoff_session"], "resolution cutoff")
            ),
            resolved_graph_digest=_digest(item["resolved_graph_digest"], "graph digest"),
            table_row_counts={
                table: _nonnegative(raw_counts.get(table), f"{table} row count")
                for table in S7_MATERIALIZATION_TABLE_ORDER
            },
            registry_pins=tuple(
                S7MaterializationRegistryPin.from_dict(pin)
                for pin in _array(item["registry_pins"], "registry pins")
            ),
            six_release_binding_id=_digest(
                item["six_release_binding_id"], "six-release binding ID"
            ),
            s4_release_set_id=_digest(item["s4_release_set_id"], "S4 release-set ID"),
        )
        if item["source_binding_id"] != binding.source_binding_id:
            raise S7IdentityMaterializationError("source binding ID mismatch")
        return binding


@dataclass(frozen=True, slots=True)
class S7MaterializationPlan:
    source_binding_id: str
    source_binding_path: str
    source_binding_sha256: str
    source_binding_bytes: int
    contract_ids_by_table: Mapping[str, str]
    contract_resource_sha256_by_table: Mapping[str, str]
    collision_review_acceptance_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _digest(self.source_binding_id, "source binding ID")
        _relative(self.source_binding_path, "source binding path")
        _digest(self.source_binding_sha256, "source binding SHA-256")
        _nonnegative(self.source_binding_bytes, "source binding bytes")
        expected_ids = {
            table: S7_DERIVED_CONTRACTS[table].contract_id
            for table in S7_MATERIALIZATION_TABLE_ORDER
        }
        expected_resources = {
            table: S7_RESOURCE_SHA256_BY_TABLE[table] for table in S7_MATERIALIZATION_TABLE_ORDER
        }
        if dict(self.contract_ids_by_table) != expected_ids:
            raise S7IdentityMaterializationError("materialization contract IDs changed")
        if dict(self.contract_resource_sha256_by_table) != expected_resources:
            raise S7IdentityMaterializationError("materialization contract resources changed")
        object.__setattr__(self, "contract_ids_by_table", MappingProxyType(expected_ids))
        object.__setattr__(
            self,
            "contract_resource_sha256_by_table",
            MappingProxyType(expected_resources),
        )
        acceptances = tuple(sorted(self.collision_review_acceptance_ids))
        if len(set(acceptances)) != len(acceptances):
            raise S7IdentityMaterializationError("collision review acceptance IDs repeat")
        for item in acceptances:
            _digest(item, "collision review acceptance ID")
        object.__setattr__(self, "collision_review_acceptance_ids", acceptances)

    @property
    def plan_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "candidate_only": True,
            "collision_review_acceptance_ids": list(self.collision_review_acceptance_ids),
            "contract_ids_by_table": dict(self.contract_ids_by_table),
            "contract_resource_sha256_by_table": dict(self.contract_resource_sha256_by_table),
            "plan_version": S7_MATERIALIZATION_PLAN_VERSION,
            "policy_version": S7_MATERIALIZATION_POLICY_VERSION,
            "publication_authorized": False,
            "source_binding_bytes": self.source_binding_bytes,
            "source_binding_id": self.source_binding_id,
            "source_binding_path": self.source_binding_path,
            "source_binding_sha256": self.source_binding_sha256,
        }


@dataclass(frozen=True, slots=True)
class VerifiedS7MaterializationGraph:
    binding: S7MaterializationSourceBinding
    rows_by_table: Mapping[str, tuple[Mapping[str, object], ...]]
    _seal: object

    def __post_init__(self) -> None:
        if self._seal is not _SEAL:
            raise S7IdentityMaterializationError("resolved graph was not source-bound")


@dataclass(frozen=True, slots=True)
class S7MaterializationCandidateRun:
    candidate_id: str
    candidate_path: str
    manifest_sha256: str
    table_rows: Mapping[str, int]
    raw_collision_rows: int
    idempotent: bool
    state: str = S7_MATERIALIZATION_STATE


def store_s7_materialization_source_binding(
    data_root: Path, binding: S7MaterializationSourceBinding
) -> tuple[str, str, int]:
    """Store one immutable source-binding control document."""

    root = data_root.expanduser().resolve()
    content = _canonical_bytes(binding.to_dict())
    stored = write_bytes_immutable(
        root,
        safe_relative_path(root, binding.relative_path),
        content,
        temporary_directory=root / "tmp" / "s7-materialization-control-writes",
    )
    return str(stored["path"]), str(stored["sha256"]), int(stored["bytes"])


def create_s7_materialization_plan(
    binding: S7MaterializationSourceBinding,
    *,
    source_binding_sha256: str,
    source_binding_bytes: int,
    collision_review_acceptance_ids: Sequence[str] = (),
) -> S7MaterializationPlan:
    return S7MaterializationPlan(
        source_binding_id=binding.source_binding_id,
        source_binding_path=binding.relative_path,
        source_binding_sha256=source_binding_sha256,
        source_binding_bytes=source_binding_bytes,
        contract_ids_by_table={
            table: S7_DERIVED_CONTRACTS[table].contract_id
            for table in S7_MATERIALIZATION_TABLE_ORDER
        },
        contract_resource_sha256_by_table={
            table: S7_RESOURCE_SHA256_BY_TABLE[table] for table in S7_MATERIALIZATION_TABLE_ORDER
        },
        collision_review_acceptance_ids=tuple(collision_review_acceptance_ids),
    )


def verify_s7_materialization_inputs(
    data_root: Path,
    *,
    plan: S7MaterializationPlan,
    rows_by_table: Mapping[str, Sequence[Mapping[str, object]]],
    composite_evaluations_by_source_record_id: Mapping[
        str, CompositeRegistryCollisionEvaluationLike
    ]
    | None = None,
) -> VerifiedS7MaterializationGraph:
    """Reload exact controls and bind the caller's graph by content digest."""

    root = data_root.expanduser().resolve()
    binding_bytes = _read_exact_regular(
        root,
        plan.source_binding_path,
        expected_sha256=plan.source_binding_sha256,
        expected_bytes=plan.source_binding_bytes,
    )
    binding = S7MaterializationSourceBinding.from_dict(_load_json(binding_bytes, "binding"))
    if binding.source_binding_id != plan.source_binding_id:
        raise S7IdentityMaterializationError("plan/source binding ID mismatch")
    for pin in binding.registry_pins:
        _read_exact_regular(
            root,
            pin.manifest_path,
            expected_sha256=pin.manifest_sha256,
            expected_bytes=pin.manifest_bytes,
        )

    normalized = _normalize_rows_by_table(rows_by_table)
    if _resolved_graph_digest(normalized) != binding.resolved_graph_digest:
        raise S7IdentityMaterializationError("resolved graph differs from source binding")
    if {table: len(normalized[table]) for table in S7_MATERIALIZATION_TABLE_ORDER} != dict(
        binding.table_row_counts
    ):
        raise S7IdentityMaterializationError("resolved graph row counts changed")
    _validate_row_release_bindings(normalized, binding)
    if composite_evaluations_by_source_record_id is not None:
        _validate_composite_evaluation_projection(
            normalized["universe_daily"],
            composite_evaluations_by_source_record_id,
        )
    return VerifiedS7MaterializationGraph(
        binding=binding,
        rows_by_table=MappingProxyType(normalized),
        _seal=_SEAL,
    )


def resolved_graph_digest(
    rows_by_table: Mapping[str, Sequence[Mapping[str, object]]],
) -> str:
    """Compute the exact digest a source-binding gate must freeze."""

    return _resolved_graph_digest(_normalize_rows_by_table(rows_by_table))


def materialize_s7_identity_candidate(
    data_root: Path,
    *,
    plan: S7MaterializationPlan,
    graph: VerifiedS7MaterializationGraph,
) -> S7MaterializationCandidateRun:
    """Write all four tables to staging and expose them with one atomic rename."""

    if graph._seal is not _SEAL or graph.binding.source_binding_id != plan.source_binding_id:
        raise S7IdentityMaterializationError("materialization graph is outside the exact plan")
    root = data_root.expanduser().resolve()
    tables = _build_tables(graph.rows_by_table)
    qa = _validate_coordinated_graph(tables)
    candidate_id = stable_digest(
        {
            "contract_ids_by_table": dict(plan.contract_ids_by_table),
            "plan_id": plan.plan_id,
            "resolved_graph_digest": graph.binding.resolved_graph_digest,
            "source_binding_id": graph.binding.source_binding_id,
        }
    )
    candidate_relative = f"silver/identity/s7-derived-candidates/candidate_id={candidate_id}"
    target = safe_relative_path(root, candidate_relative)
    lock = safe_relative_path(
        root,
        f"manifests/silver/locks/s7-materialization-candidate-{candidate_id}.lock",
    )
    with _exclusive_lock(lock):
        if target.exists():
            manifest_sha = _verify_completed_candidate(
                target,
                candidate_id,
                tables,
                plan=plan,
                source_binding_id=graph.binding.source_binding_id,
            )
            return _candidate_run(
                candidate_id,
                candidate_relative,
                manifest_sha,
                tables,
                qa["raw_collision_rows"],
                idempotent=True,
            )
        staging = safe_relative_path(
            root,
            f"tmp/silver-s7-materialization/candidate_id={candidate_id}",
        )
        if staging.exists():
            raise S7IdentityMaterializationError(
                "incomplete S7 materialization staging requires review"
            )
        staging.mkdir(parents=True, exist_ok=False)
        outputs: dict[str, dict[str, object]] = {}
        for table_name in S7_MATERIALIZATION_TABLE_ORDER:
            relative = f"data/{table_name}.parquet"
            path = staging / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            _write_parquet_exclusive(path, tables[table_name])
            outputs[table_name] = _file_receipt(
                path,
                relative,
                row_count=tables[table_name].num_rows,
                schema_digest=S7_DERIVED_CONTRACTS[table_name].schema_digest,
            )
        qa_path = staging / "qa/qa.json"
        qa_path.parent.mkdir(parents=True, exist_ok=True)
        _write_exclusive(qa_path, _canonical_bytes(qa))
        outputs["qa"] = _file_receipt(qa_path, "qa/qa.json")
        manifest = _candidate_manifest(
            candidate_id,
            plan=plan,
            source_binding_id=graph.binding.source_binding_id,
            outputs=outputs,
            qa=qa,
        )
        manifest_path = staging / "manifest.json"
        _write_exclusive(manifest_path, _canonical_bytes(manifest))
        _fsync_tree(staging)
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():  # pragma: no cover - protected by the shared lock
            raise S7IdentityMaterializationError("candidate appeared during atomic commit")
        os.rename(staging, target)
        _fsync_directory(target.parent)
        manifest_sha = _verify_completed_candidate(
            target,
            candidate_id,
            tables,
            plan=plan,
            source_binding_id=graph.binding.source_binding_id,
        )
    return _candidate_run(
        candidate_id,
        candidate_relative,
        manifest_sha,
        tables,
        qa["raw_collision_rows"],
        idempotent=False,
    )


def _candidate_run(
    candidate_id: str,
    candidate_path: str,
    manifest_sha256: str,
    tables: Mapping[str, pa.Table],
    raw_collision_rows: int,
    *,
    idempotent: bool,
) -> S7MaterializationCandidateRun:
    return S7MaterializationCandidateRun(
        candidate_id=candidate_id,
        candidate_path=candidate_path,
        manifest_sha256=manifest_sha256,
        table_rows=MappingProxyType(
            {table: tables[table].num_rows for table in S7_MATERIALIZATION_TABLE_ORDER}
        ),
        raw_collision_rows=raw_collision_rows,
        idempotent=idempotent,
    )


def _normalize_rows_by_table(
    rows_by_table: Mapping[str, Sequence[Mapping[str, object]]],
) -> dict[str, tuple[Mapping[str, object], ...]]:
    if tuple(rows_by_table) != S7_MATERIALIZATION_TABLE_ORDER:
        raise S7IdentityMaterializationError("resolved graph table order changed")
    result: dict[str, tuple[Mapping[str, object], ...]] = {}
    for table in S7_MATERIALIZATION_TABLE_ORDER:
        contract = S7_DERIVED_CONTRACTS[table]
        columns = tuple(column.name for column in contract.columns)
        normalized_rows: list[Mapping[str, object]] = []
        for row in rows_by_table[table]:
            item = dict(row)
            if tuple(item) != columns:
                raise S7IdentityMaterializationError(
                    f"{table} row fields/order differ from the v4 contract"
                )
            normalized_rows.append(MappingProxyType(item))
        result[table] = tuple(normalized_rows)
    return result


def _resolved_graph_digest(
    rows_by_table: Mapping[str, Sequence[Mapping[str, object]]],
) -> str:
    return stable_digest(
        {
            table: [_json_value(dict(row)) for row in rows_by_table[table]]
            for table in S7_MATERIALIZATION_TABLE_ORDER
        }
    )


def _validate_row_release_bindings(
    rows_by_table: Mapping[str, Sequence[Mapping[str, object]]],
    binding: S7MaterializationSourceBinding,
) -> None:
    pins = {item.registry_name: item for item in binding.registry_pins}
    for table in S7_MATERIALIZATION_TABLE_ORDER:
        for row in rows_by_table[table]:
            if row["identity_resolution_cutoff_session"] != (
                binding.identity_resolution_cutoff_session
            ):
                raise S7IdentityMaterializationError("row resolution cutoff changed")
            if row["source_s4_release_set_id"] != binding.s4_release_set_id:
                raise S7IdentityMaterializationError("row S4 release-set binding changed")
            for registry, prefix in _REGISTRY_COLUMN_PREFIX.items():
                pin = pins[registry]
                if (
                    row[f"{prefix}_id"] != pin.release_id
                    or row[f"{prefix}_available_session"] != pin.release_available_session
                ):
                    raise S7IdentityMaterializationError(
                        f"{table} row {registry} release binding changed"
                    )
            for source_table, column in _SOURCE_RELEASE_COLUMN.items():
                if row[column] != S7_SOURCE_PINS[source_table].release_id:
                    raise S7IdentityMaterializationError(
                        f"{table} row {source_table} release binding changed"
                    )


def _validate_composite_evaluation_projection(
    universe_rows: Sequence[Mapping[str, object]],
    evaluations: Mapping[str, CompositeRegistryCollisionEvaluationLike],
) -> None:
    source_ids = {str(row["selected_source_record_id"]) for row in universe_rows}
    if set(evaluations) != source_ids:
        raise S7IdentityMaterializationError("Composite evaluation source-row coverage changed")
    for row in universe_rows:
        evaluation = evaluations[str(row["selected_source_record_id"])]
        if type(evaluation.raw_match_count) is not int or evaluation.raw_match_count < 0:
            raise S7IdentityMaterializationError("Composite registry match count is invalid")
        if row["composite_registry_match_count"] != evaluation.raw_match_count:
            raise S7IdentityMaterializationError(
                "Composite registry match count projection changed"
            )
        if row["composite_registry_collision"] is not evaluation.collision:
            raise S7IdentityMaterializationError("Composite registry collision projection changed")
        decision_ids = tuple(
            item
            for item in (
                row.get("identity_adjudication_id"),
                row.get("cross_market_adjudication_id"),
                row.get("provider_composite_override_id"),
            )
            if item is not None
        )
        registry_names = tuple(
            name
            for name, decision in (
                ("identity_adjudication", row.get("identity_adjudication_id")),
                (
                    "identity_cross_market_adjudication",
                    row.get("cross_market_adjudication_id"),
                ),
                (
                    "provider_composite_override",
                    row.get("provider_composite_override_id"),
                ),
            )
            if decision is not None
        )
        if decision_ids != tuple(evaluation.matching_decision_ids) or registry_names != tuple(
            evaluation.matching_registry_names
        ):
            raise S7IdentityMaterializationError(
                "Composite registry match lineage projection changed"
            )
        if evaluation.raw_match_count == 1 and decision_ids != (evaluation.unique_decision_id,):
            raise S7IdentityMaterializationError("unique Composite registry decision changed")
        if evaluation.collision and (
            evaluation.backtest_identity_eligible
            or evaluation.identity_resolved
            or evaluation.alias_allowed
        ):
            raise S7IdentityMaterializationError("collision evaluator did not fail closed")


def _build_tables(
    rows_by_table: Mapping[str, Sequence[Mapping[str, object]]],
) -> Mapping[str, pa.Table]:
    tables: dict[str, pa.Table] = {}
    for table_name in S7_MATERIALIZATION_TABLE_ORDER:
        contract = S7_DERIVED_CONTRACTS[table_name]
        try:
            table = pa.Table.from_pylist(
                [dict(item) for item in rows_by_table[table_name]],
                schema=contract.arrow_schema,
            )
        except (pa.ArrowException, TypeError, ValueError) as exc:
            raise S7IdentityMaterializationError(
                f"cannot construct exact {table_name} Arrow table"
            ) from exc
        if table.schema != contract.arrow_schema:
            raise S7IdentityMaterializationError(f"{table_name} schema changed")
        for field, column in zip(table.schema, table.columns, strict=True):
            if not field.nullable and column.null_count:
                raise S7IdentityMaterializationError(
                    f"{table_name}.{field.name} contains forbidden nulls"
                )
        if table.num_rows:
            table = table.sort_by([(name, "ascending") for name in contract.sort_by])
        _validate_primary_key(table, contract)
        tables[table_name] = table
    return MappingProxyType(tables)


def _validate_primary_key(table: pa.Table, contract: TableContract) -> None:
    keys = list(zip(*(table[name].to_pylist() for name in contract.primary_key), strict=True))
    if len(keys) != len(set(keys)):
        raise S7IdentityMaterializationError(f"{contract.table} primary key is duplicated")


def _validate_coordinated_graph(tables: Mapping[str, pa.Table]) -> dict[str, object]:
    assets = {str(item) for item in tables["asset_master"]["asset_id"].to_pylist()}
    issuers = {str(item) for item in tables["issuer_master"]["issuer_id"].to_pylist()}
    alias_rows = tables["ticker_alias"].to_pylist()
    aliases = {str(item["ticker_alias_id"]): item for item in alias_rows}
    universe_rows = tables["universe_daily"].to_pylist()

    relation_invalid = 0
    asset_rows = {str(item["asset_id"]): item for item in tables["asset_master"].to_pylist()}
    for asset_id, row in asset_rows.items():
        predecessors = tuple(row["predecessor_asset_ids"])
        successors = tuple(row["successor_asset_ids"])
        if (
            predecessors != tuple(sorted(set(predecessors)))
            or successors != tuple(sorted(set(successors)))
            or asset_id in predecessors
            or asset_id in successors
            or any(item not in assets for item in (*predecessors, *successors))
        ):
            relation_invalid += 1
            continue
        relation_invalid += sum(
            asset_id not in asset_rows[item]["successor_asset_ids"] for item in predecessors
        )
        relation_invalid += sum(
            asset_id not in asset_rows[item]["predecessor_asset_ids"] for item in successors
        )
    if relation_invalid:
        raise S7IdentityMaterializationError("asset_transition relation graph is invalid")

    for alias in alias_rows:
        if (
            alias["asset_id"] not in assets
            or alias["backtest_identity_eligible"] is not True
            or alias["composite_registry_collision"] is not False
            or alias["composite_registry_match_count"] not in {0, 1}
        ):
            raise S7IdentityMaterializationError("ticker_alias eligibility graph is invalid")
        _validate_decision_pair(alias, "provider_composite_override")
        _validate_decision_pair(alias, "share_class_adjudication")
        _validate_sorted_ids(alias["asset_transition_ids"], "alias transition IDs")
        if alias["share_class_adjudication_id"] is not None and (
            alias["canonical_composite_figi"] is None or alias["asset_id"] is None
        ):
            raise S7IdentityMaterializationError(
                "share-class adjudication precedes canonical Composite resolution"
            )

    collision_examples: list[dict[str, object]] = []
    collision_rows = 0
    for row in universe_rows:
        if row["active_on_date"] is not True:
            raise S7IdentityMaterializationError("universe_daily contains inactive membership")
        _validate_decision_pair(row, "provider_composite_override")
        _validate_decision_pair(row, "share_class_adjudication")
        _validate_sorted_ids(row["asset_transition_ids"], "universe transition IDs")
        collision = bool(row["composite_registry_collision"])
        count = int(row["composite_registry_match_count"])
        if collision != (count > 1):
            raise S7IdentityMaterializationError("Composite collision count/state mismatch")
        if collision:
            collision_rows += 1
            if (
                row["backtest_identity_eligible"]
                or row["asset_id"] is not None
                or row["canonical_composite_figi"] is not None
                or row["ticker_alias_id"] is not None
                or row["identity_resolution_status"] != "unresolved_registry_collision"
            ):
                raise S7IdentityMaterializationError("Composite collision did not fail closed")
            if len(collision_examples) < 20:
                collision_examples.append(
                    {
                        "selected_source_record_id": row["selected_source_record_id"],
                        "session_date": row["session_date"].isoformat(),
                        "ticker": row["ticker"],
                    }
                )
            continue
        if count not in {0, 1}:
            raise S7IdentityMaterializationError("Composite registry match count is invalid")
        if row["backtest_identity_eligible"]:
            asset_id = row["asset_id"]
            alias_id = row["ticker_alias_id"]
            if asset_id not in assets or alias_id not in aliases:
                raise S7IdentityMaterializationError("eligible universe FK is missing")
            alias = aliases[str(alias_id)]
            if not (
                alias["asset_id"] == asset_id
                and alias["ticker"] == row["ticker"]
                and alias["valid_from_session"]
                <= row["session_date"]
                <= alias["valid_through_session"]
            ):
                raise S7IdentityMaterializationError("alias/universe coverage differs")
        if row["issuer_id"] is not None and row["issuer_id"] not in issuers:
            raise S7IdentityMaterializationError("universe issuer FK is missing")
        if row["share_class_adjudication_id"] is not None and (
            row["canonical_composite_figi"] is None or row["asset_id"] is None
        ):
            raise S7IdentityMaterializationError(
                "share-class adjudication precedes canonical Composite resolution"
            )
    return {
        "critical_failure_count": 0,
        "multi_registry_composite_override_collision_alias_rows": 0,
        "multi_registry_composite_override_collision_eligible_rows": 0,
        "multi_registry_composite_override_collision_resolved_rows": 0,
        "multi_registry_composite_override_collision_rows": collision_rows,
        "raw_collision_bounded_examples": collision_examples,
        "raw_collision_rows": collision_rows,
        "state": S7_MATERIALIZATION_STATE,
    }


def _validate_decision_pair(row: Mapping[str, object], prefix: str) -> None:
    decision = row[f"{prefix}_id"]
    available = row[f"{prefix}_available_session"]
    if (decision is None) != (available is None):
        raise S7IdentityMaterializationError(f"{prefix} ID/availability are not jointly null")
    if decision is not None:
        _digest(decision, f"{prefix} decision ID")
        if available > row["identity_resolution_cutoff_session"]:
            raise S7IdentityMaterializationError(f"{prefix} decision is unavailable at cutoff")


def _validate_sorted_ids(value: object, label: str) -> None:
    if not isinstance(value, list):
        raise S7IdentityMaterializationError(f"{label} must be a list")
    if value != sorted(set(value)):
        raise S7IdentityMaterializationError(f"{label} must be sorted and distinct")
    for item in value:
        _digest(item, label)


def _candidate_manifest(
    candidate_id: str,
    *,
    plan: S7MaterializationPlan,
    source_binding_id: str,
    outputs: Mapping[str, Mapping[str, object]],
    qa: Mapping[str, object],
) -> dict[str, object]:
    return {
        "candidate_id": candidate_id,
        "candidate_version": S7_MATERIALIZATION_CANDIDATE_VERSION,
        "capabilities": {
            "full_run_authorized": False,
            "publish_authorized": False,
            "registry_mutation_authorized": False,
        },
        "collision_review_acceptance_ids": list(plan.collision_review_acceptance_ids),
        "collision_review_required": qa["raw_collision_rows"] > 0,
        "contract_ids_by_table": dict(plan.contract_ids_by_table),
        "outputs": {key: dict(value) for key, value in outputs.items()},
        "plan_id": plan.plan_id,
        "policy_version": S7_MATERIALIZATION_POLICY_VERSION,
        "source_binding_id": source_binding_id,
        "source_binding_path": plan.source_binding_path,
        "source_binding_sha256": plan.source_binding_sha256,
        "state": S7_MATERIALIZATION_STATE,
    }


def _verify_completed_candidate(
    target: Path,
    candidate_id: str,
    expected_tables: Mapping[str, pa.Table],
    *,
    plan: S7MaterializationPlan,
    source_binding_id: str,
) -> str:
    if target.is_symlink() or not target.is_dir():
        raise S7IdentityMaterializationError("candidate path is not a regular directory")
    expected_files = {
        "manifest.json",
        "qa/qa.json",
        *(f"data/{table}.parquet" for table in S7_MATERIALIZATION_TABLE_ORDER),
    }
    actual_files = {
        item.relative_to(target).as_posix() for item in target.rglob("*") if item.is_file()
    }
    if actual_files != expected_files:
        raise S7IdentityMaterializationError("candidate file set changed")
    manifest_path = target / "manifest.json"
    manifest_bytes = _read_regular_path(manifest_path)
    manifest = _load_json(manifest_bytes, "candidate manifest")
    expected_qa = _validate_coordinated_graph(expected_tables)
    qa_path = target / "qa/qa.json"
    if _read_regular_path(qa_path) != _canonical_bytes(expected_qa):
        raise S7IdentityMaterializationError("completed candidate QA changed")
    expected_outputs: dict[str, dict[str, object]] = {}
    for table in S7_MATERIALIZATION_TABLE_ORDER:
        path = target / f"data/{table}.parquet"
        content = _read_regular_path(path)
        observed = pq.read_table(path)
        if observed.schema != S7_DERIVED_CONTRACTS[table].arrow_schema or (
            observed.to_pylist() != expected_tables[table].to_pylist()
        ):
            raise S7IdentityMaterializationError(f"completed {table} output changed")
        expected_outputs[table] = {
            "bytes": len(content),
            "path": f"data/{table}.parquet",
            "row_count": observed.num_rows,
            "schema_digest": S7_DERIVED_CONTRACTS[table].schema_digest,
            "sha256": hashlib.sha256(content).hexdigest(),
        }
    expected_outputs["qa"] = _file_receipt(qa_path, "qa/qa.json")
    expected_manifest = _candidate_manifest(
        candidate_id,
        plan=plan,
        source_binding_id=source_binding_id,
        outputs=expected_outputs,
        qa=expected_qa,
    )
    if manifest != expected_manifest:
        raise S7IdentityMaterializationError("candidate manifest binding changed")
    return hashlib.sha256(manifest_bytes).hexdigest()


def _file_receipt(
    path: Path,
    relative_path: str,
    *,
    row_count: int | None = None,
    schema_digest: str | None = None,
) -> dict[str, object]:
    content = _read_regular_path(path)
    return {
        "bytes": len(content),
        "path": relative_path,
        "row_count": row_count,
        "schema_digest": schema_digest,
        "sha256": hashlib.sha256(content).hexdigest(),
    }


def _write_parquet_exclusive(path: Path, table: pa.Table) -> None:
    if path.exists():
        raise S7IdentityMaterializationError("staging Parquet already exists")
    try:
        pq.write_table(
            table,
            path,
            compression="zstd",
            version="2.6",
            write_statistics=True,
        )
    except (OSError, pa.ArrowException) as exc:
        raise S7IdentityMaterializationError("cannot write S7 staging Parquet") from exc
    _fsync_regular_file(path)


def _write_exclusive(path: Path, content: bytes) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags, 0o600)
    except OSError as exc:
        raise S7IdentityMaterializationError(f"cannot create immutable file: {path}") from exc
    try:
        offset = 0
        while offset < len(content):
            written = os.write(descriptor, content[offset:])
            if written <= 0:  # pragma: no cover - defensive OS boundary
                raise S7IdentityMaterializationError("immutable file write made no progress")
            offset += written
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


class _exclusive_lock:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.descriptor: int | None = None

    def __enter__(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        flags = os.O_RDWR | os.O_CREAT
        if hasattr(os, "O_CLOEXEC"):
            flags |= os.O_CLOEXEC
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = os.open(self.path, flags, 0o600)
        file_stat = os.fstat(descriptor)
        if not stat.S_ISREG(file_stat.st_mode) or file_stat.st_nlink != 1:
            os.close(descriptor)
            raise S7IdentityMaterializationError("materialization lock is unsafe")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            os.close(descriptor)
            raise S7IdentityMaterializationError(
                "another S7 materializer holds the candidate lock"
            ) from exc
        self.descriptor = descriptor

    def __exit__(self, *_: object) -> None:
        assert self.descriptor is not None
        fcntl.flock(self.descriptor, fcntl.LOCK_UN)
        os.close(self.descriptor)


def _read_exact_regular(
    root: Path, relative: str, *, expected_sha256: str, expected_bytes: int
) -> bytes:
    content = _read_regular_path(safe_relative_path(root, relative))
    if len(content) != expected_bytes or hashlib.sha256(content).hexdigest() != expected_sha256:
        raise S7IdentityMaterializationError(f"immutable input changed: {relative}")
    return content


def _read_regular_path(path: Path) -> bytes:
    if path.is_symlink() or not path.is_file():
        raise S7IdentityMaterializationError(f"input is not a regular file: {path}")
    try:
        return path.read_bytes()
    except OSError as exc:
        raise S7IdentityMaterializationError(f"cannot read file: {path}") from exc


def _fsync_regular_file(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _fsync_tree(root: Path) -> None:
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise S7IdentityMaterializationError("staging tree contains a symlink")
        if path.is_file():
            _fsync_regular_file(path)
    for path in sorted(
        (item for item in root.rglob("*") if item.is_dir()),
        key=lambda item: len(item.parts),
        reverse=True,
    ):
        _fsync_directory(path)
    _fsync_directory(root)


def _canonical_bytes(value: Mapping[str, object]) -> bytes:
    return (
        json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True).encode() + b"\n"
    )


def _load_json(content: bytes, label: str) -> dict[str, object]:
    try:
        value = json.loads(content, object_pairs_hook=_reject_duplicates)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise S7IdentityMaterializationError(f"{label} is invalid JSON") from exc
    if not isinstance(value, dict) or _canonical_bytes(value) != content:
        raise S7IdentityMaterializationError(f"{label} is not canonical JSON")
    return value


def _json_value(value: object) -> object:
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


def _mapping(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise S7IdentityMaterializationError(f"{label} must be an object")
    return dict(value)


def _array(value: object, label: str) -> list[object]:
    if not isinstance(value, list):
        raise S7IdentityMaterializationError(f"{label} must be an array")
    return value


def _expect_keys(value: Mapping[str, object], expected: set[str], label: str) -> None:
    if set(value) != expected:
        raise S7IdentityMaterializationError(f"{label} fields changed")


def _text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise S7IdentityMaterializationError(f"{label} must be trimmed text")
    return value


def _digest(value: object, label: str) -> str:
    if not isinstance(value, str) or not _DIGEST.fullmatch(value):
        raise S7IdentityMaterializationError(f"{label} must be a lowercase SHA-256")
    return value


def _relative(value: object, label: str) -> str:
    text = _text(value, label)
    path = Path(text)
    if path.is_absolute() or path.as_posix() != text or ".." in path.parts:
        raise S7IdentityMaterializationError(f"{label} must be a normalized relative path")
    return text


def _nonnegative(value: object, label: str) -> int:
    if type(value) is not int or value < 0:
        raise S7IdentityMaterializationError(f"{label} must be a nonnegative integer")
    return value


def _date(value: object, label: str) -> date:
    if type(value) is not date:
        raise S7IdentityMaterializationError(f"{label} must be a date")
    return value


def _reject_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate key: {key}")
        result[key] = value
    return result


__all__ = [
    "CompositeRegistryCollisionEvaluationLike",
    "S7IdentityMaterializationError",
    "S7MaterializationCandidateRun",
    "S7MaterializationPlan",
    "S7MaterializationRegistryPin",
    "S7MaterializationSourceBinding",
    "VerifiedS7MaterializationGraph",
    "create_s7_materialization_plan",
    "materialize_s7_identity_candidate",
    "resolved_graph_digest",
    "store_s7_materialization_source_binding",
    "verify_s7_materialization_inputs",
]
