"""Typed, content-addressed controls for one S4 Assets session increment.

The normal S4 hot path persists only a run spec and a final run receipt.  Source,
frontier, reference and partition objects are nested values rather than another
plan/request/approval workflow.  The existing ten-year Full controls remain the
independent reconciliation path.
"""

from __future__ import annotations

import math
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import StrEnum
from pathlib import PurePosixPath
from types import MappingProxyType

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver.contracts import (
    SilverContractError,
    SourceInventory,
    SourceLayer,
)
from ame_stocks_api.silver.incremental_contract import ArtifactPin

_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_GIT_COMMIT = re.compile(r"^[0-9a-f]{40}$")
_S4_TABLES = frozenset(
    {
        "asset_observation_daily",
        "asset_observation_version",
        "universe_source_daily",
    }
)


class S4AssetIncrementalContractError(SilverContractError):
    """Raised when a single-session S4 control is ambiguous or unsafe."""


class S4ParentKind(StrEnum):
    """Closed parent-frontier representation."""

    BASE_RELEASE = "base_release"
    SESSION_RECEIPT = "session_receipt"


@dataclass(frozen=True, slots=True)
class S4BaseFrontier:
    """Metadata-only adapter for the immutable published S4 base frontier."""

    base_release_set_id: str
    terminal_session: date
    terminal_partition_set_digest: str
    calendar_artifact_id: str
    reference_binding_id: str
    contract_ids_by_table: Mapping[str, str]
    schema_digests_by_table: Mapping[str, str]
    transform_semantics_digest: str
    parquet_writer_policy: Mapping[str, object]
    release_available_session: date

    def __post_init__(self) -> None:
        _digest(self.base_release_set_id, "base release-set ID")
        _session(self.terminal_session, "base terminal session")
        _digest(self.terminal_partition_set_digest, "base terminal partition-set digest")
        _digest(self.calendar_artifact_id, "base calendar artifact ID")
        _digest(self.reference_binding_id, "base reference binding ID")
        contract_ids = _digest_map(self.contract_ids_by_table, "base S4 contract ID")
        schema_digests = _digest_map(self.schema_digests_by_table, "base S4 schema digest")
        if set(contract_ids) != _S4_TABLES or set(schema_digests) != _S4_TABLES:
            raise S4AssetIncrementalContractError(
                "S4 base frontier must bind exactly the three approved tables"
            )
        _digest(self.transform_semantics_digest, "base transform semantics digest")
        writer_policy = _json_mapping(self.parquet_writer_policy, "base Parquet writer policy")
        if not writer_policy:
            raise S4AssetIncrementalContractError("base Parquet writer policy cannot be empty")
        available = _session(
            self.release_available_session,
            "base release available session",
        )
        if available < self.terminal_session:
            raise S4AssetIncrementalContractError(
                "base release availability precedes its terminal session"
            )
        object.__setattr__(self, "contract_ids_by_table", contract_ids)
        object.__setattr__(self, "schema_digests_by_table", schema_digests)
        object.__setattr__(self, "parquet_writer_policy", writer_policy)

    @property
    def frontier_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "base_release_set_id": self.base_release_set_id,
            "calendar_artifact_id": self.calendar_artifact_id,
            "contract_ids_by_table": dict(self.contract_ids_by_table),
            "parquet_writer_policy": dict(self.parquet_writer_policy),
            "reference_binding_id": self.reference_binding_id,
            "release_available_session": self.release_available_session.isoformat(),
            "rule_version": "s4_assets_base_frontier_v1",
            "schema_digests_by_table": dict(self.schema_digests_by_table),
            "terminal_partition_set_digest": self.terminal_partition_set_digest,
            "terminal_session": self.terminal_session.isoformat(),
            "transform_semantics_digest": self.transform_semantics_digest,
        }

    def to_dict(self) -> dict[str, object]:
        return {"frontier_id": self.frontier_id, **self.logical_payload()}

    @classmethod
    def from_dict(cls, value: object) -> S4BaseFrontier:
        document = _mapping(value, "S4 base frontier")
        _exact_keys(
            document,
            {
                "base_release_set_id",
                "calendar_artifact_id",
                "contract_ids_by_table",
                "frontier_id",
                "parquet_writer_policy",
                "reference_binding_id",
                "release_available_session",
                "rule_version",
                "schema_digests_by_table",
                "terminal_partition_set_digest",
                "terminal_session",
                "transform_semantics_digest",
            },
            "S4 base frontier",
        )
        if document["rule_version"] != "s4_assets_base_frontier_v1":
            raise S4AssetIncrementalContractError("S4 base frontier rule version changed")
        result = cls(
            base_release_set_id=_text(document["base_release_set_id"], "base release-set ID"),
            terminal_session=_date_text(document["terminal_session"], "base terminal session"),
            terminal_partition_set_digest=_text(
                document["terminal_partition_set_digest"],
                "base terminal partition-set digest",
            ),
            calendar_artifact_id=_text(
                document["calendar_artifact_id"],
                "base calendar artifact ID",
            ),
            reference_binding_id=_text(
                document["reference_binding_id"],
                "base reference binding ID",
            ),
            contract_ids_by_table=_mapping(
                document["contract_ids_by_table"],
                "base S4 contract IDs",
            ),
            schema_digests_by_table=_mapping(
                document["schema_digests_by_table"],
                "base S4 schema digests",
            ),
            transform_semantics_digest=_text(
                document["transform_semantics_digest"],
                "base transform semantics digest",
            ),
            parquet_writer_policy=_mapping(
                document["parquet_writer_policy"],
                "base Parquet writer policy",
            ),
            release_available_session=_date_text(
                document["release_available_session"],
                "base release available session",
            ),
        )
        if document["frontier_id"] != result.frontier_id:
            raise S4AssetIncrementalContractError("S4 base frontier ID does not reproduce")
        return result


@dataclass(frozen=True, slots=True)
class S4ParentFrontierPin:
    """Exact metadata artifact used as the next session's parent frontier."""

    parent_kind: S4ParentKind
    terminal_session: date
    terminal_receipt_id: str
    artifact: ArtifactPin

    def __post_init__(self) -> None:
        if not isinstance(self.parent_kind, S4ParentKind):
            raise S4AssetIncrementalContractError("S4 parent kind is invalid")
        _session(self.terminal_session, "parent terminal session")
        _digest(self.terminal_receipt_id, "parent terminal receipt ID")
        if not isinstance(self.artifact, ArtifactPin):
            raise S4AssetIncrementalContractError("S4 parent artifact pin is invalid")

    @property
    def parent_frontier_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "parent_kind": self.parent_kind.value,
            "rule_version": "s4_assets_parent_frontier_pin_v1",
            "terminal_receipt_id": self.terminal_receipt_id,
            "terminal_session": self.terminal_session.isoformat(),
        }

    def to_dict(self) -> dict[str, object]:
        return {
            "artifact": self.artifact.to_dict(),
            "parent_frontier_id": self.parent_frontier_id,
            **self.logical_payload(),
        }

    @classmethod
    def from_dict(cls, value: object) -> S4ParentFrontierPin:
        document = _mapping(value, "S4 parent frontier pin")
        _exact_keys(
            document,
            {
                "artifact",
                "parent_frontier_id",
                "parent_kind",
                "rule_version",
                "terminal_receipt_id",
                "terminal_session",
            },
            "S4 parent frontier pin",
        )
        if document["rule_version"] != "s4_assets_parent_frontier_pin_v1":
            raise S4AssetIncrementalContractError("S4 parent frontier rule version changed")
        try:
            parent_kind = S4ParentKind(document["parent_kind"])
        except (TypeError, ValueError) as exc:
            raise S4AssetIncrementalContractError("S4 parent kind is invalid") from exc
        result = cls(
            parent_kind=parent_kind,
            terminal_session=_date_text(
                document["terminal_session"],
                "parent terminal session",
            ),
            terminal_receipt_id=_text(
                document["terminal_receipt_id"],
                "parent terminal receipt ID",
            ),
            artifact=ArtifactPin(**_artifact_kwargs(document["artifact"])),
        )
        if document["parent_frontier_id"] != result.parent_frontier_id:
            raise S4AssetIncrementalContractError("S4 parent frontier ID does not reproduce")
        return result


@dataclass(frozen=True, slots=True)
class S4ReferenceBinding:
    """Exact S1/S2 vocabulary values and release-document pins."""

    ticker_types: tuple[str, ...]
    exchange_mics: tuple[str, ...]
    dependency_pins: tuple[ArtifactPin, ...]

    def __post_init__(self) -> None:
        ticker_types = _sorted_unique_tokens(self.ticker_types, "ticker type")
        exchange_mics = _sorted_unique_tokens(self.exchange_mics, "exchange MIC")
        pins = tuple(self.dependency_pins)
        if len(pins) != 2 or not all(isinstance(item, ArtifactPin) for item in pins):
            raise S4AssetIncrementalContractError(
                "S4 reference binding requires exact S1 and S2 dependency pins"
            )
        if tuple(item.path for item in pins) != tuple(sorted({item.path for item in pins})):
            raise S4AssetIncrementalContractError(
                "S4 reference dependency pins must be sorted with unique paths"
            )
        object.__setattr__(self, "ticker_types", ticker_types)
        object.__setattr__(self, "exchange_mics", exchange_mics)
        object.__setattr__(self, "dependency_pins", pins)

    @property
    def binding_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "dependency_pins": [item.to_dict() for item in self.dependency_pins],
            "exchange_mics": list(self.exchange_mics),
            "rule_version": "s4_assets_reference_binding_v1",
            "ticker_types": list(self.ticker_types),
        }

    def to_dict(self) -> dict[str, object]:
        return {"binding_id": self.binding_id, **self.logical_payload()}

    @classmethod
    def from_dict(cls, value: object) -> S4ReferenceBinding:
        document = _mapping(value, "S4 reference binding")
        _exact_keys(
            document,
            {
                "binding_id",
                "dependency_pins",
                "exchange_mics",
                "rule_version",
                "ticker_types",
            },
            "S4 reference binding",
        )
        if document["rule_version"] != "s4_assets_reference_binding_v1":
            raise S4AssetIncrementalContractError("S4 reference binding rule version changed")
        result = cls(
            ticker_types=_string_tuple(document["ticker_types"], "ticker types"),
            exchange_mics=_string_tuple(document["exchange_mics"], "exchange MICs"),
            dependency_pins=tuple(
                ArtifactPin(**_artifact_kwargs(item))
                for item in _array(document["dependency_pins"], "dependency pins")
            ),
        )
        if document["binding_id"] != result.binding_id:
            raise S4AssetIncrementalContractError("S4 reference binding ID does not reproduce")
        return result


@dataclass(frozen=True, slots=True)
class S4SessionSourceBinding:
    """Manifest-derived exact Bronze pair for one session."""

    session_date: date
    inventory: SourceInventory
    active_request_id: str
    inactive_request_id: str
    pair_capture_completed_at_utc: datetime
    pair_available_session: date
    page_count: int
    declared_row_count: int

    def __post_init__(self) -> None:
        session = _session(self.session_date, "source session")
        if not isinstance(self.inventory, SourceInventory):
            raise S4AssetIncrementalContractError("S4 source inventory is invalid")
        if (
            self.inventory.source_dataset != "assets"
            or self.inventory.source_layer is not SourceLayer.BRONZE
        ):
            raise S4AssetIncrementalContractError("S4 source binding must use Bronze assets")
        if len(self.inventory.upstream_manifests) != 2:
            raise S4AssetIncrementalContractError(
                "S4 source binding requires exactly two manifests"
            )
        _digest(self.active_request_id, "active request ID")
        _digest(self.inactive_request_id, "inactive request ID")
        if self.active_request_id == self.inactive_request_id:
            raise S4AssetIncrementalContractError("active and inactive request IDs collide")
        capture = _aware_utc(
            self.pair_capture_completed_at_utc,
            "pair capture completion",
        )
        object.__setattr__(self, "pair_capture_completed_at_utc", capture)
        available = _session(self.pair_available_session, "pair available session")
        if available < session:
            raise S4AssetIncrementalContractError("pair availability precedes the source session")
        _positive_int(self.page_count, "source page count")
        _nonnegative_int(self.declared_row_count, "declared source rows")
        if len(self.inventory.artifacts) != self.page_count:
            raise S4AssetIncrementalContractError(
                "source page count differs from its exact inventory"
            )
        if sum(item.row_count for item in self.inventory.artifacts) != self.declared_row_count:
            raise S4AssetIncrementalContractError(
                "declared source rows differ from the exact inventory"
            )

    @property
    def source_binding_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        inventory_payload = self.inventory.logical_payload()
        inventory_payload.pop("git_commit")
        return {
            "active_request_id": self.active_request_id,
            "declared_row_count": self.declared_row_count,
            "inactive_request_id": self.inactive_request_id,
            "inventory": inventory_payload,
            "page_count": self.page_count,
            "pair_available_session": self.pair_available_session.isoformat(),
            "pair_capture_completed_at_utc": _format_utc(self.pair_capture_completed_at_utc),
            "rule_version": "s4_assets_session_source_binding_v1",
            "session_date": self.session_date.isoformat(),
        }

    def to_dict(self) -> dict[str, object]:
        return {
            "active_request_id": self.active_request_id,
            "declared_row_count": self.declared_row_count,
            "inactive_request_id": self.inactive_request_id,
            "inventory": self.inventory.to_dict(),
            "page_count": self.page_count,
            "pair_available_session": self.pair_available_session.isoformat(),
            "pair_capture_completed_at_utc": _format_utc(self.pair_capture_completed_at_utc),
            "rule_version": "s4_assets_session_source_binding_v1",
            "session_date": self.session_date.isoformat(),
            "source_binding_id": self.source_binding_id,
        }

    @classmethod
    def from_dict(cls, value: object) -> S4SessionSourceBinding:
        document = _mapping(value, "S4 source binding")
        _exact_keys(
            document,
            {
                "active_request_id",
                "declared_row_count",
                "inactive_request_id",
                "inventory",
                "page_count",
                "pair_available_session",
                "pair_capture_completed_at_utc",
                "rule_version",
                "session_date",
                "source_binding_id",
            },
            "S4 source binding",
        )
        if document["rule_version"] != "s4_assets_session_source_binding_v1":
            raise S4AssetIncrementalContractError("S4 source binding rule version changed")
        result = cls(
            session_date=_date_text(document["session_date"], "source session"),
            inventory=SourceInventory.from_dict(document["inventory"]),
            active_request_id=_text(document["active_request_id"], "active request ID"),
            inactive_request_id=_text(
                document["inactive_request_id"],
                "inactive request ID",
            ),
            pair_capture_completed_at_utc=_datetime_text(
                document["pair_capture_completed_at_utc"],
                "pair capture completion",
            ),
            pair_available_session=_date_text(
                document["pair_available_session"],
                "pair available session",
            ),
            page_count=_int(document["page_count"], "source page count"),
            declared_row_count=_int(
                document["declared_row_count"],
                "declared source rows",
            ),
        )
        if document["source_binding_id"] != result.source_binding_id:
            raise S4AssetIncrementalContractError("S4 source binding ID does not reproduce")
        return result


@dataclass(frozen=True, slots=True)
class S4SessionRunSpec:
    """One immutable, manifest-derived execution intent."""

    parent_frontier: S4ParentFrontierPin
    source_binding: S4SessionSourceBinding
    reference_binding: S4ReferenceBinding
    calendar_artifact_id: str
    calendar_artifact: ArtifactPin
    contract_ids_by_table: Mapping[str, str]
    schema_digests_by_table: Mapping[str, str]
    transform_semantics_digest: str
    parquet_writer_policy: Mapping[str, object]
    receipt_available_session: date
    writer_git_commit: str

    def __post_init__(self) -> None:
        if not isinstance(self.parent_frontier, S4ParentFrontierPin):
            raise S4AssetIncrementalContractError("S4 run spec parent is invalid")
        if not isinstance(self.source_binding, S4SessionSourceBinding):
            raise S4AssetIncrementalContractError("S4 run spec source binding is invalid")
        if not isinstance(self.reference_binding, S4ReferenceBinding):
            raise S4AssetIncrementalContractError("S4 run spec reference binding is invalid")
        _digest(self.calendar_artifact_id, "calendar artifact ID")
        if not isinstance(self.calendar_artifact, ArtifactPin):
            raise S4AssetIncrementalContractError("calendar artifact pin is invalid")
        contract_ids = _digest_map(self.contract_ids_by_table, "S4 contract ID")
        schema_digests = _digest_map(self.schema_digests_by_table, "S4 schema digest")
        if set(contract_ids) != _S4_TABLES or set(schema_digests) != _S4_TABLES:
            raise S4AssetIncrementalContractError(
                "S4 run spec must bind exactly the three approved tables"
            )
        _digest(self.transform_semantics_digest, "transform semantics digest")
        writer_policy = _json_mapping(self.parquet_writer_policy, "Parquet writer policy")
        if not writer_policy:
            raise S4AssetIncrementalContractError("Parquet writer policy cannot be empty")
        receipt_available = _session(
            self.receipt_available_session,
            "receipt available session",
        )
        if receipt_available < self.source_binding.pair_available_session:
            raise S4AssetIncrementalContractError(
                "receipt availability precedes source-pair availability"
            )
        if not isinstance(self.writer_git_commit, str) or not _GIT_COMMIT.fullmatch(
            self.writer_git_commit
        ):
            raise S4AssetIncrementalContractError(
                "writer Git commit must be a full lowercase object ID"
            )
        object.__setattr__(self, "contract_ids_by_table", contract_ids)
        object.__setattr__(self, "schema_digests_by_table", schema_digests)
        object.__setattr__(self, "parquet_writer_policy", writer_policy)

    @property
    def run_spec_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "calendar_artifact": self.calendar_artifact.to_dict(),
            "calendar_artifact_id": self.calendar_artifact_id,
            "contract_ids_by_table": dict(self.contract_ids_by_table),
            "parent_frontier_id": self.parent_frontier.parent_frontier_id,
            "parquet_writer_policy": dict(self.parquet_writer_policy),
            "reference_binding_id": self.reference_binding.binding_id,
            "receipt_available_session": self.receipt_available_session.isoformat(),
            "rule_version": "s4_assets_session_run_spec_v1",
            "schema_digests_by_table": dict(self.schema_digests_by_table),
            "source_binding_id": self.source_binding.source_binding_id,
            "transform_semantics_digest": self.transform_semantics_digest,
        }

    def to_dict(self) -> dict[str, object]:
        return {
            **self.logical_payload(),
            "parent_frontier": self.parent_frontier.to_dict(),
            "reference_binding": self.reference_binding.to_dict(),
            "run_spec_id": self.run_spec_id,
            "source_binding": self.source_binding.to_dict(),
            "writer_runtime_provenance": {"git_commit": self.writer_git_commit},
        }

    @classmethod
    def from_dict(cls, value: object) -> S4SessionRunSpec:
        document = _mapping(value, "S4 session run spec")
        _exact_keys(
            document,
            {
                "calendar_artifact",
                "calendar_artifact_id",
                "contract_ids_by_table",
                "parent_frontier",
                "parent_frontier_id",
                "parquet_writer_policy",
                "reference_binding",
                "reference_binding_id",
                "receipt_available_session",
                "rule_version",
                "run_spec_id",
                "schema_digests_by_table",
                "source_binding",
                "source_binding_id",
                "transform_semantics_digest",
                "writer_runtime_provenance",
            },
            "S4 session run spec",
        )
        if document["rule_version"] != "s4_assets_session_run_spec_v1":
            raise S4AssetIncrementalContractError("S4 run-spec rule version changed")
        runtime = _mapping(document["writer_runtime_provenance"], "writer provenance")
        _exact_keys(runtime, {"git_commit"}, "writer provenance")
        result = cls(
            parent_frontier=S4ParentFrontierPin.from_dict(document["parent_frontier"]),
            source_binding=S4SessionSourceBinding.from_dict(document["source_binding"]),
            reference_binding=S4ReferenceBinding.from_dict(document["reference_binding"]),
            calendar_artifact_id=_text(
                document["calendar_artifact_id"],
                "calendar artifact ID",
            ),
            calendar_artifact=ArtifactPin(**_artifact_kwargs(document["calendar_artifact"])),
            contract_ids_by_table=_mapping(
                document["contract_ids_by_table"],
                "S4 contract IDs",
            ),
            schema_digests_by_table=_mapping(
                document["schema_digests_by_table"],
                "S4 schema digests",
            ),
            transform_semantics_digest=_text(
                document["transform_semantics_digest"],
                "transform semantics digest",
            ),
            parquet_writer_policy=_mapping(
                document["parquet_writer_policy"],
                "Parquet writer policy",
            ),
            receipt_available_session=_date_text(
                document["receipt_available_session"],
                "receipt available session",
            ),
            writer_git_commit=_text(runtime["git_commit"], "writer Git commit"),
        )
        if document["source_binding_id"] != result.source_binding.source_binding_id:
            raise S4AssetIncrementalContractError("run spec source binding ID changed")
        if document["parent_frontier_id"] != result.parent_frontier.parent_frontier_id:
            raise S4AssetIncrementalContractError("run spec parent frontier ID changed")
        if document["reference_binding_id"] != result.reference_binding.binding_id:
            raise S4AssetIncrementalContractError("run spec reference binding ID changed")
        if document["run_spec_id"] != result.run_spec_id:
            raise S4AssetIncrementalContractError("S4 run-spec ID does not reproduce")
        return result


@dataclass(frozen=True, slots=True)
class S4SessionPartitionReceipt:
    """One immutable data partition produced by a session run."""

    table_name: str
    session_date: date
    artifact: ArtifactPin
    row_count: int
    contract_id: str
    schema_digest: str
    source_binding_id: str
    row_funnel_digest: str
    qa_result_set_digest: str

    def __post_init__(self) -> None:
        if self.table_name not in _S4_TABLES:
            raise S4AssetIncrementalContractError("S4 partition table is invalid")
        _session(self.session_date, "partition session")
        if not isinstance(self.artifact, ArtifactPin):
            raise S4AssetIncrementalContractError("S4 partition artifact is invalid")
        _nonnegative_int(self.row_count, "partition row count")
        for value, label in (
            (self.contract_id, "partition contract ID"),
            (self.schema_digest, "partition schema digest"),
            (self.source_binding_id, "partition source binding ID"),
            (self.row_funnel_digest, "partition row-funnel digest"),
            (self.qa_result_set_digest, "partition QA result-set digest"),
        ):
            _digest(value, label)

    @property
    def partition_receipt_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "artifact": self.artifact.to_dict(),
            "contract_id": self.contract_id,
            "qa_result_set_digest": self.qa_result_set_digest,
            "row_count": self.row_count,
            "row_funnel_digest": self.row_funnel_digest,
            "rule_version": "s4_assets_session_partition_receipt_v1",
            "schema_digest": self.schema_digest,
            "session_date": self.session_date.isoformat(),
            "source_binding_id": self.source_binding_id,
            "table_name": self.table_name,
        }

    def to_dict(self) -> dict[str, object]:
        return {"partition_receipt_id": self.partition_receipt_id, **self.logical_payload()}

    @classmethod
    def from_dict(cls, value: object) -> S4SessionPartitionReceipt:
        document = _mapping(value, "S4 partition receipt")
        _exact_keys(
            document,
            {
                "artifact",
                "contract_id",
                "partition_receipt_id",
                "qa_result_set_digest",
                "row_count",
                "row_funnel_digest",
                "rule_version",
                "schema_digest",
                "session_date",
                "source_binding_id",
                "table_name",
            },
            "S4 partition receipt",
        )
        if document["rule_version"] != "s4_assets_session_partition_receipt_v1":
            raise S4AssetIncrementalContractError("S4 partition rule version changed")
        result = cls(
            table_name=_text(document["table_name"], "partition table"),
            session_date=_date_text(document["session_date"], "partition session"),
            artifact=ArtifactPin(**_artifact_kwargs(document["artifact"])),
            row_count=_int(document["row_count"], "partition row count"),
            contract_id=_text(document["contract_id"], "partition contract ID"),
            schema_digest=_text(document["schema_digest"], "partition schema digest"),
            source_binding_id=_text(
                document["source_binding_id"],
                "partition source binding ID",
            ),
            row_funnel_digest=_text(
                document["row_funnel_digest"],
                "partition row-funnel digest",
            ),
            qa_result_set_digest=_text(
                document["qa_result_set_digest"],
                "partition QA result-set digest",
            ),
        )
        if document["partition_receipt_id"] != result.partition_receipt_id:
            raise S4AssetIncrementalContractError("S4 partition receipt ID does not reproduce")
        return result


@dataclass(frozen=True, slots=True)
class S4SessionRunReceipt:
    """Final atomic completion marker for one S4 session run."""

    run_spec_id: str
    run_spec_artifact: ArtifactPin
    parent_frontier_id: str
    session_date: date
    source_binding_id: str
    pair_available_session: date
    receipt_available_session: date
    partition_receipts: tuple[S4SessionPartitionReceipt, ...]
    qa_details_artifact: ArtifactPin
    qa_result_set_digest: str

    def __post_init__(self) -> None:
        for value, label in (
            (self.run_spec_id, "receipt run-spec ID"),
            (self.parent_frontier_id, "receipt parent-frontier ID"),
            (self.source_binding_id, "receipt source-binding ID"),
            (self.qa_result_set_digest, "receipt QA result-set digest"),
        ):
            _digest(value, label)
        if not isinstance(self.run_spec_artifact, ArtifactPin):
            raise S4AssetIncrementalContractError("receipt run-spec artifact is invalid")
        session = _session(self.session_date, "receipt session")
        available = _session(self.pair_available_session, "receipt pair availability")
        if available < session:
            raise S4AssetIncrementalContractError("receipt pair availability precedes its session")
        receipt_available = _session(
            self.receipt_available_session,
            "receipt available session",
        )
        if receipt_available < available:
            raise S4AssetIncrementalContractError(
                "receipt availability precedes source-pair availability"
            )
        partitions = tuple(self.partition_receipts)
        if not all(isinstance(item, S4SessionPartitionReceipt) for item in partitions):
            raise S4AssetIncrementalContractError("S4 partition receipt set is invalid")
        if tuple(item.table_name for item in partitions) != tuple(sorted(_S4_TABLES)):
            raise S4AssetIncrementalContractError(
                "S4 run receipt requires exactly three sorted partition receipts"
            )
        if any(
            item.session_date != session or item.source_binding_id != self.source_binding_id
            for item in partitions
        ):
            raise S4AssetIncrementalContractError(
                "S4 partition receipts differ from the run receipt scope"
            )
        expected_qa_digest = stable_digest(
            {
                "table_qa_result_set_digests": {
                    item.table_name: item.qa_result_set_digest for item in partitions
                }
            }
        )
        if self.qa_result_set_digest != expected_qa_digest:
            raise S4AssetIncrementalContractError("S4 run-receipt QA digest does not reproduce")
        if not isinstance(self.qa_details_artifact, ArtifactPin):
            raise S4AssetIncrementalContractError("S4 QA details artifact is invalid")
        object.__setattr__(self, "partition_receipts", partitions)

    @property
    def receipt_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "pair_available_session": self.pair_available_session.isoformat(),
            "parent_frontier_id": self.parent_frontier_id,
            "partition_receipts": [item.to_dict() for item in self.partition_receipts],
            "qa_details_artifact": self.qa_details_artifact.to_dict(),
            "qa_result_set_digest": self.qa_result_set_digest,
            "receipt_available_session": self.receipt_available_session.isoformat(),
            "rule_version": "s4_assets_session_run_receipt_v1",
            "run_spec_id": self.run_spec_id,
            "session_date": self.session_date.isoformat(),
            "source_binding_id": self.source_binding_id,
        }

    def to_dict(self) -> dict[str, object]:
        return {
            "receipt_id": self.receipt_id,
            **self.logical_payload(),
            # The exact run-spec bytes retain audit provenance, including the
            # writer commit.  The semantic run_spec_id already binds every
            # transform input, so this operational pin must not make the
            # receipt identity drift across code-only provenance changes.
            "run_spec_artifact": self.run_spec_artifact.to_dict(),
        }

    @classmethod
    def from_dict(cls, value: object) -> S4SessionRunReceipt:
        document = _mapping(value, "S4 session run receipt")
        _exact_keys(
            document,
            {
                "pair_available_session",
                "parent_frontier_id",
                "partition_receipts",
                "qa_details_artifact",
                "qa_result_set_digest",
                "receipt_id",
                "receipt_available_session",
                "rule_version",
                "run_spec_artifact",
                "run_spec_id",
                "session_date",
                "source_binding_id",
            },
            "S4 session run receipt",
        )
        if document["rule_version"] != "s4_assets_session_run_receipt_v1":
            raise S4AssetIncrementalContractError("S4 run-receipt rule version changed")
        result = cls(
            run_spec_id=_text(document["run_spec_id"], "receipt run-spec ID"),
            run_spec_artifact=ArtifactPin(**_artifact_kwargs(document["run_spec_artifact"])),
            parent_frontier_id=_text(
                document["parent_frontier_id"],
                "receipt parent-frontier ID",
            ),
            session_date=_date_text(document["session_date"], "receipt session"),
            source_binding_id=_text(
                document["source_binding_id"],
                "receipt source-binding ID",
            ),
            pair_available_session=_date_text(
                document["pair_available_session"],
                "receipt pair availability",
            ),
            receipt_available_session=_date_text(
                document["receipt_available_session"],
                "receipt available session",
            ),
            partition_receipts=tuple(
                S4SessionPartitionReceipt.from_dict(item)
                for item in _array(document["partition_receipts"], "partition receipts")
            ),
            qa_details_artifact=ArtifactPin(**_artifact_kwargs(document["qa_details_artifact"])),
            qa_result_set_digest=_text(
                document["qa_result_set_digest"],
                "receipt QA result-set digest",
            ),
        )
        if document["receipt_id"] != result.receipt_id:
            raise S4AssetIncrementalContractError("S4 run-receipt ID does not reproduce")
        return result


def _artifact_kwargs(value: object) -> dict[str, object]:
    document = _mapping(value, "artifact pin")
    _exact_keys(document, {"bytes", "path", "sha256"}, "artifact pin")
    return {
        "bytes": _int(document["bytes"], "artifact bytes"),
        "path": _text(document["path"], "artifact path"),
        "sha256": _text(document["sha256"], "artifact SHA-256"),
    }


def _digest_map(value: object, label: str) -> Mapping[str, str]:
    document = _mapping(value, label)
    normalized: dict[str, str] = {}
    for key, item in document.items():
        if not isinstance(key, str):
            raise S4AssetIncrementalContractError(f"{label} keys must be strings")
        _digest(item, f"{label}:{key}")
        normalized[key] = item
    return MappingProxyType(dict(sorted(normalized.items())))


def _json_mapping(value: object, label: str) -> Mapping[str, object]:
    document = _mapping(value, label)
    normalized: dict[str, object] = {}
    for key, item in document.items():
        if not isinstance(key, str) or not key:
            raise S4AssetIncrementalContractError(f"{label} keys must be nonempty strings")
        if (
            item is None
            or type(item) in {bool, int, str}
            or (type(item) is float and math.isfinite(item))
        ):
            normalized[key] = item
        else:
            raise S4AssetIncrementalContractError(f"{label} values must be flat JSON scalars")
    return MappingProxyType(dict(sorted(normalized.items())))


def _sorted_unique_tokens(value: object, label: str) -> tuple[str, ...]:
    items = tuple(value) if isinstance(value, (tuple, list)) else ()
    if not items or not all(
        isinstance(item, str) and item and item == item.strip() for item in items
    ):
        raise S4AssetIncrementalContractError(f"{label} values must be trimmed text")
    if items != tuple(sorted(set(items))):
        raise S4AssetIncrementalContractError(f"{label} values must be sorted and unique")
    return items


def _mapping(value: object, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise S4AssetIncrementalContractError(f"{label} must be an object")
    return value


def _array(value: object, label: str) -> list[object]:
    if not isinstance(value, list):
        raise S4AssetIncrementalContractError(f"{label} must be an array")
    return value


def _string_tuple(value: object, label: str) -> tuple[str, ...]:
    return tuple(_text(item, label) for item in _array(value, label))


def _exact_keys(document: Mapping[str, object], expected: set[str], label: str) -> None:
    if set(document) != expected:
        raise S4AssetIncrementalContractError(f"{label} schema is not exact")


def _text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise S4AssetIncrementalContractError(f"{label} must be trimmed nonempty text")
    return value


def _digest(value: object, label: str) -> str:
    if not isinstance(value, str) or not _DIGEST.fullmatch(value):
        raise S4AssetIncrementalContractError(f"{label} must be a lowercase SHA-256")
    return value


def _session(value: object, label: str) -> date:
    if type(value) is not date:
        raise S4AssetIncrementalContractError(f"{label} must be a native date")
    return value


def _date_text(value: object, label: str) -> date:
    text = _text(value, label)
    try:
        parsed = date.fromisoformat(text)
    except ValueError as exc:
        raise S4AssetIncrementalContractError(f"{label} must be an ISO date") from exc
    if parsed.isoformat() != text:
        raise S4AssetIncrementalContractError(f"{label} must be a canonical ISO date")
    return parsed


def _aware_utc(value: object, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise S4AssetIncrementalContractError(f"{label} must be timezone-aware")
    return value.astimezone(UTC)


def _datetime_text(value: object, label: str) -> datetime:
    text = _text(value, label)
    if not text.endswith("Z"):
        raise S4AssetIncrementalContractError(f"{label} must be canonical UTC text")
    try:
        parsed = datetime.fromisoformat(f"{text[:-1]}+00:00")
    except ValueError as exc:
        raise S4AssetIncrementalContractError(f"{label} must be canonical UTC text") from exc
    if _format_utc(parsed) != text:
        raise S4AssetIncrementalContractError(f"{label} must be canonical UTC text")
    return parsed


def _format_utc(value: datetime) -> str:
    return _aware_utc(value, "UTC timestamp").isoformat().replace("+00:00", "Z")


def _int(value: object, label: str) -> int:
    if type(value) is not int:
        raise S4AssetIncrementalContractError(f"{label} must be a native int")
    return value


def _positive_int(value: object, label: str) -> int:
    parsed = _int(value, label)
    if parsed <= 0:
        raise S4AssetIncrementalContractError(f"{label} must be positive")
    return parsed


def _nonnegative_int(value: object, label: str) -> int:
    parsed = _int(value, label)
    if parsed < 0:
        raise S4AssetIncrementalContractError(f"{label} must be nonnegative")
    return parsed


def validate_relative_path(value: object, label: str) -> str:
    text = _text(value, label)
    path = PurePosixPath(text)
    if path.is_absolute() or path.as_posix() != text or ".." in path.parts:
        raise S4AssetIncrementalContractError(f"{label} must be a normalized relative path")
    return text


__all__ = [
    "S4AssetIncrementalContractError",
    "S4BaseFrontier",
    "S4ParentFrontierPin",
    "S4ParentKind",
    "S4ReferenceBinding",
    "S4SessionPartitionReceipt",
    "S4SessionRunReceipt",
    "S4SessionRunSpec",
    "S4SessionSourceBinding",
]
