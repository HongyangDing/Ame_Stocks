"""Exact, no-publish checkpoint compaction for S7.5 I7.

The producer reads one fully authenticated I6 research-top snapshot, rewrites
the three versioned tables to independent compacted Parquet segments, freezes a
new universe dataset index over the already authenticated immutable members,
and creates a new native-v2 checkpoint and manifest.  It never changes a
research pointer and cannot publish.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import math
import os
import shutil
import tempfile
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import date
from pathlib import Path
from typing import Final, Protocol, Self

import pyarrow as pa
import pyarrow.parquet as pq

from ame_stocks_api.artifacts import safe_relative_path, stable_digest
from ame_stocks_api.silver.incremental_contract import ArtifactPin
from ame_stocks_api.silver.incremental_i3_checkpoint import (
    I3_V2_CONTRACTS,
    I3_V2_SCHEMA_BUNDLE_DIGEST,
    I3_V2_TABLE_ORDER,
    NATIVE_V2_RELEASE_FAMILY,
    I3CheckpointState,
    NativeV2OutputArtifact,
    NativeV2ParentReleasePin,
    NativeV2ReleaseManifest,
    TerminalRowVersionState,
    i3_resolved_state_digest,
)
from ame_stocks_api.silver.incremental_i3_production_contract import (
    I3ProductionDatasetIndex,
    I3ProductionOutputStorage,
    I3ProductionRowsetIndex,
    I3ProductionRunKind,
    I3ProductionSegmentPin,
    I3ProductionTableOutput,
)

I7_CHECKPOINT_COMPACTION_RULE_VERSION: Final = "s7_5_i7_checkpoint_compaction_v1"
I7_CHECKPOINT_COMPACTION_STATE: Final = "awaiting_review"
I7_CHECKPOINT_COMPACTION_AUTHORITY: Final = "production_exact_i6_research_top"
I7_CHECKPOINT_COMPACTION_FIXTURE_AUTHORITY: Final = "local_fixture_non_authoritative"

_CONTROL_ROOT: Final = "manifests/silver/identity/s7-5-checkpoint-compactions"
_OUTPUT_ROOT: Final = "silver/schema=v2/identity/checkpoint_compactions"
_SMALL_TABLES: Final = ("asset_master", "ticker_alias", "issuer_master")
_VERSION_FIELD: Final = {
    "asset_master": "asset_master_version_id",
    "ticker_alias": "alias_resolution_version_id",
    "issuer_master": "issuer_master_version_id",
}


class I7CheckpointCompactionError(RuntimeError):
    """Raised when the independent checkpoint producer cannot prove its output."""

    def __init__(self, message: str, *, failed_receipt: ArtifactPin | None = None) -> None:
        super().__init__(message)
        self.failed_receipt = failed_receipt


@dataclass(frozen=True, slots=True)
class I7CheckpointCompactionResourceCaps:
    peak_rss_bytes: int
    disk_free_bytes_floor: int
    input_bytes_cap: int
    output_bytes_cap: int
    row_count_cap: int

    def __post_init__(self) -> None:
        for value, label in (
            (self.peak_rss_bytes, "peak RSS cap"),
            (self.disk_free_bytes_floor, "disk floor"),
            (self.input_bytes_cap, "input bytes cap"),
            (self.output_bytes_cap, "output bytes cap"),
            (self.row_count_cap, "row-count cap"),
        ):
            _positive_int(value, label)

    def to_dict(self) -> dict[str, int]:
        return {
            "disk_free_bytes_floor": self.disk_free_bytes_floor,
            "input_bytes_cap": self.input_bytes_cap,
            "output_bytes_cap": self.output_bytes_cap,
            "peak_rss_bytes": self.peak_rss_bytes,
            "row_count_cap": self.row_count_cap,
        }

    @classmethod
    def from_dict(cls, value: object) -> Self:
        item = _closed_mapping(
            value,
            {
                "disk_free_bytes_floor",
                "input_bytes_cap",
                "output_bytes_cap",
                "peak_rss_bytes",
                "row_count_cap",
            },
            "compaction resource caps",
        )
        return cls(**{key: _integer(item[key], key) for key in item})


@dataclass(frozen=True, slots=True)
class I7CheckpointCompactionSource:
    """Closed projection of the exact I6 top needed by the compactor."""

    snapshot_id: str
    release_id: str
    native_v2_release_id: str
    checkpoint_id: str
    terminal_session: date
    producer_available_session: date
    source_binding_digest: str
    schema_bundle_digest: str
    transform_semantics_digest: str
    identity_policy_bundle_id: str
    calendar_digest: str
    top_pointer_artifact: ArtifactPin
    gate_c_approval_artifact: ArtifactPin
    release_completion_artifact: ArtifactPin
    deep_attestation_artifact: ArtifactPin
    checkpoint_artifact: ArtifactPin
    table_outputs: tuple[I3ProductionTableOutput, ...]
    authority_artifacts: tuple[ArtifactPin, ...]

    def __post_init__(self) -> None:
        for value, label in (
            (self.snapshot_id, "source snapshot ID"),
            (self.release_id, "source Gate-A release ID"),
            (self.native_v2_release_id, "source native-v2 release ID"),
            (self.checkpoint_id, "source checkpoint ID"),
            (self.source_binding_digest, "source binding digest"),
            (self.schema_bundle_digest, "source schema digest"),
            (self.transform_semantics_digest, "source transform digest"),
            (self.identity_policy_bundle_id, "source policy ID"),
            (self.calendar_digest, "source calendar digest"),
        ):
            _digest(value, label)
        if self.schema_bundle_digest != I3_V2_SCHEMA_BUNDLE_DIGEST:
            raise I7CheckpointCompactionError("source is not the frozen native-v2 schema")
        _session(self.terminal_session, "source terminal session")
        _session(self.producer_available_session, "source producer availability")
        if self.producer_available_session < self.terminal_session:
            raise I7CheckpointCompactionError("source availability predates terminal session")
        for pin, label in (
            (self.top_pointer_artifact, "source top pointer"),
            (self.gate_c_approval_artifact, "source Gate C"),
            (self.release_completion_artifact, "source completion"),
            (self.deep_attestation_artifact, "source deep attestation"),
            (self.checkpoint_artifact, "source checkpoint"),
        ):
            _artifact(pin, label)
        if (
            type(self.table_outputs) is not tuple
            or tuple(item.table_name for item in self.table_outputs) != I3_V2_TABLE_ORDER
            or not all(type(item) is I3ProductionTableOutput for item in self.table_outputs)
        ):
            raise I7CheckpointCompactionError("source lacks the exact four-table output set")
        if not isinstance(self.authority_artifacts, tuple) or not self.authority_artifacts:
            raise I7CheckpointCompactionError("source authority artifact set is empty")
        _unique_pins(self.authority_artifacts, "source authority artifacts")

    @property
    def source_identity_digest(self) -> str:
        return stable_digest(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            "authority_artifacts": [item.to_dict() for item in self.authority_artifacts],
            "calendar_digest": self.calendar_digest,
            "checkpoint_artifact": self.checkpoint_artifact.to_dict(),
            "checkpoint_id": self.checkpoint_id,
            "deep_attestation_artifact": self.deep_attestation_artifact.to_dict(),
            "gate_c_approval_artifact": self.gate_c_approval_artifact.to_dict(),
            "identity_policy_bundle_id": self.identity_policy_bundle_id,
            "native_v2_release_id": self.native_v2_release_id,
            "producer_available_session": self.producer_available_session.isoformat(),
            "release_completion_artifact": self.release_completion_artifact.to_dict(),
            "release_id": self.release_id,
            "schema_bundle_digest": self.schema_bundle_digest,
            "snapshot_id": self.snapshot_id,
            "source_binding_digest": self.source_binding_digest,
            "table_outputs": [item.to_dict() for item in self.table_outputs],
            "terminal_session": self.terminal_session.isoformat(),
            "top_pointer_artifact": self.top_pointer_artifact.to_dict(),
            "transform_semantics_digest": self.transform_semantics_digest,
        }

    @classmethod
    def from_dict(cls, value: object) -> Self:
        fields = {
            "authority_artifacts",
            "calendar_digest",
            "checkpoint_artifact",
            "checkpoint_id",
            "deep_attestation_artifact",
            "gate_c_approval_artifact",
            "identity_policy_bundle_id",
            "native_v2_release_id",
            "producer_available_session",
            "release_completion_artifact",
            "release_id",
            "schema_bundle_digest",
            "snapshot_id",
            "source_binding_digest",
            "table_outputs",
            "terminal_session",
            "top_pointer_artifact",
            "transform_semantics_digest",
        }
        item = _closed_mapping(value, fields, "compaction source")
        return cls(
            snapshot_id=_text(item["snapshot_id"], "source snapshot ID"),
            release_id=_text(item["release_id"], "source release ID"),
            native_v2_release_id=_text(item["native_v2_release_id"], "source native release"),
            checkpoint_id=_text(item["checkpoint_id"], "source checkpoint ID"),
            terminal_session=_date_value(item["terminal_session"], "source terminal"),
            producer_available_session=_date_value(
                item["producer_available_session"], "source availability"
            ),
            source_binding_digest=_text(item["source_binding_digest"], "source binding"),
            schema_bundle_digest=_text(item["schema_bundle_digest"], "source schema"),
            transform_semantics_digest=_text(
                item["transform_semantics_digest"], "source transform"
            ),
            identity_policy_bundle_id=_text(item["identity_policy_bundle_id"], "source policy"),
            calendar_digest=_text(item["calendar_digest"], "source calendar"),
            top_pointer_artifact=_artifact_from_dict(item["top_pointer_artifact"]),
            gate_c_approval_artifact=_artifact_from_dict(item["gate_c_approval_artifact"]),
            release_completion_artifact=_artifact_from_dict(item["release_completion_artifact"]),
            deep_attestation_artifact=_artifact_from_dict(item["deep_attestation_artifact"]),
            checkpoint_artifact=_artifact_from_dict(item["checkpoint_artifact"]),
            table_outputs=tuple(
                I3ProductionTableOutput.from_dict(entry)
                for entry in _array(item["table_outputs"], "source table outputs")
            ),
            authority_artifacts=tuple(
                _artifact_from_dict(entry)
                for entry in _array(item["authority_artifacts"], "source authorities")
            ),
        )


@dataclass(frozen=True, slots=True)
class I7CheckpointCompactionRunSpec:
    authority: str
    source: I7CheckpointCompactionSource
    completion_available_session: date
    resource_caps: I7CheckpointCompactionResourceCaps

    def __post_init__(self) -> None:
        if self.authority not in {
            I7_CHECKPOINT_COMPACTION_AUTHORITY,
            I7_CHECKPOINT_COMPACTION_FIXTURE_AUTHORITY,
        }:
            raise I7CheckpointCompactionError("compaction authority is invalid")
        if not isinstance(self.source, I7CheckpointCompactionSource):
            raise I7CheckpointCompactionError("compaction source is invalid")
        _session(self.completion_available_session, "compaction availability")
        if self.completion_available_session < self.source.producer_available_session:
            raise I7CheckpointCompactionError("compaction predates its I6 source")
        if not isinstance(self.resource_caps, I7CheckpointCompactionResourceCaps):
            raise I7CheckpointCompactionError("compaction resource caps are invalid")

    @property
    def run_spec_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "artifact_type": "s7_5_i7_checkpoint_compaction_run_spec",
            "authority": self.authority,
            "completion_available_session": self.completion_available_session.isoformat(),
            "resource_caps": self.resource_caps.to_dict(),
            "rule_version": I7_CHECKPOINT_COMPACTION_RULE_VERSION,
            "source": self.source.to_dict(),
        }

    def to_dict(self) -> dict[str, object]:
        return {"run_spec_id": self.run_spec_id, **self.logical_payload()}

    def canonical_bytes(self) -> bytes:
        return _canonical(self.to_dict())

    @classmethod
    def from_dict(cls, value: object) -> Self:
        item = _closed_mapping(
            value,
            {
                "artifact_type",
                "authority",
                "completion_available_session",
                "resource_caps",
                "rule_version",
                "run_spec_id",
                "source",
            },
            "compaction RunSpec",
        )
        if (
            item["artifact_type"] != "s7_5_i7_checkpoint_compaction_run_spec"
            or item["rule_version"] != I7_CHECKPOINT_COMPACTION_RULE_VERSION
        ):
            raise I7CheckpointCompactionError("compaction RunSpec type or rule differs")
        result = cls(
            authority=_text(item["authority"], "compaction authority"),
            source=I7CheckpointCompactionSource.from_dict(item["source"]),
            completion_available_session=_date_value(
                item["completion_available_session"], "compaction availability"
            ),
            resource_caps=I7CheckpointCompactionResourceCaps.from_dict(item["resource_caps"]),
        )
        if item["run_spec_id"] != result.run_spec_id:
            raise I7CheckpointCompactionError("compaction RunSpec ID does not reproduce")
        return result


@dataclass(frozen=True, slots=True)
class I7CheckpointCompactionCompletion:
    run_spec_id: str
    run_spec_artifact: ArtifactPin
    source_snapshot_id: str
    compacted_release_id: str
    compacted_manifest_artifact: ArtifactPin
    compacted_checkpoint_id: str
    compacted_checkpoint_artifact: ArtifactPin
    proof_artifact: ArtifactPin
    output_artifacts: tuple[I3ProductionTableOutput, ...]
    input_bytes: int
    output_bytes: int
    peak_rss_bytes: int
    minimum_disk_free_bytes: int
    elapsed_seconds: int
    completion_available_session: date
    state: str = I7_CHECKPOINT_COMPACTION_STATE
    publish_authorized: bool = False

    def __post_init__(self) -> None:
        for value, label in (
            (self.run_spec_id, "completion RunSpec ID"),
            (self.source_snapshot_id, "completion source snapshot ID"),
            (self.compacted_release_id, "compacted release ID"),
            (self.compacted_checkpoint_id, "compacted checkpoint ID"),
        ):
            _digest(value, label)
        for pin, label in (
            (self.run_spec_artifact, "completion RunSpec"),
            (self.compacted_manifest_artifact, "compacted manifest"),
            (self.compacted_checkpoint_artifact, "compacted checkpoint"),
            (self.proof_artifact, "compaction proof"),
        ):
            _artifact(pin, label)
        if (
            type(self.output_artifacts) is not tuple
            or tuple(item.table_name for item in self.output_artifacts) != I3_V2_TABLE_ORDER
        ):
            raise I7CheckpointCompactionError("completion output set differs")
        for value, label in (
            (self.input_bytes, "completion input bytes"),
            (self.output_bytes, "completion output bytes"),
            (self.peak_rss_bytes, "completion peak RSS"),
            (self.minimum_disk_free_bytes, "completion disk floor"),
            (self.elapsed_seconds, "completion elapsed seconds"),
        ):
            _nonnegative_int(value, label)
        _session(self.completion_available_session, "completion availability")
        if self.state != I7_CHECKPOINT_COMPACTION_STATE or self.publish_authorized is not False:
            raise I7CheckpointCompactionError("compaction completion cannot publish")

    @property
    def completion_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "artifact_type": "s7_5_i7_checkpoint_compaction_completion",
            "compacted_checkpoint_artifact": self.compacted_checkpoint_artifact.to_dict(),
            "compacted_checkpoint_id": self.compacted_checkpoint_id,
            "compacted_manifest_artifact": self.compacted_manifest_artifact.to_dict(),
            "compacted_release_id": self.compacted_release_id,
            "completion_available_session": self.completion_available_session.isoformat(),
            "elapsed_seconds": self.elapsed_seconds,
            "input_bytes": self.input_bytes,
            "minimum_disk_free_bytes": self.minimum_disk_free_bytes,
            "output_artifacts": [item.to_dict() for item in self.output_artifacts],
            "output_bytes": self.output_bytes,
            "peak_rss_bytes": self.peak_rss_bytes,
            "proof_artifact": self.proof_artifact.to_dict(),
            "publish_authorized": False,
            "rule_version": I7_CHECKPOINT_COMPACTION_RULE_VERSION,
            "run_spec_artifact": self.run_spec_artifact.to_dict(),
            "run_spec_id": self.run_spec_id,
            "source_snapshot_id": self.source_snapshot_id,
            "state": self.state,
        }

    def to_dict(self) -> dict[str, object]:
        return {"completion_id": self.completion_id, **self.logical_payload()}

    def canonical_bytes(self) -> bytes:
        return _canonical(self.to_dict())


@dataclass(frozen=True, slots=True)
class LoadedI7CheckpointCompaction:
    run_spec: I7CheckpointCompactionRunSpec
    run_spec_artifact: ArtifactPin
    source_checkpoint: I3CheckpointState
    compacted_manifest: NativeV2ReleaseManifest
    compacted_checkpoint: I3CheckpointState
    output_artifacts: tuple[I3ProductionTableOutput, ...]
    proof_artifact: ArtifactPin
    completion: I7CheckpointCompactionCompletion
    completion_artifact: ArtifactPin
    reused: bool


class _SourceLoader(Protocol):
    def __call__(self, data_root: Path) -> I7CheckpointCompactionSource: ...


def prepare_i7_checkpoint_compaction(
    data_root: Path,
    *,
    completion_available_session: date,
    resource_caps: I7CheckpointCompactionResourceCaps,
) -> tuple[I7CheckpointCompactionRunSpec, ArtifactPin]:
    """Freeze the production RunSpec without writing output data."""

    return _prepare_i7_checkpoint_compaction(
        data_root,
        completion_available_session=completion_available_session,
        resource_caps=resource_caps,
        authority=I7_CHECKPOINT_COMPACTION_AUTHORITY,
        source_loader=_load_production_source,
    )


def stage_i7_checkpoint_compaction(
    data_root: Path,
    run_spec_artifact: ArtifactPin,
) -> LoadedI7CheckpointCompaction:
    """Materialize one independent checkpoint candidate and stop awaiting review."""

    return _stage_i7_checkpoint_compaction(
        data_root,
        run_spec_artifact,
        authority=I7_CHECKPOINT_COMPACTION_AUTHORITY,
        source_loader=_load_production_source,
    )


def verify_i7_checkpoint_compaction(
    data_root: Path,
    completion_artifact: ArtifactPin,
) -> LoadedI7CheckpointCompaction:
    """Deep replay a production compaction completion from exact bytes."""

    return _verify_i7_checkpoint_compaction(
        data_root,
        completion_artifact,
        authority=I7_CHECKPOINT_COMPACTION_AUTHORITY,
        source_loader=_load_production_source,
    )


def _prepare_i7_checkpoint_compaction(
    data_root: Path,
    *,
    completion_available_session: date,
    resource_caps: I7CheckpointCompactionResourceCaps,
    authority: str,
    source_loader: _SourceLoader,
) -> tuple[I7CheckpointCompactionRunSpec, ArtifactPin]:
    root = _root(data_root)
    source = source_loader(root)
    run_spec = I7CheckpointCompactionRunSpec(
        authority=authority,
        source=source,
        completion_available_session=completion_available_session,
        resource_caps=resource_caps,
    )
    relative = _run_spec_path(run_spec.run_spec_id, authority=authority)
    pin = _write_immutable(root, relative, run_spec.canonical_bytes(), "compaction RunSpec")
    return run_spec, pin


def _stage_i7_checkpoint_compaction(
    data_root: Path,
    run_spec_artifact: ArtifactPin,
    *,
    authority: str,
    source_loader: _SourceLoader,
) -> LoadedI7CheckpointCompaction:
    root = _root(data_root)
    _validate_control_locator(run_spec_artifact, "run-spec", authority=authority)
    run_spec = _load_run_spec(root, run_spec_artifact, authority=authority)
    if run_spec.authority != authority:
        raise I7CheckpointCompactionError("compaction authority differs")
    completion_relative = _completion_path(run_spec.run_spec_id, authority=authority)
    completion_path = safe_relative_path(root, completion_relative)
    lock_path = safe_relative_path(root, _lock_path(run_spec.run_spec_id, authority=authority))
    with _exclusive_lock(lock_path):
        if completion_path.exists() or completion_path.is_symlink():
            return _verify_i7_checkpoint_compaction(
                root,
                _pin_existing(root, completion_relative),
                authority=authority,
                source_loader=source_loader,
            )
        started = time.monotonic()
        minimum_disk = shutil.disk_usage(root).free
        try:
            source = source_loader(root)
            if source != run_spec.source:
                raise I7CheckpointCompactionError("I6 source changed after prepare")
            _preflight_resources(root, run_spec)
            source_checkpoint, source_outputs = _load_source_state(root, run_spec, source)
            input_bytes = _source_input_bytes(source, source_outputs)
            if input_bytes > run_spec.resource_caps.input_bytes_cap:
                raise I7CheckpointCompactionError("compaction input-byte cap exceeded")
            rows_by_table = {
                table: _resolved_terminal_rows(
                    root,
                    checkpoint=source_checkpoint,
                    output=_output_by_name(source_outputs)[table],
                    table_name=table,
                )
                for table in _SMALL_TABLES
            }
            if sum(len(rows) for rows in rows_by_table.values()) > (
                run_spec.resource_caps.row_count_cap
            ):
                raise I7CheckpointCompactionError("compaction row-count cap exceeded")
            outputs, terminal_rows, output_bytes = _materialize_outputs(
                root,
                run_spec=run_spec,
                source_outputs=source_outputs,
                source_checkpoint=source_checkpoint,
                rows_by_table=rows_by_table,
            )
            compacted_manifest, compacted_manifest_pin, compacted_checkpoint = (
                _materialize_checkpoint(
                    root,
                    run_spec=run_spec,
                    source_checkpoint=source_checkpoint,
                    outputs=outputs,
                    terminal_rows=terminal_rows,
                )
            )
            compacted_checkpoint_pin = _write_immutable(
                root,
                _checkpoint_path(run_spec.run_spec_id, authority=authority),
                compacted_checkpoint.canonical_bytes(),
                "compacted checkpoint",
            )
            output_bytes += compacted_manifest_pin.bytes + compacted_checkpoint_pin.bytes
            proof = _proof_document(
                run_spec=run_spec,
                source_checkpoint=source_checkpoint,
                compacted_manifest=compacted_manifest,
                compacted_checkpoint=compacted_checkpoint,
                outputs=outputs,
                rows_by_table=rows_by_table,
            )
            proof_pin = _write_immutable(
                root,
                _proof_path(run_spec.run_spec_id, authority=authority),
                _canonical(proof),
                "compaction proof",
            )
            output_bytes += proof_pin.bytes
            minimum_disk = min(minimum_disk, shutil.disk_usage(root).free)
            elapsed = max(0, math.ceil(time.monotonic() - started))
            peak_rss = _peak_rss_bytes()
            if output_bytes > run_spec.resource_caps.output_bytes_cap:
                raise I7CheckpointCompactionError("compaction output-byte cap exceeded")
            _enforce_live_resources(
                root,
                run_spec.resource_caps,
                peak_rss=peak_rss,
                minimum_disk=minimum_disk,
            )
            completion = I7CheckpointCompactionCompletion(
                run_spec_id=run_spec.run_spec_id,
                run_spec_artifact=run_spec_artifact,
                source_snapshot_id=source.snapshot_id,
                compacted_release_id=compacted_manifest.release_id,
                compacted_manifest_artifact=compacted_manifest_pin,
                compacted_checkpoint_id=compacted_checkpoint.checkpoint_id,
                compacted_checkpoint_artifact=compacted_checkpoint_pin,
                proof_artifact=proof_pin,
                output_artifacts=outputs,
                input_bytes=input_bytes,
                output_bytes=output_bytes,
                peak_rss_bytes=peak_rss,
                minimum_disk_free_bytes=minimum_disk,
                elapsed_seconds=elapsed,
                completion_available_session=run_spec.completion_available_session,
            )
            completion_pin = _atomic_publish_completion(
                root,
                completion_relative,
                completion.canonical_bytes(),
                caps=run_spec.resource_caps,
                minimum_disk=minimum_disk,
            )
            return LoadedI7CheckpointCompaction(
                run_spec=run_spec,
                run_spec_artifact=run_spec_artifact,
                source_checkpoint=source_checkpoint,
                compacted_manifest=compacted_manifest,
                compacted_checkpoint=compacted_checkpoint,
                output_artifacts=outputs,
                proof_artifact=proof_pin,
                completion=completion,
                completion_artifact=completion_pin,
                reused=False,
            )
        except Exception as exc:
            failed = _write_failed(root, run_spec, exc)
            if isinstance(exc, I7CheckpointCompactionError):
                raise I7CheckpointCompactionError(str(exc), failed_receipt=failed) from exc
            raise I7CheckpointCompactionError(
                f"checkpoint compaction failed: {type(exc).__name__}: {exc}",
                failed_receipt=failed,
            ) from exc


def _verify_i7_checkpoint_compaction(
    data_root: Path,
    completion_artifact: ArtifactPin,
    *,
    authority: str,
    source_loader: _SourceLoader,
) -> LoadedI7CheckpointCompaction:
    root = _root(data_root)
    run_spec_id = _validate_control_locator(completion_artifact, "completion", authority=authority)
    completion = _completion_from_dict(
        _read_canonical(root, completion_artifact, "compaction completion")
    )
    if completion.run_spec_id != run_spec_id:
        raise I7CheckpointCompactionError("completion directory ID differs")
    run_spec = _load_run_spec(root, completion.run_spec_artifact, authority=authority)
    if run_spec.run_spec_id != run_spec_id or run_spec.authority != authority:
        raise I7CheckpointCompactionError("completion RunSpec differs")
    source = source_loader(root)
    if source != run_spec.source or completion.source_snapshot_id != source.snapshot_id:
        raise I7CheckpointCompactionError("completion source authority differs")
    source_checkpoint, source_outputs = _load_source_state(root, run_spec, source)
    manifest = NativeV2ReleaseManifest.from_dict(
        _read_canonical(root, completion.compacted_manifest_artifact, "compacted manifest")
    )
    checkpoint = I3CheckpointState.from_dict(
        _read_canonical(root, completion.compacted_checkpoint_artifact, "compacted checkpoint")
    )
    if (
        manifest.release_id != completion.compacted_release_id
        or checkpoint.checkpoint_id != completion.compacted_checkpoint_id
        or checkpoint.parent_release.release_id != manifest.release_id
        or checkpoint.parent_release.manifest != completion.compacted_manifest_artifact
        or tuple(manifest.output_artifacts)
        != tuple(item.manifest_output for item in completion.output_artifacts)
    ):
        raise I7CheckpointCompactionError("compacted manifest/checkpoint chain differs")
    rows_by_table: dict[str, tuple[dict[str, object], ...]] = {}
    for table in _SMALL_TABLES:
        source_rows = _resolved_terminal_rows(
            root,
            checkpoint=source_checkpoint,
            output=_output_by_name(source_outputs)[table],
            table_name=table,
        )
        compacted_rows = _resolved_terminal_rows(
            root,
            checkpoint=checkpoint,
            output=_output_by_name(completion.output_artifacts)[table],
            table_name=table,
        )
        if _rows_digest(source_rows) != _rows_digest(compacted_rows):
            raise I7CheckpointCompactionError(f"compacted {table} logical rows differ")
        rows_by_table[table] = source_rows
    source_universe = _output_by_name(source_outputs)["universe_daily"]
    compacted_universe = _output_by_name(completion.output_artifacts)["universe_daily"]
    if (
        source_universe.dataset_index is None
        or compacted_universe.dataset_index is None
        or source_universe.dataset_index.partitions != compacted_universe.dataset_index.partitions
    ):
        raise I7CheckpointCompactionError("compacted universe members differ")
    _validate_module_owned_materialization(
        run_spec=run_spec,
        source_checkpoint=source_checkpoint,
        source_outputs=source_outputs,
        rows_by_table=rows_by_table,
        outputs=completion.output_artifacts,
        manifest=manifest,
        checkpoint=checkpoint,
    )
    for output in completion.output_artifacts:
        _verify_output_exact(root, output)
    proof = _read_canonical(root, completion.proof_artifact, "compaction proof")
    expected_proof = _proof_document(
        run_spec=run_spec,
        source_checkpoint=source_checkpoint,
        compacted_manifest=manifest,
        compacted_checkpoint=checkpoint,
        outputs=completion.output_artifacts,
        rows_by_table=rows_by_table,
    )
    if proof != expected_proof:
        raise I7CheckpointCompactionError("compaction proof does not reproduce")
    output_pins = [
        completion.compacted_manifest_artifact,
        completion.compacted_checkpoint_artifact,
        completion.proof_artifact,
    ]
    for output in completion.output_artifacts:
        output_pins.append(output.manifest_output.artifact)
        if output.rowset_index is not None:
            output_pins.extend(item.artifact for item in output.rowset_index.segments)
    expected_output_bytes = sum(pin.bytes for pin in _unique_pins(tuple(output_pins), "outputs"))
    if (
        completion.input_bytes != _source_input_bytes(source, source_outputs)
        or completion.output_bytes != expected_output_bytes
        or completion.completion_available_session != run_spec.completion_available_session
    ):
        raise I7CheckpointCompactionError("compaction completion resource lineage differs")
    expected_completion = replace(completion)
    if completion_artifact != _pin_bytes(
        completion_artifact.path,
        expected_completion.canonical_bytes(),
    ):
        raise I7CheckpointCompactionError("compaction completion exact pin differs")
    _enforce_live_resources(
        root,
        run_spec.resource_caps,
        peak_rss=completion.peak_rss_bytes,
        minimum_disk=completion.minimum_disk_free_bytes,
    )
    return LoadedI7CheckpointCompaction(
        run_spec=run_spec,
        run_spec_artifact=completion.run_spec_artifact,
        source_checkpoint=source_checkpoint,
        compacted_manifest=manifest,
        compacted_checkpoint=checkpoint,
        output_artifacts=completion.output_artifacts,
        proof_artifact=completion.proof_artifact,
        completion=completion,
        completion_artifact=completion_artifact,
        reused=True,
    )


def _load_production_source(data_root: Path) -> I7CheckpointCompactionSource:
    try:
        from ame_stocks_api.silver import incremental_i6_pointer_runtime as i6

        snapshot = i6.load_research_top_snapshot_exact(data_root)
    except Exception as exc:
        raise I7CheckpointCompactionError("exact I6 research-top snapshot is unavailable") from exc
    if type(snapshot) is not i6.ResearchTopSnapshot:
        raise I7CheckpointCompactionError("I6 loader returned another snapshot type")
    authority_artifacts = (
        snapshot.research_top_event_artifact,
        snapshot.gate_c_approval_artifact,
        snapshot.gate_b_approval_artifact,
        snapshot.shadow_stage_receipt_artifact,
        snapshot.shadow_pointer_event_artifact,
        snapshot.rollback_stage_receipt_artifact,
        snapshot.rollback_pointer_event_artifact,
        snapshot.rollback_receipt_artifact,
        snapshot.research_top_stage_receipt_artifact,
        snapshot.release_completion_artifact,
        snapshot.deep_attestation_artifact,
        snapshot.checkpoint_artifact,
        *(item.manifest_output.artifact for item in snapshot.table_outputs),
    )
    return I7CheckpointCompactionSource(
        snapshot_id=snapshot.snapshot_id,
        release_id=snapshot.release_id,
        native_v2_release_id=snapshot.native_v2_release_id,
        checkpoint_id=snapshot.checkpoint_id,
        terminal_session=snapshot.terminal_session,
        producer_available_session=snapshot.producer_available_session,
        source_binding_digest=snapshot.source_binding_digest,
        schema_bundle_digest=snapshot.schema_bundle_digest,
        transform_semantics_digest=snapshot.transform_semantics_digest,
        identity_policy_bundle_id=snapshot.identity_policy_bundle_id,
        calendar_digest=snapshot.calendar_digest,
        top_pointer_artifact=snapshot.research_top_event_artifact,
        gate_c_approval_artifact=snapshot.gate_c_approval_artifact,
        release_completion_artifact=snapshot.release_completion_artifact,
        deep_attestation_artifact=snapshot.deep_attestation_artifact,
        checkpoint_artifact=snapshot.checkpoint_artifact,
        table_outputs=snapshot.table_outputs,
        authority_artifacts=_unique_pins(authority_artifacts, "I6 source authority"),
    )


def _load_source_state(
    root: Path,
    run_spec: I7CheckpointCompactionRunSpec,
    source: I7CheckpointCompactionSource,
) -> tuple[I3CheckpointState, tuple[I3ProductionTableOutput, ...]]:
    if run_spec.authority == I7_CHECKPOINT_COMPACTION_AUTHORITY:
        try:
            from ame_stocks_api.silver.incremental_i3_production import (
                verify_i3_production_deep_attestation,
            )

            loaded = verify_i3_production_deep_attestation(
                root,
                source.release_completion_artifact,
                source.deep_attestation_artifact,
                expected_kind=I3ProductionRunKind.DELTA,
            )
        except Exception as exc:
            raise I7CheckpointCompactionError("source I3 deep replay failed") from exc
        if (
            loaded.gate_a_manifest.release_id != source.release_id
            or loaded.manifest.release_id != source.native_v2_release_id
            or loaded.checkpoint.checkpoint_id != source.checkpoint_id
            or loaded.receipt.output_set is None
            or loaded.receipt.output_set.table_outputs != source.table_outputs
        ):
            raise I7CheckpointCompactionError("source I3 authority differs from I6")
        checkpoint = loaded.checkpoint
        outputs = loaded.receipt.output_set.table_outputs
    else:
        checkpoint = I3CheckpointState.from_dict(
            _read_canonical(root, source.checkpoint_artifact, "fixture source checkpoint")
        )
        outputs = source.table_outputs
    if (
        checkpoint.checkpoint_id != source.checkpoint_id
        or checkpoint.last_session != source.terminal_session
        or checkpoint.parent_release.release_id != source.native_v2_release_id
        or checkpoint.identity_policy_bundle.identity_policy_bundle_id
        != source.identity_policy_bundle_id
        or checkpoint.schema_digest != source.schema_bundle_digest
        or checkpoint.transform_semantics_digest != source.transform_semantics_digest
        or checkpoint.calendar_digest != source.calendar_digest
    ):
        raise I7CheckpointCompactionError("source checkpoint semantics differ")
    for output in outputs:
        _verify_output_exact(root, output, verify_members=False)
    return checkpoint, outputs


def _materialize_outputs(
    root: Path,
    *,
    run_spec: I7CheckpointCompactionRunSpec,
    source_outputs: tuple[I3ProductionTableOutput, ...],
    source_checkpoint: I3CheckpointState,
    rows_by_table: Mapping[str, tuple[dict[str, object], ...]],
) -> tuple[
    tuple[I3ProductionTableOutput, ...],
    tuple[TerminalRowVersionState, ...],
    int,
]:
    output_by_name = _output_by_name(source_outputs)
    outputs: list[I3ProductionTableOutput] = []
    new_segments: dict[str, ArtifactPin] = {}
    written = 0
    for table_name in _SMALL_TABLES:
        rows = rows_by_table[table_name]
        content = _parquet_bytes(table_name, rows)
        logical_digest = _rows_digest(rows)
        segment_id = stable_digest(
            {
                "logical_row_digest": logical_digest,
                "rule_version": I7_CHECKPOINT_COMPACTION_RULE_VERSION,
                "source_checkpoint_id": source_checkpoint.checkpoint_id,
                "source_snapshot_id": run_spec.source.snapshot_id,
                "table_name": table_name,
            }
        )
        segment_relative = (
            f"{_output_run_root(run_spec.run_spec_id, run_spec.authority)}/"
            f"table={table_name}/segment_id={segment_id}/part-00000.parquet"
        )
        segment_artifact = _write_immutable(
            root,
            segment_relative,
            content,
            f"compacted {table_name} Parquet",
        )
        segment = I3ProductionSegmentPin(
            table_name=table_name,
            segment_id=segment_id,
            artifact=segment_artifact,
            row_count=len(rows),
            contract_id=I3_V2_CONTRACTS[table_name].contract_id,
            schema_digest=I3_V2_CONTRACTS[table_name].schema_digest,
            availability_session=run_spec.completion_available_session,
        )
        rowset = I3ProductionRowsetIndex(
            table_name=table_name,
            terminal_session=run_spec.source.terminal_session,
            segments=(segment,),
        )
        index_relative = (
            f"{_output_run_root(run_spec.run_spec_id, run_spec.authority)}/"
            f"table={table_name}/rowset-index.json"
        )
        index_artifact = _write_immutable(
            root,
            index_relative,
            rowset.canonical_bytes(),
            f"compacted {table_name} rowset index",
        )
        if index_artifact != rowset.exact_pin(path=index_relative):
            raise I7CheckpointCompactionError("compacted rowset index pin changed")
        output = I3ProductionTableOutput(
            storage=I3ProductionOutputStorage.ROWSET_INDEX,
            manifest_output=NativeV2OutputArtifact(
                table_name=table_name,
                session_date=run_spec.source.terminal_session,
                row_count=len(rows),
                contract_id=I3_V2_CONTRACTS[table_name].contract_id,
                schema_digest=I3_V2_CONTRACTS[table_name].schema_digest,
                artifact=index_artifact,
            ),
            rowset_index=rowset,
        )
        outputs.append(output)
        new_segments[table_name] = segment_artifact
        written += segment_artifact.bytes + index_artifact.bytes
    source_universe = output_by_name["universe_daily"]
    if source_universe.dataset_index is None:
        raise I7CheckpointCompactionError("source universe dataset index is absent")
    universe = I3ProductionDatasetIndex(
        table_name="universe_daily",
        terminal_session=source_universe.dataset_index.terminal_session,
        partitions=source_universe.dataset_index.partitions,
    )
    universe_relative = (
        f"{_output_run_root(run_spec.run_spec_id, run_spec.authority)}/"
        "table=universe_daily/dataset-index.json"
    )
    universe_artifact = _write_immutable(
        root,
        universe_relative,
        universe.canonical_bytes(),
        "compacted universe dataset index",
    )
    outputs.append(
        I3ProductionTableOutput(
            storage=I3ProductionOutputStorage.DATASET_INDEX,
            manifest_output=NativeV2OutputArtifact(
                table_name="universe_daily",
                session_date=run_spec.source.terminal_session,
                row_count=universe.row_count,
                contract_id=I3_V2_CONTRACTS["universe_daily"].contract_id,
                schema_digest=I3_V2_CONTRACTS["universe_daily"].schema_digest,
                artifact=universe_artifact,
            ),
            dataset_index=universe,
        )
    )
    written += universe_artifact.bytes
    terminal = tuple(
        replace(item, index_artifact=new_segments[item.table_name])
        for item in source_checkpoint.terminal_row_versions
    )
    return tuple(outputs), terminal, written


def _materialize_checkpoint(
    root: Path,
    *,
    run_spec: I7CheckpointCompactionRunSpec,
    source_checkpoint: I3CheckpointState,
    outputs: tuple[I3ProductionTableOutput, ...],
    terminal_rows: tuple[TerminalRowVersionState, ...],
) -> tuple[NativeV2ReleaseManifest, ArtifactPin, I3CheckpointState]:
    resolved_digest = i3_resolved_state_digest(
        last_session=source_checkpoint.last_session,
        source_cutoff_session=source_checkpoint.source_cutoff_session,
        availability_cutoff_session=run_spec.completion_available_session,
        s4_terminal_pins=source_checkpoint.s4_terminal_pins,
        calendar_digest=source_checkpoint.calendar_digest,
        schema_digest=source_checkpoint.schema_digest,
        transform_semantics_digest=source_checkpoint.transform_semantics_digest,
        identity_policy_bundle=source_checkpoint.identity_policy_bundle,
        identity_policy_bundle_artifact=source_checkpoint.identity_policy_bundle_artifact,
        open_aliases=source_checkpoint.open_aliases,
        asset_aggregates=source_checkpoint.asset_aggregates,
        issuer_aggregates=source_checkpoint.issuer_aggregates,
        unresolved_subjects=source_checkpoint.unresolved_subjects,
        resolved_partition_map=source_checkpoint.resolved_partition_map,
        terminal_row_versions=terminal_rows,
    )
    manifest = NativeV2ReleaseManifest(
        release_family=NATIVE_V2_RELEASE_FAMILY,
        terminal_session=source_checkpoint.last_session,
        release_available_session=run_spec.completion_available_session,
        native_v2_migration_id=stable_digest(
            {
                "rule_version": I7_CHECKPOINT_COMPACTION_RULE_VERSION,
                "source_migration_id": source_checkpoint.parent_release.native_v2_migration_id,
                "source_snapshot_id": run_spec.source.snapshot_id,
            }
        ),
        identity_policy_bundle_id=source_checkpoint.identity_policy_bundle.identity_policy_bundle_id,
        transform_semantics_digest=source_checkpoint.transform_semantics_digest,
        resolved_state_digest=resolved_digest,
        output_artifacts=tuple(item.manifest_output for item in outputs),
        parent_release_id=run_spec.source.native_v2_release_id,
        source_checkpoint_id=source_checkpoint.checkpoint_id,
    )
    manifest_relative = _manifest_path(run_spec.run_spec_id, authority=run_spec.authority)
    manifest_pin = _write_immutable(
        root,
        manifest_relative,
        manifest.canonical_bytes(),
        "compacted native-v2 manifest",
    )
    checkpoint = I3CheckpointState(
        parent_release=NativeV2ParentReleasePin.from_manifest(manifest, path=manifest_relative),
        last_session=source_checkpoint.last_session,
        source_cutoff_session=source_checkpoint.source_cutoff_session,
        availability_cutoff_session=run_spec.completion_available_session,
        s4_terminal_pins=source_checkpoint.s4_terminal_pins,
        calendar_digest=source_checkpoint.calendar_digest,
        schema_digest=source_checkpoint.schema_digest,
        transform_semantics_digest=source_checkpoint.transform_semantics_digest,
        identity_policy_bundle=source_checkpoint.identity_policy_bundle,
        identity_policy_bundle_artifact=source_checkpoint.identity_policy_bundle_artifact,
        open_aliases=source_checkpoint.open_aliases,
        asset_aggregates=source_checkpoint.asset_aggregates,
        issuer_aggregates=source_checkpoint.issuer_aggregates,
        unresolved_subjects=source_checkpoint.unresolved_subjects,
        resolved_partition_map=source_checkpoint.resolved_partition_map,
        terminal_row_versions=terminal_rows,
    )
    return manifest, manifest_pin, checkpoint


def _proof_document(
    *,
    run_spec: I7CheckpointCompactionRunSpec,
    source_checkpoint: I3CheckpointState,
    compacted_manifest: NativeV2ReleaseManifest,
    compacted_checkpoint: I3CheckpointState,
    outputs: tuple[I3ProductionTableOutput, ...],
    rows_by_table: Mapping[str, tuple[dict[str, object], ...]],
) -> dict[str, object]:
    body: dict[str, object] = {
        "artifact_type": "s7_5_i7_checkpoint_compaction_proof",
        "compacted_checkpoint_id": compacted_checkpoint.checkpoint_id,
        "compacted_release_id": compacted_manifest.release_id,
        "logical_tables": {
            table: {
                "row_count": len(rows_by_table[table]),
                "row_digest": _rows_digest(rows_by_table[table]),
            }
            for table in _SMALL_TABLES
        },
        "output_artifacts": [item.to_dict() for item in outputs],
        "publish_authorized": False,
        "rule_version": I7_CHECKPOINT_COMPACTION_RULE_VERSION,
        "run_spec_id": run_spec.run_spec_id,
        "source_checkpoint_id": source_checkpoint.checkpoint_id,
        "source_native_v2_release_id": run_spec.source.native_v2_release_id,
        "source_snapshot_id": run_spec.source.snapshot_id,
        "state": I7_CHECKPOINT_COMPACTION_STATE,
        "universe_partition_set_digest": stable_digest(
            [
                item.to_dict()
                for item in _output_by_name(outputs)["universe_daily"].dataset_index.partitions
            ]
        ),
    }
    body["proof_id"] = stable_digest(body)
    return body


def _validate_module_owned_materialization(
    *,
    run_spec: I7CheckpointCompactionRunSpec,
    source_checkpoint: I3CheckpointState,
    source_outputs: tuple[I3ProductionTableOutput, ...],
    rows_by_table: Mapping[str, tuple[dict[str, object], ...]],
    outputs: tuple[I3ProductionTableOutput, ...],
    manifest: NativeV2ReleaseManifest,
    checkpoint: I3CheckpointState,
) -> None:
    """Rebuild every deterministic physical/control identity without writing."""

    expected_outputs: list[I3ProductionTableOutput] = []
    new_segments: dict[str, ArtifactPin] = {}
    for table_name in _SMALL_TABLES:
        rows = rows_by_table[table_name]
        segment_id = stable_digest(
            {
                "logical_row_digest": _rows_digest(rows),
                "rule_version": I7_CHECKPOINT_COMPACTION_RULE_VERSION,
                "source_checkpoint_id": source_checkpoint.checkpoint_id,
                "source_snapshot_id": run_spec.source.snapshot_id,
                "table_name": table_name,
            }
        )
        segment_relative = (
            f"{_output_run_root(run_spec.run_spec_id, run_spec.authority)}/"
            f"table={table_name}/segment_id={segment_id}/part-00000.parquet"
        )
        segment_artifact = _pin_bytes(
            segment_relative,
            _parquet_bytes(table_name, rows),
        )
        segment = I3ProductionSegmentPin(
            table_name=table_name,
            segment_id=segment_id,
            artifact=segment_artifact,
            row_count=len(rows),
            contract_id=I3_V2_CONTRACTS[table_name].contract_id,
            schema_digest=I3_V2_CONTRACTS[table_name].schema_digest,
            availability_session=run_spec.completion_available_session,
        )
        rowset = I3ProductionRowsetIndex(
            table_name=table_name,
            terminal_session=run_spec.source.terminal_session,
            segments=(segment,),
        )
        index_relative = (
            f"{_output_run_root(run_spec.run_spec_id, run_spec.authority)}/"
            f"table={table_name}/rowset-index.json"
        )
        index_artifact = _pin_bytes(index_relative, rowset.canonical_bytes())
        expected_outputs.append(
            I3ProductionTableOutput(
                storage=I3ProductionOutputStorage.ROWSET_INDEX,
                manifest_output=NativeV2OutputArtifact(
                    table_name=table_name,
                    session_date=run_spec.source.terminal_session,
                    row_count=len(rows),
                    contract_id=I3_V2_CONTRACTS[table_name].contract_id,
                    schema_digest=I3_V2_CONTRACTS[table_name].schema_digest,
                    artifact=index_artifact,
                ),
                rowset_index=rowset,
            )
        )
        new_segments[table_name] = segment_artifact
    source_universe = _output_by_name(source_outputs)["universe_daily"]
    if source_universe.dataset_index is None:
        raise I7CheckpointCompactionError("source universe dataset index is absent")
    universe = I3ProductionDatasetIndex(
        table_name="universe_daily",
        terminal_session=source_universe.dataset_index.terminal_session,
        partitions=source_universe.dataset_index.partitions,
    )
    universe_relative = (
        f"{_output_run_root(run_spec.run_spec_id, run_spec.authority)}/"
        "table=universe_daily/dataset-index.json"
    )
    universe_artifact = _pin_bytes(universe_relative, universe.canonical_bytes())
    expected_outputs.append(
        I3ProductionTableOutput(
            storage=I3ProductionOutputStorage.DATASET_INDEX,
            manifest_output=NativeV2OutputArtifact(
                table_name="universe_daily",
                session_date=run_spec.source.terminal_session,
                row_count=universe.row_count,
                contract_id=I3_V2_CONTRACTS["universe_daily"].contract_id,
                schema_digest=I3_V2_CONTRACTS["universe_daily"].schema_digest,
                artifact=universe_artifact,
            ),
            dataset_index=universe,
        )
    )
    expected_output_tuple = tuple(expected_outputs)
    if expected_output_tuple != outputs:
        raise I7CheckpointCompactionError("compacted output is not module-owned deterministic")
    expected_terminal = tuple(
        replace(item, index_artifact=new_segments[item.table_name])
        for item in source_checkpoint.terminal_row_versions
    )
    resolved_digest = i3_resolved_state_digest(
        last_session=source_checkpoint.last_session,
        source_cutoff_session=source_checkpoint.source_cutoff_session,
        availability_cutoff_session=run_spec.completion_available_session,
        s4_terminal_pins=source_checkpoint.s4_terminal_pins,
        calendar_digest=source_checkpoint.calendar_digest,
        schema_digest=source_checkpoint.schema_digest,
        transform_semantics_digest=source_checkpoint.transform_semantics_digest,
        identity_policy_bundle=source_checkpoint.identity_policy_bundle,
        identity_policy_bundle_artifact=source_checkpoint.identity_policy_bundle_artifact,
        open_aliases=source_checkpoint.open_aliases,
        asset_aggregates=source_checkpoint.asset_aggregates,
        issuer_aggregates=source_checkpoint.issuer_aggregates,
        unresolved_subjects=source_checkpoint.unresolved_subjects,
        resolved_partition_map=source_checkpoint.resolved_partition_map,
        terminal_row_versions=expected_terminal,
    )
    expected_manifest = NativeV2ReleaseManifest(
        release_family=NATIVE_V2_RELEASE_FAMILY,
        terminal_session=source_checkpoint.last_session,
        release_available_session=run_spec.completion_available_session,
        native_v2_migration_id=stable_digest(
            {
                "rule_version": I7_CHECKPOINT_COMPACTION_RULE_VERSION,
                "source_migration_id": source_checkpoint.parent_release.native_v2_migration_id,
                "source_snapshot_id": run_spec.source.snapshot_id,
            }
        ),
        identity_policy_bundle_id=source_checkpoint.identity_policy_bundle.identity_policy_bundle_id,
        transform_semantics_digest=source_checkpoint.transform_semantics_digest,
        resolved_state_digest=resolved_digest,
        output_artifacts=tuple(item.manifest_output for item in expected_output_tuple),
        parent_release_id=run_spec.source.native_v2_release_id,
        source_checkpoint_id=source_checkpoint.checkpoint_id,
    )
    if manifest != expected_manifest:
        raise I7CheckpointCompactionError("compacted manifest is not module-owned deterministic")
    manifest_relative = _manifest_path(run_spec.run_spec_id, authority=run_spec.authority)
    expected_checkpoint = I3CheckpointState(
        parent_release=NativeV2ParentReleasePin.from_manifest(
            expected_manifest,
            path=manifest_relative,
        ),
        last_session=source_checkpoint.last_session,
        source_cutoff_session=source_checkpoint.source_cutoff_session,
        availability_cutoff_session=run_spec.completion_available_session,
        s4_terminal_pins=source_checkpoint.s4_terminal_pins,
        calendar_digest=source_checkpoint.calendar_digest,
        schema_digest=source_checkpoint.schema_digest,
        transform_semantics_digest=source_checkpoint.transform_semantics_digest,
        identity_policy_bundle=source_checkpoint.identity_policy_bundle,
        identity_policy_bundle_artifact=source_checkpoint.identity_policy_bundle_artifact,
        open_aliases=source_checkpoint.open_aliases,
        asset_aggregates=source_checkpoint.asset_aggregates,
        issuer_aggregates=source_checkpoint.issuer_aggregates,
        unresolved_subjects=source_checkpoint.unresolved_subjects,
        resolved_partition_map=source_checkpoint.resolved_partition_map,
        terminal_row_versions=expected_terminal,
    )
    if checkpoint != expected_checkpoint:
        raise I7CheckpointCompactionError("compacted checkpoint is not module-owned deterministic")


def _resolved_terminal_rows(
    root: Path,
    *,
    checkpoint: I3CheckpointState,
    output: I3ProductionTableOutput,
    table_name: str,
) -> tuple[dict[str, object], ...]:
    if output.storage is not I3ProductionOutputStorage.ROWSET_INDEX or output.rowset_index is None:
        raise I7CheckpointCompactionError(f"{table_name} lacks a rowset index")
    segments = {item.artifact: item for item in output.rowset_index.segments}
    states = tuple(
        item for item in checkpoint.terminal_row_versions if item.table_name == table_name
    )
    if not states:
        raise I7CheckpointCompactionError(f"{table_name} terminal state is empty")
    rows_by_pin: dict[ArtifactPin, tuple[dict[str, object], ...]] = {}
    for state in states:
        segment = segments.get(state.index_artifact)
        if segment is None:
            raise I7CheckpointCompactionError("terminal row names an absent segment")
        if state.index_artifact not in rows_by_pin:
            rows_by_pin[state.index_artifact] = _read_parquet_rows(
                root,
                state.index_artifact,
                table_name=table_name,
                expected_rows=segment.row_count,
            )
    version_field = _VERSION_FIELD[table_name]
    result: list[dict[str, object]] = []
    for state in states:
        matches = [
            row
            for row in rows_by_pin[state.index_artifact]
            if row[version_field] == state.row_version_id
        ]
        if len(matches) != 1:
            raise I7CheckpointCompactionError("terminal row does not resolve uniquely")
        row = matches[0]
        if stable_digest(_json_value(row)) != state.row_payload_digest:
            raise I7CheckpointCompactionError("terminal row payload digest differs")
        result.append(row)
    primary_key = tuple(I3_V2_CONTRACTS[table_name].primary_key)
    return tuple(
        sorted(
            result,
            key=lambda row: tuple(_sort_value(row[field]) for field in primary_key),
        )
    )


def _verify_output_exact(
    root: Path,
    output: I3ProductionTableOutput,
    *,
    verify_members: bool = True,
) -> None:
    if output.table_name == "universe_daily":
        if output.dataset_index is None:
            raise I7CheckpointCompactionError("universe output lacks a dataset index")
        content = _read_exact(root, output.manifest_output.artifact, "universe index")
        parsed = I3ProductionDatasetIndex.from_dict(_closed_json(content, "universe index"))
        if parsed != output.dataset_index:
            raise I7CheckpointCompactionError("universe index differs")
        if verify_members:
            for member in parsed.partitions:
                _verify_parquet_pin(
                    root,
                    member.artifact,
                    table_name="universe_daily",
                    expected_rows=member.row_count,
                )
        return
    if output.rowset_index is None:
        raise I7CheckpointCompactionError("small output lacks a rowset index")
    content = _read_exact(root, output.manifest_output.artifact, "rowset index")
    parsed = I3ProductionRowsetIndex.from_dict(_closed_json(content, "rowset index"))
    if parsed != output.rowset_index:
        raise I7CheckpointCompactionError("rowset index differs")
    if verify_members:
        for segment in parsed.segments:
            _verify_parquet_pin(
                root,
                segment.artifact,
                table_name=output.table_name,
                expected_rows=segment.row_count,
            )


def _read_parquet_rows(
    root: Path,
    artifact: ArtifactPin,
    *,
    table_name: str,
    expected_rows: int,
) -> tuple[dict[str, object], ...]:
    content = _read_exact(root, artifact, f"{table_name} Parquet")
    try:
        parquet = pq.ParquetFile(pa.BufferReader(content))
        if (
            parquet.schema_arrow != I3_V2_CONTRACTS[table_name].arrow_schema
            or parquet.metadata.num_rows != expected_rows
        ):
            raise I7CheckpointCompactionError("Parquet schema or row count differs")
        return tuple(dict(row) for row in parquet.read().to_pylist())
    except I7CheckpointCompactionError:
        raise
    except (OSError, pa.ArrowException) as exc:
        raise I7CheckpointCompactionError("compaction input is not readable Parquet") from exc


def _verify_parquet_pin(
    root: Path,
    artifact: ArtifactPin,
    *,
    table_name: str,
    expected_rows: int,
) -> None:
    _read_parquet_rows(
        root,
        artifact,
        table_name=table_name,
        expected_rows=expected_rows,
    )


def _parquet_bytes(table_name: str, rows: Sequence[Mapping[str, object]]) -> bytes:
    try:
        table = pa.Table.from_pylist(list(rows), schema=I3_V2_CONTRACTS[table_name].arrow_schema)
        sink = pa.BufferOutputStream()
        pq.write_table(
            table,
            sink,
            compression="zstd",
            compression_level=6,
            data_page_version="2.0",
            row_group_size=65_536,
            use_dictionary=True,
            version="2.6",
            write_statistics=True,
        )
        return sink.getvalue().to_pybytes()
    except (TypeError, ValueError, pa.ArrowException) as exc:
        raise I7CheckpointCompactionError(f"cannot encode compacted {table_name}") from exc


def _completion_from_dict(value: object) -> I7CheckpointCompactionCompletion:
    fields = {
        "artifact_type",
        "compacted_checkpoint_artifact",
        "compacted_checkpoint_id",
        "compacted_manifest_artifact",
        "compacted_release_id",
        "completion_available_session",
        "completion_id",
        "elapsed_seconds",
        "input_bytes",
        "minimum_disk_free_bytes",
        "output_artifacts",
        "output_bytes",
        "peak_rss_bytes",
        "proof_artifact",
        "publish_authorized",
        "rule_version",
        "run_spec_artifact",
        "run_spec_id",
        "source_snapshot_id",
        "state",
    }
    item = _closed_mapping(value, fields, "compaction completion")
    if (
        item["artifact_type"] != "s7_5_i7_checkpoint_compaction_completion"
        or item["rule_version"] != I7_CHECKPOINT_COMPACTION_RULE_VERSION
    ):
        raise I7CheckpointCompactionError("compaction completion type or rule differs")
    result = I7CheckpointCompactionCompletion(
        run_spec_id=_text(item["run_spec_id"], "completion RunSpec ID"),
        run_spec_artifact=_artifact_from_dict(item["run_spec_artifact"]),
        source_snapshot_id=_text(item["source_snapshot_id"], "completion source"),
        compacted_release_id=_text(item["compacted_release_id"], "compacted release"),
        compacted_manifest_artifact=_artifact_from_dict(item["compacted_manifest_artifact"]),
        compacted_checkpoint_id=_text(item["compacted_checkpoint_id"], "compacted checkpoint"),
        compacted_checkpoint_artifact=_artifact_from_dict(item["compacted_checkpoint_artifact"]),
        proof_artifact=_artifact_from_dict(item["proof_artifact"]),
        output_artifacts=tuple(
            I3ProductionTableOutput.from_dict(entry)
            for entry in _array(item["output_artifacts"], "completion outputs")
        ),
        input_bytes=_integer(item["input_bytes"], "completion input bytes"),
        output_bytes=_integer(item["output_bytes"], "completion output bytes"),
        peak_rss_bytes=_integer(item["peak_rss_bytes"], "completion RSS"),
        minimum_disk_free_bytes=_integer(item["minimum_disk_free_bytes"], "completion disk floor"),
        elapsed_seconds=_integer(item["elapsed_seconds"], "completion elapsed"),
        completion_available_session=_date_value(
            item["completion_available_session"], "completion availability"
        ),
        state=_text(item["state"], "completion state"),
        publish_authorized=_boolean(item["publish_authorized"], "publish authority"),
    )
    if item["completion_id"] != result.completion_id:
        raise I7CheckpointCompactionError("compaction completion ID does not reproduce")
    return result


def _load_run_spec(
    root: Path,
    artifact: ArtifactPin,
    *,
    authority: str,
) -> I7CheckpointCompactionRunSpec:
    run_spec_id = _validate_control_locator(artifact, "run-spec", authority=authority)
    result = I7CheckpointCompactionRunSpec.from_dict(
        _read_canonical(root, artifact, "compaction RunSpec")
    )
    if result.run_spec_id != run_spec_id:
        raise I7CheckpointCompactionError("RunSpec directory ID differs")
    return result


def _preflight_resources(root: Path, run_spec: I7CheckpointCompactionRunSpec) -> None:
    input_bytes = _source_input_bytes(run_spec.source, run_spec.source.table_outputs)
    estimated_output = (
        sum(item.manifest_output.artifact.bytes for item in run_spec.source.table_outputs[:-1]) * 2
        + 16 * 1024 * 1024
    )
    if input_bytes > run_spec.resource_caps.input_bytes_cap:
        raise I7CheckpointCompactionError("declared compaction input exceeds cap")
    if estimated_output > run_spec.resource_caps.output_bytes_cap:
        raise I7CheckpointCompactionError("estimated compaction output exceeds cap")
    required = run_spec.resource_caps.disk_free_bytes_floor + estimated_output
    if shutil.disk_usage(root).free < required:
        raise I7CheckpointCompactionError("compaction would breach disk floor")
    if _peak_rss_bytes() > run_spec.resource_caps.peak_rss_bytes:
        raise I7CheckpointCompactionError("compaction entry RSS exceeds cap")


def _enforce_live_resources(
    root: Path,
    caps: I7CheckpointCompactionResourceCaps,
    *,
    peak_rss: int,
    minimum_disk: int,
) -> None:
    if peak_rss > caps.peak_rss_bytes:
        raise I7CheckpointCompactionError("compaction peak RSS cap exceeded")
    if min(minimum_disk, shutil.disk_usage(root).free) < caps.disk_free_bytes_floor:
        raise I7CheckpointCompactionError("compaction disk floor breached")


def _source_input_bytes(
    source: I7CheckpointCompactionSource,
    outputs: tuple[I3ProductionTableOutput, ...],
) -> int:
    pins = list(source.authority_artifacts)
    for output in outputs:
        pins.append(output.manifest_output.artifact)
        if output.rowset_index is not None:
            pins.extend(item.artifact for item in output.rowset_index.segments)
        if output.dataset_index is not None:
            pins.extend(item.artifact for item in output.dataset_index.partitions)
    return sum(item.bytes for item in _unique_pins(tuple(pins), "compaction source inputs"))


def _write_failed(
    root: Path,
    run_spec: I7CheckpointCompactionRunSpec,
    exc: Exception,
) -> ArtifactPin:
    body: dict[str, object] = {
        "artifact_type": "s7_5_i7_checkpoint_compaction_failed_receipt",
        "error_digest": stable_digest({"message": str(exc), "type": type(exc).__name__}),
        "publish_authorized": False,
        "rule_version": I7_CHECKPOINT_COMPACTION_RULE_VERSION,
        "run_spec_id": run_spec.run_spec_id,
        "state": "failed",
    }
    body["failed_receipt_id"] = stable_digest(body)
    relative = (
        f"{_run_root(run_spec.run_spec_id, run_spec.authority)}/failed-receipts/"
        f"failed_receipt_id={body['failed_receipt_id']}.json"
    )
    return _write_immutable(root, relative, _canonical(body), "compaction failed receipt")


def _atomic_publish_completion(
    root: Path,
    relative: str,
    content: bytes,
    *,
    caps: I7CheckpointCompactionResourceCaps,
    minimum_disk: int,
) -> ArtifactPin:
    target = safe_relative_path(root, relative)
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists() or target.is_symlink():
        existing = _pin_existing(root, relative)
        expected = _pin_bytes(relative, content)
        if existing != expected:
            raise I7CheckpointCompactionError("completion no-clobber conflict")
        return existing
    descriptor, temporary_name = tempfile.mkstemp(prefix=".completion-staged-", dir=target.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        _enforce_live_resources(
            root,
            caps,
            peak_rss=_peak_rss_bytes(),
            minimum_disk=min(minimum_disk, shutil.disk_usage(root).free),
        )
        try:
            os.link(temporary, target)
        except FileExistsError as exc:
            raise I7CheckpointCompactionError("completion lost no-clobber race") from exc
        _fsync_directory(target.parent)
    finally:
        temporary.unlink(missing_ok=True)
    return _pin_existing(root, relative)


def _write_immutable(root: Path, relative: str, content: bytes, label: str) -> ArtifactPin:
    expected = _pin_bytes(relative, content)
    path = safe_relative_path(root, relative)
    if path.exists() or path.is_symlink():
        observed = _pin_existing(root, relative)
        if observed != expected:
            raise I7CheckpointCompactionError(f"{label} no-clobber conflict")
        return observed
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o444)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
    except Exception:
        path.unlink(missing_ok=True)
        raise
    _fsync_directory(path.parent)
    return _pin_existing(root, relative)


class _exclusive_lock:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.stream = None

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.stream = self.path.open("a+b")
        try:
            fcntl.flock(self.stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            self.stream.close()
            raise I7CheckpointCompactionError("another compaction holds the exact lock") from exc
        return self

    def __exit__(self, exc_type, exc, traceback):
        if self.stream is not None:
            fcntl.flock(self.stream.fileno(), fcntl.LOCK_UN)
            self.stream.close()
        return False


def _validate_control_locator(
    artifact: ArtifactPin,
    kind: str,
    *,
    authority: str,
) -> str:
    _artifact(artifact, f"compaction {kind}")
    root = _control_root(authority)
    suffix = "run-spec.json" if kind == "run-spec" else "completion.json"
    prefix = f"{root}/{'run-specs' if kind == 'run-spec' else 'runs'}/run_spec_id="
    path = artifact.path
    expected_tail = f"/{suffix}"
    if not path.startswith(prefix) or not path.endswith(expected_tail):
        raise I7CheckpointCompactionError(f"compaction {kind} path is noncanonical")
    identifier = path[len(prefix) : -len(expected_tail)]
    _digest(identifier, f"compaction {kind} directory ID")
    return identifier


def _control_root(authority: str) -> str:
    if authority == I7_CHECKPOINT_COMPACTION_AUTHORITY:
        return _CONTROL_ROOT
    if authority == I7_CHECKPOINT_COMPACTION_FIXTURE_AUTHORITY:
        return f"manifests/fixtures/{_CONTROL_ROOT}"
    raise I7CheckpointCompactionError("compaction authority is invalid")


def _run_spec_path(run_spec_id: str, *, authority: str) -> str:
    return f"{_control_root(authority)}/run-specs/run_spec_id={run_spec_id}/run-spec.json"


def _run_root(run_spec_id: str, authority: str) -> str:
    return f"{_control_root(authority)}/runs/run_spec_id={run_spec_id}"


def _completion_path(run_spec_id: str, *, authority: str) -> str:
    return f"{_run_root(run_spec_id, authority)}/completion.json"


def _proof_path(run_spec_id: str, *, authority: str) -> str:
    return f"{_run_root(run_spec_id, authority)}/compaction-proof.json"


def _manifest_path(run_spec_id: str, *, authority: str) -> str:
    return f"{_run_root(run_spec_id, authority)}/native-v2-release.json"


def _checkpoint_path(run_spec_id: str, *, authority: str) -> str:
    return f"{_run_root(run_spec_id, authority)}/checkpoint.json"


def _lock_path(run_spec_id: str, *, authority: str) -> str:
    return f"{_control_root(authority)}/locks/run_spec_id={run_spec_id}.lock"


def _output_run_root(run_spec_id: str, authority: str) -> str:
    if authority == I7_CHECKPOINT_COMPACTION_AUTHORITY:
        return f"{_OUTPUT_ROOT}/run_spec_id={run_spec_id}"
    return f"fixtures/{_OUTPUT_ROOT}/run_spec_id={run_spec_id}"


def _read_canonical(root: Path, artifact: ArtifactPin, label: str) -> dict[str, object]:
    content = _read_exact(root, artifact, label)
    item = _closed_json(content, label)
    if _canonical(item) != content:
        raise I7CheckpointCompactionError(f"{label} is not canonical JSON")
    return item


def _read_exact(root: Path, artifact: ArtifactPin, label: str) -> bytes:
    _artifact(artifact, label)
    path = safe_relative_path(root, artifact.path)
    if not path.is_file() or path.is_symlink():
        raise I7CheckpointCompactionError(f"{label} is missing or unsafe")
    content = path.read_bytes()
    if len(content) != artifact.bytes or hashlib.sha256(content).hexdigest() != artifact.sha256:
        raise I7CheckpointCompactionError(f"{label} exact pin differs")
    return content


def _pin_existing(root: Path, relative: str) -> ArtifactPin:
    path = safe_relative_path(root, relative)
    if not path.is_file() or path.is_symlink():
        raise I7CheckpointCompactionError("exact immutable artifact is unavailable")
    content = path.read_bytes()
    return _pin_bytes(relative, content)


def _pin_bytes(relative: str, content: bytes) -> ArtifactPin:
    return ArtifactPin(
        path=relative,
        sha256=hashlib.sha256(content).hexdigest(),
        bytes=len(content),
    )


def _root(value: Path) -> Path:
    root = value.expanduser().resolve()
    if not root.is_dir() or root.is_symlink():
        raise I7CheckpointCompactionError("compaction data root is invalid")
    return root


def _output_by_name(
    outputs: tuple[I3ProductionTableOutput, ...],
) -> dict[str, I3ProductionTableOutput]:
    result = {item.table_name: item for item in outputs}
    if tuple(result) != I3_V2_TABLE_ORDER:
        raise I7CheckpointCompactionError("four-table output order differs")
    return result


def _rows_digest(rows: Sequence[Mapping[str, object]]) -> str:
    return stable_digest([_json_value(row) for row in rows])


def _json_value(value: object) -> object:
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in sorted(value.items())}
    if isinstance(value, (tuple, list)):
        return [_json_value(item) for item in value]
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.hex()
    return value


def _sort_value(value: object) -> tuple[int, str]:
    if value is None:
        return (0, "")
    return (1, str(_json_value(value)))


def _unique_pins(pins: tuple[ArtifactPin, ...], label: str) -> tuple[ArtifactPin, ...]:
    by_path: dict[str, ArtifactPin] = {}
    for pin in pins:
        _artifact(pin, label)
        prior = by_path.get(pin.path)
        if prior is not None and prior != pin:
            raise I7CheckpointCompactionError(f"{label} path has conflicting pins")
        by_path[pin.path] = pin
    return tuple(by_path[path] for path in sorted(by_path))


def _peak_rss_bytes() -> int:
    import resource

    observed = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(observed if os.uname().sysname == "Darwin" else observed * 1024)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _canonical(value: object) -> bytes:
    return (
        json.dumps(
            value,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        + b"\n"
    )


def _closed_json(content: bytes, label: str) -> dict[str, object]:
    try:
        value = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise I7CheckpointCompactionError(f"{label} is not JSON") from exc
    if not isinstance(value, dict):
        raise I7CheckpointCompactionError(f"{label} must be an object")
    return value


def _closed_mapping(value: object, fields: set[str], label: str) -> dict[str, object]:
    if not isinstance(value, Mapping) or set(value) != fields:
        raise I7CheckpointCompactionError(f"{label} fields differ")
    return dict(value)


def _array(value: object, label: str) -> tuple[object, ...]:
    if not isinstance(value, list):
        raise I7CheckpointCompactionError(f"{label} must be an array")
    return tuple(value)


def _artifact_from_dict(value: object) -> ArtifactPin:
    item = _closed_mapping(value, {"bytes", "path", "sha256"}, "artifact pin")
    return ArtifactPin(
        path=_text(item["path"], "artifact path"),
        sha256=_text(item["sha256"], "artifact SHA"),
        bytes=_integer(item["bytes"], "artifact bytes"),
    )


def _artifact(value: object, label: str) -> ArtifactPin:
    if not isinstance(value, ArtifactPin):
        raise I7CheckpointCompactionError(f"{label} is not an ArtifactPin")
    return value


def _text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise I7CheckpointCompactionError(f"{label} must be nonempty text")
    return value


def _digest(value: object, label: str) -> str:
    text = _text(value, label)
    if (
        len(text) != 64
        or text != text.lower()
        or any(char not in "0123456789abcdef" for char in text)
    ):
        raise I7CheckpointCompactionError(f"{label} must be a lowercase SHA-256")
    return text


def _integer(value: object, label: str) -> int:
    if type(value) is not int:
        raise I7CheckpointCompactionError(f"{label} must be an integer")
    return value


def _positive_int(value: object, label: str) -> int:
    result = _integer(value, label)
    if result <= 0:
        raise I7CheckpointCompactionError(f"{label} must be positive")
    return result


def _nonnegative_int(value: object, label: str) -> int:
    result = _integer(value, label)
    if result < 0:
        raise I7CheckpointCompactionError(f"{label} must be nonnegative")
    return result


def _boolean(value: object, label: str) -> bool:
    if type(value) is not bool:
        raise I7CheckpointCompactionError(f"{label} must be boolean")
    return value


def _date_value(value: object, label: str) -> date:
    text = _text(value, label)
    try:
        result = date.fromisoformat(text)
    except ValueError as exc:
        raise I7CheckpointCompactionError(f"{label} is not an ISO date") from exc
    if result.isoformat() != text:
        raise I7CheckpointCompactionError(f"{label} is not canonical")
    return result


def _session(value: object, label: str) -> date:
    if not isinstance(value, date):
        raise I7CheckpointCompactionError(f"{label} is not a date")
    return value


__all__ = [
    "I7CheckpointCompactionCompletion",
    "I7CheckpointCompactionError",
    "I7CheckpointCompactionResourceCaps",
    "I7CheckpointCompactionRunSpec",
    "I7CheckpointCompactionSource",
    "LoadedI7CheckpointCompaction",
    "prepare_i7_checkpoint_compaction",
    "stage_i7_checkpoint_compaction",
    "verify_i7_checkpoint_compaction",
]
