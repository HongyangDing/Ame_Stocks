"""Exact, non-publishing S7.5 I5 shadow-equivalence runtime.

The runtime consumes two independently materialized candidates:

* one exact, deep-attested native-v2 I3 DELTA; and
* one exact legacy streaming Full candidate at the same cutoff.

It deliberately cannot launch either producer and it never resolves ``latest``.
Every authority enters as a complete path/SHA/byte pin.  The legacy Full side is
accepted only after its plan/request/approval/source-binding chain and immutable
``awaiting_review`` completion have been replayed.  Consequently, if no Full
candidate exists for the requested cutoff, I5 stops at the explicit P0 oracle
seam rather than accepting caller-supplied row or digest claims.

Successful execution writes only immutable review evidence.  It creates no Gate
B approval, pointer event, publication marker, deletion, or mutable selector.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import math
import os
import re
import resource
import shutil
import stat
import sys
import time
from collections.abc import Mapping, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass, replace
from datetime import date
from pathlib import Path, PurePosixPath
from types import MappingProxyType
from typing import Final, Self

import pyarrow as pa
import pyarrow.parquet as pq

from ame_stocks_api.artifacts import safe_relative_path, stable_digest
from ame_stocks_api.silver import identity_materialization_streaming as full_runtime
from ame_stocks_api.silver import incremental_i3_checkpoint as i3_checkpoint
from ame_stocks_api.silver import incremental_i3_production as i3_runtime
from ame_stocks_api.silver.identity_resolution_contract import S7_DERIVED_CONTRACTS
from ame_stocks_api.silver.incremental_contract import ArtifactPin, ViewKind
from ame_stocks_api.silver.incremental_i3_contract import I3_V2_CONTRACTS, I3_V2_TABLE_ORDER
from ame_stocks_api.silver.incremental_i3_delta_io import (
    load_production_delta_input_binding,
    load_production_delta_materializer,
)
from ame_stocks_api.silver.incremental_i3_production import (
    stage_i3_production_delta,
    verify_i3_production_deep_attestation,
)
from ame_stocks_api.silver.incremental_i3_production_contract import (
    I3ProductionContractError,
    I3ProductionOutputStorage,
    I3ProductionRunKind,
    LoadedI3ProductionStaging,
    load_i3_production_deep_attestation_exact,
    load_i3_production_parent_shallow_exact,
)
from ame_stocks_api.silver.incremental_i5_lifecycle import (
    EquivalenceProjection,
    FailureRecoveryReceipt,
    FailureScenario,
    IdempotencyReceipt,
    ProjectionComparisonReceipt,
    ProjectionPolicy,
    ResourceGatePolicy,
    ResourceObservation,
    ShadowEquivalenceReceipt,
    ShadowEquivalenceSpec,
    validate_shadow_equivalence,
)

I5_SHADOW_RUNTIME_RULE_VERSION: Final = "s7_5_i5_exact_shadow_runtime_v1"
I5_SHADOW_RUN_SPEC_RULE_VERSION: Final = "s7_5_i5_exact_shadow_run_spec_v1"
I5_SHADOW_COMPLETION_RULE_VERSION: Final = "s7_5_i5_awaiting_review_completion_v1"
I5_CANONICAL_PROJECTION_RULE_VERSION: Final = "s7_5_i5_canonical_research_projection_v1"
I5_PHYSICAL_REUSE_PROJECTION_RULE_VERSION: Final = "s7_5_i5_physical_reuse_projection_v1"
I5_FAILURE_EXERCISE_RULE_VERSION: Final = "s7_5_i5_failure_recovery_exercise_v1"
I5_PRODUCTION_AUTHORITY: Final = "production_exact_completed_candidates"
I5_FIXTURE_AUTHORITY: Final = "fixture_non_authoritative"
I5_STATE: Final = "awaiting_review"
I5_TARGET_SESSION: Final = date(2026, 7, 10)
I5_REQUIRED_COMPARISON_SESSIONS: Final = (
    date(2022, 2, 8),  # known provider cross-market contamination
    date(2023, 11, 24),  # XNYS half-day session
    date(2025, 1, 2),  # SOR genuine transition/stale-provider boundary
    date(2025, 11, 5),  # XZO IPO identity boundary
    date(2026, 4, 20),  # ANABV temporary ex-distribution boundary
    I5_TARGET_SESSION,  # fixed first native-v2 DELTA
)
I5_TABLE_ORDER: Final = tuple(I3_V2_TABLE_ORDER)

_I5_SCOPE_DOCUMENT: Final = {
    "artifact_type": "s7_5_i5_module_owned_shadow_scope",
    "comparison_sessions": [item.isoformat() for item in I5_REQUIRED_COMPARISON_SESSIONS],
    "rule_version": "s7_5_i5_module_owned_shadow_scope_v1",
}
_I5_SCOPE_BYTES: Final = (
    json.dumps(
        _I5_SCOPE_DOCUMENT,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    + "\n"
).encode("utf-8")
I5_SCOPE_ARTIFACT: Final = ArtifactPin(
    path="embedded-contracts/silver/s7_5_i5_shadow_scope-v1.json",
    sha256=hashlib.sha256(_I5_SCOPE_BYTES).hexdigest(),
    bytes=len(_I5_SCOPE_BYTES),
)

_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_SESSION_PARTITION = re.compile(r"(?:^|/)session_date=(\d{4}-\d{2}-\d{2})(?:/|$)")
_FORBIDDEN_AUTHORITY_PARTS = {"latest", "tmp", ".tmp", "fixtures", "fixture", "test"}
_CANONICAL_SEMANTICS = stable_digest(
    {
        "alias_id_projection": "recompute_v1_from_canonical_interval",
        "excluded_envelope": "native_v2_resolution_and_physical_version_fields",
        "rule_version": I5_CANONICAL_PROJECTION_RULE_VERSION,
        "table_order": list(I5_TABLE_ORDER),
    }
)
_PHYSICAL_SEMANTICS = stable_digest(
    {
        "clean_delta": "exact_parent_prefix_plus_one_target_suffix",
        "rule_version": I5_PHYSICAL_REUSE_PROJECTION_RULE_VERSION,
        "table_order": list(I5_TABLE_ORDER),
        "target_session": I5_TARGET_SESSION.isoformat(),
    }
)
_INTERRUPTED_RUN_SEAM_MESSAGE: Final = (
    "P0 interrupted-run evidence unavailable: the exact production I3 interrupted-retry "
    "control chain did not replay"
)


class I5ShadowRuntimeError(RuntimeError):
    """Raised before untrusted or non-equivalent shadow evidence can complete."""


class I5FullOracleSeamError(I5ShadowRuntimeError):
    """P0: no independently completed Full oracle can be proven for the exact scope."""


class I5ProductionFailureExerciseSeamError(I5ShadowRuntimeError):
    """P0: exact production interrupted-run recovery evidence cannot be replayed."""


class I5ProductionReadMeterSeamError(I5ShadowRuntimeError):
    """P0: producer replay cannot run without an exact process read-byte counter."""


@dataclass(frozen=True, slots=True)
class ShadowRunConfig:
    """Caller-selected exact pins and bounded comparison dates; never digests."""

    incremental_completion_artifact: ArtifactPin
    incremental_deep_attestation_artifact: ArtifactPin
    full_oracle_completion_artifact: ArtifactPin
    comparison_sessions: tuple[date, ...]
    receipt_available_session: date
    resource_policy: ResourceGatePolicy

    def __post_init__(self) -> None:
        for value, label in (
            (self.incremental_completion_artifact, "incremental completion"),
            (self.incremental_deep_attestation_artifact, "incremental deep attestation"),
            (self.full_oracle_completion_artifact, "Full oracle completion"),
        ):
            _artifact(value, label)
            _production_authority_path(value.path, label)
        _sessions(self.comparison_sessions)
        if self.comparison_sessions != I5_REQUIRED_COMPARISON_SESSIONS:
            raise I5ShadowRuntimeError("shadow scope differs from the module-owned challenge set")
        if not isinstance(self.receipt_available_session, date):
            raise I5ShadowRuntimeError("shadow receipt availability must be a date")
        if self.receipt_available_session < I5_TARGET_SESSION:
            raise I5ShadowRuntimeError("shadow receipt availability predates the comparison")
        if not isinstance(self.resource_policy, ResourceGatePolicy):
            raise I5ShadowRuntimeError("shadow resource policy is invalid")


@dataclass(frozen=True, slots=True)
class ShadowRunSpec:
    """Runtime-owned binding of both exact producers and lifecycle semantics."""

    authority: str
    incremental_completion_artifact: ArtifactPin
    incremental_deep_attestation_artifact: ArtifactPin
    full_oracle_completion_artifact: ArtifactPin
    incremental_release_id: str
    full_oracle_release_id: str
    common_parent_release_id: str
    source_binding_digest: str
    schema_bundle_digest: str
    transform_semantics_digest: str
    identity_policy_bundle_id: str
    calendar_digest: str
    scope_artifact: ArtifactPin
    comparison_sessions: tuple[date, ...]
    receipt_available_session: date
    resource_policy: ResourceGatePolicy

    def __post_init__(self) -> None:
        if self.authority not in {I5_PRODUCTION_AUTHORITY, I5_FIXTURE_AUTHORITY}:
            raise I5ShadowRuntimeError("shadow RunSpec authority is invalid")
        for value, label in (
            (self.incremental_completion_artifact, "incremental completion"),
            (self.incremental_deep_attestation_artifact, "incremental deep attestation"),
            (self.full_oracle_completion_artifact, "Full oracle completion"),
        ):
            _artifact(value, label)
        for value, label in (
            (self.incremental_release_id, "incremental release ID"),
            (self.full_oracle_release_id, "Full oracle release ID"),
            (self.common_parent_release_id, "common parent release ID"),
            (self.source_binding_digest, "source binding digest"),
            (self.schema_bundle_digest, "schema bundle digest"),
            (self.transform_semantics_digest, "transform semantics digest"),
            (self.identity_policy_bundle_id, "identity policy bundle ID"),
            (self.calendar_digest, "calendar digest"),
        ):
            _digest(value, label)
        _artifact(self.scope_artifact, "module-owned shadow scope")
        if self.scope_artifact != I5_SCOPE_ARTIFACT:
            raise I5ShadowRuntimeError("shadow RunSpec scope pin differs")
        if self.incremental_release_id == self.full_oracle_release_id:
            raise I5ShadowRuntimeError("Full oracle must be independently identified")
        _sessions(self.comparison_sessions)
        if self.comparison_sessions != I5_REQUIRED_COMPARISON_SESSIONS:
            raise I5ShadowRuntimeError("shadow RunSpec challenge set differs")
        if self.receipt_available_session < I5_TARGET_SESSION:
            raise I5ShadowRuntimeError("shadow RunSpec availability predates its cutoff")
        if not isinstance(self.resource_policy, ResourceGatePolicy):
            raise I5ShadowRuntimeError("shadow RunSpec resource policy is invalid")

    @property
    def projection_policies(self) -> tuple[ProjectionPolicy, ...]:
        return (
            ProjectionPolicy(
                EquivalenceProjection.CANONICAL_RESEARCH,
                _CANONICAL_SEMANTICS,
            ),
            ProjectionPolicy(
                EquivalenceProjection.PHYSICAL_REUSE,
                _PHYSICAL_SEMANTICS,
            ),
        )

    @property
    def lifecycle_spec(self) -> ShadowEquivalenceSpec:
        return ShadowEquivalenceSpec(
            incremental_release_id=self.incremental_release_id,
            full_oracle_release_id=self.full_oracle_release_id,
            common_parent_release_id=self.common_parent_release_id,
            source_binding_digest=self.source_binding_digest,
            schema_bundle_digest=self.schema_bundle_digest,
            transform_semantics_digest=self.transform_semantics_digest,
            identity_policy_bundle_id=self.identity_policy_bundle_id,
            calendar_digest=self.calendar_digest,
            view=ViewKind.LATEST_REVIEWED_RESEARCH,
            comparison_cutoff_session=I5_TARGET_SESSION,
            comparison_sessions=self.comparison_sessions,
            projection_policies=self.projection_policies,
            resource_policy=self.resource_policy,
        )

    @property
    def run_spec_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "authority": self.authority,
            "calendar_digest": self.calendar_digest,
            "common_parent_release_id": self.common_parent_release_id,
            "comparison_sessions": [item.isoformat() for item in self.comparison_sessions],
            "full_oracle_completion_artifact": (self.full_oracle_completion_artifact.to_dict()),
            "full_oracle_release_id": self.full_oracle_release_id,
            "identity_policy_bundle_id": self.identity_policy_bundle_id,
            "incremental_completion_artifact": (self.incremental_completion_artifact.to_dict()),
            "incremental_deep_attestation_artifact": (
                self.incremental_deep_attestation_artifact.to_dict()
            ),
            "incremental_release_id": self.incremental_release_id,
            "projection_policies": [item.to_dict() for item in self.projection_policies],
            "receipt_available_session": self.receipt_available_session.isoformat(),
            "resource_policy": self.resource_policy.to_dict(),
            "rule_version": I5_SHADOW_RUN_SPEC_RULE_VERSION,
            "schema_bundle_digest": self.schema_bundle_digest,
            "scope_artifact": self.scope_artifact.to_dict(),
            "source_binding_digest": self.source_binding_digest,
            "transform_semantics_digest": self.transform_semantics_digest,
        }

    def to_dict(self) -> dict[str, object]:
        return {"run_spec_id": self.run_spec_id, **self.logical_payload()}

    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.to_dict())

    def exact_pin(self, *, path: str) -> ArtifactPin:
        return _pin_bytes(path, self.canonical_bytes())


@dataclass(frozen=True, slots=True)
class ShadowRunCompletion:
    """Immutable awaiting-review evidence; deliberately carries no Gate B authority."""

    run_spec_id: str
    run_spec_artifact: ArtifactPin
    receipt: ShadowEquivalenceReceipt
    receipt_available_session: date
    authority: str

    def __post_init__(self) -> None:
        _digest(self.run_spec_id, "shadow completion RunSpec ID")
        _artifact(self.run_spec_artifact, "shadow completion RunSpec artifact")
        if not isinstance(self.receipt, ShadowEquivalenceReceipt):
            raise I5ShadowRuntimeError("shadow completion receipt is invalid")
        if self.receipt_available_session != self.receipt.receipt_available_session:
            raise I5ShadowRuntimeError("shadow completion availability differs")
        if self.authority not in {I5_PRODUCTION_AUTHORITY, I5_FIXTURE_AUTHORITY}:
            raise I5ShadowRuntimeError("shadow completion authority is invalid")

    @property
    def completion_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "artifact_type": "s7_5_i5_shadow_equivalence_completion",
            "authority": self.authority,
            "cutover_authorized": False,
            "gate_b_authorized": False,
            "publish_authorized": False,
            "receipt": self.receipt.to_dict(),
            "receipt_available_session": self.receipt_available_session.isoformat(),
            "rule_version": I5_SHADOW_COMPLETION_RULE_VERSION,
            "run_spec_artifact": self.run_spec_artifact.to_dict(),
            "run_spec_id": self.run_spec_id,
            "state": I5_STATE,
        }

    def to_dict(self) -> dict[str, object]:
        return {"completion_id": self.completion_id, **self.logical_payload()}

    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.to_dict())

    def exact_pin(self, *, path: str) -> ArtifactPin:
        return _pin_bytes(path, self.canonical_bytes())


@dataclass(frozen=True, slots=True)
class ShadowRunResult:
    run_spec: ShadowRunSpec
    run_spec_artifact: ArtifactPin
    completion: ShadowRunCompletion
    completion_artifact: ArtifactPin
    idempotent: bool


@dataclass(frozen=True, slots=True)
class _ResolvedSide:
    release_id: str
    rows: Mapping[str, Mapping[str, tuple[dict[str, object], ...]]]
    physical: Mapping[str, tuple[dict[str, object], ...]]
    source_binding_digest: str
    schema_bundle_digest: str
    transform_semantics_digest: str
    identity_policy_bundle_id: str
    calendar_digest: str
    checkpoint_id: str
    run_receipt_id: str
    manifest_sha256: str
    replayed_bytes_floor: int = 0


@dataclass(frozen=True, slots=True)
class _ProductionInputs:
    incremental: _ResolvedSide
    oracle: _ResolvedSide
    loaded_incremental: LoadedI3ProductionStaging | None
    loaded_parent: LoadedI3ProductionStaging | None
    parent_physical: Mapping[str, tuple[dict[str, object], ...]]
    parent_reader_digest: str
    chain_resolution_milliseconds: int


@dataclass(frozen=True, slots=True)
class _ProcessIOCounter:
    rchar: int
    wchar: int
    syscr: int
    syscw: int
    physical_read_bytes: int
    physical_write_bytes: int
    backend: str


@dataclass(frozen=True, slots=True)
class _ProcessIODelta:
    read_characters: int
    write_characters: int
    read_syscalls: int
    write_syscalls: int
    physical_read_bytes: int
    physical_write_bytes: int

    @property
    def read_bytes(self) -> int:
        return max(self.read_characters, self.physical_read_bytes)

    @property
    def write_bytes(self) -> int:
        # ``wchar`` is the kernel-audited byte count passed to write-like
        # syscalls.  Unlike ``write_bytes``, it is not block-rounded or delayed
        # by writeback, so the immutable completion can reproduce it exactly.
        return self.write_characters


@dataclass(slots=True)
class _ProcessIOAudit:
    baseline: _ProcessIOCounter
    latest: _ProcessIODelta | None = None

    def sample(self, meter: _ReadMeter) -> _ProcessIODelta:
        delta = _process_io_delta(
            self.baseline,
            _process_io_snapshot(require_exact=True),
        )
        meter.bytes = max(meter.bytes, delta.read_bytes)
        self.latest = delta
        return delta

    @property
    def write_bytes(self) -> int:
        return 0 if self.latest is None else self.latest.write_bytes


@dataclass(slots=True)
class _DiskFloorMonitor:
    root: Path
    floor_bytes: int
    minimum_effective_free_bytes: int | None = None

    def sample(self, phase: str, *, reserve_bytes: int = 0) -> int:
        if not phase or reserve_bytes < 0:
            raise I5ShadowRuntimeError("shadow disk sample is invalid")
        observed = _disk_free(self.root)
        effective = max(0, observed - reserve_bytes)
        if self.minimum_effective_free_bytes is None:
            self.minimum_effective_free_bytes = effective
        else:
            self.minimum_effective_free_bytes = min(
                self.minimum_effective_free_bytes,
                effective,
            )
        _require_disk_floor(effective, self.floor_bytes)
        return effective

    @property
    def minimum(self) -> int:
        if self.minimum_effective_free_bytes is None:
            raise I5ShadowRuntimeError("shadow disk floor was never sampled")
        return self.minimum_effective_free_bytes


class _ReadMeter:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.bytes = 0

    def read_pin(self, pin: ArtifactPin, *, label: str) -> bytes:
        _artifact(pin, label)
        path = safe_relative_path(self.root, pin.path)
        if not path.is_file() or path.is_symlink():
            raise I5ShadowRuntimeError(f"{label} is missing or unsafe")
        try:
            opened = path.stat(follow_symlinks=False)
            content = path.read_bytes()
        except OSError as exc:
            raise I5ShadowRuntimeError(f"cannot read {label}") from exc
        self.bytes += len(content)
        if (
            opened.st_size != pin.bytes
            or len(content) != pin.bytes
            or hashlib.sha256(content).hexdigest() != pin.sha256
        ):
            raise I5ShadowRuntimeError(f"{label} exact pin differs")
        return content

    def read_path(self, relative: str) -> bytes:
        path = safe_relative_path(self.root, relative)
        if not path.is_file() or path.is_symlink():
            raise I5ShadowRuntimeError("exact lifecycle artifact is missing or unsafe")
        content = path.read_bytes()
        self.bytes += len(content)
        return content


class _exclusive_nonblocking_lock(AbstractContextManager["_exclusive_nonblocking_lock"]):
    def __init__(self, path: Path) -> None:
        self.path = path
        self.descriptor: int | None = None

    def __enter__(self) -> Self:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        flags = os.O_RDWR | os.O_CREAT
        if hasattr(os, "O_CLOEXEC"):
            flags |= os.O_CLOEXEC
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor: int | None = None
        try:
            descriptor = os.open(self.path, flags, 0o600)
            opened = os.fstat(descriptor)
            if not stat.S_ISREG(opened.st_mode) or opened.st_nlink != 1:
                raise I5ShadowRuntimeError("shadow lock is not a safe regular file")
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            if descriptor is not None:
                os.close(descriptor)
            raise I5ShadowRuntimeError("another process holds the exact shadow lock") from exc
        except Exception:
            if descriptor is not None:
                os.close(descriptor)
            raise
        self.descriptor = descriptor
        return self

    def __exit__(self, *_: object) -> None:
        if self.descriptor is not None:
            try:
                fcntl.flock(self.descriptor, fcntl.LOCK_UN)
            finally:
                os.close(self.descriptor)
                self.descriptor = None


def _process_io_snapshot(*, require_exact: bool) -> _ProcessIOCounter:
    """Read kernel counters around every non-injectable shadow phase."""

    path = Path("/proc/self/io")
    if path.is_file() and not path.is_symlink():
        try:
            values = {}
            for line in path.read_text(encoding="ascii").splitlines():
                key, separator, raw = line.partition(":")
                if separator:
                    values[key] = int(raw.strip())
            rchar = values["rchar"]
            wchar = values["wchar"]
            syscr = values["syscr"]
            syscw = values["syscw"]
            physical_read = values["read_bytes"]
            physical_write = values["write_bytes"]
        except (KeyError, OSError, UnicodeError, ValueError) as exc:
            raise I5ProductionReadMeterSeamError(
                "P0 shadow resource meter: /proc/self/io cannot be parsed"
            ) from exc
        return _ProcessIOCounter(
            rchar=rchar,
            wchar=wchar,
            syscr=syscr,
            syscw=syscw,
            physical_read_bytes=physical_read,
            physical_write_bytes=physical_write,
            backend="linux_proc_io",
        )
    if require_exact:
        raise I5ProductionReadMeterSeamError(
            "P0 shadow resource meter: non-injectable I3/Full verifiers require Linux "
            "/proc/self/io rchar/wchar/syscr/syscw/read_bytes/write_bytes counters"
        )
    return _ProcessIOCounter(
        rchar=0,
        wchar=0,
        syscr=0,
        syscw=0,
        physical_read_bytes=0,
        physical_write_bytes=0,
        backend="fixture_none",
    )


def _process_io_delta(
    before: _ProcessIOCounter,
    after: _ProcessIOCounter,
) -> _ProcessIODelta:
    if before.backend != after.backend or before.backend != "linux_proc_io":
        raise I5ProductionReadMeterSeamError("P0 producer read meter backend changed")
    values = _ProcessIODelta(
        read_characters=after.rchar - before.rchar,
        write_characters=after.wchar - before.wchar,
        read_syscalls=after.syscr - before.syscr,
        write_syscalls=after.syscw - before.syscw,
        physical_read_bytes=after.physical_read_bytes - before.physical_read_bytes,
        physical_write_bytes=after.physical_write_bytes - before.physical_write_bytes,
    )
    if any(
        value < 0
        for value in (
            values.read_characters,
            values.write_characters,
            values.read_syscalls,
            values.write_syscalls,
            values.physical_read_bytes,
            values.physical_write_bytes,
        )
    ):
        raise I5ProductionReadMeterSeamError("P0 shadow resource counters moved backwards")
    return values


def execute_i5_shadow_run(data_root: Path, *, config: ShadowRunConfig) -> ShadowRunResult:
    """Execute one exact I5 comparison and stop at immutable ``awaiting_review``.

    The function has no producer fallback.  In particular, a missing Full
    candidate is a P0 seam, not permission to synthesize an oracle from the
    incremental output.
    """

    if not isinstance(config, ShadowRunConfig):
        raise I5ShadowRuntimeError("shadow execution requires a typed exact config")
    root = data_root.expanduser().resolve()
    if not root.is_dir() or root.is_symlink():
        raise I5ShadowRuntimeError("shadow data root is missing or unsafe")
    started = time.monotonic()
    io_audit = _ProcessIOAudit(_process_io_snapshot(require_exact=True))
    meter = _ReadMeter(root)
    disk_monitor = _DiskFloorMonitor(root, config.resource_policy.min_free_disk_bytes)
    minimum_disk = disk_monitor.sample("shadow_entry")
    _enforce_resource_policy(
        config.resource_policy,
        started=started,
        meter=meter,
        written_bytes=0,
        chain_resolution_milliseconds=0,
        free_disk_bytes=minimum_disk,
    )
    inputs = _load_production_inputs(
        root,
        config=config,
        meter=meter,
        io_audit=io_audit,
        disk_monitor=disk_monitor,
    )
    _enforce_resource_policy(
        config.resource_policy,
        started=started,
        meter=meter,
        written_bytes=io_audit.write_bytes,
        chain_resolution_milliseconds=inputs.chain_resolution_milliseconds,
        free_disk_bytes=disk_monitor.minimum,
    )
    spec = _build_run_spec(config, inputs, authority=I5_PRODUCTION_AUTHORITY)
    return _execute_loaded_shadow(
        root,
        spec=spec,
        inputs=inputs,
        meter=meter,
        started=started,
        minimum_disk=minimum_disk,
        production=True,
        io_audit=io_audit,
        disk_monitor=disk_monitor,
    )


def _load_production_inputs(
    root: Path,
    *,
    config: ShadowRunConfig,
    meter: _ReadMeter,
    io_audit: _ProcessIOAudit,
    disk_monitor: _DiskFloorMonitor | None = None,
) -> _ProductionInputs:
    chain_started = time.monotonic()
    if disk_monitor is not None:
        disk_monitor.sample("before_i3_producer_replay")
    try:
        loaded = verify_i3_production_deep_attestation(
            root,
            config.incremental_completion_artifact,
            config.incremental_deep_attestation_artifact,
            expected_kind=I3ProductionRunKind.DELTA,
        )
    except Exception as exc:
        raise I5ShadowRuntimeError("exact incremental DELTA replay failed") from exc
    deep = load_i3_production_deep_attestation_exact(
        config.incremental_deep_attestation_artifact,
        meter.read_path,
    )
    loaded = replace(loaded, deep_attestation=deep)
    if (
        loaded.run_spec.terminal_session != I5_TARGET_SESSION
        or loaded.gate_a_manifest.release_available_session > config.receipt_available_session
        or loaded.completion.completion_available_session > config.receipt_available_session
        or loaded.deep_attestation is None
        or loaded.deep_attestation.attestation_available_session > config.receipt_available_session
    ):
        raise I5ShadowRuntimeError("incremental DELTA scope or availability differs")
    parent = load_i3_production_parent_shallow_exact(root, loaded.run_spec)
    if parent is None:
        raise I5ShadowRuntimeError("incremental DELTA lacks its exact parent")
    try:
        delta_inputs = load_production_delta_input_binding(
            data_root=root,
            run_spec=loaded.run_spec,
            parent=parent,
        )
    except Exception as exc:
        raise I5ShadowRuntimeError("exact incremental DELTA inputs cannot be replayed") from exc
    incremental = _incremental_side(
        root,
        loaded=loaded,
        comparison_sessions=config.comparison_sessions,
        completion_pin=config.incremental_completion_artifact,
        deep_attestation_pin=config.incremental_deep_attestation_artifact,
        meter=meter,
    )
    oracle, oracle_binding = _full_oracle_side(
        root,
        completion_pin=config.full_oracle_completion_artifact,
        comparison_sessions=config.comparison_sessions,
        receipt_available_session=config.receipt_available_session,
        meter=meter,
        disk_monitor=disk_monitor,
    )
    if delta_inputs.source_binding != oracle_binding:
        raise I5FullOracleSeamError(
            "P0 full-oracle seam: producers do not bind the same exact S7 inputs"
        )
    _reconcile_producer_authorities(loaded, oracle_binding, incremental, oracle)
    io_delta = io_audit.sample(meter)
    meter.bytes = max(
        meter.bytes,
        io_delta.read_bytes,
        incremental.replayed_bytes_floor + oracle.replayed_bytes_floor,
    )
    if disk_monitor is not None:
        disk_monitor.sample("after_initial_producer_replay")
    chain_milliseconds = math.ceil((time.monotonic() - chain_started) * 1000)
    return _ProductionInputs(
        incremental=incremental,
        oracle=oracle,
        loaded_incremental=loaded,
        loaded_parent=parent,
        parent_physical=_incremental_physical_projection(parent),
        parent_reader_digest=_parent_reader_digest(parent),
        chain_resolution_milliseconds=chain_milliseconds,
    )


def _incremental_side(
    root: Path,
    *,
    loaded: LoadedI3ProductionStaging,
    comparison_sessions: tuple[date, ...],
    completion_pin: ArtifactPin,
    deep_attestation_pin: ArtifactPin,
    meter: _ReadMeter,
) -> _ResolvedSide:
    output_set = loaded.receipt.output_set
    if output_set is None:  # pragma: no cover - successful exact loader invariant
        raise I5ShadowRuntimeError("incremental completion lost its OutputSet")
    outputs = {item.table_name: item for item in output_set.table_outputs}
    alias_rows = _resolved_incremental_small_rows(
        root,
        loaded=loaded,
        table_name="ticker_alias",
        meter=meter,
    )
    alias_reverse = _legacy_alias_reverse(alias_rows)
    rows: dict[str, Mapping[str, tuple[dict[str, object], ...]]] = {}
    for table_name in ("asset_master", "ticker_alias", "issuer_master"):
        table_rows = (
            alias_rows
            if table_name == "ticker_alias"
            else _resolved_incremental_small_rows(
                root,
                loaded=loaded,
                table_name=table_name,
                meter=meter,
            )
        )
        rows[table_name] = MappingProxyType(
            {"__table__": _canonical_v1_rows(table_name, table_rows, alias_reverse)}
        )
    universe = outputs["universe_daily"].dataset_index
    if universe is None:
        raise I5ShadowRuntimeError("incremental universe dataset index is missing")
    by_session = {item.session_date: item for item in universe.partitions}
    selected: dict[str, tuple[dict[str, object], ...]] = {}
    for session in comparison_sessions:
        pin = by_session.get(session)
        if pin is None:
            raise I5ShadowRuntimeError(
                f"incremental snapshot lacks comparison session {session.isoformat()}"
            )
        table = _read_parquet_exact(
            root,
            artifact=pin.artifact,
            table_name="universe_daily",
            expected_schema=I3_V2_CONTRACTS["universe_daily"].arrow_schema,
            expected_rows=pin.row_count,
            meter=meter,
        )
        raw = tuple(dict(item) for item in table.to_pylist())
        if any(item["session_date"] != session for item in raw):
            raise I5ShadowRuntimeError("incremental universe partition crosses sessions")
        selected[session.isoformat()] = _canonical_v1_rows("universe_daily", raw, alias_reverse)
    rows["universe_daily"] = MappingProxyType(selected)
    physical = _incremental_physical_projection(loaded)
    lineage = _source_lineage_digest(rows["universe_daily"])
    return _ResolvedSide(
        release_id=loaded.gate_a_manifest.release_id,
        rows=MappingProxyType(rows),
        physical=physical,
        source_binding_digest=lineage,
        schema_bundle_digest=_canonical_schema_bundle_digest(),
        transform_semantics_digest=loaded.run_spec.transform_semantics_digest,
        identity_policy_bundle_id=(
            loaded.run_spec.identity_policy_bundle.identity_policy_bundle_id
        ),
        calendar_digest=loaded.run_spec.calendar.calendar_artifact_id,
        checkpoint_id=loaded.checkpoint.checkpoint_id,
        run_receipt_id=loaded.receipt.receipt_id,
        manifest_sha256=output_set.gate_a_manifest_pin.manifest_sha256,
        replayed_bytes_floor=completion_pin.bytes + deep_attestation_pin.bytes,
    )


def _resolved_incremental_small_rows(
    root: Path,
    *,
    loaded: LoadedI3ProductionStaging,
    table_name: str,
    meter: _ReadMeter,
) -> tuple[dict[str, object], ...]:
    output_set = loaded.receipt.output_set
    if output_set is None:  # pragma: no cover
        raise I5ShadowRuntimeError("incremental OutputSet is absent")
    output = output_set.table_outputs[I5_TABLE_ORDER.index(table_name)]
    if output.storage is not I3ProductionOutputStorage.ROWSET_INDEX or output.rowset_index is None:
        raise I5ShadowRuntimeError("incremental small table lacks an exact rowset index")
    segments = {item.artifact: item for item in output.rowset_index.segments}
    terminal = tuple(
        item for item in loaded.checkpoint.terminal_row_versions if item.table_name == table_name
    )
    if not terminal:
        raise I5ShadowRuntimeError(f"incremental {table_name} has no terminal rows")
    by_artifact: dict[ArtifactPin, tuple[dict[str, object], ...]] = {}
    for item in terminal:
        segment = segments.get(item.index_artifact)
        if segment is None:
            raise I5ShadowRuntimeError("terminal row locator names an absent rowset segment")
        if item.index_artifact not in by_artifact:
            table = _read_parquet_exact(
                root,
                artifact=item.index_artifact,
                table_name=table_name,
                expected_schema=I3_V2_CONTRACTS[table_name].arrow_schema,
                expected_rows=segment.row_count,
                meter=meter,
            )
            by_artifact[item.index_artifact] = tuple(dict(value) for value in table.to_pylist())
    version_field = {
        "asset_master": "asset_master_version_id",
        "ticker_alias": "alias_resolution_version_id",
        "issuer_master": "issuer_master_version_id",
    }[table_name]
    rows: list[dict[str, object]] = []
    for state in terminal:
        matches = [
            row
            for row in by_artifact[state.index_artifact]
            if row[version_field] == state.row_version_id
        ]
        if len(matches) != 1:
            raise I5ShadowRuntimeError("terminal row version does not resolve uniquely")
        row = matches[0]
        if stable_digest(_json_value(row)) != state.row_payload_digest:
            raise I5ShadowRuntimeError("terminal row payload digest differs")
        if table_name == "ticker_alias" and row["alias_is_tombstone"]:
            continue
        rows.append(row)
    return tuple(rows)


def _full_oracle_side(
    root: Path,
    *,
    completion_pin: ArtifactPin,
    comparison_sessions: tuple[date, ...],
    receipt_available_session: date | None = None,
    meter: _ReadMeter,
    disk_monitor: _DiskFloorMonitor | None = None,
) -> tuple[_ResolvedSide, object]:
    """Replay a completed legacy Full candidate through its exact immutable pins."""

    try:
        content = meter.read_pin(completion_pin, label="Full oracle completion")
    except I5ShadowRuntimeError as exc:
        raise I5FullOracleSeamError(
            "P0 full-oracle seam: exact completed Full candidate is unavailable"
        ) from exc
    completion = _closed_json(content, label="Full oracle completion")
    required = {
        "approval_id",
        "artifact_type",
        "candidate_id",
        "candidate_manifest",
        "capabilities",
        "complete",
        "completed_at_utc",
        "completion_id",
        "completion_state",
        "completion_version",
        "plan_id",
        "raw_collision_rows",
        "source_binding_id",
        "source_row_count",
        "table_row_counts",
    }
    _keys(completion, required, "Full oracle completion")
    body = dict(completion)
    completion_id = _digest(body.pop("completion_id"), "Full oracle completion ID")
    if (
        stable_digest(body) != completion_id
        or completion["artifact_type"] != "s7_streaming_four_table_full_execution_completion"
        or completion["completion_state"] != full_runtime.STREAMING_STATE
        or completion["completion_version"] != full_runtime.STREAMING_COMPLETION_VERSION
        or completion["complete"] is not True
        or completion["capabilities"] != dict(full_runtime._FALSE_CAPABILITIES)
    ):
        raise I5FullOracleSeamError("P0 full-oracle seam: completion semantics differ")
    plan_id = _digest(completion["plan_id"], "Full oracle plan ID")
    approval_id = _digest(completion["approval_id"], "Full oracle approval ID")
    expected_completion = full_runtime._completion_path(plan_id, approval_id)
    if completion_pin.path != expected_completion:
        raise I5FullOracleSeamError("P0 full-oracle seam: completion path is not exact")
    try:
        controls = full_runtime._load_execution_controls(
            root, plan_id=plan_id, approval_id=approval_id
        )
    except Exception as exc:
        raise I5FullOracleSeamError(
            "P0 full-oracle seam: Full control chain cannot be replayed"
        ) from exc
    binding = controls["binding"]
    plan = controls["plan"]
    approval = controls["approval"]
    candidate_id = _digest(completion["candidate_id"], "Full oracle candidate ID")
    if (
        approval_id != approval.approval_id
        or completion["source_binding_id"] != binding.source_binding_id
        or binding.cutoff_session != I5_TARGET_SESSION
        or plan["source_binding_id"] != binding.source_binding_id
        or approval.plan_id != plan_id
    ):
        raise I5FullOracleSeamError("P0 full-oracle seam: exact scope or authority differs")
    expected_candidate_id = stable_digest(
        {
            "adapter_version": full_runtime.PRODUCTION_ADAPTER_VERSION,
            "approval_id": approval_id,
            "engine_version": full_runtime.STREAMING_POLICY_VERSION,
            "plan_id": plan_id,
            "source_binding_id": binding.source_binding_id,
        }
    )
    if candidate_id != expected_candidate_id:
        raise I5FullOracleSeamError("P0 full-oracle seam: candidate identity differs")
    try:
        completed_at = full_runtime._utc_from_text(
            completion["completed_at_utc"], "Full oracle completion time"
        )
    except Exception as exc:
        raise I5FullOracleSeamError("P0 full-oracle seam: completion time is invalid") from exc
    if completed_at < approval.approved_at_utc:
        raise I5FullOracleSeamError("P0 full-oracle seam: completion predates approval")
    try:
        completion_availability = full_runtime._calendar_availability(
            root,
            calendar_artifact_id=binding.calendar_artifact_id,
            calendar_artifact_sha256=binding.calendar_artifact_sha256,
            recorded_at=completed_at,
        )
        completion_available_session = date.fromisoformat(
            _text(
                completion_availability["source_available_session"],
                "Full oracle completion availability",
            )
        )
    except Exception as exc:
        raise I5FullOracleSeamError(
            "P0 full-oracle seam: completion availability cannot be reproduced"
        ) from exc
    if (
        receipt_available_session is not None
        and completion_available_session > receipt_available_session
    ):
        raise I5FullOracleSeamError(
            "P0 full-oracle seam: shadow availability predates Full completion"
        )
    try:
        caps = full_runtime.StreamingResourceCaps.from_dict(plan["resource_caps"])
        sqlite_reserve = _official_full_sqlite_reserve_bytes(caps)
        if disk_monitor is not None:
            disk_monitor.sample(
                "before_official_full_replay",
                reserve_bytes=sqlite_reserve,
            )
        official = full_runtime._verify_completion_and_candidate(
            root,
            safe_relative_path(root, expected_completion),
            plan=plan,
            approval=approval,
            binding=binding,
            expected_candidate_id=expected_candidate_id,
            caps=caps,
            idempotent=True,
        )
    except Exception as exc:
        raise I5FullOracleSeamError(
            "P0 full-oracle seam: official Full tree replay failed"
        ) from exc
    finally:
        if disk_monitor is not None:
            disk_monitor.sample("after_official_full_replay")
    if (
        official.candidate_id != candidate_id
        or official.completion_id != completion_id
        or official.plan_id != plan_id
        or official.approval_id != approval_id
        or official.source_row_count != binding.row_count
        or official.session_count != binding.session_count
    ):
        raise I5FullOracleSeamError("P0 full-oracle seam: official Full replay summary differs")
    candidate_pin = _artifact_from_mapping(
        completion["candidate_manifest"], "Full oracle candidate manifest"
    )
    expected_candidate_path = f"{full_runtime._candidate_path(candidate_id)}/manifest.json"
    if candidate_pin.path != expected_candidate_path:
        raise I5FullOracleSeamError("P0 full-oracle seam: candidate path differs")
    candidate_content = meter.read_pin(candidate_pin, label="Full oracle candidate manifest")
    candidate = _closed_json(candidate_content, label="Full oracle candidate manifest")
    candidate_required = {
        "adapter_version",
        "approval_id",
        "artifact_type",
        "candidate_id",
        "candidate_version",
        "capabilities",
        "contract_pins",
        "intent",
        "manifest_id",
        "outputs",
        "plan_id",
        "policy_version",
        "source_binding_id",
        "state",
        "table_row_counts",
    }
    _keys(candidate, candidate_required, "Full oracle candidate manifest")
    candidate_body = dict(candidate)
    manifest_id = _digest(candidate_body.pop("manifest_id"), "Full oracle manifest ID")
    if (
        stable_digest(candidate_body) != manifest_id
        or candidate["candidate_id"] != candidate_id
        or candidate_id != expected_candidate_id
        or candidate["approval_id"] != approval_id
        or candidate["plan_id"] != plan_id
        or candidate["source_binding_id"] != binding.source_binding_id
        or candidate["artifact_type"] != "s7_streaming_four_table_full_candidate"
        or candidate["candidate_version"] != full_runtime.STREAMING_CANDIDATE_VERSION
        or candidate["adapter_version"] != full_runtime.PRODUCTION_ADAPTER_VERSION
        or candidate["policy_version"] != full_runtime.STREAMING_POLICY_VERSION
        or candidate["state"] != full_runtime.STREAMING_STATE
        or candidate["capabilities"] != dict(full_runtime._FALSE_CAPABILITIES)
        or candidate["contract_pins"] != full_runtime._contract_pins()
    ):
        raise I5FullOracleSeamError("P0 full-oracle seam: candidate semantics differ")
    intent_pin = _artifact_from_mapping(candidate["intent"], "Full oracle run intent")
    expected_intent_path = (
        "manifests/silver/identity/s7-streaming-full-run-intents/"
        f"plan_id={plan_id}/approval_id={approval_id}/manifest.json"
    )
    if intent_pin.path != expected_intent_path:
        raise I5FullOracleSeamError("P0 full-oracle seam: run-intent path differs")
    intent_content = meter.read_pin(intent_pin, label="Full oracle run intent")
    intent = _closed_json(intent_content, label="Full oracle run intent")
    _keys(
        intent,
        {
            "approval_id",
            "artifact_type",
            "candidate_id",
            "capabilities",
            "captured_at_utc",
            "intent_id",
            "intent_version",
            "plan_id",
            "source_binding_id",
            "state",
        },
        "Full oracle run intent",
    )
    intent_body = dict(intent)
    intent_id = _digest(intent_body.pop("intent_id"), "Full oracle run-intent ID")
    try:
        intent_captured_at = full_runtime._utc_from_text(
            intent["captured_at_utc"], "Full oracle run-intent time"
        )
    except Exception as exc:
        raise I5FullOracleSeamError("P0 full-oracle seam: run-intent time is invalid") from exc
    if (
        intent_id != stable_digest(intent_body)
        or intent["artifact_type"] != "s7_streaming_four_table_full_run_intent"
        or intent["approval_id"] != approval_id
        or intent["candidate_id"] != candidate_id
        or intent["capabilities"] != dict(full_runtime._FALSE_CAPABILITIES)
        or intent["intent_version"] != full_runtime.STREAMING_INTENT_VERSION
        or intent["plan_id"] != plan_id
        or intent["source_binding_id"] != binding.source_binding_id
        or intent["state"] != "authorized_awaiting_execution"
        or intent_captured_at < approval.approved_at_utc
        or completed_at < intent_captured_at
    ):
        raise I5FullOracleSeamError("P0 full-oracle seam: run-intent semantics differ")
    rows, physical, output_bytes, qa = _read_oracle_outputs(
        root,
        candidate_id=candidate_id,
        candidate=candidate,
        comparison_sessions=comparison_sessions,
        expected_universe_sessions=tuple(
            item.session_date for item in binding.membership_artifacts
        ),
        meter=meter,
    )
    if disk_monitor is not None:
        disk_monitor.sample("after_full_projection_replay")
    table_counts = _int_mapping(candidate["table_row_counts"], "oracle table row counts")
    completion_counts = _int_mapping(completion["table_row_counts"], "oracle completion row counts")
    qa_counts = _int_mapping(qa["table_row_counts"], "oracle QA table row counts")
    completion_source_rows = _nonnegative_int(
        completion["source_row_count"], "oracle completion source rows"
    )
    completion_collision_rows = _nonnegative_int(
        completion["raw_collision_rows"], "oracle completion collision rows"
    )
    qa_source_rows = _nonnegative_int(qa["source_membership_rows"], "oracle QA source rows")
    qa_collision_rows = _nonnegative_int(
        qa["multi_registry_composite_override_collision_rows"],
        "oracle QA collision rows",
    )
    qa_alias_rows = _nonnegative_int(qa["ticker_alias_rows"], "oracle QA ticker-alias rows")
    qa_sessions = _positive_int(qa["session_count"], "oracle QA session count")
    if (
        set(table_counts) != set(I5_TABLE_ORDER)
        or table_counts != completion_counts
        or qa_counts != table_counts
        or qa_source_rows != completion_source_rows
        or qa_source_rows != binding.row_count
        or qa_source_rows != table_counts["universe_daily"]
        or qa_collision_rows != completion_collision_rows
        or qa_alias_rows != table_counts["ticker_alias"]
        or qa_sessions != binding.session_count
        or qa_sessions != len(binding.membership_artifacts)
    ):
        raise I5FullOracleSeamError("P0 full-oracle seam: table counts differ")
    if output_bytes <= 0:
        raise I5FullOracleSeamError("P0 full-oracle seam: no output bytes were replayed")
    lineage = _source_lineage_digest(rows["universe_daily"])
    registry_ids = {item.registry_name: item.release_id for item in binding.registry_pins}
    policy_digest = stable_digest(
        {
            "registry_release_ids": registry_ids,
            "rule_version": "s7_5_i5_full_oracle_registry_bundle_projection_v1",
        }
    )
    side = _ResolvedSide(
        release_id=candidate_id,
        rows=MappingProxyType(rows),
        physical=physical,
        source_binding_digest=lineage,
        schema_bundle_digest=_canonical_schema_bundle_digest(),
        transform_semantics_digest=stable_digest(
            {
                "adapter_version": candidate["adapter_version"],
                "policy_version": candidate["policy_version"],
                "runtime_binding": plan["runtime_binding"],
                "rule_version": "s7_5_i5_full_oracle_transform_binding_v1",
            }
        ),
        identity_policy_bundle_id=policy_digest,
        calendar_digest=binding.calendar_artifact_id,
        checkpoint_id=stable_digest(
            {
                "candidate_id": candidate_id,
                "rule_version": "s7_5_i5_full_oracle_snapshot_checkpoint_v1",
            }
        ),
        run_receipt_id=completion_id,
        manifest_sha256=candidate_pin.sha256,
        replayed_bytes_floor=(
            completion_pin.bytes
            + candidate_pin.bytes
            + intent_pin.bytes
            + _full_declared_output_bytes(candidate["outputs"])
        ),
    )
    return side, binding


def _official_full_sqlite_reserve_bytes(
    caps: full_runtime.StreamingResourceCaps,
) -> int:
    """Reserve the Full plan's authenticated temporary-byte upper bound.

    The official verifier creates a transient SQLite alias index and removes it
    before returning, so endpoint free-space samples cannot observe its peak.
    The exact Full plan's ``tmp_bytes_cap`` is the only safe upper bound at this
    boundary; using a caller estimate would permit systematic under-reporting.
    """

    reserve = caps.tmp_bytes_cap
    if type(reserve) is not int or reserve <= 0:
        raise I5ProductionReadMeterSeamError(
            "P0 shadow resource meter: Full SQLite temporary reserve is unavailable"
        )
    return reserve


def _full_declared_output_bytes(value: object) -> int:
    outputs = _mapping(value, "Full oracle outputs")
    _keys(outputs, {*I5_TABLE_ORDER, "qa"}, "Full oracle outputs")
    receipts = [
        _mapping(outputs[table], f"Full oracle {table} output")
        for table in ("asset_master", "ticker_alias", "issuer_master", "qa")
    ]
    receipts.extend(
        _mapping(item, "Full oracle universe output")
        for item in _sequence(outputs["universe_daily"], "Full oracle universe outputs")
    )
    return sum(
        _nonnegative_int(receipt.get("bytes"), "Full oracle declared output bytes")
        for receipt in receipts
    )


def _read_oracle_outputs(
    root: Path,
    *,
    candidate_id: str,
    candidate: Mapping[str, object],
    comparison_sessions: tuple[date, ...],
    expected_universe_sessions: tuple[date, ...],
    meter: _ReadMeter,
) -> tuple[
    dict[str, Mapping[str, tuple[dict[str, object], ...]]],
    Mapping[str, tuple[dict[str, object], ...]],
    int,
    dict[str, object],
]:
    outputs = _mapping(candidate["outputs"], "Full oracle outputs")
    _keys(outputs, {*I5_TABLE_ORDER, "qa"}, "Full oracle outputs")
    candidate_root = full_runtime._candidate_path(candidate_id)
    table_counts = _int_mapping(candidate["table_row_counts"], "Full oracle table counts")
    rows: dict[str, Mapping[str, tuple[dict[str, object], ...]]] = {}
    physical: dict[str, tuple[dict[str, object], ...]] = {}
    total_bytes = 0
    for table_name in ("asset_master", "ticker_alias", "issuer_master"):
        receipt = _oracle_output_receipt(
            outputs[table_name],
            candidate_root=candidate_root,
            label=f"Full oracle {table_name}",
            expected_schema_digest=S7_DERIVED_CONTRACTS[table_name].schema_digest,
        )
        if receipt["artifact"].path != f"{candidate_root}/data/{table_name}.parquet":
            raise I5FullOracleSeamError(f"P0 full-oracle seam: {table_name} output path differs")
        table = _read_parquet_exact(
            root,
            artifact=receipt["artifact"],
            table_name=table_name,
            expected_schema=S7_DERIVED_CONTRACTS[table_name].arrow_schema,
            expected_rows=receipt["row_count"],
            meter=meter,
        )
        if receipt["row_count"] != table_counts[table_name]:
            raise I5FullOracleSeamError("P0 full-oracle seam: small-table count differs")
        table_rows = tuple(dict(item) for item in table.to_pylist())
        rows[table_name] = MappingProxyType(
            {"__table__": _sorted_canonical_rows(table_name, table_rows)}
        )
        physical[table_name] = (receipt["physical"],)
        total_bytes += receipt["artifact"].bytes
    universe_receipts = _sequence(outputs["universe_daily"], "Full oracle universe outputs")
    by_session: dict[date, dict[str, object]] = {}
    observed_rows = 0
    for raw in universe_receipts:
        receipt = _oracle_output_receipt(
            raw,
            candidate_root=candidate_root,
            label="Full oracle universe partition",
            expected_schema_digest=S7_DERIVED_CONTRACTS["universe_daily"].schema_digest,
        )
        match = _SESSION_PARTITION.search(receipt["artifact"].path)
        if match is None:
            raise I5FullOracleSeamError(
                "P0 full-oracle seam: universe receipt has no session partition"
            )
        session = date.fromisoformat(match.group(1))
        expected_path = (
            f"{candidate_root}/data/universe_daily/"
            f"session_date={session.isoformat()}/part-00000.parquet"
        )
        if receipt["artifact"].path != expected_path:
            raise I5FullOracleSeamError("P0 full-oracle seam: universe partition path differs")
        if session in by_session:
            raise I5FullOracleSeamError("P0 full-oracle seam: universe session receipt repeats")
        by_session[session] = receipt
        observed_rows += receipt["row_count"]
    if observed_rows != table_counts["universe_daily"]:
        raise I5FullOracleSeamError("P0 full-oracle seam: universe row total differs")
    if tuple(by_session) != expected_universe_sessions:
        raise I5FullOracleSeamError(
            "P0 full-oracle seam: universe partition sessions differ from source binding"
        )
    selected: dict[str, tuple[dict[str, object], ...]] = {}
    selected_physical: list[dict[str, object]] = []
    for session in comparison_sessions:
        receipt = by_session.get(session)
        if receipt is None:
            raise I5FullOracleSeamError(
                "P0 full-oracle seam: configured comparison session is absent"
            )
        table = _read_parquet_exact(
            root,
            artifact=receipt["artifact"],
            table_name="universe_daily",
            expected_schema=S7_DERIVED_CONTRACTS["universe_daily"].arrow_schema,
            expected_rows=receipt["row_count"],
            meter=meter,
        )
        table_rows = tuple(dict(item) for item in table.to_pylist())
        if any(item["session_date"] != session for item in table_rows):
            raise I5FullOracleSeamError("P0 full-oracle seam: universe partition crosses sessions")
        selected[session.isoformat()] = _sorted_canonical_rows("universe_daily", table_rows)
        selected_physical.append(receipt["physical"])
        total_bytes += receipt["artifact"].bytes
    rows["universe_daily"] = MappingProxyType(selected)
    physical["universe_daily"] = tuple(selected_physical)

    qa = _oracle_output_receipt(
        outputs["qa"],
        candidate_root=candidate_root,
        label="Full oracle QA",
        expected_schema_digest=None,
        expect_row_count=False,
    )
    if qa["artifact"].path != f"{candidate_root}/qa/qa.json":
        raise I5FullOracleSeamError("P0 full-oracle seam: QA output path differs")
    qa_content = meter.read_pin(qa["artifact"], label="Full oracle QA")
    qa_body = _closed_json(qa_content, label="Full oracle QA")
    required_qa = {
        "artifact_type",
        "bounded_collision_examples",
        "bounded_share_class_conflict_examples",
        "critical_failure_count",
        "gate_b_relation_share_class_conflict_rows",
        "gate_b_relation_share_class_mismatch_rows",
        "identity_quality_forced_liquidation_rows",
        "inactive_or_delisted_inferred_from_identity_quality_rows",
        "missing_eligible_alias_rows",
        "multi_registry_composite_override_collision_alias_rows",
        "multi_registry_composite_override_collision_eligible_rows",
        "multi_registry_composite_override_collision_resolved_rows",
        "multi_registry_composite_override_collision_rows",
        "publish_authorized",
        "reference_inventory_unattempted_rows",
        "session_count",
        "share_class_correction_before_unique_composite_rows",
        "source_membership_omission_or_duplication_rows",
        "source_membership_rows",
        "source_membership_streaming_lineage_digest",
        "state",
        "table_row_counts",
        "ticker_alias_rows",
        "transition_automatic_return_stitching_rows",
        "unapproved_canonical_override_rows",
        "unadjudicated_gate_b_share_class_conflict_eligible_rows",
        "unadjudicated_gate_b_share_class_conflict_rows",
        "unknown_or_unapproved_foreign_identity_eligible_rows",
        "unresolved_rows",
    }
    _keys(qa_body, required_qa, "Full oracle QA")
    zero_safety_fields = (
        "critical_failure_count",
        "identity_quality_forced_liquidation_rows",
        "inactive_or_delisted_inferred_from_identity_quality_rows",
        "missing_eligible_alias_rows",
        "multi_registry_composite_override_collision_alias_rows",
        "multi_registry_composite_override_collision_eligible_rows",
        "multi_registry_composite_override_collision_resolved_rows",
        "reference_inventory_unattempted_rows",
        "share_class_correction_before_unique_composite_rows",
        "source_membership_omission_or_duplication_rows",
        "transition_automatic_return_stitching_rows",
        "unapproved_canonical_override_rows",
        "unadjudicated_gate_b_share_class_conflict_eligible_rows",
        "unknown_or_unapproved_foreign_identity_eligible_rows",
    )
    unsafe_qa = any(
        _nonnegative_int(qa_body[field], f"Full oracle QA {field}") != 0
        for field in zero_safety_fields
    )
    if (
        qa_body["artifact_type"] != "s7_streaming_four_table_full_qa"
        or qa_body["state"] != full_runtime.STREAMING_STATE
        or qa_body["publish_authorized"] is not False
        or unsafe_qa
    ):
        raise I5FullOracleSeamError("P0 full-oracle seam: critical QA is nonzero")
    _digest(
        qa_body["source_membership_streaming_lineage_digest"],
        "Full oracle source lineage digest",
    )
    total_bytes += qa["artifact"].bytes
    return rows, MappingProxyType(physical), total_bytes, qa_body


def _oracle_output_receipt(
    value: object,
    *,
    candidate_root: str,
    label: str,
    expected_schema_digest: str | None,
    expect_row_count: bool = True,
) -> dict[str, object]:
    item = _mapping(value, label)
    required = {"bytes", "path", "sha256"}
    if not required.issubset(item) or set(item) - {
        *required,
        "row_count",
        "schema_digest",
    }:
        raise I5FullOracleSeamError(f"P0 full-oracle seam: {label} fields differ")
    relative = _relative(item["path"], f"{label} path")
    path = f"{candidate_root}/{relative}"
    artifact = ArtifactPin(
        path=path,
        sha256=_digest(item["sha256"], f"{label} SHA-256"),
        bytes=_nonnegative_int(item["bytes"], f"{label} bytes"),
    )
    row_count = (
        0 if "row_count" not in item else _nonnegative_int(item["row_count"], f"{label} row count")
    )
    schema_digest = item.get("schema_digest")
    if expect_row_count != ("row_count" in item):
        raise I5FullOracleSeamError(f"P0 full-oracle seam: {label} row-count fields differ")
    if schema_digest != expected_schema_digest:
        raise I5FullOracleSeamError(f"P0 full-oracle seam: {label} schema digest differs")
    if schema_digest is not None:
        _digest(schema_digest, f"{label} schema digest")
    return {
        "artifact": artifact,
        "physical": {
            "artifact": artifact.to_dict(),
            "row_count": row_count,
            "schema_digest": schema_digest,
        },
        "row_count": row_count,
        "schema_digest": schema_digest,
    }


def _reconcile_producer_authorities(
    loaded: LoadedI3ProductionStaging,
    oracle_binding: object,
    incremental: _ResolvedSide,
    oracle: _ResolvedSide,
) -> None:
    if incremental.source_binding_digest != oracle.source_binding_digest:
        raise I5FullOracleSeamError(
            "P0 full-oracle seam: selected source lineage differs between producers"
        )
    if incremental.schema_bundle_digest != oracle.schema_bundle_digest:
        raise I5FullOracleSeamError("P0 full-oracle seam: canonical schemas differ")
    if incremental.calendar_digest != oracle.calendar_digest:
        raise I5FullOracleSeamError("P0 full-oracle seam: calendar authority differs")
    i3_registry_ids = {
        item.registry_kind.value: item.release_id
        for item in loaded.run_spec.identity_policy_bundle.registry_releases
    }
    oracle_registry_ids = {
        item.registry_name: item.release_id for item in oracle_binding.registry_pins
    }
    if i3_registry_ids != oracle_registry_ids:
        raise I5FullOracleSeamError("P0 full-oracle seam: identity registry releases differ")


def _build_run_spec(
    config: ShadowRunConfig,
    inputs: _ProductionInputs,
    *,
    authority: str,
) -> ShadowRunSpec:
    if inputs.loaded_parent is None:
        raise I5ShadowRuntimeError("production RunSpec builder lacks an exact parent")
    return ShadowRunSpec(
        authority=authority,
        incremental_completion_artifact=config.incremental_completion_artifact,
        incremental_deep_attestation_artifact=(config.incremental_deep_attestation_artifact),
        full_oracle_completion_artifact=config.full_oracle_completion_artifact,
        incremental_release_id=inputs.incremental.release_id,
        full_oracle_release_id=inputs.oracle.release_id,
        common_parent_release_id=inputs.loaded_parent.gate_a_manifest.release_id,
        source_binding_digest=inputs.incremental.source_binding_digest,
        schema_bundle_digest=inputs.incremental.schema_bundle_digest,
        transform_semantics_digest=stable_digest(
            {
                "full_oracle_transform": inputs.oracle.transform_semantics_digest,
                "incremental_transform": inputs.incremental.transform_semantics_digest,
                "rule_version": "s7_5_i5_exact_transform_pair_binding_v1",
            }
        ),
        identity_policy_bundle_id=inputs.incremental.identity_policy_bundle_id,
        calendar_digest=inputs.incremental.calendar_digest,
        scope_artifact=I5_SCOPE_ARTIFACT,
        comparison_sessions=config.comparison_sessions,
        receipt_available_session=config.receipt_available_session,
        resource_policy=config.resource_policy,
    )


def _canonical_v1_rows(
    table_name: str,
    rows: Sequence[Mapping[str, object]],
    alias_reverse: Mapping[str, tuple[str, str]],
) -> tuple[dict[str, object], ...]:
    fields = S7_DERIVED_CONTRACTS[table_name].arrow_schema.names
    projected: list[dict[str, object]] = []
    if table_name in {"asset_master", "issuer_master"}:
        for row in rows:
            projected.append({field: row[field] for field in fields})
    elif table_name == "ticker_alias":
        for row in rows:
            segment_id = str(row["alias_segment_id"])
            legacy = alias_reverse.get(segment_id)
            if legacy is None:
                raise I5ShadowRuntimeError("canonical alias projection lacks a reverse ID")
            body = {
                field: row[field]
                for field in fields
                if field not in {"ticker_alias_id", "ticker_alias_id_rule_version"}
            }
            body["ticker_alias_id"] = legacy[0]
            body["ticker_alias_id_rule_version"] = legacy[1]
            projected.append(body)
    elif table_name == "universe_daily":
        for row in rows:
            body = {field: row[field] for field in fields if field != "ticker_alias_id"}
            segment_id = row["alias_segment_id"]
            if segment_id is None:
                body["ticker_alias_id"] = None
            else:
                legacy = alias_reverse.get(str(segment_id))
                if legacy is None:
                    raise I5ShadowRuntimeError(
                        "canonical universe projection lacks an alias reverse ID"
                    )
                body["ticker_alias_id"] = legacy[0]
            projected.append(body)
    else:  # pragma: no cover - closed table order
        raise I5ShadowRuntimeError("canonical projection table is invalid")
    return _sorted_canonical_rows(table_name, projected)


def _legacy_alias_reverse(
    alias_rows: Sequence[Mapping[str, object]],
) -> Mapping[str, tuple[str, str]]:
    result: dict[str, tuple[str, str]] = {}
    for row in alias_rows:
        segment_id = _digest(row["alias_segment_id"], "alias segment ID")
        payload = {
            "asset_id": row["asset_id"],
            "canonical_composite_figi": row["canonical_composite_figi"],
            "canonical_share_class_figi": row["canonical_share_class_figi"],
            "identity_adjudication_id": row["identity_adjudication_id"],
            "identity_case_id": row["identity_case_id"],
            "identity_resolution_cutoff_session": row["identity_resolution_cutoff_session"],
            "namespace": "ame_stocks.identity.ticker_alias",
            "observed_composite_figi": row["observed_composite_figi"],
            "observed_share_class_figi": row["observed_share_class_figi"],
            "provider_composite_override_id": row["provider_composite_override_id"],
            "rule_version": full_runtime.TICKER_ALIAS_ID_RULE_VERSION,
            "share_class_adjudication_id": row["share_class_adjudication_id"],
            "ticker": row["ticker"],
            "valid_from_session": row["valid_from_session"],
        }
        legacy_id = stable_digest(_json_value(payload))
        prior = result.get(segment_id)
        value = (legacy_id, full_runtime.TICKER_ALIAS_ID_RULE_VERSION)
        if prior is not None and prior != value:
            raise I5ShadowRuntimeError("one alias segment maps to multiple legacy IDs")
        result[segment_id] = value
    return MappingProxyType(result)


def _sorted_canonical_rows(
    table_name: str, rows: Sequence[Mapping[str, object]]
) -> tuple[dict[str, object], ...]:
    fields = tuple(S7_DERIVED_CONTRACTS[table_name].arrow_schema.names)
    keys = tuple(S7_DERIVED_CONTRACTS[table_name].primary_key)
    normalized = []
    observed: set[tuple[object, ...]] = set()
    for source in rows:
        if set(source) != set(fields):
            raise I5ShadowRuntimeError(f"{table_name} canonical row fields differ")
        row = {field: _json_value(source[field]) for field in fields}
        key = tuple(row[field] for field in keys)
        if key in observed:
            raise I5ShadowRuntimeError(f"{table_name} canonical key repeats")
        observed.add(key)
        normalized.append(row)
    normalized.sort(key=lambda row: tuple(str(row[field]) for field in keys))
    return tuple(normalized)


def _source_lineage_digest(
    partitions: Mapping[str, tuple[dict[str, object], ...]],
) -> str:
    rows = []
    for partition_key in sorted(partitions):
        for row in partitions[partition_key]:
            rows.append(
                {
                    "observed_cik_normalized": row["observed_cik_normalized"],
                    "observed_composite_figi": row["observed_composite_figi"],
                    "observed_share_class_figi": row["observed_share_class_figi"],
                    "selected_source_record_id": row["selected_source_record_id"],
                    "session_date": row["session_date"],
                    "source_selection_status": row["source_selection_status"],
                    "source_version_count": row["source_version_count"],
                    "ticker": row["ticker"],
                }
            )
    if not rows:
        raise I5ShadowRuntimeError("source-lineage projection is empty")
    return stable_digest({"rows": rows, "rule_version": "s7_5_i5_selected_source_lineage_v1"})


def _canonical_schema_bundle_digest() -> str:
    return stable_digest(
        {
            "schemas": {
                table: S7_DERIVED_CONTRACTS[table].schema_digest for table in I5_TABLE_ORDER
            },
            "table_order": list(I5_TABLE_ORDER),
            "rule_version": "s7_5_i5_canonical_schema_bundle_v1",
        }
    )


def _incremental_physical_projection(
    loaded: LoadedI3ProductionStaging,
) -> Mapping[str, tuple[dict[str, object], ...]]:
    output_set = loaded.receipt.output_set
    if output_set is None:  # pragma: no cover
        raise I5ShadowRuntimeError("incremental OutputSet is missing")
    projection: dict[str, tuple[dict[str, object], ...]] = {}
    for output in output_set.table_outputs:
        if output.rowset_index is not None:
            projection[output.table_name] = tuple(
                {
                    "artifact": item.artifact.to_dict(),
                    "partition_key": item.segment_id,
                    "row_count": item.row_count,
                    "schema_digest": item.schema_digest,
                }
                for item in output.rowset_index.segments
            )
        elif output.dataset_index is not None:
            projection[output.table_name] = tuple(
                {
                    "artifact": item.artifact.to_dict(),
                    "partition_key": item.session_date.isoformat(),
                    "row_count": item.row_count,
                    "schema_digest": item.schema_digest,
                }
                for item in output.dataset_index.partitions
            )
        else:  # pragma: no cover - production contract invariant
            raise I5ShadowRuntimeError("incremental output lacks an exact physical index")
    return MappingProxyType(projection)


def _execute_loaded_shadow(
    root: Path,
    *,
    spec: ShadowRunSpec,
    inputs: _ProductionInputs,
    meter: _ReadMeter,
    started: float,
    minimum_disk: int,
    production: bool,
    io_audit: _ProcessIOAudit | None = None,
    disk_monitor: _DiskFloorMonitor | None = None,
) -> ShadowRunResult:
    expected_authority = I5_PRODUCTION_AUTHORITY if production else I5_FIXTURE_AUTHORITY
    if spec.authority != expected_authority:
        raise I5ShadowRuntimeError("shadow execution authority differs")
    spec_relative = _run_spec_path(spec.run_spec_id, production=production)
    completion_relative = _completion_path(spec.run_spec_id, production=production)
    lock_path = safe_relative_path(root, _lock_path(spec.run_spec_id, production=production))
    if production and (io_audit is None or disk_monitor is None):
        raise I5ProductionReadMeterSeamError(
            "P0 shadow resource meter: production run lacks whole-interval auditing"
        )
    if disk_monitor is not None:
        minimum_disk = min(minimum_disk, disk_monitor.sample("before_shadow_lock"))
    with _exclusive_nonblocking_lock(lock_path):
        completion_path = safe_relative_path(root, completion_relative)
        if completion_path.exists() or completion_path.is_symlink():
            completion = load_i5_shadow_completion_exact(
                root,
                ArtifactPin(
                    path=completion_relative,
                    sha256=_sha256_file(completion_path),
                    bytes=completion_path.stat().st_size,
                ),
                production=production,
            )
            if completion.run_spec_id != spec.run_spec_id:
                raise I5ShadowRuntimeError("existing shadow completion belongs to another spec")
            return ShadowRunResult(
                run_spec=spec,
                run_spec_artifact=completion.run_spec_artifact,
                completion=completion,
                completion_artifact=completion.exact_pin(path=completion_relative),
                idempotent=True,
            )
        run_spec_artifact = _write_immutable(
            root,
            spec_relative,
            spec.canonical_bytes(),
            label="shadow RunSpec",
        )
        comparisons, comparison_documents = _comparison_receipts(spec, inputs)
        written = run_spec_artifact.bytes
        for receipt, document in zip(comparisons, comparison_documents, strict=True):
            pin = _write_immutable(
                root,
                receipt.details_artifact.path,
                _canonical_json_bytes(document),
                label=f"{receipt.projection.value} details",
            )
            if pin != receipt.details_artifact:
                raise I5ShadowRuntimeError("comparison details exact pin differs")
            written += pin.bytes
        failure_receipts, failure_documents = _failure_recovery_receipts(
            root,
            spec=spec,
            inputs=inputs,
            meter=meter,
            completion_relative=completion_relative,
            lock_path=lock_path,
            production=production,
            io_audit=io_audit,
            disk_monitor=disk_monitor,
            started=started,
        )
        for receipt, document in zip(failure_receipts, failure_documents, strict=True):
            pin = _write_immutable(
                root,
                receipt.details_artifact.path,
                _canonical_json_bytes(document),
                label=f"{receipt.scenario.value} recovery details",
            )
            if pin != receipt.details_artifact:
                raise I5ShadowRuntimeError("failure details exact pin differs")
            written += pin.bytes
        idempotency = IdempotencyReceipt(
            first_run_receipt_id=inputs.incremental.run_receipt_id,
            second_run_receipt_id=inputs.incremental.run_receipt_id,
            first_checkpoint_id=inputs.incremental.checkpoint_id,
            second_checkpoint_id=inputs.incremental.checkpoint_id,
            first_release_id=inputs.incremental.release_id,
            second_release_id=inputs.incremental.release_id,
            first_manifest_sha256=inputs.incremental.manifest_sha256,
            second_manifest_sha256=inputs.incremental.manifest_sha256,
        )
        if io_audit is not None:
            io_audit.sample(meter)
        if disk_monitor is not None:
            minimum_disk = min(
                minimum_disk,
                disk_monitor.sample("before_provisional_receipt"),
                disk_monitor.minimum,
            )
        else:
            minimum_disk = min(minimum_disk, _disk_free(root))
        process_write_bytes = 0 if io_audit is None else io_audit.write_bytes
        observed_written = max(written, process_write_bytes)
        provisional_observation = ResourceObservation(
            wall_clock_seconds=max(1, math.ceil(time.monotonic() - started)),
            peak_rss_bytes=_peak_rss_bytes(),
            free_disk_bytes_at_floor=minimum_disk,
            read_bytes=meter.bytes,
            write_bytes=observed_written,
            chain_resolution_milliseconds=inputs.chain_resolution_milliseconds,
        )
        _enforce_resource_policy(
            spec.resource_policy,
            started=started,
            meter=meter,
            written_bytes=observed_written,
            chain_resolution_milliseconds=inputs.chain_resolution_milliseconds,
            free_disk_bytes=minimum_disk,
        )
        provisional_receipt = ShadowEquivalenceReceipt(
            spec_id=spec.lifecycle_spec.spec_id,
            incremental_release_id=spec.incremental_release_id,
            full_oracle_release_id=spec.full_oracle_release_id,
            source_binding_digest=spec.source_binding_digest,
            comparisons=comparisons,
            resource_observation=provisional_observation,
            failure_recovery=failure_receipts,
            idempotency=idempotency,
            receipt_available_session=spec.receipt_available_session,
        )
        validate_shadow_equivalence(
            spec.lifecycle_spec,
            provisional_receipt,
            availability_cutoff_session=spec.receipt_available_session,
            artifact_reader=meter.read_path,
        )
        if io_audit is not None:
            io_audit.sample(meter)
            process_write_bytes = io_audit.write_bytes
        if disk_monitor is not None:
            minimum_disk = min(
                minimum_disk,
                disk_monitor.sample("before_completion_freeze"),
                disk_monitor.minimum,
            )
        else:
            minimum_disk = min(minimum_disk, _disk_free(root))
        completion, completion_bytes, final_write_bytes = (
            _freeze_completion_with_resource_fixed_point(
                spec=spec,
                run_spec_artifact=run_spec_artifact,
                comparisons=comparisons,
                failure_receipts=failure_receipts,
                idempotency=idempotency,
                started=started,
                meter=meter,
                written_before_completion=written,
                process_write_bytes_before_completion=process_write_bytes,
                minimum_disk=minimum_disk,
                chain_resolution_milliseconds=inputs.chain_resolution_milliseconds,
            )
        )
        _enforce_resource_policy(
            spec.resource_policy,
            started=started,
            meter=meter,
            written_bytes=final_write_bytes,
            chain_resolution_milliseconds=inputs.chain_resolution_milliseconds,
            free_disk_bytes=minimum_disk,
        )
        if disk_monitor is not None:
            disk_monitor.sample(
                "before_completion_write",
                reserve_bytes=len(completion_bytes),
            )
        else:
            _require_disk_floor(
                max(0, _disk_free(root) - len(completion_bytes)),
                spec.resource_policy.min_free_disk_bytes,
            )
        completion_artifact = _write_immutable(
            root,
            completion_relative,
            completion_bytes,
            label="shadow completion",
        )
        expected_final_write_bytes = max(
            written + completion_artifact.bytes,
            process_write_bytes + completion_artifact.bytes,
        )
        if final_write_bytes != expected_final_write_bytes:
            raise I5ShadowRuntimeError("shadow final write-byte observation does not reproduce")
        loaded = load_i5_shadow_completion_exact(
            root,
            completion_artifact,
            production=production,
        )
        if loaded != completion:
            raise I5ShadowRuntimeError("stored shadow completion replay differs")
        return ShadowRunResult(
            run_spec=spec,
            run_spec_artifact=run_spec_artifact,
            completion=completion,
            completion_artifact=completion_artifact,
            idempotent=False,
        )


def _freeze_completion_with_resource_fixed_point(
    *,
    spec: ShadowRunSpec,
    run_spec_artifact: ArtifactPin,
    comparisons: tuple[ProjectionComparisonReceipt, ...],
    failure_receipts: tuple[FailureRecoveryReceipt, ...],
    idempotency: IdempotencyReceipt,
    started: float,
    meter: _ReadMeter,
    written_before_completion: int,
    process_write_bytes_before_completion: int,
    minimum_disk: int,
    chain_resolution_milliseconds: int,
) -> tuple[ShadowRunCompletion, bytes, int]:
    """Include the immutable completion itself in the observed write budget.

    The decimal write-byte value is embedded in the completion, so its byte
    length is solved as a small deterministic fixed point before the first
    completion byte is written.
    """

    final_write_bytes = max(
        written_before_completion,
        process_write_bytes_before_completion,
    )
    for _ in range(16):
        observation = ResourceObservation(
            wall_clock_seconds=max(1, math.ceil(time.monotonic() - started)),
            peak_rss_bytes=_peak_rss_bytes(),
            free_disk_bytes_at_floor=minimum_disk,
            read_bytes=meter.bytes,
            write_bytes=final_write_bytes,
            chain_resolution_milliseconds=chain_resolution_milliseconds,
        )
        receipt = ShadowEquivalenceReceipt(
            spec_id=spec.lifecycle_spec.spec_id,
            incremental_release_id=spec.incremental_release_id,
            full_oracle_release_id=spec.full_oracle_release_id,
            source_binding_digest=spec.source_binding_digest,
            comparisons=comparisons,
            resource_observation=observation,
            failure_recovery=failure_receipts,
            idempotency=idempotency,
            receipt_available_session=spec.receipt_available_session,
        )
        completion = ShadowRunCompletion(
            run_spec_id=spec.run_spec_id,
            run_spec_artifact=run_spec_artifact,
            receipt=receipt,
            receipt_available_session=spec.receipt_available_session,
            authority=spec.authority,
        )
        content = completion.canonical_bytes()
        next_write_bytes = max(
            written_before_completion + len(content),
            process_write_bytes_before_completion + len(content),
        )
        if next_write_bytes == final_write_bytes:
            return completion, content, final_write_bytes
        final_write_bytes = next_write_bytes
    raise I5ShadowRuntimeError("shadow completion write-byte fixed point did not converge")


def _comparison_receipts(
    spec: ShadowRunSpec,
    inputs: _ProductionInputs,
) -> tuple[
    tuple[ProjectionComparisonReceipt, ...],
    tuple[dict[str, object], ...],
]:
    canonical_document = _canonical_comparison_document(spec, inputs)
    canonical_pin = _document_pin(
        _comparison_details_path(
            spec.run_spec_id,
            EquivalenceProjection.CANONICAL_RESEARCH,
            production=spec.authority == I5_PRODUCTION_AUTHORITY,
        ),
        canonical_document,
    )
    canonical = ProjectionComparisonReceipt(
        projection=EquivalenceProjection.CANONICAL_RESEARCH,
        semantics_digest=_CANONICAL_SEMANTICS,
        compared_row_count=_nonnegative_int(
            canonical_document["compared_row_count"], "canonical compared rows"
        ),
        incremental_projection_digest=_digest(
            canonical_document["incremental_projection_digest"],
            "canonical incremental digest",
        ),
        oracle_projection_digest=_digest(
            canonical_document["oracle_projection_digest"],
            "canonical oracle digest",
        ),
        unexpected_difference_count=_nonnegative_int(
            canonical_document["unexpected_difference_count"],
            "canonical difference count",
        ),
        details_artifact=canonical_pin,
    )
    physical_document = _physical_comparison_document(spec, inputs)
    physical_pin = _document_pin(
        _comparison_details_path(
            spec.run_spec_id,
            EquivalenceProjection.PHYSICAL_REUSE,
            production=spec.authority == I5_PRODUCTION_AUTHORITY,
        ),
        physical_document,
    )
    physical = ProjectionComparisonReceipt(
        projection=EquivalenceProjection.PHYSICAL_REUSE,
        semantics_digest=_PHYSICAL_SEMANTICS,
        compared_row_count=_nonnegative_int(
            physical_document["compared_row_count"], "physical compared rows"
        ),
        incremental_projection_digest=_digest(
            physical_document["incremental_projection_digest"],
            "physical incremental digest",
        ),
        oracle_projection_digest=_digest(
            physical_document["oracle_projection_digest"],
            "physical oracle digest",
        ),
        unexpected_difference_count=_nonnegative_int(
            physical_document["unexpected_difference_count"],
            "physical difference count",
        ),
        details_artifact=physical_pin,
    )
    if (
        canonical.unexpected_difference_count
        or canonical.incremental_projection_digest != canonical.oracle_projection_digest
    ):
        raise I5ShadowRuntimeError("canonical shadow equivalence has unexpected differences")
    if (
        physical.unexpected_difference_count
        or physical.incremental_projection_digest != physical.oracle_projection_digest
    ):
        raise I5ShadowRuntimeError("physical clean-append reuse differs")
    return (canonical, physical), (canonical_document, physical_document)


def _canonical_comparison_document(
    spec: ShadowRunSpec,
    inputs: _ProductionInputs,
) -> dict[str, object]:
    tables = []
    all_incremental: list[dict[str, object]] = []
    all_oracle: list[dict[str, object]] = []
    total_rows = 0
    total_differences = 0
    examples: list[dict[str, object]] = []
    for table_name in I5_TABLE_ORDER:
        incremental_partitions = inputs.incremental.rows[table_name]
        oracle_partitions = inputs.oracle.rows[table_name]
        if tuple(incremental_partitions) != tuple(oracle_partitions):
            raise I5ShadowRuntimeError(f"{table_name} comparison partition set differs")
        partition_details = []
        for partition_key in incremental_partitions:
            incremental_rows = incremental_partitions[partition_key]
            oracle_rows = oracle_partitions[partition_key]
            comparison = _compare_partition_rows(
                table_name,
                partition_key,
                incremental_rows,
                oracle_rows,
            )
            total_rows += comparison["compared_row_count"]
            total_differences += comparison["unexpected_difference_count"]
            examples.extend(comparison["bounded_examples"][: 20 - len(examples)])
            partition_details.append(comparison)
            all_incremental.append(
                {
                    "digest": comparison["incremental_projection_digest"],
                    "partition_key": partition_key,
                    "table_name": table_name,
                }
            )
            all_oracle.append(
                {
                    "digest": comparison["oracle_projection_digest"],
                    "partition_key": partition_key,
                    "table_name": table_name,
                }
            )
        tables.append(
            {
                "partitions": partition_details,
                "schema_digest": S7_DERIVED_CONTRACTS[table_name].schema_digest,
                "table_name": table_name,
            }
        )
    incremental_digest = stable_digest(
        {
            "partitions": all_incremental,
            "rule_version": I5_CANONICAL_PROJECTION_RULE_VERSION,
        }
    )
    oracle_digest = stable_digest(
        {
            "partitions": all_oracle,
            "rule_version": I5_CANONICAL_PROJECTION_RULE_VERSION,
        }
    )
    document = {
        "artifact_type": "s7_5_i5_projection_comparison_details",
        "bounded_examples": examples,
        "compared_row_count": total_rows,
        "incremental_projection_digest": incremental_digest,
        "oracle_projection_digest": oracle_digest,
        "projection": EquivalenceProjection.CANONICAL_RESEARCH.value,
        "rule_version": I5_CANONICAL_PROJECTION_RULE_VERSION,
        "semantics_digest": _CANONICAL_SEMANTICS,
        "spec_id": spec.run_spec_id,
        "tables": tables,
        "unexpected_difference_count": total_differences,
    }
    return _with_id(document, "details_id")


def _compare_partition_rows(
    table_name: str,
    partition_key: str,
    incremental_rows: tuple[dict[str, object], ...],
    oracle_rows: tuple[dict[str, object], ...],
) -> dict[str, object]:
    key_fields = tuple(S7_DERIVED_CONTRACTS[table_name].primary_key)
    incremental = {tuple(row[field] for field in key_fields): row for row in incremental_rows}
    oracle = {tuple(row[field] for field in key_fields): row for row in oracle_rows}
    if len(incremental) != len(incremental_rows) or len(oracle) != len(oracle_rows):
        raise I5ShadowRuntimeError("comparison projection contains duplicate primary keys")
    differences = []
    for key in sorted(set(incremental) | set(oracle), key=str):
        left = incremental.get(key)
        right = oracle.get(key)
        if left != right:
            differing_fields = sorted(
                field
                for field in set(left or {}) | set(right or {})
                if (left or {}).get(field) != (right or {}).get(field)
            )
            differences.append(
                {
                    "differing_fields": differing_fields,
                    "key": list(key),
                    "left_present": left is not None,
                    "right_present": right is not None,
                }
            )
    return {
        "bounded_examples": differences[:20],
        "compared_row_count": len(set(incremental) | set(oracle)),
        "incremental_projection_digest": stable_digest(list(incremental_rows)),
        "oracle_projection_digest": stable_digest(list(oracle_rows)),
        "partition_key": partition_key,
        "unexpected_difference_count": len(differences),
    }


def _physical_comparison_document(
    spec: ShadowRunSpec,
    inputs: _ProductionInputs,
) -> dict[str, object]:
    parent = inputs.parent_physical
    child = inputs.incremental.physical
    tables = []
    expected_parts: list[dict[str, object]] = []
    actual_parts: list[dict[str, object]] = []
    differences = 0
    compared = 0
    for table_name in I5_TABLE_ORDER:
        prefix = parent[table_name]
        actual = child[table_name]
        suffix = actual[len(prefix) :]
        valid = (
            len(actual) == len(prefix) + 1 and actual[: len(prefix)] == prefix and len(suffix) == 1
        )
        if table_name == "universe_daily":
            valid = valid and suffix[0]["partition_key"] == I5_TARGET_SESSION.isoformat()
        if not valid:
            differences += 1
        expected = (*prefix, *suffix) if valid else prefix
        compared += len(set(item["partition_key"] for item in (*actual, *expected)))
        actual_record = {
            "partitions": list(actual),
            "table_name": table_name,
        }
        expected_record = {
            "partitions": list(expected),
            "table_name": table_name,
        }
        actual_parts.append(actual_record)
        expected_parts.append(expected_record)
        tables.append(
            {
                "append_count": len(suffix),
                "child_partitions": list(actual),
                "exact_parent_prefix_unchanged": actual[: len(prefix)] == prefix,
                "expected_partitions": list(expected),
                "independent_full_oracle_evidence": list(inputs.oracle.physical[table_name]),
                "parent_partition_count": len(prefix),
                "table_name": table_name,
                "unexpected_difference_count": 0 if valid else 1,
            }
        )
    incremental_digest = stable_digest(
        {
            "tables": actual_parts,
            "rule_version": I5_PHYSICAL_REUSE_PROJECTION_RULE_VERSION,
        }
    )
    oracle_digest = stable_digest(
        {
            "tables": expected_parts,
            "rule_version": I5_PHYSICAL_REUSE_PROJECTION_RULE_VERSION,
        }
    )
    document = {
        "artifact_type": "s7_5_i5_projection_comparison_details",
        "compared_row_count": compared,
        "incremental_projection_digest": incremental_digest,
        "oracle_projection_digest": oracle_digest,
        "projection": EquivalenceProjection.PHYSICAL_REUSE.value,
        "rule_version": I5_PHYSICAL_REUSE_PROJECTION_RULE_VERSION,
        "semantics_digest": _PHYSICAL_SEMANTICS,
        "spec_id": spec.run_spec_id,
        "tables": tables,
        "unexpected_difference_count": differences,
    }
    return _with_id(document, "details_id")


def _exercise_production_checkpoint_corruption(
    root: Path,
    *,
    loaded: LoadedI3ProductionStaging,
    completion_pin: ArtifactPin,
    deep_attestation_pin: ArtifactPin,
    meter: _ReadMeter,
) -> dict[str, object]:
    """Corrupt the exact child checkpoint at all three production read seams."""

    output_set = loaded.receipt.output_set
    if output_set is None:
        raise I5ShadowRuntimeError("production checkpoint exercise lacks deep child authority")
    deep = load_i3_production_deep_attestation_exact(
        deep_attestation_pin,
        meter.read_path,
    )
    checkpoint_pin = output_set.checkpoint_artifact
    if (
        loaded.completion.exact_pin(path=completion_pin.path) != completion_pin
        or loaded.completion.receipt_id != loaded.receipt.receipt_id
        or loaded.completion.output_set_id != output_set.output_set_id
        or loaded.completion.checkpoint_id != output_set.checkpoint_id
        or deep.completion_artifact != completion_pin
        or deep.output_set_id != output_set.output_set_id
        or deep.checkpoint_artifact != checkpoint_pin
        or deep.checkpoint_id != output_set.checkpoint_id
        or loaded.checkpoint.exact_pin(path=checkpoint_pin.path) != checkpoint_pin
    ):
        raise I5ShadowRuntimeError("production child checkpoint authority does not reconcile")
    checkpoint_bytes = meter.read_pin(
        checkpoint_pin,
        label="child checkpoint corruption exercise input",
    )
    corrupted = bytearray(checkpoint_bytes)
    if not corrupted:
        raise I5ShadowRuntimeError("production child checkpoint is empty")
    corrupted[0] ^= 1
    corrupted_bytes = bytes(corrupted)

    exact_reader_rejected = False
    try:
        i3_checkpoint.ExactPinReadCache().read(
            checkpoint_pin,
            lambda relative: (
                corrupted_bytes if relative == checkpoint_pin.path else meter.read_path(relative)
            ),
        )
    except i3_checkpoint.I3CheckpointError:
        exact_reader_rejected = True

    forged_pin = ArtifactPin(
        path=checkpoint_pin.path,
        sha256=hashlib.sha256(corrupted_bytes).hexdigest(),
        bytes=len(corrupted_bytes),
    )
    parser_rejected = False
    try:
        forged_content = i3_checkpoint.ExactPinReadCache().read(
            forged_pin,
            lambda relative: (
                corrupted_bytes if relative == forged_pin.path else meter.read_path(relative)
            ),
        )
        i3_checkpoint.I3CheckpointState.from_dict(
            i3_checkpoint._strict_json_document(forged_content)
        )
    except i3_checkpoint.I3CheckpointError:
        parser_rejected = True

    deep_chain_rejected = False
    try:
        tampered_output_set = replace(output_set, checkpoint_artifact=forged_pin)
        tampered_loaded = replace(
            loaded,
            receipt=replace(loaded.receipt, output_set=tampered_output_set),
        )
        i3_runtime._verify_deep_attestation(
            root,
            completion_pin,
            deep_attestation_pin,
            tampered_loaded,
        )
    except (
        i3_checkpoint.I3CheckpointError,
        I3ProductionContractError,
        i3_runtime.I3ProductionStageError,
    ):
        deep_chain_rejected = True

    if not (exact_reader_rejected and parser_rejected and deep_chain_rejected):
        raise I5ShadowRuntimeError(
            "production child checkpoint corruption did not fail at every exact loader"
        )
    return {
        "actual_child_checkpoint_path": checkpoint_pin.path,
        "actual_child_checkpoint_sha256": checkpoint_pin.sha256,
        "deep_chain_loader_invoked": True,
        "exact_pin_reader_invoked": True,
        "injected": "one_byte_actual_child_checkpoint_corruption",
        "strict_checkpoint_parser_invoked": True,
        "rejected_before_visibility": True,
    }


def _failure_recovery_receipts(
    root: Path,
    *,
    spec: ShadowRunSpec,
    inputs: _ProductionInputs,
    meter: _ReadMeter,
    completion_relative: str,
    lock_path: Path,
    production: bool,
    io_audit: _ProcessIOAudit | None = None,
    disk_monitor: _DiskFloorMonitor | None = None,
    started: float | None = None,
) -> tuple[tuple[FailureRecoveryReceipt, ...], tuple[dict[str, object], ...]]:
    parent_before = inputs.parent_reader_digest
    parent_paths_before = {
        str(item["artifact"]["path"])
        for partitions in inputs.parent_physical.values()
        for item in partitions
    }
    completion_path = safe_relative_path(root, completion_relative)
    if completion_path.exists() or completion_path.is_symlink():
        raise I5ShadowRuntimeError("failure exercises require an unpublished shadow result")
    outcomes: dict[FailureScenario, dict[str, object]] = {}

    if production:
        if inputs.loaded_incremental is None:
            raise I5ShadowRuntimeError("production failure exercise lacks an exact DELTA")
        outcomes[FailureScenario.CHECKPOINT_CORRUPTION] = (
            _exercise_production_checkpoint_corruption(
                root,
                loaded=inputs.loaded_incremental,
                completion_pin=spec.incremental_completion_artifact,
                deep_attestation_pin=spec.incremental_deep_attestation_artifact,
                meter=meter,
            )
        )
    else:
        checkpoint_bytes = _canonical_json_bytes(
            {"checkpoint_id": inputs.incremental.checkpoint_id}
        )
        checkpoint_artifact = _pin_bytes("manifests/fixtures/i5/checkpoint.json", checkpoint_bytes)
        corrupted = bytearray(checkpoint_bytes)
        corrupted[0] = corrupted[0] ^ 1
        rejected = False
        try:
            _require_pin_content(
                checkpoint_artifact,
                bytes(corrupted),
                label="corrupted checkpoint",
            )
        except I5ShadowRuntimeError:
            rejected = True
        if not rejected:
            raise I5ShadowRuntimeError("checkpoint corruption exercise did not fail closed")
        outcomes[FailureScenario.CHECKPOINT_CORRUPTION] = {
            "injected": "one_byte_in_memory_checkpoint_corruption",
            "production_deep_loader_invoked": False,
            "rejected_before_visibility": True,
        }

    concurrent_rejected = False
    try:
        with _exclusive_nonblocking_lock(lock_path):
            pass
    except I5ShadowRuntimeError as exc:
        concurrent_rejected = "holds the exact shadow lock" in str(exc)
    if not concurrent_rejected:
        raise I5ShadowRuntimeError("concurrent-lock exercise did not fail closed")
    outcomes[FailureScenario.CONCURRENT_LOCK] = {
        "injected": "second_nonblocking_lock_attempt",
        "rejected_before_visibility": True,
    }

    observed_free = _disk_free(root)
    disk_rejected = False
    try:
        _require_disk_floor(observed_free, observed_free + 1)
    except I5ShadowRuntimeError:
        disk_rejected = True
    if not disk_rejected:
        raise I5ShadowRuntimeError("disk-floor exercise did not fail closed")
    outcomes[FailureScenario.DISK_HARD_FLOOR] = {
        "injected_floor_bytes": observed_free + 1,
        "observed_free_bytes": observed_free,
        "rejected_before_visibility": True,
    }

    if production:
        if inputs.loaded_incremental is None:  # pragma: no cover
            raise I5ShadowRuntimeError("production duplicate exercise lacks a DELTA")
        if disk_monitor is not None:
            disk_monitor.sample("before_duplicate_production_stage")
        materializer = load_production_delta_materializer(
            data_root=root,
            run_spec=inputs.loaded_incremental.run_spec,
        )
        replayed = stage_i3_production_delta(
            root,
            inputs.loaded_incremental.receipt.run_spec_artifact,
            materializer=materializer,
        )
        duplicate_same = (
            replayed.reused is True
            and replayed.completion_pin == spec.incremental_completion_artifact
            and replayed.deep_attestation_pin == spec.incremental_deep_attestation_artifact
            and replayed.loaded.completion == inputs.loaded_incremental.completion
        )
        if io_audit is None:
            raise I5ProductionReadMeterSeamError(
                "P0 shadow resource meter: duplicate stage lacks process audit"
            )
        duplicate_delta = io_audit.sample(meter)
        if disk_monitor is not None:
            disk_monitor.sample("after_duplicate_production_stage")
        if started is None or disk_monitor is None:
            raise I5ProductionReadMeterSeamError(
                "P0 shadow resource meter: duplicate stage lacks whole-run bounds"
            )
        _enforce_resource_policy(
            spec.resource_policy,
            started=started,
            meter=meter,
            written_bytes=duplicate_delta.write_bytes,
            chain_resolution_milliseconds=inputs.chain_resolution_milliseconds,
            free_disk_bytes=disk_monitor.minimum,
        )
    else:
        duplicate_same = True
        duplicate_delta = None
    if not duplicate_same:
        raise I5ShadowRuntimeError("duplicate-retry exercise did not reproduce")
    duplicate_outcome: dict[str, object] = {
        "exact_completion_replayed_twice": not production,
        "production_stage_reused": production,
        "receipt_id": inputs.incremental.run_receipt_id,
        "rejected_before_visibility": False,
    }
    if duplicate_delta is not None:
        duplicate_outcome.update(
            {
                "process_read_bytes_through_duplicate": duplicate_delta.read_bytes,
                "process_write_bytes_through_duplicate": duplicate_delta.write_bytes,
                "process_write_syscalls_through_duplicate": duplicate_delta.write_syscalls,
            }
        )
    outcomes[FailureScenario.DUPLICATE_RETRY] = duplicate_outcome

    missing_pin = ArtifactPin(
        path=(
            "manifests/silver/incremental/i5/missing-parent/"
            f"release_id={stable_digest({'spec_id': spec.run_spec_id})}/manifest.json"
        ),
        sha256=stable_digest({"missing-parent": spec.run_spec_id}),
        bytes=1,
    )
    missing_rejected = False
    if production:
        try:
            if inputs.loaded_incremental is None:  # pragma: no cover
                raise I5ShadowRuntimeError("production missing-parent exercise lacks a DELTA")
            missing_parent_spec = replace(
                inputs.loaded_incremental.run_spec,
                parent_shadow_completion_artifact=missing_pin,
            )
            load_i3_production_parent_shallow_exact(root, missing_parent_spec)
        except I3ProductionContractError:
            missing_rejected = True
    else:
        try:
            meter.read_pin(missing_pin, label="missing parent exercise")
        except I5ShadowRuntimeError:
            missing_rejected = True
    if not missing_rejected:
        raise I5ShadowRuntimeError("missing-parent exercise did not fail closed")
    outcomes[FailureScenario.MISSING_PARENT] = {
        "exact_missing_parent_path": missing_pin.path,
        "fallback_discovery_attempted": False,
        "production_parent_loader_invoked": production,
        "rejected_before_visibility": True,
    }

    if production:
        if inputs.loaded_incremental is None:  # pragma: no cover
            raise I5ShadowRuntimeError("production interrupted exercise lacks a DELTA")
        outcomes[FailureScenario.INTERRUPTED_RUN] = _production_interrupted_retry_outcome(
            root,
            loaded=inputs.loaded_incremental,
            completion_artifact=spec.incremental_completion_artifact,
            deep_attestation_artifact=spec.incremental_deep_attestation_artifact,
        )
    else:
        interrupted = False
        try:
            raise _InjectedInterruption("injected before immutable completion write")
        except _InjectedInterruption:
            interrupted = True
        if not interrupted or completion_path.exists() or completion_path.is_symlink():
            raise I5ShadowRuntimeError("interrupted-run exercise exposed an output")
        outcomes[FailureScenario.INTERRUPTED_RUN] = {
            "injected": "exception_before_completion_write",
            "recovery": "exact parent remains selected",
            "rejected_before_visibility": True,
        }

    if production:
        reloaded_parent = load_i3_production_parent_shallow_exact(
            root, inputs.loaded_incremental.run_spec
        )
        if reloaded_parent is None:
            raise I5ShadowRuntimeError("parent disappeared after failure exercises")
        parent_after = _parent_reader_digest(reloaded_parent)
        parent_physical_after = _incremental_physical_projection(reloaded_parent)
    else:
        parent_after = parent_before
        parent_physical_after = inputs.parent_physical
    parent_paths_after = {
        str(item["artifact"]["path"])
        for partitions in parent_physical_after.values()
        for item in partitions
    }
    deleted_artifact_count = len(parent_paths_before - parent_paths_after)
    unpublished_visible_count = int(completion_path.exists() or completion_path.is_symlink())
    if parent_after != parent_before:
        raise I5ShadowRuntimeError("failure exercises changed the exact parent reader")
    if deleted_artifact_count or unpublished_visible_count:
        raise I5ShadowRuntimeError("failure exercises deleted or exposed an artifact")

    receipts = []
    documents = []
    for scenario in FailureScenario:
        outcome = outcomes[scenario]
        exercise_digest = stable_digest(
            {
                "outcome": outcome,
                "rule_version": I5_FAILURE_EXERCISE_RULE_VERSION,
                "scenario": scenario.value,
                "spec_id": spec.run_spec_id,
            }
        )
        document = _with_id(
            {
                "artifact_type": "s7_5_i5_failure_recovery_details",
                "deleted_artifact_count": deleted_artifact_count,
                "exercise_digest": exercise_digest,
                "outcome": outcome,
                "parent_reader_after_digest": parent_after,
                "parent_reader_before_digest": parent_before,
                "rule_version": I5_FAILURE_EXERCISE_RULE_VERSION,
                "scenario": scenario.value,
                "spec_id": spec.run_spec_id,
                "unpublished_visible_count": unpublished_visible_count,
            },
            "details_id",
        )
        pin = _document_pin(
            _failure_details_path(
                spec.run_spec_id,
                scenario,
                production=production,
            ),
            document,
        )
        receipts.append(
            FailureRecoveryReceipt(
                scenario=scenario,
                exercise_digest=exercise_digest,
                parent_reader_before_digest=parent_before,
                parent_reader_after_digest=parent_after,
                unpublished_visible_count=unpublished_visible_count,
                deleted_artifact_count=deleted_artifact_count,
                details_artifact=pin,
            )
        )
        documents.append(document)
    return tuple(receipts), tuple(documents)


class _InjectedInterruption(RuntimeError):
    pass


def _production_interrupted_retry_outcome(
    root: Path,
    *,
    loaded: LoadedI3ProductionStaging,
    completion_artifact: ArtifactPin,
    deep_attestation_artifact: ArtifactPin,
) -> dict[str, object]:
    """Replay I3's durable two-process failure exercise; never synthesize evidence."""

    if loaded.deep_attestation is None:
        raise I5ProductionFailureExerciseSeamError(_INTERRUPTED_RUN_SEAM_MESSAGE)
    try:
        result = i3_runtime.exercise_i3_production_interrupted_retry(
            root,
            loaded.receipt.run_spec_artifact,
            fail_after=i3_runtime.FAILED_RECEIPT_DURABLE_BEFORE_COMPLETION,
        )
    except Exception as exc:
        raise I5ProductionFailureExerciseSeamError(_INTERRUPTED_RUN_SEAM_MESSAGE) from exc
    if not isinstance(result, i3_runtime.I3ProductionInterruptedRetryResult):
        raise I5ProductionFailureExerciseSeamError(_INTERRUPTED_RUN_SEAM_MESSAGE)
    recovery = result.receipt
    if (
        result.reused is not True
        or recovery.run_spec_id != loaded.run_spec.run_spec_id
        or recovery.run_spec_artifact != loaded.receipt.run_spec_artifact
        or recovery.completion_id != loaded.completion.completion_id
        or recovery.completion_artifact != completion_artifact
        or recovery.completion_artifact != loaded.deep_attestation.completion_artifact
        or recovery.deep_attestation_id != loaded.deep_attestation.deep_attestation_id
        or recovery.deep_attestation_artifact != deep_attestation_artifact
        or result.stage_result.completion_pin != recovery.completion_artifact
        or result.stage_result.deep_attestation_pin != recovery.deep_attestation_artifact
        or result.stage_result.loaded.completion != loaded.completion
        or recovery.deleted_artifact_count != 0
        or recovery.unpublished_visible_count != 0
        or recovery.parent_reader_before_digest != recovery.parent_reader_after_digest
    ):
        raise I5ProductionFailureExerciseSeamError(
            "P0 interrupted-run evidence differs from the exact I3 DELTA"
        )
    return {
        "deleted_artifact_count": recovery.deleted_artifact_count,
        "fail_after": recovery.fail_after,
        "failed_receipt_artifact": recovery.failed_receipt_artifact.to_dict(),
        "failed_receipt_id": recovery.failed_receipt_id,
        "frozen_envelope_digest": recovery.frozen_envelope_digest,
        "interrupted_retry_receipt_artifact": result.receipt_artifact.to_dict(),
        "interrupted_retry_receipt_id": recovery.receipt_id,
        "parent_reader_after_digest": recovery.parent_reader_after_digest,
        "parent_reader_before_digest": recovery.parent_reader_before_digest,
        "phase_one_artifact": recovery.phase_one_artifact.to_dict(),
        "recovery_completion_artifact": recovery.completion_artifact.to_dict(),
        "recovery_completion_id": recovery.completion_id,
        "recovery_deep_attestation_artifact": recovery.deep_attestation_artifact.to_dict(),
        "recovery_deep_attestation_id": recovery.deep_attestation_id,
        "rejected_before_visibility": True,
        "reused_exact_replay": result.reused,
        "unpublished_visible_count": recovery.unpublished_visible_count,
    }


def _parent_reader_digest(parent: LoadedI3ProductionStaging) -> str:
    output_set = parent.receipt.output_set
    if output_set is None:
        raise I5ShadowRuntimeError("parent reader has no OutputSet")
    return stable_digest(
        {
            "checkpoint_id": parent.checkpoint.checkpoint_id,
            "completion_id": parent.completion.completion_id,
            "gate_a_release_id": parent.gate_a_manifest.release_id,
            "output_set_id": output_set.output_set_id,
            "physical": {
                table: list(parts)
                for table, parts in _incremental_physical_projection(parent).items()
            },
            "rule_version": "s7_5_i5_exact_parent_reader_projection_v1",
        }
    )


def load_i5_shadow_completion_exact(
    data_root: Path,
    completion_pin: ArtifactPin,
    *,
    production: bool = True,
) -> ShadowRunCompletion:
    """Load one exact completion and prove it remains Gate-B-free."""

    root = data_root.expanduser().resolve()
    meter = _ReadMeter(root)
    content = meter.read_pin(completion_pin, label="I5 shadow completion")
    item = _closed_json(content, label="I5 shadow completion")
    required = {
        "artifact_type",
        "authority",
        "completion_id",
        "cutover_authorized",
        "gate_b_authorized",
        "publish_authorized",
        "receipt",
        "receipt_available_session",
        "rule_version",
        "run_spec_artifact",
        "run_spec_id",
        "state",
    }
    _keys(item, required, "I5 shadow completion")
    payload = dict(item)
    claimed = _digest(payload.pop("completion_id"), "I5 completion ID")
    expected_authority = I5_PRODUCTION_AUTHORITY if production else I5_FIXTURE_AUTHORITY
    if (
        claimed != stable_digest(payload)
        or item["artifact_type"] != "s7_5_i5_shadow_equivalence_completion"
        or item["authority"] != expected_authority
        or item["rule_version"] != I5_SHADOW_COMPLETION_RULE_VERSION
        or item["state"] != I5_STATE
        or item["publish_authorized"] is not False
        or item["gate_b_authorized"] is not False
        or item["cutover_authorized"] is not False
    ):
        raise I5ShadowRuntimeError("I5 shadow completion semantics differ")
    spec_pin = _artifact_from_mapping(item["run_spec_artifact"], "I5 RunSpec")
    spec_content = meter.read_pin(spec_pin, label="I5 shadow RunSpec")
    spec = _run_spec_from_dict(_closed_json(spec_content, label="I5 shadow RunSpec"))
    if (
        spec.run_spec_id != item["run_spec_id"]
        or spec.exact_pin(path=spec_pin.path) != spec_pin
        or spec.authority != expected_authority
        or completion_pin.path != _completion_path(spec.run_spec_id, production=production)
    ):
        raise I5ShadowRuntimeError("I5 completion RunSpec binding differs")
    receipt = _receipt_from_dict(item["receipt"])
    if receipt.spec_id != spec.lifecycle_spec.spec_id:
        raise I5ShadowRuntimeError("I5 completion receipt names another lifecycle spec")
    if production:
        _verify_production_completion_against_producers(
            root,
            spec=spec,
            receipt=receipt,
            meter=meter,
        )
    _verify_i5_details_exact(root, meter=meter, spec=spec, receipt=receipt, production=production)
    completion = ShadowRunCompletion(
        run_spec_id=spec.run_spec_id,
        run_spec_artifact=spec_pin,
        receipt=receipt,
        receipt_available_session=_date_value(
            item["receipt_available_session"], "I5 completion availability"
        ),
        authority=expected_authority,
    )
    if completion.completion_id != claimed or completion.canonical_bytes() != content:
        raise I5ShadowRuntimeError("I5 shadow completion does not reproduce")
    validate_shadow_equivalence(
        spec.lifecycle_spec,
        receipt,
        availability_cutoff_session=spec.receipt_available_session,
        artifact_reader=meter.read_path,
    )
    return completion


def _verify_production_completion_against_producers(
    root: Path,
    *,
    spec: ShadowRunSpec,
    receipt: ShadowEquivalenceReceipt,
    meter: _ReadMeter,
) -> None:
    """Rebuild I5 from both producer authorities; stored I5 hashes are not authority."""

    config = ShadowRunConfig(
        incremental_completion_artifact=spec.incremental_completion_artifact,
        incremental_deep_attestation_artifact=(spec.incremental_deep_attestation_artifact),
        full_oracle_completion_artifact=spec.full_oracle_completion_artifact,
        comparison_sessions=spec.comparison_sessions,
        receipt_available_session=spec.receipt_available_session,
        resource_policy=spec.resource_policy,
    )
    io_audit = _ProcessIOAudit(_process_io_snapshot(require_exact=True))
    disk_monitor = _DiskFloorMonitor(root, spec.resource_policy.min_free_disk_bytes)
    disk_monitor.sample("production_completion_replay_entry")
    inputs = _load_production_inputs(
        root,
        config=config,
        meter=meter,
        io_audit=io_audit,
        disk_monitor=disk_monitor,
    )
    replay_delta = io_audit.sample(meter)
    rebuilt_spec = _build_run_spec(config, inputs, authority=I5_PRODUCTION_AUTHORITY)
    if rebuilt_spec != spec or rebuilt_spec.canonical_bytes() != spec.canonical_bytes():
        raise I5ShadowRuntimeError("production I5 RunSpec does not reproduce its producers")
    exact_replay_floor = (
        inputs.incremental.replayed_bytes_floor + inputs.oracle.replayed_bytes_floor
    )
    if receipt.resource_observation.read_bytes < max(
        exact_replay_floor,
        replay_delta.read_bytes,
    ):
        raise I5ProductionReadMeterSeamError(
            "P0 producer read meter: stored read bytes underreport exact producer artifacts"
        )
    if receipt.resource_observation.write_bytes < replay_delta.write_bytes:
        raise I5ProductionReadMeterSeamError(
            "P0 producer write meter: stored write bytes underreport producer replay"
        )
    comparisons, documents = _comparison_receipts(rebuilt_spec, inputs)
    if comparisons != receipt.comparisons:
        raise I5ShadowRuntimeError("production I5 comparisons do not reproduce")
    for comparison, document in zip(comparisons, documents, strict=True):
        content = meter.read_pin(
            comparison.details_artifact,
            label=f"rebuilt {comparison.projection.value} details",
        )
        if content != _canonical_json_bytes(document):
            raise I5ShadowRuntimeError("production I5 comparison details bytes differ")
    if inputs.loaded_incremental is None:
        raise I5ProductionFailureExerciseSeamError(_INTERRUPTED_RUN_SEAM_MESSAGE)
    interrupted_outcome = _production_interrupted_retry_outcome(
        root,
        loaded=inputs.loaded_incremental,
        completion_artifact=spec.incremental_completion_artifact,
        deep_attestation_artifact=spec.incremental_deep_attestation_artifact,
    )
    interrupted_receipt = next(
        (
            item
            for item in receipt.failure_recovery
            if item.scenario is FailureScenario.INTERRUPTED_RUN
        ),
        None,
    )
    if interrupted_receipt is None:
        raise I5ProductionFailureExerciseSeamError(
            "P0 interrupted-run receipt is absent from the stored I5 completion"
        )
    interrupted_document = _closed_json(
        meter.read_pin(
            interrupted_receipt.details_artifact,
            label="rebuilt interrupted-run recovery details",
        ),
        label="rebuilt interrupted-run recovery details",
    )
    if interrupted_document.get("outcome") != interrupted_outcome:
        raise I5ProductionFailureExerciseSeamError(
            "P0 interrupted-run details differ from the exact I3 recovery receipt"
        )
    final_replay_delta = io_audit.sample(meter)
    if receipt.resource_observation.read_bytes < max(
        exact_replay_floor,
        final_replay_delta.read_bytes,
    ):
        raise I5ProductionReadMeterSeamError(
            "P0 producer read meter: stored read bytes underreport interrupted replay"
        )
    if receipt.resource_observation.write_bytes < final_replay_delta.write_bytes:
        raise I5ProductionReadMeterSeamError(
            "P0 producer write meter: stored write bytes underreport interrupted replay"
        )


def _verify_i5_details_exact(
    root: Path,
    *,
    meter: _ReadMeter,
    spec: ShadowRunSpec,
    receipt: ShadowEquivalenceReceipt,
    production: bool,
) -> None:
    """Replay every details body, not merely the lifecycle-level exact pin."""

    del root  # The module-owned meter is the only filesystem reader at this boundary.
    for comparison in receipt.comparisons:
        expected_path = _comparison_details_path(
            spec.run_spec_id,
            comparison.projection,
            production=production,
        )
        if comparison.details_artifact.path != expected_path:
            raise I5ShadowRuntimeError("comparison details path differs from the exact RunSpec")
        document = _closed_json(
            meter.read_pin(
                comparison.details_artifact,
                label=f"{comparison.projection.value} details",
            ),
            label=f"{comparison.projection.value} details",
        )
        required = {
            "artifact_type",
            "compared_row_count",
            "details_id",
            "incremental_projection_digest",
            "oracle_projection_digest",
            "projection",
            "rule_version",
            "semantics_digest",
            "spec_id",
            "tables",
            "unexpected_difference_count",
        }
        if comparison.projection is EquivalenceProjection.CANONICAL_RESEARCH:
            required |= {"bounded_examples"}
            expected_rule = I5_CANONICAL_PROJECTION_RULE_VERSION
        else:
            expected_rule = I5_PHYSICAL_REUSE_PROJECTION_RULE_VERSION
        _keys(document, required, "I5 comparison details")
        body = dict(document)
        details_id = _digest(body.pop("details_id"), "I5 comparison details ID")
        if (
            details_id != stable_digest(body)
            or document["artifact_type"] != "s7_5_i5_projection_comparison_details"
            or document["projection"] != comparison.projection.value
            or document["rule_version"] != expected_rule
            or document["semantics_digest"] != comparison.semantics_digest
            or document["spec_id"] != spec.run_spec_id
            or document["compared_row_count"] != comparison.compared_row_count
            or document["incremental_projection_digest"] != comparison.incremental_projection_digest
            or document["oracle_projection_digest"] != comparison.oracle_projection_digest
            or document["unexpected_difference_count"] != comparison.unexpected_difference_count
            or not isinstance(document["tables"], list)
        ):
            raise I5ShadowRuntimeError("I5 comparison details do not reproduce their receipt")
        if comparison.projection is EquivalenceProjection.CANONICAL_RESEARCH:
            _verify_canonical_details_body(document, spec=spec, receipt=comparison)
        else:
            _verify_physical_details_body(document, spec=spec, receipt=comparison)

    for failure in receipt.failure_recovery:
        expected_path = _failure_details_path(
            spec.run_spec_id,
            failure.scenario,
            production=production,
        )
        if failure.details_artifact.path != expected_path:
            raise I5ShadowRuntimeError("failure details path differs from the exact RunSpec")
        document = _closed_json(
            meter.read_pin(
                failure.details_artifact,
                label=f"{failure.scenario.value} failure details",
            ),
            label=f"{failure.scenario.value} failure details",
        )
        _keys(
            document,
            {
                "artifact_type",
                "deleted_artifact_count",
                "details_id",
                "exercise_digest",
                "outcome",
                "parent_reader_after_digest",
                "parent_reader_before_digest",
                "rule_version",
                "scenario",
                "spec_id",
                "unpublished_visible_count",
            },
            "I5 failure details",
        )
        body = dict(document)
        details_id = _digest(body.pop("details_id"), "I5 failure details ID")
        if (
            details_id != stable_digest(body)
            or document["artifact_type"] != "s7_5_i5_failure_recovery_details"
            or document["rule_version"] != I5_FAILURE_EXERCISE_RULE_VERSION
            or document["scenario"] != failure.scenario.value
            or document["spec_id"] != spec.run_spec_id
            or document["exercise_digest"] != failure.exercise_digest
            or document["parent_reader_before_digest"] != failure.parent_reader_before_digest
            or document["parent_reader_after_digest"] != failure.parent_reader_after_digest
            or document["unpublished_visible_count"] != failure.unpublished_visible_count
            or document["deleted_artifact_count"] != failure.deleted_artifact_count
            or not isinstance(document["outcome"], dict)
        ):
            raise I5ShadowRuntimeError("I5 failure details do not reproduce their receipt")
        _verify_failure_details_body(document, spec=spec, receipt=failure)


def _verify_canonical_details_body(
    document: Mapping[str, object],
    *,
    spec: ShadowRunSpec,
    receipt: ProjectionComparisonReceipt,
) -> None:
    tables = _sequence(document["tables"], "canonical comparison tables")
    if len(tables) != len(I5_TABLE_ORDER):
        raise I5ShadowRuntimeError("canonical comparison table set differs")
    aggregate_incremental: list[dict[str, object]] = []
    aggregate_oracle: list[dict[str, object]] = []
    total_rows = 0
    total_differences = 0
    aggregate_examples: list[object] = []
    for expected_table, raw_table in zip(I5_TABLE_ORDER, tables, strict=True):
        table = _mapping(raw_table, "canonical comparison table")
        _keys(
            table,
            {"partitions", "schema_digest", "table_name"},
            "canonical comparison table",
        )
        if (
            table["table_name"] != expected_table
            or table["schema_digest"] != S7_DERIVED_CONTRACTS[expected_table].schema_digest
        ):
            raise I5ShadowRuntimeError("canonical comparison table authority differs")
        partitions = _sequence(table["partitions"], "canonical comparison partitions")
        expected_partition_keys = (
            tuple(item.isoformat() for item in spec.comparison_sessions)
            if expected_table == "universe_daily"
            else ("__table__",)
        )
        if len(partitions) != len(expected_partition_keys):
            raise I5ShadowRuntimeError("canonical comparison partition set differs")
        for expected_key, raw_partition in zip(expected_partition_keys, partitions, strict=True):
            partition = _mapping(raw_partition, "canonical comparison partition")
            _keys(
                partition,
                {
                    "bounded_examples",
                    "compared_row_count",
                    "incremental_projection_digest",
                    "oracle_projection_digest",
                    "partition_key",
                    "unexpected_difference_count",
                },
                "canonical comparison partition",
            )
            incremental_digest = _digest(
                partition["incremental_projection_digest"],
                "canonical partition incremental digest",
            )
            oracle_digest = _digest(
                partition["oracle_projection_digest"],
                "canonical partition oracle digest",
            )
            row_count = _nonnegative_int(
                partition["compared_row_count"], "canonical partition row count"
            )
            differences = _nonnegative_int(
                partition["unexpected_difference_count"],
                "canonical partition difference count",
            )
            examples = _sequence(partition["bounded_examples"], "canonical partition examples")
            if (
                partition["partition_key"] != expected_key
                or differences != 0
                or incremental_digest != oracle_digest
                or examples
            ):
                raise I5ShadowRuntimeError("canonical comparison partition semantics differ")
            total_rows += row_count
            total_differences += differences
            aggregate_examples.extend(examples[: 20 - len(aggregate_examples)])
            aggregate_incremental.append(
                {
                    "digest": incremental_digest,
                    "partition_key": expected_key,
                    "table_name": expected_table,
                }
            )
            aggregate_oracle.append(
                {
                    "digest": oracle_digest,
                    "partition_key": expected_key,
                    "table_name": expected_table,
                }
            )
    expected_incremental = stable_digest(
        {
            "partitions": aggregate_incremental,
            "rule_version": I5_CANONICAL_PROJECTION_RULE_VERSION,
        }
    )
    expected_oracle = stable_digest(
        {
            "partitions": aggregate_oracle,
            "rule_version": I5_CANONICAL_PROJECTION_RULE_VERSION,
        }
    )
    if (
        total_rows != receipt.compared_row_count
        or total_differences != receipt.unexpected_difference_count
        or document["bounded_examples"] != aggregate_examples
        or expected_incremental != receipt.incremental_projection_digest
        or expected_oracle != receipt.oracle_projection_digest
    ):
        raise I5ShadowRuntimeError("canonical comparison aggregate does not reproduce")


def _verify_physical_entry(
    value: object,
    *,
    table_name: str,
    oracle_evidence: bool,
) -> dict[str, object]:
    item = _mapping(value, "physical comparison entry")
    expected_fields = {"artifact", "row_count", "schema_digest"}
    if not oracle_evidence:
        expected_fields.add("partition_key")
    _keys(item, expected_fields, "physical comparison entry")
    _artifact_from_mapping(item["artifact"], "physical comparison artifact")
    _nonnegative_int(item["row_count"], "physical comparison row count")
    expected_schema = (
        S7_DERIVED_CONTRACTS[table_name].schema_digest
        if oracle_evidence
        else I3_V2_CONTRACTS[table_name].schema_digest
    )
    if item["schema_digest"] != expected_schema:
        raise I5ShadowRuntimeError("physical comparison schema digest differs")
    if not oracle_evidence:
        partition_key = _text(item["partition_key"], "physical partition key")
        if table_name == "universe_daily":
            _date_value(partition_key, "physical universe partition key")
        else:
            _digest(partition_key, "physical rowset segment ID")
    return item


def _verify_physical_details_body(
    document: Mapping[str, object],
    *,
    spec: ShadowRunSpec,
    receipt: ProjectionComparisonReceipt,
) -> None:
    tables = _sequence(document["tables"], "physical comparison tables")
    if len(tables) != len(I5_TABLE_ORDER):
        raise I5ShadowRuntimeError("physical comparison table set differs")
    actual_records: list[dict[str, object]] = []
    expected_records: list[dict[str, object]] = []
    compared = 0
    differences = 0
    for expected_table, raw_table in zip(I5_TABLE_ORDER, tables, strict=True):
        table = _mapping(raw_table, "physical comparison table")
        _keys(
            table,
            {
                "append_count",
                "child_partitions",
                "exact_parent_prefix_unchanged",
                "expected_partitions",
                "independent_full_oracle_evidence",
                "parent_partition_count",
                "table_name",
                "unexpected_difference_count",
            },
            "physical comparison table",
        )
        children = tuple(
            _verify_physical_entry(
                item,
                table_name=expected_table,
                oracle_evidence=False,
            )
            for item in _sequence(table["child_partitions"], "child partitions")
        )
        expected = tuple(
            _verify_physical_entry(
                item,
                table_name=expected_table,
                oracle_evidence=False,
            )
            for item in _sequence(table["expected_partitions"], "expected partitions")
        )
        oracle_evidence = tuple(
            _verify_physical_entry(
                item,
                table_name=expected_table,
                oracle_evidence=True,
            )
            for item in _sequence(
                table["independent_full_oracle_evidence"],
                "independent Full oracle evidence",
            )
        )
        parent_count = _nonnegative_int(
            table["parent_partition_count"], "physical parent partition count"
        )
        append_count = _nonnegative_int(table["append_count"], "physical append count")
        table_differences = _nonnegative_int(
            table["unexpected_difference_count"], "physical table differences"
        )
        if (
            table["table_name"] != expected_table
            or table["exact_parent_prefix_unchanged"] is not True
            or append_count != 1
            or table_differences != 0
            or len(children) != parent_count + 1
            or expected != children
            or len(oracle_evidence)
            != (len(spec.comparison_sessions) if expected_table == "universe_daily" else 1)
        ):
            raise I5ShadowRuntimeError("physical clean-append table semantics differ")
        if (
            expected_table == "universe_daily"
            and children[-1]["partition_key"] != I5_TARGET_SESSION.isoformat()
        ):
            raise I5ShadowRuntimeError("physical universe suffix is not the target DELTA")
        compared += len({item["partition_key"] for item in (*children, *expected)})
        differences += table_differences
        actual_records.append({"partitions": list(children), "table_name": expected_table})
        expected_records.append({"partitions": list(expected), "table_name": expected_table})
    expected_incremental = stable_digest(
        {
            "tables": actual_records,
            "rule_version": I5_PHYSICAL_REUSE_PROJECTION_RULE_VERSION,
        }
    )
    expected_oracle = stable_digest(
        {
            "tables": expected_records,
            "rule_version": I5_PHYSICAL_REUSE_PROJECTION_RULE_VERSION,
        }
    )
    if (
        compared != receipt.compared_row_count
        or differences != receipt.unexpected_difference_count
        or expected_incremental != receipt.incremental_projection_digest
        or expected_oracle != receipt.oracle_projection_digest
    ):
        raise I5ShadowRuntimeError("physical comparison aggregate does not reproduce")


def _verify_failure_details_body(
    document: Mapping[str, object],
    *,
    spec: ShadowRunSpec,
    receipt: FailureRecoveryReceipt,
) -> None:
    outcome = _mapping(document["outcome"], "failure exercise outcome")
    if (
        document["parent_reader_before_digest"] != document["parent_reader_after_digest"]
        or document["unpublished_visible_count"] != 0
        or document["deleted_artifact_count"] != 0
    ):
        raise I5ShadowRuntimeError("failure exercise safety invariant differs")
    scenario = receipt.scenario
    if scenario is FailureScenario.CHECKPOINT_CORRUPTION:
        if spec.authority == I5_PRODUCTION_AUTHORITY:
            expected_fields = {
                "actual_child_checkpoint_path",
                "actual_child_checkpoint_sha256",
                "deep_chain_loader_invoked",
                "exact_pin_reader_invoked",
                "injected",
                "strict_checkpoint_parser_invoked",
                "rejected_before_visibility",
            }
            _relative(
                outcome.get("actual_child_checkpoint_path"),
                "checkpoint exercise child path",
            )
            _digest(
                outcome.get("actual_child_checkpoint_sha256"),
                "checkpoint exercise child SHA-256",
            )
            valid = (
                outcome.get("injected") == "one_byte_actual_child_checkpoint_corruption"
                and outcome.get("exact_pin_reader_invoked") is True
                and outcome.get("strict_checkpoint_parser_invoked") is True
                and outcome.get("deep_chain_loader_invoked") is True
                and outcome.get("rejected_before_visibility") is True
            )
        else:
            expected_fields = {
                "injected",
                "production_deep_loader_invoked",
                "rejected_before_visibility",
            }
            valid = (
                outcome.get("injected") == "one_byte_in_memory_checkpoint_corruption"
                and outcome.get("production_deep_loader_invoked") is False
                and outcome.get("rejected_before_visibility") is True
            )
    elif scenario is FailureScenario.CONCURRENT_LOCK:
        expected_fields = {"injected", "rejected_before_visibility"}
        valid = (
            outcome.get("injected") == "second_nonblocking_lock_attempt"
            and outcome.get("rejected_before_visibility") is True
        )
    elif scenario is FailureScenario.DISK_HARD_FLOOR:
        expected_fields = {
            "injected_floor_bytes",
            "observed_free_bytes",
            "rejected_before_visibility",
        }
        observed = _nonnegative_int(
            outcome.get("observed_free_bytes"), "disk exercise observed free bytes"
        )
        injected = _positive_int(
            outcome.get("injected_floor_bytes"), "disk exercise injected floor"
        )
        valid = injected == observed + 1 and outcome.get("rejected_before_visibility") is True
    elif scenario is FailureScenario.DUPLICATE_RETRY:
        expected_fields = {
            "exact_completion_replayed_twice",
            "production_stage_reused",
            "receipt_id",
            "rejected_before_visibility",
        }
        if spec.authority == I5_PRODUCTION_AUTHORITY:
            expected_fields.update(
                {
                    "process_read_bytes_through_duplicate",
                    "process_write_bytes_through_duplicate",
                    "process_write_syscalls_through_duplicate",
                }
            )
            _nonnegative_int(
                outcome.get("process_read_bytes_through_duplicate"),
                "duplicate exercise process read bytes",
            )
            _nonnegative_int(
                outcome.get("process_write_bytes_through_duplicate"),
                "duplicate exercise process write bytes",
            )
            _nonnegative_int(
                outcome.get("process_write_syscalls_through_duplicate"),
                "duplicate exercise process write syscalls",
            )
        _digest(outcome.get("receipt_id"), "duplicate exercise receipt ID")
        valid = (
            outcome.get("exact_completion_replayed_twice")
            is (spec.authority != I5_PRODUCTION_AUTHORITY)
            and outcome.get("production_stage_reused")
            is (spec.authority == I5_PRODUCTION_AUTHORITY)
            and outcome.get("rejected_before_visibility") is False
        )
    elif scenario is FailureScenario.INTERRUPTED_RUN:
        if spec.authority == I5_PRODUCTION_AUTHORITY:
            expected_fields = {
                "deleted_artifact_count",
                "fail_after",
                "failed_receipt_artifact",
                "failed_receipt_id",
                "frozen_envelope_digest",
                "interrupted_retry_receipt_artifact",
                "interrupted_retry_receipt_id",
                "parent_reader_after_digest",
                "parent_reader_before_digest",
                "phase_one_artifact",
                "recovery_completion_artifact",
                "recovery_completion_id",
                "recovery_deep_attestation_artifact",
                "recovery_deep_attestation_id",
                "rejected_before_visibility",
                "reused_exact_replay",
                "unpublished_visible_count",
            }
            for field in (
                "failed_receipt_artifact",
                "interrupted_retry_receipt_artifact",
                "phase_one_artifact",
                "recovery_completion_artifact",
                "recovery_deep_attestation_artifact",
            ):
                _artifact_from_mapping(outcome.get(field), field)
            for field in (
                "failed_receipt_id",
                "frozen_envelope_digest",
                "interrupted_retry_receipt_id",
                "parent_reader_after_digest",
                "parent_reader_before_digest",
                "recovery_completion_id",
                "recovery_deep_attestation_id",
            ):
                _digest(outcome.get(field), field)
            valid = (
                outcome.get("fail_after") == i3_runtime.FAILED_RECEIPT_DURABLE_BEFORE_COMPLETION
                and outcome.get("parent_reader_before_digest")
                == outcome.get("parent_reader_after_digest")
                and outcome.get("deleted_artifact_count") == 0
                and outcome.get("unpublished_visible_count") == 0
                and outcome.get("rejected_before_visibility") is True
                and outcome.get("reused_exact_replay") is True
            )
        else:
            expected_fields = {"injected", "recovery", "rejected_before_visibility"}
            valid = (
                outcome.get("injected") == "exception_before_completion_write"
                and outcome.get("recovery") == "exact parent remains selected"
                and outcome.get("rejected_before_visibility") is True
            )
    else:
        expected_fields = {
            "exact_missing_parent_path",
            "fallback_discovery_attempted",
            "production_parent_loader_invoked",
            "rejected_before_visibility",
        }
        missing_release = stable_digest({"spec_id": spec.run_spec_id})
        valid = (
            outcome.get("exact_missing_parent_path")
            == (
                "manifests/silver/incremental/i5/missing-parent/"
                f"release_id={missing_release}/manifest.json"
            )
            and outcome.get("fallback_discovery_attempted") is False
            and outcome.get("production_parent_loader_invoked")
            is (spec.authority == I5_PRODUCTION_AUTHORITY)
            and outcome.get("rejected_before_visibility") is True
        )
    _keys(outcome, expected_fields, "failure exercise outcome")
    expected_exercise_digest = stable_digest(
        {
            "outcome": outcome,
            "rule_version": I5_FAILURE_EXERCISE_RULE_VERSION,
            "scenario": scenario.value,
            "spec_id": spec.run_spec_id,
        }
    )
    if not valid or expected_exercise_digest != receipt.exercise_digest:
        raise I5ShadowRuntimeError("failure exercise outcome semantics differ")


def _execute_i5_shadow_fixture(
    data_root: Path,
    *,
    incremental: _ResolvedSide,
    oracle: _ResolvedSide,
    parent_physical: Mapping[str, tuple[dict[str, object], ...]],
    common_parent_release_id: str,
    comparison_sessions: tuple[date, ...],
    receipt_available_session: date,
    resource_policy: ResourceGatePolicy,
) -> ShadowRunResult:
    """Non-authoritative small-fixture boundary used only by attack tests."""

    root = data_root.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    _sessions(comparison_sessions)
    if comparison_sessions[-1] != I5_TARGET_SESSION:
        raise I5ShadowRuntimeError("fixture shadow cutoff differs")
    if incremental.source_binding_digest != oracle.source_binding_digest:
        raise I5FullOracleSeamError("fixture source lineage differs")
    meter = _ReadMeter(root)
    started = time.monotonic()
    dummy_completion = _fixture_pin("incremental-completion", incremental.release_id)
    dummy_deep = _fixture_pin("incremental-deep", incremental.checkpoint_id)
    dummy_oracle = _fixture_pin("full-oracle-completion", oracle.release_id)
    spec = ShadowRunSpec(
        authority=I5_FIXTURE_AUTHORITY,
        incremental_completion_artifact=dummy_completion,
        incremental_deep_attestation_artifact=dummy_deep,
        full_oracle_completion_artifact=dummy_oracle,
        incremental_release_id=incremental.release_id,
        full_oracle_release_id=oracle.release_id,
        common_parent_release_id=_digest(common_parent_release_id, "fixture common parent"),
        source_binding_digest=incremental.source_binding_digest,
        schema_bundle_digest=incremental.schema_bundle_digest,
        transform_semantics_digest=stable_digest(
            {
                "full": oracle.transform_semantics_digest,
                "incremental": incremental.transform_semantics_digest,
                "rule_version": "s7_5_i5_fixture_transform_pair_v1",
            }
        ),
        identity_policy_bundle_id=incremental.identity_policy_bundle_id,
        calendar_digest=incremental.calendar_digest,
        scope_artifact=I5_SCOPE_ARTIFACT,
        comparison_sessions=comparison_sessions,
        receipt_available_session=receipt_available_session,
        resource_policy=resource_policy,
    )
    parent_digest = stable_digest(
        {
            "physical": {key: list(value) for key, value in parent_physical.items()},
            "rule_version": "s7_5_i5_fixture_parent_reader_v1",
        }
    )
    inputs = _ProductionInputs(
        incremental=incremental,
        oracle=oracle,
        loaded_incremental=None,
        loaded_parent=None,
        parent_physical=MappingProxyType(dict(parent_physical)),
        parent_reader_digest=parent_digest,
        chain_resolution_milliseconds=1,
    )
    return _execute_loaded_shadow(
        root,
        spec=spec,
        inputs=inputs,
        meter=meter,
        started=started,
        minimum_disk=_disk_free(root),
        production=False,
    )


def _run_spec_from_dict(value: object) -> ShadowRunSpec:
    item = _mapping(value, "I5 shadow RunSpec")
    required = {
        "authority",
        "calendar_digest",
        "common_parent_release_id",
        "comparison_sessions",
        "full_oracle_completion_artifact",
        "full_oracle_release_id",
        "identity_policy_bundle_id",
        "incremental_completion_artifact",
        "incremental_deep_attestation_artifact",
        "incremental_release_id",
        "projection_policies",
        "receipt_available_session",
        "resource_policy",
        "rule_version",
        "run_spec_id",
        "schema_bundle_digest",
        "scope_artifact",
        "source_binding_digest",
        "transform_semantics_digest",
    }
    _keys(item, required, "I5 shadow RunSpec")
    if item["rule_version"] != I5_SHADOW_RUN_SPEC_RULE_VERSION:
        raise I5ShadowRuntimeError("I5 shadow RunSpec rule differs")
    resource = _resource_policy_from_dict(item["resource_policy"])
    result = ShadowRunSpec(
        authority=_text(item["authority"], "I5 RunSpec authority"),
        incremental_completion_artifact=_artifact_from_mapping(
            item["incremental_completion_artifact"], "incremental completion"
        ),
        incremental_deep_attestation_artifact=_artifact_from_mapping(
            item["incremental_deep_attestation_artifact"],
            "incremental deep attestation",
        ),
        full_oracle_completion_artifact=_artifact_from_mapping(
            item["full_oracle_completion_artifact"], "Full oracle completion"
        ),
        incremental_release_id=_digest(item["incremental_release_id"], "incremental release ID"),
        full_oracle_release_id=_digest(item["full_oracle_release_id"], "Full oracle release ID"),
        common_parent_release_id=_digest(
            item["common_parent_release_id"], "common parent release ID"
        ),
        source_binding_digest=_digest(item["source_binding_digest"], "source binding digest"),
        schema_bundle_digest=_digest(item["schema_bundle_digest"], "schema bundle digest"),
        transform_semantics_digest=_digest(
            item["transform_semantics_digest"], "transform semantics digest"
        ),
        identity_policy_bundle_id=_digest(
            item["identity_policy_bundle_id"], "identity policy bundle ID"
        ),
        calendar_digest=_digest(item["calendar_digest"], "calendar digest"),
        scope_artifact=_artifact_from_mapping(item["scope_artifact"], "module-owned shadow scope"),
        comparison_sessions=tuple(
            _date_value(value, "comparison session")
            for value in _sequence(item["comparison_sessions"], "comparison sessions")
        ),
        receipt_available_session=_date_value(
            item["receipt_available_session"], "RunSpec availability"
        ),
        resource_policy=resource,
    )
    policies = tuple(
        _projection_policy_from_dict(entry)
        for entry in _sequence(item["projection_policies"], "projection policies")
    )
    if (
        item["run_spec_id"] != result.run_spec_id
        or policies != result.projection_policies
        or result.to_dict() != item
    ):
        raise I5ShadowRuntimeError("I5 shadow RunSpec does not reproduce")
    return result


def _receipt_from_dict(value: object) -> ShadowEquivalenceReceipt:
    item = _mapping(value, "shadow equivalence receipt")
    required = {
        "comparisons",
        "failure_recovery",
        "full_oracle_release_id",
        "idempotency",
        "incremental_release_id",
        "receipt_available_session",
        "receipt_id",
        "resource_observation",
        "source_binding_digest",
        "spec_id",
    }
    _keys(item, required, "shadow equivalence receipt")
    comparisons = tuple(
        _comparison_receipt_from_dict(entry)
        for entry in _sequence(item["comparisons"], "comparison receipts")
    )
    failures = tuple(
        _failure_receipt_from_dict(entry)
        for entry in _sequence(item["failure_recovery"], "failure receipts")
    )
    observation = _mapping(item["resource_observation"], "resource observation")
    _keys(
        observation,
        {
            "chain_resolution_milliseconds",
            "free_disk_bytes_at_floor",
            "peak_rss_bytes",
            "read_bytes",
            "wall_clock_seconds",
            "write_bytes",
        },
        "resource observation",
    )
    idempotency = _mapping(item["idempotency"], "idempotency receipt")
    _keys(
        idempotency,
        {
            "first_checkpoint_id",
            "first_manifest_sha256",
            "first_release_id",
            "first_run_receipt_id",
            "reproduces",
            "second_checkpoint_id",
            "second_manifest_sha256",
            "second_release_id",
            "second_run_receipt_id",
        },
        "idempotency receipt",
    )
    result = ShadowEquivalenceReceipt(
        spec_id=_digest(item["spec_id"], "shadow receipt spec ID"),
        incremental_release_id=_digest(
            item["incremental_release_id"], "shadow incremental release ID"
        ),
        full_oracle_release_id=_digest(item["full_oracle_release_id"], "shadow oracle release ID"),
        source_binding_digest=_digest(item["source_binding_digest"], "shadow source binding"),
        comparisons=comparisons,
        resource_observation=ResourceObservation(
            wall_clock_seconds=_nonnegative_int(observation["wall_clock_seconds"], "wall clock"),
            peak_rss_bytes=_nonnegative_int(observation["peak_rss_bytes"], "peak RSS"),
            free_disk_bytes_at_floor=_nonnegative_int(
                observation["free_disk_bytes_at_floor"], "free disk"
            ),
            read_bytes=_nonnegative_int(observation["read_bytes"], "read bytes"),
            write_bytes=_nonnegative_int(observation["write_bytes"], "write bytes"),
            chain_resolution_milliseconds=_nonnegative_int(
                observation["chain_resolution_milliseconds"], "chain resolution"
            ),
        ),
        failure_recovery=failures,
        idempotency=IdempotencyReceipt(
            first_run_receipt_id=_digest(idempotency["first_run_receipt_id"], "first run receipt"),
            second_run_receipt_id=_digest(
                idempotency["second_run_receipt_id"], "second run receipt"
            ),
            first_checkpoint_id=_digest(idempotency["first_checkpoint_id"], "first checkpoint"),
            second_checkpoint_id=_digest(idempotency["second_checkpoint_id"], "second checkpoint"),
            first_release_id=_digest(idempotency["first_release_id"], "first release"),
            second_release_id=_digest(idempotency["second_release_id"], "second release"),
            first_manifest_sha256=_digest(
                idempotency["first_manifest_sha256"], "first manifest SHA"
            ),
            second_manifest_sha256=_digest(
                idempotency["second_manifest_sha256"], "second manifest SHA"
            ),
        ),
        receipt_available_session=_date_value(
            item["receipt_available_session"], "shadow receipt availability"
        ),
    )
    if item["receipt_id"] != result.receipt_id or item != result.to_dict():
        raise I5ShadowRuntimeError("shadow equivalence receipt does not reproduce")
    return result


def _comparison_receipt_from_dict(value: object) -> ProjectionComparisonReceipt:
    item = _mapping(value, "comparison receipt")
    _keys(
        item,
        {
            "compared_row_count",
            "details_artifact",
            "incremental_projection_digest",
            "oracle_projection_digest",
            "projection",
            "semantics_digest",
            "unexpected_difference_count",
        },
        "comparison receipt",
    )
    try:
        projection = EquivalenceProjection(_text(item["projection"], "projection"))
    except ValueError as exc:
        raise I5ShadowRuntimeError("comparison projection is invalid") from exc
    return ProjectionComparisonReceipt(
        projection=projection,
        semantics_digest=_digest(item["semantics_digest"], "comparison semantics"),
        compared_row_count=_nonnegative_int(item["compared_row_count"], "compared rows"),
        incremental_projection_digest=_digest(
            item["incremental_projection_digest"], "incremental projection"
        ),
        oracle_projection_digest=_digest(item["oracle_projection_digest"], "oracle projection"),
        unexpected_difference_count=_nonnegative_int(
            item["unexpected_difference_count"], "unexpected differences"
        ),
        details_artifact=_artifact_from_mapping(item["details_artifact"], "comparison details"),
    )


def _failure_receipt_from_dict(value: object) -> FailureRecoveryReceipt:
    item = _mapping(value, "failure receipt")
    _keys(
        item,
        {
            "deleted_artifact_count",
            "details_artifact",
            "exercise_digest",
            "parent_reader_after_digest",
            "parent_reader_before_digest",
            "scenario",
            "unpublished_visible_count",
        },
        "failure receipt",
    )
    try:
        scenario = FailureScenario(_text(item["scenario"], "failure scenario"))
    except ValueError as exc:
        raise I5ShadowRuntimeError("failure scenario is invalid") from exc
    return FailureRecoveryReceipt(
        scenario=scenario,
        exercise_digest=_digest(item["exercise_digest"], "failure exercise digest"),
        parent_reader_before_digest=_digest(
            item["parent_reader_before_digest"], "parent reader before"
        ),
        parent_reader_after_digest=_digest(
            item["parent_reader_after_digest"], "parent reader after"
        ),
        unpublished_visible_count=_nonnegative_int(
            item["unpublished_visible_count"], "unpublished visible count"
        ),
        deleted_artifact_count=_nonnegative_int(
            item["deleted_artifact_count"], "deleted artifact count"
        ),
        details_artifact=_artifact_from_mapping(item["details_artifact"], "failure details"),
    )


def _resource_policy_from_dict(value: object) -> ResourceGatePolicy:
    item = _mapping(value, "resource policy")
    _keys(
        item,
        {
            "max_chain_resolution_milliseconds",
            "max_peak_rss_bytes",
            "max_read_bytes",
            "max_wall_clock_seconds",
            "max_write_bytes",
            "min_free_disk_bytes",
            "policy_id",
            "rule_version",
        },
        "resource policy",
    )
    if item["rule_version"] != "s7_5_i5_resource_gate_v1":
        raise I5ShadowRuntimeError("resource policy rule differs")
    result = ResourceGatePolicy(
        max_wall_clock_seconds=_positive_int(item["max_wall_clock_seconds"], "maximum wall clock"),
        max_peak_rss_bytes=_positive_int(item["max_peak_rss_bytes"], "maximum RSS"),
        min_free_disk_bytes=_positive_int(item["min_free_disk_bytes"], "disk floor"),
        max_read_bytes=_positive_int(item["max_read_bytes"], "maximum read bytes"),
        max_write_bytes=_positive_int(item["max_write_bytes"], "maximum write bytes"),
        max_chain_resolution_milliseconds=_positive_int(
            item["max_chain_resolution_milliseconds"], "maximum chain resolution"
        ),
    )
    if result.to_dict() != item:
        raise I5ShadowRuntimeError("resource policy does not reproduce")
    return result


def _projection_policy_from_dict(value: object) -> ProjectionPolicy:
    item = _mapping(value, "projection policy")
    _keys(item, {"projection", "semantics_digest"}, "projection policy")
    try:
        projection = EquivalenceProjection(_text(item["projection"], "projection policy kind"))
    except ValueError as exc:
        raise I5ShadowRuntimeError("projection policy kind is invalid") from exc
    return ProjectionPolicy(
        projection=projection,
        semantics_digest=_digest(item["semantics_digest"], "projection semantics"),
    )


def _read_parquet_exact(
    root: Path,
    *,
    artifact: ArtifactPin,
    table_name: str,
    expected_schema: pa.Schema,
    expected_rows: int,
    meter: _ReadMeter,
) -> pa.Table:
    content = meter.read_pin(artifact, label=f"{table_name} Parquet")
    try:
        parquet = pq.ParquetFile(pa.BufferReader(content))
        if parquet.schema_arrow != expected_schema or parquet.metadata.num_rows != expected_rows:
            raise I5ShadowRuntimeError(f"{table_name} Parquet schema or row count differs")
        table = parquet.read()
    except I5ShadowRuntimeError:
        raise
    except (OSError, pa.ArrowException) as exc:
        raise I5ShadowRuntimeError(f"{table_name} exact artifact is not readable Parquet") from exc
    if table.schema != expected_schema or table.num_rows != expected_rows:
        raise I5ShadowRuntimeError(f"{table_name} Parquet readback differs")
    return table


def _enforce_resource_policy(
    policy: ResourceGatePolicy,
    *,
    started: float,
    meter: _ReadMeter,
    written_bytes: int,
    chain_resolution_milliseconds: int,
    free_disk_bytes: int,
) -> None:
    elapsed = math.ceil(time.monotonic() - started)
    if elapsed > policy.max_wall_clock_seconds:
        raise I5ShadowRuntimeError("shadow wall-clock resource gate failed")
    if _peak_rss_bytes() > policy.max_peak_rss_bytes:
        raise I5ShadowRuntimeError("shadow peak-RSS resource gate failed")
    if free_disk_bytes < policy.min_free_disk_bytes:
        raise I5ShadowRuntimeError("shadow disk-floor resource gate failed")
    if meter.bytes > policy.max_read_bytes:
        raise I5ShadowRuntimeError("shadow read-byte resource gate failed")
    if written_bytes > policy.max_write_bytes:
        raise I5ShadowRuntimeError("shadow write-byte resource gate failed")
    if chain_resolution_milliseconds > policy.max_chain_resolution_milliseconds:
        raise I5ShadowRuntimeError("shadow chain-resolution resource gate failed")


def _require_disk_floor(observed: int, floor: int) -> None:
    if observed < floor:
        raise I5ShadowRuntimeError("shadow disk floor breached before write")


def _disk_free(root: Path) -> int:
    try:
        return int(shutil.disk_usage(root).free)
    except OSError as exc:
        raise I5ShadowRuntimeError("cannot inspect shadow disk free bytes") from exc


def _peak_rss_bytes() -> int:
    value = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    return value if sys.platform == "darwin" else value * 1024


def _write_immutable(root: Path, relative: str, content: bytes, *, label: str) -> ArtifactPin:
    normalized = _relative(relative, f"{label} path")
    path = safe_relative_path(root, normalized)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() or path.is_symlink():
        if not path.is_file() or path.is_symlink():
            raise I5ShadowRuntimeError(f"immutable {label} target is unsafe")
        existing = path.read_bytes()
        if existing != content:
            raise I5ShadowRuntimeError(f"immutable {label} bytes differ")
        return _pin_bytes(normalized, content)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags, 0o640)
        try:
            with os.fdopen(descriptor, "wb", closefd=False) as stream:
                stream.write(content)
                stream.flush()
                os.fsync(stream.fileno())
        finally:
            os.close(descriptor)
        _fsync_directory(path.parent)
    except OSError as exc:
        raise I5ShadowRuntimeError(f"cannot write immutable {label}") from exc
    return _pin_bytes(normalized, content)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _require_pin_content(pin: ArtifactPin, content: bytes, *, label: str) -> None:
    if len(content) != pin.bytes or hashlib.sha256(content).hexdigest() != pin.sha256:
        raise I5ShadowRuntimeError(f"{label} exact pin differs")


def _document_pin(path: str, document: Mapping[str, object]) -> ArtifactPin:
    return _pin_bytes(path, _canonical_json_bytes(document))


def _pin_bytes(path: str, content: bytes) -> ArtifactPin:
    return ArtifactPin(
        path=_relative(path, "artifact path"),
        sha256=hashlib.sha256(content).hexdigest(),
        bytes=len(content),
    )


def _fixture_pin(label: str, identity: str) -> ArtifactPin:
    content = _canonical_json_bytes({"fixture": label, "identity": identity})
    return _pin_bytes(f"manifests/fixtures/i5/{label}.json", content)


def _artifact_from_mapping(value: object, label: str) -> ArtifactPin:
    item = _mapping(value, label)
    _keys(item, {"bytes", "path", "sha256"}, label)
    return ArtifactPin(
        path=_relative(item["path"], f"{label} path"),
        sha256=_digest(item["sha256"], f"{label} SHA-256"),
        bytes=_nonnegative_int(item["bytes"], f"{label} bytes"),
    )


def _artifact(value: object, label: str) -> ArtifactPin:
    if not isinstance(value, ArtifactPin):
        raise I5ShadowRuntimeError(f"{label} pin is invalid")
    _relative(value.path, f"{label} path")
    _digest(value.sha256, f"{label} SHA-256")
    _nonnegative_int(value.bytes, f"{label} bytes")
    return value


def _production_authority_path(value: str, label: str) -> str:
    relative = _relative(value, f"{label} path")
    lowered = tuple(part.lower() for part in PurePosixPath(relative).parts)
    forbidden = any(
        part in _FORBIDDEN_AUTHORITY_PARTS
        or any(
            part.startswith(f"{token}.") or part.startswith(f"{token}=")
            for token in _FORBIDDEN_AUTHORITY_PARTS
        )
        for part in lowered
    )
    if forbidden or any(character in relative for character in "*?["):
        raise I5ShadowRuntimeError(f"{label} path is not production authority")
    return relative


def _run_spec_path(spec_id: str, *, production: bool) -> str:
    prefix = "manifests/silver/incremental/i5" if production else "manifests/fixtures/i5"
    return f"{prefix}/shadow-run-specs/spec_id={_digest(spec_id, 'spec ID')}/manifest.json"


def _completion_path(spec_id: str, *, production: bool) -> str:
    prefix = "manifests/silver/incremental/i5" if production else "manifests/fixtures/i5"
    return f"{prefix}/shadow-runs/spec_id={_digest(spec_id, 'spec ID')}/completion.json"


def _comparison_details_path(
    spec_id: str,
    projection: EquivalenceProjection,
    *,
    production: bool,
) -> str:
    prefix = "manifests/silver/incremental/i5" if production else "manifests/fixtures/i5"
    return (
        f"{prefix}/shadow-runs/spec_id={_digest(spec_id, 'spec ID')}/"
        f"comparisons/projection={projection.value}/details.json"
    )


def _failure_details_path(
    spec_id: str,
    scenario: FailureScenario,
    *,
    production: bool,
) -> str:
    prefix = "manifests/silver/incremental/i5" if production else "manifests/fixtures/i5"
    return (
        f"{prefix}/shadow-runs/spec_id={_digest(spec_id, 'spec ID')}/"
        f"failure-recovery/scenario={scenario.value}/details.json"
    )


def _lock_path(spec_id: str, *, production: bool) -> str:
    prefix = "manifests/silver/locks" if production else "manifests/fixtures/i5/locks"
    return f"{prefix}/i5-shadow-spec-{_digest(spec_id, 'spec ID')}.lock"


def _with_id(document: Mapping[str, object], key: str) -> dict[str, object]:
    payload = dict(document)
    if key in payload:
        raise I5ShadowRuntimeError("content-addressed document already carries its ID")
    return {**payload, key: stable_digest(payload)}


def _closed_json(content: bytes, *, label: str) -> dict[str, object]:
    def reject_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate key: {key}")
            result[key] = value
        return result

    def reject_constant(value: str) -> None:
        raise ValueError(f"invalid JSON constant: {value}")

    try:
        value = json.loads(
            content.decode("utf-8"),
            object_pairs_hook=reject_pairs,
            parse_constant=reject_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise I5ShadowRuntimeError(f"{label} is invalid JSON") from exc
    if not isinstance(value, dict) or _canonical_json_bytes(value) != content:
        raise I5ShadowRuntimeError(f"{label} is not canonical JSON")
    return value


def _canonical_json_bytes(value: object) -> bytes:
    return (
        json.dumps(
            _json_value(value),
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def _json_value(value: object) -> object:
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    if isinstance(value, (str, int, bool)) or value is None:
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise I5ShadowRuntimeError("non-finite value cannot enter canonical JSON")
        return value
    if hasattr(value, "as_py"):
        return _json_value(value.as_py())
    raise I5ShadowRuntimeError(f"unsupported canonical JSON value: {type(value).__name__}")


def _mapping(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise I5ShadowRuntimeError(f"{label} must be an object")
    return dict(value)


def _sequence(value: object, label: str) -> tuple[object, ...]:
    if not isinstance(value, list | tuple):
        raise I5ShadowRuntimeError(f"{label} must be an array")
    return tuple(value)


def _keys(value: Mapping[str, object], expected: set[str], label: str) -> None:
    if set(value) != expected:
        raise I5ShadowRuntimeError(f"{label} fields differ")


def _text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise I5ShadowRuntimeError(f"{label} must be a nonempty string")
    return value


def _relative(value: object, label: str) -> str:
    text = _text(value, label)
    candidate = PurePosixPath(text)
    if candidate.is_absolute() or ".." in candidate.parts or candidate.as_posix() != text:
        raise I5ShadowRuntimeError(f"{label} must be normalized and relative")
    return text


def _digest(value: object, label: str) -> str:
    text = _text(value, label)
    if _DIGEST.fullmatch(text) is None:
        raise I5ShadowRuntimeError(f"{label} must be a lowercase SHA-256 digest")
    return text


def _nonnegative_int(value: object, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise I5ShadowRuntimeError(f"{label} must be a nonnegative integer")
    return value


def _positive_int(value: object, label: str) -> int:
    result = _nonnegative_int(value, label)
    if result == 0:
        raise I5ShadowRuntimeError(f"{label} must be positive")
    return result


def _date_value(value: object, label: str) -> date:
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError as exc:
            raise I5ShadowRuntimeError(f"{label} must be an ISO date") from exc
    raise I5ShadowRuntimeError(f"{label} must be a date")


def _sessions(value: object) -> tuple[date, ...]:
    if type(value) is not tuple or not value or not all(isinstance(item, date) for item in value):
        raise I5ShadowRuntimeError("comparison sessions must be a nonempty date tuple")
    if value != tuple(sorted(set(value))):
        raise I5ShadowRuntimeError("comparison sessions must be sorted and unique")
    return value


def _int_mapping(value: object, label: str) -> dict[str, int]:
    item = _mapping(value, label)
    return {str(key): _nonnegative_int(raw, f"{label} {key}") for key, raw in item.items()}


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


__all__ = [
    "I5_REQUIRED_COMPARISON_SESSIONS",
    "I5_SCOPE_ARTIFACT",
    "I5FullOracleSeamError",
    "I5ProductionFailureExerciseSeamError",
    "I5ProductionReadMeterSeamError",
    "I5ShadowRuntimeError",
    "ShadowRunCompletion",
    "ShadowRunConfig",
    "ShadowRunResult",
    "ShadowRunSpec",
    "execute_i5_shadow_run",
    "load_i5_shadow_completion_exact",
]
