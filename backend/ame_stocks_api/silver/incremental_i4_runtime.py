"""Filesystem runtime for an approved S7.5 I4 registry correction.

This module stops at immutable ``awaiting_review`` controls.  It has no
pointer, publish, network, or approval-minting API.  The sealed I4 factory owns
all correction semantics; this runtime only exact-reads approved inputs,
enforces local execution guards, and writes a reviewable overlay checkpoint
and release candidate without modifying the parent release.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import resource
import shutil
import sys
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path, PurePosixPath
from typing import Self

from ame_stocks_api.artifacts import safe_relative_path, stable_digest, write_bytes_immutable
from ame_stocks_api.silver.incremental_contract import (
    ArtifactPin,
    IncrementalReleaseManifest,
    ManifestPin,
    PartitionReceipt,
    RowSemanticProofReceipt,
    RowVersionOperation,
    RowVersionReceipt,
    RunReceipt,
)
from ame_stocks_api.silver.incremental_gate import (
    CorrectionAuthorization,
    CorrectionAuthorizedAction,
    GateArtifactPin,
    GateEvidencePin,
    PinnedCorrectionAuthorization,
)
from ame_stocks_api.silver.incremental_i3_checkpoint import (
    I3CheckpointState,
    IdentityPolicyBundle,
    IdentityRegistryKind,
    OpenAliasState,
)
from ame_stocks_api.silver.incremental_i3_dispatch import IdentityPolicySnapshot
from ame_stocks_api.silver.incremental_i4_correction import (
    ExactIdentityGroup,
    I4AliasStateLedgerEntry,
    I4AliasStateLedgerRelease,
    I4ApprovalEvent,
    I4ApprovalLedgerEntry,
    I4ApprovalLedgerRelease,
    I4RegistryChangeLedgerRelease,
    I4RegistryLedgerEntry,
    ProductionI4CorrectionCapability,
    RegistryChangeOperation,
    mint_production_i4_correction_capability,
)

I4_RUNTIME_RULE_VERSION = "s7_5_i4_registry_correction_runtime_v2"
I4_AUTHORITY_ENVELOPE_RULE_VERSION = "s7_5_i4_runtime_authority_envelope_v1"
I4_RUNTIME_STATE = "awaiting_review"
_CONTROL_ROOT = "manifests/silver/identity/s7-5-i4-correction-staging"
_LOCK_ROOT = "locks/silver/identity/s7-5-i4-correction-staging"


class I4RuntimeError(RuntimeError):
    """Fail-closed runtime error, optionally naming an immutable failed receipt."""

    def __init__(self, message: str, *, failed_receipt_pin: ArtifactPin | None = None) -> None:
        super().__init__(message)
        self.failed_receipt_pin = failed_receipt_pin


@dataclass(frozen=True, slots=True)
class I4RuntimeResourceCaps:
    rss_cap_bytes: int
    disk_floor_bytes: int
    wall_clock_cap_seconds: int

    def __post_init__(self) -> None:
        for value, label in (
            (self.rss_cap_bytes, "RSS cap"),
            (self.disk_floor_bytes, "disk floor"),
            (self.wall_clock_cap_seconds, "wall-clock cap"),
        ):
            if type(value) is not int or value <= 0:
                raise I4RuntimeError(f"{label} must be a positive integer")

    def to_dict(self) -> dict[str, int]:
        return {
            "disk_floor_bytes": self.disk_floor_bytes,
            "rss_cap_bytes": self.rss_cap_bytes,
            "wall_clock_cap_seconds": self.wall_clock_cap_seconds,
        }

    @classmethod
    def from_dict(cls, value: object) -> Self:
        item = _closed(value, {"disk_floor_bytes", "rss_cap_bytes", "wall_clock_cap_seconds"})
        return cls(
            rss_cap_bytes=_positive_int(item["rss_cap_bytes"], "RSS cap"),
            disk_floor_bytes=_positive_int(item["disk_floor_bytes"], "disk floor"),
            wall_clock_cap_seconds=_positive_int(item["wall_clock_cap_seconds"], "wall-clock cap"),
        )


@dataclass(frozen=True, slots=True)
class I4CorrectionPrepareConfig:
    parent_manifest_pin: ManifestPin
    parent_run_receipt_artifact: ArtifactPin
    parent_checkpoint_artifact: ArtifactPin
    replacement_partition_receipts: tuple[PartitionReceipt, ...]
    prior_policy_snapshot_artifact: ArtifactPin
    target_policy_snapshot_artifact: ArtifactPin
    target_policy_bundle_artifact: ArtifactPin
    registry_ledger_artifact: ArtifactPin
    alias_state_ledger_artifact: ArtifactPin
    authorization_artifact: ArtifactPin
    approval_event_artifact: ArtifactPin
    approval_ledger_artifact: ArtifactPin
    alias_row_artifact: ArtifactPin
    alias_proof_artifact: ArtifactPin
    row_receipt_digest: str
    availability_cutoff_session: date
    resource_caps: I4RuntimeResourceCaps
    correction_cause: str = "registry_correction"

    def __post_init__(self) -> None:
        if self.correction_cause != "registry_correction":
            raise I4RuntimeError(
                "late-source runtime is disabled pending S4HistoricalSourceCorrectionReceipt"
            )
        if not isinstance(self.parent_manifest_pin, ManifestPin):
            raise I4RuntimeError("parent manifest pin is invalid")
        if (
            type(self.replacement_partition_receipts) is not tuple
            or not (self.replacement_partition_receipts)
            or not all(
                isinstance(item, PartitionReceipt) for item in self.replacement_partition_receipts
            )
        ):
            raise I4RuntimeError("runtime requires replacement partition receipts")
        sessions = tuple(item.partition_key for item in self.replacement_partition_receipts)
        if sessions != tuple(sorted(set(sessions))):
            raise I4RuntimeError("replacement sessions must be sorted and unique")
        pins = self.input_pins
        if len({item.path for item in pins}) != len(pins):
            raise I4RuntimeError("runtime exact input paths must be unique")
        _digest(self.row_receipt_digest, "row receipt digest")
        if type(self.availability_cutoff_session) is not date:
            raise I4RuntimeError("availability cutoff must be a date")
        if not isinstance(self.resource_caps, I4RuntimeResourceCaps):
            raise I4RuntimeError("runtime resource caps are invalid")

    @property
    def input_pins(self) -> tuple[ArtifactPin, ...]:
        manifest = ArtifactPin(
            path=self.parent_manifest_pin.manifest_path,
            sha256=self.parent_manifest_pin.manifest_sha256,
            bytes=self.parent_manifest_pin.manifest_bytes,
        )
        values = (
            manifest,
            self.parent_run_receipt_artifact,
            self.parent_checkpoint_artifact,
            self.prior_policy_snapshot_artifact,
            self.target_policy_snapshot_artifact,
            self.target_policy_bundle_artifact,
            self.registry_ledger_artifact,
            self.alias_state_ledger_artifact,
            self.authorization_artifact,
            self.approval_event_artifact,
            self.approval_ledger_artifact,
            self.alias_row_artifact,
            self.alias_proof_artifact,
            *(item.receipt for item in self.replacement_partition_receipts),
        )
        return tuple(sorted(values, key=lambda item: item.path))

    @property
    def config_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "alias_proof_artifact": self.alias_proof_artifact.to_dict(),
            "alias_row_artifact": self.alias_row_artifact.to_dict(),
            "alias_state_ledger_artifact": self.alias_state_ledger_artifact.to_dict(),
            "approval_event_artifact": self.approval_event_artifact.to_dict(),
            "approval_ledger_artifact": self.approval_ledger_artifact.to_dict(),
            "authorization_artifact": self.authorization_artifact.to_dict(),
            "availability_cutoff_session": self.availability_cutoff_session.isoformat(),
            "correction_cause": self.correction_cause,
            "parent_checkpoint_artifact": self.parent_checkpoint_artifact.to_dict(),
            "parent_manifest_pin": self.parent_manifest_pin.to_dict(),
            "parent_run_receipt_artifact": self.parent_run_receipt_artifact.to_dict(),
            "prior_policy_snapshot_artifact": self.prior_policy_snapshot_artifact.to_dict(),
            "registry_ledger_artifact": self.registry_ledger_artifact.to_dict(),
            "replacement_partition_receipts": [
                item.to_dict() for item in self.replacement_partition_receipts
            ],
            "resource_caps": self.resource_caps.to_dict(),
            "row_receipt_digest": self.row_receipt_digest,
            "rule_version": I4_RUNTIME_RULE_VERSION,
            "target_policy_bundle_artifact": self.target_policy_bundle_artifact.to_dict(),
            "target_policy_snapshot_artifact": self.target_policy_snapshot_artifact.to_dict(),
        }

    def to_dict(self) -> dict[str, object]:
        return {"config_id": self.config_id, **self.logical_payload()}

    def canonical_bytes(self) -> bytes:
        return _canonical(self.to_dict())

    @classmethod
    def from_dict(cls, value: object) -> Self:
        item = _closed(value, set(cls._fields()))
        if item["rule_version"] != I4_RUNTIME_RULE_VERSION:
            raise I4RuntimeError("runtime config rule version differs")
        result = cls(
            parent_manifest_pin=_manifest_pin(item["parent_manifest_pin"]),
            parent_run_receipt_artifact=_artifact(item["parent_run_receipt_artifact"]),
            parent_checkpoint_artifact=_artifact(item["parent_checkpoint_artifact"]),
            replacement_partition_receipts=tuple(
                _partition_receipt(value)
                for value in _array(item["replacement_partition_receipts"])
            ),
            prior_policy_snapshot_artifact=_artifact(item["prior_policy_snapshot_artifact"]),
            target_policy_snapshot_artifact=_artifact(item["target_policy_snapshot_artifact"]),
            target_policy_bundle_artifact=_artifact(item["target_policy_bundle_artifact"]),
            registry_ledger_artifact=_artifact(item["registry_ledger_artifact"]),
            alias_state_ledger_artifact=_artifact(item["alias_state_ledger_artifact"]),
            authorization_artifact=_artifact(item["authorization_artifact"]),
            approval_event_artifact=_artifact(item["approval_event_artifact"]),
            approval_ledger_artifact=_artifact(item["approval_ledger_artifact"]),
            alias_row_artifact=_artifact(item["alias_row_artifact"]),
            alias_proof_artifact=_artifact(item["alias_proof_artifact"]),
            row_receipt_digest=_text(item["row_receipt_digest"], "row receipt digest"),
            availability_cutoff_session=date.fromisoformat(
                _text(item["availability_cutoff_session"], "availability cutoff session")
            ),
            resource_caps=I4RuntimeResourceCaps.from_dict(item["resource_caps"]),
            correction_cause=_text(item["correction_cause"], "correction cause"),
        )
        if item["config_id"] != result.config_id:
            raise I4RuntimeError("runtime config ID does not reproduce")
        return result

    @staticmethod
    def _fields() -> tuple[str, ...]:
        return (
            "alias_proof_artifact",
            "alias_row_artifact",
            "alias_state_ledger_artifact",
            "approval_event_artifact",
            "approval_ledger_artifact",
            "authorization_artifact",
            "availability_cutoff_session",
            "config_id",
            "correction_cause",
            "parent_checkpoint_artifact",
            "parent_manifest_pin",
            "parent_run_receipt_artifact",
            "prior_policy_snapshot_artifact",
            "registry_ledger_artifact",
            "replacement_partition_receipts",
            "resource_caps",
            "row_receipt_digest",
            "rule_version",
            "target_policy_bundle_artifact",
            "target_policy_snapshot_artifact",
        )


@dataclass(frozen=True, slots=True)
class I4CorrectionRunSpec:
    config_artifact: ArtifactPin
    authority_artifact: ArtifactPin
    config_id: str
    exact_input_digest: str
    authority_digest: str

    def __post_init__(self) -> None:
        if not isinstance(self.config_artifact, ArtifactPin):
            raise I4RuntimeError("RunSpec config artifact is invalid")
        if not isinstance(self.authority_artifact, ArtifactPin):
            raise I4RuntimeError("RunSpec authority artifact is invalid")
        _digest(self.config_id, "RunSpec config ID")
        _digest(self.exact_input_digest, "RunSpec exact input digest")
        _digest(self.authority_digest, "RunSpec authority digest")

    @property
    def run_spec_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "artifact_type": "s7_5_i4_correction_run_spec",
            "authority_artifact": self.authority_artifact.to_dict(),
            "authority_digest": self.authority_digest,
            "config_artifact": self.config_artifact.to_dict(),
            "config_id": self.config_id,
            "exact_input_digest": self.exact_input_digest,
            "rule_version": I4_RUNTIME_RULE_VERSION,
        }

    def to_dict(self) -> dict[str, object]:
        return {"run_spec_id": self.run_spec_id, **self.logical_payload()}

    def canonical_bytes(self) -> bytes:
        return _canonical(self.to_dict())

    @classmethod
    def from_dict(cls, value: object) -> Self:
        item = _closed(
            value,
            {
                "artifact_type",
                "authority_artifact",
                "authority_digest",
                "config_artifact",
                "config_id",
                "exact_input_digest",
                "rule_version",
                "run_spec_id",
            },
        )
        if (
            item["artifact_type"] != "s7_5_i4_correction_run_spec"
            or item["rule_version"] != I4_RUNTIME_RULE_VERSION
        ):
            raise I4RuntimeError("runtime RunSpec type or rule differs")
        result = cls(
            config_artifact=_artifact(item["config_artifact"]),
            authority_artifact=_artifact(item["authority_artifact"]),
            config_id=_text(item["config_id"], "RunSpec config ID"),
            exact_input_digest=_text(item["exact_input_digest"], "RunSpec exact input digest"),
            authority_digest=_text(item["authority_digest"], "RunSpec authority digest"),
        )
        if item["run_spec_id"] != result.run_spec_id:
            raise I4RuntimeError("runtime RunSpec ID does not reproduce")
        return result


@dataclass(frozen=True, slots=True)
class I4RuntimeAuthorities:
    parent_manifest: IncrementalReleaseManifest
    parent_run_receipt: RunReceipt
    checkpoint: I3CheckpointState
    prior_policy_snapshot: IdentityPolicySnapshot
    target_policy_snapshot: IdentityPolicySnapshot
    registry_ledger: I4RegistryChangeLedgerRelease
    alias_state_ledger: I4AliasStateLedgerRelease
    authorization: PinnedCorrectionAuthorization
    approval_event: I4ApprovalEvent
    approval_ledger: I4ApprovalLedgerRelease
    added_row_version_receipts: tuple[RowVersionReceipt, ...]
    superseded_row_version_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        for value, expected, label in (
            (self.parent_manifest, IncrementalReleaseManifest, "parent manifest"),
            (self.parent_run_receipt, RunReceipt, "parent run receipt"),
            (self.checkpoint, I3CheckpointState, "parent checkpoint"),
            (self.prior_policy_snapshot, IdentityPolicySnapshot, "prior policy snapshot"),
            (self.target_policy_snapshot, IdentityPolicySnapshot, "target policy snapshot"),
            (self.registry_ledger, I4RegistryChangeLedgerRelease, "registry ledger"),
            (self.alias_state_ledger, I4AliasStateLedgerRelease, "alias-state ledger"),
            (self.authorization, PinnedCorrectionAuthorization, "authorization"),
            (self.approval_event, I4ApprovalEvent, "approval event"),
            (self.approval_ledger, I4ApprovalLedgerRelease, "approval ledger"),
        ):
            if not isinstance(value, expected):
                raise I4RuntimeError(f"runtime {label} authority is invalid")
        if type(self.added_row_version_receipts) is not tuple or not all(
            isinstance(item, RowVersionReceipt) for item in self.added_row_version_receipts
        ):
            raise I4RuntimeError("runtime row-version receipt authorities are invalid")
        if type(self.superseded_row_version_ids) is not tuple:
            raise I4RuntimeError("runtime superseded row-version IDs must be a tuple")
        for item in self.superseded_row_version_ids:
            _digest(item, "runtime superseded row-version ID")

    @property
    def authority_digest(self) -> str:
        return stable_digest(
            {
                "added_row_version_receipts": [
                    item.to_dict() for item in self.added_row_version_receipts
                ],
                "alias_state_ledger": self.alias_state_ledger.to_dict(),
                "approval_event": self.approval_event.to_dict(),
                "approval_ledger": self.approval_ledger.to_dict(),
                "authorization": {
                    "artifact": self.authorization.artifact.to_dict(),
                    "body": self.authorization.authorization.to_dict(),
                },
                "checkpoint": self.checkpoint.to_dict(),
                "parent_manifest": self.parent_manifest.to_dict(),
                "parent_run_receipt": self.parent_run_receipt.to_dict(),
                "prior_policy_snapshot": self.prior_policy_snapshot.to_dict(),
                "registry_ledger": self.registry_ledger.to_dict(),
                "superseded_row_version_ids": list(self.superseded_row_version_ids),
                "target_policy_snapshot": self.target_policy_snapshot.to_dict(),
            }
        )


@dataclass(frozen=True, slots=True)
class LoadedI4RuntimeAuthorities:
    """Durably replayed I4 authority set, independent of process-local objects."""

    completion_artifact: ArtifactPin
    run_spec_artifact: ArtifactPin
    run_spec: I4CorrectionRunSpec
    config: I4CorrectionPrepareConfig
    authorities: I4RuntimeAuthorities


@dataclass(frozen=True, slots=True)
class PreparedI4CorrectionRun:
    config: I4CorrectionPrepareConfig
    config_artifact: ArtifactPin
    run_spec: I4CorrectionRunSpec
    run_spec_artifact: ArtifactPin


@dataclass(frozen=True, slots=True)
class I4CorrectionStageResult:
    completion_pin: ArtifactPin
    checkpoint_candidate_pin: ArtifactPin
    release_candidate_pin: ArtifactPin
    capability: ProductionI4CorrectionCapability
    reused: bool


def prepare_i4_correction_run(
    data_root: Path,
    config: I4CorrectionPrepareConfig,
    *,
    authorities: I4RuntimeAuthorities | None = None,
) -> PreparedI4CorrectionRun:
    """Store an immutable registry-correction config and RunSpec."""

    root = data_root.expanduser().resolve()
    for pin in config.input_pins:
        _read_exact(root, pin)
    if authorities is None:
        authorities = _authorities_from_prepare_controls(root, config)
    _verify_authority_binding(root, config, authorities)
    config_relative = f"{_CONTROL_ROOT}/configs/config_id={config.config_id}/config.json"
    config_pin = _write(root, config_relative, config.canonical_bytes())
    authority_document = _authority_envelope(authorities)
    authority_content = _canonical(authority_document)
    authority_pin = _write(
        root,
        _authority_relative(hashlib.sha256(authority_content).hexdigest()),
        authority_content,
    )
    _verify_authority_envelope(root, authority_pin, authorities)
    run_spec = I4CorrectionRunSpec(
        config_artifact=config_pin,
        authority_artifact=authority_pin,
        config_id=config.config_id,
        exact_input_digest=_runtime_input_digest(config, authority_pin),
        authority_digest=authorities.authority_digest,
    )
    run_relative = f"{_CONTROL_ROOT}/run-specs/run_spec_id={run_spec.run_spec_id}/run-spec.json"
    run_pin = _write(root, run_relative, run_spec.canonical_bytes())
    return PreparedI4CorrectionRun(config, config_pin, run_spec, run_pin)


def stage_i4_correction(
    data_root: Path,
    run_spec_artifact: ArtifactPin,
    *,
    authorities: I4RuntimeAuthorities | None = None,
) -> I4CorrectionStageResult:
    """Stage one exact correction and stop at ``awaiting_review``."""

    root = data_root.expanduser().resolve()
    run_spec = _load_run_spec(root, run_spec_artifact)
    config = _load_config(root, run_spec.config_artifact)
    if config.config_id != run_spec.config_id:
        raise I4RuntimeError("RunSpec binds another runtime config")
    _verify_control_paths(run_spec_artifact, run_spec)
    if authorities is None:
        authorities = _authorities_from_exact_controls(
            root,
            config=config,
            authority_artifact=run_spec.authority_artifact,
        )
    _verify_run_spec_binding(run_spec, config, authorities)
    _verify_authority_envelope(root, run_spec.authority_artifact, authorities)
    completion_relative = _completion_relative(run_spec.run_spec_id)
    with _exclusive_lock(safe_relative_path(root, _lock_relative(run_spec.run_spec_id))):
        started = time.monotonic()
        try:
            if safe_relative_path(root, completion_relative).exists():
                return verify_i4_correction(
                    root,
                    _pin_existing(root, completion_relative),
                    authorities=authorities,
                    reused=True,
                )
            _check_resources(root, config.resource_caps, started)
            _verify_authority_binding(root, config, authorities)
            reader = _FilesystemExactReader(root)
            capability = mint_production_i4_correction_capability(
                parent_manifest=authorities.parent_manifest,
                parent_manifest_pin=config.parent_manifest_pin,
                parent_run_receipt=authorities.parent_run_receipt,
                checkpoint=authorities.checkpoint,
                parent_checkpoint_artifact=config.parent_checkpoint_artifact,
                replacement_partition_receipts=config.replacement_partition_receipts,
                prior_policy_snapshot=authorities.prior_policy_snapshot,
                target_policy_snapshot=authorities.target_policy_snapshot,
                target_policy_bundle_artifact=config.target_policy_bundle_artifact,
                registry_ledger=authorities.registry_ledger,
                registry_ledger_artifact=config.registry_ledger_artifact,
                late_source_ledger=None,
                late_source_ledger_artifact=None,
                alias_state_ledger=authorities.alias_state_ledger,
                alias_state_ledger_artifact=config.alias_state_ledger_artifact,
                added_row_version_receipts=authorities.added_row_version_receipts,
                superseded_row_version_ids=authorities.superseded_row_version_ids,
                authorization=authorities.authorization,
                approval_event=authorities.approval_event,
                approval_event_artifact=_gate(config.approval_event_artifact),
                approval_ledger=authorities.approval_ledger,
                approval_ledger_artifact=_gate(config.approval_ledger_artifact),
                availability_cutoff_session=config.availability_cutoff_session,
                artifact_reader=reader,
            )
            for receipt in config.replacement_partition_receipts:
                _read_exact(root, receipt.receipt)
            checkpoint_document = _checkpoint_candidate(
                config, authorities, capability, run_spec.run_spec_id
            )
            checkpoint_relative = (
                f"{_CONTROL_ROOT}/runs/run_spec_id={run_spec.run_spec_id}/checkpoint-candidate.json"
            )
            checkpoint_pin = _write(root, checkpoint_relative, _canonical(checkpoint_document))
            release_document = _release_candidate(
                config, authorities, capability, checkpoint_pin, run_spec.run_spec_id
            )
            release_relative = (
                f"{_CONTROL_ROOT}/runs/run_spec_id={run_spec.run_spec_id}/release-candidate.json"
            )
            release_pin = _write(root, release_relative, _canonical(release_document))
            _check_resources(root, config.resource_caps, started)
            completion = {
                "artifact_type": "s7_5_i4_correction_completion",
                "capability_digest": stable_digest(capability.to_dict()),
                "checkpoint_candidate": checkpoint_pin.to_dict(),
                "release_candidate": release_pin.to_dict(),
                "rule_version": I4_RUNTIME_RULE_VERSION,
                "run_spec_id": run_spec.run_spec_id,
                "state": I4_RUNTIME_STATE,
            }
            completion["completion_id"] = stable_digest(completion)
            completion_pin = _write(root, completion_relative, _canonical(completion))
            return verify_i4_correction(
                root,
                completion_pin,
                authorities=authorities,
                reused=False,
            )
        except Exception as exc:
            failed_pin = _write_failed(root, run_spec.run_spec_id, exc)
            raise I4RuntimeError(
                f"I4 correction staging failed: {type(exc).__name__}: {exc}",
                failed_receipt_pin=failed_pin,
            ) from exc


def verify_i4_correction(
    data_root: Path,
    completion_pin: ArtifactPin,
    *,
    authorities: I4RuntimeAuthorities | None = None,
    reused: bool = False,
) -> I4CorrectionStageResult:
    """Exact-read and replay an immutable awaiting-review correction candidate."""

    root = data_root.expanduser().resolve()
    path_run_spec_id = _run_spec_id_from_completion_path(completion_pin.path)
    completion = _closed(
        _read_canonical_object(root, completion_pin, "correction completion"),
        {
            "artifact_type",
            "capability_digest",
            "checkpoint_candidate",
            "completion_id",
            "release_candidate",
            "rule_version",
            "run_spec_id",
            "state",
        },
    )
    if (
        completion["artifact_type"] != "s7_5_i4_correction_completion"
        or completion["state"] != I4_RUNTIME_STATE
        or completion["rule_version"] != I4_RUNTIME_RULE_VERSION
    ):
        raise I4RuntimeError("correction completion is not awaiting_review under this runtime")
    expected_id = completion.pop("completion_id", None)
    if expected_id != stable_digest(completion):
        raise I4RuntimeError("correction completion ID does not reproduce")
    completion["completion_id"] = expected_id
    run_spec_id = _digest(completion["run_spec_id"], "completion RunSpec ID")
    if run_spec_id != path_run_spec_id:
        raise I4RuntimeError("completion artifact is outside its exact runtime path")
    run_pin = _pin_existing(
        root, f"{_CONTROL_ROOT}/run-specs/run_spec_id={run_spec_id}/run-spec.json"
    )
    run_spec = _load_run_spec(root, run_pin)
    config = _load_config(root, run_spec.config_artifact)
    _verify_control_paths(run_pin, run_spec)
    if authorities is None:
        authorities = _authorities_from_exact_controls(
            root,
            config=config,
            authority_artifact=run_spec.authority_artifact,
        )
    _verify_run_spec_binding(run_spec, config, authorities)
    _verify_authority_envelope(root, run_spec.authority_artifact, authorities)
    _verify_authority_binding(root, config, authorities)
    checkpoint_pin = _artifact(completion["checkpoint_candidate"])
    release_pin = _artifact(completion["release_candidate"])
    expected_run_root = f"{_CONTROL_ROOT}/runs/run_spec_id={run_spec_id}"
    if checkpoint_pin.path != f"{expected_run_root}/checkpoint-candidate.json" or (
        release_pin.path != f"{expected_run_root}/release-candidate.json"
    ):
        raise I4RuntimeError("completion candidate paths differ from their RunSpec")
    checkpoint = _read_canonical_object(root, checkpoint_pin, "checkpoint candidate")
    release = _read_canonical_object(root, release_pin, "release candidate")
    if checkpoint.get("state") != I4_RUNTIME_STATE or release.get("state") != I4_RUNTIME_STATE:
        raise I4RuntimeError("correction candidates lost awaiting_review state")
    _verify_self_hash(checkpoint, "checkpoint_candidate_id", "checkpoint candidate")
    _verify_self_hash(release, "release_candidate_id", "release candidate")
    reader = _FilesystemExactReader(root)
    capability = mint_production_i4_correction_capability(
        parent_manifest=authorities.parent_manifest,
        parent_manifest_pin=config.parent_manifest_pin,
        parent_run_receipt=authorities.parent_run_receipt,
        checkpoint=authorities.checkpoint,
        parent_checkpoint_artifact=config.parent_checkpoint_artifact,
        replacement_partition_receipts=config.replacement_partition_receipts,
        prior_policy_snapshot=authorities.prior_policy_snapshot,
        target_policy_snapshot=authorities.target_policy_snapshot,
        target_policy_bundle_artifact=config.target_policy_bundle_artifact,
        registry_ledger=authorities.registry_ledger,
        registry_ledger_artifact=config.registry_ledger_artifact,
        late_source_ledger=None,
        late_source_ledger_artifact=None,
        alias_state_ledger=authorities.alias_state_ledger,
        alias_state_ledger_artifact=config.alias_state_ledger_artifact,
        added_row_version_receipts=authorities.added_row_version_receipts,
        superseded_row_version_ids=authorities.superseded_row_version_ids,
        authorization=authorities.authorization,
        approval_event=authorities.approval_event,
        approval_event_artifact=_gate(config.approval_event_artifact),
        approval_ledger=authorities.approval_ledger,
        approval_ledger_artifact=_gate(config.approval_ledger_artifact),
        availability_cutoff_session=config.availability_cutoff_session,
        artifact_reader=reader,
    )
    if completion["capability_digest"] != stable_digest(capability.to_dict()):
        raise I4RuntimeError("completion capability digest does not replay")
    expected_checkpoint = _checkpoint_candidate(
        config, authorities, capability, run_spec.run_spec_id
    )
    if checkpoint != expected_checkpoint:
        raise I4RuntimeError("checkpoint candidate does not reproduce from sealed capability")
    expected_release = _release_candidate(
        config, authorities, capability, checkpoint_pin, run_spec.run_spec_id
    )
    if release != expected_release:
        raise I4RuntimeError("release candidate does not reproduce from sealed capability")
    return I4CorrectionStageResult(completion_pin, checkpoint_pin, release_pin, capability, reused)


def load_i4_runtime_authorities_exact(
    data_root: Path,
    completion_artifact: ArtifactPin,
) -> LoadedI4RuntimeAuthorities:
    """Rebuild the sealed I4 authority set from canonical immutable bytes only."""

    root = data_root.expanduser().resolve()
    path_run_spec_id = _run_spec_id_from_completion_path(completion_artifact.path)
    completion = _closed(
        _read_canonical_object(root, completion_artifact, "correction completion"),
        {
            "artifact_type",
            "capability_digest",
            "checkpoint_candidate",
            "completion_id",
            "release_candidate",
            "rule_version",
            "run_spec_id",
            "state",
        },
    )
    if (
        completion["artifact_type"] != "s7_5_i4_correction_completion"
        or completion["state"] != I4_RUNTIME_STATE
        or completion["rule_version"] != I4_RUNTIME_RULE_VERSION
    ):
        raise I4RuntimeError("correction completion is not an I4 runtime authority")
    observed_completion_id = completion.pop("completion_id", None)
    if observed_completion_id != stable_digest(completion):
        raise I4RuntimeError("correction completion ID does not reproduce")
    run_spec_id = _digest(completion["run_spec_id"], "completion RunSpec ID")
    if run_spec_id != path_run_spec_id:
        raise I4RuntimeError("completion artifact is outside its exact runtime path")
    run_relative = f"{_CONTROL_ROOT}/run-specs/run_spec_id={run_spec_id}/run-spec.json"
    run_pin = _pin_existing(root, run_relative)
    run_spec = _load_run_spec(root, run_pin)
    config = _load_config(root, run_spec.config_artifact)
    _verify_control_paths(run_pin, run_spec)
    authorities = _authorities_from_exact_controls(
        root,
        config=config,
        authority_artifact=run_spec.authority_artifact,
    )
    _verify_run_spec_binding(run_spec, config, authorities)
    _verify_authority_binding(root, config, authorities)
    _verify_authority_envelope(root, run_spec.authority_artifact, authorities)
    return LoadedI4RuntimeAuthorities(
        completion_artifact=completion_artifact,
        run_spec_artifact=run_pin,
        run_spec=run_spec,
        config=config,
        authorities=authorities,
    )


def _authorities_from_exact_controls(
    root: Path,
    *,
    config: I4CorrectionPrepareConfig,
    authority_artifact: ArtifactPin,
) -> I4RuntimeAuthorities:
    from ame_stocks_api.silver import incremental_i3_production_contract as i3_contract

    envelope = _closed(
        _read_canonical_object(root, authority_artifact, "runtime authority envelope"),
        {
            "added_row_version_receipts",
            "artifact_type",
            "authority_digest",
            "authority_envelope_id",
            "prior_policy_bundle",
            "rule_version",
            "superseded_row_version_ids",
        },
    )
    if (
        envelope["artifact_type"] != "s7_5_i4_runtime_authority_envelope"
        or envelope["rule_version"] != I4_AUTHORITY_ENVELOPE_RULE_VERSION
    ):
        raise I4RuntimeError("runtime authority envelope type or rule differs")
    _verify_self_hash(envelope, "authority_envelope_id", "runtime authority envelope")
    if authority_artifact.path != _authority_relative(authority_artifact.sha256):
        raise I4RuntimeError("runtime authority envelope path is noncanonical")
    prior_bundle = IdentityPolicyBundle.from_dict(envelope["prior_policy_bundle"])
    added_receipts = tuple(
        i3_contract._row_version_receipt_from_dict(item)
        for item in _array(envelope["added_row_version_receipts"])
    )
    superseded_ids = tuple(
        _text(item, "superseded row-version ID")
        for item in _array(envelope["superseded_row_version_ids"])
    )
    authorities = _authorities_from_source_facts(
        root,
        config=config,
        prior_bundle=prior_bundle,
        added_receipts=added_receipts,
        superseded_ids=superseded_ids,
    )
    if envelope["authority_digest"] != authorities.authority_digest:
        raise I4RuntimeError("runtime authority digest does not reproduce from exact controls")
    return authorities


def _authorities_from_source_facts(
    root: Path,
    *,
    config: I4CorrectionPrepareConfig,
    prior_bundle: IdentityPolicyBundle,
    added_receipts: tuple[RowVersionReceipt, ...],
    superseded_ids: tuple[str, ...],
) -> I4RuntimeAuthorities:
    from ame_stocks_api.silver import incremental_i3_production_contract as i3_contract

    parent_manifest_document = _read_canonical_object(
        root,
        ArtifactPin(
            path=config.parent_manifest_pin.manifest_path,
            sha256=config.parent_manifest_pin.manifest_sha256,
            bytes=config.parent_manifest_pin.manifest_bytes,
        ),
        "I4 parent release manifest",
    )
    parent_manifest = i3_contract._gate_a_manifest_from_dict(parent_manifest_document)
    if (
        parent_manifest.to_dict() != parent_manifest_document
        or parent_manifest.release_id != config.parent_manifest_pin.release_id
        or parent_manifest.release_available_session
        != config.parent_manifest_pin.release_available_session
    ):
        raise I4RuntimeError("I4 parent release manifest differs from its exact pin")

    parent_receipt_document = _read_canonical_object(
        root,
        config.parent_run_receipt_artifact,
        "I4 parent run receipt",
    )
    parent_run_receipt = i3_contract._gate_a_run_receipt_from_dict(parent_receipt_document)
    if parent_run_receipt.to_dict() != parent_receipt_document:
        raise I4RuntimeError("I4 parent run receipt does not reproduce")

    checkpoint_document = _read_canonical_object(
        root,
        config.parent_checkpoint_artifact,
        "I4 parent checkpoint",
    )
    checkpoint = I3CheckpointState.from_dict(checkpoint_document)
    if checkpoint.to_dict() != checkpoint_document:
        raise I4RuntimeError("I4 parent checkpoint does not reproduce")

    if prior_bundle.identity_policy_bundle_id != parent_manifest.identity_policy_bundle_id:
        raise I4RuntimeError("I4 prior policy differs from its parent release")
    target_bundle_content = _read_exact(root, config.target_policy_bundle_artifact)
    target_bundle_document = _strict_json(target_bundle_content)
    if _canonical(target_bundle_document) != target_bundle_content:
        raise I4RuntimeError("I4 target policy bundle is not canonical")
    target_bundle = IdentityPolicyBundle.from_dict(target_bundle_document)
    if target_bundle.canonical_bytes() != target_bundle_content:
        raise I4RuntimeError("I4 target policy bundle does not reproduce")
    prior_policy_snapshot = _load_policy_snapshot_exact(
        root,
        bundle=prior_bundle,
        snapshot_artifact=config.prior_policy_snapshot_artifact,
        label="prior",
    )
    target_policy_snapshot = _load_policy_snapshot_exact(
        root,
        bundle=target_bundle,
        snapshot_artifact=config.target_policy_snapshot_artifact,
        label="target",
    )

    registry_ledger = _registry_ledger_from_dict(
        _read_canonical_object(root, config.registry_ledger_artifact, "I4 registry ledger")
    )
    alias_state_ledger = _alias_state_ledger_from_dict(
        _read_canonical_object(root, config.alias_state_ledger_artifact, "I4 alias-state ledger")
    )
    authorization = _pinned_authorization_from_dict(
        _read_canonical_object(root, config.authorization_artifact, "I4 authorization"),
        config.authorization_artifact,
    )
    approval_event = _approval_event_from_dict(
        _read_canonical_object(root, config.approval_event_artifact, "I4 approval event")
    )
    approval_ledger = _approval_ledger_from_dict(
        _read_canonical_object(root, config.approval_ledger_artifact, "I4 approval ledger")
    )
    authorities = I4RuntimeAuthorities(
        parent_manifest=parent_manifest,
        parent_run_receipt=parent_run_receipt,
        checkpoint=checkpoint,
        prior_policy_snapshot=prior_policy_snapshot,
        target_policy_snapshot=target_policy_snapshot,
        registry_ledger=registry_ledger,
        alias_state_ledger=alias_state_ledger,
        authorization=authorization,
        approval_event=approval_event,
        approval_ledger=approval_ledger,
        added_row_version_receipts=added_receipts,
        superseded_row_version_ids=superseded_ids,
    )
    return authorities


def _authorities_from_prepare_controls(
    root: Path,
    config: I4CorrectionPrepareConfig,
) -> I4RuntimeAuthorities:
    prior_bundle = _prior_policy_bundle_from_parent(root, config)
    proof_document = _read_canonical_object(
        root,
        config.alias_proof_artifact,
        "I4 alias semantic proof",
    )
    proof = _semantic_proof_receipt_from_artifact(proof_document, config.alias_proof_artifact)
    alias_content = _read_exact(root, config.alias_row_artifact)
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq

        rows = pq.read_table(pa.BufferReader(alias_content)).to_pylist()
    except Exception as exc:
        raise I4RuntimeError("I4 alias artifact is not readable Parquet") from exc
    if len(rows) != 1:
        raise I4RuntimeError("I4 alias artifact must contain exactly one reviewed row")
    availability = rows[0].get("alias_version_available_session")
    if isinstance(availability, str):
        availability = _date_value(availability, "alias row availability")
    if type(availability) is not date:
        raise I4RuntimeError("I4 alias row availability is not a session")
    receipt = RowVersionReceipt(
        table_name=proof.table_name,
        stable_row_key=proof.stable_row_key,
        row_version_id=proof.row_version_id,
        predecessor_row_version_id=proof.predecessor_row_version_id,
        operation=proof.operation,
        availability_session=availability,
        index_artifact=config.alias_row_artifact,
        row_locator="row_index=0",
        row_payload_digest=proof.row_payload_digest,
        semantic_proof=proof,
    )
    if receipt.predecessor_row_version_id is None:
        raise I4RuntimeError("I4 reviewed alias correction lacks a predecessor")
    if config.row_receipt_digest != stable_digest([receipt.to_dict()]):
        raise I4RuntimeError("I4 row receipt cannot be derived from exact alias evidence")
    return _authorities_from_source_facts(
        root,
        config=config,
        prior_bundle=prior_bundle,
        added_receipts=(receipt,),
        superseded_ids=(receipt.predecessor_row_version_id,),
    )


def _semantic_proof_receipt_from_artifact(
    value: object,
    artifact: ArtifactPin,
) -> RowSemanticProofReceipt:
    item = _closed(
        value,
        {
            "artifact_type",
            "operation",
            "predecessor_payload_digest",
            "predecessor_row_version_id",
            "proof_id",
            "row_payload_digest",
            "row_version_id",
            "rule_version",
            "stable_row_key",
            "table_name",
            "validator_semantics_digest",
        },
    )
    _verify_self_hash(item, "proof_id", "I4 alias semantic proof")
    if (
        item["artifact_type"] != "s7_5_i3_production_row_semantic_proof"
        or item["rule_version"] != "s7_5_i3_production_row_semantic_proof_v1"
    ):
        raise I4RuntimeError("I4 alias semantic proof type or rule differs")
    try:
        operation = RowVersionOperation(_text(item["operation"], "alias proof operation"))
    except ValueError as exc:
        raise I4RuntimeError("I4 alias proof operation is invalid") from exc
    return RowSemanticProofReceipt(
        table_name=_text(item["table_name"], "alias proof table"),
        stable_row_key=_text(item["stable_row_key"], "alias proof stable key"),
        row_version_id=_text(item["row_version_id"], "alias proof row version"),
        predecessor_row_version_id=_optional_text_value(
            item["predecessor_row_version_id"], "alias proof predecessor"
        ),
        operation=operation,
        row_payload_digest=_text(item["row_payload_digest"], "alias proof payload"),
        predecessor_payload_digest=_optional_text_value(
            item["predecessor_payload_digest"], "alias proof predecessor payload"
        ),
        validator_semantics_digest=_text(
            item["validator_semantics_digest"], "alias proof validator"
        ),
        artifact=artifact,
    )


def _prior_policy_bundle_from_parent(
    root: Path,
    config: I4CorrectionPrepareConfig,
) -> IdentityPolicyBundle:
    from ame_stocks_api.silver import incremental_i3_production_contract as i3_contract

    manifest_document = _read_canonical_object(
        root,
        ArtifactPin(
            path=config.parent_manifest_pin.manifest_path,
            sha256=config.parent_manifest_pin.manifest_sha256,
            bytes=config.parent_manifest_pin.manifest_bytes,
        ),
        "I4 parent release manifest",
    )
    manifest = i3_contract._gate_a_manifest_from_dict(manifest_document)
    if manifest.to_dict() != manifest_document:
        raise I4RuntimeError("I4 parent manifest does not reproduce")
    gate_spec_document = _read_canonical_object(
        root,
        manifest.run_spec_pin.artifact,
        "I4 parent Gate-A RunSpec",
    )
    gate_spec = i3_contract._gate_a_run_spec_from_dict(gate_spec_document)
    if gate_spec.to_dict() != gate_spec_document:
        raise I4RuntimeError("I4 parent Gate-A RunSpec does not reproduce")
    matches: list[IdentityPolicyBundle] = []
    for pin in gate_spec.input_pins:
        raw = _read_exact(root, pin)
        try:
            candidate_document = _strict_json(raw)
            if _canonical(candidate_document) != raw:
                continue
            candidate = IdentityPolicyBundle.from_dict(candidate_document)
        except Exception:
            continue
        if candidate.identity_policy_bundle_id == manifest.identity_policy_bundle_id:
            matches.append(candidate)
    if len(matches) != 1:
        raise I4RuntimeError("I4 parent policy bundle is not uniquely bound by its Gate-A RunSpec")
    return matches[0]


def _load_policy_snapshot_exact(
    root: Path,
    *,
    bundle: IdentityPolicyBundle,
    snapshot_artifact: ArtifactPin,
    label: str,
) -> IdentityPolicySnapshot:
    from ame_stocks_api.silver.identity_registry_workflow import (
        RegistryReleasePin,
        load_registry_release_set,
    )
    from ame_stocks_api.silver.incremental_i3_production_policy import (
        load_production_identity_policy_snapshot,
    )

    pins = tuple(
        RegistryReleasePin(
            registry_name=item.registry_kind.value,
            release_id=item.release_id,
            manifest_path=item.artifact.path,
            manifest_sha256=item.artifact.sha256,
            manifest_bytes=item.artifact.bytes,
            release_available_session=item.release_available_session,
        )
        for item in bundle.registry_releases
    )
    try:
        releases = load_registry_release_set(
            root,
            pins,
            revalidate_current_runtime=False,
        )
        snapshot = load_production_identity_policy_snapshot(releases, bundle)
    except Exception as exc:
        raise I4RuntimeError(
            f"I4 {label} production policy cannot be replayed: {type(exc).__name__}: {exc}"
        ) from exc
    expected = _canonical(snapshot.to_dict())
    if _read_exact(root, snapshot_artifact) != expected:
        raise I4RuntimeError(f"I4 {label} policy snapshot differs from registry releases")
    return snapshot


def _registry_ledger_from_dict(value: object) -> I4RegistryChangeLedgerRelease:
    item = _closed(
        value,
        {
            "entries",
            "ledger_release_id",
            "previous_ledger_release_id",
            "release_available_session",
            "release_sequence",
            "rule_version",
        },
    )
    entries: list[I4RegistryLedgerEntry] = []
    for value in _array(item["entries"]):
        row = _closed(
            value,
            {
                "change_available_session",
                "change_decision_artifact",
                "change_decision_id",
                "change_registry_release_artifact",
                "change_registry_release_id",
                "entry_id",
                "entry_sequence",
                "operation",
                "predecessor_decision_artifact",
                "predecessor_decision_id",
                "predecessor_registry_release_artifact",
                "predecessor_registry_release_id",
                "registry_kind",
                "withdrawal_reason_artifact",
                "withdrawal_reason_code",
            },
        )
        entry = I4RegistryLedgerEntry(
            entry_sequence=_positive_int(row["entry_sequence"], "registry ledger sequence"),
            registry_kind=IdentityRegistryKind(
                _text(row["registry_kind"], "registry responsibility")
            ),
            operation=RegistryChangeOperation(_text(row["operation"], "registry operation")),
            predecessor_decision_id=_text(
                row["predecessor_decision_id"], "predecessor decision ID"
            ),
            predecessor_decision_artifact=_artifact(row["predecessor_decision_artifact"]),
            predecessor_registry_release_id=_text(
                row["predecessor_registry_release_id"], "predecessor release ID"
            ),
            predecessor_registry_release_artifact=_artifact(
                row["predecessor_registry_release_artifact"]
            ),
            change_decision_id=_text(row["change_decision_id"], "change decision ID"),
            change_decision_artifact=_artifact(row["change_decision_artifact"]),
            change_registry_release_id=_text(
                row["change_registry_release_id"], "change release ID"
            ),
            change_registry_release_artifact=_artifact(row["change_registry_release_artifact"]),
            change_available_session=_date_value(
                row["change_available_session"], "registry change availability"
            ),
            withdrawal_reason_code=_optional_text_value(
                row["withdrawal_reason_code"], "withdrawal reason code"
            ),
            withdrawal_reason_artifact=(
                None
                if row["withdrawal_reason_artifact"] is None
                else _artifact(row["withdrawal_reason_artifact"])
            ),
        )
        if entry.to_dict() != row:
            raise I4RuntimeError("registry ledger entry does not reproduce")
        entries.append(entry)
    ledger = I4RegistryChangeLedgerRelease(
        release_sequence=_positive_int(item["release_sequence"], "registry release sequence"),
        previous_ledger_release_id=_optional_text_value(
            item["previous_ledger_release_id"], "previous registry ledger release ID"
        ),
        release_available_session=_date_value(
            item["release_available_session"], "registry ledger availability"
        ),
        entries=tuple(entries),
    )
    if ledger.to_dict() != item:
        raise I4RuntimeError("registry ledger release does not reproduce")
    return ledger


def _alias_state_ledger_from_dict(value: object) -> I4AliasStateLedgerRelease:
    item = _closed(
        value,
        {
            "entries",
            "ledger_release_id",
            "previous_ledger_release_id",
            "release_available_session",
            "release_sequence",
            "rule_version",
        },
    )
    entries: list[I4AliasStateLedgerEntry] = []
    for value in _array(item["entries"]):
        row = _closed(
            value,
            {
                "corrected_open_alias",
                "entry_sequence",
                "group",
                "parent_open_alias",
                "session_date",
            },
        )
        group_item = _closed(
            row["group"],
            {"provider_id", "provider_locale", "provider_market", "ticker"},
        )
        group = ExactIdentityGroup(
            provider_id=_text(group_item["provider_id"], "alias group provider"),
            provider_market=_text(group_item["provider_market"], "alias group market"),
            provider_locale=_text(group_item["provider_locale"], "alias group locale"),
            ticker=_text(group_item["ticker"], "alias group ticker"),
        )
        entry = I4AliasStateLedgerEntry(
            entry_sequence=_positive_int(row["entry_sequence"], "alias ledger sequence"),
            group=group,
            session_date=_date_value(row["session_date"], "alias ledger session"),
            parent_open_alias=OpenAliasState.from_dict(row["parent_open_alias"]),
            corrected_open_alias=OpenAliasState.from_dict(row["corrected_open_alias"]),
        )
        if entry.to_dict() != row:
            raise I4RuntimeError("alias-state ledger entry does not reproduce")
        entries.append(entry)
    ledger = I4AliasStateLedgerRelease(
        release_sequence=_positive_int(item["release_sequence"], "alias release sequence"),
        previous_ledger_release_id=_optional_text_value(
            item["previous_ledger_release_id"], "previous alias ledger release ID"
        ),
        release_available_session=_date_value(
            item["release_available_session"], "alias ledger availability"
        ),
        entries=tuple(entries),
    )
    if ledger.to_dict() != item:
        raise I4RuntimeError("alias-state ledger release does not reproduce")
    return ledger


def _pinned_authorization_from_dict(
    value: object,
    artifact: ArtifactPin,
) -> PinnedCorrectionAuthorization:
    item = _closed(
        value,
        {
            "approval_available_session",
            "approval_event_id",
            "approval_event_sha256",
            "approver_id",
            "authorization_id",
            "authorized_action",
            "calendar_digest",
            "evidence_pins",
            "expected_change_set_digest",
            "identity_policy_after_id",
            "identity_policy_before_id",
            "literal_version",
            "parent_release_id",
            "schema_digest",
            "scope_digest",
            "source_binding_digest",
            "transform_semantics_digest",
        },
    )
    evidence: list[GateEvidencePin] = []
    for value in _array(item["evidence_pins"]):
        row = _closed(value, {"artifact", "available_session"})
        evidence.append(
            GateEvidencePin(
                artifact=_gate_artifact(row["artifact"]),
                available_session=_date_value(row["available_session"], "evidence availability"),
            )
        )
    authorization = CorrectionAuthorization(
        authorized_action=CorrectionAuthorizedAction(
            _text(item["authorized_action"], "authorized action")
        ),
        literal_version=_text(item["literal_version"], "authorization literal version"),
        parent_release_id=_text(item["parent_release_id"], "authorization parent release"),
        expected_change_set_digest=_text(
            item["expected_change_set_digest"], "authorization change set"
        ),
        source_binding_digest=_text(item["source_binding_digest"], "authorization source binding"),
        schema_digest=_text(item["schema_digest"], "authorization schema"),
        transform_semantics_digest=_text(
            item["transform_semantics_digest"], "authorization transform"
        ),
        calendar_digest=_text(item["calendar_digest"], "authorization calendar"),
        identity_policy_before_id=_text(
            item["identity_policy_before_id"], "authorization prior policy"
        ),
        identity_policy_after_id=_text(
            item["identity_policy_after_id"], "authorization target policy"
        ),
        scope_digest=_text(item["scope_digest"], "authorization scope"),
        approval_event_id=_text(item["approval_event_id"], "authorization event ID"),
        approval_event_sha256=_text(item["approval_event_sha256"], "authorization event SHA-256"),
        approver_id=_text(item["approver_id"], "authorization approver"),
        approval_available_session=_date_value(
            item["approval_available_session"], "authorization availability"
        ),
        evidence_pins=tuple(evidence),
    )
    if authorization.to_dict() != item:
        raise I4RuntimeError("correction authorization does not reproduce")
    return PinnedCorrectionAuthorization(authorization=authorization, artifact=_gate(artifact))


def _approval_event_from_dict(value: object) -> I4ApprovalEvent:
    item = _closed(
        value,
        {
            "approval_event_id",
            "approver_id",
            "authorized_action",
            "calendar_digest",
            "event_available_session",
            "expected_change_set_digest",
            "identity_policy_after_id",
            "identity_policy_before_id",
            "parent_release_id",
            "rule_version",
            "schema_digest",
            "scope_digest",
            "source_binding_digest",
            "transform_semantics_digest",
        },
    )
    event = I4ApprovalEvent(
        authorized_action=CorrectionAuthorizedAction(
            _text(item["authorized_action"], "approval event action")
        ),
        parent_release_id=_text(item["parent_release_id"], "approval parent release"),
        expected_change_set_digest=_text(item["expected_change_set_digest"], "approval change set"),
        source_binding_digest=_text(item["source_binding_digest"], "approval source binding"),
        schema_digest=_text(item["schema_digest"], "approval schema"),
        transform_semantics_digest=_text(item["transform_semantics_digest"], "approval transform"),
        calendar_digest=_text(item["calendar_digest"], "approval calendar"),
        identity_policy_before_id=_text(item["identity_policy_before_id"], "approval prior policy"),
        identity_policy_after_id=_text(item["identity_policy_after_id"], "approval target policy"),
        scope_digest=_text(item["scope_digest"], "approval scope"),
        approver_id=_text(item["approver_id"], "approval approver"),
        event_available_session=_date_value(
            item["event_available_session"], "approval event availability"
        ),
    )
    if event.to_dict() != item:
        raise I4RuntimeError("approval event does not reproduce")
    return event


def _approval_ledger_from_dict(value: object) -> I4ApprovalLedgerRelease:
    item = _closed(
        value,
        {
            "entries",
            "ledger_release_id",
            "previous_ledger_release_id",
            "release_available_session",
            "release_sequence",
            "rule_version",
        },
    )
    entries: list[I4ApprovalLedgerEntry] = []
    for value in _array(item["entries"]):
        row = _closed(
            value,
            {
                "approval_event_id",
                "authorization_artifact",
                "authorization_id",
                "event_artifact",
                "ledger_index",
                "recorded_available_session",
            },
        )
        entry = I4ApprovalLedgerEntry(
            ledger_index=_positive_int(row["ledger_index"], "approval ledger index"),
            authorization_id=_text(row["authorization_id"], "approval authorization ID"),
            authorization_artifact=_gate_artifact(row["authorization_artifact"]),
            approval_event_id=_text(row["approval_event_id"], "approval event ID"),
            event_artifact=_gate_artifact(row["event_artifact"]),
            recorded_available_session=_date_value(
                row["recorded_available_session"], "approval ledger availability"
            ),
        )
        if entry.to_dict() != row:
            raise I4RuntimeError("approval ledger entry does not reproduce")
        entries.append(entry)
    ledger = I4ApprovalLedgerRelease(
        release_sequence=_positive_int(item["release_sequence"], "approval release sequence"),
        previous_ledger_release_id=_optional_text_value(
            item["previous_ledger_release_id"], "previous approval ledger release ID"
        ),
        release_available_session=_date_value(
            item["release_available_session"], "approval ledger availability"
        ),
        entries=tuple(entries),
    )
    if ledger.to_dict() != item:
        raise I4RuntimeError("approval ledger release does not reproduce")
    return ledger


def _gate_artifact(value: object) -> GateArtifactPin:
    pin = _artifact(value)
    return _gate(pin)


def _date_value(value: object, label: str) -> date:
    try:
        return date.fromisoformat(_text(value, label))
    except ValueError as exc:
        raise I4RuntimeError(f"{label} is not an ISO session") from exc


def _optional_text_value(value: object, label: str) -> str | None:
    return None if value is None else _text(value, label)


def _run_spec_id_from_completion_path(relative: str) -> str:
    path = PurePosixPath(relative)
    expected_prefix = PurePosixPath(_CONTROL_ROOT) / "runs"
    parts = path.parts
    prefix = expected_prefix.parts
    if (
        parts[: len(prefix)] != prefix
        or len(parts) != len(prefix) + 2
        or parts[-1] != "completion.json"
        or not parts[-2].startswith("run_spec_id=")
    ):
        raise I4RuntimeError("completion artifact is outside its exact runtime path")
    return _digest(parts[-2].removeprefix("run_spec_id="), "completion RunSpec path ID")


def _verify_authority_binding(
    root: Path, config: I4CorrectionPrepareConfig, authorities: I4RuntimeAuthorities
) -> None:
    if not isinstance(authorities, I4RuntimeAuthorities):
        raise I4RuntimeError("runtime authorities are not typed")
    if len(authorities.added_row_version_receipts) != 1:
        raise I4RuntimeError("runtime requires exactly one alias row-version receipt")
    authorization_artifact = ArtifactPin(
        path=authorities.authorization.artifact.path,
        sha256=authorities.authorization.artifact.sha256,
        bytes=authorities.authorization.artifact.bytes,
    )
    if config.authorization_artifact != authorization_artifact:
        raise I4RuntimeError("runtime config binds another authorization artifact")
    if config.parent_run_receipt_artifact != (authorities.parent_manifest.run_receipt_pin.artifact):
        raise I4RuntimeError("runtime config binds another parent run-receipt artifact")
    if config.row_receipt_digest != stable_digest(
        [item.to_dict() for item in authorities.added_row_version_receipts]
    ):
        raise I4RuntimeError("runtime config binds another row receipt set")
    if config.alias_row_artifact != authorities.added_row_version_receipts[0].index_artifact or (
        config.alias_proof_artifact
        != authorities.added_row_version_receipts[0].semantic_proof.artifact
    ):
        raise I4RuntimeError("runtime config row artifacts differ from typed authorities")
    exact = (
        (config.prior_policy_snapshot_artifact, authorities.prior_policy_snapshot.to_dict()),
        (config.target_policy_snapshot_artifact, authorities.target_policy_snapshot.to_dict()),
    )
    for pin, document in exact:
        if _read_exact(root, pin) != _canonical(document):
            raise I4RuntimeError("stored policy snapshot differs from typed authority")


def _verify_run_spec_binding(
    run_spec: I4CorrectionRunSpec,
    config: I4CorrectionPrepareConfig,
    authorities: I4RuntimeAuthorities,
) -> None:
    if run_spec.config_id != config.config_id:
        raise I4RuntimeError("RunSpec binds another runtime config")
    expected_inputs = _runtime_input_digest(config, run_spec.authority_artifact)
    if run_spec.exact_input_digest != expected_inputs:
        raise I4RuntimeError("RunSpec exact input digest does not reproduce")
    if run_spec.authority_digest != authorities.authority_digest:
        raise I4RuntimeError("RunSpec binds another typed authority set")


def _verify_control_paths(run_spec_artifact: ArtifactPin, run_spec: I4CorrectionRunSpec) -> None:
    expected_run = f"{_CONTROL_ROOT}/run-specs/run_spec_id={run_spec.run_spec_id}/run-spec.json"
    expected_config = f"{_CONTROL_ROOT}/configs/config_id={run_spec.config_id}/config.json"
    expected_authority = _authority_relative(run_spec.authority_artifact.sha256)
    if run_spec_artifact.path != expected_run:
        raise I4RuntimeError("RunSpec artifact is outside its content-addressed runtime path")
    if run_spec.config_artifact.path != expected_config:
        raise I4RuntimeError("runtime config is outside its content-addressed runtime path")
    if run_spec.authority_artifact.path != expected_authority:
        raise I4RuntimeError("runtime authority is outside its content-addressed runtime path")


def _runtime_input_digest(
    config: I4CorrectionPrepareConfig,
    authority_artifact: ArtifactPin,
) -> str:
    pins = tuple(sorted((*config.input_pins, authority_artifact), key=lambda item: item.path))
    if len({item.path for item in pins}) != len(pins):
        raise I4RuntimeError("runtime authority collides with an exact source input")
    return stable_digest([item.to_dict() for item in pins])


def _authority_envelope(authorities: I4RuntimeAuthorities) -> dict[str, object]:
    body: dict[str, object] = {
        "added_row_version_receipts": [
            item.to_dict() for item in authorities.added_row_version_receipts
        ],
        "artifact_type": "s7_5_i4_runtime_authority_envelope",
        "authority_digest": authorities.authority_digest,
        "prior_policy_bundle": authorities.prior_policy_snapshot.policy_bundle.to_dict(),
        "rule_version": I4_AUTHORITY_ENVELOPE_RULE_VERSION,
        "superseded_row_version_ids": list(authorities.superseded_row_version_ids),
    }
    return {"authority_envelope_id": stable_digest(body), **body}


def _verify_authority_envelope(
    root: Path,
    pin: ArtifactPin,
    authorities: I4RuntimeAuthorities,
) -> None:
    document = _read_canonical_object(root, pin, "runtime authority envelope")
    if document != _authority_envelope(authorities):
        raise I4RuntimeError("runtime authority envelope does not reproduce")
    if pin.path != _authority_relative(pin.sha256):
        raise I4RuntimeError("runtime authority envelope path is noncanonical")


def _authority_relative(content_sha256: str) -> str:
    _digest(content_sha256, "runtime authority SHA-256")
    return f"{_CONTROL_ROOT}/authorities/sha256={content_sha256}/authority.json"


def _checkpoint_candidate(
    config: I4CorrectionPrepareConfig,
    authorities: I4RuntimeAuthorities,
    capability: ProductionI4CorrectionCapability,
    run_spec_id: str,
) -> dict[str, object]:
    replacement_by_session = {
        date.fromisoformat(item.partition_key): item
        for item in config.replacement_partition_receipts
    }
    resolved = []
    for item in authorities.checkpoint.resolved_partition_map:
        replacement = replacement_by_session.get(item.session_date)
        resolved.append(
            item.to_dict()
            if replacement is None
            else {
                "artifact": replacement.receipt.to_dict(),
                "availability_session": replacement.availability_session.isoformat(),
                "partition_receipt_id": stable_digest(replacement.to_dict()),
                "row_count": replacement.row_count,
                "session_date": replacement.partition_key,
            }
        )
    body = {
        "artifact_type": "s7_5_i4_correction_checkpoint_candidate",
        "capability_digest": stable_digest(capability.to_dict()),
        "parent_checkpoint_id": authorities.checkpoint.checkpoint_id,
        "parent_checkpoint_artifact": config.parent_checkpoint_artifact.to_dict(),
        "resolved_partition_map": resolved,
        "row_version_overlay": [item.to_dict() for item in authorities.added_row_version_receipts],
        "rule_version": I4_RUNTIME_RULE_VERSION,
        "run_spec_id": run_spec_id,
        "state": I4_RUNTIME_STATE,
        "superseded_row_version_ids": list(authorities.superseded_row_version_ids),
        "target_identity_policy_bundle_id": (
            authorities.target_policy_snapshot.policy_bundle.identity_policy_bundle_id
        ),
        "unaffected_partition_receipts_digest": (capability.unaffected_partition_receipts_digest),
    }
    return {"checkpoint_candidate_id": stable_digest(body), **body}


def _release_candidate(
    config: I4CorrectionPrepareConfig,
    authorities: I4RuntimeAuthorities,
    capability: ProductionI4CorrectionCapability,
    checkpoint_pin: ArtifactPin,
    run_spec_id: str,
) -> dict[str, object]:
    body = {
        "artifact_type": "s7_5_i4_correction_release_candidate",
        "authorization_id": authorities.authorization.authorization.authorization_id,
        "capability": capability.to_dict(),
        "checkpoint_candidate": checkpoint_pin.to_dict(),
        "parent_release": config.parent_manifest_pin.to_dict(),
        "publish_authority": False,
        "rule_version": I4_RUNTIME_RULE_VERSION,
        "run_spec_id": run_spec_id,
        "state": I4_RUNTIME_STATE,
    }
    return {"release_candidate_id": stable_digest(body), **body}


class _FilesystemExactReader:
    def __init__(self, root: Path) -> None:
        self.root = root

    def __call__(self, relative: str) -> bytes:
        path = safe_relative_path(self.root, relative)
        if not path.is_file() or path.is_symlink():
            raise I4RuntimeError(f"exact artifact is missing: {relative}")
        return path.read_bytes()


class _exclusive_lock:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.handle = None

    def __enter__(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = self.path.open("a+b")
        try:
            fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            self.handle.close()
            raise I4RuntimeError(
                "another I4 correction runtime holds the nonblocking lock"
            ) from exc

    def __exit__(self, *_: object) -> None:
        assert self.handle is not None
        fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
        self.handle.close()


def _check_resources(root: Path, caps: I4RuntimeResourceCaps, started: float) -> None:
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform.startswith("linux"):
        rss *= 1024
    if rss > caps.rss_cap_bytes:
        raise I4RuntimeError("I4 correction RSS hard cap exceeded")
    if shutil.disk_usage(root).free < caps.disk_floor_bytes:
        raise I4RuntimeError("I4 correction disk hard floor violated")
    if time.monotonic() - started > caps.wall_clock_cap_seconds:
        raise I4RuntimeError("I4 correction wall-clock hard cap exceeded")


def _write_failed(root: Path, run_spec_id: str, exc: Exception) -> ArtifactPin:
    body = {
        "artifact_type": "s7_5_i4_correction_failed_receipt",
        "error_code": type(exc).__name__,
        "failure_detail_digest": stable_digest(
            {
                "error_message": str(exc),
                "error_type": type(exc).__name__,
                "run_spec_id": run_spec_id,
            }
        ),
        "rule_version": I4_RUNTIME_RULE_VERSION,
        "run_spec_id": run_spec_id,
        "state": "failed",
    }
    document = {"failed_receipt_id": stable_digest(body), **body}
    return _write(
        root,
        (
            f"{_CONTROL_ROOT}/runs/run_spec_id={run_spec_id}/failed-receipts/"
            f"receipt_id={document['failed_receipt_id']}.json"
        ),
        _canonical(document),
    )


def _load_run_spec(root: Path, pin: ArtifactPin) -> I4CorrectionRunSpec:
    content = _read_exact(root, pin)
    result = I4CorrectionRunSpec.from_dict(_strict_json(content))
    if result.canonical_bytes() != content:
        raise I4RuntimeError("runtime RunSpec is not canonical")
    return result


def _load_config(root: Path, pin: ArtifactPin) -> I4CorrectionPrepareConfig:
    content = _read_exact(root, pin)
    result = I4CorrectionPrepareConfig.from_dict(_strict_json(content))
    if result.canonical_bytes() != content:
        raise I4RuntimeError("runtime config is not canonical")
    return result


def _read_exact(root: Path, pin: ArtifactPin) -> bytes:
    content = _FilesystemExactReader(root)(pin.path)
    if len(content) != pin.bytes or hashlib.sha256(content).hexdigest() != pin.sha256:
        raise I4RuntimeError("stored artifact bytes differ from exact pin")
    return content


def _write(root: Path, relative: str, content: bytes) -> ArtifactPin:
    result = write_bytes_immutable(root, safe_relative_path(root, relative), content)
    return ArtifactPin(
        path=str(result["path"]), sha256=str(result["sha256"]), bytes=int(result["bytes"])
    )


def _pin_existing(root: Path, relative: str) -> ArtifactPin:
    path = safe_relative_path(root, relative)
    if not path.is_file() or path.is_symlink():
        raise I4RuntimeError(f"exact artifact is missing or unsafe: {relative}")
    content = path.read_bytes()
    return ArtifactPin(
        path=relative, sha256=hashlib.sha256(content).hexdigest(), bytes=len(content)
    )


def _completion_relative(run_spec_id: str) -> str:
    return f"{_CONTROL_ROOT}/runs/run_spec_id={run_spec_id}/completion.json"


def _lock_relative(run_spec_id: str) -> str:
    return f"{_LOCK_ROOT}/run_spec_id={run_spec_id}.lock"


def _canonical(value: object) -> bytes:
    return (
        json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True).encode() + b"\n"
    )


def _strict_json(content: bytes) -> dict[str, object]:
    try:
        value = json.loads(content.decode())
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise I4RuntimeError("control artifact is not JSON") from exc
    if not isinstance(value, dict):
        raise I4RuntimeError("control artifact must be an object")
    return value


def _read_canonical_object(root: Path, pin: ArtifactPin, label: str) -> dict[str, object]:
    raw = _read_exact(root, pin)
    parsed = _strict_json(raw)
    if _canonical(parsed) != raw:
        raise I4RuntimeError(f"{label} bytes are not canonical")
    return parsed


def _verify_self_hash(document: dict[str, object], field: str, label: str) -> None:
    body = dict(document)
    observed = body.pop(field, None)
    if observed != stable_digest(body):
        raise I4RuntimeError(f"{label} ID does not reproduce")


def _closed(value: object, fields: set[str]) -> dict[str, object]:
    if not isinstance(value, dict) or set(value) != fields:
        raise I4RuntimeError("control artifact fields differ")
    return value


def _array(value: object) -> list[object]:
    if not isinstance(value, list):
        raise I4RuntimeError("control artifact field must be an array")
    return value


def _artifact(value: object) -> ArtifactPin:
    item = _closed(value, {"bytes", "path", "sha256"})
    return ArtifactPin(
        path=_text(item["path"], "artifact path"),
        sha256=_text(item["sha256"], "artifact SHA-256"),
        bytes=_positive_int(item["bytes"], "artifact bytes"),
    )


def _gate(pin: ArtifactPin) -> GateArtifactPin:
    return GateArtifactPin(path=pin.path, sha256=pin.sha256, bytes=pin.bytes)


def _manifest_pin(value: object) -> ManifestPin:
    item = _closed(
        value,
        {
            "manifest_bytes",
            "manifest_path",
            "manifest_sha256",
            "release_available_session",
            "release_id",
        },
    )
    return ManifestPin(
        release_id=_text(item["release_id"], "manifest release ID"),
        manifest_path=_text(item["manifest_path"], "manifest path"),
        manifest_sha256=_text(item["manifest_sha256"], "manifest SHA-256"),
        manifest_bytes=_positive_int(item["manifest_bytes"], "manifest bytes"),
        release_available_session=date.fromisoformat(
            _text(item["release_available_session"], "manifest release availability")
        ),
    )


def _partition_receipt(value: object) -> PartitionReceipt:
    item = _closed(
        value,
        {
            "availability_session",
            "partition_key",
            "receipt",
            "row_count",
            "row_version_references",
            "schema_digest",
            "table_name",
        },
    )
    from ame_stocks_api.silver.incremental_contract import RowVersionReference

    refs = tuple(
        RowVersionReference(
            table_name=_text(
                _closed(ref, {"row_version_id", "table_name"})["table_name"],
                "row-version reference table",
            ),
            row_version_id=_text(
                _closed(ref, {"row_version_id", "table_name"})["row_version_id"],
                "row-version reference ID",
            ),
        )
        for ref in _array(item["row_version_references"])
    )
    return PartitionReceipt(
        table_name=_text(item["table_name"], "partition table"),
        partition_key=_text(item["partition_key"], "partition key"),
        receipt=_artifact(item["receipt"]),
        row_count=_positive_int(item["row_count"], "partition row count"),
        schema_digest=_text(item["schema_digest"], "partition schema digest"),
        availability_session=date.fromisoformat(
            _text(item["availability_session"], "partition availability session")
        ),
        row_version_references=refs,
    )


def _text(value: object, label: str) -> str:
    if type(value) is not str:
        raise I4RuntimeError(f"{label} must be a string")
    return value


def _positive_int(value: object, label: str) -> int:
    if type(value) is not int or value <= 0:
        raise I4RuntimeError(f"{label} must be a positive integer")
    return value


def _digest(value: object, label: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(ch not in "0123456789abcdef" for ch in value)
    ):
        raise I4RuntimeError(f"{label} must be a lowercase SHA-256")
    return value


__all__ = [
    "I4CorrectionPrepareConfig",
    "I4CorrectionRunSpec",
    "I4CorrectionStageResult",
    "I4RuntimeAuthorities",
    "I4RuntimeError",
    "I4RuntimeResourceCaps",
    "LoadedI4RuntimeAuthorities",
    "PreparedI4CorrectionRun",
    "load_i4_runtime_authorities_exact",
    "prepare_i4_correction_run",
    "stage_i4_correction",
    "verify_i4_correction",
]
