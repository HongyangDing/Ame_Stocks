"""Exact, immutable final completion sentinel for S7.5.

This module is deliberately the last consumer in the S7.5 graph.  It cannot
create Gate B, Gate C, pointer events, corrections, an I3 recovery exercise, or
an I7 reconciliation.  It replays those already completed authorities, rebuilds
the frozen thirteen-criterion lifecycle manifest, and only then writes
``S7_5_COMPLETE.json``.  Missing evidence remains a hard, reviewable blocker.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from pathlib import Path, PurePosixPath
from typing import Final, Self

from ame_stocks_api.artifacts import safe_relative_path, stable_digest
from ame_stocks_api.silver import incremental_i3_production as i3_runtime
from ame_stocks_api.silver import incremental_i4_runtime as i4_runtime
from ame_stocks_api.silver import incremental_i5_shadow_runtime as i5_runtime
from ame_stocks_api.silver import incremental_i6_pointer_runtime as i6_runtime
from ame_stocks_api.silver import incremental_i7_reconciliation_runtime as i7_runtime
from ame_stocks_api.silver.incremental_contract import ArtifactPin
from ame_stocks_api.silver.incremental_i3_production_contract import I3ProductionRunKind
from ame_stocks_api.silver.incremental_i3_production_inputs import (
    load_frozen_s7_oracle_marker_exact,
)
from ame_stocks_api.silver.incremental_i5_lifecycle import (
    ACCEPTANCE_CRITERIA,
    AcceptanceCriterionReceipt,
    PinnedGateBApproval,
    PinnedGateCApproval,
    S75CompletionManifest,
    validate_s75_completion,
)

S75_COMPLETION_RUNTIME_RULE_VERSION: Final = "s7_5_exact_completion_runtime_v1"
S75_COMPLETION_STATE: Final = "complete"
GATE_A_APPROVAL_ID: Final = "4e04a6d7865c940740f214967247eea6af0ed3d5c4b4ca5b3f95b4023460722d"
_CONTROL_ROOT: Final = "manifests/silver/incremental/s7_5/completion"
_SENTINEL_PATH: Final = "manifests/silver/incremental/s7_5/S7_5_COMPLETE.json"
_FORBIDDEN_PATH_PARTS: Final = frozenset(
    {"latest", "tmp", ".tmp", "fixture", "fixtures", "test", "tests"}
)


class S75CompletionRuntimeError(RuntimeError):
    """Raised before incomplete evidence can become the S7.5 sentinel."""


@dataclass(frozen=True, slots=True)
class S75CompletionConfig:
    i4_completion_artifact: ArtifactPin
    i5_completion_artifact: ArtifactPin
    i7_completion_artifact: ArtifactPin
    completion_available_session: date

    def __post_init__(self) -> None:
        for pin, label in (
            (self.i4_completion_artifact, "I4 completion"),
            (self.i5_completion_artifact, "I5 completion"),
            (self.i7_completion_artifact, "I7 completion"),
        ):
            _artifact(pin, label)
            _production_path(pin.path, label)
        if not isinstance(self.completion_available_session, date):
            raise S75CompletionRuntimeError("completion availability is not a date")

    @property
    def config_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "artifact_type": "s7_5_completion_config",
            "completion_available_session": self.completion_available_session.isoformat(),
            "i4_completion_artifact": self.i4_completion_artifact.to_dict(),
            "i5_completion_artifact": self.i5_completion_artifact.to_dict(),
            "i7_completion_artifact": self.i7_completion_artifact.to_dict(),
            "rule_version": S75_COMPLETION_RUNTIME_RULE_VERSION,
        }

    def to_dict(self) -> dict[str, object]:
        return {"config_id": self.config_id, **self.logical_payload()}

    def canonical_bytes(self) -> bytes:
        return _canonical(self.to_dict())

    @classmethod
    def from_dict(cls, value: object) -> Self:
        item = _closed(
            value,
            {
                "artifact_type",
                "completion_available_session",
                "config_id",
                "i4_completion_artifact",
                "i5_completion_artifact",
                "i7_completion_artifact",
                "rule_version",
            },
            "S7.5 completion config",
        )
        if (
            item["artifact_type"] != "s7_5_completion_config"
            or item["rule_version"] != S75_COMPLETION_RUNTIME_RULE_VERSION
        ):
            raise S75CompletionRuntimeError("completion config type or rule differs")
        result = cls(
            i4_completion_artifact=_artifact_from_dict(item["i4_completion_artifact"]),
            i5_completion_artifact=_artifact_from_dict(item["i5_completion_artifact"]),
            i7_completion_artifact=_artifact_from_dict(item["i7_completion_artifact"]),
            completion_available_session=_date(item["completion_available_session"]),
        )
        if item["config_id"] != result.config_id:
            raise S75CompletionRuntimeError("completion config ID does not reproduce")
        return result


@dataclass(frozen=True, slots=True)
class S75CompletionResult:
    config: S75CompletionConfig
    config_artifact: ArtifactPin
    lifecycle_manifest: S75CompletionManifest
    marker_artifact: ArtifactPin
    sentinel_artifact: ArtifactPin
    reused: bool


@dataclass(frozen=True, slots=True)
class _VerifiedEvidence:
    legacy_s7_release_set_id: str
    gate_a_approval_id: str
    i2_acceptance_receipt_id: str
    i3_acceptance_receipt_id: str
    i4_acceptance_receipt_id: str
    i5_spec: object
    i5_completion: object
    i6_snapshot: object
    i7_result: object
    evidence_artifacts: tuple[ArtifactPin, ...]


def prepare_s75_completion(
    data_root: Path,
    config: S75CompletionConfig,
) -> tuple[S75CompletionConfig, ArtifactPin]:
    """Freeze exact final inputs without asserting S7.5 completion."""

    root = _root(data_root)
    if not isinstance(config, S75CompletionConfig):
        raise S75CompletionRuntimeError("prepare requires a typed completion config")
    for pin in (
        config.i4_completion_artifact,
        config.i5_completion_artifact,
        config.i7_completion_artifact,
    ):
        _read_exact(root, pin, "completion input")
    relative = _config_path(config.config_id)
    return config, _write_immutable(root, relative, config.canonical_bytes(), "completion config")


def stage_s75_completion(
    data_root: Path,
    config_artifact: ArtifactPin,
) -> S75CompletionResult:
    """Deep-replay I0--I7 and publish only the immutable completion sentinel."""

    root = _root(data_root)
    config = _load_config(root, config_artifact)
    evidence = _load_production_evidence(root, config)
    lifecycle = _build_lifecycle_manifest(config, evidence)
    _validate_lifecycle(root, lifecycle, evidence)
    marker = _marker_document(config_artifact, config, lifecycle, evidence)
    marker_content = _canonical(marker)
    marker_relative = _marker_path(marker["marker_id"])
    marker_path = safe_relative_path(root, marker_relative)
    sentinel_path = safe_relative_path(root, _SENTINEL_PATH)
    reused = marker_path.exists() and sentinel_path.exists()
    marker_pin = _write_immutable(root, marker_relative, marker_content, "S7.5 marker")
    sentinel_pin = _write_immutable(root, _SENTINEL_PATH, marker_content, "S7.5 sentinel")
    return _verify_s75_completion(
        root,
        sentinel_pin,
        expected_marker=marker_pin,
        reused=reused,
    )


def verify_s75_completion(
    data_root: Path,
    sentinel_artifact: ArtifactPin,
) -> S75CompletionResult:
    """Rebuild every final criterion from exact producer bytes."""

    return _verify_s75_completion(
        _root(data_root),
        sentinel_artifact,
        expected_marker=None,
        reused=True,
    )


def _verify_s75_completion(
    root: Path,
    sentinel_artifact: ArtifactPin,
    *,
    expected_marker: ArtifactPin | None,
    reused: bool,
) -> S75CompletionResult:
    if sentinel_artifact.path != _SENTINEL_PATH:
        raise S75CompletionRuntimeError("S7.5 sentinel path is noncanonical")
    marker = _read_canonical(root, sentinel_artifact, "S7.5 sentinel")
    _require_marker_shape(marker)
    config_artifact = _artifact_from_dict(marker["config_artifact"])
    config = _load_config(root, config_artifact)
    evidence = _load_production_evidence(root, config)
    lifecycle = _build_lifecycle_manifest(config, evidence)
    _validate_lifecycle(root, lifecycle, evidence)
    expected_document = _marker_document(config_artifact, config, lifecycle, evidence)
    if marker != expected_document:
        raise S75CompletionRuntimeError("S7.5 sentinel does not reproduce from evidence")
    marker_relative = _marker_path(_digest(marker["marker_id"], "marker ID"))
    marker_pin = _pin_bytes(marker_relative, _canonical(expected_document))
    if expected_marker is not None and marker_pin != expected_marker:
        raise S75CompletionRuntimeError("content-addressed S7.5 marker differs")
    if _read_exact(root, marker_pin, "content-addressed S7.5 marker") != _canonical(marker):
        raise S75CompletionRuntimeError("S7.5 marker and sentinel bytes differ")
    return S75CompletionResult(
        config=config,
        config_artifact=config_artifact,
        lifecycle_manifest=lifecycle,
        marker_artifact=marker_pin,
        sentinel_artifact=sentinel_artifact,
        reused=reused,
    )


def _load_production_evidence(
    root: Path,
    config: S75CompletionConfig,
) -> _VerifiedEvidence:
    try:
        i5_completion = i5_runtime.load_i5_shadow_completion_exact(
            root,
            config.i5_completion_artifact,
            production=True,
        )
        spec_content = _read_exact(root, i5_completion.run_spec_artifact, "I5 RunSpec")
        i5_spec = i5_runtime._run_spec_from_dict(
            i5_runtime._closed_json(spec_content, label="I5 RunSpec")
        )
        if i5_spec.canonical_bytes() != spec_content:
            raise S75CompletionRuntimeError("I5 RunSpec is not canonical")
        i6_snapshot = i6_runtime.load_research_top_snapshot_exact(root)
        i7_result = i7_runtime.verify_i7_full_reconciliation(
            root,
            completion_artifact=config.i7_completion_artifact,
        )
        loaded_i3 = i3_runtime.verify_i3_production_deep_attestation(
            root,
            i5_spec.incremental_completion_artifact,
            i5_spec.incremental_deep_attestation_artifact,
            expected_kind=I3ProductionRunKind.DELTA,
        )
        retry = i3_runtime.exercise_i3_production_interrupted_retry(
            root,
            loaded_i3.receipt.run_spec_artifact,
            fail_after=i3_runtime.FAILED_RECEIPT_DURABLE_BEFORE_COMPLETION,
        )
        loaded_i4 = i4_runtime.load_i4_runtime_authorities_exact(
            root,
            config.i4_completion_artifact,
        )
        i4_result = i4_runtime.verify_i4_correction(
            root,
            config.i4_completion_artifact,
            authorities=loaded_i4.authorities,
            reused=True,
        )
    except S75CompletionRuntimeError:
        raise
    except Exception as exc:
        raise S75CompletionRuntimeError(
            f"S7.5 producer evidence cannot be replayed: {type(exc).__name__}: {exc}"
        ) from exc
    if (
        len(loaded_i3.run_spec.i2_receipts) != 1
        or retry.reused is not True
        or retry.receipt.completion_artifact != i5_spec.incremental_completion_artifact
        or retry.receipt.deep_attestation_artifact != i5_spec.incremental_deep_attestation_artifact
        or i6_snapshot.release_completion_artifact != i5_spec.incremental_completion_artifact
        or i6_snapshot.deep_attestation_artifact != i5_spec.incremental_deep_attestation_artifact
        or i6_snapshot.release_id != i5_spec.incremental_release_id
        or i7_result.run_spec.incremental.top_pointer_artifact
        != i6_snapshot.research_top_event_artifact
        or i7_result.run_spec.incremental.gate_c_approval_artifact
        != i6_snapshot.gate_c_approval_artifact
        or i7_result.completion.receipt.incremental_top_release_id != i6_snapshot.release_id
        or loaded_i4.authorities.parent_manifest.release_id != loaded_i3.gate_a_manifest.release_id
        or loaded_i4.authorities.checkpoint.checkpoint_id != loaded_i3.checkpoint.checkpoint_id
    ):
        raise S75CompletionRuntimeError("S7.5 producer evidence graph does not close")
    i4_document = _read_canonical(root, config.i4_completion_artifact, "I4 completion")
    i4_acceptance_id = _digest(i4_document.get("completion_id"), "I4 completion ID")
    if i4_result.completion_pin != config.i4_completion_artifact:
        raise S75CompletionRuntimeError("I4 completion exact pin differs")
    gate_a_id = _verify_gate_a_approval()
    legacy_pin = loaded_i3.run_spec.i0_oracle.artifact
    legacy_content = _read_exact(root, legacy_pin, "legacy S7 release-set marker")
    marker = load_frozen_s7_oracle_marker_exact(legacy_pin, content=legacy_content)
    legacy_release_id = _digest(marker["release_set_id"], "legacy S7 release-set ID")
    artifacts = _unique_pins(
        (
            config.i4_completion_artifact,
            config.i5_completion_artifact,
            config.i7_completion_artifact,
            i5_completion.run_spec_artifact,
            i6_snapshot.research_top_event_artifact,
            i6_snapshot.gate_b_approval_artifact,
            i6_snapshot.gate_c_approval_artifact,
            i6_snapshot.rollback_receipt_artifact,
            retry.receipt_artifact,
            legacy_pin,
        )
    )
    return _VerifiedEvidence(
        legacy_s7_release_set_id=legacy_release_id,
        gate_a_approval_id=gate_a_id,
        i2_acceptance_receipt_id=loaded_i3.run_spec.i2_receipts[0].receipt_id,
        i3_acceptance_receipt_id=retry.receipt.receipt_id,
        i4_acceptance_receipt_id=i4_acceptance_id,
        i5_spec=i5_spec,
        i5_completion=i5_completion,
        i6_snapshot=i6_snapshot,
        i7_result=i7_result,
        evidence_artifacts=artifacts,
    )


def _build_lifecycle_manifest(
    config: S75CompletionConfig,
    evidence: _VerifiedEvidence,
) -> S75CompletionManifest:
    top = evidence.i6_snapshot
    shadow_receipt = evidence.i5_completion.receipt
    full_receipt = evidence.i7_result.completion.receipt
    ids = {
        "legacy": evidence.legacy_s7_release_set_id,
        "i2": evidence.i2_acceptance_receipt_id,
        "i3": evidence.i3_acceptance_receipt_id,
        "i4": evidence.i4_acceptance_receipt_id,
    }
    required = {
        1: {ids["legacy"]},
        2: {ids["i2"], ids["i3"]},
        3: {ids["i2"], ids["i3"]},
        4: {shadow_receipt.receipt_id},
        5: {shadow_receipt.receipt_id},
        6: {shadow_receipt.receipt_id},
        7: {ids["i3"]},
        8: {ids["i4"]},
        9: {ids["i3"], ids["i4"]},
        10: {shadow_receipt.receipt_id, full_receipt.receipt_id},
        11: {top.rollback_pointer_event.event_id, top.rollback_receipt.receipt_id},
        12: {ids["i2"], ids["i3"], ids["i4"]},
        13: {ids["i3"]},
    }
    criteria = tuple(
        AcceptanceCriterionReceipt(
            criterion_number=number,
            criterion_id=criterion_id,
            semantics_digest=stable_digest(
                {
                    "criterion_id": criterion_id,
                    "evidence_ids": sorted(required[number]),
                    "rule_version": S75_COMPLETION_RUNTIME_RULE_VERSION,
                }
            ),
            evidence_ids=tuple(sorted(required[number])),
            passed=True,
        )
        for number, criterion_id in enumerate(ACCEPTANCE_CRITERIA, 1)
    )
    return S75CompletionManifest(
        legacy_s7_release_set_id=ids["legacy"],
        gate_a_approval_id=evidence.gate_a_approval_id,
        i2_acceptance_receipt_id=ids["i2"],
        i3_acceptance_receipt_id=ids["i3"],
        i4_acceptance_receipt_id=ids["i4"],
        shadow_equivalence_receipt_id=shadow_receipt.receipt_id,
        gate_b_approval_id=top.gate_b_approval.approval_id,
        shadow_pointer_event_id=top.shadow_pointer_event.event_id,
        rollback_pointer_event_id=top.rollback_pointer_event.event_id,
        rollback_receipt_id=top.rollback_receipt.receipt_id,
        gate_c_approval_id=top.gate_c_approval.approval_id,
        top_pointer_event_id=top.research_top_event.event_id,
        full_reconciliation_receipt_id=full_receipt.receipt_id,
        final_top_release_id=top.release_id,
        acceptance_criteria=criteria,
        completion_available_session=config.completion_available_session,
    )


def _validate_lifecycle(
    root: Path,
    manifest: S75CompletionManifest,
    evidence: _VerifiedEvidence,
) -> None:
    top = evidence.i6_snapshot
    shadow_event = top.shadow_pointer_event
    rollback_event = top.rollback_pointer_event
    top_event = top.research_top_event
    gate_b = PinnedGateBApproval(
        approval=top.gate_b_approval,
        artifact=top.gate_b_approval_artifact,
    )
    gate_c = PinnedGateCApproval(
        approval=top.gate_c_approval,
        artifact=top.gate_c_approval_artifact,
    )
    validate_s75_completion(
        manifest,
        shadow_spec=evidence.i5_spec.lifecycle_spec,
        shadow_receipt=evidence.i5_completion.receipt,
        gate_b=gate_b,
        shadow_event=shadow_event,
        rollback_event=rollback_event,
        rollback_receipt=top.rollback_receipt,
        gate_c=gate_c,
        top_event=top_event,
        full_reconciliation_spec=evidence.i7_result.run_spec.lifecycle_spec,
        full_reconciliation_receipt=evidence.i7_result.completion.receipt,
        shadow_observed_previous_event_id=shadow_event.expected_previous_event_id,
        shadow_observed_previous_release_id=shadow_event.previous_release_id,
        shadow_observed_previous_pointer_revision=shadow_event.pointer_revision - 1,
        rollback_observed_current_event_id=shadow_event.event_id,
        rollback_observed_current_release_id=shadow_event.new_release_id,
        rollback_observed_current_pointer_revision=shadow_event.pointer_revision,
        research_observed_current_event_id=top_event.expected_previous_event_id,
        research_observed_current_release_id=top_event.previous_release_id,
        research_observed_current_pointer_revision=top_event.pointer_revision - 1,
        availability_cutoff_session=manifest.completion_available_session,
        artifact_reader=lambda relative: _read_relative(root, relative),
    )


def _marker_document(
    config_artifact: ArtifactPin,
    config: S75CompletionConfig,
    lifecycle: S75CompletionManifest,
    evidence: _VerifiedEvidence,
) -> dict[str, object]:
    body: dict[str, object] = {
        "artifact_type": "s7_5_completion_marker",
        "config_artifact": config_artifact.to_dict(),
        "config_id": config.config_id,
        "evidence_artifacts": [item.to_dict() for item in evidence.evidence_artifacts],
        "lifecycle_manifest": lifecycle.to_dict(),
        "publish_authorized": False,
        "rule_version": S75_COMPLETION_RUNTIME_RULE_VERSION,
        "s7_5_complete": True,
        "state": S75_COMPLETION_STATE,
    }
    body["marker_id"] = stable_digest(body)
    return body


def _verify_gate_a_approval() -> str:
    repository = Path(__file__).resolve().parents[3]
    approval_path = (
        repository
        / "docs/silver/decisions/s7_5/gate_a"
        / f"approval_event_id={GATE_A_APPROVAL_ID}/manifest.json"
    )
    candidate_path = (
        repository
        / "docs/silver/contracts/control/s7_5_incremental_contract_bundle-v1.candidate.json"
    )
    try:
        approval_bytes = approval_path.read_bytes()
        candidate_bytes = candidate_path.read_bytes()
        approval = json.loads(approval_bytes)
        candidate = json.loads(candidate_bytes)
    except (OSError, json.JSONDecodeError) as exc:
        raise S75CompletionRuntimeError("Gate A approval evidence is unavailable") from exc
    body = dict(approval)
    claimed = body.pop("approval_event_id", None)
    approved = approval.get("approved_candidate")
    if (
        claimed != GATE_A_APPROVAL_ID
        or stable_digest(body) != GATE_A_APPROVAL_ID
        or not isinstance(approved, Mapping)
        or approved.get("candidate_sha256") != hashlib.sha256(candidate_bytes).hexdigest()
        or approved.get("contract_id") != candidate.get("contract_id")
        or approval.get("approval_literal") != "批准Gate A"
    ):
        raise S75CompletionRuntimeError("Gate A approval chain differs")
    return GATE_A_APPROVAL_ID


def _load_config(root: Path, artifact: ArtifactPin) -> S75CompletionConfig:
    _artifact(artifact, "completion config")
    expected_prefix = f"{_CONTROL_ROOT}/configs/config_id="
    if not artifact.path.startswith(expected_prefix) or not artifact.path.endswith("/config.json"):
        raise S75CompletionRuntimeError("completion config path is noncanonical")
    config = S75CompletionConfig.from_dict(_read_canonical(root, artifact, "completion config"))
    if artifact.path != _config_path(config.config_id):
        raise S75CompletionRuntimeError("completion config directory ID differs")
    return config


def _require_marker_shape(value: Mapping[str, object]) -> None:
    expected = {
        "artifact_type",
        "config_artifact",
        "config_id",
        "evidence_artifacts",
        "lifecycle_manifest",
        "marker_id",
        "publish_authorized",
        "rule_version",
        "s7_5_complete",
        "state",
    }
    if set(value) != expected:
        raise S75CompletionRuntimeError("S7.5 marker fields differ")
    body = dict(value)
    claimed = body.pop("marker_id", None)
    if (
        claimed != stable_digest(body)
        or value["artifact_type"] != "s7_5_completion_marker"
        or value["rule_version"] != S75_COMPLETION_RUNTIME_RULE_VERSION
        or value["state"] != S75_COMPLETION_STATE
        or value["s7_5_complete"] is not True
        or value["publish_authorized"] is not False
    ):
        raise S75CompletionRuntimeError("S7.5 marker semantics differ")


def _config_path(config_id: str) -> str:
    return f"{_CONTROL_ROOT}/configs/config_id={config_id}/config.json"


def _marker_path(marker_id: str) -> str:
    return f"{_CONTROL_ROOT}/markers/marker_id={marker_id}/manifest.json"


def _write_immutable(root: Path, relative: str, content: bytes, label: str) -> ArtifactPin:
    expected = _pin_bytes(relative, content)
    path = safe_relative_path(root, relative)
    if path.exists() or path.is_symlink():
        observed = _pin_existing(root, relative)
        if observed != expected:
            raise S75CompletionRuntimeError(f"{label} no-clobber conflict")
        return observed
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".staging",
        dir=path.parent,
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(temporary, 0o444)
        _before_immutable_commit(path)
        try:
            os.link(temporary, path, follow_symlinks=False)
        except FileExistsError:
            if path.is_symlink() or _pin_existing(root, relative) != expected:
                raise S75CompletionRuntimeError(f"{label} no-clobber conflict") from None
        _fsync_directory(path.parent)
        observed = _pin_existing(root, relative)
        if observed != expected:
            raise S75CompletionRuntimeError(f"{label} atomic commit differs")
        return observed
    finally:
        temporary.unlink(missing_ok=True)


def _before_immutable_commit(_path: Path) -> None:
    """Fault-injection hook before the only visible no-clobber commit."""


def _read_canonical(root: Path, artifact: ArtifactPin, label: str) -> dict[str, object]:
    content = _read_exact(root, artifact, label)
    try:
        value = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise S75CompletionRuntimeError(f"{label} is not JSON") from exc
    if not isinstance(value, dict) or _canonical(value) != content:
        raise S75CompletionRuntimeError(f"{label} is not canonical JSON")
    return value


def _read_exact(root: Path, artifact: ArtifactPin, label: str) -> bytes:
    _artifact(artifact, label)
    path = safe_relative_path(root, artifact.path)
    if not path.is_file() or path.is_symlink():
        raise S75CompletionRuntimeError(f"{label} is missing or unsafe")
    content = path.read_bytes()
    if len(content) != artifact.bytes or hashlib.sha256(content).hexdigest() != artifact.sha256:
        raise S75CompletionRuntimeError(f"{label} exact pin differs")
    return content


def _read_relative(root: Path, relative: str) -> bytes:
    path = safe_relative_path(root, _relative(relative, "lifecycle evidence path"))
    if not path.is_file() or path.is_symlink():
        raise S75CompletionRuntimeError("lifecycle evidence is missing or unsafe")
    return path.read_bytes()


def _pin_existing(root: Path, relative: str) -> ArtifactPin:
    path = safe_relative_path(root, relative)
    if not path.is_file() or path.is_symlink():
        raise S75CompletionRuntimeError("immutable completion artifact is missing or unsafe")
    content = path.read_bytes()
    return _pin_bytes(relative, content)


def _pin_bytes(relative: str, content: bytes) -> ArtifactPin:
    return ArtifactPin(
        path=relative,
        sha256=hashlib.sha256(content).hexdigest(),
        bytes=len(content),
    )


def _unique_pins(pins: tuple[ArtifactPin, ...]) -> tuple[ArtifactPin, ...]:
    by_path: dict[str, ArtifactPin] = {}
    for pin in pins:
        prior = by_path.get(pin.path)
        if prior is not None and prior != pin:
            raise S75CompletionRuntimeError("completion evidence path has conflicting pins")
        by_path[pin.path] = pin
    return tuple(by_path[path] for path in sorted(by_path))


def _root(value: Path) -> Path:
    root = value.expanduser().resolve()
    if not root.is_dir() or root.is_symlink():
        raise S75CompletionRuntimeError("completion data root is invalid")
    return root


def _production_path(value: str, label: str) -> str:
    relative = _relative(value, label)
    if any(part.lower() in _FORBIDDEN_PATH_PARTS for part in PurePosixPath(relative).parts):
        raise S75CompletionRuntimeError(f"{label} path is not production authority")
    return relative


def _relative(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise S75CompletionRuntimeError(f"{label} path is invalid")
    path = PurePosixPath(value)
    if path.is_absolute() or ".." in path.parts or path.as_posix() != value:
        raise S75CompletionRuntimeError(f"{label} path is not normalized")
    return value


def _closed(value: object, fields: set[str], label: str) -> dict[str, object]:
    if not isinstance(value, Mapping) or set(value) != fields:
        raise S75CompletionRuntimeError(f"{label} fields differ")
    return dict(value)


def _artifact_from_dict(value: object) -> ArtifactPin:
    item = _closed(value, {"bytes", "path", "sha256"}, "artifact pin")
    return ArtifactPin(
        path=_relative(item["path"], "artifact"),
        sha256=_digest(item["sha256"], "artifact SHA"),
        bytes=_positive_int(item["bytes"], "artifact bytes"),
    )


def _artifact(value: object, label: str) -> ArtifactPin:
    if not isinstance(value, ArtifactPin):
        raise S75CompletionRuntimeError(f"{label} is not an ArtifactPin")
    return value


def _digest(value: object, label: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise S75CompletionRuntimeError(f"{label} is not a lowercase SHA-256")
    return value


def _positive_int(value: object, label: str) -> int:
    if type(value) is not int or value <= 0:
        raise S75CompletionRuntimeError(f"{label} is not a positive integer")
    return value


def _date(value: object) -> date:
    if not isinstance(value, str):
        raise S75CompletionRuntimeError("completion availability is not text")
    try:
        result = date.fromisoformat(value)
    except ValueError as exc:
        raise S75CompletionRuntimeError("completion availability is not an ISO date") from exc
    if result.isoformat() != value:
        raise S75CompletionRuntimeError("completion availability is not canonical")
    return result


def _canonical(value: object) -> bytes:
    return (
        json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
        + b"\n"
    )


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


__all__ = [
    "GATE_A_APPROVAL_ID",
    "S75CompletionConfig",
    "S75CompletionResult",
    "S75CompletionRuntimeError",
    "prepare_s75_completion",
    "stage_s75_completion",
    "verify_s75_completion",
]
