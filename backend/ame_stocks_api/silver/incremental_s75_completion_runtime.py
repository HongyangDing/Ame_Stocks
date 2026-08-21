"""Factor-first S7.5 completion marker.

S7.5 exists to make the identity tables usable by factor research. Its
completion gate therefore checks the successful DELTA, the newly appended
``universe_daily`` partition, and only the invariants that can change research
results. Correction drills, failure injection, pointer ceremonies, and
periodic Full reconciliation are not S8 prerequisites and are not part of the
active daily runtime.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Final, Self

from ame_stocks_api.artifacts import safe_relative_path, stable_digest, write_bytes_immutable
from ame_stocks_api.silver.incremental_contract import ArtifactPin
from ame_stocks_api.silver.incremental_i3_contract import I3_V2_TABLE_ORDER
from ame_stocks_api.silver.incremental_i3_migration_io import (
    readback_i3_migration_parquet_exact,
)
from ame_stocks_api.silver.incremental_i3_production_contract import (
    I3ProductionCompletion,
    I3ProductionDeepVerificationAttestation,
    I3ProductionOutputSet,
    I3ProductionRunKind,
    I3ProductionRunReceipt,
    I3ProductionRunSpec,
    I3ProductionRunState,
    load_i3_production_completion_exact,
    load_i3_production_deep_attestation_exact,
    load_i3_production_run_receipt_exact,
    load_i3_production_run_spec_exact,
)

S75_COMPLETION_RUNTIME_RULE_VERSION: Final = "s7_5_factor_ready_completion_v2"
S75_COMPLETION_STATE: Final = "factor_ready_for_s8"
_CONTROL_ROOT: Final = "manifests/silver/incremental/s7_5/factor-ready"
S75_CURRENT_MARKER_PATH: Final = "manifests/silver/incremental/s7_5/S7_5_COMPLETE.json"
_CONTINUITY_UNCERTAIN: Final = "identity_uncertain_no_new_trade_no_forced_exit_run_incomplete"


class S75CompletionRuntimeError(RuntimeError):
    """Raised when the DELTA is not safe to expose to factor construction."""


@dataclass(frozen=True, slots=True)
class S75CompletionConfig:
    delta_completion_artifact: ArtifactPin
    delta_deep_attestation_artifact: ArtifactPin
    completion_available_session: date

    def __post_init__(self) -> None:
        if not isinstance(self.delta_completion_artifact, ArtifactPin):
            raise S75CompletionRuntimeError("DELTA completion pin is invalid")
        if not isinstance(self.delta_deep_attestation_artifact, ArtifactPin):
            raise S75CompletionRuntimeError("DELTA deep-attestation pin is invalid")
        if type(self.completion_available_session) is not date:
            raise S75CompletionRuntimeError("completion availability is not a native date")

    @property
    def config_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "artifact_type": "s7_5_factor_ready_config",
            "completion_available_session": self.completion_available_session.isoformat(),
            "delta_completion_artifact": self.delta_completion_artifact.to_dict(),
            "delta_deep_attestation_artifact": (self.delta_deep_attestation_artifact.to_dict()),
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
                "delta_completion_artifact",
                "delta_deep_attestation_artifact",
                "rule_version",
            },
            "S7.5 factor-ready config",
        )
        if (
            item["artifact_type"] != "s7_5_factor_ready_config"
            or item["rule_version"] != S75_COMPLETION_RUNTIME_RULE_VERSION
        ):
            raise S75CompletionRuntimeError("factor-ready config type or version differs")
        result = cls(
            delta_completion_artifact=_artifact(item["delta_completion_artifact"]),
            delta_deep_attestation_artifact=_artifact(item["delta_deep_attestation_artifact"]),
            completion_available_session=_date(item["completion_available_session"]),
        )
        if item["config_id"] != result.config_id:
            raise S75CompletionRuntimeError("factor-ready config ID does not reproduce")
        return result


@dataclass(frozen=True, slots=True)
class S75FactorSummary:
    terminal_session: date
    universe_row_count: int
    eligible_row_count: int
    ineligible_row_count: int
    unresolved_row_count: int
    target_partition: ArtifactPin

    def to_dict(self) -> dict[str, object]:
        return {
            "eligible_row_count": self.eligible_row_count,
            "ineligible_row_count": self.ineligible_row_count,
            "target_partition": self.target_partition.to_dict(),
            "terminal_session": self.terminal_session.isoformat(),
            "universe_row_count": self.universe_row_count,
            "unresolved_row_count": self.unresolved_row_count,
        }


@dataclass(frozen=True, slots=True)
class S75CompletionResult:
    config: S75CompletionConfig
    config_artifact: ArtifactPin
    completion: I3ProductionCompletion
    run_spec: I3ProductionRunSpec
    summary: S75FactorSummary
    sentinel_artifact: ArtifactPin
    reused: bool


@dataclass(frozen=True, slots=True)
class _DeltaAuthority:
    completion: I3ProductionCompletion
    receipt: I3ProductionRunReceipt
    run_spec: I3ProductionRunSpec
    deep: I3ProductionDeepVerificationAttestation
    output_set: I3ProductionOutputSet


def prepare_s75_completion(
    data_root: Path,
    config: S75CompletionConfig,
) -> tuple[S75CompletionConfig, ArtifactPin]:
    """Store the two exact DELTA pins needed by the factor-ready check."""

    root = data_root.expanduser().resolve()
    if not isinstance(config, S75CompletionConfig):
        raise S75CompletionRuntimeError("prepare requires a typed factor-ready config")
    _read_exact(root, config.delta_completion_artifact, "DELTA completion")
    _read_exact(root, config.delta_deep_attestation_artifact, "DELTA deep attestation")
    relative = f"{_CONTROL_ROOT}/config_id={config.config_id}/config.json"
    return config, _write(root, relative, config.canonical_bytes())


def stage_s75_completion(
    data_root: Path,
    config_artifact: ArtifactPin,
) -> S75CompletionResult:
    """Validate the latest DELTA for factor use and write one completion marker."""

    root = data_root.expanduser().resolve()
    config = _load_config(root, config_artifact)
    authority = _load_delta_authority(root, config)
    summary = _factor_summary(root, authority)
    document = _marker_document(config_artifact, config, authority, summary)
    content = _canonical(document)
    immutable_relative = (
        f"{_CONTROL_ROOT}/completions/session_date={summary.terminal_session.isoformat()}/"
        f"marker_id={document['marker_id']}/manifest.json"
    )
    _write(root, immutable_relative, content)
    sentinel_path = safe_relative_path(root, S75_CURRENT_MARKER_PATH)
    reused = sentinel_path.is_file() and sentinel_path.read_bytes() == content
    sentinel = _replace_current_marker(root, content)
    return S75CompletionResult(
        config=config,
        config_artifact=config_artifact,
        completion=authority.completion,
        run_spec=authority.run_spec,
        summary=summary,
        sentinel_artifact=sentinel,
        reused=reused,
    )


def verify_s75_completion(
    data_root: Path,
    sentinel_artifact: ArtifactPin,
) -> S75CompletionResult:
    """Recheck factor invariants from the pinned DELTA and target partition."""

    root = data_root.expanduser().resolve()
    if sentinel_artifact.path != S75_CURRENT_MARKER_PATH:
        raise S75CompletionRuntimeError("S7.5 completion marker path differs")
    marker = _closed(
        _read_canonical(root, sentinel_artifact, "S7.5 completion marker"),
        {
            "artifact_type",
            "completion_available_session",
            "config_artifact",
            "delta_checkpoint_id",
            "delta_completion_id",
            "delta_native_release_id",
            "delta_release_id",
            "factor_hard_invariants",
            "marker_id",
            "rule_version",
            "run_spec_id",
            "s8_started",
            "state",
            "summary",
        },
        "S7.5 completion marker",
    )
    config_artifact = _artifact(marker["config_artifact"])
    config = _load_config(root, config_artifact)
    authority = _load_delta_authority(root, config)
    summary = _factor_summary(root, authority)
    expected = _marker_document(config_artifact, config, authority, summary)
    if marker != expected:
        raise S75CompletionRuntimeError("S7.5 completion marker does not reproduce")
    return S75CompletionResult(
        config=config,
        config_artifact=config_artifact,
        completion=authority.completion,
        run_spec=authority.run_spec,
        summary=summary,
        sentinel_artifact=sentinel_artifact,
        reused=True,
    )


def _load_delta_authority(root: Path, config: S75CompletionConfig) -> _DeltaAuthority:
    def reader(relative: str) -> bytes:
        return _read_path(root, relative)

    completion = load_i3_production_completion_exact(
        config.delta_completion_artifact,
        reader,
    )
    receipt = load_i3_production_run_receipt_exact(completion.receipt_artifact, reader)
    run_spec = load_i3_production_run_spec_exact(receipt.run_spec_artifact, reader)
    deep = load_i3_production_deep_attestation_exact(
        config.delta_deep_attestation_artifact,
        reader,
    )
    output_set = receipt.output_set
    if (
        run_spec.run_kind is not I3ProductionRunKind.DELTA
        or receipt.state is not I3ProductionRunState.SUCCEEDED
        or output_set is None
        or completion.run_spec_id != run_spec.run_spec_id
        or receipt.run_spec_id != run_spec.run_spec_id
        or completion.receipt_id != receipt.receipt_id
        or completion.output_set_id != output_set.output_set_id
        or completion.release_id != output_set.gate_a_manifest_pin.release_id
        or completion.native_v2_envelope_id != output_set.release_id
        or completion.checkpoint_id != output_set.checkpoint_id
        or deep.completion_id != completion.completion_id
        or deep.completion_artifact != config.delta_completion_artifact
        or deep.output_set_id != output_set.output_set_id
        or deep.checkpoint_id != output_set.checkpoint_id
        or deep.checkpoint_artifact != output_set.checkpoint_artifact
    ):
        raise S75CompletionRuntimeError("DELTA completion chain does not reconcile")
    if tuple(item.table_name for item in output_set.table_outputs) != I3_V2_TABLE_ORDER:
        raise S75CompletionRuntimeError("DELTA does not expose the four identity tables")
    latest_available = max(
        completion.completion_available_session,
        receipt.receipt_available_session,
        deep.attestation_available_session,
    )
    if config.completion_available_session < latest_available:
        raise S75CompletionRuntimeError("factor-ready availability predates the DELTA")
    return _DeltaAuthority(
        completion=completion,
        receipt=receipt,
        run_spec=run_spec,
        deep=deep,
        output_set=output_set,
    )


def _factor_summary(root: Path, authority: _DeltaAuthority) -> S75FactorSummary:
    universe = authority.output_set.table_outputs[-1]
    index = universe.dataset_index
    if index is None or not index.partitions:
        raise S75CompletionRuntimeError("DELTA universe dataset is empty")
    target = index.partitions[-1]
    if target.session_date != authority.run_spec.terminal_session:
        raise S75CompletionRuntimeError("DELTA target partition is not the terminal session")
    table = readback_i3_migration_parquet_exact(
        data_root=root,
        artifact=target.artifact,
        table_name="universe_daily",
        row_count=target.row_count,
        session_date=target.session_date,
    )
    rows = table.to_pylist()
    tickers: set[str] = set()
    eligible = 0
    unresolved = 0
    for row in rows:
        ticker = row.get("ticker")
        if not isinstance(ticker, str) or not ticker or ticker in tickers:
            raise S75CompletionRuntimeError("target partition has duplicate/invalid ticker")
        tickers.add(ticker)
        if row.get("session_date") != target.session_date:
            raise S75CompletionRuntimeError("target partition mixes sessions")
        if type(row.get("active_on_date")) is not bool:
            raise S75CompletionRuntimeError("membership activity flag is invalid")
        if row.get("identity_quality_liquidation_signal") is not False:
            raise S75CompletionRuntimeError("identity quality emitted forced liquidation")
        row_eligible = row.get("backtest_identity_eligible") is True
        if row_eligible:
            eligible += 1
            if not all(
                isinstance(row.get(name), str) and bool(row[name])
                for name in (
                    "asset_id",
                    "alias_segment_id",
                    "alias_resolution_version_id",
                    "asset_master_version_id",
                )
            ):
                raise S75CompletionRuntimeError(
                    f"eligible membership lacks identity linkage: {ticker}"
                )
            if row.get("position_continuity_status") != "resolved_identity":
                raise S75CompletionRuntimeError(
                    f"eligible membership has uncertain continuity: {ticker}"
                )
        else:
            unresolved += int(row.get("identity_resolution_status") == "unresolved")
            if row.get("position_continuity_status") != _CONTINUITY_UNCERTAIN:
                raise S75CompletionRuntimeError(
                    f"ineligible membership has unsafe continuity status: {ticker}"
                )
            if any(
                row.get(name) is not None
                for name in (
                    "alias_segment_id",
                    "alias_resolution_version_id",
                    "asset_master_version_id",
                    "issuer_master_version_id",
                )
            ):
                raise S75CompletionRuntimeError(
                    f"ineligible membership entered the tradable identity graph: {ticker}"
                )
        if row.get("composite_registry_collision") is True and row_eligible:
            raise S75CompletionRuntimeError(f"registry collision remained eligible: {ticker}")
    return S75FactorSummary(
        terminal_session=target.session_date,
        universe_row_count=len(rows),
        eligible_row_count=eligible,
        ineligible_row_count=len(rows) - eligible,
        unresolved_row_count=unresolved,
        target_partition=target.artifact,
    )


def _marker_document(
    config_artifact: ArtifactPin,
    config: S75CompletionConfig,
    authority: _DeltaAuthority,
    summary: S75FactorSummary,
) -> dict[str, object]:
    body: dict[str, object] = {
        "artifact_type": "s7_5_factor_ready_completion",
        "completion_available_session": config.completion_available_session.isoformat(),
        "config_artifact": config_artifact.to_dict(),
        "delta_checkpoint_id": authority.output_set.checkpoint_id,
        "delta_completion_id": authority.completion.completion_id,
        "delta_native_release_id": authority.output_set.release_id,
        "delta_release_id": authority.output_set.gate_a_manifest_pin.release_id,
        "factor_hard_invariants": [
            "one_membership_per_session_ticker",
            "eligible_rows_have_resolved_asset_and_alias",
            "ineligible_rows_cannot_open_new_trades",
            "identity_quality_never_forces_liquidation",
            "registry_collisions_are_ineligible",
            "exact_target_partition_schema_and_bytes",
        ],
        "rule_version": S75_COMPLETION_RUNTIME_RULE_VERSION,
        "run_spec_id": authority.run_spec.run_spec_id,
        "s8_started": False,
        "state": S75_COMPLETION_STATE,
        "summary": summary.to_dict(),
    }
    return {"marker_id": stable_digest(body), **body}


def _load_config(root: Path, artifact: ArtifactPin) -> S75CompletionConfig:
    content = _read_exact(root, artifact, "factor-ready config")
    config = S75CompletionConfig.from_dict(_json(content, "factor-ready config"))
    if config.canonical_bytes() != content:
        raise S75CompletionRuntimeError("factor-ready config is not canonical JSON")
    return config


def _write(root: Path, relative: str, content: bytes) -> ArtifactPin:
    stored = write_bytes_immutable(
        root,
        safe_relative_path(root, relative),
        content,
        temporary_directory=root / "tmp" / "s7-5-factor-ready",
    )
    return ArtifactPin(
        path=str(stored["path"]),
        sha256=str(stored["sha256"]),
        bytes=int(stored["bytes"]),
    )


def _replace_current_marker(root: Path, content: bytes) -> ArtifactPin:
    """Atomically advance the small current marker after immutable history exists."""

    destination = safe_relative_path(root, S75_CURRENT_MARKER_PATH)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary_root = root / "tmp" / "s7-5-factor-ready"
    temporary_root.mkdir(parents=True, exist_ok=True)
    fd, raw_path = tempfile.mkstemp(prefix="S7_5_COMPLETE.", suffix=".tmp", dir=temporary_root)
    temporary = Path(raw_path)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
    finally:
        if temporary.exists():
            temporary.unlink()
    return ArtifactPin(
        path=S75_CURRENT_MARKER_PATH,
        sha256=hashlib.sha256(content).hexdigest(),
        bytes=len(content),
    )


def _read_path(root: Path, relative: str) -> bytes:
    path = safe_relative_path(root, relative)
    if not path.is_file():
        raise S75CompletionRuntimeError(f"exact artifact is missing: {relative}")
    return path.read_bytes()


def _read_exact(root: Path, artifact: ArtifactPin, label: str) -> bytes:
    content = _read_path(root, artifact.path)
    if len(content) != artifact.bytes or hashlib.sha256(content).hexdigest() != artifact.sha256:
        raise S75CompletionRuntimeError(f"{label} exact pin differs")
    return content


def _read_canonical(root: Path, artifact: ArtifactPin, label: str) -> object:
    content = _read_exact(root, artifact, label)
    value = _json(content, label)
    if _canonical(value) != content:
        raise S75CompletionRuntimeError(f"{label} is not canonical JSON")
    return value


def _json(content: bytes, label: str) -> object:
    try:
        return json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise S75CompletionRuntimeError(f"{label} is not valid JSON") from exc


def _canonical(value: object) -> bytes:
    return (
        json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True).encode() + b"\n"
    )


def _closed(value: object, expected: set[str], label: str) -> dict[str, object]:
    if not isinstance(value, dict) or set(value) != expected:
        raise S75CompletionRuntimeError(f"{label} fields differ")
    return value


def _artifact(value: object) -> ArtifactPin:
    item = _closed(value, {"bytes", "path", "sha256"}, "artifact pin")
    return ArtifactPin(
        path=str(item["path"]),
        sha256=str(item["sha256"]),
        bytes=int(item["bytes"]),
    )


def _date(value: object) -> date:
    if not isinstance(value, str):
        raise S75CompletionRuntimeError("session date is invalid")
    try:
        result = date.fromisoformat(value)
    except ValueError as exc:
        raise S75CompletionRuntimeError("session date is invalid") from exc
    if result.isoformat() != value:
        raise S75CompletionRuntimeError("session date is not canonical")
    return result


__all__ = [
    "S75_COMPLETION_RUNTIME_RULE_VERSION",
    "S75_COMPLETION_STATE",
    "S75_CURRENT_MARKER_PATH",
    "S75CompletionConfig",
    "S75CompletionResult",
    "S75CompletionRuntimeError",
    "S75FactorSummary",
    "prepare_s75_completion",
    "stage_s75_completion",
    "verify_s75_completion",
]
