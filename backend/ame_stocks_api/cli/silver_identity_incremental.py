"""Single local-only CLI for S7.5 I3 production staging and verification."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from datetime import date
from pathlib import Path

from ame_stocks_api.artifacts import safe_relative_path
from ame_stocks_api.silver.incremental_contract import ArtifactPin
from ame_stocks_api.silver.incremental_i3_delta_io import (
    I3ProductionDeltaRunConfig,
    PreparedI3ProductionDeltaRunSpec,
    prepare_i3_production_delta_run_spec,
    store_i3_production_delta_config,
)
from ame_stocks_api.silver.incremental_i3_migration_io import (
    load_compact_base_materializer,
)
from ame_stocks_api.silver.incremental_i3_production import (
    FAILED_RECEIPT_DURABLE_BEFORE_COMPLETION,
    I3ProductionInterruptedRetryPending,
    I3ProductionInterruptedRetryResult,
    I3ProductionStageError,
    I3ProductionStageResult,
    exercise_i3_production_interrupted_retry,
    stage_i3_production_base,
    stage_i3_production_delta,
    validate_production_delta_run_spec_artifact_path,
    verify_i3_production_deep_attestation,
)
from ame_stocks_api.silver.incremental_i3_production_contract import (
    I3ProductionResourceCaps,
    I3ProductionRunKind,
    LoadedI3ProductionStaging,
    load_i3_production_run_spec_exact,
)
from ame_stocks_api.silver.incremental_i3_production_inputs import (
    I3ProductionBaseRunConfig,
    PreparedI3ProductionBaseRunSpec,
    prepare_i3_production_base_run_spec,
    store_i3_production_base_config,
)


class I3ProductionCliError(RuntimeError):
    """Raised for an unavailable or malformed production CLI binding."""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ame-silver-identity-incremental",
        description=(
            "Stage or exact-verify S7.5 I3 native-v2 candidates. "
            "Every command is local-only and grants no publish authority."
        ),
    )
    commands = parser.add_subparsers(dest="command", required=True)
    prepare_base = commands.add_parser("prepare-base")
    _add_data_root(prepare_base)
    _add_pin(prepare_base, "config", required=False)
    for label in ("s7-release-set", "s4-release-set", "i2-base-frontier"):
        _add_pin(prepare_base, label, required=False)
    prepare_base.add_argument("--run-available-session")
    for field in (
        "rss-bytes-hard-cap",
        "disk-free-bytes-hard-floor",
        "temporary-bytes-hard-cap",
        "output-bytes-hard-cap",
        "output-rows-hard-cap",
    ):
        prepare_base.add_argument(f"--{field}", type=int)
    prepare_delta = commands.add_parser("prepare-delta")
    _add_data_root(prepare_delta)
    _add_pin(prepare_delta, "config", required=False)
    for label in ("parent-completion", "parent-deep-attestation", "i2-receipt"):
        _add_pin(prepare_delta, label, required=False)
    prepare_delta.add_argument("--run-available-session")
    for field in (
        "rss-bytes-hard-cap",
        "disk-free-bytes-hard-floor",
        "temporary-bytes-hard-cap",
        "output-bytes-hard-cap",
        "output-rows-hard-cap",
    ):
        prepare_delta.add_argument(f"--{field}", type=int)
    for name in ("stage-base", "stage-delta"):
        command = commands.add_parser(name)
        _add_data_root(command)
        _add_pin(command, "run-spec")
    exercise_delta = commands.add_parser("exercise-delta-retry")
    _add_data_root(exercise_delta)
    _add_pin(exercise_delta, "run-spec")
    for name in ("verify-base", "verify-delta"):
        command = commands.add_parser(name)
        _add_data_root(command)
        _add_pin(command, "completion")
        _add_pin(command, "deep-attestation")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        root = Path(args.data_root).expanduser().resolve()
        if args.command == "prepare-base":
            prepared = _prepare_base(root, args)
            payload = {
                "command": args.command,
                "config": prepared.config_artifact.to_dict(),
                "identity_policy_bundle": (prepared.identity_policy_bundle_artifact.to_dict()),
                "publish_authorized": False,
                "run_kind": "base",
                "run_spec": prepared.run_spec_artifact.to_dict(),
                "run_spec_id": prepared.run_spec.run_spec_id,
                "state": "prepared",
            }
        elif args.command == "prepare-delta":
            prepared = _prepare_delta(root, args)
            payload = {
                "command": args.command,
                "config": prepared.config_artifact.to_dict(),
                "publish_authorized": False,
                "run_kind": "delta",
                "run_spec": prepared.run_spec_artifact.to_dict(),
                "run_spec_id": prepared.run_spec.run_spec_id,
                "state": "prepared",
            }
        elif args.command == "stage-base":
            result = _stage_base(root, _pin_from_args(args, "run_spec"))
            payload = _stage_payload(args.command, result)
        elif args.command == "stage-delta":
            result = _stage_delta(root, _pin_from_args(args, "run_spec"))
            payload = _stage_payload(args.command, result)
        elif args.command == "exercise-delta-retry":
            result = exercise_i3_production_interrupted_retry(
                root,
                _pin_from_args(args, "run_spec"),
                fail_after=FAILED_RECEIPT_DURABLE_BEFORE_COMPLETION,
            )
            payload = _interrupted_retry_payload(args.command, result)
        else:
            kind = (
                I3ProductionRunKind.BASE
                if args.command == "verify-base"
                else I3ProductionRunKind.DELTA
            )
            completion = _pin_from_args(args, "completion")
            deep = _pin_from_args(args, "deep_attestation")
            loaded = verify_i3_production_deep_attestation(
                root,
                completion,
                deep,
                expected_kind=kind,
            )
            payload = _loaded_payload(
                args.command,
                loaded,
                completion_pin=completion,
                deep_attestation_pin=deep,
                reused=True,
            )
    except I3ProductionInterruptedRetryPending as exc:
        print(
            json.dumps(
                {
                    "command": args.command,
                    "failed_receipt": exc.failed_receipt_artifact.to_dict(),
                    "failpoint": FAILED_RECEIPT_DURABLE_BEFORE_COMPLETION,
                    "phase_one": exc.phase_one_artifact.to_dict(),
                    "publish_authorized": False,
                    "state": "interrupted_retry_pending",
                },
                allow_nan=False,
                sort_keys=True,
            )
        )
        return 75
    except Exception as exc:
        failure: dict[str, object] = {
            "command": args.command,
            "error": str(exc),
            "state": "failed",
        }
        if isinstance(exc, I3ProductionStageError) and exc.failed_receipt_pin is not None:
            failure["failed_receipt"] = exc.failed_receipt_pin.to_dict()
        print(json.dumps(failure, allow_nan=False, sort_keys=True), file=sys.stderr)
        return 2
    print(json.dumps(payload, allow_nan=False, sort_keys=True))
    return 0


def _stage_base(root: Path, run_spec_pin: ArtifactPin) -> I3ProductionStageResult:
    run_spec = _load_run_spec(root, run_spec_pin)
    if run_spec.run_kind is not I3ProductionRunKind.BASE:
        raise I3ProductionCliError("stage-base requires an exact base RunSpec")
    materializer = load_compact_base_materializer(data_root=root, run_spec=run_spec)
    return stage_i3_production_base(root, run_spec_pin, materializer=materializer)


def _prepare_base(
    root: Path,
    args: argparse.Namespace,
) -> PreparedI3ProductionBaseRunSpec:
    config_pin = _optional_pin_from_args(args, "config")
    direct_prefixes = ("s7_release_set", "s4_release_set", "i2_base_frontier")
    direct_pins = tuple(_optional_pin_from_args(args, item) for item in direct_prefixes)
    cap_fields = (
        "rss_bytes_hard_cap",
        "disk_free_bytes_hard_floor",
        "temporary_bytes_hard_cap",
        "output_bytes_hard_cap",
        "output_rows_hard_cap",
    )
    direct_scalars = (args.run_available_session, *(getattr(args, item) for item in cap_fields))
    if config_pin is not None:
        if any(item is not None for item in (*direct_pins, *direct_scalars)):
            raise I3ProductionCliError(
                "prepare-base accepts either one exact config pin or direct exact inputs"
            )
    else:
        if any(item is None for item in (*direct_pins, *direct_scalars)):
            raise I3ProductionCliError(
                "direct prepare-base requires three exact pins, run availability, and all caps"
            )
        try:
            available = date.fromisoformat(args.run_available_session)
        except ValueError as exc:
            raise I3ProductionCliError(
                "run availability must be a canonical ISO session date"
            ) from exc
        if available.isoformat() != args.run_available_session:
            raise I3ProductionCliError("run availability must be a canonical ISO session date")
        config = I3ProductionBaseRunConfig(
            s7_release_set_artifact=direct_pins[0],
            s4_release_set_artifact=direct_pins[1],
            i2_base_frontier_artifact=direct_pins[2],
            run_available_session=available,
            resource_caps=I3ProductionResourceCaps(
                **{field: getattr(args, field) for field in cap_fields}
            ),
        )
        config_pin = store_i3_production_base_config(root, config)
    return prepare_i3_production_base_run_spec(root, config_pin)


def _stage_delta(root: Path, run_spec_pin: ArtifactPin) -> I3ProductionStageResult:
    validate_production_delta_run_spec_artifact_path(run_spec_pin)
    run_spec = _load_run_spec(root, run_spec_pin)
    if run_spec.run_kind is not I3ProductionRunKind.DELTA:
        raise I3ProductionCliError("stage-delta requires an exact delta RunSpec")
    validate_production_delta_run_spec_artifact_path(
        run_spec_pin,
        run_spec_id=run_spec.run_spec_id,
    )
    try:
        from ame_stocks_api.silver.incremental_i3_delta_io import (
            load_production_delta_materializer,
        )
    except ModuleNotFoundError as exc:
        if exc.name != "ame_stocks_api.silver.incremental_i3_delta_io":
            raise
        raise I3ProductionCliError(
            "stage-delta is fail-closed: no production delta IO adapter is installed"
        ) from exc
    materializer = load_production_delta_materializer(
        data_root=root,
        run_spec=run_spec,
    )
    return stage_i3_production_delta(root, run_spec_pin, materializer=materializer)


def _prepare_delta(
    root: Path,
    args: argparse.Namespace,
) -> PreparedI3ProductionDeltaRunSpec:
    config_pin = _optional_pin_from_args(args, "config")
    direct_prefixes = (
        "parent_completion",
        "parent_deep_attestation",
        "i2_receipt",
    )
    direct_pins = tuple(_optional_pin_from_args(args, item) for item in direct_prefixes)
    cap_fields = (
        "rss_bytes_hard_cap",
        "disk_free_bytes_hard_floor",
        "temporary_bytes_hard_cap",
        "output_bytes_hard_cap",
        "output_rows_hard_cap",
    )
    direct_scalars = (args.run_available_session, *(getattr(args, item) for item in cap_fields))
    if config_pin is not None:
        if any(item is not None for item in (*direct_pins, *direct_scalars)):
            raise I3ProductionCliError(
                "prepare-delta accepts either one exact config pin or direct exact inputs"
            )
    else:
        if any(item is None for item in (*direct_pins, *direct_scalars)):
            raise I3ProductionCliError(
                "direct prepare-delta requires parent completion/deep, one I2 receipt, "
                "run availability, and all caps"
            )
        try:
            available = date.fromisoformat(args.run_available_session)
        except ValueError as exc:
            raise I3ProductionCliError(
                "run availability must be a canonical ISO session date"
            ) from exc
        if available.isoformat() != args.run_available_session:
            raise I3ProductionCliError("run availability must be a canonical ISO session date")
        config = I3ProductionDeltaRunConfig(
            parent_completion_artifact=direct_pins[0],
            parent_deep_attestation_artifact=direct_pins[1],
            i2_receipt_artifact=direct_pins[2],
            run_available_session=available,
            resource_caps=I3ProductionResourceCaps(
                **{field: getattr(args, field) for field in cap_fields}
            ),
        )
        config_pin = store_i3_production_delta_config(root, config)
    return prepare_i3_production_delta_run_spec(root, config_pin)


def _load_run_spec(root: Path, pin: ArtifactPin):
    return load_i3_production_run_spec_exact(
        pin,
        lambda relative: _read_exact_local(root, relative),
    )


def _read_exact_local(root: Path, relative: str) -> bytes:
    path = safe_relative_path(root, relative)
    if not path.is_file() or path.is_symlink():
        raise I3ProductionCliError(f"exact local artifact is missing: {relative}")
    return path.read_bytes()


def _stage_payload(command: str, result: I3ProductionStageResult) -> dict[str, object]:
    return _loaded_payload(
        command,
        result.loaded,
        completion_pin=result.completion_pin,
        deep_attestation_pin=result.deep_attestation_pin,
        reused=result.reused,
    )


def _loaded_payload(
    command: str,
    loaded: LoadedI3ProductionStaging,
    *,
    completion_pin: ArtifactPin,
    deep_attestation_pin: ArtifactPin,
    reused: bool,
) -> dict[str, object]:
    return {
        "checkpoint_id": loaded.checkpoint.checkpoint_id,
        "command": command,
        "completion": completion_pin.to_dict(),
        "deep_attestation": deep_attestation_pin.to_dict(),
        "gate_a_release_id": loaded.gate_a_manifest.release_id,
        "native_v2_envelope_id": loaded.manifest.release_id,
        "publish_authorized": False,
        "reused": reused,
        "run_kind": loaded.run_spec.run_kind.value,
        "run_spec_id": loaded.run_spec.run_spec_id,
        "state": "awaiting_review",
    }


def _interrupted_retry_payload(
    command: str,
    result: I3ProductionInterruptedRetryResult,
) -> dict[str, object]:
    return {
        "command": command,
        "completion": result.stage_result.completion_pin.to_dict(),
        "deep_attestation": result.stage_result.deep_attestation_pin.to_dict(),
        "exercise_receipt": result.receipt_pin.to_dict(),
        "failed_receipt": result.receipt.failed_receipt_artifact.to_dict(),
        "failpoint": result.receipt.fail_after,
        "phase_one": result.receipt.phase_one_artifact.to_dict(),
        "publish_authorized": False,
        "reused": result.reused,
        "run_kind": result.stage_result.loaded.run_spec.run_kind.value,
        "run_spec_id": result.receipt.run_spec_id,
        "state": "awaiting_review",
    }


def _add_data_root(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--data-root", required=True)


def _add_pin(
    parser: argparse.ArgumentParser,
    label: str,
    *,
    required: bool = True,
) -> None:
    option = label.replace("-", "_")
    parser.add_argument(f"--{label}-path", required=required, dest=f"{option}_path")
    parser.add_argument(f"--{label}-sha256", required=required, dest=f"{option}_sha256")
    parser.add_argument(
        f"--{label}-bytes",
        required=required,
        type=int,
        dest=f"{option}_bytes",
    )


def _pin_from_args(args: argparse.Namespace, prefix: str) -> ArtifactPin:
    return ArtifactPin(
        path=getattr(args, f"{prefix}_path"),
        sha256=getattr(args, f"{prefix}_sha256"),
        bytes=getattr(args, f"{prefix}_bytes"),
    )


def _optional_pin_from_args(
    args: argparse.Namespace,
    prefix: str,
) -> ArtifactPin | None:
    values = (
        getattr(args, f"{prefix}_path"),
        getattr(args, f"{prefix}_sha256"),
        getattr(args, f"{prefix}_bytes"),
    )
    if all(value is None for value in values):
        return None
    if any(value is None for value in values):
        raise I3ProductionCliError(f"{prefix.replace('_', '-')} pin is incomplete")
    return ArtifactPin(path=values[0], sha256=values[1], bytes=values[2])


__all__ = ["I3ProductionCliError", "build_parser", "main"]


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
