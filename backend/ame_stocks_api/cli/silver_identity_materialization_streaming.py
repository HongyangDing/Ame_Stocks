"""Run the production S7 streaming control chain through Full awaiting-review only.

This CLI has no Publish command and accepts no source rows, decision maps, caller clock,
or projection adapter.  Production Full always uses the frozen registry adapter.
"""

from __future__ import annotations

import argparse
import json
from datetime import UTC, date, datetime
from pathlib import Path

from ame_stocks_api.silver.identity_materialization_streaming import (
    S7_STANDING_AUTHORIZATION_TEXT,
    S7_STANDING_REAFFIRMATION_TEXT,
    TABLE_ORDER,
    S7StreamingMaterializationError,
    StreamingResourceCaps,
    build_and_store_production_streaming_source_binding,
    execute_streaming_bounded_profile_preview,
    execute_streaming_full_candidate,
    prepare_streaming_approval_request,
    prepare_streaming_bounded_profile_preview_plan,
    prepare_streaming_full_plan,
    record_standing_streaming_approval,
    record_standing_streaming_profile_approval,
    record_standing_v4_contract_approval,
)
from ame_stocks_api.silver.identity_materialization_streaming import (
    _canonical_bytes as canonical_bytes,
)
from ame_stocks_api.silver.identity_registry_workflow import RegistryReleasePin


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ame-silver-identity-streaming",
        description=(
            "Build the source-bound S7 profile and four-table Full candidate control "
            "chain. Every execution stops at awaiting_review; Publish is unavailable."
        ),
    )
    commands = parser.add_subparsers(dest="command", required=True)

    contract = commands.add_parser(
        "record-contract-approval", help="record one exact fixed-slot v4 contract approval"
    )
    _data_root(contract)
    contract.add_argument("--table-name", choices=TABLE_ORDER, required=True)
    contract.add_argument("--calendar-artifact-id", required=True)
    contract.add_argument("--calendar-artifact-sha256", required=True)
    contract.add_argument("--approved-by", required=True)

    binding = commands.add_parser(
        "store-source-binding",
        help="build a production binding from five exact registry releases",
    )
    _data_root(binding)
    binding.add_argument("--registry-pins-json", type=Path, required=True)
    binding.add_argument("--cutoff-session", type=date.fromisoformat, required=True)

    profile_plan = commands.add_parser(
        "prepare-profile", help="freeze the bounded size/profile plan"
    )
    _data_root(profile_plan)
    profile_plan.add_argument("--source-binding-id", required=True)
    profile_plan.add_argument("--resource-caps-json", type=Path, required=True)
    profile_plan.add_argument("--sample-session-cap", type=int, required=True)
    profile_plan.add_argument("--prepared-by", required=True)

    profile_approval = commands.add_parser(
        "approve-profile", help="bind standing S7 authority to one exact profile plan"
    )
    _data_root(profile_approval)
    profile_approval.add_argument("--plan-id", required=True)
    profile_approval.add_argument("--approved-by", required=True)

    profile_run = commands.add_parser(
        "execute-profile", help="execute the exact bounded profile and stop awaiting review"
    )
    _data_root(profile_run)
    profile_run.add_argument("--plan-id", required=True)
    profile_run.add_argument("--approval-id", required=True)

    full_plan = commands.add_parser(
        "prepare-full", help="freeze a Full plan bound to the completed profile"
    )
    _data_root(full_plan)
    full_plan.add_argument("--source-binding-id", required=True)
    full_plan.add_argument("--resource-caps-json", type=Path, required=True)
    full_plan.add_argument("--profile-plan-id", required=True)
    full_plan.add_argument("--profile-approval-id", required=True)
    full_plan.add_argument("--prepared-by", required=True)

    request = commands.add_parser("prepare-request", help="freeze the exact Full approval request")
    _data_root(request)
    request.add_argument("--plan-id", required=True)
    request.add_argument("--requested-by", required=True)

    full_approval = commands.add_parser(
        "approve-full-standing", help="bind standing S7 authority to one exact Full request"
    )
    _data_root(full_approval)
    full_approval.add_argument("--request-id", required=True)
    full_approval.add_argument("--approved-by", required=True)

    full_run = commands.add_parser(
        "execute-full", help="execute frozen production projection and stop awaiting review"
    )
    _data_root(full_run)
    full_run.add_argument("--plan-id", required=True)
    full_run.add_argument("--approval-id", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    root = args.data_root.expanduser()
    try:
        if args.command == "record-contract-approval":
            control = record_standing_v4_contract_approval(
                root,
                table_name=args.table_name,
                calendar_artifact_id=args.calendar_artifact_id,
                calendar_artifact_sha256=args.calendar_artifact_sha256,
                authorization_text=S7_STANDING_AUTHORIZATION_TEXT,
                reaffirmation_text=S7_STANDING_REAFFIRMATION_TEXT,
                approved_by=args.approved_by,
            )
            _print({"approval_id": control.logical_id, "receipt": control.receipt.to_dict()})
            return 0
        if args.command == "store-source-binding":
            binding, control = build_and_store_production_streaming_source_binding(
                root,
                registry_pins=_load_registry_pins(args.registry_pins_json),
                cutoff_session=args.cutoff_session,
            )
            _print(
                {
                    "receipt": control.receipt.to_dict(),
                    "source_binding": binding.to_dict(),
                    "source_binding_id": control.logical_id,
                }
            )
            return 0
        if args.command == "prepare-profile":
            plan, control = prepare_streaming_bounded_profile_preview_plan(
                root,
                source_binding_id=args.source_binding_id,
                full_resource_caps=_load_caps(args.resource_caps_json),
                sample_session_cap=args.sample_session_cap,
                prepared_by=args.prepared_by,
                prepared_at_utc=datetime.now(UTC),
            )
            _print({"plan": plan, "receipt": control.receipt.to_dict()})
            return 0
        if args.command == "approve-profile":
            approval, control = record_standing_streaming_profile_approval(
                root,
                plan_id=args.plan_id,
                authorization_text=S7_STANDING_AUTHORIZATION_TEXT,
                reaffirmation_text=S7_STANDING_REAFFIRMATION_TEXT,
                approved_by=args.approved_by,
            )
            _print({"approval": approval, "receipt": control.receipt.to_dict()})
            return 0
        if args.command == "execute-profile":
            completion = execute_streaming_bounded_profile_preview(
                root, plan_id=args.plan_id, approval_id=args.approval_id
            )
            _print(completion)
            return 0
        if args.command == "prepare-full":
            plan, control = prepare_streaming_full_plan(
                root,
                source_binding_id=args.source_binding_id,
                resource_caps=_load_caps(args.resource_caps_json),
                prepared_by=args.prepared_by,
                prepared_at_utc=datetime.now(UTC),
                profile_plan_id=args.profile_plan_id,
                profile_approval_id=args.profile_approval_id,
            )
            _print({"plan": plan, "receipt": control.receipt.to_dict()})
            return 0
        if args.command == "prepare-request":
            request, control = prepare_streaming_approval_request(
                root,
                plan_id=args.plan_id,
                requested_by=args.requested_by,
                requested_at_utc=datetime.now(UTC),
            )
            _print({"receipt": control.receipt.to_dict(), "request": request.to_dict()})
            return 0
        if args.command == "approve-full-standing":
            approval, control = record_standing_streaming_approval(
                root,
                request_id=args.request_id,
                authorization_text=S7_STANDING_AUTHORIZATION_TEXT,
                reaffirmation_text=S7_STANDING_REAFFIRMATION_TEXT,
                approved_by=args.approved_by,
            )
            _print({"approval": approval.to_dict(), "receipt": control.receipt.to_dict()})
            return 0
        if args.command == "execute-full":
            result = execute_streaming_full_candidate(
                root,
                plan_id=args.plan_id,
                approval_id=args.approval_id,
            )
            _print(
                {
                    "approval_id": result.approval_id,
                    "candidate_id": result.candidate_id,
                    "candidate_path": result.candidate_path,
                    "completion_id": result.completion_id,
                    "completion_path": result.completion_path,
                    "idempotent": result.idempotent,
                    "plan_id": result.plan_id,
                    "raw_collision_rows": result.raw_collision_rows,
                    "session_count": result.session_count,
                    "source_row_count": result.source_row_count,
                    "state": result.state,
                    "table_row_counts": dict(result.table_row_counts),
                }
            )
            return 0
        raise AssertionError("argparse accepted an unknown command")  # pragma: no cover
    except (json.JSONDecodeError, OSError, S7StreamingMaterializationError, ValueError) as exc:
        parser.exit(2, f"ame-silver-identity-streaming: {exc}\n")


def _data_root(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--data-root", type=Path, required=True)


def _load_json(path: Path) -> object:
    candidate = path.expanduser()
    if not candidate.is_file() or candidate.is_symlink():
        raise ValueError("JSON input is missing or unsafe")
    return json.loads(candidate.read_bytes())


def _load_canonical_manifest(path: Path) -> object:
    candidate = path.expanduser()
    if not candidate.is_file() or candidate.is_symlink():
        raise ValueError("source-binding manifest is missing or unsafe")
    content = candidate.read_bytes()
    value = json.loads(content)
    if canonical_bytes(value) != content:
        raise ValueError("source-binding manifest must be canonical JSON bytes")
    return value


def _load_registry_pins(path: Path) -> tuple[RegistryReleasePin, ...]:
    value = _load_canonical_manifest(path)
    if not isinstance(value, list):
        raise ValueError("registry pin document must be a canonical JSON array")
    return tuple(RegistryReleasePin.from_dict(item) for item in value)


def _load_caps(path: Path) -> StreamingResourceCaps:
    return StreamingResourceCaps.from_dict(_load_json(path))


def _print(value: object) -> None:
    print(json.dumps(value, allow_nan=False, indent=2, sort_keys=True))


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
