"""Run the independent S7 four-table Publish control chain.

The CLI accepts exact IDs only.  It has no clock, path override, adapter, source-row,
receipt-JSON, or latest-discovery inputs; production functions enforce the canonical
data root.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from ame_stocks_api.silver.identity_materialization_publish import (
    S7IdentityPublishError,
    load_published_s7_release_set,
    prepare_s7_publish_plan,
    publish_s7_release_set,
    record_standing_s7_publish_approval,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ame-silver-identity-publish",
        description=(
            "Prepare, approve, publish, and verify one exact visibility-atomic S7 "
            "four-table release set."
        ),
    )
    commands = parser.add_subparsers(dest="command", required=True)

    prepare = commands.add_parser(
        "prepare-plan", help="freeze one PublishPlan from an exact completed Full"
    )
    _data_root(prepare)
    prepare.add_argument("--full-plan-id", required=True)
    prepare.add_argument("--full-approval-id", required=True)
    prepare.add_argument("--expected-completion-id", required=True)
    prepare.add_argument("--expected-candidate-id", required=True)
    prepare.add_argument("--prepared-by", required=True)

    approve = commands.add_parser(
        "approve-standing", help="record standing authority for one exact PublishPlan"
    )
    _data_root(approve)
    approve.add_argument("--publish-plan-id", required=True)
    approve.add_argument("--approved-by", required=True)

    publish = commands.add_parser(
        "publish-release-set",
        help="write four hidden members and the final atomic visibility marker",
    )
    _data_root(publish)
    publish.add_argument("--publish-plan-id", required=True)
    publish.add_argument("--approval-id", required=True)

    verify = commands.add_parser(
        "verify-release-set", help="verify one exact published release-set ID"
    )
    _data_root(verify)
    verify.add_argument("--release-set-id", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    root = args.data_root.expanduser()
    try:
        if args.command == "prepare-plan":
            plan, receipt = prepare_s7_publish_plan(
                root,
                full_plan_id=args.full_plan_id,
                full_approval_id=args.full_approval_id,
                expected_completion_id=args.expected_completion_id,
                expected_candidate_id=args.expected_candidate_id,
                prepared_by=args.prepared_by,
            )
            _print({"plan": plan, "receipt": receipt.to_dict()})
            return 0
        if args.command == "approve-standing":
            approval, receipt = record_standing_s7_publish_approval(
                root,
                publish_plan_id=args.publish_plan_id,
                approved_by=args.approved_by,
            )
            _print({"approval": approval, "receipt": receipt.to_dict()})
            return 0
        if args.command == "publish-release-set":
            result = publish_s7_release_set(
                root,
                publish_plan_id=args.publish_plan_id,
                approval_id=args.approval_id,
            )
            _print(
                {
                    "approval_id": result.approval_id,
                    "idempotent": result.idempotent,
                    "intent_id": result.intent_id,
                    "member_release_ids": dict(result.member_release_ids),
                    "publish_plan_id": result.publish_plan_id,
                    "release_available_session": (result.release_available_session.isoformat()),
                    "release_set_id": result.release_set_id,
                    "release_set_path": result.release_set_path,
                    "state": result.state,
                }
            )
            return 0
        if args.command == "verify-release-set":
            marker = load_published_s7_release_set(root, release_set_id=args.release_set_id)
            _print(marker)
            return 0
        raise AssertionError("argparse accepted an unknown command")  # pragma: no cover
    except (OSError, S7IdentityPublishError, ValueError) as exc:
        parser.exit(2, f"ame-silver-identity-publish: {exc}\n")


def _data_root(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--data-root", type=Path, required=True)


def _print(value: object) -> None:
    print(json.dumps(value, allow_nan=False, indent=2, sort_keys=True))


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
