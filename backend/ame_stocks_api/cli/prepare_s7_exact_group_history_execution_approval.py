"""Record the byte-exact approval for the future S7 exact-group scan."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from ame_stocks_api.silver.identity_exact_group_history_approval import (
    IdentityExactGroupHistoryApprovalError,
    record_s7_exact_group_history_execution_approval,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Record one exact S7 full-S4 three-group execution approval."
    )
    parser.add_argument("--data-root", required=True, type=Path)
    parser.add_argument("--plan-id", required=True)
    parser.add_argument("--plan-sha256", required=True)
    parser.add_argument("--request-event-id", required=True)
    parser.add_argument("--request-event-sha256", required=True)
    parser.add_argument("--approval-literal", required=True)
    parser.add_argument("--approved-by", required=True)
    parser.add_argument("--approved-at", required=True)
    parser.add_argument("--approval-note", default="")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        _, _, approval, receipt = record_s7_exact_group_history_execution_approval(
            args.data_root,
            plan_id=args.plan_id,
            expected_plan_sha256=args.plan_sha256,
            request_event_id=args.request_event_id,
            expected_request_event_sha256=args.request_event_sha256,
            approval_literal=args.approval_literal,
            approved_by=args.approved_by,
            approved_at=args.approved_at,
            approval_note=args.approval_note,
        )
    except IdentityExactGroupHistoryApprovalError as exc:
        raise SystemExit(f"exact-group execution approval failed: {exc}") from exc
    print(
        json.dumps(
            {
                "approval_id": approval.approval_id,
                "approval_path": receipt.path,
                "approval_sha256": receipt.sha256,
                "authorized_action": approval.authorized_action,
                "state": "approved_not_executed",
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
