"""Record one exact S7 directional raw-preview execution approval receipt."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from ame_stocks_api.silver.identity_directional_raw_preview_approval import (
    IdentityDirectionalRawPreviewExecutionApprovalError,
    record_s7_directional_raw_preview_execution_approval,
)
from ame_stocks_api.silver.identity_directional_raw_preview_execution_plan import (
    IdentityDirectionalRawPreviewExecutionPlanError,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ame-silver-identity-directional-raw-preview-approval",
        description=(
            "Record one byte-exact execution approval receipt. This command cannot "
            "discover source files, read Parquet, or run the preview."
        ),
    )
    parser.add_argument("--data-root", type=Path, required=True)
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
    parser = build_parser()
    arguments = parser.parse_args(argv)
    try:
        run = record_s7_directional_raw_preview_execution_approval(
            arguments.data_root,
            plan_id=arguments.plan_id,
            expected_plan_sha256=arguments.plan_sha256,
            request_event_id=arguments.request_event_id,
            expected_request_event_sha256=arguments.request_event_sha256,
            approval_literal=arguments.approval_literal,
            approved_by=arguments.approved_by,
            approved_at=arguments.approved_at,
            approval_note=arguments.approval_note,
        )
    except (
        IdentityDirectionalRawPreviewExecutionApprovalError,
        IdentityDirectionalRawPreviewExecutionPlanError,
        OSError,
        TypeError,
        ValueError,
    ) as exc:
        parser.exit(
            2,
            f"ame-silver-identity-directional-raw-preview-approval: {exc}\n",
        )
    print(
        json.dumps(
            {
                "approval": {
                    "approval_id": run.approval.approval_id,
                    "path": run.approval_document.path,
                    "sha256": run.approval_document.sha256,
                    "approved_at_utc": run.approval.approved_at_utc.isoformat(),
                    "once_to_awaiting_review": run.approval.once_to_awaiting_review,
                    "preview_execution_authorized": (
                        run.approval.preview_execution_authorized
                    ),
                },
                "approval_document_preexisting": run.approval_document_preexisting,
                "mode": "exact_execution_approval_receipt_only",
                "preview_executed": False,
                "plan_id": run.plan.plan_id,
                "request_event_id": run.request.request_event_id,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
