"""Record the exact human literal for the S7 manifest-only preflight."""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

from ame_stocks_api.silver.identity_directional_raw_preview_manifest_approval import (
    IdentityDirectionalRawPreviewManifestApprovalError,
    record_s7_directional_raw_preview_manifest_approval,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--control-root", type=Path, required=True)
    parser.add_argument("--plan-id", required=True)
    parser.add_argument("--plan-sha256", required=True)
    parser.add_argument("--request-event-id", required=True)
    parser.add_argument("--request-event-sha256", required=True)
    parser.add_argument("--approval-literal", required=True)
    parser.add_argument("--approved-by", required=True)
    parser.add_argument("--approved-at", required=True)
    parser.add_argument("--approval-note", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        approval, receipt = record_s7_directional_raw_preview_manifest_approval(
            args.control_root,
            plan_id=args.plan_id,
            plan_sha256=args.plan_sha256,
            request_event_id=args.request_event_id,
            request_event_sha256=args.request_event_sha256,
            approval_literal=args.approval_literal,
            approved_by=args.approved_by,
            approved_at_utc=datetime.fromisoformat(args.approved_at),
            approval_note=args.approval_note,
        )
    except (IdentityDirectionalRawPreviewManifestApprovalError, OSError, ValueError) as exc:
        raise SystemExit(f"manifest approval: {exc}") from exc
    print(
        json.dumps(
            {
                "approval_id": approval.approval_id,
                "path": receipt.path,
                "sha256": receipt.sha256,
                "manifest_only": True,
                "parquet_content_read": False,
                "preview_executed": False,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
