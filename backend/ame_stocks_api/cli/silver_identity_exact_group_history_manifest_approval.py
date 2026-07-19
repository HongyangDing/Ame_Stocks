"""Record one exact manifest-only approval for S7 exact-group history."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

from ame_stocks_api.silver.identity_exact_group_history_manifest import (
    record_exact_group_history_manifest_approval,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--control-root", type=Path, required=True)
    parser.add_argument("--manifest-plan-id", required=True)
    parser.add_argument("--manifest-plan-sha256", required=True)
    parser.add_argument("--manifest-request-event-id", required=True)
    parser.add_argument("--manifest-request-event-sha256", required=True)
    parser.add_argument("--approval-literal", required=True)
    parser.add_argument("--approved-by", required=True)
    parser.add_argument("--approved-at-utc", type=datetime.fromisoformat, required=True)
    args = parser.parse_args()
    receipt = record_exact_group_history_manifest_approval(
        control_root=args.control_root,
        manifest_plan_id=args.manifest_plan_id,
        manifest_plan_sha256=args.manifest_plan_sha256,
        manifest_request_event_id=args.manifest_request_event_id,
        manifest_request_event_sha256=args.manifest_request_event_sha256,
        approval_literal=args.approval_literal,
        approved_by=args.approved_by,
        approved_at_utc=args.approved_at_utc,
    )
    print(json.dumps(asdict(receipt), separators=(",", ":"), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
