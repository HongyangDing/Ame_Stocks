"""Run the exact S7 manifest-only source-binding preflight."""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

from ame_stocks_api.silver.identity_exact_group_history_manifest import (
    run_exact_group_history_manifest_preflight,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--repository-root", type=Path, required=True)
    parser.add_argument("--manifest-plan-id", required=True)
    parser.add_argument("--manifest-plan-sha256", required=True)
    parser.add_argument("--manifest-approval-id", required=True)
    parser.add_argument("--manifest-approval-sha256", required=True)
    parser.add_argument(
        "--source-binding-created-at-utc", type=datetime.fromisoformat, required=True
    )
    args = parser.parse_args()
    result = run_exact_group_history_manifest_preflight(
        data_root=args.data_root,
        repository_root=args.repository_root,
        manifest_plan_id=args.manifest_plan_id,
        manifest_plan_sha256=args.manifest_plan_sha256,
        manifest_approval_id=args.manifest_approval_id,
        manifest_approval_sha256=args.manifest_approval_sha256,
        source_binding_created_at_utc=args.source_binding_created_at_utc,
    )
    payload = {
        "attempt_parquet_content_bytes_read": result.attempt_parquet_content_bytes_read,
        "attempt_parquet_lstats": result.attempt_parquet_lstats,
        "attempt_source_json_reads": result.attempt_source_json_reads,
        "completion_id": result.completion.completion_id,
        "completion_path": result.completion_receipt.path,
        "completion_sha256": result.completion.sha256,
        "execution_plan_id": result.completion.execution_plan.logical_id,
        "execution_request_event_id": result.completion.execution_request.logical_id,
        "recovered": result.recovered,
        "state": "awaiting_review",
    }
    print(json.dumps(payload, separators=(",", ":"), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
