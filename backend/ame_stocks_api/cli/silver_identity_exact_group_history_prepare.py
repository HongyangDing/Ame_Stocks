"""Build non-reading S7 exact-group preparation/manifest controls."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

from ame_stocks_api.silver.identity_exact_group_history_contract import (
    IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_CONTRACT_ID,
    IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_RESOURCE_SHA256,
    IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_SCHEMA_DIGEST,
)
from ame_stocks_api.silver.identity_exact_group_history_manifest import (
    build_exact_group_history_manifest_controls,
)
from ame_stocks_api.silver.identity_exact_group_history_plan import (
    prepare_exact_group_history_controls,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    initial = sub.add_parser("initial")
    initial.add_argument("--repository-root", type=Path, required=True)
    initial.add_argument("--control-root", type=Path, required=True)
    initial.add_argument("--git-commit", required=True)
    initial.add_argument("--scope-created-by", required=True)
    initial.add_argument("--plan-created-by", required=True)
    initial.add_argument("--request-created-by", required=True)
    initial.add_argument("--scope-created-at-utc", type=datetime.fromisoformat, required=True)
    initial.add_argument("--plan-created-at-utc", type=datetime.fromisoformat, required=True)
    initial.add_argument("--request-created-at-utc", type=datetime.fromisoformat, required=True)

    manifest = sub.add_parser("manifest")
    manifest.add_argument("--control-root", type=Path, required=True)
    manifest.add_argument("--preparation-plan-id", required=True)
    manifest.add_argument("--preparation-plan-sha256", required=True)
    manifest.add_argument("--preparation-request-event-id", required=True)
    manifest.add_argument("--preparation-request-event-sha256", required=True)
    manifest.add_argument("--approved-preparation-literal", required=True)
    manifest.add_argument("--preparation-approved-by", required=True)
    manifest.add_argument(
        "--preparation-approved-at-utc", type=datetime.fromisoformat, required=True
    )
    manifest.add_argument("--manifest-plan-created-by", required=True)
    manifest.add_argument(
        "--manifest-plan-created-at-utc", type=datetime.fromisoformat, required=True
    )
    manifest.add_argument("--manifest-request-created-by", required=True)
    manifest.add_argument(
        "--manifest-request-created-at-utc", type=datetime.fromisoformat, required=True
    )
    manifest.add_argument("--inventory-completion-path", required=True)
    manifest.add_argument("--directional-completion-path", required=True)
    manifest.add_argument("--future-manifest-reader-actor", required=True)
    manifest.add_argument("--future-execution-plan-actor", required=True)
    manifest.add_argument("--future-execution-request-actor", required=True)
    return parser


def main() -> int:
    args = _parser().parse_args()
    if args.command == "initial":
        scope, plan, request, request_value = prepare_exact_group_history_controls(
            repo_root=args.repository_root,
            control_root=args.control_root,
            expected_git_commit=args.git_commit,
            scope_created_by=args.scope_created_by,
            plan_created_by=args.plan_created_by,
            request_created_by=args.request_created_by,
            scope_created_at_utc=args.scope_created_at_utc,
            plan_created_at_utc=args.plan_created_at_utc,
            request_created_at_utc=args.request_created_at_utc,
            contract_id=IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_CONTRACT_ID,
            contract_schema_digest=IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_SCHEMA_DIGEST,
            contract_candidate_sha256=(IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_RESOURCE_SHA256),
        )
        result = {
            "approval_literal": request_value.canonical_approval_literal,
            "plan": asdict(plan),
            "request": asdict(request),
            "scope": asdict(scope),
        }
    else:
        authorization, plan, request, request_value = build_exact_group_history_manifest_controls(
            control_root=args.control_root,
            preparation_plan_id=args.preparation_plan_id,
            preparation_plan_sha256=args.preparation_plan_sha256,
            preparation_request_event_id=args.preparation_request_event_id,
            preparation_request_event_sha256=args.preparation_request_event_sha256,
            approved_preparation_literal=args.approved_preparation_literal,
            preparation_approved_by=args.preparation_approved_by,
            preparation_approved_at_utc=args.preparation_approved_at_utc,
            manifest_plan_created_by=args.manifest_plan_created_by,
            manifest_plan_created_at_utc=args.manifest_plan_created_at_utc,
            manifest_request_created_by=args.manifest_request_created_by,
            manifest_request_created_at_utc=args.manifest_request_created_at_utc,
            inventory_completion_path=args.inventory_completion_path,
            directional_completion_path=args.directional_completion_path,
            future_manifest_reader_actor=args.future_manifest_reader_actor,
            future_execution_plan_actor=args.future_execution_plan_actor,
            future_execution_request_actor=args.future_execution_request_actor,
        )
        result = {
            "approval_literal": request_value.canonical_approval_literal,
            "authorization": asdict(authorization),
            "plan": asdict(plan),
            "request": asdict(request),
        }
    print(json.dumps(result, ensure_ascii=False, separators=(",", ":"), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
