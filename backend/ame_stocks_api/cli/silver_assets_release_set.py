"""Complete only the exact user-approved S4 Assets release set."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from ame_stocks_api.artifacts import ArtifactError
from ame_stocks_api.silver.asset_release_set import release_asset_publish_plan
from ame_stocks_api.silver.contracts import SilverContractError
from ame_stocks_api.silver.store import SilverStoreError


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ame-silver-assets-release-set",
        description=(
            "Consume one exact approved S4 PublishPlan, publish its three members behind "
            "a hidden two-phase intent, and expose them only through the final immutable "
            "release-set marker. This command cannot start S5 or make S4 identity evidence "
            "backtest eligible."
        ),
    )
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--release-orchestration-git-commit", required=True)
    parser.add_argument("--expected-publish-plan-id", required=True)
    parser.add_argument("--expected-publish-plan-sha256", required=True)
    parser.add_argument(
        "--recorded-at",
        required=True,
        help="UTC audit timestamp; retries must reuse the exact same value",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    arguments = parser.parse_args(argv)
    try:
        run = release_asset_publish_plan(
            arguments.data_root,
            expected_publish_plan_id=arguments.expected_publish_plan_id,
            expected_publish_plan_sha256=arguments.expected_publish_plan_sha256,
            repo_root=arguments.repo_root,
            release_orchestration_git_commit=(
                arguments.release_orchestration_git_commit
            ),
            recorded_at=arguments.recorded_at,
        )
        output = {
            "accepted_quarantine_issue_ids_by_table": {
                table: list(items)
                for table, items in run.approval.accepted_quarantine_issue_ids_by_table.items()
            },
            "approval_text_sha256": run.approval.approval_text_sha256,
            "backtest_identity_eligible": False,
            "group_approval_id": run.approval.approval_id,
            "group_approval_path": run.approval_document.path,
            "group_approval_sha256": run.approval_document.sha256,
            "idempotent": run.idempotent,
            "intent_id": run.intent.intent_id,
            "intent_path": run.intent_document.path,
            "intent_sha256": run.intent_document.sha256,
            "mode": "s4_assets_release_set_completion",
            "publication_scope": run.release_set.publication_scope,
            "publish_plan_bytes": run.release_set.publish_plan_bytes,
            "publish_plan_creator_commit": (
                run.release_set.publish_plan_creator_commit
            ),
            "publish_plan_id": run.release_set.publish_plan_id,
            "publish_plan_path": run.release_set.publish_plan_path,
            "publish_plan_sha256": run.release_set.publish_plan_sha256,
            "materialization_git_commit": (
                run.release_set.materialization_git_commit
            ),
            "release_orchestration_git_commit": (
                run.release_set.release_orchestration_git_commit
            ),
            "release_set_id": run.release_set.release_set_id,
            "release_set_path": run.document.path,
            "release_set_sha256": run.document.sha256,
            "runtime_review_accepted": run.release_set.runtime_review_accepted,
            "runtime_review": {
                **run.publish_plan.runtime_review.to_dict(),
                "accepted": run.release_set.runtime_review_accepted,
                "digest": run.release_set.runtime_review_digest,
            },
            "tables": {
                member.table: {
                    "accepted_quarantine_issue_ids": list(
                        member.accepted_quarantine_issue_ids
                    ),
                    "approval_id": member.approval_id,
                    "approval_path": member.approval_path,
                    "approval_sha256": member.approval_sha256,
                    "awaiting_publish_event_sha256": (
                        member.awaiting_publish_event_sha256
                    ),
                    "build_id": member.build_id,
                    "build_manifest_sha256": member.build_manifest_sha256,
                    "full_ready_event_sha256": member.full_ready_event_sha256,
                    "full_run_plan_id": member.full_run_plan_id,
                    "full_run_plan_sha256": member.full_run_plan_sha256,
                    "published_event_sha256": member.published_event_sha256,
                    "release_id": member.release_id,
                    "release_path": member.release_path,
                    "release_sha256": member.release_sha256,
                    "sequence": run.workflows_by_table[member.table].sequence,
                    "state": run.workflows_by_table[member.table].state.value,
                    "warning_count": len(member.warning_result_ids),
                    "warning_result_ids": list(member.warning_result_ids),
                    "workflow_id": member.workflow_id,
                }
                for member in run.release_set.members
            },
        }
        print(json.dumps(output, indent=2, sort_keys=True))
        return 0
    except (
        ArtifactError,
        OSError,
        SilverContractError,
        SilverStoreError,
        TypeError,
        ValueError,
    ) as exc:
        parser.exit(2, f"ame-silver-assets-release-set: {exc}\n")


if __name__ == "__main__":
    raise SystemExit(main())
