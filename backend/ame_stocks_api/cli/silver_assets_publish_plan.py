"""Create only the immutable S4 Assets publication-review plan."""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path

from ame_stocks_api.artifacts import ArtifactError
from ame_stocks_api.silver.asset_publish_plan import create_asset_publish_plan
from ame_stocks_api.silver.contracts import SilverContractError
from ame_stocks_api.silver.store import SilverStoreError

_TABLES = (
    "asset_observation_daily",
    "asset_observation_version",
    "universe_source_daily",
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ame-silver-assets-publish-plan",
        description=(
            "Re-verify the three exact S4 full-ready builds and write one immutable "
            "publication-review plan. This command cannot approve warnings, request "
            "publication, publish a table, or create a release."
        ),
    )
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--orchestration-git-commit", required=True)
    for table in _TABLES:
        option = table.replace("_", "-")
        parser.add_argument(f"--{option}-workflow-id", required=True)
        parser.add_argument(f"--{option}-full-ready-event-sha256", required=True)
        parser.add_argument(f"--{option}-build-id", required=True)
        parser.add_argument(f"--{option}-build-manifest-sha256", required=True)
        parser.add_argument(f"--{option}-full-run-plan-id", required=True)
        parser.add_argument(f"--{option}-full-run-plan-sha256", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    arguments = parser.parse_args(argv)
    try:
        run = create_asset_publish_plan(
            arguments.data_root,
            workflow_ids_by_table=_table_map(arguments, "workflow_id"),
            full_ready_event_sha256_by_table=_table_map(
                arguments, "full_ready_event_sha256"
            ),
            build_ids_by_table=_table_map(arguments, "build_id"),
            build_manifest_sha256_by_table=_table_map(
                arguments, "build_manifest_sha256"
            ),
            full_run_plan_ids_by_table=_table_map(arguments, "full_run_plan_id"),
            full_run_plan_sha256_by_table=_table_map(
                arguments, "full_run_plan_sha256"
            ),
            repo_root=arguments.repo_root,
            orchestration_git_commit=arguments.orchestration_git_commit,
        )
        output = {
            "backtest_identity_eligible": run.plan.backtest_identity_eligible,
            "idempotent": run.idempotent,
            "mode": "publish_plan_review_only",
            "plan_id": run.plan.plan_id,
            "plan_manifest_path": run.document.path,
            "plan_manifest_sha256": run.document.sha256,
            "publication_scope": run.plan.publication_scope,
            "requires_release_set": run.plan.requires_release_set,
            "requires_runtime_review_acceptance": (
                run.plan.requires_runtime_review_acceptance
            ),
            "runtime_review": {
                "completed_sessions": run.plan.runtime_review.completed_sessions,
                "evidence_limitation": run.plan.runtime_review.evidence_limitation,
                "expected_rss_ceiling_bytes": (
                    run.plan.runtime_review.expected_rss_ceiling_bytes
                ),
                "hard_rss_limit_bytes": run.plan.runtime_review.hard_rss_limit_bytes,
                "observed_max_rss_bytes": run.plan.runtime_review.observed_max_rss_bytes,
                "qa_warning_counts_by_table": dict(
                    run.plan.runtime_review.qa_warning_counts_by_table
                ),
                "rss_review_status": run.plan.runtime_review.rss_review_status,
                "source_bytes": run.plan.runtime_review.source_bytes,
                "source_path": run.plan.runtime_review.source_path,
                "source_sha256": run.plan.runtime_review.source_sha256,
                "warning_messages": list(run.plan.runtime_review.warning_messages),
            },
            "tables": {
                item.table: {
                    "accepted_quarantine_issue_ids": [],
                    "build_id": item.build_id,
                    "build_manifest_sha256": item.build_manifest_sha256,
                    "date_end": item.date_end,
                    "date_start": item.date_start,
                    "full_ready_event_sha256": item.full_ready_event_sha256,
                    "full_run_plan_id": item.full_run_plan_id,
                    "full_run_plan_sha256": item.full_run_plan_sha256,
                    "input_manifest_count": item.input_manifest_count,
                    "input_page_count": item.input_page_count,
                    "input_rows": item.input_rows,
                    "input_session_count": item.input_session_count,
                    "output_data_bytes": item.output_data_bytes,
                    "output_data_partition_count": item.output_data_partition_count,
                    "output_rows": item.output_rows,
                    "state": "full_ready",
                    "warning_count": len(item.warnings),
                    "warnings": [
                        {
                            "check_id": warning.check.check_id,
                            "bounded_examples_path": warning.check.bounded_examples_path,
                            "denominator": warning.check.denominator,
                            "numerator": warning.check.numerator,
                            "partition_key": warning.check.partition_key,
                            "rate": warning.check.rate,
                            "result_id": warning.result_id,
                            "severity": warning.check.severity.value,
                            "status": warning.check.status.value,
                            "threshold": warning.check.threshold,
                        }
                        for warning in item.warnings
                    ],
                    "workflow_id": item.workflow_id,
                }
                for item in run.plan.tables
            },
            "workflow_mutated": False,
        }
        print(json.dumps(output, indent=2, sort_keys=True))
        return 0
    except (
        ArtifactError,
        OSError,
        SilverContractError,
        SilverStoreError,
        subprocess.SubprocessError,
        TypeError,
        ValueError,
    ) as exc:
        parser.exit(2, f"ame-silver-assets-publish-plan: {exc}\n")


def _table_map(arguments: argparse.Namespace, suffix: str) -> dict[str, str]:
    return {table: getattr(arguments, f"{table}_{suffix}") for table in _TABLES}


if __name__ == "__main__":
    raise SystemExit(main())
