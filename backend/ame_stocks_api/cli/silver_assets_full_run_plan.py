"""Create only the exact S4 Assets full-run plans for independent review."""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path

from ame_stocks_api.artifacts import ArtifactError
from ame_stocks_api.silver.asset_full_run_plan import create_asset_full_run_plans
from ame_stocks_api.silver.contracts import SilverContractError, thaw_json
from ame_stocks_api.silver.store import SilverStoreError

_TABLES = (
    "asset_observation_daily",
    "asset_observation_version",
    "universe_source_daily",
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ame-silver-assets-full-run-plan",
        description=(
            "Verify the complete versioned S4 Assets source profile and Bronze scope, "
            "record three immutable plans, and stop at full_run_plan_review. This command "
            "cannot approve, transform, or publish data."
        ),
    )
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument(
        "--repo-root",
        type=Path,
        required=True,
        help="clean Git checkout whose HEAD must equal --git-commit",
    )
    for table in _TABLES:
        option = table.replace("_", "-")
        parser.add_argument(f"--{option}-workflow-id", required=True)
        parser.add_argument(f"--{option}-expected-event-sha256", required=True)
    parser.add_argument("--source-profile-path", required=True)
    parser.add_argument("--expected-source-profile-sha256", required=True)
    parser.add_argument("--expected-manifest-inventory-sha256", required=True)
    parser.add_argument("--expected-artifact-inventory-sha256", required=True)
    parser.add_argument("--expected-input-rows", type=int, required=True)
    parser.add_argument("--git-commit", required=True)
    parser.add_argument("--recorded-at", required=True)
    parser.add_argument("--actor", default="s4-assets-full-run-plan-author")
    parser.add_argument("--note", default="")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    arguments = parser.parse_args(argv)
    try:
        workflow_ids = {
            table: getattr(arguments, f"{table}_workflow_id") for table in _TABLES
        }
        expected_events = {
            table: getattr(arguments, f"{table}_expected_event_sha256")
            for table in _TABLES
        }
        run = create_asset_full_run_plans(
            arguments.data_root,
            repo_root=arguments.repo_root,
            workflow_ids=workflow_ids,
            expected_event_sha256_by_table=expected_events,
            source_profile_path=arguments.source_profile_path,
            expected_source_profile_sha256=arguments.expected_source_profile_sha256,
            expected_manifest_inventory_sha256=(
                arguments.expected_manifest_inventory_sha256
            ),
            expected_artifact_inventory_sha256=(
                arguments.expected_artifact_inventory_sha256
            ),
            expected_input_rows=arguments.expected_input_rows,
            git_commit=arguments.git_commit,
            recorded_at=arguments.recorded_at,
            actor=arguments.actor,
            note=arguments.note,
        )
        output = {
            "inventory": {
                "inventory_id": run.inventory.inventory_id,
                "manifest_path": run.inventory_document.path,
                "manifest_sha256": run.inventory_document.sha256,
            },
            "mode": "full_run_plan_review_only",
            "preflight": {
                "disk_status": run.preflight.disk_status,
                "manifest_path": run.preflight.document.path,
                "manifest_sha256": run.preflight.document.sha256,
                "observed_free_bytes": run.preflight.observed_free_bytes,
                "observed_project_bytes": run.preflight.observed_project_bytes,
                "preflight_id": run.preflight.preflight_id,
                "resource_policy": thaw_json(
                    run.preflight.logical_scope["resource_policy"]
                ),
            },
            "scope": run.scope.to_dict(),
            "source_profile": {
                "path": run.source_profile_path,
                "sha256": run.source_profile_sha256,
            },
            "tables": {
                item.plan.table: {
                    "expected_output_rows": item.plan.parameters["expected_output_rows"],
                    "full_run_plan_id": item.plan.plan_id,
                    "full_run_plan_path": item.plan_document.path,
                    "full_run_plan_sha256": item.plan_document.sha256,
                    "required_accepted_quarantine_issue_ids": list(
                        item.required_accepted_quarantine_issue_ids
                    ),
                    "required_waived_qa_result_ids": list(
                        item.required_waived_qa_result_ids
                    ),
                    "resource_summary": {
                        "estimated_data_bytes_by_table": thaw_json(
                            item.plan.resource_projection[
                                "estimated_data_bytes_by_table"
                            ]
                        ),
                        "estimated_data_bytes_total_point": (
                            item.plan.resource_projection[
                                "estimated_data_bytes_total_point"
                            ]
                        ),
                        "expected_rss_ceiling_bytes": item.plan.resource_projection[
                            "expected_rss_ceiling_bytes"
                        ],
                        "max_in_flight_sessions": item.plan.resource_projection[
                            "max_in_flight_sessions"
                        ],
                        "runtime_estimate_seconds": item.plan.resource_projection[
                            "runtime_estimate_seconds"
                        ],
                        "workers": item.plan.resource_projection["workers"],
                    },
                    "sequence": item.workflow.sequence,
                    "state": item.workflow.state.value,
                    "workflow_event_path": item.workflow.event_path,
                    "workflow_event_sha256": item.workflow.event_sha256,
                    "workflow_id": item.workflow.workflow_id,
                }
                for item in run.table_runs
            },
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
        parser.exit(2, f"ame-silver-assets-full-run-plan: {exc}\n")


if __name__ == "__main__":
    raise SystemExit(main())
