"""Run only the exact approved S4 Assets full-history materialization."""

from __future__ import annotations

import argparse
import json
import subprocess
from collections import Counter
from pathlib import Path

from ame_stocks_api.artifacts import ArtifactError
from ame_stocks_api.silver.asset_full import run_asset_full
from ame_stocks_api.silver.contracts import ArtifactRole, SilverContractError
from ame_stocks_api.silver.store import SilverStoreError

_TABLES = (
    "asset_observation_daily",
    "asset_observation_version",
    "universe_source_daily",
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ame-silver-assets-full",
        description=(
            "Materialize the three exact approved S4 Assets full-run plans, then stop "
            "all workflows at full_ready. This command cannot approve a plan or publish."
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
        parser.add_argument(f"--{option}-approved-event-sha256", required=True)
        parser.add_argument(f"--{option}-approved-plan-id", required=True)
        parser.add_argument(f"--{option}-approved-plan-sha256", required=True)
    parser.add_argument("--git-commit", required=True)
    parser.add_argument("--workers", type=int, choices=(1,), default=1)
    parser.add_argument("--max-in-flight-sessions", type=int, choices=(1,), default=1)
    parser.add_argument("--calendar-name", choices=("XNYS",), default="XNYS")
    parser.add_argument("--actor", default="s4-assets-full-runner")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    arguments = parser.parse_args(argv)
    try:
        workflow_ids = {table: getattr(arguments, f"{table}_workflow_id") for table in _TABLES}
        approved_events = {
            table: getattr(arguments, f"{table}_approved_event_sha256") for table in _TABLES
        }
        approved_plan_ids = {
            table: getattr(arguments, f"{table}_approved_plan_id") for table in _TABLES
        }
        approved_plan_shas = {
            table: getattr(arguments, f"{table}_approved_plan_sha256") for table in _TABLES
        }
        run = run_asset_full(
            arguments.data_root,
            workflow_ids=workflow_ids,
            approved_event_sha256_by_table=approved_events,
            approved_plan_id_by_table=approved_plan_ids,
            approved_plan_sha256_by_table=approved_plan_shas,
            git_commit=arguments.git_commit,
            repo_root=arguments.repo_root,
            workers=arguments.workers,
            max_in_flight_sessions=arguments.max_in_flight_sessions,
            actor=arguments.actor,
            calendar_name=arguments.calendar_name,
        )
        output = {
            "completed_sessions": run.completed_sessions,
            "idempotent": run.idempotent,
            "mode": "full_ready_only",
            "tables": {
                table: _table_summary(item) for table, item in sorted(run.table_runs.items())
            },
            "warnings": list(run.warnings),
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
        parser.exit(2, f"ame-silver-assets-full: {exc}\n")


def _table_summary(item: object) -> dict[str, object]:
    build = item.build
    plan = item.plan
    workflow = item.workflow
    data_outputs = tuple(
        artifact for artifact in build.outputs if artifact.role is ArtifactRole.DATA
    )
    qa_status_counts = Counter(check.status.value for check in build.qa_checks)
    parameters = plan.parameters
    return {
        "build_id": build.build_id,
        "build_manifest_path": item.build_document.path,
        "build_manifest_sha256": item.build_document.sha256,
        "date_end": parameters["date_end"],
        "date_start": parameters["date_start"],
        "full_run_plan_id": plan.plan_id,
        "input_compressed_bytes": parameters["input_compressed_bytes"],
        "input_manifest_count": parameters["input_manifest_count"],
        "input_page_count": parameters["input_page_count"],
        "input_raw_bytes": parameters["input_raw_bytes"],
        "input_rows": parameters["expected_input_rows"],
        "input_session_count": parameters["input_session_count"],
        "output_artifact_count": len(build.outputs),
        "output_bytes": sum(artifact.bytes for artifact in build.outputs),
        "output_data_bytes": sum(artifact.bytes for artifact in data_outputs),
        "output_data_partition_count": len(data_outputs),
        "output_rows": sum(artifact.row_count for artifact in data_outputs),
        "qa_status_counts": dict(sorted(qa_status_counts.items())),
        "row_funnel": build.row_funnel.to_dict(),
        "sequence": workflow.sequence,
        "state": workflow.state.value,
        "workflow_event_path": workflow.event_path,
        "workflow_event_sha256": workflow.event_sha256,
        "workflow_id": workflow.workflow_id,
    }


if __name__ == "__main__":
    raise SystemExit(main())
