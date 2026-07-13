"""Approve only three explicitly identified S4 Assets full-run plans."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from ame_stocks_api.artifacts import ArtifactError
from ame_stocks_api.silver.asset_full_run_plan import approve_asset_full_run_plans
from ame_stocks_api.silver.contracts import SilverContractError
from ame_stocks_api.silver.store import SilverStoreError

_TABLES = (
    "asset_observation_daily",
    "asset_observation_version",
    "universe_source_daily",
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ame-silver-assets-full-run-plan-approval",
        description=(
            "Approve three exact S4 Assets plan ID / manifest SHA / sequence-6 event "
            "triples and stop at approved_full_run. This command cannot transform or "
            "publish data."
        ),
    )
    parser.add_argument("--data-root", type=Path, required=True)
    for table in _TABLES:
        option = table.replace("_", "-")
        parser.add_argument(f"--{option}-workflow-id", required=True)
        parser.add_argument(f"--{option}-expected-plan-id", required=True)
        parser.add_argument(f"--{option}-expected-plan-sha256", required=True)
        parser.add_argument(
            f"--{option}-expected-plan-event-sha256",
            required=True,
            help="exact full_run_plan_review (sequence 6) event SHA-256",
        )
        parser.add_argument(
            f"--{option}-waived-qa-result-id",
            action="append",
            default=None,
        )
        parser.add_argument(
            f"--{option}-accepted-quarantine-issue-id",
            action="append",
            default=None,
        )
    parser.add_argument("--approver", required=True)
    parser.add_argument("--decided-at", required=True)
    parser.add_argument("--note", default="")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    arguments = parser.parse_args(argv)
    try:
        workflow_ids = {
            table: getattr(arguments, f"{table}_workflow_id") for table in _TABLES
        }
        plan_ids = {
            table: getattr(arguments, f"{table}_expected_plan_id")
            for table in _TABLES
        }
        plan_shas = {
            table: getattr(arguments, f"{table}_expected_plan_sha256")
            for table in _TABLES
        }
        plan_events = {
            table: getattr(arguments, f"{table}_expected_plan_event_sha256")
            for table in _TABLES
        }
        waived = {
            table: tuple(getattr(arguments, f"{table}_waived_qa_result_id") or ())
            for table in _TABLES
        }
        accepted = {
            table: tuple(
                getattr(arguments, f"{table}_accepted_quarantine_issue_id") or ()
            )
            for table in _TABLES
        }
        run = approve_asset_full_run_plans(
            arguments.data_root,
            workflow_ids=workflow_ids,
            expected_plan_ids_by_table=plan_ids,
            expected_plan_sha256_by_table=plan_shas,
            expected_plan_event_sha256_by_table=plan_events,
            waived_qa_result_ids_by_table=waived,
            accepted_quarantine_issue_ids_by_table=accepted,
            approver=arguments.approver,
            decided_at=arguments.decided_at,
            note=arguments.note,
        )
        output = {
            "mode": "full_run_plan_approval_only",
            "tables": {
                table: {
                    "approved_full_run_plan_id": run.approved_plan_ids_by_table[table],
                    "sequence": run.workflows_by_table[table].sequence,
                    "state": run.workflows_by_table[table].state.value,
                    "workflow_event_sha256": (
                        run.workflows_by_table[table].event_sha256
                    ),
                    "workflow_id": run.workflows_by_table[table].workflow_id,
                }
                for table in _TABLES
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
        parser.exit(2, f"ame-silver-assets-full-run-plan-approval: {exc}\n")


if __name__ == "__main__":
    raise SystemExit(main())
