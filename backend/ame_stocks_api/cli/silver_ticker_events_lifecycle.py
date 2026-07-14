"""Run the exact paired S5 ticker-event lifecycle through publication."""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path

from ame_stocks_api.silver.contracts import SilverContractError
from ame_stocks_api.silver.store import SilverStoreError
from ame_stocks_api.silver.ticker_event_lifecycle import complete_ticker_event_lifecycle
from ame_stocks_api.silver.ticker_event_source import TickerEventSourceError
from ame_stocks_api.silver.ticker_event_source_profile import TickerEventSourceProfileError


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ame-silver-ticker-events-lifecycle",
        description=(
            "Verify the complete formal ticker-event scope, then advance the request-status "
            "parent and ticker-change child through full-scope preview, recomputation and "
            "evidence-only publication."
        ),
        epilog=(
            "Formal/pilot receipts, 15,173/11,471/3,702/13,088/12,895/193 counts, "
            "date-quality decisions and exact QA/quarantine exceptions are fixed in code."
        ),
    )
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--git-commit", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    arguments = parser.parse_args(argv)
    try:
        run = complete_ticker_event_lifecycle(
            arguments.data_root,
            repo_root=arguments.repo_root,
            git_commit=arguments.git_commit,
        )
        output = {
            "backtest_identity_eligible": False,
            "coverage_receipt_path": run.coverage_receipt_path,
            "coverage_receipt_sha256": run.coverage_receipt_sha256,
            "inventory_id": run.inventory.inventory_id,
            "inventory_manifest_path": run.inventory_document.path,
            "inventory_manifest_sha256": run.inventory_document.sha256,
            "parent_child_coverage": "exact",
            "profile_sha256": run.profile_sha256,
            "state": "published",
            "tables": {
                item.contract.table: {
                    "contract_id": item.contract.contract_id,
                    "data_files": [
                        str(path.relative_to(arguments.data_root.expanduser().resolve()))
                        for path in item.published.data_paths
                    ],
                    "full_build_id": item.full.build_id,
                    "full_manifest_sha256": item.full_document.sha256,
                    "preview_build_id": item.preview.build_id,
                    "preview_manifest_sha256": item.preview_document.sha256,
                    "qa_checks": [check.to_dict() for check in item.full.qa_checks],
                    "quarantine_issue_rows": item.full.quarantine_issue_rows,
                    "release_id": item.release.release_id,
                    "release_manifest_sha256": item.release_document.sha256,
                    "row_funnel": item.full.row_funnel.to_dict(),
                    "sequence": item.workflow.sequence,
                    "state": item.workflow.state.value,
                    "workflow_event_sha256": item.workflow.event_sha256,
                    "workflow_id": item.workflow.workflow_id,
                }
                for item in (run.request_status, run.ticker_change)
            },
        }
        print(json.dumps(output, indent=2, sort_keys=True))
        return 0
    except (
        OSError,
        subprocess.SubprocessError,
        SilverContractError,
        SilverStoreError,
        TickerEventSourceError,
        TickerEventSourceProfileError,
        TypeError,
        ValueError,
    ) as exc:
        parser.exit(2, f"ame-silver-ticker-events-lifecycle: {exc}\n")


if __name__ == "__main__":
    raise SystemExit(main())
