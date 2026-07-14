"""Run the exact S6 Ticker Overview lifecycle through evidence-only publication."""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path

from ame_stocks_api.silver.contracts import SilverContractError
from ame_stocks_api.silver.store import SilverStoreError
from ame_stocks_api.silver.ticker_overview_lifecycle import (
    complete_ticker_overview_lifecycle,
)
from ame_stocks_api.silver.ticker_overview_source import TickerOverviewSourceError
from ame_stocks_api.silver.ticker_overview_source_profile import (
    TickerOverviewSourceProfileError,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ame-silver-ticker-overview-lifecycle",
        description=(
            "Verify the exact 30,739-row Ticker Overview lifecycle scope, then advance "
            "ticker_overview_safe through full-scope preview, independent recomputation and "
            "evidence-only publication."
        ),
        epilog=(
            "The 30,739 -> 30,570 + 169 row funnel, warning profile, High quarantine "
            "acceptance and S7 hard stop are fixed in code."
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
        run = complete_ticker_overview_lifecycle(
            arguments.data_root,
            repo_root=arguments.repo_root,
            git_commit=arguments.git_commit,
        )
        output = {
            "backtest_identity_eligible": False,
            "contract_id": run.contract.contract_id,
            "coverage_receipt_path": run.coverage_receipt_path,
            "coverage_receipt_sha256": run.coverage_receipt_sha256,
            "data_files": [
                str(path.relative_to(arguments.data_root.expanduser().resolve()))
                for path in run.published.data_paths
            ],
            "full_build_id": run.full.build_id,
            "full_manifest_sha256": run.full_document.sha256,
            "inventories": {
                "lifecycle_control": {
                    "inventory_id": run.lifecycle_inventory.inventory_id,
                    "manifest_path": run.lifecycle_inventory_document.path,
                    "manifest_sha256": run.lifecycle_inventory_document.sha256,
                    "source_layer": run.lifecycle_inventory.source_layer.value,
                },
                "ticker_overview_bronze": {
                    "inventory_id": run.overview_inventory.inventory_id,
                    "manifest_path": run.overview_inventory_document.path,
                    "manifest_sha256": run.overview_inventory_document.sha256,
                    "source_layer": run.overview_inventory.source_layer.value,
                },
            },
            "preview_build_id": run.preview.build_id,
            "preview_manifest_sha256": run.preview_document.sha256,
            "profile_sha256": run.profile_sha256,
            "qa_checks": [check.to_dict() for check in run.full.qa_checks],
            "quarantine_issue_rows": run.full.quarantine_issue_rows,
            "release_id": run.release.release_id,
            "release_manifest_sha256": run.release_document.sha256,
            "row_funnel": run.full.row_funnel.to_dict(),
            "s7_started": False,
            "sequence": run.workflow.sequence,
            "state": run.workflow.state.value,
            "workflow_event_sha256": run.workflow.event_sha256,
            "workflow_id": run.workflow.workflow_id,
        }
        print(json.dumps(output, indent=2, sort_keys=True))
        return 0
    except (
        OSError,
        subprocess.SubprocessError,
        SilverContractError,
        SilverStoreError,
        TickerOverviewSourceError,
        TickerOverviewSourceProfileError,
        TypeError,
        ValueError,
    ) as exc:
        parser.exit(2, f"ame-silver-ticker-overview-lifecycle: {exc}\n")


if __name__ == "__main__":
    raise SystemExit(main())
