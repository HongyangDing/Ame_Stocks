"""Read-only CLI for inspecting the S0 Silver control plane."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from ame_stocks_api.silver.contracts import SilverContractError, TableContract, thaw_json
from ame_stocks_api.silver.fixed_cases import FIXED_CASES
from ame_stocks_api.silver.reader import PublishedSilverReader
from ame_stocks_api.silver.store import SilverStore, SilverStoreError


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ame-silver",
        description=(
            "Read-only S0 inspection. This command does not read Bronze or run transformations."
        ),
    )
    subparsers = parser.add_subparsers(dest="action", required=True)

    subparsers.add_parser("fixed-cases", help="print the 15 mandatory review cases")

    contract = subparsers.add_parser(
        "validate-contract",
        help="strictly validate one TableContract JSON file without registering it",
    )
    contract.add_argument("--contract", type=Path, required=True)

    status = subparsers.add_parser("status", help="verify and print the latest workflow state")
    status.add_argument("--data-root", type=Path, required=True)
    status.add_argument("--workflow-id", required=True)

    release = subparsers.add_parser(
        "inspect-release",
        help="verify a published release trust chain and print its fixed data files",
    )
    release.add_argument("--data-root", type=Path, required=True)
    release.add_argument("--release-id", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    arguments = parser.parse_args(argv)
    try:
        if arguments.action == "fixed-cases":
            _print_json(
                {
                    "case_count": len(FIXED_CASES),
                    "cases": [case.to_dict() for case in FIXED_CASES],
                    "mode": "metadata_only",
                }
            )
            return 0
        if arguments.action == "validate-contract":
            document = json.loads(arguments.contract.read_text(encoding="utf-8"))
            contract = TableContract.from_dict(document)
            _print_json(
                {
                    "contract_id": contract.contract_id,
                    "domain": contract.domain,
                    "schema_digest": contract.schema_digest,
                    "schema_version": contract.schema_version,
                    "status": "valid",
                    "table": contract.table,
                }
            )
            return 0
        if arguments.action == "status":
            snapshot = SilverStore(arguments.data_root).status(arguments.workflow_id)
            _print_json(
                {
                    "event_path": snapshot.event_path,
                    "event_sha256": snapshot.event_sha256,
                    "evidence": thaw_json(snapshot.evidence),
                    "sequence": snapshot.sequence,
                    "state": snapshot.state.value,
                    "workflow_id": snapshot.workflow_id,
                }
            )
            return 0
        if arguments.action == "inspect-release":
            published = PublishedSilverReader(arguments.data_root).inspect(arguments.release_id)
            _print_json(
                {
                    "build_id": published.build.build_id,
                    "contract_id": published.contract.contract_id,
                    "data_files": [
                        str(path.relative_to(arguments.data_root.expanduser().resolve()))
                        for path in published.data_paths
                    ],
                    "domain": published.release.domain,
                    "release_id": published.release.release_id,
                    "schema_version": published.release.schema_version,
                    "status": "published_and_verified",
                    "table": published.release.table,
                    "workflow_id": published.release.workflow_id,
                }
            )
            return 0
    except (
        json.JSONDecodeError,
        OSError,
        SilverContractError,
        SilverStoreError,
        TypeError,
        ValueError,
    ) as exc:
        parser.exit(2, f"ame-silver: {exc}\n")
    raise AssertionError("argparse accepted an unknown Silver action")


def _print_json(document: dict[str, object]) -> None:
    print(json.dumps(document, indent=2, sort_keys=True))


if __name__ == "__main__":
    raise SystemExit(main())
