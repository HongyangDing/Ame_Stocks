"""CLI for authoritative-plan REST Bronze semantic QA."""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

from ame_stocks_api.artifacts import write_json_atomic
from ame_stocks_api.audit.rest_semantics import (
    AUDITED_DATASETS,
    RestSemanticAuditError,
    RestSemanticAuditor,
)
from ame_stocks_core import ProviderDataset


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ame-audit-rest-semantics",
        description=(
            "Check candidate-key uniqueness, taxonomy decoding, and EDGAR accession "
            "coverage using authoritative Massive REST Bronze requests only."
        ),
    )
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    parser.add_argument(
        "--dataset",
        action="append",
        choices=sorted(dataset.value for dataset in AUDITED_DATASETS),
        help="optional repeatable subset; defaults to the complete scoped semantic catalog",
    )
    parser.add_argument("--max-examples", type=int, default=20)
    parser.add_argument("--temp-dir", type=Path)
    parser.add_argument(
        "--output",
        type=Path,
        help="optional JSON report path; relative paths are resolved under data-root",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    arguments = parser.parse_args(argv)
    datasets = (
        tuple(ProviderDataset(value) for value in arguments.dataset)
        if arguments.dataset
        else None
    )
    try:
        report = RestSemanticAuditor(
            arguments.data_root,
            start=arguments.start,
            end=arguments.end,
            datasets=datasets,
            max_examples=arguments.max_examples,
            temp_dir=arguments.temp_dir,
        ).run()
        if arguments.output:
            output = arguments.output
            if not output.is_absolute():
                output = arguments.data_root / output
            output = output.resolve()
            report["report_path"] = str(output)
            write_json_atomic(output, report)
        print(json.dumps(report, indent=2, sort_keys=True))
        return 1 if report["status"] == "failed" else 0
    except (RestSemanticAuditError, OSError, ValueError) as exc:
        parser.exit(2, f"ame-audit-rest-semantics: {exc}\n")


if __name__ == "__main__":
    raise SystemExit(main())
