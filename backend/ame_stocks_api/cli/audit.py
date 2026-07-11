"""Command line interface for credential-free Bronze verification."""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

from ame_stocks_api.artifacts import write_json_atomic
from ame_stocks_api.audit import BronzeAuditError, BronzeAuditor


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ame-audit-bronze",
        description="Offline integrity, completeness, and cross-dataset audit of Bronze data.",
    )
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    parser.add_argument("--mode", choices=("structural", "full"), default="full")
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument(
        "--output",
        type=Path,
        help="optional JSON report path; relative paths are resolved under data-root",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    arguments = parser.parse_args(argv)
    try:
        auditor = BronzeAuditor(
            arguments.data_root,
            start=arguments.start,
            end=arguments.end,
            mode=arguments.mode,
            workers=arguments.workers,
        )
        report = auditor.run()
        if arguments.output:
            output = arguments.output
            if not output.is_absolute():
                output = arguments.data_root / output
            write_json_atomic(output.resolve(), report)
            report["report_path"] = str(output.resolve())
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0 if report["status"] in {"passed", "passed_with_warnings"} else 1
    except (BronzeAuditError, OSError, ValueError) as exc:
        parser.exit(2, f"ame-audit-bronze: {exc}\n")


if __name__ == "__main__":
    raise SystemExit(main())
