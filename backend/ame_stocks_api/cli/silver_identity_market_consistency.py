"""Production CLI for the approved S7 OpenFIGI market-consistency gate."""

from __future__ import annotations

import argparse
import json
import os
from datetime import UTC, datetime
from pathlib import Path

from ame_stocks_api.silver.identity_market_consistency import (
    IdentityMarketConsistencyError,
    classify_market_consistency_run,
    execute_market_consistency_run,
    materialize_market_classification_candidate,
    prepare_approved_market_consistency_run,
)

OPENFIGI_API_KEY_ENV = "OPENFIGI_API_KEY"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ame-silver-identity-market-consistency")
    subparsers = parser.add_subparsers(dest="command", required=True)

    prepare = subparsers.add_parser("prepare")
    prepare.add_argument("--data-root", type=Path, required=True)
    prepare.add_argument("--authorization-text", required=True)
    prepare.add_argument("--reaffirmation-text", required=True)
    prepare.add_argument("--approved-by", required=True)
    prepare.add_argument("--prepared-by", required=True)

    run = subparsers.add_parser("run")
    run.add_argument("--data-root", type=Path, required=True)
    run.add_argument("--run-id", required=True)
    run.add_argument("--max-batches", type=int, default=None)

    classify = subparsers.add_parser("classify")
    classify.add_argument("--data-root", type=Path, required=True)
    classify.add_argument("--run-id", required=True)
    classify.add_argument("--materialize", action="store_true")
    classify.add_argument("--materialized-by")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    arguments = parser.parse_args(argv)
    try:
        if arguments.command == "prepare":
            result = prepare_approved_market_consistency_run(
                arguments.data_root,
                authorization_text=arguments.authorization_text,
                reaffirmation_text=arguments.reaffirmation_text,
                approved_by=arguments.approved_by,
                prepared_by=arguments.prepared_by,
                authenticated=bool(os.environ.get(OPENFIGI_API_KEY_ENV)),
            )
            output = {
                "batch_count": result.batch_count,
                "composite_count": result.composite_count,
                "mode": "approved_production_preparation_without_network",
                "request_manifest_path": result.request_manifest_path,
                "run_id": result.run_id,
            }
        elif arguments.command == "run":
            # Deliberately fixed: a configurable environment-variable name could cause
            # MASSIVE_API_KEY (or another unrelated secret) to be sent to OpenFIGI.
            result = execute_market_consistency_run(
                arguments.data_root,
                run_id=arguments.run_id,
                api_key=os.environ.get(OPENFIGI_API_KEY_ENV),
                max_batches=arguments.max_batches,
                require_production_approval=True,
            )
            output = {
                "batch_count": result.batch_count,
                "completed_batch_count": result.completed_batch_count,
                "composite_count": result.composite_count,
                "final_manifest_path": result.final_manifest_path,
                "idempotent": result.idempotent,
                "mode": "approved_production_network_capture",
                "run_id": result.run_id,
            }
        elif arguments.materialize:
            if not arguments.materialized_by:
                parser.error("--materialized-by is required with --materialize")
            candidate = materialize_market_classification_candidate(
                arguments.data_root,
                run_id=arguments.run_id,
                materialized_at_utc=datetime.now(UTC).isoformat(),
                materialized_by=arguments.materialized_by,
                require_production_approval=True,
            )
            output = {
                "candidate_id": candidate.candidate_id,
                "composite_count": candidate.composite_count,
                "idempotent": candidate.idempotent,
                "manifest_path": candidate.manifest_path,
                "mode": "approved_production_offline_classification_candidate",
                "non_us_composite_count": candidate.non_us_composite_count,
                "non_us_provider_row_count": candidate.non_us_provider_row_count,
                "run_id": arguments.run_id,
                "unresolved_composite_count": candidate.unresolved_composite_count,
                "unresolved_provider_row_count": candidate.unresolved_provider_row_count,
                "us_composite_count": candidate.us_composite_count,
            }
        else:
            rows = classify_market_consistency_run(
                arguments.data_root,
                run_id=arguments.run_id,
                require_production_approval=True,
            )
            counts: dict[str, int] = {}
            for row in rows:
                counts[row.classification] = counts.get(row.classification, 0) + 1
            output = {
                "classification_counts": dict(sorted(counts.items())),
                "composite_count": len(rows),
                "mode": "approved_production_offline_classification",
                "run_id": arguments.run_id,
            }
    except (IdentityMarketConsistencyError, OSError, ValueError) as exc:
        parser.exit(2, f"ame-silver-identity-market-consistency: {exc}\n")
    print(json.dumps(output, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
