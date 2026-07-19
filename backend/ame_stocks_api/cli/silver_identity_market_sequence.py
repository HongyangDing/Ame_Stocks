"""Run the exact review-only S7 Gate-C full-sequence scan."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from ame_stocks_api.silver.identity_market_sequence import (
    IdentityMarketSequenceError,
    authorize_market_sequence_plan_under_standing_grant,
    prepare_market_sequence_plan,
    run_source_bound_market_sequence,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ame-silver-identity-market-sequence")
    commands = parser.add_subparsers(dest="command", required=True)
    prepare = commands.add_parser("prepare")
    prepare.add_argument("--data-root", type=Path, required=True)
    prepare.add_argument("--classification-candidate-path", required=True)
    prepare.add_argument("--classification-candidate-id", required=True)
    prepare.add_argument("--classification-candidate-sha256", required=True)
    prepare.add_argument("--classification-data-path", required=True)
    prepare.add_argument("--classification-data-sha256", required=True)
    prepare.add_argument("--classification-data-bytes", type=int, required=True)
    prepare.add_argument("--classification-data-row-count", type=int, required=True)
    prepare.add_argument("--classification-source-available-session", required=True)
    prepare.add_argument("--prepared-by", required=True)

    authorize = commands.add_parser("authorize-standing")
    authorize.add_argument("--data-root", type=Path, required=True)
    authorize.add_argument("--plan-path", required=True)
    authorize.add_argument("--plan-id", required=True)
    authorize.add_argument("--plan-sha256", required=True)
    authorize.add_argument("--request-path", required=True)
    authorize.add_argument("--request-id", required=True)
    authorize.add_argument("--request-sha256", required=True)
    authorize.add_argument("--recorded-by", required=True)

    run = commands.add_parser("run")
    run.add_argument("--data-root", type=Path, required=True)
    run.add_argument("--plan-path", required=True)
    run.add_argument("--plan-id", required=True)
    run.add_argument("--plan-sha256", required=True)
    run.add_argument("--authorization-path", required=True)
    run.add_argument("--authorization-id", required=True)
    run.add_argument("--authorization-sha256", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    arguments = parser.parse_args(argv)
    try:
        if arguments.command == "prepare":
            plan = prepare_market_sequence_plan(
                arguments.data_root,
                classification_candidate_path=arguments.classification_candidate_path,
                classification_candidate_id=arguments.classification_candidate_id,
                classification_candidate_sha256=arguments.classification_candidate_sha256,
                classification_data_path=arguments.classification_data_path,
                classification_data_sha256=arguments.classification_data_sha256,
                classification_data_bytes=arguments.classification_data_bytes,
                classification_data_row_count=arguments.classification_data_row_count,
                classification_source_available_session=(
                    arguments.classification_source_available_session
                ),
                prepared_by=arguments.prepared_by,
            )
            output = {
                "idempotent": plan.idempotent,
                "intent_available_session": plan.intent_available_session,
                "intent_captured_at_utc": plan.intent_captured_at_utc,
                "mode": "durable_authorization_plan_without_parquet_read",
                "plan_id": plan.plan_id,
                "plan_path": plan.plan_path,
                "plan_sha256": plan.plan_sha256,
                "request_id": plan.request_id,
                "request_path": plan.request_path,
                "request_sha256": plan.request_sha256,
                "state": plan.state,
            }
        elif arguments.command == "authorize-standing":
            authorization = authorize_market_sequence_plan_under_standing_grant(
                arguments.data_root,
                plan_path=arguments.plan_path,
                plan_id=arguments.plan_id,
                plan_sha256=arguments.plan_sha256,
                request_path=arguments.request_path,
                request_id=arguments.request_id,
                request_sha256=arguments.request_sha256,
                recorded_by=arguments.recorded_by,
            )
            output = {
                "authorization_id": authorization.authorization_id,
                "authorization_path": authorization.authorization_path,
                "authorization_sha256": authorization.authorization_sha256,
                "idempotent": authorization.idempotent,
                "mode": "plan_bound_current_standing_authorization_receipt",
                "plan_id": authorization.plan_id,
                "recorded_at_utc": authorization.recorded_at_utc,
                "request_id": authorization.request_id,
                "state": authorization.state,
            }
        else:
            result = run_source_bound_market_sequence(
                arguments.data_root,
                plan_path=arguments.plan_path,
                plan_id=arguments.plan_id,
                plan_sha256=arguments.plan_sha256,
                authorization_path=arguments.authorization_path,
                authorization_id=arguments.authorization_id,
                authorization_sha256=arguments.authorization_sha256,
            )
            output = {
                "candidate_id": result.candidate_id,
                "completion_id": result.completion_id,
                "completion_path": result.completion_path,
                "completion_sha256": result.completion_sha256,
                "daily_reason_counts_path": result.daily_reason_counts_path,
                "examples_path": result.examples_path,
                "idempotent": result.idempotent,
                "interval_data_path": result.interval_data_path,
                "interval_row_count": result.interval_row_count,
                "long_standing_foreign_rows": result.long_standing_foreign_rows,
                "manifest_path": result.manifest_path,
                "mode": "source_bound_full_sequence_scan_awaiting_review",
                "qa_path": result.qa_path,
                "reviewed_case_count": result.reviewed_case_count,
                "reviewed_evidence_path": result.reviewed_evidence_path,
                "reviewed_foreign_row_count": result.reviewed_foreign_row_count,
                "source_row_count": result.source_row_count,
                "unresolved_rows": result.unresolved_rows,
                "us_locale_non_us_composite_figi_rows": (
                    result.us_locale_non_us_composite_figi_rows
                ),
            }
    except (IdentityMarketSequenceError, OSError, ValueError) as exc:
        parser.exit(2, f"ame-silver-identity-market-sequence: {exc}\n")
    print(json.dumps(output, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
