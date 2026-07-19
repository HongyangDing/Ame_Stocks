"""Low-level fixture/internal S7 registry CLI; production uses the fixed ingress CLI."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from ame_stocks_api.silver.identity_registry_workflow import (
    REGISTRY_ORDER,
    RegistryCandidateManifest,
    RegistryReleasePin,
    StoredControlDocument,
    create_approval_request,
    create_decision_plan,
    current_registry_contract_pin,
    load_approval_receipt_control,
    load_approval_request_control,
    load_candidate_control,
    load_decision_plan_control,
    load_registry_release,
    load_registry_release_set,
    publish_release,
    publish_release_under_standing_authority,
    record_exact_approval,
    record_standing_candidate_authorization,
    require_fixture_registry_root,
    store_approval_request,
    store_candidate,
    store_decision_plan,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("show-contract-pins")

    store_candidate_parser = subparsers.add_parser(
        "store-candidate",
        help="fixture/internal only; never use caller-authored candidate JSON in production",
    )
    _root(store_candidate_parser)
    store_candidate_parser.add_argument("--candidate-json", type=Path, required=True)

    store_plan_parser = subparsers.add_parser("store-plan")
    _root(store_plan_parser)
    store_plan_parser.add_argument("--candidate-ref-json", type=Path, required=True)

    store_request_parser = subparsers.add_parser("store-request")
    _root(store_request_parser)
    store_request_parser.add_argument("--plan-ref-json", type=Path, required=True)

    show_literal_parser = subparsers.add_parser("show-request-literal")
    _root(show_literal_parser)
    show_literal_parser.add_argument("--request-ref-json", type=Path, required=True)

    approve_parser = subparsers.add_parser("record-approval")
    _root(approve_parser)
    approve_parser.add_argument("--request-ref-json", type=Path, required=True)
    approve_parser.add_argument("--literal-json", type=Path, required=True)
    approve_parser.add_argument("--approved-by", required=True)
    approve_parser.add_argument("--approved-at-utc", required=True)
    approve_parser.add_argument(
        "--approval-available-session", type=date.fromisoformat, required=True
    )

    release_parser = subparsers.add_parser(
        "publish-release",
        help="fixture/internal only; production publication rebuilds frozen candidate rows",
    )
    _root(release_parser)
    release_parser.add_argument("--plan-ref-json", type=Path, required=True)
    release_parser.add_argument("--request-ref-json", type=Path, required=True)
    release_parser.add_argument("--receipt-ref-json", type=Path, required=True)
    release_parser.add_argument("--rows-parquet", type=Path, required=True)
    release_parser.add_argument("--published-at-utc", required=True)
    release_parser.add_argument(
        "--release-available-session", type=date.fromisoformat, required=True
    )

    standing_release_parser = subparsers.add_parser("publish-standing-release")
    _root(standing_release_parser)
    standing_release_parser.add_argument("--request-ref-json", type=Path, required=True)
    standing_release_parser.add_argument(
        "--standing-authorization-literal-file", type=Path, required=True
    )
    standing_release_parser.add_argument("--reaffirmation-literal-file", type=Path, required=True)
    standing_release_parser.add_argument("--approved-by", required=True)

    standing_authorization_parser = subparsers.add_parser("record-standing-candidate-authorization")
    _root(standing_authorization_parser)
    standing_authorization_parser.add_argument("--authorization-type", required=True)
    standing_authorization_parser.add_argument("--registry-name", required=True)
    standing_authorization_parser.add_argument("--target-refs-json", type=Path, required=True)
    standing_authorization_parser.add_argument("--availability-calendar-id", required=True)
    standing_authorization_parser.add_argument("--availability-calendar-sha256", required=True)
    standing_authorization_parser.add_argument(
        "--standing-authorization-literal-file", type=Path, required=True
    )
    standing_authorization_parser.add_argument(
        "--reaffirmation-literal-file", type=Path, required=True
    )
    standing_authorization_parser.add_argument("--approved-by", required=True)

    verify_parser = subparsers.add_parser("verify-release")
    _root(verify_parser)
    verify_parser.add_argument("--release-pin-json", type=Path, required=True)

    verify_set_parser = subparsers.add_parser("verify-release-set")
    _root(verify_set_parser)
    verify_set_parser.add_argument("--release-pins-json", type=Path, required=True)
    return parser


def _root(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--data-root", type=Path, required=True)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "show-contract-pins":
        _print(
            {
                "note": "byte pins only; this command does not assert schema approval",
                "registry_contract_pins": [
                    current_registry_contract_pin(name).to_dict() for name in REGISTRY_ORDER
                ],
            }
        )
        return 0

    data_root = args.data_root.expanduser().resolve()
    require_fixture_registry_root(data_root)
    if args.command == "store-candidate":
        candidate = RegistryCandidateManifest.from_dict(_load(args.candidate_json))
        _print(store_candidate(data_root, candidate).to_dict())
        return 0
    if args.command == "store-plan":
        candidate_ref = StoredControlDocument.from_dict(_load(args.candidate_ref_json))
        candidate = load_candidate_control(data_root, candidate_ref)
        plan = create_decision_plan(candidate, candidate_ref)
        _print(store_decision_plan(data_root, plan).to_dict())
        return 0
    if args.command == "store-request":
        plan_ref = StoredControlDocument.from_dict(_load(args.plan_ref_json))
        plan = load_decision_plan_control(data_root, plan_ref)
        request = create_approval_request(plan, plan_ref)
        request_ref = store_approval_request(data_root, request)
        _print({"request": request_ref.to_dict(), "literal": request.literal_payload()})
        return 0
    if args.command == "show-request-literal":
        request_ref = StoredControlDocument.from_dict(_load(args.request_ref_json))
        request = load_approval_request_control(data_root, request_ref)
        _print(request.literal_payload())
        return 0
    if args.command == "record-approval":
        request_ref = StoredControlDocument.from_dict(_load(args.request_ref_json))
        request = load_approval_request_control(data_root, request_ref)
        receipt, receipt_ref = record_exact_approval(
            data_root,
            request=request,
            request_document=request_ref,
            literal=_object(_load(args.literal_json), "approval literal"),
            approved_by=args.approved_by,
            approved_at_utc=_parse_utc(args.approved_at_utc),
            approval_available_session=args.approval_available_session,
        )
        _print({"receipt": receipt_ref.to_dict(), "receipt_id": receipt.receipt_id})
        return 0
    if args.command == "publish-release":
        plan_ref = StoredControlDocument.from_dict(_load(args.plan_ref_json))
        request_ref = StoredControlDocument.from_dict(_load(args.request_ref_json))
        receipt_ref = StoredControlDocument.from_dict(_load(args.receipt_ref_json))
        plan = load_decision_plan_control(data_root, plan_ref)
        request = load_approval_request_control(data_root, request_ref)
        receipt = load_approval_receipt_control(data_root, receipt_ref)
        table = pq.read_table(args.rows_parquet)
        pin = publish_release(
            data_root,
            plan=plan,
            plan_document=plan_ref,
            request=request,
            request_document=request_ref,
            approval_receipt=receipt,
            approval_receipt_document=receipt_ref,
            decision_rows=table.to_pylist(),
            published_at_utc=_parse_utc(args.published_at_utc),
            release_available_session=args.release_available_session,
        )
        _print(pin.to_dict())
        return 0
    if args.command == "publish-standing-release":
        request_ref = StoredControlDocument.from_dict(_load(args.request_ref_json))
        receipt, receipt_ref, pin = publish_release_under_standing_authority(
            data_root,
            request_document=request_ref,
            standing_authorization_literal=args.standing_authorization_literal_file.read_bytes(),
            reaffirmation_literal=args.reaffirmation_literal_file.read_bytes(),
            approved_by=args.approved_by,
        )
        _print(
            {
                "approval_mode": receipt.authorization_mode,
                "receipt": receipt_ref.to_dict(),
                "release": pin.to_dict(),
            }
        )
        return 0
    if args.command == "record-standing-candidate-authorization":
        raw_targets = _load(args.target_refs_json)
        if not isinstance(raw_targets, list):
            raise ValueError("target refs JSON must be an array")
        targets: list[tuple[str, str]] = []
        for value in raw_targets:
            item = _object(value, "target ref")
            if set(item) != {"artifact_id", "sha256"}:
                raise ValueError("target ref fields must be artifact_id and sha256")
            targets.append((str(item["artifact_id"]), str(item["sha256"])))
        binding = record_standing_candidate_authorization(
            data_root,
            authorization_type=args.authorization_type,
            registry_name=args.registry_name,
            target_refs=tuple(targets),
            availability_calendar_id=args.availability_calendar_id,
            availability_calendar_sha256=args.availability_calendar_sha256,
            standing_authorization_literal=args.standing_authorization_literal_file.read_bytes(),
            reaffirmation_literal=args.reaffirmation_literal_file.read_bytes(),
            approved_by=args.approved_by,
        )
        _print(binding.to_dict())
        return 0
    if args.command == "verify-release":
        pin = RegistryReleasePin.from_dict(_load(args.release_pin_json))
        loaded = load_registry_release(data_root, pin)
        _print(
            {
                "decision_count": len(loaded.decision_rows),
                "registry_name": loaded.registry_name,
                "release_available_session": loaded.release_available_session.isoformat(),
                "release_id": loaded.release_id,
                "status": "verified",
            }
        )
        return 0
    raw_pins = _load(args.release_pins_json)
    if not isinstance(raw_pins, list):
        raise ValueError("release pins JSON must be an array")
    release_set = load_registry_release_set(
        data_root,
        tuple(RegistryReleasePin.from_dict(value) for value in raw_pins),
    )
    _print(
        {
            "registry_count": len(release_set.releases),
            "release_ids": [item.release_id for item in release_set.releases],
            "status": "verified",
        }
    )
    return 0


def _load(path: Path) -> object:
    return json.loads(path.expanduser().read_text(encoding="utf-8"))


def _object(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise ValueError(f"{label} must be an object")
    return value


def _parse_utc(value: str) -> datetime:
    if not value.endswith("Z"):
        raise ValueError("UTC timestamps must end in Z")
    parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    if parsed.tzinfo != UTC:
        raise ValueError("UTC timestamp must use exact UTC")
    return parsed


def _print(value: object) -> None:
    print(json.dumps(value, allow_nan=False, indent=2, sort_keys=True))


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
