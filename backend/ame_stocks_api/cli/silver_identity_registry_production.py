"""Production-only CLI for internally constructed fixed S7 registry decisions."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from ame_stocks_api.silver.identity_market_sequence import (
    CALENDAR_ARTIFACT_ID,
    CALENDAR_ARTIFACT_SHA256,
)
from ame_stocks_api.silver.identity_registry_production import (
    import_fixed_external_evidence_package,
    prepare_fixed_production_request,
)
from ame_stocks_api.silver.identity_registry_workflow import (
    STANDING_AUTHORIZATION_LITERAL,
    STANDING_REAFFIRMATION_LITERAL,
    ExactArtifactBinding,
    RegistryReleasePin,
    StoredControlDocument,
    publish_release_under_standing_authority,
    record_production_prerequisite_authorization,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    import_evidence = subparsers.add_parser("import-fixed-evidence-package")
    import_evidence.add_argument("--data-root", type=Path, required=True)
    import_evidence.add_argument(
        "--evidence-kind",
        choices=("cross-market", "exact-group"),
        required=True,
    )

    prepare = subparsers.add_parser("prepare-fixed-request")
    prepare.add_argument("--data-root", type=Path, required=True)
    prepare.add_argument("--registry-name", required=True)
    prepare.add_argument("--candidate-authorization-refs-json", type=Path, required=True)
    prepare.add_argument("--gate-c-completion-ref-json", type=Path)
    prepare.add_argument("--exact-group-candidate-ref-json", type=Path)
    prepare.add_argument("--exact-group-completion-ref-json", type=Path)
    prepare.add_argument("--asset-transition-release-pin-json", type=Path)

    publish = subparsers.add_parser("publish-fixed-standing-release")
    publish.add_argument("--data-root", type=Path, required=True)
    publish.add_argument("--request-ref-json", type=Path, required=True)
    publish.add_argument("--approved-by", required=True)

    authorize = subparsers.add_parser("record-fixed-prerequisite-authorization")
    authorize.add_argument("--data-root", type=Path, required=True)
    authorize.add_argument("--authorization-type", required=True)
    authorize.add_argument("--registry-name", required=True)
    authorize.add_argument("--target-refs-json", type=Path, required=True)
    authorize.add_argument("--approved-by", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    root = args.data_root.expanduser().resolve()
    if args.command == "import-fixed-evidence-package":
        evidence_type = {
            "cross-market": "identity_cross_market_external_evidence",
            "exact-group": "identity_exact_group_external_evidence",
        }[args.evidence_kind]
        imported = import_fixed_external_evidence_package(
            root,
            evidence_type=evidence_type,
        )
        _print(
            {
                "evidence_type": imported.evidence_type,
                "import_receipt": imported.import_receipt.to_dict(),
                "manifest": imported.manifest.to_dict(),
                "runtime_binding": imported.runtime_binding.to_dict(),
            }
        )
        return 0
    if args.command == "record-fixed-prerequisite-authorization":
        raw_targets = _load(args.target_refs_json)
        if not isinstance(raw_targets, list):
            raise ValueError("target refs must be an array")
        targets: list[tuple[str, str]] = []
        for value in raw_targets:
            if not isinstance(value, dict) or set(value) != {"artifact_id", "sha256"}:
                raise ValueError("target ref fields must be artifact_id and sha256")
            targets.append((str(value["artifact_id"]), str(value["sha256"])))
        binding = record_production_prerequisite_authorization(
            root,
            authorization_type=args.authorization_type,
            registry_name=args.registry_name,
            target_refs=tuple(targets),
            availability_calendar_id=CALENDAR_ARTIFACT_ID,
            availability_calendar_sha256=CALENDAR_ARTIFACT_SHA256,
            standing_authorization_literal=STANDING_AUTHORIZATION_LITERAL.encode("utf-8"),
            reaffirmation_literal=STANDING_REAFFIRMATION_LITERAL.encode("utf-8"),
            approved_by=args.approved_by,
        )
        _print(binding.to_dict())
        return 0
    if args.command == "prepare-fixed-request":
        raw_authorizations = _load(args.candidate_authorization_refs_json)
        if not isinstance(raw_authorizations, list):
            raise ValueError("candidate authorization refs must be an array")
        prepared = prepare_fixed_production_request(
            root,
            registry_name=args.registry_name,
            candidate_authorizations=tuple(
                ExactArtifactBinding.from_dict(value) for value in raw_authorizations
            ),
            gate_c_completion=_optional_ref(args.gate_c_completion_ref_json),
            exact_group_candidate=_optional_ref(args.exact_group_candidate_ref_json),
            exact_group_completion=_optional_ref(args.exact_group_completion_ref_json),
            asset_transition_release=(
                None
                if args.asset_transition_release_pin_json is None
                else RegistryReleasePin.from_dict(_load(args.asset_transition_release_pin_json))
            ),
        )
        _print(
            {
                "candidate": prepared.candidate.to_dict(),
                "decision_ids": list(prepared.decision_ids),
                "plan": prepared.plan.to_dict(),
                "registry_name": prepared.registry_name,
                "request": prepared.request.to_dict(),
            }
        )
        return 0

    request = StoredControlDocument.from_dict(_load(args.request_ref_json))
    receipt, receipt_ref, release = publish_release_under_standing_authority(
        root,
        request_document=request,
        standing_authorization_literal=STANDING_AUTHORIZATION_LITERAL.encode("utf-8"),
        reaffirmation_literal=STANDING_REAFFIRMATION_LITERAL.encode("utf-8"),
        approved_by=args.approved_by,
    )
    _print(
        {
            "approval_mode": receipt.authorization_mode,
            "receipt": receipt_ref.to_dict(),
            "release": release.to_dict(),
        }
    )
    return 0


def _optional_ref(path: Path | None) -> StoredControlDocument | None:
    return None if path is None else StoredControlDocument.from_dict(_load(path))


def _load(path: Path) -> object:
    return json.loads(path.expanduser().read_text(encoding="utf-8"))


def _print(value: object) -> None:
    print(json.dumps(value, allow_nan=False, indent=2, sort_keys=True))


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
