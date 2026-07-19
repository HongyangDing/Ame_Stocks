"""Production-only ingress for the fixed S7 identity registry decisions.

Unlike the low-level fixture API in :mod:`identity_registry_workflow`, this
module never accepts decision rows, source-record IDs, timestamps, or
availability dates.  It replays exact upstream completions/evidence, builds the
reviewed fixed decisions internally, and stores the immutable
candidate -> plan -> approval-request chain.
"""

from __future__ import annotations

import hashlib
import json
import os
import stat
import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from types import MappingProxyType
from typing import Final

import pyarrow as pa
import pyarrow.parquet as pq

from ame_stocks_api.artifacts import (
    ArtifactError,
    safe_relative_path,
    stable_digest,
    write_bytes_immutable,
)
from ame_stocks_api.silver.calendar_artifact import load_xnys_calendar_artifact
from ame_stocks_api.silver.identity_cross_market import (
    ApprovedCrossMarketAdjudication,
    CrossMarketAdjudicationDisposition,
    IdentityCaseResolutionRole,
    LinkedIdentityCase,
)
from ame_stocks_api.silver.identity_market_sequence import (
    CALENDAR_ARTIFACT_ID,
    CALENDAR_ARTIFACT_SHA256,
    REVIEWED_EVIDENCE_SCHEMA,
    REVIEWED_FOREIGN_ROW_COUNT,
    S7MarketSequenceResourceCaps,
)
from ame_stocks_api.silver.identity_provider_evidence import S7_SIX_RELEASE_BINDING_ID
from ame_stocks_api.silver.identity_registry_exact_group_scopes import (
    LoadedExactGroupRegistryScopes,
    load_identity_registry_exact_group_scopes,
)
from ame_stocks_api.silver.identity_registry_workflow import (
    PRODUCTION_INGRESS_ATTESTATION_TYPE,
    PRODUCTION_INGRESS_ATTESTATION_VERSION,
    REGISTRY_ORDER,
    REQUIRED_CANDIDATE_AUTHORIZATION_ROLES,
    ExactArtifactBinding,
    ExactSourceRow,
    ExactSourceScope,
    RegistryCandidateManifest,
    RegistryName,
    RegistryReleasePin,
    RegistryRuntimeBinding,
    StoredControlDocument,
    capture_registry_runtime_binding,
    create_approval_request,
    create_decision_plan,
    create_registry_decision_candidate,
    current_registry_contract_pin,
    is_canonical_production_data_root,
    load_registry_release,
    require_current_registry_runtime_binding,
    store_approval_request,
    store_candidate,
    store_decision_plan,
)
from ame_stocks_api.silver.identity_relation_registries import (
    AssetTransitionDecision,
    AssetTransitionDisposition,
    AssetTransitionType,
    ProviderCompositeOverrideDecision,
    ProviderCompositeOverrideDisposition,
    ShareClassAdjudicationDecision,
    ShareClassAdjudicationDisposition,
)
from ame_stocks_api.silver.identity_resolution import canonical_asset_id
from ame_stocks_api.silver.identity_source import S7_S4_RELEASE_SET_ID

PRODUCTION_INGRESS_VERSION: Final = "s7_fixed_registry_production_ingress_v1"
EVIDENCE_PACKAGE_IMPORT_VERSION: Final = "s7_fixed_external_evidence_import_v1"
PRODUCTION_PREPARATION_ORDER: Final = (
    RegistryName.ASSET_TRANSITION.value,
    RegistryName.PROVIDER_COMPOSITE_OVERRIDE.value,
    RegistryName.SHARE_CLASS_ADJUDICATION.value,
    RegistryName.IDENTITY_ADJUDICATION.value,
    RegistryName.IDENTITY_CROSS_MARKET_ADJUDICATION.value,
)

_RELATION_REGISTRIES: Final = frozenset(
    {
        RegistryName.ASSET_TRANSITION.value,
        RegistryName.PROVIDER_COMPOSITE_OVERRIDE.value,
        RegistryName.SHARE_CLASS_ADJUDICATION.value,
    }
)
_PLACEHOLDER_DIGEST: Final = "0" * 64
_PLACEHOLDER_PATH: Final = "pending/filled-from-immutable-approval-chain.json"
_EVIDENCE_REPO_MANIFESTS: Final[Mapping[str, str]] = MappingProxyType(
    {
        "identity_cross_market_external_evidence": (
            "docs/silver/evidence/s7-cross-market/"
            "identity-cross-market-external-evidence-manifest.candidate.json"
        ),
        "identity_exact_group_external_evidence": (
            "docs/silver/evidence/s7-exact-groups/"
            "identity-exact-group-external-evidence-manifest.candidate.json"
        ),
    }
)
_EVIDENCE_ALLOWED_PREFIXES: Final = (
    "docs/silver/evidence/s7-cross-market/",
    "docs/silver/evidence/s7-exact-groups/",
)


class IdentityRegistryProductionError(RuntimeError):
    """Raised before caller-controlled facts can enter a production candidate."""


@dataclass(frozen=True, slots=True)
class PreparedProductionRegistryRequest:
    registry_name: str
    candidate: StoredControlDocument
    plan: StoredControlDocument
    request: StoredControlDocument
    decision_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ImportedExternalEvidencePackage:
    evidence_type: str
    manifest: StoredControlDocument
    import_receipt: ExactArtifactBinding
    runtime_binding: RegistryRuntimeBinding


@dataclass(frozen=True, slots=True)
class _GateCSource:
    candidate: ExactArtifactBinding
    completion: ExactArtifactBinding
    source_six_release_binding_id: str
    detector_preview_id: str
    detector_preview_sha256: str
    external_evidence_id: str
    external_evidence_sha256: str
    scopes: Mapping[str, ExactSourceScope]
    case_roles: Mapping[str, Mapping[str, str]]


def import_fixed_external_evidence_package(
    data_root: Path,
    *,
    evidence_type: str,
) -> ImportedExternalEvidencePackage:
    """Snapshot one code-pinned evidence package from the current Git tree.

    No caller path is admitted.  Bytes are read from ``git show <commit>:<path>``
    after a clean-runtime capture, copied immutably to the canonical data root,
    and wrapped by a content-addressed receipt binding the Git commit/tree.
    """

    root = data_root.expanduser().resolve()
    if not is_canonical_production_data_root(root):
        raise IdentityRegistryProductionError(
            "fixed evidence import requires the canonical production root"
        )
    try:
        manifest_relative = _EVIDENCE_REPO_MANIFESTS[evidence_type]
    except KeyError as exc:
        raise IdentityRegistryProductionError("unsupported fixed evidence package") from exc
    runtime_binding = capture_registry_runtime_binding()
    repo_root = Path(__file__).resolve().parents[3]
    manifest_content = _read_git_blob(
        repo_root,
        runtime_binding.git_commit,
        manifest_relative,
    )
    document = _decode_json_no_duplicates(manifest_content, "fixed evidence manifest")
    canonical = (
        json.dumps(
            document,
            allow_nan=False,
            ensure_ascii=True,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")
    payload = dict(document)
    manifest_id = payload.pop("manifest_id", None)
    if (
        canonical != manifest_content
        or document.get("manifest_type") != evidence_type
        or document.get("manifest_status") != "candidate_not_approved"
        or not isinstance(manifest_id, str)
        or stable_digest(payload) != manifest_id
    ):
        raise IdentityRegistryProductionError(
            "fixed repository evidence manifest identity/status changed"
        )
    artifacts, _ = _validate_external_evidence_manifest_envelope(
        document,
        expected_type=evidence_type,
    )
    source_blobs: list[tuple[str, bytes]] = []
    for index, artifact in enumerate(artifacts):
        relative = artifact.get("path")
        if not isinstance(relative, str) or not relative.startswith(_EVIDENCE_ALLOWED_PREFIXES):
            raise IdentityRegistryProductionError(
                "fixed evidence artifact escaped the approved package prefixes"
            )
        content = _read_git_blob(repo_root, runtime_binding.git_commit, relative)
        if (
            artifact.get("bytes") != len(content)
            or artifact.get("sha256") != hashlib.sha256(content).hexdigest()
        ):
            raise IdentityRegistryProductionError(
                f"fixed evidence artifact {index} differs from its manifest receipt"
            )
        source_blobs.append((relative, content))

    raw_refs: list[dict[str, object]] = []
    for relative, content in source_blobs:
        try:
            written = write_bytes_immutable(
                root,
                safe_relative_path(root, relative),
                content,
                temporary_directory=root / "tmp" / "s7-evidence-import-writes",
            )
        except ArtifactError as exc:
            raise IdentityRegistryProductionError(
                "fixed evidence raw artifact import failed closed"
            ) from exc
        raw_refs.append(
            {
                "bytes": int(written["bytes"]),
                "path": str(written["path"]),
                "sha256": str(written["sha256"]),
            }
        )
    try:
        manifest_written = write_bytes_immutable(
            root,
            safe_relative_path(root, manifest_relative),
            manifest_content,
            temporary_directory=root / "tmp" / "s7-evidence-import-writes",
        )
    except ArtifactError as exc:
        raise IdentityRegistryProductionError(
            "fixed evidence manifest import failed closed"
        ) from exc
    manifest_ref = StoredControlDocument(
        object_id=manifest_id,
        path=str(manifest_written["path"]),
        sha256=str(manifest_written["sha256"]),
        bytes=int(manifest_written["bytes"]),
    )
    loaded = _load_external_evidence(root, manifest_ref, expected_type=evidence_type)
    evidence_available = date.fromisoformat(
        str(_mapping(loaded["availability"], "evidence availability")["available_session"])
    )
    calendar = load_xnys_calendar_artifact(
        root,
        calendar_artifact_id=CALENDAR_ARTIFACT_ID,
        expected_sha256=CALENDAR_ARTIFACT_SHA256,
    )
    imported_at = _utc_now()
    import_available = max(
        evidence_available,
        calendar.first_open_after(imported_at)[0],
    )
    slot_id = stable_digest(
        {
            "evidence_type": evidence_type,
            "manifest": manifest_ref.to_dict(),
            "production_data_root": root.as_posix(),
            "runtime_binding_id": runtime_binding.runtime_binding_id,
            "version": EVIDENCE_PACKAGE_IMPORT_VERSION,
        }
    )
    relative_path = (
        "manifests/silver/identity/external-evidence-imports/"
        f"evidence_type={evidence_type}/slot_id={slot_id}/receipt.json"
    )
    existing = _load_fixed_evidence_import_receipt(
        root,
        relative_path,
        expected_type=evidence_type,
        expected_manifest=manifest_ref,
        expected_raw_refs=tuple(raw_refs),
        expected_runtime=runtime_binding,
        expected_available_session=None,
        revalidate_runtime=True,
    )
    if existing is not None:
        return ImportedExternalEvidencePackage(
            evidence_type=evidence_type,
            manifest=manifest_ref,
            import_receipt=existing,
            runtime_binding=runtime_binding,
        )
    logical: dict[str, object] = {
        "artifact_type": "s7_fixed_external_evidence_import_receipt",
        "artifact_version": EVIDENCE_PACKAGE_IMPORT_VERSION,
        "evidence_type": evidence_type,
        "import_available_session": import_available.isoformat(),
        "import_slot_id": slot_id,
        "imported_at_utc": _utc_text(imported_at),
        "manifest": manifest_ref.to_dict(),
        "production_data_root": root.as_posix(),
        "raw_artifacts": raw_refs,
        "runtime_binding": runtime_binding.to_dict(),
    }
    receipt_document = {"import_id": stable_digest(logical), **logical}
    receipt_content = _canonical_control_bytes(receipt_document)
    try:
        written = write_bytes_immutable(
            root,
            safe_relative_path(root, relative_path),
            receipt_content,
            temporary_directory=root / "tmp" / "s7-evidence-import-writes",
        )
    except ArtifactError:
        raced = _load_fixed_evidence_import_receipt(
            root,
            relative_path,
            expected_type=evidence_type,
            expected_manifest=manifest_ref,
            expected_raw_refs=tuple(raw_refs),
            expected_runtime=runtime_binding,
            expected_available_session=None,
            revalidate_runtime=True,
        )
        if raced is None:
            raise
        binding = raced
    else:
        binding = ExactArtifactBinding(
            role="external_evidence_import_receipt",
            artifact_id=str(receipt_document["import_id"]),
            path=str(written["path"]),
            sha256=str(written["sha256"]),
            bytes=int(written["bytes"]),
            available_session=import_available,
            embedded_id_field="import_id",
        )
        replayed = _load_fixed_evidence_import_receipt(
            root,
            relative_path,
            expected_type=evidence_type,
            expected_manifest=manifest_ref,
            expected_raw_refs=tuple(raw_refs),
            expected_runtime=runtime_binding,
            expected_available_session=import_available,
            revalidate_runtime=True,
        )
        if replayed != binding:
            raise IdentityRegistryProductionError(
                "fixed evidence import receipt failed post-write replay"
            )
    return ImportedExternalEvidencePackage(
        evidence_type=evidence_type,
        manifest=manifest_ref,
        import_receipt=binding,
        runtime_binding=runtime_binding,
    )


def prepare_fixed_production_request(
    data_root: Path,
    *,
    registry_name: str,
    candidate_authorizations: Sequence[ExactArtifactBinding],
    gate_c_completion: StoredControlDocument | None = None,
    exact_group_candidate: StoredControlDocument | None = None,
    exact_group_completion: StoredControlDocument | None = None,
    asset_transition_release: RegistryReleasePin | None = None,
) -> PreparedProductionRegistryRequest:
    """Build and store one fixed production candidate/request without fact inputs.

    The caller selects only exact immutable artifacts.  Registry decisions,
    source scopes, timestamps, availability and IDs are derived internally.
    """

    root = data_root.expanduser().resolve()
    if not is_canonical_production_data_root(root):
        raise IdentityRegistryProductionError(
            "fixed production ingress requires the canonical production data root"
        )
    if registry_name not in REGISTRY_ORDER:
        raise IdentityRegistryProductionError("unsupported production registry")
    if (
        registry_name == RegistryName.PROVIDER_COMPOSITE_OVERRIDE.value
        and asset_transition_release is None
    ):
        raise IdentityRegistryProductionError(
            "provider Composite override requires the exact asset-transition release"
        )
    if (
        registry_name != RegistryName.PROVIDER_COMPOSITE_OVERRIDE.value
        and asset_transition_release is not None
    ):
        raise IdentityRegistryProductionError(
            "only provider Composite override may consume an asset-transition release"
        )
    calendar = load_xnys_calendar_artifact(
        root,
        calendar_artifact_id=CALENDAR_ARTIFACT_ID,
        expected_sha256=CALENDAR_ARTIFACT_SHA256,
    )
    evidence_type = (
        "identity_exact_group_external_evidence"
        if registry_name in _RELATION_REGISTRIES
        else "identity_cross_market_external_evidence"
    )
    imported_evidence = import_fixed_external_evidence_package(
        root,
        evidence_type=evidence_type,
    )
    external_evidence = imported_evidence.manifest

    if registry_name in _RELATION_REGISTRIES:
        if exact_group_candidate is None or exact_group_completion is None:
            raise IdentityRegistryProductionError(
                "relation production ingress requires exact-group candidate/completion"
            )
        if gate_c_completion is not None:
            raise IdentityRegistryProductionError("relation ingress cannot consume Gate C")
        exact_loaded = load_identity_registry_exact_group_scopes(
            root,
            candidate_pin=_pin_mapping(exact_group_candidate),
            completion_pin=_pin_mapping(exact_group_completion),
        )
        source_available = calendar.first_open_after(
            _exact_group_completion_time(root, exact_group_completion)
        )[0]
        source_artifacts = (
            _binding(
                exact_group_candidate,
                role="source_exact_group_candidate_manifest",
                available_session=source_available,
            ),
            _binding(
                exact_group_completion,
                role="source_exact_group_completion_manifest",
                available_session=source_available,
            ),
        )
        evidence_document = _load_external_evidence(
            root,
            external_evidence,
            expected_type="identity_exact_group_external_evidence",
        )
        evidence_available = date.fromisoformat(
            str(
                _mapping(evidence_document["availability"], "evidence availability")[
                    "available_session"
                ]
            )
        )
        evidence_artifacts = (
            _binding(
                external_evidence,
                role="external_evidence",
                available_session=evidence_available,
            ),
        )
        gate_c = None
    else:
        if gate_c_completion is None:
            raise IdentityRegistryProductionError(
                "episode/cross-market production ingress requires exact Gate C completion"
            )
        if exact_group_candidate is not None or exact_group_completion is not None:
            raise IdentityRegistryProductionError(
                "episode/cross-market ingress cannot consume exact-group inputs"
            )
        gate_c = _load_gate_c_source(root, gate_c_completion)
        source_artifacts = (gate_c.candidate, gate_c.completion)
        evidence_document = _load_external_evidence(
            root,
            external_evidence,
            expected_type="identity_cross_market_external_evidence",
        )
        evidence_available = date.fromisoformat(
            str(
                _mapping(evidence_document["availability"], "evidence availability")[
                    "available_session"
                ]
            )
        )
        evidence_artifacts = (
            _binding(
                external_evidence,
                role="external_evidence",
                available_session=evidence_available,
            ),
        )
        if evidence_document.get("source_six_release_binding_id") != (S7_SIX_RELEASE_BINDING_ID):
            raise IdentityRegistryProductionError(
                "cross-market evidence six-release binding differs"
            )
        if (
            external_evidence.object_id != gate_c.external_evidence_id
            or external_evidence.sha256 != gate_c.external_evidence_sha256
        ):
            raise IdentityRegistryProductionError(
                "Gate C and production ingress bind different external evidence"
            )
        exact_loaded = None

    contract_pin = current_registry_contract_pin(registry_name)
    ingress_document, ingress_artifact = _record_or_replay_production_ingress_attestation(
        root,
        registry_name=registry_name,
        contract_pin=contract_pin.to_dict(),
        source_artifacts=tuple(source_artifacts),
        evidence_artifacts=tuple(evidence_artifacts),
        authorization_artifacts=tuple(candidate_authorizations),
        evidence_import_artifact=imported_evidence.import_receipt,
        asset_transition_release=asset_transition_release,
        calendar=calendar,
        runtime_binding=imported_evidence.runtime_binding,
    )
    created_at = _parse_utc(str(ingress_document["created_at_utc"]))
    candidate_available = ingress_artifact.available_session
    if registry_name in _RELATION_REGISTRIES:
        assert exact_loaded is not None
        decisions = _build_relation_decisions(
            root,
            registry_name=registry_name,
            exact=exact_loaded,
            source_candidate=source_artifacts[0],
            source_completion=source_artifacts[1],
            evidence=evidence_artifacts[0],
            evidence_document=evidence_document,
            candidate_available=candidate_available,
            asset_transition_release=asset_transition_release,
        )
    else:
        assert gate_c is not None
        decisions = (
            ()
            if registry_name == RegistryName.IDENTITY_ADJUDICATION.value
            else _build_cross_market_decisions(
                gate_c,
                evidence=evidence_artifacts[0],
                evidence_document=evidence_document,
                candidate_available=candidate_available,
            )
        )

    candidate = RegistryCandidateManifest(
        registry_name=registry_name,
        contract_pin=contract_pin,
        source_artifacts=tuple(source_artifacts),
        evidence_artifacts=tuple(evidence_artifacts),
        authorization_artifacts=tuple(candidate_authorizations),
        availability_calendar_id=CALENDAR_ARTIFACT_ID,
        availability_calendar_sha256=CALENDAR_ARTIFACT_SHA256,
        created_at_utc=created_at,
        candidate_available_session=candidate_available,
        decisions=tuple(sorted(decisions, key=lambda item: item.decision_id)),
        production_ingress_artifact=ingress_artifact,
    )
    candidate_ref = store_candidate(root, candidate)
    # Reload the first-writer candidate: retry time cannot fork plan/request IDs.
    from ame_stocks_api.silver.identity_registry_workflow import load_candidate_control

    stored_candidate = load_candidate_control(root, candidate_ref)
    plan = create_decision_plan(stored_candidate, candidate_ref)
    plan_ref = store_decision_plan(root, plan)
    request = create_approval_request(plan, plan_ref)
    request_ref = store_approval_request(root, request)
    return PreparedProductionRegistryRequest(
        registry_name=registry_name,
        candidate=candidate_ref,
        plan=plan_ref,
        request=request_ref,
        decision_ids=plan.decision_ids,
    )


def _record_or_replay_production_ingress_attestation(
    root: Path,
    *,
    registry_name: str,
    contract_pin: Mapping[str, object],
    source_artifacts: tuple[ExactArtifactBinding, ...],
    evidence_artifacts: tuple[ExactArtifactBinding, ...],
    authorization_artifacts: tuple[ExactArtifactBinding, ...],
    evidence_import_artifact: ExactArtifactBinding,
    asset_transition_release: RegistryReleasePin | None,
    calendar: object,
    runtime_binding: RegistryRuntimeBinding,
) -> tuple[Mapping[str, object], ExactArtifactBinding]:
    if not is_canonical_production_data_root(root):
        raise IdentityRegistryProductionError(
            "production ingress attestation requires the canonical root"
        )
    if (
        getattr(calendar, "calendar_artifact_id", None) != CALENDAR_ARTIFACT_ID
        or getattr(calendar, "sha256", None) != CALENDAR_ARTIFACT_SHA256
    ):
        raise IdentityRegistryProductionError(
            "production ingress calendar differs from the fixed calendar"
        )
    if {
        item.role for item in authorization_artifacts
    } != REQUIRED_CANDIDATE_AUTHORIZATION_ROLES or len(authorization_artifacts) != len(
        REQUIRED_CANDIDATE_AUTHORIZATION_ROLES
    ):
        raise IdentityRegistryProductionError(
            "production ingress requires the exact three prerequisite authorizations"
        )
    if not source_artifacts or not evidence_artifacts:
        raise IdentityRegistryProductionError("production ingress inputs cannot be empty")
    if evidence_import_artifact.role != "external_evidence_import_receipt":
        raise IdentityRegistryProductionError("production ingress evidence import role changed")
    require_current_registry_runtime_binding(runtime_binding)
    slot_id = _production_ingress_slot_id(
        root,
        registry_name=registry_name,
        contract_pin=contract_pin,
        source_artifacts=source_artifacts,
        evidence_artifacts=evidence_artifacts,
        authorization_artifacts=authorization_artifacts,
        evidence_import_artifact=evidence_import_artifact,
        asset_transition_release=asset_transition_release,
        runtime_binding=runtime_binding,
    )
    relative_path = _production_ingress_path(registry_name, slot_id)
    existing = _load_existing_production_ingress_attestation(
        root,
        relative_path,
        expected_registry_name=registry_name,
        expected_contract_pin=contract_pin,
        expected_sources=source_artifacts,
        expected_evidence=evidence_artifacts,
        expected_authorizations=authorization_artifacts,
        expected_evidence_import=evidence_import_artifact,
        expected_transition_release=asset_transition_release,
        expected_runtime_binding=runtime_binding,
        calendar=calendar,
    )
    if existing is not None:
        return existing
    created_at = _utc_now()
    upstream_sessions = [
        calendar.first_open_after(created_at)[0],  # type: ignore[attr-defined]
        *(item.available_session for item in source_artifacts),
        *(item.available_session for item in evidence_artifacts),
        *(item.available_session for item in authorization_artifacts),
        evidence_import_artifact.available_session,
    ]
    if asset_transition_release is not None:
        upstream_sessions.append(asset_transition_release.release_available_session)
    available_session = max(upstream_sessions)
    logical: dict[str, object] = {
        "artifact_type": PRODUCTION_INGRESS_ATTESTATION_TYPE,
        "artifact_version": PRODUCTION_INGRESS_ATTESTATION_VERSION,
        "asset_transition_release": (
            None if asset_transition_release is None else asset_transition_release.to_dict()
        ),
        "authorization_artifacts": [item.to_dict() for item in authorization_artifacts],
        "authorization_effect": "none_provenance_only",
        "availability_calendar_id": CALENDAR_ARTIFACT_ID,
        "availability_calendar_sha256": CALENDAR_ARTIFACT_SHA256,
        "available_session": available_session.isoformat(),
        "contract_pin": dict(contract_pin),
        "created_at_utc": _utc_text(created_at),
        "evidence_artifacts": [item.to_dict() for item in evidence_artifacts],
        "evidence_import_artifact": evidence_import_artifact.to_dict(),
        "ingress_slot_id": slot_id,
        "production_data_root": root.as_posix(),
        "registry_name": registry_name,
        "runtime_binding": runtime_binding.to_dict(),
        "source_artifacts": [item.to_dict() for item in source_artifacts],
    }
    document = {"attestation_id": stable_digest(logical), **logical}
    content = _canonical_control_bytes(document)
    try:
        receipt = write_bytes_immutable(
            root,
            safe_relative_path(root, relative_path),
            content,
            temporary_directory=root / "tmp" / "s7-registry-control-writes",
        )
    except ArtifactError:
        raced = _load_existing_production_ingress_attestation(
            root,
            relative_path,
            expected_registry_name=registry_name,
            expected_contract_pin=contract_pin,
            expected_sources=source_artifacts,
            expected_evidence=evidence_artifacts,
            expected_authorizations=authorization_artifacts,
            expected_evidence_import=evidence_import_artifact,
            expected_transition_release=asset_transition_release,
            expected_runtime_binding=runtime_binding,
            calendar=calendar,
        )
        if raced is None:
            raise
        return raced
    binding = ExactArtifactBinding(
        role="production_ingress_attestation",
        artifact_id=str(document["attestation_id"]),
        path=str(receipt["path"]),
        sha256=str(receipt["sha256"]),
        bytes=int(receipt["bytes"]),
        available_session=available_session,
        embedded_id_field="attestation_id",
    )
    replayed = _load_existing_production_ingress_attestation(
        root,
        relative_path,
        expected_registry_name=registry_name,
        expected_contract_pin=contract_pin,
        expected_sources=source_artifacts,
        expected_evidence=evidence_artifacts,
        expected_authorizations=authorization_artifacts,
        expected_evidence_import=evidence_import_artifact,
        expected_transition_release=asset_transition_release,
        expected_runtime_binding=runtime_binding,
        calendar=calendar,
    )
    if replayed is None or replayed[1] != binding:
        raise IdentityRegistryProductionError(
            "production ingress attestation failed post-write replay"
        )
    return replayed


def _production_ingress_slot_id(
    root: Path,
    *,
    registry_name: str,
    contract_pin: Mapping[str, object],
    source_artifacts: tuple[ExactArtifactBinding, ...],
    evidence_artifacts: tuple[ExactArtifactBinding, ...],
    authorization_artifacts: tuple[ExactArtifactBinding, ...],
    evidence_import_artifact: ExactArtifactBinding,
    asset_transition_release: RegistryReleasePin | None,
    runtime_binding: RegistryRuntimeBinding,
) -> str:
    return stable_digest(
        {
            "artifact_type": PRODUCTION_INGRESS_ATTESTATION_TYPE,
            "artifact_version": PRODUCTION_INGRESS_ATTESTATION_VERSION,
            "asset_transition_release": (
                None if asset_transition_release is None else asset_transition_release.to_dict()
            ),
            "authorization_artifacts": [item.to_dict() for item in authorization_artifacts],
            "availability_calendar_id": CALENDAR_ARTIFACT_ID,
            "availability_calendar_sha256": CALENDAR_ARTIFACT_SHA256,
            "contract_pin": dict(contract_pin),
            "evidence_artifacts": [item.to_dict() for item in evidence_artifacts],
            "evidence_import_artifact": evidence_import_artifact.to_dict(),
            "production_data_root": root.as_posix(),
            "registry_name": registry_name,
            "runtime_binding_id": runtime_binding.runtime_binding_id,
            "source_artifacts": [item.to_dict() for item in source_artifacts],
        }
    )


def _production_ingress_path(registry_name: str, slot_id: str) -> str:
    return (
        "manifests/silver/identity/registry-workflow/"
        f"registry={registry_name}/production-ingress-attestations/"
        f"slot_id={slot_id}/attestation.json"
    )


def _load_existing_production_ingress_attestation(
    root: Path,
    relative_path: str,
    *,
    expected_registry_name: str,
    expected_contract_pin: Mapping[str, object],
    expected_sources: tuple[ExactArtifactBinding, ...],
    expected_evidence: tuple[ExactArtifactBinding, ...],
    expected_authorizations: tuple[ExactArtifactBinding, ...],
    expected_evidence_import: ExactArtifactBinding,
    expected_transition_release: RegistryReleasePin | None,
    expected_runtime_binding: RegistryRuntimeBinding,
    calendar: object,
) -> tuple[Mapping[str, object], ExactArtifactBinding] | None:
    try:
        path = safe_relative_path(root, relative_path)
    except ArtifactError as exc:
        raise IdentityRegistryProductionError("production ingress path is unsafe") from exc
    if not path.exists():
        return None
    if path.is_symlink() or not path.is_file():
        raise IdentityRegistryProductionError("production ingress attestation is unsafe")
    content = _read_regular_file_snapshot(path, "production ingress attestation")
    document = _decode_json_no_duplicates(content, "production ingress attestation")
    available_session = date.fromisoformat(str(document.get("available_session")))
    binding = ExactArtifactBinding(
        role="production_ingress_attestation",
        artifact_id=str(document.get("attestation_id")),
        path=relative_path,
        sha256=hashlib.sha256(content).hexdigest(),
        bytes=len(content),
        available_session=available_session,
        embedded_id_field="attestation_id",
    )
    _validate_production_ingress_attestation(
        root,
        document,
        content=content,
        artifact=binding,
        expected_registry_name=expected_registry_name,
        expected_contract_pin=expected_contract_pin,
        expected_sources=expected_sources,
        expected_evidence=expected_evidence,
        expected_authorizations=expected_authorizations,
        expected_evidence_import=expected_evidence_import,
        expected_transition_release=expected_transition_release,
        expected_runtime_binding=expected_runtime_binding,
        calendar=calendar,
        revalidate_runtime=True,
    )
    return document, binding


def _validate_production_ingress_attestation(
    root: Path,
    document: Mapping[str, object],
    *,
    content: bytes,
    artifact: ExactArtifactBinding,
    expected_registry_name: str,
    expected_contract_pin: Mapping[str, object],
    expected_sources: tuple[ExactArtifactBinding, ...],
    expected_evidence: tuple[ExactArtifactBinding, ...],
    expected_authorizations: tuple[ExactArtifactBinding, ...],
    expected_evidence_import: ExactArtifactBinding,
    expected_transition_release: RegistryReleasePin | None,
    expected_runtime_binding: RegistryRuntimeBinding | None,
    calendar: object,
    revalidate_runtime: bool,
) -> None:
    expected_keys = {
        "artifact_type",
        "artifact_version",
        "asset_transition_release",
        "attestation_id",
        "authorization_artifacts",
        "authorization_effect",
        "availability_calendar_id",
        "availability_calendar_sha256",
        "available_session",
        "contract_pin",
        "created_at_utc",
        "evidence_artifacts",
        "evidence_import_artifact",
        "ingress_slot_id",
        "production_data_root",
        "registry_name",
        "runtime_binding",
        "source_artifacts",
    }
    if set(document) != expected_keys:
        raise IdentityRegistryProductionError("production ingress attestation fields changed")
    if _canonical_control_bytes(document) != content:
        raise IdentityRegistryProductionError(
            "production ingress attestation is not canonical JSON"
        )
    sources = tuple(
        ExactArtifactBinding.from_dict(item)
        for item in _array(document["source_artifacts"], "attested sources")
    )
    evidence = tuple(
        ExactArtifactBinding.from_dict(item)
        for item in _array(document["evidence_artifacts"], "attested evidence")
    )
    authorizations = tuple(
        ExactArtifactBinding.from_dict(item)
        for item in _array(document["authorization_artifacts"], "attested authorizations")
    )
    transition = (
        None
        if document["asset_transition_release"] is None
        else RegistryReleasePin.from_dict(document["asset_transition_release"])
    )
    evidence_import = ExactArtifactBinding.from_dict(document["evidence_import_artifact"])
    runtime_binding = RegistryRuntimeBinding.from_dict(document["runtime_binding"])
    created_at = _parse_utc(str(document["created_at_utc"]))
    available_session = date.fromisoformat(str(document["available_session"]))
    contract_pin = _mapping(document["contract_pin"], "attested contract pin")
    slot_id = _production_ingress_slot_id(
        root,
        registry_name=expected_registry_name,
        contract_pin=contract_pin,
        source_artifacts=sources,
        evidence_artifacts=evidence,
        authorization_artifacts=authorizations,
        evidence_import_artifact=evidence_import,
        asset_transition_release=transition,
        runtime_binding=runtime_binding,
    )
    logical = {key: value for key, value in document.items() if key != "attestation_id"}
    upstream_sessions = [
        calendar.first_open_after(created_at)[0],  # type: ignore[attr-defined]
        *(item.available_session for item in sources),
        *(item.available_session for item in evidence),
        *(item.available_session for item in authorizations),
        evidence_import.available_session,
    ]
    if transition is not None:
        upstream_sessions.append(transition.release_available_session)
    if (
        not is_canonical_production_data_root(root)
        or getattr(calendar, "calendar_artifact_id", None) != CALENDAR_ARTIFACT_ID
        or getattr(calendar, "sha256", None) != CALENDAR_ARTIFACT_SHA256
        or document["artifact_type"] != PRODUCTION_INGRESS_ATTESTATION_TYPE
        or document["artifact_version"] != PRODUCTION_INGRESS_ATTESTATION_VERSION
        or document["attestation_id"] != artifact.artifact_id
        or document["attestation_id"] != stable_digest(logical)
        or document["authorization_effect"] != "none_provenance_only"
        or document["availability_calendar_id"] != CALENDAR_ARTIFACT_ID
        or document["availability_calendar_sha256"] != CALENDAR_ARTIFACT_SHA256
        or available_session != max(upstream_sessions)
        or artifact.available_session != available_session
        or dict(contract_pin) != dict(expected_contract_pin)
        or document["ingress_slot_id"] != slot_id
        or artifact.path != _production_ingress_path(expected_registry_name, slot_id)
        or document["production_data_root"] != root.as_posix()
        or document["registry_name"] != expected_registry_name
        or sources != expected_sources
        or evidence != expected_evidence
        or authorizations != expected_authorizations
        or evidence_import != expected_evidence_import
        or transition != expected_transition_release
        or (expected_runtime_binding is not None and runtime_binding != expected_runtime_binding)
    ):
        raise IdentityRegistryProductionError("production ingress attestation binding changed")
    evidence_type = (
        "identity_exact_group_external_evidence"
        if expected_registry_name in _RELATION_REGISTRIES
        else "identity_cross_market_external_evidence"
    )
    if len(expected_evidence) != 1:
        raise IdentityRegistryProductionError("production ingress must bind one evidence manifest")
    _replay_bound_evidence_import(
        root,
        evidence_import,
        expected_type=evidence_type,
        expected_manifest=_stored(expected_evidence[0]),
        expected_runtime=runtime_binding,
    )
    if revalidate_runtime:
        require_current_registry_runtime_binding(runtime_binding)


def validate_production_candidate_rebuild(
    data_root: Path,
    candidate: RegistryCandidateManifest,
) -> None:
    """Rebuild one production candidate solely from its exact ingress attestation."""

    root = data_root.expanduser().resolve()
    artifact = candidate.production_ingress_artifact
    if not is_canonical_production_data_root(root) or artifact is None:
        raise IdentityRegistryProductionError(
            "production candidate lacks canonical ingress provenance"
        )
    content = _read_exact_receipt_bytes(root, artifact.to_dict(), "production ingress")
    document = _decode_json_no_duplicates(content, "production ingress")
    transition = (
        None
        if document.get("asset_transition_release") is None
        else RegistryReleasePin.from_dict(document["asset_transition_release"])
    )
    evidence_import = ExactArtifactBinding.from_dict(document.get("evidence_import_artifact"))
    calendar = load_xnys_calendar_artifact(
        root,
        calendar_artifact_id=candidate.availability_calendar_id,
        expected_sha256=candidate.availability_calendar_sha256,
    )
    _validate_production_ingress_attestation(
        root,
        document,
        content=content,
        artifact=artifact,
        expected_registry_name=candidate.registry_name,
        expected_contract_pin=candidate.contract_pin.to_dict(),
        expected_sources=candidate.source_artifacts,
        expected_evidence=candidate.evidence_artifacts,
        expected_authorizations=candidate.authorization_artifacts,
        expected_evidence_import=evidence_import,
        expected_transition_release=transition,
        expected_runtime_binding=None,
        calendar=calendar,
        revalidate_runtime=True,
    )
    created_at = _parse_utc(str(document["created_at_utc"]))
    if (
        candidate.created_at_utc != created_at
        or candidate.candidate_available_session != artifact.available_session
    ):
        raise IdentityRegistryProductionError(
            "production candidate time/availability differs from ingress"
        )
    source_by_role = {item.role: item for item in candidate.source_artifacts}
    evidence_ref = _stored(candidate.evidence_artifacts[0])
    if candidate.registry_name in _RELATION_REGISTRIES:
        exact = load_identity_registry_exact_group_scopes(
            root,
            candidate_pin=source_by_role["source_exact_group_candidate_manifest"].to_dict(),
            completion_pin=source_by_role["source_exact_group_completion_manifest"].to_dict(),
        )
        evidence_document = _load_external_evidence(
            root,
            evidence_ref,
            expected_type="identity_exact_group_external_evidence",
        )
        rebuilt = _build_relation_decisions(
            root,
            registry_name=candidate.registry_name,
            exact=exact,
            source_candidate=source_by_role["source_exact_group_candidate_manifest"],
            source_completion=source_by_role["source_exact_group_completion_manifest"],
            evidence=candidate.evidence_artifacts[0],
            evidence_document=evidence_document,
            candidate_available=candidate.candidate_available_session,
            asset_transition_release=transition,
        )
    else:
        gate_c = _load_gate_c_source(
            root,
            _stored(source_by_role["source_gate_c_completion_manifest"]),
        )
        evidence_document = _load_external_evidence(
            root,
            evidence_ref,
            expected_type="identity_cross_market_external_evidence",
        )
        if candidate.registry_name == RegistryName.IDENTITY_ADJUDICATION.value:
            rebuilt = ()
        else:
            rebuilt = _build_cross_market_decisions(
                gate_c,
                evidence=candidate.evidence_artifacts[0],
                evidence_document=evidence_document,
                candidate_available=candidate.candidate_available_session,
            )
    if tuple(sorted(rebuilt, key=lambda item: item.decision_id)) != candidate.decisions:
        raise IdentityRegistryProductionError(
            "production candidate decisions differ from fixed ingress rebuild"
        )


def _build_relation_decisions(
    root: Path,
    *,
    registry_name: str,
    exact: LoadedExactGroupRegistryScopes,
    source_candidate: ExactArtifactBinding,
    source_completion: ExactArtifactBinding,
    evidence: ExactArtifactBinding,
    evidence_document: Mapping[str, object],
    candidate_available: date,
    asset_transition_release: RegistryReleasePin | None,
) -> tuple[object, ...]:
    common = _pending_relation_controls(
        source_candidate=source_candidate,
        evidence=evidence,
        candidate_available=candidate_available,
    )
    if registry_name == RegistryName.ASSET_TRANSITION.value:
        if asset_transition_release is not None:
            raise IdentityRegistryProductionError("asset transition cannot depend on itself")
        scope = _scope(exact, "asset_transition:SOR")
        decision = AssetTransitionDecision(
            **common,
            observed_ticker="SOR",
            transition_type=AssetTransitionType.CORPORATE_REORGANIZATION_SUCCESSOR_SECURITY,
            legal_effective_date=date(2025, 1, 1),
            predecessor_last_session=date(2024, 12, 31),
            successor_first_session=date(2025, 1, 2),
            predecessor_composite_figi="BBG000KMY6N2",
            successor_composite_figi="BBG01RK6N4M5",
            boundary_source_record_ids=scope.source_record_ids,
            disposition=AssetTransitionDisposition.CONFIRMED_GENUINE_TRANSITION,
            decision_version=1,
            supersedes_asset_transition_id=None,
            reason_code="source_capital_reorganization",
            reason_detail=(
                "Frozen SEC and issuer evidence establishes the successor security boundary."
            ),
        )
        return (
            create_registry_decision_candidate(
                registry_name=registry_name,
                case_key="asset_transition:SOR",
                proposed_contract_row=decision.to_registry_row(),
                source_scope=scope,
            ),
        )
    if registry_name == RegistryName.PROVIDER_COMPOSITE_OVERRIDE.value:
        if asset_transition_release is None:
            raise IdentityRegistryProductionError(
                "provider override requires the published asset-transition release"
            )
        released = load_registry_release(root, asset_transition_release)
        if (
            released.registry_name != RegistryName.ASSET_TRANSITION.value
            or len(released.decision_rows) != 1
            or len(released.candidate.decisions) != 1
        ):
            raise IdentityRegistryProductionError(
                "provider override dependency is not the one-row transition release"
            )
        transition_candidate = released.candidate.decisions[0]
        if (
            transition_candidate.case_key != "asset_transition:SOR"
            or released.candidate.source_artifacts != (source_candidate, source_completion)
            or released.candidate.evidence_artifacts != (evidence,)
        ):
            raise IdentityRegistryProductionError(
                "provider override dependency does not replay the exact SOR source/evidence"
            )
        transition = next(iter(released.decision_rows.values()))
        if (
            transition["observed_ticker"] != "SOR"
            or transition["successor_composite_figi"] != "BBG01RK6N4M5"
            or transition["asset_transition_id"] != transition_candidate.decision_id
            or transition["asset_transition_series_id"]
            != transition_candidate.frozen_row_claims["asset_transition_series_id"]
        ):
            raise IdentityRegistryProductionError("SOR transition dependency changed")
        scope = _scope(exact, "provider_composite_override:SOR")
        decision = ProviderCompositeOverrideDecision(
            **common,
            observed_ticker="SOR",
            observed_composite_figi="BBG000KMY6N2",
            canonical_composite_figi="BBG01RK6N4M5",
            observed_composite_market_code="US",
            canonical_composite_market_code="US",
            valid_from_session=date(2025, 1, 2),
            valid_through_session=date(2026, 7, 9),
            scoped_source_record_ids=scope.source_record_ids,
            asset_transition_series_id=str(transition["asset_transition_series_id"]),
            asset_transition_id=str(transition["asset_transition_id"]),
            asset_transition_available_session=transition["transition_available_session"],
            disposition=ProviderCompositeOverrideDisposition.CONFIRMED_STALE_AFTER_TRANSITION,
            decision_version=1,
            supersedes_provider_composite_override_id=None,
            reason_code="same_market_stale_after_transition",
            reason_detail="Provider retained the predecessor Composite after succession.",
        )
        return (
            create_registry_decision_candidate(
                registry_name=registry_name,
                case_key="provider_composite_override:SOR",
                proposed_contract_row=decision.to_registry_row(),
                source_scope=scope,
            ),
        )
    if asset_transition_release is not None:
        raise IdentityRegistryProductionError("Share Class decisions do not consume transition")
    rows = []
    for ticker, composite, observed, canonical in (
        ("ANABV", "BBG021DMXXT2", "BBG0026ZDHT8", "BBG021GNPBR6"),
        ("XZO", "BBG01XL8FHT0", "BBG01XL8FJS7", "BBG01227MF17"),
    ):
        case_key = f"share_class_adjudication:{ticker}"
        scope = _scope(exact, case_key)
        decision = ShareClassAdjudicationDecision(
            **common,
            observed_ticker=ticker,
            observed_composite_figi=composite,
            required_unique_canonical_composite_figi=composite,
            observed_share_class_figi=observed,
            canonical_share_class_figi=canonical,
            valid_from_session=scope.rows[0].session_date,
            valid_through_session=scope.rows[-1].session_date,
            scoped_source_record_ids=scope.source_record_ids,
            disposition=ShareClassAdjudicationDisposition.CONFIRMED_CORRECTION,
            decision_version=1,
            supersedes_share_class_adjudication_id=None,
            reason_code="frozen_share_class_correction",
            reason_detail="Frozen external hierarchy evidence supports this exact correction.",
        )
        rows.append(
            create_registry_decision_candidate(
                registry_name=registry_name,
                case_key=case_key,
                proposed_contract_row=decision.to_registry_row(),
                source_scope=scope,
            )
        )
    return tuple(rows)


def _build_cross_market_decisions(
    gate_c: _GateCSource,
    *,
    evidence: ExactArtifactBinding,
    evidence_document: Mapping[str, object],
    candidate_available: date,
) -> tuple[object, ...]:
    groups = {
        str(item["ticker"]): item
        for item in _array(evidence_document["groups"], "cross-market evidence groups")
    }
    if set(groups) != {key.split(":", 1)[1] for key in gate_c.scopes}:
        raise IdentityRegistryProductionError("Gate C/evidence group set differs")
    decisions = []
    for ticker in sorted(groups):
        group = groups[ticker]
        scope = gate_c.scopes[f"identity_cross_market_adjudication:{ticker}"]
        roles = gate_c.case_roles[ticker]
        linked = tuple(
            LinkedIdentityCase(
                identity_case_id=case_id,
                role=IdentityCaseResolutionRole(role),
            )
            for case_id, role in sorted(roles.items())
        )
        approved = ApprovedCrossMarketAdjudication(
            provider_id="massive",
            provider_market="stocks",
            provider_locale="us",
            ticker=ticker,
            share_class_figi=str(group["share_class_figi"]),
            observed_foreign_composite_figi=str(group["observed_foreign_composite_figi"]),
            disposition=CrossMarketAdjudicationDisposition.CONFIRMED_PROVIDER_CONTAMINATION,
            canonical_us_composite_figi=str(group["canonical_us_composite_figi"]),
            observed_composite_market_code=str(group["observed_foreign_market_code"]),
            canonical_composite_market_code="US",
            valid_from_session=date.fromisoformat(str(group["valid_from_session"])),
            valid_through_session=date.fromisoformat(str(group["valid_through_session"])),
            scoped_source_record_ids=scope.source_record_ids,
            source_s4_release_set_id=S7_S4_RELEASE_SET_ID,
            source_six_release_binding_id=gate_c.source_six_release_binding_id,
            source_market_consistency_candidate_manifest_id=gate_c.candidate.artifact_id,
            source_market_consistency_candidate_manifest_sha256=gate_c.candidate.sha256,
            candidate_available_session=candidate_available,
            source_external_evidence_manifest_id=evidence.artifact_id,
            source_external_evidence_manifest_sha256=evidence.sha256,
            external_evidence_available_session=evidence.available_session,
            approval_receipt_id=_PLACEHOLDER_DIGEST,
            approval_receipt_sha256=_PLACEHOLDER_DIGEST,
            approved_by="pending_immutable_registry_approval",
            approved_at_utc=datetime(2000, 1, 3, tzinfo=UTC),
            approval_available_session=date(2000, 1, 4),
            decision_version=1,
            supersedes_cross_market_adjudication_id=None,
            linked_identity_cases=linked,
            reason_code="same_share_class_non_us_composite_in_us_locale",
            reason_detail=(
                "Pinned US-locale lineage and external identifier evidence establish "
                "the non-US Composite observation."
            ),
        )
        row = _cross_market_row(
            approved,
            scope=scope,
            detector_preview_id=gate_c.detector_preview_id,
            detector_preview_sha256=gate_c.detector_preview_sha256,
            evidence_claim_digest=stable_digest(
                {"evidence_group": dict(group), "source_scope": scope.to_dict()}
            ),
        )
        decisions.append(
            create_registry_decision_candidate(
                registry_name=RegistryName.IDENTITY_CROSS_MARKET_ADJUDICATION.value,
                case_key=f"identity_cross_market_adjudication:{ticker}",
                proposed_contract_row=row,
                source_scope=scope,
            )
        )
    return tuple(decisions)


def _cross_market_row(
    item: ApprovedCrossMarketAdjudication,
    *,
    scope: ExactSourceScope,
    detector_preview_id: str,
    detector_preview_sha256: str,
    evidence_claim_digest: str,
) -> dict[str, object]:
    case_ids = [case.identity_case_id for case in item.linked_identity_cases]
    case_roles = {case.identity_case_id: case.role.value for case in item.linked_identity_cases}
    return {
        "cross_market_adjudication_id": item.cross_market_adjudication_id,
        "cross_market_series_id": item.cross_market_series_id,
        "decision_version": item.decision_version,
        "supersedes_cross_market_adjudication_id": item.supersedes_cross_market_adjudication_id,
        "cross_market_subject_id": item.cross_market_subject_id,
        "cross_market_scope_id": item.cross_market_scope_id,
        "provider_id": item.provider_id,
        "provider_market": item.provider_market,
        "provider_locale": item.provider_locale,
        "observed_ticker": item.ticker,
        "share_class_figi": item.share_class_figi,
        "observed_foreign_composite_figi": item.observed_foreign_composite_figi,
        "observed_composite_market_code": item.observed_composite_market_code,
        "canonical_us_composite_figi": item.canonical_us_composite_figi,
        "canonical_composite_market_code": item.canonical_composite_market_code,
        "canonical_asset_id": canonical_asset_id(str(item.canonical_us_composite_figi)),
        "valid_from_session": item.valid_from_session,
        "valid_through_session": item.valid_through_session,
        "scoped_source_record_count": len(scope.rows),
        "scoped_source_record_set_digest": scope.source_record_set_digest,
        "scoped_source_record_ids_json": _compact_json(list(scope.source_record_ids)),
        "related_identity_case_count": len(case_ids),
        "related_identity_case_ids_json": _compact_json(case_ids),
        "related_identity_case_roles_json": _compact_json(case_roles),
        "identity_disposition": item.disposition.value,
        "canonical_override": True,
        "identity_effect": "canonical_research_identity_only",
        "membership_effect": "none",
        "active_status_effect": "none",
        "identity_quality_liquidation_signal": False,
        "reason_code": item.reason_code,
        "reason_detail": item.reason_detail,
        "approval_status": "pending",
        "approval_request_event_id": _PLACEHOLDER_DIGEST,
        "approval_request_event_sha256": _PLACEHOLDER_DIGEST,
        "approval_receipt_id": _PLACEHOLDER_DIGEST,
        "approval_receipt_path": _PLACEHOLDER_PATH,
        "approval_receipt_sha256": _PLACEHOLDER_DIGEST,
        "approved_by": item.approved_by,
        "approved_at_utc": item.approved_at_utc,
        "approval_available_session": item.approval_available_session,
        "adjudication_available_session": item.adjudication_available_session,
        "source_six_release_binding_id": item.source_six_release_binding_id,
        "source_s4_release_set_id": item.source_s4_release_set_id,
        "source_identity_case_candidate_manifest_id": detector_preview_id,
        "source_identity_case_candidate_manifest_sha256": detector_preview_sha256,
        "source_identity_market_consistency_candidate_manifest_id": (
            item.source_market_consistency_candidate_manifest_id
        ),
        "source_identity_market_consistency_candidate_manifest_sha256": (
            item.source_market_consistency_candidate_manifest_sha256
        ),
        "candidate_available_session": item.candidate_available_session,
        "source_external_evidence_manifest_id": item.source_external_evidence_manifest_id,
        "source_external_evidence_manifest_sha256": item.source_external_evidence_manifest_sha256,
        "external_evidence_available_session": item.external_evidence_available_session,
        "evidence_claim_digest": evidence_claim_digest,
        "market_classification_rule_version": "s7_openfigi_cross_market_relationship_v1",
        "scope_match_rule_version": "s7_cross_market_exact_scope_match_v1",
        "cross_market_adjudication_id_rule_version": "s7_cross_market_adjudication_id_v1",
        "outcome_or_backtest_evidence_used": False,
        "availability_calendar_id": CALENDAR_ARTIFACT_ID,
        "availability_calendar_sha256": CALENDAR_ARTIFACT_SHA256,
        "availability_rule": "max_candidate_evidence_approval_first_xnys_open_v1",
    }


def _load_gate_c_source(root: Path, completion_ref: StoredControlDocument) -> _GateCSource:
    from ame_stocks_api.silver import identity_market_sequence as gate_c_module

    completion = _load_exact_json(root, completion_ref, "Gate C completion")
    plan_ref = _mapping(completion.get("plan"), "Gate C plan ref")
    authorization_ref = _mapping(completion.get("authorization"), "Gate C authorization ref")
    candidate_ref = _mapping(completion.get("candidate"), "Gate C candidate ref")
    plan_path = str(plan_ref["path"])
    plan, plan_sha = gate_c_module._load_canonical_json_file(
        gate_c_module._safe(root, plan_path), "Gate C plan"
    )
    if plan_sha != plan_ref["sha256"] or plan.get("plan_id") != plan_ref["plan_id"]:
        raise IdentityRegistryProductionError("Gate C plan binding differs")
    result = gate_c_module._candidate_from_completion(
        root,
        completion_relative=completion_ref.path,
        plan=plan,
        plan_path=plan_path,
        plan_id=str(plan_ref["plan_id"]),
        plan_sha256=str(plan_ref["sha256"]),
        authorization_path=str(authorization_ref["path"]),
        authorization_id=str(authorization_ref["authorization_id"]),
        authorization_sha256=str(authorization_ref["sha256"]),
        caps=S7MarketSequenceResourceCaps(
            **_mapping(plan["resource_caps"], "Gate C resource caps")
        ),
        idempotent=True,
    )
    if (
        result.completion_id != completion_ref.object_id
        or result.completion_sha256 != completion_ref.sha256
        or result.reviewed_foreign_row_count != REVIEWED_FOREIGN_ROW_COUNT
    ):
        raise IdentityRegistryProductionError("Gate C completion/count binding differs")
    candidate_content = _read_exact_receipt_bytes(root, candidate_ref, "Gate C candidate")
    candidate_document = _mapping(
        gate_c_module._decode_canonical_json(candidate_content, "Gate C candidate"),
        "Gate C candidate",
    )
    if (
        candidate_ref.get("candidate_id") != result.candidate_id
        or candidate_ref.get("path") != result.manifest_path
        or candidate_document.get("candidate_id") != result.candidate_id
        or candidate_document.get("manifest_id") != candidate_ref.get("manifest_id")
    ):
        raise IdentityRegistryProductionError("Gate C candidate exact binding differs")
    candidate_available = date.fromisoformat(
        str(
            _mapping(candidate_document["availability"], "Gate C availability")[
                "candidate_available_session"
            ]
        )
    )
    candidate_binding = ExactArtifactBinding(
        role="source_gate_c_candidate_manifest",
        artifact_id=result.candidate_id,
        path=result.manifest_path,
        sha256=str(candidate_ref["sha256"]),
        bytes=int(candidate_ref["bytes"]),
        available_session=candidate_available,
        embedded_id_field="candidate_id",
    )
    completion_binding = _binding(
        completion_ref,
        role="source_gate_c_completion_manifest",
        available_session=candidate_available,
    )
    outputs = _mapping(candidate_document.get("outputs"), "Gate C outputs")
    reviewed_ref = _mapping(
        outputs.get("reviewed_foreign_source_evidence"),
        "Gate C reviewed evidence ref",
    )
    if reviewed_ref.get("path") != result.reviewed_evidence_path:
        raise IdentityRegistryProductionError("Gate C reviewed evidence path differs")
    reviewed_content = _read_exact_receipt_bytes(
        root,
        reviewed_ref,
        "Gate C reviewed evidence",
    )
    try:
        table = pq.read_table(pa.BufferReader(reviewed_content))
    except (OSError, pa.ArrowException) as exc:
        raise IdentityRegistryProductionError("Gate C reviewed evidence is not Parquet") from exc
    if table.schema != REVIEWED_EVIDENCE_SCHEMA or table.num_rows != REVIEWED_FOREIGN_ROW_COUNT:
        raise IdentityRegistryProductionError("Gate C reviewed evidence schema/count differs")
    scopes: dict[str, ExactSourceScope] = {}
    roles_by_ticker: dict[str, dict[str, str]] = {}
    grouped: dict[str, list[ExactSourceRow]] = {}
    for row in table.to_pylist():
        ticker = str(row["ticker"])
        grouped.setdefault(ticker, []).append(
            ExactSourceRow(
                session_date=row["session_date"],
                source_record_id=str(row["selected_source_record_id"]),
                source_dataset="asset_observation_daily",
                source_s4_release_set_id=S7_S4_RELEASE_SET_ID,
                provider_id=str(row["provider"]),
                provider_market=str(row["market"]),
                provider_locale=str(row["locale"]),
                ticker=ticker,
                observed_composite_figi=str(row["observed_composite_figi"]),
                observed_share_class_figi=str(row["observed_share_class_figi"]),
                primary_exchange_mic=str(row["primary_exchange_mic"]),
            )
        )
        bindings = _mapping(
            json.loads(str(row["related_case_bindings_json"])), "Gate C case binding"
        )
        for relation in _array(bindings["related_cases"], "Gate C related cases"):
            case_id = str(relation["identity_case_id"])
            role = str(relation["identity_case_resolution_role"])
            previous = roles_by_ticker.setdefault(ticker, {}).get(case_id)
            if previous is not None and previous != role:
                raise IdentityRegistryProductionError(
                    "Gate C identity case has divergent semantic roles"
                )
            roles_by_ticker[ticker][case_id] = role
    for ticker, rows in grouped.items():
        scopes[f"identity_cross_market_adjudication:{ticker}"] = ExactSourceScope(
            tuple(sorted(rows))
        )
    refs = _mapping(candidate_document["registry_loader_source_refs"], "Gate C source refs")
    preview = _mapping(refs["detector_preview"], "detector preview ref")
    reviewed_evidence = _mapping(refs["reviewed_external_evidence"], "Gate C external evidence ref")
    return _GateCSource(
        candidate=candidate_binding,
        completion=completion_binding,
        source_six_release_binding_id=S7_SIX_RELEASE_BINDING_ID,
        detector_preview_id=str(preview["preview_artifact_id"]),
        detector_preview_sha256=str(preview["sha256"]),
        external_evidence_id=str(reviewed_evidence["manifest_id"]),
        external_evidence_sha256=str(reviewed_evidence["sha256"]),
        scopes=MappingProxyType(scopes),
        case_roles=MappingProxyType(
            {ticker: MappingProxyType(roles) for ticker, roles in roles_by_ticker.items()}
        ),
    )


def _pending_relation_controls(
    *,
    source_candidate: ExactArtifactBinding,
    evidence: ExactArtifactBinding,
    candidate_available: date,
) -> dict[str, object]:
    return {
        "provider_id": "massive",
        "provider_market": "stocks",
        "provider_locale": "us",
        "source_s4_release_set_id": S7_S4_RELEASE_SET_ID,
        "source_exact_group_candidate_manifest_id": source_candidate.artifact_id,
        "source_exact_group_candidate_manifest_sha256": source_candidate.sha256,
        "candidate_available_session": candidate_available,
        "source_external_evidence_manifest_id": evidence.artifact_id,
        "source_external_evidence_manifest_sha256": evidence.sha256,
        "external_evidence_available_session": evidence.available_session,
        "source_decision_plan_id": _PLACEHOLDER_DIGEST,
        "source_decision_plan_path": _PLACEHOLDER_PATH,
        "source_decision_plan_sha256": _PLACEHOLDER_DIGEST,
        "approval_request_event_id": _PLACEHOLDER_DIGEST,
        "approval_request_event_sha256": _PLACEHOLDER_DIGEST,
        "approval_receipt_id": _PLACEHOLDER_DIGEST,
        "approval_receipt_sha256": _PLACEHOLDER_DIGEST,
        "approved_by": "pending_immutable_registry_approval",
        "approved_at_utc": datetime(2000, 1, 3, tzinfo=UTC),
        "approval_available_session": date(2000, 1, 4),
        "availability_calendar_id": CALENDAR_ARTIFACT_ID,
        "availability_calendar_sha256": CALENDAR_ARTIFACT_SHA256,
    }


def _scope(exact: LoadedExactGroupRegistryScopes, case_key: str) -> ExactSourceScope:
    return ExactSourceScope.from_dict(exact.require_scope(case_key).to_dict())


def _exact_group_completion_time(root: Path, ref: StoredControlDocument) -> datetime:
    document = _load_exact_json(root, ref, "exact-group completion")
    value = str(document["completed_at_utc"])
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def _load_external_evidence(
    root: Path,
    ref: StoredControlDocument,
    *,
    expected_type: str,
) -> Mapping[str, object]:
    content = _read_exact_receipt_bytes(root, ref.to_dict(), "external evidence")
    document = _decode_json_no_duplicates(content, "external evidence")
    canonical = (
        json.dumps(
            document,
            allow_nan=False,
            ensure_ascii=True,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")
    if canonical != content:
        raise IdentityRegistryProductionError(
            "external evidence manifest is not canonical pretty JSON"
        )
    payload = dict(document)
    manifest_id = payload.pop("manifest_id", None)
    if (
        manifest_id != ref.object_id
        or stable_digest(payload) != ref.object_id
        or document.get("manifest_type") != expected_type
        or document.get("manifest_status") != "candidate_not_approved"
    ):
        raise IdentityRegistryProductionError("external evidence identity/type differs")
    artifacts, _ = _validate_external_evidence_manifest_envelope(
        document,
        expected_type=expected_type,
    )
    for index, artifact in enumerate(artifacts):
        _read_exact_receipt_bytes(
            root,
            artifact,
            f"external evidence raw artifact {index}",
        )
    return document


def _validate_external_evidence_manifest_envelope(
    document: Mapping[str, object],
    *,
    expected_type: str,
) -> tuple[list[Mapping[str, object]], date]:
    capabilities = document.get("non_executable_capabilities")
    if expected_type == "identity_exact_group_external_evidence":
        if (
            not isinstance(capabilities, Mapping)
            or not capabilities
            or any(value is not False for value in capabilities.values())
        ):
            raise IdentityRegistryProductionError(
                "exact-group external evidence capabilities are executable"
            )
    elif capabilities is not None and (
        not isinstance(capabilities, Mapping)
        or not capabilities
        or any(value is not False for value in capabilities.values())
    ):
        raise IdentityRegistryProductionError(
            "cross-market external evidence capabilities are executable"
        )
    artifacts = _array(document.get("artifacts"), "external evidence artifacts")
    if not artifacts:
        raise IdentityRegistryProductionError("external evidence artifacts are empty")
    paths: set[str] = set()
    sessions: list[date] = []
    for artifact in artifacts:
        path = artifact.get("path")
        if not isinstance(path, str) or path in paths:
            raise IdentityRegistryProductionError(
                "external evidence artifact paths are invalid or repeated"
            )
        paths.add(path)
        if artifact.get("content_scope") not in {
            "exact_raw_bytes",
            "allowlisted_response_headers_without_cookie_or_credentials",
        }:
            raise IdentityRegistryProductionError(
                "external evidence artifact content scope is unsafe"
            )
        try:
            sessions.append(date.fromisoformat(str(artifact["source_available_session"])))
        except (KeyError, ValueError) as exc:
            raise IdentityRegistryProductionError(
                "external evidence artifact availability is invalid"
            ) from exc
    availability = _mapping(document.get("availability"), "evidence availability")
    try:
        manifest_available = date.fromisoformat(str(availability["available_session"]))
    except (KeyError, ValueError) as exc:
        raise IdentityRegistryProductionError(
            "external evidence manifest availability is invalid"
        ) from exc
    if manifest_available != max(sessions):
        raise IdentityRegistryProductionError(
            "external evidence availability differs from raw artifact receipts"
        )
    return artifacts, manifest_available


def _load_fixed_evidence_import_receipt(
    root: Path,
    relative_path: str,
    *,
    expected_type: str,
    expected_manifest: StoredControlDocument,
    expected_raw_refs: tuple[Mapping[str, object], ...],
    expected_runtime: RegistryRuntimeBinding,
    expected_available_session: date | None,
    revalidate_runtime: bool,
) -> ExactArtifactBinding | None:
    try:
        path = safe_relative_path(root, relative_path)
    except ArtifactError as exc:
        raise IdentityRegistryProductionError("evidence import receipt path is unsafe") from exc
    if not path.exists():
        return None
    if path.is_symlink() or not path.is_file():
        raise IdentityRegistryProductionError("evidence import receipt is unsafe")
    content = _read_regular_file_snapshot(path, "evidence import receipt")
    document = _decode_json_no_duplicates(content, "evidence import receipt")
    expected_keys = {
        "artifact_type",
        "artifact_version",
        "evidence_type",
        "import_available_session",
        "import_id",
        "import_slot_id",
        "imported_at_utc",
        "manifest",
        "production_data_root",
        "raw_artifacts",
        "runtime_binding",
    }
    if set(document) != expected_keys or _canonical_control_bytes(document) != content:
        raise IdentityRegistryProductionError("evidence import receipt shape changed")
    logical = {key: value for key, value in document.items() if key != "import_id"}
    manifest = StoredControlDocument.from_dict(document["manifest"])
    runtime = RegistryRuntimeBinding.from_dict(document["runtime_binding"])
    raw_refs = tuple(_array(document["raw_artifacts"], "imported raw artifacts"))
    imported_at = _parse_utc(str(document["imported_at_utc"]))
    available = date.fromisoformat(str(document["import_available_session"]))
    try:
        fixed_manifest_path = _EVIDENCE_REPO_MANIFESTS[expected_type]
    except KeyError as exc:
        raise IdentityRegistryProductionError("unsupported fixed evidence package") from exc
    if manifest.path != fixed_manifest_path:
        raise IdentityRegistryProductionError(
            "evidence import manifest path differs from the fixed repository package"
        )
    repo_root = Path(__file__).resolve().parents[3]
    imported_manifest_bytes = _read_exact_receipt_bytes(
        root,
        manifest.to_dict(),
        "imported evidence manifest",
    )
    if imported_manifest_bytes != _read_git_blob(
        repo_root,
        runtime.git_commit,
        fixed_manifest_path,
    ):
        raise IdentityRegistryProductionError(
            "imported evidence manifest differs from its fixed Git bytes"
        )
    slot_id = stable_digest(
        {
            "evidence_type": expected_type,
            "manifest": manifest.to_dict(),
            "production_data_root": root.as_posix(),
            "runtime_binding_id": runtime.runtime_binding_id,
            "version": EVIDENCE_PACKAGE_IMPORT_VERSION,
        }
    )
    loaded_manifest = _load_external_evidence(
        root,
        manifest,
        expected_type=expected_type,
    )
    manifest_raw_refs = tuple(
        {
            "bytes": artifact["bytes"],
            "path": artifact["path"],
            "sha256": artifact["sha256"],
        }
        for artifact in _array(
            loaded_manifest["artifacts"],
            "imported evidence manifest artifacts",
        )
    )
    for index, artifact in enumerate(
        _array(loaded_manifest["artifacts"], "imported evidence manifest artifacts")
    ):
        raw_path = artifact.get("path")
        if not isinstance(raw_path, str) or not raw_path.startswith(_EVIDENCE_ALLOWED_PREFIXES):
            raise IdentityRegistryProductionError(
                "imported evidence raw path escaped the fixed repository packages"
            )
        imported_raw_bytes = _read_exact_receipt_bytes(
            root,
            artifact,
            f"imported evidence Git raw artifact {index}",
        )
        if imported_raw_bytes != _read_git_blob(
            repo_root,
            runtime.git_commit,
            raw_path,
        ):
            raise IdentityRegistryProductionError(
                f"imported evidence raw artifact {index} differs from its fixed Git bytes"
            )
    evidence_available = date.fromisoformat(
        str(_mapping(loaded_manifest["availability"], "evidence availability")["available_session"])
    )
    calendar = load_xnys_calendar_artifact(
        root,
        calendar_artifact_id=CALENDAR_ARTIFACT_ID,
        expected_sha256=CALENDAR_ARTIFACT_SHA256,
    )
    recomputed_available = max(
        evidence_available,
        calendar.first_open_after(imported_at)[0],
    )
    if (
        document["artifact_type"] != "s7_fixed_external_evidence_import_receipt"
        or document["artifact_version"] != EVIDENCE_PACKAGE_IMPORT_VERSION
        or document["evidence_type"] != expected_type
        or document["import_id"] != stable_digest(logical)
        or document["import_slot_id"] != slot_id
        or document["production_data_root"] != root.as_posix()
        or manifest != expected_manifest
        or raw_refs != manifest_raw_refs
        or raw_refs != expected_raw_refs
        or runtime != expected_runtime
        or available != recomputed_available
        or (expected_available_session is not None and available != expected_available_session)
        or relative_path
        != (
            "manifests/silver/identity/external-evidence-imports/"
            f"evidence_type={expected_type}/slot_id={slot_id}/receipt.json"
        )
    ):
        raise IdentityRegistryProductionError("evidence import receipt binding changed")
    for index, raw in enumerate(raw_refs):
        _read_exact_receipt_bytes(root, raw, f"import receipt raw artifact {index}")
    binding = ExactArtifactBinding(
        role="external_evidence_import_receipt",
        artifact_id=str(document["import_id"]),
        path=relative_path,
        sha256=hashlib.sha256(content).hexdigest(),
        bytes=len(content),
        available_session=available,
        embedded_id_field="import_id",
    )
    if revalidate_runtime:
        require_current_registry_runtime_binding(runtime)
    return binding


def _replay_bound_evidence_import(
    root: Path,
    binding: ExactArtifactBinding,
    *,
    expected_type: str,
    expected_manifest: StoredControlDocument,
    expected_runtime: RegistryRuntimeBinding,
) -> None:
    if (
        binding.role != "external_evidence_import_receipt"
        or binding.embedded_id_field != "import_id"
    ):
        raise IdentityRegistryProductionError("evidence import binding role changed")
    content = _read_exact_receipt_bytes(root, binding.to_dict(), "evidence import receipt")
    document = _decode_json_no_duplicates(content, "evidence import receipt")
    raw_refs = tuple(_array(document.get("raw_artifacts"), "imported raw artifacts"))
    replayed = _load_fixed_evidence_import_receipt(
        root,
        binding.path,
        expected_type=expected_type,
        expected_manifest=expected_manifest,
        expected_raw_refs=raw_refs,
        expected_runtime=expected_runtime,
        expected_available_session=binding.available_session,
        revalidate_runtime=True,
    )
    if replayed != binding:
        raise IdentityRegistryProductionError("evidence import binding failed exact replay")


def _read_git_blob(repo_root: Path, commit: str, relative_path: str) -> bytes:
    if not relative_path.startswith(_EVIDENCE_ALLOWED_PREFIXES):
        raise IdentityRegistryProductionError("fixed Git evidence path is outside package")
    try:
        safe_relative_path(repo_root, relative_path)
    except ArtifactError as exc:
        raise IdentityRegistryProductionError("fixed Git evidence path is unsafe") from exc
    result = subprocess.run(
        ("git", "-C", str(repo_root), "show", f"{commit}:{relative_path}"),
        check=False,
        capture_output=True,
    )
    if result.returncode != 0:
        raise IdentityRegistryProductionError(
            f"fixed evidence blob is absent from runtime Git tree: {relative_path}"
        )
    return result.stdout


def _read_regular_file_snapshot(path: Path, label: str) -> bytes:
    try:
        flags = os.O_RDONLY
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise IdentityRegistryProductionError(f"{label} is missing or unsafe") from exc
    try:
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode):
            raise IdentityRegistryProductionError(f"{label} is not a regular file")
        with os.fdopen(descriptor, "rb", closefd=False) as handle:
            content = handle.read(opened.st_size + 1)
        try:
            visible = os.stat(path, follow_symlinks=False)
        except OSError as exc:
            raise IdentityRegistryProductionError(f"{label} path changed during read") from exc
        if len(content) != opened.st_size or (opened.st_dev, opened.st_ino) != (
            visible.st_dev,
            visible.st_ino,
        ):
            raise IdentityRegistryProductionError(f"{label} changed during read")
    finally:
        os.close(descriptor)
    return content


def _read_exact_receipt_bytes(
    root: Path,
    receipt: Mapping[str, object],
    label: str,
) -> bytes:
    relative = receipt.get("path")
    expected_bytes = receipt.get("bytes")
    expected_sha256 = receipt.get("sha256")
    if (
        not isinstance(relative, str)
        or type(expected_bytes) is not int
        or expected_bytes < 0
        or not isinstance(expected_sha256, str)
        or len(expected_sha256) != 64
    ):
        raise IdentityRegistryProductionError(f"{label} receipt is malformed")
    try:
        path = safe_relative_path(root, relative)
        flags = os.O_RDONLY
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = os.open(path, flags)
    except (ArtifactError, OSError) as exc:
        raise IdentityRegistryProductionError(f"{label} is missing or unsafe") from exc
    try:
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode) or opened.st_size != expected_bytes:
            raise IdentityRegistryProductionError(f"{label} bytes/type differs")
        with os.fdopen(descriptor, "rb", closefd=False) as handle:
            content = handle.read(expected_bytes + 1)
        try:
            visible = os.stat(path, follow_symlinks=False)
        except OSError as exc:
            raise IdentityRegistryProductionError(f"{label} path changed during read") from exc
        if (opened.st_dev, opened.st_ino) != (visible.st_dev, visible.st_ino):
            raise IdentityRegistryProductionError(f"{label} path changed during read")
    finally:
        os.close(descriptor)
    if len(content) != expected_bytes or hashlib.sha256(content).hexdigest() != expected_sha256:
        raise IdentityRegistryProductionError(f"{label} bytes/hash differs")
    return content


def _load_exact_json(root: Path, ref: StoredControlDocument, label: str) -> Mapping[str, object]:
    content = _read_exact_receipt_bytes(root, ref.to_dict(), label)
    return _decode_json_no_duplicates(content, label)


def _binding(
    ref: StoredControlDocument,
    *,
    role: str,
    available_session: date,
) -> ExactArtifactBinding:
    embedded = {
        "source_exact_group_candidate_manifest": "candidate_id",
        "source_exact_group_completion_manifest": "completion_id",
        "source_gate_c_completion_manifest": "completion_id",
        "external_evidence": "manifest_id",
    }[role]
    return ExactArtifactBinding(
        role=role,
        artifact_id=ref.object_id,
        path=ref.path,
        sha256=ref.sha256,
        bytes=ref.bytes,
        available_session=available_session,
        embedded_id_field=embedded,
    )


def _pin_mapping(ref: StoredControlDocument) -> dict[str, object]:
    return {
        "artifact_id": ref.object_id,
        "path": ref.path,
        "sha256": ref.sha256,
        "bytes": ref.bytes,
    }


def _stored(binding: ExactArtifactBinding) -> StoredControlDocument:
    return StoredControlDocument(
        object_id=binding.artifact_id,
        path=binding.path,
        sha256=binding.sha256,
        bytes=binding.bytes,
    )


def _mapping(value: object, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or not all(isinstance(key, str) for key in value):
        raise IdentityRegistryProductionError(f"{label} must be an object")
    return value


def _array(value: object, label: str) -> list[Mapping[str, object]]:
    if not isinstance(value, list) or not all(isinstance(item, Mapping) for item in value):
        raise IdentityRegistryProductionError(f"{label} must be an object array")
    return list(value)


def _compact_json(value: object) -> str:
    return json.dumps(
        value, allow_nan=False, ensure_ascii=True, separators=(",", ":"), sort_keys=True
    )


def _canonical_control_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _decode_json_no_duplicates(content: bytes, label: str) -> Mapping[str, object]:
    def object_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise IdentityRegistryProductionError(f"{label} contains duplicate JSON keys")
            result[key] = value
        return result

    try:
        return _mapping(
            json.loads(content, object_pairs_hook=object_pairs),
            label,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise IdentityRegistryProductionError(f"{label} is not JSON") from exc


def _parse_utc(value: str) -> datetime:
    if not value.endswith("Z"):
        raise IdentityRegistryProductionError("UTC timestamp must end in Z")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise IdentityRegistryProductionError("UTC timestamp is invalid") from exc
    if parsed.tzinfo != UTC:
        raise IdentityRegistryProductionError("timestamp is not UTC")
    return parsed


def _utc_text(value: datetime) -> str:
    if value.tzinfo != UTC:
        raise IdentityRegistryProductionError("timestamp is not exact UTC")
    return value.isoformat().replace("+00:00", "Z")


def _utc_now() -> datetime:
    return datetime.now(UTC)


__all__ = [
    "EVIDENCE_PACKAGE_IMPORT_VERSION",
    "PRODUCTION_INGRESS_VERSION",
    "PRODUCTION_PREPARATION_ORDER",
    "IdentityRegistryProductionError",
    "ImportedExternalEvidencePackage",
    "PreparedProductionRegistryRequest",
    "import_fixed_external_evidence_package",
    "prepare_fixed_production_request",
]
