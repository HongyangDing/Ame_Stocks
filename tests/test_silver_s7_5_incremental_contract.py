from __future__ import annotations

import json
from dataclasses import replace
from datetime import date
from pathlib import Path

import pytest

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver import incremental_contract as incremental_contract_module
from ame_stocks_api.silver.incremental_contract import (
    CHECKPOINT_RECEIPT_RULE_VERSION,
    ArtifactPin,
    CheckpointReceipt,
    ContentAttestedRelease,
    ControlObjectKind,
    ControlObjectPin,
    ControlValidatedCandidate,
    IncrementalContractError,
    IncrementalReleaseManifest,
    ManifestPin,
    PartitionReceipt,
    PartitionReplacement,
    ReleaseType,
    RowSemanticProofReceipt,
    RowVersionOperation,
    RowVersionReceipt,
    RowVersionReference,
    RunReceipt,
    RunSpec,
    ViewKind,
    checkpoint_rebuild_basis_from_change_digest,
    control_object_pin,
    correction_scope_digest,
    input_set_digest,
    logical_change_set_digest,
    release_change_set_digest,
    replace_runtime_observation,
    validate_release_projection,
    verify_content_attested_release,
    verify_control_validated_candidate,
)
from ame_stocks_api.silver.incremental_gate import (
    CORRECTION_AUTHORIZATION_LITERAL_VERSION,
    CorrectionAuthorization,
    CorrectionAuthorizedAction,
    GateArtifactPin,
    GateEvidencePin,
    PinnedCorrectionAuthorization,
    QaCheckPolicy,
    QaCheckResult,
    QaPolicy,
    QaReceipt,
    QaSeverity,
)
from ame_stocks_api.silver.incremental_identity import (
    ALIAS_RESOLUTION_VERSION_ID_SUBJECT_FIELD_ORDER,
    ALIAS_RESOLUTION_VERSION_ID_SUBJECT_FIELDS,
    ALIAS_SEGMENT_ID_SUBJECT_FIELD_ORDER,
    ALIAS_SEGMENT_ID_SUBJECT_FIELDS,
    AliasResolutionDisposition,
    AliasResolutionMethod,
    ShareClassResolutionMethod,
)
from ame_stocks_api.silver.incremental_resolver import (
    IncrementalResolutionError,
    _resolve_manifest_snapshot,
    resolve_incremental_snapshot,
    validate_release_content_attestation,
)

_SOURCE_SESSION = date(2026, 7, 29)
_AVAILABLE_SESSION = date(2026, 8, 3)
_LATER_SESSION = date(2026, 8, 4)
_NEXT_AVAILABLE_SESSION = date(2026, 8, 5)
_NEXT_SOURCE_SESSION = date(2026, 7, 30)
_ROOT = Path(__file__).resolve().parents[1]
_CANDIDATE_PATH = (
    _ROOT / "docs/silver/contracts/control/s7_5_incremental_contract_bundle-v1.candidate.json"
)


def _digest(label: str) -> str:
    return stable_digest({"fixture": label})


def _artifact(label: str, *, path: str | None = None) -> ArtifactPin:
    return ArtifactPin(
        path=path or f"artifacts/{label}.json",
        sha256=_digest(label),
        bytes=100 + len(label),
    )


def _parent_pin() -> ManifestPin:
    return ManifestPin(
        release_id=_digest("parent-release"),
        manifest_path="releases/parent/manifest.json",
        manifest_sha256=_digest("parent-manifest"),
        manifest_bytes=901,
        release_available_session=_AVAILABLE_SESSION,
    )


def _partition(
    *,
    receipt_label: str = "partition",
    availability: date = _AVAILABLE_SESSION,
    reference_id: str | None = None,
    partition_session: date = _SOURCE_SESSION,
) -> PartitionReceipt:
    references = (
        () if reference_id is None else (RowVersionReference("ticker_alias", reference_id),)
    )
    return PartitionReceipt(
        table_name="universe_daily",
        partition_key=partition_session.isoformat(),
        receipt=_artifact(receipt_label, path=f"silver/{receipt_label}.parquet"),
        row_count=42,
        schema_digest=_digest("universe-schema"),
        availability_session=availability,
        row_version_references=references,
    )


def _row(
    *,
    version_label: str = "alias-version-v1",
    stable_label: str = "alias-segment",
    operation: RowVersionOperation = RowVersionOperation.NEW_ROOT,
    predecessor: str | None = None,
    availability: date = _AVAILABLE_SESSION,
    tombstone_reason: str | None = None,
) -> RowVersionReceipt:
    row_payload_digest = _digest(f"payload-{version_label}")
    semantic_proof = RowSemanticProofReceipt(
        table_name="ticker_alias",
        stable_row_key=_digest(stable_label),
        row_version_id=_digest(version_label),
        predecessor_row_version_id=predecessor,
        operation=operation,
        row_payload_digest=row_payload_digest,
        predecessor_payload_digest=(
            None if predecessor is None else _digest(f"predecessor-{version_label}")
        ),
        validator_semantics_digest=_digest("transform"),
        artifact=_artifact(
            f"semantic-proof-{version_label}",
            path=f"silver/proofs/{version_label}.json",
        ),
    )
    return RowVersionReceipt(
        table_name="ticker_alias",
        stable_row_key=_digest(stable_label),
        row_version_id=_digest(version_label),
        predecessor_row_version_id=predecessor,
        operation=operation,
        availability_session=availability,
        index_artifact=_artifact(
            f"index-{version_label}",
            path=f"silver/index/{version_label}.json",
        ),
        row_locator=f"rows/{version_label}",
        row_payload_digest=row_payload_digest,
        semantic_proof=semantic_proof,
        tombstone_reason=tombstone_reason,
    )


def _qa_policy() -> QaPolicy:
    return QaPolicy(
        checks=(
            QaCheckPolicy(
                "partition_session_calendar_contiguous",
                QaSeverity.HIGH,
                _digest("qa-calendar-semantics"),
                0,
            ),
            QaCheckPolicy(
                "row_semantic_proof_complete",
                QaSeverity.CRITICAL,
                _digest("qa-row-proof-semantics"),
                0,
            ),
        )
    )


def _gate_artifact(label: str) -> GateArtifactPin:
    return GateArtifactPin(
        path=f"evidence/{label}.json",
        sha256=_digest(f"evidence-{label}"),
        bytes=100 + len(label),
    )


def _gate_evidence(label: str) -> GateEvidencePin:
    return GateEvidencePin(
        artifact=_gate_artifact(label),
        available_session=_AVAILABLE_SESSION,
    )


def _correction_authorization(
    *,
    parent_release: ContentAttestedRelease,
    change_set_digest: str,
    source_binding_digest: str,
    identity_policy_after_id: str,
) -> tuple[str, PinnedCorrectionAuthorization]:
    scope = correction_scope_digest(
        parent_release_id=parent_release.manifest_pin.release_id,
        change_set_digest=change_set_digest,
    )
    authorization = CorrectionAuthorization(
        authorized_action=CorrectionAuthorizedAction.PUBLISH_EXACT_CORRECTION,
        literal_version=CORRECTION_AUTHORIZATION_LITERAL_VERSION,
        parent_release_id=parent_release.manifest_pin.release_id,
        expected_change_set_digest=change_set_digest,
        source_binding_digest=source_binding_digest,
        schema_digest=_digest("schema"),
        transform_semantics_digest=_digest("transform"),
        calendar_digest=_digest("calendar"),
        identity_policy_before_id=parent_release.manifest.identity_policy_bundle_id,
        identity_policy_after_id=identity_policy_after_id,
        scope_digest=scope,
        approval_event_id=_digest("approval-event"),
        approval_event_sha256=_digest("approval-event-bytes"),
        approver_id="joe",
        approval_available_session=_AVAILABLE_SESSION,
        evidence_pins=(_gate_evidence("evidence"),),
    )
    return scope, PinnedCorrectionAuthorization.freeze(
        authorization,
        path="approvals/correction.json",
    )


def _spec(
    *,
    release_type: ReleaseType = ReleaseType.BASE,
    parent: ManifestPin | None = None,
    parent_identity_policy_bundle_id: str | None = None,
    expected_change_set_digest: str | None = None,
    correction_scope: str | None = None,
    authorization: PinnedCorrectionAuthorization | None = None,
    availability_cutoff_session: date = _AVAILABLE_SESSION,
    release_available_session: date = _AVAILABLE_SESSION,
    source_cutoff_session: date = _SOURCE_SESSION,
    rss_cap_bytes: int = 2 * 1024**3,
    wall_clock_cap_seconds: int | None = None,
) -> RunSpec:
    inputs = (
        _artifact("calendar", path="inputs/calendar.json"),
        _artifact("source", path="inputs/source.json"),
    )
    return RunSpec(
        release_type=release_type,
        parent_release_pin=parent,
        parent_identity_policy_bundle_id=parent_identity_policy_bundle_id,
        resolved_view=ViewKind.LATEST_REVIEWED_RESEARCH,
        source_binding_digest=input_set_digest(inputs),
        source_cutoff_session=source_cutoff_session,
        availability_cutoff_session=availability_cutoff_session,
        release_available_session=release_available_session,
        schema_digest=_digest("schema"),
        transform_semantics_digest=_digest("transform"),
        identity_policy_bundle_id=_digest("policy"),
        calendar_digest=_digest("calendar"),
        input_pins=inputs,
        expected_change_set_digest=expected_change_set_digest or _digest("expected-change"),
        qa_policy=_qa_policy(),
        correction_scope_digest=correction_scope,
        correction_authorization=authorization,
        rss_cap_bytes=rss_cap_bytes,
        disk_floor_bytes=40 * 1024**3,
        wall_clock_cap_seconds=wall_clock_cap_seconds,
    )


def _projection(
    *,
    release_type: ReleaseType = ReleaseType.BASE,
    parent_release: ContentAttestedRelease | None = None,
    authorization: PinnedCorrectionAuthorization | None = None,
    added_partitions: tuple[PartitionReceipt, ...] | None = None,
    replacements: tuple[PartitionReplacement, ...] = (),
    rows: tuple[RowVersionReceipt, ...] | None = None,
    rss_cap_bytes: int = 2 * 1024**3,
    release_available_session: date = _AVAILABLE_SESSION,
    source_cutoff_session: date = _SOURCE_SESSION,
) -> tuple[RunSpec, RunReceipt, IncrementalReleaseManifest, ManifestPin]:
    selected_rows = () if rows is None else rows
    selected_partitions = (
        (_partition(partition_session=source_cutoff_session),)
        if added_partitions is None
        else added_partitions
    )
    superseded = tuple(
        sorted(
            item.predecessor_row_version_id
            for item in selected_rows
            if item.predecessor_row_version_id is not None
        )
    )
    change_digest = logical_change_set_digest(
        added_partition_receipts=selected_partitions,
        partition_replacements=replacements,
        added_row_version_receipts=selected_rows,
        superseded_row_version_ids=superseded,
    )
    parent = parent_release.manifest_pin if parent_release is not None else None
    selected_availability_cutoff = (
        max(_AVAILABLE_SESSION, parent.release_available_session)
        if parent is not None
        else _AVAILABLE_SESSION
    )
    selected_release_availability = max(
        release_available_session,
        selected_availability_cutoff,
    )
    inputs = (
        _artifact("calendar", path="inputs/calendar.json"),
        _artifact("source", path="inputs/source.json"),
    )
    source_binding = input_set_digest(inputs)
    correction_scope = None
    selected_authorization = authorization
    if release_type is ReleaseType.CORRECTION and selected_authorization is None:
        if parent_release is None:
            raise AssertionError("correction fixture requires a validated parent")
        correction_scope, selected_authorization = _correction_authorization(
            parent_release=parent_release,
            change_set_digest=change_digest,
            source_binding_digest=source_binding,
            identity_policy_after_id=_digest("policy"),
        )
    elif selected_authorization is not None:
        correction_scope = selected_authorization.authorization.scope_digest
    spec = _spec(
        release_type=release_type,
        parent=parent,
        parent_identity_policy_bundle_id=(
            parent_release.manifest.identity_policy_bundle_id
            if parent_release is not None
            else None
        ),
        expected_change_set_digest=change_digest,
        correction_scope=correction_scope,
        authorization=selected_authorization,
        availability_cutoff_session=selected_availability_cutoff,
        release_available_session=selected_release_availability,
        source_cutoff_session=source_cutoff_session,
        rss_cap_bytes=rss_cap_bytes,
    )
    resolved_content_digest = _digest("resolved-content")
    checkpoint = CheckpointReceipt(
        artifact=_artifact("checkpoint", path="checkpoints/state.json"),
        parent_release_id=parent.release_id if parent is not None else None,
        run_spec_id=spec.run_spec_id,
        last_session=source_cutoff_session,
        resolved_content_digest=resolved_content_digest,
        rebuild_basis_digest=checkpoint_rebuild_basis_from_change_digest(
            spec,
            change_set_digest=change_digest,
        ),
    )
    receipt = RunReceipt(
        run_spec_id=spec.run_spec_id,
        actual_input_set_digest=spec.source_binding_digest,
        output_set_digest=change_digest,
        qa_receipt=QaReceipt(
            qa_policy_id=spec.qa_policy.qa_policy_id,
            run_spec_id=spec.run_spec_id,
            source_binding_digest=spec.source_binding_digest,
            change_set_digest=change_digest,
            qa_available_session=selected_availability_cutoff,
            results=tuple(
                QaCheckResult(
                    check_id=item.check_id,
                    semantics_digest=item.semantics_digest,
                    observed_count=(
                        len(selected_partitions) + len(replacements)
                        if item.check_id == "partition_session_calendar_contiguous"
                        else len(selected_rows)
                    ),
                    failure_count=0,
                    details_artifact=_gate_artifact(f"qa-{item.check_id}"),
                )
                for item in spec.qa_policy.checks
            ),
        ),
        checkpoint=checkpoint,
        succeeded=True,
        error_codes=(),
        receipt_available_session=selected_availability_cutoff,
        runtime_seconds=123.5,
        peak_rss_bytes=900_000_000,
        minimum_free_disk_bytes=80 * 1024**3,
    )
    manifest = IncrementalReleaseManifest(
        release_type=release_type,
        parent_release_pin=parent,
        resolved_view=spec.resolved_view,
        schema_digest=spec.schema_digest,
        transform_semantics_digest=spec.transform_semantics_digest,
        identity_policy_bundle_id=spec.identity_policy_bundle_id,
        calendar_digest=spec.calendar_digest,
        source_binding_digest=spec.source_binding_digest,
        source_cutoff_session=spec.source_cutoff_session,
        availability_cutoff_session=spec.availability_cutoff_session,
        release_available_session=spec.release_available_session,
        added_partition_receipts=selected_partitions,
        partition_replacements=replacements,
        added_row_version_receipts=selected_rows,
        superseded_row_version_ids=superseded,
        resolved_content_digest=resolved_content_digest,
        qa_policy_id=spec.qa_policy.qa_policy_id,
        qa_receipt_id=receipt.qa_receipt.qa_receipt_id,
        correction_authorization_id=(
            selected_authorization.authorization.authorization_id
            if selected_authorization is not None
            else None
        ),
        run_spec_pin=control_object_pin(spec, path="control/run-spec.json"),
        run_receipt_pin=control_object_pin(receipt, path="control/run-receipt.json"),
    )
    pin = manifest.exact_pin(manifest_path="releases/current/manifest.json")
    return spec, receipt, manifest, pin


def _validated_base(*, rss_cap_bytes: int = 2 * 1024**3) -> ContentAttestedRelease:
    spec, receipt, manifest, pin = _attested_projection(rss_cap_bytes=rss_cap_bytes)
    candidate = validate_release_projection(
        spec,
        receipt,
        manifest,
        manifest_pin=pin,
        parent_release=None,
    )
    return validate_release_content_attestation(
        candidate,
        load_parent=lambda parent_pin: None,
    )


def _attested_projection(
    *,
    rss_cap_bytes: int = 2 * 1024**3,
) -> tuple[RunSpec, RunReceipt, IncrementalReleaseManifest, ManifestPin]:
    spec, receipt, provisional, provisional_pin = _projection(
        release_available_session=_LATER_SESSION,
        rss_cap_bytes=rss_cap_bytes,
    )
    _ = validate_release_projection(
        spec,
        receipt,
        provisional,
        manifest_pin=provisional_pin,
        parent_release=None,
    )
    provisional_snapshot = _resolve_manifest_snapshot(
        provisional_pin,
        view_kind=provisional.resolved_view,
        cutoff_session=provisional.release_available_session,
        load_parent=(lambda pin: provisional if pin == provisional_pin else None),
    )
    checkpoint = replace(
        receipt.checkpoint,
        resolved_content_digest=provisional_snapshot.resolved_content_digest,
    )
    attested_receipt = replace(receipt, checkpoint=checkpoint)
    attested_manifest = replace(
        provisional,
        resolved_content_digest=provisional_snapshot.resolved_content_digest,
        run_receipt_pin=control_object_pin(
            attested_receipt,
            path=provisional.run_receipt_pin.artifact.path,
        ),
    )
    attested_pin = attested_manifest.exact_pin(manifest_path=provisional_pin.manifest_path)
    return spec, attested_receipt, attested_manifest, attested_pin


def _validated_delta(parent: ContentAttestedRelease) -> ContentAttestedRelease:
    spec, receipt, provisional, provisional_pin = _projection(
        release_type=ReleaseType.DELTA,
        parent_release=parent,
        release_available_session=_NEXT_AVAILABLE_SESSION,
        source_cutoff_session=_NEXT_SOURCE_SESSION,
    )
    _ = validate_release_projection(
        spec,
        receipt,
        provisional,
        manifest_pin=provisional_pin,
        parent_release=parent,
    )
    provisional_snapshot = _resolve_manifest_snapshot(
        provisional_pin,
        view_kind=provisional.resolved_view,
        cutoff_session=provisional.release_available_session,
        load_parent=lambda pin: (
            provisional
            if pin == provisional_pin
            else parent.manifest
            if pin == parent.manifest_pin
            else None
        ),
    )
    attested_receipt = replace(
        receipt,
        checkpoint=replace(
            receipt.checkpoint,
            resolved_content_digest=provisional_snapshot.resolved_content_digest,
        ),
    )
    attested_manifest = replace(
        provisional,
        resolved_content_digest=provisional_snapshot.resolved_content_digest,
        run_receipt_pin=control_object_pin(
            attested_receipt,
            path=provisional.run_receipt_pin.artifact.path,
        ),
    )
    attested_pin = attested_manifest.exact_pin(manifest_path=provisional_pin.manifest_path)
    candidate = validate_release_projection(
        spec,
        attested_receipt,
        attested_manifest,
        manifest_pin=attested_pin,
        parent_release=parent,
    )
    return validate_release_content_attestation(
        candidate,
        load_parent=lambda pin: parent if pin == parent.manifest_pin else None,
    )


def test_three_control_objects_form_one_exact_non_self_referential_projection() -> None:
    spec, receipt, manifest, pin = _projection()

    validated = validate_release_projection(
        spec,
        receipt,
        manifest,
        manifest_pin=pin,
        parent_release=None,
    )

    assert manifest.release_id == pin.release_id
    assert manifest.run_spec_pin.object_id == spec.run_spec_id
    assert manifest.run_receipt_pin.object_id == receipt.run_receipt_id
    assert validated.manifest_pin == pin
    assert release_change_set_digest(manifest) == receipt.output_set_digest
    assert "manifest_pin" not in manifest.to_dict()
    assert "manifest_sha256" not in manifest.to_dict()
    assert "run_spec_id" not in manifest.release_identity_payload()
    assert "run_receipt_id" not in manifest.release_identity_payload()
    assert "lineage_digest" not in receipt.checkpoint.logical_payload()  # type: ignore[union-attr]
    assert "release_id" not in receipt.checkpoint.logical_payload()  # type: ignore[union-attr]
    assert (
        receipt.checkpoint.logical_payload()["rule_version"]  # type: ignore[union-attr]
        == CHECKPOINT_RECEIPT_RULE_VERSION
    )


def test_resource_caps_change_control_provenance_not_logical_release_id() -> None:
    first_spec, first_receipt, first_manifest, first_pin = _projection(rss_cap_bytes=2 * 1024**3)
    second_spec, second_receipt, second_manifest, second_pin = _projection(
        rss_cap_bytes=3 * 1024**3
    )

    assert first_spec.run_spec_id != second_spec.run_spec_id
    assert first_receipt.run_receipt_id != second_receipt.run_receipt_id
    assert first_manifest.release_id == second_manifest.release_id
    assert first_pin.manifest_sha256 != second_pin.manifest_sha256


def test_manifest_and_control_pin_tampering_fail_closed() -> None:
    spec, receipt, manifest, pin = _projection()

    with pytest.raises(IncrementalContractError, match="external manifest pin"):
        validate_release_projection(
            spec,
            receipt,
            manifest,
            manifest_pin=replace(pin, manifest_sha256=_digest("tampered-manifest")),
            parent_release=None,
        )

    tampered_control = replace(
        manifest.run_spec_pin,
        artifact=replace(
            manifest.run_spec_pin.artifact,
            sha256=_digest("tampered-control"),
        ),
    )
    with pytest.raises(IncrementalContractError, match="control object exact pin"):
        validate_release_projection(
            spec,
            receipt,
            replace(manifest, run_spec_pin=tampered_control),
            manifest_pin=pin,
            parent_release=None,
        )


def test_failed_receipt_can_stop_before_outputs_but_cannot_publish() -> None:
    spec, _, manifest, _ = _projection()
    failed = RunReceipt(
        run_spec_id=spec.run_spec_id,
        actual_input_set_digest=None,
        output_set_digest=None,
        qa_receipt=None,
        checkpoint=None,
        succeeded=False,
        error_codes=("source_read_failed",),
        receipt_available_session=_AVAILABLE_SESSION,
        runtime_seconds=0,
        peak_rss_bytes=None,
        minimum_free_disk_bytes=0,
    )

    assert failed.checkpoint is None
    with pytest.raises(IncrementalContractError, match="failed run"):
        validate_release_projection(
            spec,
            failed,
            manifest,
            manifest_pin=manifest.exact_pin(manifest_path="releases/current/manifest.json"),
            parent_release=None,
        )


def test_failed_receipt_accepts_only_causal_prefixes_and_none_can_publish() -> None:
    spec, successful, manifest, pin = _projection()
    actual = successful.actual_input_set_digest
    output = successful.output_set_digest
    assert actual is not None and output is not None
    failed_prefixes = (
        replace(
            successful,
            output_set_digest=None,
            qa_receipt=None,
            checkpoint=None,
            succeeded=False,
            error_codes=("transform_failed",),
        ),
        replace(
            successful,
            qa_receipt=None,
            checkpoint=None,
            succeeded=False,
            error_codes=("qa_not_started",),
        ),
        replace(
            successful,
            checkpoint=None,
            succeeded=False,
            error_codes=("checkpoint_failed",),
        ),
        replace(
            successful,
            succeeded=False,
            error_codes=("publish_not_attempted",),
        ),
    )
    for failed in failed_prefixes:
        with pytest.raises(IncrementalContractError, match="failed run"):
            validate_release_projection(
                spec,
                failed,
                manifest,
                manifest_pin=pin,
                parent_release=None,
            )

    with pytest.raises(IncrementalContractError, match="outputs require"):
        replace(
            successful,
            actual_input_set_digest=None,
            qa_receipt=None,
            checkpoint=None,
            succeeded=False,
            error_codes=("impossible_prefix",),
        )
    with pytest.raises(IncrementalContractError, match="QA receipt requires"):
        replace(
            successful,
            output_set_digest=None,
            checkpoint=None,
            succeeded=False,
            error_codes=("impossible_prefix",),
        )
    with pytest.raises(IncrementalContractError, match="checkpoint requires"):
        replace(
            successful,
            output_set_digest=None,
            qa_receipt=None,
            succeeded=False,
            error_codes=("impossible_prefix",),
        )
    with pytest.raises(IncrementalContractError, match="structured QA"):
        replace(
            successful,
            qa_receipt=None,
            succeeded=False,
            error_codes=("impossible_prefix",),
        )


def test_runtime_observations_do_not_change_run_receipt_identity() -> None:
    _, receipt, _, _ = _projection()
    revised = replace_runtime_observation(
        receipt,
        runtime_seconds=999.0,
        peak_rss_bytes=1_200_000_000,
        minimum_free_disk_bytes=70 * 1024**3,
    )

    assert revised.run_receipt_id == receipt.run_receipt_id
    with pytest.raises(IncrementalContractError, match="only runtime"):
        replace_runtime_observation(receipt, succeeded=False)


def test_legacy_control_projection_fingerprint_omits_absent_i3_attestations() -> None:
    spec, receipt, manifest, pin = _projection()
    candidate = validate_release_projection(
        spec,
        receipt,
        manifest,
        manifest_pin=pin,
        parent_release=None,
    )
    expected = stable_digest(
        {
            "correction_authorization_id": manifest.correction_authorization_id,
            "manifest_pin": pin.to_dict(),
            "qa_policy_id": manifest.qa_policy_id,
            "qa_receipt_id": manifest.qa_receipt_id,
            "rule_version": "s7_5_control_projection_v1",
            "run_receipt_pin": manifest.run_receipt_pin.to_dict(),
            "run_spec_pin": manifest.run_spec_pin.to_dict(),
        }
    )
    assert candidate.row_semantic_attestation_digest is None
    assert candidate.parent_frontier_attestation_digest is None
    assert candidate.control_projection_digest == expected


def test_input_binding_requires_unique_sorted_paths_and_exact_digest() -> None:
    spec = _spec()
    duplicate_path = replace(spec.input_pins[1], path=spec.input_pins[0].path)

    with pytest.raises(IncrementalContractError, match="unique paths"):
        replace(spec, input_pins=(spec.input_pins[0], duplicate_path))
    with pytest.raises(IncrementalContractError, match="does not reproduce"):
        replace(spec, source_binding_digest=_digest("wrong-source-binding"))
    with pytest.raises(IncrementalContractError, match="at least one exact input"):
        replace(spec, input_pins=(), source_binding_digest=input_set_digest(()))


def test_correction_authorization_is_bound_in_spec_and_manifest() -> None:
    parent_release = _validated_base()
    old = parent_release.manifest.added_partition_receipts[0]
    replacement = PartitionReplacement(
        old,
        replace(old, receipt=_artifact("corrected", path="silver/corrected.parquet")),
    )
    spec, receipt, manifest, pin = _projection(
        release_type=ReleaseType.CORRECTION,
        parent_release=parent_release,
        added_partitions=(),
        replacements=(replacement,),
    )
    validate_release_projection(
        spec,
        receipt,
        manifest,
        manifest_pin=pin,
        parent_release=parent_release,
    )

    with pytest.raises(IncrementalContractError, match="requires exact scope"):
        _spec(
            release_type=ReleaseType.CORRECTION,
            parent=parent_release.manifest_pin,
            parent_identity_policy_bundle_id=parent_release.manifest.identity_policy_bundle_id,
            availability_cutoff_session=parent_release.manifest_pin.release_available_session,
            release_available_session=parent_release.manifest_pin.release_available_session,
        )
    with pytest.raises(IncrementalContractError, match="cannot carry"):
        _spec(
            correction_scope=spec.correction_scope_digest,
            authorization=spec.correction_authorization,
        )
    with pytest.raises(IncrementalContractError, match="authorization ID differs"):
        validate_release_projection(
            spec,
            receipt,
            replace(
                manifest,
                correction_authorization_id=_digest("other-approval"),
            ),
            manifest_pin=pin,
            parent_release=parent_release,
        )

    wrong_parent = _validated_base(rss_cap_bytes=3 * 1024**3)
    assert wrong_parent.manifest_pin != parent_release.manifest_pin
    with pytest.raises(IncrementalContractError, match="exact parent pin"):
        validate_release_projection(
            spec,
            receipt,
            manifest,
            manifest_pin=pin,
            parent_release=wrong_parent,
        )


def test_gate_a_correction_cannot_receive_reader_capability_without_trusted_ledger() -> None:
    parent_release = _validated_base()
    old = parent_release.manifest.added_partition_receipts[0]
    replacement = PartitionReplacement(
        old,
        replace(old, receipt=_artifact("corrected-gate-a", path="silver/corrected.json")),
    )
    spec, receipt, manifest, pin = _projection(
        release_type=ReleaseType.CORRECTION,
        parent_release=parent_release,
        added_partitions=(),
        replacements=(replacement,),
    )
    candidate = validate_release_projection(
        spec,
        receipt,
        manifest,
        manifest_pin=pin,
        parent_release=parent_release,
    )

    with pytest.raises(
        IncrementalResolutionError,
        match="approval-event attestation is unavailable",
    ):
        validate_release_content_attestation(
            candidate,
            load_parent=lambda exact_pin: (
                parent_release if exact_pin == parent_release.manifest_pin else None
            ),
        )

    forged_snapshot_digest = _digest("forged-correction-snapshot")
    forged = ContentAttestedRelease(
        candidate=candidate,
        attested_resolved_content_digest=manifest.resolved_content_digest,
        attested_snapshot_digest=forged_snapshot_digest,
        content_attestation_digest=stable_digest(
            {
                "control_projection_digest": candidate.control_projection_digest,
                "manifest_pin": pin.to_dict(),
                "resolved_content_digest": manifest.resolved_content_digest,
                "rule_version": "s7_5_content_attestation_v1",
                "snapshot_digest": forged_snapshot_digest,
            }
        ),
        _seal=parent_release._seal,
    )
    with pytest.raises(IncrementalContractError, match="reader capability is disabled"):
        verify_content_attested_release(forged)


def test_row_operation_shapes_and_delta_safety_boundary() -> None:
    predecessor = _digest("v1")
    with pytest.raises(IncrementalContractError, match="cannot carry predecessor"):
        _row(predecessor=predecessor)
    with pytest.raises(IncrementalContractError, match="requires exact predecessor"):
        _row(operation=RowVersionOperation.MECHANICAL_SUCCESSOR)
    with pytest.raises(IncrementalContractError, match="requires a reason"):
        _row(
            operation=RowVersionOperation.TOMBSTONE,
            predecessor=predecessor,
        )

    correction_row = _row(
        version_label="v2",
        operation=RowVersionOperation.REVIEWED_CORRECTION,
        predecessor=predecessor,
    )
    with pytest.raises(IncrementalContractError, match="delta release cannot perform"):
        _projection(
            release_type=ReleaseType.DELTA,
            parent_release=_validated_base(),
            rows=(correction_row,),
        )


def test_gate_a_default_denies_every_row_bearing_release() -> None:
    parent_release = _validated_base()
    spec, receipt, manifest, pin = _projection(
        release_type=ReleaseType.DELTA,
        parent_release=parent_release,
        rows=(_row(version_label="new-alias-root"),),
    )

    with pytest.raises(IncrementalContractError, match="dispatcher is disabled"):
        validate_release_projection(
            spec,
            receipt,
            manifest,
            manifest_pin=pin,
            parent_release=parent_release,
        )
    with pytest.raises(TypeError):
        validate_release_projection(
            spec,
            receipt,
            manifest,
            manifest_pin=pin,
            parent_release=parent_release,
            validate_row_semantics=lambda row: True,  # type: ignore[call-arg]
        )


def test_validated_release_capability_cannot_be_minted_by_a_caller() -> None:
    spec, receipt, manifest, pin = _projection()
    with pytest.raises(IncrementalContractError, match="only be minted"):
        ControlValidatedCandidate(
            manifest_pin=pin,
            run_spec=spec,
            run_receipt=receipt,
            manifest=manifest,
            parent_release=None,
            control_projection_digest=_digest("forged-projection"),
            _seal=object(),
        )

    validated = validate_release_projection(
        spec,
        receipt,
        manifest,
        manifest_pin=pin,
        parent_release=None,
    )
    with pytest.raises(TypeError):
        replace(
            validated,
            manifest=replace(
                manifest,
                resolved_content_digest=_digest("mutated-after-validation"),
            ),
        )
    forged = ControlValidatedCandidate(
        manifest_pin=pin,
        run_spec=spec,
        run_receipt=receipt,
        manifest=replace(
            manifest,
            resolved_content_digest=_digest("mutated-with-reused-seal"),
        ),
        parent_release=None,
        control_projection_digest=validated.control_projection_digest,
        _seal=validated._seal,
    )
    with pytest.raises(IncrementalContractError, match="resolved content"):
        verify_control_validated_candidate(forged)


def test_content_capability_verification_is_iterative_for_long_daily_chains(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = _validated_base()
    by_pin: dict[ManifestPin, ControlValidatedCandidate] = {base.manifest_pin: base.candidate}
    top = base
    for index in range(1, 1_201):
        pin = ManifestPin(
            release_id=_digest(f"long-chain-release-{index}"),
            manifest_path=f"releases/long-chain/{index}/manifest.json",
            manifest_sha256=_digest(f"long-chain-manifest-{index}"),
            manifest_bytes=1_000 + index,
            release_available_session=_LATER_SESSION,
        )
        control_digest = _digest(f"long-chain-control-{index}")
        candidate = ControlValidatedCandidate(
            manifest_pin=pin,
            run_spec=base.run_spec,
            run_receipt=base.run_receipt,
            manifest=base.manifest,
            parent_release=top,
            control_projection_digest=control_digest,
            _seal=base.candidate._seal,
        )
        snapshot_digest = _digest(f"long-chain-snapshot-{index}")
        top = ContentAttestedRelease(
            candidate=candidate,
            attested_resolved_content_digest=base.manifest.resolved_content_digest,
            attested_snapshot_digest=snapshot_digest,
            content_attestation_digest=stable_digest(
                {
                    "control_projection_digest": control_digest,
                    "manifest_pin": pin.to_dict(),
                    "resolved_content_digest": base.manifest.resolved_content_digest,
                    "rule_version": "s7_5_content_attestation_v1",
                    "snapshot_digest": snapshot_digest,
                }
            ),
            _seal=base._seal,
        )
        by_pin[pin] = candidate

    calls = 0

    def replay_one(
        spec: RunSpec,
        receipt: RunReceipt,
        manifest: IncrementalReleaseManifest,
        *,
        manifest_pin: ManifestPin,
        parent_release: ContentAttestedRelease | None,
    ) -> ControlValidatedCandidate:
        del spec, receipt, manifest, parent_release
        nonlocal calls
        calls += 1
        return by_pin[manifest_pin]

    monkeypatch.setattr(
        incremental_contract_module,
        "_validate_release_projection_once",
        replay_one,
    )

    assert verify_content_attested_release(top) is top
    assert calls == 1_201


def test_content_capability_rejects_same_release_id_under_a_different_pin() -> None:
    base = _validated_base()
    conflicting_pin = replace(
        base.manifest_pin,
        manifest_path="releases/conflicting/manifest.json",
        manifest_sha256=_digest("conflicting-manifest-bytes"),
    )
    conflicting = ControlValidatedCandidate(
        manifest_pin=conflicting_pin,
        run_spec=base.run_spec,
        run_receipt=base.run_receipt,
        manifest=base.manifest,
        parent_release=base,
        control_projection_digest=_digest("conflicting-control"),
        _seal=base.candidate._seal,
    )

    with pytest.raises(IncrementalContractError, match="conflicting parent manifest pins"):
        verify_control_validated_candidate(conflicting)


def test_iterative_verifier_rejects_malformed_top_and_middle_pins_with_contract_error() -> None:
    base = _validated_base()
    malformed = ControlValidatedCandidate(
        manifest_pin=[],  # type: ignore[arg-type]
        run_spec=base.run_spec,
        run_receipt=base.run_receipt,
        manifest=base.manifest,
        parent_release=None,
        control_projection_digest=_digest("malformed-control"),
        _seal=base.candidate._seal,
    )
    with pytest.raises(IncrementalContractError, match="manifest pin is invalid"):
        verify_control_validated_candidate(malformed)

    malformed_parent = ContentAttestedRelease(
        candidate=malformed,
        attested_resolved_content_digest=base.manifest.resolved_content_digest,
        attested_snapshot_digest=_digest("malformed-snapshot"),
        content_attestation_digest=_digest("malformed-attestation"),
        _seal=base._seal,
    )
    top = ControlValidatedCandidate(
        manifest_pin=replace(
            base.manifest_pin,
            release_id=_digest("valid-top-release"),
            manifest_path="releases/valid-top/manifest.json",
            manifest_sha256=_digest("valid-top-manifest"),
        ),
        run_spec=base.run_spec,
        run_receipt=base.run_receipt,
        manifest=base.manifest,
        parent_release=malformed_parent,
        control_projection_digest=_digest("valid-top-control"),
        _seal=base.candidate._seal,
    )
    with pytest.raises(IncrementalContractError, match="manifest pin is invalid"):
        verify_control_validated_candidate(top)


def test_noop_release_and_supersession_projection_fail_closed() -> None:
    with pytest.raises(IncrementalContractError, match="at least one logical change"):
        _projection(added_partitions=(), rows=())

    _, _, manifest, _ = _projection()
    with pytest.raises(IncrementalContractError, match="predecessor projection"):
        replace(manifest, superseded_row_version_ids=(_digest("not-a-predecessor"),))


def test_partition_replacement_is_correction_only_and_exact_same_key() -> None:
    parent_release = _validated_base()
    old = parent_release.manifest.added_partition_receipts[0]
    new = replace(old, receipt=_artifact("new", path="silver/new.parquet"))
    replacement = PartitionReplacement(old, new)

    spec, receipt, manifest, pin = _projection(
        release_type=ReleaseType.CORRECTION,
        parent_release=parent_release,
        added_partitions=(),
        replacements=(replacement,),
        rows=(),
    )
    validate_release_projection(
        spec,
        receipt,
        manifest,
        manifest_pin=pin,
        parent_release=parent_release,
    )

    with pytest.raises(IncrementalContractError, match="delta release cannot replace"):
        _projection(
            release_type=ReleaseType.DELTA,
            parent_release=parent_release,
            added_partitions=(),
            replacements=(replacement,),
            rows=(),
        )
    with pytest.raises(IncrementalContractError, match="crossed a logical key"):
        PartitionReplacement(
            old,
            replace(new, partition_key=date(2026, 7, 30).isoformat()),
        )


def test_parent_receipt_and_checkpoint_time_causality_fail_closed() -> None:
    parent = replace(_parent_pin(), release_available_session=_LATER_SESSION)
    with pytest.raises(IncrementalContractError, match="precedes parent"):
        _spec(
            release_type=ReleaseType.DELTA,
            parent=parent,
            parent_identity_policy_bundle_id=_digest("policy"),
        )

    timed_spec, timed_receipt, timed_manifest, _ = _projection(
        release_available_session=_LATER_SESSION
    )
    early_qa = replace(
        timed_receipt.qa_receipt,
        qa_available_session=date(2026, 8, 2),
    )
    early_qa_receipt = replace(
        timed_receipt,
        qa_receipt=early_qa,
    )
    early_qa_manifest = replace(
        timed_manifest,
        qa_receipt_id=early_qa.qa_receipt_id,
        run_receipt_pin=control_object_pin(
            early_qa_receipt,
            path=timed_manifest.run_receipt_pin.artifact.path,
        ),
    )
    validate_release_projection(
        timed_spec,
        early_qa_receipt,
        early_qa_manifest,
        manifest_pin=early_qa_manifest.exact_pin(manifest_path="releases/early-qa/manifest.json"),
        parent_release=None,
    )

    early_receipt = replace(
        timed_receipt,
        qa_receipt=early_qa,
        receipt_available_session=date(2026, 8, 2),
    )
    early_manifest = replace(
        timed_manifest,
        qa_receipt_id=early_qa.qa_receipt_id,
        run_receipt_pin=control_object_pin(
            early_receipt,
            path=timed_manifest.run_receipt_pin.artifact.path,
        ),
    )
    with pytest.raises(IncrementalContractError, match="run availability cutoff"):
        validate_release_projection(
            timed_spec,
            early_receipt,
            early_manifest,
            manifest_pin=early_manifest.exact_pin(
                manifest_path="releases/early-receipt/manifest.json"
            ),
            parent_release=None,
        )

    spec, receipt, manifest, pin = _projection()
    late_checkpoint = replace(receipt.checkpoint, last_session=_LATER_SESSION)
    late_receipt = replace(receipt, checkpoint=late_checkpoint)
    late_manifest = replace(
        manifest,
        run_receipt_pin=control_object_pin(
            late_receipt,
            path=manifest.run_receipt_pin.artifact.path,
        ),
    )
    with pytest.raises(IncrementalContractError, match="last session"):
        validate_release_projection(
            spec,
            late_receipt,
            late_manifest,
            manifest_pin=pin,
            parent_release=None,
        )

    stale_checkpoint = replace(
        receipt.checkpoint,
        last_session=date(2026, 7, 28),
    )
    stale_receipt = replace(receipt, checkpoint=stale_checkpoint)
    stale_manifest = replace(
        manifest,
        run_receipt_pin=control_object_pin(
            stale_receipt,
            path=manifest.run_receipt_pin.artifact.path,
        ),
    )
    with pytest.raises(IncrementalContractError, match="partition frontier"):
        validate_release_projection(
            spec,
            stale_receipt,
            stale_manifest,
            manifest_pin=pin,
            parent_release=None,
        )

    late_partition = _partition(availability=_LATER_SESSION)
    with pytest.raises(IncrementalContractError, match="partition receipt availability"):
        _projection(
            added_partitions=(late_partition,),
            rows=(),
        )


def test_release_availability_is_body_bound_and_cannot_be_re_pinned() -> None:
    spec, receipt, manifest, pin = _projection()
    with pytest.raises(IncrementalContractError, match="external manifest pin"):
        validate_release_projection(
            spec,
            receipt,
            manifest,
            manifest_pin=replace(pin, release_available_session=_LATER_SESSION),
            parent_release=None,
        )
    later_manifest = replace(
        manifest,
        release_available_session=_LATER_SESSION,
    )
    assert later_manifest.release_id != manifest.release_id


def test_qa_receipt_and_run_receipt_availability_cannot_exceed_release() -> None:
    spec, receipt, manifest, pin = _projection()
    with pytest.raises(IncrementalContractError, match="QA availability exceeds"):
        replace(
            receipt,
            qa_receipt=replace(
                receipt.qa_receipt,
                qa_available_session=_LATER_SESSION,
            ),
        )

    later_receipt = replace(receipt, receipt_available_session=_LATER_SESSION)
    later_manifest = replace(
        manifest,
        run_receipt_pin=control_object_pin(
            later_receipt,
            path=manifest.run_receipt_pin.artifact.path,
        ),
    )
    with pytest.raises(IncrementalContractError, match="exceeds release availability"):
        validate_release_projection(
            spec,
            later_receipt,
            later_manifest,
            manifest_pin=pin,
            parent_release=None,
        )


def test_partition_session_and_manifest_cutoffs_fail_closed() -> None:
    with pytest.raises(
        IncrementalContractError,
        match="partition session exceeds partition receipt availability",
    ):
        replace(
            _partition(),
            partition_key=_LATER_SESSION.isoformat(),
        )

    beyond_source_cutoff = replace(
        _partition(),
        partition_key=date(2026, 7, 30).isoformat(),
    )
    with pytest.raises(IncrementalContractError, match="manifest source cutoff"):
        _projection(
            added_partitions=(beyond_source_cutoff,),
            rows=(),
        )


def test_projection_rejects_cross_wired_outputs_qa_and_checkpoint() -> None:
    spec, receipt, manifest, pin = _projection()
    wrong_output = _digest("wrong-output")
    wrong_output_receipt = replace(
        receipt,
        output_set_digest=wrong_output,
        qa_receipt=replace(receipt.qa_receipt, change_set_digest=wrong_output),
    )
    with pytest.raises(IncrementalContractError, match="output digest"):
        validate_release_projection(
            spec,
            wrong_output_receipt,
            replace(
                manifest,
                run_receipt_pin=control_object_pin(
                    wrong_output_receipt,
                    path=manifest.run_receipt_pin.artifact.path,
                ),
            ),
            manifest_pin=pin,
            parent_release=None,
        )


def test_projection_recomputes_structured_qa_publish_verdict() -> None:
    spec, receipt, manifest, pin = _projection()
    failed_result = replace(receipt.qa_receipt.results[0], failure_count=1)
    failed_qa = replace(
        receipt.qa_receipt,
        results=(failed_result, *receipt.qa_receipt.results[1:]),
    )
    failed_receipt = replace(receipt, qa_receipt=failed_qa)
    failed_manifest = replace(
        manifest,
        qa_receipt_id=failed_qa.qa_receipt_id,
        run_receipt_pin=control_object_pin(
            failed_receipt,
            path=manifest.run_receipt_pin.artifact.path,
        ),
    )
    with pytest.raises(IncrementalContractError, match="blocking failures"):
        validate_release_projection(
            spec,
            failed_receipt,
            failed_manifest,
            manifest_pin=pin,
            parent_release=None,
        )

    wrong_count_result = replace(
        receipt.qa_receipt.results[0],
        observed_count=receipt.qa_receipt.results[0].observed_count + 1,
    )
    wrong_count_qa = replace(
        receipt.qa_receipt,
        results=(wrong_count_result, *receipt.qa_receipt.results[1:]),
    )
    wrong_count_receipt = replace(receipt, qa_receipt=wrong_count_qa)
    wrong_count_manifest = replace(
        manifest,
        qa_receipt_id=wrong_count_qa.qa_receipt_id,
        run_receipt_pin=control_object_pin(
            wrong_count_receipt,
            path=manifest.run_receipt_pin.artifact.path,
        ),
    )
    with pytest.raises(IncrementalContractError, match="observation count"):
        validate_release_projection(
            spec,
            wrong_count_receipt,
            wrong_count_manifest,
            manifest_pin=pin,
            parent_release=None,
        )
    with pytest.raises(IncrementalContractError, match="QA receipt ID"):
        validate_release_projection(
            spec,
            receipt,
            replace(manifest, qa_receipt_id=_digest("wrong-qa")),
            manifest_pin=pin,
            parent_release=None,
        )
    with pytest.raises(IncrementalContractError, match="resolved content"):
        validate_release_projection(
            spec,
            receipt,
            replace(manifest, resolved_content_digest=_digest("wrong-content")),
            manifest_pin=pin,
            parent_release=None,
        )


def test_wall_clock_cap_is_optional_while_rss_and_disk_guards_remain_required() -> None:
    assert _spec(wall_clock_cap_seconds=None).wall_clock_cap_seconds is None
    assert _spec(wall_clock_cap_seconds=3600).wall_clock_cap_seconds == 3600
    with pytest.raises(IncrementalContractError, match="RSS cap"):
        replace(_spec(), rss_cap_bytes=0)
    with pytest.raises(IncrementalContractError, match="disk floor"):
        replace(_spec(), disk_floor_bytes=0)

    spec, receipt, manifest, pin = _projection()
    missing_rss = replace(receipt, peak_rss_bytes=None)
    with pytest.raises(IncrementalContractError, match="peak RSS"):
        validate_release_projection(
            spec,
            missing_rss,
            manifest,
            manifest_pin=pin,
            parent_release=None,
        )
    over_rss = replace(receipt, peak_rss_bytes=spec.rss_cap_bytes + 1)
    with pytest.raises(IncrementalContractError, match="RSS hard cap"):
        validate_release_projection(
            spec,
            over_rss,
            manifest,
            manifest_pin=pin,
            parent_release=None,
        )
    low_disk = replace(receipt, minimum_free_disk_bytes=spec.disk_floor_bytes - 1)
    with pytest.raises(IncrementalContractError, match="disk hard floor"):
        validate_release_projection(
            spec,
            low_disk,
            manifest,
            manifest_pin=pin,
            parent_release=None,
        )

    capped_spec = replace(spec, wall_clock_cap_seconds=1)
    capped_checkpoint = replace(
        receipt.checkpoint,
        run_spec_id=capped_spec.run_spec_id,
        rebuild_basis_digest=checkpoint_rebuild_basis_from_change_digest(
            capped_spec,
            change_set_digest=receipt.output_set_digest,
        ),
    )
    capped_receipt = replace(
        receipt,
        run_spec_id=capped_spec.run_spec_id,
        qa_receipt=replace(receipt.qa_receipt, run_spec_id=capped_spec.run_spec_id),
        checkpoint=capped_checkpoint,
    )
    capped_manifest = replace(
        manifest,
        run_spec_pin=control_object_pin(capped_spec, path="control/capped-spec.json"),
        run_receipt_pin=control_object_pin(
            capped_receipt,
            path="control/capped-receipt.json",
        ),
    )
    with pytest.raises(IncrementalContractError, match="wall-clock cap"):
        validate_release_projection(
            capped_spec,
            capped_receipt,
            capped_manifest,
            manifest_pin=pin,
            parent_release=None,
        )


def test_real_contract_objects_integrate_with_exact_pin_resolver() -> None:
    spec, receipt, manifest, pin = _attested_projection()
    candidate = validate_release_projection(
        spec,
        receipt,
        manifest,
        manifest_pin=pin,
        parent_release=None,
    )
    attested = validate_release_content_attestation(
        candidate,
        load_parent=lambda parent_pin: None,
    )
    requested: list[ManifestPin] = []

    def load(exact_pin: ManifestPin) -> ContentAttestedRelease | None:
        requested.append(exact_pin)
        return attested if exact_pin == pin else None

    resolved = resolve_incremental_snapshot(
        pin,
        view_kind=ViewKind.LATEST_REVIEWED_RESEARCH,
        cutoff_session=_AVAILABLE_SESSION,
        load_parent=load,
    )

    assert requested == [pin]
    assert resolved.release_chain == (pin,)
    assert set(resolved.partition_receipts) == {("universe_daily", _SOURCE_SESSION.isoformat())}
    assert not resolved.row_version_catalog
    assert resolved.audit_row_version_catalog == resolved.row_version_catalog
    assert len(resolved.resolved_content_digest) == 64

    with pytest.raises(IncrementalResolutionError, match="without content attestation"):
        resolve_incremental_snapshot(
            pin,
            view_kind=ViewKind.LATEST_REVIEWED_RESEARCH,
            cutoff_session=_AVAILABLE_SESSION,
            load_parent=lambda exact_pin: manifest,  # type: ignore[return-value]
        )
    with pytest.raises(IncrementalResolutionError, match="without content attestation"):
        resolve_incremental_snapshot(
            pin,
            view_kind=ViewKind.LATEST_REVIEWED_RESEARCH,
            cutoff_session=_AVAILABLE_SESSION,
            load_parent=lambda exact_pin: candidate,  # type: ignore[return-value]
        )


def test_real_base_to_delta_chain_attests_and_resolves_through_public_api() -> None:
    base = _validated_base()
    delta = _validated_delta(base)
    requested: list[ManifestPin] = []

    def load(exact_pin: ManifestPin) -> ContentAttestedRelease | None:
        requested.append(exact_pin)
        return delta if exact_pin == delta.manifest_pin else None

    resolved = resolve_incremental_snapshot(
        delta.manifest_pin,
        view_kind=ViewKind.LATEST_REVIEWED_RESEARCH,
        cutoff_session=_NEXT_AVAILABLE_SESSION,
        load_parent=load,
    )

    assert requested == [delta.manifest_pin]
    assert resolved.release_chain == (base.manifest_pin, delta.manifest_pin)
    assert set(resolved.partition_receipts) == {
        ("universe_daily", _SOURCE_SESSION.isoformat()),
        ("universe_daily", _NEXT_SOURCE_SESSION.isoformat()),
    }
    assert resolved.resolved_content_digest == delta.manifest.resolved_content_digest

    historical_before_delta = resolve_incremental_snapshot(
        delta.manifest_pin,
        view_kind=ViewKind.HISTORICAL_AS_KNOWN,
        cutoff_session=base.manifest.release_available_session,
        load_parent=lambda exact_pin: delta if exact_pin == delta.manifest_pin else None,
    )
    assert set(historical_before_delta.partition_receipts) == {
        ("universe_daily", _SOURCE_SESSION.isoformat())
    }

    with pytest.raises(
        IncrementalContractError,
        match="manifest availability cutoff precedes parent release",
    ):
        replace(
            delta.manifest,
            availability_cutoff_session=_AVAILABLE_SESSION,
        )

    unrelated = _validated_base(rss_cap_bytes=3 * 1024**3)
    with pytest.raises(IncrementalContractError, match="exact parent pin"):
        validate_release_projection(
            delta.run_spec,
            delta.run_receipt,
            delta.manifest,
            manifest_pin=delta.manifest_pin,
            parent_release=unrelated,
        )


def test_publish_attestation_uses_manifest_view_cutoff_and_exact_top_pin() -> None:
    spec, receipt, manifest, pin = _attested_projection()

    candidate = validate_release_projection(
        spec,
        receipt,
        manifest,
        manifest_pin=pin,
        parent_release=None,
    )
    attested = validate_release_content_attestation(
        candidate,
        load_parent=lambda parent_pin: None,
    )
    resolved = resolve_incremental_snapshot(
        pin,
        view_kind=manifest.resolved_view,
        cutoff_session=manifest.release_available_session,
        load_parent=lambda parent_pin: attested if parent_pin == pin else None,
    )

    assert resolved.view_kind is manifest.resolved_view
    assert resolved.cutoff_session == manifest.release_available_session
    assert resolved.resolved_content_digest == manifest.resolved_content_digest

    with pytest.raises(IncrementalResolutionError, match="does not match"):
        resolve_incremental_snapshot(
            replace(pin, manifest_sha256=_digest("wrong-exact-top-pin")),
            view_kind=manifest.resolved_view,
            cutoff_session=manifest.release_available_session,
            load_parent=lambda parent_pin: attested,
        )


def test_publish_attestation_rejects_internally_consistent_forged_content_digest() -> None:
    spec, receipt, forged_manifest, forged_pin = _projection()

    # The control/checkpoint projection is internally consistent, but its declared content
    # digest was never derived from the exact resolved release state.
    validated = validate_release_projection(
        spec,
        receipt,
        forged_manifest,
        manifest_pin=forged_pin,
        parent_release=None,
    )
    with pytest.raises(IncrementalResolutionError, match="resolved-content attestation"):
        validate_release_content_attestation(
            validated,
            load_parent=lambda parent_pin: None,
        )


def test_public_resolver_recomputes_content_instead_of_trusting_runtime_seal() -> None:
    legal_attested = _validated_base()
    spec, receipt, manifest, pin = _projection()
    forged_candidate = validate_release_projection(
        spec,
        receipt,
        manifest,
        manifest_pin=pin,
        parent_release=None,
    )
    forged_snapshot_digest = _digest("forged-snapshot")
    forged_attestation_digest = stable_digest(
        {
            "control_projection_digest": forged_candidate.control_projection_digest,
            "manifest_pin": pin.to_dict(),
            "resolved_content_digest": manifest.resolved_content_digest,
            "rule_version": "s7_5_content_attestation_v1",
            "snapshot_digest": forged_snapshot_digest,
        }
    )
    forged_attested = ContentAttestedRelease(
        candidate=forged_candidate,
        attested_resolved_content_digest=manifest.resolved_content_digest,
        attested_snapshot_digest=forged_snapshot_digest,
        content_attestation_digest=forged_attestation_digest,
        _seal=legal_attested._seal,
    )
    verify_content_attested_release(forged_attested)

    with pytest.raises(IncrementalResolutionError, match="does not reproduce"):
        resolve_incremental_snapshot(
            pin,
            view_kind=manifest.resolved_view,
            cutoff_session=manifest.release_available_session,
            load_parent=lambda exact_pin: forged_attested,
        )


def test_control_pin_kind_cannot_be_crossed() -> None:
    spec, _, manifest, _ = _projection()
    crossed = ControlObjectPin(
        object_kind=ControlObjectKind.RUN_RECEIPT,
        object_id=spec.run_spec_id,
        artifact=manifest.run_spec_pin.artifact,
    )
    with pytest.raises(IncrementalContractError, match="run-spec pin"):
        replace(manifest, run_spec_pin=crossed)


def test_gate_a_candidate_hash_and_code_vocabulary_are_exact() -> None:
    document = json.loads(_CANDIDATE_PATH.read_text(encoding="utf-8"))
    contract = document["logical_contract"]

    assert document["contract_id"] == stable_digest(contract)
    assert set(ALIAS_SEGMENT_ID_SUBJECT_FIELD_ORDER) == ALIAS_SEGMENT_ID_SUBJECT_FIELDS
    assert contract["alias_v2"]["segment"]["subject_fields"] == list(
        ALIAS_SEGMENT_ID_SUBJECT_FIELD_ORDER
    )
    assert set(ALIAS_RESOLUTION_VERSION_ID_SUBJECT_FIELD_ORDER) == (
        ALIAS_RESOLUTION_VERSION_ID_SUBJECT_FIELDS
    )
    assert contract["alias_v2"]["resolution_version"]["subject_fields"] == list(
        ALIAS_RESOLUTION_VERSION_ID_SUBJECT_FIELD_ORDER
    )
    assert contract["alias_v2"]["resolution_version"]["closed_methods"] == sorted(
        item.value for item in AliasResolutionMethod
    )
    assert contract["alias_v2"]["resolution_version"]["closed_share_class_methods"] == sorted(
        item.value for item in ShareClassResolutionMethod
    )
    assert contract["alias_v2"]["resolution_version"]["closed_dispositions"] == sorted(
        item.value for item in AliasResolutionDisposition
    )

    _, _, manifest, _ = _projection()
    assert contract["release"]["logical_identity_fields"] == list(
        manifest.release_identity_payload()
    )
    assert contract["control_plane"]["durable_objects"] == [
        "run_spec",
        "run_receipt",
        "release_manifest",
    ]
    runtime_capabilities = contract["control_plane"]["runtime_capabilities"]
    assert set(runtime_capabilities) == {
        "content_attested_release",
        "control_validated_candidate",
        "private_seals",
    }
    assert runtime_capabilities["control_validated_candidate"]["resolver_requires"] is False
    assert runtime_capabilities["content_attested_release"]["resolver_requires"] is True
    assert (
        contract["control_plane"]["production_publication_trust"]["base_cutover"]
        == "default_deny_until_i2_i3_attests_exact_append_only_approval_event"
    )
    assert "content_attested_release_loader" in contract["resolver"]["input"]
    assert (
        contract["non_session_row_versions"]["semantic_proof_receipt"]["caller_callback_allowed"]
        is False
    )
    assert contract["structured_qa"]["result_fields"] == [
        "check_id",
        "details_artifact",
        "failure_count",
        "observed_count",
        "semantics_digest",
    ]
    assert "details_digest" not in contract["structured_qa"]["result_fields"]
    assert contract["capabilities"] == {
        "incremental_execution_authorized": False,
        "parquet_content_read_authorized": False,
        "publish_authorized": False,
        "registry_mutation_authorized": False,
        "s8_execution_authorized": False,
    }
    assert contract["checkpoint"]["receipt_rule_version"] == (CHECKPOINT_RECEIPT_RULE_VERSION)
