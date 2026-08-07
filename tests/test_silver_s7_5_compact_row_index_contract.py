from __future__ import annotations

from dataclasses import replace

import pytest
from test_silver_s7_5_incremental_contract import (
    _AVAILABLE_SESSION,
    _digest,
    _projection,
    _row,
)

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver import incremental_contract as contract
from ame_stocks_api.silver.incremental_contract import (
    ArtifactPin,
    CheckpointReceipt,
    IncrementalContractError,
    IncrementalReleaseManifest,
    RowVersionChangeIndexPin,
    RunReceipt,
    checkpoint_rebuild_basis_from_change_digest,
    control_object_pin,
    logical_change_set_digest,
    validate_release_projection,
)
from ame_stocks_api.silver.incremental_gate import QaReceipt


def _index(*, suffix: str = "one", row_count: int = 7) -> RowVersionChangeIndexPin:
    return RowVersionChangeIndexPin(
        artifact=ArtifactPin(
            path=f"changes/row-version-index-{suffix}.parquet",
            sha256=_digest(f"index-artifact-{suffix}"),
            bytes=1024,
        ),
        row_count=row_count,
        logical_receipts_digest=_digest(f"logical-receipts-{suffix}"),
        superseded_row_version_count=3,
        superseded_row_version_ids_digest=_digest(f"superseded-{suffix}"),
        schema_digest=_digest("row-index-schema"),
        availability_session=_AVAILABLE_SESSION,
    )


def _indexed_projection() -> tuple[object, RunReceipt, IncrementalReleaseManifest, object]:
    prior_spec, prior_receipt, prior_manifest, _ = _projection(rows=())
    index = _index()
    change_digest = logical_change_set_digest(
        added_partition_receipts=prior_manifest.added_partition_receipts,
        partition_replacements=(),
        added_row_version_receipts=(),
        superseded_row_version_ids=(),
        row_version_change_index=index,
    )
    spec = replace(prior_spec, expected_change_set_digest=change_digest)
    assert prior_receipt.qa_receipt is not None
    qa_results = tuple(
        replace(
            result,
            observed_count=(
                index.row_count
                if result.check_id == "row_semantic_proof_complete"
                else result.observed_count
            ),
        )
        for result in prior_receipt.qa_receipt.results
    )
    qa = QaReceipt(
        qa_policy_id=spec.qa_policy.qa_policy_id,
        run_spec_id=spec.run_spec_id,
        source_binding_digest=spec.source_binding_digest,
        change_set_digest=change_digest,
        qa_available_session=spec.availability_cutoff_session,
        results=qa_results,
    )
    assert prior_receipt.checkpoint is not None
    checkpoint = CheckpointReceipt(
        artifact=prior_receipt.checkpoint.artifact,
        parent_release_id=None,
        run_spec_id=spec.run_spec_id,
        last_session=prior_receipt.checkpoint.last_session,
        resolved_content_digest=prior_receipt.checkpoint.resolved_content_digest,
        rebuild_basis_digest=checkpoint_rebuild_basis_from_change_digest(
            spec,
            change_set_digest=change_digest,
        ),
    )
    receipt = RunReceipt(
        run_spec_id=spec.run_spec_id,
        actual_input_set_digest=spec.source_binding_digest,
        output_set_digest=change_digest,
        qa_receipt=qa,
        checkpoint=checkpoint,
        succeeded=True,
        error_codes=(),
        receipt_available_session=spec.availability_cutoff_session,
        runtime_seconds=1,
        peak_rss_bytes=1,
        minimum_free_disk_bytes=spec.disk_floor_bytes,
    )
    manifest = replace(
        prior_manifest,
        added_row_version_receipts=(),
        superseded_row_version_ids=(),
        row_version_change_index=index,
        qa_receipt_id=qa.qa_receipt_id,
        run_spec_pin=control_object_pin(spec, path="control/indexed-run-spec.json"),
        run_receipt_pin=control_object_pin(receipt, path="control/indexed-run-receipt.json"),
    )
    pin = manifest.exact_pin(manifest_path="releases/indexed/manifest.json")
    return spec, receipt, manifest, pin


def test_inline_manifest_identity_and_change_digest_keep_legacy_shape() -> None:
    _spec, _receipt, manifest, _pin = _projection(rows=())
    assert "row_version_change_index" not in manifest.to_dict()
    assert manifest.release_id == stable_digest(manifest.release_identity_payload())
    legacy_change_payload = {
        "added_partition_receipts": [item.to_dict() for item in manifest.added_partition_receipts],
        "added_row_version_receipts": [
            item.to_dict() for item in manifest.added_row_version_receipts
        ],
        "partition_replacements": [],
        "superseded_row_version_ids": list(manifest.superseded_row_version_ids),
    }
    assert contract.release_change_set_digest(manifest) == stable_digest(legacy_change_payload)
    assert contract.release_change_set_digest(manifest) == logical_change_set_digest(
        added_partition_receipts=manifest.added_partition_receipts,
        partition_replacements=(),
        added_row_version_receipts=manifest.added_row_version_receipts,
        superseded_row_version_ids=manifest.superseded_row_version_ids,
        row_version_change_index=None,
    )


def test_index_pin_is_in_release_identity_and_change_digest() -> None:
    _spec, _receipt, manifest, _pin = _indexed_projection()
    assert manifest.to_dict()["row_version_change_index"] == _index().to_dict()
    changed = replace(manifest, row_version_change_index=_index(suffix="two"))
    assert changed.release_id != manifest.release_id
    assert contract.release_change_set_digest(changed) != contract.release_change_set_digest(
        manifest
    )


def test_indexed_and_inline_row_changes_are_mutually_exclusive() -> None:
    _spec, _receipt, inline, _pin = _projection(rows=(_row(),))
    with pytest.raises(IncrementalContractError, match="mutually exclusive"):
        replace(inline, row_version_change_index=_index())


def test_generic_validator_keeps_indexed_release_fail_closed() -> None:
    spec, receipt, manifest, pin = _indexed_projection()
    with pytest.raises(IncrementalContractError, match="dispatcher is disabled"):
        validate_release_projection(
            spec,
            receipt,
            manifest,
            manifest_pin=pin,
            parent_release=None,
        )
    candidate = contract._validate_i3_release_projection_after_row_attestation(
        spec,
        receipt,
        manifest,
        manifest_pin=pin,
        parent_release=None,
        row_semantic_attestation_digest=_digest("deep-index-replay"),
    )
    assert candidate.manifest is manifest


def test_index_availability_cannot_exceed_manifest_cutoff() -> None:
    _spec, _receipt, manifest, _pin = _indexed_projection()
    late = replace(
        manifest.row_version_change_index,
        availability_session=manifest.availability_cutoff_session.replace(
            year=manifest.availability_cutoff_session.year + 1
        ),
    )
    with pytest.raises(IncrementalContractError, match="availability"):
        replace(manifest, row_version_change_index=late)
