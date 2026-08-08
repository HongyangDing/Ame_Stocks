from __future__ import annotations

import json
from dataclasses import replace
from datetime import date
from pathlib import Path

import pytest
from test_silver_s7_5_i3_delta_io import _parent_fixture

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver import incremental_i7_checkpoint_compaction as compaction
from ame_stocks_api.silver.incremental_contract import ArtifactPin


def _write(root: Path, relative: str, content: bytes) -> ArtifactPin:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return compaction._pin_bytes(relative, content)


def _fixture(root: Path):
    (
        base_spec,
        native,
        _native_artifact,
        checkpoint,
        checkpoint_artifact,
        outputs,
    ) = _parent_fixture(root)
    top = _write(root, "manifests/fixtures/i6/top-event.json", b'{"top":true}\n')
    gate_c = _write(root, "manifests/fixtures/i6/gate-c.json", b'{"gate":"c"}\n')
    completion = _write(root, "manifests/fixtures/i3/completion.json", b'{"done":true}\n')
    deep = _write(root, "manifests/fixtures/i3/deep.json", b'{"deep":true}\n')
    source = compaction.I7CheckpointCompactionSource(
        snapshot_id=stable_digest({"fixture": "snapshot"}),
        release_id=stable_digest({"fixture": "gate-a-release"}),
        native_v2_release_id=native.release_id,
        checkpoint_id=checkpoint.checkpoint_id,
        terminal_session=checkpoint.last_session,
        producer_available_session=base_spec.run_available_session,
        source_binding_digest=stable_digest({"fixture": "source-binding"}),
        schema_bundle_digest=checkpoint.schema_digest,
        transform_semantics_digest=checkpoint.transform_semantics_digest,
        identity_policy_bundle_id=checkpoint.identity_policy_bundle.identity_policy_bundle_id,
        calendar_digest=checkpoint.calendar_digest,
        top_pointer_artifact=top,
        gate_c_approval_artifact=gate_c,
        release_completion_artifact=completion,
        deep_attestation_artifact=deep,
        checkpoint_artifact=checkpoint_artifact,
        table_outputs=outputs,
        authority_artifacts=(top, gate_c, completion, deep, checkpoint_artifact),
    )
    caps = compaction.I7CheckpointCompactionResourceCaps(
        peak_rss_bytes=8 * 1024**3,
        disk_free_bytes_floor=1,
        input_bytes_cap=1024**3,
        output_bytes_cap=1024**3,
        row_count_cap=1_000_000,
    )

    def loader(_root: Path) -> compaction.I7CheckpointCompactionSource:
        return source

    run_spec, run_pin = compaction._prepare_i7_checkpoint_compaction(
        root,
        completion_available_session=base_spec.run_available_session,
        resource_caps=caps,
        authority=compaction.I7_CHECKPOINT_COMPACTION_FIXTURE_AUTHORITY,
        source_loader=loader,
    )
    return source, checkpoint, outputs, loader, run_spec, run_pin


def test_prepare_stage_verify_compacts_small_tables_and_reuses_universe(
    tmp_path: Path,
) -> None:
    source, checkpoint, source_outputs, loader, run_spec, run_pin = _fixture(tmp_path)
    staged = compaction._stage_i7_checkpoint_compaction(
        tmp_path,
        run_pin,
        authority=compaction.I7_CHECKPOINT_COMPACTION_FIXTURE_AUTHORITY,
        source_loader=loader,
    )
    assert staged.completion.state == "awaiting_review"
    assert staged.completion.publish_authorized is False
    assert staged.compacted_manifest.parent_release_id == source.native_v2_release_id
    assert staged.compacted_manifest.source_checkpoint_id == checkpoint.checkpoint_id
    assert staged.compacted_checkpoint.parent_release.release_id == (
        staged.compacted_manifest.release_id
    )
    source_by_name = compaction._output_by_name(source_outputs)
    compacted = compaction._output_by_name(staged.output_artifacts)
    for table in compaction._SMALL_TABLES:
        assert compacted[table].rowset_index is not None
        assert len(compacted[table].rowset_index.segments) == 1
        assert compacted[table].rowset_index.segments[0].artifact not in {
            item.artifact for item in source_by_name[table].rowset_index.segments
        }
    assert compacted["universe_daily"].dataset_index.partitions == (
        source_by_name["universe_daily"].dataset_index.partitions
    )
    assert compacted["universe_daily"].manifest_output.artifact != (
        source_by_name["universe_daily"].manifest_output.artifact
    )
    verified = compaction._verify_i7_checkpoint_compaction(
        tmp_path,
        staged.completion_artifact,
        authority=compaction.I7_CHECKPOINT_COMPACTION_FIXTURE_AUTHORITY,
        source_loader=loader,
    )
    assert verified.reused is True
    assert verified.compacted_checkpoint == staged.compacted_checkpoint
    replay = compaction._stage_i7_checkpoint_compaction(
        tmp_path,
        run_pin,
        authority=compaction.I7_CHECKPOINT_COMPACTION_FIXTURE_AUTHORITY,
        source_loader=loader,
    )
    assert replay.reused is True
    assert replay.completion_artifact == staged.completion_artifact
    assert run_spec.run_spec_id == staged.completion.run_spec_id


def test_tampered_compacted_segment_is_rejected(tmp_path: Path) -> None:
    _source, _checkpoint, _outputs, loader, _spec, run_pin = _fixture(tmp_path)
    staged = compaction._stage_i7_checkpoint_compaction(
        tmp_path,
        run_pin,
        authority=compaction.I7_CHECKPOINT_COMPACTION_FIXTURE_AUTHORITY,
        source_loader=loader,
    )
    segment = staged.output_artifacts[0].rowset_index.segments[0].artifact
    path = tmp_path / segment.path
    path.chmod(0o644)
    content = bytearray(path.read_bytes())
    content[-1] ^= 1
    path.write_bytes(content)
    with pytest.raises(compaction.I7CheckpointCompactionError, match="exact pin differs"):
        compaction._verify_i7_checkpoint_compaction(
            tmp_path,
            staged.completion_artifact,
            authority=compaction.I7_CHECKPOINT_COMPACTION_FIXTURE_AUTHORITY,
            source_loader=loader,
        )


def test_source_change_after_prepare_is_rejected_without_completion(tmp_path: Path) -> None:
    source, _checkpoint, _outputs, _loader, run_spec, run_pin = _fixture(tmp_path)
    changed = replace(source, snapshot_id=stable_digest({"fixture": "changed"}))
    with pytest.raises(compaction.I7CheckpointCompactionError, match="source changed"):
        compaction._stage_i7_checkpoint_compaction(
            tmp_path,
            run_pin,
            authority=compaction.I7_CHECKPOINT_COMPACTION_FIXTURE_AUTHORITY,
            source_loader=lambda _root: changed,
        )
    assert not (
        tmp_path
        / compaction._completion_path(
            run_spec.run_spec_id,
            authority=compaction.I7_CHECKPOINT_COMPACTION_FIXTURE_AUTHORITY,
        )
    ).exists()


def test_resource_failure_leaves_no_visible_completion(tmp_path: Path) -> None:
    source, _checkpoint, _outputs, _loader, run_spec, _run_pin = _fixture(tmp_path)
    strict = replace(
        run_spec,
        resource_caps=replace(run_spec.resource_caps, input_bytes_cap=1),
    )
    strict_pin = compaction._write_immutable(
        tmp_path,
        compaction._run_spec_path(
            strict.run_spec_id,
            authority=compaction.I7_CHECKPOINT_COMPACTION_FIXTURE_AUTHORITY,
        ),
        strict.canonical_bytes(),
        "strict RunSpec",
    )
    with pytest.raises(compaction.I7CheckpointCompactionError, match="input"):
        compaction._stage_i7_checkpoint_compaction(
            tmp_path,
            strict_pin,
            authority=compaction.I7_CHECKPOINT_COMPACTION_FIXTURE_AUTHORITY,
            source_loader=lambda _root: source,
        )
    assert not (
        tmp_path
        / compaction._completion_path(
            strict.run_spec_id,
            authority=compaction.I7_CHECKPOINT_COMPACTION_FIXTURE_AUTHORITY,
        )
    ).exists()


@pytest.mark.parametrize("kind", ["run-spec", "completion"])
def test_noncanonical_control_locator_is_rejected_before_read(
    tmp_path: Path,
    kind: str,
) -> None:
    _source, _checkpoint, _outputs, loader, _run_spec, run_pin = _fixture(tmp_path)
    if kind == "completion":
        staged = compaction._stage_i7_checkpoint_compaction(
            tmp_path,
            run_pin,
            authority=compaction.I7_CHECKPOINT_COMPACTION_FIXTURE_AUTHORITY,
            source_loader=loader,
        )
        original = staged.completion_artifact
    else:
        original = run_pin
    copied = _write(
        tmp_path, f"manifests/fixtures/latest/{kind}.json", (tmp_path / original.path).read_bytes()
    )
    with pytest.raises(compaction.I7CheckpointCompactionError, match="noncanonical"):
        if kind == "run-spec":
            compaction._stage_i7_checkpoint_compaction(
                tmp_path,
                copied,
                authority=compaction.I7_CHECKPOINT_COMPACTION_FIXTURE_AUTHORITY,
                source_loader=loader,
            )
        else:
            compaction._verify_i7_checkpoint_compaction(
                tmp_path,
                copied,
                authority=compaction.I7_CHECKPOINT_COMPACTION_FIXTURE_AUTHORITY,
                source_loader=loader,
            )


def test_noncanonical_completion_bytes_are_rejected(tmp_path: Path) -> None:
    _source, _checkpoint, _outputs, loader, _spec, run_pin = _fixture(tmp_path)
    staged = compaction._stage_i7_checkpoint_compaction(
        tmp_path,
        run_pin,
        authority=compaction.I7_CHECKPOINT_COMPACTION_FIXTURE_AUTHORITY,
        source_loader=loader,
    )
    path = tmp_path / staged.completion_artifact.path
    document = json.loads(path.read_bytes())
    content = json.dumps(document, indent=2, sort_keys=True).encode() + b"\n"
    path.unlink()
    path.write_bytes(content)
    repinned = compaction._pin_bytes(staged.completion_artifact.path, content)
    with pytest.raises(compaction.I7CheckpointCompactionError, match="canonical"):
        compaction._verify_i7_checkpoint_compaction(
            tmp_path,
            repinned,
            authority=compaction.I7_CHECKPOINT_COMPACTION_FIXTURE_AUTHORITY,
            source_loader=loader,
        )


def test_completion_availability_cannot_predate_source(tmp_path: Path) -> None:
    source, _checkpoint, _outputs, _loader, _spec, _pin = _fixture(tmp_path)
    with pytest.raises(compaction.I7CheckpointCompactionError, match="predates"):
        compaction._prepare_i7_checkpoint_compaction(
            tmp_path,
            completion_available_session=date(2020, 1, 1),
            resource_caps=compaction.I7CheckpointCompactionResourceCaps(
                peak_rss_bytes=8 * 1024**3,
                disk_free_bytes_floor=1,
                input_bytes_cap=1024**3,
                output_bytes_cap=1024**3,
                row_count_cap=1_000_000,
            ),
            authority=compaction.I7_CHECKPOINT_COMPACTION_FIXTURE_AUTHORITY,
            source_loader=lambda _root: source,
        )
