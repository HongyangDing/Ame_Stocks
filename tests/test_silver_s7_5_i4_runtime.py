from __future__ import annotations

import fcntl
import json
import os
from dataclasses import replace
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from test_silver_s7_5_i4_correction import _canonical_bytes, _production_case

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver import incremental_i4_runtime as i4_runtime
from ame_stocks_api.silver.incremental_contract import (
    ArtifactPin,
    control_object_pin,
    input_set_digest,
)
from ame_stocks_api.silver.incremental_i4_correction import (
    ExactGroupExpansionRequired,
    ExactIdentityGroup,
    I4CorrectionError,
)
from ame_stocks_api.silver.incremental_i4_runtime import (
    I4CorrectionPrepareConfig,
    I4RuntimeAuthorities,
    I4RuntimeError,
    I4RuntimeResourceCaps,
    load_i4_runtime_authorities_exact,
    prepare_i4_correction_run,
    stage_i4_correction,
    verify_i4_correction,
)


def _pin(path: str, content: bytes) -> ArtifactPin:
    import hashlib

    return ArtifactPin(path=path, sha256=hashlib.sha256(content).hexdigest(), bytes=len(content))


def _write(root: Path, pin: ArtifactPin, content: bytes) -> None:
    path = root / pin.path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def _runtime_case(tmp_path: Path, *, disk_floor_bytes: int = 1) -> SimpleNamespace:
    case = _production_case()
    for relative, content in case.artifacts.items():
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)

    prior_content = _canonical_bytes(case.kwargs["prior_policy_snapshot"].to_dict())
    target_content = _canonical_bytes(case.target_policy.to_dict())
    prior_pin = _pin("runtime/i4/prior-policy-snapshot.json", prior_content)
    target_pin = _pin("runtime/i4/target-policy-snapshot.json", target_content)
    _write(tmp_path, prior_pin, prior_content)
    _write(tmp_path, target_pin, target_content)

    row_receipts = case.kwargs["added_row_version_receipts"]
    authorities = I4RuntimeAuthorities(
        parent_manifest=case.parent_manifest,
        parent_run_receipt=case.kwargs["parent_run_receipt"],
        checkpoint=case.checkpoint,
        prior_policy_snapshot=case.kwargs["prior_policy_snapshot"],
        target_policy_snapshot=case.target_policy,
        registry_ledger=case.registry_ledger,
        alias_state_ledger=case.alias_state_ledger,
        authorization=case.authorization,
        approval_event=case.approval_event,
        approval_ledger=case.kwargs["approval_ledger"],
        added_row_version_receipts=row_receipts,
        superseded_row_version_ids=case.kwargs["superseded_row_version_ids"],
    )
    authorization = case.authorization.artifact
    event = case.approval_event_pin
    approval_ledger = case.kwargs["approval_ledger_artifact"]
    config = I4CorrectionPrepareConfig(
        parent_manifest_pin=case.kwargs["parent_manifest_pin"],
        parent_run_receipt_artifact=case.parent_manifest.run_receipt_pin.artifact,
        parent_checkpoint_artifact=case.kwargs["parent_checkpoint_artifact"],
        replacement_partition_receipts=case.kwargs["replacement_partition_receipts"],
        prior_policy_snapshot_artifact=prior_pin,
        target_policy_snapshot_artifact=target_pin,
        target_policy_bundle_artifact=case.target_bundle_pin,
        registry_ledger_artifact=case.registry_ledger_pin,
        alias_state_ledger_artifact=case.alias_state_ledger_pin,
        authorization_artifact=ArtifactPin(
            authorization.path, authorization.sha256, authorization.bytes
        ),
        approval_event_artifact=ArtifactPin(event.path, event.sha256, event.bytes),
        approval_ledger_artifact=ArtifactPin(
            approval_ledger.path,
            approval_ledger.sha256,
            approval_ledger.bytes,
        ),
        alias_row_artifact=case.alias_row_pin,
        alias_proof_artifact=case.alias_proof_pin,
        row_receipt_digest=stable_digest([item.to_dict() for item in row_receipts]),
        availability_cutoff_session=case.kwargs["availability_cutoff_session"],
        resource_caps=I4RuntimeResourceCaps(
            rss_cap_bytes=10**12,
            disk_floor_bytes=disk_floor_bytes,
            wall_clock_cap_seconds=30,
        ),
    )
    return SimpleNamespace(case=case, config=config, authorities=authorities)


def _load(root: Path, pin: ArtifactPin) -> dict[str, object]:
    return json.loads((root / pin.path).read_text())


def _rewrite_control(
    root: Path,
    pin: ArtifactPin,
    document: dict[str, object],
    *,
    canonical: bool,
) -> ArtifactPin:
    content = (
        _canonical_bytes(document)
        if canonical
        else json.dumps(document, indent=2, ensure_ascii=False).encode() + b"\n"
    )
    path = root / pin.path
    path.chmod(0o644)
    path.write_bytes(content)
    return _pin(pin.path, content)


def _resign(document: dict[str, object], id_field: str) -> dict[str, object]:
    body = dict(document)
    body.pop(id_field)
    return {id_field: stable_digest(body), **body}


def test_runtime_stages_azpn_shaped_registry_correction_awaiting_review(
    tmp_path: Path,
) -> None:
    fixture = _runtime_case(tmp_path)
    parent_bytes = {
        item.artifact.path: (tmp_path / item.artifact.path).read_bytes()
        for item in fixture.case.checkpoint.resolved_partition_map
    }
    prepared = prepare_i4_correction_run(
        tmp_path,
        fixture.config,
        authorities=fixture.authorities,
    )

    result = stage_i4_correction(
        tmp_path,
        prepared.run_spec_artifact,
        authorities=fixture.authorities,
    )
    completion = _load(tmp_path, result.completion_pin)
    checkpoint = _load(tmp_path, result.checkpoint_candidate_pin)
    release = _load(tmp_path, result.release_candidate_pin)

    assert completion["state"] == "awaiting_review"
    assert checkpoint["state"] == "awaiting_review"
    assert release["state"] == "awaiting_review"
    assert release["publish_authority"] is False
    assert result.capability.exact_scope.group.ticker == "AAPL"
    assert result.capability.exact_scope.recompute_sessions
    assert (
        fixture.case.checkpoint.resolved_partition_map[0].to_dict()
        in checkpoint["resolved_partition_map"]
    )
    assert all((tmp_path / path).read_bytes() == content for path, content in parent_bytes.items())
    first_replacement = fixture.config.replacement_partition_receipts[0]
    first_parent = next(
        item
        for item in fixture.case.checkpoint.resolved_partition_map
        if item.session_date.isoformat() == first_replacement.partition_key
    )
    parent_rows = pq.read_table(
        pa.BufferReader((tmp_path / first_parent.artifact.path).read_bytes())
    ).to_pylist()
    replacement_rows = pq.read_table(
        pa.BufferReader((tmp_path / first_replacement.receipt.path).read_bytes())
    ).to_pylist()
    parent_cross_market = next(row for row in parent_rows if row["ticker"] == "MSFT")
    replacement_cross_market = next(row for row in replacement_rows if row["ticker"] == "MSFT")
    assert parent_cross_market["observed_composite_market_code"] == "GB"
    for field in (
        "observed_composite_figi",
        "canonical_composite_figi",
        "canonical_share_class_figi",
        "asset_id",
        "share_class_id",
        "issuer_id",
        "alias_segment_id",
        "alias_resolution_version_id",
        "cross_market_adjudication_id",
    ):
        assert replacement_cross_market[field] == parent_cross_market[field]
    assert not any("pointer" in path.name or "latest" in path.name for path in tmp_path.rglob("*"))
    for pin in (
        prepared.config_artifact,
        prepared.run_spec_artifact,
        result.checkpoint_candidate_pin,
        result.release_candidate_pin,
        result.completion_pin,
    ):
        assert (tmp_path / pin.path).stat().st_mode & 0o222 == 0

    replayed = verify_i4_correction(
        tmp_path,
        result.completion_pin,
        authorities=fixture.authorities,
    )
    reused = stage_i4_correction(
        tmp_path,
        prepared.run_spec_artifact,
        authorities=fixture.authorities,
    )
    assert replayed.capability.to_dict() == result.capability.to_dict()
    assert reused.reused is True
    assert reused.completion_pin == result.completion_pin


def test_runtime_authorities_replay_from_exact_bytes_without_caller_objects(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _runtime_case(tmp_path)
    prepared = prepare_i4_correction_run(
        tmp_path,
        fixture.config,
        authorities=fixture.authorities,
    )
    result = stage_i4_correction(
        tmp_path,
        prepared.run_spec_artifact,
        authorities=fixture.authorities,
    )

    def load_policy(_root: Path, *, bundle: object, snapshot_artifact: object, label: str):
        del bundle, snapshot_artifact
        return (
            fixture.authorities.prior_policy_snapshot
            if label == "prior"
            else fixture.authorities.target_policy_snapshot
        )

    monkeypatch.setattr(i4_runtime, "_load_policy_snapshot_exact", load_policy)
    loaded = load_i4_runtime_authorities_exact(tmp_path, result.completion_pin)

    assert loaded.run_spec == prepared.run_spec
    assert loaded.config == fixture.config
    assert loaded.authorities.authority_digest == fixture.authorities.authority_digest
    replay = verify_i4_correction(
        tmp_path,
        result.completion_pin,
        authorities=loaded.authorities,
    )
    assert replay.capability.to_dict() == result.capability.to_dict()


def test_runtime_prepare_stage_verify_have_an_exact_no_caller_authority_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _runtime_case(tmp_path)

    def load_policy(_root: Path, *, bundle: object, snapshot_artifact: object, label: str):
        del bundle, snapshot_artifact
        return (
            fixture.authorities.prior_policy_snapshot
            if label == "prior"
            else fixture.authorities.target_policy_snapshot
        )

    monkeypatch.setattr(i4_runtime, "_load_policy_snapshot_exact", load_policy)
    monkeypatch.setattr(
        i4_runtime,
        "_prior_policy_bundle_from_parent",
        lambda *_args, **_kwargs: fixture.authorities.prior_policy_snapshot.policy_bundle,
    )
    prepared = prepare_i4_correction_run(tmp_path, fixture.config)
    staged = stage_i4_correction(tmp_path, prepared.run_spec_artifact)
    replayed = verify_i4_correction(tmp_path, staged.completion_pin)

    assert prepared.run_spec.authority_digest == fixture.authorities.authority_digest
    assert staged.capability.to_dict() == replayed.capability.to_dict()
    assert replayed.reused is False


def test_runtime_authority_loader_rejects_envelope_tamper_before_policy_replay(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _runtime_case(tmp_path)
    prepared = prepare_i4_correction_run(
        tmp_path,
        fixture.config,
        authorities=fixture.authorities,
    )
    result = stage_i4_correction(
        tmp_path,
        prepared.run_spec_artifact,
        authorities=fixture.authorities,
    )
    path = tmp_path / prepared.run_spec.authority_artifact.path
    path.chmod(0o644)
    path.write_bytes(path.read_bytes() + b"tamper")
    monkeypatch.setattr(
        i4_runtime,
        "_load_policy_snapshot_exact",
        lambda *_args, **_kwargs: pytest.fail("policy replay must not run after pin tamper"),
    )

    with pytest.raises(I4RuntimeError, match="exact pin"):
        load_i4_runtime_authorities_exact(tmp_path, result.completion_pin)


def test_runtime_prior_policy_bundle_is_derived_from_exact_parent_gate_a_inputs(
    tmp_path: Path,
) -> None:
    from test_silver_s7_5_incremental_contract import _projection

    fixture = _runtime_case(tmp_path)
    bundle = fixture.authorities.prior_policy_snapshot.policy_bundle
    bundle_pin = fixture.case.checkpoint.identity_policy_bundle_artifact
    seed_spec, _receipt, _manifest, _pin = _projection()
    gate_spec = replace(
        seed_spec,
        input_pins=(bundle_pin,),
        source_binding_digest=input_set_digest((bundle_pin,)),
        identity_policy_bundle_id=bundle.identity_policy_bundle_id,
    )
    gate_pin = control_object_pin(gate_spec, path="runtime/i4/parent/gate-a-run-spec.json")
    parent_manifest = replace(
        fixture.case.parent_manifest,
        identity_policy_bundle_id=bundle.identity_policy_bundle_id,
        run_spec_pin=gate_pin,
    )
    parent_pin = parent_manifest.exact_pin(manifest_path="runtime/i4/parent/gate-a-manifest.json")
    _write(tmp_path, gate_pin.artifact, _canonical_bytes(gate_spec.to_dict()))
    _write(
        tmp_path,
        ArtifactPin(
            path=parent_pin.manifest_path,
            sha256=parent_pin.manifest_sha256,
            bytes=parent_pin.manifest_bytes,
        ),
        parent_manifest.canonical_bytes(),
    )
    config = replace(fixture.config, parent_manifest_pin=parent_pin)

    assert i4_runtime._prior_policy_bundle_from_parent(tmp_path, config) == bundle


@pytest.mark.parametrize(
    ("field", "message"),
    (
        ("authorization_artifact", "another authorization artifact"),
        ("parent_run_receipt_artifact", "another parent run-receipt artifact"),
    ),
)
def test_runtime_prepare_rejects_exact_byte_decoy_authority_path(
    tmp_path: Path,
    field: str,
    message: str,
) -> None:
    fixture = _runtime_case(tmp_path)
    authentic = getattr(fixture.config, field)
    content = (tmp_path / authentic.path).read_bytes()
    decoy = _pin(f"runtime/i4/decoys/{field}.json", content)
    _write(tmp_path, decoy, content)
    forged = replace(fixture.config, **{field: decoy})

    with pytest.raises(I4RuntimeError, match=message):
        prepare_i4_correction_run(
            tmp_path,
            forged,
            authorities=fixture.authorities,
        )


def test_runtime_replacement_inputs_have_no_bytes_mode_or_hardlink_side_effect(
    tmp_path: Path,
) -> None:
    fixture = _runtime_case(tmp_path)
    replacement = fixture.config.replacement_partition_receipts[0].receipt
    replacement_path = tmp_path / replacement.path
    hardlink_path = tmp_path / "runtime/i4/hardlinks/replacement.parquet"
    hardlink_path.parent.mkdir(parents=True)
    os.link(replacement_path, hardlink_path)
    before = (
        replacement_path.read_bytes(),
        replacement_path.stat().st_mode,
        replacement_path.stat().st_ino,
        replacement_path.stat().st_nlink,
        hardlink_path.read_bytes(),
        hardlink_path.stat().st_mode,
        hardlink_path.stat().st_ino,
        hardlink_path.stat().st_nlink,
    )

    prepared = prepare_i4_correction_run(tmp_path, fixture.config, authorities=fixture.authorities)
    stage_i4_correction(tmp_path, prepared.run_spec_artifact, authorities=fixture.authorities)

    after = (
        replacement_path.read_bytes(),
        replacement_path.stat().st_mode,
        replacement_path.stat().st_ino,
        replacement_path.stat().st_nlink,
        hardlink_path.read_bytes(),
        hardlink_path.stat().st_mode,
        hardlink_path.stat().st_ino,
        hardlink_path.stat().st_nlink,
    )
    assert after == before


def test_runtime_tamper_or_missing_approval_writes_failed_receipt(tmp_path: Path) -> None:
    fixture = _runtime_case(tmp_path)
    prepared = prepare_i4_correction_run(tmp_path, fixture.config, authorities=fixture.authorities)
    target = tmp_path / fixture.config.replacement_partition_receipts[0].receipt.path
    os.chmod(target, 0o644)
    target.write_bytes(target.read_bytes() + b"tamper")

    with pytest.raises(I4RuntimeError, match="staging failed") as caught:
        stage_i4_correction(tmp_path, prepared.run_spec_artifact, authorities=fixture.authorities)
    assert caught.value.failed_receipt_pin is not None
    assert _load(tmp_path, caught.value.failed_receipt_pin)["state"] == "failed"

    second_root = tmp_path / "missing-approval"
    second = _runtime_case(second_root)
    prepared_second = prepare_i4_correction_run(
        second_root, second.config, authorities=second.authorities
    )
    (second_root / second.config.approval_ledger_artifact.path).unlink()
    with pytest.raises(I4RuntimeError) as missing:
        stage_i4_correction(
            second_root,
            prepared_second.run_spec_artifact,
            authorities=second.authorities,
        )
    assert missing.value.failed_receipt_pin is not None


def test_runtime_resources_lock_late_source_and_genesis_fail_closed(tmp_path: Path) -> None:
    fixture = _runtime_case(tmp_path, disk_floor_bytes=10**18)
    prepared = prepare_i4_correction_run(tmp_path, fixture.config, authorities=fixture.authorities)
    with pytest.raises(I4RuntimeError, match="disk hard floor") as resource_failure:
        stage_i4_correction(tmp_path, prepared.run_spec_artifact, authorities=fixture.authorities)
    assert resource_failure.value.failed_receipt_pin is not None

    with pytest.raises(I4RuntimeError, match="late-source runtime is disabled"):
        replace(fixture.config, correction_cause="late_source")
    with pytest.raises(I4CorrectionError, match="genesis-only"):
        replace(fixture.case.registry_ledger, release_sequence=True)

    lock_root = tmp_path / "locked"
    locked = _runtime_case(lock_root)
    locked_prepared = prepare_i4_correction_run(
        lock_root, locked.config, authorities=locked.authorities
    )
    lock_path = (
        lock_root
        / "locks/silver/identity/s7-5-i4-correction-staging"
        / f"run_spec_id={locked_prepared.run_spec.run_spec_id}.lock"
    )
    lock_path.parent.mkdir(parents=True)
    with lock_path.open("a+b") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(I4RuntimeError, match="nonblocking lock"):
            stage_i4_correction(
                lock_root,
                locked_prepared.run_spec_artifact,
                authorities=locked.authorities,
            )


def test_runtime_stable_boundary_expansion_fails_without_completion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _runtime_case(tmp_path)
    prepared = prepare_i4_correction_run(tmp_path, fixture.config, authorities=fixture.authorities)

    def require_more_history(**_: object) -> None:
        raise ExactGroupExpansionRequired(
            ExactIdentityGroup("polygon", "stocks", "us", "AAPL"),
            from_session=date(2026, 7, 7),
            reason="fixture exact-group history never reaches a stable boundary",
        )

    monkeypatch.setattr(
        i4_runtime,
        "mint_production_i4_correction_capability",
        require_more_history,
    )
    with pytest.raises(I4RuntimeError, match="ExactGroupExpansionRequired") as caught:
        stage_i4_correction(
            tmp_path,
            prepared.run_spec_artifact,
            authorities=fixture.authorities,
        )

    assert caught.value.failed_receipt_pin is not None
    run_root = (
        tmp_path
        / "manifests/silver/identity/s7-5-i4-correction-staging/runs"
        / f"run_spec_id={prepared.run_spec.run_spec_id}"
    )
    assert not (run_root / "completion.json").exists()
    assert not (run_root / "checkpoint-candidate.json").exists()
    assert not (run_root / "release-candidate.json").exists()


def test_runtime_never_clobbers_preexisting_candidate(tmp_path: Path) -> None:
    fixture = _runtime_case(tmp_path)
    prepared = prepare_i4_correction_run(tmp_path, fixture.config, authorities=fixture.authorities)
    candidate = (
        tmp_path
        / "manifests/silver/identity/s7-5-i4-correction-staging/runs"
        / f"run_spec_id={prepared.run_spec.run_spec_id}"
        / "checkpoint-candidate.json"
    )
    candidate.parent.mkdir(parents=True)
    sentinel = b'{"foreign":"owner"}\n'
    candidate.write_bytes(sentinel)

    with pytest.raises(I4RuntimeError, match="refusing to overwrite") as caught:
        stage_i4_correction(
            tmp_path,
            prepared.run_spec_artifact,
            authorities=fixture.authorities,
        )

    assert candidate.read_bytes() == sentinel
    assert caught.value.failed_receipt_pin is not None
    assert not (candidate.parent / "completion.json").exists()
    assert not (candidate.parent / "release-candidate.json").exists()


def test_runtime_verify_rejects_jointly_resigned_candidate_tamper(tmp_path: Path) -> None:
    fixture = _runtime_case(tmp_path)
    prepared = prepare_i4_correction_run(tmp_path, fixture.config, authorities=fixture.authorities)
    result = stage_i4_correction(
        tmp_path, prepared.run_spec_artifact, authorities=fixture.authorities
    )

    checkpoint = _load(tmp_path, result.checkpoint_candidate_pin)
    checkpoint["unaffected_partition_receipts_digest"] = stable_digest("forged-unaffected-frontier")
    checkpoint.pop("checkpoint_candidate_id")
    checkpoint = {"checkpoint_candidate_id": stable_digest(checkpoint), **checkpoint}
    checkpoint_content = _canonical_bytes(checkpoint)
    checkpoint_path = tmp_path / result.checkpoint_candidate_pin.path
    checkpoint_path.chmod(0o644)
    checkpoint_path.write_bytes(checkpoint_content)
    checkpoint_pin = _pin(result.checkpoint_candidate_pin.path, checkpoint_content)

    release = _load(tmp_path, result.release_candidate_pin)
    release["checkpoint_candidate"] = checkpoint_pin.to_dict()
    release.pop("release_candidate_id")
    release = {"release_candidate_id": stable_digest(release), **release}
    release_content = _canonical_bytes(release)
    release_path = tmp_path / result.release_candidate_pin.path
    release_path.chmod(0o644)
    release_path.write_bytes(release_content)
    release_pin = _pin(result.release_candidate_pin.path, release_content)

    completion = _load(tmp_path, result.completion_pin)
    completion["checkpoint_candidate"] = checkpoint_pin.to_dict()
    completion["release_candidate"] = release_pin.to_dict()
    completion.pop("completion_id")
    completion["completion_id"] = stable_digest(completion)
    completion_content = _canonical_bytes(completion)
    completion_path = tmp_path / result.completion_pin.path
    completion_path.chmod(0o644)
    completion_path.write_bytes(completion_content)
    completion_pin = _pin(result.completion_pin.path, completion_content)

    with pytest.raises(I4RuntimeError, match="checkpoint candidate does not reproduce"):
        verify_i4_correction(
            tmp_path,
            completion_pin,
            authorities=fixture.authorities,
        )


@pytest.mark.parametrize(
    ("artifact_kind", "message"),
    (
        ("checkpoint", "checkpoint candidate bytes are not canonical"),
        ("release", "release candidate bytes are not canonical"),
        ("completion", "correction completion bytes are not canonical"),
    ),
)
def test_runtime_verify_rejects_noncanonical_exact_resigned_controls(
    tmp_path: Path,
    artifact_kind: str,
    message: str,
) -> None:
    root = tmp_path / artifact_kind
    fixture = _runtime_case(root)
    prepared = prepare_i4_correction_run(root, fixture.config, authorities=fixture.authorities)
    result = stage_i4_correction(root, prepared.run_spec_artifact, authorities=fixture.authorities)
    checkpoint_pin = result.checkpoint_candidate_pin
    release_pin = result.release_candidate_pin
    completion_pin = result.completion_pin

    if artifact_kind == "checkpoint":
        checkpoint_pin = _rewrite_control(
            root,
            checkpoint_pin,
            _load(root, checkpoint_pin),
            canonical=False,
        )
        release = _load(root, release_pin)
        release["checkpoint_candidate"] = checkpoint_pin.to_dict()
        release_pin = _rewrite_control(
            root,
            release_pin,
            _resign(release, "release_candidate_id"),
            canonical=True,
        )
        completion = _load(root, completion_pin)
        completion["checkpoint_candidate"] = checkpoint_pin.to_dict()
        completion["release_candidate"] = release_pin.to_dict()
        completion_pin = _rewrite_control(
            root,
            completion_pin,
            _resign(completion, "completion_id"),
            canonical=True,
        )
    elif artifact_kind == "release":
        release_pin = _rewrite_control(
            root,
            release_pin,
            _load(root, release_pin),
            canonical=False,
        )
        completion = _load(root, completion_pin)
        completion["release_candidate"] = release_pin.to_dict()
        completion_pin = _rewrite_control(
            root,
            completion_pin,
            _resign(completion, "completion_id"),
            canonical=True,
        )
    else:
        completion_pin = _rewrite_control(
            root,
            completion_pin,
            _load(root, completion_pin),
            canonical=False,
        )

    with pytest.raises(I4RuntimeError, match=message):
        verify_i4_correction(
            root,
            completion_pin,
            authorities=fixture.authorities,
        )


def test_runtime_config_decoder_rejects_bool_resource_cap(tmp_path: Path) -> None:
    fixture = _runtime_case(tmp_path)
    document = fixture.config.to_dict()
    caps = dict(document["resource_caps"])
    caps["rss_cap_bytes"] = True
    document["resource_caps"] = caps

    with pytest.raises(I4RuntimeError, match="RSS cap must be a positive integer"):
        I4CorrectionPrepareConfig.from_dict(document)
