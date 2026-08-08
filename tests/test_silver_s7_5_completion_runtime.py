from __future__ import annotations

import hashlib
from dataclasses import replace
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest
from test_silver_s7_5_i5_lifecycle import _chain

from ame_stocks_api.artifacts import ArtifactError, stable_digest
from ame_stocks_api.silver import incremental_s75_completion_runtime as runtime
from ame_stocks_api.silver.incremental_contract import ArtifactPin


def _write(root: Path, relative: str, content: bytes) -> ArtifactPin:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return ArtifactPin(
        path=relative,
        sha256=hashlib.sha256(content).hexdigest(),
        bytes=len(content),
    )


def _materialize_lifecycle_artifacts(root: Path, values: tuple[object, ...]) -> None:
    (
        spec,
        receipt,
        gate_b,
        shadow,
        rollback_event,
        rollback,
        gate_c,
        top,
        full_spec,
        full,
        _completion,
    ) = values
    del spec, shadow, rollback_event, top, full_spec
    exact = {
        gate_b.artifact.path: runtime._canonical(gate_b.approval.to_dict()),
        gate_c.artifact.path: runtime._canonical(gate_c.approval.to_dict()),
    }
    pins = [
        *(item.details_artifact for item in receipt.comparisons),
        *(item.details_artifact for item in receipt.failure_recovery),
        rollback.details_artifact,
        full.qa_artifact,
        full.details_artifact,
        *(item.details_artifact for table in full.table_evidence for item in table.partitions),
    ]
    for pin in pins:
        label = pin.path.rsplit("/", 1)[-1].removesuffix(".json")
        exact[pin.path] = f"{label}\n".encode()
    for relative, content in exact.items():
        written = _write(root, relative, content)
        expected = gate_b.artifact if relative == gate_b.artifact.path else None
        if relative == gate_c.artifact.path:
            expected = gate_c.artifact
        if expected is not None:
            assert written == expected


def _fixture(root: Path, monkeypatch: pytest.MonkeyPatch):
    values = _chain()
    (
        spec,
        receipt,
        gate_b,
        shadow,
        rollback_event,
        rollback,
        gate_c,
        top,
        full_spec,
        full,
        _completion,
    ) = values
    _materialize_lifecycle_artifacts(root, values)
    i4 = _write(
        root,
        "manifests/silver/incremental/i4/corrections/run_spec_id="
        f"{stable_digest({'fixture': 'i4-run'})}/completion.json",
        b'{"i4":true}\n',
    )
    i5 = _write(
        root,
        "manifests/silver/incremental/i5/shadow-equivalence/completions/run_spec_id="
        f"{stable_digest({'fixture': 'i5-run'})}/manifest.json",
        b'{"i5":true}\n',
    )
    i7 = _write(
        root,
        "manifests/silver/incremental/i7/completions/run_spec_id="
        f"{stable_digest({'fixture': 'i7-run'})}/manifest.json",
        b'{"i7":true}\n',
    )
    evidence = runtime._VerifiedEvidence(
        legacy_s7_release_set_id=stable_digest({"fixture": "legacy"}),
        gate_a_approval_id=runtime.GATE_A_APPROVAL_ID,
        i2_acceptance_receipt_id=stable_digest({"fixture": "i2"}),
        i3_acceptance_receipt_id=stable_digest({"fixture": "i3"}),
        i4_acceptance_receipt_id=stable_digest({"fixture": "i4"}),
        i5_spec=SimpleNamespace(lifecycle_spec=spec),
        i5_completion=SimpleNamespace(receipt=receipt),
        i6_snapshot=SimpleNamespace(
            gate_b_approval=gate_b.approval,
            gate_b_approval_artifact=gate_b.artifact,
            gate_c_approval=gate_c.approval,
            gate_c_approval_artifact=gate_c.artifact,
            shadow_pointer_event=shadow,
            rollback_pointer_event=rollback_event,
            rollback_receipt=rollback,
            research_top_event=top,
            release_id=top.new_release_id,
        ),
        i7_result=SimpleNamespace(
            run_spec=SimpleNamespace(lifecycle_spec=full_spec),
            completion=SimpleNamespace(receipt=full),
        ),
        evidence_artifacts=(i4, i5, i7, gate_b.artifact, gate_c.artifact),
    )
    monkeypatch.setattr(runtime, "_load_production_evidence", lambda *_args, **_kwargs: evidence)
    config = runtime.S75CompletionConfig(
        i4_completion_artifact=i4,
        i5_completion_artifact=i5,
        i7_completion_artifact=i7,
        completion_available_session=full.receipt_available_session,
    )
    return config, evidence


def test_prepare_stage_verify_writes_exact_final_sentinel(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, _evidence = _fixture(tmp_path, monkeypatch)
    _config, config_pin = runtime.prepare_s75_completion(tmp_path, config)
    result = runtime.stage_s75_completion(tmp_path, config_pin)
    assert result.lifecycle_manifest.completion_available_session == (
        config.completion_available_session
    )
    assert len(result.lifecycle_manifest.acceptance_criteria) == 13
    assert all(item.passed for item in result.lifecycle_manifest.acceptance_criteria)
    assert result.sentinel_artifact.path.endswith("/S7_5_COMPLETE.json")
    replay = runtime.verify_s75_completion(tmp_path, result.sentinel_artifact)
    assert replay.lifecycle_manifest == result.lifecycle_manifest
    repeated = runtime.stage_s75_completion(tmp_path, config_pin)
    assert repeated.reused is True
    assert repeated.sentinel_artifact == result.sentinel_artifact


def test_tampered_sentinel_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, _evidence = _fixture(tmp_path, monkeypatch)
    _, config_pin = runtime.prepare_s75_completion(tmp_path, config)
    result = runtime.stage_s75_completion(tmp_path, config_pin)
    sentinel = tmp_path / result.sentinel_artifact.path
    sentinel.chmod(0o644)
    content = bytearray(sentinel.read_bytes())
    content[-2] ^= 1
    sentinel.write_bytes(content)
    with pytest.raises(runtime.S75CompletionRuntimeError, match="exact pin differs"):
        runtime.verify_s75_completion(tmp_path, result.sentinel_artifact)


def test_changed_evidence_cannot_reuse_marker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, evidence = _fixture(tmp_path, monkeypatch)
    _, config_pin = runtime.prepare_s75_completion(tmp_path, config)
    result = runtime.stage_s75_completion(tmp_path, config_pin)
    changed = replace(
        evidence,
        i3_acceptance_receipt_id=stable_digest({"fixture": "changed-i3"}),
    )
    monkeypatch.setattr(runtime, "_load_production_evidence", lambda *_args, **_kwargs: changed)
    with pytest.raises(runtime.S75CompletionRuntimeError, match="does not reproduce"):
        runtime.verify_s75_completion(tmp_path, result.sentinel_artifact)


def test_completion_commit_failure_never_leaves_a_visible_partial_sentinel(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, _evidence = _fixture(tmp_path, monkeypatch)
    _, config_pin = runtime.prepare_s75_completion(tmp_path, config)

    def interrupt(path: Path) -> None:
        if path.name == "S7_5_COMPLETE.json":
            raise RuntimeError("fixture interruption before visible completion")

    monkeypatch.setattr(runtime, "_before_immutable_commit", interrupt)
    with pytest.raises(RuntimeError, match="before visible completion"):
        runtime.stage_s75_completion(tmp_path, config_pin)
    assert not (tmp_path / "manifests/silver/incremental/s7_5/S7_5_COMPLETE.json").exists()

    monkeypatch.setattr(runtime, "_before_immutable_commit", lambda _path: None)
    resumed = runtime.stage_s75_completion(tmp_path, config_pin)
    assert resumed.sentinel_artifact.path.endswith("S7_5_COMPLETE.json")


def test_existing_sentinel_symlink_is_rejected_without_following_it(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, _evidence = _fixture(tmp_path, monkeypatch)
    _, config_pin = runtime.prepare_s75_completion(tmp_path, config)
    target = tmp_path / "outside.json"
    target.write_bytes(b"untrusted\n")
    sentinel = tmp_path / "manifests/silver/incremental/s7_5/S7_5_COMPLETE.json"
    sentinel.parent.mkdir(parents=True, exist_ok=True)
    sentinel.symlink_to(target)

    with pytest.raises(ArtifactError, match="path through symlink"):
        runtime.stage_s75_completion(tmp_path, config_pin)
    assert target.read_bytes() == b"untrusted\n"


def test_nonproduction_completion_input_path_is_rejected() -> None:
    bad = ArtifactPin(path="manifests/latest/i4.json", sha256="a" * 64, bytes=1)
    good = ArtifactPin(path="manifests/silver/i5.json", sha256="b" * 64, bytes=1)
    with pytest.raises(runtime.S75CompletionRuntimeError, match="production authority"):
        runtime.S75CompletionConfig(
            i4_completion_artifact=bad,
            i5_completion_artifact=good,
            i7_completion_artifact=good,
            completion_available_session=date(2026, 8, 12),
        )


def test_gate_a_repository_evidence_replays() -> None:
    assert runtime._verify_gate_a_approval() == runtime.GATE_A_APPROVAL_ID
