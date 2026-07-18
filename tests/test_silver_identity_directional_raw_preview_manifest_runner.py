from __future__ import annotations

import ast
import fcntl
import hashlib
import os
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from ame_stocks_api.silver import identity_directional_raw_preview_manifest_runner as module
from ame_stocks_api.silver.identity_directional_raw_preview_manifest_runner import (
    IdentityDirectionalRawPreviewManifestRunnerError,
)


def _intent():
    return module.S7DirectionalRawPreviewManifestRunIntent(
        manifest_plan_id="1" * 64,
        manifest_plan_sha256="2" * 64,
        manifest_request_event_id="3" * 64,
        manifest_request_event_sha256="4" * 64,
        manifest_approval_id="5" * 64,
        manifest_approval_sha256="6" * 64,
        approval_literal_sha256="7" * 64,
        input_binding_digest="8" * 64,
        execution_data_root=module.MANIFEST_EXECUTION_DATA_ROOT,
        source_binding_created_by="source-reader",
        source_binding_created_at_utc=datetime.now(UTC) - timedelta(minutes=1),
        execution_plan_created_by="execution-planner",
        execution_request_created_by="execution-requester",
    )


def _write_exact(root: Path, relative: str, content: bytes):
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        assert path.read_bytes() == content
    else:
        path.write_bytes(content)
    return SimpleNamespace(
        path=relative,
        sha256=hashlib.sha256(content).hexdigest(),
        bytes=len(content),
    )


def _locked_controls(intent):
    caps = SimpleNamespace(
        output_bytes_hard_cap=10_000_000,
        wall_clock_seconds_hard_cap=1,
    )
    plan = SimpleNamespace(
        plan_id=intent.manifest_plan_id,
        sha256=intent.manifest_plan_sha256,
        future_manifest_reader_actor=intent.source_binding_created_by,
        runtime_files=(),
        verification_files=(),
        git_commit="a" * 40,
        git_tree="b" * 40,
        resource_caps=caps,
    )
    request = SimpleNamespace(
        request_event_id=intent.manifest_request_event_id,
        sha256=intent.manifest_request_event_sha256,
    )
    approval = SimpleNamespace(
        approval_id=intent.manifest_approval_id,
        sha256=intent.manifest_approval_sha256,
        approval_literal_sha256=intent.approval_literal_sha256,
    )
    return plan, request, approval


def _selected(root):
    result = []
    for table in ("asset_observation_daily", "universe_source_daily"):
        for index, session in enumerate(module._FIXED_SESSIONS):
            relative = f"silver/{table}/session_date={session}/part.parquet"
            path = root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"x" * (index + 1))
            result.append(
                (
                    table,
                    SimpleNamespace(
                        path=relative,
                        bytes=index + 1,
                        row_count=index,
                        sha256=f"{index + 1:064x}",
                    ),
                    next(
                        str(pin["release_manifest_sha256"])
                        for pin in module.S4_SOURCE_PINS
                        if pin["table"] == table
                    ),
                )
            )
    return tuple(result)


def test_lstat_phase_selects_exactly_22_without_opening_parquet(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    selected = _selected(tmp_path)
    monkeypatch.setattr(module, "_read_release_manifests", lambda root, **kwargs: ((), selected))
    real_open = module.os.open
    real_lstat = module.os.lstat
    lstat_paths = []

    def guarded_open(path, *args, **kwargs):
        assert not str(path).endswith(".parquet")
        return real_open(path, *args, **kwargs)

    def counted_lstat(path, *args, **kwargs):
        if str(path).endswith(".parquet"):
            lstat_paths.append(Path(path))
        return real_lstat(path, *args, **kwargs)

    monkeypatch.setattr(module.os, "open", guarded_open)
    monkeypatch.setattr(module.os, "lstat", counted_lstat)
    monkeypatch.setattr(
        module,
        "sha256_file",
        lambda path: (_ for _ in ()).throw(AssertionError(f"unexpected hash: {path}")),
    )
    manifests, refs = module._read_release_manifests_and_lstat(tmp_path)
    assert manifests == ()
    assert len(refs) == 22
    assert all(item.content_opened is False for item in refs)
    assert all(item.disk_size_bytes == item.bytes for item in refs)
    assert len(lstat_paths) == 22
    assert len(set(lstat_paths)) == 22


def test_lstat_rejects_symlink(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    selected = list(_selected(tmp_path))
    target = tmp_path / selected[0][1].path
    target.unlink()
    target.symlink_to(tmp_path / selected[1][1].path)
    monkeypatch.setattr(
        module, "_read_release_manifests", lambda root, **kwargs: ((), tuple(selected))
    )
    with pytest.raises(IdentityDirectionalRawPreviewManifestRunnerError):
        module._read_release_manifests_and_lstat(tmp_path)


def test_runner_imports_no_network_or_parquet_library() -> None:
    tree = ast.parse(Path(module.__file__).read_text(encoding="utf-8"))
    names = {
        node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.module
    } | {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    assert not names & {"pyarrow", "pandas", "requests", "socket", "urllib.request"}


def test_existing_intent_without_binding_fails_before_any_source_read(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    intent = _intent()
    plan, request, approval = _locked_controls(intent)
    intent_receipt = _write_exact(tmp_path, intent.relative_path, intent.content)

    class IntentOnlyStore:
        def __init__(self, root):
            self.root = root

        def store_run_intent(self, value):
            assert value == intent
            return intent_receipt

        def load_run_intent(self, plan_id, approval_id, *, expected_sha256):
            return intent, intent_receipt

    monkeypatch.setattr(module, "IdentityDirectionalRawPreviewManifestStore", IntentOnlyStore)
    monkeypatch.setattr(module, "_load_existing_manifest_completion", lambda *a, **k: None)
    monkeypatch.setattr(
        module,
        "_read_release_manifests_and_lstat",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("source reread")),
    )
    monkeypatch.setattr(
        module,
        "_read_inventory_manifests",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("source reread")),
    )

    with pytest.raises(
        IdentityDirectionalRawPreviewManifestRunnerError,
        match="source-read state is ambiguous",
    ):
        module._run_manifest_preflight_locked(
            root=tmp_path,
            plan=plan,
            request=request,
            approval=approval,
            intent=intent,
            started=time.monotonic(),
        )


def test_new_intent_is_persisted_before_the_only_source_read(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    intent = _intent()
    plan, request, approval = _locked_controls(intent)
    state = {"release_reads": 0, "inventory_reads": 0}

    class IntentStore:
        def __init__(self, root):
            self.root = root

        def store_run_intent(self, value):
            return _write_exact(tmp_path, value.relative_path, value.content)

        def load_run_intent(self, plan_id, approval_id, *, expected_sha256):
            return intent, _write_exact(tmp_path, intent.relative_path, intent.content)

    class StopAfterRead(Exception):
        pass

    class BindingFactory:
        def __init__(self, **kwargs):
            assert (tmp_path / intent.relative_path).is_file()
            assert state == {"release_reads": 1, "inventory_reads": 1}
            raise StopAfterRead

    def release_read(root, *, json_budget, operation_counts):
        assert (tmp_path / intent.relative_path).is_file()
        state["release_reads"] += 1
        operation_counts["json_reads"] += 2
        operation_counts["lstats"] += 22
        return (), ()

    def inventory_read(root, *, json_budget, operation_counts):
        state["inventory_reads"] += 1
        operation_counts["json_reads"] += 2
        return None, None

    monkeypatch.setattr(module, "IdentityDirectionalRawPreviewManifestStore", IntentStore)
    monkeypatch.setattr(module, "S7DirectionalRawPreviewSourceBinding", BindingFactory)
    monkeypatch.setattr(module, "_read_release_manifests_and_lstat", release_read)
    monkeypatch.setattr(module, "_read_inventory_manifests", inventory_read)
    monkeypatch.setattr(module, "_check_resource_caps", lambda *a, **k: None)
    with pytest.raises(StopAfterRead):
        module._run_manifest_preflight_locked(
            root=tmp_path,
            plan=plan,
            request=request,
            approval=approval,
            intent=intent,
            started=time.monotonic(),
        )
    assert state == {"release_reads": 1, "inventory_reads": 1}


def test_orphan_completion_is_rejected_after_new_intent_claim(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    intent = _intent()
    plan, request, approval = _locked_controls(intent)
    completion_path = module.directional_manifest_preflight_completion_path(
        plan.plan_id, approval.approval_id
    )
    _write_exact(tmp_path, completion_path, b"orphan\n")

    class IntentStore:
        def __init__(self, root):
            self.root = root

        def store_run_intent(self, value):
            return _write_exact(tmp_path, value.relative_path, value.content)

        def load_run_intent(self, plan_id, approval_id, *, expected_sha256):
            return intent, _write_exact(tmp_path, intent.relative_path, intent.content)

    monkeypatch.setattr(module, "IdentityDirectionalRawPreviewManifestStore", IntentStore)
    monkeypatch.setattr(
        module,
        "_read_release_manifests_and_lstat",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("source read")),
    )
    with pytest.raises(
        IdentityDirectionalRawPreviewManifestRunnerError,
        match="downstream output exists without",
    ):
        module._run_manifest_preflight_locked(
            root=tmp_path,
            plan=plan,
            request=request,
            approval=approval,
            intent=intent,
            started=time.monotonic(),
        )


@pytest.mark.parametrize("partial_outputs", (1, 2, 3))
def test_existing_binding_resumes_partial_outputs_with_zero_source_operations(
    tmp_path, monkeypatch: pytest.MonkeyPatch, partial_outputs: int
) -> None:
    intent = _intent()
    plan, request, approval = _locked_controls(intent)
    intent_receipt = _write_exact(tmp_path, intent.relative_path, intent.content)
    binding_content = b"canonical-source-binding\n"
    binding = SimpleNamespace(
        source_binding_id="9" * 64,
        relative_path=module.directional_source_binding_path(intent.intent_id),
        sha256=hashlib.sha256(binding_content).hexdigest(),
        content=binding_content,
    )
    binding_receipt = _write_exact(tmp_path, binding.relative_path, binding.content)
    plan_content = b"canonical-execution-plan\n"
    execution_plan = SimpleNamespace(
        plan_id="a" * 64,
        relative_path="controls/execution-plan.json",
        sha256=hashlib.sha256(plan_content).hexdigest(),
        content=plan_content,
        source_binding_manifest_id=binding.source_binding_id,
    )
    request_content = b"canonical-execution-request\n"
    execution_request = SimpleNamespace(
        request_event_id="b" * 64,
        relative_path="controls/execution-request.json",
        sha256=hashlib.sha256(request_content).hexdigest(),
        content=request_content,
        plan_id=execution_plan.plan_id,
        manifest_preflight_intent_id=intent.intent_id,
    )
    if partial_outputs >= 2:
        _write_exact(tmp_path, execution_plan.relative_path, execution_plan.content)
    if partial_outputs >= 3:
        _write_exact(tmp_path, execution_request.relative_path, execution_request.content)

    state = {}

    class ManifestStore:
        def __init__(self, root):
            self.root = root

        def store_run_intent(self, value):
            return intent_receipt

        def load_run_intent(self, plan_id, approval_id, *, expected_sha256):
            return intent, intent_receipt

        def load_source_binding_for_intent(self, run_intent_id):
            assert run_intent_id == intent.intent_id
            return binding, binding_receipt

        def store_preflight_completion(self, value):
            state["completion"] = value
            return _write_exact(tmp_path, value.relative_path, value.content)

        def load_preflight_completion(self, plan_id, approval_id, *, expected_sha256):
            value = state["completion"]
            return value, _write_exact(tmp_path, value.relative_path, value.content)

    class ExecutionStore:
        def __init__(self, root):
            self.root = root

        def store_execution_plan(self, value):
            return _write_exact(tmp_path, value.relative_path, value.content)

        def load_execution_plan(self, plan_id, *, expected_sha256):
            return execution_plan, _write_exact(
                tmp_path, execution_plan.relative_path, execution_plan.content
            )

        def store_execution_request(self, value):
            return _write_exact(tmp_path, value.relative_path, value.content)

        def load_execution_request(self, request_event_id, *, expected_sha256):
            return execution_request, _write_exact(
                tmp_path, execution_request.relative_path, execution_request.content
            )

    class ExecutionPlanFactory:
        @staticmethod
        def create(**kwargs):
            return execution_plan

    class ExecutionRequestFactory:
        @staticmethod
        def create(*args, **kwargs):
            return execution_request

    monkeypatch.setattr(module, "IdentityDirectionalRawPreviewManifestStore", ManifestStore)
    monkeypatch.setattr(module, "DirectionalRawPreviewExecutionPlanStore", ExecutionStore)
    monkeypatch.setattr(module, "S7DirectionalRawPreviewExecutionPlan", ExecutionPlanFactory)
    monkeypatch.setattr(module, "S7DirectionalRawPreviewExecutionRequest", ExecutionRequestFactory)
    monkeypatch.setattr(module, "_check_resource_caps", lambda *a, **k: None)
    monkeypatch.setattr(
        module,
        "_read_release_manifests_and_lstat",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("source reread")),
    )
    monkeypatch.setattr(
        module,
        "_read_inventory_manifests",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("source reread")),
    )

    run = module._run_manifest_preflight_locked(
        root=tmp_path,
        plan=plan,
        request=request,
        approval=approval,
        intent=intent,
        started=time.monotonic(),
    )
    assert run.json_manifest_reads == 0
    assert run.parquet_lstats == 0
    assert run.all_documents_preexisting is False


def test_completion_retry_returns_zero_zero_and_replays_exact_bytes(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    intent = _intent()
    plan, _request, approval = _locked_controls(intent)
    intent_document = _write_exact(tmp_path, intent.relative_path, intent.content)
    binding = SimpleNamespace(
        source_binding_id="9" * 64,
        relative_path=module.directional_source_binding_path(intent.intent_id),
    )
    binding_document = SimpleNamespace(
        path=binding.relative_path, sha256="a" * 64, bytes=101
    )
    execution_plan = SimpleNamespace(
        plan_id="b" * 64,
        relative_path="controls/execution-plan.json",
        source_binding_manifest_id=binding.source_binding_id,
    )
    execution_plan_document = SimpleNamespace(
        path=execution_plan.relative_path, sha256="c" * 64, bytes=102
    )
    execution_request = SimpleNamespace(
        request_event_id="d" * 64,
        relative_path="controls/execution-request.json",
        plan_id=execution_plan.plan_id,
        manifest_preflight_intent_id=intent.intent_id,
    )
    execution_request_document = SimpleNamespace(
        path=execution_request.relative_path, sha256="e" * 64, bytes=103
    )
    completion = module.S7DirectionalRawPreviewManifestPreflightCompletion(
        manifest_plan_id=plan.plan_id,
        manifest_plan_sha256=plan.sha256,
        manifest_approval_id=approval.approval_id,
        manifest_approval_sha256=approval.sha256,
        run_intent_id=intent.intent_id,
        run_intent_path=intent.relative_path,
        run_intent_sha256=intent.sha256,
        run_intent_bytes=intent_document.bytes,
        source_binding_id=binding.source_binding_id,
        source_binding_path=binding.relative_path,
        source_binding_sha256=binding_document.sha256,
        source_binding_bytes=binding_document.bytes,
        execution_plan_id=execution_plan.plan_id,
        execution_plan_path=execution_plan.relative_path,
        execution_plan_sha256=execution_plan_document.sha256,
        execution_plan_bytes=execution_plan_document.bytes,
        execution_request_event_id=execution_request.request_event_id,
        execution_request_path=execution_request.relative_path,
        execution_request_sha256=execution_request_document.sha256,
        execution_request_bytes=execution_request_document.bytes,
        completed_at_utc=datetime.now(UTC),
    )
    completion_document = _write_exact(
        tmp_path, completion.relative_path, completion.content
    )

    class ManifestStore:
        def __init__(self, root):
            self.root = root

        def load_preflight_completion(self, plan_id, approval_id, *, expected_sha256):
            return completion, completion_document

        def load_source_binding(self, *args, **kwargs):
            return binding, binding_document

    class ExecutionStore:
        def __init__(self, root):
            self.root = root

        def load_execution_plan(self, *args, **kwargs):
            return execution_plan, execution_plan_document

        def load_execution_request(self, *args, **kwargs):
            return execution_request, execution_request_document

    monkeypatch.setattr(module, "IdentityDirectionalRawPreviewManifestStore", ManifestStore)
    monkeypatch.setattr(module, "DirectionalRawPreviewExecutionPlanStore", ExecutionStore)
    run = module._load_existing_manifest_completion(
        tmp_path,
        plan=plan,
        approval=approval,
        expected_intent=intent,
        intent_document=intent_document,
    )
    assert run is not None
    assert (run.json_manifest_reads, run.parquet_lstats) == (0, 0)
    assert run.all_documents_preexisting is True


def test_manifest_lock_is_scoped_by_plan_and_approval(tmp_path) -> None:
    caps = SimpleNamespace(wall_clock_seconds_hard_cap=0.01)
    first = module._acquire_manifest_run_lock(
        tmp_path, "1" * 64, "2" * 64, time.monotonic(), caps
    )
    different_approval = module._acquire_manifest_run_lock(
        tmp_path, "1" * 64, "3" * 64, time.monotonic(), caps
    )
    try:
        with pytest.raises(
            IdentityDirectionalRawPreviewManifestRunnerError,
            match="lock wait exceeded",
        ):
            module._acquire_manifest_run_lock(
                tmp_path, "1" * 64, "2" * 64, time.monotonic() - 1, caps
            )
    finally:
        fcntl.flock(first, fcntl.LOCK_UN)
        fcntl.flock(different_approval, fcntl.LOCK_UN)
        os.close(first)
        os.close(different_approval)
