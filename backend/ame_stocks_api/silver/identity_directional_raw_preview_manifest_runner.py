"""Execute only the approved manifest/lstat S7 source-binding preflight."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import resource
import stat
import subprocess
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ame_stocks_api.artifacts import (
    ArtifactError,
    safe_relative_path,
    sha256_file,
    stable_digest,
)
from ame_stocks_api.silver.asset_contract import (
    ASSET_OBSERVATION_DAILY_CONTRACT,
    UNIVERSE_SOURCE_DAILY_CONTRACT,
)
from ame_stocks_api.silver.contracts import ArtifactRole, ReleaseManifest
from ame_stocks_api.silver.identity_directional_raw_preview_execution_plan import (
    DirectionalRawPreviewExecutionPlanStore,
    S7DirectionalRawPreviewExecutionPlan,
    S7DirectionalRawPreviewExecutionRequest,
    StoredDirectionalRawPreviewExecutionDocument,
)
from ame_stocks_api.silver.identity_directional_raw_preview_manifest_approval import (
    DirectionalRawPreviewManifestApprovalStore,
)
from ame_stocks_api.silver.identity_directional_raw_preview_manifest_plan import (
    INVENTORY_CANDIDATE_DATA_SHA256,
    INVENTORY_CANDIDATE_ID,
    INVENTORY_CANDIDATE_MANIFEST_SHA256,
    INVENTORY_COMPLETION_ID,
    INVENTORY_CONTRACT_ID,
    INVENTORY_EXECUTION_PLAN_ID,
    INVENTORY_EXECUTION_PLAN_SHA256,
    INVENTORY_INPUT_BINDING_DIGEST,
    INVENTORY_SCHEMA_DIGEST,
    INVENTORY_SOURCE_ARTIFACT_SET_DIGEST,
    MANIFEST_EXECUTION_DATA_ROOT,
    S4_SOURCE_PINS,
    IdentityDirectionalRawPreviewManifestPlanError,
    IdentityDirectionalRawPreviewManifestStore,
    S7DirectionalRawPreviewManifestDocumentRef,
    S7DirectionalRawPreviewManifestPreflightCompletion,
    S7DirectionalRawPreviewManifestRunIntent,
    S7DirectionalRawPreviewSourceArtifactRef,
    S7DirectionalRawPreviewSourceBinding,
    StoredDirectionalRawPreviewManifestControl,
    directional_manifest_preflight_completion_path,
    directional_source_binding_path,
    inventory_completion_discovery_directory,
)
from ame_stocks_api.silver.identity_directional_raw_preview_manifest_request import (
    load_manifest_preflight_request,
)
from ame_stocks_api.silver.identity_directional_raw_preview_plan import (
    DIRECTIONAL_RAW_PREVIEW_FIXED_CASE_SESSIONS,
    S7DirectionalRawPreviewControlFilePin,
)

_SESSION = re.compile(r"(?:^|/)session_date=(\d{4}-\d{2}-\d{2})(?:/|$)")
_APPROVAL_DIR = re.compile(r"^approval_id=[0-9a-f]{64}$")
_FIXED_SESSIONS = tuple(
    sorted(
        {
            session
            for _, sessions in DIRECTIONAL_RAW_PREVIEW_FIXED_CASE_SESSIONS
            for session in sessions
        }
    )
)
_CONTRACTS = {
    "asset_observation_daily": ASSET_OBSERVATION_DAILY_CONTRACT,
    "universe_source_daily": UNIVERSE_SOURCE_DAILY_CONTRACT,
}


class IdentityDirectionalRawPreviewManifestRunnerError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class S7DirectionalRawPreviewManifestRun:
    run_intent: S7DirectionalRawPreviewManifestRunIntent
    run_intent_document: StoredDirectionalRawPreviewManifestControl
    source_binding: S7DirectionalRawPreviewSourceBinding
    source_binding_document: StoredDirectionalRawPreviewManifestControl
    execution_plan: S7DirectionalRawPreviewExecutionPlan
    execution_plan_document: StoredDirectionalRawPreviewExecutionDocument
    execution_request: S7DirectionalRawPreviewExecutionRequest
    execution_request_document: StoredDirectionalRawPreviewExecutionDocument
    completion: S7DirectionalRawPreviewManifestPreflightCompletion
    completion_document: StoredDirectionalRawPreviewManifestControl
    json_manifest_reads: int
    parquet_lstats: int
    all_documents_preexisting: bool


def run_s7_directional_raw_preview_manifest_preflight(
    *,
    control_root: Path,
    data_root: Path,
    repo_root: Path,
    plan_id: str,
    plan_sha256: str,
    request_event_id: str,
    request_event_sha256: str,
    approval_id: str,
    approval_sha256: str,
    source_binding_created_at_utc: datetime,
) -> S7DirectionalRawPreviewManifestRun:
    """Read four JSON manifests and lstat exactly twenty-two Parquet paths."""

    started = time.monotonic()
    control_path = _root(control_root, "control_root")
    root = _root(data_root, "data_root")
    if control_path != root:
        raise IdentityDirectionalRawPreviewManifestRunnerError(
            "control_root and data_root must be the same durable root"
        )
    controls = IdentityDirectionalRawPreviewManifestStore(control_path)
    plan, _ = controls.load_plan(plan_id, expected_sha256=plan_sha256)
    request, _ = load_manifest_preflight_request(
        controls.root, request_event_id, expected_sha256=request_event_sha256
    )
    approval, _ = DirectionalRawPreviewManifestApprovalStore(controls.root).load_approval(
        approval_id, expected_sha256=approval_sha256
    )
    if (
        request.plan_id != plan.plan_id
        or approval.plan_id != plan.plan_id
        or approval.request_event_id != request.request_event_id
        or approval.approval_literal != request.canonical_approval_literal
        or approval.input_binding_digest != plan.input_binding_digest
        or plan.execution_data_root != MANIFEST_EXECUTION_DATA_ROOT
    ):
        raise IdentityDirectionalRawPreviewManifestRunnerError("manifest approval chain differs")
    repository = _root(repo_root, "repo_root")
    if str(root) != plan.execution_data_root:
        raise IdentityDirectionalRawPreviewManifestRunnerError("data root differs from plan")
    _verify_git(repository, plan)
    created = _utc(source_binding_created_at_utc)
    if created <= approval.approved_at_utc or created > datetime.now(UTC):
        raise IdentityDirectionalRawPreviewManifestRunnerError(
            "source binding must follow approval"
        )
    control_actors = {
        plan.created_by,
        request.created_by,
        approval.approved_by,
        plan.future_manifest_reader_actor,
        plan.future_execution_plan_actor,
        plan.future_execution_request_actor,
    }
    if len(control_actors) != 6:
        raise IdentityDirectionalRawPreviewManifestRunnerError(
            "source, execution-plan, and request actors must be distinct"
        )

    intent = S7DirectionalRawPreviewManifestRunIntent(
        manifest_plan_id=plan.plan_id,
        manifest_plan_sha256=plan.sha256,
        manifest_request_event_id=request.request_event_id,
        manifest_request_event_sha256=request.sha256,
        manifest_approval_id=approval.approval_id,
        manifest_approval_sha256=approval.sha256,
        approval_literal_sha256=approval.approval_literal_sha256,
        input_binding_digest=plan.input_binding_digest,
        execution_data_root=str(root),
        source_binding_created_by=plan.future_manifest_reader_actor,
        source_binding_created_at_utc=created,
        execution_plan_created_by=plan.future_execution_plan_actor,
        execution_request_created_by=plan.future_execution_request_actor,
    )
    lock_descriptor = _acquire_manifest_run_lock(
        root, plan.plan_id, approval.approval_id, started, plan.resource_caps
    )
    try:
        return _run_manifest_preflight_locked(
            root=root,
            plan=plan,
            request=request,
            approval=approval,
            intent=intent,
            started=started,
        )
    finally:
        fcntl.flock(lock_descriptor, fcntl.LOCK_UN)
        os.close(lock_descriptor)


def _run_manifest_preflight_locked(
    *,
    root: Path,
    plan: Any,
    request: Any,
    approval: Any,
    intent: S7DirectionalRawPreviewManifestRunIntent,
    started: float,
) -> S7DirectionalRawPreviewManifestRun:
    binding_store = IdentityDirectionalRawPreviewManifestStore(root)
    intent_preexisting = _preflight_destination(root, intent.relative_path, intent.sha256)
    intent_document = binding_store.store_run_intent(intent)
    loaded_intent, loaded_intent_document = binding_store.load_run_intent(
        plan.plan_id,
        approval.approval_id,
        expected_sha256=intent.sha256,
    )
    if loaded_intent != intent or loaded_intent_document != intent_document:
        raise IdentityDirectionalRawPreviewManifestRunnerError(
            "manifest run intent differs after immutable readback"
        )

    if not intent_preexisting:
        for relative in (
            directional_manifest_preflight_completion_path(
                plan.plan_id, approval.approval_id
            ),
            directional_source_binding_path(intent.intent_id),
        ):
            path = safe_relative_path(root, relative)
            if path.exists() or path.is_symlink():
                raise IdentityDirectionalRawPreviewManifestRunnerError(
                    "downstream output exists without its immutable run intent"
                )

    completed = _load_existing_manifest_completion(
        root,
        plan=plan,
        approval=approval,
        expected_intent=intent,
        intent_document=intent_document,
    )
    if completed is not None:
        return completed

    json_budget = [0]
    operation_counts = {"json_reads": 0, "lstats": 0}
    binding_path = safe_relative_path(
        root,
        # The intent-addressed binding path is the durable boundary between
        # an ambiguous crash-after-read and a resumable post-read state.
        directional_source_binding_path(intent.intent_id),
    )
    binding_path_exists = binding_path.exists() or binding_path.is_symlink()
    if intent_preexisting:
        if not binding_path_exists:
            raise IdentityDirectionalRawPreviewManifestRunnerError(
                "run intent exists without a source binding; source-read state is "
                "ambiguous and cannot be retried"
            )
        try:
            binding, binding_document = binding_store.load_source_binding_for_intent(
                intent.intent_id
            )
        except IdentityDirectionalRawPreviewManifestPlanError as exc:
            raise IdentityDirectionalRawPreviewManifestRunnerError(
                "preexisting source binding is missing or unsafe"
            ) from exc
    else:
        if binding_path_exists:
            raise IdentityDirectionalRawPreviewManifestRunnerError(
                "source binding exists without its immutable run intent"
            )
        release_refs, source_refs = _read_release_manifests_and_lstat(
            root,
            json_budget=json_budget,
            operation_counts=operation_counts,
        )
        completion_ref, candidate_ref = _read_inventory_manifests(
            root,
            json_budget=json_budget,
            operation_counts=operation_counts,
        )
        if operation_counts != {"json_reads": 4, "lstats": 22}:
            raise IdentityDirectionalRawPreviewManifestRunnerError(
                "manifest operation counts differ from exact scope"
            )
        _check_resource_caps(plan, started, json_budget[0])
        binding = S7DirectionalRawPreviewSourceBinding(
            created_by=plan.future_manifest_reader_actor,
            created_at_utc=intent.source_binding_created_at_utc,
            manifest_plan_id=plan.plan_id,
            manifest_plan_sha256=plan.sha256,
            manifest_request_event_id=request.request_event_id,
            manifest_request_event_sha256=request.sha256,
            manifest_literal_sha256=approval.approval_literal_sha256,
            manifest_approval_id=approval.approval_id,
            manifest_approval_sha256=approval.sha256,
            manifest_run_intent_id=intent.intent_id,
            manifest_run_intent_path=intent.relative_path,
            manifest_run_intent_sha256=intent.sha256,
            manifest_run_intent_execution_plan_created_by=(
                intent.execution_plan_created_by
            ),
            manifest_run_intent_execution_request_created_by=(
                intent.execution_request_created_by
            ),
            source_artifacts=source_refs,
            manifest_documents=(*release_refs, completion_ref, candidate_ref),
            resource_caps=plan.resource_caps,
        )
        binding_document = binding_store.store_source_binding(binding)
        loaded_binding, loaded_binding_document = binding_store.load_source_binding(
            intent.intent_id,
            expected_source_binding_id=binding.source_binding_id,
            expected_sha256=binding.sha256,
        )
        if loaded_binding != binding or loaded_binding_document != binding_document:
            raise IdentityDirectionalRawPreviewManifestRunnerError(
                "source binding differs after immutable readback"
            )
    runtime = tuple(
        S7DirectionalRawPreviewControlFilePin(
            path=item.path,
            git_blob=item.git_blob,
            sha256=item.sha256,
            bytes=item.bytes,
        )
        for item in plan.runtime_files
    )
    verification = tuple(
        S7DirectionalRawPreviewControlFilePin(
            path=item.path,
            git_blob=item.git_blob,
            sha256=item.sha256,
            bytes=item.bytes,
        )
        for item in plan.verification_files
    )
    execution_plan = S7DirectionalRawPreviewExecutionPlan.create(
        created_by=intent.execution_plan_created_by,
        created_at_utc=intent.source_binding_created_at_utc,
        execution_git_commit=plan.git_commit,
        execution_git_tree=plan.git_tree,
        execution_data_root=str(root),
        runtime_files=runtime,
        verification_files=verification,
        source_binding=binding,
        source_binding_receipt=binding_document,
    )
    _preflight_destination(root, execution_plan.relative_path, execution_plan.sha256)
    execution_store = DirectionalRawPreviewExecutionPlanStore(root)
    execution_plan_document = execution_store.store_execution_plan(execution_plan)
    loaded_plan, loaded_plan_document = execution_store.load_execution_plan(
        execution_plan.plan_id, expected_sha256=execution_plan.sha256
    )
    if loaded_plan != execution_plan or loaded_plan_document != execution_plan_document:
        raise IdentityDirectionalRawPreviewManifestRunnerError(
            "execution plan differs after immutable readback"
        )
    execution_request = S7DirectionalRawPreviewExecutionRequest.create(
        execution_plan,
        execution_plan_document,
        created_by=intent.execution_request_created_by,
        created_at_utc=intent.source_binding_created_at_utc,
    )
    _preflight_destination(
        root, execution_request.relative_path, execution_request.sha256
    )
    execution_request_document = execution_store.store_execution_request(execution_request)
    loaded_request, loaded_request_document = execution_store.load_execution_request(
        execution_request.request_event_id, expected_sha256=execution_request.sha256
    )
    if loaded_request != execution_request or loaded_request_document != execution_request_document:
        raise IdentityDirectionalRawPreviewManifestRunnerError(
            "manifest outputs differ after immutable readback"
        )

    completion = S7DirectionalRawPreviewManifestPreflightCompletion(
        manifest_plan_id=plan.plan_id,
        manifest_plan_sha256=plan.sha256,
        manifest_approval_id=approval.approval_id,
        manifest_approval_sha256=approval.sha256,
        run_intent_id=intent.intent_id,
        run_intent_path=intent.relative_path,
        run_intent_sha256=intent.sha256,
        run_intent_bytes=len(intent.content),
        source_binding_id=binding.source_binding_id,
        source_binding_path=binding.relative_path,
        source_binding_sha256=binding.sha256,
        source_binding_bytes=len(binding.content),
        execution_plan_id=execution_plan.plan_id,
        execution_plan_path=execution_plan.relative_path,
        execution_plan_sha256=execution_plan.sha256,
        execution_plan_bytes=len(execution_plan.content),
        execution_request_event_id=execution_request.request_event_id,
        execution_request_path=execution_request.relative_path,
        execution_request_sha256=execution_request.sha256,
        execution_request_bytes=len(execution_request.content),
        completed_at_utc=datetime.now(UTC),
    )
    output_bytes = sum(
        len(content)
        for content in (
            intent.content,
            binding.content,
            execution_plan.content,
            execution_request.content,
            completion.content,
        )
    )
    if output_bytes > plan.resource_caps.output_bytes_hard_cap:
        raise IdentityDirectionalRawPreviewManifestRunnerError("output bytes cap exceeded")
    _check_resource_caps(plan, started, json_budget[0])
    completion_document = binding_store.store_preflight_completion(completion)
    loaded_completion, loaded_completion_document = binding_store.load_preflight_completion(
        plan.plan_id,
        approval.approval_id,
        expected_sha256=completion.sha256,
    )
    if (
        loaded_completion != completion
        or loaded_completion_document != completion_document
    ):
        raise IdentityDirectionalRawPreviewManifestRunnerError(
            "manifest completion differs after immutable readback"
        )
    return S7DirectionalRawPreviewManifestRun(
        intent,
        intent_document,
        binding,
        binding_document,
        execution_plan,
        execution_plan_document,
        execution_request,
        execution_request_document,
        completion,
        completion_document,
        operation_counts["json_reads"],
        operation_counts["lstats"],
        False,
    )


def _load_existing_manifest_completion(
    root: Path,
    *,
    plan: Any,
    approval: Any,
    expected_intent: S7DirectionalRawPreviewManifestRunIntent,
    intent_document: StoredDirectionalRawPreviewManifestControl,
) -> S7DirectionalRawPreviewManifestRun | None:
    relative = directional_manifest_preflight_completion_path(
        plan.plan_id, approval.approval_id
    )
    path = safe_relative_path(root, relative)
    if not path.exists() and not path.is_symlink():
        return None
    if not path.is_file() or path.is_symlink():
        raise IdentityDirectionalRawPreviewManifestRunnerError(
            "manifest completion path is unsafe"
        )
    checksum = sha256_file(path)
    binding_store = IdentityDirectionalRawPreviewManifestStore(root)
    completion, completion_document = binding_store.load_preflight_completion(
        plan.plan_id,
        approval.approval_id,
        expected_sha256=checksum,
    )
    if (
        completion.manifest_plan_sha256 != plan.sha256
        or completion.manifest_approval_sha256 != approval.sha256
        or completion.run_intent_id != expected_intent.intent_id
        or completion.run_intent_path != expected_intent.relative_path
        or completion.run_intent_sha256 != expected_intent.sha256
        or completion.run_intent_bytes != intent_document.bytes
    ):
        raise IdentityDirectionalRawPreviewManifestRunnerError(
            "manifest completion differs from requested immutable intent"
        )
    binding, binding_document = binding_store.load_source_binding(
        completion.run_intent_id,
        expected_source_binding_id=completion.source_binding_id,
        expected_sha256=completion.source_binding_sha256,
    )
    execution_store = DirectionalRawPreviewExecutionPlanStore(root)
    execution_plan, execution_plan_document = execution_store.load_execution_plan(
        completion.execution_plan_id,
        expected_sha256=completion.execution_plan_sha256,
    )
    execution_request, execution_request_document = execution_store.load_execution_request(
        completion.execution_request_event_id,
        expected_sha256=completion.execution_request_sha256,
    )
    if (
        binding.relative_path != completion.source_binding_path
        or binding_document.bytes != completion.source_binding_bytes
        or execution_plan.relative_path != completion.execution_plan_path
        or execution_plan_document.bytes != completion.execution_plan_bytes
        or execution_request.relative_path != completion.execution_request_path
        or execution_request_document.bytes != completion.execution_request_bytes
        or execution_plan.source_binding_manifest_id != binding.source_binding_id
        or execution_request.plan_id != execution_plan.plan_id
        or execution_request.manifest_preflight_intent_id != expected_intent.intent_id
    ):
        raise IdentityDirectionalRawPreviewManifestRunnerError(
            "manifest completion output readback differs"
        )
    return S7DirectionalRawPreviewManifestRun(
        expected_intent,
        intent_document,
        binding,
        binding_document,
        execution_plan,
        execution_plan_document,
        execution_request,
        execution_request_document,
        completion,
        completion_document,
        0,
        0,
        True,
    )


def _acquire_manifest_run_lock(
    root: Path,
    plan_id: str,
    approval_id: str,
    started: float,
    caps: Any,
) -> int:
    relative = (
        "manifests/silver/identity/directional-raw-preview-manifest-preflight-locks/"
        f"plan_id={plan_id}/approval_id={approval_id}.lock"
    )
    path = safe_relative_path(root, relative)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        descriptor = os.open(
            path,
            os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
    except OSError as exc:
        raise IdentityDirectionalRawPreviewManifestRunnerError(
            "manifest run lock is unavailable"
        ) from exc
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise IdentityDirectionalRawPreviewManifestRunnerError(
                "manifest run lock is not a regular file"
            )
        while True:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return descriptor
            except BlockingIOError:
                if time.monotonic() - started > caps.wall_clock_seconds_hard_cap:
                    raise IdentityDirectionalRawPreviewManifestRunnerError(
                        "manifest run lock wait exceeded wall cap"
                    ) from None
                time.sleep(0.05)
    except BaseException:
        os.close(descriptor)
        raise


def _read_release_manifests(
    root: Path,
    *,
    json_budget: list[int],
    operation_counts: dict[str, int],
) -> tuple[
    tuple[S7DirectionalRawPreviewManifestDocumentRef, ...],
    tuple[tuple[str, Any, str], ...],
]:
    manifest_refs = []
    selected = []
    for pin in S4_SOURCE_PINS:
        table = str(pin["table"])
        relative = f"manifests/silver/releases/release_id={pin['release_id']}.json"
        _path, content, document = _read_exact_json(
            root,
            relative,
            str(pin["release_manifest_sha256"]),
            json_budget=json_budget,
            operation_counts=operation_counts,
        )
        release = ReleaseManifest.from_dict(document)
        if (
            release.release_id != pin["release_id"]
            or release.table != table
            or release.build_id != pin["build_id"]
            or len(release.outputs) != pin["artifact_count"]
            or sum(item.row_count or 0 for item in release.outputs) != pin["row_count"]
        ):
            raise IdentityDirectionalRawPreviewManifestRunnerError(
                f"release manifest differs for {table}"
            )
        by_session = {}
        for output in release.outputs:
            match = _SESSION.search(output.path)
            if match is None or output.table != table or output.role is not ArtifactRole.DATA:
                raise IdentityDirectionalRawPreviewManifestRunnerError(
                    f"release output metadata invalid for {table}"
                )
            session = date.fromisoformat(match.group(1))
            if session in by_session:
                raise IdentityDirectionalRawPreviewManifestRunnerError(
                    f"duplicate release session for {table}"
                )
            by_session[session] = output
        for session in _FIXED_SESSIONS:
            output = by_session.get(session)
            if output is None or output.row_count is None:
                raise IdentityDirectionalRawPreviewManifestRunnerError(
                    f"release missing exact session for {table}"
                )
            selected.append((table, output, str(pin["release_manifest_sha256"])))
        kind = (
            "asset_release_manifest"
            if table == "asset_observation_daily"
            else "universe_release_manifest"
        )
        manifest_refs.append(
            S7DirectionalRawPreviewManifestDocumentRef(
                kind,
                release.release_id,
                relative,
                hashlib.sha256(content).hexdigest(),
                len(content),
            )
        )
    return tuple(manifest_refs), tuple(selected)


def _read_release_manifests_and_lstat(
    root: Path,
    *,
    json_budget: list[int] | None = None,
    operation_counts: dict[str, int] | None = None,
) -> tuple[
    tuple[S7DirectionalRawPreviewManifestDocumentRef, ...],
    tuple[S7DirectionalRawPreviewSourceArtifactRef, ...],
]:
    budget = json_budget if json_budget is not None else [0]
    counts = operation_counts if operation_counts is not None else {"json_reads": 0, "lstats": 0}
    manifests, selected = _read_release_manifests(
        root,
        json_budget=budget,
        operation_counts=counts,
    )
    refs = []
    for table, output, manifest_sha in selected:
        try:
            relative = Path(output.path)
            if (
                relative.is_absolute()
                or relative.as_posix() != output.path
                or relative.name in {"", ".", ".."}
            ):
                raise ArtifactError("artifact path is not lexically normalized")
            parent_relative = relative.parent.as_posix()
            parent = (
                root
                if parent_relative == "."
                else safe_relative_path(root, parent_relative)
            )
            artifact = parent / relative.name
        except ArtifactError as exc:
            raise IdentityDirectionalRawPreviewManifestRunnerError(
                f"artifact path is unsafe: {output.path}"
            ) from exc
        try:
            info = os.lstat(artifact)
        except OSError as exc:
            raise IdentityDirectionalRawPreviewManifestRunnerError(
                f"artifact lstat failed: {output.path}"
            ) from exc
        counts["lstats"] += 1
        if (
            stat.S_ISLNK(info.st_mode)
            or not stat.S_ISREG(info.st_mode)
            or info.st_size != output.bytes
        ):
            raise IdentityDirectionalRawPreviewManifestRunnerError(
                f"artifact lstat differs: {output.path}"
            )
        match = _SESSION.search(output.path)
        assert match is not None
        contract = _CONTRACTS[table]
        pin = next(item for item in S4_SOURCE_PINS if item["table"] == table)
        refs.append(
            S7DirectionalRawPreviewSourceArtifactRef(
                table=table,
                session_date=date.fromisoformat(match.group(1)),
                release_id=str(pin["release_id"]),
                release_manifest_sha256=manifest_sha,
                source_contract_id=contract.contract_id,
                source_schema_digest=contract.schema_digest,
                path=output.path,
                sha256=output.sha256,
                bytes=output.bytes,
                row_count=output.row_count,
                disk_size_bytes=info.st_size,
            )
        )
    if len(refs) != 22:
        raise IdentityDirectionalRawPreviewManifestRunnerError("lstat count is not exactly 22")
    return manifests, tuple(refs)


def _read_inventory_manifests(
    root: Path,
    *,
    json_budget: list[int],
    operation_counts: dict[str, int],
) -> tuple[S7DirectionalRawPreviewManifestDocumentRef, S7DirectionalRawPreviewManifestDocumentRef]:
    directory_relative = inventory_completion_discovery_directory()
    directory = safe_relative_path(root, directory_relative)
    if not directory.is_dir() or directory.is_symlink():
        raise IdentityDirectionalRawPreviewManifestRunnerError(
            "inventory completion directory is missing or unsafe"
        )
    children = []
    for child in directory.iterdir():
        children.append(child)
        if len(children) > 1:
            break
    if (
        len(children) != 1
        or children[0].is_symlink()
        or not children[0].is_dir()
        or not _APPROVAL_DIR.fullmatch(children[0].name)
    ):
        raise IdentityDirectionalRawPreviewManifestRunnerError(
            "inventory completion discovery is not exactly one approval"
        )
    completion_relative = f"{directory_relative}/{children[0].name}/manifest.json"
    _completion_path, completion_content, completion = _read_exact_json(
        root,
        completion_relative,
        None,
        json_budget=json_budget,
        operation_counts=operation_counts,
    )
    completion_logical = dict(completion)
    completion_logical.pop("completion_id", None)
    if (
        completion.get("completion_id") != INVENTORY_COMPLETION_ID
        or stable_digest(completion_logical) != INVENTORY_COMPLETION_ID
        or completion.get("plan_id") != INVENTORY_EXECUTION_PLAN_ID
        or completion.get("plan_sha256") != INVENTORY_EXECUTION_PLAN_SHA256
        or completion.get("input_binding_digest") != INVENTORY_INPUT_BINDING_DIGEST
        or completion.get("source_artifact_set_digest") != INVENTORY_SOURCE_ARTIFACT_SET_DIGEST
        or completion.get("completion_state") != "awaiting_review"
        or not isinstance(completion.get("capabilities"), dict)
        or any(completion["capabilities"].values())
    ):
        raise IdentityDirectionalRawPreviewManifestRunnerError(
            "inventory completion lineage differs"
        )
    candidate = completion.get("candidate")
    if not isinstance(candidate, dict):
        raise IdentityDirectionalRawPreviewManifestRunnerError("completion candidate missing")
    candidate_relative = (
        "manifests/silver/identity/composite-inventory-candidates/"
        f"candidate_id={INVENTORY_CANDIDATE_ID}/manifest.json"
    )
    _candidate_path, candidate_content, candidate_document = _read_exact_json(
        root,
        candidate_relative,
        INVENTORY_CANDIDATE_MANIFEST_SHA256,
        json_budget=json_budget,
        operation_counts=operation_counts,
    )
    data = candidate.get("data")
    candidate_logical = dict(candidate_document)
    candidate_logical.pop("candidate_id", None)
    candidate_logical.pop("canonical_paths", None)
    candidate_contract = candidate_document.get("contract")
    if (
        completion.get("source_artifact_set_digest") is None
        or candidate.get("candidate_id") != INVENTORY_CANDIDATE_ID
        or candidate.get("path") != candidate_relative
        or candidate.get("sha256") != INVENTORY_CANDIDATE_MANIFEST_SHA256
        or candidate.get("bytes") != len(candidate_content)
        or not isinstance(data, dict)
        or data.get("sha256") != INVENTORY_CANDIDATE_DATA_SHA256
        or candidate_document.get("candidate_id") != INVENTORY_CANDIDATE_ID
        or stable_digest(candidate_logical) != INVENTORY_CANDIDATE_ID
        or candidate_document.get("input_binding_digest") != INVENTORY_INPUT_BINDING_DIGEST
        or candidate_document.get("source_artifact_set_digest")
        != completion.get("source_artifact_set_digest")
        or candidate_document.get("candidate_state") != "awaiting_review"
        or not isinstance(candidate_document.get("capabilities"), dict)
        or any(candidate_document["capabilities"].values())
        or not isinstance(candidate_contract, dict)
        or candidate_contract.get("contract_id") != INVENTORY_CONTRACT_ID
        or candidate_contract.get("schema_digest") != INVENTORY_SCHEMA_DIGEST
    ):
        raise IdentityDirectionalRawPreviewManifestRunnerError(
            "inventory candidate lineage differs"
        )
    return (
        S7DirectionalRawPreviewManifestDocumentRef(
            "inventory_completion_manifest",
            INVENTORY_COMPLETION_ID,
            completion_relative,
            hashlib.sha256(completion_content).hexdigest(),
            len(completion_content),
        ),
        S7DirectionalRawPreviewManifestDocumentRef(
            "inventory_candidate_manifest",
            INVENTORY_CANDIDATE_ID,
            candidate_relative,
            hashlib.sha256(candidate_content).hexdigest(),
            len(candidate_content),
        ),
    )


def _read_exact_json(
    root: Path,
    relative: str,
    expected_sha256: str | None,
    *,
    json_budget: list[int],
    operation_counts: dict[str, int],
) -> tuple[Path, bytes, dict[str, Any]]:
    path = safe_relative_path(root, relative)
    try:
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    except OSError as exc:
        raise IdentityDirectionalRawPreviewManifestRunnerError(
            f"JSON manifest missing or unsafe: {relative}"
        ) from exc
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            raise IdentityDirectionalRawPreviewManifestRunnerError(
                f"JSON manifest is not regular: {relative}"
            )
        if json_budget[0] + info.st_size > 32 * 1024 * 1024:
            raise IdentityDirectionalRawPreviewManifestRunnerError(
                "JSON manifest bytes cap exceeded"
            )
        chunks = []
        remaining = info.st_size
        while remaining:
            chunk = os.read(descriptor, min(remaining, 1024 * 1024))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        content = b"".join(chunks)
        if len(content) != info.st_size:
            raise IdentityDirectionalRawPreviewManifestRunnerError(
                f"JSON manifest changed while read: {relative}"
            )
    finally:
        os.close(descriptor)
    json_budget[0] += len(content)
    operation_counts["json_reads"] += 1
    checksum = hashlib.sha256(content).hexdigest()
    if expected_sha256 is not None and checksum != expected_sha256:
        raise IdentityDirectionalRawPreviewManifestRunnerError(
            f"JSON manifest SHA differs: {relative}"
        )
    try:
        document = json.loads(content, object_pairs_hook=_reject_duplicates)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise IdentityDirectionalRawPreviewManifestRunnerError(
            f"JSON manifest invalid: {relative}"
        ) from exc
    if not isinstance(document, dict):
        raise IdentityDirectionalRawPreviewManifestRunnerError(
            f"JSON manifest is not object: {relative}"
        )
    canonical = (
        json.dumps(
            document,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        + b"\n"
    )
    if canonical != content:
        raise IdentityDirectionalRawPreviewManifestRunnerError(
            f"JSON manifest is not canonical: {relative}"
        )
    return path, content, document


def _check_resource_caps(plan: Any, started: float, json_bytes: int) -> None:
    if time.monotonic() - started > plan.resource_caps.wall_clock_seconds_hard_cap:
        raise IdentityDirectionalRawPreviewManifestRunnerError("wall clock cap exceeded")
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    peak_bytes = int(peak if peak > 10_000_000 else peak * 1024)
    if peak_bytes > plan.resource_caps.rss_bytes_hard_cap:
        raise IdentityDirectionalRawPreviewManifestRunnerError("RSS cap exceeded")
    if json_bytes > plan.resource_caps.json_manifest_bytes_hard_cap:
        raise IdentityDirectionalRawPreviewManifestRunnerError("JSON bytes cap exceeded")


def _verify_git(repository: Path, plan: Any) -> None:
    try:
        if (
            Path(_git(repository, "rev-parse", "--show-toplevel")).resolve() != repository
            or _git(repository, "rev-parse", "HEAD") != plan.git_commit
            or _git(repository, "rev-parse", "HEAD^{tree}") != plan.git_tree
            or _git(repository, "status", "--porcelain", "--untracked-files=all")
        ):
            raise IdentityDirectionalRawPreviewManifestRunnerError("Git checkout differs from plan")
        for pin in (*plan.runtime_files, *plan.verification_files):
            path = repository / pin.path
            if (
                not path.is_file()
                or path.is_symlink()
                or sha256_file(path) != pin.sha256
                or path.stat().st_size != pin.bytes
                or _git(repository, "rev-parse", f"HEAD:{pin.path}") != pin.git_blob
            ):
                raise IdentityDirectionalRawPreviewManifestRunnerError(
                    f"Git file pin differs: {pin.path}"
                )
    except (OSError, subprocess.SubprocessError) as exc:
        raise IdentityDirectionalRawPreviewManifestRunnerError("Git preflight failed") from exc


def _preflight_destination(root: Path, relative: str, checksum: str) -> bool:
    path = safe_relative_path(root, relative)
    if not path.exists() and not path.is_symlink():
        return False
    if not path.is_file() or path.is_symlink() or sha256_file(path) != checksum:
        raise IdentityDirectionalRawPreviewManifestRunnerError(
            f"immutable output conflicts: {relative}"
        )
    return True


def _root(value: Path, label: str) -> Path:
    expanded = value.expanduser()
    if expanded.is_symlink():
        raise IdentityDirectionalRawPreviewManifestRunnerError(f"{label} is symlink")
    root = expanded.resolve()
    if not root.is_dir():
        raise IdentityDirectionalRawPreviewManifestRunnerError(f"{label} must exist")
    return root


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None or value.utcoffset().total_seconds() != 0:
        raise IdentityDirectionalRawPreviewManifestRunnerError("created time must be UTC")
    return value.astimezone(UTC)


def _git(root: Path, *arguments: str) -> str:
    return subprocess.run(
        ("git", "-C", str(root), *arguments),
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    ).stdout.strip()


def _reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result = {}
    for key, value in pairs:
        if key in result:
            raise IdentityDirectionalRawPreviewManifestRunnerError("duplicate JSON key")
        result[key] = value
    return result


__all__ = [
    "IdentityDirectionalRawPreviewManifestRunnerError",
    "S7DirectionalRawPreviewManifestRun",
    "run_s7_directional_raw_preview_manifest_preflight",
]
