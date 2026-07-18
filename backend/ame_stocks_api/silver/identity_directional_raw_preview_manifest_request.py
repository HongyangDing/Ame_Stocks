"""Freeze three local controls for a future manifest-only S7 preflight.

This orchestration validates the already-approved preparation controls and a
clean Git tree, then writes only: (1) the preparation authorization receipt,
(2) a manifest-only preflight Plan, and (3) its human approval Request.  It has
no release-store, directory-discovery, source-data, network, or runner import.
"""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from types import MappingProxyType
from typing import Any, Final

from ame_stocks_api.artifacts import (
    ArtifactError,
    safe_relative_path,
    sha256_file,
    stable_digest,
    write_bytes_immutable,
)
from ame_stocks_api.silver.identity_directional_raw_preview_manifest_plan import (
    MANIFEST_AUTHORIZED_ACTION,
    MANIFEST_EXECUTION_DATA_ROOT,
    MANIFEST_LITERAL_VERSION,
    MANIFEST_SELECTION_SEMANTICS_DIGEST,
    PREPARATION_LITERAL,
    PREPARATION_LITERAL_SHA256,
    PREPARATION_PLAN_ID,
    PREPARATION_PLAN_SHA256,
    PREPARATION_REQUEST_EVENT_ID,
    PREPARATION_REQUEST_EVENT_SHA256,
    PREPARATION_SCOPE_SET_ID,
    PREPARATION_SCOPE_SET_SHA256,
    REQUIRED_MANIFEST_RUNTIME_PATHS,
    REQUIRED_MANIFEST_VERIFICATION_PATHS,
    IdentityDirectionalRawPreviewManifestStore,
    S7DirectionalRawPreviewManifestFilePin,
    S7DirectionalRawPreviewManifestPreflightPlan,
    S7DirectionalRawPreviewPreparationAuthorizationReceipt,
    StoredDirectionalRawPreviewManifestControl,
    directional_manifest_preflight_plan_path,
    preparation_control_lineage,
)
from ame_stocks_api.silver.identity_directional_raw_preview_plan import (
    IdentityDirectionalRawPreviewPlanStore,
    directional_raw_preview_plan_path,
    directional_raw_preview_scope_path,
)
from ame_stocks_api.silver.identity_directional_raw_preview_request import (
    S7DirectionalRawPreviewPreparationRequest,
    directional_raw_preview_request_path,
)

REQUEST_SCHEMA_VERSION: Final = 1
REQUEST_RULE_VERSION: Final = "s7_directional_manifest_preflight_request_v1"
REQUEST_STATE: Final = "awaiting_literal_human_approval"

_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_GIT_OBJECT = re.compile(r"^[0-9a-f]{40}$")


class IdentityDirectionalRawPreviewManifestRequestError(RuntimeError):
    """Raised before any manifest preflight control can be written."""


def _canonical_bytes(value: Mapping[str, object]) -> bytes:
    return (
        json.dumps(
            dict(value),
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        + b"\n"
    )


def _text(value: object, label: str) -> str:
    if not isinstance(value, str):
        raise IdentityDirectionalRawPreviewManifestRequestError(f"{label} must be text")
    return value


def _digest(value: object, label: str) -> str:
    text = _text(value, label)
    if not _DIGEST.fullmatch(text):
        raise IdentityDirectionalRawPreviewManifestRequestError(f"{label} must be lowercase 64-hex")
    return text


def _safe_actor(value: object, label: str) -> str:
    text = _text(value, label)
    if (
        not text
        or len(text) > 200
        or text.strip() != text
        or any(ord(char) < 32 or ord(char) == 127 for char in text)
    ):
        raise IdentityDirectionalRawPreviewManifestRequestError(f"{label} is unsafe")
    return text


def _parse_utc(value: object, label: str) -> datetime:
    text = _text(value, label)
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise IdentityDirectionalRawPreviewManifestRequestError(
            f"{label} must be canonical UTC"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise IdentityDirectionalRawPreviewManifestRequestError(f"{label} must include UTC")
    normalized = parsed.astimezone(UTC)
    if parsed.utcoffset().total_seconds() != 0 or normalized.isoformat() != text:
        raise IdentityDirectionalRawPreviewManifestRequestError(f"{label} must be canonical UTC")
    return normalized


@dataclass(frozen=True, slots=True)
class S7DirectionalRawPreviewManifestPreflightRequest:
    plan_id: str
    plan_path: str
    plan_sha256: str
    preparation_authorization_id: str
    preparation_authorization_sha256: str
    input_binding_digest: str
    resource_caps_digest: str
    runtime_file_set_digest: str
    verification_file_set_digest: str
    selection_semantics_digest: str
    execution_data_root: str
    future_manifest_reader_actor: str
    future_execution_plan_actor: str
    future_execution_request_actor: str
    created_by: str
    created_at_utc: datetime
    authorized_action: str = MANIFEST_AUTHORIZED_ACTION
    request_state: str = REQUEST_STATE

    def __post_init__(self) -> None:
        for label, value in (
            ("plan ID", self.plan_id),
            ("plan SHA-256", self.plan_sha256),
            ("preparation authorization ID", self.preparation_authorization_id),
            ("preparation authorization SHA-256", self.preparation_authorization_sha256),
            ("input binding digest", self.input_binding_digest),
            ("resource caps digest", self.resource_caps_digest),
            ("runtime file-set digest", self.runtime_file_set_digest),
            ("verification file-set digest", self.verification_file_set_digest),
            ("selection semantics digest", self.selection_semantics_digest),
        ):
            _digest(value, label)
        if self.plan_path != directional_manifest_preflight_plan_path(self.plan_id):
            raise IdentityDirectionalRawPreviewManifestRequestError(
                "manifest preflight plan path is not canonical"
            )
        if self.selection_semantics_digest != MANIFEST_SELECTION_SEMANTICS_DIGEST:
            raise IdentityDirectionalRawPreviewManifestRequestError("selection semantics changed")
        if self.execution_data_root != MANIFEST_EXECUTION_DATA_ROOT:
            raise IdentityDirectionalRawPreviewManifestRequestError(
                "manifest execution data root changed"
            )
        future_actors = (
            _safe_actor(self.future_manifest_reader_actor, "future manifest reader actor"),
            _safe_actor(self.future_execution_plan_actor, "future execution plan actor"),
            _safe_actor(self.future_execution_request_actor, "future execution request actor"),
        )
        _safe_actor(self.created_by, "request created_by")
        if len(set((*future_actors, self.created_by))) != 4:
            raise IdentityDirectionalRawPreviewManifestRequestError(
                "request and future downstream actors must be distinct"
            )
        object.__setattr__(
            self, "created_at_utc", _validate_utc_datetime(self.created_at_utc, "created_at_utc")
        )
        if (
            self.authorized_action != MANIFEST_AUTHORIZED_ACTION
            or self.request_state != REQUEST_STATE
        ):
            raise IdentityDirectionalRawPreviewManifestRequestError(
                "manifest request boundary changed"
            )

    @classmethod
    def create(
        cls,
        plan: S7DirectionalRawPreviewManifestPreflightPlan,
        plan_receipt: StoredDirectionalRawPreviewManifestControl,
        *,
        created_by: str,
        created_at_utc: datetime,
    ) -> S7DirectionalRawPreviewManifestPreflightRequest:
        if not isinstance(plan, S7DirectionalRawPreviewManifestPreflightPlan):
            raise IdentityDirectionalRawPreviewManifestRequestError("plan has wrong type")
        if (
            plan_receipt.path != plan.relative_path
            or plan_receipt.sha256 != plan.sha256
            or plan_receipt.bytes != len(plan.content)
        ):
            raise IdentityDirectionalRawPreviewManifestRequestError("plan receipt differs")
        created = _validate_utc_datetime(created_at_utc, "created_at_utc")
        if created < plan.created_at_utc or created_by == plan.created_by:
            raise IdentityDirectionalRawPreviewManifestRequestError(
                "request must follow the plan and use a separate actor"
            )
        return cls(
            plan_id=plan.plan_id,
            plan_path=plan_receipt.path,
            plan_sha256=plan_receipt.sha256,
            preparation_authorization_id=plan.preparation_authorization_id,
            preparation_authorization_sha256=plan.preparation_authorization_sha256,
            input_binding_digest=plan.input_binding_digest,
            resource_caps_digest=plan.resource_caps.digest,
            runtime_file_set_digest=plan.runtime_file_set_digest,
            verification_file_set_digest=plan.verification_file_set_digest,
            selection_semantics_digest=MANIFEST_SELECTION_SEMANTICS_DIGEST,
            execution_data_root=plan.execution_data_root,
            future_manifest_reader_actor=plan.future_manifest_reader_actor,
            future_execution_plan_actor=plan.future_execution_plan_actor,
            future_execution_request_actor=plan.future_execution_request_actor,
            created_by=created_by,
            created_at_utc=created,
        )

    def logical_payload(self) -> dict[str, object]:
        return {
            "artifact_type": "s7_directional_raw_preview_manifest_preflight_request",
            "authorization_boundary": {
                "adjudication": False,
                "approval_receipt_creation": False,
                "bounded_directory_metadata_read_after_literal": True,
                "exact_four_json_manifest_read_after_literal": True,
                "full_run": False,
                "network_access": False,
                "parquet_content_read": False,
                "parquet_lstat_after_literal": True,
                "preview_execution": False,
                "publication": False,
                "registry_evaluation": False,
                "runner": False,
            },
            "authorized_action": self.authorized_action,
            "created_at_utc": self.created_at_utc.isoformat(),
            "created_by": self.created_by,
            "execution_data_root": self.execution_data_root,
            "future_manifest_reader_actor": self.future_manifest_reader_actor,
            "future_execution_plan_actor": self.future_execution_plan_actor,
            "future_execution_request_actor": self.future_execution_request_actor,
            "future_output_boundary": {
                "canonical_json_count": 5,
                "exact_execution_plan_request_only": True,
                "source_binding_manifest": True,
                "state_after_success": "awaiting_review",
            },
            "input_binding_digest": self.input_binding_digest,
            "plan_id": self.plan_id,
            "plan_path": self.plan_path,
            "plan_sha256": self.plan_sha256,
            "preparation_authorization_id": self.preparation_authorization_id,
            "preparation_authorization_sha256": self.preparation_authorization_sha256,
            "preparation_control_lineage": preparation_control_lineage(),
            "request_rule_version": REQUEST_RULE_VERSION,
            "request_state": self.request_state,
            "resource_caps_digest": self.resource_caps_digest,
            "runtime_file_set_digest": self.runtime_file_set_digest,
            "schema_version": REQUEST_SCHEMA_VERSION,
            "selection_semantics_digest": self.selection_semantics_digest,
            "verification_file_set_digest": self.verification_file_set_digest,
        }

    @property
    def request_event_id(self) -> str:
        return stable_digest(self.logical_payload())

    @property
    def document(self) -> Mapping[str, object]:
        return MappingProxyType(
            {**self.logical_payload(), "request_event_id": self.request_event_id}
        )

    @property
    def content(self) -> bytes:
        return _canonical_bytes(self.document)

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.content).hexdigest()

    @property
    def relative_path(self) -> str:
        return directional_manifest_preflight_request_path(self.request_event_id)

    @property
    def canonical_approval_literal(self) -> str:
        return json.dumps(
            {
                "authorized_action": self.authorized_action,
                "execution_data_root": self.execution_data_root,
                "expected_source_artifact_count": 22,
                "future_output_json_count": 5,
                "future_execution_plan_actor": self.future_execution_plan_actor,
                "future_execution_request_actor": self.future_execution_request_actor,
                "future_manifest_reader_actor": self.future_manifest_reader_actor,
                "input_binding_digest": self.input_binding_digest,
                "literal_version": MANIFEST_LITERAL_VERSION,
                "plan_id": self.plan_id,
                "plan_sha256": self.plan_sha256,
                "preparation_authorization_id": self.preparation_authorization_id,
                "preparation_authorization_sha256": self.preparation_authorization_sha256,
                "preparation_literal_sha256": PREPARATION_LITERAL_SHA256,
                "preparation_plan_id": PREPARATION_PLAN_ID,
                "preparation_plan_sha256": PREPARATION_PLAN_SHA256,
                "preparation_request_event_id": PREPARATION_REQUEST_EVENT_ID,
                "preparation_request_event_sha256": PREPARATION_REQUEST_EVENT_SHA256,
                "request_event_id": self.request_event_id,
                "request_event_sha256": self.sha256,
                "resource_caps_digest": self.resource_caps_digest,
                "runtime_file_set_digest": self.runtime_file_set_digest,
                "scope_set_id": PREPARATION_SCOPE_SET_ID,
                "scope_set_sha256": PREPARATION_SCOPE_SET_SHA256,
                "selection_semantics_digest": self.selection_semantics_digest,
                "verification_file_set_digest": self.verification_file_set_digest,
            },
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )

    @classmethod
    def from_dict(cls, value: object) -> S7DirectionalRawPreviewManifestPreflightRequest:
        if not isinstance(value, Mapping):
            raise IdentityDirectionalRawPreviewManifestRequestError("request must be an object")
        document = dict(value)
        result = cls(
            plan_id=_text(document.get("plan_id"), "plan_id"),
            plan_path=_text(document.get("plan_path"), "plan_path"),
            plan_sha256=_text(document.get("plan_sha256"), "plan_sha256"),
            preparation_authorization_id=_text(
                document.get("preparation_authorization_id"), "preparation_authorization_id"
            ),
            preparation_authorization_sha256=_text(
                document.get("preparation_authorization_sha256"),
                "preparation_authorization_sha256",
            ),
            input_binding_digest=_text(
                document.get("input_binding_digest"), "input_binding_digest"
            ),
            resource_caps_digest=_text(
                document.get("resource_caps_digest"), "resource_caps_digest"
            ),
            runtime_file_set_digest=_text(
                document.get("runtime_file_set_digest"), "runtime_file_set_digest"
            ),
            verification_file_set_digest=_text(
                document.get("verification_file_set_digest"), "verification_file_set_digest"
            ),
            selection_semantics_digest=_text(
                document.get("selection_semantics_digest"), "selection_semantics_digest"
            ),
            execution_data_root=_text(document.get("execution_data_root"), "execution_data_root"),
            future_manifest_reader_actor=_text(
                document.get("future_manifest_reader_actor"), "future_manifest_reader_actor"
            ),
            future_execution_plan_actor=_text(
                document.get("future_execution_plan_actor"), "future_execution_plan_actor"
            ),
            future_execution_request_actor=_text(
                document.get("future_execution_request_actor"),
                "future_execution_request_actor",
            ),
            created_by=_text(document.get("created_by"), "created_by"),
            created_at_utc=_parse_utc(document.get("created_at_utc"), "created_at_utc"),
            authorized_action=_text(document.get("authorized_action"), "authorized_action"),
            request_state=_text(document.get("request_state"), "request_state"),
        )
        if _canonical_bytes(document) != result.content:
            raise IdentityDirectionalRawPreviewManifestRequestError(
                "manifest request is not canonical or exact"
            )
        return result


@dataclass(frozen=True, slots=True)
class S7DirectionalRawPreviewManifestRequestRun:
    preparation_authorization: S7DirectionalRawPreviewPreparationAuthorizationReceipt
    preparation_authorization_document: StoredDirectionalRawPreviewManifestControl
    plan: S7DirectionalRawPreviewManifestPreflightPlan
    plan_document: StoredDirectionalRawPreviewManifestControl
    request: S7DirectionalRawPreviewManifestPreflightRequest
    request_document: StoredDirectionalRawPreviewManifestControl
    git_tree: str
    all_documents_preexisting: bool


@dataclass(frozen=True, slots=True)
class RepositoryManifestFilePin:
    path: str
    git_blob: str
    sha256: str
    bytes: int

    def to_plan_pin(self) -> S7DirectionalRawPreviewManifestFilePin:
        return S7DirectionalRawPreviewManifestFilePin(
            path=self.path,
            git_blob=self.git_blob,
            sha256=self.sha256,
            bytes=self.bytes,
        )


def create_s7_directional_raw_preview_manifest_request(
    control_root: Path,
    *,
    repo_root: Path,
    git_commit: str,
    recorded_at: str,
    preparation_authorization_recorded_by: str,
    plan_created_by: str,
    request_created_by: str,
    future_manifest_reader_actor: str,
    future_execution_plan_actor: str,
    future_execution_request_actor: str,
    execution_data_root: str = MANIFEST_EXECUTION_DATA_ROOT,
) -> S7DirectionalRawPreviewManifestRequestRun:
    """Preflight all inputs and destinations, then write exactly three JSON controls."""

    instant = parse_nonfuture_utc(recorded_at)
    _require_distinct_actors(
        preparation_authorization_recorded_by,
        plan_created_by,
        request_created_by,
        future_manifest_reader_actor,
        future_execution_plan_actor,
        future_execution_request_actor,
    )
    repository, git_tree = verify_exact_clean_checkout(repo_root, git_commit)
    root = _validated_control_root(control_root)
    _load_and_verify_exact_preparation_controls(root)
    runtime_files = tuple(
        item.to_plan_pin()
        for item in pin_tracked_files(repository, REQUIRED_MANIFEST_RUNTIME_PATHS)
    )
    verification_files = tuple(
        item.to_plan_pin()
        for item in pin_tracked_files(repository, REQUIRED_MANIFEST_VERIFICATION_PATHS)
    )

    authorization = S7DirectionalRawPreviewPreparationAuthorizationReceipt(
        recorded_by=preparation_authorization_recorded_by,
        recorded_at_utc=instant,
    )
    prospective_authorization = StoredDirectionalRawPreviewManifestControl(
        authorization.relative_path, authorization.sha256, len(authorization.content)
    )
    plan = S7DirectionalRawPreviewManifestPreflightPlan.create(
        created_by=plan_created_by,
        created_at_utc=instant,
        future_manifest_reader_actor=future_manifest_reader_actor,
        future_execution_plan_actor=future_execution_plan_actor,
        future_execution_request_actor=future_execution_request_actor,
        git_commit=git_commit,
        git_tree=git_tree,
        execution_data_root=execution_data_root,
        runtime_files=runtime_files,
        verification_files=verification_files,
        preparation_authorization=authorization,
        preparation_authorization_receipt=prospective_authorization,
    )
    prospective_plan = StoredDirectionalRawPreviewManifestControl(
        plan.relative_path, plan.sha256, len(plan.content)
    )
    request = S7DirectionalRawPreviewManifestPreflightRequest.create(
        plan,
        prospective_plan,
        created_by=request_created_by,
        created_at_utc=instant,
    )

    store = IdentityDirectionalRawPreviewManifestStore(root)
    destinations = (
        (authorization.relative_path, authorization.sha256),
        (plan.relative_path, plan.sha256),
        (request.relative_path, request.sha256),
    )
    preexisting = tuple(
        preflight_destination(root, relative, sha256) for relative, sha256 in destinations
    )
    authorization_document = store.store_preparation_authorization(authorization)
    plan_document = store.store_plan(plan)
    request_document = _store_request(root, request)

    loaded_authorization, loaded_authorization_document = store.load_preparation_authorization(
        authorization.authorization_id, expected_sha256=authorization.sha256
    )
    loaded_plan, loaded_plan_document = store.load_plan(plan.plan_id, expected_sha256=plan.sha256)
    loaded_request, loaded_request_document = load_manifest_preflight_request(
        root, request.request_event_id, expected_sha256=request.sha256
    )
    if (
        loaded_authorization != authorization
        or loaded_authorization_document != authorization_document
        or loaded_plan != plan
        or loaded_plan_document != plan_document
        or loaded_request != request
        or loaded_request_document != request_document
    ):
        raise IdentityDirectionalRawPreviewManifestRequestError(
            "manifest controls differ after immutable readback"
        )
    return S7DirectionalRawPreviewManifestRequestRun(
        preparation_authorization=authorization,
        preparation_authorization_document=authorization_document,
        plan=plan,
        plan_document=plan_document,
        request=request,
        request_document=request_document,
        git_tree=git_tree,
        all_documents_preexisting=all(preexisting),
    )


def _load_and_verify_exact_preparation_controls(root: Path) -> None:
    try:
        store = IdentityDirectionalRawPreviewPlanStore(root)
        scope, scope_receipt = store.load_scope(
            PREPARATION_SCOPE_SET_ID, expected_sha256=PREPARATION_SCOPE_SET_SHA256
        )
        plan, plan_receipt = store.load_plan(
            PREPARATION_PLAN_ID, expected_sha256=PREPARATION_PLAN_SHA256
        )
        request_relative = directional_raw_preview_request_path(PREPARATION_REQUEST_EVENT_ID)
        request_path = safe_relative_path(root, request_relative)
        if (
            not request_path.is_file()
            or request_path.is_symlink()
            or sha256_file(request_path) != PREPARATION_REQUEST_EVENT_SHA256
        ):
            raise IdentityDirectionalRawPreviewManifestRequestError(
                "exact preparation request is unavailable or altered"
            )
        request_content = request_path.read_bytes()
        request_document = json.loads(request_content, object_pairs_hook=_reject_duplicate_keys)
        if (
            not isinstance(request_document, dict)
            or _canonical_bytes(request_document) != request_content
        ):
            raise IdentityDirectionalRawPreviewManifestRequestError(
                "exact preparation request is not canonical JSON"
            )
        request = S7DirectionalRawPreviewPreparationRequest.from_dict(request_document)
    except (
        ArtifactError,
        IdentityDirectionalRawPreviewManifestRequestError,
        OSError,
        TypeError,
        ValueError,
    ):
        raise
    except Exception as exc:
        raise IdentityDirectionalRawPreviewManifestRequestError(
            "exact preparation control chain cannot be reproduced"
        ) from exc
    if (
        scope.scope_set_id != PREPARATION_SCOPE_SET_ID
        or scope_receipt.path != directional_raw_preview_scope_path(PREPARATION_SCOPE_SET_ID)
        or plan.plan_id != PREPARATION_PLAN_ID
        or plan_receipt.path != directional_raw_preview_plan_path(PREPARATION_PLAN_ID)
        or request.request_event_id != PREPARATION_REQUEST_EVENT_ID
        or request.sha256 != PREPARATION_REQUEST_EVENT_SHA256
        or request.plan_id != plan.plan_id
        or request.scope_set_id != scope.scope_set_id
        or request.canonical_approval_literal != PREPARATION_LITERAL
        or hashlib.sha256(request.canonical_approval_literal.encode()).hexdigest()
        != PREPARATION_LITERAL_SHA256
    ):
        raise IdentityDirectionalRawPreviewManifestRequestError(
            "preparation controls differ from the approved literal"
        )


def verify_exact_clean_checkout(repo_root: Path, expected_commit: str) -> tuple[Path, str]:
    if not isinstance(repo_root, Path):
        raise IdentityDirectionalRawPreviewManifestRequestError("repo_root must be a Path")
    if not isinstance(expected_commit, str) or not _GIT_OBJECT.fullmatch(expected_commit):
        raise IdentityDirectionalRawPreviewManifestRequestError("git_commit is invalid")
    expanded = repo_root.expanduser()
    if expanded.is_symlink():
        raise IdentityDirectionalRawPreviewManifestRequestError("repo_root cannot be a symlink")
    root = expanded.resolve()
    if not root.is_dir():
        raise IdentityDirectionalRawPreviewManifestRequestError("repo_root must exist")
    try:
        top = Path(_git(root, "rev-parse", "--show-toplevel")).resolve()
        head = _git(root, "rev-parse", "HEAD")
        tree = _git(root, "rev-parse", "HEAD^{tree}")
        status = _git(root, "status", "--porcelain", "--untracked-files=all")
    except (OSError, subprocess.SubprocessError) as exc:
        raise IdentityDirectionalRawPreviewManifestRequestError(
            "Git checkout cannot be verified"
        ) from exc
    if top != root or head != expected_commit or status:
        raise IdentityDirectionalRawPreviewManifestRequestError(
            "Git checkout is dirty, displaced, or at the wrong commit"
        )
    return root, tree


def pin_tracked_files(
    repo_root: Path, relative_paths: Iterable[str]
) -> tuple[RepositoryManifestFilePin, ...]:
    pins: list[RepositoryManifestFilePin] = []
    for relative in sorted(set(relative_paths)):
        path_fragment = Path(relative)
        if (
            not relative
            or path_fragment.is_absolute()
            or ".." in path_fragment.parts
            or path_fragment.as_posix() != relative
        ):
            raise IdentityDirectionalRawPreviewManifestRequestError(
                f"required tracked path is unsafe: {relative!r}"
            )
        path = repo_root / relative
        if not path.is_file() or path.is_symlink() or path.resolve() != path:
            raise IdentityDirectionalRawPreviewManifestRequestError(
                f"required tracked file is missing or unsafe: {relative}"
            )
        try:
            git_blob = _git(repo_root, "rev-parse", f"HEAD:{relative}")
            working_blob = _git(repo_root, "hash-object", "--no-filters", "--", relative)
        except (OSError, subprocess.SubprocessError) as exc:
            raise IdentityDirectionalRawPreviewManifestRequestError(
                f"required file is not tracked at HEAD: {relative}"
            ) from exc
        if git_blob != working_blob:
            raise IdentityDirectionalRawPreviewManifestRequestError(
                f"required file differs from HEAD: {relative}"
            )
        pins.append(
            RepositoryManifestFilePin(
                path=relative,
                git_blob=git_blob,
                sha256=sha256_file(path),
                bytes=path.stat().st_size,
            )
        )
    return tuple(pins)


def preflight_destination(root: Path, relative: str, expected_sha256: str) -> bool:
    try:
        path = safe_relative_path(root, relative)
    except ArtifactError as exc:
        raise IdentityDirectionalRawPreviewManifestRequestError(str(exc)) from exc
    current = root
    for part in path.parent.relative_to(root).parts:
        current /= part
        if (current.exists() or current.is_symlink()) and (
            current.is_symlink() or not current.is_dir()
        ):
            raise IdentityDirectionalRawPreviewManifestRequestError(
                f"immutable control parent is unsafe: {current.relative_to(root)}"
            )
    if not path.exists() and not path.is_symlink():
        return False
    if not path.is_file() or path.is_symlink() or sha256_file(path) != expected_sha256:
        raise IdentityDirectionalRawPreviewManifestRequestError(
            f"immutable manifest control conflicts: {relative}"
        )
    return True


def parse_nonfuture_utc(value: str) -> datetime:
    instant = _parse_utc(value, "recorded_at")
    if instant > datetime.now(UTC):
        raise IdentityDirectionalRawPreviewManifestRequestError(
            "recorded_at cannot be in the future"
        )
    return instant


def directional_manifest_preflight_request_path(request_event_id: str) -> str:
    _digest(request_event_id, "manifest request event ID")
    return (
        "manifests/silver/identity/directional-raw-preview-manifest-preflight-requests/"
        f"request_event_id={request_event_id}/manifest.json"
    )


def load_manifest_preflight_request(
    root: Path, request_event_id: str, *, expected_sha256: str
) -> tuple[
    S7DirectionalRawPreviewManifestPreflightRequest,
    StoredDirectionalRawPreviewManifestControl,
]:
    relative = directional_manifest_preflight_request_path(request_event_id)
    path = safe_relative_path(root, relative)
    if not path.is_file() or path.is_symlink() or sha256_file(path) != expected_sha256:
        raise IdentityDirectionalRawPreviewManifestRequestError(
            "manifest request is missing or altered"
        )
    content = path.read_bytes()
    try:
        document = json.loads(content, object_pairs_hook=_reject_duplicate_keys)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise IdentityDirectionalRawPreviewManifestRequestError(
            "manifest request is not JSON"
        ) from exc
    if not isinstance(document, dict) or _canonical_bytes(document) != content:
        raise IdentityDirectionalRawPreviewManifestRequestError(
            "manifest request is not canonical JSON"
        )
    request = S7DirectionalRawPreviewManifestPreflightRequest.from_dict(document)
    if request.request_event_id != request_event_id or request.sha256 != expected_sha256:
        raise IdentityDirectionalRawPreviewManifestRequestError(
            "manifest request path or bytes differ"
        )
    return request, StoredDirectionalRawPreviewManifestControl(
        relative, expected_sha256, len(content)
    )


def _store_request(
    root: Path, request: S7DirectionalRawPreviewManifestPreflightRequest
) -> StoredDirectionalRawPreviewManifestControl:
    try:
        path = safe_relative_path(root, request.relative_path)
        receipt = write_bytes_immutable(root, path, request.content)
    except ArtifactError as exc:
        raise IdentityDirectionalRawPreviewManifestRequestError(str(exc)) from exc
    return StoredDirectionalRawPreviewManifestControl(
        path=str(receipt["path"]),
        sha256=str(receipt["sha256"]),
        bytes=int(receipt["bytes"]),
    )


def _validated_control_root(value: Path) -> Path:
    if not isinstance(value, Path):
        raise IdentityDirectionalRawPreviewManifestRequestError("control_root must be a Path")
    expanded = value.expanduser()
    if expanded.is_symlink():
        raise IdentityDirectionalRawPreviewManifestRequestError("control_root cannot be a symlink")
    root = expanded.resolve()
    if not root.is_dir():
        raise IdentityDirectionalRawPreviewManifestRequestError("control_root must exist")
    return root


def _require_distinct_actors(*actors: str) -> None:
    normalized = tuple(_safe_actor(item, "control actor") for item in actors)
    if len(set(normalized)) != len(normalized):
        raise IdentityDirectionalRawPreviewManifestRequestError(
            "receipt, plan, request, and future reader actors must be distinct"
        )


def _validate_utc_datetime(value: datetime, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise IdentityDirectionalRawPreviewManifestRequestError(f"{label} must include UTC")
    if value.utcoffset().total_seconds() != 0:
        raise IdentityDirectionalRawPreviewManifestRequestError(f"{label} must be UTC")
    return value.astimezone(UTC)


def _git(root: Path, *arguments: str) -> str:
    completed = subprocess.run(
        ("git", "-C", str(root), *arguments),
        check=True,
        capture_output=True,
        text=True,
        timeout=20,
    )
    return completed.stdout.strip()


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise IdentityDirectionalRawPreviewManifestRequestError(
                "control contains duplicate JSON keys"
            )
        result[key] = value
    return result


__all__ = [
    "IdentityDirectionalRawPreviewManifestRequestError",
    "S7DirectionalRawPreviewManifestPreflightRequest",
    "S7DirectionalRawPreviewManifestRequestRun",
    "create_s7_directional_raw_preview_manifest_request",
    "directional_manifest_preflight_request_path",
    "load_manifest_preflight_request",
]
