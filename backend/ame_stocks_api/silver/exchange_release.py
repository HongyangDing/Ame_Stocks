"""Complete the reviewed S1 exchange_dim workflow without changing reviewed logic."""

from __future__ import annotations

import json
import subprocess
import sys
import tomllib
from dataclasses import dataclass, replace
from importlib.metadata import version
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from ame_stocks_api.silver.contracts import (
    ArtifactRole,
    BuildIntent,
    BuildKind,
    BuildManifest,
    QACheckResult,
    QASeverity,
    QAStatus,
    ReleaseManifest,
    SourceInventory,
    thaw_json,
)
from ame_stocks_api.silver.exchange_contract import EXCHANGE_DIM_CONTRACT
from ame_stocks_api.silver.exchange_preview import (
    _FIXED_CASE_QA_CHECKS,
    CURRENT_EXCHANGES_PREVIEW_AUTHORIZATION,
    ExchangePreviewAuthorization,
    _current_reference_snapshot_evidence,
    _git_output,
    _load_build_inventory,
    _load_event_preview,
    _load_orphan_build_if_present,
    _now_utc,
    _validate_authorized_source,
    _write_json_sample,
    _write_preview_outputs,
)
from ame_stocks_api.silver.exchange_source import read_exchange_source_inventory
from ame_stocks_api.silver.exchanges import ExchangeTransformResult, transform_exchange_batch
from ame_stocks_api.silver.reader import PublishedRelease, PublishedSilverReader
from ame_stocks_api.silver.store import (
    SilverStore,
    SilverStoreError,
    StoredDocument,
    WorkflowSnapshot,
    WorkflowState,
)

_REVIEWED_LOGIC_CLOSURE = (
    "backend/ame_stocks_api/artifacts.py",
    "backend/ame_stocks_api/silver/contracts.py",
    "backend/ame_stocks_api/silver/exchange_contract.py",
    "backend/ame_stocks_api/silver/exchange_preview.py",
    "backend/ame_stocks_api/silver/exchange_source.py",
    "backend/ame_stocks_api/silver/exchanges.py",
    "backend/ame_stocks_api/silver/reader.py",
    "backend/ame_stocks_api/silver/schema_resources/exchange_dim.schema-v1.json",
    "backend/ame_stocks_api/silver/store.py",
)
_ALLOWED_STATES = {
    WorkflowState.AWAITING_REVIEW,
    WorkflowState.APPROVED_FULL_RUN,
    WorkflowState.FULL_READY,
    WorkflowState.AWAITING_PUBLISH,
    WorkflowState.PUBLISHED,
}


@dataclass(frozen=True, slots=True)
class ExchangeReleaseRun:
    """Published S1 release and its fully verified evidence chain."""

    workflow: WorkflowSnapshot
    preview: BuildManifest
    preview_document: StoredDocument
    full: BuildManifest
    full_document: StoredDocument
    release: ReleaseManifest
    release_document: StoredDocument
    published: PublishedRelease


def complete_exchange_release(
    data_root: Path,
    *,
    workflow_id: str,
    expected_event_sha256: str,
    reviewed_preview_build_id: str,
    reviewed_preview_manifest_sha256: str,
    repo_root: Path,
    runner_git_commit: str,
    actor: str,
    approver: str,
) -> ExchangeReleaseRun:
    """Advance the exact reviewed exchanges preview to one verified published release."""

    return _complete_exchange_release_authorized(
        data_root,
        workflow_id=workflow_id,
        expected_event_sha256=expected_event_sha256,
        reviewed_preview_build_id=reviewed_preview_build_id,
        reviewed_preview_manifest_sha256=reviewed_preview_manifest_sha256,
        repo_root=repo_root,
        runner_git_commit=runner_git_commit,
        actor=actor,
        approver=approver,
        authorization=CURRENT_EXCHANGES_PREVIEW_AUTHORIZATION,
    )


def _complete_exchange_release_authorized(
    data_root: Path,
    *,
    workflow_id: str,
    expected_event_sha256: str,
    reviewed_preview_build_id: str,
    reviewed_preview_manifest_sha256: str,
    repo_root: Path,
    runner_git_commit: str,
    actor: str,
    approver: str,
    authorization: ExchangePreviewAuthorization,
) -> ExchangeReleaseRun:
    root = data_root.expanduser().resolve()
    store = SilverStore(root)
    current = store.verify_workflow_trust_chain(workflow_id, verify_artifacts=True)
    if current.event_sha256 != expected_event_sha256:
        raise SilverStoreError("stale exchanges release expected_event_sha256")
    if current.state not in _ALLOWED_STATES:
        raise SilverStoreError(f"exchanges release cannot continue from {current.state.value}")
    contract, _ = store.load_workflow_contract(workflow_id)
    if contract != EXCHANGE_DIM_CONTRACT:
        raise SilverStoreError("exchanges release workflow uses the wrong contract")
    preview, preview_document = _load_event_preview(store, current)
    if (
        preview.build_id != reviewed_preview_build_id
        or preview_document.sha256 != reviewed_preview_manifest_sha256
    ):
        raise SilverStoreError("reviewed exchanges preview identity does not match")
    _require_publishable_preview(preview, authorization.expected_rows)
    provenance = _runner_provenance(
        repo_root,
        runner_git_commit=runner_git_commit,
        preview_git_commit=preview.intent.git_commit,
        preview=preview,
    )
    inventory, _ = _load_build_inventory(root, preview)
    batch = read_exchange_source_inventory(root, inventory)
    _validate_authorized_source(
        batch,
        inventory,
        expected_manifest_sha256=authorization.manifest_sha256,
        expected_input_rows=authorization.expected_rows,
        authorization=authorization,
    )
    intent = _full_intent(preview)
    transformed = transform_exchange_batch(
        batch,
        build_id=intent.build_id,
        calendar_name="XNYS",
    )
    _require_preview_parity(root, preview, transformed)

    if current.state is WorkflowState.AWAITING_REVIEW:
        current = store.approve_full_run(
            workflow_id,
            expected_event_sha256=current.event_sha256,
            approver=approver,
            decided_at=_now_utc(),
            note=(
                "User approved S1 completion with no QA waivers or quarantine "
                f"acceptances; orchestration commit={runner_git_commit}."
            ),
        )
    if current.state is WorkflowState.APPROVED_FULL_RUN:
        full = _load_orphan_build_if_present(store, intent)
        if full is None:
            full = _materialize_full(
                root,
                intent=intent,
                inventory=inventory,
                preview=preview,
                provenance=provenance,
                authorization=authorization,
            )
        _require_full_matches_review(root, full, preview, provenance)
        _reverify_runner(repo_root, runner_git_commit, preview, provenance)
        current = store.record_full_build(
            full,
            expected_event_sha256=current.event_sha256,
            actor=actor,
            recorded_at=_now_utc(),
            note="Registered the exact review-bound S1 exchanges full build.",
        )
    if current.state is WorkflowState.FULL_READY:
        full, _ = _load_event_full(store, current)
        _require_full_matches_review(root, full, preview, provenance)
        _reverify_runner(repo_root, runner_git_commit, preview, provenance)
        current = store.request_publish(
            workflow_id,
            expected_event_sha256=current.event_sha256,
            actor=actor,
            created_at=_now_utc(),
            note="Submitted the verified S1 exchanges full build for publication.",
        )
    if current.state is WorkflowState.AWAITING_PUBLISH:
        full, _ = _load_event_full(store, current)
        _require_full_matches_review(root, full, preview, provenance)
        _reverify_runner(repo_root, runner_git_commit, preview, provenance)
        current, release = store.publish(
            workflow_id,
            expected_event_sha256=current.event_sha256,
            approver=approver,
            decided_at=_now_utc(),
            note=(
                "User authorized S1 publication with no QA waivers or quarantine "
                f"acceptances; orchestration commit={runner_git_commit}."
            ),
        )
    else:
        release = _load_event_release(store, current)

    verified = store.verify_workflow_trust_chain(workflow_id, verify_artifacts=True)
    if verified.state is not WorkflowState.PUBLISHED:
        raise SilverStoreError("exchanges workflow did not reach published")
    release_document = store.load_release(release.release_id)[1]
    published = PublishedSilverReader(root).inspect(release.release_id)
    full, full_document = store.load_build(EXCHANGE_DIM_CONTRACT.table, release.build_id)
    _require_full_matches_review(root, full, preview, provenance)
    if len(release.outputs) != 1 or release.outputs[0].role is not ArtifactRole.DATA:
        raise SilverStoreError("exchanges release must expose exactly one data artifact")
    return ExchangeReleaseRun(
        workflow=verified,
        preview=preview,
        preview_document=preview_document,
        full=full,
        full_document=full_document,
        release=release,
        release_document=release_document,
        published=published,
    )


def _full_intent(preview: BuildManifest) -> BuildIntent:
    if preview.preview is None:
        raise SilverStoreError("reviewed exchanges preview metadata is missing")
    parameters = thaw_json(preview.intent.parameters)
    parameters["approved_preview_build_id"] = preview.build_id
    return BuildIntent(
        workflow_id=preview.intent.workflow_id,
        domain=preview.intent.domain,
        table=preview.intent.table,
        schema_version=preview.intent.schema_version,
        contract_id=preview.intent.contract_id,
        kind=BuildKind.FULL,
        attempt=1,
        retry_of_build_id=None,
        transform_version=preview.intent.transform_version,
        git_commit=preview.intent.git_commit,
        exchange_calendar_version=preview.intent.exchange_calendar_version,
        inputs=preview.preview.full_run_inputs,
        parameters=parameters,
    )


def _materialize_full(
    root: Path,
    *,
    intent: BuildIntent,
    inventory: SourceInventory,
    preview: BuildManifest,
    provenance: dict[str, object],
    authorization: ExchangePreviewAuthorization,
) -> BuildManifest:
    started_at = _now_utc()
    batch = read_exchange_source_inventory(root, inventory)
    _validate_authorized_source(
        batch,
        inventory,
        expected_manifest_sha256=authorization.manifest_sha256,
        expected_input_rows=authorization.expected_rows,
        authorization=authorization,
    )
    result = transform_exchange_batch(batch, build_id=intent.build_id, calendar_name="XNYS")
    _require_preview_parity(root, preview, result)
    prefix = SilverStore.build_output_prefix(intent)
    fixed_path = f"{prefix}/samples/current-reference-snapshot.json"
    fixed_rows = _current_reference_snapshot_evidence(
        build_id=intent.build_id,
        calendar_name="XNYS",
    )
    qa_checks = tuple(
        replace(check, bounded_examples_path=fixed_path)
        if check.check_id in _FIXED_CASE_QA_CHECKS
        else check
        for check in result.qa_checks
    )
    outputs = _write_preview_outputs(
        root,
        intent=intent,
        batch=batch,
        result=result,
        qa_checks=qa_checks,
        fixed_case_path=fixed_path,
        fixed_case_rows=fixed_rows,
        sample_limit=100,
    )
    provenance_row = dict(provenance)
    provenance_row["full_build_id"] = intent.build_id
    outputs = (
        *outputs,
        _write_json_sample(
            root,
            relative_path=f"{prefix}/samples/runtime-provenance.json",
            rows=[provenance_row],
        ),
    )
    full_data = next(item for item in outputs if item.role is ArtifactRole.DATA)
    preview_data = next(item for item in preview.outputs if item.role is ArtifactRole.DATA)
    if full_data.sha256 != preview_data.sha256:
        raise SilverStoreError("full exchange data Parquet differs from the reviewed preview")
    return BuildManifest(
        intent=intent,
        outputs=outputs,
        row_funnel=result.row_funnel,
        qa_checks=qa_checks,
        quarantine_issue_rows=len(result.quarantine_records),
        quarantine_unique_source_rows=len(
            {item.source_record_id for item in result.quarantine_records}
        ),
        quarantine_issue_ids_by_severity={
            severity.value: tuple(
                item.issue_id for item in result.quarantine_records if item.severity is severity
            )
            for severity in QASeverity
        },
        started_at=started_at,
        completed_at=_now_utc(),
        preview=None,
    )


def _require_publishable_preview(preview: BuildManifest, expected_rows: int) -> None:
    if preview.intent.kind is not BuildKind.PREVIEW or preview.preview is None:
        raise SilverStoreError("reviewed exchanges build is not a preview")
    if preview.row_funnel.input_rows != expected_rows:
        raise SilverStoreError("reviewed exchanges preview has the wrong input row count")
    if preview.row_funnel.output_rows_by_table != {EXCHANGE_DIM_CONTRACT.table: expected_rows}:
        raise SilverStoreError("reviewed exchanges preview has the wrong output row count")
    if len(preview.qa_checks) != len(EXCHANGE_DIM_CONTRACT.qa_rules) or any(
        check.status is not QAStatus.PASSED or check.numerator != 0 for check in preview.qa_checks
    ):
        raise SilverStoreError("reviewed exchanges preview QA is not fully clean")
    if preview.quarantine_issue_rows or preview.quarantine_unique_source_rows:
        raise SilverStoreError("reviewed exchanges preview has quarantine rows")


def _require_preview_parity(
    root: Path,
    preview: BuildManifest,
    result: ExchangeTransformResult,
) -> None:
    preview_data = next(item for item in preview.outputs if item.role is ArtifactRole.DATA)
    preview_table = pq.ParquetFile(root / preview_data.path).read()
    if preview_table.schema != EXCHANGE_DIM_CONTRACT.arrow_schema:
        raise SilverStoreError("reviewed exchanges preview schema differs from contract")
    if not preview_table.equals(result.table):
        raise SilverStoreError("recomputed exchange rows differ from the reviewed preview")
    if result.row_funnel != preview.row_funnel:
        raise SilverStoreError("recomputed exchange row funnel differs from preview")
    preview_checks = {item.check_id: _qa_core(item) for item in preview.qa_checks}
    result_checks = {item.check_id: _qa_core(item) for item in result.qa_checks}
    if preview_checks != result_checks:
        raise SilverStoreError("recomputed exchange QA differs from the reviewed preview")
    if result.quarantine_records:
        raise SilverStoreError("recomputed exchange full build produced quarantine")


def _require_full_matches_review(
    root: Path,
    full: BuildManifest,
    preview: BuildManifest,
    provenance: dict[str, object],
) -> None:
    if full.intent != _full_intent(preview):
        raise SilverStoreError("exchange full intent differs from the reviewed preview")
    if full.row_funnel != preview.row_funnel:
        raise SilverStoreError("exchange full row funnel differs from the preview")
    if full.quarantine_issue_rows or full.quarantine_unique_source_rows:
        raise SilverStoreError("exchange full build has quarantine rows")
    if {item.check_id: _qa_core(item) for item in full.qa_checks} != {
        item.check_id: _qa_core(item) for item in preview.qa_checks
    }:
        raise SilverStoreError("exchange full QA differs from the reviewed preview")
    full_data = [item for item in full.outputs if item.role is ArtifactRole.DATA]
    preview_data = [item for item in preview.outputs if item.role is ArtifactRole.DATA]
    if len(full_data) != 1 or len(preview_data) != 1:
        raise SilverStoreError("exchange build must contain exactly one data artifact")
    if full_data[0].sha256 != preview_data[0].sha256:
        raise SilverStoreError("exchange full data differs from the reviewed preview")
    provenance_outputs = [
        item for item in full.outputs if item.path.endswith("runtime-provenance.json")
    ]
    if len(provenance_outputs) != 1:
        raise SilverStoreError("exchange full runtime provenance is missing")
    try:
        rows = json.loads((root / provenance_outputs[0].path).read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SilverStoreError("exchange full runtime provenance is unreadable") from exc
    expected = {**provenance, "full_build_id": full.build_id}
    if rows != [expected]:
        raise SilverStoreError("exchange full runtime provenance differs from the runner")


def _qa_core(check: QACheckResult) -> tuple[object, ...]:
    return (
        check.table,
        check.partition_key,
        check.check_id,
        check.severity,
        check.status,
        check.numerator,
        check.denominator,
        check.rate,
        check.threshold,
    )


def _runner_provenance(
    repo_root: Path,
    *,
    runner_git_commit: str,
    preview_git_commit: str,
    preview: BuildManifest,
) -> dict[str, object]:
    root = repo_root.expanduser().resolve()
    module_relative = _module_relative_to(root)
    top_level = Path(_git_output(root, "rev-parse", "--show-toplevel")).resolve()
    head = _git_output(root, "rev-parse", "HEAD")
    status = _git_output(root, "status", "--porcelain=v1", "--untracked-files=all")
    tracked = _git_output(root, "ls-files", "--error-unmatch", "--", module_relative)
    if top_level != root or head != runner_git_commit or status or tracked != module_relative:
        raise SilverStoreError("exchanges release runner checkout provenance is invalid")
    ancestor = subprocess.run(
        ("git", "-C", str(root), "merge-base", "--is-ancestor", preview_git_commit, head),
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if ancestor.returncode != 0:
        raise SilverStoreError("release runner commit is not a descendant of preview code")
    closure = subprocess.run(
        (
            "git",
            "-C",
            str(root),
            "diff",
            "--quiet",
            preview_git_commit,
            head,
            "--",
            *_REVIEWED_LOGIC_CLOSURE,
        ),
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if closure.returncode != 0:
        raise SilverStoreError("reviewed exchange logic closure changed after preview")
    _require_dependency_contract_unchanged(root, preview_git_commit)
    parameters = thaw_json(preview.intent.parameters)
    if parameters.get("pyarrow_version") != pa.__version__:
        raise SilverStoreError("runtime PyArrow differs from the reviewed preview")
    if preview.intent.exchange_calendar_version != (
        f"exchange-calendars=={version('exchange-calendars')}"
    ):
        raise SilverStoreError("runtime exchange calendar differs from preview")
    blobs = {
        path: _git_output(root, "rev-parse", f"{preview_git_commit}:{path}")
        for path in _REVIEWED_LOGIC_CLOSURE
    }
    return {
        "logic_closure_blobs": blobs,
        "logic_closure_paths": list(_REVIEWED_LOGIC_CLOSURE),
        "preview_transform_git_commit": preview_git_commit,
        "runner_git_commit": runner_git_commit,
        "runtime": {
            "exchange_calendars": version("exchange-calendars"),
            "pandas": version("pandas"),
            "pyarrow": pa.__version__,
            "python": sys.version.split()[0],
        },
    }


def _reverify_runner(
    repo_root: Path,
    runner_git_commit: str,
    preview: BuildManifest,
    expected: dict[str, object],
) -> None:
    observed = _runner_provenance(
        repo_root,
        runner_git_commit=runner_git_commit,
        preview_git_commit=preview.intent.git_commit,
        preview=preview,
    )
    if observed != expected:
        raise SilverStoreError("release runner provenance changed during full build")


def _require_dependency_contract_unchanged(root: Path, preview_commit: str) -> None:
    current = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))
    previous = subprocess.run(
        ("git", "-C", str(root), "show", f"{preview_commit}:pyproject.toml"),
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if previous.returncode != 0:
        raise SilverStoreError("cannot read the reviewed dependency contract")
    reviewed = tomllib.loads(previous.stdout)
    current_project = current.get("project", {})
    reviewed_project = reviewed.get("project", {})
    for key in ("requires-python", "dependencies", "optional-dependencies"):
        if current_project.get(key) != reviewed_project.get(key):
            raise SilverStoreError("dependency contract changed after preview")


def _module_relative_to(root: Path) -> str:
    try:
        return Path(__file__).resolve().relative_to(root).as_posix()
    except ValueError as exc:
        raise SilverStoreError("release runner is outside the verified checkout") from exc


def _load_event_full(
    store: SilverStore,
    snapshot: WorkflowSnapshot,
) -> tuple[BuildManifest, StoredDocument]:
    for record in reversed(store.workflow_events(snapshot.workflow_id)):
        if record.event.to_state is WorkflowState.FULL_READY:
            build_id = str(record.event.evidence["build_id"])
            build, document = store.load_build(EXCHANGE_DIM_CONTRACT.table, build_id)
            if build.intent.kind is not BuildKind.FULL:
                raise SilverStoreError("full-ready event references the wrong build")
            store.verify_build(build, EXCHANGE_DIM_CONTRACT)
            return build, document
    raise SilverStoreError("exchanges workflow has no full build")


def _load_event_release(store: SilverStore, snapshot: WorkflowSnapshot) -> ReleaseManifest:
    if snapshot.state is not WorkflowState.PUBLISHED:
        raise SilverStoreError("exchanges workflow is not published")
    release_id = snapshot.evidence.get("release_id")
    if not isinstance(release_id, str):
        raise SilverStoreError("published exchanges event has no release ID")
    return store.load_release(release_id)[0]


__all__ = ["ExchangeReleaseRun", "complete_exchange_release"]
