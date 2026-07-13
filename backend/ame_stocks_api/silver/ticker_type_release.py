"""Complete the exact reviewed S2 ticker_type_dim workflow and publish it once."""

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
from ame_stocks_api.silver.reader import PublishedRelease, PublishedSilverReader
from ame_stocks_api.silver.store import (
    SilverStore,
    SilverStoreError,
    StoredDocument,
    WorkflowSnapshot,
    WorkflowState,
)
from ame_stocks_api.silver.ticker_type_contract import TICKER_TYPE_DIM_CONTRACT
from ame_stocks_api.silver.ticker_type_preview import (
    _FIXED_CASE_QA_CHECKS,
    CURRENT_TICKER_TYPES_PREVIEW_AUTHORIZATION,
    TickerTypePreviewAuthorization,
    _current_reference_snapshot_evidence,
    _git_output,
    _load_build_inventory,
    _load_event_preview,
    _now_utc,
    _validate_authorized_source,
    _write_json_sample,
    _write_preview_outputs,
)
from ame_stocks_api.silver.ticker_type_source import read_ticker_type_source_inventory
from ame_stocks_api.silver.ticker_types import (
    TickerTypeTransformResult,
    transform_ticker_type_batch,
)

_AUTHORIZED_WORKFLOW_ID = "40cde0fb24a52dbce894b52700f25c21074ad8d97ae5011a0a83cc773cee4b97"
_AUTHORIZED_AWAITING_REVIEW_EVENT_SHA256 = (
    "b40d81b23cebfe729186638f3f1e209305e067d44581349d16d5ba9df58b2ecb"
)
_AUTHORIZED_PREVIEW_BUILD_ID = "38998bc76c2ed04f3d9064e3a019cc953e6f1ed5d6594d9485a4978862f0b90d"
_AUTHORIZED_PREVIEW_MANIFEST_SHA256 = (
    "d7ce6dde58914bebe2afcb064599925c75e8c2333821100608ce01d9ee387f66"
)
_AUTHORIZED_CONTRACT_ID = "b2297d0631ae7560e7c3a9f73a288c62154db36b3188275e62f69c642884e38d"
S2_COMPLETION_AUTHORIZATION = "继续吧，把S2推进结束"  # noqa: RUF001

_REVIEWED_LOGIC_CLOSURE = (
    "backend/ame_stocks_api/artifacts.py",
    "backend/ame_stocks_api/silver/contracts.py",
    "backend/ame_stocks_api/silver/reader.py",
    "backend/ame_stocks_api/silver/schema_resources/ticker_type_dim.schema-v1.json",
    "backend/ame_stocks_api/silver/store.py",
    "backend/ame_stocks_api/silver/ticker_type_contract.py",
    "backend/ame_stocks_api/silver/ticker_type_preview.py",
    "backend/ame_stocks_api/silver/ticker_type_source.py",
    "backend/ame_stocks_api/silver/ticker_types.py",
)
_ALLOWED_STATES = {
    WorkflowState.AWAITING_REVIEW,
    WorkflowState.APPROVED_FULL_RUN,
    WorkflowState.FULL_READY,
    WorkflowState.AWAITING_PUBLISH,
    WorkflowState.PUBLISHED,
}


@dataclass(frozen=True, slots=True)
class TickerTypeReleaseRun:
    """The published S2 release and its fully verified evidence chain."""

    workflow: WorkflowSnapshot
    preview: BuildManifest
    preview_document: StoredDocument
    full: BuildManifest
    full_document: StoredDocument
    release: ReleaseManifest
    release_document: StoredDocument
    published: PublishedRelease


def complete_ticker_type_release(
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
) -> TickerTypeReleaseRun:
    """Advance only the exact reviewed 24-row S2 preview to publication."""

    return _complete_ticker_type_release_authorized(
        data_root,
        workflow_id=workflow_id,
        expected_event_sha256=expected_event_sha256,
        reviewed_preview_build_id=reviewed_preview_build_id,
        reviewed_preview_manifest_sha256=reviewed_preview_manifest_sha256,
        repo_root=repo_root,
        runner_git_commit=runner_git_commit,
        actor=actor,
        approver=approver,
        authorization=CURRENT_TICKER_TYPES_PREVIEW_AUTHORIZATION,
    )


def _complete_ticker_type_release_authorized(
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
    authorization: TickerTypePreviewAuthorization,
) -> TickerTypeReleaseRun:
    """Fixture-capable implementation; production identities remain hard pinned."""

    _require_authorized_identities(
        workflow_id=workflow_id,
        reviewed_preview_build_id=reviewed_preview_build_id,
        reviewed_preview_manifest_sha256=reviewed_preview_manifest_sha256,
    )
    root = data_root.expanduser().resolve()
    store = SilverStore(root)
    current = store.verify_workflow_trust_chain(workflow_id, verify_artifacts=True)
    _verify_authorized_release_ancestry(store, current)
    if current.event_sha256 != expected_event_sha256:
        raise SilverStoreError("stale ticker_type release expected_event_sha256")
    if current.state not in _ALLOWED_STATES:
        raise SilverStoreError(f"ticker_type release cannot continue from {current.state.value}")
    contract, _ = store.load_workflow_contract(workflow_id)
    if contract != TICKER_TYPE_DIM_CONTRACT:
        raise SilverStoreError("ticker_type release workflow uses the wrong contract")

    preview, preview_document = _load_event_preview(store, current)
    if (
        preview.build_id != reviewed_preview_build_id
        or preview_document.sha256 != reviewed_preview_manifest_sha256
    ):
        raise SilverStoreError("reviewed ticker_type preview identity does not match")
    _require_publishable_preview(preview, authorization.expected_rows)
    provenance = _runner_provenance(
        repo_root,
        runner_git_commit=runner_git_commit,
        preview_git_commit=preview.intent.git_commit,
        preview=preview,
    )
    inventory, _ = _load_build_inventory(root, preview)
    batch = read_ticker_type_source_inventory(root, inventory)
    _validate_authorized_source(
        batch,
        inventory,
        expected_manifest_sha256=authorization.manifest_sha256,
        expected_input_rows=authorization.expected_rows,
        authorization=authorization,
    )
    intent = _full_intent(preview)
    transformed = transform_ticker_type_batch(
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
                f"User authorization: {S2_COMPLETION_AUTHORIZATION}. "
                "Approved S2 completion with no QA waivers or quarantine acceptances; "
                f"orchestration commit={runner_git_commit}."
            ),
            waived_qa_result_ids=(),
            accepted_quarantine_issue_ids=(),
        )
    if current.state is WorkflowState.APPROVED_FULL_RUN:
        full = _load_orphan_full_if_present(store, intent)
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
            note="Registered the exact review-bound S2 ticker_type full build.",
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
            note="Submitted the verified S2 ticker_type full build for publication.",
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
                f"User authorization: {S2_COMPLETION_AUTHORIZATION}. "
                "Authorized S2 publication with no QA waivers or quarantine acceptances; "
                f"orchestration commit={runner_git_commit}."
            ),
            waived_qa_result_ids=(),
            accepted_quarantine_issue_ids=(),
        )
    else:
        release = _load_event_release(store, current)

    verified = store.verify_workflow_trust_chain(workflow_id, verify_artifacts=True)
    if verified.state is not WorkflowState.PUBLISHED:
        raise SilverStoreError("ticker_type workflow did not reach published")
    release_document = store.load_release(release.release_id)[1]
    published = PublishedSilverReader(root).inspect(release.release_id)
    full, full_document = store.load_build(TICKER_TYPE_DIM_CONTRACT.table, release.build_id)
    _require_full_matches_review(root, full, preview, provenance)
    if len(release.outputs) != 1 or release.outputs[0].role is not ArtifactRole.DATA:
        raise SilverStoreError("ticker_type release must expose exactly one data artifact")
    return TickerTypeReleaseRun(
        workflow=verified,
        preview=preview,
        preview_document=preview_document,
        full=full,
        full_document=full_document,
        release=release,
        release_document=release_document,
        published=published,
    )


def _require_authorized_identities(
    *,
    workflow_id: str,
    reviewed_preview_build_id: str,
    reviewed_preview_manifest_sha256: str,
) -> None:
    if workflow_id != _AUTHORIZED_WORKFLOW_ID:
        raise SilverStoreError("ticker_type release workflow ID is not authorized")
    if reviewed_preview_build_id != _AUTHORIZED_PREVIEW_BUILD_ID:
        raise SilverStoreError("ticker_type reviewed preview identity is not authorized (build)")
    if reviewed_preview_manifest_sha256 != _AUTHORIZED_PREVIEW_MANIFEST_SHA256:
        raise SilverStoreError("ticker_type reviewed preview identity is not authorized (manifest)")
    if TICKER_TYPE_DIM_CONTRACT.contract_id != _AUTHORIZED_CONTRACT_ID:
        raise SilverStoreError("ticker_type release contract identity is not authorized")


def _verify_authorized_release_ancestry(
    store: SilverStore,
    current: WorkflowSnapshot,
) -> None:
    if current.workflow_id != _AUTHORIZED_WORKFLOW_ID:
        raise SilverStoreError("ticker_type release workflow identity changed")
    records = store.workflow_events(current.workflow_id)
    if len(records) < 5:
        raise SilverStoreError("ticker_type release has no approved awaiting-review ancestor")
    authorized = records[4]
    if (
        authorized.event.sequence != 5
        or authorized.event.to_state is not WorkflowState.AWAITING_REVIEW
        or authorized.event_sha256 != _AUTHORIZED_AWAITING_REVIEW_EVENT_SHA256
    ):
        raise SilverStoreError("ticker_type release does not descend from the reviewed preview")
    if current.state is WorkflowState.AWAITING_REVIEW and (
        current.sequence != 5 or current.event_sha256 != _AUTHORIZED_AWAITING_REVIEW_EVENT_SHA256
    ):
        raise SilverStoreError("current ticker_type awaiting-review event is not authorized")


def _full_intent(preview: BuildManifest) -> BuildIntent:
    if preview.preview is None:
        raise SilverStoreError("reviewed ticker_type preview metadata is missing")
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


def _load_orphan_full_if_present(
    store: SilverStore,
    intent: BuildIntent,
) -> BuildManifest | None:
    path = (
        store.root
        / "manifests"
        / "silver"
        / "builds"
        / intent.table
        / f"build_id={intent.build_id}"
        / "manifest.json"
    )
    if not path.exists():
        return None
    build, _ = store.load_build(intent.table, intent.build_id)
    if build.intent != intent:
        raise SilverStoreError("orphan ticker_type full intent differs from this release")
    store.verify_build(build, TICKER_TYPE_DIM_CONTRACT)
    return build


def _materialize_full(
    root: Path,
    *,
    intent: BuildIntent,
    inventory: SourceInventory,
    preview: BuildManifest,
    provenance: dict[str, object],
    authorization: TickerTypePreviewAuthorization,
) -> BuildManifest:
    started_at = _now_utc()
    batch = read_ticker_type_source_inventory(root, inventory)
    _validate_authorized_source(
        batch,
        inventory,
        expected_manifest_sha256=authorization.manifest_sha256,
        expected_input_rows=authorization.expected_rows,
        authorization=authorization,
    )
    result = transform_ticker_type_batch(batch, build_id=intent.build_id, calendar_name="XNYS")
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
        sample_limit=authorization.sample_limit,
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
        raise SilverStoreError("full ticker_type data differs from the reviewed preview")
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
        raise SilverStoreError("reviewed ticker_type build is not a preview")
    if preview.row_funnel.input_rows != expected_rows:
        raise SilverStoreError("reviewed ticker_type preview has the wrong input row count")
    if preview.row_funnel.output_rows_by_table != {TICKER_TYPE_DIM_CONTRACT.table: expected_rows}:
        raise SilverStoreError("reviewed ticker_type preview has the wrong output row count")
    if len(preview.qa_checks) != len(TICKER_TYPE_DIM_CONTRACT.qa_rules) or any(
        check.status is not QAStatus.PASSED or check.numerator != 0 for check in preview.qa_checks
    ):
        raise SilverStoreError("reviewed ticker_type preview QA is not fully clean")
    if preview.quarantine_issue_rows or preview.quarantine_unique_source_rows:
        raise SilverStoreError("reviewed ticker_type preview has quarantine rows")


def _require_preview_parity(
    root: Path,
    preview: BuildManifest,
    result: TickerTypeTransformResult,
) -> None:
    preview_data = [item for item in preview.outputs if item.role is ArtifactRole.DATA]
    if len(preview_data) != 1:
        raise SilverStoreError("reviewed ticker_type preview must have one data artifact")
    preview_table = pq.ParquetFile(root / preview_data[0].path).read()
    if preview_table.schema != TICKER_TYPE_DIM_CONTRACT.arrow_schema:
        raise SilverStoreError("reviewed ticker_type preview schema differs from contract")
    if not preview_table.equals(result.table):
        raise SilverStoreError("recomputed ticker_type rows differ from the reviewed preview")
    if result.row_funnel != preview.row_funnel:
        raise SilverStoreError("recomputed ticker_type row funnel differs from preview")
    if {item.check_id: _qa_core(item) for item in preview.qa_checks} != {
        item.check_id: _qa_core(item) for item in result.qa_checks
    }:
        raise SilverStoreError("recomputed ticker_type QA differs from the reviewed preview")
    if result.quarantine_records:
        raise SilverStoreError("recomputed ticker_type full build produced quarantine")


def _require_full_matches_review(
    root: Path,
    full: BuildManifest,
    preview: BuildManifest,
    provenance: dict[str, object],
) -> None:
    if full.intent != _full_intent(preview):
        raise SilverStoreError("ticker_type full intent differs from the reviewed preview")
    if full.row_funnel != preview.row_funnel:
        raise SilverStoreError("ticker_type full row funnel differs from the preview")
    if full.quarantine_issue_rows or full.quarantine_unique_source_rows:
        raise SilverStoreError("ticker_type full build has quarantine rows")
    if len(full.qa_checks) != len(TICKER_TYPE_DIM_CONTRACT.qa_rules) or any(
        check.status is not QAStatus.PASSED or check.numerator != 0 for check in full.qa_checks
    ):
        raise SilverStoreError("ticker_type full QA is not fully clean")
    if {item.check_id: _qa_core(item) for item in full.qa_checks} != {
        item.check_id: _qa_core(item) for item in preview.qa_checks
    }:
        raise SilverStoreError("ticker_type full QA differs from the reviewed preview")
    full_data = [item for item in full.outputs if item.role is ArtifactRole.DATA]
    preview_data = [item for item in preview.outputs if item.role is ArtifactRole.DATA]
    if len(full_data) != 1 or len(preview_data) != 1:
        raise SilverStoreError("ticker_type build must contain exactly one data artifact")
    if full_data[0].sha256 != preview_data[0].sha256:
        raise SilverStoreError("ticker_type full data differs from the reviewed preview")
    expected_roles = {
        ArtifactRole.DATA: 1,
        ArtifactRole.QA: 1,
        ArtifactRole.QUARANTINE: 1,
        ArtifactRole.SAMPLE: 4,
    }
    observed_roles = {
        role: sum(item.role is role for item in full.outputs) for role in expected_roles
    }
    if len(full.outputs) != 7 or observed_roles != expected_roles:
        raise SilverStoreError("ticker_type full artifact set is not the reviewed shape")
    provenance_outputs = [
        item for item in full.outputs if item.path.endswith("runtime-provenance.json")
    ]
    if len(provenance_outputs) != 1:
        raise SilverStoreError("ticker_type full runtime provenance is missing")
    try:
        rows = json.loads((root / provenance_outputs[0].path).read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SilverStoreError("ticker_type full runtime provenance is unreadable") from exc
    expected = {**provenance, "full_build_id": full.build_id}
    if rows != [expected]:
        raise SilverStoreError("ticker_type full runtime provenance differs from the runner")


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
        raise SilverStoreError("ticker_type release runner checkout provenance is invalid")
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
        raise SilverStoreError("reviewed ticker_type logic closure changed after preview")
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
        "completion_authorization": S2_COMPLETION_AUTHORIZATION,
        "logic_closure_blobs": blobs,
        "logic_closure_paths": list(_REVIEWED_LOGIC_CLOSURE),
        "preview_transform_git_commit": preview_git_commit,
        "runner_git_commit": runner_git_commit,
        "runner_module_blob": _git_output(root, "rev-parse", f"{head}:{module_relative}"),
        "runner_module_path": module_relative,
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
        raise SilverStoreError("ticker_type release runner provenance changed during full build")


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
            build, document = store.load_build(TICKER_TYPE_DIM_CONTRACT.table, build_id)
            if build.intent.kind is not BuildKind.FULL:
                raise SilverStoreError("full-ready event references the wrong build")
            store.verify_build(build, TICKER_TYPE_DIM_CONTRACT)
            return build, document
    raise SilverStoreError("ticker_type workflow has no full build")


def _load_event_release(store: SilverStore, snapshot: WorkflowSnapshot) -> ReleaseManifest:
    if snapshot.state is not WorkflowState.PUBLISHED:
        raise SilverStoreError("ticker_type workflow is not published")
    release_id = snapshot.evidence.get("release_id")
    if not isinstance(release_id, str):
        raise SilverStoreError("published ticker_type event has no release ID")
    return store.load_release(release_id)[0]


__all__ = [
    "S2_COMPLETION_AUTHORIZATION",
    "TickerTypeReleaseRun",
    "complete_ticker_type_release",
]
