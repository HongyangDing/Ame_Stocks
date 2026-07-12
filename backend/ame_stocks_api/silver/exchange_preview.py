"""Build and register the bounded S1 exchange_dim preview, never a full build."""

from __future__ import annotations

import hashlib
import json
import resource
import subprocess
import sys
import time
import tracemalloc
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from importlib.metadata import version
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from ame_stocks_api.artifacts import safe_relative_path, write_bytes_immutable
from ame_stocks_api.silver.contracts import (
    QA_RESULT_ARROW_SCHEMA,
    QUARANTINE_ARROW_SCHEMA,
    ArtifactRef,
    ArtifactRole,
    BuildIntent,
    BuildKind,
    BuildManifest,
    PreviewMetadata,
    QACheckResult,
    QASeverity,
    SourceInventory,
    SourceLayer,
    arrow_schema_digest,
    thaw_json,
)
from ame_stocks_api.silver.exchange_contract import EXCHANGE_DIM_CONTRACT
from ame_stocks_api.silver.exchange_source import (
    ExchangeSourceBatch,
    ExchangeSourcePage,
    ExchangeSourceSnapshot,
    build_exchange_source_inventory,
    read_exchange_source_inventory,
)
from ame_stocks_api.silver.exchanges import (
    EXCHANGE_AVAILABILITY_RULE,
    EXCHANGE_DIM_TRANSFORM_VERSION,
    EXCHANGE_SNAPSHOT_SCOPE,
    ExchangeTransformResult,
    transform_exchange_batch,
)
from ame_stocks_api.silver.store import (
    SilverStore,
    SilverStoreError,
    StoredDocument,
    WorkflowSnapshot,
    WorkflowState,
)

DEFAULT_SAMPLE_LIMIT = 100
CURRENT_EXCHANGES_MANIFEST_PATH = (
    "manifests/massive/exchanges/"
    "08b662df642512deb23442fcf12e397d5e30201f054cf9f355fde70168e6f9dc.json"
)
CURRENT_EXCHANGES_MANIFEST_SHA256 = (
    "bad8b1c15aac37870ad0d860df35aac70846b6e1d1b3339e4de8f19c82bfc8e0"
)
CURRENT_EXCHANGES_REQUEST_ID = "08b662df642512deb23442fcf12e397d5e30201f054cf9f355fde70168e6f9dc"
CURRENT_EXCHANGES_ARTIFACT_PATH = (
    f"bronze/massive/exchanges/request_id={CURRENT_EXCHANGES_REQUEST_ID}/page-00000.json.gz"
)
CURRENT_EXCHANGES_ARTIFACT_SHA256 = (
    "6130c1f31636b322c90fb56c09506bcd06a16690bdd32910471dc8bc1f406e57"
)
CURRENT_EXCHANGES_EXPECTED_ROWS = 27
EXCHANGE_PREVIEW_POLICY_VERSION = "exchange-preview-v1"
_FIXED_CASE_QA_CHECKS = (
    "availability_invalid_rows",
    "lineage_invalid_rows",
    "snapshot_scope_invalid_rows",
    "source_snapshot_cardinality_invalid",
)


@dataclass(frozen=True, slots=True)
class ExchangePreviewAuthorization:
    """Exact immutable input authorization; the CLI never permits overriding it."""

    manifest_path: str
    manifest_sha256: str
    request_id: str
    artifact_path: str
    artifact_sha256: str
    expected_rows: int

    def __post_init__(self) -> None:
        if not self.manifest_path.startswith("manifests/massive/exchanges/"):
            raise ValueError("preview authorization manifest path is outside exchanges")
        if not self.artifact_path.startswith("bronze/massive/exchanges/"):
            raise ValueError("preview authorization artifact path is outside exchanges")
        for label, value in (
            ("manifest_sha256", self.manifest_sha256),
            ("request_id", self.request_id),
            ("artifact_sha256", self.artifact_sha256),
        ):
            if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
                raise ValueError(f"preview authorization {label} is not a lowercase SHA-256")
        if type(self.expected_rows) is not int or self.expected_rows <= 0:
            raise ValueError("preview authorization expected_rows must be positive")


CURRENT_EXCHANGES_PREVIEW_AUTHORIZATION = ExchangePreviewAuthorization(
    manifest_path=CURRENT_EXCHANGES_MANIFEST_PATH,
    manifest_sha256=CURRENT_EXCHANGES_MANIFEST_SHA256,
    request_id=CURRENT_EXCHANGES_REQUEST_ID,
    artifact_path=CURRENT_EXCHANGES_ARTIFACT_PATH,
    artifact_sha256=CURRENT_EXCHANGES_ARTIFACT_SHA256,
    expected_rows=CURRENT_EXCHANGES_EXPECTED_ROWS,
)


@dataclass(frozen=True, slots=True)
class ExchangePreviewRun:
    """Registered preview evidence returned at the awaiting-review hard stop."""

    workflow: WorkflowSnapshot
    build: BuildManifest
    build_document: StoredDocument
    inventory: SourceInventory
    inventory_document: StoredDocument


def run_exchange_preview(
    data_root: Path,
    *,
    workflow_id: str,
    expected_event_sha256: str,
    manifest_paths: tuple[str, ...],
    expected_manifest_sha256: str,
    expected_input_rows: int,
    git_commit: str,
    repo_root: Path,
    actor: str = "s1-exchanges-preview-runner",
    calendar_name: str = "XNYS",
    sample_limit: int = DEFAULT_SAMPLE_LIMIT,
) -> ExchangePreviewRun:
    """Run the one production-authorized preview and stop after submitting it for review."""

    return _run_exchange_preview_authorized(
        data_root,
        workflow_id=workflow_id,
        expected_event_sha256=expected_event_sha256,
        manifest_paths=manifest_paths,
        expected_manifest_sha256=expected_manifest_sha256,
        expected_input_rows=expected_input_rows,
        git_commit=git_commit,
        repo_root=repo_root,
        actor=actor,
        calendar_name=calendar_name,
        sample_limit=sample_limit,
        authorization=CURRENT_EXCHANGES_PREVIEW_AUTHORIZATION,
    )


def _run_exchange_preview_authorized(
    data_root: Path,
    *,
    workflow_id: str,
    expected_event_sha256: str,
    manifest_paths: tuple[str, ...],
    expected_manifest_sha256: str,
    expected_input_rows: int,
    git_commit: str,
    repo_root: Path,
    actor: str,
    calendar_name: str,
    sample_limit: int,
    authorization: ExchangePreviewAuthorization,
) -> ExchangePreviewRun:
    """Internal fixture-capable implementation; operational CLI uses the frozen authorization."""

    if type(sample_limit) is not int or not 1 <= sample_limit <= 100:
        raise SilverStoreError("exchange preview sample_limit must be between 1 and 100")
    if calendar_name != "XNYS":
        raise SilverStoreError("exchange preview calendar is pinned to XNYS")
    normalized_manifests = tuple(manifest_paths)
    if normalized_manifests != (authorization.manifest_path,):
        raise SilverStoreError("exchange preview is authorized for one exact manifest only")
    if expected_manifest_sha256 != authorization.manifest_sha256:
        raise SilverStoreError("exchange preview expected manifest SHA is not authorized")
    if expected_input_rows != authorization.expected_rows:
        raise SilverStoreError("exchange preview expected row count is not authorized")
    _verify_git_checkout(repo_root, git_commit)
    calendar_version = f"exchange-calendars=={version('exchange-calendars')}"
    parameters = _intent_parameters(
        calendar_name=calendar_name,
        manifest_paths=normalized_manifests,
        expected_manifest_sha256=expected_manifest_sha256,
        expected_input_rows=expected_input_rows,
        sample_limit=sample_limit,
        authorization=authorization,
    )
    root = data_root.expanduser().resolve()
    store = SilverStore(root)
    current = store.status(workflow_id)
    if current.event_sha256 != expected_event_sha256:
        raise SilverStoreError("stale exchange preview expected_event_sha256")
    contract, _ = store.load_workflow_contract(workflow_id)
    if contract != EXCHANGE_DIM_CONTRACT:
        raise SilverStoreError("exchange preview workflow does not use the approved contract")
    if current.state not in {
        WorkflowState.CODE_READY,
        WorkflowState.PREVIEW_READY,
        WorkflowState.AWAITING_REVIEW,
    }:
        raise SilverStoreError(
            f"exchange preview cannot run from workflow state {current.state.value}"
        )

    if current.state in {WorkflowState.PREVIEW_READY, WorkflowState.AWAITING_REVIEW}:
        build, build_document = _load_event_preview(store, current)
        inventory, inventory_document = _load_build_inventory(root, build)
        _require_matching_existing_preview(
            build,
            inventory,
            git_commit=git_commit,
            calendar_version=calendar_version,
            parameters=parameters,
            expected_manifest_sha256=expected_manifest_sha256,
            expected_input_rows=expected_input_rows,
            authorization=authorization,
        )
        if current.state is WorkflowState.PREVIEW_READY:
            current = store.request_preview_review(
                workflow_id,
                expected_event_sha256=current.event_sha256,
                actor=actor,
                created_at=_now_utc(),
                note="S1 exchanges preview submitted for user review; full run remains gated.",
            )
        return ExchangePreviewRun(
            workflow=current,
            build=build,
            build_document=build_document,
            inventory=inventory,
            inventory_document=inventory_document,
        )

    inventory = build_exchange_source_inventory(
        root,
        manifest_paths=normalized_manifests,
        git_commit=git_commit,
    )
    batch = read_exchange_source_inventory(root, inventory)
    _validate_authorized_source(
        batch,
        inventory,
        expected_manifest_sha256=expected_manifest_sha256,
        expected_input_rows=expected_input_rows,
        authorization=authorization,
    )
    inventory_document = store.register_source_inventory(inventory)
    inputs = tuple(
        ArtifactRef(
            path=item.path,
            sha256=item.sha256,
            bytes=item.bytes,
            row_count=item.row_count,
            media_type=item.media_type,
            role=ArtifactRole.SOURCE,
            source_dataset=inventory.source_dataset,
            source_layer=SourceLayer.BRONZE,
            lineage_manifest_path=inventory_document.path,
            lineage_manifest_sha256=inventory_document.sha256,
        )
        for item in inventory.artifacts
    )
    intent = BuildIntent(
        workflow_id=workflow_id,
        domain=contract.domain,
        table=contract.table,
        schema_version=contract.schema_version,
        contract_id=contract.contract_id,
        kind=BuildKind.PREVIEW,
        attempt=1,
        retry_of_build_id=None,
        transform_version=EXCHANGE_DIM_TRANSFORM_VERSION,
        git_commit=git_commit,
        exchange_calendar_version=calendar_version,
        inputs=inputs,
        parameters=parameters,
    )

    orphan = _load_orphan_build_if_present(store, intent)
    if orphan is None:
        build = _materialize_preview(
            root,
            intent=intent,
            inventory=inventory,
            expected_manifest_sha256=expected_manifest_sha256,
            expected_input_rows=expected_input_rows,
            authorization=authorization,
            calendar_name=calendar_name,
            sample_limit=sample_limit,
        )
    else:
        build = orphan
    _verify_git_checkout(repo_root, git_commit)
    recorded_at = _now_utc()
    current = store.record_preview_build(
        build,
        expected_event_sha256=current.event_sha256,
        actor=actor,
        recorded_at=recorded_at,
        note="Registered the bounded current exchanges preview.",
    )
    current = store.request_preview_review(
        workflow_id,
        expected_event_sha256=current.event_sha256,
        actor=actor,
        created_at=_now_utc(),
        note="S1 exchanges preview submitted for user review; full run remains gated.",
    )
    build, build_document = _load_event_preview(store, current)
    return ExchangePreviewRun(
        workflow=current,
        build=build,
        build_document=build_document,
        inventory=inventory,
        inventory_document=inventory_document,
    )


def _materialize_preview(
    root: Path,
    *,
    intent: BuildIntent,
    inventory: SourceInventory,
    expected_manifest_sha256: str,
    expected_input_rows: int,
    authorization: ExchangePreviewAuthorization,
    calendar_name: str,
    sample_limit: int,
) -> BuildManifest:
    started_at = _now_utc()
    start_ns = time.perf_counter_ns()
    tracemalloc.start()
    try:
        batch = read_exchange_source_inventory(root, inventory)
        _validate_authorized_source(
            batch,
            inventory,
            expected_manifest_sha256=expected_manifest_sha256,
            expected_input_rows=expected_input_rows,
            authorization=authorization,
        )
        result = transform_exchange_batch(
            batch,
            build_id=intent.build_id,
            calendar_name=calendar_name,
        )
        fixed_case_path = (
            f"{SilverStore.build_output_prefix(intent)}/samples/current-reference-snapshot.json"
        )
        fixed_case_rows = _current_reference_snapshot_evidence(
            build_id=intent.build_id,
            calendar_name=calendar_name,
        )
        qa_checks = tuple(
            replace(check, bounded_examples_path=fixed_case_path)
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
            fixed_case_path=fixed_case_path,
            fixed_case_rows=fixed_case_rows,
            sample_limit=sample_limit,
        )
        _, peak_traced_bytes = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    elapsed_ms = max(0, (time.perf_counter_ns() - start_ns) // 1_000_000)
    process_max_rss_bytes = _process_max_rss_bytes()
    completed_at = _now_utc()
    input_sample = next(item for item in outputs if item.path.endswith("input-sample.json"))
    output_sample = next(item for item in outputs if item.path.endswith("output-sample.json"))
    qa_by_id = {check.check_id: check for check in qa_checks}
    fixed_case_result_ids = tuple(
        qa_by_id[check_id].result_id for check_id in _FIXED_CASE_QA_CHECKS
    )
    input_compressed_bytes = sum(item.bytes for item in inventory.artifacts)
    input_raw_bytes = _source_raw_bytes(root, inventory)
    data_output_bytes = sum(item.bytes for item in outputs if item.role is ArtifactRole.DATA)
    output_artifact_bytes = sum(item.bytes for item in outputs)
    max_serialized_artifact_bytes = max(item.bytes for item in outputs)
    preview = PreviewMetadata(
        fixed_case_ids=("current_reference_snapshot",),
        fixed_case_qa_result_ids={
            "current_reference_snapshot": fixed_case_result_ids,
        },
        input_sample_path=input_sample.path,
        input_sample_rows=int(input_sample.row_count or 0),
        output_sample_path=output_sample.path,
        output_sample_rows=int(output_sample.row_count or 0),
        examples_truncated=(batch.row_count > sample_limit or result.table.num_rows > sample_limit),
        full_run_inputs=intent.inputs,
        resource_usage={
            "bronze_raw_to_compressed_ratio": float(input_raw_bytes / input_compressed_bytes),
            "data_parquet_to_raw_ratio": float(data_output_bytes / input_raw_bytes),
            "data_parquet_bytes": data_output_bytes,
            "elapsed_ms": elapsed_ms,
            "input_compressed_bytes": input_compressed_bytes,
            "input_raw_bytes": input_raw_bytes,
            "input_rows": batch.row_count,
            "max_serialized_artifact_bytes": max_serialized_artifact_bytes,
            "output_artifact_bytes": output_artifact_bytes,
            "output_rows": result.table.num_rows,
            "process_max_rss_bytes": process_max_rss_bytes,
            "python_peak_traced_bytes": peak_traced_bytes,
            "source_pages": batch.page_count,
        },
        full_run_projection={
            "basis": "preview inventory is the complete approved current snapshot",
            "estimated_elapsed_ms": elapsed_ms,
            "estimated_final_artifact_bytes": output_artifact_bytes,
            "estimated_input_bytes": input_compressed_bytes,
            "estimated_input_rows": batch.row_count,
            "estimated_output_rows": result.table.num_rows,
            "estimated_peak_rss_bytes": process_max_rss_bytes,
            "estimated_temporary_serialization_bytes": max_serialized_artifact_bytes,
            "projection_multiplier": 1.0,
            "source_inventory_id": inventory.inventory_id,
        },
    )
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
        completed_at=completed_at,
        preview=preview,
    )


def _write_preview_outputs(
    root: Path,
    *,
    intent: BuildIntent,
    batch: ExchangeSourceBatch,
    result: ExchangeTransformResult,
    qa_checks: tuple[QACheckResult, ...],
    fixed_case_path: str,
    fixed_case_rows: list[dict[str, object]],
    sample_limit: int,
) -> tuple[ArtifactRef, ...]:
    prefix = SilverStore.build_output_prefix(intent)
    outputs: list[ArtifactRef] = []
    capture_dates = sorted(set(result.table.column("capture_date").to_pylist()))
    if not capture_dates:
        outputs.append(
            _write_parquet_artifact(
                root,
                relative_path=f"{prefix}/data/part-00000.parquet",
                table=result.table,
                role=ArtifactRole.DATA,
                table_name=EXCHANGE_DIM_CONTRACT.table,
            )
        )
    for capture_date in capture_dates:
        partition = result.table.filter(
            pc.equal(
                result.table.column("capture_date"),
                pa.scalar(capture_date, type=pa.date32()),
            )
        )
        outputs.append(
            _write_parquet_artifact(
                root,
                relative_path=(
                    f"{prefix}/data/capture_date={capture_date.isoformat()}/part-00000.parquet"
                ),
                table=partition,
                role=ArtifactRole.DATA,
                table_name=EXCHANGE_DIM_CONTRACT.table,
            )
        )

    qa_table = pa.Table.from_pylist(
        [item.to_output_dict(intent.build_id) for item in qa_checks],
        schema=QA_RESULT_ARROW_SCHEMA,
    )
    outputs.append(
        _write_parquet_artifact(
            root,
            relative_path=f"{prefix}/qa/qa-check-result.parquet",
            table=qa_table,
            role=ArtifactRole.QA,
            table_name="qa_check_result",
        )
    )
    quarantine_table = pa.Table.from_pylist(
        [item.to_dict() for item in result.quarantine_records],
        schema=QUARANTINE_ARROW_SCHEMA,
    )
    outputs.append(
        _write_parquet_artifact(
            root,
            relative_path=f"{prefix}/quarantine/quarantine-record.parquet",
            table=quarantine_table,
            role=ArtifactRole.QUARANTINE,
            table_name="quarantine_record",
        )
    )

    input_rows = _input_sample(batch, sample_limit)
    output_rows = [_json_safe(item) for item in result.table.slice(0, sample_limit).to_pylist()]
    outputs.extend(
        (
            _write_json_sample(
                root,
                relative_path=f"{prefix}/samples/input-sample.json",
                rows=input_rows,
            ),
            _write_json_sample(
                root,
                relative_path=f"{prefix}/samples/output-sample.json",
                rows=output_rows,
            ),
            _write_json_sample(
                root,
                relative_path=fixed_case_path,
                rows=fixed_case_rows,
            ),
        )
    )
    return tuple(outputs)


def _input_sample(batch: ExchangeSourceBatch, limit: int) -> list[dict[str, object]]:
    sample: list[dict[str, object]] = []
    for snapshot in batch.snapshots:
        for page in snapshot.pages:
            for ordinal, raw in enumerate(page.rows):
                sample.append(
                    {
                        "raw": _json_safe(dict(raw)),
                        "source_artifact_sha256": page.source_artifact_sha256,
                        "source_capture_at_utc": snapshot.source_capture_at_utc.isoformat(),
                        "source_page_sequence": page.sequence,
                        "source_provider_request_id": page.source_provider_request_id,
                        "source_request_id": snapshot.source_request_id,
                        "source_row_ordinal": ordinal,
                    }
                )
                if len(sample) == limit:
                    return sample
    return sample


def _write_parquet_artifact(
    root: Path,
    *,
    relative_path: str,
    table: pa.Table,
    role: ArtifactRole,
    table_name: str,
) -> ArtifactRef:
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="zstd", version="2.6", write_statistics=True)
    content = sink.getvalue().to_pybytes()
    stored = write_bytes_immutable(root, root / relative_path, content)
    return ArtifactRef(
        path=str(stored["path"]),
        sha256=str(stored["sha256"]),
        bytes=int(stored["bytes"]),
        row_count=table.num_rows,
        media_type="application/vnd.apache.parquet",
        role=role,
        table=table_name,
        schema_digest=arrow_schema_digest(table.schema),
    )


def _write_json_sample(
    root: Path,
    *,
    relative_path: str,
    rows: list[dict[str, object]],
) -> ArtifactRef:
    content = (
        json.dumps(rows, allow_nan=False, ensure_ascii=False, indent=2, sort_keys=True).encode()
        + b"\n"
    )
    stored = write_bytes_immutable(root, root / relative_path, content)
    return ArtifactRef(
        path=str(stored["path"]),
        sha256=str(stored["sha256"]),
        bytes=int(stored["bytes"]),
        row_count=len(rows),
        media_type="application/json",
        role=ArtifactRole.SAMPLE,
    )


def _json_safe(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


def _load_orphan_build_if_present(
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
        raise SilverStoreError("orphan exchange preview intent differs from this run")
    store.verify_build(build, EXCHANGE_DIM_CONTRACT)
    return build


def _load_event_preview(
    store: SilverStore,
    snapshot: WorkflowSnapshot,
) -> tuple[BuildManifest, StoredDocument]:
    for record in reversed(store.workflow_events(snapshot.workflow_id)):
        if record.event.to_state is WorkflowState.PREVIEW_READY:
            build_id = str(record.event.evidence["build_id"])
            build, document = store.load_build(EXCHANGE_DIM_CONTRACT.table, build_id)
            if (
                build.intent.kind is not BuildKind.PREVIEW
                or build.intent.workflow_id != snapshot.workflow_id
            ):
                raise SilverStoreError("exchange preview event points to the wrong build")
            store.verify_build(build, EXCHANGE_DIM_CONTRACT)
            return build, document
    raise SilverStoreError("exchange workflow has no registered preview build")


def _load_build_inventory(
    root: Path,
    build: BuildManifest,
) -> tuple[SourceInventory, StoredDocument]:
    first = build.intent.inputs[0]
    if first.lineage_manifest_path is None or first.lineage_manifest_sha256 is None:
        raise SilverStoreError("exchange preview source inventory lineage is missing")
    path = safe_relative_path(root, first.lineage_manifest_path)
    try:
        content = path.read_bytes()
    except OSError as exc:
        raise SilverStoreError("cannot read exchange preview source inventory") from exc
    if hashlib.sha256(content).hexdigest() != first.lineage_manifest_sha256:
        raise SilverStoreError("exchange preview source inventory checksum mismatch")
    try:
        document = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SilverStoreError("exchange preview source inventory is invalid JSON") from exc
    inventory = SourceInventory.from_dict(document)
    stored = StoredDocument(
        path=first.lineage_manifest_path,
        sha256=first.lineage_manifest_sha256,
        bytes=len(content),
    )
    if not all(
        item.lineage_manifest_path == first.lineage_manifest_path
        and item.lineage_manifest_sha256 == first.lineage_manifest_sha256
        for item in build.intent.inputs
    ):
        raise SilverStoreError("exchange preview inputs do not share one source inventory")
    return inventory, stored


def _verify_git_checkout(repo_root: Path, git_commit: str) -> None:
    root = repo_root.expanduser().resolve()
    try:
        module_relative = Path(__file__).resolve().relative_to(root).as_posix()
    except ValueError as exc:
        raise SilverStoreError(
            "exchange preview code is not executing from the verified Git checkout"
        ) from exc
    try:
        top_level = _git_output(root, "rev-parse", "--show-toplevel")
        head = _git_output(root, "rev-parse", "HEAD")
        tracked_module = _git_output(
            root,
            "ls-files",
            "--error-unmatch",
            "--",
            module_relative,
        )
        status = _git_output(
            root,
            "status",
            "--porcelain=v1",
            "--untracked-files=all",
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise SilverStoreError("cannot verify exchange preview Git checkout") from exc
    if Path(top_level).resolve() != root:
        raise SilverStoreError("exchange preview repo_root is not the Git top level")
    if head != git_commit:
        raise SilverStoreError("exchange preview Git HEAD differs from --git-commit")
    if tracked_module != module_relative:
        raise SilverStoreError("exchange preview module is not the verified tracked source")
    if status:
        raise SilverStoreError("exchange preview Git checkout is not clean")


def _git_output(root: Path, *arguments: str) -> str:
    completed = subprocess.run(
        ("git", "-C", str(root), *arguments),
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip() or "unknown Git error"
        raise SilverStoreError(f"cannot verify exchange preview Git checkout: {detail}")
    return completed.stdout.strip()


def _intent_parameters(
    *,
    calendar_name: str,
    manifest_paths: tuple[str, ...],
    expected_manifest_sha256: str,
    expected_input_rows: int,
    sample_limit: int,
    authorization: ExchangePreviewAuthorization,
) -> dict[str, object]:
    return {
        "availability_rule": EXCHANGE_AVAILABILITY_RULE,
        "authorized_artifact_path": authorization.artifact_path,
        "authorized_artifact_sha256": authorization.artifact_sha256,
        "authorized_request_id": authorization.request_id,
        "calendar_name": calendar_name,
        "expected_input_rows": expected_input_rows,
        "expected_manifest_sha256": expected_manifest_sha256,
        "manifest_paths": list(manifest_paths),
        "preview_policy_version": EXCHANGE_PREVIEW_POLICY_VERSION,
        "pyarrow_version": pa.__version__,
        "sample_limit": sample_limit,
        "sample_policy": "first_rows_in_verified_source_order_v1",
        "snapshot_scope": EXCHANGE_SNAPSHOT_SCOPE,
    }


def _validate_authorized_inventory(
    inventory: SourceInventory,
    *,
    expected_manifest_sha256: str,
    expected_input_rows: int,
    authorization: ExchangePreviewAuthorization,
) -> None:
    if inventory.source_dataset != "exchanges" or inventory.source_layer is not SourceLayer.BRONZE:
        raise SilverStoreError("exchange preview inventory is not Bronze exchanges")
    upstream = inventory.upstream_manifests
    if len(upstream) != 1:
        raise SilverStoreError("exchange preview requires exactly one source manifest")
    if (
        upstream[0].path != authorization.manifest_path
        or upstream[0].sha256 != expected_manifest_sha256
        or upstream[0].sha256 != authorization.manifest_sha256
    ):
        raise SilverStoreError("exchange preview source manifest is not the authorized object")
    artifacts = inventory.artifacts
    if len(artifacts) != 1:
        raise SilverStoreError("exchange preview requires exactly one source page")
    artifact = artifacts[0]
    if (
        artifact.path != authorization.artifact_path
        or artifact.sha256 != authorization.artifact_sha256
        or artifact.row_count != expected_input_rows
        or artifact.row_count != authorization.expected_rows
        or artifact.media_type != "application/gzip+json"
    ):
        raise SilverStoreError("exchange preview source page is not the authorized object")


def _validate_authorized_source(
    batch: ExchangeSourceBatch,
    inventory: SourceInventory,
    *,
    expected_manifest_sha256: str,
    expected_input_rows: int,
    authorization: ExchangePreviewAuthorization,
) -> None:
    _validate_authorized_inventory(
        inventory,
        expected_manifest_sha256=expected_manifest_sha256,
        expected_input_rows=expected_input_rows,
        authorization=authorization,
    )
    if len(batch.snapshots) != 1 or batch.page_count != 1:
        raise SilverStoreError("exchange preview input must be one snapshot and one page")
    snapshot = batch.snapshots[0]
    page = snapshot.pages[0]
    if snapshot.source_request_id != authorization.request_id:
        raise SilverStoreError("exchange preview request ID is not authorized")
    if (
        page.sequence != 0
        or page.source_path != authorization.artifact_path
        or page.source_artifact_sha256 != authorization.artifact_sha256
    ):
        raise SilverStoreError("exchange preview page identity is not authorized")
    if batch.row_count != expected_input_rows or batch.row_count != authorization.expected_rows:
        raise SilverStoreError("exchange preview input row count differs from authorization")


def _require_matching_existing_preview(
    build: BuildManifest,
    inventory: SourceInventory,
    *,
    git_commit: str,
    calendar_version: str,
    parameters: dict[str, object],
    expected_manifest_sha256: str,
    expected_input_rows: int,
    authorization: ExchangePreviewAuthorization,
) -> None:
    _validate_authorized_inventory(
        inventory,
        expected_manifest_sha256=expected_manifest_sha256,
        expected_input_rows=expected_input_rows,
        authorization=authorization,
    )
    intent = build.intent
    if (
        intent.kind is not BuildKind.PREVIEW
        or intent.attempt != 1
        or intent.retry_of_build_id is not None
        or intent.contract_id != EXCHANGE_DIM_CONTRACT.contract_id
        or intent.transform_version != EXCHANGE_DIM_TRANSFORM_VERSION
        or intent.git_commit != git_commit
        or inventory.git_commit != git_commit
        or intent.exchange_calendar_version != calendar_version
        or thaw_json(intent.parameters) != parameters
    ):
        raise SilverStoreError("existing exchange preview does not match this exact run intent")
    if len(intent.inputs) != len(inventory.artifacts):
        raise SilverStoreError("existing exchange preview inputs differ from its inventory")
    for source, item in zip(intent.inputs, inventory.artifacts, strict=True):
        if (
            source.path != item.path
            or source.sha256 != item.sha256
            or source.bytes != item.bytes
            or source.row_count != item.row_count
            or source.media_type != item.media_type
            or source.source_dataset != inventory.source_dataset
            or source.source_layer is not inventory.source_layer
            or source.lineage_manifest_path is None
            or source.lineage_manifest_sha256 is None
        ):
            raise SilverStoreError("existing exchange preview input lineage is inconsistent")
    if build.preview is None or build.preview.full_run_inputs != intent.inputs:
        raise SilverStoreError("existing exchange preview full-run inventory is inconsistent")


def _source_raw_bytes(root: Path, inventory: SourceInventory) -> int:
    total = 0
    for upstream in inventory.upstream_manifests:
        path = safe_relative_path(root, upstream.path)
        try:
            content = path.read_bytes()
        except OSError as exc:
            raise SilverStoreError("cannot reread exchange source manifest for sizing") from exc
        if hashlib.sha256(content).hexdigest() != upstream.sha256:
            raise SilverStoreError("exchange source manifest changed during sizing")
        try:
            document = json.loads(content)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise SilverStoreError("exchange source manifest is invalid during sizing") from exc
        artifacts = document.get("artifacts") if isinstance(document, dict) else None
        if not isinstance(artifacts, list) or not artifacts:
            raise SilverStoreError("exchange source manifest has no sizing artifacts")
        for artifact in artifacts:
            raw_bytes = artifact.get("raw_bytes") if isinstance(artifact, dict) else None
            if type(raw_bytes) is not int or raw_bytes <= 0:
                raise SilverStoreError("exchange source manifest raw byte count is invalid")
            total += raw_bytes
    if total <= 0:
        raise SilverStoreError("exchange preview source raw byte total is empty")
    return total


def _current_reference_snapshot_evidence(
    *,
    build_id: str,
    calendar_name: str,
) -> list[dict[str, object]]:
    first_capture = datetime(2026, 7, 11, 15, 37, 41, tzinfo=UTC)
    second_capture = datetime(2026, 7, 12, 16, 37, 41, tzinfo=UTC)
    raw = {
        "acronym": "TEST",
        "asset_class": "stocks",
        "id": 1,
        "locale": "us",
        "mic": "XASE",
        "name": "Synthetic Review Exchange",
        "operating_mic": "XNYS",
        "participant_id": "A",
        "type": "exchange",
        "url": "https://example.test/exchange",
    }
    snapshots = tuple(
        ExchangeSourceSnapshot(
            source_request_id=request_id,
            source_capture_at_utc=capture_at,
            pages=(
                ExchangeSourcePage(
                    source_path=f"fixtures/exchanges/{request_id}/page-00000.json.gz",
                    source_artifact_sha256=artifact_sha256,
                    sequence=0,
                    source_provider_request_id=f"synthetic-{request_id[:8]}",
                    rows=(raw,),
                ),
            ),
        )
        for request_id, artifact_sha256, capture_at in (
            ("a" * 64, "c" * 64, first_capture),
            ("b" * 64, "d" * 64, second_capture),
        )
    )
    result = transform_exchange_batch(
        ExchangeSourceBatch(snapshots),
        build_id=build_id,
        calendar_name=calendar_name,
    )
    rows = result.table.to_pylist()
    capture_dates = [str(row["capture_date"]) for row in rows]
    availability_pairs = [
        {
            "available_at_utc": row["available_at_utc"].isoformat(),
            "source_capture_at_utc": row["source_capture_at_utc"].isoformat(),
        }
        for row in rows
    ]
    fixed_checks = {
        check_id: result.qa_by_id(check_id).status.value for check_id in _FIXED_CASE_QA_CHECKS
    }
    assertions: list[dict[str, object]] = [
        {
            "assertion_id": "capture_date_comes_from_capture_instant",
            "passed": capture_dates == ["2026-07-11", "2026-07-12"],
            "evidence": {
                "capture_dates": capture_dates,
                "synthetic_request_label": "1999-01-01",
            },
        },
        {
            "assertion_id": "availability_strictly_after_capture",
            "passed": all(row["available_at_utc"] > row["source_capture_at_utc"] for row in rows),
            "evidence": {"pairs": availability_pairs},
        },
        {
            "assertion_id": "later_capture_appends_without_backfill",
            "passed": result.table.num_rows == 2 and len(set(capture_dates)) == 2,
            "evidence": {
                "output_rows": result.table.num_rows,
                "partition_capture_dates": sorted(set(capture_dates)),
            },
        },
        {
            "assertion_id": "lineage_and_snapshot_qa_recomputable",
            "passed": all(status == "passed" for status in fixed_checks.values())
            and {row["source_request_id"] for row in rows} == {"a" * 64, "b" * 64}
            and all(row["source_row_ordinal"] == 0 for row in rows),
            "evidence": {
                "qa_statuses": fixed_checks,
                "source_record_ids": [row["source_record_id"] for row in rows],
                "source_request_ids": [row["source_request_id"] for row in rows],
                "source_row_hashes": [row["source_row_hash"] for row in rows],
            },
        },
    ]
    if not all(bool(item["passed"]) for item in assertions):
        raise SilverStoreError("current-reference-snapshot fixed-case evidence failed")
    return assertions


def _process_max_rss_bytes() -> int:
    maximum = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    return maximum if sys.platform == "darwin" else maximum * 1024


def _now_utc() -> str:
    return datetime.now(UTC).isoformat()


__all__ = ["ExchangePreviewRun", "run_exchange_preview"]
