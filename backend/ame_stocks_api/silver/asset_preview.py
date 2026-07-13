"""Build the exact one-session S4 assets preview and stop at awaiting_review.

This module deliberately has no full-build or publication path.  The production entry point
accepts explicit expected workflow events and source digests, then checks them against a frozen
2026-05-11 authorization.  All three table previews share one source inventory and one pure
transform invocation.
"""

from __future__ import annotations

import hashlib
import json
import resource
import subprocess
import sys
import time
import tracemalloc
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from importlib.metadata import version
from pathlib import Path
from types import MappingProxyType
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from ame_stocks_api.artifacts import safe_relative_path, stable_digest, write_bytes_immutable
from ame_stocks_api.silver.asset_contract import (
    ASSET_OBSERVATION_DAILY_CONTRACT,
    ASSET_OBSERVATION_VERSION_CONTRACT,
    UNIVERSE_SOURCE_DAILY_CONTRACT,
)
from ame_stocks_api.silver.asset_source import (
    AssetSourceReader,
    AssetSourceRecord,
    build_asset_source_inventory,
    read_asset_source_inventory,
)
from ame_stocks_api.silver.assets import (
    ASSET_METADATA_TIME_SCOPE,
    ASSET_REFERENCE_TIME_SCOPE,
    ASSET_SOURCE_AVAILABILITY_QUALITY,
    ASSET_SOURCE_AVAILABILITY_RULE,
    ASSET_TRANSFORM_VERSION,
    ASSET_VERSION_SELECTION_RULE,
    UNIVERSE_SOURCE_AVAILABILITY_RULE,
    AssetTableTransformResult,
    transform_asset_session,
)
from ame_stocks_api.silver.contracts import (
    QA_RESULT_ARROW_SCHEMA,
    QUARANTINE_ARROW_SCHEMA,
    SEPARATE_FULL_RUN_PLAN_POLICY,
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
    TableContract,
    UpstreamManifestRef,
    arrow_schema_digest,
)
from ame_stocks_api.silver.exchange_contract import EXCHANGE_DIM_CONTRACT
from ame_stocks_api.silver.reader import PublishedSilverReader
from ame_stocks_api.silver.store import (
    SilverStore,
    SilverStoreError,
    StoredDocument,
    WorkflowSnapshot,
    WorkflowState,
)
from ame_stocks_api.silver.ticker_type_contract import TICKER_TYPE_DIM_CONTRACT

DEFAULT_SAMPLE_LIMIT = 100
FULL_RUN_SCOPE_POLICY = SEPARATE_FULL_RUN_PLAN_POLICY
ASSET_PREVIEW_POLICY_VERSION = "asset-preview-v1"

_OBSERVATION_TABLE = ASSET_OBSERVATION_DAILY_CONTRACT.table
_VERSION_TABLE = ASSET_OBSERVATION_VERSION_CONTRACT.table
_UNIVERSE_TABLE = UNIVERSE_SOURCE_DAILY_CONTRACT.table
_TABLE_ORDER = (_OBSERVATION_TABLE, _VERSION_TABLE, _UNIVERSE_TABLE)
_CONTRACTS_BY_TABLE: Mapping[str, TableContract] = MappingProxyType(
    {
        _OBSERVATION_TABLE: ASSET_OBSERVATION_DAILY_CONTRACT,
        _VERSION_TABLE: ASSET_OBSERVATION_VERSION_CONTRACT,
        _UNIVERSE_TABLE: UNIVERSE_SOURCE_DAILY_CONTRACT,
    }
)
_AUTHORIZED_WORKFLOW_IDS: Mapping[str, str] = MappingProxyType(
    {
        _OBSERVATION_TABLE: (
            "c1bae241ed90e49aed1ae8a98b6801f511d6abaac2cef93c66ccba59d33775ec"
        ),
        _VERSION_TABLE: "989c8c513905e2710714c0b6f94352119e8fb1128147d8c2db9486c1e03df6da",
        _UNIVERSE_TABLE: "918ebc04d2eded87243387804d58fa9f24e4282ee27a8a26ac6ac22f4390b755",
    }
)
_AUTHORIZED_CODE_READY_EVENT_SHA256_BY_TABLE: Mapping[str, str] = MappingProxyType(
    {
        _OBSERVATION_TABLE: (
            "5c74b31676c709e6d9455da0c8ef8ec76fb4337754c2bc08c613be7dd9d89ef3"
        ),
        _VERSION_TABLE: "3655311e84140d523af72e2ac7bcc9e4602c135f8292f7548111fcc186c7b9b2",
        _UNIVERSE_TABLE: "d3ac371c080fb9f7317dbc66e7ae0673875d08b66826d13b063847d73a297067",
    }
)
_AUTHORIZED_CONTRACT_IDS: Mapping[str, str] = MappingProxyType(
    {
        _OBSERVATION_TABLE: (
            "dd916b8528b9ce1a341e6b8ad897ae80e40d5df118b8e102e4ea1f1ea6e9c045"
        ),
        _VERSION_TABLE: "14ce114f5911f7e4d1c15e58f0f42a8307066d6517e859d6233fa23c199616fc",
        _UNIVERSE_TABLE: "9711320ee9227df347224b7cd17a41fe10a352fddf089cd72b758bde7a7f0c58",
    }
)
_FIXED_CASE_IDS_BY_TABLE: Mapping[str, tuple[str, ...]] = MappingProxyType(
    {
        _OBSERVATION_TABLE: (
            "current_reference_snapshot",
            "delisting",
            "case_sensitive_tickers",
        ),
        _VERSION_TABLE: ("delisting", "case_sensitive_tickers"),
        _UNIVERSE_TABLE: (
            "current_reference_snapshot",
            "delisting",
            "case_sensitive_tickers",
        ),
    }
)
_FIXED_CASE_CHECK_IDS_BY_TABLE: Mapping[str, Mapping[str, tuple[str, ...]]] = (
    MappingProxyType(
        {
            _OBSERVATION_TABLE: MappingProxyType(
                {
                    "current_reference_snapshot": (
                        "current_type_dictionary_unmatched_values",
                        "current_exchange_dictionary_unmatched_values",
                        "reference_time_scope_invalid_rows",
                    ),
                    "delisting": (
                        "inactive_without_delisted_rows",
                        "timestamp_parse_invalid_rows",
                        "source_timestamp_after_capture_rows",
                    ),
                    "case_sensitive_tickers": (
                        "casefold_collision_groups",
                        "ticker_whitespace_rows",
                        "primary_key_duplicate_excess",
                    ),
                }
            ),
            _VERSION_TABLE: MappingProxyType(
                {
                    "delisting": (
                        "delisted_changed_groups",
                        "difference_fields_invalid_rows",
                        "selection_evidence_invalid_rows",
                        "nonunique_latest_selected_groups",
                    ),
                    "case_sensitive_tickers": (
                        "singleton_version_rows",
                        "version_projection_unreconciled",
                        "version_group_id_invalid_rows",
                    ),
                }
            ),
            _UNIVERSE_TABLE: MappingProxyType(
                {
                    "current_reference_snapshot": (
                        "current_dictionary_backfill_rows",
                        "current_type_dictionary_unmatched_values",
                        "current_exchange_dictionary_unmatched_values",
                    ),
                    "delisting": (
                        "inactive_without_delisted_rows",
                        "selected_timestamp_parse_invalid_rows",
                    ),
                    "case_sensitive_tickers": (
                        "casefold_collision_groups",
                        "selection_formula_invalid_rows",
                        "primary_key_duplicate_excess",
                    ),
                }
            ),
        }
    )
)

_ACTIVE_REQUEST_ID = "9e1ab3e3c1d4c09ea91e346c8eaeaf07279b698b1f1d8ae14c6437992b1b15ff"
_INACTIVE_REQUEST_ID = "f7c3f67c5966c307f470ff7468af78fb7848d83b7d5f2e25e7cda1d36dfaf90f"
_ACTIVE_MANIFEST_PATH = f"manifests/massive/assets/{_ACTIVE_REQUEST_ID}.json"
_INACTIVE_MANIFEST_PATH = f"manifests/massive/assets/{_INACTIVE_REQUEST_ID}.json"
_ACTIVE_MANIFEST_SHA256 = "b6ca5f53e3213649372c74f657ff106ad9d339d0eb5ae97bec0da5948a22ab45"
_INACTIVE_MANIFEST_SHA256 = "ffeb63f01b542f011fb4a9591096bb6abf1733582de89b85e41c78c04e745c14"

_EXCHANGE_RELEASE_ID = "feab0e1f32a5685d1115a6e4e87aab8ff50c18b99c6336a8790ecba44464d838"
_EXCHANGE_RELEASE_SHA256 = "d8789e6cf760ffb6274077736c18e37bd69330139ea1c6ecf2f420bb56f93f07"
_TICKER_TYPE_RELEASE_ID = "11a62f9c06ea5c609c159a7d619ba94cabbe39d3b07518fec279fa4758c882f6"
_TICKER_TYPE_RELEASE_SHA256 = "5568a905bb1cdfe791a300f5b12fdd1e2041e3e1c1aacfbf6cc78f4890b95f47"


def _require_sha256(value: object, label: str) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(
        character not in "0123456789abcdef" for character in value
    ):
        raise ValueError(f"{label} must be a lowercase SHA-256")
    return value


@dataclass(frozen=True, slots=True)
class AssetPreviewAuthorization:
    """Frozen source, dependency and output cardinality authorization."""

    session_date: date
    manifest_paths: tuple[str, ...]
    manifest_sha256_by_path: Mapping[str, str]
    request_ids_by_active: Mapping[bool, str]
    expected_input_rows: int
    expected_page_count: int
    expected_observation_rows: int
    expected_version_rows: int
    expected_universe_rows: int
    exchange_release_id: str
    exchange_release_sha256: str
    ticker_type_release_id: str
    ticker_type_release_sha256: str
    sample_limit: int = DEFAULT_SAMPLE_LIMIT
    dependency_lineage_required: bool = True

    def __post_init__(self) -> None:
        if type(self.session_date) is not date:
            raise ValueError("asset preview session_date must be a date")
        paths = tuple(self.manifest_paths)
        if len(paths) != 2 or len(set(paths)) != 2:
            raise ValueError("asset preview requires two distinct manifest paths")
        if any(not path.startswith("manifests/massive/assets/") for path in paths):
            raise ValueError("asset preview manifest is outside assets")
        manifest_sha = dict(self.manifest_sha256_by_path)
        if set(manifest_sha) != set(paths):
            raise ValueError("asset preview manifest SHA keys differ from manifest paths")
        request_ids = dict(self.request_ids_by_active)
        if set(request_ids) != {False, True}:
            raise ValueError("asset preview requires active and inactive request IDs")
        for label, value in (
            *((f"manifest_sha256:{path}", digest) for path, digest in manifest_sha.items()),
            *((f"request_id:{active}", digest) for active, digest in request_ids.items()),
            ("exchange_release_id", self.exchange_release_id),
            ("exchange_release_sha256", self.exchange_release_sha256),
            ("ticker_type_release_id", self.ticker_type_release_id),
            ("ticker_type_release_sha256", self.ticker_type_release_sha256),
        ):
            _require_sha256(value, label)
        for name in (
            "expected_input_rows",
            "expected_page_count",
            "expected_observation_rows",
            "expected_version_rows",
            "expected_universe_rows",
        ):
            value = getattr(self, name)
            if type(value) is not int or value <= 0:
                raise ValueError(f"asset preview {name} must be positive")
        if type(self.sample_limit) is not int or not 1 <= self.sample_limit <= 100:
            raise ValueError("asset preview sample_limit must be between 1 and 100")
        if type(self.dependency_lineage_required) is not bool:
            raise ValueError("asset preview dependency_lineage_required must be boolean")
        object.__setattr__(self, "manifest_paths", paths)
        object.__setattr__(
            self,
            "manifest_sha256_by_path",
            MappingProxyType(dict(sorted(manifest_sha.items()))),
        )
        object.__setattr__(
            self,
            "request_ids_by_active",
            MappingProxyType(dict(sorted(request_ids.items()))),
        )

    @property
    def expected_rows_by_table(self) -> Mapping[str, int]:
        return MappingProxyType(
            {
                _OBSERVATION_TABLE: self.expected_observation_rows,
                _VERSION_TABLE: self.expected_version_rows,
                _UNIVERSE_TABLE: self.expected_universe_rows,
            }
        )


CURRENT_ASSET_PREVIEW_AUTHORIZATION = AssetPreviewAuthorization(
    session_date=date(2026, 5, 11),
    manifest_paths=(_ACTIVE_MANIFEST_PATH, _INACTIVE_MANIFEST_PATH),
    manifest_sha256_by_path={
        _ACTIVE_MANIFEST_PATH: _ACTIVE_MANIFEST_SHA256,
        _INACTIVE_MANIFEST_PATH: _INACTIVE_MANIFEST_SHA256,
    },
    request_ids_by_active={True: _ACTIVE_REQUEST_ID, False: _INACTIVE_REQUEST_ID},
    expected_input_rows=35_647,
    expected_page_count=37,
    expected_observation_rows=35_647,
    expected_version_rows=82,
    expected_universe_rows=35_606,
    exchange_release_id=_EXCHANGE_RELEASE_ID,
    exchange_release_sha256=_EXCHANGE_RELEASE_SHA256,
    ticker_type_release_id=_TICKER_TYPE_RELEASE_ID,
    ticker_type_release_sha256=_TICKER_TYPE_RELEASE_SHA256,
)


@dataclass(frozen=True, slots=True)
class AssetTablePreviewRun:
    workflow: WorkflowSnapshot
    build: BuildManifest
    build_document: StoredDocument


@dataclass(frozen=True, slots=True)
class AssetPreviewRun:
    """Three registered table previews sharing one immutable Bronze inventory."""

    observation: AssetTablePreviewRun
    version: AssetTablePreviewRun
    universe: AssetTablePreviewRun
    inventory: SourceInventory
    inventory_document: StoredDocument

    @property
    def table_runs(self) -> tuple[AssetTablePreviewRun, ...]:
        return (self.observation, self.version, self.universe)


def run_asset_preview(
    data_root: Path,
    *,
    workflow_ids: Mapping[str, str],
    expected_event_sha256_by_table: Mapping[str, str],
    manifest_paths: tuple[str, ...],
    expected_manifest_sha256_by_path: Mapping[str, str],
    expected_input_rows: int,
    git_commit: str,
    repo_root: Path,
    actor: str = "s4-assets-preview-runner",
    calendar_name: str = "XNYS",
    sample_limit: int = DEFAULT_SAMPLE_LIMIT,
) -> AssetPreviewRun:
    """Run the exact production-authorized S4 preview; never build or publish full data."""

    return _run_asset_preview_authorized(
        data_root,
        workflow_ids=workflow_ids,
        expected_event_sha256_by_table=expected_event_sha256_by_table,
        manifest_paths=manifest_paths,
        expected_manifest_sha256_by_path=expected_manifest_sha256_by_path,
        expected_input_rows=expected_input_rows,
        git_commit=git_commit,
        repo_root=repo_root,
        actor=actor,
        calendar_name=calendar_name,
        sample_limit=sample_limit,
        authorization=CURRENT_ASSET_PREVIEW_AUTHORIZATION,
    )


def _run_asset_preview_authorized(
    data_root: Path,
    *,
    workflow_ids: Mapping[str, str],
    expected_event_sha256_by_table: Mapping[str, str],
    manifest_paths: tuple[str, ...],
    expected_manifest_sha256_by_path: Mapping[str, str],
    expected_input_rows: int,
    git_commit: str,
    repo_root: Path,
    actor: str,
    calendar_name: str,
    sample_limit: int,
    authorization: AssetPreviewAuthorization,
    transition_barrier: Callable[[str], None] | None = None,
) -> AssetPreviewRun:
    """Fixture-capable implementation with a recovery-safe three-workflow barrier."""

    workflows = dict(workflow_ids)
    expected_events = dict(expected_event_sha256_by_table)
    manifests = tuple(manifest_paths)
    manifest_sha = dict(expected_manifest_sha256_by_path)
    _validate_requested_authorization(
        workflows=workflows,
        expected_events=expected_events,
        manifest_paths=manifests,
        manifest_sha256_by_path=manifest_sha,
        expected_input_rows=expected_input_rows,
        calendar_name=calendar_name,
        sample_limit=sample_limit,
        authorization=authorization,
    )
    _verify_git_checkout(repo_root, git_commit)
    calendar_version = f"exchange-calendars=={version('exchange-calendars')}"
    parameters = _intent_parameters(
        calendar_name=calendar_name,
        manifest_paths=manifests,
        expected_manifest_sha256_by_path=manifest_sha,
        expected_input_rows=expected_input_rows,
        sample_limit=sample_limit,
        authorization=authorization,
    )
    root = data_root.expanduser().resolve()
    store = SilverStore(root)
    snapshots: dict[str, WorkflowSnapshot] = {}
    for table in _TABLE_ORDER:
        snapshot = store.verify_workflow_trust_chain(workflows[table], verify_artifacts=True)
        _verify_authorized_workflow_ancestry(store, table, snapshot)
        if snapshot.event_sha256 != expected_events[table]:
            raise SilverStoreError(f"stale asset preview expected event for {table}")
        contract, _ = store.load_workflow_contract(snapshot.workflow_id)
        if contract != _CONTRACTS_BY_TABLE[table]:
            raise SilverStoreError(f"asset preview workflow contract changed for {table}")
        if snapshot.state not in {
            WorkflowState.CODE_READY,
            WorkflowState.PREVIEW_READY,
            WorkflowState.AWAITING_REVIEW,
        }:
            raise SilverStoreError(
                f"asset preview cannot run {table} from {snapshot.state.value}"
            )
        snapshots[table] = snapshot

    if all(item.state is not WorkflowState.CODE_READY for item in snapshots.values()):
        table_runs, inventory, inventory_document = _load_existing_table_runs(
            root,
            store,
            snapshots,
            git_commit=git_commit,
            calendar_version=calendar_version,
            parameters=parameters,
            authorization=authorization,
        )
    else:
        bronze_inventory = build_asset_source_inventory(
            root,
            manifest_paths=manifests,
            git_commit=git_commit,
        )
        reader = read_asset_source_inventory(root, bronze_inventory)
        _validate_authorized_reader(reader, bronze_inventory, authorization=authorization)
        records = tuple(reader.iter_records())
        if len(records) != authorization.expected_input_rows:
            raise SilverStoreError("asset preview streamed row count differs from authorization")
        ticker_types, exchange_mics, dependency_lineage = _load_reference_dictionaries(
            root,
            store,
            authorization,
        )
        bound_inventory = _bind_dependency_lineage(
            bronze_inventory,
            dependency_lineage,
            authorization=authorization,
        )
        existing_inventory: SourceInventory | None = None
        existing_inventory_document: StoredDocument | None = None
        for table, snapshot in snapshots.items():
            if snapshot.state is WorkflowState.CODE_READY:
                continue
            registered = _load_event_preview(store, table, snapshot)
            loaded_inventory, loaded_document = _load_build_inventory(root, registered.build)
            _validate_authorized_inventory(loaded_inventory, authorization=authorization)
            if loaded_inventory.to_dict() != bound_inventory.to_dict():
                raise SilverStoreError(
                    "asset preview registered inventory differs from current Bronze"
                )
            if existing_inventory is None:
                existing_inventory = loaded_inventory
                existing_inventory_document = loaded_document
            elif (
                loaded_inventory.to_dict() != existing_inventory.to_dict()
                or loaded_document.sha256 != existing_inventory_document.sha256
            ):
                raise SilverStoreError(
                    "asset preview workflows do not share one source inventory"
                )
        if existing_inventory is None:
            inventory = bound_inventory
            inventory_document = store.register_source_inventory(inventory)
        else:
            inventory = existing_inventory
            if existing_inventory_document is None:  # pragma: no cover
                raise SilverStoreError("asset preview registered inventory document is missing")
            inventory_document = existing_inventory_document
        inputs = _inventory_inputs(inventory, inventory_document)
        intents = {
            table: _build_intent(
                workflow_id=workflows[table],
                contract=_CONTRACTS_BY_TABLE[table],
                inputs=inputs,
                git_commit=git_commit,
                calendar_version=calendar_version,
                parameters=parameters,
            )
            for table in _TABLE_ORDER
        }

        existing: dict[str, AssetTablePreviewRun] = {}
        for table, snapshot in snapshots.items():
            if snapshot.state is WorkflowState.CODE_READY:
                continue
            run = _load_event_preview(store, table, snapshot)
            loaded_inventory, loaded_document = _load_build_inventory(root, run.build)
            if (
                loaded_inventory.to_dict() != inventory.to_dict()
                or loaded_document.sha256 != inventory_document.sha256
            ):
                raise SilverStoreError("asset preview workflows do not share one source inventory")
            _require_matching_existing_preview(
                run.build,
                inventory,
                intent=intents[table],
                authorization=authorization,
            )
            existing[table] = run

        missing_tables = tuple(table for table in _TABLE_ORDER if table not in existing)
        transform_started_at = _now_utc()
        transform_start_ns = time.perf_counter_ns()
        tracemalloc.start()
        try:
            session = reader.sessions[0]
            transform_run_id = stable_digest(
                {
                    "build_ids": [intents[table].build_id for table in _TABLE_ORDER],
                    "preview_policy": ASSET_PREVIEW_POLICY_VERSION,
                    "session_date": authorization.session_date.isoformat(),
                }
            )
            transformed = transform_asset_session(
                session,
                records,
                build_id=transform_run_id,
                calendar_name=calendar_name,
                current_ticker_types=ticker_types,
                current_exchange_mics=exchange_mics,
            )
            _, peak_traced_bytes = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        transform_elapsed_ms = max(
            0, (time.perf_counter_ns() - transform_start_ns) // 1_000_000
        )
        results = {
            _OBSERVATION_TABLE: transformed.observation,
            _VERSION_TABLE: transformed.version,
            _UNIVERSE_TABLE: transformed.universe,
        }
        _validate_expected_results(results, authorization=authorization)
        raw_bytes = sum(
            page.raw_bytes
            for session in reader.sessions
            for request in session.requests
            for page in request.pages
        )
        compressed_bytes = sum(item.bytes for item in inventory.artifacts)
        table_runs = dict(existing)
        for table in missing_tables:
            build = _load_orphan_build_if_present(
                store,
                intent=intents[table],
                contract=_CONTRACTS_BY_TABLE[table],
                inventory=inventory,
                authorization=authorization,
            )
            if build is None:
                build = _materialize_table_preview(
                    root,
                    intent=intents[table],
                    result=results[table],
                    records=records,
                    authorization=authorization,
                    sample_limit=sample_limit,
                    transform_started_at=transform_started_at,
                    transform_elapsed_ms=transform_elapsed_ms,
                    peak_traced_bytes=peak_traced_bytes,
                    input_raw_bytes=raw_bytes,
                    input_compressed_bytes=compressed_bytes,
                    source_pages=reader.page_count,
                )
            _call_barrier(transition_barrier, f"before_record:{table}")
            _verify_git_checkout(repo_root, git_commit)
            snapshot = store.record_preview_build(
                build,
                expected_event_sha256=snapshots[table].event_sha256,
                actor=actor,
                recorded_at=_now_utc(),
                note=f"Registered bounded 2026-05-11 S4 preview for {table}.",
            )
            snapshots[table] = snapshot
            table_runs[table] = _load_event_preview(store, table, snapshot)

    for table in _TABLE_ORDER:
        snapshot = snapshots[table]
        if snapshot.state is WorkflowState.PREVIEW_READY:
            _call_barrier(transition_barrier, f"before_review:{table}")
            _verify_git_checkout(repo_root, git_commit)
            snapshot = store.request_preview_review(
                snapshot.workflow_id,
                expected_event_sha256=snapshot.event_sha256,
                actor=actor,
                created_at=_now_utc(),
                note=(
                    "S4 bounded asset preview submitted for user review; full run requires a "
                    "separate approved FullRunPlan."
                ),
            )
            snapshots[table] = snapshot
            current = table_runs[table]
            table_runs[table] = AssetTablePreviewRun(
                workflow=snapshot,
                build=current.build,
                build_document=current.build_document,
            )
        elif snapshot.state is WorkflowState.AWAITING_REVIEW:
            current = table_runs[table]
            if current.workflow != snapshot:
                table_runs[table] = AssetTablePreviewRun(
                    workflow=snapshot,
                    build=current.build,
                    build_document=current.build_document,
                )
        else:  # pragma: no cover - all CODE_READY states are recorded above
            raise SilverStoreError(f"asset preview did not register {table}")

    return AssetPreviewRun(
        observation=table_runs[_OBSERVATION_TABLE],
        version=table_runs[_VERSION_TABLE],
        universe=table_runs[_UNIVERSE_TABLE],
        inventory=inventory,
        inventory_document=inventory_document,
    )


def _materialize_table_preview(
    root: Path,
    *,
    intent: BuildIntent,
    result: AssetTableTransformResult,
    records: tuple[AssetSourceRecord, ...],
    authorization: AssetPreviewAuthorization,
    sample_limit: int,
    transform_started_at: str,
    transform_elapsed_ms: int,
    peak_traced_bytes: int,
    input_raw_bytes: int,
    input_compressed_bytes: int,
    source_pages: int,
) -> BuildManifest:
    write_start_ns = time.perf_counter_ns()
    fixed_case_path = f"{SilverStore.build_output_prefix(intent)}/samples/fixed-cases.json"
    qa_checks = tuple(
        replace(check, bounded_examples_path=fixed_case_path) for check in result.qa_checks
    )
    quarantine = tuple(
        replace(item, detected_build_id=intent.build_id) for item in result.quarantine_records
    )
    outputs = _write_preview_outputs(
        root,
        intent=intent,
        result=result,
        qa_checks=qa_checks,
        quarantine=quarantine,
        records=records,
        fixed_case_path=fixed_case_path,
        authorization=authorization,
        sample_limit=sample_limit,
    )
    write_elapsed_ms = max(0, (time.perf_counter_ns() - write_start_ns) // 1_000_000)
    input_sample = next(item for item in outputs if item.path.endswith("input-sample.json"))
    output_sample = next(item for item in outputs if item.path.endswith("output-sample.json"))
    data_bytes = sum(item.bytes for item in outputs if item.role is ArtifactRole.DATA)
    artifact_bytes = sum(item.bytes for item in outputs)
    max_artifact_bytes = max(item.bytes for item in outputs)
    fixed_case_ids = _FIXED_CASE_IDS_BY_TABLE[intent.table]
    if set(_FIXED_CASE_CHECK_IDS_BY_TABLE[intent.table]) != set(fixed_case_ids):
        raise SilverStoreError("asset preview fixed-case QA mapping is incomplete")
    qa_by_check_id = {item.check_id: item for item in qa_checks}
    fixed_case_qa_result_ids: dict[str, tuple[str, ...]] = {}
    for case_id, check_ids in _FIXED_CASE_CHECK_IDS_BY_TABLE[intent.table].items():
        missing = tuple(check_id for check_id in check_ids if check_id not in qa_by_check_id)
        if missing:
            raise SilverStoreError(
                f"asset preview fixed case {case_id} is missing QA checks: {missing}"
            )
        fixed_case_qa_result_ids[case_id] = tuple(
            qa_by_check_id[check_id].result_id for check_id in check_ids
        )
    preview = PreviewMetadata(
        fixed_case_ids=fixed_case_ids,
        fixed_case_qa_result_ids=fixed_case_qa_result_ids,
        input_sample_path=input_sample.path,
        input_sample_rows=int(input_sample.row_count or 0),
        output_sample_path=output_sample.path,
        output_sample_rows=int(output_sample.row_count or 0),
        examples_truncated=(
            authorization.expected_input_rows > sample_limit or result.table.num_rows > sample_limit
        ),
        full_run_inputs=intent.inputs,
        resource_usage={
            "data_parquet_bytes": data_bytes,
            "data_parquet_to_raw_ratio": float(data_bytes / input_raw_bytes),
            "elapsed_ms": transform_elapsed_ms + write_elapsed_ms,
            "input_compressed_bytes": input_compressed_bytes,
            "input_raw_bytes": input_raw_bytes,
            "input_rows": authorization.expected_input_rows,
            "max_serialized_artifact_bytes": max_artifact_bytes,
            "output_artifact_bytes": artifact_bytes,
            "output_rows": result.table.num_rows,
            "process_max_rss_bytes": _process_max_rss_bytes(),
            "python_peak_traced_bytes": peak_traced_bytes,
            "source_pages": source_pages,
            "transform_elapsed_ms": transform_elapsed_ms,
            "write_elapsed_ms": write_elapsed_ms,
        },
        full_run_projection={
            "scope_binding_mode": FULL_RUN_SCOPE_POLICY,
            "basis": "bounded session only; ten-year inventory requires a separate FullRunPlan",
            "bounded_preview_input_rows": authorization.expected_input_rows,
            "bounded_preview_output_rows": result.table.num_rows,
            "bounded_source_inventory_id": stable_digest(
                [item.to_dict() for item in intent.inputs]
            ),
            "projection_multiplier": 1.0,
            "status": "deferred_until_full_run_plan_approval",
        },
    )
    return BuildManifest(
        intent=intent,
        outputs=outputs,
        row_funnel=result.row_funnel,
        qa_checks=qa_checks,
        quarantine_issue_rows=len(quarantine),
        quarantine_unique_source_rows=len({item.source_record_id for item in quarantine}),
        quarantine_issue_ids_by_severity={
            severity.value: tuple(
                item.issue_id for item in quarantine if item.severity is severity
            )
            for severity in QASeverity
        },
        started_at=transform_started_at,
        completed_at=_now_utc(),
        preview=preview,
    )


def _write_preview_outputs(
    root: Path,
    *,
    intent: BuildIntent,
    result: AssetTableTransformResult,
    qa_checks: tuple[QACheckResult, ...],
    quarantine: tuple[Any, ...],
    records: tuple[AssetSourceRecord, ...],
    fixed_case_path: str,
    authorization: AssetPreviewAuthorization,
    sample_limit: int,
) -> tuple[ArtifactRef, ...]:
    prefix = SilverStore.build_output_prefix(intent)
    partition = (
        f"session_year={authorization.session_date.year}/"
        f"session_date={authorization.session_date.isoformat()}"
    )
    outputs = [
        _write_parquet_artifact(
            root,
            relative_path=f"{prefix}/data/{partition}/part-00000.parquet",
            table=result.table,
            role=ArtifactRole.DATA,
            table_name=result.contract.table,
        )
    ]
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
        [item.to_dict() for item in quarantine],
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
    input_rows = [_source_sample_row(item) for item in records[:sample_limit]]
    output_rows = [_json_safe(item) for item in result.table.slice(0, sample_limit).to_pylist()]
    fixed_rows = _fixed_case_evidence_rows(
        records,
        result_table=result.table,
        table=intent.table,
        authorization=authorization,
    )
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
            _write_json_sample(root, relative_path=fixed_case_path, rows=fixed_rows),
        )
    )
    return tuple(outputs)


def _fixed_case_evidence_rows(
    records: tuple[AssetSourceRecord, ...],
    *,
    result_table: pa.Table,
    table: str,
    authorization: AssetPreviewAuthorization,
) -> list[dict[str, object]]:
    output_rows = tuple(result_table.to_pylist())
    check_ids = _FIXED_CASE_CHECK_IDS_BY_TABLE[table]
    common = {
        "session_date": authorization.session_date.isoformat(),
        "table": table,
    }
    evidence: dict[str, dict[str, object]] = {}
    if "current_reference_snapshot" in _FIXED_CASE_IDS_BY_TABLE[table]:
        evidence["current_reference_snapshot"] = {
            **common,
            "assertion": (
                "Published S1/S2 snapshots are diagnostics only; no current label is "
                "backfilled as historical PIT metadata."
            ),
            "case_id": "current_reference_snapshot",
            "exchange_release_id": authorization.exchange_release_id,
            "qa_check_ids": list(check_ids["current_reference_snapshot"]),
            "ticker_type_release_id": authorization.ticker_type_release_id,
        }
    evidence["delisting"] = _delisting_fixed_case(
        records,
        output_rows,
        table=table,
        common=common,
        qa_check_ids=check_ids["delisting"],
    )
    evidence["case_sensitive_tickers"] = _case_sensitive_tickers_fixed_case(
        records,
        output_rows,
        table=table,
        common=common,
        qa_check_ids=check_ids["case_sensitive_tickers"],
    )
    return [evidence[case_id] for case_id in _FIXED_CASE_IDS_BY_TABLE[table]]


def _delisting_fixed_case(
    records: tuple[AssetSourceRecord, ...],
    output_rows: tuple[dict[str, object], ...],
    *,
    table: str,
    common: Mapping[str, object],
    qa_check_ids: tuple[str, ...],
) -> dict[str, object]:
    if table == _VERSION_TABLE:
        groups: dict[str, list[dict[str, object]]] = {}
        for row in output_rows:
            group_id = row.get("version_group_id")
            if isinstance(group_id, str):
                groups.setdefault(group_id, []).append(row)
        candidate: tuple[list[dict[str, object]], list[str]] | None = None
        for _, group in sorted(
            groups.items(),
            key=lambda item: (
                str(item[1][0].get("ticker")),
                bool(item[1][0].get("requested_active")),
                item[0],
            ),
        ):
            difference_fields = _difference_fields(group[0])
            selected = [row for row in group if row.get("is_selected") is True]
            updated = [row.get("last_updated_at_utc") for row in group]
            if (
                "delisted_utc" in difference_fields
                and len(group) >= 2
                and all(row.get("version_count") == len(group) for row in group)
                and len(selected) == 1
                and all(value is not None for value in updated)
                and selected[0].get("last_updated_at_utc") == max(updated)
            ):
                candidate = (group, difference_fields)
                break
        if candidate is None:
            raise SilverStoreError(
                "asset preview version delisting fixed case has no output evidence"
            )
        group, difference_fields = candidate
        source_records = [_record_for_output_row(records, row) for row in group]
        selected_row = next(row for row in group if row.get("is_selected") is True)
        return {
            **common,
            "assertion": (
                "The complete version group preserves the delisting change while selection "
                "uses the latest last_updated_at_utc, not the delisting date."
            ),
            "case_id": "delisting",
            "difference_fields": difference_fields,
            "output_rows": [_output_evidence_row(row, table=table) for row in group],
            "qa_check_ids": list(qa_check_ids),
            "raw_delisted_utc_by_source_pointer": {
                _record_pointer(record): record.row.get("delisted_utc")
                for record in source_records
            },
            "selected_source_record_id": selected_row["source_record_id"],
            "source_pointers": [_record_pointer(record) for record in source_records],
            "ticker": group[0]["ticker"],
            "version_group_id": group[0]["version_group_id"],
        }

    candidate_row: dict[str, object] | None = None
    candidate_record: AssetSourceRecord | None = None
    for row in sorted(
        output_rows,
        key=lambda item: (
            str(item.get("ticker")),
            str(item.get("source_request_id")),
            int(item.get("source_page_sequence", 0)),
            int(item.get("source_row_ordinal", 0)),
        ),
    ):
        if row.get("delisted_at_utc") is None:
            continue
        if table == _OBSERVATION_TABLE and not row.get("delisted_utc_raw"):
            continue
        record = _record_for_output_row(records, row)
        raw_delisted = record.row.get("delisted_utc")
        if isinstance(raw_delisted, str) and raw_delisted:
            candidate_row, candidate_record = row, record
            break
    if candidate_row is None or candidate_record is None:
        raise SilverStoreError("asset preview delisting fixed case has no output evidence")
    return {
        **common,
        "assertion": (
            "The table output retains a parsed source delisting value without inferring a "
            "missing date."
        ),
        "case_id": "delisting",
        "output_row": _output_evidence_row(candidate_row, table=table),
        "qa_check_ids": list(qa_check_ids),
        "raw_delisted_utc": candidate_record.row["delisted_utc"],
        "requested_active": candidate_record.requested_active,
        "source_pointer": _record_pointer(candidate_record),
        "ticker": candidate_row["ticker"],
    }


def _case_sensitive_tickers_fixed_case(
    records: tuple[AssetSourceRecord, ...],
    output_rows: tuple[dict[str, object], ...],
    *,
    table: str,
    common: Mapping[str, object],
    qa_check_ids: tuple[str, ...],
) -> dict[str, object]:
    groups: dict[tuple[bool, str], list[AssetSourceRecord]] = {}
    for record in records:
        ticker = record.row.get("ticker")
        if isinstance(ticker, str):
            groups.setdefault((record.requested_active, ticker.casefold()), []).append(record)

    selected: tuple[tuple[bool, str], list[AssetSourceRecord], list[dict[str, object]]] | None = (
        None
    )
    for key, group in sorted(groups.items()):
        exact_tickers = sorted({str(item.row["ticker"]) for item in group})
        if len(exact_tickers) < 2:
            continue
        if table == _UNIVERSE_TABLE:
            matching_output = [row for row in output_rows if row.get("ticker") in exact_tickers]
        else:
            matching_output = [
                row
                for row in output_rows
                if row.get("ticker") in exact_tickers
                and row.get("requested_active") is key[0]
            ]
        output_tickers = {str(row["ticker"]) for row in matching_output}
        source_counts = {
            ticker: sum(str(item.row["ticker"]) == ticker for item in group)
            for ticker in exact_tickers
        }
        if table == _VERSION_TABLE:
            qualifies = all(count == 1 for count in source_counts.values()) and not output_tickers
        else:
            qualifies = output_tickers == set(exact_tickers)
        if qualifies:
            selected = (key, group, matching_output)
            break
    if selected is None:
        raise SilverStoreError(
            "asset preview case-sensitive ticker fixed case has no table-bound evidence"
        )
    (requested_active, casefold_key), group, matching_output = selected
    exact_tickers = sorted({str(item.row["ticker"]) for item in group})
    source_counts = {
        ticker: sum(str(item.row["ticker"]) == ticker for item in group)
        for ticker in exact_tickers
    }
    if table == _VERSION_TABLE:
        assertion = (
            "Each exact ticker is a singleton transform group; sharing a casefold key does "
            "not merge them into a version group."
        )
    else:
        assertion = "Distinct exact tickers sharing a casefold key remain distinct outputs."
    return {
        **common,
        "assertion": assertion,
        "case_id": "case_sensitive_tickers",
        "casefold_key": casefold_key,
        "exact_tickers": exact_tickers,
        "output_exact_tickers": sorted(
            {str(row["ticker"]) for row in matching_output}
        ),
        "output_rows": [
            _output_evidence_row(row, table=table) for row in matching_output[:10]
        ],
        "qa_check_ids": list(qa_check_ids),
        "requested_active": requested_active,
        "source_occurrence_count_by_ticker": source_counts,
        "source_pointers": [_record_pointer(item) for item in group[:10]],
    }


def _difference_fields(row: Mapping[str, object]) -> list[str]:
    raw = row.get("difference_fields_json")
    try:
        value = json.loads(str(raw))
    except json.JSONDecodeError as exc:  # pragma: no cover - contract/QA guards this
        raise SilverStoreError("asset preview version difference_fields_json is invalid") from exc
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise SilverStoreError("asset preview version difference fields are invalid")
    return value


def _record_for_output_row(
    records: tuple[AssetSourceRecord, ...],
    row: Mapping[str, object],
) -> AssetSourceRecord:
    matches = [
        record
        for record in records
        if record.source_request_id == row.get("source_request_id")
        and record.source_artifact_sha256 == row.get("source_artifact_sha256")
        and record.source_page_sequence == row.get("source_page_sequence")
        and record.source_row_ordinal == row.get("source_row_ordinal")
    ]
    if len(matches) != 1:
        raise SilverStoreError("asset preview output lineage does not identify one source row")
    return matches[0]


def _output_evidence_row(
    row: Mapping[str, object],
    *,
    table: str,
) -> dict[str, object]:
    common_fields = (
        "ticker",
        "source_request_id",
        "source_artifact_sha256",
        "source_page_sequence",
        "source_row_ordinal",
    )
    fields_by_table = {
        _OBSERVATION_TABLE: (
            "requested_active",
            "source_record_id",
            "delisted_utc_raw",
            "delisted_at_utc",
        ),
        _VERSION_TABLE: (
            "requested_active",
            "version_group_id",
            "version_count",
            "source_record_id",
            "difference_fields_json",
            "last_updated_at_utc",
            "delisted_at_utc",
            "selection_rank",
            "is_selected",
            "selection_status",
            "selection_reason",
            "selected_source_record_id",
        ),
        _UNIVERSE_TABLE: (
            "active_on_date",
            "delisted_at_utc",
            "last_updated_at_utc",
            "selected_source_record_id",
            "version_group_id",
            "source_version_count",
            "selection_status",
        ),
    }
    return _json_safe(
        {field: row.get(field) for field in (*common_fields, *fields_by_table[table])}
    )


def _record_pointer(record: AssetSourceRecord) -> str:
    return (
        f"{record.source_artifact_path}#page={record.source_page_sequence}"
        f"&row={record.source_row_ordinal}"
    )


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
    stored = write_bytes_immutable(
        root,
        root / relative_path,
        content,
        temporary_directory=root / "tmp" / "silver-asset-preview-immutable-writes",
    )
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
    stored = write_bytes_immutable(
        root,
        root / relative_path,
        content,
        temporary_directory=root / "tmp" / "silver-asset-preview-immutable-writes",
    )
    return ArtifactRef(
        path=str(stored["path"]),
        sha256=str(stored["sha256"]),
        bytes=int(stored["bytes"]),
        row_count=len(rows),
        media_type="application/json",
        role=ArtifactRole.SAMPLE,
    )


def _source_sample_row(record: AssetSourceRecord) -> dict[str, object]:
    return {
        "raw": _json_safe(dict(record.row)),
        "requested_active": record.requested_active,
        "session_date": record.session_date.isoformat(),
        "source_artifact_path": record.source_artifact_path,
        "source_artifact_sha256": record.source_artifact_sha256,
        "source_capture_at_utc": record.source_capture_at_utc.isoformat(),
        "source_manifest_path": record.source_manifest_path,
        "source_manifest_sha256": record.source_manifest_sha256,
        "source_page_sequence": record.source_page_sequence,
        "source_provider_request_id": record.source_provider_request_id,
        "source_request_id": record.source_request_id,
        "source_row_ordinal": record.source_row_ordinal,
    }


def _expected_dependency_lineage(
    authorization: AssetPreviewAuthorization,
) -> tuple[UpstreamManifestRef, ...]:
    if not authorization.dependency_lineage_required:
        return ()
    return tuple(
        sorted(
            (
                UpstreamManifestRef(
                    path=(
                        "manifests/silver/releases/"
                        f"release_id={authorization.exchange_release_id}.json"
                    ),
                    sha256=authorization.exchange_release_sha256,
                ),
                UpstreamManifestRef(
                    path=(
                        "manifests/silver/releases/"
                        f"release_id={authorization.ticker_type_release_id}.json"
                    ),
                    sha256=authorization.ticker_type_release_sha256,
                ),
            ),
            key=lambda item: item.path,
        )
    )


def _bind_dependency_lineage(
    inventory: SourceInventory,
    dependency_lineage: tuple[UpstreamManifestRef, ...],
    *,
    authorization: AssetPreviewAuthorization,
) -> SourceInventory:
    expected = _expected_dependency_lineage(authorization)
    observed = tuple(sorted(dependency_lineage, key=lambda item: item.path))
    if observed != expected:
        raise SilverStoreError("asset preview dependency release lineage changed")
    existing_paths = {item.path for item in inventory.upstream_manifests}
    if any(item.path in existing_paths for item in observed):
        raise SilverStoreError("asset preview dependency lineage collides with Bronze")
    return replace(
        inventory,
        upstream_manifests=tuple(
            sorted((*inventory.upstream_manifests, *observed), key=lambda item: item.path)
        ),
    )


def _load_reference_dictionaries(
    root: Path,
    store: SilverStore,
    authorization: AssetPreviewAuthorization,
) -> tuple[frozenset[str], frozenset[str], tuple[UpstreamManifestRef, ...]]:
    ticker_release, ticker_document = store.load_release(authorization.ticker_type_release_id)
    exchange_release, exchange_document = store.load_release(authorization.exchange_release_id)
    if ticker_document.sha256 != authorization.ticker_type_release_sha256:
        raise SilverStoreError("asset preview ticker_type release SHA changed")
    if exchange_document.sha256 != authorization.exchange_release_sha256:
        raise SilverStoreError("asset preview exchange release SHA changed")
    reader = PublishedSilverReader(root)
    ticker_published = reader.inspect(ticker_release.release_id)
    exchange_published = reader.inspect(exchange_release.release_id)
    if ticker_published.contract != TICKER_TYPE_DIM_CONTRACT:
        raise SilverStoreError("asset preview ticker_type dependency contract changed")
    if exchange_published.contract != EXCHANGE_DIM_CONTRACT:
        raise SilverStoreError("asset preview exchange dependency contract changed")
    ticker_table = _read_published_table(ticker_published.data_paths)
    exchange_table = _read_published_table(exchange_published.data_paths)
    ticker_types = frozenset(str(item) for item in ticker_table.column("type_code").to_pylist())
    exchange_mics = frozenset(str(item) for item in exchange_table.column("mic").to_pylist())
    if not ticker_types or not exchange_mics:
        raise SilverStoreError("asset preview reference dictionary is empty")
    return (
        ticker_types,
        exchange_mics,
        (
            UpstreamManifestRef(
                path=exchange_document.path,
                sha256=exchange_document.sha256,
            ),
            UpstreamManifestRef(
                path=ticker_document.path,
                sha256=ticker_document.sha256,
            ),
        ),
    )


def _read_published_table(paths: tuple[Path, ...]) -> pa.Table:
    tables = [pq.read_table(path) for path in paths]
    if not tables:
        raise SilverStoreError("asset preview published dependency has no data")
    return tables[0] if len(tables) == 1 else pa.concat_tables(tables)


def _validate_requested_authorization(
    *,
    workflows: Mapping[str, str],
    expected_events: Mapping[str, str],
    manifest_paths: tuple[str, ...],
    manifest_sha256_by_path: Mapping[str, str],
    expected_input_rows: int,
    calendar_name: str,
    sample_limit: int,
    authorization: AssetPreviewAuthorization,
) -> None:
    if workflows != dict(_AUTHORIZED_WORKFLOW_IDS):
        raise SilverStoreError("asset preview workflow IDs are not authorized")
    if set(expected_events) != set(_TABLE_ORDER):
        raise SilverStoreError("asset preview expected-event keys are incomplete")
    for table, digest in expected_events.items():
        _require_sha256(digest, f"expected event:{table}")
    if manifest_paths != authorization.manifest_paths:
        raise SilverStoreError("asset preview manifest pair is not authorized")
    if manifest_sha256_by_path != dict(authorization.manifest_sha256_by_path):
        raise SilverStoreError("asset preview manifest SHA pair is not authorized")
    if expected_input_rows != authorization.expected_input_rows:
        raise SilverStoreError("asset preview expected row count is not authorized")
    if calendar_name != "XNYS":
        raise SilverStoreError("asset preview calendar is pinned to XNYS")
    if sample_limit != authorization.sample_limit:
        raise SilverStoreError(
            f"asset preview sample_limit is pinned to {authorization.sample_limit}"
        )
    if dict(_AUTHORIZED_CONTRACT_IDS) != {
        table: contract.contract_id for table, contract in _CONTRACTS_BY_TABLE.items()
    }:
        raise SilverStoreError("asset preview contract identity changed")


def _validate_authorized_reader(
    reader: AssetSourceReader,
    inventory: SourceInventory,
    *,
    authorization: AssetPreviewAuthorization,
) -> None:
    if inventory.source_dataset != "assets" or inventory.source_layer is not SourceLayer.BRONZE:
        raise SilverStoreError("asset preview inventory is not Bronze assets")
    if len(reader.sessions) != 1 or reader.sessions[0].session_date != authorization.session_date:
        raise SilverStoreError("asset preview inventory is not the authorized session")
    if reader.page_count != authorization.expected_page_count:
        raise SilverStoreError("asset preview page count differs from authorization")
    if reader.declared_row_count != authorization.expected_input_rows:
        raise SilverStoreError("asset preview declared rows differ from authorization")
    manifests = {item.path: item.sha256 for item in inventory.upstream_manifests}
    if manifests != dict(authorization.manifest_sha256_by_path):
        raise SilverStoreError("asset preview inventory manifests differ from authorization")
    requests = {
        request.requested_active: request.source_request_id
        for request in reader.sessions[0].requests
    }
    if requests != dict(authorization.request_ids_by_active):
        raise SilverStoreError("asset preview request IDs differ from authorization")


def _validate_authorized_inventory(
    inventory: SourceInventory,
    *,
    authorization: AssetPreviewAuthorization,
) -> None:
    if inventory.source_dataset != "assets" or inventory.source_layer is not SourceLayer.BRONZE:
        raise SilverStoreError("asset preview existing inventory is not Bronze assets")
    expected_manifests = dict(authorization.manifest_sha256_by_path)
    for dependency in _expected_dependency_lineage(authorization):
        if dependency.path in expected_manifests:
            raise SilverStoreError("asset preview dependency lineage path collides with Bronze")
        expected_manifests[dependency.path] = dependency.sha256
    if {
        item.path: item.sha256 for item in inventory.upstream_manifests
    } != expected_manifests:
        raise SilverStoreError("asset preview existing inventory lineage changed")
    if sum(item.row_count for item in inventory.artifacts) != authorization.expected_input_rows:
        raise SilverStoreError("asset preview existing inventory row count changed")
    if len(inventory.artifacts) != authorization.expected_page_count:
        raise SilverStoreError("asset preview existing inventory page count changed")


def _validate_expected_results(
    results: Mapping[str, AssetTableTransformResult],
    *,
    authorization: AssetPreviewAuthorization,
) -> None:
    for table in _TABLE_ORDER:
        result = results[table]
        expected = authorization.expected_rows_by_table[table]
        if result.contract != _CONTRACTS_BY_TABLE[table] or result.table.num_rows != expected:
            raise SilverStoreError(f"asset preview output cardinality changed for {table}")
        funnel = result.row_funnel
        expected_unmapped = authorization.expected_input_rows - expected
        expected_version_preserved = authorization.expected_version_rows
        if (
            funnel.input_rows != authorization.expected_input_rows
            or funnel.accepted_source_rows != authorization.expected_input_rows
            or funnel.exact_duplicate_excess != 0
            or funnel.quarantined_source_rows != 0
            or funnel.unmapped_source_rows != expected_unmapped
            or funnel.version_preserved_rows != expected_version_preserved
            or funnel.output_rows_by_table != {table: expected}
        ):
            raise SilverStoreError(f"asset preview row funnel changed for {table}")
        if result.quarantine_records or any(check.blocks_publish for check in result.qa_checks):
            raise SilverStoreError(f"asset preview blocking QA/quarantine changed for {table}")


def _inventory_inputs(
    inventory: SourceInventory,
    document: StoredDocument,
) -> tuple[ArtifactRef, ...]:
    return tuple(
        ArtifactRef(
            path=item.path,
            sha256=item.sha256,
            bytes=item.bytes,
            row_count=item.row_count,
            media_type=item.media_type,
            role=ArtifactRole.SOURCE,
            source_dataset=inventory.source_dataset,
            source_layer=inventory.source_layer,
            lineage_manifest_path=document.path,
            lineage_manifest_sha256=document.sha256,
        )
        for item in inventory.artifacts
    )


def _build_intent(
    *,
    workflow_id: str,
    contract: TableContract,
    inputs: tuple[ArtifactRef, ...],
    git_commit: str,
    calendar_version: str,
    parameters: Mapping[str, object],
) -> BuildIntent:
    return BuildIntent(
        workflow_id=workflow_id,
        domain=contract.domain,
        table=contract.table,
        schema_version=contract.schema_version,
        contract_id=contract.contract_id,
        kind=BuildKind.PREVIEW,
        attempt=1,
        retry_of_build_id=None,
        transform_version=ASSET_TRANSFORM_VERSION,
        git_commit=git_commit,
        exchange_calendar_version=calendar_version,
        inputs=inputs,
        parameters=parameters,
    )


def _intent_parameters(
    *,
    calendar_name: str,
    manifest_paths: tuple[str, ...],
    expected_manifest_sha256_by_path: Mapping[str, str],
    expected_input_rows: int,
    sample_limit: int,
    authorization: AssetPreviewAuthorization,
) -> dict[str, object]:
    return {
        "asset_metadata_time_scope": ASSET_METADATA_TIME_SCOPE,
        "asset_reference_time_scope": ASSET_REFERENCE_TIME_SCOPE,
        "asset_source_availability_quality": ASSET_SOURCE_AVAILABILITY_QUALITY,
        "asset_source_availability_rule": ASSET_SOURCE_AVAILABILITY_RULE,
        "asset_version_selection_rule": ASSET_VERSION_SELECTION_RULE,
        "calendar_name": calendar_name,
        "exchange_release_id": authorization.exchange_release_id,
        "exchange_release_sha256": authorization.exchange_release_sha256,
        "expected_input_rows": expected_input_rows,
        "expected_page_count": authorization.expected_page_count,
        "full_run_scope_policy": FULL_RUN_SCOPE_POLICY,
        "manifest_paths": list(manifest_paths),
        "manifest_sha256_by_path": dict(sorted(expected_manifest_sha256_by_path.items())),
        "preview_policy_version": ASSET_PREVIEW_POLICY_VERSION,
        "pyarrow_version": pa.__version__,
        "sample_limit": sample_limit,
        "sample_policy": "first_rows_in_verified_source_order_v1",
        "session_date": authorization.session_date.isoformat(),
        "ticker_type_release_id": authorization.ticker_type_release_id,
        "ticker_type_release_sha256": authorization.ticker_type_release_sha256,
        "universe_source_availability_rule": UNIVERSE_SOURCE_AVAILABILITY_RULE,
    }


def _load_existing_table_runs(
    root: Path,
    store: SilverStore,
    snapshots: Mapping[str, WorkflowSnapshot],
    *,
    git_commit: str,
    calendar_version: str,
    parameters: Mapping[str, object],
    authorization: AssetPreviewAuthorization,
) -> tuple[dict[str, AssetTablePreviewRun], SourceInventory, StoredDocument]:
    runs: dict[str, AssetTablePreviewRun] = {}
    shared_inventory: SourceInventory | None = None
    shared_document: StoredDocument | None = None
    for table in _TABLE_ORDER:
        run = _load_event_preview(store, table, snapshots[table])
        inventory, document = _load_build_inventory(root, run.build)
        _validate_authorized_inventory(inventory, authorization=authorization)
        intent = _build_intent(
            workflow_id=snapshots[table].workflow_id,
            contract=_CONTRACTS_BY_TABLE[table],
            inputs=_inventory_inputs(inventory, document),
            git_commit=git_commit,
            calendar_version=calendar_version,
            parameters=parameters,
        )
        _require_matching_existing_preview(
            run.build,
            inventory,
            intent=intent,
            authorization=authorization,
        )
        if shared_inventory is None:
            shared_inventory, shared_document = inventory, document
        elif inventory != shared_inventory or document.sha256 != shared_document.sha256:
            raise SilverStoreError("asset preview workflows have different source inventories")
        runs[table] = run
    if shared_inventory is None or shared_document is None:  # pragma: no cover
        raise SilverStoreError("asset preview has no existing source inventory")
    return runs, shared_inventory, shared_document


def _require_matching_existing_preview(
    build: BuildManifest,
    inventory: SourceInventory,
    *,
    intent: BuildIntent,
    authorization: AssetPreviewAuthorization,
) -> None:
    _validate_authorized_inventory(inventory, authorization=authorization)
    if inventory.git_commit != intent.git_commit:
        raise SilverStoreError("existing asset preview inventory Git commit changed")
    if build.intent != intent:
        raise SilverStoreError("existing asset preview intent differs from this exact run")
    if build.preview is None or build.preview.full_run_inputs != intent.inputs:
        raise SilverStoreError("existing asset preview bounded inputs are inconsistent")
    if build.preview.full_run_projection.get("scope_binding_mode") != FULL_RUN_SCOPE_POLICY:
        raise SilverStoreError("existing asset preview full-run scope policy changed")
    expected_output_rows = authorization.expected_rows_by_table[intent.table]
    if build.preview.fixed_case_ids != _FIXED_CASE_IDS_BY_TABLE[intent.table]:
        raise SilverStoreError("existing asset preview fixed cases differ from authorization")
    output_prefix = SilverStore.build_output_prefix(intent)
    expected_fixed_case_path = f"{output_prefix}/samples/fixed-cases.json"
    expected_input_sample_path = f"{output_prefix}/samples/input-sample.json"
    expected_output_sample_path = f"{output_prefix}/samples/output-sample.json"
    qa_by_check_id = {check.check_id: check.result_id for check in build.qa_checks}
    if (
        len(qa_by_check_id) != len(build.qa_checks)
        or set(qa_by_check_id)
        != set(_CONTRACTS_BY_TABLE[intent.table].required_qa_checks)
        or any(check.table != intent.table for check in build.qa_checks)
        or any(
            check.partition_key != authorization.session_date.isoformat()
            or check.bounded_examples_path != expected_fixed_case_path
            for check in build.qa_checks
        )
    ):
        raise SilverStoreError("existing asset preview QA set differs from authorization")
    try:
        expected_case_results = {
            case_id: tuple(sorted(qa_by_check_id[check_id] for check_id in check_ids))
            for case_id, check_ids in _FIXED_CASE_CHECK_IDS_BY_TABLE[intent.table].items()
        }
    except KeyError as exc:
        raise SilverStoreError(
            "existing asset preview is missing authorized fixed-case QA"
        ) from exc
    if dict(build.preview.fixed_case_qa_result_ids) != expected_case_results:
        raise SilverStoreError("existing asset preview fixed-case QA binding changed")
    if (
        build.preview.input_sample_path != expected_input_sample_path
        or build.preview.output_sample_path != expected_output_sample_path
        or build.preview.input_sample_rows
        != min(authorization.expected_input_rows, authorization.sample_limit)
        or build.preview.output_sample_rows
        != min(expected_output_rows, authorization.sample_limit)
        or build.preview.examples_truncated
        is not (
            authorization.expected_input_rows > authorization.sample_limit
            or expected_output_rows > authorization.sample_limit
        )
        or build.preview.full_run_projection.get("bounded_preview_input_rows")
        != authorization.expected_input_rows
        or build.preview.full_run_projection.get("bounded_preview_output_rows")
        != expected_output_rows
        or build.preview.resource_usage.get("input_rows")
        != authorization.expected_input_rows
        or build.preview.resource_usage.get("output_rows") != expected_output_rows
        or build.preview.resource_usage.get("source_pages")
        != authorization.expected_page_count
    ):
        raise SilverStoreError("existing asset preview metadata differs from authorization")
    data_outputs = tuple(item for item in build.outputs if item.role is ArtifactRole.DATA)
    qa_outputs = tuple(item for item in build.outputs if item.role is ArtifactRole.QA)
    quarantine_outputs = tuple(
        item for item in build.outputs if item.role is ArtifactRole.QUARANTINE
    )
    sample_outputs = tuple(item for item in build.outputs if item.role is ArtifactRole.SAMPLE)
    sample_by_path = {item.path: item for item in sample_outputs}
    expected_sample_paths = {
        expected_input_sample_path,
        expected_output_sample_path,
        expected_fixed_case_path,
    }
    partition = (
        f"session_year={authorization.session_date.year}/"
        f"session_date={authorization.session_date.isoformat()}"
    )
    expected_output_paths = {
        f"{output_prefix}/data/{partition}/part-00000.parquet",
        f"{output_prefix}/qa/qa-check-result.parquet",
        f"{output_prefix}/quarantine/quarantine-record.parquet",
        *expected_sample_paths,
    }
    if (
        len(data_outputs) != 1
        or data_outputs[0].table != intent.table
        or data_outputs[0].row_count != expected_output_rows
        or len(qa_outputs) != 1
        or qa_outputs[0].row_count != len(build.qa_checks)
        or len(quarantine_outputs) != 1
        or quarantine_outputs[0].row_count != 0
        or len(sample_outputs) != 3
        or {item.path for item in sample_outputs} != expected_sample_paths
        or sample_by_path.get(expected_input_sample_path) is None
        or sample_by_path[expected_input_sample_path].row_count
        != build.preview.input_sample_rows
        or sample_by_path.get(expected_output_sample_path) is None
        or sample_by_path[expected_output_sample_path].row_count
        != build.preview.output_sample_rows
        or sample_by_path.get(expected_fixed_case_path) is None
        or sample_by_path[expected_fixed_case_path].row_count
        != len(build.preview.fixed_case_ids)
        or {item.path for item in build.outputs} != expected_output_paths
    ):
        raise SilverStoreError("existing asset preview output evidence changed")
    funnel = build.row_funnel
    if (
        funnel.input_rows != authorization.expected_input_rows
        or funnel.accepted_source_rows != authorization.expected_input_rows
        or funnel.exact_duplicate_excess != 0
        or funnel.quarantined_source_rows != 0
        or funnel.unmapped_source_rows
        != authorization.expected_input_rows - expected_output_rows
        or funnel.version_preserved_rows != authorization.expected_version_rows
        or funnel.output_rows_by_table != {intent.table: expected_output_rows}
        or build.quarantine_issue_rows != 0
        or build.quarantine_unique_source_rows != 0
        or any(build.quarantine_issue_ids_by_severity.values())
        or any(check.blocks_publish for check in build.qa_checks)
    ):
        raise SilverStoreError("existing asset preview evidence differs from authorization")


def _load_orphan_build_if_present(
    store: SilverStore,
    *,
    intent: BuildIntent,
    contract: TableContract,
    inventory: SourceInventory,
    authorization: AssetPreviewAuthorization,
) -> BuildManifest | None:
    """Reuse a manifest stored before a failed event append, preserving exact evidence."""

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
    store.verify_build(build, contract)
    _require_matching_existing_preview(
        build,
        inventory,
        intent=intent,
        authorization=authorization,
    )
    return build


def _load_event_preview(
    store: SilverStore,
    table: str,
    snapshot: WorkflowSnapshot,
) -> AssetTablePreviewRun:
    for record in reversed(store.workflow_events(snapshot.workflow_id)):
        if record.event.to_state is WorkflowState.PREVIEW_READY:
            build_id = str(record.event.evidence["build_id"])
            build, document = store.load_build(table, build_id)
            if (
                build.intent.kind is not BuildKind.PREVIEW
                or build.intent.workflow_id != snapshot.workflow_id
            ):
                raise SilverStoreError("asset preview event points to the wrong build")
            store.verify_build(build, _CONTRACTS_BY_TABLE[table])
            return AssetTablePreviewRun(
                workflow=snapshot,
                build=build,
                build_document=document,
            )
    raise SilverStoreError(f"asset workflow has no registered preview build: {table}")


def _load_build_inventory(
    root: Path,
    build: BuildManifest,
) -> tuple[SourceInventory, StoredDocument]:
    first = build.intent.inputs[0]
    if first.lineage_manifest_path is None or first.lineage_manifest_sha256 is None:
        raise SilverStoreError("asset preview source inventory lineage is missing")
    path = safe_relative_path(root, first.lineage_manifest_path)
    try:
        content = path.read_bytes()
    except OSError as exc:
        raise SilverStoreError("cannot read asset preview source inventory") from exc
    if hashlib.sha256(content).hexdigest() != first.lineage_manifest_sha256:
        raise SilverStoreError("asset preview source inventory checksum mismatch")
    try:
        inventory = SourceInventory.from_dict(json.loads(content))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SilverStoreError("asset preview source inventory is invalid JSON") from exc
    if not all(
        item.lineage_manifest_path == first.lineage_manifest_path
        and item.lineage_manifest_sha256 == first.lineage_manifest_sha256
        for item in build.intent.inputs
    ):
        raise SilverStoreError("asset preview inputs do not share one source inventory")
    return inventory, StoredDocument(
        path=first.lineage_manifest_path,
        sha256=first.lineage_manifest_sha256,
        bytes=len(content),
    )


def _verify_authorized_workflow_ancestry(
    store: SilverStore,
    table: str,
    current: WorkflowSnapshot,
) -> None:
    if current.workflow_id != _AUTHORIZED_WORKFLOW_IDS[table]:
        raise SilverStoreError(f"asset preview workflow identity changed for {table}")
    records = store.workflow_events(current.workflow_id)
    if len(records) < 3:
        raise SilverStoreError(f"asset preview workflow lacks code-ready evidence for {table}")
    authorized = records[2]
    if (
        authorized.event.sequence != 3
        or authorized.event.to_state is not WorkflowState.CODE_READY
        or authorized.event_sha256 != _AUTHORIZED_CODE_READY_EVENT_SHA256_BY_TABLE[table]
    ):
        raise SilverStoreError(f"asset preview code-ready ancestry changed for {table}")
    if current.state is WorkflowState.CODE_READY and (
        current.sequence != 3
        or current.event_sha256 != _AUTHORIZED_CODE_READY_EVENT_SHA256_BY_TABLE[table]
    ):
        raise SilverStoreError(f"asset preview current code-ready event changed for {table}")


def _verify_git_checkout(repo_root: Path, git_commit: str) -> None:
    root = repo_root.expanduser().resolve()
    try:
        module_relative = Path(__file__).resolve().relative_to(root).as_posix()
    except ValueError as exc:
        raise SilverStoreError(
            "asset preview code is not executing from the verified Git checkout"
        ) from exc
    try:
        top_level = _git_output(root, "rev-parse", "--show-toplevel")
        head = _git_output(root, "rev-parse", "HEAD")
        tracked_module = _git_output(root, "ls-files", "--error-unmatch", "--", module_relative)
        status = _git_output(root, "status", "--porcelain=v1", "--untracked-files=all")
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise SilverStoreError("cannot verify asset preview Git checkout") from exc
    if Path(top_level).resolve() != root:
        raise SilverStoreError("asset preview repo_root is not the Git top level")
    if head != git_commit:
        raise SilverStoreError("asset preview Git HEAD differs from --git-commit")
    if tracked_module != module_relative:
        raise SilverStoreError("asset preview module is not the verified tracked source")
    if status:
        raise SilverStoreError("asset preview Git checkout is not clean")


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
        raise SilverStoreError(f"cannot verify asset preview Git checkout: {detail}")
    return completed.stdout.strip()


def _call_barrier(barrier: Callable[[str], None] | None, label: str) -> None:
    if barrier is not None:
        barrier(label)


def _json_safe(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


def _process_max_rss_bytes() -> int:
    value = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    return value if sys.platform == "darwin" else value * 1024


def _now_utc() -> str:
    return datetime.now(UTC).isoformat()


__all__ = [
    "CURRENT_ASSET_PREVIEW_AUTHORIZATION",
    "AssetPreviewAuthorization",
    "AssetPreviewRun",
    "AssetTablePreviewRun",
    "run_asset_preview",
]
