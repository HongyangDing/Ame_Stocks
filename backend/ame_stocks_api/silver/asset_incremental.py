"""Manifest-derived, single-session S4 Assets materialization.

This is the normal append hot path introduced by S7.5 I2.  It reads only one
canonical Massive active/inactive pair, writes three immutable session
partitions, and publishes a final receipt last.  It never discovers or reads
older S4 Parquet content.  The legacy ten-year Full runner remains unchanged as
the periodic reconciliation path.
"""

from __future__ import annotations

import fcntl
import json
import os
import re
import stat
import subprocess
from collections.abc import Callable, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime
from inspect import getfile
from pathlib import Path
from types import MappingProxyType

import pyarrow as pa
import pyarrow.parquet as pq

from ame_stocks_api.artifacts import (
    safe_relative_path,
    sha256_file,
    stable_digest,
    write_bytes_immutable,
)
from ame_stocks_api.silver.asset_contract import ASSET_CONTRACTS
from ame_stocks_api.silver.asset_incremental_contract import (
    S4AssetIncrementalContractError,
    S4BaseFrontier,
    S4ParentFrontierPin,
    S4ParentKind,
    S4ReferenceBinding,
    S4SessionPartitionReceipt,
    S4SessionRunReceipt,
    S4SessionRunSpec,
    S4SessionSourceBinding,
)
from ame_stocks_api.silver.asset_preview import (
    CURRENT_ASSET_PREVIEW_AUTHORIZATION,
    _load_reference_dictionaries,
)
from ame_stocks_api.silver.asset_release_set import load_exact_asset_release_set_control
from ame_stocks_api.silver.asset_source import (
    AssetSourceError,
    AssetSourceReader,
    build_asset_session_source_inventory,
    canonical_asset_session_manifest_paths,
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
    AssetTransformResult,
    transform_asset_session,
)
from ame_stocks_api.silver.calendar_artifact import (
    XNYSCalendarArtifact,
    load_xnys_calendar_artifact,
)
from ame_stocks_api.silver.contracts import ArtifactRef, arrow_schema_digest
from ame_stocks_api.silver.exchange_contract import EXCHANGE_DIM_CONTRACT
from ame_stocks_api.silver.incremental_contract import ArtifactPin
from ame_stocks_api.silver.reader import PublishedSilverReader
from ame_stocks_api.silver.store import SilverStore
from ame_stocks_api.silver.ticker_type_contract import TICKER_TYPE_DIM_CONTRACT

S4_ASSET_INCREMENTAL_POLICY_VERSION = "s4-assets-session-incremental-v1"
S4_BASE_TERMINAL_PARTITION_SET_RULE_VERSION = "s4_assets_base_terminal_partition_set_v1"
S4_ASSET_INCREMENTAL_PARQUET_WRITER_POLICY: Mapping[str, object] = MappingProxyType(
    {
        "compression": "zstd",
        "version": "2.6",
        "write_statistics": True,
    }
)
S4_ASSET_INCREMENTAL_TRANSFORM_SEMANTICS_DIGEST = stable_digest(
    {
        "asset_metadata_time_scope": ASSET_METADATA_TIME_SCOPE,
        "asset_reference_time_scope": ASSET_REFERENCE_TIME_SCOPE,
        "asset_source_availability_quality": ASSET_SOURCE_AVAILABILITY_QUALITY,
        "asset_source_availability_rule": ASSET_SOURCE_AVAILABILITY_RULE,
        "asset_transform_version": ASSET_TRANSFORM_VERSION,
        "asset_version_selection_rule": ASSET_VERSION_SELECTION_RULE,
        "contracts": {
            table: contract.contract_id for table, contract in sorted(ASSET_CONTRACTS.items())
        },
        "policy_version": S4_ASSET_INCREMENTAL_POLICY_VERSION,
        "universe_source_availability_rule": UNIVERSE_SOURCE_AVAILABILITY_RULE,
    }
)

_TABLES = tuple(sorted(ASSET_CONTRACTS))
_DEFERRED_FULL_HISTORY_CHECKS = frozenset({"cross_session_ticker_identity_churn_groups"})
_FULL_PLAN_COMPATIBILITY_PARAMETERS: Mapping[str, object] = MappingProxyType(
    {
        "asset_metadata_time_scope": ASSET_METADATA_TIME_SCOPE,
        "asset_reference_time_scope": ASSET_REFERENCE_TIME_SCOPE,
        "asset_source_availability_quality": ASSET_SOURCE_AVAILABILITY_QUALITY,
        "asset_source_availability_rule": ASSET_SOURCE_AVAILABILITY_RULE,
        "asset_version_selection_rule": ASSET_VERSION_SELECTION_RULE,
        "calendar_name": "XNYS",
        "universe_source_availability_rule": UNIVERSE_SOURCE_AVAILABILITY_RULE,
    }
)
_FULL_PARTITION_SUFFIX = re.compile(
    r"data/session_year=(?P<year>[0-9]{4})/"
    r"session_date=(?P<session>[0-9]{4}-[0-9]{2}-[0-9]{2})/part-00000[.]parquet"
)


class S4AssetIncrementalError(S4AssetIncrementalContractError):
    """Raised when a clean S4 append cannot safely complete."""


@dataclass(frozen=True, slots=True)
class S4AssetIncrementalRun:
    """Result of one new or idempotently reused S4 session receipt."""

    run_spec: S4SessionRunSpec
    run_spec_artifact: ArtifactPin
    receipt: S4SessionRunReceipt
    receipt_artifact: ArtifactPin
    idempotent: bool


def verify_s4_incremental_git_checkout(repo_root: Path, git_commit: str) -> None:
    """Require the exact clean checkout claimed by writer provenance."""

    root = repo_root.expanduser().resolve()
    try:
        top_level = _git_output(root, "rev-parse", "--show-toplevel")
        head = _git_output(root, "rev-parse", "HEAD")
        status = _git_output(root, "status", "--porcelain=v1", "--untracked-files=all")
        tracked_relatives = tuple(
            sorted(
                {
                    Path(__file__).resolve().relative_to(root).as_posix(),
                    Path(getfile(S4SessionRunSpec)).resolve().relative_to(root).as_posix(),
                    "backend/ame_stocks_api/cli/silver_assets_incremental.py",
                    "backend/ame_stocks_api/cli/silver_assets_incremental_bootstrap.py",
                    "pyproject.toml",
                }
            )
        )
        tracked_output = _git_output(
            root,
            "ls-files",
            "--error-unmatch",
            "--",
            *tracked_relatives,
        )
    except (OSError, subprocess.TimeoutExpired, ValueError) as exc:
        raise S4AssetIncrementalError("cannot verify S4 incremental Git checkout") from exc
    if Path(top_level).resolve() != root:
        raise S4AssetIncrementalError("S4 incremental repo root is not the Git top level")
    if head != git_commit:
        raise S4AssetIncrementalError("S4 incremental Git HEAD differs from --git-commit")
    if tuple(sorted(tracked_output.splitlines())) != tracked_relatives:
        raise S4AssetIncrementalError("S4 incremental modules are not tracked source")
    if status:
        raise S4AssetIncrementalError("S4 incremental Git checkout is not clean")


def derive_s4_base_frontier(
    data_root: Path,
    *,
    release_set_artifact: ArtifactPin,
    calendar_artifact_id: str,
    calendar_artifact_sha256: str,
    reference_binding: S4ReferenceBinding,
) -> S4BaseFrontier:
    """Derive one trusted base adapter from exact published S4 controls.

    This boundary authenticates the release-set control plane and its declared
    partition metadata.  It intentionally never opens an historical S4 DATA
    Parquet file; the only Parquet reads are the two tiny, exact S1/S2 releases
    used to reproduce ``reference_binding``.
    """

    root = data_root.expanduser().resolve()
    release_set_id = _release_set_id_from_artifact(release_set_artifact)
    release_set, release_document = load_exact_asset_release_set_control(
        root,
        release_set_id=release_set_id,
        expected_sha256=release_set_artifact.sha256,
        expected_bytes=release_set_artifact.bytes,
    )
    release_document_pin = ArtifactPin(
        path=release_document.path,
        sha256=release_document.sha256,
        bytes=release_document.bytes,
    )
    if release_document_pin != release_set_artifact:
        raise S4AssetIncrementalError("S4 base release-set artifact pin changed")

    calendar = load_xnys_calendar_artifact(
        root,
        calendar_artifact_id=calendar_artifact_id,
        expected_sha256=calendar_artifact_sha256,
    )
    if _pin_existing(root, calendar.relative_path).sha256 != calendar_artifact_sha256:
        raise S4AssetIncrementalError("S4 base calendar artifact pin changed")
    verified_reference = _load_reference_binding_from_exact_releases(
        root,
        reference_binding.dependency_pins,
    )
    if verified_reference != reference_binding:
        raise S4AssetIncrementalError(
            "S4 base reference values differ from their exact published releases"
        )

    expected_contracts = {
        table: contract.contract_id for table, contract in ASSET_CONTRACTS.items()
    }
    expected_schemas = {
        table: arrow_schema_digest(contract.arrow_schema)
        for table, contract in ASSET_CONTRACTS.items()
    }
    members = {item.table: item for item in release_set.members}
    if set(members) != set(_TABLES):
        raise S4AssetIncrementalError("S4 base release set does not contain three exact tables")

    store = SilverStore(root)
    sessions_by_table: dict[str, tuple[date, ...]] = {}
    outputs_by_table: dict[str, dict[date, ArtifactRef]] = {}
    full_date_ranges: set[tuple[date, date]] = set()
    expected_reference_release_pins: set[tuple[str, str]] = set()
    for table in _TABLES:
        member = members[table]
        contract = ASSET_CONTRACTS[table]
        if (
            member.domain != contract.domain
            or member.schema_version != contract.schema_version
            or member.contract_id != contract.contract_id
        ):
            raise S4AssetIncrementalError(f"S4 base member contract changed for {table}")

        full_plan, full_plan_document = store.load_full_run_plan(
            table,
            member.full_run_plan_id,
        )
        full_writer_policy = full_plan.parameters.get("parquet_writer_policy")
        if (
            full_plan_document.sha256 != member.full_run_plan_sha256
            or full_plan.table != table
            or full_plan.domain != member.domain
            or full_plan.schema_version != member.schema_version
            or full_plan.contract_id != member.contract_id
            or full_plan.transform_version != ASSET_TRANSFORM_VERSION
            or any(
                full_plan.parameters.get(key) != value
                for key, value in _FULL_PLAN_COMPATIBILITY_PARAMETERS.items()
            )
            or not isinstance(full_writer_policy, Mapping)
            or dict(full_writer_policy) != dict(S4_ASSET_INCREMENTAL_PARQUET_WRITER_POLICY)
        ):
            raise S4AssetIncrementalError(
                f"S4 base FullRunPlan is not incrementally compatible for {table}"
            )
        full_start = _date_parameter(full_plan.parameters.get("date_start"), "date_start")
        full_end = _date_parameter(full_plan.parameters.get("date_end"), "date_end")
        if full_start > full_end:
            raise S4AssetIncrementalError("S4 base FullRunPlan date range is invalid")
        full_date_ranges.add((full_start, full_end))
        expected_reference_release_pins.update(
            {
                _full_plan_reference_pin(full_plan.parameters, "exchange"),
                _full_plan_reference_pin(full_plan.parameters, "ticker_type"),
            }
        )

        output_by_session: dict[date, ArtifactRef] = {}
        prefix = (
            f"silver/schema=v{member.schema_version}/{member.domain}/{table}/"
            f"build_id={member.build_id}/"
        )
        for output in member.outputs:
            if (
                not output.path.startswith(prefix)
                or output.schema_digest != expected_schemas[table]
                or output.table != table
            ):
                raise S4AssetIncrementalError(f"S4 base partition metadata changed for {table}")
            suffix = output.path[len(prefix) :]
            match = _FULL_PARTITION_SUFFIX.fullmatch(suffix)
            if match is None:
                raise S4AssetIncrementalError(f"S4 base partition path is noncanonical for {table}")
            session = _date_parameter(match.group("session"), "partition session")
            if int(match.group("year")) != session.year or session in output_by_session:
                raise S4AssetIncrementalError(
                    f"S4 base partition session is invalid or duplicated for {table}"
                )
            output_by_session[session] = output
        sessions_by_table[table] = tuple(sorted(output_by_session))
        outputs_by_table[table] = output_by_session

    if len(full_date_ranges) != 1 or len(expected_reference_release_pins) != 2:
        raise S4AssetIncrementalError("S4 base FullRunPlan controls disagree across tables")
    actual_reference_release_pins = {
        (item.path, item.sha256) for item in reference_binding.dependency_pins
    }
    if actual_reference_release_pins != expected_reference_release_pins:
        raise S4AssetIncrementalError(
            "S4 base reference releases differ from the published FullRunPlans"
        )
    session_sets = {sessions for sessions in sessions_by_table.values()}
    if len(session_sets) != 1:
        raise S4AssetIncrementalError("S4 base table session sets differ")
    sessions = next(iter(session_sets))
    if not sessions:
        raise S4AssetIncrementalError("S4 base table session set is empty")
    full_start, full_end = next(iter(full_date_ranges))
    expected_sessions = tuple(
        item.session_date
        for item in calendar.sessions
        if full_start <= item.session_date <= full_end
    )
    if sessions != expected_sessions or sessions[0] != full_start or sessions[-1] != full_end:
        raise S4AssetIncrementalError(
            "S4 base partitions are not the complete pinned-calendar FullRunPlan range"
        )

    terminal_session = sessions[-1]
    terminal_partition_set_digest = stable_digest(
        {
            "base_release_set_id": release_set.release_set_id,
            "partitions": [
                {
                    "artifact": outputs_by_table[table][terminal_session].to_dict(),
                    "contract_id": expected_contracts[table],
                    "table": table,
                }
                for table in _TABLES
            ],
            "rule_version": S4_BASE_TERMINAL_PARTITION_SET_RULE_VERSION,
            "terminal_session": terminal_session.isoformat(),
        }
    )
    committed_at = datetime.fromisoformat(release_set.committed_at.replace("Z", "+00:00"))
    release_available_session, _ = calendar.first_open_after(committed_at)
    if release_available_session < terminal_session:
        raise S4AssetIncrementalError("S4 base release availability precedes its terminal data")
    return S4BaseFrontier(
        base_release_set_id=release_set.release_set_id,
        base_release_set_artifact=release_set_artifact,
        terminal_session=terminal_session,
        terminal_partition_set_digest=terminal_partition_set_digest,
        calendar_artifact_id=calendar.calendar_artifact_id,
        reference_binding_id=reference_binding.binding_id,
        contract_ids_by_table=expected_contracts,
        schema_digests_by_table=expected_schemas,
        transform_semantics_digest=S4_ASSET_INCREMENTAL_TRANSFORM_SEMANTICS_DIGEST,
        parquet_writer_policy=S4_ASSET_INCREMENTAL_PARQUET_WRITER_POLICY,
        release_available_session=release_available_session,
    )


def write_s4_base_frontier(
    data_root: Path,
    frontier: S4BaseFrontier,
) -> S4ParentFrontierPin:
    """Write a metadata-only base adapter without reading any S4 Parquet.

    Creating a production instance remains an explicitly approved migration
    action.  This helper merely freezes and validates its deterministic bytes.
    """

    root = data_root.expanduser().resolve()
    relative = (
        "manifests/silver/incremental/s4/assets/base-frontiers/"
        f"frontier_id={frontier.frontier_id}/manifest.json"
    )
    stored = write_bytes_immutable(
        root,
        safe_relative_path(root, relative),
        _canonical_json_bytes(frontier.to_dict()),
        temporary_directory=root / "tmp" / "silver-asset-incremental-immutable-writes",
    )
    return S4ParentFrontierPin(
        parent_kind=S4ParentKind.BASE_RELEASE,
        terminal_session=frontier.terminal_session,
        terminal_receipt_id=frontier.frontier_id,
        artifact=_artifact_pin(stored),
    )


def parent_frontier_from_session_receipt(
    receipt: S4SessionRunReceipt,
    receipt_artifact: ArtifactPin,
) -> S4ParentFrontierPin:
    """Use an exact prior session receipt as the next clean-append frontier."""

    return S4ParentFrontierPin(
        parent_kind=S4ParentKind.SESSION_RECEIPT,
        terminal_session=receipt.session_date,
        terminal_receipt_id=receipt.receipt_id,
        artifact=receipt_artifact,
    )


def load_completed_s4_asset_session_run(
    data_root: Path,
    session_date: date,
) -> S4AssetIncrementalRun | None:
    """Return an authenticated completed session without rereading Bronze pages."""

    if type(session_date) is not date:
        raise S4AssetIncrementalError("target session must be a native date")
    root = data_root.expanduser().resolve()
    receipt_relative = _receipt_relative_path(session_date)
    receipt_path = safe_relative_path(root, receipt_relative)
    if not receipt_path.exists():
        return None
    receipt, receipt_artifact = _load_receipt(root, receipt_relative)
    if receipt.session_date != session_date:
        raise S4AssetIncrementalError("completed S4 receipt session changed")
    run_spec_artifact = _pin_existing(root, _run_spec_relative_path(session_date))
    run_spec = _load_run_spec_from_pin(root, run_spec_artifact)
    calendar, _ = _validate_run_spec_inputs(root, run_spec)
    _reauthenticate_completed_source_manifests(root, run_spec.source_binding, calendar)
    _validate_receipt_against_spec(receipt, run_spec, run_spec_artifact)
    _verify_receipt_artifacts(root, receipt)
    return S4AssetIncrementalRun(
        run_spec=run_spec,
        run_spec_artifact=run_spec_artifact,
        receipt=receipt,
        receipt_artifact=receipt_artifact,
        idempotent=True,
    )


def load_current_s4_reference_binding(data_root: Path) -> S4ReferenceBinding:
    """Load the exact, already-published S1/S2 dictionaries used by S4.

    These are small reference dependencies, not historical S4 session
    partitions.  Their release documents are pinned into the run spec.
    """

    root = data_root.expanduser().resolve()
    ticker_types, exchange_mics, dependency_refs = _load_reference_dictionaries(
        root,
        SilverStore(root),
        CURRENT_ASSET_PREVIEW_AUTHORIZATION,
    )
    pins = tuple(
        sorted(
            (_pin_existing(root, item.path) for item in dependency_refs),
            key=lambda item: item.path,
        )
    )
    return S4ReferenceBinding(
        ticker_types=tuple(sorted(ticker_types)),
        exchange_mics=tuple(sorted(exchange_mics)),
        dependency_pins=pins,
    )


def prepare_s4_asset_session_run_spec(
    data_root: Path,
    *,
    session_date: date,
    parent_frontier: S4ParentFrontierPin,
    calendar_artifact_id: str,
    calendar_artifact_sha256: str,
    reference_binding: S4ReferenceBinding,
    receipt_available_session: date,
    writer_git_commit: str,
) -> S4SessionRunSpec:
    """Derive one run spec from exact parent, calendar and Bronze manifests.

    No directory discovery and no old S4 Parquet read occurs.  Bronze page
    bounds and row counts are derived from the selected pair's manifests.
    """

    if type(session_date) is not date:
        raise S4AssetIncrementalError("target session must be a native date")
    root = data_root.expanduser().resolve()
    parent = _load_parent_frontier(root, parent_frontier)
    calendar = load_xnys_calendar_artifact(
        root,
        calendar_artifact_id=calendar_artifact_id,
        expected_sha256=calendar_artifact_sha256,
    )
    calendar_pin = _pin_existing(root, calendar.relative_path)
    _require_next_session(calendar, parent_frontier.terminal_session, session_date)
    _require_availability_sessions(
        calendar,
        parent_available_session=_parent_available_session(parent),
        receipt_available_session=receipt_available_session,
    )
    if isinstance(parent, S4BaseFrontier) and parent.calendar_artifact_id != calendar_artifact_id:
        raise S4AssetIncrementalError("base frontier calendar differs from the run calendar")

    verified_reference = _load_reference_binding_from_exact_releases(
        root,
        reference_binding.dependency_pins,
    )
    if verified_reference != reference_binding:
        raise S4AssetIncrementalError(
            "S4 reference values differ from their exact published releases"
        )

    if isinstance(parent, S4BaseFrontier):
        _require_base_compatibility(
            root,
            parent,
            calendar_artifact_id=calendar_artifact_id,
            calendar_artifact_sha256=calendar_artifact_sha256,
            reference_binding=reference_binding,
        )

    inventory = build_asset_session_source_inventory(
        root,
        session_date,
        writer_git_commit,
    )
    reader = read_asset_source_inventory(root, inventory)
    source_session = _single_source_session(reader, session_date)
    pair_available_session, _ = calendar.first_open_after(source_session.capture_completed_at_utc)
    source_binding = S4SessionSourceBinding(
        session_date=session_date,
        inventory=inventory,
        active_request_id=source_session.active_request.source_request_id,
        inactive_request_id=source_session.inactive_request.source_request_id,
        pair_capture_completed_at_utc=source_session.capture_completed_at_utc,
        pair_available_session=pair_available_session,
        page_count=reader.page_count,
        declared_row_count=reader.declared_row_count,
    )
    if receipt_available_session < max(
        pair_available_session,
        _parent_available_session(parent),
    ):
        raise S4AssetIncrementalError("receipt availability precedes its source or parent frontier")

    return S4SessionRunSpec(
        parent_frontier=parent_frontier,
        source_binding=source_binding,
        reference_binding=reference_binding,
        calendar_artifact_id=calendar_artifact_id,
        calendar_artifact=calendar_pin,
        contract_ids_by_table={
            table: contract.contract_id for table, contract in ASSET_CONTRACTS.items()
        },
        schema_digests_by_table={
            table: arrow_schema_digest(contract.arrow_schema)
            for table, contract in ASSET_CONTRACTS.items()
        },
        transform_semantics_digest=S4_ASSET_INCREMENTAL_TRANSFORM_SEMANTICS_DIGEST,
        parquet_writer_policy=S4_ASSET_INCREMENTAL_PARQUET_WRITER_POLICY,
        receipt_available_session=receipt_available_session,
        writer_git_commit=writer_git_commit,
    )


def run_s4_asset_session_incremental(
    data_root: Path,
    run_spec: S4SessionRunSpec,
    *,
    transition_barrier: Callable[[str], None] | None = None,
) -> S4AssetIncrementalRun:
    """Materialize one exact S4 session and write the final receipt last."""

    root = data_root.expanduser().resolve()
    calendar, parent = _validate_run_spec_inputs(root, run_spec)
    if run_spec.receipt_available_session < max(
        run_spec.source_binding.pair_available_session,
        _parent_available_session(parent),
    ):
        raise S4AssetIncrementalError("receipt availability precedes its source or parent frontier")

    lock_path = safe_relative_path(root, _lock_relative_path(run_spec.source_binding.session_date))
    with _exclusive_lock(lock_path):
        receipt_relative = _receipt_relative_path(run_spec.source_binding.session_date)
        receipt_path = safe_relative_path(root, receipt_relative)
        if receipt_path.exists():
            receipt, receipt_artifact = _load_receipt(root, receipt_relative)
            run_spec_artifact = _pin_existing(root, _run_spec_relative_path(receipt.session_date))
            stored_run_spec = _load_run_spec_from_pin(root, run_spec_artifact)
            if stored_run_spec.run_spec_id != run_spec.run_spec_id:
                raise S4AssetIncrementalError(
                    "correction_required: existing run-spec semantics differ"
                )
            _validate_receipt_against_spec(receipt, stored_run_spec, run_spec_artifact)
            _reauthenticate_completed_source_manifests(
                root,
                stored_run_spec.source_binding,
                calendar,
            )
            _verify_receipt_artifacts(root, receipt)
            return S4AssetIncrementalRun(
                run_spec=stored_run_spec,
                run_spec_artifact=run_spec_artifact,
                receipt=receipt,
                receipt_artifact=receipt_artifact,
                idempotent=True,
            )

        reader = read_asset_source_inventory(root, run_spec.source_binding.inventory)
        source_session = _single_source_session(reader, run_spec.source_binding.session_date)
        _validate_reader_against_binding(reader, run_spec.source_binding, calendar)
        effective_run_spec, run_spec_artifact = _write_or_reuse_run_spec(root, run_spec)
        records = tuple(reader.iter_session_records(source_session.session_date))
        if len(records) != run_spec.source_binding.declared_row_count:
            raise S4AssetIncrementalError("streamed source rows differ from manifest bounds")
        transform_build_id = stable_digest(
            {
                "policy_version": S4_ASSET_INCREMENTAL_POLICY_VERSION,
                "run_spec_id": effective_run_spec.run_spec_id,
                "session_date": source_session.session_date.isoformat(),
            }
        )
        transformed = transform_asset_session(
            source_session,
            records,
            build_id=transform_build_id,
            calendar_name="XNYS",
            current_ticker_types=effective_run_spec.reference_binding.ticker_types,
            current_exchange_mics=effective_run_spec.reference_binding.exchange_mics,
        )
        results = _results_by_table(transformed)
        _validate_transform_results(results, effective_run_spec)
        _call_barrier(transition_barrier, "after_transform")

        table_qa = {table: _table_qa_projection(results[table]) for table in _TABLES}
        partition_receipts: list[S4SessionPartitionReceipt] = []
        for table in _TABLES:
            result = results[table]
            artifact = _write_partition(
                root,
                effective_run_spec,
                table=table,
                result=result,
            )
            partition_receipts.append(
                S4SessionPartitionReceipt(
                    table_name=table,
                    session_date=source_session.session_date,
                    artifact=artifact,
                    row_count=result.table.num_rows,
                    contract_id=result.contract.contract_id,
                    schema_digest=arrow_schema_digest(result.table.schema),
                    source_binding_id=effective_run_spec.source_binding.source_binding_id,
                    row_funnel_digest=table_qa[table]["row_funnel_digest"],
                    qa_result_set_digest=table_qa[table]["qa_result_set_digest"],
                )
            )
            _call_barrier(transition_barrier, f"after_partition:{table}")

        qa_result_set_digest = _combined_qa_digest(partition_receipts)
        qa_details_artifact = _write_qa_details(
            root,
            effective_run_spec,
            table_qa=table_qa,
            qa_result_set_digest=qa_result_set_digest,
        )
        _call_barrier(transition_barrier, "after_qa_details")
        receipt = S4SessionRunReceipt(
            run_spec_id=effective_run_spec.run_spec_id,
            run_spec_artifact=run_spec_artifact,
            parent_frontier_id=effective_run_spec.parent_frontier.parent_frontier_id,
            session_date=source_session.session_date,
            source_binding_id=effective_run_spec.source_binding.source_binding_id,
            pair_available_session=effective_run_spec.source_binding.pair_available_session,
            receipt_available_session=effective_run_spec.receipt_available_session,
            partition_receipts=tuple(partition_receipts),
            qa_details_artifact=qa_details_artifact,
            qa_result_set_digest=qa_result_set_digest,
        )
        _call_barrier(transition_barrier, "before_receipt")
        receipt_artifact = _write_receipt(root, receipt)
        return S4AssetIncrementalRun(
            run_spec=effective_run_spec,
            run_spec_artifact=run_spec_artifact,
            receipt=receipt,
            receipt_artifact=receipt_artifact,
            idempotent=False,
        )


def _validate_run_spec_inputs(
    root: Path,
    run_spec: S4SessionRunSpec,
) -> tuple[XNYSCalendarArtifact, S4BaseFrontier | S4SessionRunReceipt]:
    _validate_canonical_source_projection(run_spec.source_binding)
    if run_spec.transform_semantics_digest != S4_ASSET_INCREMENTAL_TRANSFORM_SEMANTICS_DIGEST:
        raise S4AssetIncrementalError("S4 transform semantics differ from the runner")
    if dict(run_spec.parquet_writer_policy) != dict(S4_ASSET_INCREMENTAL_PARQUET_WRITER_POLICY):
        raise S4AssetIncrementalError("S4 Parquet writer policy differs from the runner")
    expected_contracts = {
        table: contract.contract_id for table, contract in ASSET_CONTRACTS.items()
    }
    expected_schemas = {
        table: arrow_schema_digest(contract.arrow_schema)
        for table, contract in ASSET_CONTRACTS.items()
    }
    if (
        dict(run_spec.contract_ids_by_table) != expected_contracts
        or dict(run_spec.schema_digests_by_table) != expected_schemas
    ):
        raise S4AssetIncrementalError("S4 table contracts differ from the runner")
    _verify_pin(root, run_spec.calendar_artifact)
    calendar = load_xnys_calendar_artifact(
        root,
        calendar_artifact_id=run_spec.calendar_artifact_id,
        expected_sha256=run_spec.calendar_artifact.sha256,
    )
    if _pin_existing(root, calendar.relative_path) != run_spec.calendar_artifact:
        raise S4AssetIncrementalError("calendar artifact pin differs from its canonical path")
    parent = _load_parent_frontier(root, run_spec.parent_frontier)
    _require_next_session(
        calendar,
        run_spec.parent_frontier.terminal_session,
        run_spec.source_binding.session_date,
    )
    _require_availability_sessions(
        calendar,
        parent_available_session=_parent_available_session(parent),
        receipt_available_session=run_spec.receipt_available_session,
    )
    if isinstance(parent, S4BaseFrontier):
        _require_base_compatibility(
            root,
            parent,
            calendar_artifact_id=run_spec.calendar_artifact_id,
            calendar_artifact_sha256=run_spec.calendar_artifact.sha256,
            reference_binding=run_spec.reference_binding,
        )
    else:
        parent_spec = _load_run_spec_from_pin(root, parent.run_spec_artifact)
        _require_clean_compatibility(parent_spec, run_spec)
    verified_reference = _load_reference_binding_from_exact_releases(
        root,
        run_spec.reference_binding.dependency_pins,
    )
    if verified_reference != run_spec.reference_binding:
        raise S4AssetIncrementalError(
            "S4 reference values differ from their exact published releases"
        )
    if run_spec.receipt_available_session < max(
        run_spec.source_binding.pair_available_session,
        _parent_available_session(parent),
    ):
        raise S4AssetIncrementalError("receipt availability precedes its source or parent frontier")
    return calendar, parent


def _validate_canonical_source_projection(binding: S4SessionSourceBinding) -> None:
    canonical_manifest_paths = canonical_asset_session_manifest_paths(binding.session_date)
    canonical_request_ids = tuple(Path(path).stem for path in canonical_manifest_paths)
    if (
        binding.active_request_id,
        binding.inactive_request_id,
    ) != canonical_request_ids or frozenset(
        item.path for item in binding.inventory.upstream_manifests
    ) != frozenset(canonical_manifest_paths):
        raise S4AssetIncrementalError(
            "S4 source binding is not the canonical active/inactive request pair"
        )
    expected_page_parents = {
        f"bronze/massive/assets/request_id={request_id}" for request_id in canonical_request_ids
    }
    if any(
        Path(item.path).parent.as_posix() not in expected_page_parents
        for item in binding.inventory.artifacts
    ):
        raise S4AssetIncrementalError("S4 source inventory contains noncanonical page paths")


def _reauthenticate_completed_source_manifests(
    root: Path,
    binding: S4SessionSourceBinding,
    calendar: XNYSCalendarArtifact,
) -> None:
    """Recheck two small manifest files while intentionally skipping Bronze pages."""

    try:
        reader = read_asset_source_inventory(root, binding.inventory)
        _validate_reader_against_binding(reader, binding, calendar)
    except AssetSourceError as exc:
        raise S4AssetIncrementalError(
            "correction_required: completed session source manifests changed"
        ) from exc


def _require_base_compatibility(
    root: Path,
    base: S4BaseFrontier,
    *,
    calendar_artifact_id: str,
    calendar_artifact_sha256: str,
    reference_binding: S4ReferenceBinding,
) -> None:
    derived = derive_s4_base_frontier(
        root,
        release_set_artifact=base.base_release_set_artifact,
        calendar_artifact_id=calendar_artifact_id,
        calendar_artifact_sha256=calendar_artifact_sha256,
        reference_binding=reference_binding,
    )
    if derived != base:
        raise S4AssetIncrementalError(
            "base frontier is not clean-append compatible with the S4 incremental runner"
        )


def _require_clean_compatibility(
    parent: S4SessionRunSpec,
    child: S4SessionRunSpec,
) -> None:
    if (
        parent.calendar_artifact_id != child.calendar_artifact_id
        or parent.calendar_artifact != child.calendar_artifact
        or parent.reference_binding.binding_id != child.reference_binding.binding_id
        or parent.transform_semantics_digest != child.transform_semantics_digest
        or dict(parent.contract_ids_by_table) != dict(child.contract_ids_by_table)
        or dict(parent.schema_digests_by_table) != dict(child.schema_digests_by_table)
        or dict(parent.parquet_writer_policy) != dict(child.parquet_writer_policy)
    ):
        raise S4AssetIncrementalError(
            "clean append compatibility changed; a new base or reviewed correction is required"
        )


def _load_parent_frontier(
    root: Path,
    pin: S4ParentFrontierPin,
) -> S4BaseFrontier | S4SessionRunReceipt:
    content = _read_exact_pin(root, pin.artifact)
    document = _canonical_json_document(content, "S4 parent frontier")
    if pin.parent_kind is S4ParentKind.BASE_RELEASE:
        parent: S4BaseFrontier | S4SessionRunReceipt = S4BaseFrontier.from_dict(document)
        parent_id = parent.frontier_id
        terminal_session = parent.terminal_session
        expected_path = (
            "manifests/silver/incremental/s4/assets/base-frontiers/"
            f"frontier_id={parent.frontier_id}/manifest.json"
        )
    else:
        parent = S4SessionRunReceipt.from_dict(document)
        parent_id = parent.receipt_id
        terminal_session = parent.session_date
        expected_path = _receipt_relative_path(parent.session_date)
        parent_spec = _load_run_spec_from_pin(root, parent.run_spec_artifact)
        _validate_receipt_against_spec(
            parent,
            parent_spec,
            parent.run_spec_artifact,
        )
    if (
        parent_id != pin.terminal_receipt_id
        or terminal_session != pin.terminal_session
        or pin.artifact.path != expected_path
    ):
        raise S4AssetIncrementalError("parent frontier facts differ from exact bytes")
    return parent


def _parent_available_session(parent: S4BaseFrontier | S4SessionRunReceipt) -> date:
    if isinstance(parent, S4BaseFrontier):
        return parent.release_available_session
    return parent.receipt_available_session


def _load_run_spec_from_pin(root: Path, pin: ArtifactPin) -> S4SessionRunSpec:
    document = _canonical_json_document(_read_exact_pin(root, pin), "S4 parent run spec")
    run_spec = S4SessionRunSpec.from_dict(document)
    if run_spec.run_spec_id != document.get("run_spec_id"):
        raise S4AssetIncrementalError("parent run-spec ID changed")
    return run_spec


def _validate_reader_against_binding(
    reader: AssetSourceReader,
    binding: S4SessionSourceBinding,
    calendar: XNYSCalendarArtifact,
) -> None:
    session = _single_source_session(reader, binding.session_date)
    canonical_manifest_paths = canonical_asset_session_manifest_paths(binding.session_date)
    canonical_request_ids = tuple(Path(path).stem for path in canonical_manifest_paths)
    expected_request_ids = (
        session.active_request.source_request_id,
        session.inactive_request.source_request_id,
    )
    if (
        reader.page_count != binding.page_count
        or reader.declared_row_count != binding.declared_row_count
        or expected_request_ids != canonical_request_ids
        or expected_request_ids != (binding.active_request_id, binding.inactive_request_id)
        or frozenset(item.path for item in binding.inventory.upstream_manifests)
        != frozenset(canonical_manifest_paths)
        or tuple(item.source_manifest_path for item in session.requests) != canonical_manifest_paths
        or session.capture_completed_at_utc != binding.pair_capture_completed_at_utc
    ):
        raise S4AssetIncrementalError("S4 source reader differs from its run-spec binding")
    available, _ = calendar.first_open_after(session.capture_completed_at_utc)
    if available != binding.pair_available_session:
        raise S4AssetIncrementalError("S4 source availability changed from its run spec")


def _load_reference_binding_from_exact_releases(
    root: Path,
    dependency_pins: tuple[ArtifactPin, ...],
) -> S4ReferenceBinding:
    """Rebuild S1/S2 values from the two exact published release chains."""

    if len(dependency_pins) != 2:
        raise S4AssetIncrementalError("S4 reference binding requires two exact releases")
    reader = PublishedSilverReader(root)
    store = SilverStore(root)
    values: dict[str, tuple[str, ...]] = {}
    for pin in dependency_pins:
        _verify_pin(root, pin)
        relative = Path(pin.path)
        prefix = "release_id="
        if (
            relative.parent.as_posix() != "manifests/silver/releases"
            or not relative.name.startswith(prefix)
            or relative.suffix != ".json"
        ):
            raise S4AssetIncrementalError(
                "S4 reference dependency is not a canonical release manifest"
            )
        release_id = relative.name[len(prefix) : -len(".json")]
        release, stored = store.load_release(release_id)
        if ArtifactPin(path=stored.path, sha256=stored.sha256, bytes=stored.bytes) != pin:
            raise S4AssetIncrementalError("S4 reference release pin changed")
        published = reader.inspect(release.release_id)
        tables = [pq.read_table(path) for path in published.data_paths]
        if not tables:
            raise S4AssetIncrementalError("S4 reference release has no data")
        table = tables[0] if len(tables) == 1 else pa.concat_tables(tables)
        if published.contract == TICKER_TYPE_DIM_CONTRACT:
            role = "ticker_types"
            column = "type_code"
        elif published.contract == EXCHANGE_DIM_CONTRACT:
            role = "exchange_mics"
            column = "mic"
        else:
            raise S4AssetIncrementalError("S4 reference release contract is not S1 or S2")
        if role in values:
            raise S4AssetIncrementalError("S4 reference release role is duplicated")
        values[role] = tuple(sorted({str(item) for item in table.column(column).to_pylist()}))
    if set(values) != {"ticker_types", "exchange_mics"}:
        raise S4AssetIncrementalError("S4 reference releases do not cover S1 and S2")
    return S4ReferenceBinding(
        ticker_types=values["ticker_types"],
        exchange_mics=values["exchange_mics"],
        dependency_pins=dependency_pins,
    )


def _single_source_session(reader: AssetSourceReader, expected: date):
    if len(reader.sessions) != 1 or reader.sessions[0].session_date != expected:
        raise S4AssetIncrementalError("S4 incremental source must contain one target session")
    return reader.sessions[0]


def _require_next_session(
    calendar: XNYSCalendarArtifact,
    parent_session: date,
    target_session: date,
) -> None:
    sessions = tuple(item.session_date for item in calendar.sessions)
    try:
        parent_index = sessions.index(parent_session)
    except ValueError as exc:
        raise S4AssetIncrementalError(
            "parent terminal session is absent from the exact calendar"
        ) from exc
    if parent_index + 1 >= len(sessions) or sessions[parent_index + 1] != target_session:
        raise S4AssetIncrementalError(
            "source_gap: target is not the exact next XNYS session after the parent"
        )


def _require_availability_sessions(
    calendar: XNYSCalendarArtifact,
    *,
    parent_available_session: date,
    receipt_available_session: date,
) -> None:
    sessions = {item.session_date for item in calendar.sessions}
    if parent_available_session not in sessions or receipt_available_session not in sessions:
        raise S4AssetIncrementalError(
            "parent and receipt availability must be sessions in the exact calendar"
        )


def _validate_transform_results(
    results: Mapping[str, AssetTableTransformResult],
    run_spec: S4SessionRunSpec,
) -> None:
    if tuple(sorted(results)) != _TABLES:
        raise S4AssetIncrementalError("S4 transform did not return exactly three tables")
    for table in _TABLES:
        result = results[table]
        contract = ASSET_CONTRACTS[table]
        if (
            result.contract != contract
            or result.table.schema != contract.arrow_schema
            or arrow_schema_digest(result.table.schema) != run_spec.schema_digests_by_table[table]
        ):
            raise S4AssetIncrementalError(f"S4 transform schema changed for {table}")
        if result.row_funnel.input_rows != run_spec.source_binding.declared_row_count:
            raise S4AssetIncrementalError(f"S4 row funnel changed for {table}")
        if result.row_funnel.output_rows_by_table != {table: result.table.num_rows}:
            raise S4AssetIncrementalError(f"S4 row-funnel output count changed for {table}")
        if result.quarantine_records:
            raise S4AssetIncrementalError(
                f"S4 session has quarantine rows for {table}; no final receipt was written"
            )
        blocking = tuple(item.check_id for item in result.qa_checks if item.blocks_publish)
        if blocking:
            raise S4AssetIncrementalError(f"S4 session has blocking QA for {table}: {blocking}")


def _results_by_table(
    transformed: AssetTransformResult,
) -> Mapping[str, AssetTableTransformResult]:
    return MappingProxyType(
        {
            transformed.observation.contract.table: transformed.observation,
            transformed.version.contract.table: transformed.version,
            transformed.universe.contract.table: transformed.universe,
        }
    )


def _table_qa_projection(result: AssetTableTransformResult) -> dict[str, object]:
    checks = tuple(sorted(result.qa_checks, key=lambda item: item.check_id))
    checks_document = [item.to_dict() for item in checks]
    row_funnel_document = result.row_funnel.to_dict()
    deferred = sorted(
        item.check_id for item in checks if item.check_id in _DEFERRED_FULL_HISTORY_CHECKS
    )
    return {
        "checks": checks_document,
        "deferred_full_history_check_ids": deferred,
        "qa_result_set_digest": stable_digest(checks_document),
        "row_funnel": row_funnel_document,
        "row_funnel_digest": stable_digest(row_funnel_document),
        "scope": "session_local",
    }


def _combined_qa_digest(
    partitions: list[S4SessionPartitionReceipt],
) -> str:
    return stable_digest(
        {
            "table_qa_result_set_digests": {
                item.table_name: item.qa_result_set_digest for item in partitions
            }
        }
    )


def _write_partition(
    root: Path,
    run_spec: S4SessionRunSpec,
    *,
    table: str,
    result: AssetTableTransformResult,
) -> ArtifactPin:
    relative = _partition_relative_path(run_spec, table)
    sink = pa.BufferOutputStream()
    pq.write_table(
        result.table,
        sink,
        compression="zstd",
        version="2.6",
        write_statistics=True,
    )
    stored = write_bytes_immutable(
        root,
        safe_relative_path(root, relative),
        sink.getvalue().to_pybytes(),
        temporary_directory=root / "tmp" / "silver-asset-incremental-immutable-writes",
    )
    return _artifact_pin(stored)


def _write_qa_details(
    root: Path,
    run_spec: S4SessionRunSpec,
    *,
    table_qa: Mapping[str, Mapping[str, object]],
    qa_result_set_digest: str,
) -> ArtifactPin:
    body: dict[str, object] = {
        "artifact_type": "s4_assets_session_qa_details",
        "qa_result_set_digest": qa_result_set_digest,
        "rule_version": "s4_assets_session_qa_details_v1",
        "run_spec_id": run_spec.run_spec_id,
        "session_date": run_spec.source_binding.session_date.isoformat(),
        "source_binding_id": run_spec.source_binding.source_binding_id,
        "tables": {table: dict(table_qa[table]) for table in _TABLES},
    }
    document = {"qa_details_id": stable_digest(body), **body}
    stored = write_bytes_immutable(
        root,
        safe_relative_path(root, _qa_relative_path(run_spec)),
        _canonical_json_bytes(document),
        temporary_directory=root / "tmp" / "silver-asset-incremental-immutable-writes",
    )
    return _artifact_pin(stored)


def _write_or_reuse_run_spec(
    root: Path,
    run_spec: S4SessionRunSpec,
) -> tuple[S4SessionRunSpec, ArtifactPin]:
    relative = _run_spec_relative_path(run_spec.source_binding.session_date)
    path = safe_relative_path(root, relative)
    if path.exists():
        artifact = _pin_existing(root, relative)
        stored = _load_run_spec_from_pin(root, artifact)
        if stored.run_spec_id != run_spec.run_spec_id:
            raise S4AssetIncrementalError(
                "correction_required: the session already has a different run spec"
            )
        return stored, artifact
    stored = write_bytes_immutable(
        root,
        path,
        _canonical_json_bytes(run_spec.to_dict()),
        temporary_directory=root / "tmp" / "silver-asset-incremental-immutable-writes",
    )
    return run_spec, _artifact_pin(stored)


def _write_receipt(root: Path, receipt: S4SessionRunReceipt) -> ArtifactPin:
    stored = write_bytes_immutable(
        root,
        safe_relative_path(root, _receipt_relative_path(receipt.session_date)),
        _canonical_json_bytes(receipt.to_dict()),
        temporary_directory=root / "tmp" / "silver-asset-incremental-immutable-writes",
    )
    return _artifact_pin(stored)


def _load_receipt(
    root: Path,
    relative_path: str,
) -> tuple[S4SessionRunReceipt, ArtifactPin]:
    artifact = _pin_existing(root, relative_path)
    document = _canonical_json_document(
        _read_exact_pin(root, artifact),
        "S4 session receipt",
    )
    return S4SessionRunReceipt.from_dict(document), artifact


def _validate_receipt_against_spec(
    receipt: S4SessionRunReceipt,
    run_spec: S4SessionRunSpec,
    run_spec_artifact: ArtifactPin,
) -> None:
    if (
        receipt.run_spec_id != run_spec.run_spec_id
        or receipt.run_spec_artifact != run_spec_artifact
        or run_spec_artifact.path != _run_spec_relative_path(receipt.session_date)
        or receipt.parent_frontier_id != run_spec.parent_frontier.parent_frontier_id
        or receipt.session_date != run_spec.source_binding.session_date
        or receipt.source_binding_id != run_spec.source_binding.source_binding_id
        or receipt.pair_available_session != run_spec.source_binding.pair_available_session
        or receipt.receipt_available_session != run_spec.receipt_available_session
    ):
        raise S4AssetIncrementalError(
            "correction_required: existing receipt differs from the requested clean append"
        )
    for partition in receipt.partition_receipts:
        table = partition.table_name
        if (
            partition.contract_id != run_spec.contract_ids_by_table[table]
            or partition.schema_digest != run_spec.schema_digests_by_table[table]
            or partition.artifact.path != _partition_relative_path(run_spec, table)
        ):
            raise S4AssetIncrementalError("existing S4 partition receipt changed")
    if receipt.qa_details_artifact.path != _qa_relative_path(run_spec):
        raise S4AssetIncrementalError("existing S4 QA receipt path changed")
    if receipt.qa_result_set_digest != _combined_qa_digest(list(receipt.partition_receipts)):
        raise S4AssetIncrementalError("existing S4 QA result-set digest changed")


def _verify_receipt_artifacts(root: Path, receipt: S4SessionRunReceipt) -> None:
    _verify_pin(root, receipt.run_spec_artifact)
    _verify_pin(root, receipt.qa_details_artifact)
    for partition in receipt.partition_receipts:
        _verify_pin(root, partition.artifact)


def _run_prefix(run_spec: S4SessionRunSpec, table: str) -> str:
    contract = ASSET_CONTRACTS[table]
    return (
        f"silver/schema=v{contract.schema_version}/{contract.domain}/{table}/"
        f"build_id={run_spec.run_spec_id}"
    )


def _partition_relative_path(run_spec: S4SessionRunSpec, table: str) -> str:
    session = run_spec.source_binding.session_date
    return (
        f"{_run_prefix(run_spec, table)}/data/session_year={session.year}/"
        f"session_date={session.isoformat()}/part-00000.parquet"
    )


def _control_prefix(session: date) -> str:
    return (
        "manifests/silver/incremental/s4/assets/"
        f"session_year={session.year}/session_date={session.isoformat()}"
    )


def _run_spec_relative_path(session: date) -> str:
    return f"{_control_prefix(session)}/run-spec.json"


def _receipt_relative_path(session: date) -> str:
    return f"{_control_prefix(session)}/run-receipt.json"


def _qa_relative_path(run_spec: S4SessionRunSpec) -> str:
    return (
        f"{_control_prefix(run_spec.source_binding.session_date)}/"
        f"run_spec_id={run_spec.run_spec_id}/qa-details.json"
    )


def _lock_relative_path(session: date) -> str:
    return f"manifests/silver/locks/s4-assets-incremental-{session.isoformat()}.lock"


def _release_set_id_from_artifact(artifact: ArtifactPin) -> str:
    prefix = "manifests/silver/release-sets/assets/release_set_id="
    suffix = "/manifest.json"
    if not artifact.path.startswith(prefix) or not artifact.path.endswith(suffix):
        raise S4AssetIncrementalError("S4 base release-set artifact path is noncanonical")
    release_set_id = artifact.path[len(prefix) : -len(suffix)]
    if len(release_set_id) != 64 or any(
        character not in "0123456789abcdef" for character in release_set_id
    ):
        raise S4AssetIncrementalError("S4 base release-set artifact ID is invalid")
    return release_set_id


def _date_parameter(value: object, label: str) -> date:
    if not isinstance(value, str):
        raise S4AssetIncrementalError(f"S4 base {label} is not an ISO date")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise S4AssetIncrementalError(f"S4 base {label} is not an ISO date") from exc
    if parsed.isoformat() != value:
        raise S4AssetIncrementalError(f"S4 base {label} is not a canonical ISO date")
    return parsed


def _full_plan_reference_pin(
    parameters: Mapping[str, object],
    role: str,
) -> tuple[str, str]:
    release_id = parameters.get(f"{role}_release_id")
    release_sha256 = parameters.get(f"{role}_release_sha256")
    if (
        not isinstance(release_id, str)
        or len(release_id) != 64
        or any(character not in "0123456789abcdef" for character in release_id)
        or not isinstance(release_sha256, str)
        or len(release_sha256) != 64
        or any(character not in "0123456789abcdef" for character in release_sha256)
    ):
        raise S4AssetIncrementalError(f"S4 base {role} release pin is invalid")
    return (
        f"manifests/silver/releases/release_id={release_id}.json",
        release_sha256,
    )


def _pin_existing(root: Path, relative_path: str) -> ArtifactPin:
    path = safe_relative_path(root, relative_path)
    if not path.is_file() or path.is_symlink():
        raise S4AssetIncrementalError(f"exact artifact is missing: {relative_path}")
    return ArtifactPin(
        path=relative_path,
        sha256=sha256_file(path),
        bytes=path.stat().st_size,
    )


def _verify_pin(root: Path, pin: ArtifactPin) -> None:
    path = safe_relative_path(root, pin.path)
    if not path.is_file() or path.is_symlink():
        raise S4AssetIncrementalError(f"exact artifact is missing: {pin.path}")
    if path.stat().st_size != pin.bytes or sha256_file(path) != pin.sha256:
        raise S4AssetIncrementalError(f"exact artifact pin mismatch: {pin.path}")


def _read_exact_pin(root: Path, pin: ArtifactPin) -> bytes:
    _verify_pin(root, pin)
    try:
        return safe_relative_path(root, pin.path).read_bytes()
    except OSError as exc:
        raise S4AssetIncrementalError(f"cannot read exact artifact: {pin.path}") from exc


def _artifact_pin(stored: Mapping[str, object]) -> ArtifactPin:
    return ArtifactPin(
        path=str(stored["path"]),
        sha256=str(stored["sha256"]),
        bytes=int(stored["bytes"]),
    )


def _canonical_json_bytes(value: object) -> bytes:
    return (
        json.dumps(
            value,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        + b"\n"
    )


def _canonical_json_document(content: bytes, label: str) -> dict[str, object]:
    try:
        value = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise S4AssetIncrementalError(f"{label} is not valid JSON") from exc
    if not isinstance(value, dict) or _canonical_json_bytes(value) != content:
        raise S4AssetIncrementalError(f"{label} bytes are not canonical")
    return value


@contextmanager
def _exclusive_lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    flags = os.O_RDWR | os.O_CREAT
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags, 0o600)
    try:
        file_stat = os.fstat(descriptor)
        if not stat.S_ISREG(file_stat.st_mode) or file_stat.st_nlink != 1:
            raise S4AssetIncrementalError("S4 incremental lock is not a regular file")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise S4AssetIncrementalError(
                "another S4 incremental run holds the session lock"
            ) from exc
        try:
            yield
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
    finally:
        os.close(descriptor)


def _call_barrier(barrier: Callable[[str], None] | None, stage: str) -> None:
    if barrier is not None:
        barrier(stage)


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
        raise S4AssetIncrementalError(f"cannot verify S4 incremental Git checkout: {detail}")
    return completed.stdout.strip()


__all__ = [
    "S4_ASSET_INCREMENTAL_PARQUET_WRITER_POLICY",
    "S4_ASSET_INCREMENTAL_POLICY_VERSION",
    "S4_ASSET_INCREMENTAL_TRANSFORM_SEMANTICS_DIGEST",
    "S4_BASE_TERMINAL_PARTITION_SET_RULE_VERSION",
    "S4AssetIncrementalError",
    "S4AssetIncrementalRun",
    "derive_s4_base_frontier",
    "load_completed_s4_asset_session_run",
    "load_current_s4_reference_binding",
    "parent_frontier_from_session_receipt",
    "prepare_s4_asset_session_run_spec",
    "run_s4_asset_session_incremental",
    "verify_s4_incremental_git_checkout",
    "write_s4_base_frontier",
]
