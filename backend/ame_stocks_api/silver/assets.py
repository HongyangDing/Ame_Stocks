"""Pure, session-bounded S4 Assets transformation.

The real ten-year build is intentionally not implemented here.  This module consumes one
already verified active/inactive :class:`AssetSourceSession` at a time, keeps every accepted
source occurrence in the observation table, projects only multi-version groups, and creates a
source universe only when the frozen selection evidence is resolved.
"""

from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from functools import lru_cache
from zoneinfo import ZoneInfo

import exchange_calendars as xcals
import pandas as pd
import pyarrow as pa

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver.asset_contract import (
    ASSET_OBSERVATION_DAILY_CONTRACT,
    ASSET_OBSERVATION_VERSION_CONTRACT,
    UNIVERSE_SOURCE_DAILY_CONTRACT,
)
from ame_stocks_api.silver.asset_source import AssetSourceRecord, AssetSourceSession
from ame_stocks_api.silver.contracts import (
    QACheckResult,
    QASeverity,
    QuarantineRecord,
    QuarantineReviewStatus,
    RowFunnel,
    SilverContractError,
    TableContract,
)

ASSET_TRANSFORM_VERSION = "s4-assets-v1.0.0"
ASSET_VERSION_SELECTION_RULE = "s4_asset_source_version_selection_v1"
ASSET_REFERENCE_TIME_SCOPE = "provider_historical_date_membership_snapshot_v1"
ASSET_METADATA_TIME_SCOPE = "metadata_as_returned_at_source_capture_not_historical_vintage_v1"
ASSET_SOURCE_AVAILABILITY_RULE = "first_xnys_open_after_source_capture_v1"
UNIVERSE_SOURCE_AVAILABILITY_RULE = "first_xnys_open_after_complete_active_inactive_pair_v1"
ASSET_SOURCE_AVAILABILITY_QUALITY = "reconstructed_historical_snapshot_without_archived_vintage"

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_UTC_TIMESTAMP = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})$"
)
_NEW_YORK = ZoneInfo("America/New_York")
_PROVIDER_FIELDS = (
    "ticker",
    "name",
    "market",
    "locale",
    "primary_exchange",
    "type",
    "active",
    "currency_name",
    "cik",
    "composite_figi",
    "share_class_figi",
    "last_updated_utc",
    "delisted_utc",
)
_PROVIDER_FIELD_SET = frozenset(_PROVIDER_FIELDS)
_OPTIONAL_STRING_FIELDS = tuple(
    field for field in _PROVIDER_FIELDS if field not in {"ticker", "active"}
)
_IDENTITY_FIELDS = (
    "active",
    "ticker",
    "type",
    "name",
    "market",
    "locale",
    "primary_exchange",
    "currency_name",
    "cik",
    "composite_figi",
    "share_class_figi",
)
_ALLOWED_DIFFERENCE_FIELDS = frozenset(
    {
        (),
        ("last_updated_utc",),
        ("delisted_utc", "last_updated_utc"),
    }
)
_RESOLVED_STATUSES = frozenset({"resolved_exact_duplicate", "resolved_unique_latest_last_updated"})
_UNRESOLVED_STATUSES = frozenset(
    {
        "unresolved_identity_conflict",
        "unresolved_timestamp_missing_or_invalid",
        "unresolved_timestamp_tie",
        "unresolved_difference_set",
    }
)


class AssetTransformError(SilverContractError):
    """Raised when the pure S4 transform cannot honor the approved contracts."""


@dataclass(frozen=True, slots=True)
class AssetTableTransformResult:
    contract: TableContract
    table: pa.Table
    qa_checks: tuple[QACheckResult, ...]
    quarantine_records: tuple[QuarantineRecord, ...]
    row_funnel: RowFunnel

    def __post_init__(self) -> None:
        if self.table.schema != self.contract.arrow_schema:
            raise AssetTransformError(f"{self.contract.table} output differs from approved schema")
        if {item.check_id for item in self.qa_checks} != set(self.contract.required_qa_checks):
            raise AssetTransformError(f"{self.contract.table} QA differs from approved contract")
        if any(item.table != self.contract.table for item in self.qa_checks):
            raise AssetTransformError(f"{self.contract.table} QA has a wrong table identity")
        quarantines = tuple(self.quarantine_records)
        if any(item.table_name != self.contract.table for item in quarantines):
            raise AssetTransformError(
                f"{self.contract.table} quarantine has a wrong table identity"
            )
        if len({item.source_record_id for item in quarantines}) != (
            self.row_funnel.quarantined_source_rows
        ):
            raise AssetTransformError(f"{self.contract.table} quarantine differs from its funnel")
        object.__setattr__(self, "quarantine_records", quarantines)

    def qa_by_id(self, check_id: str) -> QACheckResult:
        for item in self.qa_checks:
            if item.check_id == check_id:
                return item
        raise KeyError(check_id)


@dataclass(frozen=True, slots=True)
class AssetTransformResult:
    observation: AssetTableTransformResult
    version: AssetTableTransformResult
    universe: AssetTableTransformResult

    @property
    def quarantine_records(self) -> tuple[QuarantineRecord, ...]:
        """Backward-compatible observation quarantine view."""

        return self.observation.quarantine_records

    @property
    def all_quarantine_records(self) -> tuple[QuarantineRecord, ...]:
        return tuple(
            item
            for result in (self.observation, self.version, self.universe)
            for item in result.quarantine_records
        )

    @property
    def blocks_publish(self) -> bool:
        return any(
            check.blocks_publish
            for result in (self.observation, self.version, self.universe)
            for check in result.qa_checks
        )


@dataclass(frozen=True, slots=True)
class _Context:
    source: AssetSourceRecord
    raw: Mapping[str, object]
    source_row_hash: str
    source_record_id: str
    delisted_at_utc: datetime | None
    last_updated_at_utc: datetime | None
    source_available_session: date
    source_available_at_utc: datetime

    @property
    def source_pointer(self) -> str:
        return (
            f"{self.source.source_artifact_path}#page={self.source.source_page_sequence}"
            f"&row={self.source.source_row_ordinal}"
        )


@dataclass(frozen=True, slots=True)
class _Decision:
    contexts: tuple[_Context, ...]
    version_group_id: str | None
    difference_fields: tuple[str, ...]
    status: str
    selected_source_record_id: str | None
    ranks: Mapping[str, int | None]
    reasons: Mapping[str, str]

    @property
    def selected(self) -> _Context | None:
        if self.selected_source_record_id is None:
            return None
        for context in self.contexts:
            if context.source_record_id == self.selected_source_record_id:
                return context
        raise AssetTransformError("selection points outside its version group")


def transform_asset_session(
    session: AssetSourceSession,
    records: Iterable[AssetSourceRecord],
    *,
    build_id: str,
    calendar_name: str = "XNYS",
    current_ticker_types: Iterable[str] | None = None,
    current_exchange_mics: Iterable[str] | None = None,
) -> AssetTransformResult:
    """Transform one complete, verified session pair without performing any I/O."""

    if not isinstance(build_id, str) or not _SHA256.fullmatch(build_id):
        raise AssetTransformError("asset transform build_id must be a lowercase SHA-256")
    source_records = tuple(records)
    contexts = tuple(_context(record, calendar_name=calendar_name) for record in source_records)
    quarantine = _quarantine_required_invalid(contexts, session=session, build_id=build_id)
    quarantined_ids = {item.source_record_id for item in quarantine}
    accepted = tuple(item for item in contexts if item.source_record_id not in quarantined_ids)

    groups: dict[tuple[bool, str], list[_Context]] = defaultdict(list)
    for context in accepted:
        ticker = context.raw.get("ticker")
        if isinstance(ticker, str):
            groups[(context.source.requested_active, ticker)].append(context)
    decisions = tuple(
        _select_group(session.session_date, requested_active, ticker, tuple(group))
        for (requested_active, ticker), group in sorted(groups.items())
    )

    observation_rows = sorted(
        (_observation_row(item) for item in accepted),
        key=lambda row: (
            row["session_date"],
            row["ticker"],
            row["requested_active"],
            row["source_page_sequence"],
            row["source_row_ordinal"],
            row["source_record_id"],
        ),
    )
    version_rows = sorted(
        (
            row
            for decision in decisions
            if len(decision.contexts) > 1
            for row in _version_rows(decision)
        ),
        key=lambda row: (
            row["session_date"],
            row["ticker"],
            row["requested_active"],
            row["version_group_id"],
            row["selection_rank"] is None,
            0 if row["selection_rank"] is None else row["selection_rank"],
            row["source_record_id"],
        ),
    )
    active_tickers = {
        str(item.raw["ticker"])
        for item in accepted
        if item.source.requested_active and isinstance(item.raw.get("ticker"), str)
    }
    inactive_tickers = {
        str(item.raw["ticker"])
        for item in accepted
        if not item.source.requested_active and isinstance(item.raw.get("ticker"), str)
    }
    overlap = active_tickers & inactive_tickers
    universe_rows = sorted(
        (
            _universe_row(session, decision, calendar_name=calendar_name)
            for decision in decisions
            if decision.selected is not None
            and str(decision.selected.raw["ticker"]) not in overlap
            and str(decision.selected.raw["ticker"]) == str(decision.selected.raw["ticker"]).strip()
        ),
        key=lambda row: (row["session_date"], row["ticker"]),
    )

    observation_table = pa.Table.from_pylist(
        observation_rows, schema=ASSET_OBSERVATION_DAILY_CONTRACT.arrow_schema
    )
    version_table = pa.Table.from_pylist(
        version_rows, schema=ASSET_OBSERVATION_VERSION_CONTRACT.arrow_schema
    )
    universe_table = pa.Table.from_pylist(
        universe_rows, schema=UNIVERSE_SOURCE_DAILY_CONTRACT.arrow_schema
    )
    observation_funnel = RowFunnel(
        input_rows=len(contexts),
        accepted_source_rows=len(accepted),
        exact_duplicate_excess=0,
        quarantined_source_rows=len(quarantined_ids),
        unmapped_source_rows=0,
        version_preserved_rows=len(version_rows),
        output_rows_by_table={
            ASSET_OBSERVATION_DAILY_CONTRACT.table: observation_table.num_rows,
        },
    )
    version_funnel = RowFunnel(
        input_rows=len(contexts),
        accepted_source_rows=len(accepted),
        exact_duplicate_excess=0,
        quarantined_source_rows=len(quarantined_ids),
        unmapped_source_rows=len(accepted) - len(version_rows),
        version_preserved_rows=len(version_rows),
        output_rows_by_table={
            ASSET_OBSERVATION_VERSION_CONTRACT.table: version_table.num_rows,
        },
    )
    universe_funnel = RowFunnel(
        input_rows=len(contexts),
        accepted_source_rows=len(accepted),
        exact_duplicate_excess=0,
        quarantined_source_rows=len(quarantined_ids),
        unmapped_source_rows=len(accepted) - len(universe_rows),
        version_preserved_rows=len(version_rows),
        output_rows_by_table={
            UNIVERSE_SOURCE_DAILY_CONTRACT.table: universe_table.num_rows,
        },
    )

    type_dictionary = None if current_ticker_types is None else frozenset(current_ticker_types)
    exchange_dictionary = (
        None if current_exchange_mics is None else frozenset(current_exchange_mics)
    )
    observation_metrics = _observation_metrics(
        session,
        contexts,
        accepted,
        observation_rows,
        decisions,
        observation_funnel,
        overlap,
        calendar_name=calendar_name,
        current_ticker_types=type_dictionary,
        current_exchange_mics=exchange_dictionary,
    )
    version_metrics = _version_metrics(
        accepted,
        decisions,
        observation_rows,
        version_rows,
        version_funnel,
        source_input_rows=len(contexts),
        quarantined_source_rows=len(quarantined_ids),
    )
    universe_metrics = _universe_metrics(
        session,
        decisions,
        observation_rows,
        version_rows,
        universe_rows,
        overlap,
        universe_funnel,
        calendar_name=calendar_name,
        current_ticker_types=type_dictionary,
        current_exchange_mics=exchange_dictionary,
        source_input_rows=len(contexts),
        quarantined_source_rows=len(quarantined_ids),
    )
    partition_key = session.session_date.isoformat()
    quarantine_by_table = {
        contract.table: tuple(
            replace(item, table_name=contract.table)
            for item in sorted(quarantine, key=lambda record: record.issue_id)
        )
        for contract in (
            ASSET_OBSERVATION_DAILY_CONTRACT,
            ASSET_OBSERVATION_VERSION_CONTRACT,
            UNIVERSE_SOURCE_DAILY_CONTRACT,
        )
    }
    return AssetTransformResult(
        observation=AssetTableTransformResult(
            contract=ASSET_OBSERVATION_DAILY_CONTRACT,
            table=observation_table,
            qa_checks=_qa_results(
                ASSET_OBSERVATION_DAILY_CONTRACT,
                observation_metrics,
                partition_key=partition_key,
            ),
            quarantine_records=quarantine_by_table[ASSET_OBSERVATION_DAILY_CONTRACT.table],
            row_funnel=observation_funnel,
        ),
        version=AssetTableTransformResult(
            contract=ASSET_OBSERVATION_VERSION_CONTRACT,
            table=version_table,
            qa_checks=_qa_results(
                ASSET_OBSERVATION_VERSION_CONTRACT,
                version_metrics,
                partition_key=partition_key,
            ),
            quarantine_records=quarantine_by_table[ASSET_OBSERVATION_VERSION_CONTRACT.table],
            row_funnel=version_funnel,
        ),
        universe=AssetTableTransformResult(
            contract=UNIVERSE_SOURCE_DAILY_CONTRACT,
            table=universe_table,
            qa_checks=_qa_results(
                UNIVERSE_SOURCE_DAILY_CONTRACT,
                universe_metrics,
                partition_key=partition_key,
            ),
            quarantine_records=quarantine_by_table[UNIVERSE_SOURCE_DAILY_CONTRACT.table],
            row_funnel=universe_funnel,
        ),
    )


def _context(record: AssetSourceRecord, *, calendar_name: str) -> _Context:
    raw = dict(record.row)
    row_hash = stable_digest(raw)
    record_id = stable_digest(
        {
            "dataset": "assets",
            "source_artifact_sha256": record.source_artifact_sha256,
            "source_page_sequence": record.source_page_sequence,
            "source_row_ordinal": record.source_row_ordinal,
            "source_request_id": record.source_request_id,
            "source_row_hash": row_hash,
        }
    )
    available_session, available_at = _first_market_open_after(
        record.source_capture_at_utc, calendar_name=calendar_name
    )
    return _Context(
        source=record,
        raw=raw,
        source_row_hash=row_hash,
        source_record_id=record_id,
        delisted_at_utc=_parse_utc(raw.get("delisted_utc")),
        last_updated_at_utc=_parse_utc(raw.get("last_updated_utc")),
        source_available_session=available_session,
        source_available_at_utc=available_at,
    )


def _quarantine_required_invalid(
    contexts: tuple[_Context, ...],
    *,
    session: AssetSourceSession,
    build_id: str,
) -> tuple[QuarantineRecord, ...]:
    records: list[QuarantineRecord] = []
    for context in contexts:
        ticker = context.raw.get("ticker")
        provider_active = context.raw.get("active")
        invalid = (
            context.source.session_date != session.session_date
            or context.source.source_request_id
            not in {
                session.active_request.source_request_id,
                session.inactive_request.source_request_id,
            }
            or not isinstance(ticker, str)
            or not ticker.strip()
            or type(provider_active) is not bool
            or provider_active is not context.source.requested_active
        )
        if not invalid:
            continue
        records.append(
            QuarantineRecord(
                source_record_id=context.source_record_id,
                table_name=ASSET_OBSERVATION_DAILY_CONTRACT.table,
                issue_code="required_field_invalid_rows",
                severity=QASeverity.CRITICAL,
                detected_build_id=build_id,
                source_pointer=context.source_pointer,
                field_name=None,
                observed_value=_bounded_json(
                    {
                        "provider_active": provider_active,
                        "requested_active": context.source.requested_active,
                        "session_date": context.source.session_date.isoformat(),
                        "ticker": ticker,
                    }
                ),
                expected_rule=(
                    "Session/request scope, native active Boolean, and a nonblank string ticker "
                    "must be valid before an observation can enter Silver."
                ),
                review_status=QuarantineReviewStatus.PENDING,
            )
        )
    return tuple(records)


def _observation_row(context: _Context) -> dict[str, object]:
    raw = context.raw
    return {
        "session_year": context.source.session_date.year,
        "session_date": context.source.session_date,
        "requested_active": context.source.requested_active,
        "provider_active": raw["active"],
        "ticker": raw["ticker"],
        "type_code": _string_or_none(raw.get("type")),
        "name": _string_or_none(raw.get("name")),
        "market": _string_or_none(raw.get("market")),
        "locale": _string_or_none(raw.get("locale")),
        "primary_exchange_mic": _string_or_none(raw.get("primary_exchange")),
        "currency_name": _string_or_none(raw.get("currency_name")),
        "cik": _string_or_none(raw.get("cik")),
        "composite_figi": _string_or_none(raw.get("composite_figi")),
        "share_class_figi": _string_or_none(raw.get("share_class_figi")),
        "delisted_utc_raw": _string_or_none(raw.get("delisted_utc")),
        "delisted_at_utc": context.delisted_at_utc,
        "last_updated_utc_raw": _string_or_none(raw.get("last_updated_utc")),
        "last_updated_at_utc": context.last_updated_at_utc,
        "reference_time_scope": ASSET_REFERENCE_TIME_SCOPE,
        "metadata_time_scope": ASSET_METADATA_TIME_SCOPE,
        "source_capture_at_utc": context.source.source_capture_at_utc,
        "source_available_session": context.source_available_session,
        "source_available_at_utc": context.source_available_at_utc,
        "source_availability_rule": ASSET_SOURCE_AVAILABILITY_RULE,
        "source_availability_quality": ASSET_SOURCE_AVAILABILITY_QUALITY,
        "source_record_id": context.source_record_id,
        "source_request_id": context.source.source_request_id,
        "source_provider_request_id": context.source.source_provider_request_id,
        "source_artifact_sha256": context.source.source_artifact_sha256,
        "source_page_sequence": context.source.source_page_sequence,
        "source_row_ordinal": context.source.source_row_ordinal,
        "source_row_hash": context.source_row_hash,
    }


def _select_group(
    session_date: date,
    requested_active: bool,
    ticker: str,
    contexts: tuple[_Context, ...],
) -> _Decision:
    ordered_source = tuple(
        sorted(
            contexts,
            key=lambda item: (
                item.source.source_page_sequence,
                item.source.source_row_ordinal,
                item.source_record_id,
            ),
        )
    )
    if len(ordered_source) == 1:
        only = ordered_source[0]
        return _Decision(
            contexts=ordered_source,
            version_group_id=None,
            difference_fields=(),
            status="singleton",
            selected_source_record_id=only.source_record_id,
            ranks={only.source_record_id: 1},
            reasons={only.source_record_id: "singleton"},
        )

    group_id = stable_digest(
        {
            "requested_active": requested_active,
            "session_date": session_date.isoformat(),
            "ticker": ticker,
        }
    )
    differences = _difference_fields(ordered_source)
    if len({item.source_row_hash for item in ordered_source}) == 1:
        selected = ordered_source[0]
        ranks = {item.source_record_id: index for index, item in enumerate(ordered_source, 1)}
        reasons = {
            item.source_record_id: (
                "selected_min_source_position_exact_duplicate"
                if item is selected
                else "rejected_later_source_position_exact_duplicate"
            )
            for item in ordered_source
        }
        return _Decision(
            contexts=ordered_source,
            version_group_id=group_id,
            difference_fields=differences,
            status="resolved_exact_duplicate",
            selected_source_record_id=selected.source_record_id,
            ranks=ranks,
            reasons=reasons,
        )

    identity_signatures = {_identity_signature(item.raw) for item in ordered_source}
    if len(identity_signatures) != 1:
        return _unresolved_decision(
            ordered_source,
            group_id=group_id,
            differences=differences,
            status="unresolved_identity_conflict",
        )
    if differences not in _ALLOWED_DIFFERENCE_FIELDS:
        return _unresolved_decision(
            ordered_source,
            group_id=group_id,
            differences=differences,
            status="unresolved_difference_set",
        )
    if any(item.last_updated_at_utc is None for item in ordered_source):
        return _unresolved_decision(
            ordered_source,
            group_id=group_id,
            differences=differences,
            status="unresolved_timestamp_missing_or_invalid",
        )
    latest = max(item.last_updated_at_utc for item in ordered_source if item.last_updated_at_utc)
    winners = [item for item in ordered_source if item.last_updated_at_utc == latest]
    if len(winners) != 1:
        return _unresolved_decision(
            ordered_source,
            group_id=group_id,
            differences=differences,
            status="unresolved_timestamp_tie",
        )
    ranked = tuple(
        sorted(
            ordered_source,
            key=lambda item: (
                -_timestamp_ns(item.last_updated_at_utc),
                item.source_record_id,
            ),
        )
    )
    selected = winners[0]
    return _Decision(
        contexts=ordered_source,
        version_group_id=group_id,
        difference_fields=differences,
        status="resolved_unique_latest_last_updated",
        selected_source_record_id=selected.source_record_id,
        ranks={item.source_record_id: index for index, item in enumerate(ranked, 1)},
        reasons={
            item.source_record_id: (
                "selected_unique_max_last_updated"
                if item is selected
                else "rejected_older_last_updated"
            )
            for item in ordered_source
        },
    )


def _unresolved_decision(
    contexts: tuple[_Context, ...],
    *,
    group_id: str,
    differences: tuple[str, ...],
    status: str,
) -> _Decision:
    return _Decision(
        contexts=contexts,
        version_group_id=group_id,
        difference_fields=differences,
        status=status,
        selected_source_record_id=None,
        ranks={item.source_record_id: None for item in contexts},
        reasons={item.source_record_id: status for item in contexts},
    )


def _version_rows(decision: _Decision) -> tuple[dict[str, object], ...]:
    if decision.version_group_id is None or len(decision.contexts) <= 1:
        return ()
    difference_json = json.dumps(
        list(decision.difference_fields), ensure_ascii=False, separators=(",", ":")
    )
    rows: list[dict[str, object]] = []
    for context in decision.contexts:
        rows.append(
            {
                "session_year": context.source.session_date.year,
                "session_date": context.source.session_date,
                "requested_active": context.source.requested_active,
                "ticker": context.raw["ticker"],
                "version_group_id": decision.version_group_id,
                "version_count": len(decision.contexts),
                "source_record_id": context.source_record_id,
                "identity_signature": _identity_signature(context.raw),
                "difference_fields_json": difference_json,
                "last_updated_at_utc": context.last_updated_at_utc,
                "delisted_at_utc": context.delisted_at_utc,
                "selection_rank": decision.ranks[context.source_record_id],
                "is_selected": context.source_record_id == decision.selected_source_record_id,
                "selection_status": decision.status,
                "selection_reason": decision.reasons[context.source_record_id],
                "selection_rule_version": ASSET_VERSION_SELECTION_RULE,
                "selected_source_record_id": decision.selected_source_record_id,
                "source_capture_at_utc": context.source.source_capture_at_utc,
                "source_request_id": context.source.source_request_id,
                "source_provider_request_id": context.source.source_provider_request_id,
                "source_artifact_sha256": context.source.source_artifact_sha256,
                "source_page_sequence": context.source.source_page_sequence,
                "source_row_ordinal": context.source.source_row_ordinal,
                "source_row_hash": context.source_row_hash,
            }
        )
    return tuple(rows)


def _universe_row(
    session: AssetSourceSession,
    decision: _Decision,
    *,
    calendar_name: str,
) -> dict[str, object]:
    selected = decision.selected
    if selected is None:
        raise AssetTransformError("unresolved decision cannot enter source universe")
    raw = selected.raw
    pair_completed = session.capture_completed_at_utc
    available_session, available_at = _first_market_open_after(
        pair_completed, calendar_name=calendar_name
    )
    source_pair_id = stable_digest(
        {
            "active_source_request_id": session.active_request.source_request_id,
            "inactive_source_request_id": session.inactive_request.source_request_id,
            "session_date": session.session_date.isoformat(),
        }
    )
    identity_count = sum(
        isinstance(raw.get(field), str) for field in ("composite_figi", "share_class_figi", "cik")
    )
    identity_status = {
        0: "insufficient_identity_evidence_pending_s7",
        1: "single_identifier_evidence_pending_s7",
        2: "multi_identifier_evidence_pending_s7",
        3: "multi_identifier_evidence_pending_s7",
    }[identity_count]
    return {
        "session_year": session.session_date.year,
        "session_date": session.session_date,
        "ticker": raw["ticker"],
        "active_on_date": raw["active"],
        "type_code": _string_or_none(raw.get("type")),
        "name": _string_or_none(raw.get("name")),
        "market": _string_or_none(raw.get("market")),
        "locale": _string_or_none(raw.get("locale")),
        "primary_exchange_mic": _string_or_none(raw.get("primary_exchange")),
        "currency_name": _string_or_none(raw.get("currency_name")),
        "cik": _string_or_none(raw.get("cik")),
        "composite_figi": _string_or_none(raw.get("composite_figi")),
        "share_class_figi": _string_or_none(raw.get("share_class_figi")),
        "delisted_at_utc": selected.delisted_at_utc,
        "last_updated_at_utc": selected.last_updated_at_utc,
        "identity_link_status": identity_status,
        "selected_source_record_id": selected.source_record_id,
        "version_group_id": decision.version_group_id,
        "source_version_count": len(decision.contexts),
        "selection_status": decision.status,
        "selection_rule_version": ASSET_VERSION_SELECTION_RULE,
        "reference_time_scope": ASSET_REFERENCE_TIME_SCOPE,
        "metadata_time_scope": ASSET_METADATA_TIME_SCOPE,
        "active_source_request_id": session.active_request.source_request_id,
        "inactive_source_request_id": session.inactive_request.source_request_id,
        "source_pair_id": source_pair_id,
        "selected_source_capture_at_utc": selected.source.source_capture_at_utc,
        "universe_capture_completed_at_utc": pair_completed,
        "source_available_session": available_session,
        "source_available_at_utc": available_at,
        "source_availability_rule": UNIVERSE_SOURCE_AVAILABILITY_RULE,
        "source_availability_quality": ASSET_SOURCE_AVAILABILITY_QUALITY,
        "source_request_id": selected.source.source_request_id,
        "source_provider_request_id": selected.source.source_provider_request_id,
        "source_artifact_sha256": selected.source.source_artifact_sha256,
        "source_page_sequence": selected.source.source_page_sequence,
        "source_row_ordinal": selected.source.source_row_ordinal,
        "source_row_hash": selected.source_row_hash,
    }


def _source_record_audit(
    session: AssetSourceSession,
    contexts: tuple[_Context, ...],
) -> dict[str, int]:
    """Reconcile materialized records to the reader-verified session envelope."""

    requests = {
        True: session.active_request,
        False: session.inactive_request,
    }
    page_counts: Counter[tuple[bool, int]] = Counter()
    page_ordinals: defaultdict[tuple[bool, int], list[int]] = defaultdict(list)
    query_date_invalid = 0
    plan_invalid = int(session.declared_row_count != len(contexts))
    integrity_invalid = 0
    envelope_invalid = 0
    for context in contexts:
        source = context.source
        request = requests[source.requested_active]
        if source.session_date != session.session_date:
            query_date_invalid += 1
        if source.source_request_id != request.source_request_id:
            plan_invalid += 1
        if (
            source.source_manifest_path != request.source_manifest_path
            or source.source_manifest_sha256 != request.source_manifest_sha256
            or source.source_created_at_utc != request.source_created_at_utc
            or source.source_capture_at_utc != request.source_capture_at_utc
            or source.source_updated_at_utc != request.source_updated_at_utc
        ):
            integrity_invalid += 1
        sequence = source.source_page_sequence
        if sequence < 0 or sequence >= len(request.pages):
            integrity_invalid += 1
            continue
        page = request.pages[sequence]
        page_counts[(source.requested_active, sequence)] += 1
        page_ordinals[(source.requested_active, sequence)].append(source.source_row_ordinal)
        if (
            source.source_artifact_path != page.source_path
            or source.source_artifact_sha256 != page.source_artifact_sha256
            or source.source_row_ordinal < 0
            or source.source_row_ordinal >= page.record_count
        ):
            integrity_invalid += 1
        provider_request_id = source.source_provider_request_id
        if (
            not isinstance(provider_request_id, str)
            or not provider_request_id
            or provider_request_id != provider_request_id.strip()
        ):
            envelope_invalid += 1
    for active, request in requests.items():
        for page in request.pages:
            if page_counts[(active, page.sequence)] != page.record_count:
                integrity_invalid += 1
            if sorted(page_ordinals[(active, page.sequence)]) != list(range(page.record_count)):
                integrity_invalid += 1
    return {
        "envelope_invalid": envelope_invalid,
        "integrity_invalid": integrity_invalid,
        "plan_invalid": plan_invalid,
        "query_date_invalid": query_date_invalid,
    }


def _observation_metrics(
    session: AssetSourceSession,
    contexts: tuple[_Context, ...],
    accepted: tuple[_Context, ...],
    rows: list[dict[str, object]],
    decisions: tuple[_Decision, ...],
    funnel: RowFunnel,
    overlap: set[str],
    *,
    calendar_name: str,
    current_ticker_types: frozenset[str] | None,
    current_exchange_mics: frozenset[str] | None,
) -> dict[str, tuple[int, int]]:
    total = len(contexts)
    accepted_total = len(accepted)
    invalid_ids = {item.source_record_id for item in contexts} - {
        item.source_record_id for item in accepted
    }
    source_audit = _source_record_audit(session, contexts)
    plan_invalid = source_audit["plan_invalid"]
    provider_mismatch = sum(
        type(item.raw.get("active")) is not bool
        or item.raw.get("active") is not item.source.requested_active
        for item in contexts
    )
    active_count = sum(item.source.requested_active for item in accepted)
    optional_type_invalid = sum(
        any(
            field in item.raw
            and item.raw[field] is not None
            and not isinstance(item.raw[field], str)
            for field in _OPTIONAL_STRING_FIELDS
        )
        for item in accepted
    )
    timestamp_invalid = sum(
        any(
            field in item.raw
            and item.raw[field] is not None
            and _parse_utc(item.raw[field]) is None
            for field in ("last_updated_utc", "delisted_utc")
        )
        for item in accepted
    )
    timestamp_after_capture = sum(
        any(
            value is not None and value > item.source.source_capture_at_utc
            for value in (item.last_updated_at_utc, item.delisted_at_utc)
        )
        for item in accepted
    )
    provider_scope_invalid = sum(
        (item.raw.get("market") is not None and item.raw.get("market") != "stocks")
        or (item.raw.get("locale") is not None and item.raw.get("locale") != "us")
        for item in accepted
    )
    duplicate_decisions = [item for item in decisions if len(item.contexts) > 1]
    identity_conflict = sum(
        item.status == "unresolved_identity_conflict" for item in duplicate_decisions
    )
    exact_duplicate_excess = sum(
        len(item.contexts) - 1
        for item in duplicate_decisions
        if item.status == "resolved_exact_duplicate"
    )
    unexpected = sum(bool(set(item.raw) - _PROVIDER_FIELD_SET) for item in accepted)
    optional_whitespace = sum(
        any(
            isinstance(item.raw.get(field), str)
            and str(item.raw[field]) != str(item.raw[field]).strip()
            for field in _OPTIONAL_STRING_FIELDS
        )
        for item in accepted
    )
    tickers = {str(item.raw["ticker"]) for item in accepted}
    casefold_groups: defaultdict[str, set[str]] = defaultdict(set)
    for ticker in tickers:
        casefold_groups[ticker.casefold()].add(ticker)
    casefold_collisions = sum(len(values) > 1 for values in casefold_groups.values())
    type_values = {
        str(item.raw["type"]) for item in accepted if isinstance(item.raw.get("type"), str)
    }
    exchange_values = {
        str(item.raw["primary_exchange"])
        for item in accepted
        if isinstance(item.raw.get("primary_exchange"), str)
    }
    type_unmatched = 0 if current_ticker_types is None else len(type_values - current_ticker_types)
    exchange_unmatched = (
        0 if current_exchange_mics is None else len(exchange_values - current_exchange_mics)
    )
    metadata_after_session = sum(
        item.last_updated_at_utc is not None
        and item.last_updated_at_utc.date() > session.session_date
        for item in accepted
    )
    inactive_without_delisted = sum(
        not item.source.requested_active and item.delisted_at_utc is None for item in accepted
    )
    lineage_invalid = sum(
        not _observation_lineage_valid(item, row)
        for item, row in _match_observation_rows(accepted, rows)
    )
    availability_invalid = sum(
        not _observation_availability_valid(row, calendar_name=calendar_name) for row in rows
    )
    metrics = {
        "schema_exact": (0, 1),
        "source_plan_invalid": (
            plan_invalid,
            total + 1,
        ),
        "source_integrity_invalid": (
            source_audit["integrity_invalid"],
            (2 * total) + (2 * session.page_count),
        ),
        "source_envelope_invalid": (
            source_audit["envelope_invalid"],
            total,
        ),
        "source_session_pair_cardinality_invalid": (0, 1),
        "source_query_date_invalid": (
            source_audit["query_date_invalid"],
            total,
        ),
        "source_calendar_coverage_invalid": (
            0 if _is_calendar_session(session.session_date, calendar_name) else 1,
            1,
        ),
        "active_snapshot_empty_sessions": (int(active_count == 0), 1),
        "required_field_invalid_rows": (len(invalid_ids), total),
        "provider_active_mismatch_rows": (
            provider_mismatch,
            total,
        ),
        "row_funnel_unreconciled": (
            _funnel_mismatch(
                funnel,
                input_rows=total,
                accepted_rows=accepted_total,
                quarantine_rows=total - accepted_total,
                unmapped_rows=0,
                version_rows=sum(len(item.contexts) for item in duplicate_decisions),
                table=ASSET_OBSERVATION_DAILY_CONTRACT.table,
                output_rows=len(rows),
            ),
            1,
        ),
        "primary_key_duplicate_excess": (
            _pk_excess(rows, ("session_date", "source_record_id")),
            len(rows),
        ),
        "lineage_invalid_rows": (lineage_invalid, len(rows)),
        "source_availability_invalid_rows": (
            availability_invalid,
            len(rows),
        ),
        "reference_time_scope_invalid_rows": (
            sum(row["reference_time_scope"] != ASSET_REFERENCE_TIME_SCOPE for row in rows),
            len(rows),
        ),
        "metadata_time_scope_invalid_rows": (
            sum(row["metadata_time_scope"] != ASSET_METADATA_TIME_SCOPE for row in rows),
            len(rows),
        ),
        "source_availability_quality_invalid_rows": (
            sum(
                row["source_availability_quality"] != ASSET_SOURCE_AVAILABILITY_QUALITY
                for row in rows
            ),
            len(rows),
        ),
        "active_inactive_overlap_rows": (
            sum(str(item.raw["ticker"]) in overlap for item in accepted),
            accepted_total,
        ),
        "session_year_invalid_rows": (
            sum(row["session_year"] != row["session_date"].year for row in rows),
            len(rows),
        ),
        "ticker_whitespace_rows": (
            sum(str(item.raw["ticker"]) != str(item.raw["ticker"]).strip() for item in accepted),
            accepted_total,
        ),
        "timestamp_parse_invalid_rows": (timestamp_invalid, accepted_total),
        "source_timestamp_after_capture_rows": (
            timestamp_after_capture,
            accepted_total,
        ),
        "optional_field_type_invalid_rows": (
            optional_type_invalid,
            accepted_total,
        ),
        "provider_scope_invalid_rows": (
            provider_scope_invalid,
            accepted_total,
        ),
        "same_session_ticker_identity_conflict_groups": (
            identity_conflict,
            len(duplicate_decisions),
        ),
        "exact_duplicate_excess_rows": (
            exact_duplicate_excess,
            accepted_total,
        ),
        "unexpected_source_field_rows": (unexpected, accepted_total),
        "optional_string_whitespace_rows": (
            optional_whitespace,
            accepted_total,
        ),
        "currency_domain_unreviewed_rows": (
            sum(
                item.raw.get("currency_name") is not None and item.raw.get("currency_name") != "usd"
                for item in accepted
            ),
            accepted_total,
        ),
        "casefold_collision_groups": (
            casefold_collisions,
            len(casefold_groups),
        ),
        "current_type_dictionary_unmatched_values": (
            type_unmatched,
            0 if current_ticker_types is None else len(type_values),
        ),
        "current_exchange_dictionary_unmatched_values": (
            exchange_unmatched,
            0 if current_exchange_mics is None else len(exchange_values),
        ),
        "metadata_updated_after_session_rows": (
            metadata_after_session,
            accepted_total,
        ),
        "inactive_without_delisted_rows": (
            inactive_without_delisted,
            accepted_total,
        ),
        "cross_session_ticker_identity_churn_groups": (0, 0),
    }
    return metrics


def _version_metrics(
    accepted: tuple[_Context, ...],
    decisions: tuple[_Decision, ...],
    observation_rows: list[dict[str, object]],
    rows: list[dict[str, object]],
    funnel: RowFunnel,
    *,
    source_input_rows: int,
    quarantined_source_rows: int,
) -> dict[str, tuple[int, int]]:
    duplicate_decisions = [item for item in decisions if len(item.contexts) > 1]
    groups = len(duplicate_decisions)
    unresolved = sum(item.status in _UNRESOLVED_STATUSES for item in duplicate_decisions)
    ties = sum(item.status == "unresolved_timestamp_tie" for item in duplicate_decisions)
    exact = sum(item.status == "resolved_exact_duplicate" for item in duplicate_decisions)
    delisted_changed = sum("delisted_utc" in item.difference_fields for item in duplicate_decisions)
    unexpected_difference = sum(
        item.status == "unresolved_difference_set" for item in duplicate_decisions
    )
    expected_projection = sum(len(item.contexts) for item in duplicate_decisions)
    observation_ids = {(row["session_date"], row["source_record_id"]) for row in observation_rows}
    decisions_by_group = {
        item.version_group_id: item
        for item in duplicate_decisions
        if item.version_group_id is not None
    }
    contexts_by_id = {item.source_record_id: item for item in accepted}
    rows_by_group: defaultdict[str, list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        rows_by_group[str(row["version_group_id"])].append(row)
    parent_missing = sum(
        (row["session_date"], row["source_record_id"]) not in observation_ids for row in rows
    )
    cardinality_invalid = sum(
        len(item.contexts) <= 1
        or len(rows_by_group[str(item.version_group_id)]) != len(item.contexts)
        for item in duplicate_decisions
    )
    selection_count_invalid = sum(
        sum(bool(row["is_selected"]) for row in rows_by_group[str(item.version_group_id)])
        != (1 if item.status in _RESOLVED_STATUSES else 0)
        for item in duplicate_decisions
    )
    group_id_invalid = 0
    difference_invalid = 0
    signature_invalid = 0
    selection_evidence_invalid = 0
    selected_id_invalid = 0
    lineage_invalid = 0
    for row in rows:
        group_id = str(row["version_group_id"])
        decision = decisions_by_group.get(group_id)
        context = contexts_by_id.get(str(row["source_record_id"]))
        expected_group_id = stable_digest(
            {
                "requested_active": row["requested_active"],
                "session_date": row["session_date"].isoformat(),
                "ticker": row["ticker"],
            }
        )
        group_id_invalid += group_id != expected_group_id
        if decision is None or context is None:
            difference_invalid += 1
            signature_invalid += 1
            selection_evidence_invalid += 1
            selected_id_invalid += 1
            lineage_invalid += 1
            continue
        expected_difference = json.dumps(
            list(decision.difference_fields),
            ensure_ascii=False,
            separators=(",", ":"),
        )
        difference_invalid += row["difference_fields_json"] != expected_difference
        signature_invalid += row["identity_signature"] != _identity_signature(context.raw)
        expected_selected = context.source_record_id == decision.selected_source_record_id
        selection_evidence_invalid += any(
            (
                row["version_count"] != len(decision.contexts),
                row["selection_rank"] != decision.ranks[context.source_record_id],
                row["is_selected"] is not expected_selected,
                row["selection_status"] != decision.status,
                row["selection_reason"] != decision.reasons[context.source_record_id],
                row["selection_rule_version"] != ASSET_VERSION_SELECTION_RULE,
            )
        )
        selected_id_invalid += row["selected_source_record_id"] != (
            decision.selected_source_record_id
        )
        lineage_invalid += not _version_lineage_valid(row, context)
    identity_conflict_selected = sum(
        item.status == "unresolved_identity_conflict"
        and any(bool(row["is_selected"]) for row in rows_by_group[str(item.version_group_id)])
        for item in duplicate_decisions
    )
    invalid_last_selected = sum(
        item.status in _RESOLVED_STATUSES
        and item.status != "resolved_exact_duplicate"
        and any(context.last_updated_at_utc is None for context in item.contexts)
        for item in duplicate_decisions
    )
    nonunique_latest_selected = sum(
        item.status == "resolved_unique_latest_last_updated"
        and _latest_timestamp_count(item.contexts) != 1
        for item in duplicate_decisions
    )
    hash_only_semantic = sum(
        item.status in _RESOLVED_STATUSES
        and item.status != "resolved_exact_duplicate"
        and len({context.source_row_hash for context in item.contexts}) > 1
        and (
            len({_identity_signature(context.raw) for context in item.contexts}) != 1
            or item.difference_fields not in _ALLOWED_DIFFERENCE_FIELDS
        )
        for item in duplicate_decisions
    )
    metrics = {
        "schema_exact": (0, 1),
        "observation_parent_missing_rows": (parent_missing, len(rows)),
        "version_projection_unreconciled": (int(len(rows) != expected_projection), 1),
        "singleton_version_rows": (
            sum(row["version_count"] <= 1 for row in rows),
            len(rows),
        ),
        "version_group_id_invalid_rows": (group_id_invalid, len(rows)),
        "version_group_cardinality_invalid_groups": (
            cardinality_invalid,
            groups,
        ),
        "difference_fields_invalid_rows": (difference_invalid, len(rows)),
        "identity_signature_invalid_rows": (signature_invalid, len(rows)),
        "selection_evidence_invalid_rows": (
            selection_evidence_invalid,
            len(rows),
        ),
        "selection_count_invalid_groups": (
            selection_count_invalid,
            groups,
        ),
        "selected_source_record_id_invalid_rows": (
            selected_id_invalid,
            len(rows),
        ),
        "identity_conflict_selected_groups": (
            identity_conflict_selected,
            groups,
        ),
        "invalid_last_updated_selected_groups": (
            invalid_last_selected,
            groups,
        ),
        "nonunique_latest_selected_groups": (
            nonunique_latest_selected,
            groups,
        ),
        "hash_only_semantic_selection_groups": (
            hash_only_semantic,
            groups,
        ),
        "lineage_invalid_rows": (lineage_invalid, len(rows)),
        "row_funnel_unreconciled": (
            _funnel_mismatch(
                funnel,
                input_rows=source_input_rows,
                accepted_rows=len(accepted),
                quarantine_rows=quarantined_source_rows,
                unmapped_rows=len(accepted) - len(rows),
                version_rows=len(rows),
                table=ASSET_OBSERVATION_VERSION_CONTRACT.table,
                output_rows=len(rows),
            ),
            1,
        ),
        "primary_key_duplicate_excess": (
            _pk_excess(rows, ("session_date", "version_group_id", "source_record_id")),
            len(rows),
        ),
        "session_year_invalid_rows": (
            sum(row["session_year"] != row["session_date"].year for row in rows),
            len(rows),
        ),
        "unresolved_version_groups": (unresolved, groups),
        "semantic_tie_groups": (ties, groups),
        "exact_duplicate_groups": (exact, groups),
        "delisted_changed_groups": (delisted_changed, groups),
        "unexpected_difference_field_groups": (
            unexpected_difference,
            groups,
        ),
        "selection_rule_unreviewed_rows": (
            sum(row["selection_rule_version"] != ASSET_VERSION_SELECTION_RULE for row in rows),
            len(rows),
        ),
    }
    return metrics


def _universe_metrics(
    session: AssetSourceSession,
    decisions: tuple[_Decision, ...],
    observation_rows: list[dict[str, object]],
    version_rows: list[dict[str, object]],
    rows: list[dict[str, object]],
    overlap: set[str],
    funnel: RowFunnel,
    *,
    calendar_name: str,
    current_ticker_types: frozenset[str] | None,
    current_exchange_mics: frozenset[str] | None,
    source_input_rows: int,
    quarantined_source_rows: int,
) -> dict[str, tuple[int, int]]:
    observation_ids = {(row["session_date"], row["source_record_id"]) for row in observation_rows}
    selected_version_ids = {
        (row["version_group_id"], row["source_record_id"])
        for row in version_rows
        if row["is_selected"]
    }
    parent_missing = sum(
        (row["session_date"], row["selected_source_record_id"]) not in observation_ids
        for row in rows
    )
    version_parent_missing = sum(
        (
            row["version_group_id"] is not None
            and (row["version_group_id"], row["selected_source_record_id"])
            not in selected_version_ids
        )
        or (
            row["version_group_id"] is None
            and (row["source_version_count"] != 1 or row["selection_status"] != "singleton")
        )
        for row in rows
    )
    excess = sum(len(item.contexts) - 1 for item in decisions)
    expected_formula = len(observation_rows) - excess
    unresolved = sum(item.status in _UNRESOLVED_STATUSES for item in decisions)
    formula_invalid = int(
        bool(overlap)
        or unresolved > 0
        or any(
            str(item.contexts[0].raw["ticker"]) != str(item.contexts[0].raw["ticker"]).strip()
            for item in decisions
        )
        or len(rows) != expected_formula
        or _funnel_mismatch(
            funnel,
            input_rows=source_input_rows,
            accepted_rows=len(observation_rows),
            quarantine_rows=quarantined_source_rows,
            unmapped_rows=len(observation_rows) - len(rows),
            version_rows=len(version_rows),
            table=UNIVERSE_SOURCE_DAILY_CONTRACT.table,
            output_rows=len(rows),
        )
    )
    pair_id = stable_digest(
        {
            "active_source_request_id": session.active_request.source_request_id,
            "inactive_source_request_id": session.inactive_request.source_request_id,
            "session_date": session.session_date.isoformat(),
        }
    )
    pair_lineage_invalid = sum(
        row["active_source_request_id"] != session.active_request.source_request_id
        or row["inactive_source_request_id"] != session.inactive_request.source_request_id
        or row["source_pair_id"] != pair_id
        for row in rows
    )
    availability_invalid = sum(
        not _universe_availability_valid(
            row,
            pair_completed=session.capture_completed_at_utc,
            calendar_name=calendar_name,
        )
        for row in rows
    )
    identity_invalid = sum(row["identity_link_status"] != _identity_status(row) for row in rows)
    contexts_by_id = {
        context.source_record_id: context for decision in decisions for context in decision.contexts
    }
    decisions_by_selected_id = {
        item.selected_source_record_id: item
        for item in decisions
        if item.selected_source_record_id is not None
    }
    unresolved_entered = 0
    selection_formula_invalid = 0
    selected_id_invalid = 0
    lineage_invalid = 0
    current_backfill = 0
    identity_conflict_entered = 0
    for row in rows:
        selected_id = str(row["selected_source_record_id"])
        context = contexts_by_id.get(selected_id)
        decision = decisions_by_selected_id.get(selected_id)
        if context is None or decision is None:
            unresolved_entered += 1
            selection_formula_invalid += 1
            selected_id_invalid += 1
            lineage_invalid += 1
            current_backfill += 1
            continue
        unresolved_entered += decision.status in _UNRESOLVED_STATUSES
        selection_formula_invalid += any(
            (
                decision.selected_source_record_id != selected_id,
                str(context.raw.get("ticker")) in overlap,
                str(context.raw.get("ticker")) != str(context.raw.get("ticker")).strip(),
            )
        )
        selected_id_invalid += any(
            (
                row["ticker"] != context.raw.get("ticker"),
                row["selected_source_record_id"] != context.source_record_id,
                row["version_group_id"] != decision.version_group_id,
                row["source_version_count"] != len(decision.contexts),
                row["selection_status"] != decision.status,
                row["selection_rule_version"] != ASSET_VERSION_SELECTION_RULE,
            )
        )
        lineage_invalid += not _universe_lineage_valid(row, context)
        current_backfill += not _universe_source_fields_valid(row, context)
        identity_conflict_entered += (
            len({_identity_signature(item.raw) for item in decision.contexts}) != 1
        )
    expected_selected = sum(
        item.selected is not None
        and str(item.selected.raw["ticker"]) not in overlap
        and str(item.selected.raw["ticker"]) == str(item.selected.raw["ticker"]).strip()
        for item in decisions
    )
    selection_formula_invalid += int(expected_selected != len(rows))
    selected_timestamp_invalid = sum(
        _context_timestamp_invalid(contexts_by_id.get(str(row["selected_source_record_id"])))
        for row in rows
    )
    figi_groups = _identifier_multi_ticker_groups(rows, "composite_figi")
    share_groups = _identifier_multi_ticker_groups(rows, "share_class_figi")
    casefold_groups: defaultdict[str, set[str]] = defaultdict(set)
    for row in rows:
        casefold_groups[str(row["ticker"]).casefold()].add(str(row["ticker"]))
    type_values = {str(row["type_code"]) for row in rows if row["type_code"] is not None}
    exchange_values = {
        str(row["primary_exchange_mic"]) for row in rows if row["primary_exchange_mic"] is not None
    }
    type_unmatched = 0 if current_ticker_types is None else len(type_values - current_ticker_types)
    exchange_unmatched = (
        0 if current_exchange_mics is None else len(exchange_values - current_exchange_mics)
    )
    metrics = {
        "schema_exact": (0, 1),
        "source_session_pair_cardinality_invalid": (0, 1),
        "observation_parent_missing_rows": (parent_missing, len(rows)),
        "version_parent_missing_rows": (
            version_parent_missing,
            len(rows),
        ),
        "unresolved_version_groups_entered": (
            unresolved_entered,
            len(rows),
        ),
        "active_inactive_overlap_rows": (
            sum(
                len(decision.contexts)
                for decision in decisions
                if str(decision.contexts[0].raw["ticker"]) in overlap
            ),
            len(observation_rows),
        ),
        "universe_row_formula_invalid": (formula_invalid, 1),
        "selection_formula_invalid_rows": (
            selection_formula_invalid,
            len(rows) + 1,
        ),
        "selected_source_record_id_invalid_rows": (
            selected_id_invalid,
            len(rows),
        ),
        "primary_key_duplicate_excess": (
            _pk_excess(rows, ("session_date", "ticker")),
            len(rows),
        ),
        "lineage_invalid_rows": (lineage_invalid, len(rows)),
        "source_pair_lineage_invalid_rows": (
            pair_lineage_invalid,
            len(rows),
        ),
        "universe_availability_invalid_rows": (
            availability_invalid,
            len(rows),
        ),
        "reference_time_scope_invalid_rows": (
            sum(row["reference_time_scope"] != ASSET_REFERENCE_TIME_SCOPE for row in rows),
            len(rows),
        ),
        "metadata_time_scope_invalid_rows": (
            sum(row["metadata_time_scope"] != ASSET_METADATA_TIME_SCOPE for row in rows),
            len(rows),
        ),
        "source_availability_quality_invalid_rows": (
            sum(
                row["source_availability_quality"] != ASSET_SOURCE_AVAILABILITY_QUALITY
                for row in rows
            ),
            len(rows),
        ),
        "session_year_invalid_rows": (
            sum(row["session_year"] != row["session_date"].year for row in rows),
            len(rows),
        ),
        "active_snapshot_empty_sessions": (
            int(not any(row["active_on_date"] for row in rows)),
            1,
        ),
        "current_dictionary_backfill_rows": (
            current_backfill,
            len(rows),
        ),
        "identity_link_status_invalid_rows": (
            identity_invalid,
            len(rows),
        ),
        "same_session_ticker_identity_conflict_rows": (
            identity_conflict_entered,
            len(rows),
        ),
        "selected_timestamp_parse_invalid_rows": (
            selected_timestamp_invalid,
            len(rows),
        ),
        "identity_evidence_missing_rows": (
            sum(
                row["identity_link_status"] == "insufficient_identity_evidence_pending_s7"
                for row in rows
            ),
            len(rows),
        ),
        "same_session_composite_figi_multiple_ticker_groups": (
            figi_groups,
            len(rows),
        ),
        "same_session_share_class_figi_multiple_ticker_groups": (
            share_groups,
            len(rows),
        ),
        "casefold_collision_groups": (
            sum(len(values) > 1 for values in casefold_groups.values()),
            len(casefold_groups),
        ),
        "current_type_dictionary_unmatched_values": (
            type_unmatched,
            0 if current_ticker_types is None else len(type_values),
        ),
        "current_exchange_dictionary_unmatched_values": (
            exchange_unmatched,
            0 if current_exchange_mics is None else len(exchange_values),
        ),
        "metadata_updated_after_session_rows": (
            sum(
                row["last_updated_at_utc"] is not None
                and row["last_updated_at_utc"].date() > session.session_date
                for row in rows
            ),
            len(rows),
        ),
        "inactive_without_delisted_rows": (
            sum(not row["active_on_date"] and row["delisted_at_utc"] is None for row in rows),
            len(rows),
        ),
        "cross_session_ticker_identity_churn_groups": (0, 0),
    }
    return metrics


def _qa_results(
    contract: TableContract,
    metrics: Mapping[str, tuple[int, int]],
    *,
    partition_key: str,
) -> tuple[QACheckResult, ...]:
    if set(metrics) != set(contract.required_qa_checks):
        missing = sorted(set(contract.required_qa_checks) - set(metrics))
        extra = sorted(set(metrics) - set(contract.required_qa_checks))
        detail = f"missing={missing}, extra={extra}"
        raise AssetTransformError(f"{contract.table} QA metric set differs from contract: {detail}")
    results: list[QACheckResult] = []
    for rule in contract.qa_rules:
        numerator, denominator = metrics[rule.check_id]
        if numerator > denominator:
            raise AssetTransformError(
                f"{contract.table} QA numerator exceeds denominator: {rule.check_id}"
            )
        rate = None if denominator == 0 else float(numerator / denominator)
        results.append(
            QACheckResult(
                table=contract.table,
                partition_key=partition_key,
                check_id=rule.check_id,
                severity=rule.severity,
                status=rule.expected_status(numerator=numerator, rate=rate),
                numerator=numerator,
                denominator=denominator,
                rate=rate,
                threshold=rule.threshold_expression,
            )
        )
    return tuple(results)


def _difference_fields(contexts: tuple[_Context, ...]) -> tuple[str, ...]:
    fields: list[str] = []
    for field in _PROVIDER_FIELDS:
        values = {
            stable_digest({"present": field in item.raw, "value": item.raw.get(field)})
            for item in contexts
        }
        if len(values) > 1:
            fields.append(field)
    return tuple(sorted(fields))


def _identity_signature(raw: Mapping[str, object]) -> str:
    return stable_digest({field: raw.get(field) for field in _IDENTITY_FIELDS})


def _parse_utc(value: object) -> datetime | None:
    if value is None or not isinstance(value, str) or not _UTC_TIMESTAMP.fullmatch(value):
        return None
    try:
        parsed = pd.Timestamp(value)
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.tz_convert("UTC")


@lru_cache(maxsize=4_096)
def _first_market_open_after(
    capture_at_utc: datetime,
    *,
    calendar_name: str,
) -> tuple[date, datetime]:
    calendar = xcals.get_calendar(calendar_name)
    local_date = capture_at_utc.astimezone(_NEW_YORK).date()
    capture = pd.Timestamp(capture_at_utc)
    for session in calendar.sessions_in_range(
        (local_date - timedelta(days=1)).isoformat(),
        (local_date + timedelta(days=14)).isoformat(),
    ):
        opening = calendar.session_open(session)
        if opening > capture:
            return session.date(), opening.to_pydatetime().astimezone(UTC)
    raise AssetTransformError("cannot find XNYS open after Assets source capture")


def _is_calendar_session(value: date, calendar_name: str) -> bool:
    calendar = xcals.get_calendar(calendar_name)
    try:
        return bool(calendar.is_session(pd.Timestamp(value)))
    except (TypeError, ValueError):
        return False


def _observation_lineage_valid(context: _Context, output: Mapping[str, object]) -> bool:
    expected = _observation_row(context)
    return all(output.get(key) == expected[key] for key in expected)


def _version_lineage_valid(row: Mapping[str, object], context: _Context) -> bool:
    return (
        row.get("source_capture_at_utc") == context.source.source_capture_at_utc
        and row.get("source_request_id") == context.source.source_request_id
        and row.get("source_provider_request_id") == context.source.source_provider_request_id
        and row.get("source_artifact_sha256") == context.source.source_artifact_sha256
        and row.get("source_page_sequence") == context.source.source_page_sequence
        and row.get("source_row_ordinal") == context.source.source_row_ordinal
        and row.get("source_row_hash") == context.source_row_hash
    )


def _universe_lineage_valid(row: Mapping[str, object], context: _Context) -> bool:
    return (
        row.get("selected_source_capture_at_utc") == context.source.source_capture_at_utc
        and row.get("source_request_id") == context.source.source_request_id
        and row.get("source_provider_request_id") == context.source.source_provider_request_id
        and row.get("source_artifact_sha256") == context.source.source_artifact_sha256
        and row.get("source_page_sequence") == context.source.source_page_sequence
        and row.get("source_row_ordinal") == context.source.source_row_ordinal
        and row.get("source_row_hash") == context.source_row_hash
    )


def _universe_source_fields_valid(row: Mapping[str, object], context: _Context) -> bool:
    raw = context.raw
    return (
        row.get("active_on_date") == raw.get("active")
        and row.get("type_code") == _string_or_none(raw.get("type"))
        and row.get("name") == _string_or_none(raw.get("name"))
        and row.get("market") == _string_or_none(raw.get("market"))
        and row.get("locale") == _string_or_none(raw.get("locale"))
        and row.get("primary_exchange_mic") == _string_or_none(raw.get("primary_exchange"))
        and row.get("currency_name") == _string_or_none(raw.get("currency_name"))
        and row.get("cik") == _string_or_none(raw.get("cik"))
        and row.get("composite_figi") == _string_or_none(raw.get("composite_figi"))
        and row.get("share_class_figi") == _string_or_none(raw.get("share_class_figi"))
        and row.get("delisted_at_utc") == context.delisted_at_utc
        and row.get("last_updated_at_utc") == context.last_updated_at_utc
    )


def _latest_timestamp_count(contexts: tuple[_Context, ...]) -> int:
    values = [item.last_updated_at_utc for item in contexts]
    if not values or any(value is None for value in values):
        return 0
    latest = max(value for value in values if value is not None)
    return sum(value == latest for value in values)


def _match_observation_rows(
    contexts: tuple[_Context, ...],
    rows: list[dict[str, object]],
) -> tuple[tuple[_Context, dict[str, object]], ...]:
    by_id = {item.source_record_id: item for item in contexts}
    return tuple((by_id[str(row["source_record_id"])], row) for row in rows)


def _observation_availability_valid(row: Mapping[str, object], *, calendar_name: str) -> bool:
    captured = row.get("source_capture_at_utc")
    if not isinstance(captured, datetime):
        return False
    session, opening = _first_market_open_after(captured, calendar_name=calendar_name)
    return (
        row.get("source_available_session") == session
        and row.get("source_available_at_utc") == opening
        and row.get("source_availability_rule") == ASSET_SOURCE_AVAILABILITY_RULE
    )


def _universe_availability_valid(
    row: Mapping[str, object],
    *,
    pair_completed: datetime,
    calendar_name: str,
) -> bool:
    session, opening = _first_market_open_after(pair_completed, calendar_name=calendar_name)
    return (
        row.get("universe_capture_completed_at_utc") == pair_completed
        and row.get("source_available_session") == session
        and row.get("source_available_at_utc") == opening
        and row.get("source_availability_rule") == UNIVERSE_SOURCE_AVAILABILITY_RULE
    )


def _identity_status(row: Mapping[str, object]) -> str:
    count = sum(
        row.get(field) is not None for field in ("composite_figi", "share_class_figi", "cik")
    )
    if count == 0:
        return "insufficient_identity_evidence_pending_s7"
    if count == 1:
        return "single_identifier_evidence_pending_s7"
    return "multi_identifier_evidence_pending_s7"


def _context_timestamp_invalid(context: _Context | None) -> bool:
    if context is None:
        return True
    return any(
        context.raw.get(field) is not None and parsed is None
        for field, parsed in (
            ("last_updated_utc", context.last_updated_at_utc),
            ("delisted_utc", context.delisted_at_utc),
        )
    )


def _identifier_multi_ticker_groups(rows: list[dict[str, object]], field: str) -> int:
    grouped: defaultdict[str, set[str]] = defaultdict(set)
    for row in rows:
        value = row.get(field)
        if isinstance(value, str):
            grouped[value].add(str(row["ticker"]))
    return sum(len(tickers) > 1 for tickers in grouped.values())


def _funnel_mismatch(
    funnel: RowFunnel,
    *,
    input_rows: int,
    accepted_rows: int,
    quarantine_rows: int,
    unmapped_rows: int,
    version_rows: int,
    table: str,
    output_rows: int,
) -> int:
    return int(
        funnel.input_rows != input_rows
        or funnel.accepted_source_rows != accepted_rows
        or funnel.exact_duplicate_excess != 0
        or funnel.quarantined_source_rows != quarantine_rows
        or funnel.unmapped_source_rows != unmapped_rows
        or funnel.version_preserved_rows != version_rows
        or dict(funnel.output_rows_by_table) != {table: output_rows}
    )


def _pk_excess(rows: list[dict[str, object]], fields: tuple[str, ...]) -> int:
    counts = Counter(tuple(row[field] for field in fields) for row in rows)
    return sum(count - 1 for count in counts.values())


def _string_or_none(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _timestamp_ns(value: datetime | None) -> int:
    if value is None:
        raise AssetTransformError("cannot rank a missing timestamp")
    return int(pd.Timestamp(value).value)


def _bounded_json(value: object) -> str:
    text = json.dumps(
        value, allow_nan=False, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    )
    if len(text) <= 4_000:
        return text
    return f"{text[:3900]}...[sha256={stable_digest(value)}]"


__all__ = [
    "ASSET_METADATA_TIME_SCOPE",
    "ASSET_REFERENCE_TIME_SCOPE",
    "ASSET_SOURCE_AVAILABILITY_QUALITY",
    "ASSET_SOURCE_AVAILABILITY_RULE",
    "ASSET_TRANSFORM_VERSION",
    "ASSET_VERSION_SELECTION_RULE",
    "UNIVERSE_SOURCE_AVAILABILITY_RULE",
    "AssetTableTransformResult",
    "AssetTransformError",
    "AssetTransformResult",
    "transform_asset_session",
]
