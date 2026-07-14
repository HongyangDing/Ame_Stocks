"""Pure S6 transform for reviewed, allowlisted Ticker Overview evidence."""

from __future__ import annotations

import json
import re
from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import exchange_calendars as xcals
import pandas as pd
import pyarrow as pa

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver.contracts import (
    QACheckResult,
    QASeverity,
    QuarantineRecord,
    QuarantineReviewStatus,
    RowFunnel,
    SilverContractError,
    TableContract,
)
from ame_stocks_api.silver.ticker_overview_contract import TICKER_OVERVIEW_SAFE_CONTRACT

TICKER_OVERVIEW_SAFE_TRANSFORM_VERSION = "s6-ticker-overview-safe-v1.0.0"

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_NEW_YORK = ZoneInfo("America/New_York")
_IDENTITY_BASES = frozenset({"share_class_figi", "composite_figi", "cik", "ticker"})
_IDENTITY_SCOPE = "evidence_only_pending_s7"
_AVAILABILITY_RULE = "first_xnys_open_after_source_capture_v1"
_AVAILABILITY_QUALITY = "retrospective_historical_query_without_archived_vintage_v1"
_UNSAFE_FIELDS = frozenset(
    {"market_cap", "share_class_shares_outstanding", "weighted_shares_outstanding"}
)


class TickerOverviewSafeTransformError(SilverContractError):
    """Raised when S6 input cannot be represented without guessing."""


@dataclass(frozen=True, slots=True)
class TickerOverviewSafeInput:
    lifecycle_id: str
    source_request_id: str
    query_ticker: str
    query_date: date | str
    first_active_date: date | str
    last_active_date: date | str
    identity_type: str
    identity_value: str
    identity_match: bool
    identity_match_basis: str | None
    identity_evidence_status: str
    ticker: str
    name: str | None
    type: str | None
    market: str | None
    locale: str | None
    active: bool
    primary_exchange: str | None
    currency_name: str | None
    cik: str | None
    composite_figi: str | None
    share_class_figi: str | None
    sic_code: str | None
    sic_description: str | None
    list_date: date | str | None
    delisted_utc: str | None
    ticker_root: str | None
    ticker_suffix: str | None
    source_manifest_created_at_utc: datetime | str
    source_capture_at_utc: datetime | str
    source_manifest_path: str
    source_manifest_sha256: str
    source_artifact_path: str
    source_artifact_sha256: str
    source_artifact_raw_sha256: str
    source_page_sequence: int
    source_row_ordinal: int
    source_provider_request_id: str
    source_result_hash: str

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> TickerOverviewSafeInput:
        unsafe = sorted(_UNSAFE_FIELDS.intersection(value))
        if unsafe:
            raise TickerOverviewSafeTransformError(
                f"unsafe Ticker Overview fields entered S6 input: {unsafe}"
            )
        try:
            return cls(**{name: value[name] for name in cls.__dataclass_fields__})  # type: ignore[arg-type]
        except (KeyError, TypeError) as exc:
            raise TickerOverviewSafeTransformError("ticker-overview input is malformed") from exc


@dataclass(frozen=True, slots=True)
class TickerOverviewSafeTransformResult:
    contract: TableContract
    table: pa.Table
    qa_checks: tuple[QACheckResult, ...]
    quarantine_records: tuple[QuarantineRecord, ...]
    row_funnel: RowFunnel

    def __post_init__(self) -> None:
        if self.contract != TICKER_OVERVIEW_SAFE_CONTRACT:
            raise TickerOverviewSafeTransformError("S6 result has the wrong contract")
        if self.table.schema != self.contract.arrow_schema:
            raise TickerOverviewSafeTransformError("S6 output schema differs from its contract")
        if {item.check_id for item in self.qa_checks} != set(self.contract.required_qa_checks):
            raise TickerOverviewSafeTransformError("S6 QA set differs from its contract")
        if any(item.table != self.contract.table for item in self.qa_checks):
            raise TickerOverviewSafeTransformError("S6 QA table identity differs")
        if any(item.table_name != self.contract.table for item in self.quarantine_records):
            raise TickerOverviewSafeTransformError("S6 quarantine table identity differs")
        if len({item.source_record_id for item in self.quarantine_records}) != (
            self.row_funnel.quarantined_source_rows
        ):
            raise TickerOverviewSafeTransformError("S6 quarantine differs from its row funnel")

    def qa_by_id(self, check_id: str) -> QACheckResult:
        for item in self.qa_checks:
            if item.check_id == check_id:
                return item
        raise KeyError(check_id)

    @property
    def blocks_publish(self) -> bool:
        return any(item.blocks_publish for item in self.qa_checks)


@dataclass(frozen=True, slots=True)
class _Context:
    source: TickerOverviewSafeInput
    query_date: date
    first_active_date: date
    last_active_date: date
    list_date: date | None
    manifest_created: datetime
    capture: datetime
    capture_date: date
    available_session: date
    available_at: datetime
    source_record_id: str
    source_pointer: str


def transform_ticker_overview_safe(
    records: Iterable[TickerOverviewSafeInput | Mapping[str, object]],
    *,
    build_id: str,
    calendar_name: str = "XNYS",
    source_integrity_invalid: int = 0,
    source_envelope_invalid: int = 0,
    unexpected_source_field_rows: int = 0,
) -> TickerOverviewSafeTransformResult:
    """Build S6 DATA and quarantine from detached, already verified source rows."""

    if not isinstance(build_id, str) or not _SHA256.fullmatch(build_id):
        raise TickerOverviewSafeTransformError("build_id must be a lowercase SHA-256")
    for name, value in (
        ("source_integrity_invalid", source_integrity_invalid),
        ("source_envelope_invalid", source_envelope_invalid),
        ("unexpected_source_field_rows", unexpected_source_field_rows),
    ):
        if type(value) is not int or value < 0:
            raise TickerOverviewSafeTransformError(f"{name} must be a nonnegative integer")

    inputs = tuple(
        item
        if isinstance(item, TickerOverviewSafeInput)
        else TickerOverviewSafeInput.from_mapping(item)
        for item in records
    )
    contexts = tuple(_context(item, calendar_name=calendar_name) for item in inputs)
    quarantines = tuple(
        _identity_quarantine(item, build_id=build_id)
        for item in contexts
        if item.source.identity_match is False
    )
    quarantined_ids = {item.source_record_id for item in quarantines}
    accepted = tuple(item for item in contexts if item.source_record_id not in quarantined_ids)
    rows = sorted(
        (_output_row(item) for item in accepted),
        key=lambda item: tuple(item[name] for name in TICKER_OVERVIEW_SAFE_CONTRACT.sort_by),
    )
    table = pa.Table.from_pylist(rows, schema=TICKER_OVERVIEW_SAFE_CONTRACT.arrow_schema)
    funnel = RowFunnel(
        input_rows=len(contexts),
        accepted_source_rows=len(accepted),
        exact_duplicate_excess=0,
        quarantined_source_rows=len(quarantined_ids),
        unmapped_source_rows=0,
        version_preserved_rows=0,
        output_rows_by_table={TICKER_OVERVIEW_SAFE_CONTRACT.table: len(rows)},
    )
    metrics = _metrics(
        contexts,
        rows,
        quarantines,
        funnel,
        source_integrity_invalid=source_integrity_invalid,
        source_envelope_invalid=source_envelope_invalid,
        unexpected_source_field_rows=unexpected_source_field_rows,
        calendar_name=calendar_name,
    )
    return TickerOverviewSafeTransformResult(
        TICKER_OVERVIEW_SAFE_CONTRACT,
        table,
        _qa_results(table, metrics),
        tuple(sorted(quarantines, key=lambda item: item.issue_id)),
        funnel,
    )


def _context(item: TickerOverviewSafeInput, *, calendar_name: str) -> _Context:
    query_date = _as_date(item.query_date, "query_date")
    first_active_date = _as_date(item.first_active_date, "first_active_date")
    last_active_date = _as_date(item.last_active_date, "last_active_date")
    list_date = None if item.list_date is None else _as_date(item.list_date, "list_date")
    manifest_created = _timestamp(
        item.source_manifest_created_at_utc, "source_manifest_created_at_utc"
    )
    capture = _timestamp(item.source_capture_at_utc, "source_capture_at_utc")
    _validate_required(item)
    available_session, available_at = _first_open(capture, calendar_name)
    source_pointer = (
        f"{item.source_artifact_path}#page={item.source_page_sequence}"
        f"&row={item.source_row_ordinal}"
    )
    source_record_id = stable_digest(
        {
            "dataset": "ticker_overview",
            "lifecycle_id": item.lifecycle_id,
            "source_request_id": item.source_request_id,
            "source_manifest_sha256": item.source_manifest_sha256,
            "source_artifact_sha256": item.source_artifact_sha256,
            "source_artifact_raw_sha256": item.source_artifact_raw_sha256,
            "source_result_hash": item.source_result_hash,
        }
    )
    return _Context(
        item,
        query_date,
        first_active_date,
        last_active_date,
        list_date,
        manifest_created,
        capture,
        capture.astimezone(_NEW_YORK).date(),
        available_session,
        available_at,
        source_record_id,
        source_pointer,
    )


def _validate_required(item: TickerOverviewSafeInput) -> None:
    strings = (
        item.lifecycle_id,
        item.source_request_id,
        item.query_ticker,
        item.identity_type,
        item.identity_value,
        item.ticker,
        item.source_manifest_path,
        item.source_manifest_sha256,
        item.source_artifact_path,
        item.source_artifact_sha256,
        item.source_artifact_raw_sha256,
        item.source_provider_request_id,
        item.source_result_hash,
    )
    if any(not isinstance(value, str) or not value for value in strings):
        raise TickerOverviewSafeTransformError("S6 required string field is missing")
    hashes = (
        item.lifecycle_id,
        item.source_request_id,
        item.source_manifest_sha256,
        item.source_artifact_sha256,
        item.source_artifact_raw_sha256,
        item.source_result_hash,
    )
    if any(not _SHA256.fullmatch(value) for value in hashes):
        raise TickerOverviewSafeTransformError("S6 lineage digest is invalid")
    if type(item.identity_match) is not bool or type(item.active) is not bool:
        raise TickerOverviewSafeTransformError(
            "S6 identity_match and active must be native booleans"
        )
    if type(item.source_page_sequence) is not int or item.source_page_sequence != 0:
        raise TickerOverviewSafeTransformError("Ticker Overview page sequence must be zero")
    if type(item.source_row_ordinal) is not int or item.source_row_ordinal != 0:
        raise TickerOverviewSafeTransformError("Ticker Overview result ordinal must be zero")
    if item.identity_match and item.identity_match_basis not in _IDENTITY_BASES:
        raise TickerOverviewSafeTransformError("matching identity evidence has an invalid basis")
    if not item.identity_match and item.identity_match_basis is not None:
        raise TickerOverviewSafeTransformError("unresolved identity evidence cannot claim a basis")
    expected_status = "matched" if item.identity_match else "no_comparable_identity"
    if item.identity_evidence_status != expected_status:
        raise TickerOverviewSafeTransformError(
            "S6 input contains an unreviewed identity evidence status"
        )


def _output_row(context: _Context) -> dict[str, object]:
    item = context.source
    return {
        "source_capture_date": context.capture_date,
        "lifecycle_id": item.lifecycle_id,
        "query_ticker": item.query_ticker,
        "query_date": context.query_date,
        "first_active_date": context.first_active_date,
        "last_active_date": context.last_active_date,
        "identity_type": item.identity_type,
        "identity_value": item.identity_value,
        "identity_match": item.identity_match,
        "identity_match_basis": item.identity_match_basis,
        "ticker": item.ticker,
        "name": item.name,
        "type": item.type,
        "market": item.market,
        "locale": item.locale,
        "active": item.active,
        "primary_exchange": item.primary_exchange,
        "currency_name": item.currency_name,
        "cik": item.cik,
        "composite_figi": item.composite_figi,
        "share_class_figi": item.share_class_figi,
        "sic_code": item.sic_code,
        "sic_description": item.sic_description,
        "list_date": context.list_date,
        "delisted_utc": item.delisted_utc,
        "ticker_root": item.ticker_root,
        "ticker_suffix": item.ticker_suffix,
        "identity_evidence_scope": _IDENTITY_SCOPE,
        "backtest_identity_eligible": False,
        "source_manifest_created_at_utc": context.manifest_created,
        "source_capture_at_utc": context.capture,
        "source_available_session": context.available_session,
        "source_available_at_utc": context.available_at,
        "source_availability_rule": _AVAILABILITY_RULE,
        "source_availability_quality": _AVAILABILITY_QUALITY,
        "source_record_id": context.source_record_id,
        "source_request_id": item.source_request_id,
        "source_provider_request_id": item.source_provider_request_id,
        "source_manifest_path": item.source_manifest_path,
        "source_manifest_sha256": item.source_manifest_sha256,
        "source_artifact_path": item.source_artifact_path,
        "source_artifact_sha256": item.source_artifact_sha256,
        "source_artifact_raw_sha256": item.source_artifact_raw_sha256,
        "source_page_sequence": item.source_page_sequence,
        "source_row_ordinal": item.source_row_ordinal,
        "source_result_hash": item.source_result_hash,
        "source_pointer": context.source_pointer,
    }


def _identity_quarantine(context: _Context, *, build_id: str) -> QuarantineRecord:
    item = context.source
    observed = json.dumps(
        {
            "identity_type": item.identity_type,
            "identity_value": item.identity_value,
            "identity_evidence_status": item.identity_evidence_status,
            "response_cik": item.cik,
            "response_composite_figi": item.composite_figi,
            "response_share_class_figi": item.share_class_figi,
            "response_ticker": item.ticker,
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return QuarantineRecord(
        context.source_record_id,
        TICKER_OVERVIEW_SAFE_CONTRACT.table,
        "identity_evidence_unresolved",
        QASeverity.HIGH,
        build_id,
        context.source_pointer,
        "identity_match",
        observed,
        (
            "S6 admits a lifecycle only when the current Overview result has comparable "
            "matching identity evidence; unresolved rows remain pending S7 review."
        ),
        QuarantineReviewStatus.PENDING,
    )


def _metrics(
    contexts: tuple[_Context, ...],
    output: list[dict[str, object]],
    quarantine: tuple[QuarantineRecord, ...],
    funnel: RowFunnel,
    *,
    source_integrity_invalid: int,
    source_envelope_invalid: int,
    unexpected_source_field_rows: int,
    calendar_name: str,
) -> dict[str, tuple[int, int]]:
    total = len(contexts)
    unresolved = len(quarantine)
    output_count = len(output)
    return {
        "schema_exact": (0, 1),
        "source_plan_invalid": (int(total != 30_739), 1),
        "source_integrity_invalid": (
            source_integrity_invalid,
            max(total, source_integrity_invalid, 1),
        ),
        "source_envelope_invalid": (
            source_envelope_invalid,
            max(total, source_envelope_invalid, 1),
        ),
        "unexpected_source_field_rows": (
            unexpected_source_field_rows,
            max(total, unexpected_source_field_rows, 1),
        ),
        "formal_lifecycle_cardinality_invalid": (abs(total - 30_739), max(total, 30_739)),
        "source_record_contract_invalid_rows": (0, max(total, 1)),
        "primary_key_duplicate_excess": (
            _duplicate_excess(output, ("lifecycle_id",)),
            max(output_count, 1),
        ),
        "lifecycle_request_key_duplicate_excess": (
            _context_duplicate_excess(contexts, ("query_ticker", "query_date")),
            max(total, 1),
        ),
        "source_request_id_duplicate_excess": (
            _context_duplicate_excess(contexts, ("source_request_id",)),
            max(total, 1),
        ),
        "lifecycle_date_contract_invalid_rows": (
            sum(
                not (item.first_active_date <= item.last_active_date == item.query_date)
                for item in contexts
            ),
            max(total, 1),
        ),
        "list_date_after_query_date_rows": (
            sum(
                item.list_date is not None and item.list_date > item.query_date for item in contexts
            ),
            max(total, 1),
        ),
        "identity_match_false_output_rows": (
            sum(not bool(item["identity_match"]) for item in output),
            max(output_count, 1),
        ),
        "identity_basis_invalid_rows": (
            sum(item["identity_match_basis"] not in _IDENTITY_BASES for item in output),
            max(output_count, 1),
        ),
        "unsafe_output_columns": (
            len(_UNSAFE_FIELDS.intersection(output[0] if output else ())),
            len(_UNSAFE_FIELDS),
        ),
        "lineage_invalid_rows": (
            sum(not _lineage_valid(item) for item in output),
            max(output_count, 1),
        ),
        "availability_invalid_rows": (
            sum(not _availability_valid(item, calendar_name) for item in output),
            max(output_count, 1),
        ),
        "identity_evidence_scope_invalid_rows": (
            sum(item["identity_evidence_scope"] != _IDENTITY_SCOPE for item in output),
            max(output_count, 1),
        ),
        "backtest_identity_eligible_rows": (
            sum(bool(item["backtest_identity_eligible"]) for item in output),
            max(output_count, 1),
        ),
        "row_funnel_unreconciled": (
            int(total != unresolved + output_count or funnel.accepted_source_rows != output_count),
            1,
        ),
        "unresolved_identity_count_drift": (abs(unresolved - 169), max(unresolved, 169)),
        "unexpected_quarantine_issue_rows": (
            sum(item.issue_code != "identity_evidence_unresolved" for item in quarantine),
            max(unresolved, 1),
        ),
        "unresolved_identity_rows": (unresolved, max(total, 1)),
        "sic_code_missing_rows": (
            sum(item.source.sic_code is None for item in contexts),
            max(total, 1),
        ),
        "list_date_missing_rows": (sum(item.list_date is None for item in contexts), max(total, 1)),
        "retrospective_query_without_archived_vintage_rows": (output_count, max(output_count, 1)),
    }


def _qa_results(
    table: pa.Table, metrics: Mapping[str, tuple[int, int]]
) -> tuple[QACheckResult, ...]:
    contract = TICKER_OVERVIEW_SAFE_CONTRACT
    if set(metrics) != set(contract.required_qa_checks):
        raise TickerOverviewSafeTransformError("S6 QA metric set differs from its contract")
    values = table.column("source_capture_date").to_pylist()
    unique = sorted(set(values))
    partition = (
        "source_capture_date=__empty__"
        if not unique
        else f"source_capture_date={unique[0].isoformat()}"
        if len(unique) == 1
        else "source_capture_date=__multiple__"
    )
    results: list[QACheckResult] = []
    for rule in contract.qa_rules:
        numerator, denominator = metrics[rule.check_id]
        denominator = max(numerator, denominator)
        rate = None if denominator == 0 else float(numerator / denominator)
        results.append(
            QACheckResult(
                contract.table,
                partition,
                rule.check_id,
                rule.severity,
                rule.expected_status(numerator=numerator, rate=rate),
                numerator,
                denominator,
                rate,
                rule.threshold_expression,
            )
        )
    return tuple(results)


def _as_date(value: date | str, label: str) -> date:
    if isinstance(value, datetime):
        raise TickerOverviewSafeTransformError(f"{label} must be a date, not a timestamp")
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            parsed = date.fromisoformat(value)
        except ValueError as exc:
            raise TickerOverviewSafeTransformError(f"{label} is not an ISO date") from exc
        if parsed.isoformat() == value:
            return parsed
    raise TickerOverviewSafeTransformError(f"{label} is not an exact ISO date")


def _timestamp(value: datetime | str, label: str) -> datetime:
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError as exc:
            raise TickerOverviewSafeTransformError(f"{label} is not an ISO timestamp") from exc
    elif isinstance(value, datetime):
        parsed = value
    else:
        raise TickerOverviewSafeTransformError(f"{label} is missing")
    if parsed.tzinfo is None:
        raise TickerOverviewSafeTransformError(f"{label} must be timezone-aware")
    return parsed.astimezone(UTC)


def _first_open(value: datetime, calendar_name: str) -> tuple[date, datetime]:
    try:
        calendar = xcals.get_calendar(calendar_name)
        local = value.astimezone(_NEW_YORK).date()
        sessions = calendar.sessions_in_range(
            (local - timedelta(days=1)).isoformat(),
            (local + timedelta(days=14)).isoformat(),
        )
    except Exception as exc:
        raise TickerOverviewSafeTransformError(
            f"invalid exchange calendar: {calendar_name}"
        ) from exc
    for session in sessions:
        opening = calendar.session_open(session)
        if opening > pd.Timestamp(value):
            return session.date(), opening.to_pydatetime().astimezone(UTC)
    raise TickerOverviewSafeTransformError("cannot find XNYS open after source capture")


def _duplicate_excess(rows: Iterable[Mapping[str, object]], keys: tuple[str, ...]) -> int:
    counts = Counter(tuple(row[key] for key in keys) for row in rows)
    return sum(count - 1 for count in counts.values())


def _context_duplicate_excess(rows: Iterable[_Context], keys: tuple[str, ...]) -> int:
    counts = Counter(
        tuple(getattr(row, key, getattr(row.source, key)) for key in keys) for row in rows
    )
    return sum(count - 1 for count in counts.values())


def _lineage_valid(row: Mapping[str, object]) -> bool:
    manifest_path = row["source_manifest_path"]
    artifact_path = row["source_artifact_path"]
    return (
        all(
            isinstance(row[name], str) and _SHA256.fullmatch(str(row[name]))
            for name in (
                "source_record_id",
                "source_request_id",
                "source_manifest_sha256",
                "source_artifact_sha256",
                "source_artifact_raw_sha256",
                "source_result_hash",
            )
        )
        and row["source_page_sequence"] == 0
        and row["source_row_ordinal"] == 0
        and isinstance(manifest_path, str)
        and manifest_path.startswith("manifests/massive/ticker_overview/")
        and isinstance(artifact_path, str)
        and artifact_path.startswith("bronze/massive/ticker_overview/")
        and row["source_pointer"] == f"{artifact_path}#page=0&row=0"
    )


def _availability_valid(row: Mapping[str, object], calendar_name: str) -> bool:
    session, available = _first_open(row["source_capture_at_utc"], calendar_name)  # type: ignore[arg-type]
    capture = row["source_capture_at_utc"]
    return (
        isinstance(capture, datetime)
        and row["source_capture_date"] == capture.astimezone(_NEW_YORK).date()
        and row["source_available_session"] == session
        and row["source_available_at_utc"] == available
        and row["source_availability_rule"] == _AVAILABILITY_RULE
        and row["source_availability_quality"] == _AVAILABILITY_QUALITY
    )


__all__ = [
    "TICKER_OVERVIEW_SAFE_TRANSFORM_VERSION",
    "TickerOverviewSafeInput",
    "TickerOverviewSafeTransformError",
    "TickerOverviewSafeTransformResult",
    "transform_ticker_overview_safe",
]
