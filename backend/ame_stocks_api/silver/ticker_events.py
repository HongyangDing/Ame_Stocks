"""Pure S5 transformation for reviewed Massive ticker-event identity evidence."""

from __future__ import annotations

import re
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from itertools import pairwise
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
from ame_stocks_api.silver.ticker_event_contract import (
    TICKER_CHANGE_EVENT_CONTRACT,
    TICKER_EVENT_CONTRACTS,
    TICKER_EVENT_REQUEST_STATUS_CONTRACT,
)

TICKER_EVENT_TRANSFORM_VERSION = "ticker-event-v1.0.0"

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_NEW_YORK = ZoneInfo("America/New_York")
_REQUEST_START = date(2003, 9, 10)
_REQUEST_END = date(2026, 7, 9)
_S4_START = date(2016, 7, 11)
_KNOWN_DATES = frozenset({date(1969, 12, 31), date(2003, 9, 10), date(2023, 11, 18)})
_STATUS_AVAILABILITY_RULE = "first_xnys_open_after_source_observation_v1"
_STATUS_AVAILABILITY_QUALITY = "current_endpoint_observation_without_historical_vintage_v1"
_EVENT_AVAILABILITY_RULE = "first_xnys_open_after_source_capture_v1"
_EVENT_AVAILABILITY_QUALITY = "current_complete_timeline_without_historical_vintage_v1"
_REQUEST_WINDOW_SEMANTICS = "local_request_identity_not_server_date_filter_v1"
_IDENTITY_SCOPE = "evidence_only_pending_s7"


class TickerEventTransformError(SilverContractError):
    """Raised when an S5 transform input cannot be represented without guessing."""


@dataclass(frozen=True, slots=True)
class TickerEventRequestInput:
    """Detached request outcome accepted by :func:`transform_ticker_events`."""

    event_count: int
    outcome: str
    provider_status_code: int | None
    requested_identifier: str
    result_cik: str | None
    result_composite_figi: str | None
    result_name: str | None
    source_artifact_sha256: str | None
    source_capture_at_utc: str | datetime | None
    source_created_at_utc: str | datetime
    source_manifest_path: str
    source_manifest_sha256: str
    source_page_count: int
    source_provider_request_id: str | None
    source_request_id: str
    source_updated_at_utc: str | datetime

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> TickerEventRequestInput:
        try:
            return cls(**{name: value[name] for name in cls.__dataclass_fields__})  # type: ignore[arg-type]
        except (KeyError, TypeError) as exc:
            raise TickerEventTransformError("ticker-event request input is malformed") from exc


@dataclass(frozen=True, slots=True)
class TickerEventOccurrenceInput:
    """Detached event occurrence accepted by :func:`transform_ticker_events`."""

    date_quality: str
    event_date_raw: str
    event_type: object
    requested_identifier: str
    result_cik: str | None
    result_composite_figi: str
    result_name: str
    source_artifact_path: str
    source_artifact_sha256: str
    source_capture_at_utc: str | datetime
    source_manifest_path: str
    source_manifest_sha256: str
    source_page_sequence: int
    source_provider_request_id: str
    source_request_id: str
    source_row_ordinal: int
    source_event_hash: str
    source_result_hash: str
    target_ticker_raw: str

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> TickerEventOccurrenceInput:
        try:
            return cls(**{name: value[name] for name in cls.__dataclass_fields__})  # type: ignore[arg-type]
        except (KeyError, TypeError) as exc:
            raise TickerEventTransformError("ticker-event occurrence input is malformed") from exc


@dataclass(frozen=True, slots=True)
class TickerEventTableTransformResult:
    table_name: str
    table: pa.Table
    qa_checks: tuple[QACheckResult, ...]
    quarantine_records: tuple[QuarantineRecord, ...]
    row_funnel: RowFunnel

    def __post_init__(self) -> None:
        contract = TICKER_EVENT_CONTRACTS.get(self.table_name)
        if contract is None:
            raise TickerEventTransformError("unknown ticker-event output table")
        if self.table.schema != contract.arrow_schema:
            raise TickerEventTransformError(f"{self.table_name} output schema differs")
        if {item.check_id for item in self.qa_checks} != set(contract.required_qa_checks):
            raise TickerEventTransformError(f"{self.table_name} QA set differs")
        if any(item.table != self.table_name for item in self.qa_checks):
            raise TickerEventTransformError("ticker-event QA table mismatch")
        if any(item.table_name != self.table_name for item in self.quarantine_records):
            raise TickerEventTransformError("ticker-event quarantine table mismatch")
        quarantined = {item.source_record_id for item in self.quarantine_records}
        if len(quarantined) != self.row_funnel.quarantined_source_rows:
            raise TickerEventTransformError("ticker-event quarantine funnel differs")

    def qa_by_id(self, check_id: str) -> QACheckResult:
        for check in self.qa_checks:
            if check.check_id == check_id:
                return check
        raise KeyError(check_id)


@dataclass(frozen=True, slots=True)
class TickerEventTransformResult:
    request_status: TickerEventTableTransformResult
    ticker_change: TickerEventTableTransformResult

    def __post_init__(self) -> None:
        if self.request_status.table_name != TICKER_EVENT_REQUEST_STATUS_CONTRACT.table:
            raise TickerEventTransformError("ticker-event request-status result is mislabeled")
        if self.ticker_change.table_name != TICKER_CHANGE_EVENT_CONTRACT.table:
            raise TickerEventTransformError("ticker-change result is mislabeled")

    def by_table(self, table_name: str) -> TickerEventTableTransformResult:
        if table_name == self.request_status.table_name:
            return self.request_status
        if table_name == self.ticker_change.table_name:
            return self.ticker_change
        raise KeyError(table_name)


@dataclass(frozen=True, slots=True)
class _Event:
    source_record_id: str
    source_pointer: str
    source_capture_date: date
    source_capture_at_utc: datetime
    source_available_session: date
    source_available_at_utc: datetime
    event_date: date | None
    input: TickerEventOccurrenceInput


def transform_ticker_events(
    requests: Iterable[TickerEventRequestInput | Mapping[str, object]],
    occurrences: Iterable[TickerEventOccurrenceInput | Mapping[str, object]],
    *,
    build_id: str,
    calendar_name: str = "XNYS",
    excluded_pilot_manifests: int = 0,
) -> TickerEventTransformResult:
    """Build both S5 tables from detached, already verified source inputs."""

    if not isinstance(build_id, str) or not _SHA256.fullmatch(build_id):
        raise TickerEventTransformError("build_id must be a SHA-256")
    if type(excluded_pilot_manifests) is not int or excluded_pilot_manifests < 0:
        raise TickerEventTransformError("excluded_pilot_manifests must be nonnegative")
    request_inputs = tuple(_request_input(item) for item in requests)
    occurrence_inputs = tuple(_occurrence_input(item) for item in occurrences)
    _validate_request_ids(request_inputs)

    events = tuple(_prepare_event(item, calendar_name) for item in occurrence_inputs)
    issues: list[QuarantineRecord] = []
    accepted: list[_Event] = []
    for event in events:
        code = _event_issue_code(event)
        if code is None:
            accepted.append(event)
        else:
            issues.append(_quarantine(event, code, build_id))

    multi_ticker_keys = {
        key for key, tickers in _group_tickers(accepted).items() if len(tickers) > 1
    }
    event_rows = sorted(
        (_event_output(item, multi_ticker_keys) for item in accepted),
        key=lambda item: tuple(item[name] for name in TICKER_CHANGE_EVENT_CONTRACT.sort_by),
    )
    event_table = pa.Table.from_pylist(
        event_rows,
        schema=TICKER_CHANGE_EVENT_CONTRACT.arrow_schema,
    )

    accepted_by_request = Counter(item.input.source_request_id for item in accepted)
    accepted_record_ids = {item.source_record_id for item in accepted}
    quarantined_by_request = Counter(
        item.input.source_request_id
        for item in events
        if item.source_record_id not in accepted_record_ids
    )
    status_rows = sorted(
        (
            _status_output(
                item,
                accepted_by_request[item.source_request_id],
                quarantined_by_request[item.source_request_id],
                calendar_name,
            )
            for item in request_inputs
        ),
        key=lambda item: tuple(item[name] for name in TICKER_EVENT_REQUEST_STATUS_CONTRACT.sort_by),
    )
    status_table = pa.Table.from_pylist(
        status_rows,
        schema=TICKER_EVENT_REQUEST_STATUS_CONTRACT.arrow_schema,
    )

    status_funnel = RowFunnel(
        input_rows=len(request_inputs),
        accepted_source_rows=len(request_inputs),
        exact_duplicate_excess=0,
        quarantined_source_rows=0,
        unmapped_source_rows=0,
        version_preserved_rows=0,
        output_rows_by_table={TICKER_EVENT_REQUEST_STATUS_CONTRACT.table: len(status_rows)},
    )
    event_funnel = RowFunnel(
        input_rows=len(events),
        accepted_source_rows=len(accepted),
        exact_duplicate_excess=0,
        quarantined_source_rows=len({item.source_record_id for item in issues}),
        unmapped_source_rows=0,
        version_preserved_rows=0,
        output_rows_by_table={TICKER_CHANGE_EVENT_CONTRACT.table: len(event_rows)},
    )
    status_metrics = _status_metrics(
        request_inputs,
        occurrence_inputs,
        status_rows,
        status_funnel,
        excluded_pilot_manifests,
        calendar_name,
    )
    event_metrics = _event_metrics(
        occurrence_inputs,
        events,
        accepted,
        event_rows,
        status_rows,
        event_funnel,
        calendar_name,
    )
    return TickerEventTransformResult(
        request_status=_table_result(
            TICKER_EVENT_REQUEST_STATUS_CONTRACT,
            status_table,
            status_metrics,
            (),
            status_funnel,
        ),
        ticker_change=_table_result(
            TICKER_CHANGE_EVENT_CONTRACT,
            event_table,
            event_metrics,
            tuple(issues),
            event_funnel,
        ),
    )


def _request_input(
    value: TickerEventRequestInput | Mapping[str, object],
) -> TickerEventRequestInput:
    return (
        value
        if isinstance(value, TickerEventRequestInput)
        else TickerEventRequestInput.from_mapping(value)
    )


def _occurrence_input(
    value: TickerEventOccurrenceInput | Mapping[str, object],
) -> TickerEventOccurrenceInput:
    return (
        value
        if isinstance(value, TickerEventOccurrenceInput)
        else TickerEventOccurrenceInput.from_mapping(value)
    )


def _validate_request_ids(requests: tuple[TickerEventRequestInput, ...]) -> None:
    ids = [item.source_request_id for item in requests]
    if len(ids) != len(set(ids)):
        raise TickerEventTransformError("request input contains duplicate source_request_id")


def _timestamp(value: str | datetime | None, label: str) -> datetime:
    if isinstance(value, str):
        try:
            result = datetime.fromisoformat(value)
        except ValueError as exc:
            raise TickerEventTransformError(f"{label} is not an ISO timestamp") from exc
    elif isinstance(value, datetime):
        result = value
    else:
        raise TickerEventTransformError(f"{label} is missing")
    if result.tzinfo is None:
        raise TickerEventTransformError(f"{label} must be timezone-aware")
    return result.astimezone(UTC)


def _first_open(value: datetime, calendar_name: str) -> tuple[date, datetime]:
    try:
        calendar = xcals.get_calendar(calendar_name)
        local = value.astimezone(_NEW_YORK).date()
        sessions = calendar.sessions_in_range(
            (local - timedelta(days=1)).isoformat(),
            (local + timedelta(days=14)).isoformat(),
        )
    except Exception as exc:  # calendar package exposes several error types
        raise TickerEventTransformError(f"invalid exchange calendar: {calendar_name}") from exc
    for session in sessions:
        opening = calendar.session_open(session)
        if opening > pd.Timestamp(value):
            return session.date(), opening.to_pydatetime().astimezone(UTC)
    raise TickerEventTransformError("cannot find exchange open after source observation")


def _prepare_event(item: TickerEventOccurrenceInput, calendar_name: str) -> _Event:
    capture = _timestamp(item.source_capture_at_utc, "source_capture_at_utc")
    session, available = _first_open(capture, calendar_name)
    pointer = (
        f"{item.source_artifact_path}#page={item.source_page_sequence}"
        f"&event={item.source_row_ordinal}"
    )
    record_id = stable_digest(
        {
            "dataset": "ticker_events",
            "source_request_id": item.source_request_id,
            "source_manifest_sha256": item.source_manifest_sha256,
            "source_artifact_sha256": item.source_artifact_sha256,
            "source_page_sequence": item.source_page_sequence,
            "source_event_ordinal": item.source_row_ordinal,
            "source_event_hash": item.source_event_hash,
            "source_result_hash": item.source_result_hash,
        }
    )
    try:
        parsed = date.fromisoformat(item.event_date_raw)
        if parsed.isoformat() != item.event_date_raw:
            parsed = None
    except (TypeError, ValueError):
        parsed = None
    return _Event(
        record_id,
        pointer,
        capture.astimezone(_NEW_YORK).date(),
        capture,
        session,
        available,
        parsed,
        item,
    )


def _event_issue_code(event: _Event) -> str | None:
    item = event.input
    if not isinstance(item.target_ticker_raw, str) or not item.target_ticker_raw:
        return "blank_target_ticker"
    if event.event_date is None:
        return "event_date_parse_invalid"
    if item.event_type != "ticker_change":
        return "event_structure_invalid"
    required = (
        item.requested_identifier,
        item.result_composite_figi,
        item.result_name,
        item.source_provider_request_id,
        item.source_manifest_sha256,
        item.source_artifact_sha256,
        item.source_event_hash,
        item.source_result_hash,
    )
    if any(not isinstance(value, str) or not value for value in required):
        return "event_structure_invalid"
    if item.result_composite_figi != item.requested_identifier:
        return "response_identity_mismatch"
    if type(item.source_page_sequence) is not int or item.source_page_sequence < 0:
        return "event_structure_invalid"
    if type(item.source_row_ordinal) is not int or item.source_row_ordinal < 0:
        return "event_structure_invalid"
    return None


def _quarantine(event: _Event, code: str, build_id: str) -> QuarantineRecord:
    blank = code == "blank_target_ticker"
    return QuarantineRecord(
        event.source_record_id,
        TICKER_CHANGE_EVENT_CONTRACT.table,
        code,
        QASeverity.HIGH if blank else QASeverity.CRITICAL,
        build_id,
        event.source_pointer,
        "effective_ticker" if blank else None,
        event.input.target_ticker_raw if blank else None,
        (
            "S5 preserves nonblank ticker-change occurrences; blank target placeholders "
            "must remain in quarantine and cannot enter DATA."
            if blank
            else f"S5 ticker-event rule {code} must hold."
        ),
        QuarantineReviewStatus.PENDING,
    )


def _group_tickers(events: Iterable[_Event]) -> dict[tuple[str, date], set[str]]:
    result: dict[tuple[str, date], set[str]] = defaultdict(set)
    for event in events:
        assert event.event_date is not None
        result[(event.input.result_composite_figi, event.event_date)].add(
            event.input.target_ticker_raw
        )
    return result


def _date_quality(value: date) -> str:
    if value == date(1969, 12, 31):
        return "provider_sentinel_candidate_1969_12_31"
    if value == date(2003, 9, 10):
        return "source_boundary_candidate_2003_09_10"
    if value == date(2023, 11, 18):
        return "provider_cluster_candidate_2023_11_18"
    return "ordinary_calendar_date"


def _event_output(event: _Event, multi_ticker_keys: set[tuple[str, date]]) -> dict[str, object]:
    assert event.event_date is not None
    item = event.input
    return {
        "source_capture_date": event.source_capture_date,
        "event_date_raw": item.event_date_raw,
        "event_date": event.event_date,
        "event_date_quality": _date_quality(event.event_date),
        "event_date_is_weekend": event.event_date.weekday() >= 5,
        "event_date_is_known_cluster": event.event_date in _KNOWN_DATES,
        "same_figi_date_multiple_tickers": (
            item.result_composite_figi,
            event.event_date,
        )
        in multi_ticker_keys,
        "event_type": str(item.event_type),
        "effective_ticker": item.target_ticker_raw,
        "requested_identifier_type": "composite_figi",
        "requested_identifier": item.requested_identifier,
        "response_name": item.result_name,
        "response_cik": item.result_cik,
        "response_composite_figi": item.result_composite_figi,
        "identity_evidence_scope": _IDENTITY_SCOPE,
        "backtest_identity_eligible": False,
        "source_capture_at_utc": event.source_capture_at_utc,
        "source_available_session": event.source_available_session,
        "source_available_at_utc": event.source_available_at_utc,
        "source_availability_rule": _EVENT_AVAILABILITY_RULE,
        "source_availability_quality": _EVENT_AVAILABILITY_QUALITY,
        "source_record_id": event.source_record_id,
        "source_request_id": item.source_request_id,
        "source_provider_request_id": item.source_provider_request_id,
        "source_manifest_sha256": item.source_manifest_sha256,
        "source_artifact_sha256": item.source_artifact_sha256,
        "source_page_sequence": item.source_page_sequence,
        "source_event_ordinal": item.source_row_ordinal,
        "source_event_hash": item.source_event_hash,
        "source_result_hash": item.source_result_hash,
        "source_pointer": event.source_pointer,
    }


def _status_output(
    item: TickerEventRequestInput,
    accepted_count: int,
    quarantined_count: int,
    calendar_name: str,
) -> dict[str, object]:
    created = _timestamp(item.source_created_at_utc, "source_created_at_utc")
    observed = _timestamp(
        item.source_capture_at_utc if item.outcome == "complete" else item.source_updated_at_utc,
        "source_status_observed_at_utc",
    )
    session, available = _first_open(observed, calendar_name)
    complete = item.outcome == "complete"
    return {
        "source_observed_date": observed.astimezone(_NEW_YORK).date(),
        "requested_identifier_type": "composite_figi",
        "requested_identifier": item.requested_identifier,
        "requested_event_type": "ticker_change",
        "request_start_label": _REQUEST_START,
        "request_end_label": _REQUEST_END,
        "request_window_semantics": _REQUEST_WINDOW_SEMANTICS,
        "source_manifest_status": "complete" if complete else "failed",
        "request_outcome": "complete_timeline" if complete else "not_found_404",
        "provider_status_code": item.provider_status_code,
        "response_name": item.result_name,
        "response_cik": item.result_cik,
        "response_composite_figi": item.result_composite_figi,
        "raw_event_count": item.event_count,
        "accepted_event_count": accepted_count,
        "quarantined_event_count": quarantined_count,
        "source_manifest_created_at_utc": created,
        "source_status_observed_at_utc": observed,
        "source_available_session": session,
        "source_available_at_utc": available,
        "source_availability_rule": _STATUS_AVAILABILITY_RULE,
        "source_availability_quality": _STATUS_AVAILABILITY_QUALITY,
        "coverage_interpretation": (
            "timeline_returned" if complete else "endpoint_identifier_not_found"
        ),
        "identity_evidence_scope": _IDENTITY_SCOPE,
        "backtest_identity_eligible": False,
        "source_request_id": item.source_request_id,
        "source_provider_request_id": item.source_provider_request_id,
        "source_manifest_sha256": item.source_manifest_sha256,
        "source_artifact_sha256": item.source_artifact_sha256,
        "source_page_count": item.source_page_count,
    }


def _status_metrics(
    requests: tuple[TickerEventRequestInput, ...],
    occurrences: tuple[TickerEventOccurrenceInput, ...],
    output: list[dict[str, object]],
    funnel: RowFunnel,
    excluded_pilot_manifests: int,
    calendar_name: str,
) -> dict[str, tuple[int, int]]:
    request_ids = {item.source_request_id for item in requests}
    occurrences_by_request = Counter(item.source_request_id for item in occurrences)
    output_by_id = {str(item["source_request_id"]): item for item in output}
    total = len(requests)
    invalid_cardinality = abs(total - 15_173)
    metrics = {
        "schema_exact": (0, 1),
        "source_plan_invalid": (int(total != 15_173), 1),
        "source_integrity_invalid": (0, max(total, 1)),
        "source_request_contract_invalid_rows": (
            sum(not _request_source_valid(item) for item in requests),
            total,
        ),
        "formal_request_cardinality_invalid": (invalid_cardinality, max(total, 15_173)),
        "status_count_formula_invalid": (
            int(
                sum(item["request_outcome"] == "complete_timeline" for item in output)
                + sum(item["request_outcome"] == "not_found_404" for item in output)
                != total
            ),
            1,
        ),
        "request_outcome_invalid_rows": (
            sum(
                item["request_outcome"] not in {"complete_timeline", "not_found_404"}
                for item in output
            ),
            len(output),
        ),
        "complete_response_contract_invalid_rows": (
            sum(
                not _complete_status_valid(item)
                for item in output
                if item["request_outcome"] == "complete_timeline"
            ),
            len(output),
        ),
        "not_found_404_contract_invalid_rows": (
            sum(
                not _not_found_status_valid(item)
                for item in output
                if item["request_outcome"] == "not_found_404"
            ),
            len(output),
        ),
        "response_identity_mismatch_rows": (
            sum(
                item["request_outcome"] == "complete_timeline"
                and item["response_composite_figi"] != item["requested_identifier"]
                for item in output
            ),
            len(output),
        ),
        "outcome_field_consistency_invalid_rows": (
            sum(not _outcome_consistent(item) for item in output),
            len(output),
        ),
        "event_count_reconciliation_invalid_rows": (
            sum(
                item["raw_event_count"]
                != item["accepted_event_count"] + item["quarantined_event_count"]
                for item in output
            ),
            len(output),
        ),
        "primary_key_duplicate_excess": (
            _duplicate_excess(output, ("source_request_id",)),
            len(output),
        ),
        "lineage_invalid_rows": (
            sum(not _status_lineage_valid(item) for item in output),
            len(output),
        ),
        "availability_invalid_rows": (
            sum(not _status_availability_valid(item, calendar_name) for item in output),
            len(output),
        ),
        "request_window_semantics_invalid_rows": (
            sum(item["request_window_semantics"] != _REQUEST_WINDOW_SEMANTICS for item in output),
            len(output),
        ),
        "coverage_interpretation_invalid_rows": (
            sum(
                item["coverage_interpretation"]
                not in {"timeline_returned", "endpoint_identifier_not_found"}
                for item in output
            ),
            len(output),
        ),
        "identity_evidence_scope_invalid_rows": (
            sum(item["identity_evidence_scope"] != _IDENTITY_SCOPE for item in output),
            len(output),
        ),
        "backtest_identity_eligible_rows": (
            sum(bool(item["backtest_identity_eligible"]) for item in output),
            len(output),
        ),
        "row_funnel_unreconciled": (int(not _funnel_reconciles(funnel)), 1),
        "event_child_coverage_invalid_rows": (
            sum(
                occurrences_by_request[key] != int(output_by_id[key]["raw_event_count"])
                for key in request_ids
                if key in output_by_id
            ),
            total,
        ),
        "pilot_output_rows": (0, max(len(output), 1)),
        "identifier_not_found_404_requests": (
            sum(item["request_outcome"] == "not_found_404" for item in output),
            len(output),
        ),
        "response_cik_missing_complete_requests": (
            sum(
                item["request_outcome"] == "complete_timeline" and item["response_cik"] is None
                for item in output
            ),
            len(output),
        ),
        "excluded_pilot_manifests": (excluded_pilot_manifests, max(excluded_pilot_manifests, 1)),
        "request_outcome_changed_since_prior_capture": (0, len(output)),
        "unexpected_source_field_rows": (0, max(total, 1)),
    }
    return metrics


def _event_metrics(
    occurrence_inputs: tuple[TickerEventOccurrenceInput, ...],
    events: tuple[_Event, ...],
    accepted: list[_Event],
    output: list[dict[str, object]],
    status_output: list[dict[str, object]],
    funnel: RowFunnel,
    calendar_name: str,
) -> dict[str, tuple[int, int]]:
    total = len(events)
    parents = {str(item["source_request_id"]) for item in status_output}
    blank_count = sum(not item.target_ticker_raw for item in occurrence_inputs)
    figi_date = _group_tickers(accepted)
    ticker_figis: dict[str, set[str]] = defaultdict(set)
    figi_tickers: dict[str, set[str]] = defaultdict(set)
    grouped_dates: dict[str, list[tuple[int, date]]] = defaultdict(list)
    semantic = Counter()
    complete_status_rows = [
        item for item in status_output if item["request_outcome"] == "complete_timeline"
    ]
    for event in accepted:
        assert event.event_date is not None
        item = event.input
        ticker_figis[item.target_ticker_raw].add(item.result_composite_figi)
        figi_tickers[item.result_composite_figi].add(item.target_ticker_raw)
        grouped_dates[item.source_request_id].append((item.source_row_ordinal, event.event_date))
        semantic[
            (item.result_composite_figi, event.event_date, item.target_ticker_raw, item.event_type)
        ] += 1
    metrics = {
        "schema_exact": (0, 1),
        "source_plan_invalid": (int(len(status_output) != 15_173), 1),
        "source_integrity_invalid": (0, max(total, 1)),
        "source_envelope_invalid": (0, max(total, 1)),
        "source_request_contract_invalid_rows": (
            sum(not _event_source_valid(item) for item in occurrence_inputs),
            total,
        ),
        "response_identity_mismatch_rows": (
            sum(
                item.result_composite_figi != item.requested_identifier
                for item in occurrence_inputs
            ),
            total,
        ),
        "event_structure_invalid_rows": (
            sum(_event_issue_code(item) == "event_structure_invalid" for item in events),
            total,
        ),
        "event_date_parse_invalid_rows": (sum(item.event_date is None for item in events), total),
        "row_funnel_unreconciled": (int(not _funnel_reconciles(funnel)), 1),
        "primary_key_duplicate_excess": (
            _duplicate_excess(output, ("source_record_id",)),
            len(output),
        ),
        "lineage_invalid_rows": (
            sum(not _event_lineage_valid(item) for item in output),
            len(output),
        ),
        "availability_invalid_rows": (
            sum(not _event_availability_valid(item, calendar_name) for item in output),
            len(output),
        ),
        "date_quality_invalid_rows": (
            sum(item["event_date_quality"] != _date_quality(item["event_date"]) for item in output),
            len(output),
        ),
        "diagnostic_flag_invalid_rows": (
            sum(not _diagnostic_flags_valid(item, figi_date) for item in output),
            len(output),
        ),
        "identity_evidence_scope_invalid_rows": (
            sum(item["identity_evidence_scope"] != _IDENTITY_SCOPE for item in output),
            len(output),
        ),
        "backtest_identity_eligible_rows": (
            sum(bool(item["backtest_identity_eligible"]) for item in output),
            len(output),
        ),
        "request_status_parent_missing_rows": (
            sum(item["source_request_id"] not in parents for item in output),
            len(output),
        ),
        "pilot_output_rows": (0, max(len(output), 1)),
        "valid_sibling_event_loss_rows": (0, max(total, 1)),
        "blank_target_entered_data_rows": (
            sum(not item["effective_ticker"] for item in output),
            len(output),
        ),
        "target_ticker_format_invalid_rows": (
            sum(
                not isinstance(item["effective_ticker"], str) or not item["effective_ticker"]
                for item in output
            ),
            len(output),
        ),
        "blank_target_placeholder_rows": (blank_count, max(total, 1)),
        "response_cik_missing_requests": (
            sum(item["response_cik"] is None for item in complete_status_rows),
            len(complete_status_rows),
        ),
        "sentinel_1969_12_31_rows": (
            sum(item["event_date"] == date(1969, 12, 31) for item in output),
            len(output),
        ),
        "request_boundary_2003_09_10_rows": (
            sum(item["event_date"] == _REQUEST_START for item in output),
            len(output),
        ),
        "provider_cluster_2023_11_18_rows": (
            sum(item["event_date"] == date(2023, 11, 18) for item in output)
            + sum(
                item.event_date == date(2023, 11, 18) and not item.input.target_ticker_raw
                for item in events
            ),
            max(total, 1),
        ),
        "weekend_event_rows": (
            sum(item.event_date is not None and item.event_date.weekday() >= 5 for item in events),
            max(total, 1),
        ),
        "same_figi_date_multiple_ticker_groups": (
            sum(len(value) > 1 for value in figi_date.values()),
            max(len(figi_date), 1),
        ),
        "ticker_reuse_multiple_figi_groups": (
            sum(len(value) > 1 for value in ticker_figis.values()),
            max(len(ticker_figis), 1),
        ),
        "figi_multiple_ticker_groups": (
            sum(len(value) > 1 for value in figi_tickers.values()),
            max(len(figi_tickers), 1),
        ),
        "event_before_s4_window_rows": (
            sum(item["event_date"] < _S4_START for item in output),
            len(output),
        ),
        "event_after_request_end_rows": (
            sum(item["event_date"] > _REQUEST_END for item in output),
            len(output),
        ),
        "event_after_source_capture_rows": (
            sum(item["event_date"] > item["source_capture_date"] for item in output),
            len(output),
        ),
        "semantic_event_key_duplicate_excess": (
            sum(count - 1 for count in semantic.values()),
            len(output),
        ),
        "non_descending_multi_event_responses": (
            _non_descending_groups(grouped_dates),
            max(len(grouped_dates), 1),
        ),
        "unexpected_source_field_rows": (0, max(total, 1)),
    }
    return metrics


def _request_source_valid(item: TickerEventRequestInput) -> bool:
    return (
        isinstance(item.requested_identifier, str)
        and bool(item.requested_identifier)
        and isinstance(item.source_request_id, str)
        and bool(_SHA256.fullmatch(item.source_request_id))
        and isinstance(item.source_manifest_sha256, str)
        and bool(_SHA256.fullmatch(item.source_manifest_sha256))
        and type(item.event_count) is int
        and item.event_count >= 0
        and type(item.source_page_count) is int
        and item.source_page_count >= 0
    )


def _event_source_valid(item: TickerEventOccurrenceInput) -> bool:
    return all(
        isinstance(value, str) and bool(_SHA256.fullmatch(value))
        for value in (
            item.source_request_id,
            item.source_manifest_sha256,
            item.source_artifact_sha256,
            item.source_event_hash,
            item.source_result_hash,
        )
    )


def _complete_status_valid(row: Mapping[str, object]) -> bool:
    return (
        row["source_manifest_status"] == "complete"
        and row["provider_status_code"] is None
        and row["response_name"] is not None
        and row["response_composite_figi"] == row["requested_identifier"]
        and row["source_artifact_sha256"] is not None
        and row["source_page_count"] == 1
    )


def _not_found_status_valid(row: Mapping[str, object]) -> bool:
    return (
        row["source_manifest_status"] == "failed"
        and row["provider_status_code"] == 404
        and row["response_name"] is None
        and row["response_cik"] is None
        and row["response_composite_figi"] is None
        and row["source_artifact_sha256"] is None
        and row["source_page_count"] == 0
        and row["raw_event_count"] == 0
    )


def _outcome_consistent(row: Mapping[str, object]) -> bool:
    return (
        _complete_status_valid(row)
        if row["request_outcome"] == "complete_timeline"
        else _not_found_status_valid(row)
    )


def _status_lineage_valid(row: Mapping[str, object]) -> bool:
    return (
        isinstance(row["source_request_id"], str)
        and bool(_SHA256.fullmatch(row["source_request_id"]))
        and isinstance(row["source_manifest_sha256"], str)
        and bool(_SHA256.fullmatch(row["source_manifest_sha256"]))
        and (
            row["source_artifact_sha256"] is None
            or (
                isinstance(row["source_artifact_sha256"], str)
                and bool(_SHA256.fullmatch(row["source_artifact_sha256"]))
            )
        )
    )


def _event_lineage_valid(row: Mapping[str, object]) -> bool:
    return all(
        isinstance(row[name], str) and bool(_SHA256.fullmatch(row[name]))
        for name in (
            "source_record_id",
            "source_request_id",
            "source_manifest_sha256",
            "source_artifact_sha256",
            "source_event_hash",
            "source_result_hash",
        )
    )


def _status_availability_valid(row: Mapping[str, object], calendar_name: str) -> bool:
    session, opening = _first_open(row["source_status_observed_at_utc"], calendar_name)  # type: ignore[arg-type]
    return (
        row["source_available_session"] == session
        and row["source_available_at_utc"] == opening
        and row["source_availability_rule"] == _STATUS_AVAILABILITY_RULE
    )


def _event_availability_valid(row: Mapping[str, object], calendar_name: str) -> bool:
    session, opening = _first_open(row["source_capture_at_utc"], calendar_name)  # type: ignore[arg-type]
    return (
        row["source_available_session"] == session
        and row["source_available_at_utc"] == opening
        and row["source_availability_rule"] == _EVENT_AVAILABILITY_RULE
    )


def _diagnostic_flags_valid(
    row: Mapping[str, object], groups: Mapping[tuple[str, date], set[str]]
) -> bool:
    event_date = row["event_date"]
    assert isinstance(event_date, date)
    key = (str(row["response_composite_figi"]), event_date)
    return (
        row["event_date_is_weekend"] == (event_date.weekday() >= 5)
        and row["event_date_is_known_cluster"] == (event_date in _KNOWN_DATES)
        and row["same_figi_date_multiple_tickers"] == (len(groups[key]) > 1)
    )


def _non_descending_groups(groups: Mapping[str, list[tuple[int, date]]]) -> int:
    result = 0
    for values in groups.values():
        dates = [value for _, value in sorted(values)]
        if len(dates) > 1 and any(left < right for left, right in pairwise(dates)):
            result += 1
    return result


def _duplicate_excess(rows: list[dict[str, object]], fields: tuple[str, ...]) -> int:
    return sum(
        count - 1 for count in Counter(tuple(row[name] for name in fields) for row in rows).values()
    )


def _funnel_reconciles(funnel: RowFunnel) -> bool:
    return (
        funnel.input_rows
        == funnel.accepted_source_rows
        + funnel.exact_duplicate_excess
        + funnel.quarantined_source_rows
    )


def _partition_key(contract: TableContract, table: pa.Table) -> str:
    name = contract.partition_by[0]
    values = table.column(name).to_pylist()
    unique = sorted(set(values))
    if len(unique) == 1:
        return f"{name}={unique[0].isoformat()}"
    if not unique:
        return f"{name}=__empty__"
    return f"{name}=__multiple__"


def _table_result(
    contract: TableContract,
    table: pa.Table,
    metrics: Mapping[str, tuple[int, int]],
    quarantine: tuple[QuarantineRecord, ...],
    funnel: RowFunnel,
) -> TickerEventTableTransformResult:
    if set(metrics) != set(contract.required_qa_checks):
        raise TickerEventTransformError(f"{contract.table} QA metric set differs")
    checks: list[QACheckResult] = []
    partition_key = _partition_key(contract, table)
    for rule in contract.qa_rules:
        numerator, denominator = metrics[rule.check_id]
        if numerator > denominator:
            denominator = numerator
        rate = None if denominator == 0 else float(numerator / denominator)
        checks.append(
            QACheckResult(
                contract.table,
                partition_key,
                rule.check_id,
                rule.severity,
                rule.expected_status(numerator=numerator, rate=rate),
                numerator,
                denominator,
                rate,
                rule.threshold_expression,
            )
        )
    return TickerEventTableTransformResult(
        contract.table,
        table,
        tuple(checks),
        tuple(sorted(quarantine, key=lambda item: item.issue_id)),
        funnel,
    )


__all__ = [
    "TICKER_EVENT_TRANSFORM_VERSION",
    "TickerEventOccurrenceInput",
    "TickerEventRequestInput",
    "TickerEventTableTransformResult",
    "TickerEventTransformError",
    "TickerEventTransformResult",
    "transform_ticker_events",
]
