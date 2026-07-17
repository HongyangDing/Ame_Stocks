"""Deterministic streaming engine for the S7 Composite-FIGI inventory.

``asset_observation_daily`` is the only inventory authority.  The paired
``universe_source_daily`` partition is read solely to prove that every selected
membership row still points to the exact S4 parent and preserves its projected
values.  Universe rows therefore never increment inventory counts or lineage.

The engine deliberately has no Arrow, filesystem, publication, adjudication, or
market-classification capability.  It accepts small ordered batches and retains
only aggregate state plus the two maps required to reconcile the current day.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import date
from typing import Final

ASSET_AUTHORITY_TABLE: Final = "asset_observation_daily"
UNIVERSE_RECONCILIATION_TABLE: Final = "universe_source_daily"
LINEAGE_RULE_VERSION: Final = "s7_composite_inventory_source_record_lineage_v1"
SCAN_ORDER_RULE: Final = "artifact_path_asc,row_group_asc,row_index_asc"

FIGI_INVALID_REASON_SUFFIX_PRECEDENCE: Final = (
    "null",
    "empty",
    "whitespace_only",
    "surrounding_whitespace",
    "length_not_12",
    "non_upper_ascii_alnum",
    "prefix_not_BBG",
)
COMPOSITE_FIGI_INVALID_REASON_PRECEDENCE: Final = tuple(
    f"composite_figi_{suffix}" for suffix in FIGI_INVALID_REASON_SUFFIX_PRECEDENCE
)
SHARE_CLASS_FIGI_INVALID_REASON_PRECEDENCE: Final = tuple(
    f"share_class_figi_{suffix}" for suffix in FIGI_INVALID_REASON_SUFFIX_PRECEDENCE
)

_SOURCE_RECORD_ID = re.compile(r"^[0-9a-f]{64}$")
_UPPER_ASCII_ALNUM = re.compile(r"^[0-9A-Z]+$")

# Every pair is (asset_observation_daily field, universe_source_daily field).
# The two source-availability timelines intentionally are not compared: the
# asset row is available after its individual request, while universe membership
# is available only after the complete active/inactive pair.
UNIVERSE_PARENT_PROJECTION: Final = (
    ("session_year", "session_year"),
    ("session_date", "session_date"),
    ("requested_active", "active_on_date"),
    ("ticker", "ticker"),
    ("provider_active", "active_on_date"),
    ("type_code", "type_code"),
    ("name", "name"),
    ("market", "market"),
    ("locale", "locale"),
    ("primary_exchange_mic", "primary_exchange_mic"),
    ("currency_name", "currency_name"),
    ("cik", "cik"),
    ("composite_figi", "composite_figi"),
    ("share_class_figi", "share_class_figi"),
    ("delisted_at_utc", "delisted_at_utc"),
    ("last_updated_at_utc", "last_updated_at_utc"),
    ("reference_time_scope", "reference_time_scope"),
    ("metadata_time_scope", "metadata_time_scope"),
    ("source_capture_at_utc", "selected_source_capture_at_utc"),
    ("source_availability_quality", "source_availability_quality"),
    ("source_record_id", "selected_source_record_id"),
    ("source_request_id", "source_request_id"),
    ("source_provider_request_id", "source_provider_request_id"),
    ("source_artifact_sha256", "source_artifact_sha256"),
    ("source_page_sequence", "source_page_sequence"),
    ("source_row_ordinal", "source_row_ordinal"),
    ("source_row_hash", "source_row_hash"),
)


class CompositeInventoryError(RuntimeError):
    """Raised when source order, cardinality, or S4 reconciliation is invalid."""

    def __init__(self, check_id: str, message: str) -> None:
        super().__init__(f"{check_id}: {message}")
        self.check_id = check_id


@dataclass(frozen=True, slots=True)
class CompositeInventoryCaps:
    """Semantic cardinality caps enforced while streaming the authority table."""

    max_distinct_composite_figis: int = 100_000
    max_distinct_composite_share_class_pairs: int = 250_000
    bounded_example_limit: int = 20

    def __post_init__(self) -> None:
        for name, value in (
            ("max_distinct_composite_figis", self.max_distinct_composite_figis),
            (
                "max_distinct_composite_share_class_pairs",
                self.max_distinct_composite_share_class_pairs,
            ),
            ("bounded_example_limit", self.bounded_example_limit),
        ):
            if type(value) is not int or value <= 0:
                raise CompositeInventoryError("resource_caps_invalid", f"{name} must be positive")


@dataclass(frozen=True, slots=True)
class InvalidFigiExample:
    table: str
    field: str
    reason: str
    session_date: date
    artifact_path: str
    artifact_sha256: str
    row_group: int
    row_index_in_group: int
    provider_active: bool
    ticker: str
    source_record_id: str
    observed_value: str | None

    def to_dict(self) -> dict[str, object]:
        return {
            "table": self.table,
            "field": self.field,
            "reason": self.reason,
            "session_date": self.session_date.isoformat(),
            "artifact_path": self.artifact_path,
            "artifact_sha256": self.artifact_sha256,
            "row_group": self.row_group,
            "row_index_in_group": self.row_index_in_group,
            "provider_active": self.provider_active,
            "ticker": self.ticker,
            "source_record_id": self.source_record_id,
            "observed_value": self.observed_value,
        }


@dataclass(frozen=True, slots=True)
class CompositeInventoryRecord:
    observed_composite_figi: str
    observed_share_class_figis: tuple[str, ...]
    share_class_conflict: bool
    first_session: date
    last_session: date
    active_row_count: int
    inactive_row_count: int
    session_count: int
    ticker_count: int
    provider_locale_count: int
    provider_market_count: int
    primary_exchange_count: int
    parent_table_count: int
    source_release_count: int
    source_record_lineage_digest: str

    def to_dict(self) -> dict[str, object]:
        """Return a contract-shaped record while retaining native date values."""

        return {
            "observed_composite_figi": self.observed_composite_figi,
            "observed_share_class_figis": list(self.observed_share_class_figis),
            "share_class_conflict": self.share_class_conflict,
            "first_session": self.first_session,
            "last_session": self.last_session,
            "active_row_count": self.active_row_count,
            "inactive_row_count": self.inactive_row_count,
            "session_count": self.session_count,
            "ticker_count": self.ticker_count,
            "provider_locale_count": self.provider_locale_count,
            "provider_market_count": self.provider_market_count,
            "primary_exchange_count": self.primary_exchange_count,
            "parent_table_count": self.parent_table_count,
            "source_release_count": self.source_release_count,
            "source_record_lineage_digest": self.source_record_lineage_digest,
        }


@dataclass(frozen=True, slots=True)
class CompositeInventoryDiagnostics:
    authority_row_count: int
    reconciliation_row_count: int
    authority_universe_row_count_difference: int
    nonselected_authority_row_count: int
    completed_session_count: int
    valid_composite_row_count: int
    invalid_composite_reason_counts: tuple[tuple[str, int], ...]
    invalid_share_class_reason_counts: tuple[tuple[str, int], ...]
    share_class_conflict_groups: int
    distinct_composite_share_class_pair_count: int
    bounded_invalid_examples: tuple[InvalidFigiExample, ...]

    @property
    def invalid_composite_figi_rows(self) -> int:
        return sum(count for _, count in self.invalid_composite_reason_counts)

    @property
    def invalid_share_class_figi_rows(self) -> int:
        return sum(count for _, count in self.invalid_share_class_reason_counts)

    def to_dict(self) -> dict[str, object]:
        return {
            "authority_row_count": self.authority_row_count,
            "reconciliation_row_count": self.reconciliation_row_count,
            "authority_universe_row_count_difference": (
                self.authority_universe_row_count_difference
            ),
            "nonselected_authority_row_count": self.nonselected_authority_row_count,
            "completed_session_count": self.completed_session_count,
            "valid_composite_row_count": self.valid_composite_row_count,
            "invalid_composite_figi_rows": self.invalid_composite_figi_rows,
            "invalid_composite_reason_counts": dict(self.invalid_composite_reason_counts),
            "invalid_share_class_figi_rows": self.invalid_share_class_figi_rows,
            "invalid_share_class_reason_counts": dict(self.invalid_share_class_reason_counts),
            "share_class_conflict_groups": self.share_class_conflict_groups,
            "distinct_composite_share_class_pair_count": (
                self.distinct_composite_share_class_pair_count
            ),
            "bounded_invalid_examples": [
                example.to_dict() for example in self.bounded_invalid_examples
            ],
        }


@dataclass(frozen=True, slots=True)
class CompositeInventoryResult:
    authority_release_id: str
    lineage_rule_version: str
    scan_order_rule: str
    records: tuple[CompositeInventoryRecord, ...]
    diagnostics: CompositeInventoryDiagnostics

    def output_rows(self) -> list[dict[str, object]]:
        return [record.to_dict() for record in self.records]


@dataclass(slots=True)
class _Aggregate:
    first_session: date
    last_session: date
    last_counted_session: date
    lineage: object
    active_row_count: int = 0
    inactive_row_count: int = 0
    session_count: int = 0
    tickers: set[str] = field(default_factory=set)
    locales: set[str] = field(default_factory=set)
    markets: set[str] = field(default_factory=set)
    primary_exchanges: set[str] = field(default_factory=set)
    share_class_figis: set[str] = field(default_factory=set)


@dataclass(frozen=True, slots=True)
class _AssetProjection:
    source_record_id: str
    ticker: str
    values: tuple[object, ...]


@dataclass(frozen=True, slots=True)
class _PhysicalLocation:
    artifact_path: str
    artifact_sha256: str
    row_group: int
    row_index_in_group: int

    @property
    def scan_key(self) -> tuple[str, int, int]:
        return (self.artifact_path, self.row_group, self.row_index_in_group)


class _BoundedExamples:
    def __init__(self, limit: int) -> None:
        self._limit = limit
        self._items: dict[str, list[InvalidFigiExample]] = {}

    def add(self, example: InvalidFigiExample) -> None:
        items = self._items.setdefault(example.reason, [])
        if len(items) < self._limit:
            items.append(example)

    def freeze(self) -> tuple[InvalidFigiExample, ...]:
        precedence = (
            *COMPOSITE_FIGI_INVALID_REASON_PRECEDENCE,
            *SHARE_CLASS_FIGI_INVALID_REASON_PRECEDENCE,
        )
        return tuple(example for reason in precedence for example in self._items.get(reason, ()))


class CompositeInventoryEngine:
    """Consume exact S4 daily partitions in deterministic physical scan order."""

    def __init__(
        self,
        *,
        authority_release_id: str,
        caps: CompositeInventoryCaps | None = None,
    ) -> None:
        _require_digest(authority_release_id, "authority_release_id")
        self._authority_release_id = authority_release_id
        self._caps = caps or CompositeInventoryCaps()
        self._lineage_seed = _lineage_seed(authority_release_id)
        self._aggregates: dict[str, _Aggregate] = {}
        self._pair_count = 0
        self._invalid_composite: Counter[str] = Counter()
        self._invalid_share: Counter[str] = Counter()
        self._examples = _BoundedExamples(self._caps.bounded_example_limit)
        self._authority_rows = 0
        self._reconciliation_rows = 0
        self._nonselected_authority_rows = 0
        self._valid_composite_rows = 0
        self._completed_sessions = 0
        self._current_session: date | None = None
        self._last_completed_session: date | None = None
        self._asset_rows: dict[str, _AssetProjection] = {}
        self._universe_rows: dict[str, Mapping[str, object]] = {}
        self._universe_tickers: set[str] = set()
        self._last_asset_row_key: tuple[str, int, int] | None = None
        self._last_universe_row_key: tuple[str, int, int] | None = None
        self._asset_phase_started = False
        self._universe_phase_started = False
        self._finalized = False
        self._faulted = False

    def start_session(self, session_date: date) -> None:
        self._require_usable()
        if type(session_date) is not date:
            self._fail("source_session_invalid", "session_date must be a native date")
        if self._current_session is not None:
            self._fail("source_session_invalid", "previous session has not been finished")
        if (
            self._last_completed_session is not None
            and session_date <= self._last_completed_session
        ):
            self._fail("source_scan_order_invalid", "sessions must be strictly increasing")
        self._current_session = session_date
        self._asset_rows = {}
        self._universe_rows = {}
        self._universe_tickers = set()
        self._last_asset_row_key = None
        self._last_universe_row_key = None
        self._asset_phase_started = False
        self._universe_phase_started = False

    def consume_asset_batch(
        self,
        rows: Iterable[Mapping[str, object]],
        *,
        artifact_path: str,
        artifact_sha256: str,
        row_group: int,
        row_index_base: int,
    ) -> None:
        self._require_open_session()
        if self._universe_phase_started:
            self._fail("source_binding_invalid", "authority phase is already sealed")
        self._asset_phase_started = True
        path, sha256, group, base = self._validate_batch_locator(
            artifact_path, artifact_sha256, row_group, row_index_base, ASSET_AUTHORITY_TABLE
        )
        try:
            for offset, row in enumerate(rows):
                location = _PhysicalLocation(path, sha256, group, base + offset)
                self._last_asset_row_key = self._advance_scan_key(
                    location.scan_key,
                    previous=self._last_asset_row_key,
                    table=ASSET_AUTHORITY_TABLE,
                )
                self._consume_asset_row(row, location)
        except CompositeInventoryError:
            self._faulted = True
            raise

    def consume_universe_batch(
        self,
        rows: Iterable[Mapping[str, object]],
        *,
        artifact_path: str,
        artifact_sha256: str,
        row_group: int,
        row_index_base: int,
    ) -> None:
        self._require_open_session()
        if not self._asset_phase_started:
            self._fail("source_binding_invalid", "authority phase must precede universe phase")
        self._universe_phase_started = True
        path, sha256, group, base = self._validate_batch_locator(
            artifact_path,
            artifact_sha256,
            row_group,
            row_index_base,
            UNIVERSE_RECONCILIATION_TABLE,
        )
        try:
            for offset, row in enumerate(rows):
                location = _PhysicalLocation(path, sha256, group, base + offset)
                self._last_universe_row_key = self._advance_scan_key(
                    location.scan_key,
                    previous=self._last_universe_row_key,
                    table=UNIVERSE_RECONCILIATION_TABLE,
                )
                self._consume_universe_row(row)
        except CompositeInventoryError:
            self._faulted = True
            raise

    def finish_session(self) -> None:
        self._require_open_session()
        assert self._current_session is not None
        try:
            if not self._asset_phase_started or not self._universe_phase_started:
                self._fail(
                    "session_spine_mismatch",
                    "each session must consume both authority and reconciliation partitions",
                )
            if len(self._universe_rows) > len(self._asset_rows):
                self._fail(
                    "source_count_mismatch",
                    "daily universe row count exceeds daily authority row count",
                )
            for source_id, universe in self._universe_rows.items():
                asset = self._asset_rows.get(source_id)
                if asset is None:
                    self._fail(
                        "universe_parent_missing",
                        f"selected source_record_id {source_id} has no same-session parent",
                    )
                universe_values = tuple(
                    _required(universe, universe_field, UNIVERSE_RECONCILIATION_TABLE)
                    for _, universe_field in UNIVERSE_PARENT_PROJECTION
                )
                if any(
                    not _exact_equal(left, right)
                    for left, right in zip(asset.values, universe_values, strict=True)
                ):
                    mismatches = [
                        f"{asset_field}->{universe_field}"
                        for (asset_field, universe_field), left, right in zip(
                            UNIVERSE_PARENT_PROJECTION,
                            asset.values,
                            universe_values,
                            strict=True,
                        )
                        if not _exact_equal(left, right)
                    ]
                    self._fail(
                        "universe_projection_mismatch",
                        f"selected parent {source_id} differs at {mismatches}",
                    )
            self._nonselected_authority_rows += len(self._asset_rows) - len(self._universe_rows)
            self._last_completed_session = self._current_session
            self._completed_sessions += 1
            self._current_session = None
            self._asset_rows = {}
            self._universe_rows = {}
            self._universe_tickers = set()
            self._last_asset_row_key = None
            self._last_universe_row_key = None
            self._asset_phase_started = False
            self._universe_phase_started = False
        except CompositeInventoryError:
            self._faulted = True
            raise

    def finalize(self) -> CompositeInventoryResult:
        self._require_usable()
        if self._current_session is not None:
            self._fail("source_session_invalid", "current session has not been finished")
        self._finalized = True
        records = tuple(
            self._freeze_record(figi, aggregate)
            for figi, aggregate in sorted(self._aggregates.items())
        )
        diagnostics = CompositeInventoryDiagnostics(
            authority_row_count=self._authority_rows,
            reconciliation_row_count=self._reconciliation_rows,
            authority_universe_row_count_difference=(
                self._authority_rows - self._reconciliation_rows
            ),
            nonselected_authority_row_count=self._nonselected_authority_rows,
            completed_session_count=self._completed_sessions,
            valid_composite_row_count=self._valid_composite_rows,
            invalid_composite_reason_counts=_freeze_reason_counts(
                self._invalid_composite,
                COMPOSITE_FIGI_INVALID_REASON_PRECEDENCE,
            ),
            invalid_share_class_reason_counts=_freeze_reason_counts(
                self._invalid_share,
                SHARE_CLASS_FIGI_INVALID_REASON_PRECEDENCE,
            ),
            share_class_conflict_groups=sum(
                len(item.share_class_figis) > 1 for item in self._aggregates.values()
            ),
            distinct_composite_share_class_pair_count=self._pair_count,
            bounded_invalid_examples=self._examples.freeze(),
        )
        return CompositeInventoryResult(
            authority_release_id=self._authority_release_id,
            lineage_rule_version=LINEAGE_RULE_VERSION,
            scan_order_rule=SCAN_ORDER_RULE,
            records=records,
            diagnostics=diagnostics,
        )

    def _consume_asset_row(
        self,
        row: Mapping[str, object],
        location: _PhysicalLocation,
    ) -> None:
        if not isinstance(row, Mapping):
            self._fail("asset_source_row_invalid", "authority row must be a mapping")
        session = _native_date(_required(row, "session_date", ASSET_AUTHORITY_TABLE))
        if session != self._current_session:
            self._fail("asset_source_session_mismatch_rows", "authority row is in wrong session")
        source_id = _require_digest(
            _required(row, "source_record_id", ASSET_AUTHORITY_TABLE),
            "source_record_id",
        )
        if source_id in self._asset_rows:
            self._fail(
                "authority_source_record_duplicate",
                f"duplicate same-session source_record_id {source_id}",
            )
        ticker = _required_text(row, "ticker", ASSET_AUTHORITY_TABLE)
        requested_active = _required_bool(row, "requested_active", ASSET_AUTHORITY_TABLE)
        provider_active = _required_bool(row, "provider_active", ASSET_AUTHORITY_TABLE)
        if requested_active is not provider_active:
            self._fail(
                "requested_provider_active_mismatch",
                f"source_record_id {source_id} requested/provider active differs",
            )

        projection = tuple(
            _required(row, asset_field, ASSET_AUTHORITY_TABLE)
            for asset_field, _ in UNIVERSE_PARENT_PROJECTION
        )
        self._asset_rows[source_id] = _AssetProjection(source_id, ticker, projection)
        self._authority_rows += 1

        composite = _optional_figi_text(row, "composite_figi", ASSET_AUTHORITY_TABLE)
        composite_reason = figi_invalid_reason(composite, field="composite_figi")
        if composite_reason is not None:
            self._record_invalid(
                field="composite_figi",
                reason=composite_reason,
                value=composite,
                session=session,
                ticker=ticker,
                source_id=source_id,
                provider_active=provider_active,
                location=location,
            )
            return
        assert composite is not None
        self._valid_composite_rows += 1
        share = _optional_figi_text(row, "share_class_figi", ASSET_AUTHORITY_TABLE)
        share_reason = figi_invalid_reason(share, field="share_class_figi")
        if share_reason is not None:
            self._record_invalid(
                field="share_class_figi",
                reason=share_reason,
                value=share,
                session=session,
                ticker=ticker,
                source_id=source_id,
                provider_active=provider_active,
                location=location,
            )

        aggregate = self._aggregates.get(composite)
        if aggregate is None:
            if len(self._aggregates) >= self._caps.max_distinct_composite_figis:
                self._fail(
                    "resource_cap_exceeded",
                    "authority input exceeds the approved Composite-FIGI cap",
                )
            aggregate = _Aggregate(
                first_session=session,
                last_session=session,
                last_counted_session=session,
                lineage=hashlib.sha256(self._lineage_seed),
                session_count=1,
            )
            self._aggregates[composite] = aggregate
        elif aggregate.last_counted_session != session:
            aggregate.last_counted_session = session
            aggregate.session_count += 1
            aggregate.last_session = session

        if provider_active:
            aggregate.active_row_count += 1
        else:
            aggregate.inactive_row_count += 1
        aggregate.tickers.add(ticker)
        _add_non_null_text(aggregate.locales, row, "locale", ASSET_AUTHORITY_TABLE)
        _add_non_null_text(aggregate.markets, row, "market", ASSET_AUTHORITY_TABLE)
        _add_non_null_text(
            aggregate.primary_exchanges,
            row,
            "primary_exchange_mic",
            ASSET_AUTHORITY_TABLE,
        )
        if share_reason is None:
            assert share is not None
            if share not in aggregate.share_class_figis:
                if self._pair_count >= self._caps.max_distinct_composite_share_class_pairs:
                    self._fail(
                        "resource_cap_exceeded",
                        "authority input exceeds the approved Composite/Share-Class pair cap",
                    )
                aggregate.share_class_figis.add(share)
                self._pair_count += 1
        aggregate.lineage.update(bytes.fromhex(source_id))

    def _consume_universe_row(self, row: Mapping[str, object]) -> None:
        if not isinstance(row, Mapping):
            self._fail("universe_source_row_invalid", "universe row must be a mapping")
        session = _native_date(_required(row, "session_date", UNIVERSE_RECONCILIATION_TABLE))
        if session != self._current_session:
            self._fail("universe_source_session_mismatch_rows", "universe row is in wrong session")
        source_id = _require_digest(
            _required(row, "selected_source_record_id", UNIVERSE_RECONCILIATION_TABLE),
            "selected_source_record_id",
        )
        if source_id in self._universe_rows:
            self._fail(
                "universe_selected_source_record_duplicate",
                f"duplicate selected_source_record_id {source_id}",
            )
        ticker = _required_text(row, "ticker", UNIVERSE_RECONCILIATION_TABLE)
        if ticker in self._universe_tickers:
            self._fail(
                "universe_duplicate_ticker_rows", f"duplicate same-session ticker {ticker!r}"
            )
        self._universe_rows[source_id] = row
        self._universe_tickers.add(ticker)
        self._reconciliation_rows += 1

    def _record_invalid(
        self,
        *,
        field: str,
        reason: str,
        value: str | None,
        session: date,
        ticker: str,
        source_id: str,
        provider_active: bool,
        location: _PhysicalLocation,
    ) -> None:
        counter = self._invalid_composite if field == "composite_figi" else self._invalid_share
        counter[reason] += 1
        self._examples.add(
            InvalidFigiExample(
                table=ASSET_AUTHORITY_TABLE,
                field=field,
                reason=reason,
                session_date=session,
                artifact_path=location.artifact_path,
                artifact_sha256=location.artifact_sha256,
                row_group=location.row_group,
                row_index_in_group=location.row_index_in_group,
                provider_active=provider_active,
                ticker=ticker,
                source_record_id=source_id,
                observed_value=value,
            )
        )

    def _freeze_record(self, figi: str, aggregate: _Aggregate) -> CompositeInventoryRecord:
        return CompositeInventoryRecord(
            observed_composite_figi=figi,
            observed_share_class_figis=tuple(sorted(aggregate.share_class_figis)),
            share_class_conflict=len(aggregate.share_class_figis) > 1,
            first_session=aggregate.first_session,
            last_session=aggregate.last_session,
            active_row_count=aggregate.active_row_count,
            inactive_row_count=aggregate.inactive_row_count,
            session_count=aggregate.session_count,
            ticker_count=len(aggregate.tickers),
            provider_locale_count=len(aggregate.locales),
            provider_market_count=len(aggregate.markets),
            primary_exchange_count=len(aggregate.primary_exchanges),
            parent_table_count=1,
            source_release_count=1,
            source_record_lineage_digest=aggregate.lineage.hexdigest(),
        )

    def _validate_batch_locator(
        self,
        artifact_path: str,
        artifact_sha256: str,
        row_group: int,
        row_index_base: int,
        table: str,
    ) -> tuple[str, str, int, int]:
        if not isinstance(artifact_path, str) or not artifact_path:
            self._fail("source_binding_invalid", f"{table} artifact_path is invalid")
        sha256 = _require_digest(artifact_sha256, f"{table} artifact_sha256")
        if type(row_group) is not int or row_group < 0:
            self._fail("source_binding_invalid", f"{table} row_group is invalid")
        if type(row_index_base) is not int or row_index_base < 0:
            self._fail("source_binding_invalid", f"{table} row_index_base is invalid")
        return artifact_path, sha256, row_group, row_index_base

    def _advance_scan_key(
        self,
        key: tuple[str, int, int],
        *,
        previous: tuple[str, int, int] | None,
        table: str,
    ) -> tuple[str, int, int]:
        if previous is not None and key <= previous:
            self._fail(
                "source_binding_invalid",
                f"{table} rows must follow artifact-path/row-group/row-index order",
            )
        return key

    def _require_open_session(self) -> None:
        self._require_usable()
        if self._current_session is None:
            self._fail("source_session_invalid", "no current session is open")

    def _require_usable(self) -> None:
        if self._faulted:
            raise CompositeInventoryError("engine_faulted", "discard this failed engine")
        if self._finalized:
            raise CompositeInventoryError("engine_finalized", "engine is already finalized")

    def _fail(self, check_id: str, message: str) -> None:
        self._faulted = True
        raise CompositeInventoryError(check_id, message)


def figi_invalid_reason(
    value: str | None,
    *,
    field: str = "composite_figi",
) -> str | None:
    """Return the first mutually-exclusive FIGI lexical failure reason."""

    if field not in {"composite_figi", "share_class_figi"}:
        raise CompositeInventoryError("figi_field_invalid", "FIGI field name is invalid")
    if value is None:
        return f"{field}_null"
    if not isinstance(value, str):
        raise CompositeInventoryError("figi_source_type_invalid", "FIGI must be text or null")
    if value == "":
        return f"{field}_empty"
    if value.isspace():
        return f"{field}_whitespace_only"
    if value != value.strip():
        return f"{field}_surrounding_whitespace"
    if len(value) != 12:
        return f"{field}_length_not_12"
    if _UPPER_ASCII_ALNUM.fullmatch(value) is None:
        return f"{field}_non_upper_ascii_alnum"
    if not value.startswith("BBG"):
        return f"{field}_prefix_not_BBG"
    return None


def _lineage_seed(authority_release_id: str) -> bytes:
    payload = {
        "parent_table": ASSET_AUTHORITY_TABLE,
        "release_id": authority_release_id,
        "rule_version": LINEAGE_RULE_VERSION,
        "scan_order": SCAN_ORDER_RULE,
    }
    return json.dumps(
        payload,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _freeze_reason_counts(
    counter: Counter[str],
    precedence: tuple[str, ...],
) -> tuple[tuple[str, int], ...]:
    return tuple((reason, counter[reason]) for reason in precedence if counter[reason])


def _required(row: Mapping[str, object], field: str, table: str) -> object:
    if field not in row:
        raise CompositeInventoryError(
            f"{table}_schema_missing_field", f"required field {field!r} is absent"
        )
    return row[field]


def _required_text(row: Mapping[str, object], field: str, table: str) -> str:
    value = _required(row, field, table)
    if not isinstance(value, str):
        raise CompositeInventoryError(
            f"{table}_source_type_invalid", f"{field} must be non-null text"
        )
    return value


def _optional_figi_text(row: Mapping[str, object], field: str, table: str) -> str | None:
    value = _required(row, field, table)
    if value is not None and not isinstance(value, str):
        raise CompositeInventoryError(
            f"{table}_source_type_invalid", f"{field} must be text or null"
        )
    return value


def _required_bool(row: Mapping[str, object], field: str, table: str) -> bool:
    value = _required(row, field, table)
    if type(value) is not bool:
        raise CompositeInventoryError(
            f"{table}_source_type_invalid", f"{field} must be a native bool"
        )
    return value


def _native_date(value: object) -> date:
    if type(value) is not date:
        raise CompositeInventoryError("source_session_invalid", "session_date must be a date")
    return value


def _require_digest(value: object, label: str) -> str:
    if not isinstance(value, str) or _SOURCE_RECORD_ID.fullmatch(value) is None:
        raise CompositeInventoryError("source_digest_invalid", f"{label} must be lowercase SHA-256")
    return value


def _add_non_null_text(
    target: set[str],
    row: Mapping[str, object],
    field: str,
    table: str,
) -> None:
    value = _required(row, field, table)
    if value is None:
        return
    if not isinstance(value, str):
        raise CompositeInventoryError(
            f"{table}_source_type_invalid", f"{field} must be text or null"
        )
    target.add(value)


def _exact_equal(left: object, right: object) -> bool:
    """Compare projected source values without coercion or normalization."""

    return type(left) is type(right) and left == right


__all__ = [
    "ASSET_AUTHORITY_TABLE",
    "COMPOSITE_FIGI_INVALID_REASON_PRECEDENCE",
    "FIGI_INVALID_REASON_SUFFIX_PRECEDENCE",
    "LINEAGE_RULE_VERSION",
    "SCAN_ORDER_RULE",
    "SHARE_CLASS_FIGI_INVALID_REASON_PRECEDENCE",
    "UNIVERSE_PARENT_PROJECTION",
    "UNIVERSE_RECONCILIATION_TABLE",
    "CompositeInventoryCaps",
    "CompositeInventoryDiagnostics",
    "CompositeInventoryEngine",
    "CompositeInventoryError",
    "CompositeInventoryRecord",
    "CompositeInventoryResult",
    "InvalidFigiExample",
    "figi_invalid_reason",
]
