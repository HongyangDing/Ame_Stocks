"""Bounded-memory semantic QA for authoritative Massive REST Bronze requests.

The byte-level Bronze audit remains the source of truth for physical integrity.  This
module deliberately has a smaller job: select only deterministic production request IDs,
stream their response pages, and use a temporary SQLite database to find cross-page key
conflicts and cross-dataset reference mismatches without retaining the corpus in memory.
Non-authoritative pilot manifests are counted but never opened.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import sqlite3
import tempfile
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Literal

from ame_stocks_api.artifacts import ArtifactError, safe_relative_path, stable_digest
from ame_stocks_api.audit.row_contracts import (
    legacy_filing_accession,
    valid_daily_bar,
    valid_legacy_financials,
)
from ame_stocks_api.downloads import build_download_plan
from ame_stocks_core import ProviderDataset, ProviderRequest

IssueKind = Literal["corruption", "difference"]

REST_SEMANTIC_AUDIT_SCHEMA_VERSION = 7

KEY_DATASETS: dict[ProviderDataset, tuple[str, ...]] = {
    ProviderDataset.DAILY_BARS: ("T", "t"),
    ProviderDataset.SPLITS: ("id",),
    ProviderDataset.DIVIDENDS: ("id",),
    ProviderDataset.NEWS: ("id",),
    ProviderDataset.SHORT_INTEREST: ("settlement_date", "ticker"),
    ProviderDataset.SHORT_VOLUME: ("date", "ticker"),
    ProviderDataset.FLOAT: ("effective_date", "ticker"),
    ProviderDataset.IPOS: ("listing_date", "ticker"),
    ProviderDataset.LEGACY_FINANCIALS: (
        "accession",
        "cik",
        "start_date",
        "end_date",
        "filing_date",
        "timeframe",
        "fiscal_period",
        "fiscal_year",
    ),
    ProviderDataset.TREASURY_YIELDS: ("date",),
    ProviderDataset.INFLATION: ("date",),
    ProviderDataset.INFLATION_EXPECTATIONS: ("date",),
    ProviderDataset.LABOR_MARKET: ("date",),
    ProviderDataset.TICKER_TYPES: ("code",),
    ProviderDataset.EXCHANGES: ("id",),
    ProviderDataset.TEN_K_SECTIONS: ("cik", "filing_date", "section"),
    # One SEC submission can cover several registrants in a combined filing.
    ProviderDataset.EDGAR_INDEX: ("accession_number", "cik"),
}
_DIAGNOSTIC_DUPLICATE_DATASETS = frozenset(
    {
        ProviderDataset.CONDITION_CODES.value,
        ProviderDataset.EDGAR_INDEX.value,
        ProviderDataset.EIGHT_K_DISCLOSURES.value,
        ProviderDataset.EIGHT_K_TEXT.value,
        ProviderDataset.FORM_3.value,
        ProviderDataset.FORM_4.value,
        ProviderDataset.FLOAT.value,
        ProviderDataset.IPOS.value,
        ProviderDataset.RISK_FACTORS.value,
        ProviderDataset.SHORT_INTEREST.value,
        ProviderDataset.SHORT_VOLUME.value,
        ProviderDataset.TEN_K_SECTIONS.value,
    }
)
TAXONOMY_DEFINITIONS: dict[ProviderDataset, str] = {
    ProviderDataset.DISCLOSURE_TAXONOMY: "disclosure",
    ProviderDataset.RISK_TAXONOMY: "risk",
}
TAXONOMY_USES: dict[ProviderDataset, str] = {
    ProviderDataset.EIGHT_K_DISCLOSURES: "disclosure",
    ProviderDataset.RISK_FACTORS: "risk",
}
ACCESSION_DETAILS = frozenset(
    {
        ProviderDataset.FORM_3,
        ProviderDataset.FORM_4,
        ProviderDataset.FORM_13F,
        ProviderDataset.LEGACY_FINANCIALS,
        ProviderDataset.EIGHT_K_TEXT,
        ProviderDataset.EIGHT_K_DISCLOSURES,
    }
)
_EXACT_ROW_DATASETS = frozenset(
    (
        ACCESSION_DETAILS
        - {ProviderDataset.FORM_13F, ProviderDataset.LEGACY_FINANCIALS}
    )
    | frozenset(TAXONOMY_USES)
)
AUDITED_DATASETS = frozenset(
    {
        *KEY_DATASETS,
        ProviderDataset.CONDITION_CODES,
        *TAXONOMY_DEFINITIONS,
        *TAXONOMY_USES,
        *ACCESSION_DETAILS,
    }
)

_EARLIEST_STARTS = {
    ProviderDataset.DAILY_BARS: date(2016, 7, 13),
    ProviderDataset.SPLITS: date(2003, 9, 10),
    ProviderDataset.DIVIDENDS: date(2003, 9, 10),
    ProviderDataset.NEWS: date(2016, 6, 22),
    ProviderDataset.SHORT_INTEREST: date(2017, 12, 29),
    ProviderDataset.SHORT_VOLUME: date(2024, 2, 6),
    ProviderDataset.IPOS: date(2008, 1, 1),
    ProviderDataset.LEGACY_FINANCIALS: date(2009, 3, 29),
    ProviderDataset.TREASURY_YIELDS: date(1962, 1, 2),
    ProviderDataset.INFLATION: date(1947, 1, 1),
    ProviderDataset.INFLATION_EXPECTATIONS: date(1982, 1, 1),
    ProviderDataset.LABOR_MARKET: date(1948, 1, 1),
}
_SNAPSHOTS = frozenset(
    {
        ProviderDataset.CONDITION_CODES,
        ProviderDataset.DISCLOSURE_TAXONOMY,
        ProviderDataset.EXCHANGES,
        ProviderDataset.FLOAT,
        ProviderDataset.RISK_TAXONOMY,
        ProviderDataset.TICKER_TYPES,
    }
)
_TAXONOMY_FIELDS = ("primary_category", "secondary_category", "tertiary_category")


class RestSemanticAuditError(RuntimeError):
    """Raised when the semantic audit cannot be configured or started safely."""


@dataclass(slots=True)
class DatasetMetrics:
    dataset: str
    expected_manifests: int = 0
    complete_manifests: int = 0
    missing_manifests: int = 0
    invalid_manifests: int = 0
    ignored_non_authoritative_manifests: int = 0
    pages: int = 0
    rows: int = 0
    rows_without_accession: int = 0
    candidate_key_rows: int = 0
    exact_duplicate_excess_rows: int = 0
    conflicting_keys: int = 0


@dataclass(frozen=True, slots=True)
class _Issue:
    kind: IssueKind
    code: str
    dataset: str
    count: int
    message: str
    examples: tuple[str, ...] = ()


class _Issues:
    def __init__(self, max_examples: int) -> None:
        self.max_examples = max_examples
        self._counts: Counter[tuple[IssueKind, str, str, str]] = Counter()
        self._examples: dict[tuple[IssueKind, str, str, str], list[str]] = defaultdict(list)

    def add(
        self,
        kind: IssueKind,
        code: str,
        dataset: str,
        message: str,
        *,
        count: int = 1,
        example: object | None = None,
    ) -> None:
        if count <= 0:
            return
        key = (kind, code, dataset, message)
        self._counts[key] += count
        if example is not None and len(self._examples[key]) < self.max_examples:
            rendered = str(example)
            if rendered not in self._examples[key]:
                self._examples[key].append(rendered[:500])

    def report(self) -> list[dict[str, object]]:
        issues = [
            _Issue(
                kind=kind,
                code=code,
                dataset=dataset,
                count=count,
                message=message,
                examples=tuple(self._examples[(kind, code, dataset, message)]),
            )
            for (kind, code, dataset, message), count in self._counts.items()
        ]
        return [
            {key: value for key, value in asdict(issue).items() if value != ()}
            for issue in sorted(issues, key=lambda item: (item.kind, item.dataset, item.code))
        ]

    def total(self, kind: IssueKind) -> int:
        return sum(count for key, count in self._counts.items() if key[0] == kind)

    def code_counts(self, kind: IssueKind) -> dict[str, int]:
        counts: Counter[str] = Counter()
        for (observed_kind, code, _dataset, _message), count in self._counts.items():
            if observed_kind == kind:
                counts[code] += count
        return dict(sorted(counts.items()))


@dataclass(slots=True)
class _PageBatch:
    keys: Counter[tuple[str, str, bytes]]
    taxonomies: Counter[tuple[str, str, str]]
    taxonomy_versions: Counter[tuple[str, str]]
    accessions: Counter[tuple[str, str, str, str, str]]

    @classmethod
    def empty(cls) -> _PageBatch:
        return cls(Counter(), Counter(), Counter(), Counter())


class RestSemanticAuditor:
    """Audit semantic identities using only authoritative production request IDs."""

    def __init__(
        self,
        data_root: Path,
        *,
        start: date,
        end: date,
        datasets: tuple[ProviderDataset, ...] | None = None,
        max_examples: int = 20,
        temp_dir: Path | None = None,
    ) -> None:
        if start > end:
            raise ValueError("start must be on or before end")
        if max_examples < 1:
            raise ValueError("max_examples must be positive")
        selected = AUDITED_DATASETS if datasets is None else frozenset(datasets)
        unsupported = selected - AUDITED_DATASETS
        if unsupported:
            names = ", ".join(sorted(dataset.value for dataset in unsupported))
            raise ValueError(f"unsupported semantic-audit datasets: {names}")
        self.data_root = data_root.expanduser().resolve()
        self.start = start
        self.end = end
        self.datasets = frozenset(selected)
        self.max_examples = max_examples
        self.temp_dir = (
            temp_dir.expanduser().resolve()
            if temp_dir
            else self.data_root / "tmp" / "rest_semantics_audit"
        )
        self.issues = _Issues(max_examples)
        self.metrics = {
            dataset.value: DatasetMetrics(dataset.value)
            for dataset in sorted(self.datasets, key=lambda item: item.value)
        }

    def run(self) -> dict[str, object]:
        if not self.data_root.is_dir():
            raise RestSemanticAuditError(f"data root is missing: {self.data_root}")
        try:
            self.temp_dir.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise RestSemanticAuditError(
                f"temporary directory is unavailable: {self.temp_dir}"
            ) from exc

        started = datetime.now(UTC)
        with tempfile.TemporaryDirectory(
            prefix="ame-rest-semantics-", dir=str(self.temp_dir)
        ) as temporary:
            database_path = Path(temporary) / "semantic-audit.sqlite3"
            with sqlite3.connect(database_path) as connection:
                self._initialize_database(connection)
                for dataset in sorted(self.datasets, key=lambda item: item.value):
                    self._scan_dataset(connection, dataset)
                uniqueness = self._finalize_uniqueness(connection)
                taxonomy = self._finalize_taxonomy(connection)
                accessions = self._finalize_accessions(connection)

        completed = datetime.now(UTC)
        corruption = self.issues.total("corruption")
        differences = self.issues.total("difference")
        corruption_code_counts = self.issues.code_counts("corruption")
        status = (
            "failed"
            if corruption
            else ("passed_with_differences" if differences else "passed")
        )
        hard_key_failures = any(
            dataset not in _DIAGNOSTIC_DUPLICATE_DATASETS
            and (
                int(details["exact_duplicate_excess_rows"]) > 0
                or int(details["conflicting_keys"]) > 0
            )
            for dataset, details in uniqueness.items()
        )
        diagnostic_key_differences = any(
            dataset in _DIAGNOSTIC_DUPLICATE_DATASETS
            and (
                int(details["exact_duplicate_excess_rows"]) > 0
                or int(details["conflicting_keys"]) > 0
            )
            for dataset, details in uniqueness.items()
        )
        candidate_key_corruption = any(
            corruption_code_counts.get(code, 0)
            for code in ("missing_candidate_key", "row_not_canonical_json")
        )
        return {
            "audit_schema_version": REST_SEMANTIC_AUDIT_SCHEMA_VERSION,
            "status": status,
            "data_root": str(self.data_root),
            "window": {"start": self.start.isoformat(), "end": self.end.isoformat()},
            "dataset_windows": {
                **(
                    {
                        ProviderDataset.DAILY_BARS.value: {
                            "start": max(
                                self.start, _EARLIEST_STARTS[ProviderDataset.DAILY_BARS]
                            ).isoformat(),
                            "end": self.end.isoformat(),
                            "basis": "grouped endpoint rolling-ten-year entitlement",
                        }
                    }
                    if ProviderDataset.DAILY_BARS in self.datasets
                    else {}
                ),
                **(
                    {
                        ProviderDataset.LEGACY_FINANCIALS.value: {
                            "start": _EARLIEST_STARTS[
                                ProviderDataset.LEGACY_FINANCIALS
                            ].isoformat(),
                            "end": self.end.isoformat(),
                            "basis": "earliest verified accessible filing-date history",
                        }
                    }
                    if ProviderDataset.LEGACY_FINANCIALS in self.datasets
                    else {}
                ),
            },
            "datasets": [
                asdict(self.metrics[key]) for key in sorted(self.metrics)
            ],
            "gates": {
                "semantic_corruption": "failed" if corruption else "passed",
                "candidate_key_consistency": (
                    "failed"
                    if hard_key_failures or candidate_key_corruption
                    else ("different" if diagnostic_key_differences else "matched")
                ),
                "accession_coverage": accessions["status"],
            },
            "summary": {
                "corruption_count": corruption,
                "difference_count": differences,
                "corruption_code_counts": corruption_code_counts,
                "difference_code_counts": self.issues.code_counts("difference"),
                "count_semantics": (
                    "Heterogeneous violation instances (rows, pages, manifests, keys, or "
                    "taxonomy versions), not unique bad rows; one observation may contribute "
                    "to more than one code."
                ),
                "expected_manifests": sum(
                    metric.expected_manifests for metric in self.metrics.values()
                ),
                "ignored_non_authoritative_manifests": sum(
                    metric.ignored_non_authoritative_manifests
                    for metric in self.metrics.values()
                ),
                "pages": sum(metric.pages for metric in self.metrics.values()),
                "rows": sum(metric.rows for metric in self.metrics.values()),
            },
            "uniqueness": uniqueness,
            "taxonomy_coverage": taxonomy,
            "accession_coverage": accessions,
            "issues": self.issues.report(),
            "started_at": started.isoformat(),
            "completed_at": completed.isoformat(),
            "duration_seconds": round((completed - started).total_seconds(), 3),
            "method": (
                "Only request IDs rebuilt from the authoritative plan are opened. Each gzip JSON "
                "page is decoded independently; candidate keys, canonical row SHA-256 values, "
                "taxonomy paths, and accession sets are aggregated in an automatically removed "
                "temporary SQLite database. Form 13-F participates in accession coverage and "
                "filing-date agreement without materializing 103 million row hashes; its filer "
                "CIK/form identity is matched to EDGAR, and its holding fields are enforced by "
                "the full Bronze audit. Grouped daily bars begin at the verified entitlement "
                "boundary 2016-07-13; legacy financials cover filing dates from 2009-03-29."
            ),
        }

    @staticmethod
    def _initialize_database(connection: sqlite3.Connection) -> None:
        connection.executescript(
            """
            PRAGMA journal_mode = OFF;
            PRAGMA synchronous = OFF;
            PRAGMA temp_store = FILE;
            CREATE TABLE key_records (
                dataset TEXT NOT NULL,
                candidate_key TEXT NOT NULL,
                row_sha256 BLOB NOT NULL,
                occurrences INTEGER NOT NULL,
                PRIMARY KEY (dataset, candidate_key, row_sha256)
            ) WITHOUT ROWID;
            CREATE TABLE taxonomy_paths (
                family TEXT NOT NULL,
                role TEXT NOT NULL,
                path TEXT NOT NULL,
                occurrences INTEGER NOT NULL,
                PRIMARY KEY (family, role, path)
            ) WITHOUT ROWID;
            CREATE TABLE taxonomy_versions (
                family TEXT NOT NULL,
                version TEXT NOT NULL,
                occurrences INTEGER NOT NULL,
                PRIMARY KEY (family, version)
            ) WITHOUT ROWID;
            CREATE TABLE accessions (
                dataset TEXT NOT NULL,
                accession TEXT NOT NULL,
                filing_date TEXT NOT NULL,
                cik TEXT NOT NULL,
                form_type TEXT NOT NULL,
                occurrences INTEGER NOT NULL,
                PRIMARY KEY (dataset, accession, filing_date, cik, form_type)
            ) WITHOUT ROWID;
            """
        )

    def _scan_dataset(self, connection: sqlite3.Connection, dataset: ProviderDataset) -> None:
        plan = self._authoritative_plan(dataset)
        metric = self.metrics[dataset.value]
        metric.expected_manifests = len(plan)
        manifest_root = self.data_root / "manifests" / "massive" / dataset.value
        expected_ids = {request.request_id for request in plan}
        actual_paths = set(manifest_root.glob("*.json")) if manifest_root.is_dir() else set()
        ignored = sorted(path for path in actual_paths if path.stem not in expected_ids)
        metric.ignored_non_authoritative_manifests = len(ignored)

        for request in plan:
            path = manifest_root / f"{request.request_id}.json"
            if not path.is_file():
                metric.missing_manifests += 1
                self.issues.add(
                    "corruption",
                    "missing_authoritative_manifest",
                    dataset.value,
                    "an authoritative production request has no manifest",
                    example=request.request_id,
                )
                continue
            self._scan_manifest(connection, dataset, request, path)
        if (
            dataset in _SNAPSHOTS
            and metric.complete_manifests == metric.expected_manifests
            and metric.rows == 0
        ):
            self.issues.add(
                "corruption",
                "empty_reference_snapshot",
                dataset.value,
                "authoritative reference snapshot contains no rows",
            )

    def _authoritative_plan(self, dataset: ProviderDataset) -> tuple[ProviderRequest, ...]:
        if dataset in _SNAPSHOTS:
            start = end = self.end
        elif dataset is ProviderDataset.DAILY_BARS:
            start = max(self.start, _EARLIEST_STARTS[dataset])
            end = self.end
        else:
            start = _EARLIEST_STARTS.get(dataset, self.start)
            end = self.end
        return build_download_plan(dataset=dataset, start=start, end=end).requests

    def _scan_manifest(
        self,
        connection: sqlite3.Connection,
        dataset: ProviderDataset,
        request: ProviderRequest,
        path: Path,
    ) -> None:
        metric = self.metrics[dataset.value]
        try:
            manifest = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            metric.invalid_manifests += 1
            self.issues.add(
                "corruption",
                "manifest_unreadable",
                dataset.value,
                "authoritative manifest is not valid JSON",
                example=f"{path.name}: {type(exc).__name__}",
            )
            return
        if not isinstance(manifest, dict):
            self._invalid_manifest(metric, dataset, path, "manifest root is not an object")
            return
        if (
            manifest.get("status") != "complete"
            or manifest.get("dataset") != dataset.value
            or manifest.get("request_id") != request.request_id
            or manifest.get("request") != request.canonical_dict()
            or path.stem != request.request_id
            or stable_digest(manifest.get("request")) != request.request_id
        ):
            self._invalid_manifest(
                metric, dataset, path, "status, dataset, or canonical request identity differs"
            )
            return
        artifacts = manifest.get("artifacts")
        if not isinstance(artifacts, list) or not artifacts:
            self._invalid_manifest(metric, dataset, path, "complete manifest has no artifacts")
            return
        sequences = [item.get("sequence") for item in artifacts if isinstance(item, dict)]
        if sequences != list(range(len(artifacts))):
            self._invalid_manifest(metric, dataset, path, "artifact sequence is not contiguous")
            return
        raw_hashes = [
            str(item["raw_sha256"])
            for item in artifacts
            if isinstance(item, dict)
            and isinstance(item.get("raw_sha256"), str)
            and item["raw_sha256"]
        ]
        if len(raw_hashes) != len(set(raw_hashes)):
            self.issues.add(
                "corruption",
                "duplicate_page_payload",
                dataset.value,
                "one authoritative manifest repeats a raw page SHA-256",
                example=path.name,
            )
        continuations = [
            str(item["next_continuation"])
            for item in artifacts
            if isinstance(item, dict) and item.get("next_continuation") not in {None, ""}
        ]
        if len(continuations) != len(set(continuations)):
            self.issues.add(
                "corruption",
                "duplicate_page_continuation",
                dataset.value,
                "one authoritative manifest repeats a pagination continuation",
                example=path.name,
            )

        metric.complete_manifests += 1
        for artifact in artifacts:
            if not isinstance(artifact, dict):
                self.issues.add(
                    "corruption",
                    "artifact_metadata_invalid",
                    dataset.value,
                    "artifact metadata is not an object",
                    example=path.name,
                )
                continue
            self._scan_page(connection, dataset, artifact, metric)

    def _invalid_manifest(
        self,
        metric: DatasetMetrics,
        dataset: ProviderDataset,
        path: Path,
        detail: str,
    ) -> None:
        metric.invalid_manifests += 1
        self.issues.add(
            "corruption",
            "authoritative_manifest_invalid",
            dataset.value,
            detail,
            example=path.name,
        )

    def _scan_page(
        self,
        connection: sqlite3.Connection,
        dataset: ProviderDataset,
        artifact: dict[str, Any],
        metric: DatasetMetrics,
    ) -> None:
        try:
            page_path = safe_relative_path(self.data_root, artifact.get("path"))
            with gzip.open(page_path, "rt", encoding="utf-8") as handle:
                document = json.load(handle)
        except (
            ArtifactError,
            OSError,
            EOFError,
            UnicodeError,
            json.JSONDecodeError,
        ) as exc:
            self.issues.add(
                "corruption",
                "page_unreadable",
                dataset.value,
                "authoritative gzip JSON page cannot be decoded",
                example=f"{artifact.get('path')}: {type(exc).__name__}",
            )
            return
        if (
            not isinstance(document, dict)
            or str(document.get("status", "")).upper() != "OK"
            or not isinstance(document.get("request_id"), str)
            or not str(document["request_id"]).strip()
        ):
            self.issues.add(
                "corruption",
                "response_envelope_invalid",
                dataset.value,
                "provider response root/status is invalid",
                example=artifact.get("path"),
            )
            return
        rows = document.get("results")
        if rows is None and "results" not in document:
            if _valid_empty_results_response(document, artifact):
                rows = []
            else:
                self.issues.add(
                    "corruption",
                    "results_missing",
                    dataset.value,
                    "provider response omits results without a zero-row terminal contract",
                    example=artifact.get("path"),
                )
                return
        if not isinstance(rows, list):
            self.issues.add(
                "corruption",
                "results_not_array",
                dataset.value,
                "provider results is not an array",
                example=artifact.get("path"),
            )
            return
        declared_count = artifact.get("record_count")
        if isinstance(declared_count, bool) or not isinstance(declared_count, int):
            declared_count = -1
        if declared_count != len(rows):
            self.issues.add(
                "corruption",
                "semantic_record_count_mismatch",
                dataset.value,
                "manifest record_count differs from decoded results length",
                example=artifact.get("path"),
            )

        batch = _PageBatch.empty()
        for row in rows:
            metric.rows += 1
            if not isinstance(row, dict):
                self.issues.add(
                    "corruption",
                    "row_not_object",
                    dataset.value,
                    "result row is not an object",
                    example=artifact.get("path"),
                )
                continue
            self._collect_row(dataset, row, batch, metric)
        self._flush_page(connection, batch)
        metric.pages += 1

    def _collect_row(
        self,
        dataset: ProviderDataset,
        row: dict[str, Any],
        batch: _PageBatch,
        metric: DatasetMetrics,
    ) -> None:
        if dataset is ProviderDataset.DAILY_BARS and not valid_daily_bar(row):
            self.issues.add(
                "corruption",
                "row_contract_invalid",
                dataset.value,
                (
                    "row violates grouped daily-bar ticker/nominal-window/OHLCV or optional "
                    "n/vw/otc constraints"
                ),
            )
        if dataset is ProviderDataset.LEGACY_FINANCIALS and not valid_legacy_financials(row):
            self.issues.add(
                "corruption",
                "row_contract_invalid",
                dataset.value,
                (
                    "row violates legacy financials filing_date/CIK/timeframe/end_date/"
                    "financials constraints"
                ),
            )
        if dataset in KEY_DATASETS:
            fields = KEY_DATASETS[dataset]
            values = (
                (
                    legacy_filing_accession(row.get("source_filing_url")),
                    *(row.get(field) for field in fields[1:]),
                )
                if dataset is ProviderDataset.LEGACY_FINANCIALS
                else tuple(row.get(field) for field in fields)
            )
            normalized_values = tuple(_normalized_scalar(value) for value in values)
            if any(value is None for value in normalized_values):
                self.issues.add(
                    "corruption",
                    "missing_candidate_key",
                    dataset.value,
                    f"row has no nonblank candidate-key fields: {', '.join(fields)}",
                )
            else:
                digest = _canonical_row_sha256(row)
                if digest is None:
                    self.issues.add(
                        "corruption",
                        "row_not_canonical_json",
                        dataset.value,
                        "row cannot be represented as canonical JSON",
                        example=values,
                    )
                else:
                    batch.keys[
                        (dataset.value, _json_key(normalized_values), digest)
                    ] += 1
                    metric.candidate_key_rows += 1

        if dataset is ProviderDataset.CONDITION_CODES:
            self._collect_condition(row, batch, metric)

        family = TAXONOMY_DEFINITIONS.get(dataset)
        if family is not None:
            path = self._taxonomy_path(dataset, row)
            if path is not None:
                digest = _canonical_row_sha256(row)
                if digest is not None:
                    batch.keys[(dataset.value, path, digest)] += 1
                    batch.taxonomies[(family, "definition", path)] += 1
                    metric.candidate_key_rows += 1
            taxonomy_version = _normalized_scalar(row.get("taxonomy"))
            if taxonomy_version is None:
                self.issues.add(
                    "corruption",
                    "taxonomy_version_missing",
                    dataset.value,
                    "taxonomy definition row has no scalar taxonomy version",
                )
            else:
                batch.taxonomy_versions[(family, taxonomy_version)] += 1

        family = TAXONOMY_USES.get(dataset)
        if family is not None:
            path = self._taxonomy_path(dataset, row)
            if path is not None:
                batch.taxonomies[(family, "use", path)] += 1

        if dataset in _EXACT_ROW_DATASETS:
            digest = _canonical_row_sha256(row)
            if digest is None:
                self.issues.add(
                    "corruption",
                    "row_not_canonical_json",
                    dataset.value,
                    "row cannot be represented as canonical JSON",
                )
            else:
                batch.keys[(dataset.value, f"row:{digest.hex()}", digest)] += 1
                metric.candidate_key_rows += 1

        if dataset is ProviderDataset.EDGAR_INDEX:
            accession = row.get("accession_number")
            if isinstance(accession, str) and accession.strip():
                batch.accessions[
                    _accession_key(dataset, accession, row)
                ] += 1
            else:
                metric.rows_without_accession += 1
                self.issues.add(
                    "corruption",
                    "accession_number_missing",
                    dataset.value,
                    "EDGAR index row has no nonblank accession_number",
                )
        elif dataset is ProviderDataset.LEGACY_FINANCIALS:
            filing_date = row.get("filing_date")
            if (
                isinstance(filing_date, str)
                and self.start.isoformat() <= filing_date <= self.end.isoformat()
            ):
                accession = legacy_filing_accession(row.get("source_filing_url"))
                if accession is not None:
                    batch.accessions[
                        _accession_key(dataset, accession, row)
                    ] += 1
                else:
                    metric.rows_without_accession += 1
                    self.issues.add(
                        "corruption",
                        "accession_number_missing",
                        dataset.value,
                        "legacy financial row has no canonical source filing accession",
                    )
        elif dataset in ACCESSION_DETAILS:
            accession = row.get("accession_number")
            if isinstance(accession, str) and accession.strip():
                batch.accessions[
                    _accession_key(dataset, accession, row)
                ] += 1
            else:
                metric.rows_without_accession += 1
                self.issues.add(
                    "corruption",
                    "accession_number_missing",
                    dataset.value,
                    "detail filing row has no nonblank accession_number",
                )

    def _collect_condition(
        self,
        row: dict[str, Any],
        batch: _PageBatch,
        metric: DatasetMetrics,
    ) -> None:
        asset_class = row.get("asset_class")
        condition_id = row.get("id")
        data_types = row.get("data_types")
        if (
            not isinstance(asset_class, str)
            or not asset_class.strip()
            or isinstance(condition_id, bool)
            or not isinstance(condition_id, (str, int))
            or not isinstance(data_types, list)
            or not data_types
        ):
            self.issues.add(
                "corruption",
                "condition_identity_invalid",
                ProviderDataset.CONDITION_CODES.value,
                "condition needs asset_class, id, and a nonempty data_types array",
            )
            return
        normalized_types: list[str] = []
        for value in data_types:
            if not isinstance(value, str) or not value.strip():
                self.issues.add(
                    "corruption",
                    "condition_data_type_invalid",
                    ProviderDataset.CONDITION_CODES.value,
                    "condition data_types contains a non-string or blank value",
                    example=condition_id,
                )
                continue
            normalized_types.append(value.strip())
        if len(normalized_types) != len(set(normalized_types)):
            self.issues.add(
                "corruption",
                "condition_data_type_duplicate",
                ProviderDataset.CONDITION_CODES.value,
                "one condition repeats the same data_type",
                example=condition_id,
            )
        digest = _canonical_row_sha256(row)
        if digest is None:
            self.issues.add(
                "corruption",
                "row_not_canonical_json",
                ProviderDataset.CONDITION_CODES.value,
                "condition row cannot be represented as canonical JSON",
                example=condition_id,
            )
            return
        for data_type in sorted(set(normalized_types)):
            key = _json_key((asset_class.strip(), data_type, condition_id))
            batch.keys[(ProviderDataset.CONDITION_CODES.value, key, digest)] += 1
            metric.candidate_key_rows += 1

    def _taxonomy_path(
        self, dataset: ProviderDataset, row: dict[str, Any]
    ) -> str | None:
        values: list[str] = []
        for field in _TAXONOMY_FIELDS:
            value = row.get(field)
            if not isinstance(value, str) or not value.strip():
                self.issues.add(
                    "corruption",
                    "taxonomy_path_invalid",
                    dataset.value,
                    "taxonomy path needs three nonblank category strings",
                    example=field,
                )
                return None
            values.append(value.strip())
        return _json_key(tuple(values))

    @staticmethod
    def _flush_page(connection: sqlite3.Connection, batch: _PageBatch) -> None:
        with connection:
            connection.executemany(
                """
                INSERT INTO key_records(dataset, candidate_key, row_sha256, occurrences)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(dataset, candidate_key, row_sha256)
                DO UPDATE SET occurrences = occurrences + excluded.occurrences
                """,
                [(*key, count) for key, count in batch.keys.items()],
            )
            connection.executemany(
                """
                INSERT INTO taxonomy_paths(family, role, path, occurrences)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(family, role, path)
                DO UPDATE SET occurrences = occurrences + excluded.occurrences
                """,
                [(*key, count) for key, count in batch.taxonomies.items()],
            )
            connection.executemany(
                """
                INSERT INTO taxonomy_versions(family, version, occurrences)
                VALUES (?, ?, ?)
                ON CONFLICT(family, version)
                DO UPDATE SET occurrences = occurrences + excluded.occurrences
                """,
                [(*key, count) for key, count in batch.taxonomy_versions.items()],
            )
            connection.executemany(
                """
                INSERT INTO accessions(
                    dataset, accession, filing_date, cik, form_type, occurrences
                )
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(dataset, accession, filing_date, cik, form_type)
                DO UPDATE SET occurrences = occurrences + excluded.occurrences
                """,
                [(*key, count) for key, count in batch.accessions.items()],
            )

    def _finalize_uniqueness(
        self, connection: sqlite3.Connection
    ) -> dict[str, dict[str, object]]:
        audited = {
            *[dataset.value for dataset in KEY_DATASETS if dataset in self.datasets],
            *(
                [ProviderDataset.CONDITION_CODES.value]
                if ProviderDataset.CONDITION_CODES in self.datasets
                else []
            ),
            *[
                dataset.value
                for dataset in TAXONOMY_DEFINITIONS
                if dataset in self.datasets
            ],
            *[
                dataset.value
                for dataset in _EXACT_ROW_DATASETS
                if dataset in self.datasets
            ],
        }
        result: dict[str, dict[str, object]] = {}
        for dataset in sorted(audited):
            excess = int(
                connection.execute(
                    """
                    SELECT COALESCE(SUM(occurrences - 1), 0)
                    FROM key_records
                    WHERE dataset = ? AND occurrences > 1
                    """,
                    (dataset,),
                ).fetchone()[0]
            )
            conflicts = int(
                connection.execute(
                    """
                    SELECT COUNT(*) FROM (
                        SELECT candidate_key
                        FROM key_records
                        WHERE dataset = ?
                        GROUP BY candidate_key
                        HAVING COUNT(*) > 1
                    )
                    """,
                    (dataset,),
                ).fetchone()[0]
            )
            distinct_keys = int(
                connection.execute(
                    "SELECT COUNT(DISTINCT candidate_key) FROM key_records WHERE dataset = ?",
                    (dataset,),
                ).fetchone()[0]
            )
            duplicate_examples = [
                str(row[0])
                for row in connection.execute(
                    """
                    SELECT candidate_key FROM key_records
                    WHERE dataset = ? AND occurrences > 1
                    ORDER BY candidate_key LIMIT ?
                    """,
                    (dataset, self.max_examples),
                )
            ]
            conflict_examples = [
                str(row[0])
                for row in connection.execute(
                    """
                    SELECT candidate_key FROM key_records
                    WHERE dataset = ?
                    GROUP BY candidate_key HAVING COUNT(*) > 1
                    ORDER BY candidate_key LIMIT ?
                    """,
                    (dataset, self.max_examples),
                )
            ]
            metric = self.metrics[dataset]
            metric.exact_duplicate_excess_rows = excess
            metric.conflicting_keys = conflicts
            diagnostic = dataset in _DIAGNOSTIC_DUPLICATE_DATASETS
            if excess:
                self.issues.add(
                    "difference" if diagnostic else "corruption",
                    "provider_exact_duplicate_rows" if diagnostic else "exact_duplicate_rows",
                    dataset,
                    "candidate key and canonical row repeat exactly",
                    count=excess,
                    example=duplicate_examples[0] if duplicate_examples else None,
                )
            if conflicts:
                self.issues.add(
                    "difference" if diagnostic else "corruption",
                    "candidate_key_ambiguity" if diagnostic else "conflicting_candidate_keys",
                    dataset,
                    "one candidate key maps to multiple canonical rows",
                    count=conflicts,
                    example=conflict_examples[0] if conflict_examples else None,
                )
            result[dataset] = {
                "distinct_candidate_keys": distinct_keys,
                "exact_duplicate_excess_rows": excess,
                "conflicting_keys": conflicts,
                "duplicate_examples": duplicate_examples,
                "conflict_examples": conflict_examples,
            }
        return result

    def _finalize_taxonomy(self, connection: sqlite3.Connection) -> dict[str, object]:
        result: dict[str, object] = {}
        for family in ("disclosure", "risk"):
            definition_dataset = next(
                (
                    dataset
                    for dataset, observed in TAXONOMY_DEFINITIONS.items()
                    if observed == family
                ),
                None,
            )
            use_dataset = next(
                (dataset for dataset, observed in TAXONOMY_USES.items() if observed == family),
                None,
            )
            if definition_dataset not in self.datasets:
                result[family] = {"status": "not_run"}
                continue
            definitions = int(
                connection.execute(
                    "SELECT COUNT(*) FROM taxonomy_paths WHERE family = ? AND role = 'definition'",
                    (family,),
                ).fetchone()[0]
            )
            versions = [
                str(row[0])
                for row in connection.execute(
                    "SELECT version FROM taxonomy_versions WHERE family = ? ORDER BY version",
                    (family,),
                )
            ]
            if len(versions) != 1:
                self.issues.add(
                    "corruption",
                    "taxonomy_version_ambiguous",
                    definition_dataset.value,
                    "authoritative taxonomy snapshot must contain exactly one version",
                    count=max(1, len(versions)),
                    example=versions,
                )
            if use_dataset not in self.datasets:
                result[family] = {
                    "status": "failed" if len(versions) != 1 else "definition_only",
                    "definition_paths": definitions,
                    "definition_versions": versions,
                }
                continue
            used_paths = int(
                connection.execute(
                    "SELECT COUNT(*) FROM taxonomy_paths WHERE family = ? AND role = 'use'",
                    (family,),
                ).fetchone()[0]
            )
            missing_rows = int(
                connection.execute(
                    """
                    SELECT COALESCE(SUM(u.occurrences), 0)
                    FROM taxonomy_paths AS u
                    LEFT JOIN taxonomy_paths AS d
                      ON d.family = u.family AND d.role = 'definition' AND d.path = u.path
                    WHERE u.family = ? AND u.role = 'use' AND d.path IS NULL
                    """,
                    (family,),
                ).fetchone()[0]
            )
            missing_paths = [
                str(row[0])
                for row in connection.execute(
                    """
                    SELECT u.path FROM taxonomy_paths AS u
                    LEFT JOIN taxonomy_paths AS d
                      ON d.family = u.family AND d.role = 'definition' AND d.path = u.path
                    WHERE u.family = ? AND u.role = 'use' AND d.path IS NULL
                    ORDER BY u.path LIMIT ?
                    """,
                    (family, self.max_examples),
                )
            ]
            if missing_rows:
                self.issues.add(
                    "corruption",
                    "taxonomy_path_not_decodable",
                    use_dataset.value,
                    "used taxonomy path is absent from its authoritative taxonomy",
                    count=missing_rows,
                    example=missing_paths[0] if missing_paths else None,
                )
            result[family] = {
                "status": "failed" if missing_rows or len(versions) != 1 else "matched",
                "definition_paths": definitions,
                "definition_versions": versions,
                "used_paths": used_paths,
                "undecodable_usage_rows": missing_rows,
                "undecodable_path_examples": missing_paths,
            }
        return result

    def _finalize_accessions(self, connection: sqlite3.Connection) -> dict[str, object]:
        if ProviderDataset.EDGAR_INDEX not in self.datasets:
            rows_without_accession = sum(
                self.metrics[dataset.value].rows_without_accession
                for dataset in ACCESSION_DETAILS & self.datasets
            )
            return {
                "status": "failed" if rows_without_accession else "not_run",
                "rows_without_accession": rows_without_accession,
                "datasets": {},
            }
        datasets: dict[str, object] = {}
        missing_total = 0
        filing_date_mismatch_total = 0
        identity_mismatch_total = 0
        rows_without_accession_total = self.metrics[
            ProviderDataset.EDGAR_INDEX.value
        ].rows_without_accession
        for dataset in sorted(ACCESSION_DETAILS & self.datasets, key=lambda item: item.value):
            distinct_accessions = int(
                connection.execute(
                    "SELECT COUNT(DISTINCT accession) FROM accessions WHERE dataset = ?",
                    (dataset.value,),
                ).fetchone()[0]
            )
            missing_rows = int(
                connection.execute(
                    """
                    SELECT COALESCE(SUM(detail.occurrences), 0)
                    FROM accessions AS detail
                    WHERE detail.dataset = ? AND NOT EXISTS (
                        SELECT 1 FROM accessions AS edgar
                        WHERE edgar.dataset = ? AND edgar.accession = detail.accession
                    )
                    """,
                    (dataset.value, ProviderDataset.EDGAR_INDEX.value),
                ).fetchone()[0]
            )
            examples = [
                str(row[0])
                for row in connection.execute(
                    """
                    SELECT detail.accession
                    FROM accessions AS detail
                    WHERE detail.dataset = ? AND NOT EXISTS (
                        SELECT 1 FROM accessions AS edgar
                        WHERE edgar.dataset = ? AND edgar.accession = detail.accession
                    )
                    ORDER BY detail.accession LIMIT ?
                    """,
                    (dataset.value, ProviderDataset.EDGAR_INDEX.value, self.max_examples),
                )
            ]
            filing_date_mismatch_rows = int(
                connection.execute(
                    """
                    SELECT COALESCE(SUM(detail.occurrences), 0)
                    FROM accessions AS detail
                    WHERE detail.dataset = ? AND detail.filing_date != ''
                      AND EXISTS (
                        SELECT 1 FROM accessions AS edgar
                        WHERE edgar.dataset = ? AND edgar.accession = detail.accession
                      )
                      AND NOT EXISTS (
                        SELECT 1 FROM accessions AS edgar
                        WHERE edgar.dataset = ? AND edgar.accession = detail.accession
                          AND edgar.filing_date = detail.filing_date
                      )
                    """,
                    (
                        dataset.value,
                        ProviderDataset.EDGAR_INDEX.value,
                        ProviderDataset.EDGAR_INDEX.value,
                    ),
                ).fetchone()[0]
            )
            filing_date_mismatch_examples = [
                f"{row[0]}:{row[1]}"
                for row in connection.execute(
                    """
                    SELECT detail.accession, detail.filing_date
                    FROM accessions AS detail
                    WHERE detail.dataset = ? AND detail.filing_date != ''
                      AND EXISTS (
                        SELECT 1 FROM accessions AS edgar
                        WHERE edgar.dataset = ? AND edgar.accession = detail.accession
                      )
                      AND NOT EXISTS (
                        SELECT 1 FROM accessions AS edgar
                        WHERE edgar.dataset = ? AND edgar.accession = detail.accession
                          AND edgar.filing_date = detail.filing_date
                      )
                    ORDER BY detail.accession, detail.filing_date LIMIT ?
                    """,
                    (
                        dataset.value,
                        ProviderDataset.EDGAR_INDEX.value,
                        ProviderDataset.EDGAR_INDEX.value,
                        self.max_examples,
                    ),
                )
            ]
            identity_mismatch_rows = 0
            identity_mismatch_examples: list[str] = []
            if dataset in {
                ProviderDataset.FORM_13F,
                ProviderDataset.LEGACY_FINANCIALS,
            }:
                identity_predicate = (
                    "AND edgar.form_type = detail.form_type"
                    if dataset is ProviderDataset.FORM_13F
                    else ""
                )
                identity_mismatch_rows = int(
                    connection.execute(
                        f"""
                        SELECT COALESCE(SUM(detail.occurrences), 0)
                        FROM accessions AS detail
                        WHERE detail.dataset = ?
                          AND EXISTS (
                            SELECT 1 FROM accessions AS edgar
                            WHERE edgar.dataset = ? AND edgar.accession = detail.accession
                              AND edgar.filing_date = detail.filing_date
                          )
                          AND NOT EXISTS (
                            SELECT 1 FROM accessions AS edgar
                            WHERE edgar.dataset = ? AND edgar.accession = detail.accession
                              AND edgar.filing_date = detail.filing_date
                              AND edgar.cik = detail.cik
                              {identity_predicate}
                          )
                        """,
                        (
                            dataset.value,
                            ProviderDataset.EDGAR_INDEX.value,
                            ProviderDataset.EDGAR_INDEX.value,
                        ),
                    ).fetchone()[0]
                )
                identity_mismatch_examples = [
                    f"{row[0]}:{row[1]}:{row[2]}"
                    for row in connection.execute(
                        f"""
                        SELECT detail.accession, detail.cik, detail.form_type
                        FROM accessions AS detail
                        WHERE detail.dataset = ?
                          AND EXISTS (
                            SELECT 1 FROM accessions AS edgar
                            WHERE edgar.dataset = ? AND edgar.accession = detail.accession
                              AND edgar.filing_date = detail.filing_date
                          )
                          AND NOT EXISTS (
                            SELECT 1 FROM accessions AS edgar
                            WHERE edgar.dataset = ? AND edgar.accession = detail.accession
                              AND edgar.filing_date = detail.filing_date
                              AND edgar.cik = detail.cik
                              {identity_predicate}
                          )
                        ORDER BY detail.accession, detail.cik, detail.form_type LIMIT ?
                        """,
                        (
                            dataset.value,
                            ProviderDataset.EDGAR_INDEX.value,
                            ProviderDataset.EDGAR_INDEX.value,
                            self.max_examples,
                        ),
                    )
                ]
            missing_total += missing_rows
            filing_date_mismatch_total += filing_date_mismatch_rows
            identity_mismatch_total += identity_mismatch_rows
            rows_without_accession = self.metrics[dataset.value].rows_without_accession
            rows_without_accession_total += rows_without_accession
            if missing_rows:
                self.issues.add(
                    "difference",
                    "accession_absent_from_edgar_index",
                    dataset.value,
                    "detail filing accession is absent from the downloaded EDGAR index",
                    count=missing_rows,
                    example=examples[0] if examples else None,
                )
            if filing_date_mismatch_rows:
                self.issues.add(
                    "corruption",
                    "accession_filing_date_mismatch",
                    dataset.value,
                    "detail filing_date differs from EDGAR for the same accession",
                    count=filing_date_mismatch_rows,
                    example=(
                        filing_date_mismatch_examples[0]
                        if filing_date_mismatch_examples
                        else None
                    ),
                )
            if identity_mismatch_rows:
                self.issues.add(
                    "corruption",
                    "accession_identity_mismatch",
                    dataset.value,
                    (
                        "Form 13F filer_cik/form_type is not an EDGAR identity for the accession"
                        if dataset is ProviderDataset.FORM_13F
                        else "legacy financial CIK is not an EDGAR identity for the accession"
                    ),
                    count=identity_mismatch_rows,
                    example=(
                        identity_mismatch_examples[0]
                        if identity_mismatch_examples
                        else None
                    ),
                )
            datasets[dataset.value] = {
                "distinct_accessions": distinct_accessions,
                "rows_without_accession": rows_without_accession,
                "missing_edgar_rows": missing_rows,
                "missing_edgar_examples": examples,
                "filing_date_mismatch_rows": filing_date_mismatch_rows,
                "filing_date_mismatch_examples": filing_date_mismatch_examples,
                "identity_mismatch_rows": identity_mismatch_rows,
                "identity_mismatch_examples": identity_mismatch_examples,
            }
        return {
            "status": (
                "failed"
                if (
                    rows_without_accession_total
                    or filing_date_mismatch_total
                    or identity_mismatch_total
                )
                else ("different" if missing_total else "matched")
            ),
            "filing_date_mismatch_rows": filing_date_mismatch_total,
            "identity_mismatch_rows": identity_mismatch_total,
            "missing_edgar_rows": missing_total,
            "rows_without_accession": rows_without_accession_total,
            "datasets": datasets,
        }


def _valid_empty_results_response(
    document: dict[str, Any], artifact: dict[str, Any]
) -> bool:
    declared_count = artifact.get("record_count")
    if (
        isinstance(declared_count, bool)
        or not isinstance(declared_count, int)
        or declared_count != 0
        or artifact.get("is_last") is not True
        or artifact.get("next_continuation") not in (None, "")
        or document.get("next_url") not in (None, "")
    ):
        return False
    explicit_zero_count = False
    for field_name in ("count", "queryCount", "resultsCount"):
        if field_name not in document:
            continue
        value = document[field_name]
        if isinstance(value, bool) or not isinstance(value, int) or value != 0:
            return False
        explicit_zero_count = True
    return explicit_zero_count


def _canonical_row_sha256(row: dict[str, Any]) -> bytes | None:
    try:
        serialized = json.dumps(
            row,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError):
        return None
    return hashlib.sha256(serialized).digest()


def _normalized_scalar(value: object) -> str | None:
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        return None
    rendered = str(value).strip()
    return rendered or None


def _accession_key(
    dataset: ProviderDataset,
    accession: str,
    row: dict[str, Any],
) -> tuple[str, str, str, str, str]:
    cik = row.get("cik") or row.get("issuer_cik") or row.get("filer_cik")
    return (
        dataset.value,
        accession.strip(),
        _normalized_scalar(row.get("filing_date")) or "",
        _normalized_scalar(cik) or "",
        _normalized_scalar(row.get("form_type")) or "",
    )


def _json_key(values: tuple[object, ...]) -> str:
    return json.dumps(values, ensure_ascii=False, separators=(",", ":"))


__all__ = [
    "ACCESSION_DETAILS",
    "AUDITED_DATASETS",
    "KEY_DATASETS",
    "REST_SEMANTIC_AUDIT_SCHEMA_VERSION",
    "RestSemanticAuditError",
    "RestSemanticAuditor",
]
