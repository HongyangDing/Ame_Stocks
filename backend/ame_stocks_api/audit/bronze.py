"""Repository-wide, credential-free validation of Massive Bronze artifacts.

The downloader validates each object before it is published.  This module is the
independent, repeatable second line of defence: it rebuilds identities from canonical
requests, verifies every referenced byte, replays gzip CRC checks, and reconciles the
authoritative ten-year request plan with what is present on disk.
"""

from __future__ import annotations

import csv
import gzip
import hashlib
import json
import zlib
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Literal
from urllib.parse import parse_qsl, urlsplit, urlunsplit

from ame_stocks_api.artifacts import ArtifactError, safe_relative_path, stable_digest
from ame_stocks_api.downloads import build_download_plan, market_session_dates
from ame_stocks_api.flatfiles import FlatFileDataset, FlatFileObject, build_flat_file_plan
from ame_stocks_core import PROVIDER_CONTRACT_VERSION, ProviderDataset

AuditMode = Literal["structural", "full"]
Severity = Literal["info", "warning", "error", "critical"]

_REST_MANIFEST_VERSION = 1
_FLAT_MANIFEST_VERSION = 1
_REST_PROVIDER = "massive"
_FLAT_REQUIRED_COLUMNS = (
    "ticker",
    "volume",
    "open",
    "close",
    "high",
    "low",
    "window_start",
    "transactions",
)
_ANNUAL_DATASETS = (
    ProviderDataset.SHORT_INTEREST,
    ProviderDataset.SHORT_VOLUME,
    ProviderDataset.IPOS,
    ProviderDataset.INCOME_STATEMENTS,
    ProviderDataset.BALANCE_SHEETS,
    ProviderDataset.CASH_FLOW_STATEMENTS,
    ProviderDataset.EDGAR_INDEX,
    ProviderDataset.FORM_3,
    ProviderDataset.FORM_4,
    ProviderDataset.RISK_FACTORS,
    ProviderDataset.TEN_K_SECTIONS,
    ProviderDataset.EIGHT_K_TEXT,
    ProviderDataset.EIGHT_K_DISCLOSURES,
    ProviderDataset.NEWS,
)
_QUARTERLY_DATASETS = (ProviderDataset.FORM_13F,)
_RANGE_STARTS: dict[ProviderDataset, date] = {
    ProviderDataset.SPLITS: date(2003, 9, 10),
    ProviderDataset.DIVIDENDS: date(2003, 9, 10),
    ProviderDataset.SHORT_INTEREST: date(2017, 12, 29),
    ProviderDataset.SHORT_VOLUME: date(2024, 2, 6),
    ProviderDataset.IPOS: date(2008, 1, 1),
    ProviderDataset.INCOME_STATEMENTS: date(2009, 3, 29),
    ProviderDataset.BALANCE_SHEETS: date(2009, 3, 29),
    ProviderDataset.CASH_FLOW_STATEMENTS: date(2009, 3, 29),
    ProviderDataset.NEWS: date(2016, 6, 22),
    ProviderDataset.TREASURY_YIELDS: date(1962, 1, 2),
    ProviderDataset.INFLATION: date(1947, 1, 1),
    ProviderDataset.INFLATION_EXPECTATIONS: date(1982, 1, 1),
    ProviderDataset.LABOR_MARKET: date(1948, 1, 1),
}
_RANGE_DATASETS = (
    ProviderDataset.SPLITS,
    ProviderDataset.DIVIDENDS,
    ProviderDataset.TREASURY_YIELDS,
    ProviderDataset.INFLATION,
    ProviderDataset.INFLATION_EXPECTATIONS,
    ProviderDataset.LABOR_MARKET,
)
_SNAPSHOT_DATASETS = (
    ProviderDataset.FLOAT,
    ProviderDataset.TICKER_TYPES,
    ProviderDataset.EXCHANGES,
    ProviderDataset.CONDITION_CODES,
    ProviderDataset.RATIOS,
    ProviderDataset.RISK_TAXONOMY,
    ProviderDataset.DISCLOSURE_TAXONOMY,
)
# This is the authoritative Bronze catalog already acquired for the research platform.
# Financial statements and ratios are intentionally absent: the current account returned
# HTTP 403 for that separate entitlement, so their absence must not make the storage audit
# fail.  If any optional dataset is present, it is still audited and reconciled below.
_REQUIRED_REST_DATASETS = frozenset(
    {
        ProviderDataset.ASSETS,
        ProviderDataset.SPLITS,
        ProviderDataset.DIVIDENDS,
        ProviderDataset.SHORT_INTEREST,
        ProviderDataset.SHORT_VOLUME,
        ProviderDataset.FLOAT,
        ProviderDataset.IPOS,
        ProviderDataset.TICKER_OVERVIEW,
        ProviderDataset.TICKER_EVENTS,
        ProviderDataset.TICKER_TYPES,
        ProviderDataset.EXCHANGES,
        ProviderDataset.CONDITION_CODES,
        ProviderDataset.EDGAR_INDEX,
        ProviderDataset.FORM_3,
        ProviderDataset.FORM_4,
        ProviderDataset.FORM_13F,
        ProviderDataset.RISK_FACTORS,
        ProviderDataset.TEN_K_SECTIONS,
        ProviderDataset.EIGHT_K_TEXT,
        ProviderDataset.EIGHT_K_DISCLOSURES,
        ProviderDataset.DISCLOSURE_TAXONOMY,
        ProviderDataset.NEWS,
        ProviderDataset.TREASURY_YIELDS,
        ProviderDataset.INFLATION,
        ProviderDataset.INFLATION_EXPECTATIONS,
        ProviderDataset.LABOR_MARKET,
        ProviderDataset.RISK_TAXONOMY,
    }
)
_OPTIONAL_ENTITLEMENT_DATASETS = frozenset(
    {
        ProviderDataset.INCOME_STATEMENTS,
        ProviderDataset.BALANCE_SHEETS,
        ProviderDataset.CASH_FLOW_STATEMENTS,
        ProviderDataset.RATIOS,
    }
)
_DATE_FIELDS: dict[str, str] = {
    ProviderDataset.SPLITS.value: "execution_date",
    ProviderDataset.DIVIDENDS.value: "ex_dividend_date",
    ProviderDataset.SHORT_INTEREST.value: "settlement_date",
    ProviderDataset.SHORT_VOLUME.value: "date",
    ProviderDataset.IPOS.value: "listing_date",
    ProviderDataset.INCOME_STATEMENTS.value: "filing_date",
    ProviderDataset.BALANCE_SHEETS.value: "filing_date",
    ProviderDataset.CASH_FLOW_STATEMENTS.value: "filing_date",
    ProviderDataset.EDGAR_INDEX.value: "filing_date",
    ProviderDataset.FORM_3.value: "filing_date",
    ProviderDataset.FORM_4.value: "filing_date",
    ProviderDataset.FORM_13F.value: "filing_date",
    ProviderDataset.RISK_FACTORS.value: "filing_date",
    ProviderDataset.TEN_K_SECTIONS.value: "filing_date",
    ProviderDataset.EIGHT_K_TEXT.value: "filing_date",
    ProviderDataset.EIGHT_K_DISCLOSURES.value: "filing_date",
    ProviderDataset.NEWS.value: "published_utc",
    ProviderDataset.TREASURY_YIELDS.value: "date",
    ProviderDataset.INFLATION.value: "date",
    ProviderDataset.INFLATION_EXPECTATIONS.value: "date",
    ProviderDataset.LABOR_MARKET.value: "date",
}
_REQUIRED_ROW_FIELDS: dict[str, tuple[str, ...]] = {
    ProviderDataset.ASSETS.value: ("ticker", "active"),
    ProviderDataset.SPLITS.value: ("ticker", "execution_date"),
    ProviderDataset.DIVIDENDS.value: ("ticker", "ex_dividend_date"),
    ProviderDataset.SHORT_INTEREST.value: ("ticker", "settlement_date"),
    ProviderDataset.SHORT_VOLUME.value: ("ticker", "date"),
    ProviderDataset.FLOAT.value: ("ticker",),
    ProviderDataset.IPOS.value: ("listing_date",),
    ProviderDataset.INCOME_STATEMENTS.value: ("filing_date",),
    ProviderDataset.BALANCE_SHEETS.value: ("filing_date",),
    ProviderDataset.CASH_FLOW_STATEMENTS.value: ("filing_date",),
    ProviderDataset.RATIOS.value: ("ticker",),
    ProviderDataset.TICKER_OVERVIEW.value: ("ticker",),
    ProviderDataset.TICKER_TYPES.value: ("code",),
    ProviderDataset.EXCHANGES.value: ("id",),
    ProviderDataset.CONDITION_CODES.value: ("id",),
    ProviderDataset.EDGAR_INDEX.value: ("filing_date",),
    ProviderDataset.FORM_3.value: ("filing_date",),
    ProviderDataset.FORM_4.value: ("filing_date",),
    ProviderDataset.FORM_13F.value: ("filing_date",),
    ProviderDataset.RISK_FACTORS.value: ("filing_date",),
    ProviderDataset.TEN_K_SECTIONS.value: ("filing_date",),
    ProviderDataset.EIGHT_K_TEXT.value: ("filing_date",),
    ProviderDataset.EIGHT_K_DISCLOSURES.value: ("filing_date",),
    ProviderDataset.NEWS.value: ("published_utc",),
    ProviderDataset.TREASURY_YIELDS.value: ("date",),
    ProviderDataset.INFLATION.value: ("date",),
    ProviderDataset.INFLATION_EXPECTATIONS.value: ("date",),
    ProviderDataset.LABOR_MARKET.value: ("date",),
}
_PHYSICAL_FAILURE_CODES = frozenset(
    {
        "artifact_missing",
        "asset_reconciliation_unreadable",
        "audit_internal_error",
        "compressed_bytes_mismatch",
        "flat_schema_mismatch",
        "flat_size_mismatch",
        "gzip_corrupt",
        "json_corrupt",
        "missing_flat_artifact",
        "missing_rest_artifact",
        "raw_bytes_mismatch",
        "raw_sha256_mismatch",
        "record_count_mismatch",
        "stored_sha256_mismatch",
    }
)
_PLAN_FAILURE_CODES = frozenset(
    {
        "flat_manifest_incomplete",
        "manifest_failed",
        "manifest_incomplete",
        "market_partition_mismatch",
        "ticker_event_plan_invalid",
        "ticker_event_plan_missing",
        "ticker_overview_plan_invalid",
        "ticker_overview_plan_missing",
    }
)
_SEMANTIC_FAILURE_CODES = frozenset(
    {
        "asset_active_flag_mismatch",
        "asset_active_inactive_overlap",
        "asset_duplicate_ticker",
        "invalid_date_value",
        "required_fields_missing",
        "response_shape_invalid",
        "response_status_not_ok",
        "results_shape_invalid",
        "row_date_outside_request",
        "row_shape_invalid",
    }
)


class BronzeAuditError(RuntimeError):
    """Raised when the audit cannot safely start or publish its report."""


@dataclass(frozen=True, slots=True)
class AuditIssue:
    severity: Severity
    code: str
    message: str
    dataset: str | None = None
    path: str | None = None


@dataclass(slots=True)
class DatasetStats:
    dataset: str
    manifests: int = 0
    complete_manifests: int = 0
    failed_manifests: int = 0
    in_progress_manifests: int = 0
    artifacts: int = 0
    declared_records: int = 0
    verified_records: int = 0
    compressed_bytes: int = 0
    raw_bytes: int = 0
    flat_file_rows: int = 0
    verified_files: int = 0
    observed_min_date: str | None = None
    observed_max_date: str | None = None
    expected_objects: int | None = None
    missing_expected: int = 0
    extra_objects: int = 0
    unavailable_expected: int = 0

    def merge(self, other: DatasetStats) -> None:
        for name in (
            "manifests",
            "complete_manifests",
            "failed_manifests",
            "in_progress_manifests",
            "artifacts",
            "declared_records",
            "verified_records",
            "compressed_bytes",
            "raw_bytes",
            "flat_file_rows",
            "verified_files",
            "missing_expected",
            "extra_objects",
            "unavailable_expected",
        ):
            setattr(self, name, getattr(self, name) + getattr(other, name))
        self.observed_min_date = _min_optional(self.observed_min_date, other.observed_min_date)
        self.observed_max_date = _max_optional(self.observed_max_date, other.observed_max_date)


@dataclass(slots=True)
class _ManifestResult:
    stats: DatasetStats
    issues: list[AuditIssue] = field(default_factory=list)
    referenced_paths: set[str] = field(default_factory=set)
    request_id: str | None = None
    request: dict[str, Any] | None = None
    status: str | None = None
    provider_status_code: int | None = None


class _IssueCollector:
    def __init__(self, *, sample_limit: int = 2_000, samples_per_code: int = 20) -> None:
        self.counts: Counter[str] = Counter()
        self.code_counts: Counter[str] = Counter()
        self.samples: list[AuditIssue] = []
        self.sample_limit = sample_limit
        self.samples_per_code = samples_per_code

    def add(self, issue: AuditIssue) -> None:
        self.counts[issue.severity] += 1
        self.code_counts[issue.code] += 1
        if (
            len(self.samples) < self.sample_limit
            and self.code_counts[issue.code] <= self.samples_per_code
        ):
            self.samples.append(issue)

    def extend(self, issues: list[AuditIssue]) -> None:
        for issue in issues:
            self.add(issue)


class _HashingReader:
    """Hash compressed bytes while gzip consumes the same sequential stream."""

    def __init__(self, handle: Any) -> None:
        self.handle = handle
        self.digest = hashlib.sha256()
        self.bytes_read = 0

    def read(self, size: int = -1) -> bytes:
        content = self.handle.read(size)
        self.digest.update(content)
        self.bytes_read += len(content)
        return content

    def tell(self) -> int:
        return self.handle.tell()


class BronzeAuditor:
    """Perform a bounded-memory audit of all Massive Bronze files under one root."""

    report_schema_version = 2

    def __init__(
        self,
        data_root: Path,
        *,
        start: date,
        end: date,
        mode: AuditMode = "full",
        workers: int = 2,
        ticker_overview_plan: Path | None = None,
        ticker_event_plan: Path | None = None,
        ticker_event_start: date = date(2003, 9, 10),
        required_rest_datasets: tuple[ProviderDataset, ...] | None = None,
    ) -> None:
        if start > end:
            raise ValueError("start must be on or before end")
        if mode not in {"structural", "full"}:
            raise ValueError("mode must be structural or full")
        if workers < 1:
            raise ValueError("workers must be positive")
        self.data_root = data_root.expanduser().resolve()
        self.start = start
        self.end = end
        self.mode = mode
        self.workers = workers
        self.ticker_overview_plan = ticker_overview_plan
        self.ticker_event_plan = ticker_event_plan
        self.ticker_event_start = ticker_event_start
        self.required_rest_datasets = frozenset(
            _REQUIRED_REST_DATASETS
            if required_rest_datasets is None
            else required_rest_datasets
        )
        self._issues = _IssueCollector()
        self._stats: dict[str, DatasetStats] = {}
        self._rest_results: list[_ManifestResult] = []
        self._flat_results: list[_ManifestResult] = []
        self._plan_receipts: list[dict[str, object]] = []

    def run(self) -> dict[str, Any]:
        if not self.data_root.is_dir():
            raise BronzeAuditError(f"data root is missing: {self.data_root}")

        started = datetime.now(UTC)
        self._rest_results = self._run_manifest_group(
            self._rest_manifest_paths(), self._audit_rest_manifest
        )
        self._flat_results = self._run_manifest_group(
            self._flat_manifest_paths(), self._audit_flat_manifest
        )
        self._reconcile_expected_plans()
        self._reconcile_assets()
        self._find_orphans()
        self._find_partial_files()
        finished = datetime.now(UTC)

        severity = self._issues.counts
        code_counts = self._issues.code_counts
        status = (
            "failed"
            if severity["critical"] or severity["error"]
            else ("passed_with_warnings" if severity["warning"] else "passed")
        )
        datasets = [asdict(self._stats[key]) for key in sorted(self._stats)]
        return {
            "report_schema_version": self.report_schema_version,
            "status": status,
            "mode": self.mode,
            "data_root": str(self.data_root),
            "expected_window": {"start": self.start.isoformat(), "end": self.end.isoformat()},
            "required_profile": {
                "rest_datasets": sorted(dataset.value for dataset in self.required_rest_datasets),
                "flat_file_datasets": sorted(dataset.value for dataset in FlatFileDataset),
                "optional_entitlement_datasets": sorted(
                    dataset.value for dataset in _OPTIONAL_ENTITLEMENT_DATASETS
                ),
            },
            "started_at": started.isoformat(),
            "completed_at": finished.isoformat(),
            "duration_seconds": round((finished - started).total_seconds(), 3),
            "summary": {
                "datasets": len(datasets),
                "manifests": sum(item["manifests"] for item in datasets),
                "artifacts": sum(item["artifacts"] for item in datasets),
                "verified_files": sum(item["verified_files"] for item in datasets),
                "compressed_bytes": sum(item["compressed_bytes"] for item in datasets),
                "declared_records": sum(item["declared_records"] for item in datasets),
                "verified_records": sum(item["verified_records"] for item in datasets),
                "issue_counts": dict(sorted(severity.items())),
                "issue_code_counts": dict(sorted(code_counts.items())),
            },
            "gates": {
                "physical_integrity": _gate_status(code_counts, _PHYSICAL_FAILURE_CODES),
                "authoritative_plan": _gate_status(
                    code_counts,
                    _PLAN_FAILURE_CODES,
                    prefix="missing_expected_",
                ),
                "semantic_consistency": _gate_status(code_counts, _SEMANTIC_FAILURE_CODES),
            },
            "datasets": datasets,
            "plan_receipts": self._plan_receipts,
            "issue_samples": [asdict(issue) for issue in self._issues.samples],
            "method": {
                "full": (
                    "SHA-256, complete gzip read/CRC, raw SHA-256, bytes, JSON/CSV header, "
                    "record counts, pagination, canonical identity, expected plans, orphans, "
                    "and active/inactive mutual exclusion"
                ),
                "structural": (
                    "manifest identity, paths, sizes, statuses, expected plans, and orphans; "
                    "payload bytes are not reread"
                ),
            }[self.mode],
        }

    def _run_manifest_group(self, paths: list[Path], function: Any) -> list[_ManifestResult]:
        results: list[_ManifestResult] = []
        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            futures = {executor.submit(function, path): path for path in paths}
            for future in as_completed(futures):
                path = futures[future]
                try:
                    result = future.result()
                except Exception as exc:  # defensive: one bad file must not abort the audit
                    result = _ManifestResult(
                        stats=DatasetStats(dataset=_dataset_from_path(path)),
                        issues=[
                            AuditIssue(
                                "critical",
                                "audit_internal_error",
                                f"auditor could not inspect file: {type(exc).__name__}: {exc}",
                                _dataset_from_path(path),
                                str(path),
                            )
                        ],
                    )
                results.append(result)
                self._stats.setdefault(
                    result.stats.dataset, DatasetStats(result.stats.dataset)
                ).merge(result.stats)
                self._issues.extend(result.issues)
        return results

    def _rest_manifest_paths(self) -> list[Path]:
        root = self.data_root / "manifests" / _REST_PROVIDER
        if not root.is_dir():
            self._issues.add(
                AuditIssue(
                    "critical", "rest_manifest_root_missing", "REST manifest root is missing"
                )
            )
            return []
        return sorted(path for path in root.glob("*/*.json") if path.parent.name != "flatfiles")

    def _flat_manifest_paths(self) -> list[Path]:
        root = self.data_root / "manifests" / _REST_PROVIDER / "flatfiles"
        if not root.is_dir():
            self._issues.add(
                AuditIssue(
                    "critical", "flat_manifest_root_missing", "Flat File manifest root is missing"
                )
            )
            return []
        return sorted(root.glob("*/*.json"))

    def _audit_rest_manifest(self, path: Path) -> _ManifestResult:
        dataset = path.parent.name
        stats = DatasetStats(dataset=dataset, manifests=1)
        issues: list[AuditIssue] = []
        try:
            manifest = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            return _ManifestResult(
                stats,
                [AuditIssue("critical", "manifest_invalid_json", str(exc), dataset, str(path))],
            )
        if not isinstance(manifest, dict):
            return _ManifestResult(
                stats,
                [
                    AuditIssue(
                        "critical",
                        "manifest_invalid_root",
                        "manifest root is not an object",
                        dataset,
                        str(path),
                    )
                ],
            )

        status = str(manifest.get("status", ""))
        failure = manifest.get("failure")
        provider_status = failure.get("provider_status_code") if isinstance(failure, dict) else None
        provider_status = provider_status if isinstance(provider_status, int) else None
        stats.complete_manifests = int(status == "complete")
        stats.failed_manifests = int(status == "failed")
        stats.in_progress_manifests = int(status in {"pending", "in_progress"})
        if status == "failed":
            if dataset == ProviderDataset.TICKER_EVENTS.value and provider_status == 404:
                issues.append(
                    AuditIssue(
                        "warning",
                        "ticker_event_identifier_not_found",
                        "Massive returned HTTP 404 for this identifier",
                        dataset,
                        str(path),
                    )
                )
            else:
                issues.append(
                    AuditIssue(
                        "error",
                        "manifest_failed",
                        "download manifest is failed",
                        dataset,
                        str(path),
                    )
                )
        elif status != "complete":
            issues.append(
                AuditIssue(
                    "error",
                    "manifest_incomplete",
                    f"manifest status is {status!r}",
                    dataset,
                    str(path),
                )
            )

        request = manifest.get("request")
        request_id = str(manifest.get("request_id", ""))
        if not isinstance(request, dict):
            issues.append(
                AuditIssue(
                    "critical",
                    "manifest_request_invalid",
                    "request is not an object",
                    dataset,
                    str(path),
                )
            )
            request = None
        else:
            calculated = stable_digest(request)
            if calculated != request_id or path.stem != request_id:
                issues.append(
                    AuditIssue(
                        "critical",
                        "request_identity_mismatch",
                        "canonical request hash, manifest request_id, and filename differ",
                        dataset,
                        str(path),
                    )
                )
            if request.get("dataset") != dataset or manifest.get("dataset") != dataset:
                issues.append(
                    AuditIssue(
                        "critical",
                        "dataset_identity_mismatch",
                        "directory, manifest dataset, and request dataset differ",
                        dataset,
                        str(path),
                    )
                )

        if manifest.get("manifest_schema_version") != _REST_MANIFEST_VERSION:
            issues.append(
                AuditIssue(
                    "critical",
                    "manifest_schema_mismatch",
                    "unexpected REST manifest schema",
                    dataset,
                    str(path),
                )
            )
        if manifest.get("provider") != _REST_PROVIDER:
            issues.append(
                AuditIssue(
                    "critical", "provider_mismatch", "unexpected provider", dataset, str(path)
                )
            )
        if manifest.get("provider_contract_version") != PROVIDER_CONTRACT_VERSION:
            issues.append(
                AuditIssue(
                    "critical",
                    "provider_contract_mismatch",
                    "provider contract version differs",
                    dataset,
                    str(path),
                )
            )

        artifacts = manifest.get("artifacts")
        if not isinstance(artifacts, list):
            artifacts = []
            issues.append(
                AuditIssue(
                    "critical", "artifacts_invalid", "artifacts is not an array", dataset, str(path)
                )
            )
        if status == "complete" and (not artifacts or manifest.get("checkpoint") is not None):
            issues.append(
                AuditIssue(
                    "critical",
                    "complete_manifest_invalid_state",
                    "complete manifest needs artifacts and no checkpoint",
                    dataset,
                    str(path),
                )
            )
        sequences = [item.get("sequence") for item in artifacts if isinstance(item, dict)]
        if sequences != list(range(len(artifacts))):
            issues.append(
                AuditIssue(
                    "critical",
                    "page_sequence_mismatch",
                    "page sequence is not contiguous",
                    dataset,
                    str(path),
                )
            )

        referenced: set[str] = set()
        for index, artifact in enumerate(artifacts):
            if not isinstance(artifact, dict):
                issues.append(
                    AuditIssue(
                        "critical",
                        "artifact_invalid",
                        "artifact is not an object",
                        dataset,
                        str(path),
                    )
                )
                continue
            stats.artifacts += 1
            stats.declared_records += _safe_int(artifact.get("record_count"))
            stats.compressed_bytes += _safe_int(artifact.get("compressed_bytes"))
            stats.raw_bytes += _safe_int(artifact.get("raw_bytes"))
            relative = artifact.get("path")
            expected = (
                f"bronze/{_REST_PROVIDER}/{dataset}/request_id={request_id}/"
                f"page-{index:05d}.json.gz"
            )
            if relative != expected:
                issues.append(
                    AuditIssue(
                        "critical",
                        "artifact_path_mismatch",
                        f"expected {expected}",
                        dataset,
                        str(path),
                    )
                )
            if isinstance(relative, str):
                referenced.add(relative)
            if self.mode == "full":
                self._verify_rest_artifact(
                    dataset=dataset,
                    manifest_path=path,
                    artifact=artifact,
                    expected_sequence=index,
                    stats=stats,
                    issues=issues,
                    request=request,
                )
            else:
                self._verify_structural_file(dataset, path, artifact, stats, issues)

        return _ManifestResult(
            stats,
            issues,
            referenced,
            request_id,
            request,
            status,
            provider_status,
        )

    def _verify_structural_file(
        self,
        dataset: str,
        manifest_path: Path,
        artifact: dict[str, Any],
        stats: DatasetStats,
        issues: list[AuditIssue],
    ) -> None:
        try:
            payload_path = safe_relative_path(self.data_root, artifact.get("path"))
            size = payload_path.stat().st_size
        except (ArtifactError, OSError, ValueError) as exc:
            issues.append(
                AuditIssue("critical", "artifact_missing", str(exc), dataset, str(manifest_path))
            )
            return
        if size != _safe_int(artifact.get("compressed_bytes")):
            issues.append(
                AuditIssue(
                    "critical",
                    "compressed_bytes_mismatch",
                    "file size differs",
                    dataset,
                    str(payload_path),
                )
            )
        stats.verified_files += 1

    def _verify_rest_artifact(
        self,
        *,
        dataset: str,
        manifest_path: Path,
        artifact: dict[str, Any],
        expected_sequence: int,
        stats: DatasetStats,
        issues: list[AuditIssue],
        request: dict[str, Any] | None,
    ) -> None:
        try:
            payload_path = safe_relative_path(self.data_root, artifact.get("path"))
            compressed = payload_path.read_bytes()
        except (ArtifactError, OSError, ValueError) as exc:
            issues.append(
                AuditIssue("critical", "artifact_missing", str(exc), dataset, str(manifest_path))
            )
            return
        if len(compressed) != _safe_int(artifact.get("compressed_bytes")):
            issues.append(
                AuditIssue(
                    "critical",
                    "compressed_bytes_mismatch",
                    "compressed byte count differs",
                    dataset,
                    str(payload_path),
                )
            )
        if hashlib.sha256(compressed).hexdigest() != artifact.get("stored_sha256"):
            issues.append(
                AuditIssue(
                    "critical",
                    "stored_sha256_mismatch",
                    "compressed SHA-256 differs",
                    dataset,
                    str(payload_path),
                )
            )
        try:
            raw = gzip.decompress(compressed)
        except (OSError, EOFError) as exc:
            issues.append(
                AuditIssue(
                    "critical",
                    "gzip_corrupt",
                    f"gzip validation failed: {exc}",
                    dataset,
                    str(payload_path),
                )
            )
            return
        if len(raw) != _safe_int(artifact.get("raw_bytes")):
            issues.append(
                AuditIssue(
                    "critical",
                    "raw_bytes_mismatch",
                    "raw byte count differs",
                    dataset,
                    str(payload_path),
                )
            )
        if hashlib.sha256(raw).hexdigest() != artifact.get("raw_sha256"):
            issues.append(
                AuditIssue(
                    "critical",
                    "raw_sha256_mismatch",
                    "raw SHA-256 differs",
                    dataset,
                    str(payload_path),
                )
            )
        try:
            document = json.loads(raw)
        except (UnicodeError, json.JSONDecodeError) as exc:
            issues.append(
                AuditIssue("critical", "json_corrupt", str(exc), dataset, str(payload_path))
            )
            return
        if not isinstance(document, dict):
            issues.append(
                AuditIssue(
                    "critical",
                    "response_shape_invalid",
                    "response root is not an object",
                    dataset,
                    str(payload_path),
                )
            )
            return
        if document.get("status") is not None and str(document.get("status")).upper() != "OK":
            issues.append(
                AuditIssue(
                    "critical",
                    "response_status_not_ok",
                    "provider response status is not OK",
                    dataset,
                    str(payload_path),
                )
            )
        rows = _result_rows(document)
        if rows is None:
            issues.append(
                AuditIssue(
                    "critical",
                    "results_shape_invalid",
                    "results has an unsupported shape",
                    dataset,
                    str(payload_path),
                )
            )
            rows = []
        actual_count = len(rows)
        stats.verified_records += actual_count
        stats.verified_files += 1
        if actual_count != _safe_int(artifact.get("record_count")):
            issues.append(
                AuditIssue(
                    "critical",
                    "record_count_mismatch",
                    "manifest and response row count differ",
                    dataset,
                    str(payload_path),
                )
            )
        response_count = document.get("count")
        if isinstance(response_count, int) and response_count != actual_count:
            issues.append(
                AuditIssue(
                    "warning",
                    "response_count_mismatch",
                    "provider count differs from current page rows",
                    dataset,
                    str(payload_path),
                )
            )

        continuation = _safe_continuation(document.get("next_url"))
        declared_continuation = artifact.get("next_continuation")
        is_last = bool(artifact.get("is_last"))
        if continuation != declared_continuation:
            issues.append(
                AuditIssue(
                    "critical",
                    "continuation_mismatch",
                    "next_url and manifest continuation differ",
                    dataset,
                    str(payload_path),
                )
            )
        if is_last != (continuation is None):
            issues.append(
                AuditIssue(
                    "critical",
                    "last_page_mismatch",
                    "is_last conflicts with continuation",
                    dataset,
                    str(payload_path),
                )
            )
        if int(artifact.get("sequence", -1)) != expected_sequence:
            issues.append(
                AuditIssue(
                    "critical",
                    "page_sequence_mismatch",
                    "artifact sequence differs",
                    dataset,
                    str(payload_path),
                )
            )

        required = _REQUIRED_ROW_FIELDS.get(dataset, ())
        missing_required = 0
        invalid_dates = 0
        observed_dates: list[str] = []
        date_field = _DATE_FIELDS.get(dataset)
        for row in rows:
            if not isinstance(row, dict):
                issues.append(
                    AuditIssue(
                        "critical",
                        "row_shape_invalid",
                        "result row is not an object",
                        dataset,
                        str(payload_path),
                    )
                )
                continue
            if any(
                field_name not in row or row[field_name] in {None, ""} for field_name in required
            ):
                missing_required += 1
            if date_field:
                raw_date = row.get(date_field)
                normalized = _iso_date(raw_date)
                if normalized:
                    observed_dates.append(normalized)
                    if request and not _inside_request(normalized, request):
                        issues.append(
                            AuditIssue(
                                "error",
                                "row_date_outside_request",
                                f"{date_field}={normalized} is outside request window",
                                dataset,
                                str(payload_path),
                            )
                        )
                elif raw_date is not None and raw_date != "":
                    invalid_dates += 1
        if missing_required:
            issues.append(
                AuditIssue(
                    "error",
                    "required_fields_missing",
                    f"{missing_required} rows omit one or more required audit fields",
                    dataset,
                    str(payload_path),
                )
            )
        if invalid_dates:
            issues.append(
                AuditIssue(
                    "error",
                    "invalid_date_value",
                    f"{invalid_dates} rows contain a non-ISO {date_field} value",
                    dataset,
                    str(payload_path),
                )
            )
        if observed_dates:
            stats.observed_min_date = min(observed_dates)
            stats.observed_max_date = max(observed_dates)

    def _audit_flat_manifest(self, path: Path) -> _ManifestResult:
        dataset = path.parent.name
        stats = DatasetStats(dataset=dataset, manifests=1)
        issues: list[AuditIssue] = []
        try:
            manifest = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            return _ManifestResult(
                stats,
                [AuditIssue("critical", "manifest_invalid_json", str(exc), dataset, str(path))],
            )
        if not isinstance(manifest, dict):
            return _ManifestResult(
                stats,
                [
                    AuditIssue(
                        "critical",
                        "manifest_invalid_root",
                        "manifest root is not an object",
                        dataset,
                        str(path),
                    )
                ],
            )
        status = str(manifest.get("status", ""))
        stats.complete_manifests = int(status == "complete")
        stats.failed_manifests = int(status == "failed")
        stats.in_progress_manifests = int(status in {"pending", "in_progress"})
        if status != "complete":
            issues.append(
                AuditIssue(
                    "error",
                    "flat_manifest_incomplete",
                    f"manifest status is {status!r}",
                    dataset,
                    str(path),
                )
            )
        if manifest.get("flat_file_manifest_schema_version") != _FLAT_MANIFEST_VERSION:
            issues.append(
                AuditIssue(
                    "critical",
                    "manifest_schema_mismatch",
                    "unexpected Flat File manifest schema",
                    dataset,
                    str(path),
                )
            )
        try:
            session = date.fromisoformat(str(manifest.get("session_date")))
            flat_dataset = FlatFileDataset(dataset)
            item = FlatFileObject(flat_dataset, session)
        except ValueError as exc:
            issues.append(
                AuditIssue("critical", "flat_identity_invalid", str(exc), dataset, str(path))
            )
            return _ManifestResult(stats, issues, status=status)
        if (
            path.stem != session.isoformat()
            or manifest.get("dataset") != dataset
            or manifest.get("object_key") != item.object_key
            or manifest.get("object_id") != item.object_id
        ):
            issues.append(
                AuditIssue(
                    "critical",
                    "flat_identity_mismatch",
                    "date, dataset, object key, object ID, or filename differs",
                    dataset,
                    str(path),
                )
            )
        output = manifest.get("output")
        if not isinstance(output, dict):
            issues.append(
                AuditIssue(
                    "critical",
                    "flat_output_missing",
                    "complete manifest has no output",
                    dataset,
                    str(path),
                )
            )
            return _ManifestResult(stats, issues, status=status)
        relative = output.get("path")
        expected = f"bronze/{_REST_PROVIDER}/flatfiles/{item.object_key}"
        if relative != expected:
            issues.append(
                AuditIssue(
                    "critical", "flat_path_mismatch", f"expected {expected}", dataset, str(path)
                )
            )
        referenced = {relative} if isinstance(relative, str) else set()
        stats.artifacts = 1
        stats.compressed_bytes = _safe_int(output.get("bytes"))
        stats.observed_min_date = stats.observed_max_date = session.isoformat()
        try:
            payload_path = safe_relative_path(self.data_root, relative)
            size = payload_path.stat().st_size
        except (ArtifactError, OSError, ValueError) as exc:
            issues.append(AuditIssue("critical", "artifact_missing", str(exc), dataset, str(path)))
            return _ManifestResult(stats, issues, referenced, status=status)
        remote = manifest.get("remote")
        remote_size = _safe_int(remote.get("content_length")) if isinstance(remote, dict) else -1
        if size != _safe_int(output.get("bytes")) or size != remote_size:
            issues.append(
                AuditIssue(
                    "critical",
                    "flat_size_mismatch",
                    "file stat, output bytes, and remote content length differ",
                    dataset,
                    str(payload_path),
                )
            )
        if self.mode == "full":
            physical = _stream_gzip_file(payload_path)
            if physical["sha256"] != output.get("sha256"):
                issues.append(
                    AuditIssue(
                        "critical",
                        "stored_sha256_mismatch",
                        "compressed SHA-256 differs",
                        dataset,
                        str(payload_path),
                    )
                )
            if physical["error"]:
                issues.append(
                    AuditIssue(
                        "critical",
                        "gzip_corrupt",
                        str(physical["error"]),
                        dataset,
                        str(payload_path),
                    )
                )
            header = physical["header"]
            if header != list(_FLAT_REQUIRED_COLUMNS) or header != output.get("csv_header"):
                issues.append(
                    AuditIssue(
                        "critical",
                        "flat_schema_mismatch",
                        f"unexpected CSV header: {header}",
                        dataset,
                        str(payload_path),
                    )
                )
            stats.flat_file_rows = int(physical["rows"])
        stats.verified_files = 1
        return _ManifestResult(stats, issues, referenced, status=status)

    def _reconcile_expected_plans(self) -> None:
        # Seed the required catalog before looking at observed manifests.  Without this,
        # deleting an entire dataset directory made it disappear from both sides of the
        # comparison and the audit could falsely pass.
        for dataset in self.required_rest_datasets:
            self._stats.setdefault(dataset.value, DatasetStats(dataset.value))

        sessions = set(market_session_dates(self.start, self.end))
        flat_by_dataset: dict[str, set[date]] = defaultdict(set)
        for result in self._flat_results:
            if result.status != "complete" or result.stats.observed_min_date is None:
                continue
            flat_by_dataset[result.stats.dataset].add(
                date.fromisoformat(result.stats.observed_min_date)
            )
        for dataset in FlatFileDataset:
            expected = set(
                item.session_date
                for item in build_flat_file_plan(
                    dataset=dataset, start=self.start, end=self.end
                ).objects
            )
            actual = flat_by_dataset[dataset.value]
            self._record_plan_diff(dataset.value, expected, actual, "flat_session")

        complete_by_dataset: dict[str, set[str]] = defaultdict(set)
        terminal_by_dataset: dict[str, set[str]] = defaultdict(set)
        unavailable_by_dataset: dict[str, set[str]] = defaultdict(set)
        for result in self._rest_results:
            if result.request_id and result.status == "complete":
                complete_by_dataset[result.stats.dataset].add(result.request_id)
                terminal_by_dataset[result.stats.dataset].add(result.request_id)
            elif (
                result.request_id
                and result.stats.dataset == ProviderDataset.TICKER_EVENTS.value
                and result.status == "failed"
                and result.provider_status_code == 404
            ):
                terminal_by_dataset[result.stats.dataset].add(result.request_id)
                unavailable_by_dataset[result.stats.dataset].add(result.request_id)
        expected_requests: dict[str, set[str]] = defaultdict(set)
        assets = build_download_plan(
            dataset=ProviderDataset.ASSETS, start=self.start, end=self.end, active="both"
        )
        expected_requests[ProviderDataset.ASSETS.value].update(
            request.request_id for request in assets.requests
        )
        for dataset in (*_ANNUAL_DATASETS, *_QUARTERLY_DATASETS):
            if dataset.value not in self._stats:
                continue
            plan = build_download_plan(
                dataset=dataset,
                start=_RANGE_STARTS.get(dataset, self.start),
                end=self.end,
            )
            expected_requests[dataset.value].update(request.request_id for request in plan.requests)
        for dataset in _RANGE_DATASETS:
            if dataset.value not in self._stats:
                continue
            plan = build_download_plan(
                dataset=dataset,
                start=_RANGE_STARTS[dataset],
                end=self.end,
            )
            expected_requests[dataset.value].update(request.request_id for request in plan.requests)
        for dataset in _SNAPSHOT_DATASETS:
            if dataset.value not in self._stats:
                continue
            plan = build_download_plan(dataset=dataset, start=self.end, end=self.end)
            expected_requests[dataset.value].update(request.request_id for request in plan.requests)
        self._add_ticker_overview_plan(expected_requests)
        self._add_ticker_event_plan(expected_requests)
        for dataset, request_ids in expected_requests.items():
            actual = (
                terminal_by_dataset[dataset]
                if dataset == ProviderDataset.TICKER_EVENTS.value
                else complete_by_dataset[dataset]
            )
            self._record_plan_diff(dataset, request_ids, actual, "rest_request")
            if dataset == ProviderDataset.TICKER_EVENTS.value:
                self._stats[dataset].unavailable_expected = len(
                    request_ids & unavailable_by_dataset[dataset]
                )

        minute = flat_by_dataset[FlatFileDataset.MINUTE_AGGREGATES.value]
        daily = flat_by_dataset[FlatFileDataset.DAY_AGGREGATES.value]
        if minute != daily or minute != sessions:
            self._issues.add(
                AuditIssue(
                    "error",
                    "market_partition_mismatch",
                    (
                        f"expected={len(sessions)}, minute={len(minute)}, day={len(daily)}, "
                        f"minute_only={len(minute - daily)}, day_only={len(daily - minute)}"
                    ),
                )
            )

    def _add_ticker_overview_plan(self, expected: dict[str, set[str]]) -> None:
        dataset = ProviderDataset.TICKER_OVERVIEW
        if dataset.value not in self._stats:
            return
        path = self.ticker_overview_plan or (
            self.data_root
            / "staging"
            / "ticker_overview"
            / "schema=v2"
            / f"window={self.start.isoformat()}_{self.end.isoformat()}"
            / "requests.csv"
        )
        if not path.is_file():
            self._issues.add(
                AuditIssue(
                    "error",
                    "ticker_overview_plan_missing",
                    f"authoritative ticker/query-date receipt is missing: {path}",
                    dataset.value,
                )
            )
            return
        ticker_dates: list[tuple[str, date]] = []
        try:
            with path.open(encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                if reader.fieldnames != ["ticker", "query_date"]:
                    raise ValueError("receipt must have exact ticker,query_date columns")
                for row in reader:
                    ticker_dates.append((str(row["ticker"]), date.fromisoformat(row["query_date"])))
        except (OSError, ValueError) as exc:
            self._issues.add(
                AuditIssue(
                    "error", "ticker_overview_plan_invalid", str(exc), dataset.value, str(path)
                )
            )
            return
        plan = build_download_plan(
            dataset=dataset,
            start=self.start,
            end=self.end,
            ticker_dates=tuple(ticker_dates),
        )
        expected[dataset.value].update(request.request_id for request in plan.requests)
        self._record_plan_receipt(path, dataset.value, len(plan.requests))

    def _add_ticker_event_plan(self, expected: dict[str, set[str]]) -> None:
        dataset = ProviderDataset.TICKER_EVENTS
        if dataset.value not in self._stats:
            return
        path = self.ticker_event_plan or (
            self.data_root / "manifests" / "plans" / "ticker_events" / "identifiers.txt"
        )
        if not path.is_file():
            self._issues.add(
                AuditIssue(
                    "error",
                    "ticker_event_plan_missing",
                    f"authoritative identifier receipt is missing: {path}",
                    dataset.value,
                )
            )
            return
        try:
            identifiers = tuple(
                line.partition("#")[0].strip()
                for line in path.read_text(encoding="utf-8").splitlines()
                if line.partition("#")[0].strip()
            )
            plan = build_download_plan(
                dataset=dataset,
                start=self.ticker_event_start,
                end=self.end,
                tickers=identifiers,
            )
        except (OSError, ValueError) as exc:
            self._issues.add(
                AuditIssue("error", "ticker_event_plan_invalid", str(exc), dataset.value, str(path))
            )
            return
        expected[dataset.value].update(request.request_id for request in plan.requests)
        self._record_plan_receipt(path, dataset.value, len(plan.requests))

    def _record_plan_receipt(self, path: Path, dataset: str, rows: int) -> None:
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        self._plan_receipts.append(
            {"dataset": dataset, "path": str(path.resolve()), "rows": rows, "sha256": digest}
        )

    def _record_plan_diff(
        self,
        dataset: str,
        expected: set[Any],
        actual: set[Any],
        kind: str,
    ) -> None:
        stats = self._stats.setdefault(dataset, DatasetStats(dataset))
        stats.expected_objects = len(expected)
        missing = expected - actual
        extras = actual - expected
        stats.missing_expected = len(missing)
        stats.extra_objects = len(extras)
        if missing:
            examples = ", ".join(str(item) for item in sorted(missing)[:5])
            self._issues.add(
                AuditIssue(
                    "error",
                    f"missing_expected_{kind}",
                    f"{len(missing)} expected items are not complete; examples: {examples}",
                    dataset,
                )
            )
        if extras:
            examples = ", ".join(str(item) for item in sorted(extras)[:5])
            self._issues.add(
                AuditIssue(
                    "warning",
                    f"extra_{kind}",
                    f"{len(extras)} complete items are outside the authoritative plan; "
                    f"examples: {examples}",
                    dataset,
                )
            )

    def _reconcile_assets(self) -> None:
        """Exhaustively prove each daily active/inactive pair is mutually exclusive."""

        if self.mode != "full":
            return
        by_date: dict[str, dict[str, _ManifestResult]] = defaultdict(dict)
        for result in self._rest_results:
            if result.stats.dataset != ProviderDataset.ASSETS.value or result.status != "complete":
                continue
            request = result.request or {}
            parameters = request.get("parameters")
            active = parameters.get("active") if isinstance(parameters, dict) else None
            if request.get("start") == request.get("end") and active in {"true", "false"}:
                by_date[str(request["start"])][str(active)] = result

        for session in market_session_dates(self.start, self.end):
            pair = by_date.get(session.isoformat(), {})
            if set(pair) != {"true", "false"}:
                continue
            try:
                active_tickers, active_bad, active_duplicates = self._asset_tickers(pair["true"])
                inactive_tickers, inactive_bad, inactive_duplicates = self._asset_tickers(
                    pair["false"]
                )
            except Exception as exc:  # already-invalid bytes must not suppress the JSON report
                self._issues.add(
                    AuditIssue(
                        "critical",
                        "asset_reconciliation_unreadable",
                        "active/inactive reconciliation could not reread the verified pages: "
                        f"{type(exc).__name__}: {exc}",
                        ProviderDataset.ASSETS.value,
                        session.isoformat(),
                    )
                )
                continue
            if active_bad or inactive_bad:
                self._issues.add(
                    AuditIssue(
                        "error",
                        "asset_active_flag_mismatch",
                        f"{active_bad + inactive_bad} rows contradict the request active flag",
                        ProviderDataset.ASSETS.value,
                        session.isoformat(),
                    )
                )
            if active_duplicates or inactive_duplicates:
                self._issues.add(
                    AuditIssue(
                        "error",
                        "asset_duplicate_ticker",
                        f"{active_duplicates + inactive_duplicates} duplicate ticker rows",
                        ProviderDataset.ASSETS.value,
                        session.isoformat(),
                    )
                )
            overlap = active_tickers & inactive_tickers
            if overlap:
                self._issues.add(
                    AuditIssue(
                        "critical",
                        "asset_active_inactive_overlap",
                        f"{len(overlap)} tickers appear in both snapshots; "
                        f"examples: {sorted(overlap)[:5]}",
                        ProviderDataset.ASSETS.value,
                        session.isoformat(),
                    )
                )

    def _asset_tickers(self, result: _ManifestResult) -> tuple[set[str], int, int]:
        expected_flag = (result.request or {}).get("parameters", {}).get("active") == "true"
        tickers: set[str] = set()
        bad_flags = duplicates = 0
        manifest_path = (
            self.data_root
            / "manifests"
            / _REST_PROVIDER
            / ProviderDataset.ASSETS.value
            / f"{result.request_id}.json"
        )
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        for artifact in manifest.get("artifacts", []):
            page = safe_relative_path(self.data_root, artifact.get("path"))
            document = json.loads(gzip.decompress(page.read_bytes()))
            for row in document.get("results", []):
                ticker = row.get("ticker") if isinstance(row, dict) else None
                if not isinstance(ticker, str) or not ticker:
                    continue
                if ticker in tickers:
                    duplicates += 1
                tickers.add(ticker)
                if row.get("active") is not expected_flag:
                    bad_flags += 1
        return tickers, bad_flags, duplicates

    def _find_orphans(self) -> None:
        rest_referenced = set().union(*(result.referenced_paths for result in self._rest_results))
        flat_referenced = set().union(*(result.referenced_paths for result in self._flat_results))
        rest_root = self.data_root / "bronze" / _REST_PROVIDER
        actual_rest = {
            str(path.relative_to(self.data_root))
            for path in rest_root.glob("*/request_id=*/page-*.json.gz")
        }
        actual_flat = {
            str(path.relative_to(self.data_root))
            for path in (rest_root / "flatfiles").glob("**/*.csv.gz")
        }
        for label, actual, referenced in (
            ("rest", actual_rest, rest_referenced),
            ("flat", actual_flat, flat_referenced),
        ):
            orphan = actual - referenced
            missing = referenced - actual
            if orphan:
                self._issues.add(
                    AuditIssue(
                        "warning",
                        f"orphan_{label}_artifact",
                        f"{len(orphan)} files are not referenced; examples: {sorted(orphan)[:5]}",
                    )
                )
            if missing:
                self._issues.add(
                    AuditIssue(
                        "critical",
                        f"missing_{label}_artifact",
                        f"{len(missing)} referenced files are absent; "
                        f"examples: {sorted(missing)[:5]}",
                    )
                )

    def _find_partial_files(self) -> None:
        partials = list((self.data_root / "tmp" / "massive_flatfiles").glob("**/*.part"))
        invalid = list((self.data_root / "tmp" / "massive_flatfiles").glob("**/*.invalid-*"))
        if partials:
            self._issues.add(
                AuditIssue(
                    "warning",
                    "partial_flat_files",
                    f"{len(partials)} resumable partial files remain",
                )
            )
        if invalid:
            self._issues.add(
                AuditIssue(
                    "error",
                    "quarantined_flat_files",
                    f"{len(invalid)} quarantined invalid files remain",
                )
            )


def _result_rows(document: dict[str, Any]) -> list[Any] | None:
    results = document.get("results")
    if isinstance(results, list):
        return results
    if isinstance(results, dict):
        events = results.get("events")
        if isinstance(events, list):
            return events
        return [results]
    if results is None:
        return []
    return None


def _safe_continuation(value: object) -> str | None:
    if value in {None, ""}:
        return None
    if not isinstance(value, str):
        return "[invalid]"
    parsed = urlsplit(value)
    if any(key.lower() == "apikey" for key, _ in parse_qsl(parsed.query)):
        return "[credential-in-url]"
    return urlunsplit(("", "", parsed.path, parsed.query, ""))


def _stream_gzip_file(path: Path) -> dict[str, object]:
    rows = 0
    header_bytes = b""
    tail = b""
    error: str | None = None
    header: list[str] = []
    with path.open("rb") as raw:
        tracked = _HashingReader(raw)
        try:
            with gzip.GzipFile(fileobj=tracked, mode="rb") as handle:
                first = True
                while chunk := handle.read(8 * 1024 * 1024):
                    if first:
                        header_bytes, separator, remainder = chunk.partition(b"\n")
                        if not separator:
                            while separator == b"":
                                continuation = handle.read(1024 * 1024)
                                if not continuation:
                                    break
                                combined = header_bytes + continuation
                                header_bytes, separator, remainder = combined.partition(b"\n")
                        chunk = remainder
                        first = False
                    rows += chunk.count(b"\n")
                    tail = chunk[-1:] or tail
            if tail and tail != b"\n":
                rows += 1
            header = next(csv.reader([header_bytes.decode("utf-8").rstrip("\r")]))
        except (OSError, EOFError, UnicodeError, csv.Error, StopIteration, zlib.error) as exc:
            error = f"{type(exc).__name__}: {exc}"
        finally:
            while tracked.read(8 * 1024 * 1024):
                pass
    return {
        "sha256": tracked.digest.hexdigest(),
        "compressed_bytes": tracked.bytes_read,
        "rows": rows,
        "header": header,
        "error": error,
    }


def _iso_date(value: object) -> str | None:
    if not isinstance(value, str) or len(value) < 10:
        return None
    if len(value) > 10 and value[10] not in {"T", " "}:
        return None
    candidate = value[:10]
    try:
        return date.fromisoformat(candidate).isoformat()
    except ValueError:
        return None


def _inside_request(value: str, request: dict[str, Any]) -> bool:
    start = str(request.get("start", ""))
    end = str(request.get("end", ""))
    return start <= value <= end


def _safe_int(value: object) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return -1


def _dataset_from_path(path: Path) -> str:
    return path.parent.name


def _min_optional(first: str | None, second: str | None) -> str | None:
    values = [value for value in (first, second) if value is not None]
    return min(values) if values else None


def _max_optional(first: str | None, second: str | None) -> str | None:
    values = [value for value in (first, second) if value is not None]
    return max(values) if values else None


def _gate_status(
    counts: Counter[str],
    exact_codes: frozenset[str],
    *,
    prefix: str | None = None,
) -> str:
    failed = any(counts[code] for code in exact_codes)
    if prefix:
        failed = failed or any(count and code.startswith(prefix) for code, count in counts.items())
    return "failed" if failed else "passed"
