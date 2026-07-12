"""Independent cross-product QA for REST grouped daily bars and Day Flat Files."""

from __future__ import annotations

import gzip
import hashlib
import json
import math
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import polars as pl

from ame_stocks_api.artifacts import (
    ArtifactError,
    safe_relative_path,
    sha256_file,
    stable_digest,
    write_json_atomic,
)
from ame_stocks_api.audit.market import MarketAuditTolerance
from ame_stocks_api.downloads import build_download_plan, market_session_dates
from ame_stocks_api.flatfiles import FlatFileDataset, FlatFileObject
from ame_stocks_core import PROVIDER_CONTRACT_VERSION, ProviderDataset, ProviderRequest

DAILY_PRODUCT_AUDIT_SCHEMA_VERSION = 1
DAILY_PRODUCT_CACHE_SCHEMA_VERSION = 1
DAILY_BARS_AVAILABLE_FROM = date(2016, 7, 13)

_NEW_YORK = ZoneInfo("America/New_York")
_PRICE_FIELDS = ("open", "high", "low", "close")
_FLOAT_FIELDS = (*_PRICE_FIELDS, "volume")
_FLAT_COLUMNS = (
    "ticker",
    "volume",
    "open",
    "close",
    "high",
    "low",
    "window_start",
    "transactions",
)
_FLAT_SCHEMA = {
    "ticker": pl.String,
    "volume": pl.Float64,
    "open": pl.Float64,
    "close": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "window_start": pl.Int64,
    "transactions": pl.Int64,
}


class DailyProductAuditError(RuntimeError):
    """Raised when the daily-product audit cannot be configured or started."""


@dataclass(frozen=True, slots=True)
class _ArtifactSource:
    path: Path
    bytes: int
    mtime_ns: int
    expected_sha256: str
    observed_sha256: str
    metadata: dict[str, object]

    def binding(self, root: Path) -> dict[str, object]:
        return {
            "bytes": self.bytes,
            "expected_sha256": self.expected_sha256,
            "metadata": self.metadata,
            "mtime_ns": self.mtime_ns,
            "observed_sha256": self.observed_sha256,
            "path": str(self.path.relative_to(root)),
        }


@dataclass(frozen=True, slots=True)
class _Source:
    dataset: str
    manifest_path: Path
    manifest_bytes: int
    manifest_mtime_ns: int
    manifest_sha256: str
    artifacts: tuple[_ArtifactSource, ...]

    def binding(self, root: Path) -> dict[str, object]:
        return {
            "artifacts": [artifact.binding(root) for artifact in self.artifacts],
            "dataset": self.dataset,
            "manifest_bytes": self.manifest_bytes,
            "manifest_mtime_ns": self.manifest_mtime_ns,
            "manifest_path": str(self.manifest_path.relative_to(root)),
            "manifest_sha256": self.manifest_sha256,
        }


class DailyProductCrossAuditor:
    """Compare two independently delivered Massive daily aggregate products."""

    def __init__(
        self,
        data_root: Path,
        *,
        start: date,
        end: date,
        workers: int = 2,
        cache_dir: Path | None = None,
        use_cache: bool = True,
        tolerance: MarketAuditTolerance | None = None,
        max_examples: int = 20,
    ) -> None:
        if end < start:
            raise ValueError("end cannot precede start")
        if workers < 1:
            raise ValueError("workers must be positive")
        if max_examples < 1:
            raise ValueError("max_examples must be positive")
        self.data_root = data_root.expanduser().resolve()
        self.start = start
        self.end = end
        self.effective_start = max(start, DAILY_BARS_AVAILABLE_FROM)
        if self.end < self.effective_start:
            raise ValueError(
                f"daily_bars is unavailable before {DAILY_BARS_AVAILABLE_FROM.isoformat()}"
            )
        self.workers = workers
        self.use_cache = use_cache
        self.tolerance = tolerance or MarketAuditTolerance()
        self.max_examples = max_examples
        default_cache = (
            self.data_root
            / "manifests"
            / "audits"
            / "daily_product_crosscheck"
            / f"schema=v{DAILY_PRODUCT_AUDIT_SCHEMA_VERSION}"
        )
        self.cache_dir = (cache_dir or default_cache).expanduser().resolve()

    def run(self) -> dict[str, object]:
        if not self.data_root.is_dir():
            raise DailyProductAuditError(f"data root is missing: {self.data_root}")
        sessions = list(market_session_dates(self.effective_start, self.end))
        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            results = list(executor.map(self.audit_session, sessions))
        results.sort(key=lambda item: str(item["session_date"]))

        issue_counts: Counter[str] = Counter()
        mismatch_counts: Counter[str] = Counter()
        comparison_counts: Counter[str] = Counter()
        for result in results:
            for issue in result["issues"]:
                issue_counts[str(issue["code"])] += int(issue["count"])
            mismatches = result.get("comparison", {}).get("field_mismatches", {})
            if isinstance(mismatches, dict):
                for field, details in mismatches.items():
                    if isinstance(details, dict):
                        mismatch_counts[str(field)] += int(details.get("count", 0))
                        comparison_counts[str(field)] += int(details.get("compared", 0))

        source_failed = any(
            result["gates"]["source_integrity"] == "failed" for result in results
        )
        coverage_gate = _combined_gate(
            [str(result["gates"]["ticker_coverage"]) for result in results]
        )
        numerical_gate = _combined_gate(
            [str(result["gates"]["numerical_reconciliation"]) for result in results]
        )
        coverage_different = coverage_gate == "different"
        numerical_different = numerical_gate == "different"
        gates = {
            "source_integrity": "failed" if source_failed else "passed",
            "ticker_coverage": coverage_gate,
            "numerical_reconciliation": numerical_gate,
        }
        status = (
            "failed"
            if source_failed
            else (
                "passed_with_differences"
                if coverage_different or numerical_different
                else "passed"
            )
        )
        return {
            "audit_schema_version": DAILY_PRODUCT_AUDIT_SCHEMA_VERSION,
            "config": self._config(),
            "gates": gates,
            "sessions": results,
            "status": status,
            "summary": {
                "cache_reused": sum(
                    result["cache_status"] == "reused" for result in results
                ),
                "common_tickers": sum(
                    int(result.get("comparison", {}).get("common_tickers", 0))
                    for result in results
                ),
                "difference_sessions": sum(
                    result["status"] == "passed_with_differences" for result in results
                ),
                "failed_sessions": sum(result["status"] == "failed" for result in results),
                "field_comparison_counts": dict(sorted(comparison_counts.items())),
                "field_mismatch_counts": dict(sorted(mismatch_counts.items())),
                "field_mismatch_rates": {
                    field: mismatch_counts[field] / compared if compared else None
                    for field, compared in sorted(comparison_counts.items())
                },
                "flat_only_tickers": sum(
                    int(
                        result.get("comparison", {})
                        .get("flat_only", {})
                        .get("count", 0)
                    )
                    for result in results
                ),
                "issue_code_counts": dict(sorted(issue_counts.items())),
                "passed_sessions": sum(result["status"] == "passed" for result in results),
                "rest_only_tickers": sum(
                    int(
                        result.get("comparison", {})
                        .get("rest_only", {})
                        .get("count", 0)
                    )
                    for result in results
                ),
                "sessions": len(results),
            },
        }

    def audit_session(self, session: date) -> dict[str, object]:
        sources: dict[str, _Source] = {}
        source_issues: list[dict[str, object]] = []
        for label, loader in (
            ("flat_day", self._load_flat_source),
            ("rest_daily", self._load_rest_source),
        ):
            try:
                sources[label] = loader(session)
            except (ArtifactError, DailyProductAuditError, OSError, ValueError) as exc:
                source_issues.append(
                    _issue(
                        "source_unavailable",
                        label,
                        1,
                        f"cannot load source: {exc}",
                        kind="source_integrity",
                    )
                )
        if source_issues:
            return self._source_failure(session, sources, source_issues)

        binding = {
            "config_digest": stable_digest(self._cache_config()),
            **{
                label: source.binding(self.data_root)
                for label, source in sorted(sources.items())
            },
        }
        if self.use_cache:
            cached = self._load_cache(session, binding)
            if cached is not None and _sources_unchanged(sources):
                cached["cache_status"] = "reused"
                return cached

        result = self._compute_session(session, sources)
        result["cache_status"] = "computed"
        if self.use_cache:
            cached_result = dict(result)
            cached_result.pop("cache_status", None)
            write_json_atomic(
                self._cache_path(session),
                {
                    "binding": binding,
                    "cache_schema_version": DAILY_PRODUCT_CACHE_SCHEMA_VERSION,
                    "result": cached_result,
                    "result_sha256": stable_digest(cached_result),
                    "session_date": session.isoformat(),
                },
            )
        return result

    def _source_failure(
        self,
        session: date,
        sources: dict[str, _Source],
        issues: list[dict[str, object]],
    ) -> dict[str, object]:
        return {
            "audit_schema_version": DAILY_PRODUCT_AUDIT_SCHEMA_VERSION,
            "cache_status": "not_written",
            "comparison": {"status": "not_run"},
            "datasets": {},
            "gates": {
                "source_integrity": "failed",
                "ticker_coverage": "not_run",
                "numerical_reconciliation": "not_run",
            },
            "issues": _sort_issues(issues),
            "session_date": session.isoformat(),
            "sources": {
                label: source.binding(self.data_root)
                for label, source in sorted(sources.items())
            },
            "status": "failed",
        }

    def _config(self) -> dict[str, object]:
        return {
            "requested_start": self.start.isoformat(),
            "effective_start": self.effective_start.isoformat(),
            "end": self.end.isoformat(),
            "effective_include_otc": False,
            "include_otc_basis": (
                "MassiveProvider default; legacy canonical requests omit the parameter and "
                "the source binding records provider_version"
            ),
            "products": ["flat_day", "rest_daily"],
            **self._cache_config(),
        }

    def _cache_config(self) -> dict[str, object]:
        return {
            "engine_versions": {
                "polars": pl.__version__,
                "tzdata": _package_version("tzdata"),
            },
            "max_examples": self.max_examples,
            "tolerance": self.tolerance.to_dict(),
        }

    def _cache_path(self, session: date) -> Path:
        return self.cache_dir / f"{session.isoformat()}.json"

    def _load_cache(
        self, session: date, binding: dict[str, object]
    ) -> dict[str, object] | None:
        path = self._cache_path(session)
        try:
            document = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return None
        except (OSError, UnicodeError, json.JSONDecodeError):
            return None
        if (
            not isinstance(document, dict)
            or document.get("cache_schema_version") != DAILY_PRODUCT_CACHE_SCHEMA_VERSION
            or document.get("session_date") != session.isoformat()
            or document.get("binding") != binding
            or not isinstance(document.get("result"), dict)
        ):
            return None
        result = document["result"]
        expected_sources = {
            label: binding[label] for label in ("flat_day", "rest_daily")
        }
        if (
            result.get("audit_schema_version") != DAILY_PRODUCT_AUDIT_SCHEMA_VERSION
            or result.get("session_date") != session.isoformat()
            or result.get("sources") != expected_sources
            or document.get("result_sha256") != stable_digest(result)
        ):
            return None
        return dict(result)

    def _load_flat_source(self, session: date) -> _Source:
        dataset = FlatFileDataset.DAY_AGGREGATES
        item = FlatFileObject(dataset=dataset, session_date=session)
        manifest_path = (
            self.data_root
            / "manifests"
            / "massive"
            / "flatfiles"
            / dataset.value
            / f"{session.isoformat()}.json"
        )
        manifest_bytes, manifest, manifest_mtime_ns = _read_manifest(manifest_path)
        if (
            manifest.get("status") != "complete"
            or manifest.get("flat_file_manifest_schema_version") != 1
            or manifest.get("dataset") != dataset.value
            or manifest.get("session_date") != session.isoformat()
            or manifest.get("object_id") != item.object_id
            or manifest.get("object_key") != item.object_key
        ):
            raise DailyProductAuditError(f"Flat File manifest identity differs: {manifest_path}")
        output = manifest.get("output")
        if not isinstance(output, dict):
            raise DailyProductAuditError(f"Flat File manifest output is absent: {manifest_path}")
        if output.get("csv_header") != list(_FLAT_COLUMNS):
            raise DailyProductAuditError(f"Flat File declared CSV header differs: {manifest_path}")
        expected_path = f"bronze/massive/flatfiles/{item.object_key}"
        if output.get("path") != expected_path:
            raise DailyProductAuditError(f"Flat File artifact path differs: {manifest_path}")
        remote = manifest.get("remote")
        declared_bytes = _strict_nonnegative_int(output.get("bytes"), "output.bytes")
        if (
            not isinstance(remote, dict)
            or _strict_nonnegative_int(remote.get("content_length"), "remote.content_length")
            != declared_bytes
        ):
            raise DailyProductAuditError(f"Flat File byte declarations differ: {manifest_path}")
        artifact = self._bind_artifact(
            output.get("path"),
            declared_bytes,
            output.get("sha256"),
            {"csv_header": output.get("csv_header")},
        )
        return _Source(
            dataset=dataset.value,
            manifest_path=manifest_path,
            manifest_bytes=len(manifest_bytes),
            manifest_mtime_ns=manifest_mtime_ns,
            manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
            artifacts=(artifact,),
        )

    def _load_rest_source(self, session: date) -> _Source:
        request = _daily_request(session)
        manifest_path = (
            self.data_root
            / "manifests"
            / "massive"
            / ProviderDataset.DAILY_BARS.value
            / f"{request.request_id}.json"
        )
        manifest_bytes, manifest, manifest_mtime_ns = _read_manifest(manifest_path)
        if (
            manifest.get("status") != "complete"
            or manifest.get("manifest_schema_version") != 1
            or manifest.get("provider") != "massive"
            or manifest.get("provider_contract_version") != PROVIDER_CONTRACT_VERSION
            or manifest.get("dataset") != ProviderDataset.DAILY_BARS.value
            or manifest.get("request_id") != request.request_id
            or manifest.get("request") != request.canonical_dict()
            or manifest_path.stem != request.request_id
            or stable_digest(manifest.get("request")) != request.request_id
            or manifest.get("checkpoint") is not None
        ):
            raise DailyProductAuditError(f"REST manifest identity differs: {manifest_path}")
        artifacts = manifest.get("artifacts")
        if not isinstance(artifacts, list) or len(artifacts) != 1:
            raise DailyProductAuditError(
                f"grouped daily REST manifest must contain exactly one page: {manifest_path}"
            )
        metadata = artifacts[0]
        if (
            not isinstance(metadata, dict)
            or metadata.get("sequence") != 0
            or metadata.get("is_last") is not True
            or metadata.get("next_continuation") not in {None, ""}
        ):
            raise DailyProductAuditError(f"REST page metadata is inconsistent: {manifest_path}")
        expected_path = (
            f"bronze/massive/daily_bars/request_id={request.request_id}/page-00000.json.gz"
        )
        if metadata.get("path") != expected_path:
            raise DailyProductAuditError(f"REST artifact path differs: {manifest_path}")
        artifact = self._bind_artifact(
            metadata.get("path"),
            _strict_nonnegative_int(metadata.get("compressed_bytes"), "compressed_bytes"),
            metadata.get("stored_sha256"),
            {
                "provider_version": manifest.get("provider_version"),
                "raw_bytes": metadata.get("raw_bytes"),
                "raw_sha256": metadata.get("raw_sha256"),
                "record_count": metadata.get("record_count"),
                "sequence": metadata.get("sequence"),
            },
        )
        return _Source(
            dataset=ProviderDataset.DAILY_BARS.value,
            manifest_path=manifest_path,
            manifest_bytes=len(manifest_bytes),
            manifest_mtime_ns=manifest_mtime_ns,
            manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
            artifacts=(artifact,),
        )

    def _bind_artifact(
        self,
        relative_path: object,
        declared_bytes: int,
        declared_sha256: object,
        metadata: dict[str, object],
    ) -> _ArtifactSource:
        if not isinstance(declared_sha256, str) or len(declared_sha256) != 64:
            raise DailyProductAuditError("artifact SHA-256 declaration is invalid")
        path = safe_relative_path(self.data_root, relative_path)
        stat = path.stat()
        if not path.is_file() or stat.st_size != declared_bytes:
            raise DailyProductAuditError(f"artifact size or type differs: {path}")
        observed_sha256 = sha256_file(path)
        post_hash = path.stat()
        if post_hash.st_size != stat.st_size or post_hash.st_mtime_ns != stat.st_mtime_ns:
            raise DailyProductAuditError(f"artifact changed while hashing: {path}")
        if observed_sha256 != declared_sha256:
            raise DailyProductAuditError(f"artifact SHA-256 differs: {path}")
        return _ArtifactSource(
            path=path,
            bytes=stat.st_size,
            mtime_ns=stat.st_mtime_ns,
            expected_sha256=declared_sha256,
            observed_sha256=observed_sha256,
            metadata=metadata,
        )

    def _compute_session(
        self, session: date, sources: dict[str, _Source]
    ) -> dict[str, object]:
        issues: list[dict[str, object]] = []
        frames: dict[str, pl.DataFrame] = {}
        stats: dict[str, dict[str, object]] = {}
        try:
            flat, flat_stats, flat_issues = self._parse_flat(session, sources["flat_day"])
            frames["flat_day"] = flat
            stats["flat_day"] = flat_stats
            issues.extend(flat_issues)
        except Exception as exc:
            issues.append(
                _issue(
                    "source_parse_failed",
                    "flat_day",
                    1,
                    (
                        "Flat day file cannot be completely parsed: "
                        f"{type(exc).__name__}: {exc}"
                    ),
                    kind="source_integrity",
                )
            )
        try:
            rest, rest_stats, rest_issues = self._parse_rest(session, sources["rest_daily"])
            frames["rest_daily"] = rest
            stats["rest_daily"] = rest_stats
            issues.extend(rest_issues)
        except Exception as exc:
            issues.append(
                _issue(
                    "source_parse_failed",
                    "rest_daily",
                    1,
                    (
                        "REST daily page cannot be completely parsed: "
                        f"{type(exc).__name__}: {exc}"
                    ),
                    kind="source_integrity",
                )
            )

        comparison: dict[str, object] = {"status": "not_run"}
        if set(frames) == {"flat_day", "rest_daily"}:
            comparison, comparison_issues = self._compare(
                frames["flat_day"], frames["rest_daily"]
            )
            issues.extend(comparison_issues)
        if not _sources_unchanged(sources):
            issues.append(
                _issue(
                    "source_changed_during_audit",
                    "source",
                    1,
                    "source size or mtime changed after its SHA-256 was bound",
                    kind="source_integrity",
                )
            )
        issues = _sort_issues(issues)
        source_failed = any(issue["kind"] == "source_integrity" for issue in issues)
        coverage_different = bool(
            comparison.get("flat_only", {}).get("count", 0)
            or comparison.get("rest_only", {}).get("count", 0)
        )
        numerical_different = any(
            int(details.get("count", 0)) > 0
            for details in comparison.get("field_mismatches", {}).values()
            if isinstance(details, dict)
        )
        return {
            "audit_schema_version": DAILY_PRODUCT_AUDIT_SCHEMA_VERSION,
            "comparison": comparison,
            "datasets": stats,
            "gates": {
                "source_integrity": "failed" if source_failed else "passed",
                "ticker_coverage": (
                    "different" if coverage_different else "matched"
                )
                if comparison.get("status") != "not_run"
                else "not_run",
                "numerical_reconciliation": (
                    "different" if numerical_different else "matched"
                )
                if comparison.get("status") != "not_run"
                else "not_run",
            },
            "issues": issues,
            "session_date": session.isoformat(),
            "sources": {
                label: source.binding(self.data_root)
                for label, source in sorted(sources.items())
            },
            "status": (
                "failed"
                if source_failed
                else (
                    "passed_with_differences"
                    if coverage_different or numerical_different
                    else "passed"
                )
            ),
        }

    def _parse_flat(
        self, session: date, source: _Source
    ) -> tuple[pl.DataFrame, dict[str, object], list[dict[str, object]]]:
        frame = pl.read_csv(
            source.artifacts[0].path,
            schema_overrides=_FLAT_SCHEMA,
            null_values=["", "null"],
        )
        issues = _validate_flat_frame(frame, session)
        if frame.height == 0:
            issues.append(
                _issue(
                    "empty_source",
                    "flat_day",
                    1,
                    "Flat day source has no rows for an expected market session",
                    kind="source_integrity",
                )
            )
        canonical = _canonicalize_flat(frame)
        return (
            canonical,
            {"columns": list(frame.columns), "rows": frame.height, "tickers": canonical.height},
            issues,
        )

    def _parse_rest(
        self, session: date, source: _Source
    ) -> tuple[pl.DataFrame, dict[str, object], list[dict[str, object]]]:
        artifact = source.artifacts[0]
        compressed = artifact.path.read_bytes()
        raw = gzip.decompress(compressed)
        metadata = artifact.metadata
        if len(raw) != _strict_nonnegative_int(metadata.get("raw_bytes"), "raw_bytes"):
            raise DailyProductAuditError("REST raw byte count differs")
        if hashlib.sha256(raw).hexdigest() != metadata.get("raw_sha256"):
            raise DailyProductAuditError("REST raw SHA-256 differs")
        document = json.loads(raw)
        if (
            not isinstance(document, dict)
            or str(document.get("status", "")).upper() != "OK"
            or not isinstance(document.get("request_id"), str)
            or not str(document["request_id"]).strip()
            or document.get("adjusted") is not False
            or not isinstance(document.get("results"), list)
        ):
            raise DailyProductAuditError("REST response envelope is invalid")
        rows = document["results"]
        if len(rows) != _strict_nonnegative_int(metadata.get("record_count"), "record_count"):
            raise DailyProductAuditError("REST record count differs")
        results_count = document.get("resultsCount")
        if results_count is not None and (
            isinstance(results_count, bool)
            or not isinstance(results_count, int)
            or results_count != len(rows)
        ):
            raise DailyProductAuditError("REST resultsCount differs")
        query_count = document.get("queryCount")
        if (
            isinstance(query_count, bool)
            or not isinstance(query_count, int)
            or query_count != len(rows)
        ):
            raise DailyProductAuditError("REST queryCount differs")

        normalized: list[dict[str, object]] = []
        issues: list[dict[str, object]] = []
        vw_present = 0
        transactions_present = 0
        expected_start_ms = _session_start_ns(session) // 1_000_000
        if not rows:
            issues.append(
                _issue(
                    "empty_source",
                    "rest_daily",
                    1,
                    "REST grouped daily source has no rows for an expected market session",
                    kind="source_integrity",
                )
            )
        for row in rows:
            if not isinstance(row, dict):
                issues.append(
                    _issue(
                        "row_not_object",
                        "rest_daily",
                        1,
                        "REST result row is not an object",
                        kind="source_integrity",
                    )
                )
                continue
            row_issues = _validate_rest_row(row, session, expected_start_ms)
            issues.extend(row_issues)
            vw = row.get("vw")
            transaction = row.get("n")
            vw_present += vw is not None
            transactions_present += transaction is not None
            normalized.append(
                {
                    "ticker": row.get("T"),
                    "open": _number_or_none(row.get("o")),
                    "high": _number_or_none(row.get("h")),
                    "low": _number_or_none(row.get("l")),
                    "close": _number_or_none(row.get("c")),
                    "volume": _number_or_none(row.get("v")),
                    "transactions": (
                        transaction
                        if isinstance(transaction, int) and not isinstance(transaction, bool)
                        else None
                    ),
                    "vwap": _number_or_none(vw),
                }
            )
        frame = pl.DataFrame(
            normalized,
            schema={
                "ticker": pl.String,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "volume": pl.Float64,
                "transactions": pl.Int64,
                "vwap": pl.Float64,
            },
            strict=False,
        )
        duplicate_count = _duplicate_ticker_count(frame)
        if duplicate_count:
            issues.append(
                _issue(
                    "duplicate_ticker",
                    "rest_daily",
                    duplicate_count,
                    "REST grouped daily response repeats a ticker",
                    kind="source_integrity",
                )
            )
        canonical = _canonicalize_rest(frame)
        return (
            canonical,
            {
                "rows": len(rows),
                "tickers": canonical.height,
                "transactions_present": transactions_present,
                "transactions_missing": len(rows) - transactions_present,
                "vwap_present": vw_present,
                "vwap_missing": len(rows) - vw_present,
            },
            issues,
        )

    def _compare(
        self, flat: pl.DataFrame, rest: pl.DataFrame
    ) -> tuple[dict[str, object], list[dict[str, object]]]:
        flat_daily = flat.select(
            "ticker",
            *(pl.col(field).alias(f"flat_{field}") for field in _FLOAT_FIELDS),
            pl.col("transactions").alias("flat_transactions"),
        ).with_columns(pl.lit(True).alias("_in_flat"))
        rest_daily = rest.select(
            "ticker",
            *(pl.col(field).alias(f"rest_{field}") for field in _FLOAT_FIELDS),
            pl.col("transactions").alias("rest_transactions"),
        ).with_columns(pl.lit(True).alias("_in_rest"))
        joined = flat_daily.join(
            rest_daily, on="ticker", how="full", coalesce=True
        ).sort("ticker")
        flat_only = joined.filter(pl.col("_in_rest").is_null())["ticker"].to_list()
        rest_only = joined.filter(pl.col("_in_flat").is_null())["ticker"].to_list()
        common = joined.filter(
            pl.col("_in_flat").is_not_null() & pl.col("_in_rest").is_not_null()
        )
        issues: list[dict[str, object]] = []
        if flat_only:
            issues.append(
                _issue(
                    "flat_ticker_missing_from_rest",
                    "cross_product",
                    len(flat_only),
                    "Flat-only tickers are a product-universe difference, including possible OTC",
                    flat_only[: self.max_examples],
                    kind="product_difference",
                )
            )
        if rest_only:
            issues.append(
                _issue(
                    "rest_ticker_missing_from_flat",
                    "cross_product",
                    len(rest_only),
                    "REST grouped-daily tickers are absent from the Day Flat File",
                    rest_only[: self.max_examples],
                    kind="product_difference",
                )
            )

        mismatches: dict[str, dict[str, object]] = {}
        for field in _FLOAT_FIELDS:
            absolute = (
                self.tolerance.price_absolute
                if field in _PRICE_FIELDS
                else self.tolerance.volume_absolute
            )
            relative = (
                self.tolerance.price_relative
                if field in _PRICE_FIELDS
                else self.tolerance.volume_relative
            )
            details = _float_mismatch_details(
                common,
                field,
                absolute,
                relative,
                max_examples=self.max_examples,
            )
            mismatches[field] = details
            if details["count"]:
                issues.append(
                    _issue(
                        f"{field}_mismatch",
                        "cross_product",
                        int(details["count"]),
                        f"REST {field} differs from the Day Flat File",
                        [str(row["ticker"]) for row in details["examples"]],
                        kind="product_difference",
                    )
                )
        transaction_rows = common.filter(
            pl.col("flat_transactions").is_not_null()
            & pl.col("rest_transactions").is_not_null()
        )
        transaction_mismatches = transaction_rows.filter(
            pl.col("flat_transactions").ne(pl.col("rest_transactions"))
        ).select("ticker", "flat_transactions", "rest_transactions")
        transaction_examples = transaction_mismatches.head(self.max_examples).to_dicts()
        mismatches["transactions"] = {
            "compared": transaction_rows.height,
            "count": transaction_mismatches.height,
            "examples": transaction_examples,
            "rate": (
                transaction_mismatches.height / transaction_rows.height
                if transaction_rows.height
                else None
            ),
            "rest_missing_on_common": int(
                common.filter(pl.col("rest_transactions").is_null()).height
            ),
        }
        if transaction_mismatches.height:
            issues.append(
                _issue(
                    "transactions_mismatch",
                    "cross_product",
                    transaction_mismatches.height,
                    "REST transaction count differs from the Day Flat File",
                    [str(row["ticker"]) for row in transaction_examples],
                    kind="product_difference",
                )
            )
        return (
            {
                "common_tickers": common.height,
                "coverage": {
                    "flat_tickers": flat.height,
                    "flat_covered_by_rest_fraction": (
                        common.height / flat.height if flat.height else None
                    ),
                    "rest_tickers": rest.height,
                    "rest_covered_by_flat_fraction": (
                        common.height / rest.height if rest.height else None
                    ),
                },
                "field_mismatches": mismatches,
                "flat_only": {
                    "count": len(flat_only),
                    "examples": flat_only[: self.max_examples],
                },
                "rest_only": {
                    "count": len(rest_only),
                    "examples": rest_only[: self.max_examples],
                },
                "status": "different" if issues else "matched",
                "tolerance": self.tolerance.to_dict(),
            },
            issues,
        )


def _daily_request(session: date) -> ProviderRequest:
    return build_download_plan(
        dataset=ProviderDataset.DAILY_BARS,
        start=session,
        end=session,
    ).requests[0]


def _read_manifest(path: Path) -> tuple[bytes, dict[str, Any], int]:
    try:
        before = path.stat()
        content = path.read_bytes()
        after = path.stat()
        document = json.loads(content)
    except FileNotFoundError as exc:
        raise DailyProductAuditError(f"manifest is missing: {path}") from exc
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise DailyProductAuditError(f"manifest is unreadable: {path}") from exc
    if not isinstance(document, dict):
        raise DailyProductAuditError(f"manifest root is not an object: {path}")
    if before.st_size != after.st_size or before.st_mtime_ns != after.st_mtime_ns:
        raise DailyProductAuditError(f"manifest changed while reading: {path}")
    if len(content) != after.st_size:
        raise DailyProductAuditError(f"manifest size differs while reading: {path}")
    return content, document, after.st_mtime_ns


def _validate_flat_frame(frame: pl.DataFrame, session: date) -> list[dict[str, object]]:
    issues: list[dict[str, object]] = []
    if tuple(frame.columns) != _FLAT_COLUMNS:
        issues.append(
            _issue(
                "schema_mismatch",
                "flat_day",
                1,
                f"expected exact columns {list(_FLAT_COLUMNS)}; observed {frame.columns}",
                kind="source_integrity",
            )
        )
    if not set(_FLAT_COLUMNS).issubset(frame.columns):
        return issues
    required = frame.select(_FLAT_COLUMNS)
    null_count = sum(required.null_count().row(0))
    if null_count:
        issues.append(
            _issue(
                "null_required_field",
                "flat_day",
                int(null_count),
                "Flat day contains null required values",
                kind="source_integrity",
            )
        )
    invalid = _invalid_market_rows(required)
    if invalid:
        issues.append(
            _issue(
                "invalid_market_value",
                "flat_day",
                invalid,
                "Flat day contains invalid ticker/OHLCV/transaction values",
                kind="source_integrity",
            )
        )
    expected_start = _session_start_ns(session)
    bad_timestamp = _count(
        required,
        pl.col("window_start").is_not_null()
        & pl.col("window_start").ne(expected_start),
    )
    if bad_timestamp:
        issues.append(
            _issue(
                "noncanonical_session_timestamp",
                "flat_day",
                bad_timestamp,
                "Flat day timestamp must equal midnight America/New_York",
                kind="source_integrity",
            )
        )
    duplicates = _duplicate_ticker_count(required)
    if duplicates:
        issues.append(
            _issue(
                "duplicate_ticker",
                "flat_day",
                duplicates,
                "Flat day contains more than one row for a ticker",
                kind="source_integrity",
            )
        )
    return issues


def _validate_rest_row(
    row: dict[str, Any], session: date, expected_start_ms: int
) -> list[dict[str, object]]:
    issues: list[dict[str, object]] = []
    ticker = row.get("T")
    if not isinstance(ticker, str) or not ticker.strip() or ticker != ticker.strip():
        issues.append(
            _issue(
                "invalid_ticker",
                "rest_daily",
                1,
                "REST ticker is blank, non-string, or has outer whitespace",
                kind="source_integrity",
            )
        )
    prices = [row.get(key) for key in ("o", "h", "l", "c")]
    volume = row.get("v")
    if any(not _valid_number(value, positive=True) for value in prices) or not _valid_number(
        volume, positive=False
    ):
        issues.append(
            _issue(
                "invalid_ohlcv",
                "rest_daily",
                1,
                "REST OHLC must be positive and volume finite nonnegative",
                kind="source_integrity",
            )
        )
    elif not (
        float(row["l"]) <= float(row["o"]) <= float(row["h"])
        and float(row["l"]) <= float(row["c"]) <= float(row["h"])
    ):
        issues.append(
            _issue(
                "invalid_ohlc_range",
                "rest_daily",
                1,
                "REST high/low do not contain open and close",
                kind="source_integrity",
            )
        )
    timestamp = row.get("t")
    if isinstance(timestamp, bool) or not isinstance(timestamp, int):
        issues.append(
            _issue(
                "invalid_timestamp",
                "rest_daily",
                1,
                "REST timestamp must be integer Unix milliseconds",
                kind="source_integrity",
            )
        )
    else:
        try:
            et_date = datetime.fromtimestamp(timestamp / 1000, tz=UTC).astimezone(
                _NEW_YORK
            ).date()
        except (OSError, OverflowError, ValueError):
            et_date = None
        if et_date != session or timestamp != expected_start_ms:
            issues.append(
                _issue(
                    "noncanonical_session_timestamp",
                    "rest_daily",
                    1,
                    "REST timestamp must identify ET midnight for the requested session",
                    kind="source_integrity",
                )
            )
    transaction = row.get("n")
    if transaction is not None and (
        isinstance(transaction, bool)
        or not isinstance(transaction, int)
        or transaction < 0
    ):
        issues.append(
            _issue(
                "invalid_transactions",
                "rest_daily",
                1,
                "optional REST transaction count must be a nonnegative integer",
                kind="source_integrity",
            )
        )
    vwap = row.get("vw")
    if vwap is not None and not _valid_number(vwap, positive=False):
        issues.append(
            _issue(
                "invalid_vwap",
                "rest_daily",
                1,
                "optional REST VWAP must be finite and nonnegative",
                kind="source_integrity",
            )
        )
    otc = row.get("otc")
    if otc is not None and not isinstance(otc, bool):
        issues.append(
            _issue(
                "invalid_otc_flag",
                "rest_daily",
                1,
                "optional REST otc flag must be boolean",
                kind="source_integrity",
            )
        )
    if otc is True:
        issues.append(
            _issue(
                "unexpected_otc_row",
                "rest_daily",
                1,
                "REST request used include_otc=false but returned an OTC row",
                kind="source_integrity",
            )
        )
    return issues


def _invalid_market_rows(frame: pl.DataFrame) -> int:
    return _count(
        frame,
        (
            pl.col("ticker").is_not_null()
            & (
                pl.col("ticker").str.strip_chars().eq("")
                | pl.col("ticker").ne(pl.col("ticker").str.strip_chars())
            )
        )
        | pl.any_horizontal(
            *(
                pl.col(field).is_not_null()
                & (~pl.col(field).is_finite() | pl.col(field).le(0))
                for field in _PRICE_FIELDS
            )
        )
        | (
            pl.col("volume").is_not_null()
            & (~pl.col("volume").is_finite() | pl.col("volume").lt(0))
        )
        | (
            pl.col("transactions").is_not_null() & pl.col("transactions").lt(0)
        )
        | pl.col("high").lt(pl.col("low"))
        | pl.col("open").lt(pl.col("low"))
        | pl.col("open").gt(pl.col("high"))
        | pl.col("close").lt(pl.col("low"))
        | pl.col("close").gt(pl.col("high")),
    )


def _canonicalize_flat(frame: pl.DataFrame) -> pl.DataFrame:
    if not set(_FLAT_COLUMNS).issubset(frame.columns):
        return pl.DataFrame(
            schema={
                "ticker": pl.String,
                **{field: pl.Float64 for field in _FLOAT_FIELDS},
                "transactions": pl.Int64,
            }
        )
    return (
        frame.select(
            "ticker",
            *_FLOAT_FIELDS,
            "transactions",
            "window_start",
        )
        .sort(["ticker", "window_start"], nulls_last=True)
        .unique(subset=["ticker"], keep="first", maintain_order=True)
        .drop("window_start")
    )


def _canonicalize_rest(frame: pl.DataFrame) -> pl.DataFrame:
    if not frame.height:
        return frame
    return frame.sort("ticker", nulls_last=True).unique(
        subset=["ticker"], keep="first", maintain_order=True
    )


def _duplicate_ticker_count(frame: pl.DataFrame) -> int:
    if "ticker" not in frame.columns or not frame.height:
        return 0
    return frame.group_by("ticker").len().filter(pl.col("len") > 1).height


def _float_mismatch_details(
    frame: pl.DataFrame,
    field: str,
    absolute: float,
    relative: float,
    *,
    max_examples: int,
) -> dict[str, object]:
    flat = pl.col(f"flat_{field}")
    rest = pl.col(f"rest_{field}")
    comparable = (
        flat.is_not_null()
        & rest.is_not_null()
        & flat.is_finite()
        & rest.is_finite()
    )
    compared = _count(frame, comparable)
    difference = (rest - flat).abs()
    allowed = pl.max_horizontal(
        pl.lit(absolute), pl.max_horizontal(rest.abs(), flat.abs()) * relative
    )
    mismatches = (
        frame.with_columns(
            difference.alias("absolute_difference"), allowed.alias("allowed_tolerance")
        )
        .filter(comparable & pl.col("absolute_difference").gt(pl.col("allowed_tolerance")))
        .select(
            "ticker",
            flat.alias("flat"),
            rest.alias("rest"),
            "absolute_difference",
            "allowed_tolerance",
        )
    )
    return {
        "compared": compared,
        "count": mismatches.height,
        "examples": mismatches.head(max_examples).to_dicts(),
        "rate": mismatches.height / compared if compared else None,
    }


def _sources_unchanged(sources: dict[str, _Source]) -> bool:
    try:
        for source in sources.values():
            manifest_stat = source.manifest_path.stat()
            if (
                manifest_stat.st_size != source.manifest_bytes
                or manifest_stat.st_mtime_ns != source.manifest_mtime_ns
                or sha256_file(source.manifest_path) != source.manifest_sha256
            ):
                return False
            for artifact in source.artifacts:
                observed = artifact.path.stat()
                if (
                    observed.st_size != artifact.bytes
                    or observed.st_mtime_ns != artifact.mtime_ns
                    or sha256_file(artifact.path) != artifact.observed_sha256
                ):
                    return False
        return True
    except OSError:
        return False


def _session_start_ns(session: date) -> int:
    observed = datetime.combine(session, time.min, tzinfo=_NEW_YORK)
    return int(observed.timestamp() * 1_000_000_000)


def _valid_number(value: object, *, positive: bool) -> bool:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    if isinstance(value, int):
        return value > 0 if positive else value >= 0
    return math.isfinite(value) and (value > 0 if positive else value >= 0)


def _number_or_none(value: object) -> float | None:
    return float(value) if _valid_number(value, positive=False) else None


def _strict_nonnegative_int(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise DailyProductAuditError(f"{label} must be a nonnegative integer")
    return value


def _count(frame: pl.DataFrame, expression: pl.Expr) -> int:
    return int(frame.select(expression.fill_null(False).sum()).item() or 0)


def _combined_gate(values: list[str]) -> str:
    if "different" in values:
        return "different"
    if "matched" in values:
        return "matched"
    return "not_run"


def _package_version(package: str) -> str:
    try:
        return version(package)
    except PackageNotFoundError:
        return "not-installed"


def _issue(
    code: str,
    dataset: str,
    count: int,
    message: str,
    examples: list[str] | None = None,
    *,
    kind: str,
) -> dict[str, object]:
    issue: dict[str, object] = {
        "code": code,
        "count": int(count),
        "dataset": dataset,
        "kind": kind,
        "message": message,
    }
    if examples:
        issue["examples"] = examples
    return issue


def _sort_issues(issues: list[dict[str, object]]) -> list[dict[str, object]]:
    return sorted(
        issues,
        key=lambda item: (
            str(item["kind"]),
            str(item["dataset"]),
            str(item["code"]),
        ),
    )


__all__ = [
    "DAILY_BARS_AVAILABLE_FROM",
    "DAILY_PRODUCT_AUDIT_SCHEMA_VERSION",
    "DailyProductAuditError",
    "DailyProductCrossAuditor",
]
