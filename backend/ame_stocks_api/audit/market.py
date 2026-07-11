"""Resumable cross-table QA for Massive minute and day aggregate Flat Files.

The cache deliberately binds the byte-level SHA-256 of both source manifests.
Artifact metadata is included as an additional guard, while the uncached path
also verifies the compressed artifact checksum before Polars parses every row.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from zoneinfo import ZoneInfo

import exchange_calendars as xcals
import polars as pl

from ame_stocks_api.artifacts import (
    ArtifactError,
    safe_relative_path,
    sha256_file,
    stable_digest,
    write_json_atomic,
)
from ame_stocks_api.downloads import market_session_dates
from ame_stocks_api.flatfiles import FlatFileDataset, FlatFileObject

MARKET_AUDIT_SCHEMA_VERSION = 5
CACHE_SCHEMA_VERSION = 5
EXPECTED_COLUMNS = (
    "ticker",
    "volume",
    "open",
    "close",
    "high",
    "low",
    "window_start",
    "transactions",
)
PRICE_FIELDS = ("open", "high", "low", "close")
VALUE_FIELDS = (*PRICE_FIELDS, "volume", "transactions")
KEY_FIELDS = ("ticker", "window_start")
NS_PER_MINUTE = 60_000_000_000
_NEW_YORK = ZoneInfo("America/New_York")
_XNYS = xcals.get_calendar("XNYS")
_CSV_SCHEMA = {
    "ticker": pl.String,
    "volume": pl.Float64,
    "open": pl.Float64,
    "close": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "window_start": pl.Int64,
    "transactions": pl.Int64,
}


class MarketAuditError(RuntimeError):
    """Raised when the market cross-table audit cannot be configured or executed."""


@dataclass(frozen=True, slots=True)
class MarketAuditTolerance:
    """Numerical equality policy for the minute-to-day reconciliation."""

    price_absolute: float = 1e-8
    price_relative: float = 1e-9
    volume_absolute: float = 1e-6
    volume_relative: float = 1e-9

    def __post_init__(self) -> None:
        for name, value in asdict(self).items():
            if not math.isfinite(value) or value < 0:
                raise ValueError(f"{name} must be finite and nonnegative")

    def to_dict(self) -> dict[str, object]:
        return {
            **asdict(self),
            "float_formula": (
                "abs(minute-day) <= max(absolute, relative * max(abs(minute), abs(day)))"
            ),
            "transactions": "exact integer equality",
        }


@dataclass(frozen=True, slots=True)
class MarketCoveragePolicy:
    """Escalate a material cross-product ticker loss without failing on sparse edge cases."""

    max_missing_fraction: float = 0.10
    minimum_missing_tickers: int = 2

    def __post_init__(self) -> None:
        if not math.isfinite(self.max_missing_fraction) or not (
            0 <= self.max_missing_fraction <= 1
        ):
            raise ValueError("max_missing_fraction must be between zero and one")
        if self.minimum_missing_tickers < 1:
            raise ValueError("minimum_missing_tickers must be positive")

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class _Source:
    dataset: str
    manifest_path: Path
    manifest_sha256: str
    artifact_path: Path
    artifact_sha256: str
    observed_sha256: str
    artifact_bytes: int
    artifact_mtime_ns: int

    def binding(self, root: Path) -> dict[str, object]:
        return {
            "artifact_bytes": self.artifact_bytes,
            "artifact_mtime_ns": self.artifact_mtime_ns,
            "artifact_path": str(self.artifact_path.relative_to(root)),
            "artifact_sha256": self.artifact_sha256,
            "observed_sha256": self.observed_sha256,
            "manifest_path": str(self.manifest_path.relative_to(root)),
            "manifest_sha256": self.manifest_sha256,
        }


class MarketCrossAuditor:
    """Audit and reconcile every expected market session in a date range."""

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
        coverage_policy: MarketCoveragePolicy | None = None,
        max_examples: int = 20,
    ) -> None:
        self.data_root = data_root.expanduser().resolve()
        if end < start:
            raise ValueError("end cannot precede start")
        if workers < 1:
            raise ValueError("workers must be positive")
        if max_examples < 1:
            raise ValueError("max_examples must be positive")
        self.start = start
        self.end = end
        self.workers = workers
        self.use_cache = use_cache
        self.tolerance = tolerance or MarketAuditTolerance()
        self.coverage_policy = coverage_policy or MarketCoveragePolicy()
        self.max_examples = max_examples
        default_cache = (
            self.data_root
            / "manifests"
            / "audits"
            / "market_crosscheck"
            / f"schema=v{MARKET_AUDIT_SCHEMA_VERSION}"
        )
        self.cache_dir = (cache_dir or default_cache).expanduser().resolve()

    def run(self) -> dict[str, object]:
        sessions = list(market_session_dates(self.start, self.end))
        if not sessions:
            raise MarketAuditError("date range contains no expected market sessions")
        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            results = list(executor.map(self.audit_session, sessions))
        results.sort(key=lambda item: str(item["session_date"]))
        issue_counts: Counter[str] = Counter()
        mismatch_counts: Counter[str] = Counter()
        comparison_counts: Counter[str] = Counter()
        for result in results:
            for issue in result["issues"]:
                issue_counts[str(issue["code"])] += int(issue["count"])
            comparison = result.get("comparison", {})
            if isinstance(comparison, dict):
                mismatches = comparison.get("field_mismatches", {})
                if isinstance(mismatches, dict):
                    for field, details in mismatches.items():
                        if isinstance(details, dict):
                            mismatch_counts[str(field)] += int(details.get("count", 0))
                            comparison_counts[str(field)] += int(
                                details.get("compared", 0)
                            )
        failed = sum(result["status"] == "failed" for result in results)
        differences = sum(
            result["status"] == "passed_with_differences" for result in results
        )
        summary = {
            "cache_reused": sum(result["cache_status"] == "reused" for result in results),
            "day_rows": sum(
                int(result["datasets"].get("day", {}).get("rows", 0)) for result in results
            ),
            "difference_sessions": differences,
            "failed_sessions": failed,
            "field_mismatch_counts": dict(sorted(mismatch_counts.items())),
            "field_comparison_counts": dict(sorted(comparison_counts.items())),
            "field_mismatch_rates": {
                field: mismatch_counts[field] / compared if compared else None
                for field, compared in sorted(comparison_counts.items())
            },
            "issue_code_counts": dict(sorted(issue_counts.items())),
            "minute_rows": sum(
                int(result["datasets"].get("minute", {}).get("rows", 0)) for result in results
            ),
            "passed_sessions": len(results) - failed - differences,
            "sessions": len(results),
            "day_ticker_session_pairs": sum(
                int(result["datasets"].get("day", {}).get("tickers", 0))
                for result in results
            ),
            "minute_ticker_session_pairs": sum(
                int(result["datasets"].get("minute", {}).get("tickers", 0))
                for result in results
            ),
            "ticker_session_pairs_in_both_files": sum(
                int(result.get("comparison", {}).get("ticker_pairs_in_both_files", 0))
                for result in results
            ),
        }
        gate_values = {
            gate: [str(result.get("gates", {}).get(gate, "not_run")) for result in results]
            for gate in (
                "source_and_row_integrity",
                "ticker_coverage",
                "cross_product_reconciliation",
            )
        }
        gates = {
            "source_and_row_integrity": (
                "failed"
                if "failed" in gate_values["source_and_row_integrity"]
                else "passed"
            ),
            "ticker_coverage": _combined_gate_status(gate_values["ticker_coverage"]),
            "cross_product_reconciliation": _combined_gate_status(
                gate_values["cross_product_reconciliation"]
            ),
        }
        return {
            "audit_schema_version": MARKET_AUDIT_SCHEMA_VERSION,
            "config": self._config(),
            "gates": gates,
            "sessions": results,
            "status": (
                "failed"
                if failed
                else ("passed_with_differences" if differences else "passed")
            ),
            "summary": summary,
        }

    def audit_session(self, session: date) -> dict[str, object]:
        sources: dict[str, _Source] = {}
        source_issues: list[dict[str, object]] = []
        for label, dataset in (
            ("minute", FlatFileDataset.MINUTE_AGGREGATES),
            ("day", FlatFileDataset.DAY_AGGREGATES),
        ):
            try:
                sources[label] = self._load_source(dataset, session)
            except (MarketAuditError, OSError, ValueError) as exc:
                source_issues.append(
                    _issue(
                        "source_unavailable",
                        label,
                        1,
                        f"cannot load the {label} Flat File source: {exc}",
                    )
                )
        if source_issues:
            return {
                "audit_schema_version": MARKET_AUDIT_SCHEMA_VERSION,
                "cache_status": "not_written",
                "comparison": {"status": "not_run"},
                "datasets": {},
                "gates": {
                    "cross_product_reconciliation": "not_run",
                    "source_and_row_integrity": "failed",
                    "ticker_coverage": "not_run",
                },
                "issues": _sort_issues(source_issues),
                "session_date": session.isoformat(),
                "sources": {
                    label: source.binding(self.data_root)
                    for label, source in sorted(sources.items())
                },
                "status": "failed",
            }

        binding = {
            "config_digest": stable_digest(self._cache_config()),
            "day": sources["day"].binding(self.data_root),
            "minute": sources["minute"].binding(self.data_root),
        }
        if self.use_cache:
            cached = self._load_cache(session, binding)
            if cached is not None:
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
                    "cache_schema_version": CACHE_SCHEMA_VERSION,
                    "result": cached_result,
                    "result_sha256": stable_digest(cached_result),
                    "session_date": session.isoformat(),
                },
            )
        return result

    def _config(self) -> dict[str, object]:
        return {
            "end": self.end.isoformat(),
            "start": self.start.isoformat(),
            **self._cache_config(),
        }

    def _cache_config(self) -> dict[str, object]:
        """Return only policy that can change one session's deterministic result."""

        return {
            "coverage_policy": self.coverage_policy.to_dict(),
            "engine_versions": {
                "exchange_calendars": _package_version("exchange_calendars"),
                "polars": pl.__version__,
                "tzdata": _package_version("tzdata"),
            },
            "max_examples": self.max_examples,
            "tolerance": self.tolerance.to_dict(),
        }

    def _cache_path(self, session: date) -> Path:
        return self.cache_dir / f"{session.isoformat()}.json"

    def _load_cache(self, session: date, binding: dict[str, object]) -> dict[str, object] | None:
        path = self._cache_path(session)
        try:
            document = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return None
        except (OSError, UnicodeError, json.JSONDecodeError):
            return None
        if (
            not isinstance(document, dict)
            or document.get("cache_schema_version") != CACHE_SCHEMA_VERSION
            or document.get("session_date") != session.isoformat()
            or document.get("binding") != binding
            or not isinstance(document.get("result"), dict)
        ):
            return None
        result = document["result"]
        expected_sources = {"day": binding["day"], "minute": binding["minute"]}
        if (
            result.get("audit_schema_version") != MARKET_AUDIT_SCHEMA_VERSION
            or result.get("session_date") != session.isoformat()
            or result.get("sources") != expected_sources
            or document.get("result_sha256") != stable_digest(result)
        ):
            return None
        return dict(result)

    def _load_source(self, dataset: FlatFileDataset, session: date) -> _Source:
        manifest_path = (
            self.data_root
            / "manifests"
            / "massive"
            / "flatfiles"
            / dataset.value
            / f"{session.isoformat()}.json"
        )
        try:
            manifest_bytes = manifest_path.read_bytes()
            manifest = json.loads(manifest_bytes)
        except FileNotFoundError as exc:
            raise MarketAuditError(f"manifest is missing: {manifest_path}") from exc
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise MarketAuditError(f"manifest is unreadable: {manifest_path}") from exc
        if not isinstance(manifest, dict) or manifest.get("status") != "complete":
            raise MarketAuditError(f"manifest is not complete: {manifest_path}")
        item = FlatFileObject(dataset=dataset, session_date=session)
        if (
            manifest.get("flat_file_manifest_schema_version") != 1
            or manifest.get("dataset") != dataset.value
            or manifest.get("session_date") != session.isoformat()
            or manifest.get("object_id") != item.object_id
            or manifest.get("object_key") != item.object_key
        ):
            raise MarketAuditError(f"manifest identity mismatch: {manifest_path}")
        output = manifest.get("output")
        if not isinstance(output, dict) or not isinstance(output.get("sha256"), str):
            raise MarketAuditError(f"manifest output is incomplete: {manifest_path}")
        expected_path = f"bronze/massive/flatfiles/{item.object_key}"
        if output.get("path") != expected_path:
            raise MarketAuditError(
                f"manifest output path differs from {expected_path}: {manifest_path}"
            )
        try:
            artifact_path = safe_relative_path(self.data_root, output.get("path"))
            stat = artifact_path.stat()
        except (ArtifactError, OSError, ValueError) as exc:
            raise MarketAuditError(f"artifact is missing or unsafe: {manifest_path}") from exc
        if not artifact_path.is_file():
            raise MarketAuditError(f"artifact is not a regular file: {artifact_path}")
        expected_bytes = _safe_int(output.get("bytes"))
        if expected_bytes != stat.st_size:
            raise MarketAuditError(
                f"artifact size {stat.st_size} differs from manifest {expected_bytes}: "
                f"{artifact_path}"
            )
        remote = manifest.get("remote")
        if (
            not isinstance(remote, dict)
            or _safe_int(remote.get("content_length")) != expected_bytes
        ):
            raise MarketAuditError(
                f"remote content length differs from output bytes: {manifest_path}"
            )
        observed_sha256 = sha256_file(artifact_path)
        return _Source(
            dataset=dataset.value,
            manifest_path=manifest_path,
            manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
            artifact_path=artifact_path,
            artifact_sha256=str(output["sha256"]),
            observed_sha256=observed_sha256,
            artifact_bytes=stat.st_size,
            artifact_mtime_ns=stat.st_mtime_ns,
        )

    def _compute_session(self, session: date, sources: dict[str, _Source]) -> dict[str, object]:
        issues: list[dict[str, object]] = []
        frames: dict[str, pl.DataFrame] = {}
        dataset_stats: dict[str, dict[str, object]] = {}
        for label in ("minute", "day"):
            source = sources[label]
            try:
                if source.observed_sha256 != source.artifact_sha256:
                    issues.append(
                        _issue(
                            "artifact_sha256_mismatch",
                            label,
                            1,
                            "compressed artifact SHA-256 differs from its manifest",
                        )
                    )
                frame = pl.read_csv(
                    source.artifact_path,
                    schema_overrides=_CSV_SCHEMA,
                    null_values=["", "null"],
                )
                post_parse_stat = source.artifact_path.stat()
                if (
                    post_parse_stat.st_size != source.artifact_bytes
                    or post_parse_stat.st_mtime_ns != source.artifact_mtime_ns
                ):
                    issues.append(
                        _issue(
                            "artifact_changed_during_audit",
                            label,
                            1,
                            "artifact size or mtime changed between hashing and parsing",
                        )
                    )
            except Exception as exc:  # Polars exposes several parser/decompression exception types.
                issues.append(
                    _issue(
                        "csv_unreadable",
                        label,
                        1,
                        f"Polars could not parse the complete gzip CSV: {exc}",
                    )
                )
                continue
            frames[label] = frame
            stats, frame_issues = self._validate_frame(frame, label, session)
            dataset_stats[label] = stats
            issues.extend(frame_issues)

        comparison: dict[str, object] = {"status": "not_run"}
        if set(frames) == {"minute", "day"} and all(
            set(EXPECTED_COLUMNS).issubset(frame.columns) for frame in frames.values()
        ):
            comparison, comparison_issues = self._compare(
                session, frames["minute"], frames["day"]
            )
            issues.extend(comparison_issues)
        issues = _sort_issues(issues)
        integrity_issues = [issue for issue in issues if issue["dataset"] != "cross_table"]
        reconciliation_issues = [
            issue for issue in issues if issue["dataset"] == "cross_table"
        ]
        coverage_status = str(comparison.get("coverage", {}).get("status", "not_run"))
        numerical_differences = any(
            str(issue["code"]).endswith("_mismatch") for issue in reconciliation_issues
        )
        gates = {
            "source_and_row_integrity": "failed" if integrity_issues else "passed",
            "ticker_coverage": coverage_status,
            "cross_product_reconciliation": (
                "different" if numerical_differences else "matched"
            )
            if comparison.get("status") != "not_run"
            else "not_run",
        }
        status = (
            "failed"
            if integrity_issues or coverage_status == "failed"
            else ("passed_with_differences" if reconciliation_issues else "passed")
        )
        return {
            "audit_schema_version": MARKET_AUDIT_SCHEMA_VERSION,
            "comparison": comparison,
            "datasets": dataset_stats,
            "gates": gates,
            "issues": issues,
            "session_date": session.isoformat(),
            "sources": {
                label: source.binding(self.data_root) for label, source in sorted(sources.items())
            },
            "status": status,
        }

    def _validate_frame(
        self, frame: pl.DataFrame, label: str, session: date
    ) -> tuple[dict[str, object], list[dict[str, object]]]:
        issues: list[dict[str, object]] = []
        actual_columns = tuple(frame.columns)
        if actual_columns != EXPECTED_COLUMNS:
            missing = sorted(set(EXPECTED_COLUMNS) - set(actual_columns))
            extra = sorted(set(actual_columns) - set(EXPECTED_COLUMNS))
            issues.append(
                _issue(
                    "schema_mismatch",
                    label,
                    max(1, len(missing) + len(extra)),
                    f"expected exact ordered columns {list(EXPECTED_COLUMNS)}; "
                    f"observed {list(actual_columns)}; missing={missing}; extra={extra}",
                )
            )
        if not set(EXPECTED_COLUMNS).issubset(frame.columns):
            return {"columns": list(actual_columns), "rows": frame.height}, issues

        frame = frame.select(EXPECTED_COLUMNS)
        if frame.height == 0:
            issues.append(_issue("empty_file", label, 1, "Flat File contains no data rows"))
        nulls = {
            name: int(value)
            for name, value in frame.null_count().row(0, named=True).items()
            if value
        }
        for field, count in sorted(nulls.items()):
            issues.append(
                _issue("null_required_field", label, count, f"{field} contains null values")
            )

        ticker_invalid = _count(
            frame,
            pl.col("ticker").is_not_null()
            & (
                pl.col("ticker").str.strip_chars().eq("")
                | pl.col("ticker").ne(pl.col("ticker").str.strip_chars())
            ),
        )
        if ticker_invalid:
            issues.append(
                _issue(
                    "invalid_ticker",
                    label,
                    ticker_invalid,
                    "ticker is blank or has leading/trailing whitespace",
                )
            )

        invalid_prices = _count(
            frame,
            pl.any_horizontal(
                *(
                    pl.col(field).is_not_null() & (~pl.col(field).is_finite() | pl.col(field).le(0))
                    for field in PRICE_FIELDS
                )
            ),
        )
        if invalid_prices:
            issues.append(
                _issue(
                    "invalid_ohlc_value",
                    label,
                    invalid_prices,
                    "one or more OHLC values are non-finite or non-positive",
                )
            )
        invalid_range = _count(
            frame,
            (
                pl.col("high").lt(pl.col("low"))
                | pl.col("open").lt(pl.col("low"))
                | pl.col("open").gt(pl.col("high"))
                | pl.col("close").lt(pl.col("low"))
                | pl.col("close").gt(pl.col("high"))
            ).fill_null(False),
        )
        if invalid_range:
            issues.append(
                _issue(
                    "invalid_ohlc_range",
                    label,
                    invalid_range,
                    "high/low do not contain open and close",
                )
            )
        invalid_volume = _count(
            frame,
            pl.col("volume").is_not_null()
            & (~pl.col("volume").is_finite() | pl.col("volume").lt(0)),
        )
        if invalid_volume:
            issues.append(
                _issue(
                    "invalid_volume",
                    label,
                    invalid_volume,
                    "volume is non-finite or negative",
                )
            )
        negative_transactions = _count(
            frame,
            pl.col("transactions").is_not_null() & pl.col("transactions").lt(0),
        )
        if negative_transactions:
            issues.append(
                _issue(
                    "negative_transactions",
                    label,
                    negative_transactions,
                    "transactions is negative",
                )
            )

        start_ns, end_ns = _session_bounds_ns(session)
        wrong_session = _count(
            frame,
            pl.col("window_start").is_not_null()
            & (pl.col("window_start").lt(start_ns) | pl.col("window_start").ge(end_ns)),
        )
        if wrong_session:
            issues.append(
                _issue(
                    "timestamp_outside_et_session_date",
                    label,
                    wrong_session,
                    f"window_start is not on {session.isoformat()} in America/New_York",
                )
            )
        if label == "minute":
            unaligned = _count(
                frame,
                pl.col("window_start").is_not_null()
                & (pl.col("window_start") % NS_PER_MINUTE).ne(0),
            )
            if unaligned:
                issues.append(
                    _issue(
                        "minute_timestamp_unaligned",
                        label,
                        unaligned,
                        "window_start is not aligned to an exact UTC minute",
                    )
                )
        else:
            expected_day_start = start_ns
            noncanonical_day_timestamp = _count(
                frame,
                pl.col("window_start").is_not_null()
                & pl.col("window_start").ne(expected_day_start),
            )
            if noncanonical_day_timestamp:
                issues.append(
                    _issue(
                        "noncanonical_day_timestamp",
                        label,
                        noncanonical_day_timestamp,
                        "day window_start must equal midnight America/New_York",
                    )
                )

        duplicate_stats = _duplicate_stats(frame)
        if duplicate_stats["duplicate_keys"]:
            issues.append(
                _issue(
                    "duplicate_keys",
                    label,
                    duplicate_stats["duplicate_keys"],
                    "ticker/window_start keys repeat; conflicting groups are also flagged",
                )
            )
        if duplicate_stats["conflicting_duplicate_keys"]:
            issues.append(
                _issue(
                    "conflicting_duplicate_keys",
                    label,
                    duplicate_stats["conflicting_duplicate_keys"],
                    "ticker/window_start keys repeat with conflicting values",
                )
            )
        if label == "day":
            canonical_keys = frame.unique(subset=list(KEY_FIELDS), keep="first")
            multiple_tickers = _group_count(canonical_keys, ["ticker"])
            if multiple_tickers:
                issues.append(
                    _issue(
                        "multiple_day_rows_per_ticker",
                        label,
                        multiple_tickers,
                        "ticker has more than one distinct day bar",
                    )
                )
        stats = {
            "columns": list(actual_columns),
            **duplicate_stats,
            "rows": frame.height,
            "tickers": int(frame["ticker"].drop_nulls().n_unique()),
        }
        return stats, issues

    def _compare(
        self,
        session: date,
        minute: pl.DataFrame,
        day: pl.DataFrame,
    ) -> tuple[dict[str, object], list[dict[str, object]]]:
        minute = _canonicalize(minute)
        day = _canonicalize(day)
        market_open = int(_XNYS.session_open(session.isoformat()).value)
        market_close = int(_XNYS.session_close(session.isoformat()).value)
        minute_all_day = (
            minute.sort(["ticker", "window_start"])
            .group_by("ticker", maintain_order=True)
            .agg(
                pl.col("volume").sum().alias("minute_volume"),
                pl.col("transactions").sum().alias("minute_transactions"),
                pl.len().alias("minute_count"),
            )
            .with_columns(pl.lit(True).alias("_in_minute"))
        )
        minute_regular = (
            minute.filter(
                pl.col("window_start").ge(market_open)
                & pl.col("window_start").lt(market_close)
            )
            .sort(["ticker", "window_start"])
            .group_by("ticker", maintain_order=True)
            .agg(
                pl.col("open").first().alias("minute_open"),
                pl.col("high").max().alias("minute_high"),
                pl.col("low").min().alias("minute_low"),
                pl.col("close").last().alias("minute_close"),
                pl.len().alias("regular_minute_count"),
            )
        )
        minute_daily = minute_all_day.join(
            minute_regular,
            on="ticker",
            how="left",
        )
        day_daily = (
            day.sort(["ticker", "window_start"])
            .unique(subset=["ticker"], keep="first", maintain_order=True)
            .select(
                "ticker",
                *(pl.col(field).alias(f"day_{field}") for field in VALUE_FIELDS),
            )
            .with_columns(pl.lit(True).alias("_in_day"))
        )
        joined = minute_daily.join(day_daily, on="ticker", how="full", coalesce=True).sort("ticker")
        minute_only = joined.filter(pl.col("_in_day").is_null())["ticker"].to_list()
        day_only = joined.filter(pl.col("_in_minute").is_null())["ticker"].to_list()
        both = joined.filter(pl.col("_in_minute").is_not_null() & pl.col("_in_day").is_not_null())
        no_regular_session = both.filter(
            pl.col("regular_minute_count").is_null()
            | pl.col("regular_minute_count").eq(0)
        )["ticker"].to_list()

        ticker_pairs_in_both = both.height
        ticker_union = ticker_pairs_in_both + len(minute_only) + len(day_only)
        missing_tickers = len(minute_only) + len(day_only)
        missing_fraction = missing_tickers / ticker_union if ticker_union else 0.0
        no_regular_fraction = (
            len(no_regular_session) / ticker_pairs_in_both if ticker_pairs_in_both else 0.0
        )
        coverage_failed = (
            missing_tickers >= self.coverage_policy.minimum_missing_tickers
            and missing_fraction > self.coverage_policy.max_missing_fraction
        )
        coverage_status = (
            "failed"
            if coverage_failed
            else (
                "different"
                if missing_tickers or no_regular_session
                else "matched"
            )
        )

        issues: list[dict[str, object]] = []
        if minute_only:
            issues.append(
                _issue(
                    "ticker_missing_from_day",
                    "cross_table",
                    len(minute_only),
                    "minute-derived tickers are absent from day aggregates",
                    minute_only[: self.max_examples],
                )
            )
        if day_only:
            issues.append(
                _issue(
                    "ticker_missing_from_minute",
                    "cross_table",
                    len(day_only),
                    "day aggregate tickers are absent from minute aggregates",
                    day_only[: self.max_examples],
                )
            )
        if no_regular_session:
            issues.append(
                _issue(
                    "ticker_without_regular_session_minutes",
                    "cross_table",
                    len(no_regular_session),
                    "ticker has a day aggregate and minute activity but no XNYS "
                    "regular-session bars",
                    no_regular_session[: self.max_examples],
                )
            )

        mismatches: dict[str, dict[str, object]] = {}
        for field in (*PRICE_FIELDS, "volume"):
            absolute = (
                self.tolerance.price_absolute
                if field in PRICE_FIELDS
                else self.tolerance.volume_absolute
            )
            relative = (
                self.tolerance.price_relative
                if field in PRICE_FIELDS
                else self.tolerance.volume_relative
            )
            mismatch_frame = _float_mismatches(both, field, absolute, relative)
            examples = mismatch_frame.head(self.max_examples).to_dicts()
            compared = _float_comparison_count(both, field)
            mismatches[field] = {
                "compared": compared,
                "count": mismatch_frame.height,
                "examples": examples,
                "rate": mismatch_frame.height / compared if compared else None,
            }
            if mismatch_frame.height:
                issues.append(
                    _issue(
                        f"{field}_mismatch",
                        "cross_table",
                        mismatch_frame.height,
                        f"minute-derived {field} differs from the day aggregate",
                        [str(row["ticker"]) for row in examples],
                    )
                )
        transaction_mismatches = (
            both.filter(
                pl.col("minute_transactions").is_not_null()
                & pl.col("day_transactions").is_not_null()
                & pl.col("minute_transactions").ne(pl.col("day_transactions"))
            )
            .select("ticker", "minute_transactions", "day_transactions")
            .sort("ticker")
        )
        transaction_examples = transaction_mismatches.head(self.max_examples).to_dicts()
        transactions_compared = _count(
            both,
            pl.col("minute_transactions").is_not_null()
            & pl.col("day_transactions").is_not_null(),
        )
        mismatches["transactions"] = {
            "compared": transactions_compared,
            "count": transaction_mismatches.height,
            "examples": transaction_examples,
            "rate": (
                transaction_mismatches.height / transactions_compared
                if transactions_compared
                else None
            ),
        }
        if transaction_mismatches.height:
            issues.append(
                _issue(
                    "transactions_mismatch",
                    "cross_table",
                    transaction_mismatches.height,
                    "summed minute transactions differ from the day aggregate",
                    [str(row["ticker"]) for row in transaction_examples],
                )
            )
        return (
            {
                "compared_tickers": ticker_pairs_in_both,
                "ticker_pairs_in_both_files": ticker_pairs_in_both,
                "field_mismatches": mismatches,
                "missing_tickers": {
                    "day_only": {
                        "count": len(day_only),
                        "examples": day_only[: self.max_examples],
                    },
                    "minute_only": {
                        "count": len(minute_only),
                        "examples": minute_only[: self.max_examples],
                    },
                },
                "regular_session_missing": {
                    "count": len(no_regular_session),
                    "examples": no_regular_session[: self.max_examples],
                },
                "coverage": {
                    "missing_fraction": missing_fraction,
                    "missing_tickers": missing_tickers,
                    "no_regular_session_fraction": no_regular_fraction,
                    "policy": self.coverage_policy.to_dict(),
                    "status": coverage_status,
                    "ticker_union": ticker_union,
                },
                "status": "different" if issues else "matched",
                "tolerance": self.tolerance.to_dict(),
                "reconstruction": {
                    "ohlc": "XNYS regular-session minute bars [open, close)",
                    "volume_and_transactions": "all minute bars in the ET session date",
                },
            },
            issues,
        )


def _canonicalize(frame: pl.DataFrame) -> pl.DataFrame:
    """Choose one deterministic row per native key after duplicate reporting."""

    return (
        frame.select(EXPECTED_COLUMNS)
        .sort([*KEY_FIELDS, *VALUE_FIELDS], nulls_last=True)
        .unique(subset=list(KEY_FIELDS), keep="first", maintain_order=True)
    )


def _duplicate_stats(frame: pl.DataFrame) -> dict[str, int]:
    if not frame.height:
        return {
            "conflicting_duplicate_keys": 0,
            "duplicate_excess_rows": 0,
            "duplicate_keys": 0,
            "exact_duplicate_keys": 0,
        }
    groups = (
        frame.group_by(list(KEY_FIELDS))
        .agg(
            pl.len().alias("_rows"),
            pl.struct(VALUE_FIELDS).n_unique().alias("_variants"),
        )
        .filter(pl.col("_rows") > 1)
    )
    if not groups.height:
        return {
            "conflicting_duplicate_keys": 0,
            "duplicate_excess_rows": 0,
            "duplicate_keys": 0,
            "exact_duplicate_keys": 0,
        }
    return {
        "conflicting_duplicate_keys": _count(groups, pl.col("_variants") > 1),
        "duplicate_excess_rows": int(groups.select((pl.col("_rows") - 1).sum()).item() or 0),
        "duplicate_keys": groups.height,
        "exact_duplicate_keys": _count(groups, pl.col("_variants") == 1),
    }


def _group_count(frame: pl.DataFrame, keys: list[str]) -> int:
    if not frame.height:
        return 0
    return frame.group_by(keys).len().filter(pl.col("len") > 1).height


def _float_mismatches(
    frame: pl.DataFrame, field: str, absolute: float, relative: float
) -> pl.DataFrame:
    minute = pl.col(f"minute_{field}")
    day = pl.col(f"day_{field}")
    difference = (minute - day).abs()
    allowed = pl.max_horizontal(
        pl.lit(absolute), pl.max_horizontal(minute.abs(), day.abs()) * relative
    )
    return (
        frame.with_columns(
            difference.alias("absolute_difference"), allowed.alias("allowed_tolerance")
        )
        .filter(
            minute.is_not_null()
            & day.is_not_null()
            & minute.is_finite()
            & day.is_finite()
            & pl.col("absolute_difference").gt(pl.col("allowed_tolerance"))
        )
        .select(
            "ticker",
            minute.alias("minute"),
            day.alias("day"),
            "absolute_difference",
            "allowed_tolerance",
        )
        .sort("ticker")
    )


def _float_comparison_count(frame: pl.DataFrame, field: str) -> int:
    minute = pl.col(f"minute_{field}")
    day = pl.col(f"day_{field}")
    return _count(
        frame,
        minute.is_not_null()
        & day.is_not_null()
        & minute.is_finite()
        & day.is_finite(),
    )


def _session_bounds_ns(session: date) -> tuple[int, int]:
    start = datetime.combine(session, time.min, tzinfo=_NEW_YORK)
    end = datetime.combine(session + timedelta(days=1), time.min, tzinfo=_NEW_YORK)
    return int(start.timestamp() * 1_000_000_000), int(end.timestamp() * 1_000_000_000)


def _combined_gate_status(values: list[str]) -> str:
    if "failed" in values:
        return "failed"
    if "different" in values:
        return "different"
    if "matched" in values:
        return "matched"
    if "passed" in values:
        return "passed"
    return "not_run"


def _package_version(package: str) -> str:
    try:
        return version(package)
    except PackageNotFoundError:
        return "not-installed"


def _count(frame: pl.DataFrame, expression: pl.Expr) -> int:
    return int(frame.select(expression.fill_null(False).sum()).item() or 0)


def _safe_int(value: object) -> int:
    if isinstance(value, bool):
        return -1
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError, OverflowError):
        return -1


def _issue(
    code: str,
    dataset: str,
    count: int,
    message: str,
    examples: list[str] | None = None,
) -> dict[str, object]:
    issue: dict[str, object] = {
        "code": code,
        "count": int(count),
        "dataset": dataset,
        "message": message,
    }
    if examples:
        issue["examples"] = examples
    return issue


def _sort_issues(issues: list[dict[str, object]]) -> list[dict[str, object]]:
    return sorted(issues, key=lambda item: (str(item["dataset"]), str(item["code"])))


__all__ = [
    "EXPECTED_COLUMNS",
    "MARKET_AUDIT_SCHEMA_VERSION",
    "MarketAuditError",
    "MarketAuditTolerance",
    "MarketCoveragePolicy",
    "MarketCrossAuditor",
]
