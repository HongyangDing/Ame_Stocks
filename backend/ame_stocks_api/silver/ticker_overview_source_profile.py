"""Read-only S6 profiling and accepted coverage for Ticker Overview.

The legacy ``silver_unadjusted`` safe-v2 Parquet is an oracle, never a formal
Silver input.  This module rebuilds the allowlisted rows from the exact v2
lifecycle receipt and every manifest-bound Bronze response, then requires an
exact schema/value match with that oracle before it emits an accepted coverage
receipt.
"""

from __future__ import annotations

import csv
import gzip
import hashlib
import io
import json
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from ame_stocks_api.artifacts import safe_relative_path, stable_digest
from ame_stocks_api.downloads.plan import build_download_plan
from ame_stocks_core import ProviderDataset

PROFILE_SCHEMA_VERSION = 1
COVERAGE_RECEIPT_SCHEMA_VERSION = 1

PRODUCTION_START = date(2016, 7, 11)
PRODUCTION_END = date(2026, 7, 9)
PRODUCTION_LIFECYCLE_MANIFEST_PATH = (
    "manifests/materialized/ticker_overview_lifecycles/schema=v2/2016-07-11_2026-07-09.json"
)
PRODUCTION_LIFECYCLE_MANIFEST_SHA256 = (
    "62a0cb055b92836e2b8c85d1f9c6c9d87899da9f45fbd5ebe2b9295b20d7785b"
)
PRODUCTION_LIFECYCLE_PATH = (
    "staging/ticker_overview/schema=v2/window=2016-07-11_2026-07-09/lifecycles.parquet"
)
PRODUCTION_LIFECYCLE_SHA256 = "8288f2c88190d8048fa6687a3ce0ed7aedbac0a62acb1e8028df1e8860dd8544"
PRODUCTION_REQUESTS_PATH = (
    "staging/ticker_overview/schema=v2/window=2016-07-11_2026-07-09/requests.csv"
)
PRODUCTION_REQUESTS_SHA256 = "c39a6a9a54cd6b181a11d6a4af065760e55656ff7393ab85b41232a5718614a0"
PRODUCTION_ORACLE_MANIFEST_PATH = (
    "manifests/materialized/ticker_overview_safe/schema=v2/2016-07-11_2026-07-09.json"
)
PRODUCTION_ORACLE_MANIFEST_SHA256 = (
    "a0c08afc566cc080704db9454a8c2224d47947e84d92e2eb15cb165fe6b2c9f5"
)
PRODUCTION_ORACLE_PATH = (
    "silver_unadjusted/reference/ticker_overview_safe/schema=v2/"
    "window=2016-07-11_2026-07-09/ticker_overview.parquet"
)
PRODUCTION_ORACLE_SHA256 = "0094448bae7e238779ee100d85818ec150b958fb69d3c897058b5b036de159aa"
PRODUCTION_LIFECYCLE_PLAN_PATH = (
    "manifests/plans/ticker_overview/lifecycles-2016-07-11_2026-07-09.jsonl"
)
PRODUCTION_COVERAGE_RECEIPT_NAMESPACE = "manifests/silver/source-coverage/ticker_overview"

_MANIFEST_PREFIX = "manifests/massive/ticker_overview"
_ARTIFACT_PREFIX = "bronze/massive/ticker_overview"
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_LIFECYCLE_COLUMNS = (
    "lifecycle_id",
    "ticker",
    "first_active_date",
    "last_active_date",
    "query_date",
    "identity_type",
    "identity_value",
    "cik",
    "composite_figi",
    "share_class_figi",
    "type",
    "name",
    "primary_exchange",
)
_SAFE_COLUMNS = (
    "lifecycle_id",
    "source_request_id",
    "query_ticker",
    "query_date",
    "first_active_date",
    "last_active_date",
    "identity_type",
    "identity_value",
    "identity_match",
    "identity_match_basis",
    "ticker",
    "name",
    "type",
    "market",
    "locale",
    "active",
    "primary_exchange",
    "currency_name",
    "cik",
    "composite_figi",
    "share_class_figi",
    "sic_code",
    "sic_description",
    "list_date",
    "delisted_utc",
    "ticker_root",
    "ticker_suffix",
)
_SAFE_RESPONSE_FIELDS = _SAFE_COLUMNS[10:]
_IDENTITY_FIELDS = ("share_class_figi", "composite_figi", "cik")
_KNOWN_RESULT_FIELDS = frozenset(
    {
        "active",
        "address",
        "branding",
        "cik",
        "composite_figi",
        "currency_name",
        "delisted_utc",
        "description",
        "homepage_url",
        "list_date",
        "locale",
        "market",
        "market_cap",
        "name",
        "phone_number",
        "primary_exchange",
        "round_lot",
        "share_class_figi",
        "share_class_shares_outstanding",
        "sic_code",
        "sic_description",
        "ticker",
        "ticker_root",
        "ticker_suffix",
        "total_employees",
        "type",
        "weighted_shares_outstanding",
    }
)
_FORBIDDEN_SAFE_FIELDS = frozenset(
    {"market_cap", "share_class_shares_outstanding", "weighted_shares_outstanding"}
)


class TickerOverviewSourceProfileError(ValueError):
    """Raised when the v2 lifecycle, Bronze, or safe oracle cannot be trusted."""


@dataclass(frozen=True, slots=True)
class TickerOverviewCoverageExpectation:
    lifecycle_rows: int
    response_rows: int
    identity_match_rows: int
    identity_mismatch_rows: int
    sic_code_rows: int
    list_date_rows: int

    def __post_init__(self) -> None:
        for name in (
            "lifecycle_rows",
            "response_rows",
            "identity_match_rows",
            "identity_mismatch_rows",
            "sic_code_rows",
            "list_date_rows",
        ):
            value = getattr(self, name)
            if type(value) is not int or value < 0:
                raise TickerOverviewSourceProfileError(
                    f"{name} must be a native nonnegative integer"
                )
        if self.identity_match_rows + self.identity_mismatch_rows != self.response_rows:
            raise TickerOverviewSourceProfileError("identity coverage does not reconcile")
        if self.lifecycle_rows != self.response_rows:
            raise TickerOverviewSourceProfileError("lifecycle/response coverage differs")


PRODUCTION_TICKER_OVERVIEW_COVERAGE = TickerOverviewCoverageExpectation(
    lifecycle_rows=30_739,
    response_rows=30_739,
    identity_match_rows=30_570,
    identity_mismatch_rows=169,
    sic_code_rows=16_682,
    list_date_rows=23_417,
)


def profile_ticker_overview_source(
    data_root: Path,
    *,
    lifecycle_manifest_path: str = PRODUCTION_LIFECYCLE_MANIFEST_PATH,
    oracle_manifest_path: str = PRODUCTION_ORACLE_MANIFEST_PATH,
    lifecycle_plan_path: str = PRODUCTION_LIFECYCLE_PLAN_PATH,
    expected: TickerOverviewCoverageExpectation = PRODUCTION_TICKER_OVERVIEW_COVERAGE,
    start: date = PRODUCTION_START,
    end: date = PRODUCTION_END,
) -> dict[str, object]:
    """Verify the complete S6 source and return a deterministic accepted receipt."""

    root = data_root.expanduser().resolve()
    lifecycle_manifest, lifecycle_manifest_ref = _load_manifest(root, lifecycle_manifest_path)
    oracle_manifest, oracle_manifest_ref = _load_manifest(root, oracle_manifest_path)
    if expected == PRODUCTION_TICKER_OVERVIEW_COVERAGE:
        _require_production_identity(
            lifecycle_manifest_path=lifecycle_manifest_path,
            lifecycle_manifest_sha256=str(lifecycle_manifest_ref["sha256"]),
            oracle_manifest_path=oracle_manifest_path,
            oracle_manifest_sha256=str(oracle_manifest_ref["sha256"]),
            lifecycle_plan_path=lifecycle_plan_path,
            start=start,
            end=end,
        )
    lifecycle_path, requests_path = _validate_lifecycle_manifest(
        lifecycle_manifest, start=start, end=end, expected=expected
    )
    lifecycle_bytes, lifecycle_ref = _read_bound_file(
        root,
        lifecycle_path,
        expected_sha256=_manifest_output_sha(lifecycle_manifest, lifecycle_path),
    )
    requests_bytes, requests_ref = _read_bound_file(
        root, requests_path, expected_sha256=_manifest_output_sha(lifecycle_manifest, requests_path)
    )
    lifecycle_table = _read_parquet_bytes(lifecycle_bytes, "lifecycle")
    lifecycle_rows = _lifecycle_rows(lifecycle_table, expected=expected)
    request_pairs = _request_pairs(requests_bytes, start=start, end=end)
    expected_pairs = tuple(
        sorted(
            ((str(row["ticker"]), row["query_date"]) for row in lifecycle_rows),
            key=lambda item: (item[1], item[0]),
        )
    )
    if request_pairs != expected_pairs:
        raise TickerOverviewSourceProfileError(
            "ticker overview requests receipt differs from lifecycle rows"
        )
    lifecycle_plan_bytes = lifecycle_plan_content(lifecycle_rows)
    lifecycle_plan = {
        "bytes": len(lifecycle_plan_bytes),
        "media_type": "text/plain",
        "path": lifecycle_plan_path,
        "row_count": len(lifecycle_rows),
        "sha256": hashlib.sha256(lifecycle_plan_bytes).hexdigest(),
    }

    plan = build_download_plan(
        dataset=ProviderDataset.TICKER_OVERVIEW,
        start=start,
        end=end,
        ticker_dates=request_pairs,
    )
    requests = {request.request_id: request for request in plan.requests}
    if len(requests) != expected.response_rows:
        raise TickerOverviewSourceProfileError("ticker overview request plan cardinality changed")
    manifest_dir = safe_relative_path(root, _MANIFEST_PREFIX)
    actual = {path.stem: path for path in sorted(manifest_dir.glob("*.json"))}
    if set(actual) != set(requests):
        raise TickerOverviewSourceProfileError(
            "ticker overview manifest coverage differs from exact lifecycle plan"
        )

    lifecycle_by_pair = {(str(row["ticker"]), row["query_date"]): row for row in lifecycle_rows}
    manifest_refs: list[dict[str, object]] = []
    artifacts: list[dict[str, object]] = []
    source_rows: list[dict[str, object]] = []
    for request_id in sorted(requests):
        request = requests[request_id]
        verified = _verify_manifest_and_payload(
            root,
            actual[request_id],
            request_id=request_id,
            expected_request=request.canonical_dict(),
        )
        key = (request.asset_ids[0], request.start)
        try:
            lifecycle = lifecycle_by_pair[key]
        except KeyError as exc:  # pragma: no cover - plan was built from these pairs
            raise TickerOverviewSourceProfileError("verified request has no lifecycle row") from exc
        safe_row = _safe_row(lifecycle, request_id=request_id, result=verified["result"])
        source_rows.append(safe_row)
        manifest_refs.append(
            {
                "artifact": verified["artifact"],
                "completed_at": verified["completed_at"],
                "created_at": verified["created_at"],
                "path": verified["manifest_path"],
                "query_date": request.start.isoformat(),
                "query_ticker": request.asset_ids[0],
                "request_id": request_id,
                "sha256": verified["manifest_sha256"],
                "updated_at": verified["updated_at"],
            }
        )
        artifacts.append(dict(verified["artifact"]))

    oracle_path = _validate_oracle_manifest(
        oracle_manifest,
        start=start,
        end=end,
        expected=expected,
    )
    oracle_bytes, oracle_ref = _read_bound_file(
        root, oracle_path, expected_sha256=_manifest_output_sha(oracle_manifest, oracle_path)
    )
    oracle = _read_parquet_bytes(oracle_bytes, "safe-v2 oracle")
    _compare_oracle(oracle, source_rows, expected=expected)
    diagnostics = _diagnostics(source_rows)
    expected_diagnostics = {
        "identity_conflict_rows": 0,
        "identity_match_rows": expected.identity_match_rows,
        "identity_mismatch_rows": expected.identity_mismatch_rows,
        "identity_no_comparable_rows": expected.identity_mismatch_rows,
        "list_date_after_query_date_rows": 0,
        "list_date_rows": expected.list_date_rows,
        "sic_code_rows": expected.sic_code_rows,
        "unsafe_output_columns": [],
    }
    if any(diagnostics.get(key) != value for key, value in expected_diagnostics.items()):
        raise TickerOverviewSourceProfileError("ticker overview reviewed diagnostics changed")
    if expected == PRODUCTION_TICKER_OVERVIEW_COVERAGE and diagnostics.get(
        "identity_mismatch_by_identity_type"
    ) != {"cik": 21, "composite_figi": 3, "share_class_figi": 145, "ticker": 0}:
        raise TickerOverviewSourceProfileError(
            "ticker overview unresolved identity-type profile changed"
        )

    receipt: dict[str, object] = {
        "artifacts": artifacts,
        "coverage_receipt_schema_version": COVERAGE_RECEIPT_SCHEMA_VERSION,
        "diagnostics": diagnostics,
        "lifecycle": {
            "manifest": lifecycle_manifest_ref,
            "parquet": {**lifecycle_ref, "row_count": len(lifecycle_rows)},
            "requests": {**requests_ref, "row_count": len(request_pairs)},
        },
        "lifecycle_plan": lifecycle_plan,
        "manifest_refs": manifest_refs,
        "oracle": {
            "manifest": oracle_manifest_ref,
            "parquet": {**oracle_ref, "row_count": oracle.num_rows},
            "role": "comparison_only_not_formal_source",
        },
        "source_dataset": "ticker_overview",
        "status": "passed_with_warnings",
        "window": {"end": end.isoformat(), "start": start.isoformat()},
    }
    receipt["coverage_receipt_id"] = stable_digest(receipt)
    report: dict[str, object] = {
        "accepted_coverage_receipt": receipt,
        "hard_gate_counts": {
            "bronze_integrity_errors": 0,
            "coverage_errors": 0,
            "lifecycle_contract_errors": 0,
            "oracle_mismatches": 0,
            "unsafe_allowlist_columns": 0,
        },
        "profile_schema_version": PROFILE_SCHEMA_VERSION,
        "status": "passed_with_warnings",
        "warning_counts": {"identity_mismatch_rows": expected.identity_mismatch_rows},
    }
    report["profile_sha256"] = stable_digest(report)
    return report


def accepted_coverage_receipt(profile: dict[str, object]) -> dict[str, object]:
    """Return a validated detached receipt from one accepted profile."""

    if profile.get("status") != "passed_with_warnings":
        raise TickerOverviewSourceProfileError("ticker overview profile is not accepted")
    gates = profile.get("hard_gate_counts")
    if not isinstance(gates, dict) or any(value != 0 for value in gates.values()):
        raise TickerOverviewSourceProfileError("ticker overview profile has a nonzero hard gate")
    receipt = profile.get("accepted_coverage_receipt")
    return validate_ticker_overview_coverage_receipt(receipt)


def validate_ticker_overview_coverage_receipt(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TickerOverviewSourceProfileError("ticker overview coverage receipt must be an object")
    receipt = json.loads(json.dumps(value, allow_nan=False, sort_keys=True))
    receipt_id = receipt.pop("coverage_receipt_id", None)
    if not isinstance(receipt_id, str) or not _SHA256.fullmatch(receipt_id):
        raise TickerOverviewSourceProfileError("ticker overview coverage receipt ID is invalid")
    if stable_digest(receipt) != receipt_id:
        raise TickerOverviewSourceProfileError("ticker overview coverage receipt digest mismatch")
    receipt["coverage_receipt_id"] = receipt_id
    if (
        receipt.get("coverage_receipt_schema_version") != COVERAGE_RECEIPT_SCHEMA_VERSION
        or receipt.get("source_dataset") != "ticker_overview"
        or receipt.get("status") != "passed_with_warnings"
        or not isinstance(receipt.get("manifest_refs"), list)
        or not isinstance(receipt.get("artifacts"), list)
        or len(receipt["manifest_refs"]) != len(receipt["artifacts"])
    ):
        raise TickerOverviewSourceProfileError("ticker overview coverage receipt shape changed")
    artifacts_by_path: dict[str, dict[str, object]] = {}
    for raw in receipt["artifacts"]:
        if not isinstance(raw, dict) or not isinstance(raw.get("path"), str):
            raise TickerOverviewSourceProfileError("coverage receipt artifact is invalid")
        path = str(raw["path"])
        if path in artifacts_by_path:
            raise TickerOverviewSourceProfileError("coverage receipt repeats a Bronze artifact")
        artifacts_by_path[path] = raw
    manifest_paths: set[str] = set()
    request_ids: set[str] = set()
    nested_artifact_paths: set[str] = set()
    for raw in receipt["manifest_refs"]:
        if not isinstance(raw, dict):
            raise TickerOverviewSourceProfileError("coverage receipt manifest ref is invalid")
        manifest_path = raw.get("path")
        request_id = raw.get("request_id")
        nested = raw.get("artifact")
        if (
            not isinstance(manifest_path, str)
            or not isinstance(request_id, str)
            or not isinstance(nested, dict)
            or not isinstance(nested.get("path"), str)
            or manifest_path in manifest_paths
            or request_id in request_ids
        ):
            raise TickerOverviewSourceProfileError(
                "coverage receipt repeats or malforms a Bronze manifest"
            )
        nested_path = str(nested["path"])
        if nested_path in nested_artifact_paths or artifacts_by_path.get(nested_path) != nested:
            raise TickerOverviewSourceProfileError(
                "coverage receipt manifest/artifact binding is not one-to-one"
            )
        manifest_paths.add(manifest_path)
        request_ids.add(request_id)
        nested_artifact_paths.add(nested_path)
    if nested_artifact_paths != set(artifacts_by_path):
        raise TickerOverviewSourceProfileError(
            "coverage receipt manifest/artifact coverage differs"
        )
    return receipt


def coverage_receipt_bytes(receipt: dict[str, object]) -> bytes:
    validated = validate_ticker_overview_coverage_receipt(receipt)
    return json.dumps(validated, allow_nan=False, indent=2, sort_keys=True).encode("utf-8") + b"\n"


def lifecycle_plan_content(rows: list[dict[str, object]]) -> bytes:
    """Encode all 13 lifecycle fields as deterministic JSONL control rows."""

    lines: list[bytes] = []
    for row in sorted(
        rows,
        key=lambda item: (
            str(item["query_date"]),
            str(item["ticker"]),
            str(item["lifecycle_id"]),
        ),
    ):
        item = {
            name: (row[name].isoformat() if isinstance(row[name], date) else row[name])
            for name in _LIFECYCLE_COLUMNS
        }
        lines.append(
            json.dumps(item, allow_nan=False, separators=(",", ":"), sort_keys=True).encode()
            + b"\n"
        )
    return b"".join(lines)


def _verify_manifest_and_payload(
    root: Path,
    manifest_path: Path,
    *,
    request_id: str,
    expected_request: dict[str, object],
) -> dict[str, object]:
    manifest_bytes = manifest_path.read_bytes()
    manifest = _json_object(manifest_bytes, "ticker overview manifest")
    if (
        manifest.get("dataset") != "ticker_overview"
        or manifest.get("provider") != "massive"
        or manifest.get("request_id") != request_id
        or manifest.get("request") != expected_request
        or manifest.get("status") != "complete"
        or manifest.get("checkpoint") is not None
    ):
        raise TickerOverviewSourceProfileError("ticker overview manifest identity/status changed")
    created = _utc_timestamp(manifest.get("created_at"), "created_at")
    completed = _utc_timestamp(manifest.get("completed_at"), "completed_at")
    updated = _utc_timestamp(manifest.get("updated_at"), "updated_at")
    if not created <= completed <= updated:
        raise TickerOverviewSourceProfileError("ticker overview manifest timestamps are reversed")
    raw_artifacts = manifest.get("artifacts")
    if not isinstance(raw_artifacts, list) or len(raw_artifacts) != 1:
        raise TickerOverviewSourceProfileError("ticker overview requires exactly one page")
    artifact = raw_artifacts[0]
    if not isinstance(artifact, dict):
        raise TickerOverviewSourceProfileError("ticker overview artifact must be an object")
    expected_path = f"{_ARTIFACT_PREFIX}/request_id={request_id}/page-00000.json.gz"
    if (
        artifact.get("path") != expected_path
        or artifact.get("sequence") != 0
        or artifact.get("record_count") != 1
        or artifact.get("is_last") is not True
        or artifact.get("next_continuation") is not None
        or artifact.get("content_type") != "application/json"
    ):
        raise TickerOverviewSourceProfileError("ticker overview artifact contract changed")
    content = safe_relative_path(root, expected_path).read_bytes()
    if len(content) != artifact.get("compressed_bytes") or hashlib.sha256(
        content
    ).hexdigest() != artifact.get("stored_sha256"):
        raise TickerOverviewSourceProfileError("ticker overview stored page checksum changed")
    try:
        raw = gzip.decompress(content)
    except (OSError, EOFError) as exc:
        raise TickerOverviewSourceProfileError("ticker overview page is invalid gzip") from exc
    if len(raw) != artifact.get("raw_bytes") or hashlib.sha256(raw).hexdigest() != artifact.get(
        "raw_sha256"
    ):
        raise TickerOverviewSourceProfileError("ticker overview raw page checksum changed")
    document = _json_object(raw, "ticker overview response")
    if set(document) != {"request_id", "results", "status"} or document.get("status") != "OK":
        raise TickerOverviewSourceProfileError("ticker overview response envelope changed")
    provider_request_id = document.get("request_id")
    result = document.get("results")
    if not isinstance(provider_request_id, str) or not provider_request_id.strip():
        raise TickerOverviewSourceProfileError("ticker overview provider request ID is invalid")
    if not isinstance(result, dict) or set(result).difference(_KNOWN_RESULT_FIELDS):
        raise TickerOverviewSourceProfileError("ticker overview result schema drifted")
    ticker = result.get("ticker")
    if ticker != expected_request["asset_ids"][0]:
        raise TickerOverviewSourceProfileError(
            "ticker overview response ticker differs from request"
        )
    if type(result.get("active")) is not bool:
        raise TickerOverviewSourceProfileError("ticker overview active field is not Boolean")
    for field in _SAFE_RESPONSE_FIELDS:
        value = result.get(field)
        if field == "active" or value is None:
            continue
        if not isinstance(value, str):
            raise TickerOverviewSourceProfileError(
                f"ticker overview safe string field changed type: {field}"
            )
    if result.get("list_date") is not None:
        _iso_date(result["list_date"], "list_date")
    artifact_ref = {
        "bytes": int(artifact["compressed_bytes"]),
        "media_type": "application/gzip+json",
        "path": expected_path,
        "raw_bytes": int(artifact["raw_bytes"]),
        "raw_sha256": str(artifact["raw_sha256"]),
        "row_count": 1,
        "sequence": 0,
        "sha256": str(artifact["stored_sha256"]),
    }
    return {
        "artifact": artifact_ref,
        "completed_at": completed.isoformat(),
        "created_at": created.isoformat(),
        "manifest_path": manifest_path.relative_to(root).as_posix(),
        "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        "provider_request_id": provider_request_id,
        "result": result,
        "result_hash": stable_digest(result),
        "updated_at": updated.isoformat(),
    }


def _safe_row(
    lifecycle: dict[str, object], *, request_id: str, result: dict[str, object]
) -> dict[str, object]:
    identities = {field: _clean_string(result.get(field)) for field in _IDENTITY_FIELDS}
    identity_match, basis, evidence_status = _identity_match(
        lifecycle,
        response_identities=identities,
        response_ticker=_clean_string(result.get("ticker")),
    )
    return {
        "lifecycle_id": lifecycle["lifecycle_id"],
        "source_request_id": request_id,
        "query_ticker": lifecycle["ticker"],
        "query_date": lifecycle["query_date"],
        "first_active_date": lifecycle["first_active_date"],
        "last_active_date": lifecycle["last_active_date"],
        "identity_type": lifecycle["identity_type"],
        "identity_value": lifecycle["identity_value"],
        "identity_match": identity_match,
        "identity_match_basis": basis,
        "identity_evidence_status": evidence_status,
        "ticker": _clean_string(result.get("ticker")),
        "name": _clean_string(result.get("name")),
        "type": _clean_string(result.get("type")),
        "market": _clean_string(result.get("market")),
        "locale": _clean_string(result.get("locale")),
        "active": result.get("active"),
        "primary_exchange": _clean_string(result.get("primary_exchange")),
        "currency_name": _clean_string(result.get("currency_name")),
        **identities,
        "sic_code": _clean_string(result.get("sic_code")),
        "sic_description": _clean_string(result.get("sic_description")),
        "list_date": (
            None if result.get("list_date") is None else _iso_date(result["list_date"], "list_date")
        ),
        "delisted_utc": _clean_string(result.get("delisted_utc")),
        "ticker_root": _clean_string(result.get("ticker_root")),
        "ticker_suffix": _clean_string(result.get("ticker_suffix")),
    }


def _identity_match(
    lifecycle: dict[str, object],
    *,
    response_identities: dict[str, str | None],
    response_ticker: str | None,
) -> tuple[bool, str | None, str]:
    if lifecycle["identity_type"] == "ticker":
        matches = response_ticker == _clean_string(lifecycle.get("ticker"))
        return (
            matches,
            "ticker" if matches else None,
            "matched" if matches else "comparable_identity_conflict",
        )
    comparable: list[tuple[str, bool]] = []
    for field in _IDENTITY_FIELDS:
        expected = _clean_string(lifecycle.get(field))
        actual = response_identities[field]
        if expected is not None and actual is not None:
            comparable.append((field, expected == actual))
    if not comparable:
        return False, None, "no_comparable_identity"
    if any(not matches for _, matches in comparable):
        return False, None, "comparable_identity_conflict"
    matched = {field for field, matches in comparable if matches}
    return True, next(field for field in _IDENTITY_FIELDS if field in matched), "matched"


def _compare_oracle(
    oracle: pa.Table,
    source_rows: list[dict[str, object]],
    *,
    expected: TickerOverviewCoverageExpectation,
) -> None:
    if tuple(oracle.column_names) != _SAFE_COLUMNS or oracle.num_rows != expected.response_rows:
        raise TickerOverviewSourceProfileError("safe-v2 oracle schema/cardinality changed")
    if _FORBIDDEN_SAFE_FIELDS.intersection(oracle.column_names):
        raise TickerOverviewSourceProfileError("unsafe field entered safe-v2 oracle")
    derived = pa.Table.from_pylist(source_rows, schema=oracle.schema)
    indices = pc.sort_indices(
        derived,
        sort_keys=[
            ("query_date", "ascending"),
            ("query_ticker", "ascending"),
            ("lifecycle_id", "ascending"),
        ],
    )
    derived = pc.take(derived, indices)
    if not oracle.combine_chunks().equals(derived.combine_chunks()):
        raise TickerOverviewSourceProfileError(
            "source-derived allowlist differs from safe-v2 oracle values"
        )


def _diagnostics(rows: list[dict[str, object]]) -> dict[str, object]:
    mismatch_by_type = {
        identity_type: sum(
            item["identity_match"] is False and item["identity_type"] == identity_type
            for item in rows
        )
        for identity_type in (*_IDENTITY_FIELDS, "ticker")
    }
    return {
        "identity_conflict_rows": sum(
            item["identity_evidence_status"] == "comparable_identity_conflict"
            for item in rows
        ),
        "identity_match_rows": sum(item["identity_match"] is True for item in rows),
        "identity_mismatch_rows": sum(item["identity_match"] is False for item in rows),
        "identity_mismatch_by_identity_type": mismatch_by_type,
        "identity_no_comparable_rows": sum(
            item["identity_evidence_status"] == "no_comparable_identity" for item in rows
        ),
        "list_date_after_query_date_rows": sum(
            item["list_date"] is not None and item["list_date"] > item["query_date"]
            for item in rows
        ),
        "list_date_rows": sum(item["list_date"] is not None for item in rows),
        "sic_code_rows": sum(item["sic_code"] is not None for item in rows),
        "unsafe_output_columns": sorted(_FORBIDDEN_SAFE_FIELDS.intersection(_SAFE_COLUMNS)),
    }


def _validate_lifecycle_manifest(
    manifest: dict[str, object],
    *,
    start: date,
    end: date,
    expected: TickerOverviewCoverageExpectation,
) -> tuple[str, str]:
    if (
        manifest.get("kind") != "ticker_identity_lifecycle_requests"
        or manifest.get("status") != "complete"
        or manifest.get("schema_version") != 2
        or manifest.get("window") != {"start": start.isoformat(), "end": end.isoformat()}
        or manifest.get("lifecycle_count") != expected.lifecycle_rows
        or manifest.get("request_count") != expected.lifecycle_rows
        or manifest.get("identity_priority") != list(_IDENTITY_FIELDS)
    ):
        raise TickerOverviewSourceProfileError("ticker overview lifecycle manifest changed")
    lifecycle_path = manifest.get("lifecycle_file")
    requests_path = manifest.get("request_file")
    if not isinstance(lifecycle_path, str) or not isinstance(requests_path, str):
        raise TickerOverviewSourceProfileError("lifecycle manifest output paths are invalid")
    return lifecycle_path, requests_path


def _validate_oracle_manifest(
    manifest: dict[str, object],
    *,
    start: date,
    end: date,
    expected: TickerOverviewCoverageExpectation,
) -> str:
    if (
        manifest.get("kind") != "ticker_overview_allowlisted_reference"
        or manifest.get("status") != "complete"
        or manifest.get("schema_version") != 2
        or manifest.get("window") != {"start": start.isoformat(), "end": end.isoformat()}
        or manifest.get("lifecycle_count") != expected.lifecycle_rows
        or manifest.get("row_count") != expected.response_rows
        or manifest.get("failed_request_count") != 0
    ):
        raise TickerOverviewSourceProfileError("ticker overview safe-v2 manifest changed")
    policy = manifest.get("field_policy")
    if not isinstance(policy, dict) or policy.get("allowlisted_output_columns") != list(
        _SAFE_COLUMNS
    ):
        raise TickerOverviewSourceProfileError("safe-v2 allowlist policy changed")
    if set(policy.get("quarantined_bronze_only_fields", [])) != _FORBIDDEN_SAFE_FIELDS:
        raise TickerOverviewSourceProfileError("safe-v2 unsafe-field policy changed")
    output = manifest.get("output_file")
    if not isinstance(output, str):
        raise TickerOverviewSourceProfileError("safe-v2 output path is invalid")
    return output


def _lifecycle_rows(
    table: pa.Table, *, expected: TickerOverviewCoverageExpectation
) -> list[dict[str, object]]:
    if tuple(table.column_names) != _LIFECYCLE_COLUMNS or table.num_rows != expected.lifecycle_rows:
        raise TickerOverviewSourceProfileError(
            "ticker overview lifecycle schema/cardinality changed"
        )
    rows = table.to_pylist()
    lifecycle_ids = [item["lifecycle_id"] for item in rows]
    pairs = [(item["ticker"], item["query_date"]) for item in rows]
    if len(set(lifecycle_ids)) != len(rows) or len(set(pairs)) != len(rows):
        raise TickerOverviewSourceProfileError("ticker overview lifecycle keys are duplicated")
    for item in rows:
        if (
            not isinstance(item["lifecycle_id"], str)
            or not isinstance(item["ticker"], str)
            or item["identity_type"] not in {*_IDENTITY_FIELDS, "ticker"}
            or not isinstance(item["identity_value"], str)
            or not item["first_active_date"] <= item["last_active_date"]
            or item["query_date"] != item["last_active_date"]
        ):
            raise TickerOverviewSourceProfileError("ticker overview lifecycle row is invalid")
    return rows


def _request_pairs(content: bytes, *, start: date, end: date) -> tuple[tuple[str, date], ...]:
    try:
        reader = csv.DictReader(io.StringIO(content.decode("utf-8"), newline=""))
        if reader.fieldnames != ["ticker", "query_date"]:
            raise ValueError("header")
        rows = tuple((row["ticker"], date.fromisoformat(row["query_date"])) for row in reader)
    except (UnicodeDecodeError, KeyError, TypeError, ValueError) as exc:
        raise TickerOverviewSourceProfileError(
            "ticker overview requests receipt is invalid"
        ) from exc
    if len(rows) != len(set(rows)) or any(
        not ticker or ticker != ticker.strip() or not start <= when <= end for ticker, when in rows
    ):
        raise TickerOverviewSourceProfileError("ticker overview requests receipt rows are invalid")
    return rows


def _load_manifest(root: Path, relative: str) -> tuple[dict[str, object], dict[str, object]]:
    content = safe_relative_path(root, relative).read_bytes()
    return _json_object(content, "materialized manifest"), {
        "bytes": len(content),
        "path": relative,
        "sha256": hashlib.sha256(content).hexdigest(),
    }


def _read_bound_file(
    root: Path, relative: str, *, expected_sha256: str
) -> tuple[bytes, dict[str, object]]:
    content = safe_relative_path(root, relative).read_bytes()
    digest = hashlib.sha256(content).hexdigest()
    if digest != expected_sha256:
        raise TickerOverviewSourceProfileError(f"bound file checksum changed: {relative}")
    return content, {"bytes": len(content), "path": relative, "sha256": digest}


def _manifest_output_sha(manifest: dict[str, object], path: str) -> str:
    outputs = manifest.get("outputs")
    if not isinstance(outputs, list):
        raise TickerOverviewSourceProfileError("materialized manifest outputs are invalid")
    matches = [item for item in outputs if isinstance(item, dict) and item.get("path") == path]
    if len(matches) != 1 or not isinstance(matches[0].get("sha256"), str):
        raise TickerOverviewSourceProfileError("materialized manifest does not bind output")
    return str(matches[0]["sha256"])


def _read_parquet_bytes(content: bytes, label: str) -> pa.Table:
    try:
        return pq.ParquetFile(pa.BufferReader(content)).read()
    except pa.ArrowException as exc:
        raise TickerOverviewSourceProfileError(f"cannot read {label} Parquet") from exc


def _json_object(content: bytes, label: str) -> dict[str, Any]:
    try:
        value = json.loads(
            content,
            parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)),
        )
    except (UnicodeDecodeError, ValueError) as exc:
        raise TickerOverviewSourceProfileError(f"{label} is invalid JSON") from exc
    if not isinstance(value, dict):
        raise TickerOverviewSourceProfileError(f"{label} must be an object")
    return value


def _utc_timestamp(value: object, label: str) -> datetime:
    if not isinstance(value, str):
        raise TickerOverviewSourceProfileError(f"ticker overview {label} is invalid")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise TickerOverviewSourceProfileError(f"ticker overview {label} is invalid") from exc
    if parsed.tzinfo is None:
        raise TickerOverviewSourceProfileError(f"ticker overview {label} is not timezone-aware")
    return parsed.astimezone(UTC)


def _iso_date(value: object, label: str) -> date:
    if not isinstance(value, str):
        raise TickerOverviewSourceProfileError(f"ticker overview {label} is invalid")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise TickerOverviewSourceProfileError(f"ticker overview {label} is invalid") from exc


def _clean_string(value: object) -> str | None:
    if value is None:
        return None
    clean = str(value).strip()
    return clean or None


def _require_production_identity(
    *,
    lifecycle_manifest_path: str,
    lifecycle_manifest_sha256: str,
    oracle_manifest_path: str,
    oracle_manifest_sha256: str,
    lifecycle_plan_path: str,
    start: date,
    end: date,
) -> None:
    if (
        lifecycle_manifest_path != PRODUCTION_LIFECYCLE_MANIFEST_PATH
        or lifecycle_manifest_sha256 != PRODUCTION_LIFECYCLE_MANIFEST_SHA256
        or oracle_manifest_path != PRODUCTION_ORACLE_MANIFEST_PATH
        or oracle_manifest_sha256 != PRODUCTION_ORACLE_MANIFEST_SHA256
        or lifecycle_plan_path != PRODUCTION_LIFECYCLE_PLAN_PATH
        or start != PRODUCTION_START
        or end != PRODUCTION_END
    ):
        raise TickerOverviewSourceProfileError("production ticker overview source identity changed")


__all__ = [
    "COVERAGE_RECEIPT_SCHEMA_VERSION",
    "PRODUCTION_COVERAGE_RECEIPT_NAMESPACE",
    "PRODUCTION_END",
    "PRODUCTION_LIFECYCLE_MANIFEST_PATH",
    "PRODUCTION_LIFECYCLE_PLAN_PATH",
    "PRODUCTION_ORACLE_MANIFEST_PATH",
    "PRODUCTION_START",
    "PRODUCTION_TICKER_OVERVIEW_COVERAGE",
    "PROFILE_SCHEMA_VERSION",
    "TickerOverviewCoverageExpectation",
    "TickerOverviewSourceProfileError",
    "accepted_coverage_receipt",
    "coverage_receipt_bytes",
    "lifecycle_plan_content",
    "profile_ticker_overview_source",
    "validate_ticker_overview_coverage_receipt",
]
