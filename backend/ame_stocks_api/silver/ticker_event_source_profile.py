"""Read-only S5 profiling and accepted formal-coverage receipt construction.

Ticker Events is unusual among the Massive Bronze datasets: the authoritative plan is a
receipt of Composite FIGIs, successful responses have one page each, and an HTTP 404 is a
reviewed terminal coverage outcome rather than a corrupt download.  This module turns that
mixed terminal state into one deterministic, Silver-readable coverage receipt.  Pilot ticker
requests are profiled but are never admitted to the formal receipt.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import re
from collections import Counter, defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ame_stocks_api.artifacts import safe_relative_path
from ame_stocks_api.downloads.plan import build_download_plan
from ame_stocks_core import ProviderDataset

PROFILE_SCHEMA_VERSION = 1
COVERAGE_RECEIPT_SCHEMA_VERSION = 2

PRODUCTION_FORMAL_IDENTIFIER_RECEIPT_PATH = "manifests/plans/ticker_events/identifiers.txt"
PRODUCTION_FORMAL_IDENTIFIER_RECEIPT_SHA256 = (
    "c0386e3a19c5fadb5a976052ebc964e72836b3b60644e842a740d8e6dcdfd312"
)
PRODUCTION_PILOT_IDENTIFIER_RECEIPT_PATH = (
    "manifests/plans/ticker_events/audit-only-pilot-identifiers.txt"
)
PRODUCTION_PILOT_IDENTIFIER_RECEIPT_SHA256 = (
    "7224f77455ec463e5f8a3cb2856e91908bfccde3496b49781890613b1a3fa0a8"
)
PRODUCTION_REQUEST_START = date(2003, 9, 10)
PRODUCTION_REQUEST_END = date(2026, 7, 9)
PRODUCTION_COVERAGE_RECEIPT_NAMESPACE = "manifests/silver/source-coverage/ticker_events"

_MANIFEST_PREFIX = "manifests/massive/ticker_events"
_ARTIFACT_PREFIX = "bronze/massive/ticker_events"
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_FIGI = re.compile(r"^BBG[0-9A-Z]{9}$")
_CIK = re.compile(r"^[0-9]{10}$")
_COMPLETE_MANIFEST_KEYS = frozenset(
    {
        "artifacts",
        "checkpoint",
        "completed_at",
        "created_at",
        "dataset",
        "manifest_schema_version",
        "provider",
        "provider_contract_version",
        "provider_version",
        "request",
        "request_id",
        "status",
        "updated_at",
    }
)
_FAILED_MANIFEST_KEYS = frozenset((_COMPLETE_MANIFEST_KEYS - {"completed_at"}) | {"failure"})
_REQUEST_KEYS = frozenset({"adjusted", "asset_ids", "dataset", "end", "parameters", "start"})
_ARTIFACT_KEYS = frozenset(
    {
        "compressed_bytes",
        "content_type",
        "is_last",
        "next_continuation",
        "path",
        "raw_bytes",
        "raw_sha256",
        "record_count",
        "sequence",
        "stored_sha256",
    }
)
_FAILURE_KEYS = frozenset({"error_type", "message", "provider_status_code"})
_RESPONSE_KEYS = frozenset({"request_id", "results", "status"})
_EVENT_KEYS = frozenset({"date", "ticker_change", "type"})
_TICKER_CHANGE_KEYS = frozenset({"ticker"})


class TickerEventSourceProfileError(ValueError):
    """Raised when the formal/pilot scope or any bound Bronze byte is unsafe."""


@dataclass(frozen=True, slots=True)
class TickerEventCoverageExpectation:
    """Frozen cardinalities that distinguish reviewed coverage from a partial backfill."""

    formal_identifiers: int
    formal_complete: int
    formal_not_found_404: int
    pilot_identifiers: int
    pilot_complete: int
    pilot_not_found_404: int
    formal_events: int | None = None
    formal_blank_targets: int | None = None
    formal_sentinel_dates: int | None = None
    formal_coverage_floor_dates: int | None = None
    formal_after_declared_end_dates: int | None = None

    def __post_init__(self) -> None:
        for name in (
            "formal_identifiers",
            "formal_complete",
            "formal_not_found_404",
            "pilot_identifiers",
            "pilot_complete",
            "pilot_not_found_404",
        ):
            value = getattr(self, name)
            if type(value) is not int or value < 0:
                raise TickerEventSourceProfileError(f"{name} must be a native nonnegative integer")
        if self.formal_complete + self.formal_not_found_404 != self.formal_identifiers:
            raise TickerEventSourceProfileError("formal coverage expectation does not reconcile")
        if self.pilot_complete + self.pilot_not_found_404 != self.pilot_identifiers:
            raise TickerEventSourceProfileError("pilot coverage expectation does not reconcile")
        for name in (
            "formal_events",
            "formal_blank_targets",
            "formal_sentinel_dates",
            "formal_coverage_floor_dates",
            "formal_after_declared_end_dates",
        ):
            value = getattr(self, name)
            if value is not None and (type(value) is not int or value < 0):
                raise TickerEventSourceProfileError(f"{name} must be a native nonnegative integer")

    def to_dict(self) -> dict[str, int | None]:
        return {
            "formal_after_declared_end_dates": self.formal_after_declared_end_dates,
            "formal_blank_targets": self.formal_blank_targets,
            "formal_complete": self.formal_complete,
            "formal_coverage_floor_dates": self.formal_coverage_floor_dates,
            "formal_events": self.formal_events,
            "formal_identifiers": self.formal_identifiers,
            "formal_not_found_404": self.formal_not_found_404,
            "formal_sentinel_dates": self.formal_sentinel_dates,
            "pilot_complete": self.pilot_complete,
            "pilot_identifiers": self.pilot_identifiers,
            "pilot_not_found_404": self.pilot_not_found_404,
        }


PRODUCTION_TICKER_EVENT_COVERAGE = TickerEventCoverageExpectation(
    formal_identifiers=15_173,
    formal_complete=11_471,
    formal_not_found_404=3_702,
    pilot_identifiers=100,
    pilot_complete=16,
    pilot_not_found_404=84,
    formal_events=13_088,
    formal_blank_targets=193,
    formal_sentinel_dates=766,
    formal_coverage_floor_dates=1_334,
    formal_after_declared_end_dates=1,
)


def profile_ticker_event_source(
    data_root: Path,
    *,
    formal_receipt_path: str = PRODUCTION_FORMAL_IDENTIFIER_RECEIPT_PATH,
    pilot_receipt_path: str = PRODUCTION_PILOT_IDENTIFIER_RECEIPT_PATH,
    expected: TickerEventCoverageExpectation = PRODUCTION_TICKER_EVENT_COVERAGE,
    request_start: date = PRODUCTION_REQUEST_START,
    request_end: date = PRODUCTION_REQUEST_END,
) -> dict[str, object]:
    """Fully verify formal and pilot manifests/pages without writing any file.

    The returned report has terminal status ``passed_with_warnings`` because four reviewed
    provider-quality classes remain visible: 1969 sentinels, coverage-floor baseline rows, one
    event after the declared planning end, and blank-target placeholders.  Structural drift,
    non-404 failures, checksum/count/envelope errors and formal FIGI mismatches raise instead.
    """

    if (
        type(request_start) is not date
        or type(request_end) is not date
        or request_start > request_end
    ):
        raise TickerEventSourceProfileError("ticker-event request window is invalid")
    root = data_root.expanduser().resolve()
    formal_receipt = _load_identifier_receipt(root, formal_receipt_path, formal=True)
    pilot_receipt = _load_identifier_receipt(root, pilot_receipt_path, formal=False)
    if expected == PRODUCTION_TICKER_EVENT_COVERAGE and (
        formal_receipt_path != PRODUCTION_FORMAL_IDENTIFIER_RECEIPT_PATH
        or formal_receipt["sha256"] != PRODUCTION_FORMAL_IDENTIFIER_RECEIPT_SHA256
        or pilot_receipt_path != PRODUCTION_PILOT_IDENTIFIER_RECEIPT_PATH
        or pilot_receipt["sha256"] != PRODUCTION_PILOT_IDENTIFIER_RECEIPT_SHA256
    ):
        raise TickerEventSourceProfileError(
            "production ticker-event identifier receipt path/SHA changed"
        )
    formal_identifiers = tuple(formal_receipt["identifiers"])
    pilot_identifiers = tuple(pilot_receipt["identifiers"])
    if set(formal_identifiers) & set(pilot_identifiers):
        raise TickerEventSourceProfileError("formal and pilot identifier receipts overlap")

    plans = {
        "formal": build_download_plan(
            dataset=ProviderDataset.TICKER_EVENTS,
            start=request_start,
            end=request_end,
            tickers=formal_identifiers,
        ),
        "pilot": build_download_plan(
            dataset=ProviderDataset.TICKER_EVENTS,
            start=request_start,
            end=request_end,
            tickers=pilot_identifiers,
        ),
    }
    planned = {
        scope: {request.request_id: request for request in plan.requests}
        for scope, plan in plans.items()
    }
    if set(planned["formal"]) & set(planned["pilot"]):
        raise TickerEventSourceProfileError("formal and pilot request IDs overlap")

    actual_manifest_paths = {
        path.stem: path for path in sorted((root / _MANIFEST_PREFIX).glob("*.json"))
    }
    expected_request_ids = set(planned["formal"]) | set(planned["pilot"])
    if set(actual_manifest_paths) != expected_request_ids:
        missing = len(expected_request_ids - set(actual_manifest_paths))
        extra = len(set(actual_manifest_paths) - expected_request_ids)
        raise TickerEventSourceProfileError(
            f"ticker-event manifest coverage changed: missing={missing}, extra={extra}"
        )

    scope_counts = {scope: Counter() for scope in ("formal", "pilot")}
    formal_manifest_refs: list[dict[str, object]] = []
    formal_artifacts: list[dict[str, object]] = []
    manifest_inventory_lines: list[str] = []
    artifact_inventory_lines: list[str] = []
    result_keysets = {scope: Counter() for scope in ("formal", "pilot")}
    event_keysets = {scope: Counter() for scope in ("formal", "pilot")}
    ticker_change_keysets = {scope: Counter() for scope in ("formal", "pilot")}
    formal_dates = Counter()
    formal_targets: defaultdict[str, set[str]] = defaultdict(set)
    formal_ticker_figis: defaultdict[str, set[str]] = defaultdict(set)
    formal_ticker_dates: defaultdict[str, set[str]] = defaultdict(set)
    formal_date_ticker_figis: defaultdict[tuple[str, str], set[str]] = defaultdict(set)
    formal_figi_date_targets: defaultdict[tuple[str, str], set[str]] = defaultdict(set)
    formal_semantic_keys: Counter[tuple[str, str, str, str]] = Counter()
    formal_cik = Counter()
    failure_message_digests: set[str] = set()
    capture_created: list[str] = []
    capture_updated: list[str] = []
    capture_completed: list[str] = []
    referenced_artifacts: set[str] = set()

    for scope in ("formal", "pilot"):
        for request_id, request in sorted(planned[scope].items()):
            path = actual_manifest_paths[request_id]
            expected_request = request.canonical_dict()
            verified = _verify_manifest_and_payload(
                root,
                path,
                request_id=request_id,
                expected_request=expected_request,
                scope=scope,
            )
            status = str(verified["status"])
            scope_counts[scope][status] += 1
            scope_counts[scope]["events"] += int(verified["event_count"])
            scope_counts[scope]["artifacts"] += int(verified["artifact"] is not None)
            failure_digest = verified.get("failure_message_sha256")
            if isinstance(failure_digest, str):
                failure_message_digests.add(failure_digest)
            capture_created.append(str(verified["created_at"]))
            capture_updated.append(str(verified["updated_at"]))
            if verified["completed_at"] is not None:
                capture_completed.append(str(verified["completed_at"]))
            if status == "complete":
                result_keysets[scope][tuple(verified["result_keys"])] += 1
            for keyset, count in verified["event_keysets"].items():
                event_keysets[scope][keyset] += count
            for keyset, count in verified["ticker_change_keysets"].items():
                ticker_change_keysets[scope][keyset] += count

            manifest_ref = {
                "artifact": verified["artifact"],
                "completed_at": verified["completed_at"],
                "created_at": verified["created_at"],
                "event_count": verified["event_count"],
                "identifier": request.asset_ids[0],
                "path": verified["manifest_path"],
                "request_id": request_id,
                "sha256": verified["manifest_sha256"],
                "status": status,
                "updated_at": verified["updated_at"],
            }
            manifest_inventory_lines.append(
                f"{scope}\t{request_id}\t{verified['manifest_path']}\t"
                f"{verified['manifest_bytes']}\t{verified['manifest_sha256']}\t{status}\n"
            )
            artifact = verified["artifact"]
            if isinstance(artifact, dict):
                artifact_path = str(artifact["path"])
                if artifact_path in referenced_artifacts:
                    raise TickerEventSourceProfileError(
                        "ticker-event artifact is bound by multiple manifests"
                    )
                referenced_artifacts.add(artifact_path)
                artifact_inventory_lines.append(
                    f"{scope}\t{artifact_path}\t{artifact['sha256']}\t{artifact['bytes']}\t"
                    f"{artifact['raw_sha256']}\t{artifact['raw_bytes']}\t"
                    f"{artifact['row_count']}\n"
                )
            if scope == "formal":
                formal_manifest_refs.append(manifest_ref)
                if isinstance(artifact, dict):
                    formal_artifacts.append(dict(artifact))
                if any(str(event["target"]) == "" for event in verified["events"]):
                    scope_counts[scope]["responses_with_blank_target"] += 1
                    scope_counts[scope]["valid_siblings_in_blank_target_responses"] += sum(
                        str(event["target"]) != "" for event in verified["events"]
                    )
                for event in verified["events"]:
                    event_date = str(event["date"])
                    target = str(event["target"])
                    figi = str(verified["returned_composite_figi"])
                    formal_dates[event_date] += 1
                    quality = _date_target_quality(
                        event_date,
                        target,
                        request_start=request_start,
                        request_end=request_end,
                    )
                    scope_counts[scope][f"quality:{quality}"] += 1
                    parsed = date.fromisoformat(event_date)
                    scope_counts[scope]["weekend_events"] += parsed.weekday() >= 5
                    scope_counts[scope]["weekend_blank_targets"] += (
                        parsed.weekday() >= 5 and target == ""
                    )
                    if target:
                        formal_targets[figi].add(target)
                        formal_ticker_figis[target].add(figi)
                        formal_ticker_dates[target].add(event_date)
                        formal_date_ticker_figis[(event_date, target)].add(figi)
                        formal_figi_date_targets[(figi, event_date)].add(target)
                        formal_semantic_keys[(figi, event_date, "ticker_change", target)] += 1
                    if event_date == "2023-11-18":
                        scope_counts[scope]["date_2023_11_18_events"] += 1
                        scope_counts[scope]["date_2023_11_18_blank_targets"] += target == ""
                # CIK coverage is a property of returned issuer identities. A stable
                # 404 has no response identity, so counting it as a missing CIK would
                # conflate endpoint coverage with field completeness.
                if isinstance(artifact, dict):
                    if verified["returned_cik"] is None:
                        formal_cik["missing"] += 1
                    else:
                        formal_cik["present"] += 1

    actual_artifacts = {
        path.relative_to(root).as_posix()
        for path in (root / _ARTIFACT_PREFIX).glob("request_id=*/page-*.json.gz")
    }
    if actual_artifacts != referenced_artifacts:
        raise TickerEventSourceProfileError(
            "ticker-event Bronze artifact coverage has missing or orphan pages"
        )
    if len(failure_message_digests) != 1:
        raise TickerEventSourceProfileError("stable ticker-event 404 receipts changed message")
    duplicate_semantic_groups = sum(count > 1 for count in formal_semantic_keys.values())
    if duplicate_semantic_groups:
        raise TickerEventSourceProfileError(
            "formal ticker-event responses repeat a FIGI/date/type/target key"
        )

    _require_expected_counts(scope_counts, expected)
    date_quality = {
        key.removeprefix("quality:"): value
        for key, value in sorted(scope_counts["formal"].items())
        if key.startswith("quality:")
    }
    same_figi_date_multi = {
        f"{figi}|{event_date}": sorted(targets)
        for (figi, event_date), targets in sorted(formal_figi_date_targets.items())
        if len(targets) > 1
    }
    ticker_reuse = {
        ticker: figis
        for (_event_date, ticker), figis in formal_date_ticker_figis.items()
        if len(figis) > 1
    }
    diagnostic = {
        "cik_coverage": dict(sorted(formal_cik.items())),
        "date_2023_11_18": {
            "blank_target_events": scope_counts["formal"]["date_2023_11_18_blank_targets"],
            "nonblank_target_events": (
                scope_counts["formal"]["date_2023_11_18_events"]
                - scope_counts["formal"]["date_2023_11_18_blank_targets"]
            ),
            "total_events": scope_counts["formal"]["date_2023_11_18_events"],
        },
        "date_min": min(formal_dates),
        "date_max": max(formal_dates),
        "date_quality": date_quality,
        "event_count_distribution": _event_count_distribution(formal_manifest_refs),
        "figi_distinct_target_count_distribution": dict(
            sorted(Counter(len(values) for values in formal_targets.values()).items())
        ),
        "figi_with_multiple_tickers": sum(len(values) > 1 for values in formal_targets.values()),
        "same_figi_same_date_multi_ticker_groups": len(same_figi_date_multi),
        "same_figi_same_date_multi_ticker_samples": same_figi_date_multi,
        "semantic_duplicate_groups": duplicate_semantic_groups,
        "responses_with_blank_target": scope_counts["formal"]["responses_with_blank_target"],
        "ticker_reused_across_dates": sum(
            len(values) > 1 for values in formal_ticker_dates.values()
        ),
        "ticker_reused_multiple_figis": sum(
            len(values) > 1 for values in formal_ticker_figis.values()
        ),
        "ticker_reused_same_date_across_figis_groups": len(ticker_reuse),
        "ticker_reused_same_date_across_figis_excess": sum(
            len(values) - 1 for values in ticker_reuse.values()
        ),
        "valid_siblings_in_blank_target_responses": scope_counts["formal"][
            "valid_siblings_in_blank_target_responses"
        ],
        "weekend_blank_target_events": scope_counts["formal"]["weekend_blank_targets"],
        "weekend_events": scope_counts["formal"]["weekend_events"],
    }
    source = {
        "artifact_inventory_definition": (
            "sorted scope\\tpath\\tstored_sha256\\tbytes\\traw_sha256\\traw_bytes\\trow_count\\n"
        ),
        "artifact_inventory_entries": len(artifact_inventory_lines),
        "artifact_inventory_sha256": _digest_lines(artifact_inventory_lines),
        "capture": {
            "completed_at_max": max(capture_completed),
            "completed_at_min": min(capture_completed),
            "created_at_max": max(capture_created),
            "created_at_min": min(capture_created),
            "updated_at_max": max(capture_updated),
            "updated_at_min": min(capture_updated),
        },
        "manifest_inventory_definition": (
            "sorted scope\\trequest_id\\tpath\\tbytes\\tsha256\\tstatus\\n"
        ),
        "manifest_inventory_entries": len(manifest_inventory_lines),
        "manifest_inventory_sha256": _digest_lines(manifest_inventory_lines),
    }
    field_shapes = {
        "event_keys": _render_keysets(event_keysets),
        "response_keys": {
            "formal": {str(tuple(sorted(_RESPONSE_KEYS))): expected.formal_complete},
            "pilot": {str(tuple(sorted(_RESPONSE_KEYS))): expected.pilot_complete},
        },
        "results_keys": _render_keysets(result_keysets),
        "ticker_change_keys": _render_keysets(ticker_change_keysets),
    }
    receipt = _coverage_receipt(
        formal_receipt=formal_receipt,
        pilot_receipt=pilot_receipt,
        expectation=expected,
        request_start=request_start,
        request_end=request_end,
        formal_manifest_refs=formal_manifest_refs,
        formal_artifacts=formal_artifacts,
        scope_counts=scope_counts,
        diagnostic=diagnostic,
    )
    report: dict[str, object] = {
        "accepted_coverage_receipt": receipt,
        "field_shapes": field_shapes,
        "hard_gate_counts": {
            "artifact_coverage_mismatch": 0,
            "checksum_or_byte_mismatch": 0,
            "envelope_or_schema_drift": 0,
            "formal_figi_mismatch": 0,
            "manifest_coverage_mismatch": 0,
            "non_404_failure": 0,
            "semantic_duplicate": 0,
        },
        "profile_schema_version": PROFILE_SCHEMA_VERSION,
        "source": source,
        "status": "passed_with_warnings",
    }
    preimage = _canonical_json(report)
    report["profile_sha256"] = hashlib.sha256(preimage).hexdigest()
    report["profile_sha256_preimage"] = "canonical JSON without profile_sha256 fields"
    return report


def accepted_coverage_receipt(profile: Mapping[str, object]) -> dict[str, object]:
    """Return a detached, validated accepted receipt from a full source profile."""

    if profile.get("status") != "passed_with_warnings":
        raise TickerEventSourceProfileError("ticker-event profile is not accepted")
    gates = profile.get("hard_gate_counts")
    if not isinstance(gates, Mapping) or any(value != 0 for value in gates.values()):
        raise TickerEventSourceProfileError("ticker-event profile hard gates are not all zero")
    receipt = profile.get("accepted_coverage_receipt")
    validated = validate_ticker_event_coverage_receipt(receipt)
    return json.loads(json.dumps(validated, allow_nan=False, sort_keys=True))


def coverage_receipt_bytes(receipt: Mapping[str, object]) -> bytes:
    """Serialize a validated receipt exactly as an immutable Silver JSON document."""

    validated = validate_ticker_event_coverage_receipt(receipt)
    return _canonical_json(validated) + b"\n"


def validate_ticker_event_coverage_receipt(value: object) -> dict[str, object]:
    """Validate receipt identity, accepted status, counts and all formal bindings."""

    if not isinstance(value, Mapping):
        raise TickerEventSourceProfileError("ticker-event coverage receipt must be an object")
    document = dict(value)
    required = {
        "artifacts",
        "coverage_receipt_id",
        "coverage_receipt_schema_version",
        "diagnostics",
        "formal_counts",
        "formal_identifier_receipt",
        "formal_manifest_refs",
        "pilot_exclusion",
        "request_scope",
        "source_dataset",
        "status",
    }
    _exact_keys(document, required, "coverage receipt")
    if (
        document["coverage_receipt_schema_version"] != COVERAGE_RECEIPT_SCHEMA_VERSION
        or document["source_dataset"] != "ticker_events"
        or document["status"] != "passed_with_warnings"
    ):
        raise TickerEventSourceProfileError("ticker-event coverage receipt identity is invalid")
    formal = _mapping(document["formal_identifier_receipt"], "formal identifier receipt")
    pilot = _mapping(document["pilot_exclusion"], "pilot exclusion")
    counts = _mapping(document["formal_counts"], "formal counts")
    manifests = _array(document["formal_manifest_refs"], "formal manifest refs")
    artifacts = _array(document["artifacts"], "formal artifacts")
    if pilot.get("included_in_inventory") is not False:
        raise TickerEventSourceProfileError("pilot requests must be excluded from S5 inventory")
    if counts.get("identifiers") != len(manifests):
        raise TickerEventSourceProfileError("formal manifest refs do not reconcile to identifiers")
    complete = sum(
        isinstance(item, Mapping) and item.get("status") == "complete" for item in manifests
    )
    not_found = sum(
        isinstance(item, Mapping) and item.get("status") == "not_found_404" for item in manifests
    )
    if counts.get("complete") != complete or counts.get("not_found_404") != not_found:
        raise TickerEventSourceProfileError("formal terminal status counts do not reconcile")
    if counts.get("artifacts") != len(artifacts) or len(artifacts) != complete:
        raise TickerEventSourceProfileError("formal artifact count does not reconcile")
    _exact_keys(
        formal,
        {"bytes", "identifier_count", "path", "row_count", "sha256"},
        "formal identifier receipt",
    )
    _validate_binding(formal, artifact=False)
    formal_path = str(formal["path"])
    if formal_path != PRODUCTION_FORMAL_IDENTIFIER_RECEIPT_PATH:
        raise TickerEventSourceProfileError("formal identifier receipt path is not canonical")
    formal_candidate = Path(formal_path)
    if (
        formal_candidate.is_absolute()
        or formal_candidate.as_posix() != formal_path
        or any(part in {"", ".", ".."} for part in formal_candidate.parts)
    ):
        raise TickerEventSourceProfileError("formal identifier receipt path is not normalized")
    try:
        formal_candidate.relative_to("manifests/plans/ticker_events")
    except ValueError as exc:
        raise TickerEventSourceProfileError(
            "formal identifier receipt is outside the ticker_events plan namespace"
        ) from exc
    formal_bytes = _native_nonnegative_int(formal.get("bytes"), "formal identifier receipt bytes")
    formal_row_count = _native_nonnegative_int(
        formal.get("row_count"), "formal identifier receipt row_count"
    )
    formal_identifier_count = _native_nonnegative_int(
        formal.get("identifier_count"), "formal identifier receipt identifier_count"
    )
    if formal_bytes == 0:
        raise TickerEventSourceProfileError("formal identifier receipt cannot be empty")
    if formal_identifier_count != len(manifests):
        raise TickerEventSourceProfileError("formal identifier receipt count changed")
    if formal_row_count != formal_identifier_count:
        raise TickerEventSourceProfileError(
            "formal identifier receipt row count differs from identifier count"
        )
    seen_manifests: set[str] = set()
    seen_artifacts: set[str] = set()
    artifact_by_path: dict[str, Mapping[str, object]] = {}
    for raw in artifacts:
        item = _mapping(raw, "coverage artifact")
        _validate_binding(item, artifact=True)
        path = str(item["path"])
        if path in seen_artifacts:
            raise TickerEventSourceProfileError("coverage receipt repeats an artifact")
        seen_artifacts.add(path)
        artifact_by_path[path] = item
    manifest_artifacts: set[str] = set()
    request_ids: set[str] = set()
    for raw in manifests:
        item = _mapping(raw, "coverage manifest ref")
        _validate_binding(item, artifact=False)
        path = str(item["path"])
        request_id = _sha256_text(item.get("request_id"), "request_id")
        if path in seen_manifests or request_id in request_ids:
            raise TickerEventSourceProfileError("coverage receipt repeats a formal manifest")
        seen_manifests.add(path)
        request_ids.add(request_id)
        artifact = item.get("artifact")
        if item.get("status") == "complete":
            nested = _mapping(artifact, "complete manifest artifact")
            nested_path = str(nested.get("path"))
            if artifact_by_path.get(nested_path) != nested:
                raise TickerEventSourceProfileError(
                    "coverage manifest artifact differs from top-level binding"
                )
            manifest_artifacts.add(nested_path)
        elif item.get("status") == "not_found_404" and artifact is not None:
            raise TickerEventSourceProfileError("404 coverage ref cannot bind an artifact")
        else:
            if item.get("status") not in {"complete", "not_found_404"}:
                raise TickerEventSourceProfileError("coverage receipt has unaccepted status")
    if manifest_artifacts != seen_artifacts:
        raise TickerEventSourceProfileError("coverage artifact bindings are not one-to-one")
    receipt_id = document.pop("coverage_receipt_id")
    if receipt_id != hashlib.sha256(_canonical_json(document)).hexdigest():
        raise TickerEventSourceProfileError("ticker-event coverage receipt digest mismatch")
    document["coverage_receipt_id"] = receipt_id
    return document


def _verify_manifest_and_payload(
    root: Path,
    path: Path,
    *,
    request_id: str,
    expected_request: Mapping[str, object],
    scope: str,
) -> dict[str, Any]:
    try:
        content = path.read_bytes()
        document = json.loads(content, parse_constant=_reject_constant)
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise TickerEventSourceProfileError(f"cannot read ticker-event manifest: {path}") from exc
    if not isinstance(document, dict):
        raise TickerEventSourceProfileError("ticker-event manifest root must be an object")
    status = document.get("status")
    expected_keys = _COMPLETE_MANIFEST_KEYS if status == "complete" else _FAILED_MANIFEST_KEYS
    _exact_keys(document, expected_keys, "ticker-event manifest")
    if (
        path.name != f"{request_id}.json"
        or document.get("request_id") != request_id
        or document.get("dataset") != "ticker_events"
        or document.get("provider") != "massive"
        or document.get("manifest_schema_version") != 1
        or document.get("provider_contract_version") != "1.1"
        or document.get("provider_version") != "1.2.0"
        or document.get("checkpoint") is not None
    ):
        raise TickerEventSourceProfileError("ticker-event manifest identity/schema drift")
    request = _mapping(document.get("request"), "manifest request")
    _exact_keys(request, _REQUEST_KEYS, "ticker-event request")
    if request != dict(expected_request):
        raise TickerEventSourceProfileError("ticker-event manifest request differs from receipt")
    created = _utc_text(document.get("created_at"), "created_at")
    updated = _utc_text(document.get("updated_at"), "updated_at")
    if created[0] > updated[0]:
        raise TickerEventSourceProfileError("ticker-event manifest update precedes creation")
    relative_manifest = path.relative_to(root).as_posix()
    base = {
        "completed_at": None,
        "created_at": created[1],
        "event_count": 0,
        "events": (),
        "manifest_bytes": len(content),
        "manifest_path": relative_manifest,
        "manifest_sha256": hashlib.sha256(content).hexdigest(),
        "provider_request_id": None,
        "result_hash": None,
        "result_keys": (),
        "returned_cik": None,
        "returned_composite_figi": None,
        "returned_name": None,
        "status": status,
        "updated_at": updated[1],
        "artifact": None,
        "event_keysets": Counter(),
        "ticker_change_keysets": Counter(),
    }
    if status == "failed":
        artifacts = document.get("artifacts")
        failure = _mapping(document.get("failure"), "failure")
        _exact_keys(failure, _FAILURE_KEYS, "ticker-event failure")
        message = failure.get("message")
        if (
            artifacts != []
            or failure.get("error_type") != "MassiveRequestError"
            or failure.get("provider_status_code") != 404
            or not isinstance(message, str)
            or not message.strip()
        ):
            raise TickerEventSourceProfileError("ticker-event failure is not a stable HTTP 404")
        base["status"] = "not_found_404"
        base["failure_message_sha256"] = hashlib.sha256(message.encode()).hexdigest()
        return base
    if status != "complete":
        raise TickerEventSourceProfileError("ticker-event manifest has nonterminal status")
    completed = _utc_text(document.get("completed_at"), "completed_at")
    if not (created[0] <= completed[0] <= updated[0]):
        raise TickerEventSourceProfileError("ticker-event completion timestamp ordering changed")
    base["completed_at"] = completed[1]
    artifacts = document.get("artifacts")
    if not isinstance(artifacts, list) or len(artifacts) != 1:
        raise TickerEventSourceProfileError("complete ticker-event manifest must bind one page")
    artifact = _mapping(artifacts[0], "artifact")
    _exact_keys(artifact, _ARTIFACT_KEYS, "ticker-event artifact")
    expected_path = f"{_ARTIFACT_PREFIX}/request_id={request_id}/page-00000.json.gz"
    if (
        artifact.get("path") != expected_path
        or artifact.get("sequence") != 0
        or artifact.get("content_type") != "application/json"
        or artifact.get("is_last") is not True
        or artifact.get("next_continuation") is not None
    ):
        raise TickerEventSourceProfileError("ticker-event artifact structure drift")
    compressed_bytes = _native_nonnegative_int(artifact.get("compressed_bytes"), "compressed_bytes")
    raw_bytes = _native_nonnegative_int(artifact.get("raw_bytes"), "raw_bytes")
    row_count = _native_nonnegative_int(artifact.get("record_count"), "record_count")
    stored_sha = _sha256_text(artifact.get("stored_sha256"), "stored_sha256")
    raw_sha = _sha256_text(artifact.get("raw_sha256"), "raw_sha256")
    try:
        compressed = safe_relative_path(root, expected_path).read_bytes()
    except OSError as exc:
        raise TickerEventSourceProfileError("cannot read ticker-event Bronze artifact") from exc
    if len(compressed) != compressed_bytes or hashlib.sha256(compressed).hexdigest() != stored_sha:
        raise TickerEventSourceProfileError("ticker-event stored bytes/checksum mismatch")
    try:
        raw = gzip.decompress(compressed)
        response = json.loads(raw, parse_constant=_reject_constant)
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise TickerEventSourceProfileError("ticker-event artifact is not valid gzip JSON") from exc
    if len(raw) != raw_bytes or hashlib.sha256(raw).hexdigest() != raw_sha:
        raise TickerEventSourceProfileError("ticker-event raw bytes/checksum mismatch")
    if not isinstance(response, dict):
        raise TickerEventSourceProfileError("ticker-event response root must be an object")
    _exact_keys(response, _RESPONSE_KEYS, "ticker-event response")
    provider_request_id = response.get("request_id")
    if (
        response.get("status") != "OK"
        or not isinstance(provider_request_id, str)
        or not provider_request_id.strip()
        or provider_request_id != provider_request_id.strip()
    ):
        raise TickerEventSourceProfileError("ticker-event response envelope is invalid")
    results = _mapping(response.get("results"), "response results")
    allowed = {"name", "events", "cik", "composite_figi"}
    required = (
        {"name", "events", "composite_figi"} if scope == "formal" else {"name", "events", "cik"}
    )
    if not required <= set(results) or not set(results) <= allowed:
        raise TickerEventSourceProfileError("ticker-event results schema drift")
    name = results.get("name")
    if not isinstance(name, str) or not name.strip() or name != name.strip():
        raise TickerEventSourceProfileError("ticker-event result name is invalid")
    cik = results.get("cik")
    if cik is not None and (not isinstance(cik, str) or not _CIK.fullmatch(cik)):
        raise TickerEventSourceProfileError("ticker-event result CIK is invalid")
    returned_figi = results.get("composite_figi")
    requested_identifier = expected_request["asset_ids"][0]
    if scope == "formal" and returned_figi != requested_identifier:
        raise TickerEventSourceProfileError("ticker-event returned FIGI differs from request")
    if returned_figi is not None and (
        not isinstance(returned_figi, str) or not _FIGI.fullmatch(returned_figi)
    ):
        raise TickerEventSourceProfileError("ticker-event returned FIGI is invalid")
    events = results.get("events")
    if not isinstance(events, list) or not events:
        raise TickerEventSourceProfileError("ticker-event response events must be nonempty")
    normalized_events: list[dict[str, str]] = []
    event_keysets: Counter[tuple[str, ...]] = Counter()
    ticker_change_keysets: Counter[tuple[str, ...]] = Counter()
    seen: set[tuple[str, str, str]] = set()
    for event in events:
        item = _mapping(event, "ticker event")
        _exact_keys(item, _EVENT_KEYS, "ticker event")
        event_keysets[tuple(sorted(item))] += 1
        ticker_change = _mapping(item.get("ticker_change"), "ticker_change")
        _exact_keys(ticker_change, _TICKER_CHANGE_KEYS, "ticker_change")
        ticker_change_keysets[tuple(sorted(ticker_change))] += 1
        event_date = item.get("date")
        target = ticker_change.get("ticker")
        if item.get("type") != "ticker_change" or not isinstance(target, str):
            raise TickerEventSourceProfileError("ticker-event type/target schema is invalid")
        if target != target.strip():
            raise TickerEventSourceProfileError("ticker-event target has surrounding whitespace")
        try:
            if not isinstance(event_date, str):
                raise ValueError
            date.fromisoformat(event_date)
        except ValueError as exc:
            raise TickerEventSourceProfileError("ticker-event date is not ISO date") from exc
        key = (event_date, "ticker_change", target)
        if key in seen:
            raise TickerEventSourceProfileError("ticker-event response repeats an event key")
        seen.add(key)
        normalized_events.append(
            {
                "date": event_date,
                "source_event_hash": hashlib.sha256(_canonical_json(item)).hexdigest(),
                "target": target,
                "type": "ticker_change",
            }
        )
    if len(events) != row_count:
        raise TickerEventSourceProfileError("ticker-event response count differs from manifest")
    binding = {
        "bytes": compressed_bytes,
        "path": expected_path,
        "raw_bytes": raw_bytes,
        "raw_sha256": raw_sha,
        "row_count": row_count,
        "sha256": stored_sha,
    }
    base.update(
        {
            "artifact": binding,
            "event_count": len(events),
            "events": tuple(normalized_events),
            "provider_request_id": provider_request_id,
            "result_hash": hashlib.sha256(_canonical_json(results)).hexdigest(),
            "result_keys": tuple(sorted(results)),
            "returned_cik": cik,
            "returned_composite_figi": returned_figi,
            "returned_name": name,
            "event_keysets": event_keysets,
            "ticker_change_keysets": ticker_change_keysets,
        }
    )
    return base


def _coverage_receipt(
    *,
    formal_receipt: Mapping[str, object],
    pilot_receipt: Mapping[str, object],
    expectation: TickerEventCoverageExpectation,
    request_start: date,
    request_end: date,
    formal_manifest_refs: list[dict[str, object]],
    formal_artifacts: list[dict[str, object]],
    scope_counts: Mapping[str, Counter[str]],
    diagnostic: Mapping[str, object],
) -> dict[str, object]:
    document: dict[str, object] = {
        "artifacts": sorted(formal_artifacts, key=lambda item: str(item["path"])),
        "coverage_receipt_schema_version": COVERAGE_RECEIPT_SCHEMA_VERSION,
        "diagnostics": dict(diagnostic),
        "formal_counts": {
            "artifacts": scope_counts["formal"]["artifacts"],
            "complete": scope_counts["formal"]["complete"],
            "events": scope_counts["formal"]["events"],
            "identifiers": expectation.formal_identifiers,
            "not_found_404": scope_counts["formal"]["not_found_404"],
        },
        "formal_identifier_receipt": {
            "bytes": formal_receipt["bytes"],
            "identifier_count": formal_receipt["identifier_count"],
            "path": formal_receipt["path"],
            "row_count": formal_receipt["row_count"],
            "sha256": formal_receipt["sha256"],
        },
        "formal_manifest_refs": sorted(
            formal_manifest_refs, key=lambda item: str(item["request_id"])
        ),
        "pilot_exclusion": {
            "complete": scope_counts["pilot"]["complete"],
            "identifier_count": pilot_receipt["identifier_count"],
            "included_in_inventory": False,
            "not_found_404": scope_counts["pilot"]["not_found_404"],
            "path": pilot_receipt["path"],
            "reason": "audit-only ticker probes are not formal Composite-FIGI coverage",
            "sha256": pilot_receipt["sha256"],
        },
        "request_scope": {
            "adjusted": False,
            "end_label_not_provider_filter": request_end.isoformat(),
            "parameters": {"types": "ticker_change"},
            "start_label_not_provider_filter": request_start.isoformat(),
        },
        "source_dataset": "ticker_events",
        "status": "passed_with_warnings",
    }
    receipt_preimage = dict(document)
    document["coverage_receipt_id"] = hashlib.sha256(_canonical_json(receipt_preimage)).hexdigest()
    return document


def _load_identifier_receipt(root: Path, relative_path: str, *, formal: bool) -> dict[str, object]:
    path = safe_relative_path(root, relative_path)
    try:
        content = path.read_bytes()
        text = content.decode("utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        raise TickerEventSourceProfileError("cannot read ticker-event identifier receipt") from exc
    identifiers = tuple(
        line.partition("#")[0].strip()
        for line in text.splitlines()
        if line.partition("#")[0].strip()
    )
    if not identifiers or len(identifiers) != len(set(identifiers)):
        raise TickerEventSourceProfileError("identifier receipt is empty or has exact duplicates")
    if formal and any(not _FIGI.fullmatch(item) for item in identifiers):
        raise TickerEventSourceProfileError("formal ticker-event receipt must contain only FIGIs")
    return {
        "bytes": len(content),
        "casefold_duplicate_excess": len(identifiers)
        - len({item.casefold() for item in identifiers}),
        "identifier_count": len(identifiers),
        "identifiers": identifiers,
        "path": path.relative_to(root).as_posix(),
        "row_count": len(identifiers),
        "sha256": hashlib.sha256(content).hexdigest(),
    }


def _require_expected_counts(
    counts: Mapping[str, Counter[str]], expected: TickerEventCoverageExpectation
) -> None:
    observed = {
        "formal_identifiers": counts["formal"]["complete"] + counts["formal"]["not_found_404"],
        "formal_complete": counts["formal"]["complete"],
        "formal_not_found_404": counts["formal"]["not_found_404"],
        "pilot_identifiers": counts["pilot"]["complete"] + counts["pilot"]["not_found_404"],
        "pilot_complete": counts["pilot"]["complete"],
        "pilot_not_found_404": counts["pilot"]["not_found_404"],
        "formal_events": counts["formal"]["events"],
        "formal_blank_targets": counts["formal"]["quality:blank_target_placeholder"],
        "formal_sentinel_dates": counts["formal"]["quality:provider_sentinel_unknown_date"],
        "formal_coverage_floor_dates": counts["formal"]["quality:coverage_floor_baseline"],
        "formal_after_declared_end_dates": counts["formal"][
            "quality:after_declared_snapshot_boundary"
        ],
    }
    for key, expected_value in expected.to_dict().items():
        if expected_value is not None and observed[key] != expected_value:
            raise TickerEventSourceProfileError(
                f"ticker-event expected coverage changed for {key}: "
                f"expected={expected_value}, observed={observed[key]}"
            )


def _date_target_quality(
    event_date: str, target: str, *, request_start: date, request_end: date
) -> str:
    parsed = date.fromisoformat(event_date)
    if parsed == date(1969, 12, 31):
        return "provider_sentinel_unknown_date"
    if parsed == request_start:
        return "coverage_floor_baseline"
    if parsed > request_end:
        return "after_declared_snapshot_boundary"
    if target == "":
        return "blank_target_placeholder"
    return "valid_effective_date"


def _event_count_distribution(refs: list[dict[str, object]]) -> dict[str, int]:
    counter = Counter(int(item["event_count"]) for item in refs if item["status"] == "complete")
    return {str(key): value for key, value in sorted(counter.items())}


def _render_keysets(value: Mapping[str, Counter[tuple[str, ...]]]) -> dict[str, dict[str, int]]:
    return {
        scope: {"|".join(key): count for key, count in sorted(counter.items())}
        for scope, counter in value.items()
    }


def _validate_binding(value: Mapping[str, object], *, artifact: bool) -> None:
    path = value.get("path")
    sha = value.get("sha256")
    if not isinstance(path, str) or Path(path).is_absolute() or not path:
        raise TickerEventSourceProfileError("coverage binding path is invalid")
    _sha256_text(sha, "coverage binding sha256")
    if artifact:
        _native_nonnegative_int(value.get("bytes"), "coverage artifact bytes")
        _native_nonnegative_int(value.get("row_count"), "coverage artifact row_count")
        _native_nonnegative_int(value.get("raw_bytes"), "coverage artifact raw_bytes")
        _sha256_text(value.get("raw_sha256"), "coverage artifact raw_sha256")


def _mapping(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise TickerEventSourceProfileError(f"{label} must be an object")
    return dict(value)


def _array(value: object, label: str) -> list[Any]:
    if not isinstance(value, list):
        raise TickerEventSourceProfileError(f"{label} must be an array")
    return value


def _exact_keys(
    value: Mapping[str, object], expected: set[str] | frozenset[str], label: str
) -> None:
    actual = set(value)
    if actual != set(expected):
        raise TickerEventSourceProfileError(
            f"{label} keys drift: missing={sorted(set(expected) - actual)}, "
            f"extra={sorted(actual - set(expected))}"
        )


def _sha256_text(value: object, label: str) -> str:
    if not isinstance(value, str) or not _SHA256.fullmatch(value):
        raise TickerEventSourceProfileError(f"{label} must be a lowercase SHA-256")
    return value


def _native_nonnegative_int(value: object, label: str) -> int:
    if type(value) is not int or value < 0:
        raise TickerEventSourceProfileError(f"{label} must be a native nonnegative integer")
    return value


def _utc_text(value: object, label: str) -> tuple[datetime, str]:
    if not isinstance(value, str):
        raise TickerEventSourceProfileError(f"{label} must be an ISO timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise TickerEventSourceProfileError(f"{label} must be an ISO timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise TickerEventSourceProfileError(f"{label} must be timezone-aware")
    normalized = parsed.astimezone(UTC)
    return normalized, normalized.isoformat()


def _digest_lines(lines: list[str]) -> str:
    return hashlib.sha256("".join(sorted(lines)).encode()).hexdigest()


def _canonical_json(value: object) -> bytes:
    return json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True).encode()


def _reject_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON constant is forbidden: {value}")


__all__ = [
    "COVERAGE_RECEIPT_SCHEMA_VERSION",
    "PRODUCTION_COVERAGE_RECEIPT_NAMESPACE",
    "PRODUCTION_FORMAL_IDENTIFIER_RECEIPT_PATH",
    "PRODUCTION_FORMAL_IDENTIFIER_RECEIPT_SHA256",
    "PRODUCTION_PILOT_IDENTIFIER_RECEIPT_PATH",
    "PRODUCTION_PILOT_IDENTIFIER_RECEIPT_SHA256",
    "PRODUCTION_REQUEST_END",
    "PRODUCTION_REQUEST_START",
    "PRODUCTION_TICKER_EVENT_COVERAGE",
    "PROFILE_SCHEMA_VERSION",
    "TickerEventCoverageExpectation",
    "TickerEventSourceProfileError",
    "accepted_coverage_receipt",
    "coverage_receipt_bytes",
    "profile_ticker_event_source",
    "validate_ticker_event_coverage_receipt",
]
