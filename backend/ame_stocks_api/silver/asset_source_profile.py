"""Deterministic, read-only profiling for manifest-bound Massive assets Bronze data."""

from __future__ import annotations

import gzip
import hashlib
import json
import re
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ame_stocks_api.artifacts import safe_relative_path

PROFILE_SCHEMA_VERSION = 1
EXPECTED_FIELDS = (
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
IDENTITY_FIELDS = (
    "ticker",
    "name",
    "market",
    "locale",
    "primary_exchange",
    "type",
    "currency_name",
    "cik",
    "composite_figi",
    "share_class_figi",
    "active",
)
DOMAIN_FIELDS = ("active", "market", "locale", "currency_name", "primary_exchange", "type")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_UTC_TIMESTAMP = re.compile(
    r"^(\d{4}-\d{2}-\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})$"
)
_TICKER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9./^=$*+-]*$")
_EXPECTED_JSON_TYPES = {field: "string" for field in EXPECTED_FIELDS}
_EXPECTED_JSON_TYPES["active"] = "boolean"
_HARD_GATE_COUNTERS = (
    "active_inactive_overlap_group_instances",
    "active_request_mismatch",
    "compressed_bytes_mismatch",
    "delisted_after_capture",
    "delisted_invalid",
    "duplicate_identity_conflicts",
    "envelope_count_mismatch",
    "last_updated_after_capture",
    "last_updated_invalid",
    "manifest_record_count_mismatch",
    "page_status_non_ok",
    "provider_request_id_missing",
    "raw_bytes_mismatch",
    "raw_sha256_mismatch",
    "stored_sha256_mismatch",
    "ticker_empty",
    "ticker_format_invalid",
    "ticker_missing_or_wrong_type",
    "ticker_non_ascii",
    "ticker_trim_mismatch",
    "ticker_whitespace",
    "unexpected_source_field_rows",
)


class AssetSourceProfileError(ValueError):
    """Raised when a manifest-bound profile cannot be trusted."""


def profile_asset_source(
    data_root: Path,
    *,
    manifest_paths: Iterable[Path | str] | None = None,
    workers: int = 1,
    current_exchange_mics: Iterable[str] | None = None,
    current_ticker_types: Iterable[str] | None = None,
) -> dict[str, object]:
    """Read only the selected manifests and their pages and return deterministic JSON data.

    The function never creates an inventory, cache, temporary file, or report artifact.  Multiple
    workers split complete session pairs and merge counters, sets, and identity maps exactly.
    """

    if type(workers) is not int or workers < 1:
        raise AssetSourceProfileError("workers must be a positive integer")
    root = data_root.expanduser().resolve()
    manifests, source = _load_manifests(root, manifest_paths)
    by_session: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for manifest in manifests:
        by_session[manifest["session_date"]].append(manifest)
    sessions = [
        (key, tuple(sorted(value, key=lambda item: item["active"])))
        for key, value in sorted(by_session.items())
    ]
    chunks = [
        sessions[index :: min(workers, len(sessions) or 1)]
        for index in range(min(workers, len(sessions) or 1))
    ]
    payloads = [(str(root), chunk) for chunk in chunks if chunk]
    if len(payloads) == 1:
        partials = [_profile_chunk(payloads[0])]
    else:
        with ThreadPoolExecutor(max_workers=len(payloads)) as executor:
            partials = list(executor.map(_profile_chunk, payloads))
    merged = _merge_partials(partials)
    report = _finalize(
        source,
        merged,
        current_exchange_mics=(
            None if current_exchange_mics is None else set(current_exchange_mics)
        ),
        current_ticker_types=(None if current_ticker_types is None else set(current_ticker_types)),
    )
    preimage = json.dumps(report, allow_nan=False, separators=(",", ":"), sort_keys=True).encode()
    report["profile_sha256"] = hashlib.sha256(preimage).hexdigest()
    report["profile_sha256_preimage"] = (
        "canonical JSON of this report without profile_sha256 fields"
    )
    return report


def _load_manifests(
    root: Path,
    manifest_paths: Iterable[Path | str] | None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if manifest_paths is None:
        paths = sorted((root / "manifests/massive/assets").glob("*.json"))
    else:
        paths = sorted({_resolve_input_path(root, item) for item in manifest_paths})
    if not paths:
        raise AssetSourceProfileError("no assets manifests were selected")
    manifests: list[dict[str, Any]] = []
    manifest_lines: list[str] = []
    artifact_lines: list[str] = []
    seen_request_ids: set[str] = set()
    seen_artifacts: set[str] = set()
    summary = Counter()
    status = Counter()
    manifest_versions = Counter()
    provider_versions = Counter()
    contract_versions = Counter()
    created: list[str] = []
    completed: list[str] = []
    for path in paths:
        try:
            content = path.read_bytes()
            document = json.loads(content)
        except (OSError, json.JSONDecodeError) as exc:
            raise AssetSourceProfileError(f"cannot read assets manifest: {path}") from exc
        if not isinstance(document, dict):
            raise AssetSourceProfileError("assets manifest must be a JSON object")
        request_id = _required_sha(document.get("request_id"), "request_id")
        if request_id in seen_request_ids:
            raise AssetSourceProfileError("assets request IDs must be unique")
        seen_request_ids.add(request_id)
        request = document.get("request")
        if (
            document.get("dataset") != "assets"
            or document.get("provider") != "massive"
            or not isinstance(request, dict)
            or request.get("dataset") != "assets"
        ):
            raise AssetSourceProfileError("assets manifest request is missing or has wrong dataset")
        session_date = _required_text(request.get("start"), "request.start")
        try:
            date.fromisoformat(session_date)
        except ValueError as exc:
            raise AssetSourceProfileError("assets request date is not ISO date") from exc
        if request.get("end") != session_date:
            raise AssetSourceProfileError("assets request must cover exactly one date")
        parameters = request.get("parameters")
        if not isinstance(parameters, dict) or parameters.get("active") not in ("true", "false"):
            raise AssetSourceProfileError("assets request active parameter must be true or false")
        active = parameters["active"]
        artifacts = document.get("artifacts")
        if document.get("status") != "complete" or document.get("checkpoint") is not None:
            raise AssetSourceProfileError("assets manifest must be complete without a checkpoint")
        if not isinstance(artifacts, list) or not artifacts:
            raise AssetSourceProfileError("assets manifest must contain artifacts")
        normalized_artifacts: list[dict[str, Any]] = []
        for expected_sequence, artifact in enumerate(artifacts):
            if not isinstance(artifact, dict) or artifact.get("sequence") != expected_sequence:
                raise AssetSourceProfileError("assets artifact sequence is not contiguous")
            relative = _required_text(artifact.get("path"), "artifact.path")
            if relative in seen_artifacts:
                raise AssetSourceProfileError("assets artifact is bound by more than one manifest")
            seen_artifacts.add(relative)
            if not relative.startswith(f"bronze/massive/assets/request_id={request_id}/"):
                raise AssetSourceProfileError(
                    "assets artifact path is outside its request directory"
                )
            stored_sha = _required_sha(artifact.get("stored_sha256"), "stored_sha256")
            raw_sha = _required_sha(artifact.get("raw_sha256"), "raw_sha256")
            compressed_bytes = _native_nonnegative_int(
                artifact.get("compressed_bytes"), "compressed_bytes"
            )
            raw_bytes = _native_nonnegative_int(artifact.get("raw_bytes"), "raw_bytes")
            records = _native_nonnegative_int(artifact.get("record_count"), "record_count")
            normalized_artifacts.append(
                {
                    "path": relative,
                    "sequence": expected_sequence,
                    "stored_sha256": stored_sha,
                    "raw_sha256": raw_sha,
                    "compressed_bytes": compressed_bytes,
                    "raw_bytes": raw_bytes,
                    "record_count": records,
                    "is_last": artifact.get("is_last") is True,
                }
            )
            artifact_lines.append(
                f"{relative}\t{stored_sha}\t{raw_sha}\t{compressed_bytes}\t{raw_bytes}\t{records}\n"
            )
            summary["pages"] += 1
            summary["records"] += records
            summary["compressed_bytes"] += compressed_bytes
            summary["raw_bytes"] += raw_bytes
            summary[f"active_{active}_pages"] += 1
            summary[f"active_{active}_records"] += records
        if (
            sum(item["is_last"] for item in normalized_artifacts) != 1
            or not normalized_artifacts[-1]["is_last"]
        ):
            raise AssetSourceProfileError("assets manifest must have one final terminal page")
        relative_manifest = path.relative_to(root).as_posix()
        manifest_sha = hashlib.sha256(content).hexdigest()
        manifest_lines.append(
            f"{request_id}\t{relative_manifest}\t{len(content)}\t{manifest_sha}\n"
        )
        created_at = _required_text(document.get("created_at"), "created_at")
        completed_at = _required_text(document.get("completed_at"), "completed_at")
        created.append(created_at)
        completed.append(completed_at)
        status[str(document.get("status"))] += 1
        manifest_versions[str(document.get("manifest_schema_version"))] += 1
        provider_versions[str(document.get("provider_version"))] += 1
        contract_versions[str(document.get("provider_contract_version"))] += 1
        manifests.append(
            {
                "active": active,
                "artifacts": normalized_artifacts,
                "completed_at": completed_at,
                "manifest_path": relative_manifest,
                "request_id": request_id,
                "session_date": session_date,
            }
        )
    pairs = Counter((item["session_date"], item["active"]) for item in manifests)
    union_dates = sorted({item["session_date"] for item in manifests})
    exact_pairs = sum(pairs[(date, "true")] == pairs[(date, "false")] == 1 for date in union_dates)
    source = {
        "artifact_inventory_definition": (
            "sorted UTF-8 lines path\\tstored_sha256\\traw_sha256\\tcompressed_bytes"
            "\\traw_bytes\\trecord_count\\n"
        ),
        "artifact_inventory_entries": len(artifact_lines),
        "artifact_inventory_sha256": _digest_lines(artifact_lines),
        "capture": {
            "completed_at_max": max(completed),
            "completed_at_min": min(completed),
            "created_at_max": max(created),
            "created_at_min": min(created),
        },
        "contract_versions": dict(sorted(contract_versions.items())),
        "date_max": max(union_dates),
        "date_min": min(union_dates),
        "exact_active_inactive_pairs": exact_pairs,
        "missing_or_duplicate_active_inactive_pairs": len(union_dates) - exact_pairs,
        "manifest_count": len(manifests),
        "manifest_inventory_definition": (
            "sorted UTF-8 lines request_id\\troot-relative-path\\tmanifest_bytes"
            "\\tmanifest_sha256\\n"
        ),
        "manifest_inventory_entries": len(manifest_lines),
        "manifest_inventory_sha256": _digest_lines(manifest_lines),
        "manifest_schema_versions": dict(sorted(manifest_versions.items())),
        "provider_versions": dict(sorted(provider_versions.items())),
        "sessions": len(union_dates),
        "status_counts": dict(sorted(status.items())),
        **dict(sorted(summary.items())),
    }
    return manifests, source


def _profile_chunk(
    payload: tuple[str, list[tuple[str, tuple[dict[str, Any], ...]]]],
) -> dict[str, Any]:
    root = Path(payload[0])
    counters: Counter[str] = Counter()
    presence: Counter[str] = Counter()
    nulls: Counter[str] = Counter()
    empties: Counter[str] = Counter()
    types: Counter[tuple[str, str]] = Counter()
    domains = {field: Counter() for field in DOMAIN_FIELDS}
    distincts = {
        field: set()
        for field in (
            "ticker",
            "primary_exchange",
            "type",
            "cik",
            "composite_figi",
            "share_class_figi",
        )
    }
    history = {
        name: defaultdict(set)
        for name in (
            "ticker_figi",
            "ticker_cik",
            "ticker_share",
            "figi_ticker",
            "share_ticker",
            "cik_ticker",
        )
    }
    casefold_keys: set[str] = set()
    same_day_figi_ids: set[str] = set()
    same_day_share_ids: set[str] = set()
    daily: list[tuple[str, str, int, int, int]] = []
    duplicate_fields: Counter[tuple[str, ...]] = Counter()
    duplicate_sizes: Counter[int] = Counter()
    selection = Counter()
    timestamp_cache: dict[object, tuple[bool, str | None, int | None]] = {}
    unexpected_fields: set[str] = set()
    for session_date, manifests in payload[1]:
        day_tickers: dict[str, set[str]] = {}
        day_casefold: defaultdict[str, set[str]] = defaultdict(set)
        day_figi: defaultdict[str, set[str]] = defaultdict(set)
        day_share: defaultdict[str, set[str]] = defaultdict(set)
        day_ticker_identity: defaultdict[str, dict[str, set[str]]] = defaultdict(
            lambda: {"figi": set(), "cik": set(), "share": set()}
        )
        for manifest in manifests:
            active_flag = manifest["active"]
            expected_active = active_flag == "true"
            first: dict[str, tuple[dict[str, Any], int, int]] = {}
            duplicates: defaultdict[str, list[tuple[dict[str, Any], int, int]]] = defaultdict(list)
            tickers: set[str] = set()
            request_rows = 0
            capture_valid, _, capture_key = _parse_timestamp(
                manifest["completed_at"], timestamp_cache
            )
            if not capture_valid:
                raise AssetSourceProfileError("manifest completed_at is not a UTC timestamp")
            for artifact in manifest["artifacts"]:
                path = safe_relative_path(root, artifact["path"])
                compressed = path.read_bytes()
                counters["pages_read"] += 1
                if len(compressed) != artifact["compressed_bytes"]:
                    counters["compressed_bytes_mismatch"] += 1
                if hashlib.sha256(compressed).hexdigest() != artifact["stored_sha256"]:
                    counters["stored_sha256_mismatch"] += 1
                try:
                    raw = gzip.decompress(compressed)
                    document = json.loads(raw)
                except (OSError, json.JSONDecodeError) as exc:
                    raise AssetSourceProfileError(
                        f"cannot decode assets page: {artifact['path']}"
                    ) from exc
                if len(raw) != artifact["raw_bytes"]:
                    counters["raw_bytes_mismatch"] += 1
                if hashlib.sha256(raw).hexdigest() != artifact["raw_sha256"]:
                    counters["raw_sha256_mismatch"] += 1
                if not isinstance(document, dict) or not isinstance(document.get("results"), list):
                    raise AssetSourceProfileError("assets page results must be a list")
                rows = document["results"]
                if document.get("count") != len(rows):
                    counters["envelope_count_mismatch"] += 1
                if len(rows) != artifact["record_count"]:
                    counters["manifest_record_count_mismatch"] += 1
                if not isinstance(document.get("request_id"), str) or not document["request_id"]:
                    counters["provider_request_id_missing"] += 1
                if document.get("status") != "OK":
                    counters["page_status_non_ok"] += 1
                for row_ordinal, row in enumerate(rows):
                    if not isinstance(row, dict):
                        raise AssetSourceProfileError("assets rows must be JSON objects")
                    counters["rows_read"] += 1
                    request_rows += 1
                    row_unexpected = set(row) - set(EXPECTED_FIELDS)
                    unexpected_fields.update(row_unexpected)
                    counters["unexpected_source_field_rows"] += bool(row_unexpected)
                    for field, value in row.items():
                        presence[field] += 1
                        value_type = _json_type(value)
                        types[(field, value_type)] += 1
                        if value is None:
                            nulls[field] += 1
                        elif isinstance(value, str) and value == "":
                            empties[field] += 1
                        expected_type = _EXPECTED_JSON_TYPES.get(field)
                        if value is not None and expected_type and value_type != expected_type:
                            counters[f"wrong_native_type_{field}"] += 1
                    for field in DOMAIN_FIELDS:
                        domains[field][_domain_key(row.get(field, _MISSING))] += 1
                    ticker = row.get("ticker")
                    if not isinstance(ticker, str):
                        counters["ticker_missing_or_wrong_type"] += 1
                        continue
                    tickers.add(ticker)
                    distincts["ticker"].add(ticker)
                    day_casefold[ticker.casefold()].add(ticker)
                    if ticker != ticker.strip():
                        counters["ticker_trim_mismatch"] += 1
                    if ticker == "":
                        counters["ticker_empty"] += 1
                    if any(character.isspace() for character in ticker):
                        counters["ticker_whitespace"] += 1
                    if not ticker.isascii():
                        counters["ticker_non_ascii"] += 1
                    if not _TICKER_PATTERN.fullmatch(ticker):
                        counters["ticker_format_invalid"] += 1
                    if ticker.upper() != ticker:
                        counters["ticker_lowercase_rows"] += 1
                    name = row.get("name")
                    if isinstance(name, str) and name != name.strip():
                        counters["name_trim_mismatch"] += 1
                    market = row.get("market")
                    locale = row.get("locale")
                    currency = row.get("currency_name")
                    counters["market_scope_mismatch"] += market is not None and market != "stocks"
                    counters["locale_scope_mismatch"] += locale is not None and locale != "us"
                    counters["currency_scope_mismatch"] += (
                        currency is not None and currency != "usd"
                    )
                    if row.get("active") is not expected_active:
                        counters["active_request_mismatch"] += 1
                    locator = (row, artifact["sequence"], row_ordinal)
                    if ticker in first:
                        if not duplicates[ticker]:
                            duplicates[ticker].append(first[ticker])
                        duplicates[ticker].append(locator)
                    else:
                        first[ticker] = locator
                    for field in (
                        "primary_exchange",
                        "type",
                        "cik",
                        "composite_figi",
                        "share_class_figi",
                    ):
                        value = row.get(field)
                        if isinstance(value, str):
                            distincts[field].add(value)
                    figi = row.get("composite_figi")
                    cik = row.get("cik")
                    share = row.get("share_class_figi")
                    if isinstance(figi, str):
                        history["ticker_figi"][ticker].add(figi)
                        history["figi_ticker"][figi].add(ticker)
                        day_figi[figi].add(ticker)
                        day_ticker_identity[ticker]["figi"].add(figi)
                    if isinstance(cik, str):
                        history["ticker_cik"][ticker].add(cik)
                        history["cik_ticker"][cik].add(ticker)
                        day_ticker_identity[ticker]["cik"].add(cik)
                    if isinstance(share, str):
                        history["ticker_share"][ticker].add(share)
                        history["share_ticker"][share].add(ticker)
                        day_share[share].add(ticker)
                        day_ticker_identity[ticker]["share"].add(share)
                    _profile_times(
                        row, session_date, capture_key, counters, timestamp_cache, active_flag
                    )
            day_tickers[active_flag] = tickers
            for _ticker, candidates in duplicates.items():
                duplicate_sizes[len(candidates)] += 1
                canonical = [_canonical(candidate[0]) for candidate in candidates]
                keys = set().union(*(candidate[0] for candidate in candidates))
                differing = tuple(
                    sorted(
                        field
                        for field in keys
                        if len(
                            {
                                _canonical(candidate[0].get(field, _MISSING))
                                for candidate in candidates
                            }
                        )
                        > 1
                    )
                )
                duplicate_fields[differing] += 1
                counters["duplicate_groups"] += 1
                counters["duplicate_excess_rows"] += len(candidates) - 1
                identity_conflict = any(
                    len(
                        {
                            _canonical(candidate[0].get(field, _MISSING))
                            for candidate in candidates
                        }
                    )
                    > 1
                    for field in IDENTITY_FIELDS
                )
                allowed_difference_sets = {
                    (),
                    ("last_updated_utc",),
                    ("delisted_utc", "last_updated_utc"),
                }
                if len(set(canonical)) == 1:
                    selection["resolved_exact_duplicate"] += 1
                elif identity_conflict:
                    selection["unresolved_identity_conflict"] += 1
                elif differing not in allowed_difference_sets:
                    selection["unresolved_difference_set"] += 1
                else:
                    parsed = [
                        _parse_timestamp(item[0].get("last_updated_utc"), timestamp_cache)
                        for item in candidates
                    ]
                    valid_keys = [item[2] for item in parsed if item[0]]
                    if len(valid_keys) != len(candidates):
                        selection["unresolved_timestamp_missing_or_invalid"] += 1
                    elif valid_keys.count(max(valid_keys)) == 1:
                        selection["resolved_unique_latest_last_updated"] += 1
                    else:
                        selection["unresolved_timestamp_tie"] += 1
                if identity_conflict:
                    counters["duplicate_identity_conflicts"] += 1
            daily.append((session_date, active_flag, request_rows, len(tickers), len(duplicates)))
        true_tickers = day_tickers.get("true", set())
        false_tickers = day_tickers.get("false", set())
        overlap = true_tickers & false_tickers
        counters["active_inactive_overlap_group_instances"] += len(overlap)
        counters["complete_session_pairs"] += set(day_tickers) == {"true", "false"}
        for key, values in day_casefold.items():
            if len(values) > 1:
                counters["same_day_casefold_group_instances"] += 1
                casefold_keys.add(key)
        for key, values in day_figi.items():
            if len(values) > 1:
                counters["same_day_figi_multi_ticker_group_instances"] += 1
                same_day_figi_ids.add(key)
        for key, values in day_share.items():
            if len(values) > 1:
                counters["same_day_share_multi_ticker_group_instances"] += 1
                same_day_share_ids.add(key)
        for values in day_ticker_identity.values():
            counters["same_day_ticker_multi_figi_group_instances"] += len(values["figi"]) > 1
            counters["same_day_ticker_multi_cik_group_instances"] += len(values["cik"]) > 1
            counters["same_day_ticker_multi_share_group_instances"] += len(values["share"]) > 1
    return {
        "casefold_keys": casefold_keys,
        "counters": counters,
        "daily": daily,
        "distincts": distincts,
        "domains": domains,
        "duplicate_fields": duplicate_fields,
        "duplicate_sizes": duplicate_sizes,
        "empties": empties,
        "history": history,
        "nulls": nulls,
        "presence": presence,
        "same_day_figi_ids": same_day_figi_ids,
        "same_day_share_ids": same_day_share_ids,
        "selection": selection,
        "types": types,
        "unexpected_fields": unexpected_fields,
    }


def _merge_partials(partials: list[dict[str, Any]]) -> dict[str, Any]:
    result = partials[0]
    for partial in partials[1:]:
        for name in (
            "counters",
            "duplicate_fields",
            "duplicate_sizes",
            "empties",
            "nulls",
            "presence",
            "selection",
            "types",
        ):
            result[name].update(partial[name])
        result["daily"].extend(partial["daily"])
        for field, counter in partial["domains"].items():
            result["domains"][field].update(counter)
        for field, values in partial["distincts"].items():
            result["distincts"][field].update(values)
        for family, mapping in partial["history"].items():
            for key, values in mapping.items():
                result["history"][family][key].update(values)
        for name in (
            "casefold_keys",
            "same_day_figi_ids",
            "same_day_share_ids",
            "unexpected_fields",
        ):
            result[name].update(partial[name])
    return result


def _finalize(
    source: dict[str, Any],
    partial: dict[str, Any],
    *,
    current_exchange_mics: set[str] | None,
    current_ticker_types: set[str] | None,
) -> dict[str, object]:
    rows = partial["counters"]["rows_read"]
    fields = sorted(set(EXPECTED_FIELDS) | set(partial["presence"]))
    field_profile = {}
    for field in fields:
        present = partial["presence"][field]
        field_profile[field] = {
            "empty_string": partial["empties"][field],
            "explicit_null": partial["nulls"][field],
            "missing": rows - present,
            "present": present,
            "presence_rate": _rate(present, rows),
            "types": {
                kind: count
                for (name, kind), count in sorted(partial["types"].items())
                if name == field
            },
        }
    exchange_counts = partial["domains"]["primary_exchange"]
    type_counts = partial["domains"]["type"]
    reference = {
        "scope": "current-reference diagnostic only; not historical PIT decoding",
        "primary_exchange": _reference_coverage(exchange_counts, current_exchange_mics),
        "ticker_type": _reference_coverage(type_counts, current_ticker_types),
    }
    history = partial["history"]
    identity = {
        "casefold": {
            "distinct_collision_keys": len(partial["casefold_keys"]),
            "same_day_group_instances": partial["counters"]["same_day_casefold_group_instances"],
        },
        "lifetime": {
            "cik_with_multiple_tickers": _multi_count(history["cik_ticker"]),
            "composite_figi_with_multiple_tickers": _multi_count(history["figi_ticker"]),
            "share_class_figi_with_multiple_tickers": _multi_count(history["share_ticker"]),
            "ticker_with_multiple_cik": _multi_count(history["ticker_cik"]),
            "ticker_with_multiple_composite_figi": _multi_count(history["ticker_figi"]),
            "ticker_with_multiple_share_class_figi": _multi_count(history["ticker_share"]),
        },
        "same_day": {
            "composite_figi_multi_ticker_distinct_ids": len(partial["same_day_figi_ids"]),
            "composite_figi_multi_ticker_group_instances": partial["counters"][
                "same_day_figi_multi_ticker_group_instances"
            ],
            "share_figi_multi_ticker_distinct_ids": len(partial["same_day_share_ids"]),
            "share_figi_multi_ticker_group_instances": partial["counters"][
                "same_day_share_multi_ticker_group_instances"
            ],
            "ticker_multi_cik_group_instances": partial["counters"][
                "same_day_ticker_multi_cik_group_instances"
            ],
            "ticker_multi_composite_figi_group_instances": partial["counters"][
                "same_day_ticker_multi_figi_group_instances"
            ],
            "ticker_multi_share_figi_group_instances": partial["counters"][
                "same_day_ticker_multi_share_group_instances"
            ],
        },
    }
    gate_counts = dict(sorted(partial["counters"].items()))
    for name in _HARD_GATE_COUNTERS:
        gate_counts.setdefault(name, 0)
    gate_counts["currency_scope_mismatch"] = partial["counters"]["currency_scope_mismatch"]
    gate_counts["empty_string_total"] = sum(partial["empties"].values())
    gate_counts["explicit_null_total"] = sum(partial["nulls"].values())
    gate_counts["locale_scope_mismatch"] = partial["counters"]["locale_scope_mismatch"]
    gate_counts["market_scope_mismatch"] = partial["counters"]["market_scope_mismatch"]
    gate_counts["unexpected_field_count"] = len(partial["unexpected_fields"])
    for field in EXPECTED_FIELDS:
        gate_counts.setdefault(f"wrong_native_type_{field}", 0)
    return {
        "candidate_key": {
            "duplicate_differing_field_sets": {
                "|".join(fields): count
                for fields, count in sorted(partial["duplicate_fields"].items())
            },
            "duplicate_excess_rows": partial["counters"]["duplicate_excess_rows"],
            "duplicate_group_sizes": {
                str(size): count for size, count in sorted(partial["duplicate_sizes"].items())
            },
            "duplicate_groups": partial["counters"]["duplicate_groups"],
            "key": ["session_date", "active_request_flag", "case_sensitive_ticker"],
            "selection": dict(sorted(partial["selection"].items())),
        },
        "current_reference_diagnostic": reference,
        "distinct_values": {
            field: len(values) for field, values in sorted(partial["distincts"].items())
        },
        "domains": {
            field: dict(sorted(counter.items()))
            for field, counter in sorted(partial["domains"].items())
        },
        "field_profile": field_profile,
        "identity_diagnostic": identity,
        "integrity_and_gate_counts": dict(sorted(gate_counts.items())),
        "profile_schema_version": PROFILE_SCHEMA_VERSION,
        "source": source,
        "unexpected_fields": sorted(partial["unexpected_fields"]),
    }


def _profile_times(
    row: Mapping[str, object],
    session_date: str,
    capture_key: int | None,
    counters: Counter[str],
    cache: dict[object, tuple[bool, str | None, int | None]],
    active_flag: str,
) -> None:
    last_updated = row.get("last_updated_utc")
    valid, date_text, key = _parse_timestamp(last_updated, cache)
    if last_updated is None:
        counters["last_updated_missing"] += 1
    elif not valid:
        counters["last_updated_invalid"] += 1
    else:
        counters[f"last_updated_{_date_relation(date_text, session_date)}_session"] += 1
        counters["last_updated_after_capture"] += bool(
            capture_key is not None and key > capture_key
        )
    delisted = row.get("delisted_utc")
    if delisted is None:
        counters["delisted_missing"] += 1
        counters[f"delisted_missing_active_{active_flag}"] += 1
        return
    valid, date_text, delisted_key = _parse_timestamp(delisted, cache)
    if not valid:
        counters["delisted_invalid"] += 1
        return
    counters["delisted_present"] += 1
    counters[f"delisted_present_active_{active_flag}"] += 1
    counters[f"delisted_{_date_relation(date_text, session_date)}_session"] += 1
    counters["delisted_after_capture"] += bool(
        capture_key is not None and delisted_key > capture_key
    )
    if key is not None:
        relation = "after" if key > delisted_key else "before" if key < delisted_key else "equal"
        counters[f"last_updated_{relation}_delisted"] += 1


def _parse_timestamp(
    value: object,
    cache: dict[object, tuple[bool, str | None, int | None]],
) -> tuple[bool, str | None, int | None]:
    try:
        return cache[value]
    except (KeyError, TypeError):
        pass
    if not isinstance(value, str) or (match := _UTC_TIMESTAMP.fullmatch(value)) is None:
        result = (False, None, None)
    else:
        fraction = (match.group(5) or "").ljust(9, "0")
        try:
            zone = "+00:00" if match.group(6) == "Z" else match.group(6)
            parsed = datetime.fromisoformat(
                f"{match.group(1)}T{match.group(2)}:{match.group(3)}:{match.group(4)}{zone}"
            ).astimezone(UTC)
            seconds = int(parsed.timestamp())
            result = (
                True,
                parsed.date().isoformat(),
                seconds * 1_000_000_000 + int(fraction),
            )
        except ValueError:
            result = (False, None, None)
    with suppress(TypeError):
        cache[value] = result
    return result


def _reference_coverage(counter: Counter[str], domain: set[str] | None) -> dict[str, object]:
    if domain is None:
        return {"status": "not_requested"}
    nonmissing = {key: count for key, count in counter.items() if key != "__MISSING__"}
    matched = sum(count for key, count in nonmissing.items() if key in domain)
    unmatched = {key: count for key, count in sorted(nonmissing.items()) if key not in domain}
    return {
        "dictionary_values": sorted(domain),
        "matched_rows": matched,
        "nonmissing_rows": sum(nonmissing.values()),
        "unmatched": unmatched,
        "coverage_rate": _rate(matched, sum(nonmissing.values())),
    }


def _resolve_input_path(root: Path, value: Path | str) -> Path:
    path = Path(value).expanduser()
    resolved = path.resolve() if path.is_absolute() else safe_relative_path(root, path.as_posix())
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise AssetSourceProfileError("manifest path escaped data root") from exc
    return resolved


def _required_text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise AssetSourceProfileError(f"{label} must be a nonempty trimmed string")
    return value


def _required_sha(value: object, label: str) -> str:
    text = _required_text(value, label)
    if not _SHA256.fullmatch(text):
        raise AssetSourceProfileError(f"{label} is not a SHA-256")
    return text


def _native_nonnegative_int(value: object, label: str) -> int:
    if type(value) is not int or value < 0:
        raise AssetSourceProfileError(f"{label} must be a native nonnegative integer")
    return value


def _digest_lines(lines: list[str]) -> str:
    return hashlib.sha256("".join(sorted(lines)).encode()).hexdigest()


def _canonical(value: object) -> str:
    if value is _MISSING:
        return '"__MISSING__"'
    return json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True)


def _json_type(value: object) -> str:
    if value is None:
        return "null"
    if type(value) is bool:
        return "boolean"
    if type(value) is int:
        return "integer"
    if type(value) is float:
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return type(value).__name__


def _domain_key(value: object) -> str:
    if value is _MISSING:
        return "__MISSING__"
    if type(value) is bool:
        return str(value).lower()
    if isinstance(value, str):
        return value
    return _canonical(value)


def _date_relation(left: str | None, right: str) -> str:
    if left is None:
        return "invalid"
    return "after" if left > right else "before" if left < right else "on"


def _multi_count(mapping: Mapping[str, set[str]]) -> int:
    return sum(len(values) > 1 for values in mapping.values())


def _rate(numerator: int, denominator: int) -> float | None:
    return None if denominator == 0 else round(numerator / denominator, 12)


_MISSING = object()
