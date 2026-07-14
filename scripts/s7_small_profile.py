"""Read-only S7 cross-source profile for lifecycle, S5, and S6 evidence."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path

import pyarrow.parquet as pq

ROOT = Path("/mnt/HC_Volume_106309665/american_stocks")
LIFECYCLE_PATH = ROOT / (
    "manifests/plans/ticker_overview/"
    "lifecycles-2016-07-11_2026-07-09.jsonl"
)
S5_STATUS_PATH = ROOT / (
    "silver/schema=v1/identity/ticker_event_request_status/"
    "build_id=7ff845634148274b61c2f515cb66cb9e94f8bb8a5e1abe47316343eaa9f22ca1/"
    "data/source_observed_date=2026-07-11/part-00000.parquet"
)
S5_EVENT_PATH = ROOT / (
    "silver/schema=v1/identity/ticker_change_event/"
    "build_id=7753688e3d4f19658ca5657b2dc5ccb9bf4c4b229b3c58dc68b255d5999735d2/"
    "data/source_capture_date=2026-07-11/part-00000.parquet"
)
S6_PATH = ROOT / (
    "silver/schema=v1/identity/ticker_overview_safe/"
    "build_id=f9e66da7f8aa86f9a2eacff4ee745874776f52d62182d3554d99c7f9b5b90ec0/"
    "data/source_capture_date=2026-07-11/part-00000.parquet"
)
S6_QUARANTINE_PATH = ROOT / (
    "silver/schema=v1/identity/ticker_overview_safe/"
    "build_id=f9e66da7f8aa86f9a2eacff4ee745874776f52d62182d3554d99c7f9b5b90ec0/"
    "quarantine/quarantine-record.parquet"
)


def parse_day(value: object) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def relation_profile(rows, left: str, right: str) -> dict[str, object]:
    mapping: dict[str, set[str]] = defaultdict(set)
    for row in rows:
        a = row.get(left)
        b = row.get(right)
        if a and b:
            mapping[str(a)].add(str(b))
    multi = {key: values for key, values in mapping.items() if len(values) > 1}
    examples = [
        {left: key, f"distinct_{right}": len(values), right: sorted(values)[:8]}
        for key, values in sorted(multi.items(), key=lambda item: (-len(item[1]), item[0]))[:8]
    ]
    return {
        f"distinct_{left}": len(mapping),
        f"{left}_to_multiple_{right}": len(multi),
        f"max_{right}_per_{left}": max((len(values) for values in mapping.values()), default=0),
        "examples": examples,
    }


lifecycles = [json.loads(line) for line in LIFECYCLE_PATH.read_text().splitlines()]
s5_status = pq.read_table(S5_STATUS_PATH).to_pylist()
s5_events = pq.read_table(S5_EVENT_PATH).to_pylist()
s6 = pq.read_table(S6_PATH).to_pylist()
s6_quarantine = pq.read_table(S6_QUARANTINE_PATH).to_pylist()

assert len(lifecycles) == 30_739
assert len(s5_status) == 15_173
assert len(s5_events) == 12_895
assert len(s6) == 30_570
assert len(s6_quarantine) == 169

for row in lifecycles:
    row["first_active_date"] = parse_day(row["first_active_date"])
    row["last_active_date"] = parse_day(row["last_active_date"])
    row["query_date"] = parse_day(row["query_date"])

lifecycle_ids = {row["lifecycle_id"] for row in lifecycles}
s6_ids = {row["lifecycle_id"] for row in s6}
assert len(lifecycle_ids) == len(lifecycles)
assert len(s6_ids) == len(s6)
unresolved_ids = lifecycle_ids - s6_ids
assert len(unresolved_ids) == len(s6_quarantine) == 169
unresolved = [row for row in lifecycles if row["lifecycle_id"] in unresolved_ids]

by_ticker: dict[str, list[dict[str, object]]] = defaultdict(list)
for row in lifecycles:
    by_ticker[str(row["ticker"])].append(row)

overlap_pairs = []
for ticker, rows in by_ticker.items():
    ordered = sorted(rows, key=lambda row: (row["first_active_date"], row["last_active_date"]))
    for index, left in enumerate(ordered):
        for right in ordered[index + 1 :]:
            if right["first_active_date"] > left["last_active_date"]:
                break
            overlap_pairs.append(
                {
                    "ticker": ticker,
                    "left_lifecycle_id": left["lifecycle_id"],
                    "left_first": left["first_active_date"].isoformat(),
                    "left_last": left["last_active_date"].isoformat(),
                    "left_identity": f"{left['identity_type']}:{left['identity_value']}",
                    "right_lifecycle_id": right["lifecycle_id"],
                    "right_first": right["first_active_date"].isoformat(),
                    "right_last": right["last_active_date"].isoformat(),
                    "right_identity": f"{right['identity_type']}:{right['identity_value']}",
                }
            )

def security_anchor(row: dict[str, object]) -> str | None:
    if row.get("share_class_figi"):
        return f"share_class_figi:{row['share_class_figi']}"
    if row.get("composite_figi"):
        return f"composite_figi:{row['composite_figi']}"
    return None


anchor_counts = Counter(
    "share_class_figi"
    if row.get("share_class_figi")
    else ("composite_figi_only" if row.get("composite_figi") else "no_security_figi")
    for row in lifecycles
)
unresolved_anchor_counts = Counter(
    "share_class_figi"
    if row.get("share_class_figi")
    else ("composite_figi_only" if row.get("composite_figi") else "no_security_figi")
    for row in unresolved
)

status_by_cik: dict[str, set[str]] = defaultdict(set)
for row in s5_status:
    if row.get("request_outcome") == "complete_timeline" and row.get("response_cik"):
        status_by_cik[str(row["response_cik"])].add(str(row["requested_identifier"]))

no_figi_unresolved_candidates = Counter()
no_figi_unresolved_examples = []
for row in unresolved:
    if security_anchor(row) is not None:
        continue
    candidates = status_by_cik.get(str(row.get("cik") or ""), set())
    bucket = "zero" if not candidates else ("one" if len(candidates) == 1 else "multiple")
    no_figi_unresolved_candidates[bucket] += 1
    no_figi_unresolved_examples.append(
        {
            "ticker": row["ticker"],
            "cik": row.get("cik"),
            "candidate_composite_figi_count": len(candidates),
            "candidate_composite_figi": sorted(candidates)[:8],
        }
    )

lifecycle_by_id = {row["lifecycle_id"]: row for row in lifecycles}
s6_cross_field_conflicts = Counter()
for row in s6:
    lifecycle = lifecycle_by_id[row["lifecycle_id"]]
    for field in ("share_class_figi", "composite_figi", "cik"):
        if lifecycle.get(field) and row.get(field) and lifecycle[field] != row[field]:
            s6_cross_field_conflicts[field] += 1

lifecycle_composite_pairs = {
    (str(row["composite_figi"]), str(row["ticker"]))
    for row in lifecycles
    if row.get("composite_figi")
}
lifecycle_composites = {pair[0] for pair in lifecycle_composite_pairs}
lifecycle_tickers = set(by_ticker)
pair_intervals: dict[tuple[str, str], list[tuple[date, date]]] = defaultdict(list)
for row in lifecycles:
    if row.get("composite_figi"):
        pair_intervals[(str(row["composite_figi"]), str(row["ticker"]))].append(
            (row["first_active_date"], row["last_active_date"])
        )

event_join_classes = Counter()
event_date_position = Counter()
event_examples: dict[str, list[dict[str, object]]] = defaultdict(list)
for row in s5_events:
    composite = str(row["response_composite_figi"])
    ticker = str(row["effective_ticker"])
    event_day = parse_day(row["event_date"])
    pair = (composite, ticker)
    if pair in lifecycle_composite_pairs:
        join_class = "exact_composite_ticker_lifecycle"
        intervals = pair_intervals[pair]
        if any(start <= event_day <= end for start, end in intervals):
            position = "inside_lifecycle_envelope"
        elif event_day < min(start for start, _ in intervals):
            position = "before_lifecycle_envelope"
        else:
            position = "after_lifecycle_envelope"
    elif composite in lifecycle_composites and ticker in lifecycle_tickers:
        join_class = "composite_and_ticker_exist_but_not_as_pair"
        position = "not_applicable"
    elif composite in lifecycle_composites:
        join_class = "composite_only"
        position = "not_applicable"
    elif ticker in lifecycle_tickers:
        join_class = "ticker_only"
        position = "not_applicable"
    else:
        join_class = "neither"
        position = "not_applicable"
    event_join_classes[join_class] += 1
    event_date_position[position] += 1
    if len(event_examples[join_class]) < 5:
        event_examples[join_class].append(
            {
                "composite_figi": composite,
                "ticker": ticker,
                "event_date": event_day.isoformat(),
                "event_date_quality": row["event_date_quality"],
            }
        )

request_join = Counter()
for row in s5_status:
    identifier = str(row["requested_identifier"])
    request_join[
        f"{row['request_outcome']}__"
        + ("seen_in_lifecycle" if identifier in lifecycle_composites else "absent_from_lifecycle")
    ] += 1

identity_groups: dict[tuple[str, str], set[str]] = defaultdict(set)
for row in lifecycles:
    identity_groups[(str(row["identity_type"]), str(row["identity_value"]))].add(
        str(row["ticker"])
    )
multi_ticker_identity = {
    key: tickers for key, tickers in identity_groups.items() if len(tickers) > 1
}

output = {
    "input_rows": {
        "lifecycle_plan": len(lifecycles),
        "s5_request_status": len(s5_status),
        "s5_ticker_change_event": len(s5_events),
        "s6_overview_data": len(s6),
        "s6_overview_quarantine": len(s6_quarantine),
    },
    "lifecycle": {
        "distinct_tickers": len(by_ticker),
        "identity_type_counts": dict(
            sorted(Counter(row["identity_type"] for row in lifecycles).items())
        ),
        "security_anchor_class_counts": dict(sorted(anchor_counts.items())),
        "unique_security_anchor_count": len(
            {anchor for row in lifecycles if (anchor := security_anchor(row)) is not None}
        ),
        "tickers_with_multiple_lifecycles": sum(len(rows) > 1 for rows in by_ticker.values()),
        "max_lifecycles_per_ticker": max(map(len, by_ticker.values())),
        "overlapping_lifecycle_envelope_pairs": len(overlap_pairs),
        "tickers_with_overlapping_envelopes": len({item["ticker"] for item in overlap_pairs}),
        "overlap_examples": overlap_pairs[:12],
        "primary_identity_keys_with_multiple_tickers": len(multi_ticker_identity),
        "primary_identity_multi_ticker_examples": [
            {
                "identity_type": key[0],
                "identity_value": key[1],
                "ticker_count": len(tickers),
                "tickers": sorted(tickers)[:12],
            }
            for key, tickers in sorted(
                multi_ticker_identity.items(), key=lambda item: (-len(item[1]), item[0])
            )[:12]
        ],
    },
    "identity_relations_on_lifecycles": {
        "share_class_to_composite": relation_profile(
            lifecycles, "share_class_figi", "composite_figi"
        ),
        "composite_to_share_class": relation_profile(
            lifecycles, "composite_figi", "share_class_figi"
        ),
        "share_class_to_cik": relation_profile(lifecycles, "share_class_figi", "cik"),
        "composite_to_cik": relation_profile(lifecycles, "composite_figi", "cik"),
        "cik_to_share_class": relation_profile(lifecycles, "cik", "share_class_figi"),
        "cik_to_composite": relation_profile(lifecycles, "cik", "composite_figi"),
    },
    "s6_coverage": {
        "data_lifecycle_ids": len(s6_ids),
        "unresolved_lifecycle_ids": len(unresolved_ids),
        "unresolved_identity_type_counts": dict(
            sorted(Counter(row["identity_type"] for row in unresolved).items())
        ),
        "unresolved_security_anchor_class_counts": dict(sorted(unresolved_anchor_counts.items())),
        "unresolved_without_security_figi_s5_cik_candidates": dict(
            sorted(no_figi_unresolved_candidates.items())
        ),
        "unresolved_without_security_figi_examples": no_figi_unresolved_examples[:12],
        "s6_comparable_cross_field_conflicts": dict(sorted(s6_cross_field_conflicts.items())),
    },
    "s5_to_lifecycle": {
        "request_outcome_by_lifecycle_composite_coverage": dict(sorted(request_join.items())),
        "event_join_classes": dict(sorted(event_join_classes.items())),
        "event_date_position_for_exact_pairs": dict(sorted(event_date_position.items())),
        "event_join_examples": dict(sorted(event_examples.items())),
    },
    "lifecycle_security_relation_profiles": {
        "ticker_to_share_class": relation_profile(lifecycles, "ticker", "share_class_figi"),
        "ticker_to_composite": relation_profile(lifecycles, "ticker", "composite_figi"),
        "ticker_to_cik": relation_profile(lifecycles, "ticker", "cik"),
        "share_class_to_ticker": relation_profile(lifecycles, "share_class_figi", "ticker"),
        "composite_to_ticker": relation_profile(lifecycles, "composite_figi", "ticker"),
    },
}

print(json.dumps(output, indent=2, sort_keys=True))
