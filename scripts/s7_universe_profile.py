"""Read-only, bounded-memory profile of the exact published S4 universe source."""

from __future__ import annotations

import json
import resource
import statistics
from collections import Counter
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds

BUILD_ID = "21921c72c4be79665d41077664f8f027a1beb9ac0600ff4c6610d4f40638b185"
DATA_ROOT = Path(
    "/mnt/HC_Volume_106309665/american_stocks/"
    "silver/schema=v1/reference/universe_source_daily/"
    f"build_id={BUILD_ID}/data"
)
EXPECTED_ROWS = 69_376_329


def add_group_counts(
    table: pa.Table,
    keys: list[str],
    destination: Counter[tuple[object, ...]],
) -> None:
    grouped = table.group_by(keys).aggregate([("ticker", "count")])
    for row in grouped.to_pylist():
        destination[tuple(row[key] for key in keys)] += int(row["ticker_count"])


def update_unique(
    table: pa.Table,
    column: str,
    target: set[str],
    mask: pa.Array | None = None,
) -> None:
    values = table[column]
    if mask is not None:
        values = pc.filter(values, mask)
    target.update(str(value) for value in pc.unique(pc.drop_null(values)).to_pylist())


files = sorted(DATA_ROOT.glob("**/*.parquet"))
assert len(files) == 2_513, len(files)

dataset = ds.dataset([str(path) for path in files], format="parquet")
scanner = dataset.scanner(
    columns=[
        "session_date",
        "ticker",
        "active_on_date",
        "type_code",
        "cik",
        "composite_figi",
        "share_class_figi",
        "identity_link_status",
    ],
    batch_size=262_144,
    use_threads=True,
)

rows = 0
active_rows = 0
share_anchor_rows = 0
composite_only_rows = 0
no_security_anchor_rows = 0
active_share_anchor_rows = 0
active_composite_only_rows = 0
active_no_security_anchor_rows = 0

all_tickers: set[str] = set()
active_tickers: set[str] = set()
all_share_figis: set[str] = set()
active_share_figis: set[str] = set()
all_composite_figis: set[str] = set()
active_composite_figis: set[str] = set()
all_ciks: set[str] = set()
active_ciks: set[str] = set()

status_counts: Counter[tuple[object, ...]] = Counter()
type_counts: Counter[tuple[object, ...]] = Counter()
daily_total: Counter[object] = Counter()
daily_active: Counter[object] = Counter()
daily_active_missing_anchor: Counter[object] = Counter()

for batch in scanner.to_batches():
    table = pa.Table.from_batches([batch])
    batch_rows = len(table)
    rows += batch_rows

    active = table["active_on_date"]
    active_count = int(pc.sum(pc.cast(active, pa.int64())).as_py() or 0)
    active_rows += active_count

    share_present = pc.invert(pc.is_null(table["share_class_figi"]))
    composite_present = pc.invert(pc.is_null(table["composite_figi"]))
    composite_only = pc.and_(pc.invert(share_present), composite_present)
    no_security_anchor = pc.and_(pc.invert(share_present), pc.invert(composite_present))

    share_count = int(pc.sum(pc.cast(share_present, pa.int64())).as_py() or 0)
    composite_only_count = int(pc.sum(pc.cast(composite_only, pa.int64())).as_py() or 0)
    no_anchor_count = int(pc.sum(pc.cast(no_security_anchor, pa.int64())).as_py() or 0)
    share_anchor_rows += share_count
    composite_only_rows += composite_only_count
    no_security_anchor_rows += no_anchor_count
    active_share_anchor_rows += int(
        pc.sum(pc.cast(pc.and_(active, share_present), pa.int64())).as_py() or 0
    )
    active_composite_only_rows += int(
        pc.sum(pc.cast(pc.and_(active, composite_only), pa.int64())).as_py() or 0
    )
    active_no_security_anchor_rows += int(
        pc.sum(pc.cast(pc.and_(active, no_security_anchor), pa.int64())).as_py() or 0
    )

    update_unique(table, "ticker", all_tickers)
    update_unique(table, "ticker", active_tickers, active)
    update_unique(table, "share_class_figi", all_share_figis)
    update_unique(table, "share_class_figi", active_share_figis, active)
    update_unique(table, "composite_figi", all_composite_figis)
    update_unique(table, "composite_figi", active_composite_figis, active)
    update_unique(table, "cik", all_ciks)
    update_unique(table, "cik", active_ciks, active)

    add_group_counts(table, ["active_on_date", "identity_link_status"], status_counts)
    add_group_counts(table, ["active_on_date", "type_code"], type_counts)

    daily = table.append_column("active_int", pc.cast(active, pa.int64()))
    daily = daily.append_column(
        "active_missing_anchor_int",
        pc.cast(pc.and_(active, no_security_anchor), pa.int64()),
    )
    grouped_daily = daily.group_by(["session_date"]).aggregate(
        [
            ("ticker", "count"),
            ("active_int", "sum"),
            ("active_missing_anchor_int", "sum"),
        ]
    )
    for row in grouped_daily.to_pylist():
        session_date = row["session_date"]
        daily_total[session_date] += int(row["ticker_count"])
        daily_active[session_date] += int(row["active_int_sum"] or 0)
        daily_active_missing_anchor[session_date] += int(
            row["active_missing_anchor_int_sum"] or 0
        )

assert rows == EXPECTED_ROWS, rows
assert share_anchor_rows + composite_only_rows + no_security_anchor_rows == rows
assert (
    active_share_anchor_rows
    + active_composite_only_rows
    + active_no_security_anchor_rows
    == active_rows
)
assert len(daily_total) == 2_513, len(daily_total)


def distribution(values: list[int]) -> dict[str, int | float]:
    return {
        "min": min(values),
        "median": statistics.median(values),
        "max": max(values),
    }


result = {
    "input": {
        "build_id": BUILD_ID,
        "file_count": len(files),
        "row_count": rows,
        "session_count": len(daily_total),
        "min_session_date": str(min(daily_total)),
        "max_session_date": str(max(daily_total)),
    },
    "membership": {
        "active_rows": active_rows,
        "inactive_rows": rows - active_rows,
        "active_rate": active_rows / rows,
        "daily_total_rows": distribution(list(daily_total.values())),
        "daily_active_rows": distribution(list(daily_active.values())),
    },
    "security_anchor_rows": {
        "all": {
            "share_class_figi": share_anchor_rows,
            "composite_figi_only": composite_only_rows,
            "no_security_figi": no_security_anchor_rows,
        },
        "active": {
            "share_class_figi": active_share_anchor_rows,
            "composite_figi_only": active_composite_only_rows,
            "no_security_figi": active_no_security_anchor_rows,
            "no_security_figi_rate": active_no_security_anchor_rows / active_rows,
            "daily_no_security_figi": distribution(
                list(daily_active_missing_anchor.values())
            ),
        },
    },
    "distinct_values": {
        "all_tickers": len(all_tickers),
        "active_tickers": len(active_tickers),
        "all_share_class_figis": len(all_share_figis),
        "active_share_class_figis": len(active_share_figis),
        "all_composite_figis": len(all_composite_figis),
        "active_composite_figis": len(active_composite_figis),
        "all_ciks": len(all_ciks),
        "active_ciks": len(active_ciks),
    },
    "identity_status_counts": [
        {
            "active_on_date": active,
            "identity_link_status": status,
            "row_count": count,
        }
        for (active, status), count in sorted(
            status_counts.items(), key=lambda item: (str(item[0][0]), str(item[0][1]))
        )
    ],
    "top_type_counts": [
        {"active_on_date": active, "type_code": type_code, "row_count": count}
        for (active, type_code), count in sorted(
            type_counts.items(), key=lambda item: -item[1]
        )[:30]
    ],
    "runtime": {
        "peak_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
    },
}

print(json.dumps(result, indent=2, sort_keys=True, default=str))
