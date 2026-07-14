"""Read-only cross-tab of active S4 rows by provider type and security anchor."""

from __future__ import annotations

import json
import resource
from collections import Counter
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds

DATA_ROOT = Path(
    "/mnt/HC_Volume_106309665/american_stocks/"
    "silver/schema=v1/reference/universe_source_daily/"
    "build_id=21921c72c4be79665d41077664f8f027a1beb9ac0600ff4c6610d4f40638b185/"
    "data"
)
files = sorted(DATA_ROOT.glob("**/*.parquet"))
assert len(files) == 2_513

scanner = ds.dataset([str(path) for path in files], format="parquet").scanner(
    columns=["type_code", "share_class_figi", "composite_figi"],
    filter=ds.field("active_on_date") == True,  # noqa: E712
    batch_size=262_144,
    use_threads=True,
)

counts: Counter[tuple[str | None, str]] = Counter()
rows = 0
for batch in scanner.to_batches():
    table = pa.Table.from_batches([batch])
    rows += len(table)
    share = pc.invert(pc.is_null(table["share_class_figi"]))
    composite = pc.invert(pc.is_null(table["composite_figi"]))
    classes = {
        "share_and_composite_figi": pc.and_(share, composite),
        "share_class_figi_only": pc.and_(share, pc.invert(composite)),
        "composite_figi_only": pc.and_(pc.invert(share), composite),
        "no_security_figi": pc.and_(pc.invert(share), pc.invert(composite)),
    }
    for anchor_class, mask in classes.items():
        subset = table.filter(mask)
        if len(subset) == 0:
            continue
        subset = subset.append_column(
            "_one", pa.array([1] * len(subset), type=pa.int64())
        )
        grouped = subset.group_by(["type_code"]).aggregate([("_one", "sum")])
        for row in grouped.to_pylist():
            counts[(row["type_code"], anchor_class)] += int(row["_one_sum"])

assert rows == 25_630_067, rows
assert sum(counts.values()) == rows

by_type: dict[str, dict[str, object]] = {}
for (type_code, anchor_class), count in counts.items():
    key = "<null>" if type_code is None else type_code
    item = by_type.setdefault(
        key,
        {
            "type_code": type_code,
            "share_and_composite_figi": 0,
            "share_class_figi_only": 0,
            "composite_figi_only": 0,
            "no_security_figi": 0,
        },
    )
    item[anchor_class] = count

rows_out = []
for item in by_type.values():
    total = sum(int(item[name]) for name in (
        "share_and_composite_figi", "share_class_figi_only",
        "composite_figi_only", "no_security_figi"
    ))
    rows_out.append(
        {
            **item,
            "row_count": total,
            "share_class_figi_rate": (
                int(item["share_and_composite_figi"])
                + int(item["share_class_figi_only"])
            ) / total,
            "any_security_figi_rate": (
                int(item["share_and_composite_figi"])
                + int(item["share_class_figi_only"])
                + int(item["composite_figi_only"])
            ) / total,
            "composite_figi_rate": (
                int(item["share_and_composite_figi"])
                + int(item["composite_figi_only"])
            ) / total,
        }
    )

print(
    json.dumps(
        {
            "active_row_count": rows,
            "by_type": sorted(rows_out, key=lambda item: -item["row_count"]),
            "peak_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        },
        indent=2,
        sort_keys=True,
    )
)
