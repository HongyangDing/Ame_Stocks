"""Read-only, paginated inspection of complete Parquet rows."""

from __future__ import annotations

import argparse
import json
import math
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

DEFAULT_PAGE_SIZE = 5
MAX_PAGE_SIZE = 100


class ParquetViewError(ValueError):
    """Raised when a requested Parquet page cannot be displayed safely."""


def read_parquet_page(
    path: Path,
    *,
    page: int = 1,
    page_size: int = DEFAULT_PAGE_SIZE,
    include_schema: bool = False,
) -> dict[str, object]:
    """Return one bounded page while preserving every column in each selected row."""

    if type(page) is not int or page < 1:
        raise ParquetViewError("page must be a positive integer")
    if type(page_size) is not int or not 1 <= page_size <= MAX_PAGE_SIZE:
        raise ParquetViewError(f"page_size must be between 1 and {MAX_PAGE_SIZE}")
    source = path.expanduser().resolve()
    if not source.is_file():
        raise ParquetViewError(f"Parquet file does not exist: {source}")

    parquet = pq.ParquetFile(source)
    total_rows = parquet.metadata.num_rows
    total_columns = len(parquet.schema_arrow)
    start = (page - 1) * page_size
    if total_rows == 0:
        if page != 1:
            raise ParquetViewError("empty Parquet files only have page 1")
    elif start >= total_rows:
        last_page = math.ceil(total_rows / page_size)
        raise ParquetViewError(f"page {page} is out of range; last page is {last_page}")

    rows = _read_slice(parquet, start=start, limit=page_size)
    document: dict[str, object] = {
        "file": str(source),
        "has_next": start + len(rows) < total_rows,
        "has_previous": page > 1,
        "page": page,
        "page_size": page_size,
        "row_end": start + len(rows),
        "row_start": 0 if not rows else start + 1,
        "rows": _json_safe(rows),
        "total_columns": total_columns,
        "total_rows": total_rows,
    }
    if include_schema:
        document["schema"] = [
            {
                "name": field.name,
                "nullable": field.nullable,
                "type": str(field.type),
            }
            for field in parquet.schema_arrow
        ]
    return document


def _read_slice(parquet: pq.ParquetFile, *, start: int, limit: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    scanned = 0
    for batch in parquet.iter_batches(batch_size=max(1_024, limit)):
        batch_end = scanned + batch.num_rows
        if batch_end <= start:
            scanned = batch_end
            continue
        local_start = max(0, start - scanned)
        take = min(limit - len(rows), batch.num_rows - local_start)
        rows.extend(batch.slice(local_start, take).to_pylist())
        if len(rows) == limit:
            break
        scanned = batch_end
    return rows


def _json_safe(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ame-view-parquet",
        description="Print complete Parquet rows in bounded, read-only pages.",
    )
    parser.add_argument("path", type=Path, help="path to one Parquet file")
    parser.add_argument("--page", type=int, default=1, help="one-based page number")
    parser.add_argument(
        "--page-size",
        type=int,
        default=DEFAULT_PAGE_SIZE,
        help=f"rows per page; default {DEFAULT_PAGE_SIZE}, maximum {MAX_PAGE_SIZE}",
    )
    parser.add_argument(
        "--schema",
        action="store_true",
        help="include every Arrow field, type, and nullability",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    arguments = parser.parse_args(argv)
    try:
        document = read_parquet_page(
            arguments.path,
            page=arguments.page,
            page_size=arguments.page_size,
            include_schema=arguments.schema,
        )
        print(json.dumps(document, ensure_ascii=False, indent=2, sort_keys=True))
        return 0
    except (OSError, pa.ArrowException, ParquetViewError) as exc:
        parser.exit(2, f"ame-view-parquet: {exc}\n")


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["DEFAULT_PAGE_SIZE", "ParquetViewError", "main", "read_parquet_page"]
