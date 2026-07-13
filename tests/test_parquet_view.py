from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ame_stocks_api.cli.parquet_view import ParquetViewError, main, read_parquet_page


def _write_fixture(path: Path) -> None:
    table = pa.Table.from_pylist(
        [
            {
                "as_of_date": date(2026, 7, 1),
                "identifier": row,
                "nullable_text": None if row % 2 else f"value-{row}",
                "observed_at": datetime(2026, 7, 1, row, tzinfo=UTC),
            }
            for row in range(12)
        ]
    )
    pq.write_table(table, path)


def test_parquet_view_returns_complete_rows_in_five_row_pages(tmp_path: Path) -> None:
    path = tmp_path / "fixture.parquet"
    _write_fixture(path)

    first = read_parquet_page(path, include_schema=True)
    second = read_parquet_page(path, page=2)
    third = read_parquet_page(path, page=3)

    assert (first["row_start"], first["row_end"], first["has_next"]) == (1, 5, True)
    assert (second["row_start"], second["row_end"], second["has_next"]) == (6, 10, True)
    assert (third["row_start"], third["row_end"], third["has_next"]) == (11, 12, False)
    assert [row["identifier"] for row in first["rows"]] == list(range(5))
    assert [row["identifier"] for row in second["rows"]] == list(range(5, 10))
    assert [row["identifier"] for row in third["rows"]] == [10, 11]
    assert set(first["rows"][0]) == {
        "as_of_date",
        "identifier",
        "nullable_text",
        "observed_at",
    }
    assert first["rows"][0]["as_of_date"] == "2026-07-01"
    assert first["rows"][0]["observed_at"] == "2026-07-01T00:00:00+00:00"
    assert len(first["schema"]) == first["total_columns"] == 4


def test_parquet_view_cli_prints_json_and_rejects_out_of_range_pages(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    path = tmp_path / "fixture.parquet"
    _write_fixture(path)

    assert main([str(path), "--page", "2"]) == 0
    output = json.loads(capsys.readouterr().out)
    assert output["page"] == 2
    assert len(output["rows"]) == 5
    with pytest.raises(ParquetViewError, match="last page is 3"):
        read_parquet_page(path, page=4)
