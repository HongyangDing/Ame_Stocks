from __future__ import annotations

import gzip
import hashlib
import json
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from ame_stocks_api.silver.asset_source import (
    AssetSourceError,
    AssetSourceReader,
    build_asset_source_inventory,
    read_asset_source_inventory,
)
from ame_stocks_api.silver.contracts import SourceLayer

CAPTURE_AT = datetime(2026, 7, 11, 14, 3, 15, tzinfo=UTC)


def _row(ticker: str, *, active: bool) -> dict[str, object]:
    return {
        "active": active,
        "currency_name": "usd",
        "last_updated_utc": "2026-07-01T12:00:00Z",
        "locale": "us",
        "market": "stocks",
        "name": ticker,
        "primary_exchange": "XNAS",
        "ticker": ticker,
        "type": "CS",
    }


@dataclass(frozen=True, slots=True)
class _RequestFixture:
    manifest_path: str
    page_paths: tuple[Path, ...]


def _write_request(
    root: Path,
    *,
    session: str,
    active: bool,
    pages: list[list[object]] | None = None,
    salt: str = "",
    response_overrides: dict[int, dict[str, object]] | None = None,
    continuation_overrides: dict[int, str | None] | None = None,
    raw_payloads: dict[int, bytes] | None = None,
    stored_payloads: dict[int, bytes] | None = None,
) -> _RequestFixture:
    request_id = hashlib.sha256(f"{session}:{active}:{salt}".encode()).hexdigest()
    page_rows = pages if pages is not None else [[_row("A", active=active)]]
    artifacts: list[dict[str, object]] = []
    paths: list[Path] = []
    for sequence, rows in enumerate(page_rows):
        is_last = sequence == len(page_rows) - 1
        default_continuation = None if is_last else f"/v3/reference/tickers?cursor={sequence + 1}"
        continuation = (continuation_overrides or {}).get(sequence, default_continuation)
        response: dict[str, object] = {
            "count": len(rows),
            "next_url": continuation,
            "request_id": f"provider-{request_id[:12]}-{sequence}",
            "results": rows,
            "status": "OK",
        }
        response.update((response_overrides or {}).get(sequence, {}))
        raw = (raw_payloads or {}).get(
            sequence,
            json.dumps(response, separators=(",", ":"), sort_keys=True).encode(),
        )
        compressed = (stored_payloads or {}).get(sequence, gzip.compress(raw, mtime=0))
        relative_page = f"bronze/massive/assets/request_id={request_id}/page-{sequence:05d}.json.gz"
        page_path = root / relative_page
        page_path.parent.mkdir(parents=True, exist_ok=True)
        page_path.write_bytes(compressed)
        paths.append(page_path)
        artifacts.append(
            {
                "compressed_bytes": len(compressed),
                "content_type": "application/json",
                "is_last": is_last,
                "next_continuation": continuation,
                "path": relative_page,
                "raw_bytes": len(raw),
                "raw_sha256": hashlib.sha256(raw).hexdigest(),
                "record_count": len(rows),
                "sequence": sequence,
                "stored_sha256": hashlib.sha256(compressed).hexdigest(),
            }
        )
    manifest = {
        "artifacts": artifacts,
        "checkpoint": None,
        "completed_at": CAPTURE_AT.isoformat(),
        "created_at": CAPTURE_AT.replace(second=14).isoformat(),
        "dataset": "assets",
        "manifest_schema_version": 1,
        "provider": "massive",
        "provider_contract_version": "1.1",
        "provider_version": "1.2.0",
        "request": {
            "adjusted": False,
            "asset_ids": [],
            "dataset": "assets",
            "end": session,
            "parameters": {"active": str(active).lower()},
            "start": session,
        },
        "request_id": request_id,
        "status": "complete",
        "updated_at": CAPTURE_AT.replace(second=16).isoformat(),
    }
    relative_manifest = f"manifests/massive/assets/{request_id}.json"
    manifest_path = root / relative_manifest
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
    return _RequestFixture(relative_manifest, tuple(paths))


def _pair(
    root: Path,
    *,
    session: str = "2026-05-11",
    active_pages: list[list[object]] | None = None,
    inactive_pages: list[list[object]] | None = None,
) -> tuple[_RequestFixture, _RequestFixture]:
    active = _write_request(
        root,
        session=session,
        active=True,
        pages=active_pages,
    )
    inactive = _write_request(
        root,
        session=session,
        active=False,
        pages=inactive_pages,
    )
    return active, inactive


def _inventory(root: Path, fixtures: tuple[_RequestFixture, ...]):
    return build_asset_source_inventory(
        root,
        manifest_paths=tuple(item.manifest_path for item in fixtures),
        git_commit="a" * 40,
    )


def _read_manifest(root: Path, fixture: _RequestFixture) -> dict[str, Any]:
    return json.loads((root / fixture.manifest_path).read_text(encoding="utf-8"))


def _write_manifest(root: Path, fixture: _RequestFixture, document: dict[str, Any]) -> None:
    (root / fixture.manifest_path).write_text(
        json.dumps(document, sort_keys=True), encoding="utf-8"
    )


def _file_snapshot(root: Path) -> dict[str, tuple[int, int, str]]:
    return {
        path.relative_to(root).as_posix(): (
            path.stat().st_size,
            path.stat().st_mtime_ns,
            hashlib.sha256(path.read_bytes()).hexdigest(),
        )
        for path in root.rglob("*")
        if path.is_file()
    }


def test_reader_streams_complete_pair_with_lineage_and_never_writes(tmp_path: Path) -> None:
    active, inactive = _pair(
        tmp_path,
        active_pages=[
            [_row("A", active=True), _row("B", active=True)],
            [_row("C", active=True)],
        ],
        inactive_pages=[[_row("OLD", active=False)]],
    )
    active.page_paths[0].with_name(".page-00000.json.gz.swp").write_bytes(b"stray")
    before = _file_snapshot(tmp_path)

    inventory = _inventory(tmp_path, (inactive, active))
    assert inventory.source_dataset == "assets"
    assert inventory.source_layer is SourceLayer.BRONZE
    assert len(inventory.upstream_manifests) == 2
    assert len(inventory.artifacts) == 3
    assert not any(item.path.endswith(".swp") for item in inventory.artifacts)

    reader = read_asset_source_inventory(tmp_path, inventory)
    assert isinstance(reader, AssetSourceReader)
    assert (
        reader.session_count,
        reader.request_count,
        reader.page_count,
        reader.declared_row_count,
    ) == (1, 2, 3, 4)
    session = next(reader.iter_sessions())
    assert session.active_request.requested_active is True
    assert session.inactive_request.requested_active is False
    assert session.capture_completed_at_utc == CAPTURE_AT

    records = list(reader.iter_records())
    assert [item.row["ticker"] for item in records] == ["A", "B", "C", "OLD"]
    assert [item.requested_active for item in records] == [True, True, True, False]
    assert [item.source_page_sequence for item in records] == [0, 0, 1, 0]
    assert [item.source_row_ordinal for item in records] == [0, 1, 0, 0]
    assert records[0].source_request_id == session.active_request.source_request_id
    assert records[0].source_manifest_path == active.manifest_path
    assert (
        records[0].source_manifest_sha256
        == hashlib.sha256((tmp_path / active.manifest_path).read_bytes()).hexdigest()
    )
    assert records[0].source_artifact_path.endswith("page-00000.json.gz")
    assert records[0].source_capture_at_utc == CAPTURE_AT
    assert records[0].source_provider_request_id.startswith("provider-")
    with pytest.raises(TypeError):
        records[0].row["ticker"] = "MUTATED"  # type: ignore[index]
    assert _file_snapshot(tmp_path) == before


def test_reader_supports_multi_session_and_bounded_session_iteration(tmp_path: Path) -> None:
    day_two = _pair(
        tmp_path,
        session="2026-05-12",
        active_pages=[[_row("D2A", active=True)]],
        inactive_pages=[[_row("D2I", active=False)]],
    )
    day_one = _pair(
        tmp_path,
        session="2026-05-11",
        active_pages=[[_row("D1A", active=True)]],
        inactive_pages=[[_row("D1I", active=False)]],
    )
    inventory = _inventory(tmp_path, (*day_two, *day_one))
    reader = read_asset_source_inventory(tmp_path, inventory)

    assert [item.session_date.isoformat() for item in reader.iter_sessions()] == [
        "2026-05-11",
        "2026-05-12",
    ]
    assert [item.row["ticker"] for item in reader.iter_records()] == [
        "D1A",
        "D1I",
        "D2A",
        "D2I",
    ]
    assert [item.row["ticker"] for item in reader.iter_session_records("2026-05-12")] == [
        "D2A",
        "D2I",
    ]
    with pytest.raises(AssetSourceError, match="absent from inventory"):
        list(reader.iter_session_records("2026-05-13"))


def test_inventory_requires_exactly_one_active_and_inactive_request_per_session(
    tmp_path: Path,
) -> None:
    active = _write_request(tmp_path, session="2026-05-11", active=True)
    with pytest.raises(AssetSourceError, match="one active and one inactive"):
        _inventory(tmp_path, (active,))

    inactive = _write_request(tmp_path, session="2026-05-11", active=False)
    duplicate_active = _write_request(
        tmp_path,
        session="2026-05-11",
        active=True,
        salt="duplicate",
    )
    with pytest.raises(AssetSourceError, match="duplicate active request scope"):
        _inventory(tmp_path, (active, inactive, duplicate_active))


@pytest.mark.parametrize(
    "case",
    [
        "running",
        "checkpoint",
        "missing_checkpoint",
        "wrong_provider",
        "date_range",
        "basic_date",
        "active_native_bool",
        "extra_parameter",
        "adjusted",
    ],
)
def test_inventory_rejects_noncanonical_manifest_and_request_scope(
    tmp_path: Path,
    case: str,
) -> None:
    active, inactive = _pair(tmp_path)
    document = _read_manifest(tmp_path, active)
    request = document["request"]
    if case == "running":
        document["status"] = "running"
    elif case == "checkpoint":
        document["checkpoint"] = {"page": 1}
    elif case == "missing_checkpoint":
        del document["checkpoint"]
    elif case == "wrong_provider":
        document["provider"] = "other"
    elif case == "date_range":
        request["end"] = "2026-05-12"
    elif case == "basic_date":
        request["start"] = request["end"] = "20260511"
    elif case == "active_native_bool":
        request["parameters"]["active"] = True
    elif case == "extra_parameter":
        request["parameters"]["limit"] = 1000
    elif case == "adjusted":
        request["adjusted"] = True
    _write_manifest(tmp_path, active, document)

    with pytest.raises(AssetSourceError):
        _inventory(tmp_path, (active, inactive))


@pytest.mark.parametrize("case", ["sequence", "path", "continuation", "content_type"])
def test_inventory_rejects_noncanonical_page_declarations(tmp_path: Path, case: str) -> None:
    active, inactive = _pair(tmp_path)
    document = _read_manifest(tmp_path, active)
    artifact = document["artifacts"][0]
    if case == "sequence":
        artifact["sequence"] = 1
    elif case == "path":
        artifact["path"] = "bronze/massive/assets/request_id=other/page-00000.json.gz"
    elif case == "continuation":
        artifact["next_continuation"] = "/unexpected"
    elif case == "content_type":
        artifact["content_type"] = "text/plain"
    _write_manifest(tmp_path, active, document)

    with pytest.raises(AssetSourceError):
        _inventory(tmp_path, (active, inactive))


def test_reader_detects_same_size_stored_page_mutation(tmp_path: Path) -> None:
    pair = _pair(tmp_path)
    inventory = _inventory(tmp_path, pair)
    reader = read_asset_source_inventory(tmp_path, inventory)
    page = pair[0].page_paths[0]
    mutated = bytearray(page.read_bytes())
    mutated[-1] ^= 1
    page.write_bytes(bytes(mutated))

    with pytest.raises(AssetSourceError, match="stored checksum mismatch"):
        list(reader.iter_records())


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("raw_sha256", "0" * 64, "raw checksum mismatch"),
        ("raw_bytes", 1, "raw byte count mismatch"),
        ("record_count", 2, "rows differ from manifest"),
    ],
)
def test_reader_rejects_raw_integrity_and_manifest_count_drift(
    tmp_path: Path,
    field: str,
    value: object,
    message: str,
) -> None:
    active, inactive = _pair(tmp_path)
    document = _read_manifest(tmp_path, active)
    document["artifacts"][0][field] = value
    _write_manifest(tmp_path, active, document)
    inventory = _inventory(tmp_path, (active, inactive))

    with pytest.raises(AssetSourceError, match=message):
        list(read_asset_source_inventory(tmp_path, inventory).iter_records())


@pytest.mark.parametrize("invalid_kind", ["gzip", "json"])
def test_reader_rejects_invalid_gzip_and_json(tmp_path: Path, invalid_kind: str) -> None:
    kwargs: dict[str, object]
    if invalid_kind == "gzip":
        kwargs = {"stored_payloads": {0: b"not-gzip"}}
    else:
        kwargs = {"raw_payloads": {0: b"not-json"}}
    active = _write_request(
        tmp_path,
        session="2026-05-11",
        active=True,
        **kwargs,
    )
    inactive = _write_request(tmp_path, session="2026-05-11", active=False)
    inventory = _inventory(tmp_path, (active, inactive))

    message = "not valid gzip" if invalid_kind == "gzip" else "not valid JSON"
    with pytest.raises(AssetSourceError, match=message):
        list(read_asset_source_inventory(tmp_path, inventory).iter_records())


@pytest.mark.parametrize(
    "response_override",
    [
        {"status": "ERROR"},
        {"request_id": " provider "},
        {"request_id": None},
        {"count": 2},
        {"count": True},
        {"results": "bad"},
        {"results": [1]},
    ],
)
def test_reader_rejects_invalid_response_envelopes(
    tmp_path: Path,
    response_override: dict[str, object],
) -> None:
    active = _write_request(
        tmp_path,
        session="2026-05-11",
        active=True,
        response_overrides={0: response_override},
    )
    inactive = _write_request(tmp_path, session="2026-05-11", active=False)
    inventory = _inventory(tmp_path, (active, inactive))

    with pytest.raises(AssetSourceError):
        list(read_asset_source_inventory(tmp_path, inventory).iter_records())


def test_reader_accepts_same_origin_absolute_continuation(tmp_path: Path) -> None:
    active = _write_request(
        tmp_path,
        session="2026-05-11",
        active=True,
        pages=[[_row("A", active=True)], [_row("B", active=True)]],
        response_overrides={
            0: {"next_url": "https://api.massive.com/v3/reference/tickers?cursor=1"}
        },
    )
    inactive = _write_request(tmp_path, session="2026-05-11", active=False)
    inventory = _inventory(tmp_path, (active, inactive))

    records = list(read_asset_source_inventory(tmp_path, inventory).iter_records())
    assert [item.row["ticker"] for item in records] == ["A", "B", "A"]


def test_reader_normalizes_query_only_continuation_like_downloader(tmp_path: Path) -> None:
    active = _write_request(
        tmp_path,
        session="2026-05-11",
        active=True,
        pages=[[_row("A", active=True)], [_row("B", active=True)]],
        continuation_overrides={0: "/?cursor=x"},
        response_overrides={0: {"next_url": "?cursor=x"}},
    )
    inactive = _write_request(tmp_path, session="2026-05-11", active=False)
    inventory = _inventory(tmp_path, (active, inactive))

    records = list(read_asset_source_inventory(tmp_path, inventory).iter_records())
    assert [item.row["ticker"] for item in records] == ["A", "B", "A"]


def test_reader_rejects_cross_origin_continuation(tmp_path: Path) -> None:
    active = _write_request(
        tmp_path,
        session="2026-05-11",
        active=True,
        pages=[[_row("A", active=True)], [_row("B", active=True)]],
        response_overrides={0: {"next_url": "https://example.com/v3/reference/tickers?cursor=1"}},
    )
    inactive = _write_request(tmp_path, session="2026-05-11", active=False)
    inventory = _inventory(tmp_path, (active, inactive))

    with pytest.raises(AssetSourceError, match="changed provider origin"):
        list(read_asset_source_inventory(tmp_path, inventory).iter_records())


def test_reader_treats_terminal_empty_next_url_as_null(tmp_path: Path) -> None:
    active = _write_request(
        tmp_path,
        session="2026-05-11",
        active=True,
        response_overrides={0: {"next_url": ""}},
    )
    inactive = _write_request(tmp_path, session="2026-05-11", active=False)
    inventory = _inventory(tmp_path, (active, inactive))

    records = list(read_asset_source_inventory(tmp_path, inventory).iter_records())
    assert len(records) == 2


def test_reader_rejects_provider_active_scope_mismatch(tmp_path: Path) -> None:
    active = _write_request(
        tmp_path,
        session="2026-05-11",
        active=True,
        pages=[[_row("WRONG", active=False)]],
    )
    inactive = _write_request(tmp_path, session="2026-05-11", active=False)
    inventory = _inventory(tmp_path, (active, inactive))

    with pytest.raises(AssetSourceError, match="active flag does not match"):
        list(read_asset_source_inventory(tmp_path, inventory).iter_records())


def test_reader_rejects_inventory_drift_before_streaming(tmp_path: Path) -> None:
    pair = _pair(tmp_path)
    inventory = _inventory(tmp_path, pair)
    first = replace(inventory.artifacts[0], row_count=inventory.artifacts[0].row_count + 1)
    drifted = replace(inventory, artifacts=(first, *inventory.artifacts[1:]))

    with pytest.raises(AssetSourceError, match="differs from manifest-declared bytes"):
        read_asset_source_inventory(tmp_path, drifted)


def test_reader_detects_manifest_change_after_opening(tmp_path: Path) -> None:
    pair = _pair(tmp_path)
    inventory = _inventory(tmp_path, pair)
    reader = read_asset_source_inventory(tmp_path, inventory)
    manifest = tmp_path / pair[0].manifest_path
    manifest.write_bytes(manifest.read_bytes() + b"\n")

    with pytest.raises(AssetSourceError, match="manifest changed"):
        list(reader.iter_records())


def test_inventory_rejects_empty_duplicate_and_outside_manifest_paths(tmp_path: Path) -> None:
    active, inactive = _pair(tmp_path)
    with pytest.raises(AssetSourceError, match="at least one manifest pair"):
        _inventory(tmp_path, ())
    with pytest.raises(AssetSourceError, match="must be unique"):
        _inventory(tmp_path, (active, active, inactive))
    with pytest.raises(AssetSourceError, match="canonical namespace"):
        build_asset_source_inventory(
            tmp_path,
            manifest_paths=("manifests/massive/other/file.json",),
            git_commit="a" * 40,
        )
