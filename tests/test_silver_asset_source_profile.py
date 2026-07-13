from __future__ import annotations

import gzip
import hashlib
import json
from pathlib import Path

from ame_stocks_api.silver.asset_source_profile import profile_asset_source


def _row(
    ticker: str,
    *,
    active: bool,
    exchange: str,
    ticker_type: str,
    updated: str,
    delisted: str | None = None,
) -> dict[str, object]:
    row: dict[str, object] = {
        "active": active,
        "cik": "0000000001",
        "composite_figi": f"BBG{hashlib.sha256(ticker.encode()).hexdigest()[:9].upper()}",
        "currency_name": "usd",
        "last_updated_utc": updated,
        "locale": "us",
        "market": "stocks",
        "name": ticker,
        "primary_exchange": exchange,
        "share_class_figi": f"BBG{hashlib.sha256((ticker + 's').encode()).hexdigest()[:9].upper()}",
        "ticker": ticker,
        "type": ticker_type,
    }
    if delisted is not None:
        row["delisted_utc"] = delisted
    return row


def _write_request(
    root: Path,
    *,
    session: str,
    active: bool,
    rows: list[dict[str, object]],
) -> Path:
    request_id = hashlib.sha256(f"{session}:{active}".encode()).hexdigest()
    relative_page = f"bronze/massive/assets/request_id={request_id}/page-00000.json.gz"
    page = root / relative_page
    page.parent.mkdir(parents=True)
    raw = json.dumps(
        {
            "count": len(rows),
            "request_id": f"provider-{request_id[:12]}",
            "results": rows,
            "status": "OK",
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    compressed = gzip.compress(raw, mtime=0)
    page.write_bytes(compressed)
    document = {
        "artifacts": [
            {
                "compressed_bytes": len(compressed),
                "content_type": "application/json",
                "is_last": True,
                "next_continuation": None,
                "path": relative_page,
                "raw_bytes": len(raw),
                "raw_sha256": hashlib.sha256(raw).hexdigest(),
                "record_count": len(rows),
                "sequence": 0,
                "stored_sha256": hashlib.sha256(compressed).hexdigest(),
            }
        ],
        "completed_at": "2026-07-11T14:00:00+00:00",
        "created_at": "2026-07-11T13:59:59+00:00",
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
        "updated_at": "2026-07-11T14:00:00+00:00",
    }
    manifest = root / f"manifests/massive/assets/{request_id}.json"
    manifest.parent.mkdir(parents=True, exist_ok=True)
    manifest.write_text(json.dumps(document, separators=(",", ":"), sort_keys=True))
    return manifest


def test_profile_is_worker_invariant_and_merges_distinct_domains(tmp_path: Path) -> None:
    first_updated = "2026-07-01T12:00:00Z"
    second_updated = "2026-07-02T12:00:00Z"
    exact = _row(
        "OLDX",
        active=False,
        exchange="XNYS",
        ticker_type="CS",
        updated=first_updated,
        delisted="2024-01-01T05:00:00Z",
    )
    last_a = _row(
        "OLDY",
        active=False,
        exchange="XNYS",
        ticker_type="ETF",
        updated=first_updated,
        delisted="2024-01-01T05:00:00Z",
    )
    last_b = {**last_a, "last_updated_utc": second_updated}
    delisted_a = _row(
        "OLDZ",
        active=False,
        exchange="ARCX",
        ticker_type="PFD",
        updated=first_updated,
        delisted="2024-01-01T05:00:00Z",
    )
    delisted_b = {
        **delisted_a,
        "last_updated_utc": second_updated,
        "delisted_utc": "2024-01-02T05:00:00Z",
    }
    manifests = [
        _write_request(
            tmp_path,
            session="2025-01-02",
            active=True,
            rows=[
                _row(
                    "a",
                    active=True,
                    exchange="XNAS",
                    ticker_type="CS",
                    updated=first_updated,
                ),
                _row(
                    "A",
                    active=True,
                    exchange="XNYS",
                    ticker_type="ETF",
                    updated=first_updated,
                ),
            ],
        ),
        _write_request(
            tmp_path,
            session="2025-01-02",
            active=False,
            rows=[exact, exact, last_a, last_b, delisted_a, delisted_b],
        ),
        _write_request(
            tmp_path,
            session="2025-01-03",
            active=True,
            rows=[
                _row(
                    "NEW",
                    active=True,
                    exchange="XASE",
                    ticker_type="PFD",
                    updated=second_updated,
                )
            ],
        ),
        _write_request(
            tmp_path,
            session="2025-01-03",
            active=False,
            rows=[
                _row(
                    "IDX",
                    active=False,
                    exchange="ARCX",
                    ticker_type="INDEX",
                    updated=second_updated,
                    delisted="2024-01-03T05:00:00Z",
                )
            ],
        ),
    ]
    before = {
        path: (path.stat().st_mtime_ns, hashlib.sha256(path.read_bytes()).hexdigest())
        for path in tmp_path.rglob("*")
        if path.is_file()
    }

    single = profile_asset_source(
        tmp_path,
        manifest_paths=manifests,
        workers=1,
        current_exchange_mics={"ARCX", "XASE", "XNAS", "XNYS"},
        current_ticker_types={"CS", "ETF", "PFD"},
    )
    parallel = profile_asset_source(
        tmp_path,
        manifest_paths=manifests,
        workers=2,
        current_exchange_mics={"ARCX", "XASE", "XNAS", "XNYS"},
        current_ticker_types={"CS", "ETF", "PFD"},
    )

    assert parallel == single
    assert single["source"]["manifest_count"] == 4
    assert single["source"]["exact_active_inactive_pairs"] == 2
    assert single["field_profile"]["ticker"]["present"] == 10
    assert single["candidate_key"]["duplicate_differing_field_sets"] == {
        "": 1,
        "delisted_utc|last_updated_utc": 1,
        "last_updated_utc": 1,
    }
    assert single["candidate_key"]["selection"] == {
        "resolved_exact_duplicate": 1,
        "resolved_unique_latest_last_updated": 2,
    }
    assert single["distinct_values"]["primary_exchange"] == 4
    assert single["distinct_values"]["type"] == 4
    assert single["current_reference_diagnostic"]["ticker_type"]["unmatched"] == {"INDEX": 1}
    assert single["identity_diagnostic"]["casefold"]["distinct_collision_keys"] == 1
    after = {
        path: (path.stat().st_mtime_ns, hashlib.sha256(path.read_bytes()).hexdigest())
        for path in tmp_path.rglob("*")
        if path.is_file()
    }
    assert after == before
