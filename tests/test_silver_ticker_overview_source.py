from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from copy import deepcopy
from datetime import date
from pathlib import Path

import pytest

from ame_stocks_api.artifacts import stable_digest, write_bytes_immutable
from ame_stocks_api.downloads import BronzeDownloader, build_download_plan
from ame_stocks_api.silver.store import SilverStore
from ame_stocks_api.silver.ticker_overview_source import (
    TickerOverviewSourceError,
    build_ticker_overview_lifecycle_source_inventory,
    build_ticker_overview_source_inventory,
    read_ticker_overview_source_inventory,
    ticker_overview_coverage_receipt_path,
    ticker_overview_transform_inputs,
)
from ame_stocks_api.silver.ticker_overview_source_profile import (
    TickerOverviewCoverageExpectation,
    TickerOverviewSourceProfileError,
    accepted_coverage_receipt,
    coverage_receipt_bytes,
    profile_ticker_overview_source,
    validate_ticker_overview_coverage_receipt,
)
from ame_stocks_api.transforms import (
    materialize_ticker_overview_lifecycles,
    materialize_ticker_overview_safe,
)
from ame_stocks_core import ProviderBatch, ProviderDataset, ProviderRequest


class _Provider:
    name = "massive"
    version = "fixture"

    def __init__(self, results: object) -> None:
        self.payload = json.dumps(
            {"request_id": "provider-fixture", "results": results, "status": "OK"},
            sort_keys=True,
        ).encode()

    async def fetch(
        self, request: ProviderRequest, *, checkpoint=None
    ) -> AsyncIterator[ProviderBatch]:
        yield ProviderBatch(
            provider=self.name,
            provider_version=self.version,
            dataset=request.dataset,
            request_id=request.request_id,
            sequence=0,
            payload=self.payload,
        )


def _write(root: Path, request: ProviderRequest, results: object) -> None:
    asyncio.run(BronzeDownloader(root, minimum_free_bytes=0).download(_Provider(results), request))


def _fixture(
    root: Path,
    *,
    extra_field: bool = False,
    identity_conflict: bool = False,
) -> tuple[date, str, str]:
    when = date(2026, 7, 1)
    asset_request = build_download_plan(
        dataset=ProviderDataset.ASSETS,
        start=when,
        end=when,
        active="true",
    ).requests[0]
    _write(
        root,
        asset_request,
        [
            {
                "active": True,
                "ticker": "AAPL",
                "share_class_figi": "BBG001S5N8V8",
                "cik": "0000320193",
            }
        ],
    )
    lifecycle = materialize_ticker_overview_lifecycles(root, start=when, end=when)
    request = build_download_plan(
        dataset=ProviderDataset.TICKER_OVERVIEW,
        start=when,
        end=when,
        ticker_dates=(("AAPL", when),),
    ).requests[0]
    overview = {
        "active": True,
        "ticker": "AAPL",
        "name": "Apple Inc.",
        "share_class_figi": (
            "BBG00CONFLICT" if identity_conflict else "BBG001S5N8V8"
        ),
        "cik": "0000320193",
        "market": "stocks",
        "locale": "us",
        "primary_exchange": "XNAS",
        "currency_name": "usd",
        "sic_code": "3571",
        "list_date": "1980-12-12",
        "market_cap": 1,
    }
    if extra_field:
        overview["new_provider_field"] = "drift"
    _write(root, request, overview)
    safe = materialize_ticker_overview_safe(root, start=when, end=when)
    return (
        when,
        lifecycle.manifest_path.relative_to(root).as_posix(),
        safe.manifest_path.relative_to(root).as_posix(),
    )


def _profile(root: Path) -> dict[str, object]:
    when, lifecycle_manifest, safe_manifest = _fixture(root)
    return profile_ticker_overview_source(
        root,
        lifecycle_manifest_path=lifecycle_manifest,
        oracle_manifest_path=safe_manifest,
        lifecycle_plan_path="manifests/plans/ticker_overview/test-lifecycles.jsonl",
        expected=TickerOverviewCoverageExpectation(1, 1, 1, 0, 1, 1),
        start=when,
        end=when,
    )


def test_profile_and_reader_rebuild_exact_source_and_keep_unsafe_fields_out(
    tmp_path: Path,
) -> None:
    profile = _profile(tmp_path)
    receipt = accepted_coverage_receipt(profile)
    relative = ticker_overview_coverage_receipt_path(receipt)
    stored = write_bytes_immutable(
        tmp_path,
        tmp_path / relative,
        coverage_receipt_bytes(receipt),
    )
    commit = "a" * 40
    plan_path = tmp_path / str(receipt["lifecycle_plan"]["path"])
    assert not plan_path.exists()
    lifecycle = build_ticker_overview_lifecycle_source_inventory(
        tmp_path,
        coverage_receipt_path=relative,
        coverage_receipt_sha256=str(stored["sha256"]),
        git_commit=commit,
    )
    first_plan_bytes = plan_path.read_bytes()
    lifecycle_rerun = build_ticker_overview_lifecycle_source_inventory(
        tmp_path,
        coverage_receipt_path=relative,
        coverage_receipt_sha256=str(stored["sha256"]),
        git_commit=commit,
    )
    bronze = build_ticker_overview_source_inventory(
        tmp_path,
        coverage_receipt_path=relative,
        coverage_receipt_sha256=str(stored["sha256"]),
        git_commit=commit,
    )
    store = SilverStore(tmp_path)
    lifecycle_document = store.register_source_inventory(lifecycle)
    bronze_document = store.register_source_inventory(bronze)
    batch = read_ticker_overview_source_inventory(tmp_path, bronze, lifecycle_inventory=lifecycle)
    inputs = ticker_overview_transform_inputs(batch)

    assert profile["status"] == "passed_with_warnings"
    assert lifecycle.artifacts[0].row_count == 1
    assert lifecycle.artifacts[0].path.startswith("manifests/plans/ticker_overview/")
    assert lifecycle_rerun == lifecycle
    assert plan_path.read_bytes() == first_plan_bytes
    assert len(bronze.artifacts) == 1
    assert len(bronze.upstream_manifests) == 2
    assert lifecycle_document.path.startswith("manifests/silver/source-inventories/")
    assert bronze_document.path.startswith("manifests/silver/source-inventories/")
    assert batch.row_count == 1
    assert inputs[0]["identity_match"] is True
    assert inputs[0]["source_artifact_raw_sha256"]
    assert "market_cap" not in inputs[0]


def test_profile_fails_closed_on_unreviewed_response_field(tmp_path: Path) -> None:
    when, lifecycle_manifest, safe_manifest = _fixture(tmp_path, extra_field=True)

    with pytest.raises(TickerOverviewSourceProfileError, match="schema drifted"):
        profile_ticker_overview_source(
            tmp_path,
            lifecycle_manifest_path=lifecycle_manifest,
            oracle_manifest_path=safe_manifest,
            lifecycle_plan_path="manifests/plans/ticker_overview/test.jsonl",
            expected=TickerOverviewCoverageExpectation(1, 1, 1, 0, 1, 1),
            start=when,
            end=when,
        )


def test_profile_rejects_comparable_identity_conflict_as_reviewed_absence(
    tmp_path: Path,
) -> None:
    when, lifecycle_manifest, safe_manifest = _fixture(tmp_path, identity_conflict=True)

    with pytest.raises(TickerOverviewSourceProfileError, match="reviewed diagnostics changed"):
        profile_ticker_overview_source(
            tmp_path,
            lifecycle_manifest_path=lifecycle_manifest,
            oracle_manifest_path=safe_manifest,
            lifecycle_plan_path="manifests/plans/ticker_overview/conflict.jsonl",
            expected=TickerOverviewCoverageExpectation(1, 1, 0, 1, 1, 1),
            start=when,
            end=when,
        )


def test_reader_rejects_inventories_from_different_commits(tmp_path: Path) -> None:
    profile = _profile(tmp_path)
    receipt = accepted_coverage_receipt(profile)
    relative = ticker_overview_coverage_receipt_path(receipt)
    stored = write_bytes_immutable(tmp_path, tmp_path / relative, coverage_receipt_bytes(receipt))
    lifecycle = build_ticker_overview_lifecycle_source_inventory(
        tmp_path,
        coverage_receipt_path=relative,
        coverage_receipt_sha256=str(stored["sha256"]),
        git_commit="a" * 40,
    )
    bronze = build_ticker_overview_source_inventory(
        tmp_path,
        coverage_receipt_path=relative,
        coverage_receipt_sha256=str(stored["sha256"]),
        git_commit="b" * 40,
    )

    with pytest.raises(TickerOverviewSourceError, match="share exact lineage"):
        read_ticker_overview_source_inventory(tmp_path, bronze, lifecycle_inventory=lifecycle)


def test_receipt_rejects_parallel_manifest_artifact_binding_drift(tmp_path: Path) -> None:
    receipt = accepted_coverage_receipt(_profile(tmp_path))
    drifted = deepcopy(receipt)
    drifted["artifacts"][0]["sha256"] = "f" * 64  # type: ignore[index]
    drifted.pop("coverage_receipt_id")
    drifted["coverage_receipt_id"] = stable_digest(drifted)

    with pytest.raises(TickerOverviewSourceProfileError, match="not one-to-one"):
        validate_ticker_overview_coverage_receipt(drifted)


def test_bronze_inventory_rejects_bytes_changed_after_profile(tmp_path: Path) -> None:
    profile = _profile(tmp_path)
    receipt = accepted_coverage_receipt(profile)
    relative = ticker_overview_coverage_receipt_path(receipt)
    stored = write_bytes_immutable(tmp_path, tmp_path / relative, coverage_receipt_bytes(receipt))
    artifact_path = tmp_path / str(receipt["artifacts"][0]["path"])
    artifact_path.chmod(0o644)
    artifact_path.write_bytes(b"corrupt")

    with pytest.raises(TickerOverviewSourceError, match="checksum changed"):
        build_ticker_overview_source_inventory(
            tmp_path,
            coverage_receipt_path=relative,
            coverage_receipt_sha256=str(stored["sha256"]),
            git_commit="a" * 40,
        )
