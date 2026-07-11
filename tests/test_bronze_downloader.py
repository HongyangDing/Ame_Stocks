import asyncio
import gzip
import json
from collections.abc import AsyncIterator
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest

from ame_stocks_api.downloads import BronzeDownloader, BronzeStorageError
from ame_stocks_core import FetchCheckpoint, ProviderBatch, ProviderDataset, ProviderRequest


class ScriptedProvider:
    name = "massive"
    version = "test"

    def __init__(self, payloads: list[bytes], *, fail_after: int | None = None) -> None:
        self.payloads = payloads
        self.fail_after = fail_after
        self.calls = 0
        self.checkpoints: list[FetchCheckpoint | None] = []

    async def fetch(
        self,
        request: ProviderRequest,
        *,
        checkpoint: FetchCheckpoint | None = None,
    ) -> AsyncIterator[ProviderBatch]:
        self.calls += 1
        self.checkpoints.append(checkpoint)
        start = checkpoint.next_sequence if checkpoint else 0
        for sequence in range(start, len(self.payloads)):
            if self.fail_after is not None and sequence >= self.fail_after:
                raise RuntimeError("simulated interruption")
            is_last = sequence == len(self.payloads) - 1
            yield ProviderBatch(
                provider=self.name,
                provider_version=self.version,
                dataset=request.dataset,
                request_id=request.request_id,
                sequence=sequence,
                payload=self.payloads[sequence],
                next_cursor=(None if is_last else f"/next?cursor=page-{sequence + 1}"),
                is_last=is_last,
            )


def _request() -> ProviderRequest:
    return ProviderRequest(
        dataset=ProviderDataset.MINUTE_BARS,
        start=date(2024, 7, 1),
        end=date(2026, 6, 30),
        asset_ids=("AAPL",),
        adjusted=False,
    )


def _payload(timestamp: int) -> bytes:
    return json.dumps(
        {"results": [{"c": 100.0, "t": timestamp}], "status": "OK"},
        sort_keys=True,
    ).encode()


def test_bronze_download_is_atomic_checksummed_and_idempotent(tmp_path: Path) -> None:
    provider = ScriptedProvider([_payload(1), _payload(2)])
    downloader = BronzeDownloader(tmp_path, minimum_free_bytes=0)

    first = asyncio.run(downloader.download(provider, _request()))
    second_provider = ScriptedProvider([_payload(999)])
    second = asyncio.run(downloader.download(second_provider, _request()))

    assert first.status == "downloaded"
    assert first.page_count == 2
    assert first.record_count == 2
    assert second.status == "skipped"
    assert second_provider.calls == 0

    manifest = json.loads(first.manifest_path.read_text())
    assert manifest["status"] == "complete"
    assert manifest["checkpoint"] is None
    assert manifest["provider_contract_version"] == "1.1"
    assert "authorization" not in first.manifest_path.read_text().lower()
    assert "api_key" not in first.manifest_path.read_text().lower()

    first_page = tmp_path / manifest["artifacts"][0]["path"]
    assert gzip.decompress(first_page.read_bytes()) == _payload(1)


def test_interrupted_download_resumes_from_committed_continuation(tmp_path: Path) -> None:
    payloads = [_payload(1), _payload(2), _payload(3)]
    downloader = BronzeDownloader(tmp_path, minimum_free_bytes=0)
    interrupted = ScriptedProvider(payloads, fail_after=1)

    try:
        asyncio.run(downloader.download(interrupted, _request()))
    except RuntimeError as exc:
        assert str(exc) == "simulated interruption"
    else:
        raise AssertionError("interrupted provider should fail")

    resumed = ScriptedProvider(payloads)
    result = asyncio.run(downloader.download(resumed, _request()))

    assert result.status == "resumed"
    assert result.page_count == 3
    assert resumed.checkpoints == [
        FetchCheckpoint(continuation="/next?cursor=page-1", next_sequence=1)
    ]
    manifest = json.loads(result.manifest_path.read_text())
    assert manifest["status"] == "complete"
    assert [artifact["sequence"] for artifact in manifest["artifacts"]] == [0, 1, 2]


def test_nested_ticker_event_records_are_counted() -> None:
    payload = json.dumps(
        {"results": {"events": [{"type": "ticker_change"}, {"type": "ticker_change"}]}}
    ).encode()

    assert BronzeDownloader._record_count(payload) == 2


def test_provider_status_code_is_preserved_without_exception_message(tmp_path: Path) -> None:
    class SafeFailure(RuntimeError):
        status_code = 404

    class FailingProvider(ScriptedProvider):
        async def fetch(self, request, *, checkpoint=None):
            if False:
                yield
            raise SafeFailure("secret upstream detail")

    downloader = BronzeDownloader(tmp_path, minimum_free_bytes=0)
    with pytest.raises(SafeFailure):
        asyncio.run(downloader.download(FailingProvider([]), _request()))

    manifest_path = (
        tmp_path / "manifests" / "massive" / "minute_bars" / f"{_request().request_id}.json"
    )
    manifest = json.loads(manifest_path.read_text())
    assert manifest["failure"]["provider_status_code"] == 404
    assert "secret upstream detail" not in manifest_path.read_text()


def test_rest_bronze_download_refuses_to_cross_disk_floor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "ame_stocks_api.downloads.bronze.shutil.disk_usage",
        lambda path: SimpleNamespace(free=100),
    )
    downloader = BronzeDownloader(tmp_path, minimum_free_bytes=100)

    with pytest.raises(BronzeStorageError, match="safety floor"):
        asyncio.run(downloader.download(ScriptedProvider([_payload(1)]), _request()))
