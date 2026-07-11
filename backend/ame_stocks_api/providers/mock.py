"""Deterministic, network-free provider used before real data is introduced."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator

from ame_stocks_core import FetchCheckpoint, ProviderBatch, ProviderRequest


class MockProvider:
    """Return one deterministic empty response page for any valid request."""

    name = "mock"
    version = "step1-empty-v1"

    def __init__(self, *, seed: int = 20260711) -> None:
        self.seed = seed

    async def fetch(
        self,
        request: ProviderRequest,
        *,
        checkpoint: FetchCheckpoint | None = None,
    ) -> AsyncIterator[ProviderBatch]:
        payload = json.dumps(
            {
                "provider": self.name,
                "provider_version": self.version,
                "request": request.canonical_dict(),
                "results": [],
                "seed": self.seed,
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        yield ProviderBatch(
            provider=self.name,
            provider_version=self.version,
            dataset=request.dataset,
            request_id=request.request_id,
            sequence=checkpoint.next_sequence if checkpoint else 0,
            payload=payload,
        )
