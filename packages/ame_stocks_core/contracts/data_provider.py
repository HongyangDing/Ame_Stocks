"""Source-agnostic market-data provider contract.

Providers return raw, immutable payloads in bounded batches. Parsing and normalization
belong to the Bronze-to-Silver pipeline rather than the source adapter.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import date
from enum import StrEnum
from typing import Protocol, runtime_checkable

PROVIDER_CONTRACT_VERSION = "1.0"


class ProviderDataset(StrEnum):
    """Datasets that every market-data provider may expose."""

    ASSETS = "assets"
    MINUTE_BARS = "minute_bars"
    SPLITS = "splits"
    DIVIDENDS = "dividends"


@dataclass(frozen=True, slots=True)
class ProviderRequest:
    """A deterministic request description suitable for checkpointing."""

    dataset: ProviderDataset
    start: date
    end: date
    asset_ids: tuple[str, ...] = ()
    adjusted: bool = False
    parameters: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        if self.start > self.end:
            raise ValueError("request start must be on or before request end")
        if any(not asset_id.strip() for asset_id in self.asset_ids):
            raise ValueError("asset_ids cannot contain blank values")
        if len(set(self.asset_ids)) != len(self.asset_ids):
            raise ValueError("asset_ids must be unique")
        if any(not key.strip() for key, _ in self.parameters):
            raise ValueError("parameter names cannot be blank")
        if len({key for key, _ in self.parameters}) != len(self.parameters):
            raise ValueError("parameter names must be unique")

    def canonical_dict(self) -> dict[str, object]:
        """Return the stable representation used by manifests and providers."""

        return {
            "adjusted": self.adjusted,
            "asset_ids": sorted(self.asset_ids),
            "dataset": self.dataset.value,
            "end": self.end.isoformat(),
            "parameters": dict(sorted(self.parameters)),
            "start": self.start.isoformat(),
        }

    @property
    def request_id(self) -> str:
        """Content-address the request so retries are naturally idempotent."""

        canonical = json.dumps(
            self.canonical_dict(), separators=(",", ":"), sort_keys=True
        ).encode("utf-8")
        return hashlib.sha256(canonical).hexdigest()


@dataclass(frozen=True, slots=True)
class ProviderBatch:
    """One bounded raw response page ready for immutable Bronze storage."""

    provider: str
    provider_version: str
    dataset: ProviderDataset
    request_id: str
    sequence: int
    payload: bytes
    content_type: str = "application/json"
    next_cursor: str | None = None
    is_last: bool = True

    def __post_init__(self) -> None:
        if not self.provider.strip():
            raise ValueError("provider cannot be blank")
        if not self.provider_version.strip():
            raise ValueError("provider_version cannot be blank")
        if self.sequence < 0:
            raise ValueError("sequence must be non-negative")
        if not self.request_id.strip():
            raise ValueError("request_id cannot be blank")
        if not isinstance(self.payload, bytes):
            raise TypeError("payload must be bytes")
        if not self.content_type.strip():
            raise ValueError("content_type cannot be blank")

    @property
    def sha256(self) -> str:
        """Checksum recorded by the future artifact manifest."""

        return hashlib.sha256(self.payload).hexdigest()


@runtime_checkable
class DataProvider(Protocol):
    """Protocol implemented by MockProvider now and MassiveProvider later."""

    name: str
    version: str

    def fetch(self, request: ProviderRequest) -> AsyncIterator[ProviderBatch]:
        """Stream raw response pages for a deterministic request."""

        ...
