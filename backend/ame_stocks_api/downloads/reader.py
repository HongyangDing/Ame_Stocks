"""Offline verification and reading of immutable Bronze response pages."""

from __future__ import annotations

import gzip
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ame_stocks_api.downloads.bronze import BronzeStorageError
from ame_stocks_core import ProviderRequest


@dataclass(frozen=True, slots=True)
class BronzePage:
    """One verified, decompressed Bronze page."""

    sequence: int
    document: dict[str, Any]
    path: Path
    raw_sha256: str
    stored_sha256: str


class BronzeReader:
    """Read completed Bronze requests without credentials or network access."""

    def __init__(self, data_root: Path, *, provider_name: str = "massive") -> None:
        self.data_root = data_root.expanduser().resolve()
        self.provider_name = provider_name

    def manifest(self, request: ProviderRequest) -> dict[str, Any]:
        path = self.manifest_path(request)
        try:
            document = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError as exc:
            raise BronzeStorageError(f"Bronze manifest is missing: {path}") from exc
        except (OSError, json.JSONDecodeError) as exc:
            raise BronzeStorageError(f"cannot read Bronze manifest: {path}") from exc

        if not isinstance(document, dict):
            raise BronzeStorageError("Bronze manifest root must be an object")
        if document.get("status") != "complete":
            raise BronzeStorageError(f"Bronze request is not complete: {request.request_id}")
        if document.get("provider") != self.provider_name:
            raise BronzeStorageError("Bronze manifest provider does not match reader")
        if document.get("request_id") != request.request_id:
            raise BronzeStorageError("Bronze manifest request_id does not match request")
        if document.get("request") != request.canonical_dict():
            raise BronzeStorageError("Bronze manifest request definition does not match request")
        artifacts = document.get("artifacts")
        if not isinstance(artifacts, list) or not artifacts:
            raise BronzeStorageError("complete Bronze manifest has no artifacts")
        sequences = [int(artifact.get("sequence", -1)) for artifact in artifacts]
        if sequences != list(range(len(artifacts))):
            raise BronzeStorageError("Bronze artifact page sequences are not contiguous")
        return document

    def pages(self, request: ProviderRequest) -> tuple[BronzePage, ...]:
        manifest = self.manifest(request)
        pages: list[BronzePage] = []
        for artifact in manifest["artifacts"]:
            path = self._safe_path(str(artifact["path"]))
            try:
                compressed = path.read_bytes()
            except OSError as exc:
                raise BronzeStorageError(f"cannot read Bronze artifact: {path}") from exc
            stored_sha256 = hashlib.sha256(compressed).hexdigest()
            if stored_sha256 != artifact.get("stored_sha256"):
                raise BronzeStorageError(f"Bronze stored checksum failed: {path}")
            try:
                raw = gzip.decompress(compressed)
            except gzip.BadGzipFile as exc:
                raise BronzeStorageError(f"Bronze artifact is not valid gzip: {path}") from exc
            raw_sha256 = hashlib.sha256(raw).hexdigest()
            if raw_sha256 != artifact.get("raw_sha256"):
                raise BronzeStorageError(f"Bronze raw checksum failed: {path}")
            try:
                document = json.loads(raw)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise BronzeStorageError(f"Bronze artifact is not valid JSON: {path}") from exc
            if not isinstance(document, dict):
                raise BronzeStorageError(f"Bronze artifact root must be an object: {path}")
            pages.append(
                BronzePage(
                    sequence=int(artifact["sequence"]),
                    document=document,
                    path=path,
                    raw_sha256=raw_sha256,
                    stored_sha256=stored_sha256,
                )
            )
        return tuple(pages)

    def source_entry(self, request: ProviderRequest) -> dict[str, object]:
        """Return stable source identity without decompressing the pages."""

        manifest = self.manifest(request)
        return {
            "artifacts": [
                {
                    "sequence": int(artifact["sequence"]),
                    "stored_sha256": str(artifact["stored_sha256"]),
                }
                for artifact in manifest["artifacts"]
            ],
            "request_id": request.request_id,
        }

    def manifest_path(self, request: ProviderRequest) -> Path:
        return (
            self.data_root
            / "manifests"
            / self.provider_name
            / request.dataset.value
            / f"{request.request_id}.json"
        )

    def _safe_path(self, relative_path: str) -> Path:
        path = (self.data_root / relative_path).resolve()
        try:
            path.relative_to(self.data_root)
        except ValueError as exc:
            raise BronzeStorageError("Bronze artifact escaped data root") from exc
        return path
