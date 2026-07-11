"""Immutable, resumable Bronze writer for raw provider JSON pages."""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import shutil
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

from ame_stocks_core import (
    PROVIDER_CONTRACT_VERSION,
    DataProvider,
    FetchCheckpoint,
    ProviderBatch,
    ProviderRequest,
)

DEFAULT_MINIMUM_FREE_BYTES = 40 * 1024**3


class BronzeStorageError(RuntimeError):
    """Raised when immutable Bronze state is inconsistent or unsafe."""


@dataclass(frozen=True, slots=True)
class DownloadResult:
    status: Literal["downloaded", "resumed", "skipped"]
    manifest_path: Path
    page_count: int
    record_count: int
    compressed_bytes: int


class BronzeDownloader:
    """Persist every successful page and checkpoint before requesting the next one."""

    manifest_schema_version = 1

    def __init__(
        self,
        data_root: Path,
        *,
        minimum_free_bytes: int = DEFAULT_MINIMUM_FREE_BYTES,
    ) -> None:
        if minimum_free_bytes < 0:
            raise ValueError("minimum_free_bytes cannot be negative")
        self.data_root = data_root.expanduser().resolve()
        self.minimum_free_bytes = minimum_free_bytes

    async def download(
        self,
        provider: DataProvider,
        request: ProviderRequest,
    ) -> DownloadResult:
        manifest_path = self._manifest_path(provider, request)
        existing = self._load_manifest(manifest_path)

        if existing and existing.get("status") == "complete":
            self._validate_complete_manifest(existing, request)
            self._validate_manifest_provider(existing, provider)
            return self._result("skipped", manifest_path, existing)

        resumed = existing is not None
        manifest = existing or self._new_manifest(provider, request)
        self._validate_manifest_identity(manifest, request)
        self._validate_manifest_provider(manifest, provider)
        artifacts = {int(item["sequence"]): item for item in manifest.get("artifacts", [])}
        checkpoint = self._checkpoint_from_manifest(manifest)
        expected_sequence = checkpoint.next_sequence if checkpoint else 0
        if checkpoint and sorted(artifacts) != list(range(expected_sequence)):
            raise BronzeStorageError("manifest artifacts do not match resume checkpoint")

        manifest["status"] = "in_progress"
        manifest.pop("failure", None)
        manifest["updated_at"] = self._now()
        self._write_manifest(manifest_path, manifest)

        try:
            async for batch in provider.fetch(request, checkpoint=checkpoint):
                self._validate_batch(batch, provider, request)
                if batch.sequence != expected_sequence:
                    raise BronzeStorageError(
                        f"expected page sequence {expected_sequence}, received {batch.sequence}"
                    )
                artifact = self._persist_batch(provider, request, batch)
                previous = artifacts.get(batch.sequence)
                if previous and previous != artifact:
                    raise BronzeStorageError(
                        f"immutable page conflict for sequence {batch.sequence}"
                    )
                artifacts[batch.sequence] = artifact
                manifest["artifacts"] = [artifacts[key] for key in sorted(artifacts)]
                manifest["checkpoint"] = (
                    None
                    if batch.is_last
                    else {
                        "continuation": batch.next_cursor,
                        "next_sequence": batch.sequence + 1,
                    }
                )
                manifest["updated_at"] = self._now()
                self._write_manifest(manifest_path, manifest)
                expected_sequence += 1

            if not artifacts:
                raise BronzeStorageError("provider completed without yielding a response page")
            if manifest.get("checkpoint") is not None:
                raise BronzeStorageError("provider stopped before the final page")
            if sorted(artifacts) != list(range(len(artifacts))):
                raise BronzeStorageError("Bronze page sequences are not contiguous")

            manifest["status"] = "complete"
            manifest["completed_at"] = self._now()
            manifest["updated_at"] = manifest["completed_at"]
            self._write_manifest(manifest_path, manifest)
        except Exception as exc:
            manifest["status"] = "failed"
            manifest["failure"] = {
                "error_type": type(exc).__name__,
                "message": "download interrupted; retrying this request is safe",
            }
            status_code = getattr(exc, "status_code", None)
            if isinstance(status_code, int):
                manifest["failure"]["provider_status_code"] = status_code
            manifest["updated_at"] = self._now()
            self._write_manifest(manifest_path, manifest)
            raise

        status: Literal["downloaded", "resumed"] = "resumed" if resumed else "downloaded"
        return self._result(status, manifest_path, manifest)

    def _persist_batch(
        self,
        provider: DataProvider,
        request: ProviderRequest,
        batch: ProviderBatch,
    ) -> dict[str, Any]:
        target = self._page_path(provider, request, batch.sequence)
        compressed = gzip.compress(batch.payload, compresslevel=9, mtime=0)
        stored_sha256 = hashlib.sha256(compressed).hexdigest()

        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            current_sha256 = hashlib.sha256(target.read_bytes()).hexdigest()
            if current_sha256 != stored_sha256:
                raise BronzeStorageError(f"refusing to overwrite immutable Bronze page: {target}")
        else:
            free_bytes = shutil.disk_usage(target.parent).free
            if free_bytes - len(compressed) < self.minimum_free_bytes:
                raise BronzeStorageError(
                    "download would reduce free disk space below the configured safety floor"
                )
            self._atomic_write(target, compressed)

        return {
            "compressed_bytes": len(compressed),
            "content_type": batch.content_type,
            "is_last": batch.is_last,
            "next_continuation": batch.next_cursor,
            "path": str(target.relative_to(self.data_root)),
            "raw_bytes": len(batch.payload),
            "raw_sha256": batch.sha256,
            "record_count": self._record_count(batch.payload),
            "sequence": batch.sequence,
            "stored_sha256": stored_sha256,
        }

    def _new_manifest(
        self,
        provider: DataProvider,
        request: ProviderRequest,
    ) -> dict[str, Any]:
        now = self._now()
        return {
            "artifacts": [],
            "checkpoint": None,
            "created_at": now,
            "dataset": request.dataset.value,
            "manifest_schema_version": self.manifest_schema_version,
            "provider": provider.name,
            "provider_contract_version": PROVIDER_CONTRACT_VERSION,
            "provider_version": provider.version,
            "request": request.canonical_dict(),
            "request_id": request.request_id,
            "status": "pending",
            "updated_at": now,
        }

    def _validate_complete_manifest(
        self,
        manifest: dict[str, Any],
        request: ProviderRequest,
    ) -> None:
        self._validate_manifest_identity(manifest, request)
        artifacts = manifest.get("artifacts")
        if not isinstance(artifacts, list) or not artifacts:
            raise BronzeStorageError("complete manifest has no artifacts")
        if manifest.get("checkpoint") is not None:
            raise BronzeStorageError("complete manifest cannot contain a checkpoint")
        sequences = [int(artifact["sequence"]) for artifact in artifacts]
        if sequences != list(range(len(artifacts))):
            raise BronzeStorageError("complete manifest page sequences are not contiguous")
        for artifact in artifacts:
            path = self._safe_artifact_path(str(artifact["path"]))
            if not path.is_file():
                raise BronzeStorageError(f"Bronze artifact is missing: {path}")
            actual = hashlib.sha256(path.read_bytes()).hexdigest()
            if actual != artifact["stored_sha256"]:
                raise BronzeStorageError(f"Bronze artifact checksum failed: {path}")

    @staticmethod
    def _validate_manifest_provider(
        manifest: dict[str, Any],
        provider: DataProvider,
    ) -> None:
        if manifest.get("provider") != provider.name:
            raise BronzeStorageError("manifest provider does not match provider")
        if manifest.get("provider_version") != provider.version:
            raise BronzeStorageError("manifest provider version does not match provider")
        if manifest.get("provider_contract_version") != PROVIDER_CONTRACT_VERSION:
            raise BronzeStorageError("manifest provider contract version is incompatible")
        if manifest.get("manifest_schema_version") != BronzeDownloader.manifest_schema_version:
            raise BronzeStorageError("manifest schema version is incompatible")

    @staticmethod
    def _validate_manifest_identity(
        manifest: dict[str, Any],
        request: ProviderRequest,
    ) -> None:
        if manifest.get("request_id") != request.request_id:
            raise BronzeStorageError("manifest request_id does not match request")
        if manifest.get("request") != request.canonical_dict():
            raise BronzeStorageError("manifest request definition does not match request")

    @staticmethod
    def _validate_batch(
        batch: ProviderBatch,
        provider: DataProvider,
        request: ProviderRequest,
    ) -> None:
        if batch.provider != provider.name:
            raise BronzeStorageError("batch provider does not match provider")
        if batch.dataset is not request.dataset:
            raise BronzeStorageError("batch dataset does not match request")
        if batch.request_id != request.request_id:
            raise BronzeStorageError("batch request_id does not match request")
        if not batch.is_last and not batch.next_cursor:
            raise BronzeStorageError("non-final batch must include a continuation")

    @staticmethod
    def _checkpoint_from_manifest(manifest: dict[str, Any]) -> FetchCheckpoint | None:
        checkpoint = manifest.get("checkpoint")
        if checkpoint is None:
            return None
        if not isinstance(checkpoint, dict):
            raise BronzeStorageError("manifest checkpoint must be an object")
        return FetchCheckpoint(
            continuation=str(checkpoint["continuation"]),
            next_sequence=int(checkpoint["next_sequence"]),
        )

    def _manifest_path(self, provider: DataProvider, request: ProviderRequest) -> Path:
        return (
            self.data_root
            / "manifests"
            / provider.name
            / request.dataset.value
            / f"{request.request_id}.json"
        )

    def _page_path(
        self,
        provider: DataProvider,
        request: ProviderRequest,
        sequence: int,
    ) -> Path:
        candidate = (
            self.data_root
            / "bronze"
            / provider.name
            / request.dataset.value
            / f"request_id={request.request_id}"
            / f"page-{sequence:05d}.json.gz"
        )
        resolved = candidate.resolve()
        try:
            resolved.relative_to(self.data_root)
        except ValueError as exc:
            raise BronzeStorageError("Bronze page path escaped data root") from exc
        return resolved

    def _safe_artifact_path(self, relative_path: str) -> Path:
        candidate = (self.data_root / relative_path).resolve()
        try:
            candidate.relative_to(self.data_root)
        except ValueError as exc:
            raise BronzeStorageError("manifest artifact escaped data root") from exc
        return candidate

    @staticmethod
    def _record_count(payload: bytes) -> int:
        try:
            document = json.loads(payload)
        except (UnicodeDecodeError, json.JSONDecodeError):
            return 0
        if not isinstance(document, dict):
            return 0
        results = document.get("results")
        if isinstance(results, list):
            return len(results)
        if isinstance(results, dict) and isinstance(results.get("events"), list):
            return len(results["events"])
        return int(results is not None)

    @staticmethod
    def _load_manifest(path: Path) -> dict[str, Any] | None:
        if not path.exists():
            return None
        try:
            loaded = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise BronzeStorageError(f"cannot read manifest: {path}") from exc
        if not isinstance(loaded, dict):
            raise BronzeStorageError("manifest root must be an object")
        return loaded

    @staticmethod
    def _write_manifest(path: Path, manifest: dict[str, Any]) -> None:
        serialized = json.dumps(manifest, indent=2, sort_keys=True).encode("utf-8") + b"\n"
        path.parent.mkdir(parents=True, exist_ok=True)
        BronzeDownloader._atomic_write(path, serialized)

    @staticmethod
    def _atomic_write(path: Path, content: bytes) -> None:
        temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}-{uuid4().hex}")
        try:
            with temporary.open("wb") as handle:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)

    @staticmethod
    def _now() -> str:
        return datetime.now(UTC).isoformat()

    @staticmethod
    def _result(
        status: Literal["downloaded", "resumed", "skipped"],
        manifest_path: Path,
        manifest: dict[str, Any],
    ) -> DownloadResult:
        artifacts = manifest.get("artifacts", [])
        return DownloadResult(
            status=status,
            manifest_path=manifest_path,
            page_count=len(artifacts),
            record_count=sum(int(item.get("record_count", 0)) for item in artifacts),
            compressed_bytes=sum(int(item.get("compressed_bytes", 0)) for item in artifacts),
        )
