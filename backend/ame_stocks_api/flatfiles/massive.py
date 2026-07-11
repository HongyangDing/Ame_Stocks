"""Resumable Massive S3 Flat Files downloader with immutable Bronze storage."""

from __future__ import annotations

import csv
import gzip
import json
import os
import shutil
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Protocol
from uuid import uuid4

import boto3
from botocore.config import Config

from ame_stocks_api.artifacts import now_utc, safe_relative_path, sha256_file, write_json_atomic
from ame_stocks_api.flatfiles.plan import FlatFileObject

MASSIVE_FLAT_FILES_ENDPOINT = "https://files.massive.com"
MASSIVE_FLAT_FILES_BUCKET = "flatfiles"
MASSIVE_S3_ACCESS_KEY_ENV = "MASSIVE_S3_ACCESS_KEY_ID"
MASSIVE_S3_SECRET_KEY_ENV = "MASSIVE_S3_SECRET_ACCESS_KEY"
_REQUIRED_COLUMNS = frozenset(
    {"ticker", "volume", "open", "close", "high", "low", "window_start", "transactions"}
)
DEFAULT_MINIMUM_FREE_BYTES = 40 * 1024**3


class _Body(Protocol):
    def iter_chunks(self, chunk_size: int = ...) -> Iterator[bytes]: ...

    def close(self) -> None: ...


class _S3Client(Protocol):
    def head_object(self, *, Bucket: str, Key: str) -> Mapping[str, Any]: ...

    def get_object(self, **kwargs: str) -> Mapping[str, Any]: ...


class FlatFileDownloadError(RuntimeError):
    """Raised when a Flat File cannot be safely downloaded or validated."""


@dataclass(frozen=True, slots=True)
class FlatFileDownloadResult:
    status: Literal["downloaded", "resumed", "skipped"]
    manifest_path: Path
    file_path: Path
    compressed_bytes: int
    sha256: str


class MassiveFlatFileDownloader:
    manifest_schema_version = 1

    def __init__(
        self,
        data_root: Path,
        access_key_id: str,
        secret_access_key: str,
        *,
        endpoint_url: str = MASSIVE_FLAT_FILES_ENDPOINT,
        bucket: str = MASSIVE_FLAT_FILES_BUCKET,
        client: _S3Client | None = None,
        minimum_free_bytes: int = DEFAULT_MINIMUM_FREE_BYTES,
    ) -> None:
        self.data_root = data_root.expanduser().resolve()
        self._access_key_id = access_key_id.strip()
        self._secret_access_key = secret_access_key.strip()
        if not self._access_key_id or not self._secret_access_key:
            raise FlatFileDownloadError("Massive S3 credentials are not configured")
        if not endpoint_url.startswith("https://"):
            raise FlatFileDownloadError("Massive Flat Files endpoint must use HTTPS")
        self.endpoint_url = endpoint_url.rstrip("/")
        self.bucket = bucket
        if minimum_free_bytes < 0:
            raise ValueError("minimum_free_bytes cannot be negative")
        self.minimum_free_bytes = minimum_free_bytes
        self._client: _S3Client = client or boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self._access_key_id,
            aws_secret_access_key=self._secret_access_key,
            config=Config(
                signature_version="s3v4",
                retries={"max_attempts": 8, "mode": "adaptive"},
                s3={"addressing_style": "path"},
            ),
        )

    @classmethod
    def from_env(cls, data_root: Path, **kwargs: Any) -> MassiveFlatFileDownloader:
        return cls(
            data_root,
            os.getenv(MASSIVE_S3_ACCESS_KEY_ENV, ""),
            os.getenv(MASSIVE_S3_SECRET_KEY_ENV, ""),
            **kwargs,
        )

    def __repr__(self) -> str:
        return (
            f"MassiveFlatFileDownloader(endpoint_url={self.endpoint_url!r}, "
            f"bucket={self.bucket!r}, credentials='[REDACTED]')"
        )

    def download(self, item: FlatFileObject) -> FlatFileDownloadResult:
        manifest_path = self._manifest_path(item)
        existing = self._load_manifest(manifest_path)
        if existing:
            self._validate_manifest_identity(item, existing)
        if existing and existing.get("status") == "complete":
            return self._validate_complete(item, manifest_path, existing)

        target = self._target_path(item)
        partial = self._partial_path(item)
        if target.exists():
            raise FlatFileDownloadError(
                f"Flat File exists without a valid complete manifest: {target}"
            )

        remote = self._remote_metadata(item)
        partial_size = partial.stat().st_size if partial.exists() else 0
        if partial_size > remote["content_length"]:
            raise FlatFileDownloadError("partial Flat File is larger than the S3 object")
        if partial_size and existing is None:
            raise FlatFileDownloadError("partial Flat File exists without a resume manifest")
        if partial_size and existing:
            previous_remote = existing.get("remote")
            if previous_remote != remote:
                raise FlatFileDownloadError(
                    "S3 object metadata changed while a partial download exists"
                )
        self.data_root.mkdir(parents=True, exist_ok=True)
        remaining_bytes = int(remote["content_length"]) - partial_size
        free_bytes = shutil.disk_usage(self.data_root).free
        if free_bytes - remaining_bytes < self.minimum_free_bytes:
            raise FlatFileDownloadError(
                "download would reduce free disk space below the configured safety floor"
            )

        resumed = partial_size > 0
        manifest = {
            "bucket": self.bucket,
            "created_at": (existing or {}).get("created_at", now_utc()),
            "dataset": item.dataset.value,
            "endpoint": self.endpoint_url,
            "flat_file_manifest_schema_version": self.manifest_schema_version,
            "object_id": item.object_id,
            "object_key": item.object_key,
            "partial_bytes": partial_size,
            "remote": remote,
            "session_date": item.session_date.isoformat(),
            "status": "in_progress",
            "updated_at": now_utc(),
        }
        write_json_atomic(manifest_path, manifest)

        try:
            if partial_size < remote["content_length"]:
                self._download_remaining(item, partial, partial_size, remote["content_length"])
            actual_size = partial.stat().st_size
            if actual_size != remote["content_length"]:
                raise FlatFileDownloadError(
                    f"downloaded {actual_size} bytes; expected {remote['content_length']}"
                )
            header = self._validate_gzip_csv(partial)
            checksum = sha256_file(partial)
            target.parent.mkdir(parents=True, exist_ok=True)
            os.replace(partial, target)
            manifest.update(
                {
                    "completed_at": now_utc(),
                    "output": {
                        "bytes": actual_size,
                        "csv_header": header,
                        "path": str(target.relative_to(self.data_root)),
                        "sha256": checksum,
                    },
                    "partial_bytes": 0,
                    "status": "complete",
                    "updated_at": now_utc(),
                }
            )
            write_json_atomic(manifest_path, manifest)
        except Exception as exc:
            quarantined_partial: str | None = None
            if (
                partial.exists()
                and partial.stat().st_size == remote["content_length"]
                and not target.exists()
            ):
                quarantine = partial.with_name(f"{partial.name}.invalid-{uuid4().hex}")
                os.replace(partial, quarantine)
                quarantined_partial = str(quarantine.relative_to(self.data_root))
            manifest.update(
                {
                    "failure": {
                        "error_type": type(exc).__name__,
                        "message": "Flat File download interrupted; retry is safe",
                    },
                    "partial_bytes": partial.stat().st_size if partial.exists() else 0,
                    "quarantined_partial": quarantined_partial,
                    "status": "failed",
                    "updated_at": now_utc(),
                }
            )
            write_json_atomic(manifest_path, manifest)
            raise

        return FlatFileDownloadResult(
            status="resumed" if resumed else "downloaded",
            manifest_path=manifest_path,
            file_path=target,
            compressed_bytes=actual_size,
            sha256=checksum,
        )

    def _download_remaining(
        self,
        item: FlatFileObject,
        partial: Path,
        offset: int,
        total_bytes: int,
    ) -> None:
        try:
            request = {"Bucket": self.bucket, "Key": item.object_key}
            if offset:
                request["Range"] = f"bytes={offset}-"
            response = self._client.get_object(**request)
            if offset:
                content_range = str(response.get("ContentRange", ""))
                if not content_range.startswith(f"bytes {offset}-"):
                    raise FlatFileDownloadError("S3 server did not honor the resume byte range")
            body = response.get("Body")
            if body is None or not hasattr(body, "iter_chunks"):
                raise FlatFileDownloadError("S3 response did not contain a streaming body")
            partial.parent.mkdir(parents=True, exist_ok=True)
            mode = "ab" if offset else "wb"
            written = offset
            try:
                with partial.open(mode) as handle:
                    for chunk in body.iter_chunks(chunk_size=1024 * 1024):
                        if not chunk:
                            continue
                        handle.write(chunk)
                        written += len(chunk)
                        if written > total_bytes:
                            raise FlatFileDownloadError(
                                "S3 stream exceeded advertised content length"
                            )
                    handle.flush()
                    os.fsync(handle.fileno())
            finally:
                body.close()
        except FlatFileDownloadError:
            raise
        except Exception as exc:
            raise FlatFileDownloadError("Massive S3 object stream was interrupted") from exc

    def _remote_metadata(self, item: FlatFileObject) -> dict[str, object]:
        try:
            response = self._client.head_object(Bucket=self.bucket, Key=item.object_key)
        except Exception as exc:
            raise FlatFileDownloadError("Massive S3 object metadata request failed") from exc
        try:
            content_length = int(response["ContentLength"])
        except (KeyError, TypeError, ValueError) as exc:
            raise FlatFileDownloadError("S3 HEAD response omitted ContentLength") from exc
        if content_length <= 0:
            raise FlatFileDownloadError("S3 Flat File is empty")
        last_modified = response.get("LastModified")
        return {
            "content_length": content_length,
            "etag": str(response.get("ETag", "")).strip('"'),
            "last_modified": (
                last_modified.isoformat()
                if hasattr(last_modified, "isoformat")
                else str(last_modified or "")
            ),
        }

    def _validate_gzip_csv(self, path: Path) -> list[str]:
        try:
            with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
                reader = csv.reader(handle)
                header = next(reader)
                missing = sorted(_REQUIRED_COLUMNS - set(header))
                if missing:
                    raise FlatFileDownloadError(
                        f"Flat File CSV is missing columns: {', '.join(missing)}"
                    )
                for _ in reader:
                    pass
        except (OSError, UnicodeError, csv.Error, StopIteration) as exc:
            raise FlatFileDownloadError("Flat File is not a valid gzip CSV") from exc
        return header

    def _validate_complete(
        self,
        item: FlatFileObject,
        manifest_path: Path,
        manifest: dict[str, Any],
    ) -> FlatFileDownloadResult:
        output = manifest.get("output")
        if not isinstance(output, dict):
            raise FlatFileDownloadError("complete Flat File manifest has no output")
        path = safe_relative_path(self.data_root, output.get("path"))
        if not path.is_file() or sha256_file(path) != output.get("sha256"):
            raise FlatFileDownloadError(f"Flat File output checksum failed: {path}")
        return FlatFileDownloadResult(
            status="skipped",
            manifest_path=manifest_path,
            file_path=path,
            compressed_bytes=int(output["bytes"]),
            sha256=str(output["sha256"]),
        )

    def _validate_manifest_identity(
        self,
        item: FlatFileObject,
        manifest: dict[str, Any],
    ) -> None:
        if manifest.get("flat_file_manifest_schema_version") != self.manifest_schema_version:
            raise FlatFileDownloadError("Flat File manifest schema is incompatible")
        if manifest.get("object_id") != item.object_id:
            raise FlatFileDownloadError("Flat File manifest object_id mismatch")
        if manifest.get("object_key") != item.object_key:
            raise FlatFileDownloadError("Flat File manifest object key mismatch")

    def _target_path(self, item: FlatFileObject) -> Path:
        return self.data_root / "bronze" / "massive" / "flatfiles" / item.object_key

    def _partial_path(self, item: FlatFileObject) -> Path:
        return (
            self.data_root
            / "tmp"
            / "massive_flatfiles"
            / item.dataset.value
            / f"{item.session_date.isoformat()}.csv.gz.part"
        )

    def _manifest_path(self, item: FlatFileObject) -> Path:
        return (
            self.data_root
            / "manifests"
            / "massive"
            / "flatfiles"
            / item.dataset.value
            / f"{item.session_date.isoformat()}.json"
        )

    @staticmethod
    def _load_manifest(path: Path) -> dict[str, Any] | None:
        if not path.exists():
            return None
        try:
            manifest = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise FlatFileDownloadError(f"cannot read Flat File manifest: {path}") from exc
        if not isinstance(manifest, dict):
            raise FlatFileDownloadError("Flat File manifest root must be an object")
        return manifest
