"""Shared helpers for immutable, checksummed offline artifacts."""

from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import polars as pl


class ArtifactError(RuntimeError):
    """Raised when a derived artifact is incomplete, unsafe, or non-idempotent."""


def stable_digest(value: object) -> str:
    serialized = json.dumps(value, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return hashlib.sha256(serialized).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def now_utc() -> str:
    return datetime.now(UTC).isoformat()


def safe_relative_path(root: Path, relative: object) -> Path:
    if not isinstance(relative, str):
        raise ArtifactError("artifact path must be a string")
    path = (root / relative).resolve()
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise ArtifactError("artifact path escaped data root") from exc
    return path


def write_json_atomic(path: Path, document: dict[str, Any]) -> None:
    content = json.dumps(document, indent=2, sort_keys=True).encode("utf-8") + b"\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = _temporary_path(path)
    try:
        _write_synced(temporary, content)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def write_bytes_immutable(root: Path, path: Path, content: bytes) -> dict[str, object]:
    path.parent.mkdir(parents=True, exist_ok=True)
    checksum = hashlib.sha256(content).hexdigest()
    if path.exists():
        if sha256_file(path) != checksum:
            raise ArtifactError(f"refusing to overwrite immutable artifact: {path}")
    else:
        temporary = _temporary_path(path)
        try:
            _write_synced(temporary, content)
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)
    return {
        "bytes": len(content),
        "path": str(path.relative_to(root)),
        "sha256": checksum,
    }


def write_parquet_immutable(
    root: Path,
    path: Path,
    frame: pl.DataFrame,
    *,
    extra: dict[str, object] | None = None,
) -> dict[str, object]:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = _temporary_path(path)
    try:
        frame.write_parquet(temporary, compression="zstd", statistics=True)
        _fsync_path(temporary)
        checksum = sha256_file(temporary)
        size = temporary.stat().st_size
        if path.exists():
            if sha256_file(path) != checksum:
                raise ArtifactError(f"refusing to overwrite immutable artifact: {path}")
        else:
            os.replace(temporary, path)
        output: dict[str, object] = {
            "bytes": size,
            "path": str(path.relative_to(root)),
            "row_count": frame.height,
            "sha256": checksum,
        }
        if extra:
            output.update(extra)
        return output
    finally:
        temporary.unlink(missing_ok=True)


def load_reusable_manifest(
    root: Path,
    path: Path,
    *,
    source_digest: str,
    schema_version: int,
) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ArtifactError(f"cannot read artifact manifest: {path}") from exc
    if not isinstance(manifest, dict) or manifest.get("status") != "complete":
        raise ArtifactError(f"artifact manifest is not complete: {path}")
    if manifest.get("schema_version") != schema_version:
        raise ArtifactError(f"artifact manifest schema is incompatible: {path}")
    if manifest.get("source_digest") != source_digest:
        raise ArtifactError("artifact source set changed; refusing immutable overwrite")
    verify_outputs(root, manifest.get("outputs"))
    return manifest


def verify_outputs(root: Path, outputs: object) -> None:
    if not isinstance(outputs, list):
        raise ArtifactError("manifest outputs must be an array")
    for output in outputs:
        if not isinstance(output, dict):
            raise ArtifactError("manifest output must be an object")
        path = safe_relative_path(root, output.get("path"))
        if not path.is_file():
            raise ArtifactError(f"artifact is missing: {path}")
        if sha256_file(path) != output.get("sha256"):
            raise ArtifactError(f"artifact checksum failed: {path}")


def _temporary_path(path: Path) -> Path:
    return path.with_name(f".{path.name}.tmp-{os.getpid()}-{uuid4().hex}")


def _write_synced(path: Path, content: bytes) -> None:
    with path.open("wb") as handle:
        handle.write(content)
        handle.flush()
        os.fsync(handle.fileno())


def _fsync_path(path: Path) -> None:
    with path.open("rb") as handle:
        os.fsync(handle.fileno())
