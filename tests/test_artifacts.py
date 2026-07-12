from __future__ import annotations

import os
from pathlib import Path

import polars as pl
import pytest

from ame_stocks_api.artifacts import (
    ArtifactError,
    safe_relative_path,
    stable_digest,
    write_bytes_immutable,
    write_parquet_immutable,
)


def test_stable_digest_rejects_nan() -> None:
    with pytest.raises(ValueError, match="Out of range float values"):
        stable_digest({"value": float("nan")})


def test_safe_relative_path_rejects_absolute_and_parent_escape(tmp_path: Path) -> None:
    root = tmp_path / "data"
    root.mkdir()

    with pytest.raises(ArtifactError, match="must be relative"):
        safe_relative_path(root, str(tmp_path / "outside.json"))

    with pytest.raises(ArtifactError, match="escaped data root"):
        safe_relative_path(root, "../outside.json")


def test_write_bytes_immutable_is_idempotent_and_rejects_conflicts(tmp_path: Path) -> None:
    root = tmp_path / "data"
    root.mkdir()
    target = root / "nested" / "payload.bin"

    first = write_bytes_immutable(root, target, b"fixed payload")
    second = write_bytes_immutable(root, target, b"fixed payload")

    assert second == first
    assert first["path"] == "nested/payload.bin"
    assert first["bytes"] == len(b"fixed payload")
    assert target.read_bytes() == b"fixed payload"

    with pytest.raises(ArtifactError, match="refusing to overwrite immutable artifact"):
        write_bytes_immutable(root, target, b"conflicting payload")

    assert target.read_bytes() == b"fixed payload"


def test_write_parquet_immutable_is_idempotent_and_rejects_conflicts(
    tmp_path: Path,
) -> None:
    root = tmp_path / "data"
    root.mkdir()
    target = root / "table" / "part.parquet"
    frame = pl.DataFrame({"asset_id": ["A", "B"], "value": [1, 2]})

    first = write_parquet_immutable(root, target, frame, extra={"partition": "2024-01-02"})
    second = write_parquet_immutable(root, target, frame, extra={"partition": "2024-01-02"})

    assert second == first
    assert first["path"] == "table/part.parquet"
    assert first["row_count"] == 2
    assert first["bytes"] == target.stat().st_size
    assert pl.read_parquet(target).equals(frame)

    conflicting = pl.DataFrame({"asset_id": ["A", "B"], "value": [1, 3]})
    with pytest.raises(ArtifactError, match="refusing to overwrite immutable artifact"):
        write_parquet_immutable(root, target, conflicting)

    assert pl.read_parquet(target).equals(frame)


@pytest.mark.parametrize("reserved_key", ["bytes", "path", "row_count", "sha256"])
def test_write_parquet_immutable_rejects_reserved_extra_keys(
    tmp_path: Path,
    reserved_key: str,
) -> None:
    root = tmp_path / "data"
    root.mkdir()

    with pytest.raises(ArtifactError, match="cannot override reserved fields"):
        write_parquet_immutable(
            root,
            root / "table.parquet",
            pl.DataFrame({"value": [1]}),
            extra={reserved_key: "spoofed"},
        )

    assert not (root / "table.parquet").exists()


def test_artifact_paths_cannot_escape_through_symlink(tmp_path: Path) -> None:
    root = tmp_path / "data"
    outside = tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    link = root / "escape"
    try:
        os.symlink(outside, link, target_is_directory=True)
    except (NotImplementedError, OSError) as exc:
        pytest.skip(f"symbolic links are unavailable: {exc}")

    with pytest.raises(ArtifactError, match=r"escaped data root|through symlink"):
        safe_relative_path(root, "escape/payload.bin")

    with pytest.raises(ArtifactError, match=r"escaped data root|through symlink"):
        write_bytes_immutable(root, link / "payload.bin", b"payload")

    assert not (outside / "payload.bin").exists()
