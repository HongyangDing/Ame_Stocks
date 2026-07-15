from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from ame_stocks_api.artifacts import ArtifactError, stable_digest
from ame_stocks_api.silver.calendar_artifact import (
    CALENDAR_ARTIFACT_RULE_VERSION,
    CALENDAR_ARTIFACT_SCHEMA_VERSION,
    XNYSCalendarArtifactError,
    build_xnys_calendar_artifact,
    load_xnys_calendar_artifact,
    write_xnys_calendar_artifact,
    xnys_calendar_artifact_path,
)


def _canonical(document: dict[str, object]) -> bytes:
    return (
        json.dumps(document, allow_nan=False, separators=(",", ":"), sort_keys=True).encode()
        + b"\n"
    )


def _store_document(root: Path, document: dict[str, object]) -> tuple[str, str]:
    logical = dict(document)
    logical.pop("calendar_artifact_id", None)
    artifact_id = stable_digest(logical)
    document["calendar_artifact_id"] = artifact_id
    content = _canonical(document)
    path = root / xnys_calendar_artifact_path(artifact_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return artifact_id, hashlib.sha256(content).hexdigest()


def test_builder_is_deterministic_canonical_and_preserves_dst_opens() -> None:
    first = build_xnys_calendar_artifact(date(2024, 3, 8), date(2024, 3, 12))
    repeated = build_xnys_calendar_artifact(date(2024, 3, 8), date(2024, 3, 12))

    assert first == repeated
    assert first.content.endswith(b"\n")
    assert b" " not in first.content
    assert first.sha256 == hashlib.sha256(first.content).hexdigest()
    assert first.relative_path == xnys_calendar_artifact_path(first.calendar_artifact_id)
    document = json.loads(first.content)
    logical = dict(document)
    del logical["calendar_artifact_id"]
    assert first.calendar_artifact_id == stable_digest(logical)
    assert document["schema_version"] == CALENDAR_ARTIFACT_SCHEMA_VERSION
    assert document["rule_version"] == CALENDAR_ARTIFACT_RULE_VERSION
    assert document["start_session"] == "2024-03-08"
    assert document["end_session"] == "2024-03-12"
    assert document["session_count"] == 3
    assert document["sessions"] == [
        {"market_open_utc": "2024-03-08T14:30:00Z", "session_date": "2024-03-08"},
        {"market_open_utc": "2024-03-11T13:30:00Z", "session_date": "2024-03-11"},
        {"market_open_utc": "2024-03-12T13:30:00Z", "session_date": "2024-03-12"},
    ]
    assert first.content == _canonical(document)


def test_exact_write_and_load_are_immutable_and_do_not_use_latest(tmp_path: Path) -> None:
    artifact = build_xnys_calendar_artifact(date(2024, 1, 12), date(2024, 1, 17))
    receipt = write_xnys_calendar_artifact(tmp_path, artifact)

    assert receipt == {
        "bytes": len(artifact.content),
        "calendar_artifact_id": artifact.calendar_artifact_id,
        "end_session": "2024-01-17",
        "media_type": "application/json",
        "path": artifact.relative_path,
        "session_count": 3,
        "sha256": artifact.sha256,
        "start_session": "2024-01-12",
    }
    assert write_xnys_calendar_artifact(tmp_path, artifact) == receipt
    latest = tmp_path / "manifests/silver/xnys-calendars/latest.json"
    latest.write_text("{}\n", encoding="utf-8")
    loaded = load_xnys_calendar_artifact(
        tmp_path,
        calendar_artifact_id=artifact.calendar_artifact_id,
        expected_sha256=artifact.sha256,
    )
    assert loaded == artifact

    path = tmp_path / artifact.relative_path
    path.chmod(0o644)
    path.write_bytes(b"{}\n")
    with pytest.raises(ArtifactError, match="refusing to overwrite immutable artifact"):
        write_xnys_calendar_artifact(tmp_path, artifact)


def test_loader_fails_closed_on_missing_or_checksum_mismatch(tmp_path: Path) -> None:
    artifact = build_xnys_calendar_artifact(date(2024, 1, 12), date(2024, 1, 17))
    with pytest.raises(XNYSCalendarArtifactError, match="missing"):
        load_xnys_calendar_artifact(
            tmp_path,
            calendar_artifact_id=artifact.calendar_artifact_id,
            expected_sha256=artifact.sha256,
        )

    write_xnys_calendar_artifact(tmp_path, artifact)
    with pytest.raises(XNYSCalendarArtifactError, match="SHA-256 mismatch"):
        load_xnys_calendar_artifact(
            tmp_path,
            calendar_artifact_id=artifact.calendar_artifact_id,
            expected_sha256="0" * 64,
        )


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda doc: doc.update({"unexpected": True}), "schema is not exact"),
        (lambda doc: doc.update({"session_count": 999}), "session_count does not reconcile"),
        (lambda doc: doc["sessions"].reverse(), "strictly date-ordered"),
        (
            lambda doc: doc["sessions"][1].update({"market_open_utc": "2024-01-16T14:31:00Z"}),
            "differ from the official XNYS schedule",
        ),
    ],
)
def test_loader_validates_exact_schema_count_order_and_official_opens(
    tmp_path: Path,
    mutation: object,
    message: str,
) -> None:
    artifact = build_xnys_calendar_artifact(date(2024, 1, 12), date(2024, 1, 17))
    document = artifact.document
    assert callable(mutation)
    mutation(document)
    artifact_id, sha256 = _store_document(tmp_path, document)

    with pytest.raises(XNYSCalendarArtifactError, match=message):
        load_xnys_calendar_artifact(
            tmp_path,
            calendar_artifact_id=artifact_id,
            expected_sha256=sha256,
        )


def test_loader_recomputes_id_and_requires_canonical_json_bytes(tmp_path: Path) -> None:
    artifact = build_xnys_calendar_artifact(date(2024, 1, 12), date(2024, 1, 17))
    wrong_id = "1" * 64
    document = artifact.document
    document["calendar_artifact_id"] = wrong_id
    content = _canonical(document)
    path = tmp_path / xnys_calendar_artifact_path(wrong_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    with pytest.raises(XNYSCalendarArtifactError, match="does not reproduce"):
        load_xnys_calendar_artifact(
            tmp_path,
            calendar_artifact_id=wrong_id,
            expected_sha256=hashlib.sha256(content).hexdigest(),
        )

    pretty_document = artifact.document
    pretty = (json.dumps(pretty_document, indent=2, sort_keys=True) + "\n").encode()
    pretty_path = tmp_path / artifact.relative_path
    pretty_path.parent.mkdir(parents=True, exist_ok=True)
    pretty_path.write_bytes(pretty)
    with pytest.raises(XNYSCalendarArtifactError, match="not canonical"):
        load_xnys_calendar_artifact(
            tmp_path,
            calendar_artifact_id=artifact.calendar_artifact_id,
            expected_sha256=hashlib.sha256(pretty).hexdigest(),
        )


def test_cutoff_interfaces_use_only_the_bound_calendar() -> None:
    artifact = build_xnys_calendar_artifact(date(2024, 1, 12), date(2024, 1, 17))

    assert artifact.first_open_after(datetime(2024, 1, 12, 14, 0, tzinfo=UTC)) == (
        date(2024, 1, 12),
        datetime(2024, 1, 12, 14, 30, tzinfo=UTC),
    )
    assert artifact.first_open_after(datetime(2024, 1, 12, 22, 0, tzinfo=UTC))[0] == date(
        2024, 1, 16
    )
    assert artifact.first_open_after(datetime(2024, 1, 15, 14, 0, tzinfo=UTC))[0] == date(
        2024, 1, 16
    )
    assert artifact.require_first_open_session(
        datetime(2024, 1, 15, 14, 0, tzinfo=UTC),
        date(2024, 1, 16),
        label="approval availability",
    ) == date(2024, 1, 16)
    assert artifact.require_session_open_after(
        datetime(2024, 1, 12, 14, 0, tzinfo=UTC),
        date(2024, 1, 16),
        label="evidence availability",
    ) == date(2024, 1, 16)
    assert artifact.validate_session_open(
        date(2024, 1, 16),
        datetime(2024, 1, 16, 14, 30, tzinfo=UTC),
    ) == datetime(2024, 1, 16, 14, 30, tzinfo=UTC)
    assert artifact.require_timestamp_at_or_after_open(
        datetime(2024, 1, 16, 15, 0, tzinfo=UTC),
        date(2024, 1, 16),
        label="decision plan availability",
    ) == datetime(2024, 1, 16, 15, 0, tzinfo=UTC)

    with pytest.raises(XNYSCalendarArtifactError, match="first XNYS open"):
        artifact.require_first_open_session(
            datetime(2024, 1, 15, 14, 0, tzinfo=UTC),
            date(2024, 1, 17),
            label="approval availability",
        )
    with pytest.raises(XNYSCalendarArtifactError, match="absent"):
        artifact.market_open(date(2024, 1, 15))
    with pytest.raises(XNYSCalendarArtifactError, match="differs"):
        artifact.validate_session_open(
            date(2024, 1, 16),
            datetime(2024, 1, 16, 14, 31, tzinfo=UTC),
        )
    with pytest.raises(XNYSCalendarArtifactError, match="outside"):
        artifact.first_open_after(datetime(2024, 1, 11, 22, 0, tzinfo=UTC))
    with pytest.raises(XNYSCalendarArtifactError, match="does not contain"):
        artifact.first_open_after(datetime(2024, 1, 17, 22, 0, tzinfo=UTC))


def test_builder_rejects_ambiguous_range_boundaries() -> None:
    with pytest.raises(XNYSCalendarArtifactError, match="must both be XNYS sessions"):
        build_xnys_calendar_artifact(date(2024, 1, 13), date(2024, 1, 17))
    with pytest.raises(XNYSCalendarArtifactError, match="cannot follow"):
        build_xnys_calendar_artifact(date(2024, 1, 17), date(2024, 1, 12))
