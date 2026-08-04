from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, timedelta
from enum import StrEnum
from types import SimpleNamespace

import pytest

from ame_stocks_api.silver import incremental_resolver as incremental_resolver_module
from ame_stocks_api.silver.incremental_resolver import (
    IncrementalResolutionError,
)
from ame_stocks_api.silver.incremental_resolver import (
    _resolve_manifest_snapshot as resolve_incremental_snapshot,
)


class _ViewKind(StrEnum):
    HISTORICAL_AS_KNOWN = "historical_as_known"
    LATEST_REVIEWED_RESEARCH = "latest_reviewed_research"


class _ReleaseType(StrEnum):
    BASE = "base"
    DELTA = "delta"
    CORRECTION = "correction"


@dataclass(frozen=True, slots=True)
class _Pin:
    release_id: str
    manifest_path: str
    manifest_sha256: str
    manifest_bytes: int
    release_available_session: date

    def to_dict(self) -> dict[str, object]:
        return {
            "manifest_bytes": self.manifest_bytes,
            "manifest_path": self.manifest_path,
            "manifest_sha256": self.manifest_sha256,
            "release_available_session": self.release_available_session.isoformat(),
            "release_id": self.release_id,
        }


@dataclass(frozen=True, slots=True)
class _Artifact:
    path: str
    sha256: str
    bytes: int = 1

    def to_dict(self) -> dict[str, object]:
        return {"bytes": self.bytes, "path": self.path, "sha256": self.sha256}


@dataclass(frozen=True, slots=True)
class _Reference:
    table_name: str
    row_version_id: str

    def to_dict(self) -> dict[str, str]:
        return {
            "row_version_id": self.row_version_id,
            "table_name": self.table_name,
        }


@dataclass(frozen=True, slots=True)
class _Partition:
    table_name: str
    partition_key: str
    receipt: _Artifact
    availability_session: date
    row_version_references: tuple[_Reference, ...] = ()

    @property
    def key(self) -> tuple[str, str]:
        return (self.table_name, self.partition_key)

    def to_dict(self) -> dict[str, object]:
        return {
            "availability_session": self.availability_session.isoformat(),
            "partition_key": self.partition_key,
            "receipt": self.receipt.to_dict(),
            "row_version_references": [item.to_dict() for item in self.row_version_references],
            "table_name": self.table_name,
        }


@dataclass(frozen=True, slots=True)
class _Replacement:
    replaced_receipt: _Partition
    replacement_receipt: _Partition

    @property
    def key(self) -> tuple[str, str]:
        return self.replacement_receipt.key


@dataclass(frozen=True, slots=True)
class _RowVersion:
    table_name: str
    stable_row_key: str
    row_version_id: str
    predecessor_row_version_id: str | None
    availability_session: date
    is_tombstone: bool = False
    tombstone_reason: str | None = None

    @property
    def key(self) -> tuple[str, str]:
        return (self.table_name, self.stable_row_key)

    def to_dict(self) -> dict[str, object]:
        return {
            "availability_session": self.availability_session.isoformat(),
            "is_tombstone": self.is_tombstone,
            "predecessor_row_version_id": self.predecessor_row_version_id,
            "row_version_id": self.row_version_id,
            "stable_row_key": self.stable_row_key,
            "table_name": self.table_name,
            "tombstone_reason": self.tombstone_reason,
        }


@dataclass(frozen=True, slots=True)
class _Manifest:
    manifest_pin: _Pin
    release_type: _ReleaseType
    parent_release_pin: _Pin | None
    resolved_view: _ViewKind
    schema_digest: str
    transform_semantics_digest: str
    identity_policy_bundle_id: str
    calendar_digest: str
    source_cutoff_session: date
    availability_cutoff_session: date
    release_available_session: date
    added_partition_receipts: tuple[_Partition, ...] = ()
    partition_replacements: tuple[_Replacement, ...] = ()
    added_row_versions: tuple[_RowVersion, ...] = ()
    superseded_row_version_ids: tuple[str, ...] = ()

    @property
    def release_id(self) -> str:
        # The fixture keeps its external pin nearby for concise chain construction, but the
        # resolver consumes only this logical ID from the manifest body.
        return self.manifest_pin.release_id


_SESSION_1 = date(2026, 1, 2)
_SESSION_2 = date(2026, 1, 5)
_SESSION_3 = date(2026, 1, 6)
_DIGESTS = {
    "schema_digest": "1" * 64,
    "transform_semantics_digest": "2" * 64,
    "identity_policy_bundle_id": "3" * 64,
    "calendar_digest": "4" * 64,
}


def _pin(name: str, session: date, *, sha: str | None = None) -> _Pin:
    return _Pin(
        release_id=name,
        manifest_path=f"releases/{name}/manifest.json",
        manifest_sha256=sha or (name[-1:] or "a") * 64,
        manifest_bytes=100,
        release_available_session=session,
    )


def _artifact(name: str) -> _Artifact:
    return _Artifact(path=f"data/{name}.parquet", sha256=(name[-1:] or "a") * 64)


def _partition(
    session: date,
    *,
    receipt_name: str,
    key: str | None = None,
    references: tuple[_Reference, ...] = (),
) -> _Partition:
    return _Partition(
        table_name="universe_daily",
        partition_key=key or session.isoformat(),
        receipt=_artifact(receipt_name),
        availability_session=session,
        row_version_references=references,
    )


def _row(
    version: str,
    *,
    stable: str = "alias-1",
    predecessor: str | None = None,
    availability: date = _SESSION_1,
    tombstone: bool = False,
    reason: str | None = None,
    table: str = "ticker_alias",
) -> _RowVersion:
    return _RowVersion(
        table_name=table,
        stable_row_key=stable,
        row_version_id=version,
        predecessor_row_version_id=predecessor,
        availability_session=availability,
        is_tombstone=tombstone,
        tombstone_reason=reason,
    )


def _manifest(
    name: str,
    release_type: _ReleaseType,
    session: date,
    *,
    parent: _Manifest | _Pin | None = None,
    partitions: tuple[_Partition, ...] = (),
    replacements: tuple[_Replacement, ...] = (),
    rows: tuple[_RowVersion, ...] = (),
    resolved_view: _ViewKind = _ViewKind.LATEST_REVIEWED_RESEARCH,
    source_cutoff: date | None = None,
    availability_cutoff: date | None = None,
    **digest_overrides: str,
) -> _Manifest:
    parent_pin = parent.manifest_pin if isinstance(parent, _Manifest) else parent
    return _Manifest(
        manifest_pin=_pin(name, session),
        release_type=release_type,
        parent_release_pin=parent_pin,
        resolved_view=resolved_view,
        source_cutoff_session=source_cutoff or session,
        availability_cutoff_session=availability_cutoff or session,
        release_available_session=session,
        added_partition_receipts=partitions,
        partition_replacements=replacements,
        added_row_versions=rows,
        superseded_row_version_ids=tuple(
            row.predecessor_row_version_id
            for row in rows
            if row.predecessor_row_version_id is not None
        ),
        **(_DIGESTS | digest_overrides),
    )


def _loader(*manifests: _Manifest):
    by_identity = {
        (
            manifest.manifest_pin.release_id,
            manifest.manifest_pin.manifest_path,
            manifest.manifest_pin.manifest_sha256,
            manifest.manifest_pin.manifest_bytes,
            manifest.manifest_pin.release_available_session,
        ): manifest
        for manifest in manifests
    }
    requested: list[_Pin] = []

    def load(pin: _Pin) -> _Manifest | None:
        requested.append(pin)
        return by_identity.get(
            (
                pin.release_id,
                pin.manifest_path,
                pin.manifest_sha256,
                pin.manifest_bytes,
                pin.release_available_session,
            )
        )

    return load, requested


def _resolve(
    top: _Manifest,
    *chain: _Manifest,
    view: _ViewKind = _ViewKind.LATEST_REVIEWED_RESEARCH,
    cutoff: date = _SESSION_3,
):
    load, requested = _loader(top, *chain)
    result = resolve_incremental_snapshot(
        top.manifest_pin,
        view_kind=view,
        cutoff_session=cutoff,
        load_parent=load,
    )
    return result, requested


def test_long_partition_chain_uses_a_linear_running_frontier(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release_count = 1_200
    first_session = date(2020, 1, 1)
    chain: list[_Manifest] = []
    parent: _Manifest | None = None
    for index in range(release_count):
        session = first_session + timedelta(days=index)
        release = _manifest(
            f"linear-{index:04d}",
            _ReleaseType.BASE if index == 0 else _ReleaseType.DELTA,
            session,
            parent=parent,
            partitions=(
                _partition(
                    session,
                    receipt_name=f"linear-partition-{index:04d}",
                ),
            ),
        )
        chain.append(release)
        parent = release

    session_calls = 0
    original_session = incremental_resolver_module._session

    def counting_session(value: object, *, label: str) -> date:
        nonlocal session_calls
        session_calls += 1
        return original_session(value, label=label)

    monkeypatch.setattr(incremental_resolver_module, "_session", counting_session)
    resolved, requested = _resolve(
        chain[-1],
        *chain[:-1],
        cutoff=chain[-1].release_available_session,
    )

    assert len(requested) == release_count
    assert len(resolved.partition_receipts) == release_count
    assert session_calls < release_count * 30


def test_long_row_predecessor_chain_resolves_one_terminal_without_recursion() -> None:
    release_count = 1_200
    first_session = date(2020, 1, 1)
    chain: list[_Manifest] = []
    parent: _Manifest | None = None
    predecessor: str | None = None
    for index in range(release_count):
        session = first_session + timedelta(days=index)
        version = f"alias-v{index:04d}"
        release = _manifest(
            f"row-linear-{index:04d}",
            _ReleaseType.BASE if index == 0 else _ReleaseType.CORRECTION,
            session,
            parent=parent,
            rows=(
                _row(
                    version,
                    predecessor=predecessor,
                    availability=session,
                ),
            ),
        )
        chain.append(release)
        parent = release
        predecessor = version

    resolved, requested = _resolve(
        chain[-1],
        *chain[:-1],
        cutoff=chain[-1].release_available_session,
    )

    assert len(requested) == release_count
    assert len(resolved.audit_row_version_catalog) == release_count
    assert resolved.terminal_row_versions[("ticker_alias", "alias-1")].row_version_id == (
        f"alias-v{release_count - 1:04d}"
    )


def test_resolves_exact_chain_partitions_versions_tombstones_and_digests() -> None:
    v1 = _row("v1")
    base_partition = _partition(
        _SESSION_1,
        receipt_name="p1",
        references=(_Reference("ticker_alias", "v1"),),
    )
    base = _manifest(
        "base-a",
        _ReleaseType.BASE,
        _SESSION_1,
        partitions=(base_partition,),
        rows=(v1,),
    )
    v2 = _row(
        "v2",
        predecessor="v1",
        availability=_SESSION_2,
        tombstone=True,
        reason="withdrawn exact source subject",
    )
    correction_partition = _partition(_SESSION_2, receipt_name="p2")
    correction = _manifest(
        "correction-b",
        _ReleaseType.CORRECTION,
        _SESSION_2,
        parent=base,
        partitions=(correction_partition,),
        rows=(v2,),
    )

    first, requested = _resolve(correction, base)
    second, _ = _resolve(correction, base)

    assert requested == [correction.manifest_pin, base.manifest_pin]
    assert first.release_chain == (base.manifest_pin, correction.manifest_pin)
    assert set(first.partition_receipts) == {
        ("universe_daily", _SESSION_1.isoformat()),
        ("universe_daily", _SESSION_2.isoformat()),
    }
    assert set(first.row_version_catalog) == {("ticker_alias", "v1"), ("ticker_alias", "v2")}
    assert first.row_versions_by_stable_key[("ticker_alias", "alias-1")] == (v1, v2)
    assert first.terminal_row_versions[("ticker_alias", "alias-1")] == v2
    assert first.tombstoned_row_keys == (("ticker_alias", "alias-1"),)
    assert dict(first.compatibility_digests) == _DIGESTS
    assert len(first.lineage_digest) == len(first.snapshot_digest) == 64
    assert first.snapshot_digest == second.snapshot_digest
    with pytest.raises(TypeError):
        first.partition_receipts[("universe_daily", "new")] = base_partition  # type: ignore[index]


def test_historical_view_filters_release_and_version_availability_but_latest_applies_top() -> None:
    v1 = _row("v1")
    base = _manifest("base-a", _ReleaseType.BASE, _SESSION_1, rows=(v1,))
    # The row and partition are individually old enough, but the release itself is unavailable
    # until SESSION_3.  Historical selection must gate on both levels.
    v2 = _row("v2", predecessor="v1", availability=_SESSION_1)
    correction = _manifest(
        "correction-c",
        _ReleaseType.CORRECTION,
        _SESSION_1,
        parent=base,
        partitions=(_partition(_SESSION_1, receipt_name="p3"),),
        rows=(v2,),
    )
    correction = replace(
        correction,
        release_available_session=_SESSION_3,
        manifest_pin=replace(
            correction.manifest_pin,
            release_available_session=_SESSION_3,
        ),
    )

    historical, _ = _resolve(
        correction,
        base,
        view=_ViewKind.HISTORICAL_AS_KNOWN,
        cutoff=_SESSION_2,
    )
    latest, _ = _resolve(
        correction,
        base,
        view=_ViewKind.LATEST_REVIEWED_RESEARCH,
        cutoff=_SESSION_2,
    )

    assert historical.terminal_row_versions[("ticker_alias", "alias-1")] == v1
    assert historical.partition_receipts == {}
    assert set(historical.row_version_catalog) == {("ticker_alias", "v1")}
    assert historical.row_versions_by_stable_key[("ticker_alias", "alias-1")] == (v1,)
    assert set(historical.audit_row_version_catalog) == {
        ("ticker_alias", "v1"),
        ("ticker_alias", "v2"),
    }
    assert latest.terminal_row_versions[("ticker_alias", "alias-1")] == v2
    assert (
        latest.terminal_row_versions[("ticker_alias", "alias-1")].availability_session == _SESSION_1
    )
    assert latest.release_chain[-1].release_available_session == _SESSION_3
    assert set(latest.partition_receipts) == {("universe_daily", _SESSION_1.isoformat())}


def test_historical_view_rejects_base_newer_than_cutoff() -> None:
    base = _manifest("base-a", _ReleaseType.BASE, _SESSION_2)
    load, _ = _loader(base)

    with pytest.raises(IncrementalResolutionError, match="retrospective base newer"):
        resolve_incremental_snapshot(
            base.manifest_pin,
            view_kind=_ViewKind.HISTORICAL_AS_KNOWN,
            cutoff_session=_SESSION_1,
            load_parent=load,
        )


def test_child_release_availability_cannot_precede_parent() -> None:
    base = _manifest("base-a", _ReleaseType.BASE, _SESSION_2)
    child = _manifest(
        "delta-b",
        _ReleaseType.DELTA,
        _SESSION_1,
        parent=base,
    )

    with pytest.raises(IncrementalResolutionError, match="precedes parent release"):
        _resolve(child, base)


def test_child_cutoffs_cannot_regress_and_non_base_view_cannot_change() -> None:
    base = _manifest("base-a", _ReleaseType.BASE, _SESSION_2)
    source_regression = _manifest(
        "delta-source-regression",
        _ReleaseType.DELTA,
        _SESSION_3,
        parent=base,
        source_cutoff=_SESSION_1,
    )
    with pytest.raises(IncrementalResolutionError, match="child source cutoff"):
        _resolve(source_regression, base)

    early_source = date(2026, 1, 1)
    availability_parent = _manifest(
        "base-availability",
        _ReleaseType.BASE,
        _SESSION_2,
        source_cutoff=early_source,
    )
    availability_regression = _manifest(
        "delta-availability-regression",
        _ReleaseType.DELTA,
        _SESSION_3,
        parent=availability_parent,
        source_cutoff=early_source,
        availability_cutoff=_SESSION_1,
    )
    with pytest.raises(IncrementalResolutionError, match="child availability cutoff"):
        _resolve(availability_regression, availability_parent)

    publication_late_parent = _manifest(
        "base-publication-late",
        _ReleaseType.BASE,
        _SESSION_2,
        source_cutoff=early_source,
        availability_cutoff=_SESSION_1,
    )
    child_before_parent_publication = _manifest(
        "delta-before-parent-publication",
        _ReleaseType.DELTA,
        _SESSION_3,
        parent=publication_late_parent,
        source_cutoff=early_source,
        availability_cutoff=_SESSION_1,
    )
    with pytest.raises(
        IncrementalResolutionError,
        match="cutoff precedes parent release availability",
    ):
        _resolve(child_before_parent_publication, publication_late_parent)

    view_drift = _manifest(
        "correction-view-drift",
        _ReleaseType.CORRECTION,
        _SESSION_3,
        parent=base,
        resolved_view=_ViewKind.HISTORICAL_AS_KNOWN,
    )
    with pytest.raises(IncrementalResolutionError, match="changed its parent's resolved view"):
        _resolve(view_drift, base)


def test_missing_parent_cycle_and_same_id_different_pin_fail_closed() -> None:
    missing = _manifest(
        "delta-b",
        _ReleaseType.DELTA,
        _SESSION_2,
        parent=_pin("missing-a", _SESSION_1),
    )
    load, _ = _loader(missing)
    with pytest.raises(IncrementalResolutionError, match="missing exact manifest"):
        resolve_incremental_snapshot(
            missing.manifest_pin,
            view_kind=_ViewKind.LATEST_REVIEWED_RESEARCH,
            cutoff_session=_SESSION_3,
            load_parent=load,
        )

    pin_a = _pin("release-a", _SESSION_1, sha="a" * 64)
    pin_b = _pin("release-b", _SESSION_2, sha="b" * 64)
    release_a = _manifest("ignored-a", _ReleaseType.CORRECTION, _SESSION_1, parent=pin_b)
    release_b = _manifest("ignored-b", _ReleaseType.CORRECTION, _SESSION_2, parent=pin_a)
    release_a = replace(release_a, manifest_pin=pin_a)
    release_b = replace(release_b, manifest_pin=pin_b)
    load, _ = _loader(release_a, release_b)
    with pytest.raises(IncrementalResolutionError, match="parent cycle"):
        resolve_incremental_snapshot(
            pin_a,
            view_kind=_ViewKind.LATEST_REVIEWED_RESEARCH,
            cutoff_session=_SESSION_3,
            load_parent=load,
        )

    conflicting_pin = replace(pin_a, manifest_sha256="c" * 64)
    conflict = replace(release_a, parent_release_pin=conflicting_pin)
    load, _ = _loader(conflict)
    with pytest.raises(IncrementalResolutionError, match="two different manifest pins"):
        resolve_incremental_snapshot(
            pin_a,
            view_kind=_ViewKind.LATEST_REVIEWED_RESEARCH,
            cutoff_session=_SESSION_3,
            load_parent=load,
        )


def test_loader_must_return_body_with_the_requested_release_id() -> None:
    base = _manifest("base-a", _ReleaseType.BASE, _SESSION_1)
    wrong = replace(
        base,
        manifest_pin=replace(base.manifest_pin, release_id="other-release"),
    )

    with pytest.raises(IncrementalResolutionError, match="release ID does not match exact pin"):
        resolve_incremental_snapshot(
            base.manifest_pin,
            view_kind=_ViewKind.LATEST_REVIEWED_RESEARCH,
            cutoff_session=_SESSION_3,
            load_parent=lambda pin: wrong,
        )


def test_manifest_body_does_not_need_to_expose_its_external_pin() -> None:
    base = _manifest("base-a", _ReleaseType.BASE, _SESSION_1)
    body = SimpleNamespace(
        release_id=base.release_id,
        release_type=base.release_type,
        parent_release_pin=base.parent_release_pin,
        schema_digest=base.schema_digest,
        transform_semantics_digest=base.transform_semantics_digest,
        identity_policy_bundle_id=base.identity_policy_bundle_id,
        calendar_digest=base.calendar_digest,
        resolved_view=base.resolved_view,
        source_cutoff_session=base.source_cutoff_session,
        availability_cutoff_session=base.availability_cutoff_session,
        release_available_session=base.release_available_session,
        added_partition_receipts=base.added_partition_receipts,
        partition_replacements=base.partition_replacements,
        added_row_versions=base.added_row_versions,
        superseded_row_version_ids=base.superseded_row_version_ids,
    )

    resolved = resolve_incremental_snapshot(
        base.manifest_pin,
        view_kind=_ViewKind.LATEST_REVIEWED_RESEARCH,
        cutoff_session=_SESSION_3,
        load_parent=lambda pin: body,
    )

    assert resolved.release_chain == (base.manifest_pin,)


def test_duplicate_partition_addition_and_existing_key_fail_closed() -> None:
    partition = _partition(_SESSION_1, receipt_name="p1")
    duplicate = _manifest(
        "base-a",
        _ReleaseType.BASE,
        _SESSION_1,
        partitions=(partition, partition),
    )
    with pytest.raises(IncrementalResolutionError, match="duplicate partition operation"):
        _resolve(duplicate)

    base = _manifest(
        "base-a",
        _ReleaseType.BASE,
        _SESSION_1,
        partitions=(partition,),
    )
    duplicate_delta = _manifest(
        "delta-b",
        _ReleaseType.DELTA,
        _SESSION_2,
        parent=base,
        partitions=(replace(partition, receipt=_artifact("other")),),
    )
    with pytest.raises(IncrementalResolutionError, match="already exists"):
        _resolve(duplicate_delta, base)


def test_future_partition_addition_corruption_is_validated_before_view_projection() -> None:
    current = _partition(_SESSION_1, receipt_name="p1")
    base = _manifest(
        "base-a",
        _ReleaseType.BASE,
        _SESSION_1,
        partitions=(current,),
    )
    duplicate = replace(
        current,
        receipt=_artifact("future-duplicate"),
        availability_session=_SESSION_3,
    )
    future_delta = _manifest(
        "delta-future",
        _ReleaseType.DELTA,
        _SESSION_3,
        parent=base,
        partitions=(duplicate,),
    )

    with pytest.raises(IncrementalResolutionError, match="already exists"):
        _resolve(
            future_delta,
            base,
            view=_ViewKind.HISTORICAL_AS_KNOWN,
            cutoff=_SESSION_2,
        )


def test_partition_temporality_and_delta_append_boundary_fail_closed() -> None:
    future_session = _partition(
        _SESSION_2,
        receipt_name="future-session",
        key=_SESSION_3.isoformat(),
    )
    future_base = _manifest(
        "base-future-session",
        _ReleaseType.BASE,
        _SESSION_3,
        partitions=(future_session,),
    )
    with pytest.raises(
        IncrementalResolutionError,
        match="partition session exceeds partition receipt availability",
    ):
        _resolve(future_base)

    beyond_source = _manifest(
        "base-beyond-source",
        _ReleaseType.BASE,
        _SESSION_2,
        source_cutoff=_SESSION_1,
        partitions=(_partition(_SESSION_2, receipt_name="beyond-source"),),
    )
    with pytest.raises(IncrementalResolutionError, match="manifest source cutoff"):
        _resolve(beyond_source)

    parent = _manifest(
        "base-parent-max",
        _ReleaseType.BASE,
        _SESSION_2,
        partitions=(_partition(_SESSION_2, receipt_name="parent-max"),),
    )
    backfill_delta = _manifest(
        "delta-historical-backfill",
        _ReleaseType.DELTA,
        _SESSION_3,
        parent=parent,
        partitions=(
            _partition(
                _SESSION_3,
                receipt_name="historical-backfill",
                key=_SESSION_1.isoformat(),
            ),
        ),
    )
    with pytest.raises(IncrementalResolutionError, match="not strictly later"):
        _resolve(backfill_delta, parent)


def test_delta_replacement_is_forbidden() -> None:
    current = _partition(_SESSION_1, receipt_name="p1")
    base = _manifest(
        "base-a",
        _ReleaseType.BASE,
        _SESSION_1,
        partitions=(current,),
    )
    replacement = replace(current, receipt=_artifact("p2"), availability_session=_SESSION_2)
    delta = _manifest(
        "delta-b",
        _ReleaseType.DELTA,
        _SESSION_2,
        parent=base,
        replacements=(_Replacement(current, replacement),),
    )
    with pytest.raises(IncrementalResolutionError, match="delta release cannot replace"):
        _resolve(delta, base)


def test_correction_requires_exact_current_same_key_receipt() -> None:
    current = _partition(_SESSION_1, receipt_name="p1")
    base = _manifest(
        "base-a",
        _ReleaseType.BASE,
        _SESSION_1,
        partitions=(current,),
    )
    successor = replace(current, receipt=_artifact("p2"), availability_session=_SESSION_2)
    correction = _manifest(
        "correction-c",
        _ReleaseType.CORRECTION,
        _SESSION_2,
        parent=base,
        replacements=(_Replacement(current, successor),),
    )
    resolved, _ = _resolve(correction, base)
    assert resolved.partition_receipts[current.key] == successor

    stale = replace(current, receipt=_artifact("stale"))
    stale_correction = replace(
        correction,
        manifest_pin=_pin("correction-stale", _SESSION_2),
        partition_replacements=(_Replacement(stale, successor),),
    )
    with pytest.raises(IncrementalResolutionError, match="exact current receipt"):
        _resolve(stale_correction, base)

    cross_key_old = replace(current, partition_key="2020-01-01")
    crossed = replace(
        correction,
        manifest_pin=_pin("correction-cross", _SESSION_2),
        partition_replacements=(_Replacement(cross_key_old, successor),),
    )
    with pytest.raises(IncrementalResolutionError, match="crossed logical keys"):
        _resolve(crossed, base)


def test_future_partition_replacement_corruption_is_validated_before_view_projection() -> None:
    current = _partition(_SESSION_1, receipt_name="p1")
    base = _manifest(
        "base-a",
        _ReleaseType.BASE,
        _SESSION_1,
        partitions=(current,),
    )
    stale = replace(current, receipt=_artifact("stale"))
    successor = replace(
        current,
        receipt=_artifact("future-replacement"),
        availability_session=_SESSION_3,
    )
    future_correction = _manifest(
        "correction-future",
        _ReleaseType.CORRECTION,
        _SESSION_3,
        parent=base,
        replacements=(_Replacement(stale, successor),),
    )

    with pytest.raises(IncrementalResolutionError, match="exact current receipt"):
        _resolve(
            future_correction,
            base,
            view=_ViewKind.HISTORICAL_AS_KNOWN,
            cutoff=_SESSION_2,
        )


@pytest.mark.parametrize(
    ("rows", "message"),
    [
        ((_row("v1", predecessor="missing"),), "missing predecessor"),
        (
            (
                _row("v1", stable="alias-a"),
                _row("v2", stable="alias-b", predecessor="v1"),
            ),
            "crossed stable keys",
        ),
        (
            (
                _row("v1", predecessor="v2"),
                _row("v2", predecessor="v1"),
            ),
            "introduced by a later release",
        ),
        ((_row("v1", tombstone=True),), "requires a reason"),
    ],
)
def test_invalid_row_version_graphs_fail_closed(
    rows: tuple[_RowVersion, ...],
    message: str,
) -> None:
    # Put each version in its own release so duplicate-operation validation does not mask the
    # graph invariant under test.
    base = _manifest("base-a", _ReleaseType.BASE, _SESSION_1, rows=(rows[0],))
    top = base
    chain: list[_Manifest] = [base]
    for index, row in enumerate(rows[1:], start=1):
        top = _manifest(
            f"correction-{index}",
            _ReleaseType.CORRECTION,
            _SESSION_2,
            parent=top,
            rows=(row,),
        )
        chain.append(top)

    with pytest.raises(IncrementalResolutionError, match=message):
        _resolve(top, *chain[:-1])


def test_row_version_fork_duplicate_and_multiple_roots_fail_closed() -> None:
    v1 = _row("v1")
    base = _manifest("base-a", _ReleaseType.BASE, _SESSION_1, rows=(v1,))
    v2 = _manifest(
        "correction-b",
        _ReleaseType.CORRECTION,
        _SESSION_2,
        parent=base,
        rows=(_row("v2", predecessor="v1", availability=_SESSION_2),),
    )
    fork = _manifest(
        "correction-c",
        _ReleaseType.CORRECTION,
        _SESSION_3,
        parent=v2,
        rows=(_row("v3", predecessor="v1", availability=_SESSION_3),),
    )
    with pytest.raises(IncrementalResolutionError, match="row-version fork"):
        _resolve(fork, v2, base)

    duplicate = replace(
        v2,
        manifest_pin=_pin("correction-duplicate", _SESSION_2),
        added_row_versions=(_row("v1", stable="other", availability=_SESSION_2),),
        superseded_row_version_ids=(),
    )
    with pytest.raises(IncrementalResolutionError, match="duplicate row version id"):
        _resolve(duplicate, base)

    second_root = _manifest(
        "correction-root",
        _ReleaseType.CORRECTION,
        _SESSION_2,
        parent=base,
        rows=(_row("v-root", availability=_SESSION_2),),
    )
    with pytest.raises(IncrementalResolutionError, match="multiple terminals"):
        _resolve(second_root, base)

    missing_supersession = replace(v2, superseded_row_version_ids=())
    with pytest.raises(IncrementalResolutionError, match="do not exactly match"):
        _resolve(missing_supersession, base)


def test_row_version_predecessor_cannot_be_introduced_by_future_release() -> None:
    base = _manifest(
        "base-a",
        _ReleaseType.BASE,
        _SESSION_1,
        rows=(_row("v2", predecessor="v1"),),
    )
    future = _manifest(
        "correction-future",
        _ReleaseType.CORRECTION,
        _SESSION_3,
        parent=base,
        rows=(_row("v1", availability=_SESSION_3),),
    )

    with pytest.raises(IncrementalResolutionError, match="introduced by a later release"):
        _resolve(future, base)


def test_old_foreign_key_remains_valid_after_successor_but_missing_fk_fails() -> None:
    v1 = _row("v1")
    partition = _partition(
        _SESSION_1,
        receipt_name="p1",
        references=(_Reference("ticker_alias", "v1"),),
    )
    base = _manifest(
        "base-a",
        _ReleaseType.BASE,
        _SESSION_1,
        partitions=(partition,),
        rows=(v1,),
    )
    correction = _manifest(
        "correction-c",
        _ReleaseType.CORRECTION,
        _SESSION_2,
        parent=base,
        rows=(_row("v2", predecessor="v1", availability=_SESSION_2),),
    )
    resolved, _ = _resolve(correction, base)
    assert resolved.terminal_row_versions[("ticker_alias", "alias-1")].row_version_id == "v2"
    assert (
        resolved.partition_receipts[partition.key].row_version_references[0].row_version_id == "v1"
    )

    broken_partition = replace(
        partition,
        row_version_references=(_Reference("ticker_alias", "missing"),),
    )
    broken = replace(base, added_partition_receipts=(broken_partition,))
    with pytest.raises(IncrementalResolutionError, match="missing historical row version"):
        _resolve(broken)


def test_clean_delta_compatibility_drift_fails_closed() -> None:
    base = _manifest("base-a", _ReleaseType.BASE, _SESSION_1)
    delta = _manifest(
        "delta-b",
        _ReleaseType.DELTA,
        _SESSION_2,
        parent=base,
        transform_semantics_digest="9" * 64,
    )
    with pytest.raises(IncrementalResolutionError, match="transform_semantics_digest"):
        _resolve(delta, base)


def test_correction_cannot_change_base_only_semantics_but_may_change_policy_bundle() -> None:
    base = _manifest("base-a", _ReleaseType.BASE, _SESSION_1)
    schema_drift = _manifest(
        "correction-schema",
        _ReleaseType.CORRECTION,
        _SESSION_2,
        parent=base,
        schema_digest="8" * 64,
    )
    with pytest.raises(IncrementalResolutionError, match="schema_digest"):
        _resolve(schema_drift, base)

    policy_update = _manifest(
        "correction-policy",
        _ReleaseType.CORRECTION,
        _SESSION_2,
        parent=base,
        identity_policy_bundle_id="7" * 64,
    )
    resolved, _ = _resolve(policy_update, base)
    assert resolved.compatibility_digests["identity_policy_bundle_id"] == "7" * 64

    delta_policy_drift = replace(
        policy_update,
        manifest_pin=_pin("delta-policy", _SESSION_2),
        release_type=_ReleaseType.DELTA,
    )
    with pytest.raises(IncrementalResolutionError, match="identity policy bundle"):
        _resolve(delta_policy_drift, base)


def test_receipt_availability_cannot_exceed_release_availability() -> None:
    late_partition = _partition(_SESSION_3, receipt_name="late")
    base = _manifest(
        "base-a",
        _ReleaseType.BASE,
        _SESSION_1,
        partitions=(late_partition,),
    )

    with pytest.raises(IncrementalResolutionError, match="partition receipt availability"):
        _resolve(base)

    late_row = _row("v1", availability=_SESSION_3)
    row_base = _manifest(
        "base-row",
        _ReleaseType.BASE,
        _SESSION_1,
        rows=(late_row,),
    )
    with pytest.raises(IncrementalResolutionError, match="row-version receipt availability"):
        _resolve(row_base)


def test_resolved_content_digest_excludes_external_release_lineage() -> None:
    first = _manifest("base-a", _ReleaseType.BASE, _SESSION_1)
    second = replace(first, manifest_pin=_pin("base-b", _SESSION_1, sha="e" * 64))

    first_snapshot, _ = _resolve(first)
    second_snapshot, _ = _resolve(second)

    assert first_snapshot.resolved_content_digest == second_snapshot.resolved_content_digest
    assert first_snapshot.lineage_digest != second_snapshot.lineage_digest
    assert first_snapshot.snapshot_digest != second_snapshot.snapshot_digest


def test_partition_cannot_reference_row_version_introduced_by_future_release() -> None:
    future_reference = _Reference("ticker_alias", "v2")
    base = _manifest(
        "base-a",
        _ReleaseType.BASE,
        _SESSION_1,
        partitions=(
            _partition(
                _SESSION_1,
                receipt_name="p1",
                references=(future_reference,),
            ),
        ),
        rows=(_row("v1"),),
    )
    correction = _manifest(
        "correction-c",
        _ReleaseType.CORRECTION,
        _SESSION_3,
        parent=base,
        rows=(_row("v2", predecessor="v1", availability=_SESSION_3),),
    )

    with pytest.raises(IncrementalResolutionError, match="introduced by a later release"):
        _resolve(
            correction,
            base,
            view=_ViewKind.HISTORICAL_AS_KNOWN,
            cutoff=_SESSION_2,
        )


def test_partition_and_replacement_cannot_predate_referenced_row_availability() -> None:
    late_row = _row("v1", availability=_SESSION_2)
    same_release = _manifest(
        "base-same-release-late-row",
        _ReleaseType.BASE,
        _SESSION_2,
        partitions=(
            _partition(
                _SESSION_1,
                receipt_name="early-partition",
                references=(_Reference("ticker_alias", "v1"),),
            ),
        ),
        rows=(late_row,),
    )
    with pytest.raises(IncrementalResolutionError, match="precedes its referenced row"):
        _resolve(same_release)

    v1 = _row("v1", availability=_SESSION_1)
    current = _partition(
        _SESSION_1,
        receipt_name="current",
        references=(_Reference("ticker_alias", "v1"),),
    )
    base = _manifest(
        "base-replacement-reference",
        _ReleaseType.BASE,
        _SESSION_1,
        partitions=(current,),
        rows=(v1,),
    )
    v2 = _row("v2", predecessor="v1", availability=_SESSION_3)
    replacement_receipt = replace(
        current,
        receipt=_artifact("replacement"),
        availability_session=_SESSION_2,
        row_version_references=(_Reference("ticker_alias", "v2"),),
    )
    correction = _manifest(
        "correction-early-replacement-reference",
        _ReleaseType.CORRECTION,
        _SESSION_3,
        parent=base,
        replacements=(_Replacement(current, replacement_receipt),),
        rows=(v2,),
    )
    with pytest.raises(IncrementalResolutionError, match="precedes its referenced row"):
        _resolve(correction, base)
