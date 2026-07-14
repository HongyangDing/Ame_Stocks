from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from ame_stocks_api.silver import asset_release_set
from ame_stocks_api.silver.contracts import (
    ApprovalDecision,
    ApprovalStage,
    BuildKind,
    SourceInventoryItem,
)
from ame_stocks_api.silver.reader import (
    PublishedAssetEvidenceReader,
    PublishedSilverReader,
)
from ame_stocks_api.silver.store import (
    SilverStore,
    SilverStoreError,
    StoredDocument,
    WorkflowState,
)

_RELEASE_ID = "1" * 64
_WORKFLOW_ID = "2" * 64
_BUILD_ID = "3" * 64
_BUILD_SHA = "4" * 64
_APPROVAL_ID = "5" * 64
_APPROVAL_SHA = "6" * 64
_RELEASE_SHA = "7" * 64
_PREVIOUS_EVENT_SHA = "8" * 64
_RELEASED_AT = "2026-07-14T00:00:00+00:00"


def _configured_reader(
    monkeypatch: pytest.MonkeyPatch,
    root: Path,
    *,
    table: str,
) -> tuple[PublishedSilverReader, SimpleNamespace]:
    reader = PublishedSilverReader(root)
    release = SimpleNamespace(
        workflow_id=_WORKFLOW_ID,
        release_id=_RELEASE_ID,
        contract_id="contract",
        domain="assets",
        table=table,
        schema_version=1,
        build_id=_BUILD_ID,
        build_manifest_sha256=_BUILD_SHA,
        approval_id=_APPROVAL_ID,
        approval_sha256=_APPROVAL_SHA,
        released_at=_RELEASED_AT,
        outputs=(),
    )
    release_document = StoredDocument(
        path=f"manifests/silver/releases/release_id={_RELEASE_ID}.json",
        sha256=_RELEASE_SHA,
        bytes=1,
    )
    contract = SimpleNamespace(
        contract_id="contract",
        domain="assets",
        table=table,
        schema_version=1,
    )
    build = SimpleNamespace(
        intent=SimpleNamespace(kind=BuildKind.FULL, workflow_id=_WORKFLOW_ID),
        build_id=_BUILD_ID,
        outputs=(),
    )
    build_document = StoredDocument(path="build.json", sha256=_BUILD_SHA, bytes=1)
    approval = SimpleNamespace(
        approval_id=_APPROVAL_ID,
        workflow_id=_WORKFLOW_ID,
        stage=ApprovalStage.PUBLISH,
        decision=ApprovalDecision.APPROVED,
        subject_id=_BUILD_ID,
        subject_manifest_sha256=_BUILD_SHA,
        expected_event_sha256=_PREVIOUS_EVENT_SHA,
        decided_at=_RELEASED_AT,
        waived_qa_result_ids=(),
        accepted_quarantine_issue_ids=(),
    )
    approval_document = StoredDocument(
        path="approval.json",
        sha256=_APPROVAL_SHA,
        bytes=1,
    )
    published_event = SimpleNamespace(
        to_state=WorkflowState.PUBLISHED,
        previous_event_sha256=_PREVIOUS_EVENT_SHA,
        created_at=_RELEASED_AT,
        evidence={
            "release_id": _RELEASE_ID,
            "release_path": release_document.path,
            "release_sha256": _RELEASE_SHA,
            "build_id": _BUILD_ID,
            "build_manifest_sha256": _BUILD_SHA,
            "approval_id": _APPROVAL_ID,
            "approval_path": approval_document.path,
            "approval_sha256": _APPROVAL_SHA,
        },
    )

    monkeypatch.setattr(
        reader.store,
        "load_release",
        lambda release_id: (release, release_document),
    )
    monkeypatch.setattr(
        reader.store,
        "verify_workflow_trust_chain",
        lambda workflow_id: SimpleNamespace(state=WorkflowState.PUBLISHED),
    )
    monkeypatch.setattr(
        reader.store,
        "workflow_events",
        lambda workflow_id: (SimpleNamespace(event=published_event),),
    )
    monkeypatch.setattr(
        reader.store,
        "load_workflow_contract",
        lambda workflow_id: (contract, object()),
    )
    monkeypatch.setattr(
        reader.store,
        "load_build",
        lambda loaded_table, build_id: (build, build_document),
    )
    monkeypatch.setattr(
        reader.store,
        "load_approval",
        lambda approval_id: (approval, approval_document),
    )
    monkeypatch.setattr(reader.store, "validate_build_manifest", lambda *args, **kwargs: None)
    monkeypatch.setattr(reader.store, "validate_qa_gate", lambda *args, **kwargs: None)
    return reader, release


def test_published_reader_fails_closed_before_s4_release_set_marker(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    reader, _ = _configured_reader(
        monkeypatch,
        tmp_path,
        table="asset_observation_daily",
    )
    monkeypatch.setattr(asset_release_set, "asset_release_requires_set", lambda table: True)

    def reject_missing_marker(data_root: Path, release_id: str) -> None:
        raise SilverStoreError("S4 release-set marker is missing")

    monkeypatch.setattr(
        asset_release_set,
        "require_asset_release_set_membership",
        reject_missing_marker,
    )

    with pytest.raises(SilverStoreError, match="release-set marker is missing"):
        reader.inspect(_RELEASE_ID)


def test_generic_published_reader_rejects_s4_member_after_release_set_marker(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    reader, _ = _configured_reader(
        monkeypatch,
        tmp_path,
        table="asset_observation_daily",
    )
    calls: list[tuple[Path, str]] = []
    monkeypatch.setattr(asset_release_set, "asset_release_requires_set", lambda table: True)
    monkeypatch.setattr(
        asset_release_set,
        "require_asset_release_set_membership",
        lambda data_root, release_id: calls.append((data_root, release_id)) or object(),
    )

    for access in (reader.inspect, reader.data_files):
        with pytest.raises(SilverStoreError, match="S7 identity eligibility is pending"):
            access(_RELEASE_ID)

    assert calls == [
        (tmp_path.resolve(), _RELEASE_ID),
        (tmp_path.resolve(), _RELEASE_ID),
    ]


def test_evidence_reader_accepts_s4_member_after_release_set_marker(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    generic_reader, release = _configured_reader(
        monkeypatch,
        tmp_path,
        table="asset_observation_version",
    )
    reader = PublishedAssetEvidenceReader(tmp_path)
    reader._reader = generic_reader
    calls: list[tuple[str, object]] = []

    def require_membership(data_root: Path, release_id: str) -> SimpleNamespace:
        calls.append(("membership", (data_root, release_id)))
        return SimpleNamespace(
            publication_scope=asset_release_set.ASSET_PUBLICATION_SCOPE,
            backtest_identity_eligible=False,
        )

    original_load_release = generic_reader.store.load_release

    def load_release(release_id: str):
        calls.append(("release", release_id))
        return original_load_release(release_id)

    monkeypatch.setattr(
        asset_release_set,
        "require_asset_release_set_membership",
        require_membership,
    )
    monkeypatch.setattr(asset_release_set, "asset_release_requires_set", lambda table: True)
    monkeypatch.setattr(generic_reader.store, "load_release", load_release)

    published = reader.inspect(_RELEASE_ID)

    assert published.release is release
    assert published.data_paths == ()
    assert published.publication_scope == asset_release_set.ASSET_PUBLICATION_SCOPE
    assert published.backtest_identity_eligible is False
    assert calls == [
        ("release", _RELEASE_ID),
        ("membership", (tmp_path.resolve(), _RELEASE_ID)),
    ]


def test_published_reader_does_not_require_set_for_earlier_silver_tables(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    reader, release = _configured_reader(monkeypatch, tmp_path, table="exchanges")
    monkeypatch.setattr(asset_release_set, "asset_release_requires_set", lambda table: False)
    monkeypatch.setattr(
        asset_release_set,
        "require_asset_release_set_membership",
        lambda data_root, release_id: pytest.fail("non-S4 release requested set membership"),
    )

    assert reader.inspect(_RELEASE_ID).release is release


def _configured_lineage_store(
    monkeypatch: pytest.MonkeyPatch,
    root: Path,
    *,
    table: str,
) -> tuple[SilverStore, SimpleNamespace, SourceInventoryItem]:
    store = SilverStore(root)
    output = SimpleNamespace(
        path=f"silver/assets/{table}/part-000.parquet",
        sha256="9" * 64,
        bytes=100,
        row_count=2,
        media_type="application/vnd.apache.parquet",
        table=table,
        schema_digest="a" * 64,
    )
    release = SimpleNamespace(
        release_id=_RELEASE_ID,
        workflow_id=_WORKFLOW_ID,
        table=table,
        outputs=(output,),
    )
    upstream = SimpleNamespace(
        path=f"manifests/silver/releases/release_id={_RELEASE_ID}.json",
        sha256=_RELEASE_SHA,
    )
    inventory = SimpleNamespace(upstream_manifests=(upstream,))
    expected_item = SourceInventoryItem(
        path=output.path,
        sha256=output.sha256,
        bytes=output.bytes,
        row_count=output.row_count,
        media_type=output.media_type,
        table=output.table,
        schema_digest=output.schema_digest,
    )
    monkeypatch.setattr(
        store,
        "load_release",
        lambda release_id: (
            release,
            StoredDocument(path=upstream.path, sha256=_RELEASE_SHA, bytes=1),
        ),
    )
    monkeypatch.setattr(store, "verify_workflow_trust_chain", lambda workflow_id: object())
    return store, inventory, expected_item


def test_published_silver_lineage_fails_closed_before_s4_release_set_marker(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    store, inventory, _ = _configured_lineage_store(
        monkeypatch,
        tmp_path,
        table="universe_source_daily",
    )
    monkeypatch.setattr(asset_release_set, "asset_release_requires_set", lambda table: True)

    def reject_missing_marker(data_root: Path, release_id: str) -> None:
        raise SilverStoreError("S4 release-set marker is missing")

    monkeypatch.setattr(
        asset_release_set,
        "require_asset_release_set_membership",
        reject_missing_marker,
    )

    with pytest.raises(SilverStoreError, match="release-set marker is missing"):
        store._published_source_items(inventory)


def test_published_silver_lineage_rejects_complete_s4_set_and_ignores_non_s4(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    store, inventory, _ = _configured_lineage_store(
        monkeypatch,
        tmp_path,
        table="asset_observation_version",
    )
    calls: list[tuple[Path, str]] = []
    monkeypatch.setattr(asset_release_set, "asset_release_requires_set", lambda table: True)
    monkeypatch.setattr(
        asset_release_set,
        "require_asset_release_set_membership",
        lambda data_root, release_id: calls.append((data_root, release_id)) or object(),
    )

    with pytest.raises(SilverStoreError, match="S7 identity eligibility is pending"):
        store._published_source_items(inventory)
    assert calls == [(tmp_path.resolve(), _RELEASE_ID)]

    non_s4_store, non_s4_inventory, non_s4_item = _configured_lineage_store(
        monkeypatch,
        tmp_path / "non-s4",
        table="ticker_types",
    )
    monkeypatch.setattr(asset_release_set, "asset_release_requires_set", lambda table: False)
    monkeypatch.setattr(
        asset_release_set,
        "require_asset_release_set_membership",
        lambda data_root, release_id: pytest.fail("non-S4 lineage requested set membership"),
    )
    assert non_s4_store._published_source_items(non_s4_inventory) == {non_s4_item}
