from __future__ import annotations

import hashlib
import json
import stat
from dataclasses import replace
from pathlib import Path

import pytest
from test_silver_asset_full import TABLES
from test_silver_asset_publish_plan import _full_ready_fixture

from ame_stocks_api.cli import silver_assets_release_set as cli_module
from ame_stocks_api.silver import asset_release_set
from ame_stocks_api.silver.asset_publish_plan import _create_asset_publish_plan
from ame_stocks_api.silver.asset_release_set import (
    APPROVAL_TEXT,
    APPROVAL_TEXT_SHA256,
    ASSET_PUBLICATION_SCOPE,
    AssetReleaseSet,
    AssetReleaseSetApproval,
    AssetReleaseSetIntent,
    _release_asset_publish_plan,
    release_asset_publish_plan,
    require_asset_release_set_membership,
)
from ame_stocks_api.silver.contracts import SilverContractError
from ame_stocks_api.silver.reader import (
    PublishedAssetEvidenceReader,
    PublishedSilverReader,
)
from ame_stocks_api.silver.store import SilverStoreError, WorkflowState

_RECORDED_AT = "2099-01-04T00:00:00+00:00"
_LATER_RECORDED_AT = "2099-01-05T00:00:00+00:00"


def _control_manifest_paths(data_root: Path, category: str) -> tuple[Path, ...]:
    return tuple(
        sorted((data_root / "manifests" / "silver" / category / "assets").glob("*/manifest.json"))
    )


def _assert_no_release_set_control_documents(data_root: Path) -> None:
    assert _control_manifest_paths(data_root, "release-set-approvals") == ()
    assert _control_manifest_paths(data_root, "release-set-intents") == ()
    assert _control_manifest_paths(data_root, "release-sets") == ()


def _assert_no_coordinator_events(fixture: object) -> None:
    store = fixture.plan.store
    for table in TABLES:
        assert all(
            record.event.actor != asset_release_set.COORDINATOR_ACTOR
            for record in store.workflow_events(fixture.plan.workflow_ids[table])
        )


def _tamper_immutable_file(path: Path) -> None:
    content = bytearray(path.read_bytes())
    assert content
    content[-1] = ord(" ") if content[-1] != ord(" ") else ord("\n")
    path.chmod(0o644)
    path.write_bytes(content)
    path.chmod(0o444)


def _release_fixture(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    fixture, _full, publish_arguments = _full_ready_fixture(tmp_path, monkeypatch)
    publish_run = _create_asset_publish_plan(fixture.plan.data_root, **publish_arguments)
    release_arguments = {
        "expected_publish_plan_id": publish_run.plan.plan_id,
        "expected_publish_plan_sha256": publish_run.document.sha256,
        "expected_publish_plan_bytes": publish_run.document.bytes,
        "expected_publish_plan_creator_commit": (publish_run.plan.orchestration_git_commit),
        "expected_materialization_commit": (publish_run.plan.materialization_git_commit),
        "expected_warning_result_ids_by_table": {
            item.table: item.warning_result_ids for item in publish_run.plan.tables
        },
        "repo_root": fixture.plan.repo_root,
        "release_orchestration_git_commit": fixture.plan.git_commit,
        "recorded_at": _RECORDED_AT,
        "git_verifier": lambda repo, commit, plan: None,
        "runtime_evidence_verifier": lambda root, plan: None,
    }
    return fixture, publish_run, release_arguments


def test_asset_release_set_is_visibility_atomic_and_idempotent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture, publish_run, arguments = _release_fixture(tmp_path, monkeypatch)

    first = _release_asset_publish_plan(fixture.plan.data_root, **arguments)

    assert first.idempotent is False
    assert first.release_set.publication_scope == ASSET_PUBLICATION_SCOPE
    assert first.release_set.backtest_identity_eligible is False
    assert first.release_set.runtime_review_accepted is True
    assert first.release_set.publish_plan_id == publish_run.plan.plan_id
    assert tuple(item.table for item in first.release_set.members) == TABLES
    assert all(
        snapshot.state is WorkflowState.PUBLISHED and snapshot.sequence == 10
        for snapshot in first.workflows_by_table.values()
    )
    assert first.approval.approval_text == APPROVAL_TEXT
    assert first.approval.approval_text_sha256 == APPROVAL_TEXT_SHA256
    assert first.approval.backtest_identity_eligible is False
    assert first.approval.runtime_review_accepted is True

    path = fixture.plan.data_root / first.document.path
    before = path.stat()
    assert stat.S_IMODE(before.st_mode) == 0o444
    assert before.st_nlink == 1
    assert AssetReleaseSet.from_dict(json.loads(path.read_bytes())) == first.release_set

    store = fixture.plan.store
    with pytest.raises(SilverStoreError, match="production authority"):
        require_asset_release_set_membership(
            fixture.plan.data_root, first.release_set.members[0].release_id
        )
    monkeypatch.setattr(
        asset_release_set,
        "_require_production_release_authority",
        lambda root, release_set: None,
    )
    for member in first.release_set.members:
        receipt, receipt_document = store.load_approval(member.approval_id)
        release, release_document = store.load_release(member.release_id)
        assert receipt.waived_qa_result_ids == member.warning_result_ids
        assert receipt.accepted_quarantine_issue_ids == ()
        assert receipt_document.sha256 == member.approval_sha256
        assert release_document.sha256 == member.release_sha256
        assert release.outputs == member.outputs
        assert (
            require_asset_release_set_membership(fixture.plan.data_root, member.release_id)
            == first.release_set
        )
        with pytest.raises(SilverStoreError, match="S7 identity eligibility is pending"):
            PublishedSilverReader(fixture.plan.data_root).inspect(member.release_id)
        evidence = PublishedAssetEvidenceReader(fixture.plan.data_root).inspect(member.release_id)
        assert evidence.release == release
        assert evidence.backtest_identity_eligible is False

    repeated = _release_asset_publish_plan(fixture.plan.data_root, **arguments)
    after = path.stat()
    assert repeated.idempotent is True
    assert repeated.release_set == first.release_set
    assert repeated.document == first.document
    assert (after.st_ino, after.st_mtime_ns, after.st_mode, after.st_nlink) == (
        before.st_ino,
        before.st_mtime_ns,
        before.st_mode,
        before.st_nlink,
    )
    assert all(
        len(store.workflow_events(fixture.plan.workflow_ids[table])) == 10 for table in TABLES
    )


def test_release_recorded_at_cannot_predate_any_full_ready_event(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture, _publish_run, arguments = _release_fixture(tmp_path, monkeypatch)
    arguments["recorded_at"] = "2099-01-02T23:59:59+00:00"

    with pytest.raises(SilverStoreError, match="predates full_ready"):
        _release_asset_publish_plan(fixture.plan.data_root, **arguments)

    _assert_no_release_set_control_documents(fixture.plan.data_root)
    _assert_no_coordinator_events(fixture)
    assert all(
        fixture.plan.store.status(fixture.plan.workflow_ids[table]).sequence == 8
        for table in TABLES
    )


@pytest.mark.parametrize(
    ("crash_stage", "crash_table"),
    (
        ("group_approval", None),
        ("intent", None),
        *(
            (stage, table)
            for stage in (
                "awaiting_publish",
                "publish_documents",
                "published",
            )
            for table in TABLES
        ),
        ("before_marker", None),
    ),
)
def test_asset_release_set_recovers_exact_crash_prefix_without_visibility(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    crash_stage: str,
    crash_table: str | None,
) -> None:
    fixture, _publish_run, arguments = _release_fixture(tmp_path, monkeypatch)
    tripped = False

    def crash(stage: str, table: str | None) -> None:
        nonlocal tripped
        if not tripped and stage == crash_stage and table == crash_table:
            tripped = True
            raise RuntimeError(f"fixture crash at {stage}:{table}")

    with pytest.raises(RuntimeError, match="fixture crash"):
        _release_asset_publish_plan(
            fixture.plan.data_root,
            **arguments,
            transition_barrier=crash,
        )
    assert tripped is True
    marker_paths = tuple(
        (fixture.plan.data_root / "manifests/silver/release-sets/assets").glob(
            "release_set_id=*/manifest.json"
        )
    )
    assert marker_paths == ()
    release_paths = tuple(
        (fixture.plan.data_root / "manifests/silver/releases").glob("release_id=*.json")
    )
    if release_paths:
        release_id = release_paths[0].stem.removeprefix("release_id=")
        with pytest.raises(SilverStoreError, match="release-set"):
            PublishedSilverReader(fixture.plan.data_root).inspect(release_id)

    recovered = _release_asset_publish_plan(fixture.plan.data_root, **arguments)
    assert recovered.idempotent is False
    assert all(
        snapshot.state is WorkflowState.PUBLISHED and snapshot.sequence == 10
        for snapshot in recovered.workflows_by_table.values()
    )
    assert (fixture.plan.data_root / recovered.document.path).is_file()


@pytest.mark.parametrize("crash_stage", ("group_approval", "intent"))
def test_release_rejects_a_second_authority_timestamp_after_control_crash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    crash_stage: str,
) -> None:
    fixture, _publish_run, arguments = _release_fixture(tmp_path, monkeypatch)

    def crash(stage: str, table: str | None) -> None:
        if stage == crash_stage and table is None:
            raise RuntimeError(f"fixture crash at {stage}")

    with pytest.raises(RuntimeError, match="fixture crash"):
        _release_asset_publish_plan(
            fixture.plan.data_root,
            **arguments,
            transition_barrier=crash,
        )

    before_approvals = _control_manifest_paths(fixture.plan.data_root, "release-set-approvals")
    before_intents = _control_manifest_paths(fixture.plan.data_root, "release-set-intents")
    assert len(before_approvals) == 1
    assert len(before_intents) == (1 if crash_stage == "intent" else 0)
    assert _control_manifest_paths(fixture.plan.data_root, "release-sets") == ()
    before_authority_stats = {
        path: (path.stat().st_ino, path.stat().st_mtime_ns, path.stat().st_mode)
        for path in (*before_approvals, *before_intents)
    }

    changed_arguments = {**arguments, "recorded_at": _LATER_RECORDED_AT}
    with pytest.raises(SilverStoreError, match="different immutable"):
        _release_asset_publish_plan(fixture.plan.data_root, **changed_arguments)

    assert (
        _control_manifest_paths(fixture.plan.data_root, "release-set-approvals") == before_approvals
    )
    assert _control_manifest_paths(fixture.plan.data_root, "release-set-intents") == before_intents
    assert _control_manifest_paths(fixture.plan.data_root, "release-sets") == ()
    assert {
        path: (path.stat().st_ino, path.stat().st_mtime_ns, path.stat().st_mode)
        for path in (*before_approvals, *before_intents)
    } == before_authority_stats
    _assert_no_coordinator_events(fixture)
    assert all(
        fixture.plan.store.status(fixture.plan.workflow_ids[table]).sequence == 8
        for table in TABLES
    )


def test_release_marker_barrier_crash_is_committed_and_exact_retry_is_idempotent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture, _publish_run, arguments = _release_fixture(tmp_path, monkeypatch)

    def crash_after_marker(stage: str, table: str | None) -> None:
        if stage == "marker" and table is None:
            raise RuntimeError("fixture crash after marker")

    with pytest.raises(RuntimeError, match="after marker"):
        _release_asset_publish_plan(
            fixture.plan.data_root,
            **arguments,
            transition_barrier=crash_after_marker,
        )

    marker_paths = _control_manifest_paths(fixture.plan.data_root, "release-sets")
    assert len(marker_paths) == 1
    marker_path = marker_paths[0]
    before = marker_path.stat()
    assert stat.S_IMODE(before.st_mode) == 0o444
    assert all(
        fixture.plan.store.status(fixture.plan.workflow_ids[table]).sequence == 10
        for table in TABLES
    )

    recovered = _release_asset_publish_plan(fixture.plan.data_root, **arguments)

    after = marker_path.stat()
    assert recovered.idempotent is True
    assert fixture.plan.data_root / recovered.document.path == marker_path
    assert (after.st_ino, after.st_mtime_ns, after.st_mode, after.st_nlink) == (
        before.st_ino,
        before.st_mtime_ns,
        before.st_mode,
        before.st_nlink,
    )
    assert all(
        len(fixture.plan.store.workflow_events(fixture.plan.workflow_ids[table])) == 10
        for table in TABLES
    )


def test_before_final_lock_rejects_a_competing_workflow_event_without_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture, _publish_run, arguments = _release_fixture(tmp_path, monkeypatch)
    raced_table = TABLES[0]
    workflow_id = fixture.plan.workflow_ids[raced_table]

    def race_workflow() -> None:
        current = fixture.plan.store.status(workflow_id)
        fixture.plan.store.request_publish(
            workflow_id,
            expected_event_sha256=current.event_sha256,
            actor="fixture-competing-publisher",
            created_at=_RECORDED_AT,
            note="competing request must not be adopted by the release set",
        )

    with pytest.raises(SilverStoreError, match="prefix changed"):
        _release_asset_publish_plan(
            fixture.plan.data_root,
            **arguments,
            before_final_lock=race_workflow,
        )

    _assert_no_release_set_control_documents(fixture.plan.data_root)
    _assert_no_coordinator_events(fixture)
    assert fixture.plan.store.status(workflow_id).sequence == 9
    assert (
        fixture.plan.store.workflow_events(workflow_id)[-1].event.actor
        == "fixture-competing-publisher"
    )
    assert all(
        fixture.plan.store.status(fixture.plan.workflow_ids[table]).sequence == 8
        for table in TABLES[1:]
    )


def test_before_final_lock_revalidates_publish_plan_bytes_before_control_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture, publish_run, arguments = _release_fixture(tmp_path, monkeypatch)
    plan_path = fixture.plan.data_root / publish_run.document.path

    with pytest.raises(SilverStoreError):
        _release_asset_publish_plan(
            fixture.plan.data_root,
            **arguments,
            before_final_lock=lambda: _tamper_immutable_file(plan_path),
        )

    _assert_no_release_set_control_documents(fixture.plan.data_root)
    _assert_no_coordinator_events(fixture)
    assert all(
        fixture.plan.store.status(fixture.plan.workflow_ids[table]).sequence == 8
        for table in TABLES
    )


def test_before_final_lock_revalidates_output_bytes_before_control_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture, publish_run, arguments = _release_fixture(tmp_path, monkeypatch)
    table_plan = publish_run.plan.tables[0]
    build, _ = fixture.plan.store.load_build(table_plan.table, table_plan.build_id)
    output_path = fixture.plan.data_root / next(
        output.path for output in build.outputs if output.role.value == "data"
    )

    with pytest.raises(SilverStoreError):
        _release_asset_publish_plan(
            fixture.plan.data_root,
            **arguments,
            before_final_lock=lambda: _tamper_immutable_file(output_path),
        )

    _assert_no_release_set_control_documents(fixture.plan.data_root)
    _assert_no_coordinator_events(fixture)
    assert all(
        fixture.plan.store.status(fixture.plan.workflow_ids[table]).sequence == 8
        for table in TABLES
    )


def test_final_lock_repeats_runtime_review_verification_before_control_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture, _publish_run, arguments = _release_fixture(tmp_path, monkeypatch)
    runtime_evidence_valid = True
    verification_count = 0

    def verify_runtime(root: Path, plan: object) -> None:
        nonlocal verification_count
        verification_count += 1
        if not runtime_evidence_valid:
            raise SilverStoreError("fixture runtime review evidence changed")

    def invalidate_runtime() -> None:
        nonlocal runtime_evidence_valid
        runtime_evidence_valid = False

    arguments["runtime_evidence_verifier"] = verify_runtime
    with pytest.raises(SilverStoreError, match="runtime review evidence changed"):
        _release_asset_publish_plan(
            fixture.plan.data_root,
            **arguments,
            before_final_lock=invalidate_runtime,
        )

    assert verification_count == 2
    _assert_no_release_set_control_documents(fixture.plan.data_root)
    _assert_no_coordinator_events(fixture)


def test_runtime_review_drift_before_marker_leaves_only_a_hidden_recoverable_prefix(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture, _publish_run, arguments = _release_fixture(tmp_path, monkeypatch)
    runtime_evidence_valid = True

    def verify_runtime(root: Path, plan: object) -> None:
        if not runtime_evidence_valid:
            raise SilverStoreError("fixture runtime review evidence changed")

    def invalidate_after_last_publish(stage: str, table: str | None) -> None:
        nonlocal runtime_evidence_valid
        if stage == "published" and table == TABLES[-1]:
            runtime_evidence_valid = False

    arguments["runtime_evidence_verifier"] = verify_runtime
    with pytest.raises(SilverStoreError, match="runtime review evidence changed"):
        _release_asset_publish_plan(
            fixture.plan.data_root,
            **arguments,
            transition_barrier=invalidate_after_last_publish,
        )

    assert _control_manifest_paths(fixture.plan.data_root, "release-sets") == ()
    assert len(_control_manifest_paths(fixture.plan.data_root, "release-set-approvals")) == 1
    assert len(_control_manifest_paths(fixture.plan.data_root, "release-set-intents")) == 1
    assert all(
        fixture.plan.store.status(fixture.plan.workflow_ids[table]).sequence == 10
        for table in TABLES
    )

    runtime_evidence_valid = True
    recovered = _release_asset_publish_plan(fixture.plan.data_root, **arguments)
    assert recovered.idempotent is False
    assert fixture.plan.data_root / recovered.document.path in _control_manifest_paths(
        fixture.plan.data_root, "release-sets"
    )


@pytest.mark.parametrize(
    ("field", "changed_value"),
    (
        ("workflow_id", "f" * 64),
        ("contract_id", "e" * 64),
        ("schema_version", 2),
        ("full_ready_event_sha256", "d" * 64),
        ("full_run_plan_id", "c" * 64),
        ("full_run_plan_sha256", "b" * 64),
        ("build_id", "a" * 64),
        ("build_manifest_sha256", "9" * 64),
        ("warning_result_ids", ("8" * 64,)),
    ),
)
def test_release_member_must_match_exact_publish_plan_table(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    changed_value: object,
) -> None:
    fixture, publish_run, arguments = _release_fixture(tmp_path, monkeypatch)
    run = _release_asset_publish_plan(fixture.plan.data_root, **arguments)
    member = run.release_set.members[0]
    table_plan = publish_run.plan.tables[0]

    with pytest.raises(SilverStoreError, match="does not match PublishPlan"):
        asset_release_set._assert_member_matches_publish_plan(
            replace(member, **{field: changed_value}),
            table_plan,
        )


def test_asset_release_set_rejects_warning_drift_before_control_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture, _publish_run, arguments = _release_fixture(tmp_path, monkeypatch)
    changed = dict(arguments["expected_warning_result_ids_by_table"])
    changed[TABLES[0]] = changed[TABLES[0]][1:]
    arguments["expected_warning_result_ids_by_table"] = changed

    with pytest.raises(SilverStoreError, match="warning result IDs changed"):
        _release_asset_publish_plan(fixture.plan.data_root, **arguments)

    assert not (fixture.plan.data_root / "manifests/silver/release-set-approvals").exists()
    assert all(
        fixture.plan.store.status(fixture.plan.workflow_ids[table]).sequence == 8
        for table in TABLES
    )


def test_public_asset_release_set_rejects_nonproduction_plan_before_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture, publish_run, _arguments = _release_fixture(tmp_path, monkeypatch)

    with pytest.raises(SilverStoreError, match="outside the authorized PublishPlan"):
        release_asset_publish_plan(
            fixture.plan.data_root,
            expected_publish_plan_id=publish_run.plan.plan_id,
            expected_publish_plan_sha256=publish_run.document.sha256,
            repo_root=fixture.plan.repo_root,
            release_orchestration_git_commit=fixture.plan.git_commit,
            recorded_at=_RECORDED_AT,
        )

    assert all(
        fixture.plan.store.status(fixture.plan.workflow_ids[table]).sequence == 8
        for table in TABLES
    )


def test_approval_text_digest_is_the_exact_user_authorization() -> None:
    assert APPROVAL_TEXT_SHA256 == (
        "d5f839d7ad5d6b37b11ca88556dff1f88c5cc707240d61e179b909f3a5e377c9"
    )
    assert hashlib.sha256(APPROVAL_TEXT.encode()).hexdigest() == APPROVAL_TEXT_SHA256


def test_production_authority_pins_exact_warning_and_empty_quarantine_maps() -> None:
    warning_map = dict(asset_release_set._CURRENT_WARNING_RESULT_IDS)

    assert set(warning_map) == set(TABLES)
    assert {table: len(warning_map[table]) for table in TABLES} == {
        "asset_observation_daily": 7,
        "asset_observation_version": 2,
        "universe_source_daily": 8,
    }
    all_warning_ids = tuple(result_id for table in TABLES for result_id in warning_map[table])
    assert len(all_warning_ids) == 17
    assert len(set(all_warning_ids)) == len(all_warning_ids)
    assert dict(asset_release_set._empty_quarantine_map()) == {table: () for table in TABLES}
    assert hashlib.sha256(APPROVAL_TEXT.encode("utf-8")).hexdigest() == (
        "d5f839d7ad5d6b37b11ca88556dff1f88c5cc707240d61e179b909f3a5e377c9"
    )


def test_release_set_parsers_reject_bool_versions_and_quarantine_map_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture, _publish_run, arguments = _release_fixture(tmp_path, monkeypatch)
    run = _release_asset_publish_plan(fixture.plan.data_root, **arguments)

    version_cases = (
        (
            AssetReleaseSetApproval.from_dict,
            run.approval.to_dict(),
            "asset_release_set_approval_version",
        ),
        (
            AssetReleaseSetIntent.from_dict,
            run.intent.to_dict(),
            "asset_release_set_intent_version",
        ),
        (
            AssetReleaseSet.from_dict,
            run.release_set.to_dict(),
            "asset_release_set_version",
        ),
    )
    for parser, valid_document, version_key in version_cases:
        changed = dict(valid_document)
        changed[version_key] = True
        with pytest.raises(SilverContractError, match="positive native int"):
            parser(changed)

    missing_map = run.approval.to_dict()
    del missing_map["accepted_quarantine_issue_ids_by_table"]
    with pytest.raises(SilverContractError, match="keys changed"):
        AssetReleaseSetApproval.from_dict(missing_map)

    missing_table = run.approval.to_dict()
    missing_table_quarantines = dict(missing_table["accepted_quarantine_issue_ids_by_table"])
    del missing_table_quarantines[TABLES[0]]
    missing_table["accepted_quarantine_issue_ids_by_table"] = missing_table_quarantines
    with pytest.raises(SilverContractError, match="exact three tables"):
        AssetReleaseSetApproval.from_dict(missing_table)

    nonempty_map = run.approval.to_dict()
    nonempty_quarantines = dict(nonempty_map["accepted_quarantine_issue_ids_by_table"])
    nonempty_quarantines[TABLES[0]] = ["a" * 64]
    nonempty_map["accepted_quarantine_issue_ids_by_table"] = nonempty_quarantines
    with pytest.raises(SilverContractError, match="must be empty"):
        AssetReleaseSetApproval.from_dict(nonempty_map)


def test_release_set_cli_prints_the_complete_approval_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    fixture, _publish_run, arguments = _release_fixture(tmp_path, monkeypatch)
    run = _release_asset_publish_plan(fixture.plan.data_root, **arguments)
    monkeypatch.setattr(
        cli_module,
        "release_asset_publish_plan",
        lambda *args, **kwargs: run,
    )

    assert (
        cli_module.main(
            [
                "--data-root",
                str(fixture.plan.data_root),
                "--repo-root",
                str(fixture.plan.repo_root),
                "--release-orchestration-git-commit",
                fixture.plan.git_commit,
                "--expected-publish-plan-id",
                run.release_set.publish_plan_id,
                "--expected-publish-plan-sha256",
                run.release_set.publish_plan_sha256,
                "--recorded-at",
                _RECORDED_AT,
            ]
        )
        == 0
    )
    output = json.loads(capsys.readouterr().out)

    assert output["approval_text_sha256"] == APPROVAL_TEXT_SHA256
    assert output["publish_plan_id"] == run.release_set.publish_plan_id
    assert output["publish_plan_sha256"] == run.release_set.publish_plan_sha256
    assert output["publish_plan_path"] == run.release_set.publish_plan_path
    assert output["publish_plan_bytes"] == run.release_set.publish_plan_bytes
    assert output["runtime_review"] == {
        **run.publish_plan.runtime_review.to_dict(),
        "accepted": True,
        "digest": run.release_set.runtime_review_digest,
    }
    assert output["accepted_quarantine_issue_ids_by_table"] == {
        table: [] for table in TABLES
    }
    warning_counts = {
        table: output["tables"][table]["warning_count"] for table in TABLES
    }
    assert warning_counts == {
        member.table: len(member.warning_result_ids)
        for member in run.release_set.members
    }
    assert all(
        output["tables"][table]["accepted_quarantine_issue_ids"] == []
        for table in TABLES
    )
    assert output["backtest_identity_eligible"] is False
