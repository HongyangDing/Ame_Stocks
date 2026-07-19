from __future__ import annotations

import hashlib
import inspect
import json
import stat
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
import test_silver_identity_materialization_streaming as full_fixture

from ame_stocks_api.cli import silver_identity_materialization_publish as publish_cli
from ame_stocks_api.silver import identity_materialization_publish as publish
from ame_stocks_api.silver import identity_materialization_streaming as streaming
from ame_stocks_api.silver.identity_materialization_publish import (
    S7IdentityPublishError,
    load_published_s7_release_set,
    prepare_s7_publish_plan,
    publish_s7_release_set,
    record_standing_s7_publish_approval,
)

_PLAN_TIME = datetime(2024, 1, 12, 14, 10, tzinfo=UTC)
_APPROVAL_TIME = datetime(2024, 1, 12, 14, 15, tzinfo=UTC)
_PUBLISH_TIME = datetime(2024, 1, 12, 14, 20, tzinfo=UTC)


def _loader(fixture: SimpleNamespace):
    return lambda *_args, **_kwargs: fixture.registries


def _completed_full(root: Path) -> SimpleNamespace:
    binding, registries, runtime = full_fixture._fixture(root)
    result = full_fixture._execute(root, binding, registries, runtime)
    full_plan, full_approval = full_fixture._controls(root, binding)
    return SimpleNamespace(
        root=root,
        binding=binding,
        registries=registries,
        runtime=runtime,
        full=result,
        full_plan=full_plan,
        full_approval=full_approval,
    )


def _approved_publish(root: Path) -> SimpleNamespace:
    fixture = _completed_full(root)
    plan, plan_pin = publish._prepare_s7_publish_plan_fixture(
        root,
        full_plan_id=fixture.full_plan["plan_id"],
        full_approval_id=fixture.full_approval.approval_id,
        expected_completion_id=fixture.full.completion_id,
        expected_candidate_id=fixture.full.candidate_id,
        prepared_by="publish-builder",
        runtime_probe=lambda: fixture.runtime,
        registry_loader=_loader(fixture),
        now=lambda: _PLAN_TIME,
    )
    approval, approval_pin = publish._record_standing_s7_publish_approval_fixture(
        root,
        publish_plan_id=plan["plan_id"],
        approved_by="joe",
        runtime_probe=lambda: fixture.runtime,
        registry_loader=_loader(fixture),
        now=lambda: _APPROVAL_TIME,
    )
    fixture.publish_plan = plan
    fixture.publish_plan_pin = plan_pin
    fixture.publish_approval = approval
    fixture.publish_approval_pin = approval_pin
    return fixture


def _run_publish(
    fixture: SimpleNamespace,
    *,
    now=lambda: _PUBLISH_TIME,
    checkpoint_hook=None,
):
    return publish._publish_s7_release_set_fixture(
        fixture.root,
        publish_plan_id=fixture.publish_plan["plan_id"],
        approval_id=fixture.publish_approval["approval_id"],
        runtime_probe=lambda: fixture.runtime,
        registry_loader=_loader(fixture),
        now=now,
        checkpoint_hook=checkpoint_hook,
    )


def _load_published(fixture: SimpleNamespace, release_set_id: str) -> dict[str, object]:
    return publish._load_published_s7_release_set_fixture(
        fixture.root,
        release_set_id=release_set_id,
        runtime_probe=lambda: fixture.runtime,
        registry_loader=_loader(fixture),
    )


def _upstream_artifact_path(fixture: SimpleNamespace, kind: str) -> Path:
    relative = {
        "s4_membership": fixture.binding.membership_artifacts[0].artifact.path,
        "gate_b_data": fixture.binding.gate_b.data.path,
        "gate_c_qa": fixture.binding.gate_c.qa.path,
        "registry_manifest": fixture.binding.registry_pins[0].manifest_path,
    }[kind]
    return fixture.root / relative


def _candidate_snapshot(fixture: SimpleNamespace) -> dict[str, tuple[int, int, int, str]]:
    candidate = fixture.root / fixture.full.candidate_path
    return {
        path.relative_to(candidate).as_posix(): (
            path.stat().st_ino,
            path.stat().st_mtime_ns,
            path.stat().st_size,
            hashlib.sha256(path.read_bytes()).hexdigest(),
        )
        for path in candidate.rglob("*")
        if path.is_file()
    }


def _intent_controls(fixture: SimpleNamespace):
    plan, plan_pin = publish._load_publish_plan(fixture.root, fixture.publish_plan["plan_id"])
    approval, approval_pin = publish._load_publish_approval(
        fixture.root,
        plan=plan,
        plan_pin=plan_pin,
        approval_id=fixture.publish_approval["approval_id"],
    )
    intent, intent_pin, documents = publish._load_intent(
        fixture.root,
        plan=plan,
        plan_pin=plan_pin,
        approval=approval,
        approval_pin=approval_pin,
    )
    marker = publish._release_set_document(
        plan=plan,
        plan_pin=plan_pin,
        approval=approval,
        approval_pin=approval_pin,
        intent=intent,
        intent_pin=intent_pin,
    )
    return plan, approval, intent, documents, marker


def _tamper_immutable(path: Path) -> None:
    content = bytearray(path.read_bytes())
    assert content
    content[-1] = ord(" ") if content[-1] != ord(" ") else ord("\n")
    path.chmod(0o644)
    path.write_bytes(content)
    path.chmod(0o444)


def test_publish_is_visibility_atomic_candidate_preserving_and_idempotent(
    tmp_path: Path,
) -> None:
    fixture = _approved_publish(tmp_path)
    before_candidate = _candidate_snapshot(fixture)
    first = _run_publish(fixture)

    assert first.idempotent is False
    assert first.state == "published"
    assert tuple(first.member_release_ids) == streaming.TABLE_ORDER
    assert first.release_available_session >= fixture.binding.cutoff_session
    marker_path = tmp_path / first.release_set_path
    marker_stat = marker_path.stat()
    assert stat.S_IMODE(marker_stat.st_mode) == 0o444
    assert marker_stat.st_nlink == 1
    marker = _load_published(fixture, first.release_set_id)
    assert marker["state"] == "published"
    assert [item["table_name"] for item in marker["members"]] == list(streaming.TABLE_ORDER)
    for item in marker["members"]:
        member_stat = (tmp_path / item["path"]).stat()
        assert stat.S_IMODE(member_stat.st_mode) == 0o444
        assert member_stat.st_nlink == 1
    assert _candidate_snapshot(fixture) == before_candidate

    repeated = _run_publish(
        fixture,
        now=lambda: (_ for _ in ()).throw(AssertionError("clock resampled")),
    )
    after = marker_path.stat()
    assert repeated.idempotent is True
    assert repeated.release_set_id == first.release_set_id
    assert (after.st_ino, after.st_mtime_ns, after.st_mode, after.st_nlink) == (
        marker_stat.st_ino,
        marker_stat.st_mtime_ns,
        marker_stat.st_mode,
        marker_stat.st_nlink,
    )
    assert _candidate_snapshot(fixture) == before_candidate


def test_publish_plan_and_approval_fixed_slots_preserve_first_runtime_records(
    tmp_path: Path,
) -> None:
    fixture = _approved_publish(tmp_path)
    plan_path = tmp_path / fixture.publish_plan_pin.path
    approval_path = tmp_path / fixture.publish_approval_pin.path
    plan_before = plan_path.stat()
    approval_before = approval_path.stat()

    replayed_plan, replayed_plan_pin = publish._prepare_s7_publish_plan_fixture(
        tmp_path,
        full_plan_id=fixture.full_plan["plan_id"],
        full_approval_id=fixture.full_approval.approval_id,
        expected_completion_id=fixture.full.completion_id,
        expected_candidate_id=fixture.full.candidate_id,
        prepared_by="publish-builder",
        runtime_probe=lambda: fixture.runtime,
        registry_loader=_loader(fixture),
        now=lambda: datetime(2030, 1, 1, tzinfo=UTC),
    )
    replayed_approval, replayed_approval_pin = publish._record_standing_s7_publish_approval_fixture(
        tmp_path,
        publish_plan_id=fixture.publish_plan["plan_id"],
        approved_by="joe",
        runtime_probe=lambda: fixture.runtime,
        registry_loader=_loader(fixture),
        now=lambda: datetime(2030, 1, 1, tzinfo=UTC),
    )
    assert replayed_plan == fixture.publish_plan
    assert replayed_plan_pin == fixture.publish_plan_pin
    assert replayed_approval == fixture.publish_approval
    assert replayed_approval_pin == fixture.publish_approval_pin
    plan_after = plan_path.stat()
    approval_after = approval_path.stat()
    assert (plan_after.st_ino, plan_after.st_mtime_ns) == (
        plan_before.st_ino,
        plan_before.st_mtime_ns,
    )
    assert (approval_after.st_ino, approval_after.st_mtime_ns) == (
        approval_before.st_ino,
        approval_before.st_mtime_ns,
    )
    with pytest.raises(S7IdentityPublishError, match="actor differs"):
        publish._record_standing_s7_publish_approval_fixture(
            tmp_path,
            publish_plan_id=fixture.publish_plan["plan_id"],
            approved_by="forked-actor",
            runtime_probe=lambda: fixture.runtime,
            registry_loader=_loader(fixture),
            now=lambda: datetime(2030, 1, 1, tzinfo=UTC),
        )


@pytest.mark.parametrize(
    ("crash_stage", "crash_table"),
    (
        ("intent_durable", None),
        *(("member_durable", table) for table in streaming.TABLE_ORDER),
        ("before_marker", None),
    ),
)
def test_publish_recovers_every_hidden_crash_prefix(
    tmp_path: Path, crash_stage: str, crash_table: str | None
) -> None:
    fixture = _approved_publish(tmp_path)
    tripped = False

    def crash(stage: str, table: str | None) -> None:
        nonlocal tripped
        if not tripped and (stage, table) == (crash_stage, crash_table):
            tripped = True
            raise RuntimeError(f"fixture crash {stage}:{table}")

    with pytest.raises(RuntimeError, match="fixture crash"):
        _run_publish(fixture, checkpoint_hook=crash)
    assert tripped is True
    _, _, _, _, marker = _intent_controls(fixture)
    marker_path = tmp_path / publish._release_set_path(marker["release_set_id"])
    assert not marker_path.exists()
    with pytest.raises(S7IdentityPublishError, match=r"missing|unsafe"):
        _load_published(fixture, marker["release_set_id"])

    recovered = _run_publish(fixture, now=lambda: datetime(2030, 1, 1, tzinfo=UTC))
    assert recovered.idempotent is False
    assert recovered.release_set_id == marker["release_set_id"]
    assert marker_path.is_file()


def test_crash_after_marker_retries_without_rewrite(tmp_path: Path) -> None:
    fixture = _approved_publish(tmp_path)

    def crash(stage: str, table: str | None) -> None:
        if (stage, table) == ("marker_durable", None):
            raise RuntimeError("after marker")

    with pytest.raises(RuntimeError, match="after marker"):
        _run_publish(fixture, checkpoint_hook=crash)
    _, _, _, _, marker = _intent_controls(fixture)
    marker_path = tmp_path / publish._release_set_path(marker["release_set_id"])
    before = marker_path.stat()
    recovered = _run_publish(fixture)
    after = marker_path.stat()
    assert recovered.idempotent is True
    assert (after.st_ino, after.st_mtime_ns, after.st_mode) == (
        before.st_ino,
        before.st_mtime_ns,
        before.st_mode,
    )


def test_foreign_member_is_never_overwritten_and_marker_stays_absent(
    tmp_path: Path,
) -> None:
    fixture = _approved_publish(tmp_path)

    def stop_after_intent(stage: str, table: str | None) -> None:
        if (stage, table) == ("intent_durable", None):
            raise RuntimeError("intent ready")

    with pytest.raises(RuntimeError, match="intent ready"):
        _run_publish(fixture, checkpoint_hook=stop_after_intent)
    _, _, intent, _, marker = _intent_controls(fixture)
    first_member = intent["members"][0]
    target = tmp_path / first_member["path"]
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(b"foreign")

    with pytest.raises(S7IdentityPublishError, match="cannot publish immutable"):
        _run_publish(fixture)
    assert target.read_bytes() == b"foreign"
    assert not (tmp_path / publish._release_set_path(marker["release_set_id"])).exists()


def test_tamper_in_intent_member_or_candidate_fails_closed(tmp_path: Path) -> None:
    intent_fixture = _approved_publish(tmp_path / "intent")

    def stop(stage: str, table: str | None) -> None:
        if (stage, table) == ("intent_durable", None):
            raise RuntimeError("stop")

    with pytest.raises(RuntimeError, match="stop"):
        _run_publish(intent_fixture, checkpoint_hook=stop)
    intent_path = (
        tmp_path
        / "intent"
        / publish._publish_intent_path(
            intent_fixture.publish_plan["plan_id"],
            intent_fixture.publish_approval["approval_id"],
        )
    )
    _tamper_immutable(intent_path)
    with pytest.raises(S7IdentityPublishError):
        _run_publish(intent_fixture)

    member_fixture = _approved_publish(tmp_path / "member")
    member_result = _run_publish(member_fixture)
    marker = _load_published(member_fixture, member_result.release_set_id)
    _tamper_immutable(member_fixture.root / marker["members"][0]["path"])
    with pytest.raises(S7IdentityPublishError):
        _load_published(member_fixture, member_result.release_set_id)

    data_fixture = _approved_publish(tmp_path / "data")
    data_result = _run_publish(data_fixture)
    output = data_fixture.publish_plan["members"][0]["output_receipts"][0]
    output_path = data_fixture.root / output["path"]
    output_path.write_bytes(output_path.read_bytes() + b"tamper")
    with pytest.raises(S7IdentityPublishError, match="candidate output"):
        _load_published(data_fixture, data_result.release_set_id)


@pytest.mark.parametrize(
    "upstream_kind",
    ("s4_membership", "gate_b_data", "gate_c_qa", "registry_manifest"),
)
def test_upstream_source_tamper_at_final_checkpoint_blocks_marker(
    tmp_path: Path, upstream_kind: str
) -> None:
    fixture = _approved_publish(tmp_path)
    upstream = _upstream_artifact_path(fixture, upstream_kind)
    tripped = False

    def tamper_before_marker(stage: str, table: str | None) -> None:
        nonlocal tripped
        if (stage, table) == ("before_marker", None):
            upstream.write_bytes(upstream.read_bytes() + b"tamper")
            tripped = True

    with pytest.raises(S7IdentityPublishError, match="source-chain replay failed"):
        _run_publish(fixture, checkpoint_hook=tamper_before_marker)
    assert tripped is True
    _, _, _, _, marker = _intent_controls(fixture)
    assert not (fixture.root / publish._release_set_path(marker["release_set_id"])).exists()


@pytest.mark.parametrize(
    "upstream_kind",
    ("s4_membership", "gate_b_data", "gate_c_qa", "registry_manifest"),
)
def test_upstream_source_tamper_after_marker_fails_exact_reader(
    tmp_path: Path, upstream_kind: str
) -> None:
    fixture = _approved_publish(tmp_path)
    upstream = _upstream_artifact_path(fixture, upstream_kind)
    tripped = False

    def tamper_after_marker(stage: str, table: str | None) -> None:
        nonlocal tripped
        if (stage, table) == ("marker_durable", None):
            upstream.write_bytes(upstream.read_bytes() + b"tamper")
            tripped = True

    with pytest.raises(S7IdentityPublishError, match="source-chain replay failed"):
        _run_publish(fixture, checkpoint_hook=tamper_after_marker)
    assert tripped is True
    _, _, _, _, marker = _intent_controls(fixture)
    marker_path = fixture.root / publish._release_set_path(marker["release_set_id"])
    assert marker_path.is_file()
    with pytest.raises(S7IdentityPublishError, match="source-chain replay failed"):
        _load_published(fixture, marker["release_set_id"])


def test_exact_reader_rejects_runtime_drift(tmp_path: Path) -> None:
    fixture = _approved_publish(tmp_path)
    result = _run_publish(fixture)
    drifted_runtime = {**fixture.runtime, "repository_commit": "4" * 40}

    with pytest.raises(S7IdentityPublishError, match="runtime differs"):
        publish._load_published_s7_release_set_fixture(
            fixture.root,
            release_set_id=result.release_set_id,
            runtime_probe=lambda: drifted_runtime,
            registry_loader=_loader(fixture),
        )


def test_full_completion_tamper_blocks_plan_before_publish_controls(tmp_path: Path) -> None:
    fixture = _completed_full(tmp_path)
    completion = tmp_path / fixture.full.completion_path
    completion.write_bytes(completion.read_bytes() + b"tamper")
    with pytest.raises(S7IdentityPublishError, match="Full publication replay failed"):
        publish._prepare_s7_publish_plan_fixture(
            tmp_path,
            full_plan_id=fixture.full_plan["plan_id"],
            full_approval_id=fixture.full_approval.approval_id,
            expected_completion_id=fixture.full.completion_id,
            expected_candidate_id=fixture.full.candidate_id,
            prepared_by="publish-builder",
            runtime_probe=lambda: fixture.runtime,
            registry_loader=_loader(fixture),
            now=lambda: _PLAN_TIME,
        )
    plans = tmp_path / "manifests/silver/identity/s7-four-table-publish-plans"
    assert not plans.exists()


def test_nonblocking_publish_lock_rejects_concurrent_writer_without_prefix(
    tmp_path: Path,
) -> None:
    fixture = _approved_publish(tmp_path)
    plan_id = fixture.publish_plan["plan_id"]
    lock = tmp_path / f"manifests/silver/locks/s7-four-table-publish-{plan_id}.lock"
    with (
        streaming._exclusive_nonblocking_lock(lock),
        pytest.raises(S7IdentityPublishError, match="nonblocking lock"),
    ):
        _run_publish(fixture)
    intent = tmp_path / publish._publish_intent_path(
        plan_id, fixture.publish_approval["approval_id"]
    )
    assert not intent.exists()


def test_production_api_and_cli_expose_only_exact_id_surface(tmp_path: Path) -> None:
    assert set(inspect.signature(prepare_s7_publish_plan).parameters) == {
        "data_root",
        "full_plan_id",
        "full_approval_id",
        "expected_completion_id",
        "expected_candidate_id",
        "prepared_by",
    }
    assert set(inspect.signature(record_standing_s7_publish_approval).parameters) == {
        "data_root",
        "publish_plan_id",
        "approved_by",
    }
    assert set(inspect.signature(publish_s7_release_set).parameters) == {
        "data_root",
        "publish_plan_id",
        "approval_id",
    }
    assert set(inspect.signature(load_published_s7_release_set).parameters) == {
        "data_root",
        "release_set_id",
    }
    parser = publish_cli.build_parser()
    commands = next(
        action.choices
        for action in parser._actions
        if isinstance(action.choices, dict) and "prepare-plan" in action.choices
    )
    assert tuple(commands) == (
        "prepare-plan",
        "approve-standing",
        "publish-release-set",
        "verify-release-set",
    )
    help_text = "\n".join(command.format_help() for command in commands.values())
    for forbidden in (
        "--adapter",
        "--clock",
        "--latest",
        "--now",
        "--output-path",
        "--receipts-json",
        "--source-rows",
    ):
        assert forbidden not in help_text
    with pytest.raises(S7IdentityPublishError, match="canonical production"):
        prepare_s7_publish_plan(
            tmp_path,
            full_plan_id="1" * 64,
            full_approval_id="2" * 64,
            expected_completion_id="3" * 64,
            expected_candidate_id="4" * 64,
            prepared_by="joe",
        )


def test_streaming_runtime_file_set_covers_production_semantic_dependencies() -> None:
    required = {
        "pyproject.toml",
        "backend/ame_stocks_api/artifacts.py",
        "backend/ame_stocks_api/providers/massive.py",
        "backend/ame_stocks_api/cli/silver_identity_market_sequence.py",
        "backend/ame_stocks_api/cli/silver_identity_materialization_publish.py",
        "backend/ame_stocks_api/cli/silver_identity_materialization_streaming.py",
        "backend/ame_stocks_api/silver/asset_contract.py",
        "backend/ame_stocks_api/silver/asset_full_run_plan.py",
        "backend/ame_stocks_api/silver/asset_publish_plan.py",
        "backend/ame_stocks_api/silver/asset_release_set.py",
        "backend/ame_stocks_api/silver/asset_source.py",
        "backend/ame_stocks_api/silver/assets.py",
        "backend/ame_stocks_api/silver/availability.py",
        "backend/ame_stocks_api/silver/calendar_artifact.py",
        "backend/ame_stocks_api/silver/contracts.py",
        "backend/ame_stocks_api/silver/exchange_contract.py",
        "backend/ame_stocks_api/silver/fixed_cases.py",
        "backend/ame_stocks_api/silver/identity_cross_market.py",
        "backend/ame_stocks_api/silver/identity_directional_raw_preview_approval.py",
        "backend/ame_stocks_api/silver/identity_directional_raw_preview_contract.py",
        "backend/ame_stocks_api/silver/identity_directional_raw_preview_execution_plan.py",
        "backend/ame_stocks_api/silver/identity_directional_raw_preview_manifest_plan.py",
        "backend/ame_stocks_api/silver/identity_directional_raw_preview_plan.py",
        "backend/ame_stocks_api/silver/identity_exact_group_history_approval.py",
        "backend/ame_stocks_api/silver/identity_exact_group_history_contract.py",
        "backend/ame_stocks_api/silver/identity_exact_group_history_manifest.py",
        "backend/ame_stocks_api/silver/identity_exact_group_history_plan.py",
        "backend/ame_stocks_api/silver/identity_exact_group_history_runner.py",
        "backend/ame_stocks_api/silver/identity_market_consistency.py",
        "backend/ame_stocks_api/silver/identity_market_inventory_engine.py",
        "backend/ame_stocks_api/silver/identity_market_sequence.py",
        "backend/ame_stocks_api/silver/identity_materialization_publish.py",
        "backend/ame_stocks_api/silver/identity_materialization_streaming.py",
        "backend/ame_stocks_api/silver/identity_registry_production.py",
        "backend/ame_stocks_api/silver/identity_registry_workflow.py",
        "backend/ame_stocks_api/silver/identity_relation_registries.py",
        "backend/ame_stocks_api/silver/identity_relation_registry_contract.py",
        "backend/ame_stocks_api/silver/identity_resolution.py",
        "backend/ame_stocks_api/silver/identity_resolution_contract.py",
        "backend/ame_stocks_api/silver/identity_source.py",
        "backend/ame_stocks_api/silver/reader.py",
        "backend/ame_stocks_api/silver/store.py",
        "backend/ame_stocks_api/silver/schema_resources/asset_master.schema-v1.json",
        "backend/ame_stocks_api/silver/schema_resources/asset_master.schema-v1.registry-v4.json",
        "backend/ame_stocks_api/silver/schema_resources/asset_transition.schema-v1.json",
        "backend/ame_stocks_api/silver/schema_resources/exchange_dim.schema-v1.json",
        "backend/ame_stocks_api/silver/schema_resources/identity_adjudication.schema-v1.json",
        "backend/ame_stocks_api/silver/schema_resources/identity_cross_market_adjudication.schema-v1.json",
        "backend/ame_stocks_api/silver/schema_resources/identity_directional_raw_preview_slot.schema-v1.json",
        "backend/ame_stocks_api/silver/schema_resources/identity_exact_group_history_review_slot.schema-v1.json",
        "backend/ame_stocks_api/silver/schema_resources/issuer_master.schema-v1.json",
        "backend/ame_stocks_api/silver/schema_resources/issuer_master.schema-v1.registry-v4.json",
        "backend/ame_stocks_api/silver/schema_resources/provider_composite_override.schema-v1.json",
        "backend/ame_stocks_api/silver/schema_resources/share_class_adjudication.schema-v1.json",
        "backend/ame_stocks_api/silver/schema_resources/ticker_alias.schema-v1.json",
        "backend/ame_stocks_api/silver/schema_resources/ticker_alias.schema-v1.registry-v4.json",
        "backend/ame_stocks_api/silver/schema_resources/ticker_event_request_status.schema-v1.json",
        "backend/ame_stocks_api/silver/schema_resources/ticker_type_dim.schema-v1.json",
        "backend/ame_stocks_api/silver/schema_resources/universe_daily.schema-v1.json",
        "backend/ame_stocks_api/silver/schema_resources/universe_daily.schema-v1.registry-v4.json",
        "backend/ame_stocks_api/silver/schema_resources/universe_source_daily.schema-v1.json",
        "backend/ame_stocks_api/silver/ticker_type_contract.py",
    }
    runtime_paths = streaming._RUNTIME_SOURCE_PATHS
    assert len(runtime_paths) == len(set(runtime_paths))
    assert required.issubset(runtime_paths)
    repository = Path(__file__).resolve().parents[1]
    assert all((repository / relative).is_file() for relative in runtime_paths)


def test_publish_documents_are_canonical_json(tmp_path: Path) -> None:
    fixture = _approved_publish(tmp_path)
    result = _run_publish(fixture)
    marker = json.loads((tmp_path / result.release_set_path).read_bytes())
    assert marker["release_set_id"] == result.release_set_id
    assert (tmp_path / result.release_set_path).read_bytes() == publish._canonical_bytes(marker)
