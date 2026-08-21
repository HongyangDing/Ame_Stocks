from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.cli import silver_identity_incremental as cli
from ame_stocks_api.silver.incremental_contract import ArtifactPin
from ame_stocks_api.silver.incremental_i3_production_contract import (
    I3ProductionResourceCaps,
    I3ProductionRunKind,
)


def _pin(label: str) -> ArtifactPin:
    return ArtifactPin(
        path=f"controls/{label}.json",
        sha256=stable_digest({"fixture": label}),
        bytes=10,
    )


def _delta_run_spec_pin(label: str) -> tuple[ArtifactPin, str]:
    run_spec_id = stable_digest({"fixture": f"{label}-run-spec-id"})
    return (
        ArtifactPin(
            path=(
                "manifests/silver/identity/s7-5-native-v2-staging/run-specs/"
                f"run_spec_id={run_spec_id}/run-spec.json"
            ),
            sha256=stable_digest({"fixture": label}),
            bytes=10,
        ),
        run_spec_id,
    )


def test_parser_exposes_one_command_factor_delta_path() -> None:
    parser = cli.build_parser()
    command_action = next(action for action in parser._actions if action.dest == "command")
    assert set(command_action.choices) == {
        "prepare-base",
        "prepare-delta",
        "run-delta",
        "backfill-missing-figi",
        "stage-base",
        "verify-base",
        "stage-delta",
        "verify-delta",
    }


def test_automatic_delta_config_uses_current_marker_and_next_exact_session(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    marker_path = tmp_path / cli.S75_CURRENT_MARKER_PATH
    marker_path.parent.mkdir(parents=True, exist_ok=True)
    marker_path.write_bytes(b'{"current":true}\n')
    parent_completion = _pin("current-parent-completion")
    parent_deep = _pin("current-parent-deep")
    caps = I3ProductionResourceCaps(
        rss_bytes_hard_cap=3 * 1024**3,
        disk_free_bytes_hard_floor=40 * 1024**3,
        temporary_bytes_hard_cap=32 * 1024**3,
        output_bytes_hard_cap=20 * 1024**3,
        output_rows_hard_cap=100_000_000,
    )
    current = SimpleNamespace(
        config=SimpleNamespace(
            delta_completion_artifact=parent_completion,
            delta_deep_attestation_artifact=parent_deep,
        ),
        run_spec=SimpleNamespace(
            terminal_session=date(2026, 7, 10),
            run_available_session=date(2026, 8, 3),
            resource_caps=caps,
            calendar=SimpleNamespace(
                calendar_artifact_id=stable_digest({"calendar": "id"}),
                artifact=SimpleNamespace(sha256=stable_digest({"calendar": "sha"})),
            ),
        ),
    )
    monkeypatch.setattr(cli, "verify_s75_completion", lambda root, pin: current)
    monkeypatch.setattr(
        cli,
        "load_xnys_calendar_artifact",
        lambda *args, **kwargs: SimpleNamespace(
            sessions=(
                SimpleNamespace(session_date=date(2026, 7, 10)),
                SimpleNamespace(session_date=date(2026, 7, 13)),
            )
        ),
    )
    receipt_relative = cli.production_i2_receipt_path(date(2026, 7, 13))
    receipt_path = tmp_path / receipt_relative
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    receipt_path.write_bytes(b'{"receipt":true}\n')
    monkeypatch.setattr(
        cli,
        "S4SessionRunReceipt",
        SimpleNamespace(
            from_dict=lambda value: SimpleNamespace(
                session_date=date(2026, 7, 13),
                receipt_available_session=date(2026, 8, 4),
            )
        ),
    )

    config = cli._automatic_delta_config(tmp_path)

    assert config.parent_completion_artifact == parent_completion
    assert config.parent_deep_attestation_artifact == parent_deep
    assert config.i2_receipt_artifact.path == receipt_relative
    assert config.run_available_session == date(2026, 8, 4)
    assert config.resource_caps is caps


def test_prepare_delta_direct_mode_stores_only_exact_control_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    completion = _pin("parent-completion")
    deep = _pin("parent-deep")
    i2 = ArtifactPin(
        path=(
            "manifests/silver/incremental/s4/assets/"
            "session_year=2026/session_date=2026-07-10/run-receipt.json"
        ),
        sha256=stable_digest({"fixture": "i2-receipt"}),
        bytes=10,
    )
    config_pin = _pin("delta-config")
    expected = object()
    observed: list[object] = []

    def store(root, config):
        observed.extend((root, config))
        return config_pin

    monkeypatch.setattr(cli, "store_i3_production_delta_config", store)
    monkeypatch.setattr(
        cli,
        "prepare_i3_production_delta_run_spec",
        lambda root, pin: expected if (root, pin) == (tmp_path, config_pin) else None,
    )
    args = cli.build_parser().parse_args(
        [
            "prepare-delta",
            "--data-root",
            str(tmp_path),
            "--parent-completion-path",
            completion.path,
            "--parent-completion-sha256",
            completion.sha256,
            "--parent-completion-bytes",
            str(completion.bytes),
            "--parent-deep-attestation-path",
            deep.path,
            "--parent-deep-attestation-sha256",
            deep.sha256,
            "--parent-deep-attestation-bytes",
            str(deep.bytes),
            "--i2-receipt-path",
            i2.path,
            "--i2-receipt-sha256",
            i2.sha256,
            "--i2-receipt-bytes",
            str(i2.bytes),
            "--run-available-session",
            "2026-08-07",
            "--rss-bytes-hard-cap",
            str(3 * 1024**3),
            "--disk-free-bytes-hard-floor",
            str(40 * 1024**3),
            "--temporary-bytes-hard-cap",
            str(32 * 1024**3),
            "--output-bytes-hard-cap",
            str(20 * 1024**3),
            "--output-rows-hard-cap",
            "100000000",
        ]
    )
    assert cli._prepare_delta(tmp_path, args) is expected
    config = observed[1]
    assert config.parent_completion_artifact == completion
    assert config.parent_deep_attestation_artifact == deep
    assert config.i2_receipt_artifact == i2
    assert config.parent_authority.value == "exact_staging"
    assert config.parent_pointer_event_artifact is None


def test_prepare_base_direct_mode_immutably_stores_module_owned_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    s7 = _pin("s7-release-set")
    s4 = _pin("s4-release-set")
    frontier = _pin("i2-base-frontier")
    config_pin = _pin("base-config")
    expected = object()
    observed: list[object] = []

    def store(root, config):
        observed.extend((root, config))
        return config_pin

    monkeypatch.setattr(cli, "store_i3_production_base_config", store)
    monkeypatch.setattr(
        cli,
        "prepare_i3_production_base_run_spec",
        lambda root, pin: expected if (root, pin) == (tmp_path, config_pin) else None,
    )
    args = cli.build_parser().parse_args(
        [
            "prepare-base",
            "--data-root",
            str(tmp_path),
            "--s7-release-set-path",
            s7.path,
            "--s7-release-set-sha256",
            s7.sha256,
            "--s7-release-set-bytes",
            str(s7.bytes),
            "--s4-release-set-path",
            s4.path,
            "--s4-release-set-sha256",
            s4.sha256,
            "--s4-release-set-bytes",
            str(s4.bytes),
            "--i2-base-frontier-path",
            frontier.path,
            "--i2-base-frontier-sha256",
            frontier.sha256,
            "--i2-base-frontier-bytes",
            str(frontier.bytes),
            "--run-available-session",
            "2026-08-07",
            "--rss-bytes-hard-cap",
            str(3 * 1024**3),
            "--disk-free-bytes-hard-floor",
            str(40 * 1024**3),
            "--temporary-bytes-hard-cap",
            str(32 * 1024**3),
            "--output-bytes-hard-cap",
            str(20 * 1024**3),
            "--output-rows-hard-cap",
            "100000000",
        ]
    )
    assert cli._prepare_base(tmp_path, args) is expected
    config = observed[1]
    assert config.s7_release_set_artifact == s7
    assert config.s4_release_set_artifact == s4
    assert config.i2_base_frontier_artifact == frontier
    assert not hasattr(config, "native_v2_migration_id")
    assert not hasattr(config, "transform_semantics_digest")


def test_prepare_base_rejects_mixed_or_incomplete_input_modes(tmp_path: Path) -> None:
    pin = _pin("config")
    mixed = cli.build_parser().parse_args(
        [
            "prepare-base",
            "--data-root",
            str(tmp_path),
            "--config-path",
            pin.path,
            "--config-sha256",
            pin.sha256,
            "--config-bytes",
            str(pin.bytes),
            "--run-available-session",
            "2026-08-07",
        ]
    )
    with pytest.raises(cli.I3ProductionCliError, match="either"):
        cli._prepare_base(tmp_path, mixed)

    incomplete = cli.build_parser().parse_args(
        ["prepare-base", "--data-root", str(tmp_path), "--run-available-session", "2026-08-07"]
    )
    with pytest.raises(cli.I3ProductionCliError, match="requires three exact pins"):
        cli._prepare_base(tmp_path, incomplete)


def test_prepare_delta_rejects_mixed_or_incomplete_input_modes(tmp_path: Path) -> None:
    pin = _pin("delta-config")
    mixed = cli.build_parser().parse_args(
        [
            "prepare-delta",
            "--data-root",
            str(tmp_path),
            "--config-path",
            pin.path,
            "--config-sha256",
            pin.sha256,
            "--config-bytes",
            str(pin.bytes),
            "--run-available-session",
            "2026-08-07",
        ]
    )
    with pytest.raises(cli.I3ProductionCliError, match="either"):
        cli._prepare_delta(tmp_path, mixed)

    incomplete = cli.build_parser().parse_args(
        ["prepare-delta", "--data-root", str(tmp_path), "--run-available-session", "2026-08-07"]
    )
    with pytest.raises(cli.I3ProductionCliError, match="requires parent completion"):
        cli._prepare_delta(tmp_path, incomplete)


def test_prepare_delta_cli_emits_exact_immutable_control_pins(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config = _pin("delta-config")
    run_spec = _pin("delta-run-spec")
    run_spec_id = stable_digest({"fixture": "delta-run-spec-id"})
    prepared = SimpleNamespace(
        config_artifact=config,
        run_spec_artifact=run_spec,
        run_spec=SimpleNamespace(run_spec_id=run_spec_id),
    )
    monkeypatch.setattr(cli, "_prepare_delta", lambda root, args: prepared)

    code = cli.main(
        [
            "prepare-delta",
            "--data-root",
            str(tmp_path),
            "--config-path",
            config.path,
            "--config-sha256",
            config.sha256,
            "--config-bytes",
            str(config.bytes),
        ]
    )

    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "command": "prepare-delta",
        "config": config.to_dict(),
        "publish_authorized": False,
        "run_kind": "delta",
        "run_spec": run_spec.to_dict(),
        "run_spec_id": run_spec_id,
        "state": "prepared",
    }


def test_stage_base_uses_exact_compact_input_loader(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_spec = SimpleNamespace(run_kind=I3ProductionRunKind.BASE)
    materializer = object()
    expected = object()
    monkeypatch.setattr(cli, "_load_run_spec", lambda _root, _pin: run_spec)
    monkeypatch.setattr(
        cli,
        "load_compact_base_materializer",
        lambda *, data_root, run_spec: materializer,
    )
    monkeypatch.setattr(
        cli,
        "stage_i3_production_base",
        lambda root, pin, *, materializer: expected,
    )
    assert cli._stage_base(tmp_path, _pin("run-spec")) is expected


def test_stage_delta_fails_closed_when_real_adapter_is_absent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_spec_pin, run_spec_id = _delta_run_spec_pin("run-spec")
    monkeypatch.setattr(
        cli,
        "_load_run_spec",
        lambda _root, _pin: SimpleNamespace(
            run_kind=I3ProductionRunKind.DELTA,
            run_spec_id=run_spec_id,
        ),
    )
    monkeypatch.setitem(
        sys.modules,
        "ame_stocks_api.silver.incremental_i3_delta_io",
        None,
    )
    with pytest.raises(cli.I3ProductionCliError, match="fail-closed"):
        cli._stage_delta(tmp_path, run_spec_pin)


def test_stage_delta_accepts_any_exact_run_spec_locator(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from ame_stocks_api.silver import incremental_i3_delta_io as delta_io

    path = "controls/operator-chosen-delta-run-spec.json"
    pin = ArtifactPin(
        path=path,
        sha256=stable_digest({"fixture": "copied-delta-run-spec"}),
        bytes=10,
    )
    run_spec = SimpleNamespace(run_kind=I3ProductionRunKind.DELTA)
    materializer = object()
    expected = object()
    monkeypatch.setattr(cli, "_load_run_spec", lambda _root, observed: run_spec)
    monkeypatch.setattr(
        delta_io,
        "load_production_delta_materializer",
        lambda *, data_root, run_spec: materializer,
    )
    monkeypatch.setattr(
        cli,
        "stage_i3_production_delta",
        lambda root, observed, *, materializer: expected,
    )
    assert cli._stage_delta(tmp_path, pin) is expected


def test_run_delta_prepares_stages_and_marks_factor_ready(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config = _pin("delta-config")
    run_spec_pin, run_spec_id = _delta_run_spec_pin("factor-run")
    completion = _pin("delta-completion")
    deep = _pin("delta-deep")
    parent_completion = _pin("parent-completion")
    parent_deep = _pin("parent-deep")
    external = _pin("external-figi-resolution")
    sentinel = _pin("S7_5_COMPLETE")
    prepared = SimpleNamespace(
        config_artifact=config,
        run_spec_artifact=run_spec_pin,
        run_spec=SimpleNamespace(
            run_spec_id=run_spec_id,
            run_available_session=date(2026, 8, 17),
            parent_shadow_completion_artifact=parent_completion,
            parent_deep_attestation_artifact=parent_deep,
        ),
    )
    loaded = SimpleNamespace(
        checkpoint=SimpleNamespace(checkpoint_id=stable_digest({"checkpoint": "delta"})),
        gate_a_manifest=SimpleNamespace(release_id=stable_digest({"gate-a": "delta"})),
        manifest=SimpleNamespace(release_id=stable_digest({"native": "delta"})),
        run_spec=SimpleNamespace(run_kind=I3ProductionRunKind.DELTA, run_spec_id=run_spec_id),
    )
    staged = SimpleNamespace(
        completion_pin=completion,
        deep_attestation_pin=deep,
        loaded=loaded,
        reused=False,
    )
    summary = SimpleNamespace(
        to_dict=lambda: {
            "terminal_session": "2026-07-10",
            "universe_row_count": 36000,
        }
    )
    ready = SimpleNamespace(summary=summary, sentinel_artifact=sentinel)
    ready_config_pin = _pin("factor-ready-config")
    prior = SimpleNamespace(
        config=SimpleNamespace(
            delta_completion_artifact=parent_completion,
            delta_deep_attestation_artifact=parent_deep,
            external_figi_resolution_artifact=external,
            completion_available_session=date(2026, 8, 24),
        )
    )
    observed_ready_configs: list[object] = []
    monkeypatch.setattr(cli, "_prepare_delta", lambda *_args: prepared)
    monkeypatch.setattr(cli, "_stage_delta", lambda *_args: staged)
    monkeypatch.setattr(cli, "_load_current_s75", lambda *_args, **_kwargs: prior)
    monkeypatch.setattr(
        cli,
        "prepare_s75_completion",
        lambda root, ready_config: (
            observed_ready_configs.append(ready_config) or ready_config,
            ready_config_pin,
        ),
    )
    monkeypatch.setattr(
        cli,
        "stage_s75_completion",
        lambda root, pin: ready,
    )

    code = cli.main(
        [
            "run-delta",
            "--data-root",
            str(tmp_path),
            "--config-path",
            config.path,
            "--config-sha256",
            config.sha256,
            "--config-bytes",
            str(config.bytes),
        ]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["s7_5_state"] == "factor_ready_for_s8"
    assert payload["s8_started"] is False
    assert payload["s7_5_completion"] == sentinel.to_dict()
    assert payload["factor_summary"]["terminal_session"] == "2026-07-10"
    assert observed_ready_configs[0].external_figi_resolution_artifact == external
    assert observed_ready_configs[0].completion_available_session == date(2026, 8, 24)


def test_backfill_missing_figi_updates_only_the_s75_marker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    release_pin = _pin("external-release")
    sentinel = _pin("S7_5_COMPLETE")
    summary = SimpleNamespace(
        to_dict=lambda: {
            "active_cs_missing_source_figi_rows": 1108,
            "externally_resolved_rows": 992,
            "remaining_rows": 116,
        }
    )
    backfill = SimpleNamespace(
        release_artifact=release_pin,
        release=SimpleNamespace(release_id=stable_digest({"external": "release"})),
        application_summary=summary,
        reused=False,
    )
    ready = SimpleNamespace(sentinel_artifact=sentinel)
    observed: list[object] = []

    def run(root, *, refresh_unresolved):
        observed.extend((root, refresh_unresolved))
        return backfill, ready

    monkeypatch.setattr(cli, "_backfill_missing_figi", run)
    code = cli.main(
        [
            "backfill-missing-figi",
            "--data-root",
            str(tmp_path),
            "--refresh-unresolved",
        ]
    )

    assert code == 0
    assert observed == [tmp_path.resolve(), True]
    payload = json.loads(capsys.readouterr().out)
    assert payload["external_figi_release"] == release_pin.to_dict()
    assert payload["external_figi_summary"]["externally_resolved_rows"] == 992
    assert payload["s7_5_completion"] == sentinel.to_dict()
    assert payload["s8_started"] is False
    assert payload["publish_authorized"] is False


def test_verify_cli_requires_exact_completion_and_deep_attestation_pins(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    completion = _pin("completion")
    deep = _pin("deep")
    loaded = SimpleNamespace(
        checkpoint=SimpleNamespace(checkpoint_id=stable_digest({"fixture": "checkpoint"})),
        gate_a_manifest=SimpleNamespace(release_id=stable_digest({"fixture": "gate-a"})),
        manifest=SimpleNamespace(release_id=stable_digest({"fixture": "native"})),
        run_spec=SimpleNamespace(
            run_kind=I3ProductionRunKind.BASE,
            run_spec_id=stable_digest({"fixture": "run-spec"}),
        ),
    )
    observed: list[object] = []

    def verify(root, completion_pin, deep_pin, *, expected_kind):
        observed.extend((root, completion_pin, deep_pin, expected_kind))
        return loaded

    monkeypatch.setattr(cli, "verify_i3_production_deep_attestation", verify)
    code = cli.main(
        [
            "verify-base",
            "--data-root",
            str(tmp_path),
            "--completion-path",
            completion.path,
            "--completion-sha256",
            completion.sha256,
            "--completion-bytes",
            str(completion.bytes),
            "--deep-attestation-path",
            deep.path,
            "--deep-attestation-sha256",
            deep.sha256,
            "--deep-attestation-bytes",
            str(deep.bytes),
        ]
    )
    assert code == 0
    assert observed[1:] == [completion, deep, I3ProductionRunKind.BASE]
    payload = json.loads(capsys.readouterr().out)
    assert payload["state"] == "awaiting_review"
    assert payload["publish_authorized"] is False
    assert payload["gate_a_release_id"] != payload["native_v2_envelope_id"]
