from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from ame_stocks_api.cli import silver_identity_directional_raw_preview_run as cli


def test_cli_exposes_only_exact_control_ids_and_no_scope_override(monkeypatch, capsys) -> None:
    captured = {}

    def fake_bootstrap(
        root,
        *,
        plan_id,
        expected_plan_sha256,
        approval_id,
        expected_approval_sha256,
    ):
        captured["bootstrap"] = (
            root,
            plan_id,
            expected_plan_sha256,
            approval_id,
            expected_approval_sha256,
        )

    def fake_runner(root, **kwargs):
        captured["runner"] = (root, kwargs)
        return SimpleNamespace(
            candidate_id="5" * 64,
            completion_id="6" * 64,
            plan_id="1" * 64,
            approval_id="3" * 64,
            completion_state="awaiting_review",
        )

    monkeypatch.setattr(cli, "_bootstrap_exact_checkout", fake_bootstrap)
    monkeypatch.setattr(cli, "_load_runner", lambda: (fake_runner, RuntimeError))
    assert (
        cli.main(
            [
                "--data-root",
                "/tmp",
                "--plan-id",
                "1" * 64,
                "--plan-sha256",
                "2" * 64,
                "--approval-id",
                "3" * 64,
                "--approval-sha256",
                "4" * 64,
            ]
        )
        == 0
    )
    assert set(captured["runner"][1]) == {
        "plan_id",
        "expected_plan_sha256",
        "approval_id",
        "expected_approval_sha256",
    }
    output = json.loads(capsys.readouterr().out)
    assert output["state"] == "awaiting_review"


def test_cli_rejects_ticker_or_date_scope_arguments() -> None:
    parser = cli.build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "--data-root",
                "/tmp",
                "--plan-id",
                "1" * 64,
                "--plan-sha256",
                "2" * 64,
                "--approval-id",
                "3" * 64,
                "--approval-sha256",
                "4" * 64,
                "--ticker",
                "SOR",
            ]
        )


def test_cli_fails_closed_before_runner_when_bootstrap_fails(monkeypatch) -> None:
    monkeypatch.setattr(
        cli,
        "_bootstrap_exact_checkout",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            cli.DirectionalRawPreviewRunBootstrapError("drift")
        ),
    )
    monkeypatch.setattr(
        cli,
        "_load_runner",
        lambda: (_ for _ in ()).throw(AssertionError("runner imported")),
    )
    with pytest.raises(SystemExit, match="bootstrap failed"):
        cli.main(
            [
                "--data-root",
                "/tmp",
                "--plan-id",
                "1" * 64,
                "--plan-sha256",
                "2" * 64,
                "--approval-id",
                "3" * 64,
                "--approval-sha256",
                "4" * 64,
            ]
        )


def test_cli_runner_import_failure_is_bootstrap_error_not_unboundlocal(monkeypatch) -> None:
    monkeypatch.setattr(cli, "_bootstrap_exact_checkout", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        cli,
        "_load_runner",
        lambda: (_ for _ in ()).throw(ImportError("missing pinned runner")),
    )
    with pytest.raises(SystemExit, match="bootstrap failed"):
        cli.main(
            [
                "--data-root",
                "/tmp",
                "--plan-id",
                "1" * 64,
                "--plan-sha256",
                "2" * 64,
                "--approval-id",
                "3" * 64,
                "--approval-sha256",
                "4" * 64,
            ]
        )


def test_bootstrap_pin_sets_require_runner_paths_and_exact_digests() -> None:
    def pins(paths):
        return [
            {
                "bytes": 1,
                "git_blob": "1" * 40,
                "path": path,
                "sha256": "2" * 64,
            }
            for path in sorted(paths)
        ]

    runtime = pins(cli._REQUIRED_RUNTIME_PATHS)
    verification = pins(cli._REQUIRED_VERIFICATION_PATHS)
    cli._verify_pin_set_bindings(
        runtime,
        verification,
        runtime_digest=cli._stable_digest(runtime),
        verification_digest=cli._stable_digest(verification),
    )
    with pytest.raises(cli.DirectionalRawPreviewRunBootstrapError, match="required paths"):
        cli._verify_pin_set_bindings(
            runtime[1:],
            verification,
            runtime_digest=cli._stable_digest(runtime[1:]),
            verification_digest=cli._stable_digest(verification),
        )
    with pytest.raises(cli.DirectionalRawPreviewRunBootstrapError, match="digest"):
        cli._verify_pin_set_bindings(
            runtime,
            verification,
            runtime_digest="f" * 64,
            verification_digest=cli._stable_digest(verification),
        )
