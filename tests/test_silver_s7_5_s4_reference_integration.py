from __future__ import annotations

import shutil
from pathlib import Path

import pytest
from test_silver_exchanges import (
    _advance_release_git_checkout as advance_exchange_release_checkout,
)
from test_silver_exchanges import (
    _exchange_code_ready,
    exchange_preview_module,
    exchange_release_module,
)
from test_silver_exchanges import (
    _init_release_git_checkout as init_exchange_release_checkout,
)
from test_silver_exchanges import _run_fixture_preview as run_exchange_preview
from test_silver_exchanges import _write_preview_fixture as write_exchange_fixture
from test_silver_ticker_type_release import _prepare_reviewed_preview
from test_silver_ticker_type_release import _release_arguments as ticker_release_arguments
from test_silver_ticker_type_release import release_module as ticker_release_module

from ame_stocks_api.silver import asset_incremental as incremental
from ame_stocks_api.silver.incremental_contract import ArtifactPin


def _published_exchange_fixture(
    root: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    data_root = root / "data"
    fixture = write_exchange_fixture(data_root)
    repo_root, preview_module, preview_commit = init_exchange_release_checkout(root)
    monkeypatch.setattr(exchange_preview_module, "__file__", str(preview_module))
    _, workflow_id, event_sha256 = _exchange_code_ready(data_root)
    preview = run_exchange_preview(
        data_root,
        workflow_id=workflow_id,
        event_sha256=event_sha256,
        repo_root=repo_root,
        git_commit=preview_commit,
        fixture=fixture,
    )
    release_module, runner_commit = advance_exchange_release_checkout(repo_root)
    monkeypatch.setattr(exchange_release_module, "__file__", str(release_module))
    completed = exchange_release_module._complete_exchange_release_authorized(
        data_root,
        workflow_id=workflow_id,
        expected_event_sha256=preview.workflow.event_sha256,
        reviewed_preview_build_id=preview.build.build_id,
        reviewed_preview_manifest_sha256=preview.build_document.sha256,
        repo_root=repo_root,
        runner_git_commit=runner_commit,
        actor="s7-5-reference-integration-runner",
        approver="s7-5-fixture-reviewer",
        authorization=fixture.authorization,
    )
    return data_root, completed


def test_s4_reference_binding_is_rebuilt_from_two_real_published_release_chains(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    exchange_root, exchange = _published_exchange_fixture(tmp_path / "exchange", monkeypatch)
    ticker_reviewed = _prepare_reviewed_preview(tmp_path / "ticker", monkeypatch)
    ticker = ticker_release_module._complete_ticker_type_release_authorized(
        ticker_reviewed.data_root,
        expected_event_sha256=ticker_reviewed.preview.workflow.event_sha256,
        **ticker_release_arguments(ticker_reviewed),
    )

    combined = tmp_path / "combined"
    shutil.copytree(exchange_root, combined)
    shutil.copytree(ticker_reviewed.data_root, combined, dirs_exist_ok=True)
    pins = tuple(
        sorted(
            (
                ArtifactPin(
                    path=exchange.release_document.path,
                    sha256=exchange.release_document.sha256,
                    bytes=exchange.release_document.bytes,
                ),
                ArtifactPin(
                    path=ticker.release_document.path,
                    sha256=ticker.release_document.sha256,
                    bytes=ticker.release_document.bytes,
                ),
            ),
            key=lambda item: item.path,
        )
    )

    binding = incremental._load_reference_binding_from_exact_releases(combined, pins)
    assert {"CS", "ETF"}.issubset(binding.ticker_types)
    assert {"X001", "X027"}.issubset(binding.exchange_mics)
    assert binding.dependency_pins == pins
