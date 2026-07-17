from __future__ import annotations

import ast
import hashlib
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

import ame_stocks_api.cli.silver_identity_market_inventory_execution_request as module
from ame_stocks_api.silver.identity_market_inventory_contract import (
    COMPOSITE_FIGI_INVENTORY_CONTRACT_ID,
    COMPOSITE_FIGI_INVENTORY_RESOURCE_SHA256,
    COMPOSITE_FIGI_INVENTORY_SCHEMA_DIGEST,
)
from ame_stocks_api.silver.identity_market_inventory_execution_plan import (
    IdentityMarketInventoryExecutionPlanError,
    IdentityMarketInventoryExecutionPlanStore,
    S7InventoryCandidateContractPin,
    S7InventoryRuntimeFilePin,
)

RECORDED_AT = datetime(2026, 7, 17, 5, 0, tzinfo=UTC)


def _pin(path: str) -> S7InventoryRuntimeFilePin:
    content = path.encode()
    return S7InventoryRuntimeFilePin(
        path=path,
        git_blob=hashlib.sha1(b"blob " + str(len(content)).encode() + b"\0" + content).hexdigest(),
        sha256=hashlib.sha256(content).hexdigest(),
        bytes=len(content),
    )


def _contract() -> S7InventoryCandidateContractPin:
    return S7InventoryCandidateContractPin(
        contract_id=COMPOSITE_FIGI_INVENTORY_CONTRACT_ID,
        schema_digest=COMPOSITE_FIGI_INVENTORY_SCHEMA_DIGEST,
        candidate_sha256=COMPOSITE_FIGI_INVENTORY_RESOURCE_SHA256,
        resource_sha256=COMPOSITE_FIGI_INVENTORY_RESOURCE_SHA256,
    )


def _patch_preflight(
    monkeypatch: pytest.MonkeyPatch,
    repository: Path,
) -> None:
    monkeypatch.setattr(
        module,
        "_verify_exact_clean_checkout",
        lambda repo_root, git_commit: (repository.resolve(), "b" * 40),
    )
    monkeypatch.setattr(module, "_verify_contract_bytes", lambda repo_root: _contract())
    monkeypatch.setattr(
        module,
        "_pin_tracked_files",
        lambda repo_root, paths: tuple(_pin(path) for path in sorted(paths)),
    )
    monkeypatch.setattr(
        IdentityMarketInventoryExecutionPlanStore,
        "load_exact_v1_controls",
        lambda self: (
            object(),
            SimpleNamespace(created_at_utc=RECORDED_AT - timedelta(days=1)),
        ),
    )


def _run(data_root: Path, repository: Path):
    return module.create_s7_inventory_execution_request(
        data_root,
        repo_root=repository,
        git_commit="a" * 40,
        recorded_at=RECORDED_AT.isoformat(),
        blocked_recorded_by="fixture-v1-block-recorder",
        plan_created_by="fixture-v2-planner",
        request_created_by="fixture-v2-requester",
    )


def test_request_orchestration_writes_only_three_controls_idempotently(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = tmp_path / "repo"
    repository.mkdir()
    data_root = tmp_path / "data"
    data_root.mkdir()
    _patch_preflight(monkeypatch, repository)

    first = _run(data_root, repository)
    files_before = tuple(
        sorted(
            path.relative_to(data_root).as_posix()
            for path in data_root.rglob("*")
            if path.is_file()
        )
    )
    second = _run(data_root, repository)
    files_after = tuple(
        sorted(
            path.relative_to(data_root).as_posix()
            for path in data_root.rglob("*")
            if path.is_file()
        )
    )

    assert first.all_documents_preexisting is False
    assert second.all_documents_preexisting is True
    assert first.plan == second.plan
    assert first.request == second.request
    assert files_before == files_after
    assert len(files_after) == 3
    assert all(path.endswith("manifest.json") for path in files_after)
    assert not list(data_root.rglob("*.parquet"))
    assert not (data_root / "silver").exists()


def test_all_preflights_finish_before_first_control_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = tmp_path / "repo"
    repository.mkdir()
    data_root = tmp_path / "data"
    data_root.mkdir()
    _patch_preflight(monkeypatch, repository)
    monkeypatch.setattr(
        module,
        "_pin_tracked_files",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            IdentityMarketInventoryExecutionPlanError("runtime pin mismatch")
        ),
    )

    with pytest.raises(IdentityMarketInventoryExecutionPlanError, match="runtime pin"):
        _run(data_root, repository)

    assert not list(data_root.rglob("*"))


def test_candidate_and_resource_contract_bytes_are_exact() -> None:
    repository = Path(__file__).resolve().parents[1]
    contract = module._verify_contract_bytes(repository)

    assert contract.contract_id == COMPOSITE_FIGI_INVENTORY_CONTRACT_ID
    assert contract.schema_digest == COMPOSITE_FIGI_INVENTORY_SCHEMA_DIGEST
    assert contract.candidate_sha256 == COMPOSITE_FIGI_INVENTORY_RESOURCE_SHA256
    assert contract.resource_sha256 == COMPOSITE_FIGI_INVENTORY_RESOURCE_SHA256


def test_request_module_has_no_parquet_network_approval_or_runner_capability() -> None:
    source = Path(module.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported_roots: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.split(".", 1)[0])

    assert imported_roots.isdisjoint(
        {"boto3", "httpx", "pandas", "polars", "pyarrow", "requests", "socket"}
    )
    assert "write_table" not in source
    assert "open_identity_source_bundle" not in source
    assert "store_approval(" not in source
