from __future__ import annotations

import hashlib
from dataclasses import replace
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pytest

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver import incremental_s75_completion_runtime as runtime
from ame_stocks_api.silver.external_figi_resolution import ExternalFigiApplicationSummary
from ame_stocks_api.silver.incremental_contract import ArtifactPin

TARGET = date(2026, 7, 10)
AVAILABLE = date(2026, 8, 17)


def _write(root: Path, relative: str, content: bytes) -> ArtifactPin:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return ArtifactPin(
        path=relative,
        sha256=hashlib.sha256(content).hexdigest(),
        bytes=len(content),
    )


def _row(ticker: str, *, eligible: bool) -> dict[str, object]:
    return {
        "session_date": TARGET,
        "ticker": ticker,
        "active_on_date": True,
        "asset_id": stable_digest({"asset": ticker}) if eligible else None,
        "issuer_id": stable_digest({"issuer": ticker}),
        "canonical_cik_normalized": "0000000001",
        "alias_segment_id": stable_digest({"segment": ticker}) if eligible else None,
        "alias_resolution_version_id": (
            stable_digest({"resolution": ticker}) if eligible else None
        ),
        "asset_master_version_id": stable_digest({"master": ticker}) if eligible else None,
        "issuer_master_version_id": stable_digest({"issuer-master": ticker}) if eligible else None,
        "backtest_identity_eligible": eligible,
        "position_continuity_status": (
            "resolved_identity"
            if eligible
            else "identity_uncertain_no_new_trade_no_forced_exit_run_incomplete"
        ),
        "identity_quality_liquidation_signal": False,
        "identity_resolution_status": "resolved" if eligible else "unresolved",
        "composite_registry_collision": False,
    }


def _fixture(
    root: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    rows: list[dict[str, object]] | None = None,
):
    completion_pin = _write(root, "controls/delta-completion.json", b'{"delta":true}\n')
    deep_pin = _write(root, "controls/delta-deep.json", b'{"deep":true}\n')
    target_pin = _write(root, "data/universe-2026-07-10.parquet", b"fixture")
    partition = SimpleNamespace(
        artifact=target_pin,
        row_count=2,
        session_date=TARGET,
    )
    output_set = SimpleNamespace(
        checkpoint_id=stable_digest({"checkpoint": "delta"}),
        release_id=stable_digest({"native": "delta"}),
        gate_a_manifest_pin=SimpleNamespace(release_id=stable_digest({"release": "delta"})),
        table_outputs=(
            SimpleNamespace(table_name="asset_master"),
            SimpleNamespace(table_name="ticker_alias"),
            SimpleNamespace(table_name="issuer_master"),
            SimpleNamespace(
                table_name="universe_daily",
                dataset_index=SimpleNamespace(partitions=(partition,)),
            ),
        ),
    )
    authority = SimpleNamespace(
        completion=SimpleNamespace(completion_id=stable_digest({"completion": "delta"})),
        run_spec=SimpleNamespace(
            run_spec_id=stable_digest({"run-spec": "delta"}),
            terminal_session=TARGET,
        ),
        output_set=output_set,
    )
    selected = rows or [_row("AAA", eligible=True), _row("PENDING", eligible=False)]
    monkeypatch.setattr(runtime, "_load_delta_authority", lambda *_args: authority)
    monkeypatch.setattr(
        runtime,
        "readback_i3_migration_parquet_exact",
        lambda **_kwargs: pa.Table.from_pylist(selected),
    )
    config = runtime.S75CompletionConfig(
        delta_completion_artifact=completion_pin,
        delta_deep_attestation_artifact=deep_pin,
        completion_available_session=AVAILABLE,
    )
    return config, authority


def test_factor_ready_completion_uses_only_delta_and_target_partition(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, _ = _fixture(tmp_path, monkeypatch)
    _, config_pin = runtime.prepare_s75_completion(tmp_path, config)
    result = runtime.stage_s75_completion(tmp_path, config_pin)

    assert result.summary.universe_row_count == 2
    assert result.summary.eligible_row_count == 1
    assert result.summary.ineligible_row_count == 1
    assert result.summary.unresolved_row_count == 1
    assert result.sentinel_artifact.path.endswith("S7_5_COMPLETE.json")
    marker = runtime._read_canonical(tmp_path, result.sentinel_artifact, "marker")
    assert marker["state"] == "factor_ready_for_s8"
    assert marker["s8_started"] is False
    immutable = (
        tmp_path
        / "manifests/silver/incremental/s7_5/factor-ready/completions"
        / f"session_date={TARGET.isoformat()}"
        / f"marker_id={marker['marker_id']}"
        / "manifest.json"
    )
    assert immutable.read_bytes() == (tmp_path / result.sentinel_artifact.path).read_bytes()
    assert "i4_completion_artifact" not in marker
    assert "i5_completion_artifact" not in marker
    assert "i7_completion_artifact" not in marker

    replay = runtime.verify_s75_completion(tmp_path, result.sentinel_artifact)
    assert replay.summary == result.summary
    repeated = runtime.stage_s75_completion(tmp_path, config_pin)
    assert repeated.reused is True


def test_ineligible_membership_may_keep_issuer_lineage_but_not_tradable_links(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pending = _row("PENDING", eligible=False)
    assert pending["issuer_id"] is not None
    assert pending["canonical_cik_normalized"] is not None
    _, authority = _fixture(tmp_path, monkeypatch, rows=[pending])
    summary = runtime._factor_summary(tmp_path, authority)
    assert summary.eligible_row_count == 0
    assert summary.ineligible_row_count == 1

    pending["alias_segment_id"] = stable_digest({"bad": "alias"})
    with pytest.raises(runtime.S75CompletionRuntimeError, match="tradable identity graph"):
        runtime._factor_summary(tmp_path, authority)


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        (lambda rows: rows.append(dict(rows[0])), "duplicate/invalid ticker"),
        (
            lambda rows: rows[0].update(identity_quality_liquidation_signal=True),
            "forced liquidation",
        ),
        (
            lambda rows: rows[0].update(alias_segment_id=None),
            "lacks identity linkage",
        ),
        (
            lambda rows: rows[1].update(composite_registry_collision=True),
            "",
        ),
    ),
)
def test_factor_checks_only_block_result_changing_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation,
    message: str,
) -> None:
    rows = [_row("AAA", eligible=True), _row("PENDING", eligible=False)]
    if message == "":
        rows[1]["backtest_identity_eligible"] = True
        rows[1]["position_continuity_status"] = "resolved_identity"
        rows[1]["asset_id"] = stable_digest({"bad": "asset"})
        rows[1]["alias_segment_id"] = stable_digest({"bad": "segment"})
        rows[1]["alias_resolution_version_id"] = stable_digest({"bad": "resolution"})
        rows[1]["asset_master_version_id"] = stable_digest({"bad": "master"})
        message = "registry collision"
    mutation(rows)
    _, authority = _fixture(tmp_path, monkeypatch, rows=rows)
    with pytest.raises(runtime.S75CompletionRuntimeError, match=message):
        runtime._factor_summary(tmp_path, authority)


def test_tampered_marker_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, _ = _fixture(tmp_path, monkeypatch)
    _, config_pin = runtime.prepare_s75_completion(tmp_path, config)
    result = runtime.stage_s75_completion(tmp_path, config_pin)
    path = tmp_path / result.sentinel_artifact.path
    content = bytearray(path.read_bytes())
    content[-2] ^= 1
    path.chmod(0o644)
    path.write_bytes(content)
    with pytest.raises(runtime.S75CompletionRuntimeError, match="exact pin differs"):
        runtime.verify_s75_completion(tmp_path, result.sentinel_artifact)


def test_prepare_requires_real_delta_artifacts(tmp_path: Path) -> None:
    missing = ArtifactPin(path="missing.json", sha256="a" * 64, bytes=1)
    config = runtime.S75CompletionConfig(
        delta_completion_artifact=missing,
        delta_deep_attestation_artifact=missing,
        completion_available_session=AVAILABLE,
    )
    with pytest.raises(runtime.S75CompletionRuntimeError, match="missing"):
        runtime.prepare_s75_completion(tmp_path, config)


def test_external_figi_overlay_extends_marker_without_changing_legacy_shape(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    legacy, _ = _fixture(tmp_path, monkeypatch)
    assert "external_figi_resolution_artifact" not in legacy.to_dict()
    assert runtime.S75CompletionConfig.from_dict(legacy.to_dict()) == legacy

    external_pin = _write(tmp_path, "controls/external-figi.json", b'{"release":true}\n')
    config = replace(
        legacy,
        external_figi_resolution_artifact=external_pin,
    )
    release = SimpleNamespace(release_id=stable_digest({"external": "release"}))
    application = ExternalFigiApplicationSummary(
        active_cs_missing_source_figi_rows=1108,
        externally_resolved_rows=992,
        remaining_rows=116,
        missing_cik_rows=5,
        unsupported_mic_rows=0,
    )
    monkeypatch.setattr(
        runtime,
        "_load_external_figi_overlay",
        lambda *_args: (release, application),
    )
    _, config_pin = runtime.prepare_s75_completion(tmp_path, config)
    result = runtime.stage_s75_completion(tmp_path, config_pin)
    marker = runtime._read_canonical(tmp_path, result.sentinel_artifact, "marker")

    assert marker["external_figi_resolution_artifact"] == external_pin.to_dict()
    assert marker["external_figi_resolution_id"] == release.release_id
    assert marker["summary"]["externally_resolved_active_cs_rows"] == 992
    assert marker["summary"]["remaining_active_cs_missing_figi_rows"] == 116
    assert "external_figi_never_rewrites_observed_lineage" in marker["factor_hard_invariants"]
    replay = runtime.verify_s75_completion(tmp_path, result.sentinel_artifact)
    assert replay.summary == result.summary
    assert replay.external_figi_resolution is release
