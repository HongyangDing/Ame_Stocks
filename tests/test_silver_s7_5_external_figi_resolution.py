from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pytest

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver import external_figi_resolution as figi
from ame_stocks_api.silver import incremental_s75_completion_runtime as completion_runtime
from ame_stocks_api.silver.incremental_contract import ArtifactPin

TARGET = date(2026, 7, 10)
AVAILABLE = date(2026, 8, 24)
CAPTURED = datetime(2026, 8, 21, 12, 0, tzinfo=UTC)


class _Calendar:
    def first_open_after(self, value: datetime):
        assert value.tzinfo is UTC
        return AVAILABLE, datetime(2026, 8, 24, 13, 30, tzinfo=UTC)


def _pin(label: str) -> ArtifactPin:
    content = label.encode()
    return ArtifactPin(
        path=f"data/{label}.parquet",
        sha256=hashlib.sha256(content).hexdigest(),
        bytes=len(content),
    )


def _row(
    ticker: str,
    mic: str,
    cik: str | None,
    *,
    source_suffix: str = "source",
) -> dict[str, object]:
    return {
        "active_on_date": True,
        "observed_cik_normalized": cik,
        "observed_composite_figi": None,
        "observed_share_class_figi": None,
        "primary_exchange_mic": mic,
        "selected_source_record_id": hashlib.sha256(
            f"{ticker}-{source_suffix}".encode()
        ).hexdigest(),
        "session_date": TARGET,
        "ticker": ticker,
        "type_code": "CS",
    }


def _openfigi_result(ticker: str) -> dict[str, object]:
    if ticker == "DTSS":
        return {
            "data": [
                {
                    "compositeFIGI": "BBG000000001",
                    "marketSector": "Equity",
                    "securityType2": "Common Stock",
                    "shareClassFIGI": "BBG000000002",
                    "ticker": ticker,
                },
                {
                    "compositeFIGI": "BBG000000003",
                    "marketSector": "Equity",
                    "securityType2": "Common Stock",
                    "shareClassFIGI": "BBG000000004",
                    "ticker": ticker,
                },
            ]
        }
    if ticker == "NONE":
        return {"error": "No identifier found."}
    if ticker == "NOMATCH":
        return {
            "data": [
                {
                    "compositeFIGI": "BBG000000005",
                    "marketSector": "Equity",
                    "securityType2": "Preferred Stock",
                    "shareClassFIGI": "BBG000000006",
                    "ticker": ticker,
                }
            ]
        }
    suffix = {
        "AAPL": ("BBG000B9XRY4", "BBG001S5N8V8"),
        "IBM": ("BBG000BLNNH6", "BBG001S5S399"),
        "BADLIST": ("BBG000000007", "BBG000000008"),
    }[ticker]
    return {
        "data": [
            {
                "compositeFIGI": suffix[0],
                "marketSector": "Equity",
                "securityType2": "Common Stock",
                "shareClassFIGI": suffix[1],
                "ticker": ticker,
            }
        ]
    }


class _Transport:
    def __init__(
        self,
        *,
        fail_once: bool = False,
        cardinality_error: bool = False,
        resolve_dtss: bool = False,
    ):
        self.fail_once = fail_once
        self.cardinality_error = cardinality_error
        self.resolve_dtss = resolve_dtss
        self.calls: list[tuple[str, str]] = []

    def __call__(self, *, method, url, body, headers, timeout_seconds):
        self.calls.append((method, url))
        assert timeout_seconds == 45.0
        if self.fail_once:
            self.fail_once = False
            return figi.ExternalHttpResponse(
                status=503,
                body=b"retry",
                headers={"Retry-After": "0"},
                final_url=url,
            )
        if url == figi.NASDAQ_LISTED_URL:
            response = (
                b"Symbol|Security Name|Market Category|Test Issue\n"
                b"AAPL|Apple Inc.|Q|N\n"
                b"DTSS|Datasea Inc.|S|N\n"
                b"NOMATCH|No Match Inc.|S|N\n"
                b"File Creation Time: 0821202621:31||||\n"
            )
        elif url == figi.NASDAQ_OTHER_LISTED_URL:
            response = (
                b"ACT Symbol|Security Name|Exchange|CQS Symbol|ETF|Round Lot Size|"
                b"Test Issue|NASDAQ Symbol\n"
                b"IBM|International Business Machines|N|IBM|N|100|N|IBM\n"
                b"NONE|None Corp|A|NONE|N|100|N|NONE\n"
                b"File Creation Time: 0821202621:31||||||||\n"
            )
        else:
            assert method == "POST"
            jobs = json.loads(body)
            results = [_openfigi_result(item["idValue"]) for item in jobs]
            if self.resolve_dtss:
                for index, item in enumerate(jobs):
                    if item["idValue"] == "DTSS":
                        results[index] = {
                            "data": [
                                {
                                    "compositeFIGI": "BBG000000001",
                                    "marketSector": "Equity",
                                    "securityType2": "Common Stock",
                                    "shareClassFIGI": "BBG000000002",
                                    "ticker": "DTSS",
                                }
                            ]
                        }
            if self.cardinality_error:
                results = results[:-1]
            response = json.dumps(results, separators=(",", ":")).encode()
        return figi.ExternalHttpResponse(
            status=200,
            body=response,
            headers={
                "Content-Type": "application/json",
                "Set-Cookie": "must-not-persist",
            },
            final_url=url,
        )


@pytest.fixture
def rows() -> list[dict[str, object]]:
    return [
        _row("AAPL", "XNAS", "0000320193"),
        _row("IBM", "XNYS", "0000051143"),
        _row("DTSS", "XNAS", "0001730773"),
        _row("NONE", "XASE", "0000000001"),
        _row("NOMATCH", "XNAS", "0000000002"),
        _row("BADLIST", "XNYS", "0000000003"),
        _row("NOCIK", "XNAS", None),
    ]


def _install_table(monkeypatch: pytest.MonkeyPatch, rows):
    table = pa.Table.from_pylist(rows)
    monkeypatch.setattr(
        figi,
        "readback_i3_migration_parquet_exact",
        lambda **_kwargs: table,
    )


def test_capture_replays_unique_ambiguous_missing_and_listing_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rows: list[dict[str, object]],
) -> None:
    _install_table(monkeypatch, rows)
    sleeps: list[float] = []
    transport = _Transport(fail_once=True)
    result = figi.capture_external_figi_resolution(
        tmp_path,
        target_artifact=_pin("target"),
        target_row_count=len(rows),
        target_session=TARGET,
        calendar=_Calendar(),
        api_key="secret-api-key",
        transport=transport,
        clock=lambda: CAPTURED,
        sleeper=sleeps.append,
    )

    assert result.release.resolved_count == 2
    assert result.application_summary.to_dict() == {
        "active_cs_missing_source_figi_rows": 7,
        "externally_resolved_rows": 2,
        "missing_cik_rows": 1,
        "remaining_rows": 5,
        "unsupported_mic_rows": 0,
    }
    dispositions = {item.ticker: item.disposition for item in result.release.attempts}
    assert dispositions == {
        "AAPL": "resolved_unique_corroborated",
        "BADLIST": "listing_not_corroborated",
        "DTSS": "openfigi_ambiguous",
        "IBM": "resolved_unique_corroborated",
        "NOMATCH": "openfigi_no_matching_common_stock",
        "NONE": "openfigi_no_result",
    }
    assert sleeps[0] == 0
    stored_bytes = b"".join(path.read_bytes() for path in tmp_path.rglob("*.json"))
    assert b"secret-api-key" not in stored_bytes
    assert b"must-not-persist" not in stored_bytes
    assert not list((tmp_path / "tmp/s7-5-external-figi").glob("source_set_id=*"))

    loaded, summary = figi.verify_external_figi_resolution_release(
        tmp_path,
        result.release_artifact,
        current_target_artifact=_pin("target"),
        current_target_row_count=len(rows),
        current_terminal_session=TARGET,
        calendar=_Calendar(),
    )
    assert loaded == result.release
    assert summary == result.application_summary
    identity = figi.effective_external_identity(rows[0], loaded)
    assert identity is not None
    assert identity.canonical_composite_figi == "BBG000B9XRY4"
    assert rows[0]["observed_composite_figi"] is None
    assert rows[0]["observed_share_class_figi"] is None


def test_same_scope_is_idempotent_and_new_source_row_can_reuse_key(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rows: list[dict[str, object]],
) -> None:
    _install_table(monkeypatch, rows)
    first = figi.capture_external_figi_resolution(
        tmp_path,
        target_artifact=_pin("target"),
        target_row_count=len(rows),
        target_session=TARGET,
        calendar=_Calendar(),
        transport=_Transport(),
        clock=lambda: CAPTURED,
        sleeper=lambda _seconds: None,
    )
    second = figi.capture_external_figi_resolution(
        tmp_path,
        target_artifact=_pin("target"),
        target_row_count=len(rows),
        target_session=TARGET,
        calendar=_Calendar(),
        existing_release_artifact=first.release_artifact,
        transport=lambda **_kwargs: (_ for _ in ()).throw(AssertionError("network called")),
        clock=lambda: CAPTURED,
        sleeper=lambda _seconds: None,
    )
    assert second.reused is True
    assert second.release_artifact == first.release_artifact

    later = dict(rows[0])
    later["session_date"] = date(2026, 7, 13)
    later["selected_source_record_id"] = hashlib.sha256(b"later-source").hexdigest()
    assert figi.effective_external_identity(later, second.release) is not None
    later["observed_cik_normalized"] = "0000009999"
    assert figi.effective_external_identity(later, second.release) is None


def test_response_cardinality_and_evidence_tamper_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rows: list[dict[str, object]],
) -> None:
    _install_table(monkeypatch, rows)
    with pytest.raises(figi.ExternalFigiResolutionError, match="cardinality"):
        figi.capture_external_figi_resolution(
            tmp_path,
            target_artifact=_pin("bad-target"),
            target_row_count=len(rows),
            target_session=TARGET,
            calendar=_Calendar(),
            transport=_Transport(cardinality_error=True),
            clock=lambda: CAPTURED,
            sleeper=lambda _seconds: None,
        )
    good = figi.capture_external_figi_resolution(
        tmp_path,
        target_artifact=_pin("target"),
        target_row_count=len(rows),
        target_session=TARGET,
        calendar=_Calendar(),
        transport=_Transport(),
        clock=lambda: CAPTURED,
        sleeper=lambda _seconds: None,
    )
    evidence = good.release.evidence_artifacts[0]
    path = tmp_path / evidence.path
    content = bytearray(path.read_bytes())
    content[-2] ^= 1
    path.chmod(0o644)
    path.write_bytes(content)
    with pytest.raises(figi.ExternalFigiResolutionError, match="exact pin differs"):
        figi.verify_external_figi_resolution_release(
            tmp_path,
            good.release_artifact,
            current_target_artifact=_pin("target"),
            current_target_row_count=len(rows),
            current_terminal_session=TARGET,
            calendar=_Calendar(),
        )


def test_refresh_can_promote_unresolved_but_cannot_rewrite_resolved_parent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rows: list[dict[str, object]],
) -> None:
    _install_table(monkeypatch, rows)
    first = figi.capture_external_figi_resolution(
        tmp_path,
        target_artifact=_pin("target"),
        target_row_count=len(rows),
        target_session=TARGET,
        calendar=_Calendar(),
        transport=_Transport(),
        clock=lambda: CAPTURED,
        sleeper=lambda _seconds: None,
    )
    refreshed = figi.capture_external_figi_resolution(
        tmp_path,
        target_artifact=_pin("target"),
        target_row_count=len(rows),
        target_session=TARGET,
        calendar=_Calendar(),
        existing_release_artifact=first.release_artifact,
        refresh_unresolved=True,
        transport=_Transport(resolve_dtss=True),
        clock=lambda: datetime(2026, 8, 21, 13, 0, tzinfo=UTC),
        sleeper=lambda _seconds: None,
    )

    assert refreshed.release.parent_release_artifact == first.release_artifact
    assert refreshed.release.resolved_count == 3
    first_resolved = {
        item.resolution_key_id: item.to_dict()
        for item in first.release.attempts
        if item.disposition == "resolved_unique_corroborated"
    }
    refreshed_by_key = {
        item.resolution_key_id: item.to_dict() for item in refreshed.release.attempts
    }
    assert all(refreshed_by_key[key] == value for key, value in first_resolved.items())


def test_source_row_change_invalidates_the_release(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rows: list[dict[str, object]],
) -> None:
    _install_table(monkeypatch, rows)
    result = figi.capture_external_figi_resolution(
        tmp_path,
        target_artifact=_pin("target"),
        target_row_count=len(rows),
        target_session=TARGET,
        calendar=_Calendar(),
        transport=_Transport(),
        clock=lambda: CAPTURED,
        sleeper=lambda _seconds: None,
    )
    changed = [dict(item) for item in rows]
    changed[0]["observed_cik_normalized"] = "0000009999"
    monkeypatch.setattr(
        figi,
        "readback_i3_migration_parquet_exact",
        lambda **_kwargs: pa.Table.from_pylist(changed),
    )
    with pytest.raises(figi.ExternalFigiResolutionError, match="source row identity differs"):
        figi.verify_external_figi_resolution_release(
            tmp_path,
            result.release_artifact,
            current_target_artifact=_pin("target"),
            current_target_row_count=len(changed),
            current_terminal_session=TARGET,
            calendar=_Calendar(),
        )


def test_noncanonical_or_wrong_path_release_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rows: list[dict[str, object]],
) -> None:
    _install_table(monkeypatch, rows)
    result = figi.capture_external_figi_resolution(
        tmp_path,
        target_artifact=_pin("target"),
        target_row_count=len(rows),
        target_session=TARGET,
        calendar=_Calendar(),
        transport=_Transport(),
        clock=lambda: CAPTURED,
        sleeper=lambda _seconds: None,
    )
    content = (tmp_path / result.release_artifact.path).read_bytes()
    copied = tmp_path / "manifests/latest/external-figi.json"
    copied.parent.mkdir(parents=True)
    copied.write_bytes(content)
    copied_pin = ArtifactPin(
        path="manifests/latest/external-figi.json",
        sha256=hashlib.sha256(content).hexdigest(),
        bytes=len(content),
    )
    with pytest.raises(figi.ExternalFigiResolutionError, match="path is not canonical"):
        figi.load_external_figi_resolution_release_exact(tmp_path, copied_pin)

    pretty = json.dumps(json.loads(content), indent=2, sort_keys=True).encode() + b"\n"
    pretty_path = tmp_path / result.release_artifact.path
    pretty_path.chmod(0o644)
    pretty_path.write_bytes(pretty)
    pretty_pin = ArtifactPin(
        path=result.release_artifact.path,
        sha256=hashlib.sha256(pretty).hexdigest(),
        bytes=len(pretty),
    )
    with pytest.raises(figi.ExternalFigiResolutionError, match="not canonical"):
        figi.load_external_figi_resolution_release_exact(tmp_path, pretty_pin)


def test_external_release_replays_through_the_real_s75_marker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    row = {
        **_row("AAPL", "XNAS", "0000320193"),
        "alias_resolution_version_id": None,
        "alias_segment_id": None,
        "asset_id": None,
        "asset_master_version_id": None,
        "backtest_identity_eligible": False,
        "canonical_cik_normalized": "0000320193",
        "composite_registry_collision": False,
        "identity_quality_liquidation_signal": False,
        "identity_resolution_status": "unresolved",
        "issuer_id": stable_digest({"issuer": "AAPL"}),
        "issuer_master_version_id": None,
        "position_continuity_status": (
            "identity_uncertain_no_new_trade_no_forced_exit_run_incomplete"
        ),
    }
    table = pa.Table.from_pylist([row])
    monkeypatch.setattr(
        figi,
        "readback_i3_migration_parquet_exact",
        lambda **_kwargs: table,
    )
    target = _pin("target")
    release = figi.capture_external_figi_resolution(
        tmp_path,
        target_artifact=target,
        target_row_count=1,
        target_session=TARGET,
        calendar=_Calendar(),
        transport=_Transport(),
        clock=lambda: CAPTURED,
        sleeper=lambda _seconds: None,
    )

    def write(relative: str, content: bytes) -> ArtifactPin:
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        return ArtifactPin(
            path=relative,
            sha256=hashlib.sha256(content).hexdigest(),
            bytes=len(content),
        )

    completion_pin = write("controls/completion.json", b'{"completion":true}\n')
    deep_pin = write("controls/deep.json", b'{"deep":true}\n')
    output_set = SimpleNamespace(
        checkpoint_id=stable_digest({"checkpoint": "delta"}),
        release_id=stable_digest({"native": "delta"}),
        gate_a_manifest_pin=SimpleNamespace(release_id=stable_digest({"gate": "delta"})),
        table_outputs=(
            SimpleNamespace(table_name="asset_master"),
            SimpleNamespace(table_name="ticker_alias"),
            SimpleNamespace(table_name="issuer_master"),
            SimpleNamespace(
                table_name="universe_daily",
                dataset_index=SimpleNamespace(
                    partitions=(
                        SimpleNamespace(
                            artifact=target,
                            row_count=1,
                            session_date=TARGET,
                        ),
                    )
                ),
            ),
        ),
    )
    authority = SimpleNamespace(
        completion=SimpleNamespace(completion_id=stable_digest({"completion": "delta"})),
        run_spec=SimpleNamespace(
            calendar=SimpleNamespace(
                artifact=SimpleNamespace(sha256="a" * 64),
                calendar_artifact_id="b" * 64,
            ),
            run_spec_id=stable_digest({"run": "delta"}),
            terminal_session=TARGET,
        ),
        output_set=output_set,
    )
    monkeypatch.setattr(completion_runtime, "_load_delta_authority", lambda *_args: authority)
    monkeypatch.setattr(
        completion_runtime,
        "readback_i3_migration_parquet_exact",
        lambda **_kwargs: table,
    )
    monkeypatch.setattr(
        completion_runtime,
        "load_xnys_calendar_artifact",
        lambda *_args, **_kwargs: _Calendar(),
    )
    config = completion_runtime.S75CompletionConfig(
        delta_completion_artifact=completion_pin,
        delta_deep_attestation_artifact=deep_pin,
        completion_available_session=AVAILABLE,
        external_figi_resolution_artifact=release.release_artifact,
    )
    _, config_pin = completion_runtime.prepare_s75_completion(tmp_path, config)
    ready = completion_runtime.stage_s75_completion(tmp_path, config_pin)

    assert ready.summary.externally_resolved_active_cs_rows == 1
    assert ready.summary.remaining_active_cs_missing_figi_rows == 0
    assert ready.external_figi_resolution == release.release
    replay = completion_runtime.verify_s75_completion(
        tmp_path,
        ready.sentinel_artifact,
    )
    assert replay.summary == ready.summary
