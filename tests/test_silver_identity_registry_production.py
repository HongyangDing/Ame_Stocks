from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path
from types import MappingProxyType, SimpleNamespace

import pytest

import ame_stocks_api.cli.silver_identity_registry_production as production_cli
import ame_stocks_api.silver.identity_registry_production as production
import ame_stocks_api.silver.identity_registry_workflow as workflow
from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.cli.silver_identity_registry_production import build_parser
from ame_stocks_api.silver.calendar_artifact import (
    build_xnys_calendar_artifact,
    write_xnys_calendar_artifact,
)
from ame_stocks_api.silver.identity_registry_exact_group_scopes import (
    ExactGroupRegistrySourceRow,
    ExactGroupRegistrySourceScope,
    LoadedExactGroupRegistryScopes,
)
from ame_stocks_api.silver.identity_registry_workflow import (
    FIXED_DECISION_SCOPE_SPECS,
    RUNTIME_BINDING_PATHS,
    ExactArtifactBinding,
    ExactSourceRow,
    ExactSourceScope,
    RegistryCandidateManifest,
    RegistryName,
    RegistryRuntimeBinding,
    RuntimeFilePin,
    StoredControlDocument,
)
from ame_stocks_api.silver.identity_source import S7_S4_RELEASE_SET_ID

ROOT = Path(__file__).resolve().parents[1]


def _source(
    ticker: str,
    session: date,
    composite: str,
    share_class: str | None,
) -> ExactSourceRow:
    return ExactSourceRow(
        session_date=session,
        source_record_id=stable_digest(
            {"composite": composite, "session": session.isoformat(), "ticker": ticker}
        ),
        source_dataset="asset_observation_daily",
        source_s4_release_set_id=S7_S4_RELEASE_SET_ID,
        provider_id="massive",
        provider_market="stocks",
        provider_locale="us",
        ticker=ticker,
        observed_composite_figi=composite,
        observed_share_class_figi=share_class,
        primary_exchange_mic="XNYS",
    )


def _exact_group_source(row: ExactSourceRow) -> ExactGroupRegistrySourceRow:
    return ExactGroupRegistrySourceRow(
        session_date=row.session_date,
        source_record_id=row.source_record_id,
        source_dataset=row.source_dataset,
        source_s4_release_set_id=row.source_s4_release_set_id,
        provider_id=row.provider_id,
        provider_market=row.provider_market,
        provider_locale=row.provider_locale,
        ticker=row.ticker,
        observed_composite_figi=row.observed_composite_figi,
        observed_share_class_figi=row.observed_share_class_figi,
        primary_exchange_mic=row.primary_exchange_mic,
    )


def _scope(rows: tuple[ExactSourceRow, ...]) -> ExactGroupRegistrySourceScope:
    return ExactGroupRegistrySourceScope(tuple(_exact_group_source(row) for row in rows))


def _exact_loaded() -> LoadedExactGroupRegistryScopes:
    sessions = tuple(
        item.session_date
        for item in build_xnys_calendar_artifact(date(2025, 1, 2), date(2026, 7, 9)).sessions
    )
    assert len(sessions) == 379
    return LoadedExactGroupRegistryScopes(
        candidate_id="1" * 64,
        candidate_sha256="2" * 64,
        completion_id="3" * 64,
        completion_sha256="4" * 64,
        evidence_manifest_ids=("5" * 64, "6" * 64, "7" * 64),
        scopes={
            "asset_transition:SOR": _scope(
                (
                    _source("SOR", date(2024, 12, 31), "BBG000KMY6N2", "BBG001S5W848"),
                    _source("SOR", date(2025, 1, 2), "BBG000KMY6N2", "BBG01RK6N5G9"),
                )
            ),
            "provider_composite_override:SOR": _scope(
                tuple(
                    _source("SOR", session, "BBG000KMY6N2", "BBG01RK6N5G9") for session in sessions
                )
            ),
            "share_class_adjudication:XZO": _scope(
                tuple(
                    _source("XZO", session, "BBG01XL8FHT0", "BBG01XL8FJS7")
                    for session in (date(2025, 11, 4), date(2025, 11, 5))
                )
            ),
            "share_class_adjudication:ANABV": _scope(
                (_source("ANABV", date(2026, 4, 6), "BBG021DMXXT2", "BBG0026ZDHT8"),)
            ),
        },
    )


def _artifact(role: str, artifact_id: str, sha: str) -> ExactArtifactBinding:
    return ExactArtifactBinding(
        role=role,
        artifact_id=artifact_id,
        path=f"fixture/{role}.json",
        sha256=sha,
        bytes=1,
        available_session=date(2026, 7, 20),
    )


def _runtime_binding() -> RegistryRuntimeBinding:
    return RegistryRuntimeBinding(
        git_commit="a" * 40,
        git_tree="b" * 40,
        files=tuple(
            RuntimeFilePin(
                path=path,
                git_mode="100644",
                git_blob_id="c" * 40,
                sha256="d" * 64,
                bytes=1,
            )
            for path in sorted(RUNTIME_BINDING_PATHS)
        ),
        python_implementation="CPython",
        python_version="3.13.5",
        pyarrow_version="20.0.0",
    )


def test_fixed_production_builders_construct_all_five_registry_candidate_sets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    exact = _exact_loaded()
    source = _artifact("source_exact_group_candidate_manifest", "1" * 64, "2" * 64)
    source_completion = _artifact("source_exact_group_completion_manifest", "3" * 64, "4" * 64)
    evidence = _artifact("external_evidence", "8" * 64, "9" * 64)
    common = {
        "root": ROOT,
        "exact": exact,
        "source_candidate": source,
        "source_completion": source_completion,
        "evidence": evidence,
        "evidence_document": {},
        "candidate_available": date(2026, 7, 20),
    }
    transition = production._build_relation_decisions(
        registry_name=RegistryName.ASSET_TRANSITION.value,
        asset_transition_release=None,
        **common,
    )
    share = production._build_relation_decisions(
        registry_name=RegistryName.SHARE_CLASS_ADJUDICATION.value,
        asset_transition_release=None,
        **common,
    )
    transition_row = {
        **transition[0].frozen_row_claims,
        "transition_available_session": date(2026, 7, 20),
    }
    monkeypatch.setattr(
        production,
        "load_registry_release",
        lambda *_args, **_kwargs: SimpleNamespace(
            registry_name=RegistryName.ASSET_TRANSITION.value,
            decision_rows={transition[0].decision_id: transition_row},
            candidate=SimpleNamespace(
                decisions=transition,
                source_artifacts=(source, source_completion),
                evidence_artifacts=(evidence,),
            ),
        ),
    )
    provider = production._build_relation_decisions(
        registry_name=RegistryName.PROVIDER_COMPOSITE_OVERRIDE.value,
        asset_transition_release=SimpleNamespace(),
        **common,
    )
    assert [item.case_key for item in transition] == ["asset_transition:SOR"]
    assert [item.case_key for item in provider] == ["provider_composite_override:SOR"]
    assert {item.case_key for item in share} == {
        "share_class_adjudication:ANABV",
        "share_class_adjudication:XZO",
    }
    assert len(provider[0].source_scope.rows) == 379
    monkeypatch.setattr(
        production,
        "load_registry_release",
        lambda *_args, **_kwargs: SimpleNamespace(
            registry_name=RegistryName.ASSET_TRANSITION.value,
            decision_rows={transition[0].decision_id: transition_row},
            candidate=SimpleNamespace(
                decisions=transition,
                source_artifacts=(
                    _artifact(
                        "source_exact_group_candidate_manifest",
                        "a" * 64,
                        "b" * 64,
                    ),
                    source_completion,
                ),
                evidence_artifacts=(evidence,),
            ),
        ),
    )
    with pytest.raises(
        production.IdentityRegistryProductionError,
        match="exact SOR source/evidence",
    ):
        production._build_relation_decisions(
            registry_name=RegistryName.PROVIDER_COMPOSITE_OVERRIDE.value,
            asset_transition_release=SimpleNamespace(),
            **common,
        )

    evidence_document = json.loads(
        (
            ROOT
            / "docs/silver/evidence/s7-cross-market"
            / "identity-cross-market-external-evidence-manifest.candidate.json"
        ).read_text(encoding="utf-8")
    )
    scopes: dict[str, ExactSourceScope] = {}
    roles: dict[str, MappingProxyType[str, str]] = {}
    for case_key, spec in FIXED_DECISION_SCOPE_SPECS.items():
        if not case_key.startswith("identity_cross_market_adjudication:"):
            continue
        sessions = (
            tuple(
                item.session_date
                for item in build_xnys_calendar_artifact(
                    spec.valid_from_session, spec.valid_through_session
                ).sessions
            )
            if spec.expected_source_row_count == 15
            else (spec.valid_from_session,)
        )
        scopes[case_key] = ExactSourceScope(
            tuple(
                _source(
                    spec.ticker,
                    session,
                    str(spec.observed_composite_figi),
                    spec.observed_share_class_figi,
                )
                for session in sessions
            )
        )
        case_count = 3 if len(sessions) == 15 else 1
        case_ids = [stable_digest({"case": case_key, "index": i}) for i in range(case_count)]
        roles[spec.ticker] = MappingProxyType(
            {
                case_id: (
                    "inverse_middle_is_canonical_us"
                    if case_count == 3 and i == 2
                    else "contaminated_middle_episode"
                )
                for i, case_id in enumerate(sorted(case_ids))
            }
        )
    gate_c = production._GateCSource(
        candidate=_artifact("source_gate_c_candidate_manifest", "a" * 64, "b" * 64),
        completion=_artifact("source_gate_c_completion_manifest", "c" * 64, "d" * 64),
        detector_preview=_artifact(
            "source_identity_case_preview_manifest",
            "e" * 64,
            "f" * 64,
        ),
        source_six_release_binding_id=str(evidence_document["source_six_release_binding_id"]),
        external_evidence_id=str(evidence_document["manifest_id"]),
        external_evidence_sha256="0" * 64,
        scopes=MappingProxyType(scopes),
        case_roles=MappingProxyType(roles),
    )
    cross = production._build_cross_market_decisions(
        gate_c,
        evidence=_artifact(
            "external_evidence",
            str(evidence_document["manifest_id"]),
            "0" * 64,
        ),
        evidence_document=evidence_document,
        candidate_available=date(2026, 7, 20),
    )
    assert len(cross) == 9
    assert sum(len(item.source_scope.rows) for item in cross) == 79
    authorization_artifacts = tuple(
        _artifact(role, character * 64, character.upper() * 64)
        for role, character in (
            ("external_evidence_approval", "1"),
            ("schema_contract_approval", "2"),
            ("source_candidate_approval", "3"),
        )
    )
    ingress = ExactArtifactBinding(
        role="production_ingress_attestation",
        artifact_id="4" * 64,
        path="fixture/production-ingress-attestation.json",
        sha256="5" * 64,
        bytes=1,
        available_session=date(2026, 7, 20),
        embedded_id_field="attestation_id",
    )
    candidate_kwargs = {
        "registry_name": RegistryName.IDENTITY_CROSS_MARKET_ADJUDICATION.value,
        "contract_pin": workflow.current_registry_contract_pin(
            RegistryName.IDENTITY_CROSS_MARKET_ADJUDICATION.value
        ),
        "source_artifacts": (gate_c.candidate, gate_c.completion),
        "evidence_artifacts": (
            _artifact(
                "external_evidence",
                str(evidence_document["manifest_id"]),
                "0" * 64,
            ),
        ),
        "authorization_artifacts": authorization_artifacts,
        "availability_calendar_id": production.CALENDAR_ARTIFACT_ID,
        "availability_calendar_sha256": production.CALENDAR_ARTIFACT_SHA256,
        "created_at_utc": datetime(2026, 7, 19, 14, tzinfo=UTC),
        "candidate_available_session": date(2026, 7, 20),
        "decisions": tuple(sorted(cross, key=lambda item: item.decision_id)),
    }
    candidate = RegistryCandidateManifest(
        **candidate_kwargs,
        production_ingress_artifact=ingress,
    )
    assert {item.role for item in candidate.source_artifacts} == {
        "source_gate_c_candidate_manifest",
        "source_gate_c_completion_manifest",
    }
    monkeypatch.setattr(production, "_load_gate_c_source", lambda *_args: gate_c)
    workflow._validate_gate_c_registry_scopes(ROOT, candidate)
    monkeypatch.setattr(
        production,
        "_load_gate_c_source",
        lambda *_args: replace(
            gate_c,
            detector_preview=_artifact(
                "source_identity_case_preview_manifest",
                "8" * 64,
                "9" * 64,
            ),
        ),
    )
    with pytest.raises(
        workflow.RegistryWorkflowError,
        match="identity-case binding differs",
    ):
        workflow._validate_gate_c_registry_scopes(ROOT, candidate)
    with pytest.raises(
        workflow.RegistryWorkflowError,
        match="decision manifest binding is absent",
    ):
        RegistryCandidateManifest(
            **candidate_kwargs,
            production_ingress_artifact=None,
        )
    # The fifth fixed set is intentionally empty: inverse/cross-market cases do
    # not create duplicate bounce adjudications.
    identity_adjudication: tuple[object, ...] = ()
    assert identity_adjudication == ()


def test_production_cli_has_no_decision_row_source_id_or_time_ingress() -> None:
    parser = build_parser()
    help_text = parser.format_help()
    assert "import-fixed-evidence-package" in help_text
    assert "rows-parquet" not in help_text
    assert "candidate-json" not in help_text
    assert "standing-authorization-literal-file" not in help_text
    assert "reaffirmation-literal-file" not in help_text
    for forbidden in (
        "--decision-row-json",
        "--source-record-id",
        "--created-at-utc",
        "--candidate-available-session",
        "--external-evidence-ref-json",
        "--published-at-utc",
    ):
        with pytest.raises(SystemExit):
            parser.parse_args(["prepare-fixed-request", forbidden, "forged"])


def test_production_cli_imports_only_the_fixed_evidence_kind(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    runtime = _runtime_binding()
    manifest = StoredControlDocument(
        object_id="1" * 64,
        path="docs/silver/evidence/s7-exact-groups/manifest.json",
        sha256="2" * 64,
        bytes=3,
    )
    receipt = _artifact("external_evidence_import_receipt", "3" * 64, "4" * 64)

    def fixed_import(root: Path, *, evidence_type: str) -> object:
        assert root == tmp_path.resolve()
        assert evidence_type == "identity_exact_group_external_evidence"
        return production.ImportedExternalEvidencePackage(
            evidence_type=evidence_type,
            manifest=manifest,
            import_receipt=receipt,
            runtime_binding=runtime,
        )

    monkeypatch.setattr(production_cli, "import_fixed_external_evidence_package", fixed_import)
    assert (
        production_cli.main(
            [
                "import-fixed-evidence-package",
                "--data-root",
                str(tmp_path),
                "--evidence-kind",
                "exact-group",
            ]
        )
        == 0
    )
    output = json.loads(capsys.readouterr().out)
    assert output["evidence_type"] == "identity_exact_group_external_evidence"
    assert output["manifest"] == manifest.to_dict()
    assert output["import_receipt"] == receipt.to_dict()
    assert output["runtime_binding"]["runtime_binding_id"] == runtime.runtime_binding_id


def test_fixed_evidence_import_is_git_pinned_immutable_and_replayable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calendar = build_xnys_calendar_artifact(date(2026, 7, 1), date(2026, 8, 31))
    write_xnys_calendar_artifact(tmp_path, calendar)
    runtime = _runtime_binding()
    monkeypatch.setattr(workflow, "CANONICAL_PRODUCTION_DATA_ROOT", tmp_path)
    monkeypatch.setattr(production, "CALENDAR_ARTIFACT_ID", calendar.calendar_artifact_id)
    monkeypatch.setattr(production, "CALENDAR_ARTIFACT_SHA256", calendar.sha256)
    monkeypatch.setattr(production, "capture_registry_runtime_binding", lambda: runtime)
    monkeypatch.setattr(workflow, "capture_registry_runtime_binding", lambda: runtime)
    monkeypatch.setattr(
        production,
        "_utc_now",
        lambda: datetime(2026, 7, 17, 14, tzinfo=UTC),
    )
    evidence_type = "identity_exact_group_external_evidence"
    manifest_path = production._EVIDENCE_REPO_MANIFESTS[evidence_type]
    raw_path = "docs/silver/evidence/s7-exact-groups/fixture-openfigi.json"
    raw_content = b'{"figi":"BBG01RK6N4M5"}\n'
    payload: dict[str, object] = {
        "artifacts": [
            {
                "bytes": len(raw_content),
                "captured_at_utc": "2026-07-16T12:00:00Z",
                "content_scope": "exact_raw_bytes",
                "media_type": "application/json",
                "path": raw_path,
                "sha256": hashlib.sha256(raw_content).hexdigest(),
                "source_available_session": "2026-07-17",
                "source_url": "https://example.invalid/frozen",
            }
        ],
        "availability": {
            "available_session": "2026-07-17",
            "rule": "fixture_max_source_availability_v1",
        },
        "manifest_schema_version": 1,
        "manifest_status": "candidate_not_approved",
        "manifest_type": evidence_type,
        "non_executable_capabilities": {"registry_release_authorized": False},
    }
    manifest_document = {"manifest_id": stable_digest(payload), **payload}
    manifest_content = (
        json.dumps(
            manifest_document,
            allow_nan=False,
            ensure_ascii=True,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    ).encode()
    blobs = {manifest_path: manifest_content, raw_path: b"wrong"}

    def read_blob(_root: Path, commit: str, relative: str) -> bytes:
        assert commit == runtime.git_commit
        return blobs[relative]

    monkeypatch.setattr(production, "_read_git_blob", read_blob)
    with pytest.raises(
        production.IdentityRegistryProductionError,
        match="differs from its manifest receipt",
    ):
        production.import_fixed_external_evidence_package(
            tmp_path,
            evidence_type=evidence_type,
        )
    assert not (tmp_path / raw_path).exists()
    assert not (tmp_path / manifest_path).exists()

    blobs[raw_path] = raw_content
    first = production.import_fixed_external_evidence_package(
        tmp_path,
        evidence_type=evidence_type,
    )
    second = production.import_fixed_external_evidence_package(
        tmp_path,
        evidence_type=evidence_type,
    )
    assert second == first
    assert first.manifest.object_id == manifest_document["manifest_id"]
    assert first.import_receipt.role == "external_evidence_import_receipt"
    assert first.import_receipt.available_session == date(2026, 7, 20)
    assert (tmp_path / raw_path).read_bytes() == raw_content
    assert (tmp_path / manifest_path).read_bytes() == manifest_content

    raw_refs = (
        {
            "bytes": len(raw_content),
            "path": raw_path,
            "sha256": hashlib.sha256(raw_content).hexdigest(),
        },
    )
    alternate_manifest_path = "docs/silver/evidence/s7-exact-groups/forged-alternate-manifest.json"
    alternate_path = tmp_path / alternate_manifest_path
    alternate_path.parent.mkdir(parents=True, exist_ok=True)
    alternate_path.write_bytes(manifest_content)
    alternate_manifest = StoredControlDocument(
        object_id=str(manifest_document["manifest_id"]),
        path=alternate_manifest_path,
        sha256=hashlib.sha256(manifest_content).hexdigest(),
        bytes=len(manifest_content),
    )
    forged_slot_id = stable_digest(
        {
            "evidence_type": evidence_type,
            "manifest": alternate_manifest.to_dict(),
            "production_data_root": tmp_path.resolve().as_posix(),
            "runtime_binding_id": runtime.runtime_binding_id,
            "version": production.EVIDENCE_PACKAGE_IMPORT_VERSION,
        }
    )
    forged_relative = (
        "manifests/silver/identity/external-evidence-imports/"
        f"evidence_type={evidence_type}/slot_id={forged_slot_id}/receipt.json"
    )
    forged_logical: dict[str, object] = {
        "artifact_type": "s7_fixed_external_evidence_import_receipt",
        "artifact_version": production.EVIDENCE_PACKAGE_IMPORT_VERSION,
        "evidence_type": evidence_type,
        "import_available_session": "2026-07-20",
        "import_slot_id": forged_slot_id,
        "imported_at_utc": "2026-07-17T14:00:00Z",
        "manifest": alternate_manifest.to_dict(),
        "production_data_root": tmp_path.resolve().as_posix(),
        "raw_artifacts": list(raw_refs),
        "runtime_binding": runtime.to_dict(),
    }
    forged_document = {
        "import_id": stable_digest(forged_logical),
        **forged_logical,
    }
    forged_path = tmp_path / forged_relative
    forged_path.parent.mkdir(parents=True, exist_ok=True)
    forged_path.write_bytes(production._canonical_control_bytes(forged_document))
    with pytest.raises(
        production.IdentityRegistryProductionError,
        match="manifest path differs from the fixed repository package",
    ):
        production._load_fixed_evidence_import_receipt(
            tmp_path,
            forged_relative,
            expected_type=evidence_type,
            expected_manifest=alternate_manifest,
            expected_raw_refs=raw_refs,
            expected_runtime=runtime,
            expected_available_session=date(2026, 7, 20),
            revalidate_runtime=True,
        )

    blobs[manifest_path] = b"{}\n"
    with pytest.raises(
        production.IdentityRegistryProductionError,
        match="manifest differs from its fixed Git bytes",
    ):
        production._replay_bound_evidence_import(
            tmp_path,
            first.import_receipt,
            expected_type=evidence_type,
            expected_manifest=first.manifest,
            expected_runtime=runtime,
        )
    blobs[manifest_path] = manifest_content

    blobs[raw_path] = b"different raw bytes in the bound Git commit"
    with pytest.raises(
        production.IdentityRegistryProductionError,
        match="raw artifact 0 differs from its fixed Git bytes",
    ):
        production._replay_bound_evidence_import(
            tmp_path,
            first.import_receipt,
            expected_type=evidence_type,
            expected_manifest=first.manifest,
            expected_runtime=runtime,
        )
    blobs[raw_path] = raw_content

    (tmp_path / raw_path).chmod(0o600)
    (tmp_path / raw_path).write_bytes(b"tampered")
    with pytest.raises(
        production.IdentityRegistryProductionError,
        match="raw artifact import failed closed",
    ):
        production.import_fixed_external_evidence_package(
            tmp_path,
            evidence_type=evidence_type,
        )


def test_external_evidence_replays_every_raw_artifact_and_rejects_tamper(
    tmp_path: Path,
) -> None:
    raw_path = tmp_path / "evidence/raw.json"
    raw_path.parent.mkdir(parents=True)
    raw_content = b'{"figi":"BBG000KMY6N2"}\n'
    raw_path.write_bytes(raw_content)
    payload: dict[str, object] = {
        "artifacts": [
            {
                "bytes": len(raw_content),
                "captured_at_utc": "2026-07-19T12:00:00Z",
                "content_scope": "exact_raw_bytes",
                "media_type": "application/json",
                "path": "evidence/raw.json",
                "sha256": hashlib.sha256(raw_content).hexdigest(),
                "source_available_session": "2026-07-20",
                "source_url": "https://example.invalid/frozen",
            }
        ],
        "availability": {
            "available_session": "2026-07-20",
            "rule": "fixture_max_source_availability_v1",
        },
        "manifest_schema_version": 1,
        "manifest_status": "candidate_not_approved",
        "manifest_type": "identity_exact_group_external_evidence",
        "non_executable_capabilities": {"registry_release_authorized": False},
    }
    manifest_id = stable_digest(payload)
    document = {"manifest_id": manifest_id, **payload}
    content = (
        json.dumps(
            document,
            allow_nan=False,
            ensure_ascii=True,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    ).encode()
    manifest_path = tmp_path / "evidence/manifest.json"
    manifest_path.write_bytes(content)
    ref = StoredControlDocument(
        object_id=manifest_id,
        path="evidence/manifest.json",
        sha256=hashlib.sha256(content).hexdigest(),
        bytes=len(content),
    )
    loaded = production._load_external_evidence(
        tmp_path,
        ref,
        expected_type="identity_exact_group_external_evidence",
    )
    assert loaded["manifest_id"] == manifest_id

    raw_path.write_bytes(b'{"figi":"BBG000KMY6N4"}\n')
    with pytest.raises(production.IdentityRegistryProductionError, match="raw artifact"):
        production._load_external_evidence(
            tmp_path,
            ref,
            expected_type="identity_exact_group_external_evidence",
        )


def test_production_ingress_attestation_is_first_writer_and_runtime_bound(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calendar = build_xnys_calendar_artifact(date(2026, 7, 1), date(2026, 8, 31))
    runtime = _runtime_binding()
    monkeypatch.setattr(workflow, "CANONICAL_PRODUCTION_DATA_ROOT", tmp_path)
    monkeypatch.setattr(production, "CALENDAR_ARTIFACT_ID", calendar.calendar_artifact_id)
    monkeypatch.setattr(production, "CALENDAR_ARTIFACT_SHA256", calendar.sha256)
    monkeypatch.setattr(production, "capture_registry_runtime_binding", lambda: runtime)
    monkeypatch.setattr(workflow, "capture_registry_runtime_binding", lambda: runtime)
    monkeypatch.setattr(
        production,
        "_replay_bound_evidence_import",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        production,
        "_utc_now",
        lambda: datetime(2026, 7, 17, 14, tzinfo=UTC),
    )
    sources = (
        _artifact("source_exact_group_candidate_manifest", "1" * 64, "2" * 64),
        _artifact("source_exact_group_completion_manifest", "3" * 64, "4" * 64),
    )
    evidence = (_artifact("external_evidence", "5" * 64, "6" * 64),)
    evidence_import = _artifact(
        "external_evidence_import_receipt",
        "7" * 64,
        "8" * 64,
    )
    authorizations = tuple(
        _artifact(role, str(index) * 64, chr(96 + index) * 64)
        for index, role in enumerate(
            (
                "external_evidence_approval",
                "schema_contract_approval",
                "source_candidate_approval",
            ),
            start=1,
        )
    )
    contract = workflow.current_registry_contract_pin(RegistryName.ASSET_TRANSITION.value)
    kwargs = {
        "registry_name": RegistryName.ASSET_TRANSITION.value,
        "contract_pin": contract.to_dict(),
        "source_artifacts": sources,
        "evidence_artifacts": evidence,
        "authorization_artifacts": authorizations,
        "evidence_import_artifact": evidence_import,
        "asset_transition_release": None,
        "calendar": calendar,
        "runtime_binding": runtime,
    }
    first_document, first_binding = production._record_or_replay_production_ingress_attestation(
        tmp_path,
        **kwargs,
    )
    second_document, second_binding = production._record_or_replay_production_ingress_attestation(
        tmp_path,
        **kwargs,
    )
    assert second_document == first_document
    assert second_binding == first_binding
    assert first_binding.available_session == date(2026, 7, 20)
    assert first_document["runtime_binding"] == runtime.to_dict()
    assert first_document["authorization_effect"] == "none_provenance_only"
