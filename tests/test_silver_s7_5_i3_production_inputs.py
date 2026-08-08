from __future__ import annotations

import hashlib
import json
from datetime import date
from pathlib import Path

import pytest
from test_silver_s7_5_i3_migration_io import _base_fixture

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver import incremental_i3_production_contract as production_contract
from ame_stocks_api.silver import incremental_i3_production_inputs as inputs
from ame_stocks_api.silver.asset_incremental_contract import S4BaseFrontier
from ame_stocks_api.silver.incremental_contract import ArtifactPin
from ame_stocks_api.silver.incremental_i3_checkpoint import LEGACY_S7_V1_RELEASE_SET_ID
from ame_stocks_api.silver.incremental_i3_production_contract import (
    I3ProductionResourceCaps,
)
from ame_stocks_api.silver.incremental_i3_production_semantics import (
    I3_COMPACT_BASE_INITIAL_SEGMENT_RULE_VERSION,
    I3_PRODUCTION_TRANSFORM_SEMANTICS_DIGEST,
    I3_PRODUCTION_TRANSFORM_SEMANTICS_PAYLOAD,
    production_compact_base_initial_segment_id,
    production_native_v2_migration_id,
)


def _config(run_spec) -> inputs.I3ProductionBaseRunConfig:
    return inputs.I3ProductionBaseRunConfig(
        s7_release_set_artifact=run_spec.i0_oracle.artifact,
        s4_release_set_artifact=run_spec.s4_v1_source.artifact,
        i2_base_frontier_artifact=run_spec.i2_base_frontier.artifact,
        run_available_session=run_spec.run_available_session,
        resource_caps=I3ProductionResourceCaps(),
    )


def _frozen_s7_payload() -> dict[str, object]:
    return {
        "approval": {"pin": "approval"},
        "approval_id": stable_digest({"frozen": "approval"}),
        "artifact_type": "s7_four_table_atomic_release_set",
        "candidate_id": stable_digest({"frozen": "candidate"}),
        "candidate_manifest": {"pin": "candidate"},
        "candidate_qa": {"pin": "qa"},
        "full_completion": {"pin": "completion"},
        "full_completion_id": stable_digest({"frozen": "completion"}),
        "intent": {"pin": "intent"},
        "intent_id": stable_digest({"frozen": "intent"}),
        "members": [],
        "plan": {"pin": "plan"},
        "plan_id": stable_digest({"frozen": "plan"}),
        "policy_version": "s7-four-table-atomic-release-set-v1",
        "published_at_utc": "2026-08-02T00:58:43.178962+00:00",
        "release_availability": {"release_available_session": "2026-08-03"},
        "release_set_version": 1,
        "source_binding_id": stable_digest({"frozen": "source-binding"}),
        "state": "published",
        "table_order": [
            "asset_master",
            "ticker_alias",
            "issuer_master",
            "universe_daily",
        ],
        "visibility_rule": "all_four_members_visible_only_through_this_exact_marker_v1",
    }


def _write_frozen_s7_marker(
    root: Path,
    payload: dict[str, object],
    *,
    release_set_id: str = LEGACY_S7_V1_RELEASE_SET_ID,
    canonical: bool = True,
) -> ArtifactPin:
    marker = {**payload, "release_set_id": release_set_id}
    content = (
        inputs._frozen_s7_marker_canonical_bytes(marker)
        if canonical
        else (json.dumps(marker, indent=2, sort_keys=True).encode() + b"\n")
    )
    relative = "manifests/frozen-s7/manifest.json"
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return ArtifactPin(
        path=relative,
        sha256=hashlib.sha256(content).hexdigest(),
        bytes=len(content),
    )


def _patch_frozen_release_digest(
    monkeypatch: pytest.MonkeyPatch,
    payload: dict[str, object],
) -> None:
    original = inputs.stable_digest

    def digest(value: object) -> str:
        if value == payload:
            return LEGACY_S7_V1_RELEASE_SET_ID
        return original(value)

    monkeypatch.setattr(inputs, "stable_digest", digest)


def test_base_config_exposes_only_exact_inputs_availability_and_caps(tmp_path: Path) -> None:
    run_spec, _legacy, _s4 = _base_fixture(tmp_path)
    config = _config(run_spec)
    document = config.to_dict()
    assert "native_v2_migration_id" not in document
    assert "transform_semantics_digest" not in document

    pin = inputs.store_i3_production_base_config(tmp_path, config)
    assert not pin.path.startswith("tmp/")
    assert inputs.load_i3_production_base_config_exact(pin, data_root=tmp_path) == config

    document["transform_semantics_digest"] = stable_digest({"operator": "claim"})
    with pytest.raises(inputs.I3ProductionInputError, match="fields differ"):
        inputs.I3ProductionBaseRunConfig.from_dict(document)


def test_transform_semantics_golden_binds_compact_base_initial_rowset_segment() -> None:
    assert I3_PRODUCTION_TRANSFORM_SEMANTICS_DIGEST == (
        "bde1641f7c4c5c303f003d6a4747e1060d1c75cccb80c7c6673e6a89098adeef"
    )
    assert (
        I3_PRODUCTION_TRANSFORM_SEMANTICS_PAYLOAD["materialization_rules"]["initial_rowset_segment"]
        == I3_COMPACT_BASE_INITIAL_SEGMENT_RULE_VERSION
    )

    artifact = ArtifactPin(
        path="staging/base/asset_master/base.parquet",
        sha256=stable_digest({"fixture": "segment"}),
        bytes=123,
    )
    migration_id = stable_digest({"fixture": "migration"})
    fields = {
        "table_name": "asset_master",
        "artifact": artifact,
        "terminal_session": date(2026, 7, 9),
        "availability_session": date(2026, 8, 3),
        "native_v2_migration_id": migration_id,
    }
    segment_id = production_compact_base_initial_segment_id(**fields)
    assert segment_id == "47cab70cd942dd8d0b21658be4a454b320d418f570370515752c856c1469e308"
    assert production_compact_base_initial_segment_id(**fields) == segment_id
    assert (
        production_compact_base_initial_segment_id(
            **{
                **fields,
                "artifact": ArtifactPin(
                    path=artifact.path,
                    sha256=stable_digest({"fixture": "changed-segment"}),
                    bytes=artifact.bytes,
                ),
            }
        )
        != segment_id
    )
    assert (
        production_compact_base_initial_segment_id(
            **{
                **fields,
                "native_v2_migration_id": stable_digest({"fixture": "changed-migration"}),
            }
        )
        != segment_id
    )
    with pytest.raises(ValueError, match="table is invalid"):
        production_compact_base_initial_segment_id(**{**fields, "table_name": "universe_daily"})


def test_base_config_rejects_latest_or_pattern_source_paths(tmp_path: Path) -> None:
    run_spec, _legacy, _s4 = _base_fixture(tmp_path)
    for path in ("controls/latest/manifest.json", "controls/release-*.json"):
        with pytest.raises(inputs.I3ProductionInputError, match="explicit and non-latest"):
            inputs.I3ProductionBaseRunConfig(
                s7_release_set_artifact=ArtifactPin(
                    path=path,
                    sha256=run_spec.i0_oracle.artifact.sha256,
                    bytes=run_spec.i0_oracle.artifact.bytes,
                ),
                s4_release_set_artifact=run_spec.s4_v1_source.artifact,
                i2_base_frontier_artifact=run_spec.i2_base_frontier.artifact,
                run_available_session=run_spec.run_available_session,
                resource_caps=I3ProductionResourceCaps(),
            )


def test_base_run_spec_builder_derives_module_owned_semantics_and_migration_id(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_spec, _legacy, _s4 = _base_fixture(tmp_path)
    frontier = S4BaseFrontier.from_dict(
        json.loads((tmp_path / source_spec.i2_base_frontier.artifact.path).read_bytes())
    )
    config = _config(source_spec)
    config_pin = inputs.store_i3_production_base_config(tmp_path, config)
    monkeypatch.setattr(
        inputs,
        "_load_exact_base_authorities",
        lambda _root, _config: (
            source_spec.i0_oracle,
            source_spec.s4_v1_source,
            source_spec.identity_policy_bundle,
            source_spec.calendar,
            frontier,
        ),
    )

    prepared = inputs.prepare_i3_production_base_run_spec(tmp_path, config_pin)
    expected_migration = production_native_v2_migration_id(
        i0_release_set_artifact=source_spec.i0_oracle.artifact,
        s4_release_set_artifact=source_spec.s4_v1_source.artifact,
        identity_policy_bundle=source_spec.identity_policy_bundle,
        identity_policy_bundle_artifact=prepared.identity_policy_bundle_artifact,
        calendar_artifact=source_spec.calendar.artifact,
        i2_base_frontier_artifact=source_spec.i2_base_frontier.artifact,
    )
    assert prepared.run_spec.transform_semantics_digest == (
        I3_PRODUCTION_TRANSFORM_SEMANTICS_DIGEST
    )
    assert prepared.run_spec.native_v2_migration_id == expected_migration
    assert prepared.run_spec.i2_receipts == ()
    assert prepared.run_spec.i2_base_frontier.terminal_session == frontier.terminal_session
    assert prepared.config_artifact == config_pin
    for artifact in (
        prepared.config_artifact,
        prepared.identity_policy_bundle_artifact,
        prepared.run_spec_artifact,
    ):
        assert not artifact.path.startswith("tmp/")

    repeated = inputs.prepare_i3_production_base_run_spec(tmp_path, config_pin)
    assert repeated == prepared


def test_real_i2_base_frontier_path_is_manifest_json() -> None:
    frontier_id = stable_digest({"fixture": "frontier"})
    assert inputs._i2_base_frontier_relative(frontier_id) == (
        "manifests/silver/incremental/s4/assets/base-frontiers/"
        f"frontier_id={frontier_id}/manifest.json"
    )


def test_frozen_s7_oracle_exact_loader_ignores_current_runtime_mismatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = _frozen_s7_payload()
    pin = _write_frozen_s7_marker(tmp_path, payload)
    _patch_frozen_release_digest(monkeypatch, payload)
    monkeypatch.setattr(
        inputs.streaming,
        "_repository_runtime_binding",
        lambda: (_ for _ in ()).throw(AssertionError("current runtime must not be probed")),
    )

    direct_marker = inputs.load_frozen_s7_oracle_marker_exact(
        pin,
        content=(tmp_path / pin.path).read_bytes(),
    )
    marker = production_contract._load_frozen_i0_oracle_marker_exact(tmp_path, pin)
    assert marker == direct_marker
    assert marker["release_set_id"] == LEGACY_S7_V1_RELEASE_SET_ID
    assert marker["source_binding_id"] == payload["source_binding_id"]


def test_contract_frozen_i0_loader_rejects_semantically_tampered_marker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = _frozen_s7_payload()
    payload["state"] = "staged"
    pin = _write_frozen_s7_marker(tmp_path, payload)
    _patch_frozen_release_digest(monkeypatch, payload)

    with pytest.raises(
        production_contract.I3ProductionContractError,
        match="frozen I0 oracle marker is invalid",
    ):
        production_contract._load_frozen_i0_oracle_marker_exact(tmp_path, pin)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    (
        ("artifact_type", "other", "artifact type"),
        ("policy_version", "future-policy", "policy version"),
        ("release_set_version", 2, "release-set version"),
        ("state", "staged", "state"),
        ("table_order", ["universe_daily"], "table order"),
        ("visibility_rule", "public-latest", "visibility rule"),
        ("source_binding_id", "not-a-digest", "source-binding ID"),
    ),
)
def test_frozen_s7_oracle_rejects_semantic_marker_tampering(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    value: object,
    message: str,
) -> None:
    payload = _frozen_s7_payload()
    payload[field] = value
    pin = _write_frozen_s7_marker(tmp_path, payload)
    _patch_frozen_release_digest(monkeypatch, payload)
    with pytest.raises(inputs.I3ProductionInputError, match=message):
        inputs.load_frozen_s7_oracle_marker_exact(
            pin,
            content=(tmp_path / pin.path).read_bytes(),
        )


def test_frozen_s7_oracle_rejects_extra_field_wrong_id_and_noncanonical_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = _frozen_s7_payload()
    extra = {**payload, "unexpected": True}
    extra_pin = _write_frozen_s7_marker(tmp_path, extra)
    _patch_frozen_release_digest(monkeypatch, extra)
    with pytest.raises(inputs.I3ProductionInputError, match="fields differ"):
        inputs.load_frozen_s7_oracle_marker_exact(
            extra_pin,
            content=(tmp_path / extra_pin.path).read_bytes(),
        )

    wrong_id = stable_digest({"wrong": "release-set-id"})
    wrong_pin = _write_frozen_s7_marker(
        tmp_path,
        payload,
        release_set_id=wrong_id,
    )
    _patch_frozen_release_digest(monkeypatch, payload)
    with pytest.raises(inputs.I3ProductionInputError, match="ID does not reproduce"):
        inputs.load_frozen_s7_oracle_marker_exact(
            wrong_pin,
            content=(tmp_path / wrong_pin.path).read_bytes(),
        )

    noncanonical_pin = _write_frozen_s7_marker(tmp_path, payload, canonical=False)
    with pytest.raises(inputs.I3ProductionInputError, match="not canonical JSON"):
        inputs.load_frozen_s7_oracle_marker_exact(
            noncanonical_pin,
            content=(tmp_path / noncanonical_pin.path).read_bytes(),
        )


def test_strict_config_json_rejects_nonfinite_number() -> None:
    with pytest.raises(inputs.I3ProductionInputError, match="non-finite"):
        inputs._strict_json(b'{"value":NaN}\n')
