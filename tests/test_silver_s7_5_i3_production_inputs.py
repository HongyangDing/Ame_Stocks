from __future__ import annotations

import json
from pathlib import Path

import pytest
from test_silver_s7_5_i3_migration_io import _base_fixture

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver import incremental_i3_production_inputs as inputs
from ame_stocks_api.silver.asset_incremental_contract import S4BaseFrontier
from ame_stocks_api.silver.incremental_contract import ArtifactPin
from ame_stocks_api.silver.incremental_i3_production_contract import (
    I3ProductionResourceCaps,
)
from ame_stocks_api.silver.incremental_i3_production_semantics import (
    I3_PRODUCTION_TRANSFORM_SEMANTICS_DIGEST,
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


def test_strict_config_json_rejects_nonfinite_number() -> None:
    with pytest.raises(inputs.I3ProductionInputError, match="non-finite"):
        inputs._strict_json(b'{"value":NaN}\n')
