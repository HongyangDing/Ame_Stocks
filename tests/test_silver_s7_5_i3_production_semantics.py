from __future__ import annotations

from dataclasses import replace
from datetime import date

import pytest

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver import incremental_i3_production_semantics as semantics
from ame_stocks_api.silver.incremental_contract import ArtifactPin


def _append_fields() -> dict[str, object]:
    return {
        "table_name": "asset_master",
        "parent_rowset_id": stable_digest({"fixture": "parent-rowset"}),
        "parent_segment_ids": (
            stable_digest({"fixture": "parent-segment-1"}),
            stable_digest({"fixture": "parent-segment-2"}),
        ),
        "artifact": ArtifactPin(
            path=("staging/delta/asset_master/session_date=2026-07-10/segment.parquet"),
            sha256=stable_digest({"fixture": "delta-segment"}),
            bytes=321,
        ),
        "terminal_session": date(2026, 7, 10),
        "availability_session": date(2026, 8, 4),
        "native_v2_migration_id": stable_digest({"fixture": "migration"}),
    }


def test_transform_semantics_v7_seals_bounded_base_and_every_delta_rule() -> None:
    assert semantics.I3_PRODUCTION_TRANSFORM_SEMANTICS_RULE_VERSION == (
        "s7_5_i3_production_transform_semantics_v7"
    )
    assert semantics.I3_PRODUCTION_TRANSFORM_SEMANTICS_DIGEST == (
        "fbc70e3b4c2c9708ce1b343f2a4d893761c3f2ee47c136271517cb2c361e3408"
    )
    assert semantics.I3_COMPACT_BASE_INPUT_BINDING_RULE_VERSION == (
        "s7_5_i3_compact_base_exact_input_binding_v3"
    )
    assert (
        semantics.I3_PRODUCTION_TRANSFORM_SEMANTICS_PAYLOAD["materialization_rules"][
            "exact_input_binding"
        ]
        == semantics.I3_COMPACT_BASE_INPUT_BINDING_RULE_VERSION
    )
    assert (
        semantics.I3_PRODUCTION_TRANSFORM_SEMANTICS_PAYLOAD["materialization_rules"][
            "bounded_aggregation"
        ]
        == semantics.I3_COMPACT_BASE_BOUNDED_AGGREGATION_RULE_VERSION
    )
    assert (
        semantics.I3_PRODUCTION_TRANSFORM_SEMANTICS_PAYLOAD["materialization_rules"][
            "bounded_aggregation_session_batch_cap"
        ]
        == semantics.I3_COMPACT_BASE_AGGREGATE_SESSION_BATCH_CAP
    )
    assert (
        semantics.I3_PRODUCTION_TRANSFORM_SEMANTICS_PAYLOAD["materialization_rules"][
            "exact_case_sensitive_ticker"
        ]
        == semantics.I3_EXACT_CASE_SENSITIVE_TICKER_RULE_VERSION
    )
    expected = {
        "append_segment": semantics.I3_PRODUCTION_DELTA_APPEND_SEGMENT_RULE_VERSION,
        "exact_input_binding": semantics.I3_PRODUCTION_DELTA_INPUT_BINDING_RULE_VERSION,
        "identity_fallback": semantics.I3_PRODUCTION_DELTA_IDENTITY_FALLBACK_RULE_VERSION,
        "partition_receipt": semantics.I3_PRODUCTION_DELTA_PARTITION_RECEIPT_RULE_VERSION,
        "resolution": semantics.I3_PRODUCTION_DELTA_RESOLUTION_RULE_VERSION,
        "resource_envelope": (semantics.I3_PRODUCTION_DELTA_RESOURCE_ENVELOPE_RULE_VERSION),
        "row_validator": semantics.I3_PRODUCTION_DELTA_ROW_VALIDATOR_RULE_VERSION,
        "sor_expiry": semantics.I3_PRODUCTION_DELTA_SOR_EXPIRY_RULE_VERSION,
        "source_window": semantics.I3_PRODUCTION_DELTA_SOURCE_WINDOW_RULE_VERSION,
        "state_transition": semantics.I3_PRODUCTION_DELTA_STATE_TRANSITION_RULE_VERSION,
    }
    assert (
        semantics.I3_PRODUCTION_TRANSFORM_SEMANTICS_PAYLOAD["delta_materialization_rules"]
        == expected
    )
    assert len(set(expected.values())) == len(expected)
    for name in (
        "I3_COMPACT_BASE_AGGREGATE_SESSION_BATCH_CAP",
        "I3_COMPACT_BASE_BOUNDED_AGGREGATION_RULE_VERSION",
        "I3_PRODUCTION_DELTA_APPEND_SEGMENT_RULE_VERSION",
        "I3_PRODUCTION_DELTA_IDENTITY_FALLBACK_RULE_VERSION",
        "I3_PRODUCTION_DELTA_INPUT_BINDING_RULE_VERSION",
        "I3_PRODUCTION_DELTA_PARTITION_RECEIPT_RULE_VERSION",
        "I3_PRODUCTION_DELTA_RESOLUTION_RULE_VERSION",
        "I3_PRODUCTION_DELTA_RESOURCE_ENVELOPE_RULE_VERSION",
        "I3_PRODUCTION_DELTA_ROW_VALIDATOR_RULE_VERSION",
        "I3_PRODUCTION_DELTA_SOR_EXPIRY_RULE_VERSION",
        "I3_PRODUCTION_DELTA_SOURCE_WINDOW_RULE_VERSION",
        "I3_PRODUCTION_DELTA_STATE_TRANSITION_RULE_VERSION",
        "I3_PRODUCTION_DELTA_TRANSITIVE_CONTROL_REPLAY_BYTES_CAP",
        "I3_PRODUCTION_DELTA_TRANSITIVE_CONTROL_REPLAY_RSS_RESERVE_BYTES",
        "production_delta_append_segment_id",
        "production_delta_row_validator_digest",
    ):
        assert name in semantics.__all__
    assert semantics.I3_PRODUCTION_TRANSFORM_SEMANTICS_PAYLOAD["delta_resource_limits"] == {
        "transitive_control_replay_bytes_cap": 256 * 1024**2,
        "transitive_control_replay_rss_reserve_bytes": 1024**3,
    }


def test_delta_append_segment_id_binds_parent_prefix_and_exact_output() -> None:
    fields = _append_fields()
    segment_id = semantics.production_delta_append_segment_id(**fields)
    assert segment_id == "85d3bbc185dbcb8955df2391901c4256ae25aa730f43735f5a8737e25f345d0c"
    assert semantics.production_delta_append_segment_id(**fields) == segment_id

    artifact = fields["artifact"]
    assert isinstance(artifact, ArtifactPin)
    mutations = (
        {"table_name": "ticker_alias"},
        {"parent_rowset_id": stable_digest({"fixture": "other-rowset"})},
        {
            "parent_segment_ids": tuple(
                reversed(fields["parent_segment_ids"])  # type: ignore[arg-type]
            )
        },
        {
            "artifact": replace(
                artifact,
                sha256=stable_digest({"fixture": "other-delta-segment"}),
            )
        },
        {"terminal_session": date(2026, 7, 11)},
        {"availability_session": date(2026, 8, 5)},
        {"native_v2_migration_id": stable_digest({"fixture": "other-migration"})},
    )
    for mutation in mutations:
        assert semantics.production_delta_append_segment_id(**{**fields, **mutation}) != segment_id


def test_delta_append_segment_id_rejects_noncanonical_or_incomplete_parent_prefix() -> None:
    fields = _append_fields()
    segment_ids = fields["parent_segment_ids"]
    assert isinstance(segment_ids, tuple)

    with pytest.raises(ValueError, match="parent rowset ID"):
        semantics.production_delta_append_segment_id(
            **{**fields, "parent_rowset_id": "not-a-digest"}
        )
    with pytest.raises(TypeError, match="nonempty canonical tuple"):
        semantics.production_delta_append_segment_id(
            **{**fields, "parent_segment_ids": list(segment_ids)}
        )
    with pytest.raises(TypeError, match="nonempty canonical tuple"):
        semantics.production_delta_append_segment_id(**{**fields, "parent_segment_ids": ()})
    with pytest.raises(ValueError, match="must not contain duplicates"):
        semantics.production_delta_append_segment_id(
            **{**fields, "parent_segment_ids": (segment_ids[0], segment_ids[0])}
        )
    with pytest.raises(ValueError, match="parent segment ID"):
        semantics.production_delta_append_segment_id(
            **{**fields, "parent_segment_ids": ("not-a-digest",)}
        )
    with pytest.raises(ValueError, match="table is invalid"):
        semantics.production_delta_append_segment_id(**{**fields, "table_name": "universe_daily"})
    with pytest.raises(ValueError, match="must be Parquet"):
        semantics.production_delta_append_segment_id(
            **{
                **fields,
                "artifact": replace(fields["artifact"], path="delta/segment.json"),
            }
        )
    with pytest.raises(ValueError, match="precedes its terminal"):
        semantics.production_delta_append_segment_id(
            **{
                **fields,
                "availability_session": date(2026, 7, 9),
            }
        )


def test_delta_row_validator_digest_is_operation_specific_and_closed() -> None:
    fields = {
        "table_name": "asset_master",
        "schema_digest": stable_digest({"fixture": "schema"}),
    }
    new_root = semantics.production_delta_row_validator_digest(
        **fields,
        operation="new_root",
    )
    successor = semantics.production_delta_row_validator_digest(
        **fields,
        operation="mechanical_successor",
    )
    assert new_root == "4dbe9eb1bc1c43b45e4854bf190c662fb98105509cc2f8ae1a406df9df02ff69"
    assert successor == "8ffc3f505ac460290aceed3db78af0d44aa7eb113feeff7ae1b17971af614ccd"
    assert successor != new_root

    for operation in ("correction", "NEW_ROOT", "", None):
        with pytest.raises(ValueError, match="operation is invalid"):
            semantics.production_delta_row_validator_digest(
                **fields,
                operation=operation,  # type: ignore[arg-type]
            )
    with pytest.raises(ValueError, match="table is invalid"):
        semantics.production_delta_row_validator_digest(
            **{**fields, "table_name": "universe_daily"},
            operation="new_root",
        )
    with pytest.raises(ValueError, match="schema digest"):
        semantics.production_delta_row_validator_digest(
            **{**fields, "schema_digest": "not-a-digest"},
            operation="new_root",
        )
