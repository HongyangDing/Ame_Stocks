"""Module-owned semantic identity for the production I3 native-v2 lane.

Operators select exact source controls and resource limits.  They cannot name
the transformation they hope was executed: this module fixes that identity to
the code-owned schema, migration, materialization, dispatch, and row-validation
rule bundle.  A base migration ID then binds that bundle to the exact controls
that were actually authenticated.
"""

from __future__ import annotations

from datetime import date
from typing import Final

from ame_stocks_api.artifacts import stable_digest
from ame_stocks_api.silver.incremental_contract import ArtifactPin
from ame_stocks_api.silver.incremental_i3_checkpoint import IdentityPolicyBundle
from ame_stocks_api.silver.incremental_i3_contract import (
    I3_V2_SCHEMA_BUNDLE_DIGEST,
    I3_V2_TABLE_ORDER,
)
from ame_stocks_api.silver.incremental_i3_dispatch import (
    I3_CALENDAR_RULE_VERSION,
    I3_COVERAGE_RULE_VERSION,
    I3_DISPATCH_RULE_VERSION,
    I3_LOCAL_ATTESTATION_RULE_VERSION,
    I3_POLICY_SNAPSHOT_VERIFICATION_RULE_VERSION,
    I3_ROW_PROOF_RULE_VERSION,
)
from ame_stocks_api.silver.incremental_i3_migration_core import (
    MIGRATION_RULE_VERSION,
    MIGRATION_SOURCE_SEED_RULE_VERSION,
)

I3_PRODUCTION_TRANSFORM_SEMANTICS_RULE_VERSION: Final = "s7_5_i3_production_transform_semantics_v2"
I3_PRODUCTION_MIGRATION_ID_RULE_VERSION: Final = "s7_5_i3_production_native_v2_migration_id_v1"

# These strings are deliberately defined outside migration_io so the RunSpec
# contract can import the semantic bundle without creating a production <-> IO
# import cycle.  migration_io uses these exact rule identifiers when producing
# and readback-validating the compact base.
I3_COMPACT_BASE_SOURCE_RULE_VERSION: Final = "s7_5_i3_compact_base_source_v1"
I3_COMPACT_BASE_INPUT_BINDING_RULE_VERSION: Final = "s7_5_i3_compact_base_exact_input_binding_v1"
I3_COMPACT_BASE_PARTITION_RECEIPT_RULE_VERSION: Final = "s7_5_i3_compact_base_partition_receipt_v1"
I3_COMPACT_BASE_S4_TERMINAL_RECEIPT_RULE_VERSION: Final = (
    "s7_5_i3_compact_base_s4_terminal_receipt_v1"
)
I3_COMPACT_BASE_UNRESOLVED_SEED_RULE_VERSION: Final = "s7_5_i3_compact_base_unresolved_seed_v1"
I3_COMPACT_BASE_ROW_VALIDATOR_RULE_VERSION: Final = "s7_5_i3_compact_base_new_root_validator_v1"
I3_COMPACT_BASE_INITIAL_SEGMENT_RULE_VERSION: Final = (
    "s7_5_i3_compact_base_initial_rowset_segment_v1"
)
I3_PRODUCTION_INDEXED_ROW_VALIDATOR_RULE_VERSION: Final = (
    "s7_5_i3_production_indexed_row_validator_v1"
)

I3_PRODUCTION_TRANSFORM_SEMANTICS_PAYLOAD: Final = {
    "artifact_type": "s7_5_i3_production_transform_semantics",
    "dispatcher_rules": {
        "calendar": I3_CALENDAR_RULE_VERSION,
        "coverage": I3_COVERAGE_RULE_VERSION,
        "dispatch": I3_DISPATCH_RULE_VERSION,
        "local_attestation": I3_LOCAL_ATTESTATION_RULE_VERSION,
        "policy_snapshot": I3_POLICY_SNAPSHOT_VERIFICATION_RULE_VERSION,
        "row_proof": I3_ROW_PROOF_RULE_VERSION,
    },
    "materialization_rules": {
        "exact_input_binding": I3_COMPACT_BASE_INPUT_BINDING_RULE_VERSION,
        "initial_rowset_segment": I3_COMPACT_BASE_INITIAL_SEGMENT_RULE_VERSION,
        "partition_receipt": I3_COMPACT_BASE_PARTITION_RECEIPT_RULE_VERSION,
        "row_validator": I3_COMPACT_BASE_ROW_VALIDATOR_RULE_VERSION,
        "s4_terminal_receipt": I3_COMPACT_BASE_S4_TERMINAL_RECEIPT_RULE_VERSION,
        "source": I3_COMPACT_BASE_SOURCE_RULE_VERSION,
        "unresolved_seed": I3_COMPACT_BASE_UNRESOLVED_SEED_RULE_VERSION,
    },
    "migration_rules": {
        "migration": MIGRATION_RULE_VERSION,
        "source_seed": MIGRATION_SOURCE_SEED_RULE_VERSION,
    },
    "row_change_validator_rule": I3_PRODUCTION_INDEXED_ROW_VALIDATOR_RULE_VERSION,
    "rule_version": I3_PRODUCTION_TRANSFORM_SEMANTICS_RULE_VERSION,
    "schema_bundle_digest": I3_V2_SCHEMA_BUNDLE_DIGEST,
}

I3_PRODUCTION_TRANSFORM_SEMANTICS_DIGEST: Final = stable_digest(
    I3_PRODUCTION_TRANSFORM_SEMANTICS_PAYLOAD
)


def production_compact_base_initial_segment_id(
    *,
    table_name: str,
    artifact: ArtifactPin,
    terminal_session: date,
    availability_session: date,
    native_v2_migration_id: str,
) -> str:
    """Derive the sole initial BASE segment identity for a versioned table."""

    if table_name not in I3_V2_TABLE_ORDER[:-1]:
        raise ValueError("compact-base initial segment table is invalid")
    if not isinstance(artifact, ArtifactPin):
        raise TypeError("compact-base initial segment artifact must be an exact ArtifactPin")
    if not artifact.path.endswith(".parquet"):
        raise ValueError("compact-base initial segment artifact must be Parquet")
    if type(terminal_session) is not date:
        raise TypeError("compact-base initial segment terminal must be a native date")
    if type(availability_session) is not date:
        raise TypeError("compact-base initial segment availability must be a native date")
    if availability_session < terminal_session:
        raise ValueError("compact-base initial segment availability precedes its terminal")
    if (
        not isinstance(native_v2_migration_id, str)
        or len(native_v2_migration_id) != 64
        or any(character not in "0123456789abcdef" for character in native_v2_migration_id)
    ):
        raise ValueError("native-v2 migration ID must be lowercase SHA-256")
    return stable_digest(
        {
            "artifact": artifact.to_dict(),
            "availability_session": availability_session.isoformat(),
            "native_v2_migration_id": native_v2_migration_id,
            "rule_version": I3_COMPACT_BASE_INITIAL_SEGMENT_RULE_VERSION,
            "table_name": table_name,
            "terminal_session": terminal_session.isoformat(),
        }
    )


def production_native_v2_migration_id(
    *,
    i0_release_set_artifact: ArtifactPin,
    s4_release_set_artifact: ArtifactPin,
    identity_policy_bundle: IdentityPolicyBundle,
    identity_policy_bundle_artifact: ArtifactPin,
    calendar_artifact: ArtifactPin,
    i2_base_frontier_artifact: ArtifactPin,
) -> str:
    """Derive one base migration identity from authenticated exact controls."""

    for value, label in (
        (i0_release_set_artifact, "I0 release-set artifact"),
        (s4_release_set_artifact, "S4 release-set artifact"),
        (identity_policy_bundle_artifact, "identity-policy bundle artifact"),
        (calendar_artifact, "calendar artifact"),
        (i2_base_frontier_artifact, "I2 base-frontier artifact"),
    ):
        if not isinstance(value, ArtifactPin):
            raise TypeError(f"{label} must be an exact ArtifactPin")
    if not isinstance(identity_policy_bundle, IdentityPolicyBundle):
        raise TypeError("identity policy bundle must be typed")
    if (
        identity_policy_bundle.exact_pin(path=identity_policy_bundle_artifact.path)
        != identity_policy_bundle_artifact
    ):
        raise ValueError("identity-policy bundle artifact does not reproduce")
    return stable_digest(
        {
            "artifact_type": "s7_5_i3_production_native_v2_migration_identity",
            "calendar_artifact": calendar_artifact.to_dict(),
            "i0_release_set_artifact": i0_release_set_artifact.to_dict(),
            "i2_base_frontier_artifact": i2_base_frontier_artifact.to_dict(),
            "identity_policy_bundle_artifact": identity_policy_bundle_artifact.to_dict(),
            "identity_policy_bundle_id": identity_policy_bundle.identity_policy_bundle_id,
            "migration_rule_version": MIGRATION_RULE_VERSION,
            "rule_version": I3_PRODUCTION_MIGRATION_ID_RULE_VERSION,
            "s4_release_set_artifact": s4_release_set_artifact.to_dict(),
            "schema_bundle_digest": I3_V2_SCHEMA_BUNDLE_DIGEST,
            "transform_semantics_digest": I3_PRODUCTION_TRANSFORM_SEMANTICS_DIGEST,
        }
    )


def production_compact_base_row_validator_digest(
    *,
    table_name: str,
    schema_digest: str,
) -> str:
    """Reproduce the sealed compact-base NEW_ROOT validator identity."""

    if not table_name or not isinstance(table_name, str):
        raise TypeError("table name must be nonempty text")
    if (
        not isinstance(schema_digest, str)
        or len(schema_digest) != 64
        or any(character not in "0123456789abcdef" for character in schema_digest)
    ):
        raise ValueError("schema digest must be lowercase SHA-256")
    return stable_digest(
        {
            "migration_rule_version": MIGRATION_RULE_VERSION,
            "operation": "new_root",
            "rule_version": I3_COMPACT_BASE_ROW_VALIDATOR_RULE_VERSION,
            "schema_digest": schema_digest,
            "table_name": table_name,
        }
    )


__all__ = [
    "I3_COMPACT_BASE_INITIAL_SEGMENT_RULE_VERSION",
    "I3_COMPACT_BASE_INPUT_BINDING_RULE_VERSION",
    "I3_COMPACT_BASE_PARTITION_RECEIPT_RULE_VERSION",
    "I3_COMPACT_BASE_ROW_VALIDATOR_RULE_VERSION",
    "I3_COMPACT_BASE_S4_TERMINAL_RECEIPT_RULE_VERSION",
    "I3_COMPACT_BASE_SOURCE_RULE_VERSION",
    "I3_COMPACT_BASE_UNRESOLVED_SEED_RULE_VERSION",
    "I3_PRODUCTION_INDEXED_ROW_VALIDATOR_RULE_VERSION",
    "I3_PRODUCTION_MIGRATION_ID_RULE_VERSION",
    "I3_PRODUCTION_TRANSFORM_SEMANTICS_DIGEST",
    "I3_PRODUCTION_TRANSFORM_SEMANTICS_PAYLOAD",
    "I3_PRODUCTION_TRANSFORM_SEMANTICS_RULE_VERSION",
    "production_compact_base_initial_segment_id",
    "production_compact_base_row_validator_digest",
    "production_native_v2_migration_id",
]
