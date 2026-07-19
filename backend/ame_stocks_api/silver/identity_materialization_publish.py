"""Publish one exact S7 streaming Full as a visibility-atomic four-table release set.

The streaming materializer deliberately stops at ``awaiting_review``.  This module is
the independent publication gate.  It accepts only exact Full control IDs, replays the
Full completion/candidate/QA/contracts/source binding, writes one durable group intent,
then writes four hidden immutable member manifests.  The final release-set marker is the
only consumer-visible commit point.

Production entry points require the canonical production data root and own their clock,
runtime verifier, registry loader, and lock.  Fixture seams are private and cannot touch
the canonical production namespace.
"""

from __future__ import annotations

import hashlib
import json
import stat
from collections.abc import Callable, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from types import MappingProxyType
from typing import Final

from ame_stocks_api.artifacts import (
    ArtifactError,
    safe_relative_path,
    sha256_file,
    stable_digest,
    write_bytes_immutable,
)
from ame_stocks_api.silver import identity_materialization_streaming as streaming
from ame_stocks_api.silver.identity_materialization_streaming import (
    PRODUCTION_ADAPTER_VERSION,
    S7_STANDING_AUTHORIZATION_SHA256,
    S7_STANDING_AUTHORIZATION_TEXT,
    S7_STANDING_REAFFIRMATION_SHA256,
    S7_STANDING_REAFFIRMATION_TEXT,
    STREAMING_POLICY_VERSION,
    STREAMING_STATE,
    TABLE_ORDER,
    ExactFilePin,
    S7StreamingSourceBinding,
    StreamingResourceCaps,
)
from ame_stocks_api.silver.identity_registry_workflow import (
    CANONICAL_PRODUCTION_DATA_ROOT,
    LoadedRegistryReleaseSet,
    is_canonical_production_data_root,
    load_registry_release_set,
)

PUBLISH_POLICY_VERSION: Final = "s7-four-table-atomic-release-set-v1"
PUBLISH_PLAN_VERSION: Final = 1
PUBLISH_APPROVAL_VERSION: Final = 1
PUBLISH_INTENT_VERSION: Final = 1
TABLE_RELEASE_VERSION: Final = 1
RELEASE_SET_VERSION: Final = 1
PUBLISH_AUTHORIZED_ACTION: Final = "publish_exact_s7_four_table_release_set_once"
PUBLISH_APPROVAL_LITERAL_VERSION: Final = "s7_four_table_publish_standing_approval_v1"

_PLAN_SLOT_KEYS: Final = (
    "artifact_type",
    "authorized_action",
    "calendar_artifact_id",
    "calendar_artifact_sha256",
    "candidate_id",
    "candidate_manifest",
    "candidate_qa",
    "contract_approvals",
    "contract_pins",
    "full_approval",
    "full_approval_id",
    "full_completion",
    "full_completion_id",
    "full_plan",
    "full_plan_id",
    "members",
    "plan_version",
    "policy_version",
    "runtime_binding",
    "source_binding",
    "source_binding_id",
    "source_cutoff_session",
    "table_order",
)

_PUBLISH_CAPABILITIES: Final = MappingProxyType(
    {
        "candidate_mutation_authorized": False,
        "latest_discovery_authorized": False,
        "network_authorized": False,
        "partial_member_visibility_authorized": False,
        "publish_authorized": True,
        "registry_mutation_authorized": False,
    }
)


class S7IdentityPublishError(RuntimeError):
    """Raised before an unbound or partially visible S7 publication can be accepted."""


@dataclass(frozen=True, slots=True)
class S7PublishRunResult:
    publish_plan_id: str
    approval_id: str
    intent_id: str
    release_set_id: str
    release_set_path: str
    member_release_ids: Mapping[str, str]
    release_available_session: date
    idempotent: bool
    state: str = "published"

    def __post_init__(self) -> None:
        for label, value in (
            ("publish plan ID", self.publish_plan_id),
            ("approval ID", self.approval_id),
            ("intent ID", self.intent_id),
            ("release-set ID", self.release_set_id),
        ):
            _digest(value, label)
        if tuple(self.member_release_ids) != TABLE_ORDER:
            raise S7IdentityPublishError("member release result order differs")
        object.__setattr__(
            self,
            "member_release_ids",
            MappingProxyType(dict(self.member_release_ids)),
        )


@dataclass(frozen=True, slots=True)
class _FullSnapshot:
    controls: Mapping[str, object]
    binding: S7StreamingSourceBinding
    candidate_id: str
    candidate_manifest: ExactFilePin
    candidate_qa: ExactFilePin
    completion: ExactFilePin
    completion_id: str
    members: tuple[dict[str, object], ...]


def prepare_s7_publish_plan(
    data_root: Path,
    *,
    full_plan_id: str,
    full_approval_id: str,
    expected_completion_id: str,
    expected_candidate_id: str,
    prepared_by: str,
) -> tuple[dict[str, object], ExactFilePin]:
    """Prepare the exact production PublishPlan from one completed Full."""

    root = _production_root(data_root)
    return _prepare_s7_publish_plan(
        root,
        full_plan_id=full_plan_id,
        full_approval_id=full_approval_id,
        expected_completion_id=expected_completion_id,
        expected_candidate_id=expected_candidate_id,
        prepared_by=prepared_by,
        runtime_probe=streaming._repository_runtime_binding,
        registry_loader=load_registry_release_set,
        now=lambda: datetime.now(UTC),
        production=True,
    )


def _prepare_s7_publish_plan_fixture(
    data_root: Path,
    *,
    full_plan_id: str,
    full_approval_id: str,
    expected_completion_id: str,
    expected_candidate_id: str,
    prepared_by: str,
    runtime_probe: Callable[[], Mapping[str, object]],
    registry_loader: Callable[..., LoadedRegistryReleaseSet],
    now: Callable[[], datetime],
) -> tuple[dict[str, object], ExactFilePin]:
    root = _fixture_root(data_root)
    return _prepare_s7_publish_plan(
        root,
        full_plan_id=full_plan_id,
        full_approval_id=full_approval_id,
        expected_completion_id=expected_completion_id,
        expected_candidate_id=expected_candidate_id,
        prepared_by=prepared_by,
        runtime_probe=runtime_probe,
        registry_loader=registry_loader,
        now=now,
        production=False,
    )


def _prepare_s7_publish_plan(
    root: Path,
    *,
    full_plan_id: str,
    full_approval_id: str,
    expected_completion_id: str,
    expected_candidate_id: str,
    prepared_by: str,
    runtime_probe: Callable[[], Mapping[str, object]],
    registry_loader: Callable[..., LoadedRegistryReleaseSet],
    now: Callable[[], datetime],
    production: bool,
) -> tuple[dict[str, object], ExactFilePin]:
    snapshot = _replay_full(
        root,
        full_plan_id=full_plan_id,
        full_approval_id=full_approval_id,
        expected_completion_id=expected_completion_id,
        expected_candidate_id=expected_candidate_id,
        runtime_probe=runtime_probe,
        registry_loader=registry_loader,
        production=production,
    )
    slot = _publish_plan_slot(snapshot)
    plan_id = stable_digest(slot)
    relative = _publish_plan_path(plan_id)
    target = safe_relative_path(root, relative)
    actor = _text(prepared_by, "PublishPlan preparer")
    lock = safe_relative_path(
        root, f"manifests/silver/locks/s7-four-table-publish-plan-{plan_id}.lock"
    )
    with _exclusive_lock(lock):
        if target.exists() or target.is_symlink():
            stored, pin = _load_publish_plan(root, plan_id)
            if stored["prepared_by"] != actor:
                raise S7IdentityPublishError("PublishPlan fixed-slot actor differs")
            _assert_plan_matches_snapshot(stored, snapshot)
            return stored, pin
        prepared_at = _utc(now(), "PublishPlan runtime clock")
        completion = _read_document(root, snapshot.completion.path, "Full completion")
        if prepared_at < _utc_from_text(completion["completed_at_utc"], "Full completion time"):
            raise S7IdentityPublishError("PublishPlan predates Full completion")
        document = {
            **slot,
            "artifact_type": "s7_four_table_publish_plan",
            "capabilities": dict(_PUBLISH_CAPABILITIES),
            "plan_id": plan_id,
            "prepared_at_utc": _utc_text(prepared_at),
            "prepared_availability": _calendar_availability(root, snapshot.binding, prepared_at),
            "prepared_by": actor,
            "state": "awaiting_publish_approval",
        }
        pin = _write_control(root, relative, document, "S7 PublishPlan")
        loaded, loaded_pin = _load_publish_plan(root, plan_id)
        if loaded != document or loaded_pin != pin:
            raise S7IdentityPublishError("stored PublishPlan replay differs")
        return loaded, loaded_pin


def record_standing_s7_publish_approval(
    data_root: Path,
    *,
    publish_plan_id: str,
    approved_by: str,
) -> tuple[dict[str, object], ExactFilePin]:
    """Record one fixed-slot standing approval for the exact PublishPlan."""

    root = _production_root(data_root)
    return _record_standing_s7_publish_approval(
        root,
        publish_plan_id=publish_plan_id,
        approved_by=approved_by,
        runtime_probe=streaming._repository_runtime_binding,
        registry_loader=load_registry_release_set,
        now=lambda: datetime.now(UTC),
        production=True,
    )


def _record_standing_s7_publish_approval_fixture(
    data_root: Path,
    *,
    publish_plan_id: str,
    approved_by: str,
    runtime_probe: Callable[[], Mapping[str, object]],
    registry_loader: Callable[..., LoadedRegistryReleaseSet],
    now: Callable[[], datetime],
) -> tuple[dict[str, object], ExactFilePin]:
    root = _fixture_root(data_root)
    return _record_standing_s7_publish_approval(
        root,
        publish_plan_id=publish_plan_id,
        approved_by=approved_by,
        runtime_probe=runtime_probe,
        registry_loader=registry_loader,
        now=now,
        production=False,
    )


def _record_standing_s7_publish_approval(
    root: Path,
    *,
    publish_plan_id: str,
    approved_by: str,
    runtime_probe: Callable[[], Mapping[str, object]],
    registry_loader: Callable[..., LoadedRegistryReleaseSet],
    now: Callable[[], datetime],
    production: bool,
) -> tuple[dict[str, object], ExactFilePin]:
    plan, plan_pin = _load_publish_plan(root, publish_plan_id)
    snapshot = _replay_plan_full(
        root,
        plan,
        runtime_probe=runtime_probe,
        registry_loader=registry_loader,
        production=production,
    )
    _assert_plan_matches_snapshot(plan, snapshot)
    slot = _approval_slot(plan, plan_pin)
    approval_id = stable_digest(slot)
    relative = _publish_approval_path(plan["plan_id"])
    target = safe_relative_path(root, relative)
    actor = _text(approved_by, "Publish approver")
    lock = safe_relative_path(
        root,
        f"manifests/silver/locks/s7-four-table-publish-approval-{approval_id}.lock",
    )
    with _exclusive_lock(lock):
        if target.exists() or target.is_symlink():
            stored, pin = _load_publish_approval(
                root, plan=plan, plan_pin=plan_pin, approval_id=approval_id
            )
            if stored["approved_by"] != actor:
                raise S7IdentityPublishError("Publish approval fixed-slot actor differs")
            return stored, pin
        approved_at = _utc(now(), "Publish approval runtime clock")
        if approved_at < _utc_from_text(plan["prepared_at_utc"], "PublishPlan time"):
            raise S7IdentityPublishError("Publish approval predates PublishPlan")
        availability = _calendar_availability(root, snapshot.binding, approved_at)
        document = {
            **slot,
            "approval_availability": availability,
            "approval_id": approval_id,
            "approved_at_utc": _utc_text(approved_at),
            "approved_by": actor,
            "artifact_type": "s7_four_table_publish_standing_approval",
            "capabilities": dict(_PUBLISH_CAPABILITIES),
            "exact_literal_sha256": hashlib.sha256(
                exact_s7_publish_approval_literal(plan, plan_pin).encode("utf-8")
            ).hexdigest(),
            "standing_authorization": {
                "literal_text": S7_STANDING_AUTHORIZATION_TEXT,
                "literal_text_sha256": S7_STANDING_AUTHORIZATION_SHA256,
            },
            "standing_reaffirmation": {
                "literal_text": S7_STANDING_REAFFIRMATION_TEXT,
                "literal_text_sha256": S7_STANDING_REAFFIRMATION_SHA256,
            },
            "state": "approved_for_exact_publish",
        }
        pin = _write_control(root, relative, document, "S7 Publish approval")
        loaded, loaded_pin = _load_publish_approval(
            root, plan=plan, plan_pin=plan_pin, approval_id=approval_id
        )
        if loaded != document or loaded_pin != pin:
            raise S7IdentityPublishError("stored Publish approval replay differs")
        return loaded, loaded_pin


def publish_s7_release_set(
    data_root: Path,
    *,
    publish_plan_id: str,
    approval_id: str,
) -> S7PublishRunResult:
    """Publish four hidden members and commit visibility with one final marker."""

    root = _production_root(data_root)
    return _publish_s7_release_set(
        root,
        publish_plan_id=publish_plan_id,
        approval_id=approval_id,
        runtime_probe=streaming._repository_runtime_binding,
        registry_loader=load_registry_release_set,
        now=lambda: datetime.now(UTC),
        checkpoint_hook=None,
        production=True,
    )


def _publish_s7_release_set_fixture(
    data_root: Path,
    *,
    publish_plan_id: str,
    approval_id: str,
    runtime_probe: Callable[[], Mapping[str, object]],
    registry_loader: Callable[..., LoadedRegistryReleaseSet],
    now: Callable[[], datetime],
    checkpoint_hook: Callable[[str, str | None], None] | None = None,
) -> S7PublishRunResult:
    root = _fixture_root(data_root)
    return _publish_s7_release_set(
        root,
        publish_plan_id=publish_plan_id,
        approval_id=approval_id,
        runtime_probe=runtime_probe,
        registry_loader=registry_loader,
        now=now,
        checkpoint_hook=checkpoint_hook,
        production=False,
    )


def _publish_s7_release_set(
    root: Path,
    *,
    publish_plan_id: str,
    approval_id: str,
    runtime_probe: Callable[[], Mapping[str, object]],
    registry_loader: Callable[..., LoadedRegistryReleaseSet],
    now: Callable[[], datetime],
    checkpoint_hook: Callable[[str, str | None], None] | None,
    production: bool,
) -> S7PublishRunResult:
    plan_id = _digest(publish_plan_id, "PublishPlan ID")
    expected_approval_id = _digest(approval_id, "Publish approval ID")
    lock = safe_relative_path(root, f"manifests/silver/locks/s7-four-table-publish-{plan_id}.lock")
    with _exclusive_lock(lock):
        plan, plan_pin = _load_publish_plan(root, plan_id)
        approval, approval_pin = _load_publish_approval(
            root,
            plan=plan,
            plan_pin=plan_pin,
            approval_id=expected_approval_id,
        )
        intent, intent_pin, member_documents = _load_or_store_intent(
            root,
            plan=plan,
            plan_pin=plan_pin,
            approval=approval,
            approval_pin=approval_pin,
            now=now,
        )
        _checkpoint(checkpoint_hook, "intent_durable", None)
        marker = _release_set_document(
            plan=plan,
            plan_pin=plan_pin,
            approval=approval,
            approval_pin=approval_pin,
            intent=intent,
            intent_pin=intent_pin,
        )
        release_set_id = _digest(marker["release_set_id"], "release-set ID")
        marker_relative = _release_set_path(release_set_id)
        marker_target = safe_relative_path(root, marker_relative)

        # The durable intent above is the last boundary before physical Full/source replay.
        snapshot = _replay_plan_full(
            root,
            plan,
            runtime_probe=runtime_probe,
            registry_loader=registry_loader,
            production=production,
        )
        _assert_plan_matches_snapshot(plan, snapshot)
        if marker_target.exists() or marker_target.is_symlink():
            verified = _load_published_s7_release_set(
                root,
                release_set_id,
                runtime_probe=runtime_probe,
                registry_loader=registry_loader,
                production=production,
            )
            return _result_from_marker(verified, idempotent=True)

        for descriptor, document in zip(intent["members"], member_documents, strict=True):
            member = _mapping(descriptor, "intent member")
            table = _table(member["table_name"])
            pin = _write_control(
                root,
                _relative(member["path"], "member release path"),
                document,
                f"{table} S7 member release",
            )
            if pin.to_dict() != {
                "bytes": member["bytes"],
                "path": member["path"],
                "sha256": member["sha256"],
            }:
                raise S7IdentityPublishError("member release receipt differs from intent")
            _checkpoint(checkpoint_hook, "member_durable", table)

        _checkpoint(checkpoint_hook, "before_marker", None)
        # Recheck candidate bytes and replay the complete source/registry chain after
        # the final checkpoint, immediately before the only visible commit point.
        _verify_candidate_receipts_from_plan(root, plan)
        _replay_plan_source_chain(
            root,
            plan,
            runtime_probe=runtime_probe,
            registry_loader=registry_loader,
            production=production,
        )
        marker_pin = _write_control(
            root, marker_relative, marker, "S7 four-table release-set marker"
        )
        if marker_pin.sha256 != hashlib.sha256(_canonical_bytes(marker)).hexdigest():
            raise S7IdentityPublishError("release-set marker receipt differs")
        _checkpoint(checkpoint_hook, "marker_durable", None)
        verified = _load_published_s7_release_set(
            root,
            release_set_id,
            runtime_probe=runtime_probe,
            registry_loader=registry_loader,
            production=production,
        )
        return _result_from_marker(verified, idempotent=False)


def load_published_s7_release_set(data_root: Path, *, release_set_id: str) -> dict[str, object]:
    """Load one exact production release set; no latest/discovery lane exists."""

    return _load_published_s7_release_set(
        _production_root(data_root),
        _digest(release_set_id, "release-set ID"),
        runtime_probe=streaming._repository_runtime_binding,
        registry_loader=load_registry_release_set,
        production=True,
    )


def _load_published_s7_release_set_fixture(
    data_root: Path,
    *,
    release_set_id: str,
    runtime_probe: Callable[[], Mapping[str, object]],
    registry_loader: Callable[..., LoadedRegistryReleaseSet],
) -> dict[str, object]:
    return _load_published_s7_release_set(
        _fixture_root(data_root),
        _digest(release_set_id, "release-set ID"),
        runtime_probe=runtime_probe,
        registry_loader=registry_loader,
        production=False,
    )


def exact_s7_publish_approval_literal(plan: Mapping[str, object], plan_pin: ExactFilePin) -> str:
    """Return the exact literal bound by the standing Publish approval."""

    item = _mapping(plan, "PublishPlan")
    payload = {
        "authorized_action": PUBLISH_AUTHORIZED_ACTION,
        "candidate_id": item["candidate_id"],
        "full_completion_id": item["full_completion_id"],
        "literal_version": PUBLISH_APPROVAL_LITERAL_VERSION,
        "plan": plan_pin.to_dict(),
        "plan_id": item["plan_id"],
        "source_binding_id": item["source_binding_id"],
        "table_order": list(TABLE_ORDER),
    }
    return json.dumps(payload, allow_nan=False, separators=(",", ":"), sort_keys=True)


def _replay_plan_full(
    root: Path,
    plan: Mapping[str, object],
    *,
    runtime_probe: Callable[[], Mapping[str, object]],
    registry_loader: Callable[..., LoadedRegistryReleaseSet],
    production: bool,
) -> _FullSnapshot:
    return _replay_full(
        root,
        full_plan_id=_digest(plan["full_plan_id"], "Full plan ID"),
        full_approval_id=_digest(plan["full_approval_id"], "Full approval ID"),
        expected_completion_id=_digest(plan["full_completion_id"], "Full completion ID"),
        expected_candidate_id=_digest(plan["candidate_id"], "candidate ID"),
        runtime_probe=runtime_probe,
        registry_loader=registry_loader,
        production=production,
    )


def _replay_plan_source_chain(
    root: Path,
    plan: Mapping[str, object],
    *,
    runtime_probe: Callable[[], Mapping[str, object]],
    registry_loader: Callable[..., LoadedRegistryReleaseSet],
    production: bool,
) -> None:
    """Replay every expanded upstream pin without rescanning candidate table semantics."""

    try:
        source_binding_id = _digest(plan["source_binding_id"], "source binding ID")
        binding, binding_pin = streaming._load_source_binding(root, source_binding_id)
        if binding_pin != ExactFilePin.from_dict(plan["source_binding"]):
            raise S7IdentityPublishError("Publish source-binding receipt differs")
        if production != (binding.mode == "production"):
            raise S7IdentityPublishError("production and fixture Publish boundaries crossed")
        runtime = dict(runtime_probe())
        streaming._validate_runtime_binding(runtime)
        if runtime != dict(binding.runtime_binding) or runtime != dict(
            _mapping(plan["runtime_binding"], "PublishPlan runtime binding")
        ):
            raise S7IdentityPublishError("Publish runtime differs from frozen Full runtime")
        if [item.to_dict() for item in binding.contract_approvals] != _array(
            plan["contract_approvals"], "PublishPlan contract approvals"
        ):
            raise S7IdentityPublishError("Publish contract approvals differ from source binding")
        streaming._load_verified_execution_sources(
            root,
            binding=binding,
            registry_loader=registry_loader,
        )
    except S7IdentityPublishError:
        raise
    except Exception as exc:
        raise S7IdentityPublishError("Publish source-chain replay failed") from exc


def _replay_full(
    root: Path,
    *,
    full_plan_id: str,
    full_approval_id: str,
    expected_completion_id: str,
    expected_candidate_id: str,
    runtime_probe: Callable[[], Mapping[str, object]],
    registry_loader: Callable[..., LoadedRegistryReleaseSet],
    production: bool,
) -> _FullSnapshot:
    try:
        controls = streaming._load_execution_controls(
            root,
            plan_id=_digest(full_plan_id, "Full plan ID"),
            approval_id=_digest(full_approval_id, "Full approval ID"),
        )
        binding = controls["binding"]
        if not isinstance(binding, S7StreamingSourceBinding):
            raise S7IdentityPublishError("Full source binding type differs")
        if production != (binding.mode == "production"):
            raise S7IdentityPublishError("production and fixture Publish boundaries crossed")
        runtime = dict(runtime_probe())
        streaming._validate_runtime_binding(runtime)
        if runtime != dict(binding.runtime_binding):
            raise S7IdentityPublishError("Publish runtime differs from frozen Full runtime")
        if binding.contract_approvals != streaming._trusted_contract_approvals(root, binding):
            raise S7IdentityPublishError("four v4 contract approvals changed")
        completion_relative = streaming._completion_path(
            _digest(full_plan_id, "Full plan ID"),
            _digest(full_approval_id, "Full approval ID"),
        )
        completion_content = streaming._read_exact_file(
            root, completion_relative, label="streaming Full completion"
        )
        completion = _mapping(
            streaming._load_canonical_json(completion_content, "streaming Full completion"),
            "streaming Full completion",
        )
        candidate_id = _digest(completion.get("candidate_id"), "candidate ID")
        if candidate_id != _digest(expected_candidate_id, "expected candidate ID"):
            raise S7IdentityPublishError("expected candidate ID differs")
        if completion.get("completion_id") != _digest(
            expected_completion_id, "expected completion ID"
        ):
            raise S7IdentityPublishError("expected Full completion ID differs")
        full_plan = _mapping(controls["plan"], "Full plan")
        full_approval = controls["approval"]
        if production:
            expected_production_candidate = stable_digest(
                {
                    "adapter_version": PRODUCTION_ADAPTER_VERSION,
                    "approval_id": full_approval.approval_id,
                    "engine_version": STREAMING_POLICY_VERSION,
                    "plan_id": full_plan["plan_id"],
                    "source_binding_id": binding.source_binding_id,
                }
            )
            if candidate_id != expected_production_candidate:
                raise S7IdentityPublishError("Full candidate was not built by frozen adapter")
        result = streaming._verify_completion_and_candidate(
            root,
            safe_relative_path(root, completion_relative),
            plan=full_plan,
            approval=full_approval,
            binding=binding,
            expected_candidate_id=candidate_id,
            caps=StreamingResourceCaps.from_dict(full_plan["resource_caps"]),
            idempotent=True,
        )
        if result.state != STREAMING_STATE or completion.get("complete") is not True:
            raise S7IdentityPublishError("Full completion is not awaiting_review complete")
        streaming._load_verified_execution_sources(
            root, binding=binding, registry_loader=registry_loader
        )
        candidate_relative = streaming._candidate_path(candidate_id)
        candidate_manifest_relative = f"{candidate_relative}/manifest.json"
        candidate_content = streaming._read_exact_file(
            root, candidate_manifest_relative, label="streaming candidate manifest"
        )
        candidate = _mapping(
            streaming._load_canonical_json(candidate_content, "streaming candidate manifest"),
            "streaming candidate manifest",
        )
        outputs = _mapping(candidate["outputs"], "candidate outputs")
        qa_receipt = _mapping(outputs["qa"], "candidate QA receipt")
        qa_pin = _absolute_candidate_pin(root, candidate_relative, qa_receipt, "candidate QA")
        qa = _read_document(root, qa_pin.path, "candidate QA")
        if qa.get("critical_failure_count") != 0:
            raise S7IdentityPublishError("Full candidate critical QA is nonzero")
        table_counts = _mapping(candidate["table_row_counts"], "table row counts")
        members: list[dict[str, object]] = []
        for table in TABLE_ORDER:
            raw_outputs = outputs[table]
            receipts = raw_outputs if isinstance(raw_outputs, list) else [raw_outputs]
            absolute = [
                _absolute_candidate_receipt(
                    root,
                    candidate_relative,
                    _mapping(item, f"{table} output receipt"),
                    table,
                )
                for item in receipts
            ]
            members.append(
                {
                    "contract": streaming._contract_pins()[table],
                    "output_receipts": absolute,
                    "output_set_digest": stable_digest(absolute),
                    "row_count": _nonnegative(table_counts[table], f"{table} row count"),
                    "table_name": table,
                }
            )
        completion_pin = ExactFilePin(
            completion_relative,
            hashlib.sha256(completion_content).hexdigest(),
            len(completion_content),
        )
        candidate_pin = ExactFilePin(
            candidate_manifest_relative,
            hashlib.sha256(candidate_content).hexdigest(),
            len(candidate_content),
        )
        return _FullSnapshot(
            controls=MappingProxyType(dict(controls)),
            binding=binding,
            candidate_id=candidate_id,
            candidate_manifest=candidate_pin,
            candidate_qa=qa_pin,
            completion=completion_pin,
            completion_id=_digest(completion["completion_id"], "Full completion ID"),
            members=tuple(members),
        )
    except S7IdentityPublishError:
        raise
    except Exception as exc:
        raise S7IdentityPublishError("Full publication replay failed") from exc


def _publish_plan_slot(snapshot: _FullSnapshot) -> dict[str, object]:
    controls = snapshot.controls
    plan = _mapping(controls["plan"], "Full plan")
    plan_pin = controls["plan_receipt"]
    approval_pin = controls["approval_receipt"]
    if not isinstance(plan_pin, ExactFilePin) or not isinstance(approval_pin, ExactFilePin):
        raise S7IdentityPublishError("Full control receipts differ")
    return {
        "artifact_type": "s7_four_table_publish_plan_slot",
        "authorized_action": PUBLISH_AUTHORIZED_ACTION,
        "calendar_artifact_id": snapshot.binding.calendar_artifact_id,
        "calendar_artifact_sha256": snapshot.binding.calendar_artifact_sha256,
        "candidate_id": snapshot.candidate_id,
        "candidate_manifest": snapshot.candidate_manifest.to_dict(),
        "candidate_qa": snapshot.candidate_qa.to_dict(),
        "contract_approvals": [item.to_dict() for item in snapshot.binding.contract_approvals],
        "contract_pins": streaming._contract_pins(),
        "full_approval": approval_pin.to_dict(),
        "full_approval_id": controls["approval"].approval_id,
        "full_completion": snapshot.completion.to_dict(),
        "full_completion_id": snapshot.completion_id,
        "full_plan": plan_pin.to_dict(),
        "full_plan_id": plan["plan_id"],
        "members": list(snapshot.members),
        "plan_version": PUBLISH_PLAN_VERSION,
        "policy_version": PUBLISH_POLICY_VERSION,
        "runtime_binding": dict(snapshot.binding.runtime_binding),
        "source_binding": plan["source_binding"],
        "source_binding_id": snapshot.binding.source_binding_id,
        "source_cutoff_session": snapshot.binding.cutoff_session.isoformat(),
        "table_order": list(TABLE_ORDER),
    }


def _assert_plan_matches_snapshot(plan: Mapping[str, object], snapshot: _FullSnapshot) -> None:
    expected = _publish_plan_slot(snapshot)
    observed = {key: plan[key] for key in _PLAN_SLOT_KEYS}
    observed["artifact_type"] = "s7_four_table_publish_plan_slot"
    if observed != expected:
        raise S7IdentityPublishError("PublishPlan no longer matches Full replay")


def _load_publish_plan(root: Path, publish_plan_id: str) -> tuple[dict[str, object], ExactFilePin]:
    identifier = _digest(publish_plan_id, "PublishPlan ID")
    relative = _publish_plan_path(identifier)
    content, pin = _read_control(root, relative, "S7 PublishPlan")
    plan = _mapping(_load_canonical_json(content, "S7 PublishPlan"), "S7 PublishPlan")
    expected = {
        *_PLAN_SLOT_KEYS,
        "capabilities",
        "plan_id",
        "prepared_at_utc",
        "prepared_availability",
        "prepared_by",
        "state",
    }
    _expect_keys(plan, expected, "S7 PublishPlan")
    slot = {key: plan[key] for key in _PLAN_SLOT_KEYS}
    slot["artifact_type"] = "s7_four_table_publish_plan_slot"
    if (
        plan["artifact_type"] != "s7_four_table_publish_plan"
        or plan["plan_id"] != identifier
        or stable_digest(slot) != identifier
        or plan["authorized_action"] != PUBLISH_AUTHORIZED_ACTION
        or plan["capabilities"] != dict(_PUBLISH_CAPABILITIES)
        or plan["plan_version"] != PUBLISH_PLAN_VERSION
        or plan["policy_version"] != PUBLISH_POLICY_VERSION
        or plan["state"] != "awaiting_publish_approval"
        or plan["table_order"] != list(TABLE_ORDER)
    ):
        raise S7IdentityPublishError("S7 PublishPlan semantics differ")
    prepared_at = _utc_from_text(plan["prepared_at_utc"], "PublishPlan time")
    _text(plan["prepared_by"], "PublishPlan preparer")
    if plan["prepared_availability"] != _calendar_availability(
        root, _binding_stub_from_plan(plan), prepared_at
    ):
        raise S7IdentityPublishError("PublishPlan calendar availability differs")
    _validate_plan_members(plan["members"])
    return plan, pin


def _approval_slot(plan: Mapping[str, object], plan_pin: ExactFilePin) -> dict[str, object]:
    return {
        "approval_version": PUBLISH_APPROVAL_VERSION,
        "artifact_type": "s7_four_table_publish_standing_approval_slot",
        "authorization_mode": "standing_s7_exact_publish_plan",
        "authorized_action": PUBLISH_AUTHORIZED_ACTION,
        "candidate_id": plan["candidate_id"],
        "full_completion_id": plan["full_completion_id"],
        "literal_version": PUBLISH_APPROVAL_LITERAL_VERSION,
        "plan": plan_pin.to_dict(),
        "plan_id": plan["plan_id"],
        "source_binding_id": plan["source_binding_id"],
        "standing_authorization_sha256": S7_STANDING_AUTHORIZATION_SHA256,
        "standing_reaffirmation_sha256": S7_STANDING_REAFFIRMATION_SHA256,
    }


def _load_publish_approval(
    root: Path,
    *,
    plan: Mapping[str, object],
    plan_pin: ExactFilePin,
    approval_id: str,
) -> tuple[dict[str, object], ExactFilePin]:
    identifier = _digest(approval_id, "Publish approval ID")
    relative = _publish_approval_path(_digest(plan["plan_id"], "PublishPlan ID"))
    content, pin = _read_control(root, relative, "S7 Publish approval")
    approval = _mapping(_load_canonical_json(content, "S7 Publish approval"), "S7 Publish approval")
    slot = _approval_slot(plan, plan_pin)
    expected = {
        *slot,
        "approval_availability",
        "approval_id",
        "approved_at_utc",
        "approved_by",
        "capabilities",
        "exact_literal_sha256",
        "standing_authorization",
        "standing_reaffirmation",
        "state",
    }
    _expect_keys(approval, expected, "S7 Publish approval")
    approved_at = _utc_from_text(approval["approved_at_utc"], "Publish approval time")
    observed_slot = {key: approval[key] for key in slot}
    observed_slot["artifact_type"] = "s7_four_table_publish_standing_approval_slot"
    if (
        observed_slot != slot
        or identifier != stable_digest(slot)
        or approval["approval_id"] != identifier
        or approval["artifact_type"] != "s7_four_table_publish_standing_approval"
        or approval["capabilities"] != dict(_PUBLISH_CAPABILITIES)
        or approval["state"] != "approved_for_exact_publish"
        or approval["standing_authorization"]
        != {
            "literal_text": S7_STANDING_AUTHORIZATION_TEXT,
            "literal_text_sha256": S7_STANDING_AUTHORIZATION_SHA256,
        }
        or approval["standing_reaffirmation"]
        != {
            "literal_text": S7_STANDING_REAFFIRMATION_TEXT,
            "literal_text_sha256": S7_STANDING_REAFFIRMATION_SHA256,
        }
        or approval["exact_literal_sha256"]
        != hashlib.sha256(
            exact_s7_publish_approval_literal(plan, plan_pin).encode("utf-8")
        ).hexdigest()
        or approved_at < _utc_from_text(plan["prepared_at_utc"], "PublishPlan time")
    ):
        raise S7IdentityPublishError("S7 Publish approval replay differs")
    binding = _binding_stub_from_plan(plan)
    if approval["approval_availability"] != _calendar_availability(root, binding, approved_at):
        raise S7IdentityPublishError("Publish approval calendar availability differs")
    _text(approval["approved_by"], "Publish approver")
    return approval, pin


def _load_or_store_intent(
    root: Path,
    *,
    plan: Mapping[str, object],
    plan_pin: ExactFilePin,
    approval: Mapping[str, object],
    approval_pin: ExactFilePin,
    now: Callable[[], datetime],
) -> tuple[dict[str, object], ExactFilePin, tuple[dict[str, object], ...]]:
    relative = _publish_intent_path(
        _digest(plan["plan_id"], "PublishPlan ID"),
        _digest(approval["approval_id"], "Publish approval ID"),
    )
    target = safe_relative_path(root, relative)
    if target.exists() or target.is_symlink():
        return _load_intent(
            root,
            plan=plan,
            plan_pin=plan_pin,
            approval=approval,
            approval_pin=approval_pin,
        )
    captured = _utc(now(), "Publish intent runtime clock")
    if captured < _utc_from_text(approval["approved_at_utc"], "Publish approval time"):
        raise S7IdentityPublishError("Publish intent predates approval")
    availability = _release_availability(root, plan, approval, captured)
    member_documents = _member_release_documents(
        plan=plan,
        plan_pin=plan_pin,
        approval=approval,
        approval_pin=approval_pin,
        published_at=captured,
        release_availability=availability,
    )
    descriptors = [
        _document_descriptor(_member_release_path(doc["table_name"], doc["release_id"]), doc)
        for doc in member_documents
    ]
    payload = {
        "approval": approval_pin.to_dict(),
        "approval_id": approval["approval_id"],
        "artifact_type": "s7_four_table_release_set_intent",
        "candidate_id": plan["candidate_id"],
        "candidate_manifest": plan["candidate_manifest"],
        "full_completion": plan["full_completion"],
        "full_completion_id": plan["full_completion_id"],
        "intent_version": PUBLISH_INTENT_VERSION,
        "members": descriptors,
        "plan": plan_pin.to_dict(),
        "plan_id": plan["plan_id"],
        "policy_version": PUBLISH_POLICY_VERSION,
        "published_at_utc": _utc_text(captured),
        "release_availability": availability,
        "source_binding_id": plan["source_binding_id"],
        "state": "release_prefix_authorized",
        "table_order": list(TABLE_ORDER),
    }
    document = {**payload, "intent_id": stable_digest(payload)}
    pin = _write_control(root, relative, document, "S7 release-set intent")
    loaded, loaded_pin, loaded_members = _load_intent(
        root,
        plan=plan,
        plan_pin=plan_pin,
        approval=approval,
        approval_pin=approval_pin,
    )
    if loaded != document or loaded_pin != pin or loaded_members != member_documents:
        raise S7IdentityPublishError("stored release-set intent replay differs")
    return loaded, loaded_pin, loaded_members


def _load_intent(
    root: Path,
    *,
    plan: Mapping[str, object],
    plan_pin: ExactFilePin,
    approval: Mapping[str, object],
    approval_pin: ExactFilePin,
) -> tuple[dict[str, object], ExactFilePin, tuple[dict[str, object], ...]]:
    relative = _publish_intent_path(plan["plan_id"], approval["approval_id"])
    content, pin = _read_control(root, relative, "S7 release-set intent")
    intent = _mapping(
        _load_canonical_json(content, "S7 release-set intent"),
        "S7 release-set intent",
    )
    expected = {
        "approval",
        "approval_id",
        "artifact_type",
        "candidate_id",
        "candidate_manifest",
        "full_completion",
        "full_completion_id",
        "intent_id",
        "intent_version",
        "members",
        "plan",
        "plan_id",
        "policy_version",
        "published_at_utc",
        "release_availability",
        "source_binding_id",
        "state",
        "table_order",
    }
    _expect_keys(intent, expected, "S7 release-set intent")
    payload = dict(intent)
    claimed = payload.pop("intent_id")
    published_at = _utc_from_text(intent["published_at_utc"], "intent publish time")
    if (
        claimed != stable_digest(payload)
        or intent["artifact_type"] != "s7_four_table_release_set_intent"
        or intent["intent_version"] != PUBLISH_INTENT_VERSION
        or intent["policy_version"] != PUBLISH_POLICY_VERSION
        or intent["plan"] != plan_pin.to_dict()
        or intent["plan_id"] != plan["plan_id"]
        or intent["approval"] != approval_pin.to_dict()
        or intent["approval_id"] != approval["approval_id"]
        or intent["candidate_id"] != plan["candidate_id"]
        or intent["candidate_manifest"] != plan["candidate_manifest"]
        or intent["full_completion"] != plan["full_completion"]
        or intent["full_completion_id"] != plan["full_completion_id"]
        or intent["source_binding_id"] != plan["source_binding_id"]
        or intent["state"] != "release_prefix_authorized"
        or intent["table_order"] != list(TABLE_ORDER)
        or published_at < _utc_from_text(approval["approved_at_utc"], "Publish approval time")
        or intent["release_availability"]
        != _release_availability(root, plan, approval, published_at)
    ):
        raise S7IdentityPublishError("S7 release-set intent replay differs")
    member_documents = _member_release_documents(
        plan=plan,
        plan_pin=plan_pin,
        approval=approval,
        approval_pin=approval_pin,
        published_at=published_at,
        release_availability=_mapping(intent["release_availability"], "release availability"),
    )
    descriptors = [
        _document_descriptor(_member_release_path(doc["table_name"], doc["release_id"]), doc)
        for doc in member_documents
    ]
    if intent["members"] != descriptors:
        raise S7IdentityPublishError("intent member manifest set differs")
    return intent, pin, member_documents


def _member_release_documents(
    *,
    plan: Mapping[str, object],
    plan_pin: ExactFilePin,
    approval: Mapping[str, object],
    approval_pin: ExactFilePin,
    published_at: datetime,
    release_availability: Mapping[str, object],
) -> tuple[dict[str, object], ...]:
    documents: list[dict[str, object]] = []
    for raw_member in _array(plan["members"], "PublishPlan members"):
        member = _mapping(raw_member, "PublishPlan member")
        table = _table(member["table_name"])
        payload = {
            "approval": approval_pin.to_dict(),
            "approval_id": approval["approval_id"],
            "artifact_type": "s7_four_table_hidden_member_release",
            "candidate_id": plan["candidate_id"],
            "candidate_manifest": plan["candidate_manifest"],
            "candidate_qa": plan["candidate_qa"],
            "contract": member["contract"],
            "full_completion": plan["full_completion"],
            "full_completion_id": plan["full_completion_id"],
            "output_receipts": member["output_receipts"],
            "output_set_digest": member["output_set_digest"],
            "plan": plan_pin.to_dict(),
            "plan_id": plan["plan_id"],
            "policy_version": PUBLISH_POLICY_VERSION,
            "published_at_utc": _utc_text(published_at),
            "release_availability": dict(release_availability),
            "release_version": TABLE_RELEASE_VERSION,
            "row_count": member["row_count"],
            "source_binding_id": plan["source_binding_id"],
            "state": "published_hidden_until_release_set",
            "table_name": table,
        }
        documents.append({**payload, "release_id": stable_digest(payload)})
    if tuple(item["table_name"] for item in documents) != TABLE_ORDER:
        raise S7IdentityPublishError("member release document order differs")
    return tuple(documents)


def _release_set_document(
    *,
    plan: Mapping[str, object],
    plan_pin: ExactFilePin,
    approval: Mapping[str, object],
    approval_pin: ExactFilePin,
    intent: Mapping[str, object],
    intent_pin: ExactFilePin,
) -> dict[str, object]:
    payload = {
        "approval": approval_pin.to_dict(),
        "approval_id": approval["approval_id"],
        "artifact_type": "s7_four_table_atomic_release_set",
        "candidate_id": plan["candidate_id"],
        "candidate_manifest": plan["candidate_manifest"],
        "candidate_qa": plan["candidate_qa"],
        "full_completion": plan["full_completion"],
        "full_completion_id": plan["full_completion_id"],
        "intent": intent_pin.to_dict(),
        "intent_id": intent["intent_id"],
        "members": intent["members"],
        "plan": plan_pin.to_dict(),
        "plan_id": plan["plan_id"],
        "policy_version": PUBLISH_POLICY_VERSION,
        "published_at_utc": intent["published_at_utc"],
        "release_availability": intent["release_availability"],
        "release_set_version": RELEASE_SET_VERSION,
        "source_binding_id": plan["source_binding_id"],
        "state": "published",
        "table_order": list(TABLE_ORDER),
        "visibility_rule": "all_four_members_visible_only_through_this_exact_marker_v1",
    }
    return {**payload, "release_set_id": stable_digest(payload)}


def _load_published_s7_release_set(
    root: Path,
    release_set_id: str,
    *,
    runtime_probe: Callable[[], Mapping[str, object]],
    registry_loader: Callable[..., LoadedRegistryReleaseSet],
    production: bool,
) -> dict[str, object]:
    identifier = _digest(release_set_id, "release-set ID")
    relative = _release_set_path(identifier)
    content, _ = _read_control(root, relative, "S7 release-set marker")
    marker = _mapping(
        _load_canonical_json(content, "S7 release-set marker"),
        "S7 release-set marker",
    )
    expected = {
        "approval",
        "approval_id",
        "artifact_type",
        "candidate_id",
        "candidate_manifest",
        "candidate_qa",
        "full_completion",
        "full_completion_id",
        "intent",
        "intent_id",
        "members",
        "plan",
        "plan_id",
        "policy_version",
        "published_at_utc",
        "release_availability",
        "release_set_id",
        "release_set_version",
        "source_binding_id",
        "state",
        "table_order",
        "visibility_rule",
    }
    _expect_keys(marker, expected, "S7 release-set marker")
    payload = dict(marker)
    claimed = payload.pop("release_set_id")
    if (
        claimed != identifier
        or claimed != stable_digest(payload)
        or marker["artifact_type"] != "s7_four_table_atomic_release_set"
        or marker["policy_version"] != PUBLISH_POLICY_VERSION
        or marker["release_set_version"] != RELEASE_SET_VERSION
        or marker["state"] != "published"
        or marker["table_order"] != list(TABLE_ORDER)
        or marker["visibility_rule"] != "all_four_members_visible_only_through_this_exact_marker_v1"
    ):
        raise S7IdentityPublishError("S7 release-set marker replay differs")
    plan_pin = ExactFilePin.from_dict(marker["plan"])
    _verify_pin(root, plan_pin, "release-set PublishPlan")
    plan, loaded_plan_pin = _load_publish_plan(root, marker["plan_id"])
    if loaded_plan_pin != plan_pin:
        raise S7IdentityPublishError("release-set PublishPlan pin differs")
    approval_pin = ExactFilePin.from_dict(marker["approval"])
    _verify_pin(root, approval_pin, "release-set approval")
    approval, loaded_approval_pin = _load_publish_approval(
        root,
        plan=plan,
        plan_pin=plan_pin,
        approval_id=marker["approval_id"],
    )
    if loaded_approval_pin != approval_pin:
        raise S7IdentityPublishError("release-set approval pin differs")
    intent_pin = ExactFilePin.from_dict(marker["intent"])
    _verify_pin(root, intent_pin, "release-set intent")
    intent, loaded_intent_pin, member_documents = _load_intent(
        root,
        plan=plan,
        plan_pin=plan_pin,
        approval=approval,
        approval_pin=approval_pin,
    )
    if (
        loaded_intent_pin != intent_pin
        or marker["intent_id"] != intent["intent_id"]
        or marker["members"] != intent["members"]
        or marker["candidate_id"] != plan["candidate_id"]
        or marker["candidate_manifest"] != plan["candidate_manifest"]
        or marker["candidate_qa"] != plan["candidate_qa"]
        or marker["full_completion"] != plan["full_completion"]
        or marker["full_completion_id"] != plan["full_completion_id"]
        or marker["source_binding_id"] != plan["source_binding_id"]
        or marker["published_at_utc"] != intent["published_at_utc"]
        or marker["release_availability"] != intent["release_availability"]
    ):
        raise S7IdentityPublishError("release-set control binding differs")
    for descriptor, document in zip(marker["members"], member_documents, strict=True):
        member = _mapping(descriptor, "release-set member")
        pin = ExactFilePin(
            _relative(member["path"], "member release path"),
            _digest(member["sha256"], "member release SHA-256"),
            _positive(member["bytes"], "member release bytes"),
        )
        _verify_pin(root, pin, "member release")
        content = streaming._read_exact_file(root, pin.path, label="member release")
        if _load_canonical_json(content, "member release") != document:
            raise S7IdentityPublishError("member release logical document differs")
    _verify_candidate_receipts_from_plan(root, plan)
    _replay_plan_source_chain(
        root,
        plan,
        runtime_probe=runtime_probe,
        registry_loader=registry_loader,
        production=production,
    )
    return marker


def _verify_candidate_receipts_from_plan(root: Path, plan: Mapping[str, object]) -> None:
    for field_name, label in (
        ("candidate_manifest", "candidate manifest"),
        ("candidate_qa", "candidate QA"),
        ("full_approval", "Full approval"),
        ("full_completion", "Full completion"),
        ("full_plan", "Full plan"),
        ("source_binding", "source binding"),
    ):
        _verify_data_pin(root, ExactFilePin.from_dict(plan[field_name]), label)
    for raw_pin in _array(plan["contract_approvals"], "contract approval pins"):
        _verify_data_pin(root, ExactFilePin.from_dict(raw_pin), "v4 contract approval")
    for raw_member in _array(plan["members"], "PublishPlan members"):
        member = _mapping(raw_member, "PublishPlan member")
        outputs = _array(member["output_receipts"], "member output receipts")
        if stable_digest(outputs) != member["output_set_digest"]:
            raise S7IdentityPublishError("member output-set digest differs")
        for raw_receipt in outputs:
            receipt = _mapping(raw_receipt, "candidate output receipt")
            pin = ExactFilePin(
                _relative(receipt["path"], "candidate output path"),
                _digest(receipt["sha256"], "candidate output SHA-256"),
                _positive(receipt["bytes"], "candidate output bytes"),
            )
            _verify_data_pin(root, pin, "candidate output")


def _release_availability(
    root: Path,
    plan: Mapping[str, object],
    approval: Mapping[str, object],
    published_at: datetime,
) -> dict[str, object]:
    binding = _binding_stub_from_plan(plan)
    calendar = _calendar_availability(root, binding, published_at)
    approval_availability = _mapping(
        approval["approval_availability"], "Publish approval availability"
    )
    calendar_session = date.fromisoformat(
        _text(calendar["source_available_session"], "intent calendar session")
    )
    approval_session = date.fromisoformat(
        _text(
            approval_availability["source_available_session"],
            "approval available session",
        )
    )
    cutoff = date.fromisoformat(_text(plan["source_cutoff_session"], "source cutoff session"))
    release_session = max(calendar_session, approval_session, cutoff)
    return {
        "approval_available_session": approval_session.isoformat(),
        "calendar_artifact_id": calendar["calendar_artifact_id"],
        "calendar_artifact_sha256": calendar["calendar_artifact_sha256"],
        "first_xnys_open_utc": calendar["first_xnys_open_utc"],
        "release_available_session": release_session.isoformat(),
        "rule": ("max_first_bound_xnys_open_after_runtime_publish_approval_and_source_cutoff_v1"),
        "runtime_published_at_utc": _utc_text(published_at),
        "source_cutoff_session": cutoff.isoformat(),
    }


def _calendar_availability(
    root: Path, binding: S7StreamingSourceBinding, recorded_at: datetime
) -> dict[str, object]:
    try:
        return streaming._calendar_availability(
            root,
            calendar_artifact_id=binding.calendar_artifact_id,
            calendar_artifact_sha256=binding.calendar_artifact_sha256,
            recorded_at=recorded_at,
        )
    except Exception as exc:
        raise S7IdentityPublishError("Publish calendar availability replay failed") from exc


def _binding_stub_from_plan(plan: Mapping[str, object]) -> _CalendarBinding:
    # The full source binding loader is exact-ID only and validates the stored receipt.
    source_id = _digest(plan["source_binding_id"], "source binding ID")
    # Callers always need only calendar fields here; a tiny immutable stand-in avoids an
    # unbound latest lookup while keeping the calendar helper's interface consistent.
    return _CalendarBinding(
        calendar_artifact_id=_digest(plan["calendar_artifact_id"], "calendar ID"),
        calendar_artifact_sha256=_digest(plan["calendar_artifact_sha256"], "calendar SHA-256"),
        source_binding_id=source_id,
    )


@dataclass(frozen=True, slots=True)
class _CalendarBinding:
    calendar_artifact_id: str
    calendar_artifact_sha256: str
    source_binding_id: str


def _absolute_candidate_pin(
    root: Path,
    candidate_relative: str,
    receipt: Mapping[str, object],
    label: str,
) -> ExactFilePin:
    normalized = _absolute_candidate_receipt(root, candidate_relative, receipt, label)
    return ExactFilePin(normalized["path"], normalized["sha256"], normalized["bytes"])


def _absolute_candidate_receipt(
    root: Path,
    candidate_relative: str,
    receipt: Mapping[str, object],
    label: str,
) -> dict[str, object]:
    item = dict(receipt)
    relative = _relative(item.get("path"), f"{label} candidate-relative path")
    item["path"] = f"{candidate_relative}/{relative}"
    pin = ExactFilePin(
        item["path"],
        _digest(item.get("sha256"), f"{label} SHA-256"),
        _positive(item.get("bytes"), f"{label} bytes"),
    )
    _verify_data_pin(root, pin, label)
    return item


def _document_descriptor(relative: str, document: Mapping[str, object]) -> dict[str, object]:
    content = _canonical_bytes(document)
    return {
        "bytes": len(content),
        "path": _relative(relative, "document descriptor path"),
        "release_id": _digest(document["release_id"], "member release ID"),
        "sha256": hashlib.sha256(content).hexdigest(),
        "table_name": _table(document["table_name"]),
    }


def _result_from_marker(marker: Mapping[str, object], *, idempotent: bool) -> S7PublishRunResult:
    members = _array(marker["members"], "release-set members")
    return S7PublishRunResult(
        publish_plan_id=_digest(marker["plan_id"], "PublishPlan ID"),
        approval_id=_digest(marker["approval_id"], "Publish approval ID"),
        intent_id=_digest(marker["intent_id"], "release intent ID"),
        release_set_id=_digest(marker["release_set_id"], "release-set ID"),
        release_set_path=_release_set_path(marker["release_set_id"]),
        member_release_ids={
            _table(_mapping(item, "release-set member")["table_name"]): _digest(
                _mapping(item, "release-set member")["release_id"],
                "member release ID",
            )
            for item in members
        },
        release_available_session=date.fromisoformat(
            _text(
                _mapping(marker["release_availability"], "release availability")[
                    "release_available_session"
                ],
                "release available session",
            )
        ),
        idempotent=idempotent,
    )


def _validate_plan_members(value: object) -> None:
    members = _array(value, "PublishPlan members")
    if tuple(_mapping(item, "PublishPlan member").get("table_name") for item in members) != (
        TABLE_ORDER
    ):
        raise S7IdentityPublishError("PublishPlan member order differs")
    for raw_member in members:
        member = _mapping(raw_member, "PublishPlan member")
        _expect_keys(
            member,
            {
                "contract",
                "output_receipts",
                "output_set_digest",
                "row_count",
                "table_name",
            },
            "PublishPlan member",
        )
        _table(member["table_name"])
        outputs = _array(member["output_receipts"], "PublishPlan member outputs")
        if not outputs or stable_digest(outputs) != _digest(
            member["output_set_digest"], "member output-set digest"
        ):
            raise S7IdentityPublishError("PublishPlan member output binding differs")
        _nonnegative(member["row_count"], "PublishPlan member row count")


def _read_document(root: Path, relative: str, label: str) -> dict[str, object]:
    content = streaming._read_exact_file(root, relative, label=label)
    return _mapping(_load_canonical_json(content, label), label)


def _write_control(
    root: Path, relative: str, document: Mapping[str, object], label: str
) -> ExactFilePin:
    content = _canonical_bytes(document)
    path = safe_relative_path(root, _relative(relative, f"{label} path"))
    try:
        stored = write_bytes_immutable(root, path, content)
    except (ArtifactError, OSError) as exc:
        raise S7IdentityPublishError(f"cannot publish immutable {label}") from exc
    pin = ExactFilePin.from_dict(stored)
    _verify_control_pin(root, pin, label)
    return pin


def _read_control(root: Path, relative: str, label: str) -> tuple[bytes, ExactFilePin]:
    normalized = _relative(relative, f"{label} path")
    path = safe_relative_path(root, normalized)
    if not path.is_file() or path.is_symlink():
        raise S7IdentityPublishError(f"{label} is missing or unsafe")
    content = path.read_bytes()
    pin = ExactFilePin(normalized, hashlib.sha256(content).hexdigest(), len(content))
    _verify_control_pin(root, pin, label)
    return content, pin


def _verify_control_pin(root: Path, pin: ExactFilePin, label: str) -> None:
    path = safe_relative_path(root, pin.path)
    try:
        metadata = path.stat(follow_symlinks=False)
    except OSError as exc:
        raise S7IdentityPublishError(f"cannot inspect {label}") from exc
    if (
        not stat.S_ISREG(metadata.st_mode)
        or path.is_symlink()
        or stat.S_IMODE(metadata.st_mode) != 0o444
        or metadata.st_nlink != 1
        or metadata.st_size != pin.bytes
        or sha256_file(path) != pin.sha256
    ):
        raise S7IdentityPublishError(f"immutable {label} receipt differs")


def _verify_data_pin(root: Path, pin: ExactFilePin, label: str) -> None:
    path = safe_relative_path(root, pin.path)
    if (
        not path.is_file()
        or path.is_symlink()
        or path.stat().st_size != pin.bytes
        or sha256_file(path) != pin.sha256
    ):
        raise S7IdentityPublishError(f"exact {label} receipt differs")


def _verify_pin(root: Path, pin: ExactFilePin, label: str) -> None:
    _verify_control_pin(root, pin, label)


def _publish_plan_path(plan_id: str) -> str:
    return (
        "manifests/silver/identity/s7-four-table-publish-plans/"
        f"plan_id={_digest(plan_id, 'PublishPlan ID')}/manifest.json"
    )


def _publish_approval_path(plan_id: str) -> str:
    return (
        "manifests/silver/identity/s7-four-table-publish-approvals/"
        f"plan_id={_digest(plan_id, 'PublishPlan ID')}/manifest.json"
    )


def _publish_intent_path(plan_id: str, approval_id: str) -> str:
    return (
        "manifests/silver/identity/s7-four-table-publish-intents/"
        f"plan_id={_digest(plan_id, 'PublishPlan ID')}/"
        f"approval_id={_digest(approval_id, 'Publish approval ID')}/manifest.json"
    )


def _member_release_path(table_name: object, release_id: object) -> str:
    return (
        "manifests/silver/identity/s7-four-table-releases/"
        f"table_name={_table(table_name)}/"
        f"release_id={_digest(release_id, 'member release ID')}/manifest.json"
    )


def _release_set_path(release_set_id: object) -> str:
    return (
        "manifests/silver/identity/s7-four-table-release-sets/"
        f"release_set_id={_digest(release_set_id, 'release-set ID')}/manifest.json"
    )


def _production_root(value: Path) -> Path:
    root = _root(value)
    if not is_canonical_production_data_root(root):
        raise S7IdentityPublishError(
            "S7 production Publish requires the canonical production data root"
        )
    return root


def _fixture_root(value: Path) -> Path:
    root = _root(value)
    if is_canonical_production_data_root(root):
        raise S7IdentityPublishError("fixture Publish cannot touch the production data root")
    return root


def _root(value: Path) -> Path:
    try:
        return streaming._root(value)
    except Exception as exc:
        raise S7IdentityPublishError("S7 Publish data root is invalid") from exc


def _canonical_bytes(value: object) -> bytes:
    return streaming._canonical_bytes(value)


def _load_canonical_json(content: bytes, label: str) -> object:
    try:
        return streaming._load_canonical_json(content, label)
    except Exception as exc:
        raise S7IdentityPublishError(f"{label} is not canonical JSON") from exc


def _mapping(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise S7IdentityPublishError(f"{label} must be an object")
    return dict(value)


def _array(value: object, label: str) -> list[object]:
    if not isinstance(value, list):
        raise S7IdentityPublishError(f"{label} must be an array")
    return value


def _expect_keys(value: Mapping[str, object], expected: set[str], label: str) -> None:
    if set(value) != expected:
        raise S7IdentityPublishError(f"{label} fields differ")


def _text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise S7IdentityPublishError(f"{label} must be trimmed nonempty text")
    return value


def _digest(value: object, label: str) -> str:
    try:
        return streaming._digest(value, label)
    except Exception as exc:
        raise S7IdentityPublishError(f"{label} must be a lowercase SHA-256") from exc


def _relative(value: object, label: str) -> str:
    try:
        return streaming._relative(value, label)
    except Exception as exc:
        raise S7IdentityPublishError(f"{label} must be a normalized relative path") from exc


def _nonnegative(value: object, label: str) -> int:
    if type(value) is not int or value < 0:
        raise S7IdentityPublishError(f"{label} must be a nonnegative integer")
    return value


def _positive(value: object, label: str) -> int:
    result = _nonnegative(value, label)
    if result == 0:
        raise S7IdentityPublishError(f"{label} must be positive")
    return result


def _table(value: object) -> str:
    text = _text(value, "S7 table name")
    if text not in TABLE_ORDER:
        raise S7IdentityPublishError("S7 Publish table is not in the fixed four-table set")
    return text


def _utc(value: datetime, label: str) -> datetime:
    try:
        return streaming._utc(value, label)
    except Exception as exc:
        raise S7IdentityPublishError(f"{label} must be timezone-aware") from exc


def _utc_text(value: datetime) -> str:
    return _utc(value, "UTC time").isoformat()


def _utc_from_text(value: object, label: str) -> datetime:
    try:
        return streaming._utc_from_text(value, label)
    except Exception as exc:
        raise S7IdentityPublishError(f"{label} must be an ISO-8601 UTC time") from exc


def _checkpoint(
    callback: Callable[[str, str | None], None] | None,
    stage: str,
    table: str | None,
) -> None:
    if callback is not None:
        callback(stage, table)


@contextmanager
def _exclusive_lock(path: Path):
    lock = streaming._exclusive_nonblocking_lock(path)
    try:
        lock.__enter__()
    except Exception as exc:
        raise S7IdentityPublishError("S7 Publish nonblocking lock failed") from exc
    try:
        yield
    finally:
        lock.__exit__(None, None, None)


__all__ = [
    "CANONICAL_PRODUCTION_DATA_ROOT",
    "PUBLISH_AUTHORIZED_ACTION",
    "PUBLISH_POLICY_VERSION",
    "S7IdentityPublishError",
    "S7PublishRunResult",
    "exact_s7_publish_approval_literal",
    "load_published_s7_release_set",
    "prepare_s7_publish_plan",
    "publish_s7_release_set",
    "record_standing_s7_publish_approval",
]
