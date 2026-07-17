"""Source-bound Gate-A runner for the full S4 Composite-FIGI inventory.

The public entry point accepts only one data root and exact v2 plan/approval
ID/SHA pairs.  It verifies the approved Git tree and every pinned runtime byte,
opens the official six-release S7 source bundle, scans only the two exact S4
daily tables, and stops with an immutable ``awaiting_review`` candidate.

``asset_observation_daily`` is the sole inventory authority.
``universe_source_daily`` is reconciliation-only and can never increment an
inventory aggregate.  This module has no network, market classification,
adjudication, canonical-identity, research-table materialization, release, or
publication capability.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import resource
import shutil
import stat
import subprocess
import sys
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from types import MappingProxyType
from typing import Any, Final

import pyarrow as pa
import pyarrow.parquet as pq

from ame_stocks_api.artifacts import (
    ArtifactError,
    safe_relative_path,
    sha256_file,
    stable_digest,
)
from ame_stocks_api.silver.calendar_artifact import (
    XNYSCalendarArtifact,
    XNYSCalendarArtifactError,
    load_xnys_calendar_artifact,
)
from ame_stocks_api.silver.contracts import QAStatus
from ame_stocks_api.silver.identity_market_inventory_contract import (
    COMPOSITE_FIGI_INVENTORY_CONTRACT,
    COMPOSITE_FIGI_INVENTORY_CONTRACT_ID,
    COMPOSITE_FIGI_INVENTORY_RESOURCE_SHA256,
    COMPOSITE_FIGI_INVENTORY_SCHEMA_DIGEST,
)
from ame_stocks_api.silver.identity_market_inventory_engine import (
    ASSET_AUTHORITY_TABLE,
    COMPOSITE_FIGI_INVALID_REASON_PRECEDENCE,
    SHARE_CLASS_FIGI_INVALID_REASON_PRECEDENCE,
    UNIVERSE_PARENT_PROJECTION,
    UNIVERSE_RECONCILIATION_TABLE,
    CompositeInventoryCaps,
    CompositeInventoryDiagnostics,
    CompositeInventoryEngine,
    CompositeInventoryError,
    CompositeInventoryResult,
    figi_invalid_reason,
)
from ame_stocks_api.silver.identity_market_inventory_execution_plan import (
    INVENTORY_ALGORITHM_DIGEST,
    INVENTORY_ALGORITHM_RULE_VERSION,
    INVENTORY_OUTPUT_COLUMNS,
    INVENTORY_QA_SEMANTICS_DIGEST,
    UNIVERSE_PARENT_RECONCILIATION_PROJECTION,
    IdentityMarketInventoryExecutionPlanError,
    IdentityMarketInventoryExecutionPlanStore,
    S7CompositeInventoryExecutionPlanV2,
    canonical_execution_paths,
)
from ame_stocks_api.silver.identity_market_inventory_plan import (
    INVENTORY_CALENDAR_ARTIFACT_ID,
    INVENTORY_CALENDAR_ARTIFACT_SHA256,
    INVENTORY_END_SESSION,
    INVENTORY_SESSION_COUNT,
    INVENTORY_START_SESSION,
)
from ame_stocks_api.silver.identity_source import (
    S7_SOURCE_PINS,
    IdentitySourceArtifact,
    IdentitySourceError,
    open_identity_source_bundle,
)

RUNNER_RULE_VERSION: Final = "s7_composite_inventory_source_bound_runner_v1"
CANDIDATE_SCHEMA_VERSION: Final = 1
CANDIDATE_RULE_VERSION: Final = "s7_composite_inventory_candidate_v1"
QA_ARTIFACT_SCHEMA_VERSION: Final = 1
EXAMPLE_ARTIFACT_SCHEMA_VERSION: Final = 1
COMPLETION_SCHEMA_VERSION: Final = 1
COMPLETION_RULE_VERSION: Final = "s7_composite_inventory_execution_completion_v1"
CANDIDATE_STATE: Final = "awaiting_review"

DATA_FILENAME: Final = "data/part-00000.parquet"
QA_FILENAME: Final = "qa/qa.json"
EXAMPLES_FILENAME: Final = "examples/invalid-figi.json"
MANIFEST_FILENAME: Final = "manifest.json"

ASSET_COLUMNS: Final = tuple(
    dict.fromkeys(
        (
            "session_date",
            "source_record_id",
            "ticker",
            "requested_active",
            "provider_active",
            "composite_figi",
            "share_class_figi",
            "locale",
            "market",
            "primary_exchange_mic",
            *(asset for asset, _ in UNIVERSE_PARENT_PROJECTION),
        )
    )
)
UNIVERSE_COLUMNS: Final = tuple(
    dict.fromkeys(
        (
            "session_date",
            "selected_source_record_id",
            "ticker",
            *(universe for _, universe in UNIVERSE_PARENT_PROJECTION),
        )
    )
)

_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_CANDIDATE_DIRECTORY = re.compile(r"^candidate_id=([0-9a-f]{64})$")


class IdentityMarketInventoryRunnerError(RuntimeError):
    """Raised before a trustworthy Gate-A completion can be committed."""


@dataclass(frozen=True, slots=True, order=True)
class InventorySourceArtifactRef:
    table: str
    session_date: date
    release_id: str
    release_manifest_sha256: str
    path: str
    sha256: str
    bytes: int
    row_count: int

    def __post_init__(self) -> None:
        if self.table not in {ASSET_AUTHORITY_TABLE, UNIVERSE_RECONCILIATION_TABLE}:
            raise IdentityMarketInventoryRunnerError("source artifact table is out of scope")
        if type(self.session_date) is not date:
            raise IdentityMarketInventoryRunnerError("source artifact session is invalid")
        for label, value in (
            ("source release ID", self.release_id),
            ("source release manifest SHA-256", self.release_manifest_sha256),
            ("source artifact SHA-256", self.sha256),
        ):
            _digest(value, label)
        _relative_path(self.path, "source artifact path")
        _nonnegative_int(self.bytes, "source artifact bytes")
        _nonnegative_int(self.row_count, "source artifact rows")

    def to_dict(self) -> dict[str, object]:
        return {
            "bytes": self.bytes,
            "path": self.path,
            "release_id": self.release_id,
            "release_manifest_sha256": self.release_manifest_sha256,
            "row_count": self.row_count,
            "session_date": self.session_date.isoformat(),
            "sha256": self.sha256,
            "table": self.table,
        }

    @classmethod
    def from_dict(cls, value: object) -> InventorySourceArtifactRef:
        item = _mapping(value, "source artifact ref")
        _expect_keys(
            item,
            {
                "bytes",
                "path",
                "release_id",
                "release_manifest_sha256",
                "row_count",
                "session_date",
                "sha256",
                "table",
            },
            "source artifact ref",
        )
        return cls(
            table=_string(item["table"], "source artifact table"),
            session_date=_parse_date(item["session_date"], "source artifact session"),
            release_id=_string(item["release_id"], "source release ID"),
            release_manifest_sha256=_string(
                item["release_manifest_sha256"], "source release manifest SHA-256"
            ),
            path=_string(item["path"], "source artifact path"),
            sha256=_string(item["sha256"], "source artifact SHA-256"),
            bytes=_nonnegative_int(item["bytes"], "source artifact bytes"),
            row_count=_nonnegative_int(item["row_count"], "source artifact rows"),
        )


@dataclass(frozen=True, slots=True)
class InventoryOutputArtifactRef:
    role: str
    path: str
    sha256: str
    bytes: int
    media_type: str
    row_count: int | None = None
    schema_digest: str | None = None

    def __post_init__(self) -> None:
        if self.role not in {"data", "qa", "bounded_examples"}:
            raise IdentityMarketInventoryRunnerError("candidate artifact role is invalid")
        _relative_path(self.path, "candidate artifact path")
        _digest(self.sha256, "candidate artifact SHA-256")
        _nonnegative_int(self.bytes, "candidate artifact bytes")
        if not isinstance(self.media_type, str) or not self.media_type:
            raise IdentityMarketInventoryRunnerError("candidate artifact media type is invalid")
        if self.row_count is not None:
            _nonnegative_int(self.row_count, "candidate artifact rows")
        if self.schema_digest is not None:
            _digest(self.schema_digest, "candidate artifact schema digest")
        if self.role == "data":
            if (
                self.path != DATA_FILENAME
                or self.media_type != "application/vnd.apache.parquet"
                or self.row_count is None
                or self.schema_digest != COMPOSITE_FIGI_INVENTORY_SCHEMA_DIGEST
            ):
                raise IdentityMarketInventoryRunnerError("candidate DATA ref changed")
        elif self.row_count is not None or self.schema_digest is not None:
            raise IdentityMarketInventoryRunnerError("JSON candidate ref has DATA metadata")

    def to_dict(self) -> dict[str, object]:
        result: dict[str, object] = {
            "bytes": self.bytes,
            "media_type": self.media_type,
            "path": self.path,
            "role": self.role,
            "sha256": self.sha256,
        }
        if self.row_count is not None:
            result["row_count"] = self.row_count
        if self.schema_digest is not None:
            result["schema_digest"] = self.schema_digest
        return result

    @classmethod
    def from_dict(cls, value: object) -> InventoryOutputArtifactRef:
        item = _mapping(value, "candidate artifact ref")
        required = {"bytes", "media_type", "path", "role", "sha256"}
        keys = set(item)
        if keys != required and keys != required | {"row_count", "schema_digest"}:
            raise IdentityMarketInventoryRunnerError("candidate artifact ref schema is not exact")
        return cls(
            role=_string(item["role"], "candidate artifact role"),
            path=_string(item["path"], "candidate artifact path"),
            sha256=_string(item["sha256"], "candidate artifact SHA-256"),
            bytes=_nonnegative_int(item["bytes"], "candidate artifact bytes"),
            media_type=_string(item["media_type"], "candidate artifact media type"),
            row_count=(
                None
                if "row_count" not in item
                else _nonnegative_int(item["row_count"], "candidate artifact rows")
            ),
            schema_digest=(
                None
                if "schema_digest" not in item
                else _string(item["schema_digest"], "candidate artifact schema digest")
            ),
        )


@dataclass(frozen=True, slots=True)
class S7CompositeInventoryCandidate:
    plan_id: str
    plan_sha256: str
    approval_id: str
    approval_sha256: str
    request_event_id: str
    request_event_sha256: str
    input_binding_digest: str
    resource_caps_digest: str
    runtime_file_set_digest: str
    verification_file_set_digest: str
    source_artifacts: tuple[InventorySourceArtifactRef, ...]
    source_artifact_set_digest: str
    source_artifact_count: int
    source_row_count: int
    source_bytes: int
    authority_row_count: int
    reconciliation_row_count: int
    session_count: int
    inventory_row_count: int
    valid_composite_row_count: int
    distinct_composite_share_class_pair_count: int
    diagnostics: Mapping[str, object]
    artifacts: tuple[InventoryOutputArtifactRef, ...]
    created_at_utc: datetime
    candidate_state: str = CANDIDATE_STATE

    def __post_init__(self) -> None:
        for label, value in (
            ("plan ID", self.plan_id),
            ("plan SHA-256", self.plan_sha256),
            ("approval ID", self.approval_id),
            ("approval SHA-256", self.approval_sha256),
            ("request event ID", self.request_event_id),
            ("request event SHA-256", self.request_event_sha256),
            ("input binding digest", self.input_binding_digest),
            ("resource caps digest", self.resource_caps_digest),
            ("runtime file set digest", self.runtime_file_set_digest),
            ("verification file set digest", self.verification_file_set_digest),
            ("source artifact set digest", self.source_artifact_set_digest),
        ):
            _digest(value, label)
        sources = tuple(sorted(self.source_artifacts))
        if not sources or len({item.path for item in sources}) != len(sources):
            raise IdentityMarketInventoryRunnerError("candidate source artifacts are invalid")
        if stable_digest([item.to_dict() for item in sources]) != self.source_artifact_set_digest:
            raise IdentityMarketInventoryRunnerError("source artifact set digest differs")
        object.__setattr__(self, "source_artifacts", sources)
        outputs = tuple(sorted(self.artifacts, key=lambda item: item.role))
        if {item.role for item in outputs} != {"data", "qa", "bounded_examples"}:
            raise IdentityMarketInventoryRunnerError("candidate output artifacts are incomplete")
        object.__setattr__(self, "artifacts", outputs)
        for label, value in (
            ("source artifact count", self.source_artifact_count),
            ("source row count", self.source_row_count),
            ("source bytes", self.source_bytes),
            ("authority row count", self.authority_row_count),
            ("reconciliation row count", self.reconciliation_row_count),
            ("session count", self.session_count),
            ("inventory row count", self.inventory_row_count),
            ("valid Composite row count", self.valid_composite_row_count),
            (
                "distinct Composite/Share-Class pair count",
                self.distinct_composite_share_class_pair_count,
            ),
        ):
            _nonnegative_int(value, label)
        if (
            self.source_artifact_count != len(sources)
            or self.source_row_count != sum(item.row_count for item in sources)
            or self.source_bytes != sum(item.bytes for item in sources)
            or self.source_row_count != self.authority_row_count + self.reconciliation_row_count
            or self.inventory_row_count
            != next(item.row_count for item in outputs if item.role == "data")
        ):
            raise IdentityMarketInventoryRunnerError("candidate counts do not reconcile")
        if not isinstance(self.diagnostics, Mapping):
            raise IdentityMarketInventoryRunnerError("candidate diagnostics must be an object")
        object.__setattr__(self, "diagnostics", MappingProxyType(dict(self.diagnostics)))
        object.__setattr__(self, "created_at_utc", _utc(self.created_at_utc, "created_at_utc"))
        if self.candidate_state != CANDIDATE_STATE:
            raise IdentityMarketInventoryRunnerError("candidate state is not awaiting_review")

    def logical_payload(self) -> dict[str, object]:
        return {
            "algorithm_digest": INVENTORY_ALGORITHM_DIGEST,
            "algorithm_rule_version": INVENTORY_ALGORITHM_RULE_VERSION,
            "approval_id": self.approval_id,
            "approval_sha256": self.approval_sha256,
            "artifact_type": "s7_composite_figi_inventory_candidate",
            "artifacts": [item.to_dict() for item in self.artifacts],
            "candidate_rule_version": CANDIDATE_RULE_VERSION,
            "candidate_state": self.candidate_state,
            "capabilities": _fail_closed_capabilities(),
            "contract": {
                "candidate_sha256": COMPOSITE_FIGI_INVENTORY_RESOURCE_SHA256,
                "contract_id": COMPOSITE_FIGI_INVENTORY_CONTRACT_ID,
                "schema_digest": COMPOSITE_FIGI_INVENTORY_SCHEMA_DIGEST,
            },
            "counts": {
                "authority_row_count": self.authority_row_count,
                "distinct_composite_share_class_pair_count": (
                    self.distinct_composite_share_class_pair_count
                ),
                "inventory_row_count": self.inventory_row_count,
                "reconciliation_row_count": self.reconciliation_row_count,
                "session_count": self.session_count,
                "source_artifact_count": self.source_artifact_count,
                "source_bytes": self.source_bytes,
                "source_row_count": self.source_row_count,
                "valid_composite_row_count": self.valid_composite_row_count,
            },
            "created_at_utc": _utc_text(self.created_at_utc),
            "diagnostics": dict(self.diagnostics),
            "input_binding_digest": self.input_binding_digest,
            "plan_id": self.plan_id,
            "plan_sha256": self.plan_sha256,
            "qa_semantics_digest": INVENTORY_QA_SEMANTICS_DIGEST,
            "request_event_id": self.request_event_id,
            "request_event_sha256": self.request_event_sha256,
            "resource_caps_digest": self.resource_caps_digest,
            "runtime_file_set_digest": self.runtime_file_set_digest,
            "schema_version": CANDIDATE_SCHEMA_VERSION,
            "source_artifact_set_digest": self.source_artifact_set_digest,
            "source_artifacts": [item.to_dict() for item in self.source_artifacts],
            "verification_file_set_digest": self.verification_file_set_digest,
        }

    @property
    def candidate_id(self) -> str:
        return stable_digest(self.logical_payload())

    @property
    def relative_directory(self) -> str:
        return (
            "manifests/silver/identity/composite-inventory-candidates/"
            f"candidate_id={self.candidate_id}"
        )

    @property
    def relative_path(self) -> str:
        return f"{self.relative_directory}/{MANIFEST_FILENAME}"

    @property
    def document(self) -> Mapping[str, object]:
        paths = {item.role: f"{self.relative_directory}/{item.path}" for item in self.artifacts}
        return MappingProxyType(
            {
                **self.logical_payload(),
                "candidate_id": self.candidate_id,
                "canonical_paths": {
                    "bounded_examples": paths["bounded_examples"],
                    "data": paths["data"],
                    "manifest": self.relative_path,
                    "qa": paths["qa"],
                },
            }
        )

    @property
    def content(self) -> bytes:
        return _canonical_bytes(self.document)

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.content).hexdigest()


@dataclass(frozen=True, slots=True)
class S7CompositeInventoryExecutionCompletion:
    plan_id: str
    plan_sha256: str
    approval_id: str
    approval_sha256: str
    request_event_id: str
    request_event_sha256: str
    input_binding_digest: str
    candidate_id: str
    candidate_path: str
    candidate_sha256: str
    candidate_bytes: int
    data_path: str
    data_sha256: str
    data_bytes: int
    qa_path: str
    qa_sha256: str
    qa_bytes: int
    bounded_examples_path: str
    bounded_examples_sha256: str
    bounded_examples_bytes: int
    source_artifact_set_digest: str
    source_artifact_count: int
    source_row_count: int
    source_bytes: int
    authority_row_count: int
    reconciliation_row_count: int
    inventory_row_count: int
    session_count: int
    completed_at_utc: datetime
    wall_clock_seconds: float
    peak_rss_bytes: int
    minimum_disk_free_bytes: int
    maximum_tmp_bytes: int
    disk_free_warning_triggered: bool
    output_bytes: int
    completion_state: str = CANDIDATE_STATE

    def __post_init__(self) -> None:
        for label, value in (
            ("plan ID", self.plan_id),
            ("plan SHA-256", self.plan_sha256),
            ("approval ID", self.approval_id),
            ("approval SHA-256", self.approval_sha256),
            ("request event ID", self.request_event_id),
            ("request event SHA-256", self.request_event_sha256),
            ("input binding digest", self.input_binding_digest),
            ("candidate ID", self.candidate_id),
            ("candidate SHA-256", self.candidate_sha256),
            ("DATA SHA-256", self.data_sha256),
            ("QA SHA-256", self.qa_sha256),
            ("bounded examples SHA-256", self.bounded_examples_sha256),
            ("source artifact set digest", self.source_artifact_set_digest),
        ):
            _digest(value, label)
        expected_dir = (
            "manifests/silver/identity/composite-inventory-candidates/"
            f"candidate_id={self.candidate_id}"
        )
        expected_paths = {
            "candidate": f"{expected_dir}/{MANIFEST_FILENAME}",
            "data": f"{expected_dir}/{DATA_FILENAME}",
            "qa": f"{expected_dir}/{QA_FILENAME}",
            "examples": f"{expected_dir}/{EXAMPLES_FILENAME}",
        }
        if (
            self.candidate_path != expected_paths["candidate"]
            or self.data_path != expected_paths["data"]
            or self.qa_path != expected_paths["qa"]
            or self.bounded_examples_path != expected_paths["examples"]
        ):
            raise IdentityMarketInventoryRunnerError("completion candidate paths are not canonical")
        for label, value in (
            ("candidate bytes", self.candidate_bytes),
            ("DATA bytes", self.data_bytes),
            ("QA bytes", self.qa_bytes),
            ("bounded examples bytes", self.bounded_examples_bytes),
            ("source artifact count", self.source_artifact_count),
            ("source row count", self.source_row_count),
            ("source bytes", self.source_bytes),
            ("authority row count", self.authority_row_count),
            ("reconciliation row count", self.reconciliation_row_count),
            ("inventory row count", self.inventory_row_count),
            ("session count", self.session_count),
            ("peak RSS bytes", self.peak_rss_bytes),
            ("minimum disk free bytes", self.minimum_disk_free_bytes),
            ("maximum tmp bytes", self.maximum_tmp_bytes),
            ("output bytes", self.output_bytes),
        ):
            _nonnegative_int(value, label)
        if (
            type(self.wall_clock_seconds) is not float
            or self.wall_clock_seconds < 0
            or not self.wall_clock_seconds < float("inf")
        ):
            raise IdentityMarketInventoryRunnerError("completion wall-clock metric is invalid")
        if type(self.disk_free_warning_triggered) is not bool:
            raise IdentityMarketInventoryRunnerError(
                "completion disk warning marker must be a native bool"
            )
        if self.source_row_count != self.authority_row_count + self.reconciliation_row_count:
            raise IdentityMarketInventoryRunnerError("completion source counts do not reconcile")
        object.__setattr__(
            self,
            "completed_at_utc",
            _utc(self.completed_at_utc, "completed_at_utc"),
        )
        if self.completion_state != CANDIDATE_STATE:
            raise IdentityMarketInventoryRunnerError("completion state is not awaiting_review")

    def logical_payload(self) -> dict[str, object]:
        return {
            "algorithm_digest": INVENTORY_ALGORITHM_DIGEST,
            "approval_id": self.approval_id,
            "approval_sha256": self.approval_sha256,
            "artifact_type": "s7_composite_inventory_execution_completion",
            "candidate": {
                "bounded_examples": {
                    "bytes": self.bounded_examples_bytes,
                    "path": self.bounded_examples_path,
                    "sha256": self.bounded_examples_sha256,
                },
                "bytes": self.candidate_bytes,
                "candidate_id": self.candidate_id,
                "data": {
                    "bytes": self.data_bytes,
                    "path": self.data_path,
                    "sha256": self.data_sha256,
                },
                "path": self.candidate_path,
                "qa": {
                    "bytes": self.qa_bytes,
                    "path": self.qa_path,
                    "sha256": self.qa_sha256,
                },
                "sha256": self.candidate_sha256,
                "state": CANDIDATE_STATE,
            },
            "capabilities": _fail_closed_capabilities(),
            "completed_at_utc": _utc_text(self.completed_at_utc),
            "completion_rule_version": COMPLETION_RULE_VERSION,
            "completion_state": self.completion_state,
            "counts": {
                "authority_row_count": self.authority_row_count,
                "inventory_row_count": self.inventory_row_count,
                "reconciliation_row_count": self.reconciliation_row_count,
                "session_count": self.session_count,
                "source_artifact_count": self.source_artifact_count,
                "source_bytes": self.source_bytes,
                "source_row_count": self.source_row_count,
            },
            "input_binding_digest": self.input_binding_digest,
            "plan_id": self.plan_id,
            "plan_sha256": self.plan_sha256,
            "request_event_id": self.request_event_id,
            "request_event_sha256": self.request_event_sha256,
            "resource_measurements": {
                "maximum_tmp_bytes": self.maximum_tmp_bytes,
                "minimum_disk_free_bytes": self.minimum_disk_free_bytes,
                "disk_free_warning_triggered": self.disk_free_warning_triggered,
                "output_bytes": self.output_bytes,
                "peak_rss_bytes": self.peak_rss_bytes,
                "wall_clock_seconds": self.wall_clock_seconds,
            },
            "schema_version": COMPLETION_SCHEMA_VERSION,
            "source_artifact_set_digest": self.source_artifact_set_digest,
        }

    @property
    def completion_id(self) -> str:
        return stable_digest(self.logical_payload())

    @property
    def document(self) -> Mapping[str, object]:
        return MappingProxyType({**self.logical_payload(), "completion_id": self.completion_id})

    @property
    def content(self) -> bytes:
        return _canonical_bytes(self.document)

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.content).hexdigest()

    @property
    def relative_path(self) -> str:
        return composite_inventory_completion_path(self.plan_id, self.approval_id)

    @classmethod
    def from_dict(cls, value: object) -> S7CompositeInventoryExecutionCompletion:
        document = _mapping(value, "inventory completion")
        candidate = _mapping(document.get("candidate"), "completion candidate")
        data = _mapping(candidate.get("data"), "completion DATA ref")
        qa = _mapping(candidate.get("qa"), "completion QA ref")
        examples = _mapping(candidate.get("bounded_examples"), "completion examples ref")
        counts = _mapping(document.get("counts"), "completion counts")
        metrics = _mapping(document.get("resource_measurements"), "completion metrics")
        completion = cls(
            plan_id=_string(document.get("plan_id"), "plan ID"),
            plan_sha256=_string(document.get("plan_sha256"), "plan SHA-256"),
            approval_id=_string(document.get("approval_id"), "approval ID"),
            approval_sha256=_string(document.get("approval_sha256"), "approval SHA-256"),
            request_event_id=_string(document.get("request_event_id"), "request event ID"),
            request_event_sha256=_string(
                document.get("request_event_sha256"), "request event SHA-256"
            ),
            input_binding_digest=_string(
                document.get("input_binding_digest"), "input binding digest"
            ),
            candidate_id=_string(candidate.get("candidate_id"), "candidate ID"),
            candidate_path=_string(candidate.get("path"), "candidate path"),
            candidate_sha256=_string(candidate.get("sha256"), "candidate SHA-256"),
            candidate_bytes=_nonnegative_int(candidate.get("bytes"), "candidate bytes"),
            data_path=_string(data.get("path"), "DATA path"),
            data_sha256=_string(data.get("sha256"), "DATA SHA-256"),
            data_bytes=_nonnegative_int(data.get("bytes"), "DATA bytes"),
            qa_path=_string(qa.get("path"), "QA path"),
            qa_sha256=_string(qa.get("sha256"), "QA SHA-256"),
            qa_bytes=_nonnegative_int(qa.get("bytes"), "QA bytes"),
            bounded_examples_path=_string(examples.get("path"), "examples path"),
            bounded_examples_sha256=_string(examples.get("sha256"), "examples SHA-256"),
            bounded_examples_bytes=_nonnegative_int(examples.get("bytes"), "examples bytes"),
            source_artifact_set_digest=_string(
                document.get("source_artifact_set_digest"), "source artifact set digest"
            ),
            source_artifact_count=_nonnegative_int(
                counts.get("source_artifact_count"), "source artifact count"
            ),
            source_row_count=_nonnegative_int(counts.get("source_row_count"), "source rows"),
            source_bytes=_nonnegative_int(counts.get("source_bytes"), "source bytes"),
            authority_row_count=_nonnegative_int(
                counts.get("authority_row_count"), "authority rows"
            ),
            reconciliation_row_count=_nonnegative_int(
                counts.get("reconciliation_row_count"), "reconciliation rows"
            ),
            inventory_row_count=_nonnegative_int(
                counts.get("inventory_row_count"), "inventory rows"
            ),
            session_count=_nonnegative_int(counts.get("session_count"), "session count"),
            completed_at_utc=_parse_utc(document.get("completed_at_utc"), "completed_at_utc"),
            wall_clock_seconds=_float(metrics.get("wall_clock_seconds"), "wall clock"),
            peak_rss_bytes=_nonnegative_int(metrics.get("peak_rss_bytes"), "peak RSS bytes"),
            minimum_disk_free_bytes=_nonnegative_int(
                metrics.get("minimum_disk_free_bytes"), "minimum disk free bytes"
            ),
            maximum_tmp_bytes=_nonnegative_int(
                metrics.get("maximum_tmp_bytes"), "maximum tmp bytes"
            ),
            disk_free_warning_triggered=metrics.get("disk_free_warning_triggered"),
            output_bytes=_nonnegative_int(metrics.get("output_bytes"), "output bytes"),
            completion_state=_string(document.get("completion_state"), "completion state"),
        )
        if _canonical_bytes(document) != completion.content:
            raise IdentityMarketInventoryRunnerError("completion canonical bytes do not reproduce")
        return completion


@dataclass(frozen=True, slots=True)
class _LoadedControls:
    plan: S7CompositeInventoryExecutionPlanV2
    approval: Any
    calendar: XNYSCalendarArtifact
    sessions: tuple[date, ...]


@dataclass(frozen=True, slots=True)
class _InodeIdentity:
    device: int
    inode: int


@dataclass(slots=True)
class _ResourceMonitor:
    root: Path
    staging: Path
    caps: Any
    started: float = field(default_factory=time.monotonic)
    minimum_disk_free_bytes: int = field(default=2**63 - 1)
    maximum_tmp_bytes: int = 0
    peak_rss_bytes: int = 0

    def check(self) -> None:
        elapsed = time.monotonic() - self.started
        if elapsed > self.caps.wall_clock_seconds_cap:
            raise IdentityMarketInventoryRunnerError("resource_cap_exceeded: wall clock")
        rss = _peak_rss_bytes()
        self.peak_rss_bytes = max(self.peak_rss_bytes, rss)
        if rss > self.caps.rss_bytes_cap:
            raise IdentityMarketInventoryRunnerError("resource_cap_exceeded: RSS")
        free = shutil.disk_usage(self.root).free
        self.minimum_disk_free_bytes = min(self.minimum_disk_free_bytes, free)
        if free < self.caps.disk_free_floor_bytes:
            raise IdentityMarketInventoryRunnerError("resource_cap_exceeded: disk floor")
        tmp = _tree_bytes(self.staging)
        self.maximum_tmp_bytes = max(self.maximum_tmp_bytes, tmp)
        if tmp > self.caps.tmp_bytes_cap:
            raise IdentityMarketInventoryRunnerError("resource_cap_exceeded: tmp bytes")

    @property
    def elapsed(self) -> float:
        return float(time.monotonic() - self.started)


def composite_inventory_completion_path(plan_id: str, approval_id: str) -> str:
    _digest(plan_id, "plan ID")
    _digest(approval_id, "approval ID")
    return (
        "manifests/silver/identity/composite-inventory-execution-completions/"
        f"plan_id={plan_id}/approval_id={approval_id}/manifest.json"
    )


def run_source_bound_composite_inventory(
    data_root: Path,
    *,
    plan_id: str,
    expected_plan_sha256: str,
    approval_id: str,
    expected_approval_sha256: str,
) -> S7CompositeInventoryExecutionCompletion:
    """Execute one exact approved full-history inventory and stop at review."""

    runner_started = time.monotonic()
    root = _validated_root(data_root)
    for label, value in (
        ("plan ID", plan_id),
        ("plan SHA-256", expected_plan_sha256),
        ("approval ID", approval_id),
        ("approval SHA-256", expected_approval_sha256),
    ):
        _digest(value, label)
    controls = _load_controls(
        root,
        plan_id=plan_id,
        expected_plan_sha256=expected_plan_sha256,
        approval_id=approval_id,
        expected_approval_sha256=expected_approval_sha256,
    )
    _verify_execution_root_binding(root, controls)
    _verify_git_checkout_and_pins(controls.plan)
    _verify_runtime_semantics()
    run_id = stable_digest(
        {
            "approval_id": approval_id,
            "plan_id": plan_id,
            "runner_rule_version": RUNNER_RULE_VERSION,
        }
    )
    lock_relative = canonical_execution_paths()["lock"].format(run_id=run_id)
    staging_relative = canonical_execution_paths()["staging"].format(run_id=run_id)
    lock_path = _safe(root, lock_relative)
    staging = _safe(root, staging_relative)
    completion_path = _safe(
        root,
        composite_inventory_completion_path(plan_id, approval_id),
    )
    if completion_path.is_file() and not completion_path.is_symlink():
        monitor = _ResourceMonitor(
            root=root,
            staging=staging,
            caps=controls.plan.resource_caps,
            started=runner_started,
        )
        return _read_and_revalidate_completion(
            root,
            completion_path,
            controls,
            monitor=monitor,
        )
    if completion_path.exists() or completion_path.is_symlink():
        raise IdentityMarketInventoryRunnerError("completion slot is unsafe")

    with _exclusive_nonblocking_lock(lock_path):
        if completion_path.is_file() and not completion_path.is_symlink():
            monitor = _ResourceMonitor(
                root=root,
                staging=staging,
                caps=controls.plan.resource_caps,
                started=runner_started,
            )
            return _read_and_revalidate_completion(
                root,
                completion_path,
                controls,
                monitor=monitor,
            )
        if completion_path.exists() or completion_path.is_symlink():
            raise IdentityMarketInventoryRunnerError("completion slot is unsafe")
        _fail_if_partial_candidate_exists(root, plan_id, approval_id)
        if staging.exists() or staging.is_symlink():
            raise IdentityMarketInventoryRunnerError(
                f"stale staging exists; manual review required: {staging_relative}"
            )
        staging.mkdir(parents=True, exist_ok=False)
        staging_identity = _directory_identity(staging, "new runner staging")
        monitor = _ResourceMonitor(
            root=root,
            staging=staging,
            caps=controls.plan.resource_caps,
            started=runner_started,
        )
        monitor.check()
        return _execute_new_candidate(
            root,
            controls,
            staging=staging,
            staging_identity=staging_identity,
            completion_path=completion_path,
            monitor=monitor,
        )


# Explicit alias retained for the CLI/control-plane name used in the v2 plan.
run_s7_composite_inventory = run_source_bound_composite_inventory


def _load_controls(
    root: Path,
    *,
    plan_id: str,
    expected_plan_sha256: str,
    approval_id: str,
    expected_approval_sha256: str,
) -> _LoadedControls:
    """Load the exact plan, execution approval and calendar without discovery."""

    # Local import prevents the control-plane plan builder from acquiring runner
    # capability merely by importing its own definitions.
    try:
        from ame_stocks_api.silver.identity_market_inventory_approval import (
            IdentityMarketInventoryExecutionApprovalError,
            IdentityMarketInventoryExecutionApprovalStore,
            S7CompositeInventoryExecutionApprovalV2,
        )
    except ImportError as exc:  # pragma: no cover - executable commit must contain it
        raise IdentityMarketInventoryRunnerError(
            "execution approval module is absent from the approved runtime"
        ) from exc

    try:
        plan_store = IdentityMarketInventoryExecutionPlanStore(root)
        plan, _ = plan_store.load_execution_plan_v2(
            plan_id,
            expected_sha256=expected_plan_sha256,
        )
        approval_store = IdentityMarketInventoryExecutionApprovalStore(root)
        approval, _ = approval_store.load_approval(
            approval_id,
            expected_sha256=expected_approval_sha256,
        )
        calendar = load_xnys_calendar_artifact(
            root,
            calendar_artifact_id=INVENTORY_CALENDAR_ARTIFACT_ID,
            expected_sha256=INVENTORY_CALENDAR_ARTIFACT_SHA256,
        )
    except (
        ArtifactError,
        IdentityMarketInventoryExecutionPlanError,
        IdentityMarketInventoryExecutionApprovalError,
        XNYSCalendarArtifactError,
        OSError,
    ) as exc:
        raise IdentityMarketInventoryRunnerError(
            "exact execution controls cannot be loaded"
        ) from exc
    if not isinstance(plan, S7CompositeInventoryExecutionPlanV2) or not isinstance(
        approval, S7CompositeInventoryExecutionApprovalV2
    ):
        raise IdentityMarketInventoryRunnerError("execution controls have wrong types")
    _verify_approval_plan_binding(plan, approval)
    sessions = tuple(
        item.session_date
        for item in calendar.sessions
        if INVENTORY_START_SESSION <= item.session_date <= INVENTORY_END_SESSION
    )
    if (
        len(sessions) != INVENTORY_SESSION_COUNT
        or not sessions
        or sessions[0] != INVENTORY_START_SESSION
        or sessions[-1] != INVENTORY_END_SESSION
        or tuple(sorted(set(sessions))) != sessions
    ):
        raise IdentityMarketInventoryRunnerError("exact inventory session spine does not reproduce")
    return _LoadedControls(plan=plan, approval=approval, calendar=calendar, sessions=sessions)


def _verify_execution_root_binding(root: Path, controls: _LoadedControls) -> None:
    """Bind the runner's resolved root to both immutable execution controls."""

    actual = str(root)
    if (
        controls.plan.execution_data_root != actual
        or controls.approval.execution_data_root != actual
    ):
        raise IdentityMarketInventoryRunnerError(
            "actual data_root differs from the exact approved execution_data_root"
        )


def _verify_approval_plan_binding(plan: Any, approval: Any) -> None:
    expected = {
        "plan_id": plan.plan_id,
        "plan_sha256": plan.sha256,
        "input_binding_digest": plan.input_binding_digest,
        "resource_caps_digest": plan.resource_caps.digest,
        "runtime_file_set_digest": plan.runtime_file_set_digest,
        "verification_file_set_digest": plan.verification_file_set_digest,
        "inventory_contract_id": plan.inventory_contract.contract_id,
        "inventory_schema_digest": plan.inventory_contract.schema_digest,
        "algorithm_digest": INVENTORY_ALGORITHM_DIGEST,
        "qa_semantics_digest": INVENTORY_QA_SEMANTICS_DIGEST,
        "execution_data_root": plan.execution_data_root,
    }
    for field_name, expected_value in expected.items():
        if getattr(approval, field_name, None) != expected_value:
            raise IdentityMarketInventoryRunnerError(
                f"execution approval crosses the v2 plan at {field_name}"
            )
    for field_name in ("request_event_id", "request_event_sha256"):
        _digest(getattr(approval, field_name, None), f"approval {field_name}")
    approved = getattr(approval, "approved_at_utc", None)
    if (
        not isinstance(approved, datetime)
        or approved.tzinfo is None
        or approved > datetime.now(UTC)
    ):
        raise IdentityMarketInventoryRunnerError("execution approval time is invalid")


def _verify_git_checkout_and_pins(plan: S7CompositeInventoryExecutionPlanV2) -> None:
    repo = _repository_root()
    head = _git(repo, "rev-parse", "HEAD")
    tree = _git(repo, "rev-parse", "HEAD^{tree}")
    if head != plan.execution_git_commit or tree != plan.execution_git_tree:
        raise IdentityMarketInventoryRunnerError("Git commit/tree differs from exact plan")
    if _git(repo, "status", "--porcelain=v1", "--untracked-files=all"):
        raise IdentityMarketInventoryRunnerError("Git checkout is not clean")
    pins = tuple(plan.runtime_files) + tuple(plan.verification_files)
    if not pins or len({item.path for item in pins}) != len(pins):
        raise IdentityMarketInventoryRunnerError("runtime/verification pins are incomplete")
    for pin in pins:
        path = _repo_path(repo, pin.path)
        if (
            not path.is_file()
            or path.is_symlink()
            or path.stat().st_size != pin.bytes
            or sha256_file(path) != pin.sha256
        ):
            raise IdentityMarketInventoryRunnerError(f"pinned runtime bytes differ: {pin.path}")
        output = _git(repo, "ls-tree", plan.execution_git_commit, "--", pin.path)
        parts = output.split()
        if len(parts) < 4 or parts[1] != "blob" or parts[2] != pin.git_blob:
            raise IdentityMarketInventoryRunnerError(f"pinned Git blob differs: {pin.path}")


def _verify_runtime_semantics() -> None:
    projection = tuple(
        asset if asset == universe else f"{asset}_to_{universe}"
        for asset, universe in UNIVERSE_PARENT_PROJECTION
    )
    if (
        pa.__version__ != "25.0.0"
        or projection != UNIVERSE_PARENT_RECONCILIATION_PROJECTION
        or tuple(COMPOSITE_FIGI_INVENTORY_CONTRACT.arrow_schema.names) != INVENTORY_OUTPUT_COLUMNS
    ):
        raise IdentityMarketInventoryRunnerError(
            "runtime implementation differs from frozen algorithm/serialization semantics"
        )


def _execute_new_candidate(
    root: Path,
    controls: _LoadedControls,
    *,
    staging: Path,
    staging_identity: _InodeIdentity,
    completion_path: Path,
    monitor: _ResourceMonitor,
) -> S7CompositeInventoryExecutionCompletion:
    try:
        bundle = open_identity_source_bundle(root)
        bundle.require_official()
    except (IdentitySourceError, ArtifactError, OSError) as exc:
        raise IdentityMarketInventoryRunnerError(
            "exact official S7 source bundle cannot open"
        ) from exc
    try:
        asset_artifacts = bundle.daily_partition_artifacts(
            ASSET_AUTHORITY_TABLE,
            controls.sessions,
        )
        universe_artifacts = bundle.daily_partition_artifacts(
            UNIVERSE_RECONCILIATION_TABLE,
            controls.sessions,
        )
    except IdentitySourceError as exc:
        raise IdentityMarketInventoryRunnerError(
            "exact S4 daily artifact spine cannot be selected"
        ) from exc
    source_refs = _preflight_source_scope(
        controls,
        asset_artifacts=asset_artifacts,
        universe_artifacts=universe_artifacts,
    )
    authority_release_id = bundle.sources[ASSET_AUTHORITY_TABLE].pin.release_id
    engine = CompositeInventoryEngine(
        authority_release_id=authority_release_id,
        caps=CompositeInventoryCaps(
            max_distinct_composite_figis=controls.plan.resource_caps.distinct_composite_cap,
            max_distinct_composite_share_class_pairs=(
                controls.plan.resource_caps.composite_share_class_pair_cap
            ),
            bounded_example_limit=controls.plan.resource_caps.bounded_example_cap,
        ),
    )
    scanned_rows = 0
    batches = 0
    try:
        for session, asset, universe in zip(
            controls.sessions,
            asset_artifacts,
            universe_artifacts,
            strict=True,
        ):
            engine.start_session(session)
            for physical in bundle.iter_physical_batches(
                ASSET_AUTHORITY_TABLE,
                columns=ASSET_COLUMNS,
                batch_size=controls.plan.resource_caps.batch_size,
                artifacts=(asset,),
            ):
                monitor.check()
                physical.require_official()
                engine.consume_asset_batch(
                    physical.batch.to_pylist(),
                    artifact_path=physical.artifact.ref.path,
                    artifact_sha256=physical.artifact.ref.sha256,
                    row_group=physical.row_group,
                    row_index_base=physical.row_index_in_group,
                )
                scanned_rows += physical.batch.num_rows
                batches += 1
                if batches % controls.plan.resource_caps.resource_check_interval_batches == 0:
                    monitor.check()
                if scanned_rows > controls.plan.resource_caps.scanned_row_cap:
                    raise IdentityMarketInventoryRunnerError("resource_cap_exceeded: scanned rows")
            for physical in bundle.iter_physical_batches(
                UNIVERSE_RECONCILIATION_TABLE,
                columns=UNIVERSE_COLUMNS,
                batch_size=controls.plan.resource_caps.batch_size,
                artifacts=(universe,),
            ):
                monitor.check()
                physical.require_official()
                engine.consume_universe_batch(
                    physical.batch.to_pylist(),
                    artifact_path=physical.artifact.ref.path,
                    artifact_sha256=physical.artifact.ref.sha256,
                    row_group=physical.row_group,
                    row_index_base=physical.row_index_in_group,
                )
                scanned_rows += physical.batch.num_rows
                batches += 1
                if batches % controls.plan.resource_caps.resource_check_interval_batches == 0:
                    monitor.check()
                if scanned_rows > controls.plan.resource_caps.scanned_row_cap:
                    raise IdentityMarketInventoryRunnerError("resource_cap_exceeded: scanned rows")
            engine.finish_session()
        result = engine.finalize()
    except (CompositeInventoryError, IdentitySourceError) as exc:
        raise IdentityMarketInventoryRunnerError("full physical S4 inventory scan failed") from exc
    if scanned_rows != sum(item.row_count for item in source_refs):
        raise IdentityMarketInventoryRunnerError("source_count_mismatch: scanned rows")
    _validate_result(result, controls=controls, source_refs=source_refs)
    monitor.check()
    return _stage_and_commit_candidate(
        root,
        controls,
        result=result,
        source_refs=source_refs,
        staging=staging,
        staging_identity=staging_identity,
        completion_path=completion_path,
        monitor=monitor,
    )


def _preflight_source_scope(
    controls: _LoadedControls,
    *,
    asset_artifacts: Sequence[IdentitySourceArtifact],
    universe_artifacts: Sequence[IdentitySourceArtifact],
) -> tuple[InventorySourceArtifactRef, ...]:
    sessions = controls.sessions
    if len(asset_artifacts) != len(sessions) or len(universe_artifacts) != len(sessions):
        raise IdentityMarketInventoryRunnerError("session_spine_mismatch: daily artifact count")
    refs = tuple(
        sorted(
            (
                *(
                    _source_ref(ASSET_AUTHORITY_TABLE, session, artifact)
                    for session, artifact in zip(sessions, asset_artifacts, strict=True)
                ),
                *(
                    _source_ref(UNIVERSE_RECONCILIATION_TABLE, session, artifact)
                    for session, artifact in zip(sessions, universe_artifacts, strict=True)
                ),
            )
        )
    )
    by_table = {
        table: tuple(item for item in refs if item.table == table)
        for table in (ASSET_AUTHORITY_TABLE, UNIVERSE_RECONCILIATION_TABLE)
    }
    for table, items in by_table.items():
        if tuple(item.session_date for item in items) != sessions:
            raise IdentityMarketInventoryRunnerError(
                f"session_spine_mismatch: {table} session mapping"
            )
        if tuple(item.path for item in items) != tuple(sorted(item.path for item in items)):
            raise IdentityMarketInventoryRunnerError(
                f"source scan order is not artifact-path ascending: {table}"
            )
    caps = controls.plan.resource_caps
    artifact_count = len(refs)
    row_count = sum(item.row_count for item in refs)
    byte_count = sum(item.bytes for item in refs)
    if (
        artifact_count != caps.scanned_artifact_cap
        or row_count != caps.scanned_row_cap
        or byte_count != caps.source_bytes_cap
    ):
        raise IdentityMarketInventoryRunnerError("source_count_mismatch: preflight totals")
    if len(sessions) != getattr(controls.plan, "session_count", len(sessions)):
        raise IdentityMarketInventoryRunnerError("session_spine_mismatch: plan count")
    return refs


def _source_ref(
    table: str,
    session: date,
    artifact: IdentitySourceArtifact,
) -> InventorySourceArtifactRef:
    artifact.require_official()
    if artifact.table != table or artifact.ref.row_count is None:
        raise IdentityMarketInventoryRunnerError("source artifact metadata is invalid")
    pin = S7_SOURCE_PINS[table]
    if (
        artifact.release_id != pin.release_id
        or artifact.release_manifest_sha256 != pin.release_manifest_sha256
        or artifact.ref.table != table
    ):
        raise IdentityMarketInventoryRunnerError("source artifact crosses exact S4 release")
    return InventorySourceArtifactRef(
        table=table,
        session_date=session,
        release_id=artifact.release_id,
        release_manifest_sha256=artifact.release_manifest_sha256,
        path=artifact.ref.path,
        sha256=artifact.ref.sha256,
        bytes=artifact.ref.bytes,
        row_count=artifact.ref.row_count,
    )


def _validate_result(
    result: CompositeInventoryResult,
    *,
    controls: _LoadedControls,
    source_refs: tuple[InventorySourceArtifactRef, ...],
) -> None:
    diagnostics = result.diagnostics
    authority_rows = sum(
        item.row_count for item in source_refs if item.table == ASSET_AUTHORITY_TABLE
    )
    reconciliation_rows = sum(
        item.row_count for item in source_refs if item.table == UNIVERSE_RECONCILIATION_TABLE
    )
    if (
        diagnostics.authority_row_count != authority_rows
        or diagnostics.reconciliation_row_count != reconciliation_rows
        or diagnostics.completed_session_count != len(controls.sessions)
        or diagnostics.valid_composite_row_count
        != sum(item.active_row_count + item.inactive_row_count for item in result.records)
        or len(result.records) > controls.plan.resource_caps.distinct_composite_cap
        or diagnostics.distinct_composite_share_class_pair_count
        > controls.plan.resource_caps.composite_share_class_pair_cap
    ):
        raise IdentityMarketInventoryRunnerError("aggregate_invariant_invalid: result totals")
    keys = tuple(item.observed_composite_figi for item in result.records)
    if not keys or keys != tuple(sorted(set(keys))):
        raise IdentityMarketInventoryRunnerError("output_sort_invalid or primary_key_duplicate")
    for record in result.records:
        if (
            figi_invalid_reason(record.observed_composite_figi) is not None
            or tuple(record.observed_share_class_figis)
            != tuple(sorted(set(record.observed_share_class_figis)))
            or any(
                figi_invalid_reason(value, field="share_class_figi") is not None
                for value in record.observed_share_class_figis
            )
            or record.share_class_conflict != (len(record.observed_share_class_figis) > 1)
            or record.first_session > record.last_session
            or record.first_session < controls.sessions[0]
            or record.last_session > controls.sessions[-1]
            or record.session_count <= 0
            or record.ticker_count <= 0
            or record.active_row_count + record.inactive_row_count <= 0
            or record.parent_table_count != 1
            or record.source_release_count != 1
            or _DIGEST.fullmatch(record.source_record_lineage_digest) is None
        ):
            raise IdentityMarketInventoryRunnerError(
                "aggregate_invariant_invalid: candidate record"
            )


def _stage_and_commit_candidate(
    root: Path,
    controls: _LoadedControls,
    *,
    result: CompositeInventoryResult,
    source_refs: tuple[InventorySourceArtifactRef, ...],
    staging: Path,
    staging_identity: _InodeIdentity,
    completion_path: Path,
    monitor: _ResourceMonitor,
) -> S7CompositeInventoryExecutionCompletion:
    _require_directory_identity(staging, staging_identity, "runner staging")
    stage_candidate = staging / "candidate"
    stage_candidate.mkdir(parents=False, exist_ok=False)
    candidate_identity = _directory_identity(stage_candidate, "staged candidate")
    data_path = stage_candidate / DATA_FILENAME
    qa_path = stage_candidate / QA_FILENAME
    examples_path = stage_candidate / EXAMPLES_FILENAME
    manifest_path = stage_candidate / MANIFEST_FILENAME
    data_path.parent.mkdir(parents=True, exist_ok=False)
    qa_path.parent.mkdir(parents=True, exist_ok=False)
    examples_path.parent.mkdir(parents=True, exist_ok=False)

    table = pa.Table.from_pylist(
        result.output_rows(),
        schema=COMPOSITE_FIGI_INVENTORY_CONTRACT.arrow_schema,
    )
    if not table.schema.equals(COMPOSITE_FIGI_INVENTORY_CONTRACT.arrow_schema):
        raise IdentityMarketInventoryRunnerError("schema_exact: in-memory table differs")
    _require_directory_identity(stage_candidate, candidate_identity, "staged candidate")
    _write_parquet_exclusive(table, data_path)
    _validate_staged_parquet(data_path, result)
    data_ref = InventoryOutputArtifactRef(
        role="data",
        path=DATA_FILENAME,
        sha256=sha256_file(data_path),
        bytes=data_path.stat().st_size,
        media_type="application/vnd.apache.parquet",
        row_count=table.num_rows,
        schema_digest=COMPOSITE_FIGI_INVENTORY_SCHEMA_DIGEST,
    )

    examples_document = _bounded_examples_document(controls, result.diagnostics)
    examples_content = _canonical_bytes(examples_document)
    _require_directory_identity(stage_candidate, candidate_identity, "staged candidate")
    _write_staged_bytes(examples_path, examples_content)
    examples_ref = InventoryOutputArtifactRef(
        role="bounded_examples",
        path=EXAMPLES_FILENAME,
        sha256=hashlib.sha256(examples_content).hexdigest(),
        bytes=len(examples_content),
        media_type="application/json",
    )
    qa_document = _qa_document(
        controls,
        diagnostics=result.diagnostics,
        examples_ref=examples_ref,
    )
    qa_content = _canonical_bytes(qa_document)
    _require_directory_identity(stage_candidate, candidate_identity, "staged candidate")
    _write_staged_bytes(qa_path, qa_content)
    qa_ref = InventoryOutputArtifactRef(
        role="qa",
        path=QA_FILENAME,
        sha256=hashlib.sha256(qa_content).hexdigest(),
        bytes=len(qa_content),
        media_type="application/json",
    )

    created_at = datetime.now(UTC)
    candidate = S7CompositeInventoryCandidate(
        plan_id=controls.plan.plan_id,
        plan_sha256=controls.plan.sha256,
        approval_id=controls.approval.approval_id,
        approval_sha256=controls.approval.sha256,
        request_event_id=controls.approval.request_event_id,
        request_event_sha256=controls.approval.request_event_sha256,
        input_binding_digest=controls.plan.input_binding_digest,
        resource_caps_digest=controls.plan.resource_caps.digest,
        runtime_file_set_digest=controls.plan.runtime_file_set_digest,
        verification_file_set_digest=controls.plan.verification_file_set_digest,
        source_artifacts=source_refs,
        source_artifact_set_digest=stable_digest([item.to_dict() for item in source_refs]),
        source_artifact_count=len(source_refs),
        source_row_count=sum(item.row_count for item in source_refs),
        source_bytes=sum(item.bytes for item in source_refs),
        authority_row_count=result.diagnostics.authority_row_count,
        reconciliation_row_count=result.diagnostics.reconciliation_row_count,
        session_count=result.diagnostics.completed_session_count,
        inventory_row_count=len(result.records),
        valid_composite_row_count=result.diagnostics.valid_composite_row_count,
        distinct_composite_share_class_pair_count=(
            result.diagnostics.distinct_composite_share_class_pair_count
        ),
        diagnostics=_manifest_diagnostics(result.diagnostics),
        artifacts=(data_ref, qa_ref, examples_ref),
        created_at_utc=created_at,
    )
    _require_directory_identity(stage_candidate, candidate_identity, "staged candidate")
    _write_staged_bytes(manifest_path, candidate.content)
    monitor.check()
    _validate_staged_candidate(stage_candidate, candidate)

    candidate_output_bytes = (
        len(candidate.content) + data_ref.bytes + qa_ref.bytes + examples_ref.bytes
    )
    completed_at = datetime.now(UTC)
    completion = S7CompositeInventoryExecutionCompletion(
        plan_id=controls.plan.plan_id,
        plan_sha256=controls.plan.sha256,
        approval_id=controls.approval.approval_id,
        approval_sha256=controls.approval.sha256,
        request_event_id=controls.approval.request_event_id,
        request_event_sha256=controls.approval.request_event_sha256,
        input_binding_digest=controls.plan.input_binding_digest,
        candidate_id=candidate.candidate_id,
        candidate_path=candidate.relative_path,
        candidate_sha256=candidate.sha256,
        candidate_bytes=len(candidate.content),
        data_path=f"{candidate.relative_directory}/{DATA_FILENAME}",
        data_sha256=data_ref.sha256,
        data_bytes=data_ref.bytes,
        qa_path=f"{candidate.relative_directory}/{QA_FILENAME}",
        qa_sha256=qa_ref.sha256,
        qa_bytes=qa_ref.bytes,
        bounded_examples_path=f"{candidate.relative_directory}/{EXAMPLES_FILENAME}",
        bounded_examples_sha256=examples_ref.sha256,
        bounded_examples_bytes=examples_ref.bytes,
        source_artifact_set_digest=candidate.source_artifact_set_digest,
        source_artifact_count=candidate.source_artifact_count,
        source_row_count=candidate.source_row_count,
        source_bytes=candidate.source_bytes,
        authority_row_count=candidate.authority_row_count,
        reconciliation_row_count=candidate.reconciliation_row_count,
        inventory_row_count=candidate.inventory_row_count,
        session_count=candidate.session_count,
        completed_at_utc=completed_at,
        wall_clock_seconds=monitor.elapsed,
        peak_rss_bytes=max(monitor.peak_rss_bytes, _peak_rss_bytes()),
        minimum_disk_free_bytes=monitor.minimum_disk_free_bytes,
        maximum_tmp_bytes=max(monitor.maximum_tmp_bytes, _tree_bytes(staging)),
        disk_free_warning_triggered=(
            monitor.minimum_disk_free_bytes < controls.plan.resource_caps.disk_free_warning_bytes
        ),
        output_bytes=0,
    )
    # ``output_bytes`` includes all stable candidate bytes and the final completion
    # bytes.  It is solved once because changing it changes only the completion.
    for _ in range(4):
        output_bytes = candidate_output_bytes + len(completion.content)
        if output_bytes == completion.output_bytes:
            break
        completion = _completion_with_output_bytes(completion, output_bytes)
    if completion.output_bytes != candidate_output_bytes + len(completion.content):
        raise IdentityMarketInventoryRunnerError("completion output-byte fixed point failed")
    _validate_completion_resources(completion, controls.plan.resource_caps)
    if completion.completed_at_utc < controls.approval.approved_at_utc:
        raise IdentityMarketInventoryRunnerError("candidate predates exact execution approval")
    staged_completion = staging / "completion.json"
    _require_directory_identity(staging, staging_identity, "runner staging")
    _write_staged_bytes(staged_completion, completion.content)
    completion_identity = _regular_file_identity(
        staged_completion,
        "staged completion",
    )
    monitor.check()

    final_candidate = _safe(root, candidate.relative_directory)
    if final_candidate.exists() or final_candidate.is_symlink():
        raise IdentityMarketInventoryRunnerError("candidate content-addressed slot already exists")
    final_candidate.parent.mkdir(parents=True, exist_ok=True)
    candidate_commit_attempted = False
    try:
        _require_directory_identity(stage_candidate, candidate_identity, "staged candidate")
        candidate_commit_attempted = True
        _rename_directory_noreplace(stage_candidate, final_candidate)
        _require_directory_identity(final_candidate, candidate_identity, "stable candidate")
        _fsync_directory(final_candidate.parent)
        monitor.check()
        receipt = _publish_completion_link(
            root,
            staged_completion,
            completion_path,
            expected_sha256=completion.sha256,
            expected_identity=completion_identity,
        )
        if (
            receipt["path"] != completion.relative_path
            or receipt["sha256"] != completion.sha256
            or receipt["bytes"] != len(completion.content)
            or receipt["directory_fsync_confirmed"] is not True
        ):
            raise IdentityMarketInventoryRunnerError("completion storage receipt differs")
        monitor.check()
    except BaseException as exc:
        if candidate_commit_attempted:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            raise IdentityMarketInventoryRunnerError(
                "candidate/completion commit was attempted; "
                "stable outputs retained for manual review"
            ) from exc
        if isinstance(exc, (KeyboardInterrupt, SystemExit)):
            raise
        raise IdentityMarketInventoryRunnerError(
            "candidate commit was not attempted; staging retained"
        ) from exc
    # A successful commit is authoritative.  The staged completion hardlink
    # and inode-attested run directory are intentionally retained; no exposed
    # leaf is ever removed automatically.
    return _read_and_revalidate_completion(
        root,
        completion_path,
        controls,
        monitor=monitor,
    )


def _completion_with_output_bytes(
    value: S7CompositeInventoryExecutionCompletion,
    output_bytes: int,
) -> S7CompositeInventoryExecutionCompletion:
    fields = {
        name: getattr(value, name) for name in value.__dataclass_fields__ if name != "output_bytes"
    }
    return S7CompositeInventoryExecutionCompletion(**fields, output_bytes=output_bytes)


def _rename_directory_noreplace(source: Path, target: Path) -> None:
    """Atomically rename one directory without ever replacing the target."""

    if not source.is_dir() or source.is_symlink():
        raise IdentityMarketInventoryRunnerError(
            "exclusive directory rename source is missing or unsafe"
        )
    source_stat = source.stat(follow_symlinks=False)
    try:
        _exclusive_rename_primitive(source, target)
    except OSError as exc:
        import errno

        conflicts = {errno.EEXIST, errno.ENOTEMPTY}
        unsupported = {
            errno.EINVAL,
            errno.ENOSYS,
            errno.EXDEV,
            getattr(errno, "ENOTSUP", errno.EOPNOTSUPP),
            errno.EOPNOTSUPP,
        }
        if exc.errno in conflicts:
            raise IdentityMarketInventoryRunnerError(
                "exclusive directory rename target already exists"
            ) from exc
        if exc.errno in unsupported:
            raise IdentityMarketInventoryRunnerError(
                "filesystem does not support exact no-clobber directory rename"
            ) from exc
        raise IdentityMarketInventoryRunnerError("exclusive directory rename failed") from exc
    if source.exists() or source.is_symlink():
        raise IdentityMarketInventoryRunnerError(
            "exclusive directory rename left the source visible"
        )
    if not target.is_dir() or target.is_symlink():
        raise IdentityMarketInventoryRunnerError(
            "exclusive directory rename target is missing or unsafe"
        )
    target_stat = target.stat(follow_symlinks=False)
    if (source_stat.st_dev, source_stat.st_ino) != (target_stat.st_dev, target_stat.st_ino):
        raise IdentityMarketInventoryRunnerError(
            "exclusive directory rename target identity differs"
        )


def _exclusive_rename_primitive(source: Path, target: Path) -> None:
    """Invoke the platform's atomic exclusive-rename primitive or fail closed."""

    import ctypes
    import errno

    libc = ctypes.CDLL(None, use_errno=True)
    encoded_source = os.fsencode(source)
    encoded_target = os.fsencode(target)
    ctypes.set_errno(0)
    if sys.platform.startswith("linux"):
        rename = getattr(libc, "renameat2", None)
        if rename is None:
            raise OSError(errno.ENOSYS, "libc renameat2 is unavailable")
        rename.argtypes = (
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint,
        )
        rename.restype = ctypes.c_int
        result = rename(-100, encoded_source, -100, encoded_target, 1)
    elif sys.platform == "darwin":
        rename = getattr(libc, "renamex_np", None)
        if rename is None:
            raise OSError(errno.ENOSYS, "libc renamex_np is unavailable")
        rename.argtypes = (ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint)
        rename.restype = ctypes.c_int
        result = rename(encoded_source, encoded_target, 0x00000004)
    else:
        raise OSError(errno.ENOSYS, "no exact exclusive-rename primitive for platform")
    if result != 0:
        error_number = ctypes.get_errno() or errno.EIO
        raise OSError(error_number, os.strerror(error_number), str(target))


def _validate_staged_parquet(path: Path, result: CompositeInventoryResult) -> None:
    parquet = pq.ParquetFile(path)
    if (
        not parquet.schema_arrow.equals(COMPOSITE_FIGI_INVENTORY_CONTRACT.arrow_schema)
        or parquet.metadata.num_rows != len(result.records)
        or parquet.num_row_groups != (0 if not result.records else 1)
    ):
        raise IdentityMarketInventoryRunnerError("schema_exact: staged Parquet differs")
    table = parquet.read(columns=["observed_composite_figi", "observed_share_class_figis"])
    keys = tuple(table.column("observed_composite_figi").to_pylist())
    lists = tuple(table.column("observed_share_class_figis").to_pylist())
    if keys != tuple(sorted(set(keys))) or any(values != sorted(set(values)) for values in lists):
        raise IdentityMarketInventoryRunnerError("output_sort_invalid: staged Parquet")


def _validate_staged_candidate(
    directory: Path,
    candidate: S7CompositeInventoryCandidate,
) -> None:
    manifest = directory / MANIFEST_FILENAME
    if manifest.read_bytes() != candidate.content or sha256_file(manifest) != candidate.sha256:
        raise IdentityMarketInventoryRunnerError("staged candidate manifest differs")
    for ref in candidate.artifacts:
        path = directory / ref.path
        if (
            not path.is_file()
            or path.is_symlink()
            or path.stat().st_size != ref.bytes
            or sha256_file(path) != ref.sha256
        ):
            raise IdentityMarketInventoryRunnerError(
                f"staged candidate artifact differs: {ref.role}"
            )


def _bounded_examples_document(
    controls: _LoadedControls,
    diagnostics: CompositeInventoryDiagnostics,
) -> dict[str, object]:
    examples = [item.to_dict() for item in diagnostics.bounded_invalid_examples]
    _validate_bounded_examples(
        examples,
        per_reason_cap=controls.plan.resource_caps.bounded_example_cap,
        diagnostics=diagnostics.to_dict(),
    )
    return {
        "approval_id": controls.approval.approval_id,
        "artifact_type": "s7_composite_inventory_bounded_invalid_figi_examples",
        "bounded_example_cap": controls.plan.resource_caps.bounded_example_cap,
        "examples": examples,
        "plan_id": controls.plan.plan_id,
        "schema_version": EXAMPLE_ARTIFACT_SCHEMA_VERSION,
    }


def _qa_document(
    controls: _LoadedControls,
    *,
    diagnostics: CompositeInventoryDiagnostics,
    examples_ref: InventoryOutputArtifactRef,
) -> dict[str, object]:
    numerators = _qa_numerators_from_diagnostics(diagnostics.to_dict())
    results: list[dict[str, object]] = []
    for rule in COMPOSITE_FIGI_INVENTORY_CONTRACT.qa_rules:
        numerator = numerators.get(rule.check_id, 0)
        status = rule.expected_status(numerator=numerator, rate=None)
        if rule.failure_status is QAStatus.FAILED and status is not QAStatus.PASSED:
            raise IdentityMarketInventoryRunnerError(
                f"Critical QA unexpectedly failed: {rule.check_id}"
            )
        results.append(
            {
                "bounded_examples_path": (
                    EXAMPLES_FILENAME
                    if numerator and rule.check_id in _QA_CHECKS_WITH_FIGI_EXAMPLES
                    else None
                ),
                "check_id": rule.check_id,
                "denominator": diagnostics.authority_row_count,
                "numerator": numerator,
                "severity": rule.severity.value,
                "status": status.value,
                "threshold": rule.threshold_expression,
            }
        )
    return {
        "approval_id": controls.approval.approval_id,
        "artifact_type": "s7_composite_inventory_qa",
        "bounded_examples": examples_ref.to_dict(),
        "critical_failure_count": 0,
        "plan_id": controls.plan.plan_id,
        "qa_semantics_digest": INVENTORY_QA_SEMANTICS_DIGEST,
        "results": results,
        "schema_version": QA_ARTIFACT_SCHEMA_VERSION,
    }


_QA_CHECKS_WITH_FIGI_EXAMPLES: Final = frozenset(
    {
        "malformed_composite_rows",
        "malformed_share_class_rows",
        "composite_figi_null_rows",
        "valid_composite_missing_share_class_rows",
    }
)


def _qa_numerators_from_diagnostics(diagnostics: Mapping[str, object]) -> dict[str, int]:
    invalid_composite = _mapping(
        diagnostics.get("invalid_composite_reason_counts"),
        "invalid Composite reason counts",
    )
    invalid_share = _mapping(
        diagnostics.get("invalid_share_class_reason_counts"),
        "invalid Share Class reason counts",
    )
    composite_counts = {
        reason: _nonnegative_int(count, f"{reason} count")
        for reason, count in invalid_composite.items()
    }
    share_counts = {
        reason: _nonnegative_int(count, f"{reason} count")
        for reason, count in invalid_share.items()
    }
    if set(composite_counts) - set(COMPOSITE_FIGI_INVALID_REASON_PRECEDENCE) or set(
        share_counts
    ) - set(SHARE_CLASS_FIGI_INVALID_REASON_PRECEDENCE):
        raise IdentityMarketInventoryRunnerError("candidate FIGI reason set is invalid")
    return {
        "malformed_composite_rows": sum(
            count for reason, count in composite_counts.items() if not reason.endswith("_null")
        ),
        "malformed_share_class_rows": sum(
            count for reason, count in share_counts.items() if not reason.endswith("_null")
        ),
        "share_class_conflict_groups": _nonnegative_int(
            diagnostics.get("share_class_conflict_groups"),
            "share_class_conflict_groups",
        ),
        "composite_figi_null_rows": composite_counts.get("composite_figi_null", 0),
        "valid_composite_missing_share_class_rows": share_counts.get("share_class_figi_null", 0),
    }


def _manifest_diagnostics(diagnostics: CompositeInventoryDiagnostics) -> dict[str, object]:
    value = diagnostics.to_dict()
    value.pop("bounded_invalid_examples", None)
    return value


def _read_and_revalidate_completion(
    root: Path,
    path: Path,
    controls: _LoadedControls,
    *,
    monitor: _ResourceMonitor,
) -> S7CompositeInventoryExecutionCompletion:
    monitor.check()
    if not path.is_file() or path.is_symlink():
        raise IdentityMarketInventoryRunnerError("completion is missing or unsafe")
    content = path.read_bytes()
    document = _decode_canonical_json(content, "inventory completion")
    completion = S7CompositeInventoryExecutionCompletion.from_dict(document)
    if (
        completion.relative_path != str(path.relative_to(root))
        or completion.plan_id != controls.plan.plan_id
        or completion.plan_sha256 != controls.plan.sha256
        or completion.approval_id != controls.approval.approval_id
        or completion.approval_sha256 != controls.approval.sha256
        or completion.request_event_id != controls.approval.request_event_id
        or completion.request_event_sha256 != controls.approval.request_event_sha256
        or completion.input_binding_digest != controls.plan.input_binding_digest
        or completion.completed_at_utc < controls.approval.approved_at_utc
        or completion.completed_at_utc > datetime.now(UTC)
    ):
        raise IdentityMarketInventoryRunnerError("completion crosses exact controls")
    _validate_completion_resources(completion, controls.plan.resource_caps)
    monitor.check()
    try:
        bundle = open_identity_source_bundle(root)
        bundle.require_official()
        asset_artifacts = bundle.daily_partition_artifacts(
            ASSET_AUTHORITY_TABLE,
            controls.sessions,
        )
        universe_artifacts = bundle.daily_partition_artifacts(
            UNIVERSE_RECONCILIATION_TABLE,
            controls.sessions,
        )
        current_source_refs = _preflight_source_scope(
            controls,
            asset_artifacts=asset_artifacts,
            universe_artifacts=universe_artifacts,
        )
    except (ArtifactError, IdentitySourceError, OSError) as exc:
        raise IdentityMarketInventoryRunnerError(
            "stored completion source binding cannot be revalidated"
        ) from exc
    if (
        completion.source_artifact_set_digest
        != stable_digest([item.to_dict() for item in current_source_refs])
        or completion.source_artifact_count != len(current_source_refs)
        or completion.source_row_count != sum(item.row_count for item in current_source_refs)
        or completion.source_bytes != sum(item.bytes for item in current_source_refs)
    ):
        raise IdentityMarketInventoryRunnerError("stored completion source binding differs")
    monitor.check()
    candidate_path = _safe(root, completion.candidate_path)
    if (
        not candidate_path.is_file()
        or candidate_path.is_symlink()
        or candidate_path.stat().st_size != completion.candidate_bytes
        or sha256_file(candidate_path) != completion.candidate_sha256
    ):
        raise IdentityMarketInventoryRunnerError("completion candidate manifest differs")
    candidate_document = _decode_canonical_json(candidate_path.read_bytes(), "inventory candidate")
    _validate_stored_candidate_document(
        candidate_document,
        completion,
        controls,
        current_source_refs=current_source_refs,
    )
    for role, relative, expected_sha, expected_bytes in (
        ("data", completion.data_path, completion.data_sha256, completion.data_bytes),
        ("qa", completion.qa_path, completion.qa_sha256, completion.qa_bytes),
        (
            "bounded_examples",
            completion.bounded_examples_path,
            completion.bounded_examples_sha256,
            completion.bounded_examples_bytes,
        ),
    ):
        artifact = _safe(root, relative)
        if (
            not artifact.is_file()
            or artifact.is_symlink()
            or artifact.stat().st_size != expected_bytes
            or sha256_file(artifact) != expected_sha
        ):
            raise IdentityMarketInventoryRunnerError(f"stored candidate {role} differs")
        monitor.check()
    diagnostics = _mapping(candidate_document.get("diagnostics"), "candidate diagnostics")
    _validate_stored_parquet(
        _safe(root, completion.data_path),
        completion,
        controls,
        diagnostics=diagnostics,
    )
    qa = _decode_canonical_json(_safe(root, completion.qa_path).read_bytes(), "candidate QA")
    examples = _decode_canonical_json(
        _safe(root, completion.bounded_examples_path).read_bytes(),
        "candidate bounded examples",
    )
    _validate_stored_examples_document(
        examples,
        completion,
        controls,
        current_source_refs,
        diagnostics=diagnostics,
    )
    _validate_stored_qa_document(
        qa,
        completion,
        controls,
        diagnostics=diagnostics,
    )
    monitor.check()
    # Deliberately no source Parquet rescan: exact controls and all stable output
    # bytes/hashes are revalidated, preserving the single-use completion policy.
    return completion


def _validate_completion_resources(
    completion: S7CompositeInventoryExecutionCompletion,
    caps: Any,
) -> None:
    expected_output_bytes = (
        completion.candidate_bytes
        + completion.data_bytes
        + completion.qa_bytes
        + completion.bounded_examples_bytes
        + len(completion.content)
    )
    if completion.output_bytes != expected_output_bytes:
        raise IdentityMarketInventoryRunnerError(
            "stored completion output-byte fixed point differs"
        )
    violations = (
        (completion.wall_clock_seconds > caps.wall_clock_seconds_cap, "wall clock"),
        (completion.peak_rss_bytes > caps.rss_bytes_cap, "RSS"),
        (completion.maximum_tmp_bytes > caps.tmp_bytes_cap, "tmp bytes"),
        (completion.output_bytes > caps.output_bytes_cap, "output bytes"),
        (
            completion.minimum_disk_free_bytes < caps.disk_free_floor_bytes,
            "disk floor",
        ),
    )
    for exceeded, label in violations:
        if exceeded:
            raise IdentityMarketInventoryRunnerError(
                f"stored completion exceeds resource cap: {label}"
            )
    expected_warning = completion.minimum_disk_free_bytes < caps.disk_free_warning_bytes
    if completion.disk_free_warning_triggered is not expected_warning:
        raise IdentityMarketInventoryRunnerError("stored completion disk warning marker differs")


def _validate_stored_parquet(
    path: Path,
    completion: S7CompositeInventoryExecutionCompletion,
    controls: _LoadedControls,
    *,
    diagnostics: Mapping[str, object],
) -> None:
    parquet = pq.ParquetFile(path)
    if (
        not parquet.schema_arrow.equals(COMPOSITE_FIGI_INVENTORY_CONTRACT.arrow_schema)
        or parquet.metadata.num_rows != completion.inventory_row_count
        or parquet.num_row_groups != (0 if completion.inventory_row_count == 0 else 1)
    ):
        raise IdentityMarketInventoryRunnerError("stored candidate Parquet metadata differs")
    rows = parquet.read(use_threads=False).to_pylist()
    keys = tuple(row["observed_composite_figi"] for row in rows)
    if keys != tuple(sorted(set(keys))):
        raise IdentityMarketInventoryRunnerError("stored candidate Parquet sort/key differs")
    total_observations = 0
    pair_count = 0
    conflict_count = 0
    for row in rows:
        composite = row["observed_composite_figi"]
        shares = row["observed_share_class_figis"]
        active = row["active_row_count"]
        inactive = row["inactive_row_count"]
        observation_count = active + inactive
        if (
            figi_invalid_reason(composite) is not None
            or not isinstance(shares, list)
            or shares != sorted(set(shares))
            or any(
                figi_invalid_reason(value, field="share_class_figi") is not None for value in shares
            )
            or row["share_class_conflict"] != (len(shares) > 1)
            or type(active) is not int
            or type(inactive) is not int
            or active < 0
            or inactive < 0
            or observation_count <= 0
            or row["first_session"] > row["last_session"]
            or row["first_session"] < controls.sessions[0]
            or row["last_session"] > controls.sessions[-1]
            or type(row["session_count"]) is not int
            or not 0 < row["session_count"] <= observation_count
            or type(row["ticker_count"]) is not int
            or not 0 < row["ticker_count"] <= observation_count
            or any(
                type(row[field]) is not int or not 0 <= row[field] <= observation_count
                for field in (
                    "provider_locale_count",
                    "provider_market_count",
                    "primary_exchange_count",
                )
            )
            or row["parent_table_count"] != 1
            or row["source_release_count"] != 1
            or _DIGEST.fullmatch(row["source_record_lineage_digest"]) is None
        ):
            raise IdentityMarketInventoryRunnerError("stored candidate Parquet row differs")
        total_observations += observation_count
        pair_count += len(shares)
        conflict_count += len(shares) > 1
    if (
        total_observations != diagnostics.get("valid_composite_row_count")
        or pair_count != diagnostics.get("distinct_composite_share_class_pair_count")
        or conflict_count != diagnostics.get("share_class_conflict_groups")
    ):
        raise IdentityMarketInventoryRunnerError("stored candidate Parquet totals differ")


def _validate_bounded_examples(
    value: object,
    *,
    per_reason_cap: int,
    diagnostics: Mapping[str, object] | None = None,
) -> None:
    if not isinstance(value, list):
        raise IdentityMarketInventoryRunnerError("bounded examples must be an array")
    allowed_reasons = {
        *COMPOSITE_FIGI_INVALID_REASON_PRECEDENCE,
        *SHARE_CLASS_FIGI_INVALID_REASON_PRECEDENCE,
    }
    reason_counts: dict[str, int] = {}
    precedence = (
        *COMPOSITE_FIGI_INVALID_REASON_PRECEDENCE,
        *SHARE_CLASS_FIGI_INVALID_REASON_PRECEDENCE,
    )
    rank = {reason: index for index, reason in enumerate(precedence)}
    last_rank = -1
    for item in value:
        if not isinstance(item, Mapping):
            raise IdentityMarketInventoryRunnerError("bounded example must be an object")
        reason = item.get("reason")
        if reason not in allowed_reasons:
            raise IdentityMarketInventoryRunnerError("bounded example reason is invalid")
        assert isinstance(reason, str)
        reason_counts[reason] = reason_counts.get(reason, 0) + 1
        if reason_counts[reason] > per_reason_cap:
            raise IdentityMarketInventoryRunnerError(
                "stored bounded examples exceed per-reason cap"
            )
        if rank[reason] < last_rank:
            raise IdentityMarketInventoryRunnerError(
                "stored bounded examples violate reason precedence"
            )
        last_rank = rank[reason]
    if diagnostics is None:
        return
    composite_counts = _mapping(
        diagnostics.get("invalid_composite_reason_counts"),
        "invalid Composite reason counts",
    )
    share_counts = _mapping(
        diagnostics.get("invalid_share_class_reason_counts"),
        "invalid Share Class reason counts",
    )
    expected_counts = {
        **{
            reason: _nonnegative_int(count, f"{reason} count")
            for reason, count in composite_counts.items()
        },
        **{
            reason: _nonnegative_int(count, f"{reason} count")
            for reason, count in share_counts.items()
        },
    }
    if set(expected_counts) - set(precedence):
        raise IdentityMarketInventoryRunnerError(
            "stored bounded example diagnostics contain an invalid reason"
        )
    for reason in precedence:
        if reason_counts.get(reason, 0) != min(expected_counts.get(reason, 0), per_reason_cap):
            raise IdentityMarketInventoryRunnerError(
                "stored bounded examples do not completely represent diagnostics"
            )


def _validate_stored_examples_document(
    document: Mapping[str, object],
    completion: S7CompositeInventoryExecutionCompletion,
    controls: _LoadedControls,
    current_source_refs: tuple[InventorySourceArtifactRef, ...],
    *,
    diagnostics: Mapping[str, object],
) -> None:
    _expect_keys(
        document,
        {
            "approval_id",
            "artifact_type",
            "bounded_example_cap",
            "examples",
            "plan_id",
            "schema_version",
        },
        "bounded examples document",
    )
    raw_examples = document.get("examples")
    if not isinstance(raw_examples, list):
        raise IdentityMarketInventoryRunnerError("bounded examples must be an array")
    source_lookup = {
        (item.path, item.sha256, item.session_date): item
        for item in current_source_refs
        if item.table == ASSET_AUTHORITY_TABLE
    }
    expected_keys = {
        "artifact_path",
        "artifact_sha256",
        "field",
        "observed_value",
        "provider_active",
        "reason",
        "row_group",
        "row_index_in_group",
        "session_date",
        "source_record_id",
        "table",
        "ticker",
    }
    last_locator_by_reason: dict[str, tuple[date, str, int, int]] = {}
    for raw in raw_examples:
        item = _mapping(raw, "bounded example")
        _expect_keys(item, expected_keys, "bounded example")
        field = _string(item.get("field"), "bounded example field")
        reason = _string(item.get("reason"), "bounded example reason")
        session = _parse_date(item.get("session_date"), "bounded example session")
        observed = item.get("observed_value")
        if observed is not None and not isinstance(observed, str):
            raise IdentityMarketInventoryRunnerError("bounded observed value is invalid")
        artifact_path = _string(item.get("artifact_path"), "bounded artifact path")
        row_group = _nonnegative_int(item.get("row_group"), "bounded row group")
        row_index = _nonnegative_int(item.get("row_index_in_group"), "bounded row index")
        locator = (session, artifact_path, row_group, row_index)
        if locator <= last_locator_by_reason.get(reason, (date.min, "", -1, -1)):
            raise IdentityMarketInventoryRunnerError(
                "stored bounded examples violate physical locator order"
            )
        last_locator_by_reason[reason] = locator
        expected_field = (
            "composite_figi" if reason.startswith("composite_figi_") else "share_class_figi"
        )
        if (
            item.get("table") != ASSET_AUTHORITY_TABLE
            or field != expected_field
            or figi_invalid_reason(observed, field=field) != reason
            or (
                artifact_path,
                _digest(item.get("artifact_sha256"), "bounded artifact SHA-256"),
                session,
            )
            not in source_lookup
            or type(item.get("provider_active")) is not bool
            or not _string(item.get("ticker"), "bounded ticker")
            or _DIGEST.fullmatch(_string(item.get("source_record_id"), "bounded source record ID"))
            is None
        ):
            raise IdentityMarketInventoryRunnerError("stored bounded example differs")
    _validate_bounded_examples(
        raw_examples,
        per_reason_cap=controls.plan.resource_caps.bounded_example_cap,
        diagnostics=diagnostics,
    )
    if (
        document.get("approval_id") != controls.approval.approval_id
        or document.get("plan_id") != controls.plan.plan_id
        or document.get("artifact_type") != "s7_composite_inventory_bounded_invalid_figi_examples"
        or document.get("schema_version") != EXAMPLE_ARTIFACT_SCHEMA_VERSION
        or document.get("bounded_example_cap") != controls.plan.resource_caps.bounded_example_cap
        or len(raw_examples)
        > controls.plan.resource_caps.bounded_example_cap
        * (
            len(COMPOSITE_FIGI_INVALID_REASON_PRECEDENCE)
            + len(SHARE_CLASS_FIGI_INVALID_REASON_PRECEDENCE)
        )
        or completion.bounded_examples_bytes <= 0
    ):
        raise IdentityMarketInventoryRunnerError("stored bounded examples cross controls")


def _validate_stored_qa_document(
    document: Mapping[str, object],
    completion: S7CompositeInventoryExecutionCompletion,
    controls: _LoadedControls,
    *,
    diagnostics: Mapping[str, object],
) -> None:
    _expect_keys(
        document,
        {
            "approval_id",
            "artifact_type",
            "bounded_examples",
            "critical_failure_count",
            "plan_id",
            "qa_semantics_digest",
            "results",
            "schema_version",
        },
        "candidate QA",
    )
    bounded_ref = InventoryOutputArtifactRef.from_dict(document.get("bounded_examples"))
    expected_ref = InventoryOutputArtifactRef(
        role="bounded_examples",
        path=EXAMPLES_FILENAME,
        sha256=completion.bounded_examples_sha256,
        bytes=completion.bounded_examples_bytes,
        media_type="application/json",
    )
    raw_results = document.get("results")
    if not isinstance(raw_results, list):
        raise IdentityMarketInventoryRunnerError("candidate QA results must be an array")
    numerators = _qa_numerators_from_diagnostics(diagnostics)
    if len(raw_results) != len(COMPOSITE_FIGI_INVENTORY_CONTRACT.qa_rules):
        raise IdentityMarketInventoryRunnerError("candidate QA result count differs")
    for raw, rule in zip(
        raw_results,
        COMPOSITE_FIGI_INVENTORY_CONTRACT.qa_rules,
        strict=True,
    ):
        result = _mapping(raw, "candidate QA result")
        _expect_keys(
            result,
            {
                "bounded_examples_path",
                "check_id",
                "denominator",
                "numerator",
                "severity",
                "status",
                "threshold",
            },
            "candidate QA result",
        )
        numerator = numerators.get(rule.check_id, 0)
        expected_path = (
            EXAMPLES_FILENAME
            if numerator and rule.check_id in _QA_CHECKS_WITH_FIGI_EXAMPLES
            else None
        )
        if result != {
            "bounded_examples_path": expected_path,
            "check_id": rule.check_id,
            "denominator": completion.authority_row_count,
            "numerator": numerator,
            "severity": rule.severity.value,
            "status": rule.expected_status(numerator=numerator, rate=None).value,
            "threshold": rule.threshold_expression,
        }:
            raise IdentityMarketInventoryRunnerError("candidate QA result differs")
    if (
        document.get("approval_id") != controls.approval.approval_id
        or document.get("plan_id") != controls.plan.plan_id
        or document.get("artifact_type") != "s7_composite_inventory_qa"
        or document.get("schema_version") != QA_ARTIFACT_SCHEMA_VERSION
        or document.get("qa_semantics_digest") != INVENTORY_QA_SEMANTICS_DIGEST
        or document.get("critical_failure_count") != 0
        or bounded_ref != expected_ref
    ):
        raise IdentityMarketInventoryRunnerError("stored candidate QA crosses controls")


def _validate_stored_candidate_document(
    document: Mapping[str, object],
    completion: S7CompositeInventoryExecutionCompletion,
    controls: _LoadedControls,
    *,
    current_source_refs: tuple[InventorySourceArtifactRef, ...],
) -> None:
    _expect_keys(
        document,
        {
            "algorithm_digest",
            "algorithm_rule_version",
            "approval_id",
            "approval_sha256",
            "artifact_type",
            "artifacts",
            "candidate_id",
            "candidate_rule_version",
            "candidate_state",
            "canonical_paths",
            "capabilities",
            "contract",
            "counts",
            "created_at_utc",
            "diagnostics",
            "input_binding_digest",
            "plan_id",
            "plan_sha256",
            "qa_semantics_digest",
            "request_event_id",
            "request_event_sha256",
            "resource_caps_digest",
            "runtime_file_set_digest",
            "schema_version",
            "source_artifact_set_digest",
            "source_artifacts",
            "verification_file_set_digest",
        },
        "candidate manifest",
    )
    contract = _mapping(document.get("contract"), "candidate contract")
    counts = _mapping(document.get("counts"), "candidate counts")
    capabilities = _mapping(document.get("capabilities"), "candidate capabilities")
    diagnostics = _mapping(document.get("diagnostics"), "candidate diagnostics")
    _expect_keys(
        contract,
        {"candidate_sha256", "contract_id", "schema_digest"},
        "candidate contract",
    )
    _expect_keys(
        counts,
        {
            "authority_row_count",
            "distinct_composite_share_class_pair_count",
            "inventory_row_count",
            "reconciliation_row_count",
            "session_count",
            "source_artifact_count",
            "source_bytes",
            "source_row_count",
            "valid_composite_row_count",
        },
        "candidate counts",
    )
    raw_sources = document.get("source_artifacts")
    raw_outputs = document.get("artifacts")
    if not isinstance(raw_sources, list) or not isinstance(raw_outputs, list):
        raise IdentityMarketInventoryRunnerError("candidate artifact refs must be arrays")
    sources = tuple(InventorySourceArtifactRef.from_dict(item) for item in raw_sources)
    outputs = tuple(
        sorted(
            (InventoryOutputArtifactRef.from_dict(item) for item in raw_outputs),
            key=lambda item: item.role,
        )
    )
    expected_outputs = tuple(
        sorted(
            (
                InventoryOutputArtifactRef(
                    role="data",
                    path=DATA_FILENAME,
                    sha256=completion.data_sha256,
                    bytes=completion.data_bytes,
                    media_type="application/vnd.apache.parquet",
                    row_count=completion.inventory_row_count,
                    schema_digest=COMPOSITE_FIGI_INVENTORY_SCHEMA_DIGEST,
                ),
                InventoryOutputArtifactRef(
                    role="qa",
                    path=QA_FILENAME,
                    sha256=completion.qa_sha256,
                    bytes=completion.qa_bytes,
                    media_type="application/json",
                ),
                InventoryOutputArtifactRef(
                    role="bounded_examples",
                    path=EXAMPLES_FILENAME,
                    sha256=completion.bounded_examples_sha256,
                    bytes=completion.bounded_examples_bytes,
                    media_type="application/json",
                ),
            ),
            key=lambda item: item.role,
        )
    )
    created_at = _parse_utc(document.get("created_at_utc"), "candidate created_at_utc")
    _validate_stored_diagnostics(diagnostics, counts, completion)
    if (
        document.get("candidate_id") != completion.candidate_id
        or stable_digest(_candidate_logical_from_document(document)) != completion.candidate_id
        or document.get("plan_id") != controls.plan.plan_id
        or document.get("plan_sha256") != controls.plan.sha256
        or document.get("approval_id") != controls.approval.approval_id
        or document.get("approval_sha256") != controls.approval.sha256
        or document.get("request_event_id") != controls.approval.request_event_id
        or document.get("request_event_sha256") != controls.approval.request_event_sha256
        or document.get("input_binding_digest") != controls.plan.input_binding_digest
        or document.get("resource_caps_digest") != controls.plan.resource_caps.digest
        or document.get("runtime_file_set_digest") != controls.plan.runtime_file_set_digest
        or document.get("verification_file_set_digest")
        != controls.plan.verification_file_set_digest
        or document.get("algorithm_digest") != INVENTORY_ALGORITHM_DIGEST
        or document.get("algorithm_rule_version") != INVENTORY_ALGORITHM_RULE_VERSION
        or document.get("qa_semantics_digest") != INVENTORY_QA_SEMANTICS_DIGEST
        or document.get("artifact_type") != "s7_composite_figi_inventory_candidate"
        or document.get("candidate_rule_version") != CANDIDATE_RULE_VERSION
        or document.get("schema_version") != CANDIDATE_SCHEMA_VERSION
        or document.get("candidate_state") != CANDIDATE_STATE
        or contract.get("contract_id") != COMPOSITE_FIGI_INVENTORY_CONTRACT_ID
        or contract.get("candidate_sha256") != COMPOSITE_FIGI_INVENTORY_RESOURCE_SHA256
        or contract.get("schema_digest") != COMPOSITE_FIGI_INVENTORY_SCHEMA_DIGEST
        or capabilities != _fail_closed_capabilities()
        or sources != current_source_refs
        or outputs != expected_outputs
        or counts.get("source_artifact_count") != completion.source_artifact_count
        or counts.get("source_row_count") != completion.source_row_count
        or counts.get("source_bytes") != completion.source_bytes
        or counts.get("authority_row_count") != completion.authority_row_count
        or counts.get("reconciliation_row_count") != completion.reconciliation_row_count
        or counts.get("session_count") != completion.session_count
        or counts.get("inventory_row_count") != completion.inventory_row_count
        or document.get("source_artifact_set_digest") != completion.source_artifact_set_digest
        or created_at < controls.approval.approved_at_utc
        or created_at > completion.completed_at_utc
    ):
        raise IdentityMarketInventoryRunnerError("stored candidate crosses controls")
    canonical_paths = _mapping(document.get("canonical_paths"), "candidate canonical paths")
    if canonical_paths != {
        "bounded_examples": completion.bounded_examples_path,
        "data": completion.data_path,
        "manifest": completion.candidate_path,
        "qa": completion.qa_path,
    }:
        raise IdentityMarketInventoryRunnerError("stored candidate paths differ")


def _validate_stored_diagnostics(
    diagnostics: Mapping[str, object],
    counts: Mapping[str, object],
    completion: S7CompositeInventoryExecutionCompletion,
) -> None:
    _expect_keys(
        diagnostics,
        {
            "authority_row_count",
            "authority_universe_row_count_difference",
            "completed_session_count",
            "distinct_composite_share_class_pair_count",
            "invalid_composite_figi_rows",
            "invalid_composite_reason_counts",
            "invalid_share_class_figi_rows",
            "invalid_share_class_reason_counts",
            "nonselected_authority_row_count",
            "reconciliation_row_count",
            "share_class_conflict_groups",
            "valid_composite_row_count",
        },
        "candidate diagnostics",
    )
    numerators = _qa_numerators_from_diagnostics(diagnostics)
    invalid_composite = _mapping(
        diagnostics.get("invalid_composite_reason_counts"),
        "invalid Composite reason counts",
    )
    invalid_share = _mapping(
        diagnostics.get("invalid_share_class_reason_counts"),
        "invalid Share Class reason counts",
    )
    invalid_composite_total = sum(
        _nonnegative_int(value, f"{reason} count") for reason, value in invalid_composite.items()
    )
    invalid_share_total = sum(
        _nonnegative_int(value, f"{reason} count") for reason, value in invalid_share.items()
    )
    authority = _nonnegative_int(
        diagnostics.get("authority_row_count"), "diagnostic authority rows"
    )
    reconciliation = _nonnegative_int(
        diagnostics.get("reconciliation_row_count"),
        "diagnostic reconciliation rows",
    )
    valid = _nonnegative_int(
        diagnostics.get("valid_composite_row_count"),
        "diagnostic valid Composite rows",
    )
    if (
        authority != completion.authority_row_count
        or reconciliation != completion.reconciliation_row_count
        or valid + invalid_composite_total != authority
        or diagnostics.get("invalid_composite_figi_rows") != invalid_composite_total
        or diagnostics.get("invalid_share_class_figi_rows") != invalid_share_total
        or diagnostics.get("authority_universe_row_count_difference") != authority - reconciliation
        or diagnostics.get("nonselected_authority_row_count") != authority - reconciliation
        or diagnostics.get("completed_session_count") != completion.session_count
        or diagnostics.get("distinct_composite_share_class_pair_count")
        != counts.get("distinct_composite_share_class_pair_count")
        or diagnostics.get("share_class_conflict_groups")
        != numerators["share_class_conflict_groups"]
        or counts.get("valid_composite_row_count") != valid
    ):
        raise IdentityMarketInventoryRunnerError("stored candidate diagnostics differ")


def _candidate_logical_from_document(document: Mapping[str, object]) -> dict[str, object]:
    value = dict(document)
    value.pop("candidate_id", None)
    value.pop("canonical_paths", None)
    return value


def _fail_if_partial_candidate_exists(root: Path, plan_id: str, approval_id: str) -> None:
    relative = "manifests/silver/identity/composite-inventory-candidates"
    parent = _safe(root, relative)
    if not parent.exists():
        return
    if not parent.is_dir() or parent.is_symlink():
        raise IdentityMarketInventoryRunnerError("candidate root is unsafe")
    for child in sorted(parent.iterdir(), key=lambda item: item.name):
        match = _CANDIDATE_DIRECTORY.fullmatch(child.name)
        if match is None or not child.is_dir() or child.is_symlink():
            raise IdentityMarketInventoryRunnerError("candidate root contains an unsafe entry")
        manifest = child / MANIFEST_FILENAME
        if not manifest.is_file() or manifest.is_symlink():
            raise IdentityMarketInventoryRunnerError(
                "candidate without completion/manifest requires manual review"
            )
        document = _decode_canonical_json(manifest.read_bytes(), "existing candidate")
        if document.get("plan_id") == plan_id and document.get("approval_id") == approval_id:
            raise IdentityMarketInventoryRunnerError(
                "candidate exists without completion; refusing implicit recovery"
            )


class _exclusive_nonblocking_lock:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.fd: int | None = None

    def __enter__(self) -> _exclusive_nonblocking_lock:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        flags = os.O_RDWR | os.O_CREAT
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        fd: int | None = None
        try:
            fd = os.open(self.path, flags, 0o600)
            opened = os.fstat(fd)
            if not stat.S_ISREG(opened.st_mode):
                raise IdentityMarketInventoryRunnerError("runner lock is not a regular file")
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            locked = os.fstat(fd)
            visible = os.stat(self.path, follow_symlinks=False)
            if not stat.S_ISREG(visible.st_mode) or (locked.st_dev, locked.st_ino) != (
                visible.st_dev,
                visible.st_ino,
            ):
                raise IdentityMarketInventoryRunnerError(
                    "runner lock path changed while the lock was acquired"
                )
        except BlockingIOError as exc:
            if fd is not None:
                os.close(fd)
            raise IdentityMarketInventoryRunnerError(
                "another exact inventory runner holds the nonblocking lock"
            ) from exc
        except IdentityMarketInventoryRunnerError:
            if fd is not None:
                os.close(fd)
            raise
        except OSError as exc:
            if fd is not None:
                os.close(fd)
            raise IdentityMarketInventoryRunnerError("runner lock cannot be acquired") from exc
        assert fd is not None
        self.fd = fd
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        if self.fd is not None:
            try:
                fcntl.flock(self.fd, fcntl.LOCK_UN)
            finally:
                os.close(self.fd)
                self.fd = None


def _validated_root(value: object) -> Path:
    if not isinstance(value, Path):
        raise IdentityMarketInventoryRunnerError("data_root must be a Path")
    expanded = value.expanduser()
    if expanded.is_symlink():
        raise IdentityMarketInventoryRunnerError("data_root cannot be a symlink")
    root = expanded.resolve()
    if not root.is_dir():
        raise IdentityMarketInventoryRunnerError("data_root must be an existing directory")
    return root


def _repository_root() -> Path:
    current = Path(__file__).resolve()
    for parent in current.parents:
        if (parent / ".git").exists() and (parent / "pyproject.toml").is_file():
            return parent
    raise IdentityMarketInventoryRunnerError("approved Git repository cannot be located")


def _git(root: Path, *arguments: str) -> str:
    try:
        completed = subprocess.run(
            ("git", "-C", str(root), *arguments),
            capture_output=True,
            check=True,
            text=True,
            timeout=30,
        )
    except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        raise IdentityMarketInventoryRunnerError(
            f"Git verification failed: {' '.join(arguments)}"
        ) from exc
    return completed.stdout.strip()


def _repo_path(root: Path, relative: str) -> Path:
    _relative_path(relative, "repository path")
    path = Path(os.path.abspath(root / relative))
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise IdentityMarketInventoryRunnerError("repository path escaped root") from exc
    for parent in (path, *path.parents):
        if parent == root.parent:
            break
        if parent.is_symlink():
            raise IdentityMarketInventoryRunnerError("repository pin traverses a symlink")
        if parent == root:
            break
    return path


def _safe(root: Path, relative: str) -> Path:
    try:
        return safe_relative_path(root, relative)
    except ArtifactError as exc:
        raise IdentityMarketInventoryRunnerError(str(exc)) from exc


def _relative_path(value: object, label: str) -> str:
    text = _string(value, label)
    path = Path(text)
    if not text or path.is_absolute() or ".." in path.parts or path.as_posix() != text:
        raise IdentityMarketInventoryRunnerError(f"{label} is not a safe relative path")
    return text


def _canonical_bytes(value: Mapping[str, object]) -> bytes:
    return (
        json.dumps(
            dict(value),
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        + b"\n"
    )


def _decode_canonical_json(content: bytes, label: str) -> dict[str, object]:
    def reject_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise IdentityMarketInventoryRunnerError(f"{label} has duplicate JSON keys")
            result[key] = value
        return result

    try:
        document = json.loads(content, object_pairs_hook=reject_duplicates)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise IdentityMarketInventoryRunnerError(f"{label} is not valid JSON") from exc
    if not isinstance(document, dict) or _canonical_bytes(document) != content:
        raise IdentityMarketInventoryRunnerError(f"{label} is not canonical JSON")
    return document


def _mapping(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise IdentityMarketInventoryRunnerError(f"{label} must be an object")
    return dict(value)


def _expect_keys(value: Mapping[str, object], expected: set[str], label: str) -> None:
    if set(value) != expected:
        raise IdentityMarketInventoryRunnerError(f"{label} schema is not exact")


def _string(value: object, label: str) -> str:
    if not isinstance(value, str):
        raise IdentityMarketInventoryRunnerError(f"{label} must be text")
    return value


def _digest(value: object, label: str) -> str:
    text = _string(value, label)
    if _DIGEST.fullmatch(text) is None:
        raise IdentityMarketInventoryRunnerError(f"{label} must be lowercase SHA-256")
    return text


def _nonnegative_int(value: object, label: str) -> int:
    if type(value) is not int or value < 0:
        raise IdentityMarketInventoryRunnerError(f"{label} must be a nonnegative integer")
    return value


def _float(value: object, label: str) -> float:
    if type(value) not in {float, int}:
        raise IdentityMarketInventoryRunnerError(f"{label} must be numeric")
    result = float(value)
    if result < 0 or not result < float("inf"):
        raise IdentityMarketInventoryRunnerError(f"{label} must be finite and nonnegative")
    return result


def _parse_date(value: object, label: str) -> date:
    text = _string(value, label)
    try:
        parsed = date.fromisoformat(text)
    except ValueError as exc:
        raise IdentityMarketInventoryRunnerError(f"{label} is not ISO date") from exc
    if parsed.isoformat() != text:
        raise IdentityMarketInventoryRunnerError(f"{label} is not canonical")
    return parsed


def _utc(value: object, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise IdentityMarketInventoryRunnerError(f"{label} must be timezone-aware")
    normalized = value.astimezone(UTC)
    if value.utcoffset().total_seconds() != 0:
        raise IdentityMarketInventoryRunnerError(f"{label} must be UTC")
    return normalized


def _utc_text(value: datetime) -> str:
    return _utc(value, "UTC datetime").isoformat()


def _parse_utc(value: object, label: str) -> datetime:
    text = _string(value, label)
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise IdentityMarketInventoryRunnerError(f"{label} is not ISO-8601") from exc
    normalized = _utc(parsed, label)
    if normalized.isoformat() != text:
        raise IdentityMarketInventoryRunnerError(f"{label} is not canonical UTC")
    return normalized


def _fail_closed_capabilities() -> dict[str, bool]:
    return {
        "backtest_identity_eligible": False,
        "canonical_identity_present": False,
        "identity_adjudication_present": False,
        "market_classification_present": False,
        "materialization_authorized": False,
        "publication_authorized": False,
        "registry_release_authorized": False,
    }


def _directory_identity(path: Path, label: str) -> _InodeIdentity:
    try:
        details = path.stat(follow_symlinks=False)
    except OSError as exc:
        raise IdentityMarketInventoryRunnerError(f"{label} is missing") from exc
    if not stat.S_ISDIR(details.st_mode):
        raise IdentityMarketInventoryRunnerError(f"{label} is not a safe directory")
    return _InodeIdentity(device=details.st_dev, inode=details.st_ino)


def _regular_file_identity(path: Path, label: str) -> _InodeIdentity:
    try:
        details = path.stat(follow_symlinks=False)
    except OSError as exc:
        raise IdentityMarketInventoryRunnerError(f"{label} is missing") from exc
    if not stat.S_ISREG(details.st_mode):
        raise IdentityMarketInventoryRunnerError(f"{label} is not a safe regular file")
    return _InodeIdentity(device=details.st_dev, inode=details.st_ino)


def _require_directory_identity(
    path: Path,
    expected: _InodeIdentity,
    label: str,
) -> None:
    if _directory_identity(path, label) != expected:
        raise IdentityMarketInventoryRunnerError(f"{label} inode identity differs")


def _require_regular_file_identity(
    path: Path,
    expected: _InodeIdentity,
    label: str,
) -> None:
    if _regular_file_identity(path, label) != expected:
        raise IdentityMarketInventoryRunnerError(f"{label} inode identity differs")


def _write_parquet_exclusive(table: pa.Table, path: Path) -> None:
    """Create candidate Parquet with O_EXCL semantics and fixed writer options."""

    if path.exists() or path.is_symlink():
        raise IdentityMarketInventoryRunnerError("candidate DATA staging target already exists")
    try:
        with path.open("xb") as handle:
            pq.write_table(
                table,
                handle,
                compression="zstd",
                compression_level=9,
                data_page_version="2.0",
                row_group_size=100_000,
                store_schema=True,
                use_dictionary=False,
                version="2.6",
                write_statistics=True,
            )
            handle.flush()
            os.fchmod(handle.fileno(), 0o444)
            os.fsync(handle.fileno())
        _fsync_directory(path.parent)
    except (OSError, pa.ArrowException) as exc:
        raise IdentityMarketInventoryRunnerError("candidate DATA staging failed") from exc


def _write_staged_bytes(path: Path, content: bytes) -> None:
    if path.exists() or path.is_symlink():
        raise IdentityMarketInventoryRunnerError("staging target already exists")
    try:
        with path.open("xb") as handle:
            handle.write(content)
            handle.flush()
            os.fchmod(handle.fileno(), 0o444)
            os.fsync(handle.fileno())
        _fsync_directory(path.parent)
    except OSError as exc:
        raise IdentityMarketInventoryRunnerError("staged bytes cannot be written") from exc


def _publish_completion_link(
    root: Path,
    staged: Path,
    target: Path,
    *,
    expected_sha256: str,
    expected_identity: _InodeIdentity,
) -> dict[str, object]:
    """Publish only this run's staged inode; never adopt or move a conflict."""

    _digest(expected_sha256, "completion expected SHA-256")
    _require_regular_file_identity(staged, expected_identity, "staged completion")
    if sha256_file(staged) != expected_sha256:
        raise IdentityMarketInventoryRunnerError("staged completion differs")
    expected_bytes = staged.stat().st_size
    relative_target = str(target.relative_to(root))
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists() or target.is_symlink():
        raise IdentityMarketInventoryRunnerError("completion immutable slot conflicts")
    try:
        os.link(staged, target, follow_symlinks=False)
    except FileExistsError as exc:
        raise IdentityMarketInventoryRunnerError("completion immutable slot conflicts") from exc
    try:
        _fsync_directory(target.parent)
    except OSError:
        directory_fsync_confirmed = False
    else:
        directory_fsync_confirmed = True
    return {
        "bytes": expected_bytes,
        "directory_fsync_confirmed": directory_fsync_confirmed,
        "path": relative_target,
        "sha256": expected_sha256,
    }


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _tree_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    total = 0
    for current, directories, files in os.walk(path, followlinks=False):
        current_path = Path(current)
        for name in directories:
            if (current_path / name).is_symlink():
                raise IdentityMarketInventoryRunnerError("staging contains a symlink")
        for name in files:
            item = current_path / name
            if item.is_symlink() or not item.is_file():
                raise IdentityMarketInventoryRunnerError("staging contains an unsafe file")
            total += item.stat().st_size
    return total


def _peak_rss_bytes() -> int:
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # macOS reports bytes; Linux and the common BSD/POSIX implementations used
    # by the remote deployment report KiB.
    if sys.platform == "darwin":
        return int(value)
    return int(value) * 1024


__all__ = [
    "CANDIDATE_STATE",
    "IdentityMarketInventoryRunnerError",
    "InventoryOutputArtifactRef",
    "InventorySourceArtifactRef",
    "S7CompositeInventoryCandidate",
    "S7CompositeInventoryExecutionCompletion",
    "composite_inventory_completion_path",
    "run_s7_composite_inventory",
    "run_source_bound_composite_inventory",
]
