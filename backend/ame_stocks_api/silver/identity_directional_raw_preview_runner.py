"""Exact, review-only runner for the S7 directional raw preview.

The public runner accepts only an immutable execution Plan and Approval ID/SHA
pair.  Tickers, dates, paths and rows are never caller supplied.  It scans the
twenty-two Plan-bound S4 artifacts, preserves every exact-match authority row,
attests the full physical source rows with ``ProviderRowAttestation`` v2 and
stops at an immutable ``awaiting_review`` candidate.

This module has no registry, adjudication, canonical identity, tradability,
forced-liquidation, Full-run or publication capability.  The preparation
approval is lineage only and can never authorize this entry point.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import math
import os
import re
import resource
import shutil
import stat
import subprocess
import sys
import time
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from itertools import pairwise
from pathlib import Path
from types import MappingProxyType
from typing import Any, Final

import pyarrow as pa
import pyarrow.parquet as pq

from ame_stocks_api.artifacts import safe_relative_path, sha256_file, stable_digest
from ame_stocks_api.silver.calendar_artifact import XNYSCalendarArtifact
from ame_stocks_api.silver.contracts import QAStatus
from ame_stocks_api.silver.identity_directional_raw_preview_contract import (
    DIRECTIONAL_RAW_PREVIEW_FIXED_CASE_ANCHORS,
    DIRECTIONAL_RAW_PREVIEW_FIXED_CASE_SESSIONS,
    DIRECTIONAL_RAW_PREVIEW_FIXED_PAIR_COUNT,
    DIRECTIONAL_RAW_PREVIEW_INTERVAL_INFERENCE_STATE,
    DIRECTIONAL_RAW_PREVIEW_PROVIDER_ID,
    DIRECTIONAL_RAW_PREVIEW_PROVIDER_LOCALE,
    DIRECTIONAL_RAW_PREVIEW_PROVIDER_MARKET,
    DIRECTIONAL_RAW_PREVIEW_REGISTRY_EVALUATION_STATE,
    DIRECTIONAL_RAW_PREVIEW_REGISTRY_EXCLUSIVITY_SEMANTICS_DIGEST,
    IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_CONTRACT,
    IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_CONTRACT_ID,
    IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_RESOURCE_SHA256,
    IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_SCHEMA_DIGEST,
)
from ame_stocks_api.silver.identity_market_inventory_engine import (
    UNIVERSE_PARENT_PROJECTION,
)
from ame_stocks_api.silver.identity_provider_evidence import (
    PROVIDER_ROW_ATTESTATION_SCHEMA_VERSION,
    DirectionalPreviewEvidenceAuthority,
    ProviderEvidenceError,
    ProviderRowAttestation,
    attest_directional_preview_provider_rows,
    issue_directional_preview_evidence_authority,
    replay_provider_row_attestations_from_official_bundle,
)
from ame_stocks_api.silver.identity_source import (
    IdentitySourceArtifact,
    IdentitySourceBatch,
    IdentitySourceBundle,
    IdentitySourceError,
)

RUNNER_RULE_VERSION: Final = "s7_directional_raw_preview_source_bound_runner_v1"
CANDIDATE_RULE_VERSION: Final = "s7_directional_raw_preview_candidate_v1"
CASE_EVIDENCE_RULE_VERSION: Final = "s7_directional_raw_preview_case_evidence_v1"
DIRECTIONAL_REVIEW_RULE_VERSION: Final = "s7_directional_raw_preview_sequence_v1"
COMPLETION_RULE_VERSION: Final = "s7_directional_raw_preview_completion_v1"
CANDIDATE_STATE: Final = "awaiting_review"

SLOTS_FILENAME: Final = "data/review-slots.parquet"
DIRECTIONAL_FILENAME: Final = "review/directional-sequences.json"
QA_FILENAME: Final = "qa/qa.json"
EXAMPLES_FILENAME: Final = "examples/review-anomalies.json"
MANIFEST_FILENAME: Final = "manifest.json"
EVIDENCE_DIRECTORY: Final = "evidence"
RESOURCE_MEASUREMENT_CUTOFF: Final = "pre_commit_measurement_cutoff"

ASSET_TABLE: Final = "asset_observation_daily"
UNIVERSE_TABLE: Final = "universe_source_daily"
SOURCE_TABLES: Final = (ASSET_TABLE, UNIVERSE_TABLE)

_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_FORBIDDEN_OUTPUT_KEYS: Final = frozenset(
    {
        "asset_id",
        "canonical_cik",
        "canonical_composite_figi",
        "canonical_share_class_figi",
        "disposition",
        "effective_from",
        "effective_to",
        "forced_liquidation",
        "issuer_id",
        "tradability_eligible",
    }
)
_CRITICAL_QA_IDS: Final = frozenset(
    rule.check_id
    for rule in IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_CONTRACT.qa_rules
    if rule.severity.value == "critical"
)
_HIGH_QA_IDS: Final = frozenset(
    rule.check_id
    for rule in IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_CONTRACT.qa_rules
    if rule.severity.value == "high"
)
_RUNNER_VERIFIED_CRITICAL_QA_IDS: Final = frozenset(
    {
        "inventory_binding_invalid",
        "s4_source_binding_invalid",
        "source_artifact_integrity_invalid",
        "source_scan_count_mismatch",
        "scoped_source_omission_rows",
        "provider_row_attestation_schema_invalid_rows",
        "row_attestation_replay_invalid_rows",
        "output_artifact_readback_invalid",
        "resource_cap_exceeded",
    }
)
_ENGINE_CRITICAL_QA_IDS: Final = _CRITICAL_QA_IDS - _RUNNER_VERIFIED_CRITICAL_QA_IDS


def _runner_verified_critical_numerators(
    *,
    inventory_binding_verified: bool,
    s4_source_binding_verified: bool,
    source_artifact_integrity_verified: bool,
    source_scan_count_verified: bool,
    scoped_source_complete: bool,
    provider_attestation_schema_verified: bool,
    attestation_replay_verified: bool,
    output_readback_required_before_publish: bool,
    resource_caps_verified: bool,
) -> dict[str, int]:
    proofs = {
        "inventory_binding_invalid": inventory_binding_verified,
        "s4_source_binding_invalid": s4_source_binding_verified,
        "source_artifact_integrity_invalid": source_artifact_integrity_verified,
        "source_scan_count_mismatch": source_scan_count_verified,
        "scoped_source_omission_rows": scoped_source_complete,
        "provider_row_attestation_schema_invalid_rows": (
            provider_attestation_schema_verified
        ),
        "row_attestation_replay_invalid_rows": attestation_replay_verified,
        "output_artifact_readback_invalid": output_readback_required_before_publish,
        "resource_cap_exceeded": resource_caps_verified,
    }
    if set(proofs) != _RUNNER_VERIFIED_CRITICAL_QA_IDS or any(
        value is not True for value in proofs.values()
    ):
        raise IdentityDirectionalRawPreviewRunnerError(
            "runner critical QA proof set is incomplete"
        )
    return {check_id: 0 for check_id in proofs}


class IdentityDirectionalRawPreviewRunnerError(RuntimeError):
    """Raised before a trustworthy review-only completion can be committed."""


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


def _utc_text(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise IdentityDirectionalRawPreviewRunnerError("timestamp must be timezone-aware")
    return value.astimezone(UTC).isoformat()


def _digest(value: object, label: str) -> str:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise IdentityDirectionalRawPreviewRunnerError(f"{label} must be lowercase 64-hex")
    return value


def _nonnegative(value: object, label: str) -> int:
    if type(value) is not int or value < 0:
        raise IdentityDirectionalRawPreviewRunnerError(
            f"{label} must be a nonnegative native int"
        )
    return value


def _relative_path(value: object, label: str) -> str:
    if not isinstance(value, str):
        raise IdentityDirectionalRawPreviewRunnerError(f"{label} must be text")
    path = Path(value)
    if not value or path.is_absolute() or ".." in path.parts or path.as_posix() != value:
        raise IdentityDirectionalRawPreviewRunnerError(f"{label} is not a safe relative path")
    return value


def _fail_closed_capabilities() -> dict[str, bool]:
    return {
        "adjudication": False,
        "canonical_identity_materialization": False,
        "exact_group_history_read": False,
        "external_evidence_capture": False,
        "forced_liquidation": False,
        "full_run": False,
        "network_access": False,
        "publication": False,
        "registry_evaluation": False,
        "tradability_decision": False,
    }


def _fixed_cases() -> tuple[dict[str, object], ...]:
    cases: list[dict[str, object]] = []
    for ticker, sessions in DIRECTIONAL_RAW_PREVIEW_FIXED_CASE_SESSIONS:
        subject = {
            "inventory_anchor_composite_figi": DIRECTIONAL_RAW_PREVIEW_FIXED_CASE_ANCHORS[
                ticker
            ],
            "provider_id": DIRECTIONAL_RAW_PREVIEW_PROVIDER_ID,
            "provider_locale": DIRECTIONAL_RAW_PREVIEW_PROVIDER_LOCALE,
            "provider_market": DIRECTIONAL_RAW_PREVIEW_PROVIDER_MARKET,
            "sessions": [item.isoformat() for item in sessions],
            "ticker": ticker,
        }
        cases.append({**subject, "review_case_id": stable_digest(subject)})
    return tuple(cases)


FIXED_CASES: Final = _fixed_cases()
FIXED_PAIR_TO_CASE: Final = MappingProxyType(
    {
        (str(case["ticker"]), date.fromisoformat(session)): case
        for case in FIXED_CASES
        for session in case["sessions"]
    }
)
_FIXED_EVIDENCE_PATHS: Final = MappingProxyType(
    {
        str(case["ticker"]): (
            f"{EVIDENCE_DIRECTORY}/review_case_id={case['review_case_id']}/manifest.json"
        )
        for case in FIXED_CASES
    }
)
_FIXED_OUTPUT_ROLE_SPECS: Final = MappingProxyType(
    {
        "bounded_examples": (EXAMPLES_FILENAME, "application/json", None),
        "directional_review": (DIRECTIONAL_FILENAME, "application/json", None),
        "qa": (QA_FILENAME, "application/json", None),
        "review_slots": (
            SLOTS_FILENAME,
            "application/vnd.apache.parquet",
            DIRECTIONAL_RAW_PREVIEW_FIXED_PAIR_COUNT,
        ),
        **{
            f"case_evidence:{ticker}": (path, "application/json", None)
            for ticker, path in _FIXED_EVIDENCE_PATHS.items()
        },
    }
)


@dataclass(frozen=True, slots=True, order=True)
class DirectionalSourceArtifactRef:
    table: str
    session_date: date
    release_id: str
    release_manifest_sha256: str
    path: str
    sha256: str
    bytes: int
    row_count: int
    source_contract_id: str
    schema_digest: str

    def __post_init__(self) -> None:
        if self.table not in SOURCE_TABLES or type(self.session_date) is not date:
            raise IdentityDirectionalRawPreviewRunnerError("source artifact scope is invalid")
        for label, value in (
            ("release ID", self.release_id),
            ("release manifest SHA", self.release_manifest_sha256),
            ("artifact SHA", self.sha256),
            ("source contract ID", self.source_contract_id),
            ("schema digest", self.schema_digest),
        ):
            _digest(value, label)
        _relative_path(self.path, "source artifact path")
        _nonnegative(self.bytes, "source artifact bytes")
        _nonnegative(self.row_count, "source artifact rows")

    @classmethod
    def from_plan_pin(cls, value: object) -> DirectionalSourceArtifactRef:
        try:
            return cls(
                table=value.table,
                session_date=date.fromisoformat(value.session_date),
                release_id=value.release_id,
                release_manifest_sha256=value.release_manifest_sha256,
                path=value.path,
                sha256=value.sha256,
                bytes=value.bytes,
                row_count=value.row_count,
                source_contract_id=value.source_contract_id,
                schema_digest=value.schema_digest,
            )
        except (AttributeError, TypeError, ValueError) as exc:
            raise IdentityDirectionalRawPreviewRunnerError(
                "execution source pin has the wrong shape"
            ) from exc

    @classmethod
    def from_dict(cls, value: object) -> DirectionalSourceArtifactRef:
        if not isinstance(value, Mapping):
            raise IdentityDirectionalRawPreviewRunnerError(
                "source artifact ref must be an object"
            )
        item = dict(value)
        expected = {
            "bytes",
            "path",
            "release_id",
            "release_manifest_sha256",
            "row_count",
            "schema_digest",
            "session_date",
            "sha256",
            "source_contract_id",
            "table",
        }
        if set(item) != expected:
            raise IdentityDirectionalRawPreviewRunnerError(
                "source artifact ref schema is not exact"
            )
        try:
            return cls(
                table=str(item["table"]),
                session_date=date.fromisoformat(str(item["session_date"])),
                release_id=str(item["release_id"]),
                release_manifest_sha256=str(item["release_manifest_sha256"]),
                path=str(item["path"]),
                sha256=str(item["sha256"]),
                bytes=int(item["bytes"]),
                row_count=int(item["row_count"]),
                source_contract_id=str(item["source_contract_id"]),
                schema_digest=str(item["schema_digest"]),
            )
        except (TypeError, ValueError) as exc:
            raise IdentityDirectionalRawPreviewRunnerError(
                "source artifact ref is malformed"
            ) from exc

    def to_dict(self) -> dict[str, object]:
        return {
            "bytes": self.bytes,
            "path": self.path,
            "release_id": self.release_id,
            "release_manifest_sha256": self.release_manifest_sha256,
            "row_count": self.row_count,
            "schema_digest": self.schema_digest,
            "session_date": self.session_date.isoformat(),
            "sha256": self.sha256,
            "source_contract_id": self.source_contract_id,
            "table": self.table,
        }


@dataclass(frozen=True, slots=True)
class DirectionalCaseEvidenceManifest:
    plan_id: str
    plan_sha256: str
    approval_id: str
    approval_sha256: str
    scope_set_id: str
    review_case: Mapping[str, object]
    source_artifacts: tuple[DirectionalSourceArtifactRef, ...]
    row_attestations: tuple[ProviderRowAttestation, ...]
    created_at_utc: datetime

    def __post_init__(self) -> None:
        for label, value in (
            ("plan ID", self.plan_id),
            ("plan SHA", self.plan_sha256),
            ("approval ID", self.approval_id),
            ("approval SHA", self.approval_sha256),
            ("scope-set ID", self.scope_set_id),
        ):
            _digest(value, label)
        case = dict(self.review_case)
        if case not in FIXED_CASES:
            raise IdentityDirectionalRawPreviewRunnerError("case evidence scope changed")
        object.__setattr__(self, "review_case", MappingProxyType(case))
        sources = tuple(sorted(self.source_artifacts))
        expected_sessions = set(case["sessions"])
        expected_source_pairs = {
            (table, session) for table in SOURCE_TABLES for session in expected_sessions
        }
        actual_source_pairs = {
            (item.table, item.session_date.isoformat()) for item in sources
        }
        if (
            actual_source_pairs != expected_source_pairs
            or len(sources) != len(expected_source_pairs)
            or len({item.path for item in sources}) != len(sources)
        ):
            raise IdentityDirectionalRawPreviewRunnerError(
                "case evidence does not bind both source artifacts for every session"
            )
        object.__setattr__(self, "source_artifacts", sources)
        attestations = tuple(sorted(self.row_attestations, key=lambda item: item.locator))
        if len({item.row_attestation_id for item in attestations}) != len(attestations):
            raise IdentityDirectionalRawPreviewRunnerError("case evidence repeats attestation")
        if len({item.locator for item in attestations}) != len(attestations):
            raise IdentityDirectionalRawPreviewRunnerError("case evidence repeats locator")
        allowed = {
            (str(case["ticker"]), date.fromisoformat(session)) for session in case["sessions"]
        }
        for item in attestations:
            row = item.full_row_snapshot
            if (row.get("ticker"), date.fromisoformat(str(row.get("session_date")))) not in allowed:
                raise IdentityDirectionalRawPreviewRunnerError(
                    "case evidence attestation is outside exact pair scope"
                )
            source = next(
                (
                    candidate
                    for candidate in sources
                    if candidate.table == item.dataset
                    and candidate.session_date.isoformat() == row.get("session_date")
                ),
                None,
            )
            if (
                source is None
                or item.release_id != source.release_id
                or item.release_manifest_sha256 != source.release_manifest_sha256
                or item.silver_artifact_path != source.path
                or item.silver_artifact_sha256 != source.sha256
                or item.contract_id != source.source_contract_id
                or item.arrow_schema_digest != source.schema_digest
            ):
                raise IdentityDirectionalRawPreviewRunnerError(
                    "case evidence attestation is outside its exact Plan source ref"
                )
        object.__setattr__(self, "row_attestations", attestations)
        _utc_text(self.created_at_utc)

    def logical_payload(self) -> dict[str, object]:
        return {
            "approval_id": self.approval_id,
            "approval_sha256": self.approval_sha256,
            "artifact_type": "s7_directional_raw_preview_case_evidence_manifest",
            "capabilities": _fail_closed_capabilities(),
            "created_at_utc": _utc_text(self.created_at_utc),
            "manifest_rule_version": CASE_EVIDENCE_RULE_VERSION,
            "plan_id": self.plan_id,
            "plan_sha256": self.plan_sha256,
            "provider_row_attestation_schema_version": (
                PROVIDER_ROW_ATTESTATION_SCHEMA_VERSION
            ),
            "review_case": dict(self.review_case),
            "row_attestation_set_digest": stable_digest(
                [item.row_attestation_id for item in self.row_attestations]
            ),
            "row_attestations": [item.to_dict() for item in self.row_attestations],
            "scope_set_id": self.scope_set_id,
            "source_artifact_set_digest": stable_digest(
                [item.to_dict() for item in self.source_artifacts]
            ),
            "source_artifacts": [item.to_dict() for item in self.source_artifacts],
        }

    @property
    def manifest_id(self) -> str:
        return stable_digest(self.logical_payload())

    @property
    def content(self) -> bytes:
        return _canonical_bytes({**self.logical_payload(), "manifest_id": self.manifest_id})

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.content).hexdigest()

    @property
    def candidate_relative_path(self) -> str:
        return (
            f"{EVIDENCE_DIRECTORY}/review_case_id="
            f"{self.review_case['review_case_id']}/manifest.json"
        )

    @classmethod
    def from_dict(cls, value: object) -> DirectionalCaseEvidenceManifest:
        if not isinstance(value, Mapping):
            raise IdentityDirectionalRawPreviewRunnerError(
                "case evidence manifest must be an object"
            )
        document = dict(value)
        expected = {
            "approval_id",
            "approval_sha256",
            "artifact_type",
            "capabilities",
            "created_at_utc",
            "manifest_id",
            "manifest_rule_version",
            "plan_id",
            "plan_sha256",
            "provider_row_attestation_schema_version",
            "review_case",
            "row_attestation_set_digest",
            "row_attestations",
            "scope_set_id",
            "source_artifact_set_digest",
            "source_artifacts",
        }
        if set(document) != expected:
            raise IdentityDirectionalRawPreviewRunnerError(
                "case evidence manifest schema is not exact"
            )
        try:
            manifest = cls(
                plan_id=str(document["plan_id"]),
                plan_sha256=str(document["plan_sha256"]),
                approval_id=str(document["approval_id"]),
                approval_sha256=str(document["approval_sha256"]),
                scope_set_id=str(document["scope_set_id"]),
                review_case=dict(document["review_case"]),
                source_artifacts=tuple(
                    DirectionalSourceArtifactRef.from_dict(item)
                    for item in document["source_artifacts"]
                ),
                row_attestations=tuple(
                    ProviderRowAttestation.from_dict(item)
                    for item in document["row_attestations"]
                ),
                created_at_utc=datetime.fromisoformat(str(document["created_at_utc"])),
            )
        except (KeyError, TypeError, ValueError, ProviderEvidenceError) as exc:
            raise IdentityDirectionalRawPreviewRunnerError(
                "case evidence manifest is malformed"
            ) from exc
        if _canonical_bytes(document) != manifest.content:
            raise IdentityDirectionalRawPreviewRunnerError(
                "case evidence canonical bytes do not reproduce"
            )
        return manifest


@dataclass(frozen=True, slots=True)
class DirectionalPreviewBuild:
    slots: tuple[Mapping[str, object], ...]
    evidence_manifests: tuple[DirectionalCaseEvidenceManifest, ...]
    directional_review: Mapping[str, object]
    qa: Mapping[str, object]
    examples: Mapping[str, object]


class DirectionalRawPreviewEngine:
    """Bounded in-memory collector for only the eleven exact review pairs."""

    def __init__(
        self,
        *,
        selected_asset_row_cap: int = 128,
        selected_universe_row_cap: int = 11,
        selected_total_row_cap: int = 139,
        bounded_example_cap: int = 20,
    ) -> None:
        for label, value in (
            ("selected asset row cap", selected_asset_row_cap),
            ("selected universe row cap", selected_universe_row_cap),
            ("selected total row cap", selected_total_row_cap),
        ):
            if type(value) is not int or value <= 0:
                raise IdentityDirectionalRawPreviewRunnerError(f"{label} is invalid")
        if type(bounded_example_cap) is not int or bounded_example_cap <= 0:
            raise IdentityDirectionalRawPreviewRunnerError("example cap is invalid")
        self._selected_asset_row_cap = selected_asset_row_cap
        self._selected_universe_row_cap = selected_universe_row_cap
        self._selected_total_row_cap = selected_total_row_cap
        self._bounded_example_cap = bounded_example_cap
        self._by_pair: dict[tuple[str, date], dict[str, list[ProviderRowAttestation]]] = {
            pair: {ASSET_TABLE: [], UNIVERSE_TABLE: []} for pair in FIXED_PAIR_TO_CASE
        }
        self._seen_attestation_ids: set[str] = set()
        self._seen_locators: set[tuple[str, str, str, int, int]] = set()
        self._sealed = False

    @property
    def retained_attestations(self) -> tuple[ProviderRowAttestation, ...]:
        return tuple(
            sorted(
                (
                    item
                    for pair in self._by_pair.values()
                    for table in SOURCE_TABLES
                    for item in pair[table]
                ),
                key=lambda item: item.locator,
            )
        )

    def consume_attestations(self, values: Sequence[ProviderRowAttestation]) -> None:
        """Consume already physically attested rows (also used by local fixtures)."""

        if self._sealed:
            raise IdentityDirectionalRawPreviewRunnerError("engine is already sealed")
        for item in values:
            if not isinstance(item, ProviderRowAttestation) or item.dataset not in SOURCE_TABLES:
                raise IdentityDirectionalRawPreviewRunnerError("source attestation is invalid")
            row = item.full_row_snapshot
            try:
                pair = (str(row["ticker"]), date.fromisoformat(str(row["session_date"])))
            except (KeyError, TypeError, ValueError) as exc:
                raise IdentityDirectionalRawPreviewRunnerError(
                    "attested row pair is malformed"
                ) from exc
            if pair not in FIXED_PAIR_TO_CASE:
                raise IdentityDirectionalRawPreviewRunnerError(
                    "exact_pair_scope_leakage_rows: attestation outside fixed scope"
                )
            if item.row_attestation_id in self._seen_attestation_ids:
                raise IdentityDirectionalRawPreviewRunnerError(
                    "orphan_or_duplicate_attestation_rows: repeated attestation ID"
                )
            if item.locator in self._seen_locators:
                raise IdentityDirectionalRawPreviewRunnerError(
                    "orphan_or_duplicate_attestation_rows: repeated physical locator"
                )
            self._seen_attestation_ids.add(item.row_attestation_id)
            self._seen_locators.add(item.locator)
            self._by_pair[pair][item.dataset].append(item)
            asset_count = sum(len(pair[ASSET_TABLE]) for pair in self._by_pair.values())
            universe_count = sum(len(pair[UNIVERSE_TABLE]) for pair in self._by_pair.values())
            if asset_count > self._selected_asset_row_cap:
                raise IdentityDirectionalRawPreviewRunnerError("selected asset row cap exceeded")
            if universe_count > self._selected_universe_row_cap:
                raise IdentityDirectionalRawPreviewRunnerError(
                    "selected universe row cap exceeded"
                )
            if asset_count + universe_count > self._selected_total_row_cap:
                raise IdentityDirectionalRawPreviewRunnerError("selected total row cap exceeded")

    def consume_physical_batch(
        self,
        source_batch: IdentitySourceBatch,
        *,
        calendar: XNYSCalendarArtifact,
        authority: DirectionalPreviewEvidenceAuthority,
    ) -> int:
        """Select by exact case-sensitive ticker/session and attest full source rows."""

        if not isinstance(source_batch, IdentitySourceBatch):
            raise IdentityDirectionalRawPreviewRunnerError("physical source batch is invalid")
        rows = source_batch.batch.to_pylist()
        selected: list[int] = []
        for index, row in enumerate(rows):
            if type(row.get("session_date")) is not date or not isinstance(
                row.get("ticker"), str
            ):
                raise IdentityDirectionalRawPreviewRunnerError("source pair fields are malformed")
            if (row["ticker"], row["session_date"]) in FIXED_PAIR_TO_CASE:
                selected.append(index)
        if selected:
            try:
                self.consume_attestations(
                    attest_directional_preview_provider_rows(
                        source_batch,
                        row_indices_in_batch=tuple(selected),
                        calendar=calendar,
                        authority=authority,
                    )
                )
            except ProviderEvidenceError as exc:
                raise IdentityDirectionalRawPreviewRunnerError(
                    "provider_row_attestation_schema_invalid_rows"
                ) from exc
        return len(selected)

    def finish(
        self,
        *,
        plan_id: str,
        plan_sha256: str,
        approval_id: str,
        approval_sha256: str,
        scope_set_id: str,
        source_artifacts: Sequence[DirectionalSourceArtifactRef],
        calendar: XNYSCalendarArtifact | None,
        created_at_utc: datetime,
        runner_verified_critical_numerators: Mapping[str, int],
    ) -> DirectionalPreviewBuild:
        if self._sealed:
            raise IdentityDirectionalRawPreviewRunnerError("engine is already sealed")
        self._sealed = True
        sources = tuple(sorted(source_artifacts))
        evidence: list[DirectionalCaseEvidenceManifest] = []
        slots: list[dict[str, object]] = []
        if set(runner_verified_critical_numerators) != _RUNNER_VERIFIED_CRITICAL_QA_IDS:
            raise IdentityDirectionalRawPreviewRunnerError(
                "runner verified QA map is incomplete or uncontracted"
            )
        if any(
            type(value) is not int or value < 0
            for value in runner_verified_critical_numerators.values()
        ):
            raise IdentityDirectionalRawPreviewRunnerError(
                "runner verified QA numerators are invalid"
            )
        critical: Counter[str] = Counter(runner_verified_critical_numerators)
        critical.update({check_id: 0 for check_id in _ENGINE_CRITICAL_QA_IDS})
        high: Counter[str] = Counter({check_id: 0 for check_id in _HIGH_QA_IDS})
        example_rows: dict[str, list[dict[str, object]]] = defaultdict(list)

        for case in FIXED_CASES:
            case_sessions = tuple(date.fromisoformat(item) for item in case["sessions"])
            case_sources = tuple(
                item for item in sources if item.session_date in set(case_sessions)
            )
            case_rows = tuple(
                item
                for session in case_sessions
                for table in SOURCE_TABLES
                for item in self._by_pair[(str(case["ticker"]), session)][table]
            )
            manifest = DirectionalCaseEvidenceManifest(
                plan_id=plan_id,
                plan_sha256=plan_sha256,
                approval_id=approval_id,
                approval_sha256=approval_sha256,
                scope_set_id=scope_set_id,
                review_case=case,
                source_artifacts=case_sources,
                row_attestations=case_rows,
                created_at_utc=created_at_utc,
            )
            evidence.append(manifest)
            for ordinal, session in enumerate(case_sessions):
                pair = (str(case["ticker"]), session)
                asset_rows = sorted(
                    self._by_pair[pair][ASSET_TABLE], key=lambda item: item.locator
                )
                universe_rows = sorted(
                    self._by_pair[pair][UNIVERSE_TABLE], key=lambda item: item.locator
                )
                slot = self._slot(
                    case=case,
                    session=session,
                    ordinal=ordinal,
                    previous=case_sessions[ordinal - 1] if ordinal else None,
                    calendar=calendar,
                    assets=asset_rows,
                    universes=universe_rows,
                    manifest=manifest,
                    critical=critical,
                    high=high,
                    examples=example_rows,
                    scope_set_id=scope_set_id,
                )
                slots.append(slot)

        expected_pairs = set(FIXED_PAIR_TO_CASE)
        actual_pairs = {(str(row["ticker"]), row["session_date"]) for row in slots}
        if len(slots) != DIRECTIONAL_RAW_PREVIEW_FIXED_PAIR_COUNT or actual_pairs != expected_pairs:
            critical["fixed_review_scope_invalid"] += 1
        if len({(row["review_case_id"], row["session_date"]) for row in slots}) != len(slots):
            critical["primary_key_duplicate_excess"] += 1

        evidence_ids = [
            item.row_attestation_id
            for manifest in evidence
            for item in manifest.row_attestations
        ]
        used_ids = [
            attestation_id
            for row in slots
            for attestation_id in (
                *json.loads(str(row["asset_observation_attestation_ids_json"])),
                *(
                    ()
                    if row["universe_row_attestation_id"] is None
                    else (row["universe_row_attestation_id"],)
                ),
            )
        ]
        if (
            len(evidence_ids) != len(set(evidence_ids))
            or len(used_ids) != len(set(used_ids))
            or set(evidence_ids) != set(used_ids)
        ):
            critical["orphan_or_duplicate_attestation_rows"] += 1

        slots.sort(key=lambda row: (str(row["ticker"]), row["session_date"]))
        directions = _directional_review(slots, created_at_utc=created_at_utc)
        high.update(directions["qa_numerators"])
        directional_document = {
            key: value for key, value in directions.items() if key != "qa_numerators"
        }
        _assert_no_forbidden_keys(slots)
        qa = _qa_document(critical, high, denominator=len(slots))
        failed = [
            item["check_id"]
            for item in qa["checks"]
            if item["severity"] == "critical" and item["status"] == QAStatus.FAILED.value
        ]
        if failed:
            raise IdentityDirectionalRawPreviewRunnerError(
                f"critical directional preview QA failed: {sorted(failed)}"
            )
        return DirectionalPreviewBuild(
            slots=tuple(MappingProxyType(dict(item)) for item in slots),
            evidence_manifests=tuple(evidence),
            directional_review=MappingProxyType(directional_document),
            qa=MappingProxyType(qa),
            examples=MappingProxyType(
                {
                    "artifact_type": "s7_directional_raw_preview_bounded_examples",
                    "capabilities": _fail_closed_capabilities(),
                    "examples": {
                        key: value[: self._bounded_example_cap]
                        for key, value in sorted(example_rows.items())
                    },
                    "registry_evaluation_state": (
                        DIRECTIONAL_RAW_PREVIEW_REGISTRY_EVALUATION_STATE
                    ),
                    "schema_version": 1,
                }
            ),
        )

    def _slot(
        self,
        *,
        case: Mapping[str, object],
        session: date,
        ordinal: int,
        previous: date | None,
        calendar: XNYSCalendarArtifact | None,
        assets: Sequence[ProviderRowAttestation],
        universes: Sequence[ProviderRowAttestation],
        manifest: DirectionalCaseEvidenceManifest,
        critical: Counter[str],
        high: Counter[str],
        examples: dict[str, list[dict[str, object]]],
        scope_set_id: str,
    ) -> dict[str, object]:
        if len(universes) > 1:
            critical["duplicate_universe_membership_rows"] += len(universes) - 1
        universe = universes[0] if len(universes) == 1 else None
        urow = universe.full_row_snapshot if universe is not None else None
        if universe is None:
            high["requested_slot_missing_membership_rows"] += 1
            if assets:
                high["asset_only_scope_rows"] += len(assets)
        selected_id = None if urow is None else urow["selected_source_record_id"]
        selected = [item for item in assets if item.source_record_id == selected_id]
        nonselected = [item for item in assets if item.source_record_id != selected_id]
        if universe is not None and not selected:
            critical["selected_parent_missing_rows"] += 1
        if len(selected) > 1:
            critical["selected_parent_multiple_rows"] += len(selected) - 1
        parent = selected[0] if len(selected) == 1 else None
        projection_match = (
            None
            if universe is None
            else parent is not None and _projection_matches(parent.full_row_snapshot, urow)
        )
        if projection_match is False:
            critical["selected_parent_projection_mismatch_rows"] += 1
        if nonselected:
            high["nonselected_asset_observation_rows"] += len(nonselected)
            examples["nonselected_asset_observation_rows"].append(
                _slot_example(case, session, assets, universes)
            )
        identity_variants = {
            (
                item.full_row_snapshot.get("composite_figi"),
                item.full_row_snapshot.get("share_class_figi"),
                item.full_row_snapshot.get("cik"),
            )
            for item in assets
        }
        if len(identity_variants) > 1:
            high["same_session_identity_variant_groups"] += 1
            examples["same_session_identity_variant_groups"].append(
                _slot_example(case, session, assets, universes)
            )
        observed_composite = None if urow is None else urow.get("composite_figi")
        if observed_composite != case["inventory_anchor_composite_figi"]:
            high["inventory_anchor_unobserved_slots"] += 1
        membership = (
            "absent_source_membership"
            if urow is None
            else "present_active"
            if urow["active_on_date"] is True
            else "present_inactive"
        )
        adjacent = None if previous is None else _calendar_adjacent(calendar, previous, session)
        ids = [item.row_attestation_id for item in assets]
        return {
            "review_case_id": case["review_case_id"],
            "review_scope_set_id": scope_set_id,
            "provider_id": DIRECTIONAL_RAW_PREVIEW_PROVIDER_ID,
            "provider_market": DIRECTIONAL_RAW_PREVIEW_PROVIDER_MARKET,
            "provider_locale": DIRECTIONAL_RAW_PREVIEW_PROVIDER_LOCALE,
            "ticker": case["ticker"],
            "inventory_anchor_composite_figi": case["inventory_anchor_composite_figi"],
            "session_date": session,
            "session_sequence_ordinal": ordinal,
            "previous_requested_session": previous,
            "previous_session_is_adjacent_xnys": adjacent,
            "universe_membership_count": len(universes),
            "membership_status": membership,
            "active_on_date": None if urow is None else urow["active_on_date"],
            "universe_row_attestation_id": (
                None if universe is None else universe.row_attestation_id
            ),
            "selected_source_record_id": selected_id,
            "source_version_count": None if urow is None else urow["source_version_count"],
            "version_group_id": None if urow is None else urow["version_group_id"],
            "selection_status": None if urow is None else urow["selection_status"],
            "observed_composite_figi": observed_composite,
            "observed_share_class_figi": (
                None if urow is None else urow.get("share_class_figi")
            ),
            "observed_cik": None if urow is None else urow.get("cik"),
            "observed_market": None if urow is None else urow.get("market"),
            "observed_locale": None if urow is None else urow.get("locale"),
            "observed_primary_exchange_mic": (
                None if urow is None else urow.get("primary_exchange_mic")
            ),
            "observed_type_code": None if urow is None else urow.get("type_code"),
            "universe_source_available_session": (
                None if urow is None else date.fromisoformat(str(urow["source_available_session"]))
            ),
            "asset_observation_match_count": len(assets),
            "selected_asset_parent_match_count": len(selected),
            "selected_asset_parent_attestation_id": (
                None if parent is None else parent.row_attestation_id
            ),
            "nonselected_asset_observation_count": len(nonselected),
            "asset_observation_attestation_ids_json": json.dumps(
                ids, ensure_ascii=False, separators=(",", ":")
            ),
            "selected_parent_projection_match": projection_match,
            "case_evidence_manifest_id": manifest.manifest_id,
            "case_evidence_manifest_path": manifest.candidate_relative_path,
            "case_evidence_manifest_sha256": manifest.sha256,
            "interval_inference_state": DIRECTIONAL_RAW_PREVIEW_INTERVAL_INFERENCE_STATE,
            "registry_evaluation_state": DIRECTIONAL_RAW_PREVIEW_REGISTRY_EVALUATION_STATE,
            "adjudication_eligible": False,
            "canonical_candidate_eligible": False,
            "full_run_eligible": False,
            "publication_eligible": False,
        }


def _calendar_adjacent(
    calendar: XNYSCalendarArtifact | None, previous: date, current: date
) -> bool:
    if calendar is None:
        return (current - previous).days <= 3
    sessions = tuple(item.session_date for item in calendar.sessions)
    try:
        return sessions.index(current) - sessions.index(previous) == 1
    except ValueError as exc:
        raise IdentityDirectionalRawPreviewRunnerError(
            "requested session is absent from frozen XNYS calendar"
        ) from exc


def _projection_matches(asset: Mapping[str, object], universe: Mapping[str, object]) -> bool:
    for asset_field, universe_field in UNIVERSE_PARENT_PROJECTION:
        left = asset.get(asset_field)
        right = universe.get(universe_field)
        if left != right:
            return False
    return True


def _slot_example(
    case: Mapping[str, object],
    session: date,
    assets: Sequence[ProviderRowAttestation],
    universes: Sequence[ProviderRowAttestation],
) -> dict[str, object]:
    return {
        "asset_attestation_ids": [item.row_attestation_id for item in assets],
        "review_case_id": case["review_case_id"],
        "session_date": session.isoformat(),
        "ticker": case["ticker"],
        "universe_attestation_ids": [item.row_attestation_id for item in universes],
    }


def _directional_review(
    slots: Sequence[Mapping[str, object]], *, created_at_utc: datetime
) -> dict[str, object]:
    by_ticker: dict[str, list[Mapping[str, object]]] = defaultdict(list)
    for row in slots:
        by_ticker[str(row["ticker"])].append(row)
    composite_edges = 0
    share_edges = 0
    gap_edges = 0
    cases: list[dict[str, object]] = []
    for ticker, rows in sorted(by_ticker.items()):
        ordered = sorted(rows, key=lambda item: item["session_sequence_ordinal"])
        observations = []
        edges = []
        for row in ordered:
            observations.append(
                {
                    "active_on_date": row["active_on_date"],
                    "all_asset_observation_attestation_ids": json.loads(
                        str(row["asset_observation_attestation_ids_json"])
                    ),
                    "membership_status": row["membership_status"],
                    "observed_cik": row["observed_cik"],
                    "observed_composite_figi": row["observed_composite_figi"],
                    "observed_share_class_figi": row["observed_share_class_figi"],
                    "selected_asset_parent_attestation_id": row[
                        "selected_asset_parent_attestation_id"
                    ],
                    "selected_source_record_id": row["selected_source_record_id"],
                    "session_date": row["session_date"].isoformat(),
                    "universe_row_attestation_id": row["universe_row_attestation_id"],
                }
            )
        for left, right in pairwise(ordered):
            both_present = (
                left["membership_status"] != "absent_source_membership"
                and right["membership_status"] != "absent_source_membership"
            )
            composite_comparable = (
                both_present
                and left["observed_composite_figi"] is not None
                and right["observed_composite_figi"] is not None
            )
            share_comparable = (
                both_present
                and left["observed_share_class_figi"] is not None
                and right["observed_share_class_figi"] is not None
            )
            composite_changed = bool(
                composite_comparable
                and left["observed_composite_figi"] != right["observed_composite_figi"]
            )
            share_changed = bool(
                share_comparable
                and left["observed_share_class_figi"]
                != right["observed_share_class_figi"]
            )
            sampled_gap = right["previous_session_is_adjacent_xnys"] is False
            composite_edges += int(composite_changed)
            share_edges += int(share_changed)
            gap_edges += int(sampled_gap)
            edges.append(
                {
                    "composite_changed": composite_changed,
                    "composite_comparable": composite_comparable,
                    "from_composite_figi": left["observed_composite_figi"],
                    "from_session": left["session_date"].isoformat(),
                    "from_share_class_figi": left["observed_share_class_figi"],
                    "sampled_gap": sampled_gap,
                    "share_class_changed": share_changed,
                    "share_class_comparable": share_comparable,
                    "to_composite_figi": right["observed_composite_figi"],
                    "to_session": right["session_date"].isoformat(),
                    "to_share_class_figi": right["observed_share_class_figi"],
                }
            )
        cases.append(
            {
                "exact_effective_interval_proven": False,
                "observations": observations,
                "review_case_id": ordered[0]["review_case_id"],
                "sampled_edges": edges,
                "ticker": ticker,
            }
        )
    return {
        "artifact_type": "s7_directional_raw_preview_sequences",
        "capabilities": _fail_closed_capabilities(),
        "cases": cases,
        "created_at_utc": _utc_text(created_at_utc),
        "directional_review_rule_version": DIRECTIONAL_REVIEW_RULE_VERSION,
        "exact_effective_interval_proven": False,
        "qa_numerators": {
            "directional_composite_change_edges": composite_edges,
            "directional_share_class_change_edges": share_edges,
            "sampled_gap_edges": gap_edges,
        },
        "registry_evaluation_state": DIRECTIONAL_RAW_PREVIEW_REGISTRY_EVALUATION_STATE,
        "schema_version": 1,
    }


def _qa_document(
    critical: Counter[str], high: Counter[str], *, denominator: int
) -> dict[str, object]:
    if set(critical) != _CRITICAL_QA_IDS or set(high) != _HIGH_QA_IDS:
        raise IdentityDirectionalRawPreviewRunnerError(
            "QA implementation map is incomplete or contains an uncontracted check"
        )
    checks: list[dict[str, object]] = []
    for rule in IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_CONTRACT.qa_rules:
        numerator = (
            critical[rule.check_id]
            if rule.severity.value == "critical"
            else high[rule.check_id]
        )
        status = (
            QAStatus.FAILED.value
            if rule.severity.value == "critical" and numerator != 0
            else QAStatus.WARNING.value
            if rule.severity.value == "high" and numerator != 0
            else QAStatus.PASSED.value
        )
        checks.append(
            {
                "check_id": rule.check_id,
                "denominator": denominator,
                "numerator": numerator,
                "severity": rule.severity.value,
                "status": status,
            }
        )
    return {
        "artifact_type": "s7_directional_raw_preview_qa",
        "capabilities": _fail_closed_capabilities(),
        "checks": checks,
        "critical_failure_count": sum(
            item["status"] == QAStatus.FAILED.value for item in checks
        ),
        "registry_evaluation_state": DIRECTIONAL_RAW_PREVIEW_REGISTRY_EVALUATION_STATE,
        "schema_version": 1,
        "warning_count": sum(item["status"] == QAStatus.WARNING.value for item in checks),
    }


def _assert_no_forbidden_keys(value: object) -> None:
    if isinstance(value, Mapping):
        overlap = _FORBIDDEN_OUTPUT_KEYS & set(value)
        if overlap:
            raise IdentityDirectionalRawPreviewRunnerError(
                f"forbidden identity/decision output keys: {sorted(overlap)}"
            )
        for child in value.values():
            _assert_no_forbidden_keys(child)
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for child in value:
            _assert_no_forbidden_keys(child)


@dataclass(frozen=True, slots=True)
class DirectionalOutputArtifactRef:
    role: str
    path: str
    sha256: str
    bytes: int
    media_type: str
    row_count: int | None = None

    def __post_init__(self) -> None:
        _relative_path(self.path, "output artifact path")
        _digest(self.sha256, "output artifact SHA")
        _nonnegative(self.bytes, "output artifact bytes")
        if self.row_count is not None:
            _nonnegative(self.row_count, "output artifact rows")

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
        return result


def _output_ref_from_dict(value: object) -> DirectionalOutputArtifactRef:
    if not isinstance(value, Mapping):
        raise IdentityDirectionalRawPreviewRunnerError("output ref must be an object")
    item = dict(value)
    expected = {"bytes", "media_type", "path", "role", "sha256"}
    if "row_count" in item:
        expected.add("row_count")
    if set(item) != expected:
        raise IdentityDirectionalRawPreviewRunnerError("output ref schema is not exact")
    return DirectionalOutputArtifactRef(
        role=str(item["role"]),
        path=str(item["path"]),
        sha256=str(item["sha256"]),
        bytes=int(item["bytes"]),
        media_type=str(item["media_type"]),
        row_count=None if "row_count" not in item else int(item["row_count"]),
    )


@dataclass(frozen=True, slots=True)
class S7DirectionalRawPreviewCandidate:
    created_by: str
    plan_id: str
    plan_sha256: str
    approval_id: str
    approval_sha256: str
    request_event_id: str
    request_event_sha256: str
    input_binding_digest: str
    source_artifacts: tuple[DirectionalSourceArtifactRef, ...]
    source_artifact_set_digest: str
    scope_set_id: str
    scope_set_sha256: str
    artifacts: tuple[DirectionalOutputArtifactRef, ...]
    evidence_manifest_ids: tuple[str, ...]
    created_at_utc: datetime
    candidate_state: str = CANDIDATE_STATE

    def __post_init__(self) -> None:
        if (
            not isinstance(self.created_by, str)
            or not self.created_by
            or self.created_by.strip() != self.created_by
        ):
            raise IdentityDirectionalRawPreviewRunnerError(
                "candidate created_by is invalid"
            )
        for label, value in (
            ("plan ID", self.plan_id),
            ("plan SHA", self.plan_sha256),
            ("approval ID", self.approval_id),
            ("approval SHA", self.approval_sha256),
            ("request event ID", self.request_event_id),
            ("request event SHA", self.request_event_sha256),
            ("input binding digest", self.input_binding_digest),
            ("source artifact set digest", self.source_artifact_set_digest),
            ("scope-set ID", self.scope_set_id),
            ("scope-set SHA", self.scope_set_sha256),
        ):
            _digest(value, label)
        sources = tuple(sorted(self.source_artifacts))
        if (
            len(sources) != 22
            or len({item.path for item in sources}) != len(sources)
            or stable_digest([item.to_dict() for item in sources])
            != self.source_artifact_set_digest
        ):
            raise IdentityDirectionalRawPreviewRunnerError(
                "candidate source artifact binding is invalid"
            )
        object.__setattr__(self, "source_artifacts", sources)
        outputs = tuple(sorted(self.artifacts, key=lambda item: item.role))
        if (
            {item.role for item in outputs} != set(_FIXED_OUTPUT_ROLE_SPECS)
            or len({item.path for item in outputs}) != len(outputs)
            or any(
                (item.path, item.media_type, item.row_count)
                != _FIXED_OUTPUT_ROLE_SPECS[item.role]
                for item in outputs
            )
        ):
            raise IdentityDirectionalRawPreviewRunnerError(
                "candidate output artifact set is invalid"
            )
        object.__setattr__(self, "artifacts", outputs)
        evidence_ids = tuple(sorted(self.evidence_manifest_ids))
        if len(evidence_ids) != 3 or len(set(evidence_ids)) != 3:
            raise IdentityDirectionalRawPreviewRunnerError(
                "candidate evidence manifest IDs are invalid"
            )
        for value in evidence_ids:
            _digest(value, "evidence manifest ID")
        object.__setattr__(self, "evidence_manifest_ids", evidence_ids)
        if (
            self.created_at_utc.tzinfo is None
            or self.created_at_utc > datetime.now(UTC)
            or self.candidate_state != CANDIDATE_STATE
        ):
            raise IdentityDirectionalRawPreviewRunnerError(
                "candidate timestamp or state is invalid"
            )

    def logical_payload(self) -> dict[str, object]:
        return {
            "approval_id": self.approval_id,
            "approval_sha256": self.approval_sha256,
            "artifact_type": "s7_directional_raw_preview_candidate",
            "artifacts": [item.to_dict() for item in self.artifacts],
            "candidate_rule_version": CANDIDATE_RULE_VERSION,
            "candidate_state": self.candidate_state,
            "capabilities": _fail_closed_capabilities(),
            "contract": {
                "candidate_sha256": IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_RESOURCE_SHA256,
                "contract_id": IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_CONTRACT_ID,
                "schema_digest": IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_SCHEMA_DIGEST,
            },
            "created_at_utc": _utc_text(self.created_at_utc),
            "created_by": self.created_by,
            "evidence_manifest_ids": list(self.evidence_manifest_ids),
            "input_binding_digest": self.input_binding_digest,
            "plan_id": self.plan_id,
            "plan_sha256": self.plan_sha256,
            "registry_evaluation_state": DIRECTIONAL_RAW_PREVIEW_REGISTRY_EVALUATION_STATE,
            "registry_semantics_digest": (
                DIRECTIONAL_RAW_PREVIEW_REGISTRY_EXCLUSIVITY_SEMANTICS_DIGEST
            ),
            "request_event_id": self.request_event_id,
            "request_event_sha256": self.request_event_sha256,
            "scope_set_id": self.scope_set_id,
            "scope_set_sha256": self.scope_set_sha256,
            "source_artifact_set_digest": self.source_artifact_set_digest,
            "source_artifacts": [item.to_dict() for item in self.source_artifacts],
        }

    @property
    def candidate_id(self) -> str:
        return stable_digest(self.logical_payload())

    @property
    def content(self) -> bytes:
        return _canonical_bytes({**self.logical_payload(), "candidate_id": self.candidate_id})

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.content).hexdigest()

    @classmethod
    def from_dict(cls, value: object) -> S7DirectionalRawPreviewCandidate:
        if not isinstance(value, Mapping):
            raise IdentityDirectionalRawPreviewRunnerError("candidate must be an object")
        document = dict(value)
        expected_keys = {
            "approval_id",
            "approval_sha256",
            "artifact_type",
            "artifacts",
            "candidate_id",
            "candidate_rule_version",
            "candidate_state",
            "capabilities",
            "contract",
            "created_at_utc",
            "created_by",
            "evidence_manifest_ids",
            "input_binding_digest",
            "plan_id",
            "plan_sha256",
            "registry_evaluation_state",
            "registry_semantics_digest",
            "request_event_id",
            "request_event_sha256",
            "scope_set_id",
            "scope_set_sha256",
            "source_artifact_set_digest",
            "source_artifacts",
        }
        if set(document) != expected_keys:
            raise IdentityDirectionalRawPreviewRunnerError("candidate schema is not exact")
        candidate = cls(
            created_by=str(document["created_by"]),
            plan_id=str(document["plan_id"]),
            plan_sha256=str(document["plan_sha256"]),
            approval_id=str(document["approval_id"]),
            approval_sha256=str(document["approval_sha256"]),
            request_event_id=str(document["request_event_id"]),
            request_event_sha256=str(document["request_event_sha256"]),
            input_binding_digest=str(document["input_binding_digest"]),
            source_artifacts=tuple(
                DirectionalSourceArtifactRef.from_dict(item)
                for item in document["source_artifacts"]
            ),
            source_artifact_set_digest=str(document["source_artifact_set_digest"]),
            scope_set_id=str(document["scope_set_id"]),
            scope_set_sha256=str(document["scope_set_sha256"]),
            artifacts=tuple(_output_ref_from_dict(item) for item in document["artifacts"]),
            evidence_manifest_ids=tuple(str(item) for item in document["evidence_manifest_ids"]),
            created_at_utc=datetime.fromisoformat(str(document["created_at_utc"])),
            candidate_state=str(document["candidate_state"]),
        )
        if _canonical_bytes(document) != candidate.content:
            raise IdentityDirectionalRawPreviewRunnerError(
                "candidate canonical bytes do not reproduce"
            )
        return candidate


def _validate_control_binding_shape(value: object) -> None:
    if not isinstance(value, Mapping):
        raise IdentityDirectionalRawPreviewRunnerError(
            "completion control binding must be an object"
        )
    binding = dict(value)
    expected = {
        "algorithm_digest",
        "approval",
        "contract",
        "execution_data_root",
        "input_binding_digest",
        "inventory",
        "manifest_intent",
        "manifest_preflight",
        "plan",
        "qa_semantics_digest",
        "registry_semantics_digest",
        "request",
        "resource_caps_digest",
        "runtime_file_set_digest",
        "scope",
        "source_artifact_set_digest",
        "source_binding",
        "verification_file_set_digest",
    }
    if set(binding) != expected:
        raise IdentityDirectionalRawPreviewRunnerError(
            "completion control binding schema is not exact"
        )
    nested_keys = {
        "approval": {
            "approval_id",
            "approval_literal_sha256",
            "approved_at_utc",
            "approved_by",
            "path",
            "sha256",
        },
        "contract": {"candidate_sha256", "contract_id", "schema_digest"},
        "inventory": {
            "candidate_data_sha256",
            "candidate_id",
            "candidate_manifest_path",
            "candidate_manifest_sha256",
            "completion_id",
            "completion_manifest_path",
            "completion_manifest_sha256",
        },
        "manifest_intent": {"intent_id", "path", "sha256"},
        "manifest_preflight": {"approval_id", "sha256"},
        "plan": {"created_at_utc", "created_by", "path", "plan_id", "sha256"},
        "request": {"path", "request_event_id", "sha256"},
        "scope": {"scope_set_id", "scope_set_sha256"},
        "source_binding": {"manifest_id", "path", "sha256"},
    }
    for name, keys in nested_keys.items():
        item = binding.get(name)
        if not isinstance(item, Mapping) or set(item) != keys:
            raise IdentityDirectionalRawPreviewRunnerError(
                f"completion {name} binding schema is not exact"
            )
    digest_paths = (
        ("algorithm_digest",),
        ("input_binding_digest",),
        ("qa_semantics_digest",),
        ("registry_semantics_digest",),
        ("resource_caps_digest",),
        ("runtime_file_set_digest",),
        ("source_artifact_set_digest",),
        ("verification_file_set_digest",),
        ("approval", "approval_id"),
        ("approval", "approval_literal_sha256"),
        ("approval", "sha256"),
        ("contract", "candidate_sha256"),
        ("contract", "contract_id"),
        ("contract", "schema_digest"),
        ("inventory", "candidate_data_sha256"),
        ("inventory", "candidate_id"),
        ("inventory", "candidate_manifest_sha256"),
        ("inventory", "completion_id"),
        ("inventory", "completion_manifest_sha256"),
        ("manifest_intent", "intent_id"),
        ("manifest_intent", "sha256"),
        ("manifest_preflight", "approval_id"),
        ("manifest_preflight", "sha256"),
        ("plan", "plan_id"),
        ("plan", "sha256"),
        ("request", "request_event_id"),
        ("request", "sha256"),
        ("scope", "scope_set_id"),
        ("scope", "scope_set_sha256"),
        ("source_binding", "manifest_id"),
        ("source_binding", "sha256"),
    )
    for parts in digest_paths:
        item: object = binding
        for part in parts:
            item = item[part]  # type: ignore[index]
        _digest(item, "completion control digest")


def _expected_control_binding(plan: Any, approval: Any) -> dict[str, object]:
    return {
        "algorithm_digest": plan.algorithm_digest,
        "approval": {
            "approval_id": approval.approval_id,
            "approval_literal_sha256": approval.approval_literal_sha256,
            "approved_at_utc": _utc_text(approval.approved_at_utc),
            "approved_by": approval.approved_by,
            "path": approval.relative_path,
            "sha256": approval.sha256,
        },
        "contract": {
            "candidate_sha256": plan.contract_candidate_sha256,
            "contract_id": plan.contract_id,
            "schema_digest": plan.contract_schema_digest,
        },
        "execution_data_root": plan.execution_data_root,
        "input_binding_digest": plan.input_binding_digest,
        "inventory": {
            "candidate_data_sha256": plan.inventory_candidate_data_sha256,
            "candidate_id": plan.inventory_candidate_id,
            "candidate_manifest_path": plan.inventory_candidate_path,
            "candidate_manifest_sha256": plan.inventory_candidate_manifest_sha256,
            "completion_id": plan.inventory_completion_id,
            "completion_manifest_path": plan.inventory_completion_path,
            "completion_manifest_sha256": plan.inventory_completion_sha256,
        },
        "manifest_intent": {
            "intent_id": plan.manifest_preflight_intent_id,
            "path": plan.manifest_preflight_intent_path,
            "sha256": plan.manifest_preflight_intent_sha256,
        },
        "manifest_preflight": {
            "approval_id": plan.manifest_preflight_approval_id,
            "sha256": plan.manifest_preflight_approval_sha256,
        },
        "plan": {
            "created_at_utc": _utc_text(plan.created_at_utc),
            "created_by": plan.created_by,
            "path": plan.relative_path,
            "plan_id": plan.plan_id,
            "sha256": plan.sha256,
        },
        "qa_semantics_digest": plan.qa_semantics_digest,
        "registry_semantics_digest": (
            DIRECTIONAL_RAW_PREVIEW_REGISTRY_EXCLUSIVITY_SEMANTICS_DIGEST
        ),
        "request": {
            "path": approval.request_event_path,
            "request_event_id": approval.request_event_id,
            "sha256": approval.request_event_sha256,
        },
        "resource_caps_digest": plan.resource_caps.digest,
        "runtime_file_set_digest": plan.runtime_file_set_digest,
        "scope": {
            "scope_set_id": plan.scope_set_id,
            "scope_set_sha256": plan.scope_set_sha256,
        },
        "source_artifact_set_digest": plan.source_artifact_set_digest,
        "source_binding": {
            "manifest_id": plan.source_binding_manifest_id,
            "path": plan.source_binding_manifest_path,
            "sha256": plan.source_binding_manifest_sha256,
        },
        "verification_file_set_digest": plan.verification_file_set_digest,
    }


@dataclass(frozen=True, slots=True)
class S7DirectionalRawPreviewExecutionCompletion:
    created_by: str
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
    candidate_created_at_utc: datetime
    control_binding: Mapping[str, object]
    source_artifact_set_digest: str
    output_artifacts: tuple[DirectionalOutputArtifactRef, ...]
    evidence_manifest_ids: tuple[str, ...]
    completed_at_utc: datetime
    wall_clock_seconds: float
    peak_rss_bytes: int
    minimum_disk_free_bytes: int
    maximum_tmp_bytes: int
    output_bytes: int
    candidate_tree_bytes: int
    source_artifact_count: int
    source_row_count: int
    source_bytes: int
    selected_asset_row_count: int
    selected_universe_row_count: int
    output_slot_row_count: int
    disk_free_warning_triggered: bool
    resource_measurement_cutoff: str = RESOURCE_MEASUREMENT_CUTOFF
    completion_state: str = CANDIDATE_STATE

    def __post_init__(self) -> None:
        if (
            not isinstance(self.created_by, str)
            or not self.created_by
            or self.created_by.strip() != self.created_by
        ):
            raise IdentityDirectionalRawPreviewRunnerError(
                "completion created_by is invalid"
            )
        for label, value in (
            ("plan ID", self.plan_id),
            ("plan SHA", self.plan_sha256),
            ("approval ID", self.approval_id),
            ("approval SHA", self.approval_sha256),
            ("request event ID", self.request_event_id),
            ("request event SHA", self.request_event_sha256),
            ("input binding digest", self.input_binding_digest),
            ("candidate ID", self.candidate_id),
            ("candidate SHA", self.candidate_sha256),
            ("source artifact set digest", self.source_artifact_set_digest),
        ):
            _digest(value, label)
        expected_candidate = (
            "manifests/silver/identity/directional-raw-preview-candidates/"
            f"candidate_id={self.candidate_id}/manifest.json"
        )
        if self.candidate_path != expected_candidate:
            raise IdentityDirectionalRawPreviewRunnerError(
                "completion candidate path is not canonical"
            )
        outputs = tuple(sorted(self.output_artifacts, key=lambda item: item.role))
        expected_roles = {
            "review_slots",
            "directional_review",
            "qa",
            "bounded_examples",
            "case_evidence:SOR",
            "case_evidence:XZO",
            "case_evidence:ANABV",
        }
        if (
            len(outputs) != len(expected_roles)
            or {item.role for item in outputs} != expected_roles
            or len({item.path for item in outputs}) != len(outputs)
        ):
            raise IdentityDirectionalRawPreviewRunnerError(
                "completion output refs are incomplete"
            )
        object.__setattr__(self, "output_artifacts", outputs)
        evidence_ids = tuple(sorted(self.evidence_manifest_ids))
        if len(evidence_ids) != 3 or len(set(evidence_ids)) != 3:
            raise IdentityDirectionalRawPreviewRunnerError(
                "completion evidence manifest IDs are invalid"
            )
        for value in evidence_ids:
            _digest(value, "completion evidence manifest ID")
        object.__setattr__(self, "evidence_manifest_ids", evidence_ids)
        if not isinstance(self.control_binding, Mapping):
            raise IdentityDirectionalRawPreviewRunnerError(
                "completion control binding must be an object"
            )
        try:
            frozen_control = json.loads(
                json.dumps(
                    dict(self.control_binding),
                    allow_nan=False,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    sort_keys=True,
                )
            )
        except (TypeError, ValueError) as exc:
            raise IdentityDirectionalRawPreviewRunnerError(
                "completion control binding is not canonical JSON"
            ) from exc
        _validate_control_binding_shape(frozen_control)
        object.__setattr__(self, "control_binding", MappingProxyType(frozen_control))
        for label, value in (
            ("peak RSS", self.peak_rss_bytes),
            ("minimum disk free", self.minimum_disk_free_bytes),
            ("maximum tmp", self.maximum_tmp_bytes),
            ("output bytes", self.output_bytes),
            ("candidate tree bytes", self.candidate_tree_bytes),
            ("source artifact count", self.source_artifact_count),
            ("source row count", self.source_row_count),
            ("source bytes", self.source_bytes),
            ("selected asset rows", self.selected_asset_row_count),
            ("selected universe rows", self.selected_universe_row_count),
            ("output slot rows", self.output_slot_row_count),
        ):
            _nonnegative(value, label)
        if (
            self.source_artifact_count != 22
            or self.output_slot_row_count != DIRECTIONAL_RAW_PREVIEW_FIXED_PAIR_COUNT
            or self.output_bytes != sum(item.bytes for item in outputs)
            or self.candidate_tree_bytes < self.output_bytes
            or type(self.disk_free_warning_triggered) is not bool
            or type(self.wall_clock_seconds) is not float
            or not math.isfinite(self.wall_clock_seconds)
            or self.wall_clock_seconds < 0
            or self.completed_at_utc.tzinfo is None
            or self.completed_at_utc > datetime.now(UTC)
            or self.candidate_created_at_utc.tzinfo is None
            or self.candidate_created_at_utc > self.completed_at_utc
            or self.resource_measurement_cutoff != RESOURCE_MEASUREMENT_CUTOFF
            or self.completion_state != CANDIDATE_STATE
        ):
            raise IdentityDirectionalRawPreviewRunnerError(
                "completion counts, resources, time or state are invalid"
            )

    def logical_payload(self) -> dict[str, object]:
        return {
            "approval_id": self.approval_id,
            "approval_sha256": self.approval_sha256,
            "artifact_type": "s7_directional_raw_preview_execution_completion",
            "candidate": {
                "candidate_id": self.candidate_id,
                "created_at_utc": _utc_text(self.candidate_created_at_utc),
                "path": self.candidate_path,
                "sha256": self.candidate_sha256,
                "state": self.completion_state,
            },
            "capabilities": _fail_closed_capabilities(),
            "control_binding": dict(self.control_binding),
            "completed_at_utc": _utc_text(self.completed_at_utc),
            "created_by": self.created_by,
            "completion_rule_version": COMPLETION_RULE_VERSION,
            "completion_state": self.completion_state,
            "input_binding_digest": self.input_binding_digest,
            "evidence_manifest_ids": list(self.evidence_manifest_ids),
            "output_artifacts": [item.to_dict() for item in self.output_artifacts],
            "plan_id": self.plan_id,
            "plan_sha256": self.plan_sha256,
            "request_event_id": self.request_event_id,
            "request_event_sha256": self.request_event_sha256,
            "source_artifact_set_digest": self.source_artifact_set_digest,
            "counts": {
                "output_slot_row_count": self.output_slot_row_count,
                "selected_asset_row_count": self.selected_asset_row_count,
                "selected_universe_row_count": self.selected_universe_row_count,
                "source_artifact_count": self.source_artifact_count,
                "source_bytes": self.source_bytes,
                "source_row_count": self.source_row_count,
            },
            "resource_measurements": {
                "disk_free_warning_triggered": self.disk_free_warning_triggered,
                "maximum_tmp_bytes": self.maximum_tmp_bytes,
                "minimum_disk_free_bytes": self.minimum_disk_free_bytes,
                "candidate_tree_bytes": self.candidate_tree_bytes,
                "output_bytes": self.output_bytes,
                "peak_rss_bytes": self.peak_rss_bytes,
                "wall_clock_seconds": self.wall_clock_seconds,
            },
            "resource_measurement_cutoff": self.resource_measurement_cutoff,
        }

    @property
    def completion_id(self) -> str:
        return stable_digest(self.logical_payload())

    @property
    def content(self) -> bytes:
        return _canonical_bytes({**self.logical_payload(), "completion_id": self.completion_id})

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.content).hexdigest()

    @classmethod
    def from_dict(cls, value: object) -> S7DirectionalRawPreviewExecutionCompletion:
        if not isinstance(value, Mapping):
            raise IdentityDirectionalRawPreviewRunnerError("completion must be an object")
        document = dict(value)
        expected = {
            "approval_id",
            "approval_sha256",
            "artifact_type",
            "candidate",
            "capabilities",
            "completed_at_utc",
            "completion_id",
            "completion_rule_version",
            "completion_state",
            "control_binding",
            "counts",
            "created_by",
            "evidence_manifest_ids",
            "input_binding_digest",
            "output_artifacts",
            "plan_id",
            "plan_sha256",
            "request_event_id",
            "request_event_sha256",
            "resource_measurement_cutoff",
            "resource_measurements",
            "source_artifact_set_digest",
        }
        if set(document) != expected:
            raise IdentityDirectionalRawPreviewRunnerError(
                "completion schema is not exact"
            )
        candidate = document.get("candidate")
        counts = document.get("counts")
        metrics = document.get("resource_measurements")
        if not all(isinstance(item, Mapping) for item in (candidate, counts, metrics)):
            raise IdentityDirectionalRawPreviewRunnerError("completion sections are invalid")
        completion = cls(
            created_by=str(document.get("created_by")),
            plan_id=str(document.get("plan_id")),
            plan_sha256=str(document.get("plan_sha256")),
            approval_id=str(document.get("approval_id")),
            approval_sha256=str(document.get("approval_sha256")),
            request_event_id=str(document.get("request_event_id")),
            request_event_sha256=str(document.get("request_event_sha256")),
            input_binding_digest=str(document.get("input_binding_digest")),
            candidate_id=str(candidate.get("candidate_id")),
            candidate_path=str(candidate.get("path")),
            candidate_sha256=str(candidate.get("sha256")),
            candidate_created_at_utc=datetime.fromisoformat(
                str(candidate.get("created_at_utc"))
            ),
            control_binding=dict(document.get("control_binding", {})),
            source_artifact_set_digest=str(document.get("source_artifact_set_digest")),
            output_artifacts=tuple(
                _output_ref_from_dict(item) for item in document.get("output_artifacts", [])
            ),
            evidence_manifest_ids=tuple(
                str(item) for item in document.get("evidence_manifest_ids", [])
            ),
            completed_at_utc=datetime.fromisoformat(str(document.get("completed_at_utc"))),
            wall_clock_seconds=float(metrics.get("wall_clock_seconds")),
            peak_rss_bytes=int(metrics.get("peak_rss_bytes")),
            minimum_disk_free_bytes=int(metrics.get("minimum_disk_free_bytes")),
            maximum_tmp_bytes=int(metrics.get("maximum_tmp_bytes")),
            output_bytes=int(metrics.get("output_bytes")),
            candidate_tree_bytes=int(metrics.get("candidate_tree_bytes")),
            source_artifact_count=int(counts.get("source_artifact_count")),
            source_row_count=int(counts.get("source_row_count")),
            source_bytes=int(counts.get("source_bytes")),
            selected_asset_row_count=int(counts.get("selected_asset_row_count")),
            selected_universe_row_count=int(counts.get("selected_universe_row_count")),
            output_slot_row_count=int(counts.get("output_slot_row_count")),
            disk_free_warning_triggered=metrics.get("disk_free_warning_triggered"),
            resource_measurement_cutoff=str(
                document.get("resource_measurement_cutoff")
            ),
            completion_state=str(document.get("completion_state")),
        )
        if _canonical_bytes(document) != completion.content:
            raise IdentityDirectionalRawPreviewRunnerError(
                "completion canonical bytes do not reproduce"
            )
        return completion


@dataclass(frozen=True, slots=True)
class _LoadedControls:
    plan: Any
    approval: Any
    calendar: XNYSCalendarArtifact


class _ResourceMonitor:
    def __init__(self, *, root: Path, staging: Path, caps: Any, started: float) -> None:
        self.root = root
        self.staging = staging
        self.caps = caps
        self.started = started
        self.minimum_disk_free_bytes = 2**63 - 1
        self.maximum_tmp_bytes = 0
        self.peak_rss_bytes = 0

    def check(self) -> None:
        elapsed = time.monotonic() - self.started
        if elapsed > self.caps.wall_clock_seconds_hard_cap:
            raise IdentityDirectionalRawPreviewRunnerError("resource_cap_exceeded: wall clock")
        rss = _peak_rss_bytes()
        self.peak_rss_bytes = max(self.peak_rss_bytes, rss)
        if rss > self.caps.rss_bytes_hard_cap:
            raise IdentityDirectionalRawPreviewRunnerError("resource_cap_exceeded: RSS")
        free = shutil.disk_usage(self.root).free
        self.minimum_disk_free_bytes = min(self.minimum_disk_free_bytes, free)
        if free < self.caps.disk_free_floor_bytes:
            raise IdentityDirectionalRawPreviewRunnerError("resource_cap_exceeded: disk floor")
        temporary = _tree_bytes(self.staging)
        self.maximum_tmp_bytes = max(self.maximum_tmp_bytes, temporary)
        if temporary > self.caps.temporary_bytes_hard_cap:
            raise IdentityDirectionalRawPreviewRunnerError("resource_cap_exceeded: tmp bytes")

    @property
    def elapsed(self) -> float:
        return float(time.monotonic() - self.started)

    @property
    def disk_warning_triggered(self) -> bool:
        return self.minimum_disk_free_bytes < self.caps.disk_free_warning_bytes


def run_exact_s7_directional_raw_preview(
    data_root: Path,
    *,
    plan_id: str,
    expected_plan_sha256: str,
    approval_id: str,
    expected_approval_sha256: str,
) -> S7DirectionalRawPreviewExecutionCompletion:
    """Run one separately approved exact preview; no scope override is accepted."""

    started = time.monotonic()
    root = _validated_root(data_root)
    for label, value in (
        ("plan ID", plan_id),
        ("plan SHA", expected_plan_sha256),
        ("approval ID", approval_id),
        ("approval SHA", expected_approval_sha256),
    ):
        _digest(value, label)
    controls = _load_controls(
        root,
        plan_id=plan_id,
        expected_plan_sha256=expected_plan_sha256,
        approval_id=approval_id,
        expected_approval_sha256=expected_approval_sha256,
    )
    _verify_control_bindings(root, controls)
    _verify_git_checkout_and_pins(controls.plan)
    paths = _execution_paths(plan_id, approval_id)
    completion_path = safe_relative_path(root, paths["completion"])
    if completion_path.is_file() and not completion_path.is_symlink():
        return _read_completion(root, completion_path, controls)
    if completion_path.exists() or completion_path.is_symlink():
        raise IdentityDirectionalRawPreviewRunnerError("completion slot is unsafe")
    lock_path = safe_relative_path(root, paths["lock"])
    with _exclusive_lock(lock_path):
        if completion_path.is_file() and not completion_path.is_symlink():
            return _read_completion(root, completion_path, controls)
        staging = safe_relative_path(root, paths["staging"])
        if staging.exists() or staging.is_symlink():
            return _recover_after_candidate_commit(
                root,
                controls,
                staging=staging,
                completion_path=completion_path,
            )
        staging.mkdir(parents=True, exist_ok=False)
        monitor = _ResourceMonitor(
            root=root,
            staging=staging,
            caps=controls.plan.resource_caps,
            started=started,
        )
        monitor.check()
        return _execute_new(
            root,
            controls,
            staging=staging,
            completion_path=completion_path,
            started=started,
            monitor=monitor,
        )


def _load_controls(
    root: Path,
    *,
    plan_id: str,
    expected_plan_sha256: str,
    approval_id: str,
    expected_approval_sha256: str,
) -> _LoadedControls:
    try:
        from ame_stocks_api.silver.calendar_artifact import (
            XNYSCalendarArtifactError,
            load_xnys_calendar_artifact,
        )
        from ame_stocks_api.silver.identity_directional_raw_preview_approval import (
            DirectionalRawPreviewExecutionApprovalStore,
            IdentityDirectionalRawPreviewExecutionApprovalError,
            S7DirectionalRawPreviewExecutionApproval,
        )
        from ame_stocks_api.silver.identity_directional_raw_preview_execution_plan import (
            DirectionalRawPreviewExecutionPlanStore,
            IdentityDirectionalRawPreviewExecutionPlanError,
            S7DirectionalRawPreviewExecutionPlan,
        )
        from ame_stocks_api.silver.identity_directional_raw_preview_plan import (
            CALENDAR_ARTIFACT_ID,
            CALENDAR_ARTIFACT_SHA256,
        )
    except ImportError as exc:
        raise IdentityDirectionalRawPreviewRunnerError(
            "exact directional execution control modules are unavailable"
        ) from exc
    try:
        store = DirectionalRawPreviewExecutionPlanStore(root)
        plan, _ = store.load_execution_plan(plan_id, expected_sha256=expected_plan_sha256)
        approval, _ = DirectionalRawPreviewExecutionApprovalStore(root).load_approval(
            approval_id, expected_sha256=expected_approval_sha256
        )
        calendar = load_xnys_calendar_artifact(
            root,
            calendar_artifact_id=CALENDAR_ARTIFACT_ID,
            expected_sha256=CALENDAR_ARTIFACT_SHA256,
        )
    except (
        IdentityDirectionalRawPreviewExecutionPlanError,
        IdentityDirectionalRawPreviewExecutionApprovalError,
        XNYSCalendarArtifactError,
        OSError,
    ) as exc:
        raise IdentityDirectionalRawPreviewRunnerError(
            "exact directional execution controls cannot be loaded"
        ) from exc
    if not isinstance(plan, S7DirectionalRawPreviewExecutionPlan) or not isinstance(
        approval, S7DirectionalRawPreviewExecutionApproval
    ):
        raise IdentityDirectionalRawPreviewRunnerError("execution controls have wrong types")
    return _LoadedControls(plan=plan, approval=approval, calendar=calendar)


def _verify_control_bindings(root: Path, controls: _LoadedControls) -> None:
    plan = controls.plan
    approval = controls.approval
    if plan.execution_data_root != str(root) or approval.execution_data_root != str(root):
        raise IdentityDirectionalRawPreviewRunnerError("execution data root differs")
    expected = {
        "plan_id": plan.plan_id,
        "plan_sha256": plan.sha256,
        "request_event_id": approval.request_event_id,
        "request_event_sha256": approval.request_event_sha256,
        "input_binding_digest": plan.input_binding_digest,
        "resource_caps_digest": plan.resource_caps.digest,
        "source_binding_manifest_id": plan.source_binding_manifest_id,
        "source_binding_manifest_sha256": plan.source_binding_manifest_sha256,
        "manifest_preflight_intent_id": plan.manifest_preflight_intent_id,
        "manifest_preflight_intent_path": plan.manifest_preflight_intent_path,
        "manifest_preflight_intent_sha256": plan.manifest_preflight_intent_sha256,
        "source_artifact_set_digest": plan.source_artifact_set_digest,
        "scope_set_id": plan.scope_set_id,
        "scope_set_sha256": plan.scope_set_sha256,
        "algorithm_digest": plan.algorithm_digest,
        "qa_semantics_digest": plan.qa_semantics_digest,
        "contract_id": plan.contract_id,
        "contract_schema_digest": plan.contract_schema_digest,
        "contract_candidate_sha256": plan.contract_candidate_sha256,
        "runtime_file_set_digest": plan.runtime_file_set_digest,
        "verification_file_set_digest": plan.verification_file_set_digest,
        "registry_semantics_digest": (
            DIRECTIONAL_RAW_PREVIEW_REGISTRY_EXCLUSIVITY_SEMANTICS_DIGEST
        ),
        "inventory_completion_id": plan.inventory_completion_id,
        "inventory_completion_sha256": plan.inventory_completion_sha256,
    }
    for field_name, expected_value in expected.items():
        if getattr(approval, field_name, None) != expected_value:
            raise IdentityDirectionalRawPreviewRunnerError(
                f"execution approval crosses Plan at {field_name}"
            )
    approved = getattr(approval, "approved_at_utc", None)
    if (
        not isinstance(approved, datetime)
        or approved.tzinfo is None
        or approved > datetime.now(UTC)
    ):
        raise IdentityDirectionalRawPreviewRunnerError("execution approval time is invalid")
    required_true = (
        "preview_execution_authorized",
        "data_read_authorized",
        "parquet_read_authorized",
        "once_to_awaiting_review",
    )
    required_false = (
        "source_discovery_authorized",
        "caller_scope_override_authorized",
        "exact_group_history_read_authorized",
        "network_access_authorized",
        "external_evidence_capture_authorized",
        "registry_evaluation_authorized",
        "adjudication_authorized",
        "table_materialization_authorized",
        "full_run_authorized",
        "publication_authorized",
        "forced_liquidation_authorized",
    )
    if any(getattr(approval, field, None) is not True for field in required_true) or any(
        getattr(approval, field, None) is not False for field in required_false
    ):
        raise IdentityDirectionalRawPreviewRunnerError("execution approval capabilities drifted")
    _verify_bound_control_artifacts(root, plan)
    _verify_plan_source_files(root, plan.source_artifacts)


def _verify_plan_source_files(root: Path, pins: Sequence[object]) -> None:
    if len(pins) != 22:
        raise IdentityDirectionalRawPreviewRunnerError("source artifact set is not exact")
    for pin in pins:
        try:
            path = safe_relative_path(root, pin.path)
            expected_bytes = pin.bytes
            expected_sha = pin.sha256
        except (AttributeError, OSError) as exc:
            raise IdentityDirectionalRawPreviewRunnerError(
                "source artifact pin is malformed"
            ) from exc
        if (
            _sha256_regular_nofollow(path, expected_size=expected_bytes) != expected_sha
        ):
            raise IdentityDirectionalRawPreviewRunnerError(
                "source_artifact_integrity_invalid"
            )


def _verify_bound_control_artifacts(root: Path, plan: Any) -> None:
    source_binding = _read_bound_json(
        root,
        plan.source_binding_manifest_path,
        plan.source_binding_manifest_sha256,
        "S4 source binding",
    )
    if source_binding.get("source_binding_id") != plan.source_binding_manifest_id:
        raise IdentityDirectionalRawPreviewRunnerError("s4_source_binding_invalid")
    _verify_source_artifact_projection_domains(source_binding, plan)
    completion = _read_bound_json(
        root,
        plan.inventory_completion_path,
        plan.inventory_completion_sha256,
        "inventory completion",
    )
    candidate = _read_bound_json(
        root,
        plan.inventory_candidate_path,
        plan.inventory_candidate_manifest_sha256,
        "inventory candidate",
    )
    if (
        completion.get("completion_id") != plan.inventory_completion_id
        or candidate.get("candidate_id") != plan.inventory_candidate_id
    ):
        raise IdentityDirectionalRawPreviewRunnerError("inventory_binding_invalid")
    completion_candidate = completion.get("candidate")
    if not isinstance(completion_candidate, Mapping) or (
        completion_candidate.get("candidate_id") != plan.inventory_candidate_id
        or completion_candidate.get("path") != plan.inventory_candidate_path
        or completion_candidate.get("sha256") != plan.inventory_candidate_manifest_sha256
    ):
        raise IdentityDirectionalRawPreviewRunnerError("inventory_binding_invalid")
    artifacts = candidate.get("artifacts")
    if not isinstance(artifacts, list):
        raise IdentityDirectionalRawPreviewRunnerError("inventory_binding_invalid")
    data_refs = [
        item for item in artifacts if isinstance(item, Mapping) and item.get("role") == "data"
    ]
    if len(data_refs) != 1 or data_refs[0].get("sha256") != plan.inventory_candidate_data_sha256:
        raise IdentityDirectionalRawPreviewRunnerError("inventory_binding_invalid")


def _verify_source_artifact_projection_domains(
    source_binding: Mapping[str, object], plan: Any
) -> None:
    try:
        from ame_stocks_api.silver.identity_directional_raw_preview_execution_plan import (
            IdentityDirectionalRawPreviewExecutionPlanError,
            S7DirectionalRawPreviewExecutionSourcePin,
        )
        from ame_stocks_api.silver.identity_directional_raw_preview_manifest_plan import (
            IdentityDirectionalRawPreviewManifestPlanError,
            S7DirectionalRawPreviewSourceArtifactRef,
        )
    except ImportError as exc:
        raise IdentityDirectionalRawPreviewRunnerError(
            "s4_source_binding_invalid"
        ) from exc

    raw_documents = source_binding.get("source_artifacts")
    try:
        if not isinstance(raw_documents, list):
            raise TypeError("source artifacts must be a list")
        raw_refs = tuple(
            S7DirectionalRawPreviewSourceArtifactRef.from_dict(item)
            for item in raw_documents
        )
        execution_pins = tuple(plan.source_artifacts)
        if any(
            not isinstance(item, S7DirectionalRawPreviewExecutionSourcePin)
            for item in execution_pins
        ):
            raise TypeError("execution source pin has an untrusted type")
        normalized_refs = tuple(
            S7DirectionalRawPreviewExecutionSourcePin.from_source_ref(item)
            for item in raw_refs
        )
        raw_digest = stable_digest([item.to_dict() for item in raw_refs])
        normalized_digest = stable_digest([item.to_dict() for item in execution_pins])
    except (
        AttributeError,
        IdentityDirectionalRawPreviewExecutionPlanError,
        IdentityDirectionalRawPreviewManifestPlanError,
        KeyError,
        TypeError,
        ValueError,
    ) as exc:
        raise IdentityDirectionalRawPreviewRunnerError(
            "s4_source_binding_invalid"
        ) from exc

    if (
        source_binding.get("source_artifact_set_digest") != raw_digest
        or plan.source_artifact_set_digest != normalized_digest
        or normalized_refs != execution_pins
    ):
        raise IdentityDirectionalRawPreviewRunnerError("s4_source_binding_invalid")


def _read_bound_json(
    root: Path, relative: str, expected_sha256: str, label: str
) -> dict[str, object]:
    path = safe_relative_path(root, relative)
    if (
        not path.is_file()
        or path.is_symlink()
        or sha256_file(path) != expected_sha256
    ):
        raise IdentityDirectionalRawPreviewRunnerError(f"{label} is missing or altered")
    content = path.read_bytes()
    try:
        value = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise IdentityDirectionalRawPreviewRunnerError(f"{label} is not JSON") from exc
    if not isinstance(value, dict) or _canonical_bytes(value) != content:
        raise IdentityDirectionalRawPreviewRunnerError(f"{label} is not canonical JSON")
    return value


def _verify_git_checkout_and_pins(plan: Any) -> None:
    repo = Path(__file__).resolve().parents[3]
    head = _git(repo, "rev-parse", "HEAD")
    tree = _git(repo, "rev-parse", "HEAD^{tree}")
    if head != plan.execution_git_commit or tree != plan.execution_git_tree:
        raise IdentityDirectionalRawPreviewRunnerError("Git commit/tree differs from Plan")
    if _git(repo, "status", "--porcelain=v1", "--untracked-files=all"):
        raise IdentityDirectionalRawPreviewRunnerError("Git checkout is not clean")
    pins = tuple(plan.runtime_files) + tuple(plan.verification_files)
    if len({item.path for item in pins}) != len(pins):
        raise IdentityDirectionalRawPreviewRunnerError("runtime pins overlap")
    for pin in pins:
        path = safe_relative_path(repo, pin.path)
        if (
            not path.is_file()
            or path.is_symlink()
            or path.stat().st_size != pin.bytes
            or sha256_file(path) != pin.sha256
        ):
            raise IdentityDirectionalRawPreviewRunnerError(
                f"pinned runtime bytes differ: {pin.path}"
            )
        output = _git(repo, "ls-tree", plan.execution_git_commit, "--", pin.path)
        parts = output.split()
        if len(parts) < 4 or parts[1] != "blob" or parts[2] != pin.git_blob:
            raise IdentityDirectionalRawPreviewRunnerError(
                f"pinned Git blob differs: {pin.path}"
            )


def _execute_new(
    root: Path,
    controls: _LoadedControls,
    *,
    staging: Path,
    completion_path: Path,
    started: float,
    monitor: _ResourceMonitor,
) -> S7DirectionalRawPreviewExecutionCompletion:
    plan = controls.plan
    approval = controls.approval
    bundle = _open_exact_source_bundle(root, controls)
    source_refs = tuple(
        DirectionalSourceArtifactRef.from_plan_pin(item) for item in plan.source_artifacts
    )
    _verify_source_caps(source_refs, plan.resource_caps)
    _preflight_bundle(bundle, source_refs)
    monitor.check()
    created = datetime.now(UTC)
    try:
        evidence_authority = issue_directional_preview_evidence_authority(
            data_root=root,
            bundle=bundle,
            plan=plan,
            approval=approval,
            calendar=controls.calendar,
            created_at_utc=created,
        )
    except ProviderEvidenceError as exc:
        raise IdentityDirectionalRawPreviewRunnerError(
            "directional provider evidence authority cannot be issued"
        ) from exc
    engine = DirectionalRawPreviewEngine(
        selected_asset_row_cap=plan.resource_caps.selected_asset_row_cap,
        selected_universe_row_cap=plan.resource_caps.selected_universe_row_cap,
        selected_total_row_cap=plan.resource_caps.selected_total_source_row_cap,
        bounded_example_cap=20,
    )
    scanned_rows = 0
    selected_rows = 0
    by_ref = {(item.table, item.session_date): item for item in source_refs}
    sessions = sorted({item.session_date for item in source_refs})
    for table in SOURCE_TABLES:
        artifacts = bundle.daily_partition_artifacts(table, sessions)
        for artifact in artifacts:
            expected = by_ref[(table, _artifact_session(artifact))]
            if artifact.ref.path != expected.path:
                raise IdentityDirectionalRawPreviewRunnerError("source artifact path drifted")
            for batch in bundle.iter_physical_batches(
                table,
                columns=None,
                batch_size=plan.resource_caps.batch_size,
                artifacts=(artifact,),
            ):
                scanned_rows += batch.batch.num_rows
                selected_rows += engine.consume_physical_batch(
                    batch,
                    calendar=controls.calendar,
                    authority=evidence_authority,
                )
                if scanned_rows > plan.resource_caps.scanned_total_row_hard_cap:
                    raise IdentityDirectionalRawPreviewRunnerError("scanned row cap exceeded")
                monitor.check()
    if scanned_rows != sum(item.row_count for item in source_refs):
        raise IdentityDirectionalRawPreviewRunnerError("source_scan_count_mismatch")
    if selected_rows > plan.resource_caps.selected_total_source_row_cap:
        raise IdentityDirectionalRawPreviewRunnerError("selected row cap exceeded")
    retained = engine.retained_attestations
    if len(retained) != selected_rows:
        raise IdentityDirectionalRawPreviewRunnerError("scoped_source_omission_rows")
    try:
        replayed = replay_provider_row_attestations_from_official_bundle(
            retained,
            bundle=bundle,
            calendar=controls.calendar,
        )
    except ProviderEvidenceError as exc:
        raise IdentityDirectionalRawPreviewRunnerError(
            "row_attestation_replay_invalid_rows"
        ) from exc
    if [item.to_dict() for item in replayed] != [item.to_dict() for item in retained]:
        raise IdentityDirectionalRawPreviewRunnerError(
            "row_attestation_replay_invalid_rows"
        )
    # A second exact metadata/byte preflight closes mutation between scan and commit.
    _preflight_bundle(bundle, source_refs)
    monitor.check()
    evidence_authority.require(
        bundle=bundle,
        plan=plan,
        approval=approval,
        calendar=controls.calendar,
    )
    build = engine.finish(
        plan_id=plan.plan_id,
        plan_sha256=plan.sha256,
        approval_id=approval.approval_id,
        approval_sha256=approval.sha256,
        scope_set_id=plan.scope_set_id,
        source_artifacts=source_refs,
        calendar=controls.calendar,
        created_at_utc=created,
        runner_verified_critical_numerators=_runner_verified_critical_numerators(
            inventory_binding_verified=True,
            s4_source_binding_verified=True,
            source_artifact_integrity_verified=True,
            source_scan_count_verified=True,
            scoped_source_complete=True,
            provider_attestation_schema_verified=True,
            attestation_replay_verified=True,
            output_readback_required_before_publish=True,
            resource_caps_verified=True,
        ),
    )
    return _stage_commit_and_complete(
        root,
        controls,
        build,
        source_refs=source_refs,
        staging=staging,
        completion_path=completion_path,
        created_at_utc=created,
        monitor=monitor,
        bundle=bundle,
    )


def _open_exact_source_bundle(
    root: Path, controls: _LoadedControls
) -> IdentitySourceBundle:
    try:
        from ame_stocks_api.silver.identity_source import (
            open_approved_identity_directional_raw_preview_source_bundle,
        )

        return open_approved_identity_directional_raw_preview_source_bundle(
            root,
            plan_id=controls.plan.plan_id,
            expected_plan_sha256=controls.plan.sha256,
            approval_id=controls.approval.approval_id,
            expected_approval_sha256=controls.approval.sha256,
        )
    except (ImportError, IdentitySourceError, OSError) as exc:
        raise IdentityDirectionalRawPreviewRunnerError(
            "exact approved directional source bundle cannot open"
        ) from exc


def _preflight_bundle(
    bundle: IdentitySourceBundle, refs: Sequence[DirectionalSourceArtifactRef]
) -> None:
    bundle.require_official()
    if len(refs) != 22 or len({(item.table, item.session_date) for item in refs}) != 22:
        raise IdentityDirectionalRawPreviewRunnerError("execution requires exact 22 source refs")
    by_table = defaultdict(list)
    for item in refs:
        by_table[item.table].append(item)
    for table in SOURCE_TABLES:
        sessions = sorted(item.session_date for item in by_table[table])
        artifacts = bundle.daily_partition_artifacts(table, sessions)
        for expected, actual in zip(sorted(by_table[table]), artifacts, strict=True):
            physical = actual.path
            try:
                physical_sha256 = _sha256_regular_nofollow(
                    physical, expected_size=expected.bytes
                )
            except (IdentityDirectionalRawPreviewRunnerError, OSError) as exc:
                raise IdentityDirectionalRawPreviewRunnerError(
                    "source_artifact_integrity_invalid"
                ) from exc
            if (
                actual.ref.path != expected.path
                or actual.ref.sha256 != expected.sha256
                or actual.ref.bytes != expected.bytes
                or actual.ref.row_count != expected.row_count
                or actual.ref.schema_digest != expected.schema_digest
                or actual.release_id != expected.release_id
                or actual.release_manifest_sha256 != expected.release_manifest_sha256
                or bundle.sources[table].published.contract.contract_id
                != expected.source_contract_id
                or physical_sha256 != expected.sha256
            ):
                raise IdentityDirectionalRawPreviewRunnerError(
                    "source_artifact_integrity_invalid"
                )


def _scan_exact_selected_locators(
    bundle: IdentitySourceBundle,
    refs: Sequence[DirectionalSourceArtifactRef],
    *,
    batch_size: int,
    scanned_row_cap: int,
) -> tuple[tuple[str, str, str, int, int], ...]:
    """Rescan exact Plan artifacts so replayed evidence cannot omit matching rows."""

    bundle.require_official()
    by_ref = {(item.table, item.session_date): item for item in refs}
    sessions = sorted({item.session_date for item in refs})
    locators: list[tuple[str, str, str, int, int]] = []
    scanned = 0
    for table in SOURCE_TABLES:
        artifacts = bundle.daily_partition_artifacts(table, sessions)
        for artifact in artifacts:
            session = _artifact_session(artifact)
            expected = by_ref.get((table, session))
            if expected is None or artifact.ref.path != expected.path:
                raise IdentityDirectionalRawPreviewRunnerError(
                    "output_artifact_readback_invalid: physical artifact scope"
                )
            for batch in bundle.iter_physical_batches(
                table,
                columns=None,
                batch_size=batch_size,
                artifacts=(artifact,),
            ):
                rows = batch.batch.to_pylist()
                scanned += len(rows)
                if scanned > scanned_row_cap:
                    raise IdentityDirectionalRawPreviewRunnerError(
                        "resource_cap_exceeded: semantic replay scan"
                    )
                for offset, row in enumerate(rows):
                    value = row.get("session_date")
                    try:
                        row_session = (
                            value
                            if type(value) is date
                            else date.fromisoformat(str(value))
                        )
                    except ValueError as exc:
                        raise IdentityDirectionalRawPreviewRunnerError(
                            "output_artifact_readback_invalid: physical session"
                        ) from exc
                    if row_session != session:
                        raise IdentityDirectionalRawPreviewRunnerError(
                            "output_artifact_readback_invalid: partition session mismatch"
                        )
                    pair = (row.get("ticker"), row_session)
                    if pair in FIXED_PAIR_TO_CASE:
                        locators.append(
                            (
                                table,
                                artifact.release_id,
                                artifact.ref.path,
                                batch.row_group,
                                batch.row_index_in_group + offset,
                            )
                        )
    if scanned != sum(item.row_count for item in refs):
        raise IdentityDirectionalRawPreviewRunnerError(
            "output_artifact_readback_invalid: physical scan count"
        )
    result = tuple(sorted(locators))
    if len(set(result)) != len(result):
        raise IdentityDirectionalRawPreviewRunnerError(
            "output_artifact_readback_invalid: repeated physical locator"
        )
    return result


def _verify_source_caps(refs: Sequence[DirectionalSourceArtifactRef], caps: Any) -> None:
    asset_rows = sum(item.row_count for item in refs if item.table == ASSET_TABLE)
    universe_rows = sum(item.row_count for item in refs if item.table == UNIVERSE_TABLE)
    source_bytes = sum(item.bytes for item in refs)
    if (
        len(refs) != caps.expected_physical_artifact_count
        or asset_rows > caps.scanned_asset_row_hard_cap
        or universe_rows > caps.scanned_universe_row_hard_cap
        or asset_rows + universe_rows > caps.scanned_total_row_hard_cap
        or source_bytes > caps.source_bytes_hard_cap
    ):
        raise IdentityDirectionalRawPreviewRunnerError(
            "resource_cap_exceeded: source metadata"
        )


def _artifact_session(artifact: IdentitySourceArtifact) -> date:
    match = re.search(r"session_date=(\d{4}-\d{2}-\d{2})(?:/|$)", artifact.ref.path)
    if match is None:
        raise IdentityDirectionalRawPreviewRunnerError("daily artifact path has no session")
    return date.fromisoformat(match.group(1))


def _sha256_regular_nofollow(path: Path, *, expected_size: int) -> str:
    """Hash one regular file through a single no-follow descriptor."""

    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except OSError as exc:
        raise IdentityDirectionalRawPreviewRunnerError(
            "source artifact cannot open without following links"
        ) from exc
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode) or before.st_size != expected_size:
            raise IdentityDirectionalRawPreviewRunnerError(
                "source artifact descriptor metadata differs"
            )
        digest = hashlib.sha256()
        while True:
            chunk = os.read(fd, 1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
        after = os.fstat(fd)
        visible = os.lstat(path)
        identity = (before.st_dev, before.st_ino, before.st_size)
        if (
            (after.st_dev, after.st_ino, after.st_size) != identity
            or (visible.st_dev, visible.st_ino, visible.st_size) != identity
            or not stat.S_ISREG(visible.st_mode)
        ):
            raise IdentityDirectionalRawPreviewRunnerError(
                "source artifact path changed during hashing"
            )
        return digest.hexdigest()
    finally:
        os.close(fd)


def _stage_commit_and_complete(
    root: Path,
    controls: _LoadedControls,
    build: DirectionalPreviewBuild,
    *,
    source_refs: tuple[DirectionalSourceArtifactRef, ...],
    staging: Path,
    completion_path: Path,
    created_at_utc: datetime,
    monitor: _ResourceMonitor,
    bundle: IdentitySourceBundle,
) -> S7DirectionalRawPreviewExecutionCompletion:
    plan = controls.plan
    approval = controls.approval
    if created_at_utc < approval.approved_at_utc:
        raise IdentityDirectionalRawPreviewRunnerError("candidate predates execution approval")
    staging_candidate = staging / "candidate"
    staging_candidate.mkdir(parents=True, exist_ok=False)
    monitor.check()
    table = pa.Table.from_pylist(
        [dict(item) for item in build.slots],
        schema=IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_CONTRACT.arrow_schema,
    )
    slots_path = staging_candidate / SLOTS_FILENAME
    slots_path.parent.mkdir(parents=True, exist_ok=False)
    pq.write_table(table, slots_path, compression="zstd", use_dictionary=True)
    _fsync_regular_file(slots_path)
    monitor.check()
    json_payloads = {
        DIRECTIONAL_FILENAME: _canonical_bytes(build.directional_review),
        QA_FILENAME: _canonical_bytes(build.qa),
        EXAMPLES_FILENAME: _canonical_bytes(build.examples),
    }
    for relative, content in json_payloads.items():
        target = staging_candidate / relative
        target.parent.mkdir(parents=True, exist_ok=False)
        _write_exclusive(target, content)
        monitor.check()
    for manifest in build.evidence_manifests:
        target = staging_candidate / manifest.candidate_relative_path
        target.parent.mkdir(parents=True, exist_ok=False)
        _write_exclusive(target, manifest.content)
        monitor.check()
    artifacts = _output_refs(staging_candidate, build)
    candidate = S7DirectionalRawPreviewCandidate(
        created_by=plan.created_by,
        plan_id=plan.plan_id,
        plan_sha256=plan.sha256,
        approval_id=approval.approval_id,
        approval_sha256=approval.sha256,
        request_event_id=approval.request_event_id,
        request_event_sha256=approval.request_event_sha256,
        input_binding_digest=plan.input_binding_digest,
        source_artifacts=source_refs,
        source_artifact_set_digest=plan.source_artifact_set_digest,
        scope_set_id=plan.scope_set_id,
        scope_set_sha256=plan.scope_set_sha256,
        artifacts=artifacts,
        evidence_manifest_ids=tuple(item.manifest_id for item in build.evidence_manifests),
        created_at_utc=created_at_utc,
    )
    _write_exclusive(staging_candidate / MANIFEST_FILENAME, candidate.content)
    _fsync_tree_bottom_up(staging_candidate)
    validated_build = _validate_candidate_directory(
        staging_candidate,
        candidate,
        controls,
        bundle,
        expected_build=build,
    )
    monitor.check()
    staged_bytes = _tree_bytes(staging_candidate)
    if staged_bytes > plan.resource_caps.output_bytes_hard_cap:
        raise IdentityDirectionalRawPreviewRunnerError("resource_cap_exceeded: output bytes")
    final_candidate = safe_relative_path(
        root,
        f"manifests/silver/identity/directional-raw-preview-candidates/candidate_id={candidate.candidate_id}",
    )
    final_candidate.parent.mkdir(parents=True, exist_ok=True)
    if final_candidate.exists() or final_candidate.is_symlink():
        raise IdentityDirectionalRawPreviewRunnerError(
            "exclusive candidate target already exists"
        )
    completion_path.parent.mkdir(parents=True, exist_ok=True)
    if completion_path.exists() or completion_path.is_symlink():
        raise IdentityDirectionalRawPreviewRunnerError(
            "completion target already exists"
        )
    selected_asset = sum(
        int(row["asset_observation_match_count"]) for row in validated_build.slots
    )
    selected_universe = sum(
        int(row["universe_membership_count"]) for row in validated_build.slots
    )
    completion = _build_precommit_completion(
        controls,
        candidate=candidate,
        artifacts=artifacts,
        source_refs=source_refs,
        evidence_manifest_ids=tuple(
            item.manifest_id for item in validated_build.evidence_manifests
        ),
        candidate_tree_bytes=staged_bytes,
        selected_asset=selected_asset,
        selected_universe=selected_universe,
        output_slot_rows=len(validated_build.slots),
        current_staging_bytes=_tree_bytes(staging),
        monitor=monitor,
    )
    if completion.completed_at_utc < approval.approved_at_utc:
        raise IdentityDirectionalRawPreviewRunnerError("completion predates execution approval")
    staged_completion = staging / "completion.json"
    _write_exclusive(staged_completion, completion.content)
    _fsync_directory(staging)
    _validate_precommit_completion(
        root,
        staged_completion,
        completion,
        controls,
        candidate,
        validated_build,
        staging_candidate,
        source_refs,
        bundle,
    )
    _final_precommit_cap_check(
        root=root,
        staging=staging,
        monitor=monitor,
        completion=completion,
        caps=plan.resource_caps,
    )
    # The irreversible phase contains only no-replace publication primitives and
    # parent-directory fsyncs.  All semantic and resource checks are complete.
    _rename_directory_noreplace(staging_candidate, final_candidate)
    _publish_file_noreplace(staged_completion, completion_path)
    return completion


def _build_precommit_completion(
    controls: _LoadedControls,
    *,
    candidate: S7DirectionalRawPreviewCandidate,
    artifacts: tuple[DirectionalOutputArtifactRef, ...],
    source_refs: tuple[DirectionalSourceArtifactRef, ...],
    evidence_manifest_ids: tuple[str, ...],
    candidate_tree_bytes: int,
    selected_asset: int,
    selected_universe: int,
    output_slot_rows: int,
    current_staging_bytes: int,
    monitor: _ResourceMonitor,
) -> S7DirectionalRawPreviewExecutionCompletion:
    """Freeze a cutoff measurement and include projected completion bytes."""

    plan, approval = controls.plan, controls.approval
    completed_at = datetime.now(UTC)
    wall_seconds = monitor.elapsed
    projected_completion_bytes = 0
    completion: S7DirectionalRawPreviewExecutionCompletion | None = None
    for _ in range(8):
        maximum_tmp = max(
            monitor.maximum_tmp_bytes,
            current_staging_bytes + projected_completion_bytes,
        )
        completion = S7DirectionalRawPreviewExecutionCompletion(
            created_by=plan.created_by,
            plan_id=plan.plan_id,
            plan_sha256=plan.sha256,
            approval_id=approval.approval_id,
            approval_sha256=approval.sha256,
            request_event_id=approval.request_event_id,
            request_event_sha256=approval.request_event_sha256,
            input_binding_digest=plan.input_binding_digest,
            candidate_id=candidate.candidate_id,
            candidate_path=(
                "manifests/silver/identity/directional-raw-preview-candidates/"
                f"candidate_id={candidate.candidate_id}/manifest.json"
            ),
            candidate_sha256=candidate.sha256,
            candidate_created_at_utc=candidate.created_at_utc,
            control_binding=_expected_control_binding(plan, approval),
            source_artifact_set_digest=plan.source_artifact_set_digest,
            output_artifacts=artifacts,
            evidence_manifest_ids=evidence_manifest_ids,
            completed_at_utc=completed_at,
            wall_clock_seconds=wall_seconds,
            peak_rss_bytes=monitor.peak_rss_bytes,
            minimum_disk_free_bytes=monitor.minimum_disk_free_bytes,
            maximum_tmp_bytes=maximum_tmp,
            output_bytes=sum(item.bytes for item in artifacts),
            candidate_tree_bytes=candidate_tree_bytes,
            source_artifact_count=len(source_refs),
            source_row_count=sum(item.row_count for item in source_refs),
            source_bytes=sum(item.bytes for item in source_refs),
            selected_asset_row_count=selected_asset,
            selected_universe_row_count=selected_universe,
            output_slot_row_count=output_slot_rows,
            disk_free_warning_triggered=monitor.disk_warning_triggered,
        )
        next_size = len(completion.content)
        if next_size == projected_completion_bytes and maximum_tmp == max(
            monitor.maximum_tmp_bytes, current_staging_bytes + next_size
        ):
            break
        projected_completion_bytes = next_size
    else:
        raise IdentityDirectionalRawPreviewRunnerError(
            "completion projected size did not converge"
        )
    assert completion is not None
    if completion.maximum_tmp_bytes != max(
        monitor.maximum_tmp_bytes, current_staging_bytes + len(completion.content)
    ):
        raise IdentityDirectionalRawPreviewRunnerError(
            "completion projected tmp measurement differs"
        )
    return completion


def _validate_precommit_completion(
    root: Path,
    staged_path: Path,
    expected_completion: S7DirectionalRawPreviewExecutionCompletion,
    controls: _LoadedControls,
    candidate: S7DirectionalRawPreviewCandidate,
    build: DirectionalPreviewBuild,
    candidate_dir: Path,
    source_refs: tuple[DirectionalSourceArtifactRef, ...],
    bundle: IdentitySourceBundle,
) -> None:
    if (
        not staged_path.is_file()
        or staged_path.is_symlink()
        or staged_path.read_bytes() != expected_completion.content
        or sha256_file(staged_path) != expected_completion.sha256
    ):
        raise IdentityDirectionalRawPreviewRunnerError(
            "precommit completion bytes differ"
        )
    try:
        parsed = S7DirectionalRawPreviewExecutionCompletion.from_dict(
            json.loads(staged_path.read_bytes())
        )
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise IdentityDirectionalRawPreviewRunnerError(
            "precommit completion is malformed"
        ) from exc
    if parsed != expected_completion:
        raise IdentityDirectionalRawPreviewRunnerError(
            "precommit completion object differs"
        )
    rebuilt = _validate_candidate_directory(
        candidate_dir,
        candidate,
        controls,
        bundle,
        expected_build=build,
    )
    _validate_completion_bindings_and_caps(
        root,
        parsed,
        controls,
        candidate,
        rebuilt,
        candidate_dir,
        source_refs,
    )


def _validate_completion_bindings_and_caps(
    root: Path,
    completion: S7DirectionalRawPreviewExecutionCompletion,
    controls: _LoadedControls,
    candidate: S7DirectionalRawPreviewCandidate,
    build: DirectionalPreviewBuild,
    candidate_dir: Path,
    source_refs: tuple[DirectionalSourceArtifactRef, ...],
) -> None:
    plan, approval = controls.plan, controls.approval
    expected_candidate_path = (
        "manifests/silver/identity/directional-raw-preview-candidates/"
        f"candidate_id={candidate.candidate_id}/manifest.json"
    )
    selected_asset = sum(
        int(row["asset_observation_match_count"]) for row in build.slots
    )
    selected_universe = sum(
        int(row["universe_membership_count"]) for row in build.slots
    )
    expected_evidence_ids = tuple(
        sorted(item.manifest_id for item in build.evidence_manifests)
    )
    if (
        completion.created_by != plan.created_by
        or completion.plan_id != plan.plan_id
        or completion.plan_sha256 != plan.sha256
        or completion.approval_id != approval.approval_id
        or completion.approval_sha256 != approval.sha256
        or completion.request_event_id != approval.request_event_id
        or completion.request_event_sha256 != approval.request_event_sha256
        or completion.input_binding_digest != plan.input_binding_digest
        or dict(completion.control_binding) != _expected_control_binding(plan, approval)
        or completion.source_artifact_set_digest != plan.source_artifact_set_digest
        or completion.candidate_id != candidate.candidate_id
        or completion.candidate_path != expected_candidate_path
        or completion.candidate_sha256 != candidate.sha256
        or completion.candidate_created_at_utc != candidate.created_at_utc
        or completion.output_artifacts != candidate.artifacts
        or completion.evidence_manifest_ids != expected_evidence_ids
        or completion.completed_at_utc < candidate.created_at_utc
        or completion.completed_at_utc < approval.approved_at_utc
        or completion.completion_state != CANDIDATE_STATE
    ):
        raise IdentityDirectionalRawPreviewRunnerError(
            "completion crosses exact controls or candidate"
        )
    actual_tree_bytes = _tree_bytes(candidate_dir)
    if (
        completion.source_artifact_count != len(source_refs)
        or completion.source_row_count != sum(item.row_count for item in source_refs)
        or completion.source_bytes != sum(item.bytes for item in source_refs)
        or completion.selected_asset_row_count != selected_asset
        or completion.selected_universe_row_count != selected_universe
        or completion.output_slot_row_count != len(build.slots)
        or completion.output_bytes != sum(item.bytes for item in candidate.artifacts)
        or completion.candidate_tree_bytes != actual_tree_bytes
        or completion.maximum_tmp_bytes < actual_tree_bytes
    ):
        raise IdentityDirectionalRawPreviewRunnerError(
            "completion counts or byte measurements differ"
        )
    caps = plan.resource_caps
    if (
        completion.source_artifact_count != caps.expected_physical_artifact_count
        or completion.source_row_count > caps.scanned_total_row_hard_cap
        or completion.source_bytes > caps.source_bytes_hard_cap
        or completion.selected_asset_row_count > caps.selected_asset_row_cap
        or completion.selected_universe_row_count > caps.selected_universe_row_cap
        or completion.selected_asset_row_count + completion.selected_universe_row_count
        > caps.selected_total_source_row_cap
        or completion.output_slot_row_count != caps.output_slot_row_cap
        or completion.output_bytes > caps.output_bytes_hard_cap
        or completion.candidate_tree_bytes > caps.output_bytes_hard_cap
        or completion.maximum_tmp_bytes > caps.temporary_bytes_hard_cap
        or completion.peak_rss_bytes > caps.rss_bytes_hard_cap
        or completion.wall_clock_seconds > caps.wall_clock_seconds_hard_cap
        or completion.minimum_disk_free_bytes < caps.disk_free_floor_bytes
        or completion.disk_free_warning_triggered
        != (completion.minimum_disk_free_bytes < caps.disk_free_warning_bytes)
        or completion.resource_measurement_cutoff != RESOURCE_MEASUREMENT_CUTOFF
    ):
        raise IdentityDirectionalRawPreviewRunnerError(
            "completion resource caps are invalid"
        )
    if root != Path(plan.execution_data_root):
        raise IdentityDirectionalRawPreviewRunnerError(
            "completion execution root differs"
        )


def _final_precommit_cap_check(
    *,
    root: Path,
    staging: Path,
    monitor: _ResourceMonitor,
    completion: S7DirectionalRawPreviewExecutionCompletion,
    caps: Any,
) -> None:
    elapsed = time.monotonic() - monitor.started
    rss = _peak_rss_bytes()
    free = shutil.disk_usage(root).free
    temporary = _tree_bytes(staging)
    if (
        elapsed > caps.wall_clock_seconds_hard_cap
        or rss > caps.rss_bytes_hard_cap
        or free < caps.disk_free_floor_bytes
        or temporary > caps.temporary_bytes_hard_cap
        or temporary != completion.maximum_tmp_bytes
        or completion.output_bytes > caps.output_bytes_hard_cap
        or completion.candidate_tree_bytes > caps.output_bytes_hard_cap
    ):
        raise IdentityDirectionalRawPreviewRunnerError(
            "resource_cap_exceeded: final precommit cutoff"
        )


def _output_refs(
    staging_candidate: Path, build: DirectionalPreviewBuild
) -> tuple[DirectionalOutputArtifactRef, ...]:
    refs = [
        DirectionalOutputArtifactRef(
            role="review_slots",
            path=SLOTS_FILENAME,
            sha256=sha256_file(staging_candidate / SLOTS_FILENAME),
            bytes=(staging_candidate / SLOTS_FILENAME).stat().st_size,
            media_type="application/vnd.apache.parquet",
            row_count=len(build.slots),
        ),
        *(
            DirectionalOutputArtifactRef(
                role=role,
                path=path,
                sha256=sha256_file(staging_candidate / path),
                bytes=(staging_candidate / path).stat().st_size,
                media_type="application/json",
            )
            for role, path in (
                ("directional_review", DIRECTIONAL_FILENAME),
                ("qa", QA_FILENAME),
                ("bounded_examples", EXAMPLES_FILENAME),
            )
        ),
        *(
            DirectionalOutputArtifactRef(
                role=f"case_evidence:{manifest.review_case['ticker']}",
                path=manifest.candidate_relative_path,
                sha256=manifest.sha256,
                bytes=len(manifest.content),
                media_type="application/json",
            )
            for manifest in build.evidence_manifests
        ),
    ]
    return tuple(sorted(refs, key=lambda item: item.role))


def _validate_candidate_directory(
    directory: Path,
    candidate: S7DirectionalRawPreviewCandidate,
    controls: _LoadedControls,
    bundle: IdentitySourceBundle,
    *,
    expected_build: DirectionalPreviewBuild | None = None,
) -> DirectionalPreviewBuild:
    """Independently rebuild every derived output from replayed provider rows."""

    plan, approval = controls.plan, controls.approval
    manifest_path = directory / MANIFEST_FILENAME
    try:
        manifest_bytes = manifest_path.read_bytes()
        parsed_candidate = S7DirectionalRawPreviewCandidate.from_dict(
            json.loads(manifest_bytes)
        )
    except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise IdentityDirectionalRawPreviewRunnerError(
            "output_artifact_readback_invalid: candidate manifest"
        ) from exc
    source_refs = tuple(
        sorted(
            DirectionalSourceArtifactRef.from_plan_pin(item)
            for item in plan.source_artifacts
        )
    )
    if (
        manifest_bytes != candidate.content
        or parsed_candidate != candidate
        or sha256_file(manifest_path) != candidate.sha256
        or candidate.created_by != plan.created_by
        or candidate.plan_id != plan.plan_id
        or candidate.plan_sha256 != plan.sha256
        or candidate.approval_id != approval.approval_id
        or candidate.approval_sha256 != approval.sha256
        or candidate.request_event_id != approval.request_event_id
        or candidate.request_event_sha256 != approval.request_event_sha256
        or candidate.input_binding_digest != plan.input_binding_digest
        or candidate.source_artifacts != source_refs
        or candidate.source_artifact_set_digest != plan.source_artifact_set_digest
        or candidate.scope_set_id != plan.scope_set_id
        or candidate.scope_set_sha256 != plan.scope_set_sha256
        or candidate.created_at_utc < approval.approved_at_utc
        or candidate.created_at_utc < plan.created_at_utc
    ):
        raise IdentityDirectionalRawPreviewRunnerError(
            "output_artifact_readback_invalid: candidate control binding"
        )
    expected_files = {MANIFEST_FILENAME, *(item.path for item in candidate.artifacts)}
    if _tree_relative_files(directory) != expected_files:
        raise IdentityDirectionalRawPreviewRunnerError(
            "output_artifact_readback_invalid: candidate file set"
        )
    by_role = {item.role: item for item in candidate.artifacts}
    for item in candidate.artifacts:
        path = safe_relative_path(directory, item.path)
        if (
            not path.is_file()
            or path.is_symlink()
            or path.stat(follow_symlinks=False).st_size != item.bytes
            or sha256_file(path) != item.sha256
            or (item.path, item.media_type, item.row_count)
            != _FIXED_OUTPUT_ROLE_SPECS[item.role]
        ):
            raise IdentityDirectionalRawPreviewRunnerError(
                "output_artifact_readback_invalid: output ref"
            )

    evidence_by_ticker: dict[str, DirectionalCaseEvidenceManifest] = {}
    all_attestations: list[ProviderRowAttestation] = []
    all_evidence_sources: list[DirectionalSourceArtifactRef] = []
    for case in FIXED_CASES:
        ticker = str(case["ticker"])
        role = f"case_evidence:{ticker}"
        document = _read_candidate_json(directory, by_role[role].path)
        manifest = DirectionalCaseEvidenceManifest.from_dict(document)
        if (
            manifest.review_case != case
            or manifest.candidate_relative_path != by_role[role].path
            or manifest.plan_id != plan.plan_id
            or manifest.plan_sha256 != plan.sha256
            or manifest.approval_id != approval.approval_id
            or manifest.approval_sha256 != approval.sha256
            or manifest.scope_set_id != plan.scope_set_id
            or manifest.created_at_utc != candidate.created_at_utc
        ):
            raise IdentityDirectionalRawPreviewRunnerError(
                "output_artifact_readback_invalid: evidence control binding"
            )
        evidence_by_ticker[ticker] = manifest
        all_attestations.extend(manifest.row_attestations)
        all_evidence_sources.extend(manifest.source_artifacts)
    if (
        tuple(sorted(candidate.evidence_manifest_ids))
        != tuple(sorted(item.manifest_id for item in evidence_by_ticker.values()))
        or tuple(sorted(all_evidence_sources)) != source_refs
        or len({item.row_attestation_id for item in all_attestations})
        != len(all_attestations)
        or len({item.locator for item in all_attestations}) != len(all_attestations)
    ):
        raise IdentityDirectionalRawPreviewRunnerError(
            "output_artifact_readback_invalid: evidence lineage set"
        )
    retained = tuple(sorted(all_attestations, key=lambda item: item.locator))
    physical_locators = _scan_exact_selected_locators(
        bundle,
        source_refs,
        batch_size=plan.resource_caps.batch_size,
        scanned_row_cap=plan.resource_caps.scanned_total_row_hard_cap,
    )
    if physical_locators != tuple(item.locator for item in retained):
        raise IdentityDirectionalRawPreviewRunnerError(
            "output_artifact_readback_invalid: scoped evidence omission"
        )
    try:
        replayed = replay_provider_row_attestations_from_official_bundle(
            retained,
            bundle=bundle,
            calendar=controls.calendar,
        )
    except ProviderEvidenceError as exc:
        raise IdentityDirectionalRawPreviewRunnerError(
            "output_artifact_readback_invalid: evidence replay"
        ) from exc
    if [item.to_dict() for item in replayed] != [item.to_dict() for item in retained]:
        raise IdentityDirectionalRawPreviewRunnerError(
            "output_artifact_readback_invalid: evidence replay"
        )
    engine = DirectionalRawPreviewEngine(
        selected_asset_row_cap=plan.resource_caps.selected_asset_row_cap,
        selected_universe_row_cap=plan.resource_caps.selected_universe_row_cap,
        selected_total_row_cap=plan.resource_caps.selected_total_source_row_cap,
        bounded_example_cap=20,
    )
    engine.consume_attestations(retained)
    rebuilt = engine.finish(
        plan_id=plan.plan_id,
        plan_sha256=plan.sha256,
        approval_id=approval.approval_id,
        approval_sha256=approval.sha256,
        scope_set_id=plan.scope_set_id,
        source_artifacts=source_refs,
        calendar=controls.calendar,
        created_at_utc=candidate.created_at_utc,
        runner_verified_critical_numerators=_runner_verified_critical_numerators(
            inventory_binding_verified=True,
            s4_source_binding_verified=True,
            source_artifact_integrity_verified=True,
            source_scan_count_verified=True,
            scoped_source_complete=True,
            provider_attestation_schema_verified=True,
            attestation_replay_verified=True,
            output_readback_required_before_publish=True,
            resource_caps_verified=True,
        ),
    )
    if expected_build is not None and (
        [dict(item) for item in expected_build.slots]
        != [dict(item) for item in rebuilt.slots]
        or dict(expected_build.directional_review) != dict(rebuilt.directional_review)
        or dict(expected_build.qa) != dict(rebuilt.qa)
        or dict(expected_build.examples) != dict(rebuilt.examples)
        or [item.content for item in expected_build.evidence_manifests]
        != [item.content for item in rebuilt.evidence_manifests]
    ):
        raise IdentityDirectionalRawPreviewRunnerError(
            "output_artifact_readback_invalid: in-memory build differs"
        )
    slots_path = directory / SLOTS_FILENAME
    parquet = pq.ParquetFile(slots_path)
    if (
        not parquet.schema_arrow.equals(
            IDENTITY_DIRECTIONAL_RAW_PREVIEW_SLOT_CONTRACT.arrow_schema
        )
        or parquet.metadata.num_rows != DIRECTIONAL_RAW_PREVIEW_FIXED_PAIR_COUNT
    ):
        raise IdentityDirectionalRawPreviewRunnerError(
            "output_artifact_readback_invalid: slot schema/count"
        )
    readback = parquet.read().to_pylist()
    expected = [dict(item) for item in rebuilt.slots]
    if readback != expected:
        raise IdentityDirectionalRawPreviewRunnerError(
            "output_artifact_readback_invalid: slot values"
        )
    ordered = [(str(item["ticker"]), item["session_date"]) for item in readback]
    expected_pairs = sorted(FIXED_PAIR_TO_CASE)
    if ordered != expected_pairs or len(set(ordered)) != len(ordered):
        raise IdentityDirectionalRawPreviewRunnerError(
            "output_artifact_readback_invalid: slot order/key"
        )
    for row in readback:
        if (
            row["registry_evaluation_state"]
            != DIRECTIONAL_RAW_PREVIEW_REGISTRY_EVALUATION_STATE
            or row["interval_inference_state"]
            != DIRECTIONAL_RAW_PREVIEW_INTERVAL_INFERENCE_STATE
            or row["provider_id"] != DIRECTIONAL_RAW_PREVIEW_PROVIDER_ID
            or row["provider_locale"] != DIRECTIONAL_RAW_PREVIEW_PROVIDER_LOCALE
            or row["provider_market"] != DIRECTIONAL_RAW_PREVIEW_PROVIDER_MARKET
            or any(
                row[field] is not False
                for field in (
                    "adjudication_eligible",
                    "canonical_candidate_eligible",
                    "full_run_eligible",
                    "publication_eligible",
                )
            )
        ):
            raise IdentityDirectionalRawPreviewRunnerError(
                "output_artifact_readback_invalid: slot review-only flags"
            )
    for role in ("directional_review", "qa", "bounded_examples"):
        document = _read_candidate_json(directory, by_role[role].path)
        expected_document = {
            "directional_review": dict(rebuilt.directional_review),
            "qa": dict(rebuilt.qa),
            "bounded_examples": dict(rebuilt.examples),
        }[role]
        if document != expected_document:
            raise IdentityDirectionalRawPreviewRunnerError(
                "output_artifact_readback_invalid: JSON content"
            )
        if document.get("registry_evaluation_state") != (
            DIRECTIONAL_RAW_PREVIEW_REGISTRY_EVALUATION_STATE
        ):
            raise IdentityDirectionalRawPreviewRunnerError(
                "output_artifact_readback_invalid: registry state"
            )
    for manifest in rebuilt.evidence_manifests:
        evidence_path = safe_relative_path(directory, manifest.candidate_relative_path)
        parsed = evidence_by_ticker[str(manifest.review_case["ticker"])]
        if (
            evidence_path.read_bytes() != manifest.content
            or parsed.content != manifest.content
            or parsed.manifest_id != manifest.manifest_id
        ):
            raise IdentityDirectionalRawPreviewRunnerError(
                "output_artifact_readback_invalid: evidence manifest ID"
            )
    if candidate.artifacts != _output_refs(directory, rebuilt):
        raise IdentityDirectionalRawPreviewRunnerError(
            "output_artifact_readback_invalid: recomputed output refs"
        )
    return rebuilt


def _tree_relative_files(root: Path) -> set[str]:
    files: set[str] = set()
    for current, directories, names in os.walk(root, followlinks=False):
        current_path = Path(current)
        for name in directories:
            path = current_path / name
            if path.is_symlink() or not path.is_dir():
                raise IdentityDirectionalRawPreviewRunnerError(
                    "output_artifact_readback_invalid: unsafe directory"
                )
        for name in names:
            path = current_path / name
            if path.is_symlink() or not path.is_file():
                raise IdentityDirectionalRawPreviewRunnerError(
                    "output_artifact_readback_invalid: unsafe file"
                )
            files.add(path.relative_to(root).as_posix())
    return files


def _read_candidate_json(root: Path, relative: str) -> dict[str, object]:
    path = safe_relative_path(root, relative)
    content = path.read_bytes()
    try:
        document = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise IdentityDirectionalRawPreviewRunnerError(
            "output_artifact_readback_invalid: JSON"
        ) from exc
    if not isinstance(document, dict) or _canonical_bytes(document) != content:
        raise IdentityDirectionalRawPreviewRunnerError(
            "output_artifact_readback_invalid: canonical JSON"
        )
    return document


def _recover_after_candidate_commit(
    root: Path,
    controls: _LoadedControls,
    *,
    staging: Path,
    completion_path: Path,
) -> S7DirectionalRawPreviewExecutionCompletion:
    """Resume only the exact post-rename/pre-link fail-closed state."""

    staged_completion = staging / "completion.json"
    if (
        not staging.is_dir()
        or staging.is_symlink()
        or _tree_relative_files(staging) != {"completion.json"}
        or not staged_completion.is_file()
        or staged_completion.is_symlink()
        or completion_path.exists()
        or completion_path.is_symlink()
    ):
        raise IdentityDirectionalRawPreviewRunnerError(
            "stale staging is not an exact recoverable commit state"
        )
    try:
        content = staged_completion.read_bytes()
        completion = S7DirectionalRawPreviewExecutionCompletion.from_dict(
            json.loads(content)
        )
    except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise IdentityDirectionalRawPreviewRunnerError(
            "recoverable staged completion is malformed"
        ) from exc
    if completion.content != content:
        raise IdentityDirectionalRawPreviewRunnerError(
            "recoverable staged completion bytes differ"
        )
    candidate_path = safe_relative_path(root, completion.candidate_path)
    if not candidate_path.is_file() or candidate_path.is_symlink():
        raise IdentityDirectionalRawPreviewRunnerError(
            "recoverable stable candidate is missing"
        )
    try:
        candidate = S7DirectionalRawPreviewCandidate.from_dict(
            json.loads(candidate_path.read_bytes())
        )
    except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise IdentityDirectionalRawPreviewRunnerError(
            "recoverable stable candidate is malformed"
        ) from exc
    source_refs = tuple(
        sorted(
            DirectionalSourceArtifactRef.from_plan_pin(item)
            for item in controls.plan.source_artifacts
        )
    )
    _verify_source_caps(source_refs, controls.plan.resource_caps)
    bundle = _open_exact_source_bundle(root, controls)
    _preflight_bundle(bundle, source_refs)
    build = _validate_candidate_directory(
        candidate_path.parent,
        candidate,
        controls,
        bundle,
    )
    _validate_completion_bindings_and_caps(
        root,
        completion,
        controls,
        candidate,
        build,
        candidate_path.parent,
        source_refs,
    )
    completion_path.parent.mkdir(parents=True, exist_ok=True)
    _publish_file_noreplace(staged_completion, completion_path)
    return completion


def _read_completion(
    root: Path, path: Path, controls: _LoadedControls
) -> S7DirectionalRawPreviewExecutionCompletion:
    plan, approval = controls.plan, controls.approval
    expected_path = safe_relative_path(
        root, _execution_paths(plan.plan_id, approval.approval_id)["completion"]
    )
    if (
        path != expected_path
        or not path.is_file()
        or path.is_symlink()
    ):
        raise IdentityDirectionalRawPreviewRunnerError("completion path is unsafe")
    try:
        content = path.read_bytes()
        document = json.loads(content)
        completion = S7DirectionalRawPreviewExecutionCompletion.from_dict(document)
    except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise IdentityDirectionalRawPreviewRunnerError("completion is malformed") from exc
    if completion.content != content:
        raise IdentityDirectionalRawPreviewRunnerError("completion canonical bytes differ")
    candidate_path = safe_relative_path(root, completion.candidate_path)
    if (
        not candidate_path.is_file()
        or candidate_path.is_symlink()
        or sha256_file(candidate_path) != completion.candidate_sha256
    ):
        raise IdentityDirectionalRawPreviewRunnerError("candidate manifest changed")
    try:
        candidate_document = json.loads(candidate_path.read_bytes())
        candidate = S7DirectionalRawPreviewCandidate.from_dict(candidate_document)
    except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise IdentityDirectionalRawPreviewRunnerError("candidate is malformed") from exc
    candidate_dir = candidate_path.parent
    source_refs = tuple(
        sorted(
            DirectionalSourceArtifactRef.from_plan_pin(item)
            for item in plan.source_artifacts
        )
    )
    _verify_source_caps(source_refs, plan.resource_caps)
    bundle = _open_exact_source_bundle(root, controls)
    _preflight_bundle(bundle, source_refs)
    build = _validate_candidate_directory(
        candidate_dir,
        candidate,
        controls,
        bundle,
    )
    _validate_completion_bindings_and_caps(
        root,
        completion,
        controls,
        candidate,
        build,
        candidate_dir,
        source_refs,
    )
    return completion


def _execution_paths(plan_id: str, approval_id: str) -> dict[str, str]:
    return {
        "completion": (
            "manifests/silver/identity/directional-raw-preview-execution-completions/"
            f"plan_id={plan_id}/approval_id={approval_id}/manifest.json"
        ),
        "lock": (
            "manifests/silver/identity/directional-raw-preview-execution-locks/"
            f"plan_id={plan_id}/approval_id={approval_id}.lock"
        ),
        "staging": (
            "tmp/silver/identity/directional-raw-preview/"
            f"plan_id={plan_id}/approval_id={approval_id}"
        ),
    }


def _validated_root(value: Path) -> Path:
    if not isinstance(value, Path):
        raise IdentityDirectionalRawPreviewRunnerError("data_root must be a Path")
    root = value.expanduser().resolve()
    if not root.is_dir() or root.is_symlink():
        raise IdentityDirectionalRawPreviewRunnerError("data_root is unsafe")
    return root


class _exclusive_lock:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.fd: int | None = None

    def __enter__(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.fd = os.open(self.path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            before = os.fstat(self.fd)
            after = os.lstat(self.path)
            if not stat.S_ISREG(after.st_mode) or (before.st_dev, before.st_ino) != (
                after.st_dev,
                after.st_ino,
            ):
                raise IdentityDirectionalRawPreviewRunnerError("lock path was replaced")
        except BaseException:
            os.close(self.fd)
            self.fd = None
            raise

    def __exit__(self, *_: object) -> None:
        assert self.fd is not None
        fcntl.flock(self.fd, fcntl.LOCK_UN)
        os.close(self.fd)
        self.fd = None


def _write_exclusive(path: Path, content: bytes) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(fd, "wb", closefd=True) as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
    except BaseException:
        raise


def _publish_file_noreplace(source: Path, target: Path) -> None:
    """Publish one fully fsynced file atomically without a partial final path."""

    if not source.is_file() or source.is_symlink():
        raise IdentityDirectionalRawPreviewRunnerError("staged completion is unsafe")
    try:
        os.link(source, target, follow_symlinks=False)
    except FileExistsError as exc:
        raise IdentityDirectionalRawPreviewRunnerError(
            "completion target already exists"
        ) from exc
    except OSError as exc:
        raise IdentityDirectionalRawPreviewRunnerError(
            "completion no-clobber publication failed"
        ) from exc
    if (
        not target.is_file()
        or target.is_symlink()
        or target.stat(follow_symlinks=False).st_ino
        != source.stat(follow_symlinks=False).st_ino
        or target.read_bytes() != source.read_bytes()
    ):
        raise IdentityDirectionalRawPreviewRunnerError(
            "completion publication postcondition failed"
        )
    _fsync_directory(target.parent)


def _rename_directory_noreplace(source: Path, target: Path) -> None:
    """Atomically publish a directory without replacing any existing target."""

    if not source.is_dir() or source.is_symlink():
        raise IdentityDirectionalRawPreviewRunnerError("candidate staging is unsafe")
    source_stat = source.stat(follow_symlinks=False)
    try:
        _exclusive_rename_primitive(source, target)
    except OSError as exc:
        import errno

        if exc.errno in {errno.EEXIST, errno.ENOTEMPTY}:
            raise IdentityDirectionalRawPreviewRunnerError(
                "exclusive candidate target already exists"
            ) from exc
        if exc.errno in {
            errno.EINVAL,
            errno.ENOSYS,
            errno.EXDEV,
            errno.EOPNOTSUPP,
            getattr(errno, "ENOTSUP", errno.EOPNOTSUPP),
        }:
            raise IdentityDirectionalRawPreviewRunnerError(
                "filesystem lacks exact no-clobber directory rename"
            ) from exc
        raise IdentityDirectionalRawPreviewRunnerError(
            "exclusive candidate rename failed"
        ) from exc
    if source.exists() or source.is_symlink() or not target.is_dir() or target.is_symlink():
        raise IdentityDirectionalRawPreviewRunnerError(
            "exclusive candidate rename postcondition failed"
        )
    target_stat = target.stat(follow_symlinks=False)
    if (source_stat.st_dev, source_stat.st_ino) != (target_stat.st_dev, target_stat.st_ino):
        raise IdentityDirectionalRawPreviewRunnerError(
            "exclusive candidate rename identity changed"
        )
    _fsync_directory(source.parent)
    _fsync_directory(target.parent)


def _exclusive_rename_primitive(source: Path, target: Path) -> None:
    import ctypes
    import errno

    libc = ctypes.CDLL(None, use_errno=True)
    encoded_source = os.fsencode(source)
    encoded_target = os.fsencode(target)
    ctypes.set_errno(0)
    if sys.platform.startswith("linux"):
        rename = getattr(libc, "renameat2", None)
        if rename is None:
            raise OSError(errno.ENOSYS, "renameat2 unavailable")
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
            raise OSError(errno.ENOSYS, "renamex_np unavailable")
        rename.argtypes = (ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint)
        rename.restype = ctypes.c_int
        result = rename(encoded_source, encoded_target, 0x00000004)
    else:
        raise OSError(errno.ENOSYS, "exclusive rename unavailable")
    if result != 0:
        number = ctypes.get_errno() or errno.EIO
        raise OSError(number, os.strerror(number), str(target))


def _fsync_directory(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _fsync_regular_file(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except OSError as exc:
        raise IdentityDirectionalRawPreviewRunnerError(
            "staged file cannot be opened for fsync"
        ) from exc
    try:
        metadata = os.fstat(fd)
        if not stat.S_ISREG(metadata.st_mode):
            raise IdentityDirectionalRawPreviewRunnerError(
                "staged fsync target is not regular"
            )
        os.fsync(fd)
    finally:
        os.close(fd)


def _fsync_tree_bottom_up(root: Path) -> None:
    if not root.is_dir() or root.is_symlink():
        raise IdentityDirectionalRawPreviewRunnerError(
            "candidate fsync root is unsafe"
        )
    directories: list[Path] = []
    for current, children, files in os.walk(root, topdown=True, followlinks=False):
        current_path = Path(current)
        directories.append(current_path)
        for name in children:
            child = current_path / name
            if child.is_symlink() or not child.is_dir():
                raise IdentityDirectionalRawPreviewRunnerError(
                    "candidate fsync directory is unsafe"
                )
        for name in files:
            item = current_path / name
            if item.is_symlink() or not item.is_file():
                raise IdentityDirectionalRawPreviewRunnerError(
                    "candidate fsync file is unsafe"
                )
    for directory in sorted(directories, key=lambda item: len(item.parts), reverse=True):
        _fsync_directory(directory)


def _tree_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    total = 0
    for current, directories, files in os.walk(path, followlinks=False):
        current_path = Path(current)
        if any((current_path / name).is_symlink() for name in directories):
            raise IdentityDirectionalRawPreviewRunnerError("staging contains a symlink")
        for name in files:
            item = current_path / name
            if item.is_symlink() or not item.is_file():
                raise IdentityDirectionalRawPreviewRunnerError("staging file is unsafe")
            total += item.stat().st_size
    return total


def _peak_rss_bytes() -> int:
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(value) if sys.platform == "darwin" else int(value) * 1024


def _git(root: Path, *arguments: str) -> str:
    result = subprocess.run(
        ("git", *arguments),
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if result.returncode != 0:
        raise IdentityDirectionalRawPreviewRunnerError("Git verification failed")
    return result.stdout.strip()


__all__ = [
    "DirectionalCaseEvidenceManifest",
    "DirectionalOutputArtifactRef",
    "DirectionalPreviewBuild",
    "DirectionalRawPreviewEngine",
    "DirectionalSourceArtifactRef",
    "IdentityDirectionalRawPreviewRunnerError",
    "S7DirectionalRawPreviewCandidate",
    "S7DirectionalRawPreviewExecutionCompletion",
    "run_exact_s7_directional_raw_preview",
]
