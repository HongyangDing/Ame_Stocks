"""Fail-closed loader for the three S7 exact-group source scopes.

The exact-group history candidate is review evidence, not a registry decision.
This module turns only an explicitly pinned candidate/completion pair into the
four source scopes needed by the SOR, XZO, and ANABV registry decisions.  It
never discovers a candidate, follows a ``latest`` pointer, infers an interval
from an observed run, or imports the registry workflow module.

Every returned source row is replayed from the selected-parent
``ProviderRowAttestation`` embedded in the candidate's immutable per-group
evidence manifest.  The provider-observed FIGIs remain lineage; this loader
does not emit a canonical identity or authorize a registry release.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from functools import lru_cache
from pathlib import Path
from types import MappingProxyType
from typing import Final, Protocol

import pyarrow as pa
import pyarrow.parquet as pq

from ame_stocks_api.artifacts import ArtifactError, safe_relative_path, stable_digest
from ame_stocks_api.silver.calendar_artifact import build_xnys_calendar_artifact
from ame_stocks_api.silver.contracts import QAStatus
from ame_stocks_api.silver.identity_exact_group_history_contract import (
    EXACT_GROUP_HISTORY_CAPABILITIES,
    EXACT_GROUP_HISTORY_FIXED_COMPOSITES,
    EXACT_GROUP_HISTORY_FIXED_GROUPS,
    EXACT_GROUP_HISTORY_OBSERVED_INTERVAL_STATE,
    EXACT_GROUP_HISTORY_PROVIDER_ID,
    EXACT_GROUP_HISTORY_PROVIDER_LOCALE,
    EXACT_GROUP_HISTORY_PROVIDER_MARKET,
    EXACT_GROUP_HISTORY_REGISTRY_EVALUATION_STATE,
    EXACT_GROUP_HISTORY_S4_RELEASE_SET_ID,
    EXACT_GROUP_HISTORY_S4_SOURCE_ARTIFACT_COUNT,
    EXACT_GROUP_HISTORY_S4_SOURCE_BYTES,
    EXACT_GROUP_HISTORY_S4_SOURCE_ROW_COUNT,
    IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_CONTRACT,
    exact_group_history_review_group_id,
)
from ame_stocks_api.silver.identity_exact_group_history_runner import (
    ASSET_TABLE,
    EXAMPLES_FILENAME,
    MANIFEST_FILENAME,
    QA_FILENAME,
    SEQUENCES_FILENAME,
    SLOTS_FILENAME,
    ExactGroupHistoryEvidenceManifestV2,
    ExactGroupHistoryOutputRef,
    IdentityExactGroupHistoryRunnerError,
    S7ExactGroupHistoryCandidate,
    S7ExactGroupHistoryCompletion,
    exact_group_history_completion_path,
)
from ame_stocks_api.silver.identity_market_inventory_engine import (
    UNIVERSE_PARENT_PROJECTION,
)
from ame_stocks_api.silver.identity_provider_evidence import (
    ProviderEvidenceError,
    ProviderRowAttestation,
)

_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_FIGI = re.compile(r"^BBG[0-9A-Z]{9}$")
_MIC = re.compile(r"^[A-Z0-9]{4}$")

SOR_OLD_COMPOSITE: Final = "BBG000KMY6N2"
SOR_OLD_SHARE_CLASS: Final = "BBG001S5W848"
SOR_SUCCESSOR_SHARE_CLASS: Final = "BBG01RK6N5G9"
XZO_CONTAMINATED_SHARE_CLASS: Final = "BBG01XL8FJS7"
ANABV_CONTAMINATED_SHARE_CLASS: Final = "BBG0026ZDHT8"

SOR_PREDECESSOR_SESSION: Final = date(2024, 12, 31)
SOR_SUCCESSOR_SESSION: Final = date(2025, 1, 2)
SOR_OVERRIDE_END_SESSION: Final = date(2026, 7, 9)
XZO_SCOPE_SESSIONS: Final = (date(2025, 11, 4), date(2025, 11, 5))
ANABV_SCOPE_SESSIONS: Final = (date(2026, 4, 6),)

_EXPECTED_CAPABILITIES: Final = MappingProxyType(
    {
        **dict(EXACT_GROUP_HISTORY_CAPABILITIES),
        "forced_liquidation": False,
        "membership_mutation": False,
        "source_discovery": False,
    }
)

_EXPECTED_OUTPUT_LAYOUT: Final = MappingProxyType(
    {
        "review_slots": (SLOTS_FILENAME, "application/vnd.apache.parquet", True),
        "group_sequences": (SEQUENCES_FILENAME, "application/json", False),
        "qa": (QA_FILENAME, "application/json", False),
        "bounded_examples": (EXAMPLES_FILENAME, "application/json", False),
    }
)


class IdentityRegistryExactGroupScopeError(RuntimeError):
    """Raised when a pinned exact-group candidate cannot support source scopes."""


class ExactArtifactBindingLike(Protocol):
    """Structural subset shared with the registry workflow's artifact binding."""

    artifact_id: str
    path: str
    sha256: str
    bytes: int


@dataclass(frozen=True, slots=True)
class _ArtifactPin:
    artifact_id: str
    path: str
    sha256: str
    bytes: int

    def __post_init__(self) -> None:
        _digest(self.artifact_id, "artifact ID")
        _relative_path(self.path, "artifact path")
        _digest(self.sha256, "artifact SHA-256")
        if type(self.bytes) is not int or self.bytes <= 0:
            raise IdentityRegistryExactGroupScopeError(
                "artifact bytes must be a positive native integer"
            )


@dataclass(frozen=True, slots=True, order=True)
class ExactGroupRegistrySourceRow:
    """One selected provider parent reproduced from immutable exact-group evidence."""

    session_date: date
    source_record_id: str
    source_dataset: str
    source_s4_release_set_id: str
    provider_id: str
    provider_market: str
    provider_locale: str
    ticker: str
    observed_composite_figi: str
    observed_share_class_figi: str | None
    primary_exchange_mic: str | None

    def __post_init__(self) -> None:
        if type(self.session_date) is not date:
            raise IdentityRegistryExactGroupScopeError("source session must be a date")
        _digest(self.source_record_id, "source record ID")
        if self.source_dataset != ASSET_TABLE:
            raise IdentityRegistryExactGroupScopeError(
                "registry source row must be an asset observation"
            )
        if self.source_s4_release_set_id != EXACT_GROUP_HISTORY_S4_RELEASE_SET_ID:
            raise IdentityRegistryExactGroupScopeError("source S4 release-set ID differs")
        if (
            self.provider_id != EXACT_GROUP_HISTORY_PROVIDER_ID
            or self.provider_market != EXACT_GROUP_HISTORY_PROVIDER_MARKET
            or self.provider_locale != EXACT_GROUP_HISTORY_PROVIDER_LOCALE
        ):
            raise IdentityRegistryExactGroupScopeError(
                "registry source row must be massive/stocks/us"
            )
        if not self.ticker:
            raise IdentityRegistryExactGroupScopeError("source ticker is empty")
        _figi(self.observed_composite_figi, "observed Composite FIGI")
        if self.observed_share_class_figi is not None:
            _figi(self.observed_share_class_figi, "observed Share Class FIGI")
        if self.primary_exchange_mic is not None and not _MIC.fullmatch(self.primary_exchange_mic):
            raise IdentityRegistryExactGroupScopeError("source primary exchange MIC is invalid")

    def to_dict(self) -> dict[str, object]:
        """Return the exact shape consumed by ``ExactSourceRow.from_dict``."""

        return {
            "observed_composite_figi": self.observed_composite_figi,
            "observed_share_class_figi": self.observed_share_class_figi,
            "primary_exchange_mic": self.primary_exchange_mic,
            "provider_id": self.provider_id,
            "provider_locale": self.provider_locale,
            "provider_market": self.provider_market,
            "session_date": self.session_date.isoformat(),
            "source_dataset": self.source_dataset,
            "source_record_id": self.source_record_id,
            "source_s4_release_set_id": self.source_s4_release_set_id,
            "ticker": self.ticker,
        }


@dataclass(frozen=True, slots=True)
class ExactGroupRegistrySourceScope:
    """A complete, sorted exact-row scope suitable for one registry decision."""

    rows: tuple[ExactGroupRegistrySourceRow, ...]

    def __post_init__(self) -> None:
        rows = tuple(sorted(self.rows))
        if not rows or rows != self.rows:
            raise IdentityRegistryExactGroupScopeError(
                "registry source rows must be nonempty and sorted"
            )
        ids = tuple(row.source_record_id for row in rows)
        if len(ids) != len(set(ids)):
            raise IdentityRegistryExactGroupScopeError("registry source record IDs are repeated")

    @property
    def source_record_ids(self) -> tuple[str, ...]:
        return tuple(sorted(row.source_record_id for row in self.rows))

    @property
    def source_record_set_digest(self) -> str:
        return stable_digest(list(self.source_record_ids))

    @property
    def scope_digest(self) -> str:
        return stable_digest([row.to_dict() for row in self.rows])

    def to_dict(self) -> dict[str, object]:
        """Return the exact shape consumed by ``ExactSourceScope.from_dict``."""

        return {
            "row_count": len(self.rows),
            "rows": [row.to_dict() for row in self.rows],
            "scope_digest": self.scope_digest,
            "source_record_set_digest": self.source_record_set_digest,
        }


@dataclass(frozen=True, slots=True)
class LoadedExactGroupRegistryScopes:
    """Fully replayed exact-group evidence and its four allowed decision scopes."""

    candidate_id: str
    candidate_sha256: str
    completion_id: str
    completion_sha256: str
    evidence_manifest_ids: tuple[str, ...]
    scopes: Mapping[str, ExactGroupRegistrySourceScope]

    def __post_init__(self) -> None:
        for value, label in (
            (self.candidate_id, "candidate ID"),
            (self.candidate_sha256, "candidate SHA-256"),
            (self.completion_id, "completion ID"),
            (self.completion_sha256, "completion SHA-256"),
        ):
            _digest(value, label)
        evidence = tuple(sorted(self.evidence_manifest_ids))
        if len(evidence) != 3 or len(set(evidence)) != 3:
            raise IdentityRegistryExactGroupScopeError(
                "loaded source scopes require exactly three evidence manifests"
            )
        expected = {
            "asset_transition:SOR",
            "provider_composite_override:SOR",
            "share_class_adjudication:XZO",
            "share_class_adjudication:ANABV",
        }
        scopes = dict(self.scopes)
        if set(scopes) != expected or any(
            type(scope) is not ExactGroupRegistrySourceScope for scope in scopes.values()
        ):
            raise IdentityRegistryExactGroupScopeError(
                "loaded exact-group decision scope set differs"
            )
        object.__setattr__(self, "evidence_manifest_ids", evidence)
        object.__setattr__(self, "scopes", MappingProxyType(scopes))

    def require_scope(self, case_key: str) -> ExactGroupRegistrySourceScope:
        """Return one fixed scope or fail; fuzzy case lookup is forbidden."""

        try:
            return self.scopes[case_key]
        except KeyError as exc:
            raise IdentityRegistryExactGroupScopeError(
                "requested case is outside the four exact-group registry scopes"
            ) from exc


def load_identity_registry_exact_group_scopes(
    data_root: Path,
    *,
    candidate_pin: ExactArtifactBindingLike | Mapping[str, object],
    completion_pin: ExactArtifactBindingLike | Mapping[str, object],
) -> LoadedExactGroupRegistryScopes:
    """Load exactly one pinned candidate/completion pair and derive four scopes.

    The two paths come only from the caller.  No directory enumeration is used
    to select a candidate or completion; enumeration is confined to proving
    that the already-pinned candidate directory contains exactly its declared
    seven outputs plus ``manifest.json``.
    """

    root = data_root.expanduser().resolve()
    candidate_binding = _coerce_pin(candidate_pin, "candidate pin")
    completion_binding = _coerce_pin(completion_pin, "completion pin")
    candidate_content, candidate_path = _read_pinned(root, candidate_binding)
    completion_content, _completion_path = _read_pinned(root, completion_binding)

    candidate_document = _canonical_json_document(candidate_content, "candidate")
    completion_document = _canonical_json_document(completion_content, "completion")
    try:
        candidate = S7ExactGroupHistoryCandidate.from_dict(candidate_document)
        completion = S7ExactGroupHistoryCompletion.from_dict(completion_document)
    except (IdentityExactGroupHistoryRunnerError, TypeError, ValueError) as exc:
        raise IdentityRegistryExactGroupScopeError(
            "candidate or completion is not canonical exact-group output"
        ) from exc

    _validate_candidate_completion_bindings(
        candidate,
        completion,
        candidate_binding=candidate_binding,
        completion_binding=completion_binding,
    )
    candidate_directory = candidate_path.parent
    _validate_candidate_file_set(candidate_directory, candidate)
    refs = {item.role: item for item in candidate.artifacts}
    slot_rows = _load_and_validate_slots(candidate_directory, refs["review_slots"], completion)
    _validate_qa(candidate_directory, refs["qa"], len(slot_rows))
    _load_canonical_json_output(candidate_directory, refs["group_sequences"], "group sequences")
    _load_canonical_json_output(candidate_directory, refs["bounded_examples"], "bounded examples")
    evidence = _load_evidence_manifests(candidate_directory, candidate, refs)
    selected_parents = _replay_selected_parents(slot_rows, evidence, candidate)
    scopes = _derive_fixed_scopes(slot_rows, selected_parents, evidence)

    if _tree_bytes(candidate_directory) != completion.output_bytes:
        raise IdentityRegistryExactGroupScopeError(
            "completion output bytes differ from the exact candidate tree"
        )
    return LoadedExactGroupRegistryScopes(
        candidate_id=candidate.candidate_id,
        candidate_sha256=candidate.sha256,
        completion_id=completion.completion_id,
        completion_sha256=completion.sha256,
        evidence_manifest_ids=candidate.evidence_manifest_ids,
        scopes=scopes,
    )


def _coerce_pin(value: ExactArtifactBindingLike | Mapping[str, object], label: str) -> _ArtifactPin:
    try:
        if isinstance(value, Mapping):
            artifact_id = value["artifact_id"]
            path = value["path"]
            sha256 = value["sha256"]
            byte_count = value["bytes"]
        else:
            artifact_id = value.artifact_id
            path = value.path
            sha256 = value.sha256
            byte_count = value.bytes
        return _ArtifactPin(
            artifact_id=str(artifact_id),
            path=str(path),
            sha256=str(sha256),
            bytes=byte_count if type(byte_count) is int else -1,
        )
    except (KeyError, AttributeError, TypeError) as exc:
        raise IdentityRegistryExactGroupScopeError(f"{label} is malformed") from exc


def _read_pinned(root: Path, pin: _ArtifactPin) -> tuple[bytes, Path]:
    try:
        path = safe_relative_path(root, pin.path)
    except ArtifactError as exc:
        raise IdentityRegistryExactGroupScopeError("pinned artifact path is unsafe") from exc
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise IdentityRegistryExactGroupScopeError("pinned artifact is missing") from exc
    if path.is_symlink() or not stat.S_ISREG(metadata.st_mode) or metadata.st_size != pin.bytes:
        raise IdentityRegistryExactGroupScopeError("pinned artifact type or bytes differ")
    try:
        content = path.read_bytes()
    except OSError as exc:
        raise IdentityRegistryExactGroupScopeError("pinned artifact cannot be read") from exc
    if hashlib.sha256(content).hexdigest() != pin.sha256:
        raise IdentityRegistryExactGroupScopeError("pinned artifact SHA-256 differs")
    return content, path


def _validate_candidate_completion_bindings(
    candidate: S7ExactGroupHistoryCandidate,
    completion: S7ExactGroupHistoryCompletion,
    *,
    candidate_binding: _ArtifactPin,
    completion_binding: _ArtifactPin,
) -> None:
    expected_candidate_path = f"{candidate.relative_directory}/{MANIFEST_FILENAME}"
    expected_completion_path = exact_group_history_completion_path(
        candidate.plan_id, candidate.approval_id
    )
    if (
        candidate_binding.artifact_id != candidate.candidate_id
        or candidate_binding.sha256 != candidate.sha256
        or candidate_binding.path != expected_candidate_path
        or completion_binding.artifact_id != completion.completion_id
        or completion_binding.sha256 != completion.sha256
        or completion_binding.path != expected_completion_path
    ):
        raise IdentityRegistryExactGroupScopeError(
            "candidate or completion pin does not bind its canonical identity/path"
        )
    if (
        completion.candidate_id != candidate.candidate_id
        or completion.candidate_path != expected_candidate_path
        or completion.candidate_sha256 != candidate.sha256
        or completion.output_artifacts != candidate.artifacts
        or completion.plan_id != candidate.plan_id
        or completion.plan_sha256 != candidate.plan_sha256
        or completion.approval_id != candidate.approval_id
        or completion.approval_sha256 != candidate.approval_sha256
        or completion.request_event_id != candidate.request_event_id
        or completion.request_event_sha256 != candidate.request_event_sha256
        or completion.execution_intent_id != candidate.execution_intent_id
        or completion.execution_intent_path != candidate.execution_intent_path
        or completion.execution_intent_sha256 != candidate.execution_intent_sha256
        or completion.completed_at_utc < candidate.created_at_utc
    ):
        raise IdentityRegistryExactGroupScopeError("candidate/completion cross-bindings differ")
    if (
        completion.source_artifact_count != EXACT_GROUP_HISTORY_S4_SOURCE_ARTIFACT_COUNT
        or completion.source_row_count != EXACT_GROUP_HISTORY_S4_SOURCE_ROW_COUNT
        or completion.source_bytes != EXACT_GROUP_HISTORY_S4_SOURCE_BYTES
    ):
        raise IdentityRegistryExactGroupScopeError(
            "completion full-S4 source totals differ from the frozen release"
        )
    if len(candidate.artifacts) != 7 or len(completion.output_artifacts) != 7:
        raise IdentityRegistryExactGroupScopeError(
            "candidate/completion must bind exactly seven output refs"
        )
    refs = {item.role: item for item in candidate.artifacts}
    for role, (path, media_type, has_rows) in _EXPECTED_OUTPUT_LAYOUT.items():
        ref = refs.get(role)
        if (
            ref is None
            or ref.path != path
            or ref.media_type != media_type
            or (has_rows and ref.row_count is None)
            or (not has_rows and ref.row_count is not None)
        ):
            raise IdentityRegistryExactGroupScopeError("candidate core output ref layout differs")
    for ticker, _ in EXACT_GROUP_HISTORY_FIXED_GROUPS:
        ref = refs.get(f"group_evidence:{ticker}")
        if ref is None or ref.media_type != "application/json" or ref.row_count is not None:
            raise IdentityRegistryExactGroupScopeError(
                "candidate group-evidence output ref layout differs"
            )


def _validate_candidate_file_set(directory: Path, candidate: S7ExactGroupHistoryCandidate) -> None:
    expected = {MANIFEST_FILENAME, *(item.path for item in candidate.artifacts)}
    actual: set[str] = set()
    try:
        for current, directories, files in os.walk(directory, followlinks=False):
            current_path = Path(current)
            for name in directories:
                child = current_path / name
                if child.is_symlink():
                    raise IdentityRegistryExactGroupScopeError(
                        "candidate directory contains a symlink"
                    )
            for name in files:
                child = current_path / name
                if child.is_symlink() or not child.is_file():
                    raise IdentityRegistryExactGroupScopeError(
                        "candidate directory contains a non-regular output"
                    )
                actual.add(child.relative_to(directory).as_posix())
    except OSError as exc:
        raise IdentityRegistryExactGroupScopeError(
            "candidate directory cannot be enumerated"
        ) from exc
    if actual != expected:
        raise IdentityRegistryExactGroupScopeError(
            "candidate directory file set differs from the exact seven refs"
        )
    for ref in candidate.artifacts:
        _read_output_ref(directory, ref)


def _read_output_ref(directory: Path, ref: ExactGroupHistoryOutputRef) -> bytes:
    try:
        path = safe_relative_path(directory, ref.path)
    except ArtifactError as exc:
        raise IdentityRegistryExactGroupScopeError("candidate output path is unsafe") from exc
    try:
        metadata = path.lstat()
        content = path.read_bytes()
    except OSError as exc:
        raise IdentityRegistryExactGroupScopeError("candidate output cannot be read") from exc
    if (
        path.is_symlink()
        or not stat.S_ISREG(metadata.st_mode)
        or metadata.st_size != ref.bytes
        or hashlib.sha256(content).hexdigest() != ref.sha256
    ):
        raise IdentityRegistryExactGroupScopeError(
            "candidate output hash or bytes differ from its ref"
        )
    return content


def _load_and_validate_slots(
    directory: Path,
    ref: ExactGroupHistoryOutputRef,
    completion: S7ExactGroupHistoryCompletion,
) -> tuple[Mapping[str, object], ...]:
    content = _read_output_ref(directory, ref)
    try:
        table = pq.read_table(pa.BufferReader(content))
    except (OSError, pa.ArrowException) as exc:
        raise IdentityRegistryExactGroupScopeError("review-slot Parquet cannot be read") from exc
    if not table.schema.equals(IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_CONTRACT.arrow_schema):
        raise IdentityRegistryExactGroupScopeError("review-slot Arrow schema differs")
    if ref.row_count != table.num_rows or completion.output_slot_row_count != table.num_rows:
        raise IdentityRegistryExactGroupScopeError(
            "review-slot row count differs from candidate/completion"
        )
    rows = tuple(table.to_pylist())
    if not rows:
        raise IdentityRegistryExactGroupScopeError("review-slot table is empty")
    sort_keys = tuple((str(row["ticker"]), row["session_date"]) for row in rows)
    primary_keys = tuple((str(row["review_group_id"]), row["session_date"]) for row in rows)
    if sort_keys != tuple(sorted(sort_keys)):
        raise IdentityRegistryExactGroupScopeError("review-slot output sort differs")
    if len(primary_keys) != len(set(primary_keys)):
        raise IdentityRegistryExactGroupScopeError("review-slot primary key is duplicated")
    expected_groups = dict(EXACT_GROUP_HISTORY_FIXED_GROUPS)
    for row in rows:
        ticker = str(row["ticker"])
        composite = str(row["exact_group_observed_composite_figi"])
        if (
            expected_groups.get(ticker) != composite
            or row["review_group_id"]
            != exact_group_history_review_group_id(ticker=ticker, observed_composite_figi=composite)
            or row["s4_release_set_id"] != EXACT_GROUP_HISTORY_S4_RELEASE_SET_ID
            or row["provider_id"] != EXACT_GROUP_HISTORY_PROVIDER_ID
            or row["provider_market"] != EXACT_GROUP_HISTORY_PROVIDER_MARKET
            or row["provider_locale"] != EXACT_GROUP_HISTORY_PROVIDER_LOCALE
            or row["observed_interval_state"] != EXACT_GROUP_HISTORY_OBSERVED_INTERVAL_STATE
            or row["registry_evaluation_state"] != EXACT_GROUP_HISTORY_REGISTRY_EVALUATION_STATE
            or any(
                row[field] is not False
                for field in (
                    "adjudication_eligible",
                    "canonical_candidate_eligible",
                    "transition_candidate_eligible",
                    "exact_override_interval_eligible",
                    "full_run_eligible",
                    "publication_eligible",
                )
            )
        ):
            raise IdentityRegistryExactGroupScopeError(
                "review-slot scope/capability controls differ"
            )
    return rows


def _load_canonical_json_output(
    directory: Path, ref: ExactGroupHistoryOutputRef, label: str
) -> Mapping[str, object]:
    content = _read_output_ref(directory, ref)
    return _canonical_json_document(content, label)


def _validate_qa(directory: Path, ref: ExactGroupHistoryOutputRef, row_count: int) -> None:
    qa = _load_canonical_json_output(directory, ref, "QA")
    expected_keys = {
        "artifact_type",
        "capabilities",
        "checks",
        "critical_failure_count",
        "registry_evaluation_state",
        "schema_version",
        "warning_count",
    }
    checks = qa.get("checks")
    if (
        set(qa) != expected_keys
        or qa.get("artifact_type") != "s7_exact_group_history_qa"
        or qa.get("capabilities") != dict(_EXPECTED_CAPABILITIES)
        or qa.get("registry_evaluation_state") != EXACT_GROUP_HISTORY_REGISTRY_EVALUATION_STATE
        or qa.get("schema_version") != 1
        or not isinstance(checks, list)
    ):
        raise IdentityRegistryExactGroupScopeError("QA control document differs")
    expected_rules = {
        rule.check_id: rule for rule in IDENTITY_EXACT_GROUP_HISTORY_REVIEW_SLOT_CONTRACT.qa_rules
    }
    if len(checks) != len(expected_rules):
        raise IdentityRegistryExactGroupScopeError("QA check set differs")
    observed: set[str] = set()
    warnings = 0
    for item in checks:
        if not isinstance(item, Mapping) or set(item) != {
            "check_id",
            "denominator",
            "numerator",
            "severity",
            "status",
        }:
            raise IdentityRegistryExactGroupScopeError("QA check is malformed")
        check_id = item.get("check_id")
        rule = expected_rules.get(check_id)
        numerator = item.get("numerator")
        if (
            rule is None
            or not isinstance(check_id, str)
            or check_id in observed
            or type(numerator) is not int
            or numerator < 0
            or item.get("denominator") != row_count
            or item.get("severity") != rule.severity.value
        ):
            raise IdentityRegistryExactGroupScopeError("QA check binding differs")
        expected_status = (
            QAStatus.FAILED.value
            if rule.severity.value == "critical" and numerator
            else QAStatus.WARNING.value
            if rule.severity.value == "high" and numerator
            else QAStatus.PASSED.value
        )
        if item.get("status") != expected_status:
            raise IdentityRegistryExactGroupScopeError("QA check status differs")
        if rule.severity.value == "critical" and numerator:
            raise IdentityRegistryExactGroupScopeError("candidate has nonzero Critical QA")
        if expected_status == QAStatus.WARNING.value:
            warnings += 1
        observed.add(check_id)
    if (
        observed != set(expected_rules)
        or qa.get("critical_failure_count") != 0
        or qa.get("warning_count") != warnings
    ):
        raise IdentityRegistryExactGroupScopeError("QA summary differs")


def _load_evidence_manifests(
    directory: Path,
    candidate: S7ExactGroupHistoryCandidate,
    refs: Mapping[str, ExactGroupHistoryOutputRef],
) -> Mapping[str, ExactGroupHistoryEvidenceManifestV2]:
    evidence_by_ticker: dict[str, ExactGroupHistoryEvidenceManifestV2] = {}
    attestation_ids: set[str] = set()
    for ticker, composite in EXACT_GROUP_HISTORY_FIXED_GROUPS:
        ref = refs[f"group_evidence:{ticker}"]
        content = _read_output_ref(directory, ref)
        document = _canonical_json_document(content, f"{ticker} evidence")
        try:
            evidence = ExactGroupHistoryEvidenceManifestV2.from_dict(document)
        except (IdentityExactGroupHistoryRunnerError, ProviderEvidenceError) as exc:
            raise IdentityRegistryExactGroupScopeError(
                "group evidence manifest is invalid"
            ) from exc
        if (
            evidence.ticker != ticker
            or evidence.exact_group_observed_composite_figi != composite
            or ref.path != evidence.candidate_relative_path
            or ref.sha256 != evidence.sha256
            or evidence.plan_id != candidate.plan_id
            or evidence.plan_sha256 != candidate.plan_sha256
            or evidence.approval_id != candidate.approval_id
            or evidence.approval_sha256 != candidate.approval_sha256
            or evidence.execution_intent_id != candidate.execution_intent_id
            or evidence.execution_intent_sha256 != candidate.execution_intent_sha256
            or evidence.review_scope_set_id != candidate.review_scope_set_id
            or evidence.source_artifact_set_digest != candidate.source_artifact_set_digest
            or evidence.normalized_source_artifact_set_digest
            != candidate.normalized_source_artifact_set_digest
            or evidence.created_at_utc != candidate.created_at_utc
        ):
            raise IdentityRegistryExactGroupScopeError("group evidence/candidate bindings differ")
        group_ids = {
            item.row_attestation_id
            for item in (*evidence.asset_attestations, *evidence.universe_attestations)
        }
        if attestation_ids.intersection(group_ids):
            raise IdentityRegistryExactGroupScopeError(
                "provider attestation is reused across evidence groups"
            )
        attestation_ids.update(group_ids)
        evidence_by_ticker[ticker] = evidence
    if {item.manifest_id for item in evidence_by_ticker.values()} != set(
        candidate.evidence_manifest_ids
    ):
        raise IdentityRegistryExactGroupScopeError("candidate evidence-manifest ID set differs")
    return MappingProxyType(evidence_by_ticker)


def _replay_selected_parents(
    slots: Sequence[Mapping[str, object]],
    evidence_by_ticker: Mapping[str, ExactGroupHistoryEvidenceManifestV2],
    candidate: S7ExactGroupHistoryCandidate,
) -> Mapping[tuple[str, date], ProviderRowAttestation | None]:
    selected: dict[tuple[str, date], ProviderRowAttestation | None] = {}
    for slot in slots:
        ticker = str(slot["ticker"])
        session = slot["session_date"]
        if type(session) is not date:
            raise IdentityRegistryExactGroupScopeError("slot session is not a date")
        evidence = evidence_by_ticker[ticker]
        if (
            slot["review_scope_set_id"] != candidate.review_scope_set_id
            or slot["exact_group_evidence_manifest_id"] != evidence.manifest_id
            or slot["exact_group_evidence_manifest_path"] != evidence.candidate_relative_path
            or slot["exact_group_evidence_manifest_sha256"] != evidence.sha256
        ):
            raise IdentityRegistryExactGroupScopeError("slot/evidence manifest binding differs")
        assets = tuple(
            item for item in evidence.asset_attestations if _attestation_session(item) == session
        )
        universes = tuple(
            item for item in evidence.universe_attestations if _attestation_session(item) == session
        )
        exact_assets = tuple(
            item
            for item in assets
            if _is_exact_group_asset(
                item,
                ticker=ticker,
                composite=str(slot["exact_group_observed_composite_figi"]),
            )
        )
        exact_ids = [item.row_attestation_id for item in exact_assets]
        slot_exact_ids = _canonical_json_array(
            slot["exact_asset_observation_attestation_ids_json"],
            "exact Asset attestation IDs",
        )
        if (
            slot_exact_ids != exact_ids
            or slot["exact_asset_observation_match_count"] != len(exact_assets)
            or slot["exact_group_observed_share_class_figis_json"]
            != _json_array_distinct(
                [item.full_row_snapshot.get("share_class_figi") for item in exact_assets]
            )
            or slot["exact_group_observed_ciks_json"]
            != _json_array_distinct([item.full_row_snapshot.get("cik") for item in exact_assets])
            or slot["exact_group_observed_primary_exchange_mics_json"]
            != _json_array_distinct(
                [item.full_row_snapshot.get("primary_exchange_mic") for item in exact_assets]
            )
            or slot["exact_group_observed_type_codes_json"]
            != _json_array_distinct(
                [item.full_row_snapshot.get("type_code") for item in exact_assets]
            )
            or slot["exact_group_provider_active_values_json"]
            != _json_array_distinct(
                [item.full_row_snapshot.get("provider_active") for item in exact_assets]
            )
        ):
            raise IdentityRegistryExactGroupScopeError(
                "slot exact-Asset attestation replay differs"
            )
        if slot["universe_membership_count"] != len(universes) or len(universes) > 1:
            raise IdentityRegistryExactGroupScopeError("slot universe evidence count differs")
        if not universes:
            if (
                any(
                    slot[field] is not None
                    for field in (
                        "universe_row_attestation_id",
                        "selected_source_record_id",
                        "selected_parent_attestation_id",
                        "selected_parent_projection_match",
                        "selected_parent_matches_exact_group",
                        "selected_parent_observed_composite_figi",
                        "selected_parent_observed_share_class_figi",
                        "selected_parent_observed_cik",
                        "selected_parent_observed_primary_exchange_mic",
                        "selected_parent_observed_type_code",
                        "selected_parent_source_available_session",
                    )
                )
                or slot["selected_parent_asset_match_count"] != 0
            ):
                raise IdentityRegistryExactGroupScopeError(
                    "absent membership has a selected parent"
                )
            selected[(ticker, session)] = None
            continue
        universe = universes[0]
        universe_row = universe.full_row_snapshot
        selected_id = universe_row.get("selected_source_record_id")
        parents = tuple(item for item in assets if item.source_record_id == selected_id)
        if len(parents) != 1:
            raise IdentityRegistryExactGroupScopeError(
                "selected source record does not resolve to exactly one parent attestation"
            )
        parent = parents[0]
        parent_row = parent.full_row_snapshot
        parent_exact = _is_exact_group_asset(
            parent,
            ticker=ticker,
            composite=str(slot["exact_group_observed_composite_figi"]),
        )
        projection_match = all(
            parent_row.get(asset_field) == universe_row.get(universe_field)
            for asset_field, universe_field in UNIVERSE_PARENT_PROJECTION
        )
        expected_membership = (
            "present_active" if universe_row.get("active_on_date") is True else "present_inactive"
        )
        if (
            slot["universe_row_attestation_id"] != universe.row_attestation_id
            or slot["selected_source_record_id"] != selected_id
            or slot["selected_parent_asset_match_count"] != 1
            or slot["selected_parent_attestation_id"] != parent.row_attestation_id
            or slot["selected_parent_projection_match"] is not projection_match
            or projection_match is not True
            or slot["selected_parent_matches_exact_group"] is not parent_exact
            or slot["selected_parent_observed_composite_figi"] != parent_row.get("composite_figi")
            or slot["selected_parent_observed_share_class_figi"]
            != parent_row.get("share_class_figi")
            or slot["selected_parent_observed_cik"] != parent_row.get("cik")
            or slot["selected_parent_observed_primary_exchange_mic"]
            != parent_row.get("primary_exchange_mic")
            or slot["selected_parent_observed_type_code"] != parent_row.get("type_code")
            or slot["selected_parent_source_available_session"] != parent.source_available_session
            or slot["active_on_date"] != universe_row.get("active_on_date")
            or slot["membership_status"] != expected_membership
        ):
            raise IdentityRegistryExactGroupScopeError(
                "selected-parent attestation replay differs from the slot"
            )
        selected[(ticker, session)] = parent
    return MappingProxyType(selected)


def _derive_fixed_scopes(
    slots: Sequence[Mapping[str, object]],
    selected: Mapping[tuple[str, date], ProviderRowAttestation | None],
    evidence: Mapping[str, ExactGroupHistoryEvidenceManifestV2],
) -> Mapping[str, ExactGroupRegistrySourceScope]:
    slots_by_ticker: dict[str, set[date]] = {
        ticker: set() for ticker, _ in EXACT_GROUP_HISTORY_FIXED_GROUPS
    }
    for slot in slots:
        slots_by_ticker[str(slot["ticker"])].add(slot["session_date"])

    override_sessions = _sor_override_sessions()
    expected_sor_slots = {SOR_PREDECESSOR_SESSION, *override_sessions}
    if slots_by_ticker["SOR"] != expected_sor_slots:
        raise IdentityRegistryExactGroupScopeError(
            "SOR exact-group sessions differ from the fixed boundary plus 379 XNYS rows"
        )
    _require_wrong_share_sessions(
        evidence["XZO"],
        contaminated_share=XZO_CONTAMINATED_SHARE_CLASS,
        expected_sessions=set(XZO_SCOPE_SESSIONS),
    )
    _require_wrong_share_sessions(
        evidence["ANABV"],
        contaminated_share=ANABV_CONTAMINATED_SHARE_CLASS,
        expected_sessions=set(ANABV_SCOPE_SESSIONS),
    )

    transition_rows = _source_rows(
        selected,
        ticker="SOR",
        sessions=(SOR_PREDECESSOR_SESSION, SOR_SUCCESSOR_SESSION),
        composite=SOR_OLD_COMPOSITE,
        shares=(SOR_OLD_SHARE_CLASS, SOR_SUCCESSOR_SHARE_CLASS),
    )
    override_rows = _source_rows(
        selected,
        ticker="SOR",
        sessions=override_sessions,
        composite=SOR_OLD_COMPOSITE,
        shares=(SOR_SUCCESSOR_SHARE_CLASS,) * len(override_sessions),
    )
    xzo_rows = _source_rows(
        selected,
        ticker="XZO",
        sessions=XZO_SCOPE_SESSIONS,
        composite=EXACT_GROUP_HISTORY_FIXED_COMPOSITES["XZO"],
        shares=(XZO_CONTAMINATED_SHARE_CLASS,) * len(XZO_SCOPE_SESSIONS),
    )
    anabv_rows = _source_rows(
        selected,
        ticker="ANABV",
        sessions=ANABV_SCOPE_SESSIONS,
        composite=EXACT_GROUP_HISTORY_FIXED_COMPOSITES["ANABV"],
        shares=(ANABV_CONTAMINATED_SHARE_CLASS,),
    )
    if (
        len(transition_rows) != 2
        or len(override_rows) != 379
        or len(xzo_rows) != 2
        or len(anabv_rows) != 1
    ):
        raise IdentityRegistryExactGroupScopeError("fixed exact-group source-row counts differ")
    return MappingProxyType(
        {
            "asset_transition:SOR": ExactGroupRegistrySourceScope(transition_rows),
            "provider_composite_override:SOR": ExactGroupRegistrySourceScope(override_rows),
            "share_class_adjudication:XZO": ExactGroupRegistrySourceScope(xzo_rows),
            "share_class_adjudication:ANABV": ExactGroupRegistrySourceScope(anabv_rows),
        }
    )


def _source_rows(
    selected: Mapping[tuple[str, date], ProviderRowAttestation | None],
    *,
    ticker: str,
    sessions: Sequence[date],
    composite: str,
    shares: Sequence[str],
) -> tuple[ExactGroupRegistrySourceRow, ...]:
    if len(sessions) != len(shares):
        raise IdentityRegistryExactGroupScopeError("fixed source scope shape differs")
    result: list[ExactGroupRegistrySourceRow] = []
    for session, expected_share in zip(sessions, shares, strict=True):
        parent = selected.get((ticker, session))
        if parent is None:
            raise IdentityRegistryExactGroupScopeError(
                "fixed registry session lacks a selected provider parent"
            )
        row = parent.full_row_snapshot
        if (
            parent.dataset != ASSET_TABLE
            or row.get("ticker") != ticker
            or row.get("market") != EXACT_GROUP_HISTORY_PROVIDER_MARKET
            or row.get("locale") != EXACT_GROUP_HISTORY_PROVIDER_LOCALE
            or row.get("composite_figi") != composite
            or row.get("share_class_figi") != expected_share
        ):
            raise IdentityRegistryExactGroupScopeError(
                "fixed registry selected-parent identity differs"
            )
        result.append(
            ExactGroupRegistrySourceRow(
                session_date=session,
                source_record_id=parent.source_record_id,
                source_dataset=parent.dataset,
                source_s4_release_set_id=EXACT_GROUP_HISTORY_S4_RELEASE_SET_ID,
                provider_id=EXACT_GROUP_HISTORY_PROVIDER_ID,
                provider_market=EXACT_GROUP_HISTORY_PROVIDER_MARKET,
                provider_locale=EXACT_GROUP_HISTORY_PROVIDER_LOCALE,
                ticker=ticker,
                observed_composite_figi=str(row["composite_figi"]),
                observed_share_class_figi=(
                    None if row.get("share_class_figi") is None else str(row["share_class_figi"])
                ),
                primary_exchange_mic=(
                    None
                    if row.get("primary_exchange_mic") is None
                    else str(row["primary_exchange_mic"])
                ),
            )
        )
    return tuple(sorted(result))


def _require_wrong_share_sessions(
    evidence: ExactGroupHistoryEvidenceManifestV2,
    *,
    contaminated_share: str,
    expected_sessions: set[date],
) -> None:
    observed_sessions = {
        _attestation_session(item)
        for item in evidence.asset_attestations
        if _is_exact_group_asset(
            item,
            ticker=evidence.ticker,
            composite=evidence.exact_group_observed_composite_figi,
        )
        and item.full_row_snapshot.get("share_class_figi") == contaminated_share
    }
    if observed_sessions != expected_sessions:
        raise IdentityRegistryExactGroupScopeError(
            f"{evidence.ticker} contaminated Share Class appears outside its exact scope"
        )


@lru_cache(maxsize=1)
def _sor_override_sessions() -> tuple[date, ...]:
    sessions = tuple(
        item.session_date
        for item in build_xnys_calendar_artifact(
            SOR_SUCCESSOR_SESSION, SOR_OVERRIDE_END_SESSION
        ).sessions
    )
    if (
        len(sessions) != 379
        or sessions[0] != SOR_SUCCESSOR_SESSION
        or sessions[-1] != SOR_OVERRIDE_END_SESSION
    ):
        raise IdentityRegistryExactGroupScopeError(
            "frozen SOR override calendar no longer reproduces 379 XNYS sessions"
        )
    return sessions


def _attestation_session(item: ProviderRowAttestation) -> date:
    try:
        return date.fromisoformat(str(item.full_row_snapshot["session_date"]))
    except (KeyError, TypeError, ValueError) as exc:
        raise IdentityRegistryExactGroupScopeError("attested source session is malformed") from exc


def _is_exact_group_asset(item: ProviderRowAttestation, *, ticker: str, composite: str) -> bool:
    row = item.full_row_snapshot
    return (
        item.dataset == ASSET_TABLE
        and row.get("ticker") == ticker
        and row.get("market") == EXACT_GROUP_HISTORY_PROVIDER_MARKET
        and row.get("locale") == EXACT_GROUP_HISTORY_PROVIDER_LOCALE
        and row.get("composite_figi") == composite
    )


def _canonical_json_array(value: object, label: str) -> list[object]:
    if not isinstance(value, str):
        raise IdentityRegistryExactGroupScopeError(f"{label} must be JSON text")
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as exc:
        raise IdentityRegistryExactGroupScopeError(f"{label} is malformed") from exc
    if not isinstance(parsed, list) or value != _json_array(parsed):
        raise IdentityRegistryExactGroupScopeError(f"{label} is not canonical")
    return parsed


def _json_array(values: Sequence[object]) -> str:
    return json.dumps(list(values), allow_nan=False, ensure_ascii=False, separators=(",", ":"))


def _json_array_distinct(values: Sequence[object]) -> str:
    unique = {json.dumps(value, allow_nan=False, sort_keys=True): value for value in values}
    return _json_array([unique[key] for key in sorted(unique)])


def _canonical_json_document(content: bytes, label: str) -> Mapping[str, object]:
    try:
        value = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise IdentityRegistryExactGroupScopeError(f"{label} JSON is malformed") from exc
    if not isinstance(value, Mapping) or content != _canonical_json_bytes(value):
        raise IdentityRegistryExactGroupScopeError(f"{label} JSON is not canonical")
    return value


def _canonical_json_bytes(value: Mapping[str, object]) -> bytes:
    try:
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
    except (TypeError, ValueError) as exc:
        raise IdentityRegistryExactGroupScopeError(
            "artifact cannot be encoded as canonical JSON"
        ) from exc


def _tree_bytes(directory: Path) -> int:
    total = 0
    for current, directories, files in os.walk(directory, followlinks=False):
        current_path = Path(current)
        if any((current_path / name).is_symlink() for name in directories):
            raise IdentityRegistryExactGroupScopeError("candidate tree contains a symlink")
        for name in files:
            path = current_path / name
            if path.is_symlink() or not path.is_file():
                raise IdentityRegistryExactGroupScopeError(
                    "candidate tree contains a non-regular file"
                )
            total += path.stat().st_size
    return total


def _digest(value: object, label: str) -> str:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise IdentityRegistryExactGroupScopeError(f"{label} must be lowercase 64-hex")
    return value


def _figi(value: object, label: str) -> str:
    if not isinstance(value, str) or _FIGI.fullmatch(value) is None:
        raise IdentityRegistryExactGroupScopeError(f"{label} is invalid")
    return value


def _relative_path(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise IdentityRegistryExactGroupScopeError(f"{label} must be text")
    path = Path(value)
    if (
        path.is_absolute()
        or path.as_posix() != value
        or any(part in {"", ".", ".."} for part in path.parts)
    ):
        raise IdentityRegistryExactGroupScopeError(f"{label} must be a canonical relative path")
    return value


__all__ = [
    "ANABV_CONTAMINATED_SHARE_CLASS",
    "ANABV_SCOPE_SESSIONS",
    "SOR_OLD_COMPOSITE",
    "SOR_OLD_SHARE_CLASS",
    "SOR_OVERRIDE_END_SESSION",
    "SOR_PREDECESSOR_SESSION",
    "SOR_SUCCESSOR_SESSION",
    "SOR_SUCCESSOR_SHARE_CLASS",
    "XZO_CONTAMINATED_SHARE_CLASS",
    "XZO_SCOPE_SESSIONS",
    "ExactArtifactBindingLike",
    "ExactGroupRegistrySourceRow",
    "ExactGroupRegistrySourceScope",
    "IdentityRegistryExactGroupScopeError",
    "LoadedExactGroupRegistryScopes",
    "load_identity_registry_exact_group_scopes",
]
