"""Strict, source-independent contracts for reviewed Silver artifacts."""

from __future__ import annotations

import json
import math
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from types import MappingProxyType
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from ame_stocks_api.artifacts import ArtifactError, stable_digest
from ame_stocks_api.silver.fixed_cases import FIXED_CASE_IDS

CONTRACT_SCHEMA_VERSION = 1
BUILD_MANIFEST_VERSION = 1
FULL_RUN_PLAN_VERSION = 1
APPROVAL_SCHEMA_VERSION = 1
RELEASE_SCHEMA_VERSION = 1
SOURCE_INVENTORY_VERSION = 1
SEPARATE_FULL_RUN_PLAN_POLICY = "separate_approved_plan_v1"

_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]*$")
_CHECK_ID = re.compile(r"^[a-z][a-z0-9_.-]*$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_GIT_COMMIT = re.compile(r"^[0-9a-f]{40}(?:[0-9a-f]{24})?$")
_SENSITIVE_KEYS = (
    "api_key",
    "authorization",
    "cookie",
    "credential",
    "password",
    "secret",
    "token",
)
_BEARER = re.compile(r"(?i)\bbearer\s+\S+")
_SIGNED_QUERY = re.compile(r"(?i)[?&](?:token|signature|x-amz-signature|api_key)=")
_SOURCE_MEDIA_TYPES = {
    "application/gzip+json",
    "application/json",
    "application/vnd.apache.parquet",
    "text/plain",
    "text/csv+gzip",
}


class SilverContractError(ArtifactError):
    """Raised when a Silver schema or review record is malformed."""


class ArrowType(StrEnum):
    STRING = "string"
    LIST_STRING = "list_string"
    BOOLEAN = "boolean"
    INT64 = "int64"
    FLOAT64 = "float64"
    DATE32 = "date32"
    TIMESTAMP_NS_UTC = "timestamp_ns_utc"
    JSON_STRING = "json_string"


class BuildKind(StrEnum):
    PREVIEW = "preview"
    FULL = "full"


class SourceLayer(StrEnum):
    BRONZE = "bronze"
    CONTROL_MANIFEST = "control_manifest"
    PUBLISHED_SILVER = "published_silver"
    SYNTHETIC_FIXTURE = "synthetic_fixture"


class ArtifactRole(StrEnum):
    SOURCE = "source"
    DATA = "data"
    QA = "qa"
    QUARANTINE = "quarantine"
    SAMPLE = "sample"


class QASeverity(StrEnum):
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class QAStatus(StrEnum):
    PASSED = "passed"
    WARNING = "warning"
    FAILED = "failed"


class QAMetric(StrEnum):
    NUMERATOR = "numerator"
    RATE = "rate"


class QAOperator(StrEnum):
    EQUAL = "eq"
    LESS_THAN = "lt"
    LESS_THAN_OR_EQUAL = "lte"
    GREATER_THAN = "gt"
    GREATER_THAN_OR_EQUAL = "gte"


class ApprovalStage(StrEnum):
    SCHEMA = "schema"
    FULL_RUN = "full_run"
    PUBLISH = "publish"


class ApprovalDecision(StrEnum):
    APPROVED = "approved"
    REJECTED = "rejected"


class QuarantineReviewStatus(StrEnum):
    PENDING = "pending"
    ACCEPTED = "accepted"
    CORRECTED = "corrected"
    FALSE_POSITIVE = "false_positive"


_ARROW_TYPES: dict[ArrowType, pa.DataType] = {
    ArrowType.STRING: pa.string(),
    ArrowType.LIST_STRING: pa.list_(pa.field("item", pa.string(), nullable=False)),
    ArrowType.BOOLEAN: pa.bool_(),
    ArrowType.INT64: pa.int64(),
    ArrowType.FLOAT64: pa.float64(),
    ArrowType.DATE32: pa.date32(),
    ArrowType.TIMESTAMP_NS_UTC: pa.timestamp("ns", tz="UTC"),
    ArrowType.JSON_STRING: pa.string(),
}

QA_RESULT_ARROW_SCHEMA = pa.schema(
    [
        pa.field("build_id", pa.string(), nullable=False),
        pa.field("table_name", pa.string(), nullable=False),
        pa.field("partition_key", pa.string(), nullable=False),
        pa.field("check_id", pa.string(), nullable=False),
        pa.field("severity", pa.string(), nullable=False),
        pa.field("status", pa.string(), nullable=False),
        pa.field("numerator", pa.int64(), nullable=False),
        pa.field("denominator", pa.int64(), nullable=False),
        pa.field("rate", pa.float64(), nullable=True),
        pa.field("threshold", pa.string(), nullable=False),
        pa.field("bounded_examples_path", pa.string(), nullable=True),
    ]
)

QUARANTINE_ARROW_SCHEMA = pa.schema(
    [
        pa.field("source_record_id", pa.string(), nullable=False),
        pa.field("table_name", pa.string(), nullable=False),
        pa.field("issue_code", pa.string(), nullable=False),
        pa.field("severity", pa.string(), nullable=False),
        pa.field("detected_build_id", pa.string(), nullable=False),
        pa.field("source_pointer", pa.string(), nullable=False),
        pa.field("field_name", pa.string(), nullable=True),
        pa.field("observed_value", pa.string(), nullable=True),
        pa.field("expected_rule", pa.string(), nullable=False),
        pa.field("review_status", pa.string(), nullable=False),
    ]
)


@dataclass(frozen=True, slots=True)
class ColumnSpec:
    name: str
    arrow_type: ArrowType
    nullable: bool
    description: str

    def __post_init__(self) -> None:
        _validate_identifier(self.name, "column name")
        if not isinstance(self.arrow_type, ArrowType):
            raise SilverContractError("column arrow_type must be an ArrowType")
        if type(self.nullable) is not bool:
            raise SilverContractError("column nullable must be a native bool")
        _validate_text(self.description, "column description", maximum=1_000)

    def to_dict(self) -> dict[str, object]:
        return {
            "arrow_type": self.arrow_type.value,
            "description": self.description,
            "name": self.name,
            "nullable": self.nullable,
        }

    @classmethod
    def from_dict(cls, value: object) -> ColumnSpec:
        document = _mapping(value, "column")
        _expect_keys(document, {"arrow_type", "description", "name", "nullable"}, "column")
        try:
            arrow_type = ArrowType(document["arrow_type"])
        except (TypeError, ValueError) as exc:
            raise SilverContractError("column arrow_type is invalid") from exc
        return cls(
            name=_string(document["name"], "column name"),
            arrow_type=arrow_type,
            nullable=_native_bool(document["nullable"], "column nullable"),
            description=_string(document["description"], "column description"),
        )


@dataclass(frozen=True, slots=True)
class QARule:
    check_id: str
    severity: QASeverity
    metric: QAMetric
    operator: QAOperator
    limit: float
    failure_status: QAStatus
    description: str

    def __post_init__(self) -> None:
        _validate_check_id(self.check_id)
        if not isinstance(self.severity, QASeverity):
            raise SilverContractError("QA rule severity is invalid")
        if not isinstance(self.metric, QAMetric) or not isinstance(self.operator, QAOperator):
            raise SilverContractError("QA rule metric/operator is invalid")
        if type(self.limit) is not float or not math.isfinite(self.limit):
            raise SilverContractError("QA rule limit must be a finite native float")
        if self.failure_status not in {QAStatus.WARNING, QAStatus.FAILED}:
            raise SilverContractError("QA rule failure_status must be warning or failed")
        _validate_text(self.description, "QA rule description", maximum=1_000)

    @property
    def threshold_expression(self) -> str:
        return f"{self.metric.value} {self.operator.value} {format(self.limit, '.17g')}"

    def expected_status(self, *, numerator: int, rate: float | None) -> QAStatus:
        _native_nonnegative_int(numerator, "QA numerator")
        value = float(numerator) if self.metric is QAMetric.NUMERATOR else rate
        if value is None:
            return self.failure_status
        comparisons = {
            QAOperator.EQUAL: math.isclose(value, self.limit, rel_tol=0.0, abs_tol=1e-12),
            QAOperator.LESS_THAN: value < self.limit,
            QAOperator.LESS_THAN_OR_EQUAL: value <= self.limit,
            QAOperator.GREATER_THAN: value > self.limit,
            QAOperator.GREATER_THAN_OR_EQUAL: value >= self.limit,
        }
        return QAStatus.PASSED if comparisons[self.operator] else self.failure_status

    def to_dict(self) -> dict[str, object]:
        return {
            "check_id": self.check_id,
            "description": self.description,
            "failure_status": self.failure_status.value,
            "limit": self.limit,
            "metric": self.metric.value,
            "operator": self.operator.value,
            "severity": self.severity.value,
        }

    @classmethod
    def from_dict(cls, value: object) -> QARule:
        document = _mapping(value, "QA rule")
        required = {
            "check_id",
            "description",
            "failure_status",
            "limit",
            "metric",
            "operator",
            "severity",
        }
        _expect_keys(document, required, "QA rule")
        try:
            severity = QASeverity(document["severity"])
            metric = QAMetric(document["metric"])
            operator = QAOperator(document["operator"])
            failure_status = QAStatus(document["failure_status"])
        except (TypeError, ValueError) as exc:
            raise SilverContractError("QA rule enum is invalid") from exc
        limit = document["limit"]
        if type(limit) is not float:
            raise SilverContractError("QA rule limit must be a native float")
        return cls(
            check_id=_string(document["check_id"], "check_id"),
            severity=severity,
            metric=metric,
            operator=operator,
            limit=limit,
            failure_status=failure_status,
            description=_string(document["description"], "QA rule description"),
        )


@dataclass(frozen=True, slots=True)
class TableContract:
    domain: str
    table: str
    schema_version: int
    description: str
    grain: str
    columns: tuple[ColumnSpec, ...]
    primary_key: tuple[str, ...]
    partition_by: tuple[str, ...]
    sort_by: tuple[str, ...]
    source_datasets: tuple[str, ...]
    qa_rules: tuple[QARule, ...]

    def __post_init__(self) -> None:
        for field_name in (
            "columns",
            "primary_key",
            "partition_by",
            "sort_by",
            "source_datasets",
            "qa_rules",
        ):
            object.__setattr__(
                self,
                field_name,
                _as_tuple(getattr(self, field_name), field_name),
            )
        _validate_identifier(self.domain, "contract domain")
        _validate_identifier(self.table, "contract table")
        _native_positive_int(self.schema_version, "schema_version")
        _validate_text(self.description, "contract description", maximum=4_000)
        _validate_text(self.grain, "contract grain", maximum=1_000)
        if not self.columns:
            raise SilverContractError("contract columns cannot be empty")
        names = tuple(column.name for column in self.columns)
        _unique(names, "contract column")
        if not self.primary_key:
            raise SilverContractError("contract primary_key cannot be empty")
        for field_name, fields in (
            ("primary_key", self.primary_key),
            ("partition_by", self.partition_by),
            ("sort_by", self.sort_by),
        ):
            _unique(fields, field_name)
            unknown = sorted(set(fields).difference(names))
            if unknown:
                raise SilverContractError(f"{field_name} contains unknown columns: {unknown}")
        nullable_by_name = {column.name: column.nullable for column in self.columns}
        nullable_keys = [name for name in self.primary_key if nullable_by_name[name]]
        if nullable_keys:
            raise SilverContractError(f"primary key columns cannot be nullable: {nullable_keys}")
        if not self.source_datasets:
            raise SilverContractError("source_datasets cannot be empty")
        for source in self.source_datasets:
            _validate_identifier(source, "source dataset")
        _unique(self.source_datasets, "source dataset")
        if not self.qa_rules:
            raise SilverContractError("qa_rules cannot be empty")
        _unique((rule.check_id for rule in self.qa_rules), "QA rule check_id")

    @property
    def contract_id(self) -> str:
        return stable_digest(self.logical_payload())

    @property
    def arrow_schema(self) -> pa.Schema:
        return pa.schema(
            [
                pa.field(column.name, _ARROW_TYPES[column.arrow_type], nullable=column.nullable)
                for column in self.columns
            ]
        )

    @property
    def schema_digest(self) -> str:
        return arrow_schema_digest(self.arrow_schema)

    @property
    def required_qa_checks(self) -> tuple[str, ...]:
        return tuple(rule.check_id for rule in self.qa_rules)

    def logical_payload(self) -> dict[str, object]:
        return {
            "columns": [column.to_dict() for column in self.columns],
            "contract_schema_version": CONTRACT_SCHEMA_VERSION,
            "description": self.description,
            "domain": self.domain,
            "grain": self.grain,
            "partition_by": list(self.partition_by),
            "primary_key": list(self.primary_key),
            "qa_rules": [rule.to_dict() for rule in self.qa_rules],
            "schema_version": self.schema_version,
            "sort_by": list(self.sort_by),
            "source_datasets": list(self.source_datasets),
            "table": self.table,
        }

    def to_dict(self) -> dict[str, object]:
        return {"contract_id": self.contract_id, **self.logical_payload()}

    def validate_parquet_schema(self, path: Path) -> int:
        try:
            metadata = pq.ParquetFile(path).metadata
            actual = pq.ParquetFile(path).schema_arrow
        except (OSError, pa.ArrowException) as exc:
            raise SilverContractError(f"cannot read Parquet schema: {path}") from exc
        if actual != self.arrow_schema:
            raise SilverContractError(
                f"Parquet schema mismatch for {self.domain}/{self.table}: "
                f"expected {self.arrow_schema}, got {actual}"
            )
        return int(metadata.num_rows)

    @classmethod
    def from_dict(cls, value: object) -> TableContract:
        document = _mapping(value, "table contract")
        required = {
            "columns",
            "contract_id",
            "contract_schema_version",
            "description",
            "domain",
            "grain",
            "partition_by",
            "primary_key",
            "qa_rules",
            "schema_version",
            "sort_by",
            "source_datasets",
            "table",
        }
        _expect_keys(document, required, "table contract")
        if document["contract_schema_version"] != CONTRACT_SCHEMA_VERSION:
            raise SilverContractError("unsupported contract schema version")
        contract = cls(
            domain=_string(document["domain"], "contract domain"),
            table=_string(document["table"], "contract table"),
            schema_version=_native_positive_int(document["schema_version"], "schema_version"),
            description=_string(document["description"], "contract description"),
            grain=_string(document["grain"], "contract grain"),
            columns=tuple(
                ColumnSpec.from_dict(item) for item in _array(document["columns"], "columns")
            ),
            primary_key=_string_tuple(document["primary_key"], "primary_key"),
            partition_by=_string_tuple(document["partition_by"], "partition_by"),
            sort_by=_string_tuple(document["sort_by"], "sort_by"),
            source_datasets=_string_tuple(document["source_datasets"], "source_datasets"),
            qa_rules=tuple(
                QARule.from_dict(item) for item in _array(document["qa_rules"], "qa_rules")
            ),
        )
        if document["contract_id"] != contract.contract_id:
            raise SilverContractError("table contract digest mismatch")
        return contract


@dataclass(frozen=True, slots=True)
class UpstreamManifestRef:
    path: str
    sha256: str

    def __post_init__(self) -> None:
        _validate_relative_text_path(self.path)
        _validate_sha256(self.sha256, "upstream manifest sha256")

    def to_dict(self) -> dict[str, object]:
        return {"path": self.path, "sha256": self.sha256}

    @classmethod
    def from_dict(cls, value: object) -> UpstreamManifestRef:
        document = _mapping(value, "upstream manifest")
        _expect_keys(document, {"path", "sha256"}, "upstream manifest")
        return cls(
            path=_string(document["path"], "upstream manifest path"),
            sha256=_string(document["sha256"], "upstream manifest sha256"),
        )


@dataclass(frozen=True, slots=True)
class SourceInventoryItem:
    path: str
    sha256: str
    bytes: int
    row_count: int
    media_type: str
    table: str | None = None
    schema_digest: str | None = None

    def __post_init__(self) -> None:
        _validate_relative_text_path(self.path)
        _validate_sha256(self.sha256, "source inventory sha256")
        _native_nonnegative_int(self.bytes, "source inventory bytes")
        _native_nonnegative_int(self.row_count, "source inventory row_count")
        if self.media_type not in _SOURCE_MEDIA_TYPES:
            raise SilverContractError("source inventory media_type is unsupported")
        if self.media_type == "application/vnd.apache.parquet":
            if self.table is None or self.schema_digest is None:
                raise SilverContractError(
                    "Parquet source inventory items require table and schema_digest"
                )
            _validate_identifier(self.table, "source inventory table")
            _validate_sha256(self.schema_digest, "source inventory schema_digest")
        elif self.table is not None or self.schema_digest is not None:
            raise SilverContractError(
                "non-Parquet source inventory items cannot carry table/schema_digest"
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "bytes": self.bytes,
            "media_type": self.media_type,
            "path": self.path,
            "row_count": self.row_count,
            "schema_digest": self.schema_digest,
            "sha256": self.sha256,
            "table": self.table,
        }

    @classmethod
    def from_dict(cls, value: object) -> SourceInventoryItem:
        document = _mapping(value, "source inventory item")
        required = {
            "bytes",
            "media_type",
            "path",
            "row_count",
            "schema_digest",
            "sha256",
            "table",
        }
        _expect_keys(document, required, "source inventory item")
        return cls(
            path=_string(document["path"], "source inventory path"),
            sha256=_string(document["sha256"], "source inventory sha256"),
            bytes=_native_nonnegative_int(document["bytes"], "source inventory bytes"),
            row_count=_native_nonnegative_int(document["row_count"], "source inventory row_count"),
            media_type=_string(document["media_type"], "source inventory media_type"),
            table=(
                None
                if document["table"] is None
                else _string(document["table"], "source inventory table")
            ),
            schema_digest=(
                None
                if document["schema_digest"] is None
                else _string(document["schema_digest"], "source inventory schema_digest")
            ),
        )


@dataclass(frozen=True, slots=True)
class SourceInventory:
    source_dataset: str
    source_layer: SourceLayer
    git_commit: str
    upstream_manifests: tuple[UpstreamManifestRef, ...]
    artifacts: tuple[SourceInventoryItem, ...]

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "upstream_manifests",
            _as_tuple(self.upstream_manifests, "upstream_manifests"),
        )
        object.__setattr__(self, "artifacts", _as_tuple(self.artifacts, "artifacts"))
        _validate_identifier(self.source_dataset, "source inventory dataset")
        if not isinstance(self.source_layer, SourceLayer):
            raise SilverContractError("source inventory layer is invalid")
        if not _GIT_COMMIT.fullmatch(self.git_commit):
            raise SilverContractError("source inventory git_commit is invalid")
        if not self.upstream_manifests:
            raise SilverContractError("source inventory requires upstream manifests")
        if not self.artifacts:
            raise SilverContractError("source inventory requires artifacts")
        _unique((item.path for item in self.upstream_manifests), "upstream manifest path")
        _unique((item.path for item in self.artifacts), "source inventory artifact path")
        if self.source_layer is SourceLayer.CONTROL_MANIFEST and (
            len(self.artifacts) != 1 or self.artifacts[0].media_type != "text/plain"
        ):
            raise SilverContractError(
                "control-manifest inventories require exactly one text/plain artifact"
            )

    @property
    def inventory_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "artifacts": [
                item.to_dict() for item in sorted(self.artifacts, key=lambda item: item.path)
            ],
            "git_commit": self.git_commit,
            "source_dataset": self.source_dataset,
            "source_layer": self.source_layer.value,
            "source_inventory_version": SOURCE_INVENTORY_VERSION,
            "upstream_manifests": [
                item.to_dict()
                for item in sorted(self.upstream_manifests, key=lambda item: item.path)
            ],
        }

    def to_dict(self) -> dict[str, object]:
        return {"inventory_id": self.inventory_id, **self.logical_payload()}

    @classmethod
    def from_dict(cls, value: object) -> SourceInventory:
        document = _mapping(value, "source inventory")
        required = {
            "artifacts",
            "git_commit",
            "inventory_id",
            "source_dataset",
            "source_inventory_version",
            "source_layer",
            "upstream_manifests",
        }
        _expect_keys(document, required, "source inventory")
        if document["source_inventory_version"] != SOURCE_INVENTORY_VERSION:
            raise SilverContractError("unsupported source inventory version")
        try:
            source_layer = SourceLayer(document["source_layer"])
        except (TypeError, ValueError) as exc:
            raise SilverContractError("source inventory layer is invalid") from exc
        inventory = cls(
            source_dataset=_string(document["source_dataset"], "source inventory dataset"),
            source_layer=source_layer,
            git_commit=_string(document["git_commit"], "source inventory git_commit"),
            upstream_manifests=tuple(
                UpstreamManifestRef.from_dict(item)
                for item in _array(document["upstream_manifests"], "upstream_manifests")
            ),
            artifacts=tuple(
                SourceInventoryItem.from_dict(item)
                for item in _array(document["artifacts"], "source inventory artifacts")
            ),
        )
        if document["inventory_id"] != inventory.inventory_id:
            raise SilverContractError("source inventory digest mismatch")
        return inventory


@dataclass(frozen=True, slots=True)
class ArtifactRef:
    path: str
    sha256: str
    bytes: int
    row_count: int | None
    media_type: str
    role: ArtifactRole
    source_dataset: str | None = None
    source_layer: SourceLayer | None = None
    lineage_manifest_path: str | None = None
    lineage_manifest_sha256: str | None = None
    table: str | None = None
    schema_digest: str | None = None

    def __post_init__(self) -> None:
        _validate_relative_text_path(self.path)
        _validate_sha256(self.sha256, "artifact sha256")
        _native_nonnegative_int(self.bytes, "artifact bytes")
        if self.row_count is not None:
            _native_nonnegative_int(self.row_count, "artifact row_count")
        if self.media_type not in _SOURCE_MEDIA_TYPES:
            raise SilverContractError("artifact media_type is unsupported")
        if (
            self.media_type
            in {
                "application/gzip+json",
                "text/csv+gzip",
                "text/plain",
            }
            and self.role is not ArtifactRole.SOURCE
        ):
            raise SilverContractError("raw source media types are source-only")
        if not isinstance(self.role, ArtifactRole):
            raise SilverContractError("artifact role must be an ArtifactRole")
        if self.role is ArtifactRole.DATA and self.media_type != "application/vnd.apache.parquet":
            raise SilverContractError("Silver data artifacts must be Parquet")
        if self.role in {ArtifactRole.QA, ArtifactRole.QUARANTINE}:
            if self.media_type != "application/vnd.apache.parquet":
                raise SilverContractError("QA and quarantine artifacts must be Parquet")
            expected_table = (
                "qa_check_result" if self.role is ArtifactRole.QA else "quarantine_record"
            )
            expected_schema = (
                QA_RESULT_ARROW_SCHEMA if self.role is ArtifactRole.QA else QUARANTINE_ARROW_SCHEMA
            )
            if self.table != expected_table or self.schema_digest != arrow_schema_digest(
                expected_schema
            ):
                raise SilverContractError(
                    f"{self.role.value} artifact does not use the frozen system schema"
                )
        if self.role is ArtifactRole.SAMPLE and (
            self.row_count is None or self.row_count > 100 or self.bytes > 1_000_000
        ):
            raise SilverContractError(
                "sample artifacts require at most 100 rows and 1,000,000 bytes"
            )
        if self.role is ArtifactRole.SOURCE:
            if self.row_count is None:
                raise SilverContractError("source artifacts require an auditable row_count")
            if self.source_dataset is None:
                raise SilverContractError("source artifacts require source_dataset")
            _validate_identifier(self.source_dataset, "source_dataset")
            if not isinstance(self.source_layer, SourceLayer):
                raise SilverContractError("source artifacts require a source_layer")
            if self.lineage_manifest_path is None or self.lineage_manifest_sha256 is None:
                raise SilverContractError("source artifacts require a checksummed lineage manifest")
            _validate_relative_text_path(self.lineage_manifest_path)
            _validate_sha256(self.lineage_manifest_sha256, "lineage_manifest_sha256")
            if self.lineage_manifest_path == self.path:
                raise SilverContractError("source artifact cannot be its own lineage manifest")
        elif any(
            item is not None
            for item in (
                self.source_dataset,
                self.source_layer,
                self.lineage_manifest_path,
                self.lineage_manifest_sha256,
            )
        ):
            raise SilverContractError("only source artifacts can carry source lineage fields")
        if self.table is not None:
            _validate_identifier(self.table, "artifact table")
        if self.schema_digest is not None:
            _validate_sha256(self.schema_digest, "artifact schema_digest")
        if self.media_type == "application/vnd.apache.parquet" and (
            self.row_count is None or self.table is None or self.schema_digest is None
        ):
            raise SilverContractError(
                "Parquet artifacts require row_count, table, and schema_digest"
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "bytes": self.bytes,
            "media_type": self.media_type,
            "path": self.path,
            "role": self.role.value,
            "row_count": self.row_count,
            "schema_digest": self.schema_digest,
            "sha256": self.sha256,
            "lineage_manifest_path": self.lineage_manifest_path,
            "lineage_manifest_sha256": self.lineage_manifest_sha256,
            "source_dataset": self.source_dataset,
            "source_layer": None if self.source_layer is None else self.source_layer.value,
            "table": self.table,
        }

    @classmethod
    def from_dict(cls, value: object) -> ArtifactRef:
        document = _mapping(value, "artifact")
        required = {
            "bytes",
            "media_type",
            "lineage_manifest_path",
            "lineage_manifest_sha256",
            "path",
            "role",
            "row_count",
            "schema_digest",
            "sha256",
            "source_dataset",
            "source_layer",
            "table",
        }
        _expect_keys(document, required, "artifact")
        try:
            role = ArtifactRole(document["role"])
        except (TypeError, ValueError) as exc:
            raise SilverContractError("artifact role is invalid") from exc
        try:
            source_layer = (
                None if document["source_layer"] is None else SourceLayer(document["source_layer"])
            )
        except (TypeError, ValueError) as exc:
            raise SilverContractError("artifact source_layer is invalid") from exc
        return cls(
            path=_string(document["path"], "artifact path"),
            sha256=_string(document["sha256"], "artifact sha256"),
            bytes=_native_nonnegative_int(document["bytes"], "artifact bytes"),
            row_count=(
                None
                if document["row_count"] is None
                else _native_nonnegative_int(document["row_count"], "artifact row_count")
            ),
            media_type=_string(document["media_type"], "artifact media_type"),
            role=role,
            source_dataset=(
                None
                if document["source_dataset"] is None
                else _string(document["source_dataset"], "source_dataset")
            ),
            source_layer=source_layer,
            lineage_manifest_path=(
                None
                if document["lineage_manifest_path"] is None
                else _string(document["lineage_manifest_path"], "lineage_manifest_path")
            ),
            lineage_manifest_sha256=(
                None
                if document["lineage_manifest_sha256"] is None
                else _string(
                    document["lineage_manifest_sha256"],
                    "lineage_manifest_sha256",
                )
            ),
            table=(
                None if document["table"] is None else _string(document["table"], "artifact table")
            ),
            schema_digest=(
                None
                if document["schema_digest"] is None
                else _string(document["schema_digest"], "artifact schema_digest")
            ),
        )


@dataclass(frozen=True, slots=True)
class RowFunnel:
    input_rows: int
    accepted_source_rows: int
    exact_duplicate_excess: int
    quarantined_source_rows: int
    unmapped_source_rows: int
    version_preserved_rows: int
    output_rows_by_table: Mapping[str, int]

    def __post_init__(self) -> None:
        for name in (
            "input_rows",
            "accepted_source_rows",
            "exact_duplicate_excess",
            "quarantined_source_rows",
            "unmapped_source_rows",
            "version_preserved_rows",
        ):
            _native_nonnegative_int(getattr(self, name), name)
        if self.input_rows != (
            self.accepted_source_rows + self.exact_duplicate_excess + self.quarantined_source_rows
        ):
            raise SilverContractError("row funnel source-row accounting does not reconcile")
        if self.unmapped_source_rows > self.accepted_source_rows:
            raise SilverContractError("unmapped_source_rows cannot exceed accepted_source_rows")
        if self.version_preserved_rows > self.accepted_source_rows:
            raise SilverContractError("version_preserved_rows cannot exceed accepted_source_rows")
        normalized: dict[str, int] = {}
        for table, count in self.output_rows_by_table.items():
            _validate_identifier(table, "row funnel table")
            normalized[table] = _native_nonnegative_int(count, "output row count")
        if not normalized:
            raise SilverContractError("output_rows_by_table cannot be empty")
        object.__setattr__(
            self,
            "output_rows_by_table",
            MappingProxyType(dict(sorted(normalized.items()))),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "accepted_source_rows": self.accepted_source_rows,
            "exact_duplicate_excess": self.exact_duplicate_excess,
            "input_rows": self.input_rows,
            "output_rows_by_table": dict(self.output_rows_by_table),
            "quarantined_source_rows": self.quarantined_source_rows,
            "unmapped_source_rows": self.unmapped_source_rows,
            "version_preserved_rows": self.version_preserved_rows,
        }

    @classmethod
    def from_dict(cls, value: object) -> RowFunnel:
        document = _mapping(value, "row funnel")
        required = {
            "accepted_source_rows",
            "exact_duplicate_excess",
            "input_rows",
            "output_rows_by_table",
            "quarantined_source_rows",
            "unmapped_source_rows",
            "version_preserved_rows",
        }
        _expect_keys(document, required, "row funnel")
        output_rows = _mapping(document["output_rows_by_table"], "output_rows_by_table")
        return cls(
            input_rows=_native_nonnegative_int(document["input_rows"], "input_rows"),
            accepted_source_rows=_native_nonnegative_int(
                document["accepted_source_rows"], "accepted_source_rows"
            ),
            exact_duplicate_excess=_native_nonnegative_int(
                document["exact_duplicate_excess"], "exact_duplicate_excess"
            ),
            quarantined_source_rows=_native_nonnegative_int(
                document["quarantined_source_rows"], "quarantined_source_rows"
            ),
            unmapped_source_rows=_native_nonnegative_int(
                document["unmapped_source_rows"], "unmapped_source_rows"
            ),
            version_preserved_rows=_native_nonnegative_int(
                document["version_preserved_rows"], "version_preserved_rows"
            ),
            output_rows_by_table={
                key: _native_nonnegative_int(item, "output row count")
                for key, item in output_rows.items()
            },
        )


@dataclass(frozen=True, slots=True)
class QACheckResult:
    table: str
    partition_key: str
    check_id: str
    severity: QASeverity
    status: QAStatus
    numerator: int
    denominator: int
    rate: float | None
    threshold: str
    bounded_examples_path: str | None = None

    def __post_init__(self) -> None:
        _validate_identifier(self.table, "QA table")
        _validate_text(self.partition_key, "QA partition_key", maximum=500)
        _validate_check_id(self.check_id)
        if not isinstance(self.severity, QASeverity) or not isinstance(self.status, QAStatus):
            raise SilverContractError("QA severity/status enum is invalid")
        _native_nonnegative_int(self.numerator, "QA numerator")
        _native_nonnegative_int(self.denominator, "QA denominator")
        if self.numerator > self.denominator:
            raise SilverContractError("QA numerator cannot exceed denominator")
        if self.denominator == 0:
            if self.rate is not None:
                raise SilverContractError("QA rate must be null when denominator is zero")
        else:
            if type(self.rate) is not float or not math.isfinite(self.rate):
                raise SilverContractError("QA rate must be a finite native float")
            expected = self.numerator / self.denominator
            if not math.isclose(self.rate, expected, rel_tol=0.0, abs_tol=1e-12):
                raise SilverContractError("QA rate does not match numerator/denominator")
        _validate_text(self.threshold, "QA threshold", maximum=1_000)
        if self.bounded_examples_path is not None:
            _validate_relative_text_path(self.bounded_examples_path)

    @property
    def blocks_publish(self) -> bool:
        return self.status is QAStatus.FAILED and self.severity in {
            QASeverity.CRITICAL,
            QASeverity.HIGH,
        }

    @property
    def result_id(self) -> str:
        return stable_digest(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            "bounded_examples_path": self.bounded_examples_path,
            "check_id": self.check_id,
            "denominator": self.denominator,
            "numerator": self.numerator,
            "partition_key": self.partition_key,
            "rate": self.rate,
            "severity": self.severity.value,
            "status": self.status.value,
            "table": self.table,
            "threshold": self.threshold,
        }

    def to_output_dict(self, build_id: str) -> dict[str, object]:
        _validate_sha256(build_id, "QA build_id")
        return {
            "bounded_examples_path": self.bounded_examples_path,
            "build_id": build_id,
            "check_id": self.check_id,
            "denominator": self.denominator,
            "numerator": self.numerator,
            "partition_key": self.partition_key,
            "rate": self.rate,
            "severity": self.severity.value,
            "status": self.status.value,
            "table_name": self.table,
            "threshold": self.threshold,
        }

    @classmethod
    def from_dict(cls, value: object) -> QACheckResult:
        document = _mapping(value, "QA check")
        required = {
            "bounded_examples_path",
            "check_id",
            "denominator",
            "numerator",
            "partition_key",
            "rate",
            "severity",
            "status",
            "table",
            "threshold",
        }
        _expect_keys(document, required, "QA check")
        try:
            severity = QASeverity(document["severity"])
            status = QAStatus(document["status"])
        except (TypeError, ValueError) as exc:
            raise SilverContractError("QA severity/status is invalid") from exc
        rate = document["rate"]
        if rate is not None and type(rate) is not float:
            raise SilverContractError("QA rate must be a native float or null")
        return cls(
            table=_string(document["table"], "QA table"),
            partition_key=_string(document["partition_key"], "QA partition_key"),
            check_id=_string(document["check_id"], "QA check_id"),
            severity=severity,
            status=status,
            numerator=_native_nonnegative_int(document["numerator"], "QA numerator"),
            denominator=_native_nonnegative_int(document["denominator"], "QA denominator"),
            rate=rate,
            threshold=_string(document["threshold"], "QA threshold"),
            bounded_examples_path=(
                None
                if document["bounded_examples_path"] is None
                else _string(document["bounded_examples_path"], "bounded_examples_path")
            ),
        )


@dataclass(frozen=True, slots=True)
class QuarantineRecord:
    source_record_id: str
    table_name: str
    issue_code: str
    severity: QASeverity
    detected_build_id: str
    source_pointer: str
    field_name: str | None
    observed_value: str | None
    expected_rule: str
    review_status: QuarantineReviewStatus

    def __post_init__(self) -> None:
        _validate_text(self.source_record_id, "source_record_id", maximum=500)
        _validate_identifier(self.table_name, "quarantine table_name")
        _validate_check_id(self.issue_code)
        if not isinstance(self.severity, QASeverity):
            raise SilverContractError("quarantine severity is invalid")
        _validate_sha256(self.detected_build_id, "detected_build_id")
        _validate_text(self.source_pointer, "source_pointer", maximum=2_000)
        if self.field_name is not None:
            _validate_identifier(self.field_name, "quarantine field_name")
        if self.observed_value is not None:
            _validate_text(
                self.observed_value,
                "quarantine observed_value",
                maximum=4_096,
                allow_empty=True,
            )
        _validate_text(self.expected_rule, "expected_rule", maximum=2_000)
        if not isinstance(self.review_status, QuarantineReviewStatus):
            raise SilverContractError("quarantine review_status is invalid")
        ensure_json_safe(
            {
                "expected_rule": self.expected_rule,
                "observed_value": self.observed_value,
                "source_pointer": self.source_pointer,
            },
            label="quarantine record",
        )

    @property
    def issue_id(self) -> str:
        return stable_digest(
            {
                "detected_build_id": self.detected_build_id,
                "expected_rule": self.expected_rule,
                "field_name": self.field_name,
                "issue_code": self.issue_code,
                "observed_value": self.observed_value,
                "severity": self.severity.value,
                "source_pointer": self.source_pointer,
                "source_record_id": self.source_record_id,
                "table_name": self.table_name,
            }
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "detected_build_id": self.detected_build_id,
            "expected_rule": self.expected_rule,
            "field_name": self.field_name,
            "issue_code": self.issue_code,
            "observed_value": self.observed_value,
            "review_status": self.review_status.value,
            "severity": self.severity.value,
            "source_pointer": self.source_pointer,
            "source_record_id": self.source_record_id,
            "table_name": self.table_name,
        }

    @classmethod
    def from_dict(cls, value: object) -> QuarantineRecord:
        document = _mapping(value, "quarantine record")
        required = {
            "detected_build_id",
            "expected_rule",
            "field_name",
            "issue_code",
            "observed_value",
            "review_status",
            "severity",
            "source_pointer",
            "source_record_id",
            "table_name",
        }
        _expect_keys(document, required, "quarantine record")
        try:
            severity = QASeverity(document["severity"])
            review_status = QuarantineReviewStatus(document["review_status"])
        except (TypeError, ValueError) as exc:
            raise SilverContractError("quarantine enum is invalid") from exc
        return cls(
            source_record_id=_string(document["source_record_id"], "source_record_id"),
            table_name=_string(document["table_name"], "quarantine table_name"),
            issue_code=_string(document["issue_code"], "issue_code"),
            severity=severity,
            detected_build_id=_string(document["detected_build_id"], "detected_build_id"),
            source_pointer=_string(document["source_pointer"], "source_pointer"),
            field_name=(
                None
                if document["field_name"] is None
                else _string(document["field_name"], "field_name")
            ),
            observed_value=(
                None
                if document["observed_value"] is None
                else _string(document["observed_value"], "observed_value", allow_empty=True)
            ),
            expected_rule=_string(document["expected_rule"], "expected_rule"),
            review_status=review_status,
        )


@dataclass(frozen=True, slots=True)
class PreviewMetadata:
    fixed_case_ids: tuple[str, ...]
    fixed_case_qa_result_ids: Mapping[str, tuple[str, ...]]
    input_sample_path: str
    input_sample_rows: int
    output_sample_path: str
    output_sample_rows: int
    examples_truncated: bool
    full_run_inputs: tuple[ArtifactRef, ...]
    resource_usage: Mapping[str, object]
    full_run_projection: Mapping[str, object]

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "fixed_case_ids",
            _as_tuple(self.fixed_case_ids, "fixed_case_ids"),
        )
        object.__setattr__(
            self,
            "full_run_inputs",
            _as_tuple(self.full_run_inputs, "full_run_inputs"),
        )
        if not self.fixed_case_ids:
            raise SilverContractError("preview fixed_case_ids cannot be empty")
        for case_id in self.fixed_case_ids:
            _validate_check_id(case_id)
        _unique(self.fixed_case_ids, "fixed case")
        unknown_cases = sorted(set(self.fixed_case_ids).difference(FIXED_CASE_IDS))
        if unknown_cases:
            raise SilverContractError(f"preview contains unknown fixed cases: {unknown_cases}")
        if not isinstance(self.fixed_case_qa_result_ids, Mapping):
            raise SilverContractError("fixed_case_qa_result_ids must be an object")
        normalized_case_results: dict[str, tuple[str, ...]] = {}
        for case_id, result_ids in self.fixed_case_qa_result_ids.items():
            _validate_check_id(case_id)
            if not isinstance(result_ids, (list, tuple)) or not result_ids:
                raise SilverContractError("fixed-case QA evidence cannot be empty")
            normalized = tuple(sorted(result_ids))
            for result_id in normalized:
                _validate_sha256(result_id, "fixed-case QA result ID")
            _unique(normalized, "fixed-case QA result ID")
            normalized_case_results[case_id] = normalized
        if set(normalized_case_results) != set(self.fixed_case_ids):
            raise SilverContractError("fixed-case QA evidence keys must match fixed_case_ids")
        object.__setattr__(
            self,
            "fixed_case_qa_result_ids",
            MappingProxyType(dict(sorted(normalized_case_results.items()))),
        )
        _validate_relative_text_path(self.input_sample_path)
        _validate_relative_text_path(self.output_sample_path)
        if self.input_sample_path == self.output_sample_path:
            raise SilverContractError("preview input/output samples must be distinct artifacts")
        _native_nonnegative_int(self.input_sample_rows, "input_sample_rows")
        _native_nonnegative_int(self.output_sample_rows, "output_sample_rows")
        if self.input_sample_rows > 100 or self.output_sample_rows > 100:
            raise SilverContractError("preview samples are bounded to 100 rows")
        if type(self.examples_truncated) is not bool:
            raise SilverContractError("examples_truncated must be a native bool")
        if not self.full_run_inputs:
            raise SilverContractError(
                "preview must include the reviewable full-run input inventory"
            )
        if any(item.role is not ArtifactRole.SOURCE for item in self.full_run_inputs):
            raise SilverContractError("full-run inputs must use the source role")
        _unique((item.path for item in self.full_run_inputs), "full-run input path")
        object.__setattr__(
            self,
            "full_run_inputs",
            tuple(sorted(self.full_run_inputs, key=lambda item: item.path)),
        )
        resource = ensure_json_safe(self.resource_usage, label="resource_usage")
        projection = ensure_json_safe(self.full_run_projection, label="full_run_projection")
        if not resource or not projection:
            raise SilverContractError("preview resource usage and projection cannot be empty")
        object.__setattr__(self, "resource_usage", resource)
        object.__setattr__(self, "full_run_projection", projection)

    def to_dict(self) -> dict[str, object]:
        return {
            "examples_truncated": self.examples_truncated,
            "fixed_case_ids": list(self.fixed_case_ids),
            "fixed_case_qa_result_ids": {
                key: list(value) for key, value in self.fixed_case_qa_result_ids.items()
            },
            "full_run_inputs": [
                item.to_dict()
                for item in sorted(self.full_run_inputs, key=lambda artifact: artifact.path)
            ],
            "full_run_projection": thaw_json(self.full_run_projection),
            "full_run_source_digest": self.full_run_source_digest,
            "input_sample_rows": self.input_sample_rows,
            "input_sample_path": self.input_sample_path,
            "output_sample_path": self.output_sample_path,
            "output_sample_rows": self.output_sample_rows,
            "resource_usage": thaw_json(self.resource_usage),
        }

    @property
    def full_run_source_digest(self) -> str:
        return stable_digest(
            [item.to_dict() for item in sorted(self.full_run_inputs, key=lambda item: item.path)]
        )

    @classmethod
    def from_dict(cls, value: object) -> PreviewMetadata:
        document = _mapping(value, "preview metadata")
        required = {
            "examples_truncated",
            "fixed_case_ids",
            "fixed_case_qa_result_ids",
            "full_run_inputs",
            "full_run_projection",
            "full_run_source_digest",
            "input_sample_rows",
            "input_sample_path",
            "output_sample_path",
            "output_sample_rows",
            "resource_usage",
        }
        _expect_keys(document, required, "preview metadata")
        preview = cls(
            fixed_case_ids=_string_tuple(document["fixed_case_ids"], "fixed_case_ids"),
            fixed_case_qa_result_ids={
                key: _string_tuple(item, "fixed-case QA result IDs")
                for key, item in _mapping(
                    document["fixed_case_qa_result_ids"],
                    "fixed_case_qa_result_ids",
                ).items()
            },
            input_sample_path=_string(document["input_sample_path"], "input_sample_path"),
            input_sample_rows=_native_nonnegative_int(
                document["input_sample_rows"], "input_sample_rows"
            ),
            output_sample_rows=_native_nonnegative_int(
                document["output_sample_rows"], "output_sample_rows"
            ),
            output_sample_path=_string(document["output_sample_path"], "output_sample_path"),
            examples_truncated=_native_bool(document["examples_truncated"], "examples_truncated"),
            full_run_inputs=tuple(
                ArtifactRef.from_dict(item)
                for item in _array(document["full_run_inputs"], "full_run_inputs")
            ),
            resource_usage=_mapping(document["resource_usage"], "resource_usage"),
            full_run_projection=_mapping(document["full_run_projection"], "full_run_projection"),
        )
        if document["full_run_source_digest"] != preview.full_run_source_digest:
            raise SilverContractError("preview full-run source digest mismatch")
        return preview


@dataclass(frozen=True, slots=True)
class BuildIntent:
    workflow_id: str
    domain: str
    table: str
    schema_version: int
    contract_id: str
    kind: BuildKind
    attempt: int
    retry_of_build_id: str | None
    transform_version: str
    git_commit: str
    exchange_calendar_version: str
    inputs: tuple[ArtifactRef, ...]
    parameters: Mapping[str, object]

    def __post_init__(self) -> None:
        object.__setattr__(self, "inputs", _as_tuple(self.inputs, "build inputs"))
        _validate_sha256(self.workflow_id, "workflow_id")
        _validate_identifier(self.domain, "build domain")
        _validate_identifier(self.table, "build table")
        _native_positive_int(self.schema_version, "build schema_version")
        _validate_sha256(self.contract_id, "contract_id")
        if not isinstance(self.kind, BuildKind):
            raise SilverContractError("build kind must be a BuildKind")
        _native_positive_int(self.attempt, "build attempt")
        if self.retry_of_build_id is not None:
            _validate_sha256(self.retry_of_build_id, "retry_of_build_id")
        if self.attempt == 1 and self.retry_of_build_id is not None:
            raise SilverContractError("first build attempt cannot retry another build")
        if self.attempt > 1 and self.retry_of_build_id is None:
            raise SilverContractError("retry attempts require retry_of_build_id")
        _validate_text(self.transform_version, "transform_version", maximum=200)
        if not _GIT_COMMIT.fullmatch(self.git_commit):
            raise SilverContractError("git_commit must be a full lowercase Git object ID")
        _validate_text(
            self.exchange_calendar_version,
            "exchange_calendar_version",
            maximum=200,
        )
        if not self.inputs:
            raise SilverContractError("build inputs cannot be empty")
        if any(item.role is not ArtifactRole.SOURCE for item in self.inputs):
            raise SilverContractError("build inputs must use the source role")
        _unique((item.path for item in self.inputs), "build input path")
        object.__setattr__(self, "inputs", tuple(sorted(self.inputs, key=lambda item: item.path)))
        parameters = ensure_json_safe(self.parameters, label="build parameters")
        object.__setattr__(self, "parameters", parameters)

    @property
    def source_digest(self) -> str:
        return stable_digest(
            [item.to_dict() for item in sorted(self.inputs, key=lambda artifact: artifact.path)]
        )

    @property
    def build_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "attempt": self.attempt,
            "contract_id": self.contract_id,
            "domain": self.domain,
            "exchange_calendar_version": self.exchange_calendar_version,
            "git_commit": self.git_commit,
            "inputs": [
                item.to_dict() for item in sorted(self.inputs, key=lambda artifact: artifact.path)
            ],
            "kind": self.kind.value,
            "parameters": thaw_json(self.parameters),
            "retry_of_build_id": self.retry_of_build_id,
            "schema_version": self.schema_version,
            "source_digest": self.source_digest,
            "table": self.table,
            "transform_version": self.transform_version,
            "workflow_id": self.workflow_id,
        }

    def to_dict(self) -> dict[str, object]:
        return {"build_id": self.build_id, **self.logical_payload()}

    @classmethod
    def from_dict(cls, value: object) -> BuildIntent:
        document = _mapping(value, "build intent")
        required = {
            "attempt",
            "build_id",
            "contract_id",
            "domain",
            "exchange_calendar_version",
            "git_commit",
            "inputs",
            "kind",
            "parameters",
            "retry_of_build_id",
            "schema_version",
            "source_digest",
            "table",
            "transform_version",
            "workflow_id",
        }
        _expect_keys(document, required, "build intent")
        try:
            kind = BuildKind(document["kind"])
        except (TypeError, ValueError) as exc:
            raise SilverContractError("build kind is invalid") from exc
        intent = cls(
            workflow_id=_string(document["workflow_id"], "workflow_id"),
            domain=_string(document["domain"], "build domain"),
            table=_string(document["table"], "build table"),
            schema_version=_native_positive_int(document["schema_version"], "build schema_version"),
            contract_id=_string(document["contract_id"], "contract_id"),
            kind=kind,
            attempt=_native_positive_int(document["attempt"], "build attempt"),
            retry_of_build_id=(
                None
                if document["retry_of_build_id"] is None
                else _string(document["retry_of_build_id"], "retry_of_build_id")
            ),
            transform_version=_string(document["transform_version"], "transform_version"),
            git_commit=_string(document["git_commit"], "git_commit"),
            exchange_calendar_version=_string(
                document["exchange_calendar_version"], "exchange_calendar_version"
            ),
            inputs=tuple(
                ArtifactRef.from_dict(item) for item in _array(document["inputs"], "inputs")
            ),
            parameters=_mapping(document["parameters"], "parameters"),
        )
        if document["source_digest"] != intent.source_digest:
            raise SilverContractError("build source_digest mismatch")
        if document["build_id"] != intent.build_id:
            raise SilverContractError("build_id mismatch")
        return intent


@dataclass(frozen=True, slots=True)
class FullRunPlan:
    """Immutable, separately reviewed authorization scope for a future full build."""

    workflow_id: str
    domain: str
    table: str
    schema_version: int
    contract_id: str
    reviewed_preview_build_id: str
    reviewed_preview_manifest_sha256: str
    reviewed_preview_event_sha256: str
    transform_version: str
    git_commit: str
    exchange_calendar_version: str
    inputs: tuple[ArtifactRef, ...]
    parameters: Mapping[str, object]
    resource_projection: Mapping[str, object]

    def __post_init__(self) -> None:
        object.__setattr__(self, "inputs", _as_tuple(self.inputs, "full-run plan inputs"))
        _validate_sha256(self.workflow_id, "workflow_id")
        _validate_identifier(self.domain, "full-run plan domain")
        _validate_identifier(self.table, "full-run plan table")
        _native_positive_int(self.schema_version, "full-run plan schema_version")
        for label, value in (
            ("contract_id", self.contract_id),
            ("reviewed_preview_build_id", self.reviewed_preview_build_id),
            (
                "reviewed_preview_manifest_sha256",
                self.reviewed_preview_manifest_sha256,
            ),
            ("reviewed_preview_event_sha256", self.reviewed_preview_event_sha256),
        ):
            _validate_sha256(value, label)
        _validate_text(self.transform_version, "transform_version", maximum=200)
        if not _GIT_COMMIT.fullmatch(self.git_commit):
            raise SilverContractError("git_commit must be a full lowercase Git object ID")
        _validate_text(
            self.exchange_calendar_version,
            "exchange_calendar_version",
            maximum=200,
        )
        if not self.inputs:
            raise SilverContractError("full-run plan inputs cannot be empty")
        if any(item.role is not ArtifactRole.SOURCE for item in self.inputs):
            raise SilverContractError("full-run plan inputs must use the source role")
        _unique((item.path for item in self.inputs), "full-run plan input path")
        object.__setattr__(self, "inputs", tuple(sorted(self.inputs, key=lambda item: item.path)))
        parameters = ensure_json_safe(self.parameters, label="full-run plan parameters")
        reserved = {
            "approved_full_run_plan_id",
            "approved_preview_build_id",
        }.intersection(parameters)
        if reserved:
            raise SilverContractError(
                f"full-run plan parameters contain reserved keys: {sorted(reserved)}"
            )
        object.__setattr__(self, "parameters", parameters)
        projection = ensure_json_safe(
            self.resource_projection,
            label="full-run plan resource projection",
        )
        if not projection:
            raise SilverContractError("full-run plan resource projection cannot be empty")
        object.__setattr__(self, "resource_projection", projection)

    @property
    def source_digest(self) -> str:
        return stable_digest([item.to_dict() for item in self.inputs])

    @property
    def input_artifact_count(self) -> int:
        return len(self.inputs)

    @property
    def input_rows(self) -> int:
        return sum(int(item.row_count or 0) for item in self.inputs)

    @property
    def input_bytes(self) -> int:
        return sum(item.bytes for item in self.inputs)

    @property
    def plan_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "contract_id": self.contract_id,
            "domain": self.domain,
            "exchange_calendar_version": self.exchange_calendar_version,
            "full_run_plan_version": FULL_RUN_PLAN_VERSION,
            "git_commit": self.git_commit,
            "input_artifact_count": self.input_artifact_count,
            "input_bytes": self.input_bytes,
            "input_rows": self.input_rows,
            "inputs": [item.to_dict() for item in self.inputs],
            "parameters": thaw_json(self.parameters),
            "resource_projection": thaw_json(self.resource_projection),
            "reviewed_preview_build_id": self.reviewed_preview_build_id,
            "reviewed_preview_event_sha256": self.reviewed_preview_event_sha256,
            "reviewed_preview_manifest_sha256": self.reviewed_preview_manifest_sha256,
            "schema_version": self.schema_version,
            "source_digest": self.source_digest,
            "table": self.table,
            "transform_version": self.transform_version,
            "workflow_id": self.workflow_id,
        }

    def to_dict(self) -> dict[str, object]:
        return {"plan_id": self.plan_id, **self.logical_payload()}

    @classmethod
    def from_dict(cls, value: object) -> FullRunPlan:
        document = _mapping(value, "full-run plan")
        required = {
            "contract_id",
            "domain",
            "exchange_calendar_version",
            "full_run_plan_version",
            "git_commit",
            "input_artifact_count",
            "input_bytes",
            "input_rows",
            "inputs",
            "parameters",
            "plan_id",
            "resource_projection",
            "reviewed_preview_build_id",
            "reviewed_preview_event_sha256",
            "reviewed_preview_manifest_sha256",
            "schema_version",
            "source_digest",
            "table",
            "transform_version",
            "workflow_id",
        }
        _expect_keys(document, required, "full-run plan")
        if document["full_run_plan_version"] != FULL_RUN_PLAN_VERSION:
            raise SilverContractError("unsupported full-run plan version")
        plan = cls(
            workflow_id=_string(document["workflow_id"], "workflow_id"),
            domain=_string(document["domain"], "full-run plan domain"),
            table=_string(document["table"], "full-run plan table"),
            schema_version=_native_positive_int(
                document["schema_version"],
                "full-run plan schema_version",
            ),
            contract_id=_string(document["contract_id"], "contract_id"),
            reviewed_preview_build_id=_string(
                document["reviewed_preview_build_id"],
                "reviewed_preview_build_id",
            ),
            reviewed_preview_manifest_sha256=_string(
                document["reviewed_preview_manifest_sha256"],
                "reviewed_preview_manifest_sha256",
            ),
            reviewed_preview_event_sha256=_string(
                document["reviewed_preview_event_sha256"],
                "reviewed_preview_event_sha256",
            ),
            transform_version=_string(document["transform_version"], "transform_version"),
            git_commit=_string(document["git_commit"], "git_commit"),
            exchange_calendar_version=_string(
                document["exchange_calendar_version"],
                "exchange_calendar_version",
            ),
            inputs=tuple(
                ArtifactRef.from_dict(item)
                for item in _array(document["inputs"], "full-run plan inputs")
            ),
            parameters=_mapping(document["parameters"], "full-run plan parameters"),
            resource_projection=_mapping(
                document["resource_projection"],
                "full-run plan resource projection",
            ),
        )
        if document["source_digest"] != plan.source_digest:
            raise SilverContractError("full-run plan source digest mismatch")
        for key, expected in (
            ("input_artifact_count", plan.input_artifact_count),
            ("input_rows", plan.input_rows),
            ("input_bytes", plan.input_bytes),
        ):
            actual = _native_nonnegative_int(document[key], f"full-run plan {key}")
            if actual != expected:
                raise SilverContractError(f"full-run plan {key} mismatch")
        if document["plan_id"] != plan.plan_id:
            raise SilverContractError("full-run plan digest mismatch")
        return plan


@dataclass(frozen=True, slots=True)
class BuildManifest:
    intent: BuildIntent
    outputs: tuple[ArtifactRef, ...]
    row_funnel: RowFunnel
    qa_checks: tuple[QACheckResult, ...]
    quarantine_issue_rows: int
    quarantine_unique_source_rows: int
    quarantine_issue_ids_by_severity: Mapping[str, tuple[str, ...]]
    started_at: str
    completed_at: str
    preview: PreviewMetadata | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "outputs", _as_tuple(self.outputs, "build outputs"))
        object.__setattr__(self, "qa_checks", _as_tuple(self.qa_checks, "QA checks"))
        if not self.outputs:
            raise SilverContractError("build outputs cannot be empty")
        if any(item.role is ArtifactRole.SOURCE for item in self.outputs):
            raise SilverContractError("build outputs cannot use the source role")
        _unique((item.path for item in self.outputs), "build output path")
        _unique(
            ((item.table, item.partition_key, item.check_id) for item in self.qa_checks),
            "QA result key",
        )
        object.__setattr__(
            self,
            "outputs",
            tuple(sorted(self.outputs, key=lambda item: item.path)),
        )
        object.__setattr__(
            self,
            "qa_checks",
            tuple(
                sorted(
                    self.qa_checks,
                    key=lambda item: (item.table, item.partition_key, item.check_id),
                )
            ),
        )
        _native_nonnegative_int(self.quarantine_issue_rows, "quarantine_issue_rows")
        _native_nonnegative_int(
            self.quarantine_unique_source_rows,
            "quarantine_unique_source_rows",
        )
        if not isinstance(self.quarantine_issue_ids_by_severity, Mapping):
            raise SilverContractError("quarantine_issue_ids_by_severity must be an object")
        expected_severities = {item.value for item in QASeverity}
        if set(self.quarantine_issue_ids_by_severity) != expected_severities:
            raise SilverContractError(
                "quarantine issue map must contain critical/high/medium/low keys"
            )
        normalized_issue_ids: dict[str, tuple[str, ...]] = {}
        all_issue_ids: list[str] = []
        for severity in sorted(expected_severities):
            issue_ids = _as_tuple(
                self.quarantine_issue_ids_by_severity[severity],
                f"{severity} quarantine issue IDs",
            )
            normalized = tuple(sorted(issue_ids))
            for issue_id in normalized:
                _validate_sha256(issue_id, "quarantine issue ID")
            _unique(normalized, "quarantine issue ID")
            normalized_issue_ids[severity] = normalized
            all_issue_ids.extend(normalized)
        _unique(all_issue_ids, "cross-severity quarantine issue ID")
        if len(all_issue_ids) != self.quarantine_issue_rows:
            raise SilverContractError("quarantine issue IDs do not match quarantine_issue_rows")
        object.__setattr__(
            self,
            "quarantine_issue_ids_by_severity",
            MappingProxyType(normalized_issue_ids),
        )
        if self.quarantine_unique_source_rows > self.quarantine_issue_rows:
            raise SilverContractError(
                "quarantine unique source rows cannot exceed quarantine issue rows"
            )
        if self.quarantine_unique_source_rows != self.row_funnel.quarantined_source_rows:
            raise SilverContractError("quarantine unique rows do not match the row funnel")
        started = _parse_utc(self.started_at, "started_at")
        completed = _parse_utc(self.completed_at, "completed_at")
        if started > completed:
            raise SilverContractError("build completed_at precedes started_at")
        if self.intent.kind is BuildKind.PREVIEW and self.preview is None:
            raise SilverContractError("preview builds require preview metadata")
        if self.intent.kind is BuildKind.FULL and self.preview is not None:
            raise SilverContractError("full builds cannot carry preview metadata")
        source_rows = sum(int(item.row_count or 0) for item in self.intent.inputs)
        if source_rows != self.row_funnel.input_rows:
            raise SilverContractError("source artifact rows do not match row funnel input_rows")
        sample_paths = {
            output.path for output in self.outputs if output.role is ArtifactRole.SAMPLE
        }
        for check in self.qa_checks:
            if (
                check.bounded_examples_path is not None
                and check.bounded_examples_path not in sample_paths
            ):
                raise SilverContractError(
                    "QA bounded_examples_path must reference a declared sample artifact"
                )
        if self.preview is not None:
            samples_by_path = {
                output.path: output for output in self.outputs if output.role is ArtifactRole.SAMPLE
            }
            input_sample = samples_by_path.get(self.preview.input_sample_path)
            output_sample = samples_by_path.get(self.preview.output_sample_path)
            if input_sample is None or output_sample is None:
                raise SilverContractError(
                    "preview metadata must reference declared input/output sample artifacts"
                )
            if (
                input_sample.row_count != self.preview.input_sample_rows
                or output_sample.row_count != self.preview.output_sample_rows
            ):
                raise SilverContractError(
                    "preview sample artifact row counts do not match metadata"
                )
            qa_result_ids = {check.result_id for check in self.qa_checks}
            declared_case_results = {
                result_id
                for result_ids in self.preview.fixed_case_qa_result_ids.values()
                for result_id in result_ids
            }
            if not declared_case_results.issubset(qa_result_ids):
                raise SilverContractError("fixed-case evidence references unknown QA results")
        qa_output_rows = sum(
            int(output.row_count or 0) for output in self.outputs if output.role is ArtifactRole.QA
        )
        if qa_output_rows != len(self.qa_checks):
            raise SilverContractError("QA artifact rows do not match embedded QA results")
        data_rows: dict[str, int] = {}
        for output in self.outputs:
            if output.role is ArtifactRole.DATA and output.table is not None:
                data_rows[output.table] = data_rows.get(output.table, 0) + int(
                    output.row_count or 0
                )
        if data_rows != dict(self.row_funnel.output_rows_by_table):
            raise SilverContractError("data artifact rows do not match output_rows_by_table")

    @property
    def build_id(self) -> str:
        return self.intent.build_id

    @property
    def status(self) -> str:
        return "preview_ready" if self.intent.kind is BuildKind.PREVIEW else "full_ready"

    def to_dict(self) -> dict[str, object]:
        return {
            "build_manifest_version": BUILD_MANIFEST_VERSION,
            "completed_at": self.completed_at,
            "intent": self.intent.to_dict(),
            "outputs": [item.to_dict() for item in self.outputs],
            "preview": None if self.preview is None else self.preview.to_dict(),
            "qa_checks": [item.to_dict() for item in self.qa_checks],
            "quarantine_issue_rows": self.quarantine_issue_rows,
            "quarantine_issue_ids_by_severity": {
                key: list(value) for key, value in self.quarantine_issue_ids_by_severity.items()
            },
            "quarantine_unique_source_rows": self.quarantine_unique_source_rows,
            "row_funnel": self.row_funnel.to_dict(),
            "started_at": self.started_at,
            "status": self.status,
        }

    @classmethod
    def from_dict(cls, value: object) -> BuildManifest:
        document = _mapping(value, "build manifest")
        required = {
            "build_manifest_version",
            "completed_at",
            "intent",
            "outputs",
            "preview",
            "qa_checks",
            "quarantine_issue_rows",
            "quarantine_issue_ids_by_severity",
            "quarantine_unique_source_rows",
            "row_funnel",
            "started_at",
            "status",
        }
        _expect_keys(document, required, "build manifest")
        if document["build_manifest_version"] != BUILD_MANIFEST_VERSION:
            raise SilverContractError("unsupported build manifest version")
        manifest = cls(
            intent=BuildIntent.from_dict(document["intent"]),
            outputs=tuple(
                ArtifactRef.from_dict(item) for item in _array(document["outputs"], "outputs")
            ),
            row_funnel=RowFunnel.from_dict(document["row_funnel"]),
            qa_checks=tuple(
                QACheckResult.from_dict(item) for item in _array(document["qa_checks"], "qa_checks")
            ),
            quarantine_issue_rows=_native_nonnegative_int(
                document["quarantine_issue_rows"], "quarantine_issue_rows"
            ),
            quarantine_issue_ids_by_severity={
                key: _string_tuple(item, "quarantine issue IDs")
                for key, item in _mapping(
                    document["quarantine_issue_ids_by_severity"],
                    "quarantine_issue_ids_by_severity",
                ).items()
            },
            quarantine_unique_source_rows=_native_nonnegative_int(
                document["quarantine_unique_source_rows"],
                "quarantine_unique_source_rows",
            ),
            started_at=_string(document["started_at"], "started_at"),
            completed_at=_string(document["completed_at"], "completed_at"),
            preview=(
                None
                if document["preview"] is None
                else PreviewMetadata.from_dict(document["preview"])
            ),
        )
        if document["status"] != manifest.status:
            raise SilverContractError("build status does not match build kind")
        return manifest


@dataclass(frozen=True, slots=True)
class ApprovalReceipt:
    workflow_id: str
    stage: ApprovalStage
    decision: ApprovalDecision
    subject_id: str
    subject_manifest_sha256: str
    expected_event_sha256: str
    approver: str
    decided_at: str
    note: str
    waived_qa_result_ids: tuple[str, ...] = ()
    accepted_quarantine_issue_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "waived_qa_result_ids",
            _as_tuple(self.waived_qa_result_ids, "waived_qa_result_ids"),
        )
        object.__setattr__(
            self,
            "accepted_quarantine_issue_ids",
            _as_tuple(
                self.accepted_quarantine_issue_ids,
                "accepted_quarantine_issue_ids",
            ),
        )
        for label, value in (
            ("workflow_id", self.workflow_id),
            ("subject_id", self.subject_id),
            ("subject_manifest_sha256", self.subject_manifest_sha256),
            ("expected_event_sha256", self.expected_event_sha256),
        ):
            _validate_sha256(value, label)
        if not isinstance(self.stage, ApprovalStage) or not isinstance(
            self.decision, ApprovalDecision
        ):
            raise SilverContractError("approval stage/decision enum is invalid")
        _validate_text(self.approver, "approver", maximum=200)
        _parse_utc(self.decided_at, "decided_at")
        _validate_text(self.note, "approval note", maximum=4_000, allow_empty=True)
        ensure_json_safe({"approver": self.approver, "note": self.note}, label="approval")
        for result_id in self.waived_qa_result_ids:
            _validate_sha256(result_id, "waived QA result ID")
        _unique(self.waived_qa_result_ids, "waived QA result ID")
        object.__setattr__(self, "waived_qa_result_ids", tuple(sorted(self.waived_qa_result_ids)))
        for issue_id in self.accepted_quarantine_issue_ids:
            _validate_sha256(issue_id, "accepted quarantine issue ID")
        _unique(self.accepted_quarantine_issue_ids, "accepted quarantine issue ID")
        object.__setattr__(
            self,
            "accepted_quarantine_issue_ids",
            tuple(sorted(self.accepted_quarantine_issue_ids)),
        )

    @property
    def approval_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "approval_schema_version": APPROVAL_SCHEMA_VERSION,
            "accepted_quarantine_issue_ids": list(self.accepted_quarantine_issue_ids),
            "approver": self.approver,
            "decided_at": self.decided_at,
            "decision": self.decision.value,
            "expected_event_sha256": self.expected_event_sha256,
            "note": self.note,
            "stage": self.stage.value,
            "subject_id": self.subject_id,
            "subject_manifest_sha256": self.subject_manifest_sha256,
            "waived_qa_result_ids": list(self.waived_qa_result_ids),
            "workflow_id": self.workflow_id,
        }

    def to_dict(self) -> dict[str, object]:
        return {"approval_id": self.approval_id, **self.logical_payload()}

    @classmethod
    def from_dict(cls, value: object) -> ApprovalReceipt:
        document = _mapping(value, "approval receipt")
        required = {
            "approval_id",
            "approval_schema_version",
            "accepted_quarantine_issue_ids",
            "approver",
            "decided_at",
            "decision",
            "expected_event_sha256",
            "note",
            "stage",
            "subject_id",
            "subject_manifest_sha256",
            "waived_qa_result_ids",
            "workflow_id",
        }
        _expect_keys(document, required, "approval receipt")
        if document["approval_schema_version"] != APPROVAL_SCHEMA_VERSION:
            raise SilverContractError("unsupported approval schema version")
        try:
            stage = ApprovalStage(document["stage"])
            decision = ApprovalDecision(document["decision"])
        except (TypeError, ValueError) as exc:
            raise SilverContractError("approval stage/decision is invalid") from exc
        receipt = cls(
            workflow_id=_string(document["workflow_id"], "workflow_id"),
            stage=stage,
            decision=decision,
            subject_id=_string(document["subject_id"], "subject_id"),
            subject_manifest_sha256=_string(
                document["subject_manifest_sha256"], "subject_manifest_sha256"
            ),
            expected_event_sha256=_string(
                document["expected_event_sha256"], "expected_event_sha256"
            ),
            approver=_string(document["approver"], "approver"),
            decided_at=_string(document["decided_at"], "decided_at"),
            note=_string(document["note"], "approval note", allow_empty=True),
            waived_qa_result_ids=_string_tuple(
                document["waived_qa_result_ids"], "waived_qa_result_ids"
            ),
            accepted_quarantine_issue_ids=_string_tuple(
                document["accepted_quarantine_issue_ids"],
                "accepted_quarantine_issue_ids",
            ),
        )
        if document["approval_id"] != receipt.approval_id:
            raise SilverContractError("approval receipt digest mismatch")
        return receipt


@dataclass(frozen=True, slots=True)
class ReleaseManifest:
    workflow_id: str
    domain: str
    table: str
    schema_version: int
    contract_id: str
    build_id: str
    build_manifest_sha256: str
    approval_id: str
    approval_sha256: str
    released_at: str
    outputs: tuple[ArtifactRef, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "outputs", _as_tuple(self.outputs, "release outputs"))
        for label, value in (
            ("workflow_id", self.workflow_id),
            ("contract_id", self.contract_id),
            ("build_id", self.build_id),
            ("build_manifest_sha256", self.build_manifest_sha256),
            ("approval_id", self.approval_id),
            ("approval_sha256", self.approval_sha256),
        ):
            _validate_sha256(value, label)
        _validate_identifier(self.domain, "release domain")
        _validate_identifier(self.table, "release table")
        _native_positive_int(self.schema_version, "release schema_version")
        _parse_utc(self.released_at, "released_at")
        if not self.outputs:
            raise SilverContractError("release outputs cannot be empty")
        _unique((output.path for output in self.outputs), "release output path")
        if any(output.role is not ArtifactRole.DATA for output in self.outputs):
            raise SilverContractError("release outputs can only contain reviewed data artifacts")
        if any(output.table != self.table for output in self.outputs):
            raise SilverContractError("release output table does not match the release table")
        object.__setattr__(self, "outputs", tuple(sorted(self.outputs, key=lambda item: item.path)))

    @property
    def release_id(self) -> str:
        return stable_digest(self.logical_payload())

    def logical_payload(self) -> dict[str, object]:
        return {
            "approval_id": self.approval_id,
            "approval_sha256": self.approval_sha256,
            "build_id": self.build_id,
            "build_manifest_sha256": self.build_manifest_sha256,
            "contract_id": self.contract_id,
            "domain": self.domain,
            "outputs": [item.to_dict() for item in self.outputs],
            "release_schema_version": RELEASE_SCHEMA_VERSION,
            "released_at": self.released_at,
            "schema_version": self.schema_version,
            "table": self.table,
            "workflow_id": self.workflow_id,
        }

    def to_dict(self) -> dict[str, object]:
        return {"release_id": self.release_id, **self.logical_payload()}

    @classmethod
    def from_dict(cls, value: object) -> ReleaseManifest:
        document = _mapping(value, "release manifest")
        required = {
            "approval_id",
            "approval_sha256",
            "build_id",
            "build_manifest_sha256",
            "contract_id",
            "domain",
            "outputs",
            "release_id",
            "release_schema_version",
            "released_at",
            "schema_version",
            "table",
            "workflow_id",
        }
        _expect_keys(document, required, "release manifest")
        if document["release_schema_version"] != RELEASE_SCHEMA_VERSION:
            raise SilverContractError("unsupported release schema version")
        release = cls(
            workflow_id=_string(document["workflow_id"], "workflow_id"),
            domain=_string(document["domain"], "release domain"),
            table=_string(document["table"], "release table"),
            schema_version=_native_positive_int(
                document["schema_version"], "release schema_version"
            ),
            contract_id=_string(document["contract_id"], "contract_id"),
            build_id=_string(document["build_id"], "build_id"),
            build_manifest_sha256=_string(
                document["build_manifest_sha256"], "build_manifest_sha256"
            ),
            approval_id=_string(document["approval_id"], "approval_id"),
            approval_sha256=_string(document["approval_sha256"], "approval_sha256"),
            released_at=_string(document["released_at"], "released_at"),
            outputs=tuple(
                ArtifactRef.from_dict(item) for item in _array(document["outputs"], "outputs")
            ),
        )
        if document["release_id"] != release.release_id:
            raise SilverContractError("release manifest digest mismatch")
        return release


def arrow_schema_digest(schema: pa.Schema) -> str:
    """Return a stable digest for the exact Arrow names, types, and nullability."""

    if schema.metadata:
        raise SilverContractError("Silver Arrow schemas cannot carry file-level metadata")
    fields: list[dict[str, object]] = []
    for field in schema:
        if field.metadata:
            raise SilverContractError("Silver Arrow fields cannot carry metadata")
        fields.append(
            {
                "name": field.name,
                "nullable": field.nullable,
                "type": str(field.type),
            }
        )
    return stable_digest(fields)


def ensure_json_safe(
    value: object,
    *,
    label: str,
    max_list_items: int = 1_000,
) -> Mapping[str, object]:
    """Return a canonical JSON mapping after rejecting secrets and unsafe values."""

    if type(max_list_items) is not int or not 1 <= max_list_items <= 10_000:
        raise SilverContractError("max_list_items must be a native int between 1 and 10000")
    document = _mapping(value, label)
    normalized = _json_value(
        document,
        label=label,
        depth=0,
        max_list_items=max_list_items,
    )
    if not isinstance(normalized, dict):  # pragma: no cover - guarded by _mapping
        raise SilverContractError(f"{label} must be an object")
    # Canonical serialization is also the final NaN/Infinity guard.
    json.dumps(normalized, allow_nan=False, separators=(",", ":"), sort_keys=True)
    frozen = _freeze_json(normalized)
    if not isinstance(frozen, Mapping):  # pragma: no cover - normalized is a dict
        raise SilverContractError(f"{label} must be an object")
    return frozen


def thaw_json(value: object) -> object:
    """Return a detached JSON-compatible copy of recursively frozen contract data."""

    if isinstance(value, Mapping):
        return {key: thaw_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [thaw_json(item) for item in value]
    return value


def _freeze_json(value: object) -> object:
    if isinstance(value, dict):
        return MappingProxyType({key: _freeze_json(item) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(_freeze_json(item) for item in value)
    return value


def _json_value(
    value: object,
    *,
    label: str,
    depth: int,
    max_list_items: int,
) -> object:
    if depth > 8:
        raise SilverContractError(f"{label} exceeds the maximum nesting depth")
    if value is None or type(value) in {bool, int}:
        return value
    if type(value) is float:
        if not math.isfinite(value):
            raise SilverContractError(f"{label} contains a non-finite number")
        return value
    if isinstance(value, str):
        if len(value) > 4_096:
            raise SilverContractError(f"{label} contains an oversized string")
        if _BEARER.search(value) or "MASSIVE_API_KEY" in value or _SIGNED_QUERY.search(value):
            raise SilverContractError(f"{label} contains credential-like text")
        return value
    if isinstance(value, Mapping):
        if len(value) > 1_000:
            raise SilverContractError(f"{label} contains too many keys")
        result: dict[str, object] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise SilverContractError(f"{label} keys must be strings")
            lowered = key.lower()
            if any(sensitive in lowered for sensitive in _SENSITIVE_KEYS):
                raise SilverContractError(f"{label} contains a sensitive key: {key}")
            result[key] = _json_value(
                item,
                label=label,
                depth=depth + 1,
                max_list_items=max_list_items,
            )
        return dict(sorted(result.items()))
    if isinstance(value, (list, tuple)):
        if len(value) > max_list_items:
            raise SilverContractError(f"{label} contains too many items")
        return [
            _json_value(
                item,
                label=label,
                depth=depth + 1,
                max_list_items=max_list_items,
            )
            for item in value
        ]
    raise SilverContractError(f"{label} contains a non-JSON value")


def _parse_utc(value: object, label: str) -> datetime:
    text = _string(value, label)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SilverContractError(f"{label} is not an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != UTC.utcoffset(parsed):
        raise SilverContractError(f"{label} must be timezone-aware UTC")
    return parsed


def _validate_identifier(value: object, label: str) -> str:
    text = _string(value, label)
    if not _IDENTIFIER.fullmatch(text):
        raise SilverContractError(f"{label} is not a lowercase identifier: {text!r}")
    return text


def _validate_check_id(value: object) -> str:
    text = _string(value, "check_id")
    if not _CHECK_ID.fullmatch(text):
        raise SilverContractError(f"invalid check_id: {text!r}")
    return text


def _validate_sha256(value: object, label: str) -> str:
    text = _string(value, label)
    if not _SHA256.fullmatch(text):
        raise SilverContractError(f"{label} must be 64 lowercase hexadecimal characters")
    return text


def _validate_relative_text_path(value: object) -> str:
    text = _string(value, "artifact path")
    path = Path(text)
    if path.is_absolute() or not path.parts or any(part in {"", ".", ".."} for part in path.parts):
        raise SilverContractError("artifact path must be a normalized relative path")
    if "\\" in text or "\x00" in text:
        raise SilverContractError("artifact path contains a forbidden character")
    if path.as_posix() != text:
        raise SilverContractError("artifact path must be lexically normalized")
    return text


def _native_nonnegative_int(value: object, label: str) -> int:
    if type(value) is not int or value < 0:
        raise SilverContractError(f"{label} must be a nonnegative native int")
    return value


def _native_positive_int(value: object, label: str) -> int:
    result = _native_nonnegative_int(value, label)
    if result == 0:
        raise SilverContractError(f"{label} must be positive")
    return result


def _native_bool(value: object, label: str) -> bool:
    if type(value) is not bool:
        raise SilverContractError(f"{label} must be a native bool")
    return value


def _string(value: object, label: str, *, allow_empty: bool = False) -> str:
    if not isinstance(value, str):
        raise SilverContractError(f"{label} must be a string")
    if value != value.strip():
        raise SilverContractError(f"{label} cannot have surrounding whitespace")
    if not allow_empty and not value:
        raise SilverContractError(f"{label} cannot be empty")
    if _BEARER.search(value) or "MASSIVE_API_KEY" in value or _SIGNED_QUERY.search(value):
        raise SilverContractError(f"{label} contains credential-like text")
    return value


def _validate_text(
    value: object,
    label: str,
    *,
    maximum: int,
    allow_empty: bool = False,
) -> str:
    text = _string(value, label, allow_empty=allow_empty)
    if len(text) > maximum:
        raise SilverContractError(f"{label} exceeds {maximum} characters")
    return text


def _mapping(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise SilverContractError(f"{label} must be an object")
    return dict(value)


def _array(value: object, label: str) -> list[object]:
    if not isinstance(value, list):
        raise SilverContractError(f"{label} must be an array")
    return value


def _as_tuple(value: object, label: str) -> tuple[Any, ...]:
    if not isinstance(value, (list, tuple)):
        raise SilverContractError(f"{label} must be an array or tuple")
    return tuple(value)


def _string_tuple(value: object, label: str) -> tuple[str, ...]:
    return tuple(_string(item, label) for item in _array(value, label))


def _expect_keys(document: Mapping[str, object], expected: set[str], label: str) -> None:
    actual = set(document)
    if actual != expected:
        raise SilverContractError(
            f"{label} keys mismatch: missing={sorted(expected - actual)}, "
            f"extra={sorted(actual - expected)}"
        )


def _unique(values: Iterable[object], label: str) -> None:
    items = tuple(values)
    if len(set(items)) != len(items):
        raise SilverContractError(f"{label} values must be unique")
