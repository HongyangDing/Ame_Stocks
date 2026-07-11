"""Contract for trusted, Git-managed Python factor plugins."""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from enum import IntEnum, StrEnum

import polars as pl

FACTOR_CONTRACT_VERSION = "1.0"
FACTOR_OUTPUT_COLUMNS = ("signal_date", "asset_id", "raw_value")
_FACTOR_ID_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")

FactorCompute = Callable[[pl.LazyFrame], pl.LazyFrame]


class FactorDirection(IntEnum):
    """Expected relationship between a raw signal and forward returns."""

    HIGHER_IS_BETTER = 1
    LOWER_IS_BETTER = -1


class FactorFrequency(StrEnum):
    """Supported signal frequencies for the initial platform."""

    DAILY = "daily"


@dataclass(frozen=True, slots=True)
class FactorSpec:
    """Metadata and compute entrypoint for one immutable factor version."""

    factor_id: str
    version: str
    display_name: str
    description: str
    lookback_sessions: int
    required_columns: tuple[str, ...]
    direction: FactorDirection
    compute: FactorCompute
    frequency: FactorFrequency = FactorFrequency.DAILY

    def __post_init__(self) -> None:
        if not _FACTOR_ID_PATTERN.fullmatch(self.factor_id):
            raise ValueError("factor_id must be lowercase snake_case")
        if not self.version.strip():
            raise ValueError("version cannot be blank")
        if not self.display_name.strip():
            raise ValueError("display_name cannot be blank")
        if not self.description.strip():
            raise ValueError("description cannot be blank")
        if self.lookback_sessions < 1:
            raise ValueError("lookback_sessions must be positive")
        if not self.required_columns:
            raise ValueError("required_columns cannot be empty")
        if any(not column.strip() for column in self.required_columns):
            raise ValueError("required_columns cannot contain blank values")
        if len(set(self.required_columns)) != len(self.required_columns):
            raise ValueError("required_columns must be unique")
        if not callable(self.compute):
            raise TypeError("compute must be callable")

    def run(self, frame: pl.LazyFrame) -> pl.LazyFrame:
        """Execute the plugin and normalize its public output schema."""

        output = self.compute(frame)
        if not isinstance(output, pl.LazyFrame):
            raise TypeError("factor compute must return a polars.LazyFrame")

        output_columns = tuple(output.collect_schema().names())
        if output_columns != FACTOR_OUTPUT_COLUMNS:
            raise ValueError(
                "factor output columns must be exactly "
                f"{FACTOR_OUTPUT_COLUMNS}; received {output_columns}"
            )

        return output.select(
            pl.col("signal_date").cast(pl.Date),
            pl.col("asset_id").cast(pl.String),
            pl.col("raw_value").cast(pl.Float64),
        )

    def public_metadata(self) -> dict[str, object]:
        """Serializable metadata for APIs and run manifests."""

        return {
            "contract_version": FACTOR_CONTRACT_VERSION,
            "description": self.description,
            "direction": int(self.direction),
            "display_name": self.display_name,
            "factor_id": self.factor_id,
            "frequency": self.frequency.value,
            "lookback_sessions": self.lookback_sessions,
            "output_columns": list(FACTOR_OUTPUT_COLUMNS),
            "required_columns": list(self.required_columns),
            "version": self.version,
        }
