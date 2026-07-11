"""Deterministic, network-free plans for Massive daily S3 Flat Files."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date
from enum import StrEnum

from ame_stocks_api.downloads import market_session_dates


class FlatFileDataset(StrEnum):
    MINUTE_AGGREGATES = "minute_aggregates"
    DAY_AGGREGATES = "day_aggregates"

    @property
    def prefix(self) -> str:
        if self is FlatFileDataset.MINUTE_AGGREGATES:
            return "us_stocks_sip/minute_aggs_v1"
        return "us_stocks_sip/day_aggs_v1"


@dataclass(frozen=True, slots=True)
class FlatFileObject:
    dataset: FlatFileDataset
    session_date: date

    @property
    def object_key(self) -> str:
        value = self.session_date
        return (
            f"{self.dataset.prefix}/{value.year:04d}/{value.month:02d}/{value.isoformat()}.csv.gz"
        )

    @property
    def object_id(self) -> str:
        canonical = json.dumps(
            {
                "dataset": self.dataset.value,
                "object_key": self.object_key,
                "session_date": self.session_date.isoformat(),
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        return hashlib.sha256(canonical).hexdigest()


@dataclass(frozen=True, slots=True)
class FlatFilePlan:
    dataset: FlatFileDataset
    objects: tuple[FlatFileObject, ...]

    def summary(self, *, show_all: bool = False) -> dict[str, object]:
        displayed = self.objects if show_all else self.objects[:10]
        return {
            "dataset": self.dataset.value,
            "end": self.objects[-1].session_date.isoformat(),
            "note": "Plan output is offline; object sizes require authenticated S3 HEAD requests.",
            "object_count": len(self.objects),
            "objects": [
                {
                    "object_id": item.object_id,
                    "object_key": item.object_key,
                    "session_date": item.session_date.isoformat(),
                }
                for item in displayed
            ],
            "start": self.objects[0].session_date.isoformat(),
            "truncated": not show_all and len(displayed) < len(self.objects),
        }


def build_flat_file_plan(
    *,
    dataset: FlatFileDataset,
    start: date,
    end: date,
) -> FlatFilePlan:
    return FlatFilePlan(
        dataset=dataset,
        objects=tuple(
            FlatFileObject(dataset=dataset, session_date=session)
            for session in market_session_dates(start, end)
        ),
    )
