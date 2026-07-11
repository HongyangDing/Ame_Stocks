from datetime import date

import polars as pl
import pytest

from ame_stocks_core import FactorDirection, FactorSpec


def _valid_compute(frame: pl.LazyFrame) -> pl.LazyFrame:
    return frame.select(
        pl.col("date").alias("signal_date"),
        "asset_id",
        pl.col("close").alias("raw_value"),
    )


def test_factor_spec_normalizes_public_output_schema() -> None:
    spec = FactorSpec(
        factor_id="example_signal",
        version="1.0.0",
        display_name="Example signal",
        description="A minimal contract test factor.",
        lookback_sessions=1,
        required_columns=("date", "asset_id", "close"),
        direction=FactorDirection.HIGHER_IS_BETTER,
        compute=_valid_compute,
    )
    input_frame = pl.DataFrame(
        {
            "date": [date(2026, 1, 2)],
            "asset_id": ["AAPL"],
            "close": [100],
        }
    ).lazy()

    output = spec.run(input_frame).collect()

    assert output.columns == ["signal_date", "asset_id", "raw_value"]
    assert output.schema == {
        "signal_date": pl.Date,
        "asset_id": pl.String,
        "raw_value": pl.Float64,
    }
    assert spec.public_metadata()["direction"] == 1


def test_factor_spec_rejects_nonstandard_output() -> None:
    spec = FactorSpec(
        factor_id="bad_output",
        version="1",
        display_name="Bad output",
        description="Returns the wrong column names.",
        lookback_sessions=1,
        required_columns=("close",),
        direction=FactorDirection.LOWER_IS_BETTER,
        compute=lambda frame: frame.select("close"),
    )

    with pytest.raises(ValueError, match="exactly"):
        spec.run(pl.DataFrame({"close": [1.0]}).lazy())


def test_factor_spec_rejects_invalid_identity() -> None:
    with pytest.raises(ValueError, match="snake_case"):
        FactorSpec(
            factor_id="Bad-ID",
            version="1",
            display_name="Bad identity",
            description="Invalid ID should fail early.",
            lookback_sessions=1,
            required_columns=("close",),
            direction=FactorDirection.HIGHER_IS_BETTER,
            compute=_valid_compute,
        )
