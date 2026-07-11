# Factor plugins

Factor implementations in this directory are reviewed and versioned in Git. Each plugin exports an `ame_stocks_core.FactorSpec`; arbitrary user-uploaded Python is intentionally unsupported.

Every `FactorSpec.compute` function accepts a Polars `LazyFrame` and returns exactly:

```text
signal_date : Date
asset_id    : String
raw_value   : Float64
```

The first concrete plugins—20-session momentum and 5-session reversal—arrive with the Step 4 backtest engine.
