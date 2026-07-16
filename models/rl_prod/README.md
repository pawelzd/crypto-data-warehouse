# RL production inference views

This directory is a self-contained production feature path. No model here
references the `20m_eval` dataset or any model located in `models/20m_eval`.

The live graph is intentionally small:

1. `rl_prod_hourly_bars_v` reads `core.token_ohlcv`, selects only close and
   volume, aggregates to an hourly grid, and forward-fills missing closes.
2. `rl_prod_asset_features_v` computes the shared token/BTC/SOL rolling
   features once.
3. `rl_prod_universe_weekly_inputs_v2` materializes threshold-free causal
   weekly measurements. `scripts/universe/universe_membership_v2.py` applies
   the shared YAML and writes the physical state; `rl_prod_universe_membership_pit`
   exposes the selected v1/v2 contract.
4. `rl_prod_inference_features_v` computes universe and relative features over
   members only, joins the reference series, and serves current members.

The `_history_v` graph instantiates the same three parameterized SQL macros
without a source or output time bound. Its final relation is
`rl_prod_inference_features_history_v`. It exists only for historical parity
checks; querying it recomputes the full source history and is substantially
more expensive than the live graph.

## Production universe and versioned model contracts

The v2 contract is defined only in
`config/universe_dynamic_scaling_v2.yml`. Entry requires two consecutive
weekly passes of causal born-bad evidence, observed-hour continuity, $20M
trailing median market cap, a hybrid absolute/market-relative dollar-volume
floor, Amihud and spread quality, then incumbent-first seating under a 150
member cap. Exit retains the $5M/two-week and rank-250/four-week bands, adds a
four-week quality-failure band, and immediately removes newly causal born-bad
evidence. Death/event patterns never erase pre-event history.

The production view defaults to the frozen v1 snapshot. Selecting v2 requires
`--vars '{"rl_prod_universe_version": "v2"}'` at the associated retrain/deploy
boundary. This prevents an ordinary dbt run from silently changing an existing
checkpoint's cross-sectional feature contract.

The history view retains candidate rows and adds `in_universe_pit` plus raw
`dollar_vol_24h`; `univ_*` and `rel_*` are still computed only from true
members. The live view filters to members. Those two audit columns are in
addition to the model's required 108-column contract and are ignored by the
existing scaler.

`rl_prod.btc_reference_contract_v1` pins BTC values through the trusted cutoff.
After that hour, the pipeline splices to the restored `btcusdt` series in
`core.token_ohlcv`; Q2 overlap validation found zero mismatches in hourly,
24-hour, and 7-day BTC returns. The inner join makes the pipeline fail closed if
the BTC reference is missing instead of silently turning missing BTC-relative
features into zeros.

`rl_prod.ohlcv_revision_override_v1` pins trusted price and volume inputs for
the eight tokens whose histories were revised after training. The override is
applied at the hourly-bar layer before returns, rolling windows, universe
aggregates, ranks, or costs are calculated, and only for artifact keys through
the trusted cutoff. Later live bars continue from `core.token_ohlcv`.

Asset and inference models are BigQuery views. V2 membership is stateful: the
weekly input table and shared builder must run after OHLCV/scam changes. The
Monday Airflow DAG performs that sequence and records the YAML SHA-256 hash on
every state row.

## Runtime bounds

- `rl_prod_history_hours` defaults to 960 (40 days). This covers the 168-hour
  token warm-up followed by the 720-hour universe-volatility z-score window,
  with a safety margin.
- `rl_prod_output_hours` defaults to 48. The serving view exposes recent rows
  only, while its upstream calculation retains enough history for parity.
- `rl_prod_minimum_member_coverage` defaults to 0.90. Live output fails closed
  on partially ingested hours containing fewer than 90% of weekly members.
- `rl_prod_min_mktcap` defaults to 0 and is only a candidate-quality floor.
  Weekly membership owns the actual $20M entry/$5M exit thresholds.
- `trade_size_usd` defaults to 250 and controls the cost proxies, matching the
  training view.

The newest source hour is excluded until the following hour exists, because
`next_next_open` is load-bearing for mark-to-market and must never be null.

`core.token_ohlcv` is configured as a daily partitioned table, clustered by
`chain, token_address`. The production predicate filters `price_timestamp`
directly, allowing BigQuery to scan only the recent partitions. The model has
`full_refresh=false` so even a project-wide `dbt run --full-refresh` cannot drop
this source-of-record table. A physical backup created during the partition
migration is retained as `core.token_ohlcv_backup_20260712_111221`.
