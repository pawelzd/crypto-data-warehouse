# RL production inference views

This directory is a self-contained production feature path. No model here
references the `20m_eval` dataset or any model located in `models/20m_eval`.

The live graph is intentionally small:

1. `rl_prod_hourly_bars_v` reads `core.token_ohlcv`, selects only close and
   volume, aggregates to an hourly grid, and forward-fills missing closes.
2. `rl_prod_asset_features_v` computes the shared token/BTC/SOL rolling
   features once.
3. `rl_prod_universe_membership_pit` stores the stateful weekly point-in-time
   membership derived from trailing market cap and raw dollar volume.
4. `rl_prod_inference_features_v` computes universe and relative features over
   members only, joins the reference series, and serves current members.

The `_history_v` graph instantiates the same three parameterized SQL macros
without a source or output time bound. Its final relation is
`rl_prod_inference_features_history_v`. It exists only for historical parity
checks; querying it recomputes the full source history and is substantially
more expensive than the live graph.

## Production universe and versioned model contracts

The cross-sectional universe is computed from the current production feature
inputs. A token is admitted when it has the required 168-hour lookback, passes
the configured market-cap floor, and does not appear in `scam_h_union`.
`scam_h_union` consumes hardened full-history evidence. Only born-bad patterns
with matches spanning more than 90 days, plus manual exclusions, are lifetime
filters. Death/event patterns remain evidence and are handled by PIT membership
exit hysteresis; they do not delete pre-collapse history.

Membership entry requires trailing-30d median market cap of at least $20M and
trailing-30d dollar-volume rank at most 120. Members exit after two consecutive
weeks below $5M or four consecutive weeks below rank 250. The weekly state is
rebuilt from complete OHLCV history, but every decision uses only timestamps
strictly before that week; no CMC snapshot or reconstructed future qualification
date participates.

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

All three models are BigQuery views in the `rl_prod` dataset. Source changes are
therefore visible on the next query after `core.token_ohlcv` itself has been
updated by its incremental dbt job.

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
