# Column dictionary — OHLCV features (2026-07-03)

New columns appended to `rl_inference_features_next_open_v`, keyed by
`(token_address, price_timestamp)`. All rolling features are causal
(`ROWS ... PRECEDING AND CURRENT ROW`) and NULL until the window is full.

**Model layout.** The per-token OHLC feature derivation (§4 + §7) lives in a
separate materialized table, `rl_ohlc_candle_features`, because inlining its
~10 stacked window passes into the view exceeds BigQuery's query-planning
complexity limit. The view joins that table and computes the cross-sectional
(§5A/§5B) and market-reference (§5C) columns on top. Build order:
`dbt run -s rl_ohlc_candle_features` (or `dbt run -s +rl_inference_features_next_open_v`)
before querying the view.

## Source & conventions (§1 verification)

- **OHLC source:** `token_ohlcv` (Birdeye candles), deduped to one row per
  `(token_address, price_timestamp)` keeping the highest-volume chain listing.
  This is a *different* source from the existing `price`/`volume` columns
  (which come from `public_historical_prices`); the OHLC features are
  internally consistent against `token_ohlcv`.
- **Dollar volume:** `token_ohlcv.volume` unit is unconfirmed, so the §1.1
  proxy is used: `dvol_1h = volume * typ_price_1h`.
- **BTC/SOL refs:** `token_ohlcv` `token_address IN ('btcusdt',
  'So111…112')`.
- **Deviation:** `ad_price_diverge_168h` uses a locally computed OHLC
  log-close trend slope (not the existing `trend_slope_7d`) so both z-scores
  share one row series.
- **Join:** all new columns are `LEFT JOIN`ed onto the existing view; the
  prior 162 columns and the row/key set are unchanged (§8.8).

Window shorthand: `w24` = 24 trailing rows incl. current, `w168` = 168,
`w720` = 720. "lag" features warm up one extra row (window+1).

## §4A Order flow / accumulation

| column | formula | window | range |
|---|---|---|---|
| `clv_1h` | `(2c−h−l)/(h−l)`, 0 if h=l | 1 | [−1,1] |
| `clv_mean_24h` / `_168h` | `AVG(clv_1h)` | 24 / 168 | [−1,1] |
| `flow_imbalance_24h` / `_168h` | `SUM(clv·dvol)/SUM(dvol)` | 24 / 168 | [−1,1] |
| `ad_slope_168h` | OLS slope of cumulative normalized flow line `adl` vs row idx | 168 | ℝ |
| `ad_price_diverge_168h` | `z720(ad_slope_168h) − z720(ohlc log-close slope)` | 720 | ℝ |
| `mfi_24h` | `100·ΣMF+/(ΣMF+ + ΣMF−)`, typ·dvol split by typ vs prev typ | 24 (lag) | [0,100] |
| `vwap_dist_24h` / `_168h` | `LN(close / VWAP)`, VWAP = `Σ(typ·dvol)/Σdvol` | 24 / 168 | ℝ |
| `wick_asym_24h` | `AVG(lw_frac − uw_frac)` | 24 | [−1,1] |

## §4B Range volatility & compression

| column | formula | window | range |
|---|---|---|---|
| `atr_24h` / `atr_168h` | `AVG(tr_1h)`, normalized true range | 24 / 168 (lag) | ≥0 |
| `atr_ratio_24_168` | `atr_24h / atr_168h` | — | ≥0 |
| `parkinson_rv_24h` / `_7d` | `SQRT(AVG(log_hl²)/(4·ln2))` | 24 / 168 | ≥0 |
| `rv_eff_ratio_24h` | `parkinson_rv_24h / rv_24h` (existing rv_24h) | 24 | ≥0 |
| `range_z_24h` | z-score of `range_1h` | 24 | ℝ |
| `squeeze_pctile_720h` | rolling percent-rank of `atr_24h` in its 720 window | 720 | [0,1] |
| `nr_pctrank_24h` | rolling percent-rank of `range_1h` in its 24 window | 24 | [0,1] |
| `consec_inside_6h` | consecutive trailing inside bars, capped 6 | cumulative | [0,6] |

## §4C Extremes / breakouts / path

| column | formula | window | range |
|---|---|---|---|
| `dist_to_true_high_24h/168h/720h` | `LN(close / MAX(high))`, incl. current | 24/168/720 | ≤0 |
| `dist_to_true_low_24h/168h` | `LN(close / MIN(low))` | 24/168 | ≥0 |
| `true_breakout_high_24h` | `close > MAX(high)` over 24 **excl.** current | 24 excl (lag) | {0,1} |
| `true_breakout_low_24h` | `close < MIN(low)` over 24 excl current | 24 excl (lag) | {0,1} |
| `true_range_pos_168h` | `(close−MIN low)/(MAX high−MIN low)` | 168 | [0,1] |
| `bars_since_true_high_168h` | bars since max high, `/168` | 168 | [0,1] |
| `zero_range_frac_24h` | `AVG(h=l)` | 24 | [0,1] |

## §4D Microstructure / liquidity

| column | formula | window | range |
|---|---|---|---|
| `cs_spread_24h_bps` | Corwin-Schultz `s_t` averaged, ×1e4 | 24 (lag) | ≥0 |
| `cs_spread_z_168h` | z-score of `cs_spread_24h_bps` | 168 | ℝ |
| `range_impact_24h` | `AVG(range_1h / dvol_1h)` (raw, heavy tail) | 24 | ≥0 |

## §5A Cross-sectional universe aggregates (active universe, per timestamp)

`univ_med_clv_24h`, `univ_med_flow_imbalance_24h`,
`univ_frac_true_breakout_high_24h`, `univ_frac_true_breakout_low_24h`,
`univ_med_atr_ratio_24_168`, `univ_frac_squeeze` (`squeeze_pctile_720h<0.25`),
`univ_med_cs_spread_24h`. Medians via `APPROX_QUANTILES(...)[OFFSET(50)]`.

## §5B Relative ranks (percent-rank within same-timestamp active universe)

`rel_rank_flow_imbalance_168h`, `rel_rank_atr_ratio_24_168` — both [0,1];
NULL for non-universe rows.

## §5C BTC / SOL market references

`btc_*` and `sol_*` variants of `atr_ratio_24_168`, `parkinson_rv_24h`,
`rv_eff_ratio_24h` (÷ existing `btc_rv_24h`/`sol_rv_24h`), `clv_mean_24h`,
`squeeze_pctile_720h`. Joined by `price_timestamp`.

## §7 Tier-2 per-token

| column | formula | window | range |
|---|---|---|---|
| `adx_24h` / `adx_168h` | simple-MA ADX (not Wilder); `AVG(dx)` | 24/168 (2× warmup) | [0,100] |
| `di_diff_24h` | `di_plus_24h − di_minus_24h` | 24 (lag) | ℝ |
| `choppiness_168h` | `100·log10(Σtr_raw/(max h−min l))/log10(168)` | 168 (lag) | [0,100] |
| `vortex_24h` | `Σ|h−prev l|/Σtr_raw − Σ|l−prev h|/Σtr_raw` | 24 (lag) | ℝ |
| `er_24h` / `er_168h` | Kaufman efficiency `|Δclose_N|/Σ|Δclose|` | 24/168 (lag) | [0,1] |
| `gk_rv_24h` | Garman-Klass RV, clamp neg pre-sqrt | 24 | ≥0 |
| `rs_rv_24h` | Rogers-Satchell RV, clamp neg pre-sqrt | 24 | ≥0 |
| `gap_abs_mean_24h` | `AVG(|gap_1h|)` | 24 (lag) | ≥0 |
| `max_gap_168h` | `MAX(|gap_1h|)` | 168 (lag) | ≥0 |
| `uw_frac_mean_24h` / `lw_frac_mean_24h` | `AVG(uw_frac)` / `AVG(lw_frac)` | 24 | [0,1] |
| `max_uw_frac_24h` | `MAX(uw_frac)` | 24 | [0,1] |
| `body_frac_mean_24h` | `AVG(body_frac)` | 24 | [0,1] |
| `ar_spread_24h_bps` | Abdi-Ranaldo `2·√max(AVG(x_t),0)·1e4` | 24 (lag) | ≥0 |

## Deviations from spec (documented per §6.5 / §9)

- ADX uses simple moving averages, not Wilder EMA smoothing (§6.5, accepted).
- `dvol_1h` uses the token-unit → dollar proxy (§1.1).
- `ad_price_diverge_168h` uses an OHLC-local log-close trend slope instead of
  the existing `trend_slope_7d` for z-score consistency.
- `squeeze_pctile_720h` ships the full percent-rank form (not the z-score
  fallback); cost is a 720-float array per row (§6.1 warning).

## BTC / SOL market-regime features (2026-07-05 build)

Single-series OHLCV features on the BTC (`btcusdt`) and SOL (`So111…112`)
candles, one value per `price_timestamp` broadcast to every altcoin row by
the `price_timestamp` join — a market-level *regime* signal, not a
cross-sectional one. They reuse the exact per-token recipes above (a
single-token `PARTITION BY token_address` window in `rl_ohlc_candle_features`
*is* the single series), so no new computation — they are surfaced from that
table with `btc_`/`sol_` prefixes.

**Already shipped (not rebuilt):** `{btc,sol}_atr_ratio_24_168`,
`_parkinson_rv_24h`, `_clv_mean_24h`, `_squeeze_pctile_720h`,
`_rv_eff_ratio_24h`.

**Added Tier-1 (each with both `btc_` and `sol_` prefix):**
`flow_imbalance_24h/168h`, `ad_slope_168h`, `ad_price_diverge_168h`,
`mfi_24h`, `vwap_dist_24h/168h`, `wick_asym_24h`, `atr_24h`, `atr_168h`,
`range_z_24h`, `nr_pctrank_24h`, `cs_spread_24h_bps`, `cs_spread_z_168h`,
`adx_24h`, `adx_168h`, `di_diff_24h`, `choppiness_168h`, `vortex_24h`,
`er_24h`, `er_168h`, `dist_to_true_high_24h/168h/720h`,
`dist_to_true_low_24h/168h`, `true_breakout_high_24h`, `true_breakout_low_24h`,
`true_range_pos_168h`, `bars_since_true_high_168h`, `gk_rv_24h`, `rs_rv_24h`.
Formulas/windows/ranges are identical to the per-token columns of the same
suffix above.

**Deliberately not surfaced** (degenerate on an always-liquid reference
series, per §2): `{btc,sol}_zero_range_frac_24h` (always 0),
`_range_impact_24h` (no liquidity signal), per-bar `_clv_1h` (ship smoothed
`_clv_mean_*`).

**Tier-2 (§7, gated):** not built. The spec gates Tier-2 on the Tier-1
cross-regime stability audit, which is downstream (no warehouse here). Add in
a second pass if Tier-1 clears the §7 adoption bar.

**Adoption gate (§7):** BTC/SOL features are judged on *cross-regime sign
stability* of per-regime TS-IC vs forward universe-median return — the 5
pre-existing OHLCV refs failed exactly there (e.g. `btc_clv_mean_24h` TS-IC
−0.34 bull / +0.07 crash). Run that audit before retraining.

## Horizon / rotation / regime features (2026-07-07 build, 26 cols)

Four blocks serving the 2–4 week rotation, 50/100/200-day regime, and
token-lifecycle clocks (the existing features stop at 168h).

**Model layout.** Block A (per-token, heavy 90d + unbounded-ATH windows) is a
new materialized table `rl_token_horizon_features`, computed over the *full*
per-token history from `20m_cv_prod_72_7d_before` (not the filtered
`eval_dataset`) so ATH and long windows are correct. Block B (SOL, single
series) is computed inline in the view from `cv_btc_sol_1h`. Blocks C/D
(cross-sectional) are in the view. All reuse the existing price series and
recipes (arithmetic `drawdown`/`dist_to_sma`, EXP-sum-logret `cumret`,
log-price-rel gaps, OLS log-close slope). Build order:
`dbt run -s rl_token_horizon_features` before the view.

**A. token rotation/lifecycle (9)** — `cumret_14d`, `cumret_30d` (w336/w720);
`drawdown_14d`, `drawdown_30d` (`price/MAX-1`, ≤0); `dist_to_sma_336h`,
`dist_to_sma_720h` (`price/AVG-1`); `log_gap_from_90d_high` (w2160, ≤0);
`log_gap_from_ath` (unbounded, ≤0, from bar 1); `ath_recency_frac`
(`bars_since_ath/(rn-1)`, [0,1], NULL on bar 1).

**B. SOL regime (8)** — `sol_dist_to_sma_50d/100d/200d` (w1200/2400/4800);
`sol_drawdown_from_180d_high` (log, w4320, ≤0); `sol_cumret_14d/30d`
(w336/w720); `sol_trend_slope_30d/90d` (OLS log-close, w720/w2160). One value
per `price_timestamp`, broadcast to all altcoin rows.

**C. universe breadth (3)** — `univ_pct_above_sma_720h`,
`univ_frac_near_30d_high` (within 5% of 30d high, `log_gap_from_30d_high >
LN(0.95)`), `univ_med_cumret_30d`. Active-universe aggregates per timestamp.

**D. relative rotation (6)** — `rel_rank_cumret_14d`, `rel_rank_cumret_30d`,
`rel_rank_drawdown_7d` (active-universe percent-ranks, [0,1]);
`rel_excess_vs_sol_7d/14d/30d` (`cumret_Nd − sol_cumret_Nd`, mirroring
`rel_excess_vs_btc_*`).

Per the 2026-07-08 reprioritization: Block D is the flagship (regime-neutral);
raw `cumret_14d/30d` are momentum-family (down-market harm) shipped mainly as
`rel_excess` intermediates; Block B must be tested on both configs. Every
column faces the residual-IC-vs-nearest-sibling gate downstream before retrain.

## Information-structure features (2026-07-08 build, 19 cols)

Blocks that change the *information structure* (memory / cross-section /
measurement), not per-bar transforms of the same price/volume marginal. Built
in SQL; A2 lead-lag (Python side-pipeline) and C1/C2 (offline) are deferred.

**Model layout.** Two new tables: `rl_meme_index` (per-timestamp equal-weight
active-universe index — `idx_logret_1h`, `idx_cumret_7d/30d`, and idx variance
for herding; replicates the view's `active_universe` predicate to break the
dependency loop) and `rl_info_structure_features` (per-token A1/A3/B1/B2 with
the 60d/w1440 rolling covariance kept out of the view). Index helpers, the
per-token join, and B3 herding are assembled in the view. Build order:
`rl_meme_index` → `rl_info_structure_features` → view.

**§2 index helpers (3)** — `idx_logret_1h` (active-universe `AVG(logret_1h)`),
`idx_cumret_7d/30d` (`SUM(idx_logret_1h)` over w168/w720 timestamp series, log
cumret). Note: `idx_cumret_*` are log sums per spec, whereas the incumbent
`cumret_7d`/`sol_cumret_7d` are simple `EXP(Σ)−1` — a documented spec asymmetry
in `idio_mom_idx_*`.

**A1 idio momentum (6)** — `beta_sol_60d`, `beta_idx_60d`
(`COVAR/VAR` over w1440, winsorized [−2,5]); `idio_mom_sol_7d`,
`idio_mom_idx_7d` (`cumret_7d − β·ref_cumret_7d`); `idio_mom_idx_30d`;
`idio_vol_share_60d` (`1 − CORR²`, [0,1]). Tested downstream as a **swap** for
`rel_excess_vs_btc_*`, not an add.

**A3 dormancy (2)** — `dormancy_days_log` (`LN(1 + dormancy_h/24)`),
`reawakening_score` (`dormancy_days_log · GREATEST(volume_z_24h,0)`); non-zero
only when a long-dormant token bursts now.

**B1 VPIN proxy (3)** — `vpin_24h`, `vpin_168h`
(`Σ|tanh(0.851·z)|·dvol / Σdvol`, [0,1]), `vpin_z_30d` (z over w720). Wall-clock
buckets, not equal-volume — the SQL-feasible proxy.

**B2 jump decomposition (3)** — `jump_share_24h`, `jump_share_168h`
(`(RV−BPV)⁺/RV`, [0,1]), `downside_semivar_share_24h`.

**B3 herding (2)** — `herding_ratio_168h`, `herding_ratio_720h`
(`Var(idx) / mean_i Var_i`, ~[1/N,1]; higher = universe moving as one).

Per §0-bis: build SQL blocks first (testable next day); A2 is the flagship but
needs the daily pipeline; context blocks (VPIN, herding) screened on config B
only. Residual-IC-vs-sibling gate applies to every column downstream.

## Momentum / pullback quality features (2026-07-11 build, 5 cols)

Nonlinear reorderings (z / corr / fraction / skew) of existing channels — they
create cross-sectional ordering no current column has (the explicit bet vs the
rank-preserving adds that washed). Per-token in a new table
`rl_quality_features` (full history from `20m_cv_prod_72_7d_before`), joined
into the base. Grade *which strong, liquid, pulling-back tokens continue vs
break down* — the discrimination the momentum-on-pullback policy makes blind.

| column | formula | window | range |
|---|---|---|---|
| `pullback_z_720h` | z-score of `drawdown_7d` vs its own 720h history | 720 | unbounded (≈0 median) |
| `vol_mom_align_168h` | population Pearson corr(`logret_1h`, `volume_z_24h`) | 168 | [−1,1] |
| `up_bar_frac_72h` | `AVG(logret_1h > 0)` — grind vs spike | 72 | [0,1] |
| `momentum_accel_24_168` | `cumret_24h − cumret_7d·24/168` — early vs late | — | unbounded (≈0 median) |
| `ret_skew_168h` | population skew of `logret_1h` (moment form), winsorized | 168 | [−10,10] |

`ret_skew_168h` uses the moment expansion `(E[x³] − 3mE[x²] + 2m³)/s³` (single
pass over `AVG(logret)`, `AVG(logret²)`, `AVG(logret³)`), not a nested windowed
mean. The **§4.5 degeneracy gate** is the point: within-timestamp
Spearman(`pullback_z_720h`, `drawdown_7d`) must be < 0.98 (else the z-score
collapsed to another rank-preserving dead transform); same for
`momentum_accel` vs `cumret_24h`. Downstream: residual-IC vs sibling, screen at
n≥8 (n=4 is triage only).

## Validation

- `analyses/20m_eval_ohlc_feature_validation.sql` — per-token §8.1 (warm-up),
  §8.3 (bounds), §8.5 (estimator sanity), §8.6 (universe/rank consistency).
- `analyses/20m_eval_btc_sol_regime_validation.sql` — BTC/SOL §7.4
  join-broadcast (one distinct value per timestamp) and §6.3 bounds.
- `analyses/20m_eval_horizon_feature_validation.sql` — 2026-07-07 blocks:
  §5.3 bounds, §5.4 nesting invariants (log-gap and drawdown), §5.1 warm-up,
  SOL/univ broadcast, and rel-rank centering.
- `analyses/20m_eval_infostructure_validation.sql` — 2026-07-08 blocks:
  §7.2 bounds, §7.3 beta-residual identity, §7.1 warm-up, idx/herding broadcast,
  and §7.4 regime sanity (herding / downside-semivar / vpin-z crash > bull).
- `analyses/20m_eval_quality_feature_validation.sql` — 2026-07-11 quality block:
  §4.2 bounds, §4.1 warm-up, §4.5 degeneracy (within-ts Spearman < 0.98 vs
  sibling), §4.4 sign sanity.

Every row returns `failing_rows` (0 == pass). Compiled under dbt 1.10.9; not
yet executed against BigQuery in this environment (no warehouse keyfile).
