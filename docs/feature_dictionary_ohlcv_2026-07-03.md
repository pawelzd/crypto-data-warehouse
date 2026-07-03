# Column dictionary — OHLCV features (2026-07-03)

New columns appended to `rl_inference_features_next_open_v`, keyed by
`(token_address, price_timestamp)`. All rolling features are causal
(`ROWS ... PRECEDING AND CURRENT ROW`) and NULL until the window is full.

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

## Validation

`analyses/20m_eval_ohlc_feature_validation.sql` implements §8.1 (warm-up),
§8.3 (bounds), §8.5 (estimator sanity), §8.6 (universe/rank consistency).
Every row returns `failing_rows` (0 == pass). Not yet executed against
BigQuery in this environment (no warehouse keyfile available).
