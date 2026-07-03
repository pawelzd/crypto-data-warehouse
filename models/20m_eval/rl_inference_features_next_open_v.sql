{{ config(
    materialized='view'
) }}

{% set trade_size_usd = var('trade_size_usd', 250.0) %}

WITH token_supply AS (
  SELECT
    address,
    MAX(SAFE_CAST(circulating_supply AS FLOAT64)) AS circulating_supply
  FROM {{ source('streamed_datapublic', 'public_tokens_to_monitor') }}
  GROUP BY address
),

filled_hours AS (
  SELECT
    address,
    datetime,
    AVG(SAFE_CAST(price AS FLOAT64)) AS price,
    MAX(SAFE_CAST(volume AS FLOAT64)) AS volume
  FROM {{ ref('20m_cv_prod_filled_hours') }}
  GROUP BY address, datetime
),

base AS (
SELECT
  ts.token_address,
  ts.decision_ts AS price_timestamp,
  ts.price AS next_open,
  cv.price AS next_next_open,
  ts.ret_1h,
  ts.logret_1h,
  ts.mean_ret_24h,
  ts.std_ret_24h,
  ts.mean_ret_72h,
  ts.std_ret_72h,
  ts.mean_ret_168h,
  ts.std_ret_168h,
  ts.rv_24h,
  ts.rv_7d,
  ts.sharpe_24h,
  ts.sharpe_7d,
  ts.ret_z_24h,
  ts.cumret_24h,
  ts.cumret_7d,
  ts.price_z_24h,
  ts.pct_in_range_24h,
  ts.dist_to_sma_6h,
  ts.dist_to_sma_12h,
  ts.dist_to_sma_24h,
  ts.dist_to_sma_72h,
  ts.dist_to_sma_168h,
  ts.dist_to_high_24h,
  ts.dist_to_low_24h,
  ts.breakout_high_24h,
  ts.breakout_low_24h,
  ts.drawdown_7d,
  ts.rsi_14,
  ts.rsi_vol_interaction,
  ts.dow_1_sun_7_sat,
  ts.hour_of_day,
  ts.sin_hour,
  ts.cos_hour,
  ts.sin_dow,
  ts.cos_dow,
  ts.vol_ratio_24_7d,
  ts.vol_ratio_24_72,
  ts.vol_ratio_72_168,
  ts.ret_over_rv_12h,
  ts.sma_diff_fast_slow,
  ts.volume_ret_1h,
  ts.volume_ret_24h,
  ts.volume_cv_24h,
  ts.volume_cv_168h,
  ts.volume_spike_ratio_24h_excl,
  ts.volume_z_24h,
  ts.volume_accel_6v24,
  ts.volume_accel_24v168,
  ts.sharpe_delta,
  ts.btc_ret_1h,
  ts.btc_logret_1h,
  ts.btc_mean_ret_24h,
  ts.btc_std_ret_24h,
  ts.btc_mean_ret_72h,
  ts.btc_std_ret_72h,
  ts.btc_mean_ret_168h,
  ts.btc_std_ret_168h,
  ts.btc_rv_24h,
  ts.btc_rv_7d,
  ts.btc_sharpe_24h,
  ts.btc_sharpe_7d,
  ts.btc_ret_z_24h,
  ts.btc_macd_sma_12_26h,
  ts.btc_dist_to_sma_6h,
  ts.btc_dist_to_sma_12h,
  ts.btc_dist_to_sma_24h,
  ts.btc_dist_to_sma_168h,
  ts.btc_pct_in_range_24h,
  ts.btc_dist_to_high_24h,
  ts.btc_dist_to_low_24h,
  ts.btc_breakout_high_24h,
  ts.btc_breakout_low_24h,
  ts.btc_drawdown_7d,
  ts.btc_rsi_14,
  ts.btc_sin_hour,
  ts.btc_cos_hour,
  ts.btc_sin_dow,
  ts.btc_cos_dow,
  ts.btc_vol_ratio_24_7d,
  ts.btc_sharpe_delta,
  ts.spread_ret_1h,
  ts.spread_logret_1h,
  ts.sol_ret_1h,
  ts.sol_logret_1h,
  ts.sol_mean_ret_24h,
  ts.sol_std_ret_24h,
  ts.sol_mean_ret_72h,
  ts.sol_std_ret_72h,
  ts.sol_mean_ret_168h,
  ts.sol_std_ret_168h,
  ts.sol_rv_24h,
  ts.sol_rv_7d,
  ts.sol_sharpe_24h,
  ts.sol_sharpe_7d,
  ts.sol_ret_z_24h,
  ts.sol_cumret_24h,
  ts.sol_cumret_7d,
  ts.sol_macd_sma_12_26h,
  ts.sol_dist_to_sma_6h,
  ts.sol_dist_to_sma_12h,
  ts.sol_dist_to_sma_24h,
  ts.sol_dist_to_sma_72h,
  ts.sol_dist_to_sma_168h,
  ts.sol_pct_in_range_24h,
  ts.sol_dist_to_high_24h,
  ts.sol_dist_to_low_24h,
  ts.sol_breakout_high_24h,
  ts.sol_breakout_low_24h,
  ts.sol_drawdown_7d,
  ts.sol_rsi_14,
  ts.sol_sin_hour,
  ts.sol_cos_hour,
  ts.sol_sin_dow,
  ts.sol_cos_dow,
  ts.sol_vol_ratio_24_7d,
  ts.sol_sharpe_delta,
  ts.std_ret_24h AS std_ret_24h_cost,
  SAFE_CAST(cur.volume AS FLOAT64) AS volume_cost,
  SAFE_CAST(ts.price AS FLOAT64) * s.circulating_supply AS mktcap_cost
FROM {{ ref('20m_cv_prod_eval_dataset') }} ts
LEFT JOIN filled_hours cv
  ON ts.token_address = cv.address
 AND TIMESTAMP_ADD(ts.decision_ts, INTERVAL 1 HOUR) = cv.datetime
LEFT JOIN filled_hours cur
  ON ts.token_address = cur.address
 AND ts.decision_ts = cur.datetime
LEFT JOIN token_supply s
  ON ts.token_address = s.address
),

cost_bars AS (
  SELECT
    b.*,
    COALESCE(b.volume_cost, 0.0) * COALESCE(SAFE_CAST(b.next_open AS FLOAT64), 0.0) AS dollar_volume_h,
    LAG(COALESCE(b.ret_1h, 0.0)) OVER (
      PARTITION BY b.token_address
      ORDER BY b.price_timestamp
    ) AS _ret_1h_lag_for_spread
  FROM base b
),

cost_rollups AS (
  SELECT
    cb.*,
    COALESCE(SUM(cb.dollar_volume_h) OVER w24, 0.0) AS dollar_vol_24h,
    COALESCE(SUM(cb.dollar_volume_h) OVER w24, 0.0) AS volume_usd_cost,
    COALESCE(AVG(SAFE_DIVIDE(ABS(COALESCE(cb.ret_1h, 0.0)), NULLIF(cb.volume_cost, 0.0))) OVER w24, 0.0) AS amihud_cost,
    COALESCE(AVG(SAFE_DIVIDE(ABS(COALESCE(cb.ret_1h, 0.0)), NULLIF(cb.dollar_volume_h, 0.0))) OVER w24, 0.0) AS dollar_amihud_24h,
    COALESCE(SUM(cb.dollar_volume_h) OVER w168, 0.0) AS dollar_vol_7d,
    COALESCE(EXP(SUM(COALESCE(cb.btc_logret_1h, 0.0)) OVER w24) - 1, 0.0) AS btc_cumret_24h,
    COALESCE(EXP(SUM(COALESCE(cb.btc_logret_1h, 0.0)) OVER w168) - 1, 0.0) AS btc_cumret_7d,
    COALESCE(MIN(cb.dollar_volume_h) OVER w24, 0.0) AS min_dollar_vol_h_in_24h,
    COALESCE(SAFE_DIVIDE(
      STDDEV_SAMP(cb.dollar_volume_h) OVER w24,
      NULLIF(AVG(cb.dollar_volume_h) OVER w24, 0.0)
    ), 0.0) AS volume_cv_24h_dollar,
    COVAR_SAMP(COALESCE(cb.ret_1h, 0.0), cb._ret_1h_lag_for_spread) OVER w24 AS _ret_lag_cov_24h,
    ARRAY_AGG(cb.dollar_volume_h) OVER w24 AS _dollar_vol_24h_arr
  FROM cost_bars cb
  WINDOW
    w24 AS (
      PARTITION BY cb.token_address
      ORDER BY cb.price_timestamp
      ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
    ),
    w168 AS (
      PARTITION BY cb.token_address
      ORDER BY cb.price_timestamp
      ROWS BETWEEN 167 PRECEDING AND CURRENT ROW
    )
),

cost_inputs AS (
  SELECT
    * EXCEPT (dollar_volume_h, _ret_1h_lag_for_spread, _ret_lag_cov_24h, _dollar_vol_24h_arr),
    COALESCE((
      SELECT v
      FROM (
        SELECT
          v,
          ROW_NUMBER() OVER (ORDER BY v) AS rn,
          COUNT(*) OVER () AS n
        FROM UNNEST(_dollar_vol_24h_arr) AS v
      )
      WHERE rn = CAST(FLOOR((n - 1) * 0.10) AS INT64) + 1
      LIMIT 1
    ), 0.0) AS p10_dollar_vol_h_in_24h,
    CAST(
      CASE
        WHEN _ret_lag_cov_24h < 0 THEN 2.0 * SQRT(-_ret_lag_cov_24h) * 10000.0
        ELSE 0.0
      END
    AS FLOAT64) AS effective_spread_24h_bps
  FROM cost_rollups
),

cost_intermediates AS (
  SELECT
    *,
    LN(GREATEST(COALESCE(volume_usd_cost, 0.0), 1.0)) AS _log_v,
    LN(GREATEST(COALESCE(mktcap_cost, 0.0), 1.0)) AS _log_mc,
    LN(GREATEST({{ trade_size_usd }} / GREATEST(COALESCE(volume_usd_cost, 0.0), 1.0), 1e-10)) AS _log_tv,
    LN(GREATEST(COALESCE(volume_usd_cost, 0.0) / GREATEST(COALESCE(mktcap_cost, 0.0), 1.0), 1e-10)) AS _log_vm,
    CAST({{ trade_size_usd }} AS FLOAT64) / GREATEST(COALESCE(volume_usd_cost, 0.0), 1.0) AS _trade_to_vol
  FROM cost_inputs
),

cost_cols AS (
  SELECT
    * EXCEPT (_log_v, _log_mc, _log_tv, _log_vm, _trade_to_vol),

    LEAST(
      CAST(
        EXP(
            -0.371
          + 0.573 * LN(GREATEST(CAST({{ trade_size_usd }} AS FLOAT64), 1.0))
          + 0.424 * _log_tv
          + 0.218 * _log_vm
          + 0.149 * _log_v
          - 0.069 * _log_mc
        ) / CAST({{ trade_size_usd }} AS FLOAT64) * 10000.0
      AS FLOAT64),
      500.0
    ) AS fee_bps_default,

    CAST(
      CASE
        WHEN _trade_to_vol > 0.005  THEN 300.0
        WHEN _trade_to_vol > 0.0005 THEN  50.0
        ELSE                              10.0
      END
    AS FLOAT64) AS slippage_bps_proxy
  FROM cost_intermediates
),

feature_input AS (
  SELECT
    *,
    (logret_1h IS NOT NULL AND mktcap_cost IS NOT NULL AND mktcap_cost > 0) AS active_universe
  FROM cost_cols
),

trend_prep AS (
  SELECT
    fi.*,
    ROW_NUMBER() OVER (
      PARTITION BY fi.token_address
      ORDER BY fi.price_timestamp
    ) AS _row_idx,
    SUM(COALESCE(fi.logret_1h, 0.0)) OVER (
      PARTITION BY fi.token_address
      ORDER BY fi.price_timestamp
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS _log_price_rel
  FROM feature_input fi
),

trend_features AS (
  SELECT
    tp.* EXCEPT (_row_idx, _log_price_rel),
    CASE
      WHEN COUNT(*) OVER w720 = 720
        THEN CAST(_log_price_rel - MAX(_log_price_rel) OVER w720 AS FLOAT64)
      ELSE NULL
    END AS log_gap_from_30d_high,
    CASE
      WHEN COUNT(*) OVER w720 = 720
        THEN CAST(_log_price_rel - MIN(_log_price_rel) OVER w720 AS FLOAT64)
      ELSE NULL
    END AS log_gap_to_30d_low,
    CASE
      WHEN COUNT(*) OVER w720 = 720
        THEN CAST(SAFE_DIVIDE(
          COVAR_POP(_log_price_rel, CAST(_row_idx AS FLOAT64)) OVER w720,
          NULLIF(VAR_POP(CAST(_row_idx AS FLOAT64)) OVER w720, 0.0)
        ) AS FLOAT64)
      ELSE NULL
    END AS trend_slope_30d,
    CASE
      WHEN COUNT(*) OVER w720 = 720
        THEN CAST(POW(COALESCE(CORR(_log_price_rel, CAST(_row_idx AS FLOAT64)) OVER w720, 0.0), 2) AS FLOAT64)
      ELSE NULL
    END AS trend_r2_30d,
    CASE
      WHEN COUNT(*) OVER w168 = 168
        THEN CAST(SAFE_DIVIDE(
          COVAR_POP(_log_price_rel, CAST(_row_idx AS FLOAT64)) OVER w168,
          NULLIF(VAR_POP(CAST(_row_idx AS FLOAT64)) OVER w168, 0.0)
        ) AS FLOAT64)
      ELSE NULL
    END AS trend_slope_7d,
    CASE
      WHEN COUNT(*) OVER w168 = 168
        THEN CAST(POW(COALESCE(CORR(_log_price_rel, CAST(_row_idx AS FLOAT64)) OVER w168, 0.0), 2) AS FLOAT64)
      ELSE NULL
    END AS trend_r2_7d
  FROM trend_prep tp
  WINDOW
    w168 AS (
      PARTITION BY tp.token_address
      ORDER BY tp.price_timestamp
      ROWS BETWEEN 167 PRECEDING AND CURRENT ROW
    ),
    w720 AS (
      PARTITION BY tp.token_address
      ORDER BY tp.price_timestamp
      ROWS BETWEEN 719 PRECEDING AND CURRENT ROW
    )
),

active_universe_rows AS (
  SELECT *
  FROM trend_features
  WHERE active_universe
),

univ_aggregates AS (
  SELECT
    price_timestamp,
    CAST(COUNT(*) AS FLOAT64) AS univ_n_active,
    CAST(APPROX_QUANTILES(CAST(cumret_24h AS FLOAT64), 100)[OFFSET(50)] AS FLOAT64) AS univ_med_logret_24h,
    CAST(AVG(CAST(cumret_24h AS FLOAT64)) AS FLOAT64) AS univ_mean_logret_24h,
    CAST(STDDEV_SAMP(CAST(cumret_24h AS FLOAT64)) AS FLOAT64) AS univ_disp_logret_24h,
    CAST(AVG(CASE WHEN cumret_24h > 0 THEN 1.0 ELSE 0.0 END) AS FLOAT64) AS univ_pct_pos_24h,
    CAST(AVG(CASE WHEN dist_to_sma_24h > 0 THEN 1.0 ELSE 0.0 END) AS FLOAT64) AS univ_pct_above_sma_24h,
    CAST(AVG(CASE WHEN dist_to_sma_168h > 0 THEN 1.0 ELSE 0.0 END) AS FLOAT64) AS univ_pct_above_sma_168h,
    CAST(AVG(CASE WHEN drawdown_7d <= -0.10 THEN 1.0 ELSE 0.0 END) AS FLOAT64) AS univ_pct_in_dd_gt_10pct,
    CAST(APPROX_QUANTILES(CAST(cumret_7d AS FLOAT64), 100)[OFFSET(50)] AS FLOAT64) AS univ_med_logret_168h,
    CAST(APPROX_QUANTILES(CAST(rv_24h AS FLOAT64), 100)[OFFSET(50)] AS FLOAT64) AS univ_med_rv_24h,
    CAST(ANY_VALUE(btc_rv_24h) AS FLOAT64) AS btc_rv_24h_univ
  FROM active_universe_rows
  GROUP BY price_timestamp
),

univ_regime_windows AS (
  SELECT
    ua.*,
    AVG(ua.univ_med_rv_24h) OVER w720 AS _univ_med_rv_24h_mean_30d,
    STDDEV_SAMP(ua.univ_med_rv_24h) OVER w720 AS _univ_med_rv_24h_std_30d,
    COUNT(*) OVER w720 AS _univ_rv_n_30d,
    ARRAY_AGG(ua.btc_rv_24h_univ) OVER w2160 AS _btc_rv_24h_2160_arr
  FROM univ_aggregates ua
  WINDOW
    w720 AS (
      ORDER BY ua.price_timestamp
      ROWS BETWEEN 719 PRECEDING AND CURRENT ROW
    ),
    w2160 AS (
      ORDER BY ua.price_timestamp
      ROWS BETWEEN 2159 PRECEDING AND CURRENT ROW
    )
),

univ_regime AS (
  SELECT
    * EXCEPT (
      _univ_med_rv_24h_mean_30d,
      _univ_med_rv_24h_std_30d,
      _univ_rv_n_30d,
      _btc_rv_24h_2160_arr
    ),
    CASE
      WHEN _univ_rv_n_30d = 720
        THEN CAST(SAFE_DIVIDE(
          univ_med_rv_24h - _univ_med_rv_24h_mean_30d,
          NULLIF(_univ_med_rv_24h_std_30d, 0.0)
        ) AS FLOAT64)
      ELSE NULL
    END AS univ_med_rv_24h_z_30d,
    CASE
      WHEN ARRAY_LENGTH(_btc_rv_24h_2160_arr) = 2160
        THEN CAST((
          SELECT SAFE_DIVIDE(COUNTIF(v < btc_rv_24h_univ), NULLIF(COUNT(*) - 1, 0))
          FROM UNNEST(_btc_rv_24h_2160_arr) AS v
        ) AS FLOAT64)
      ELSE NULL
    END AS btc_rv_24h_pctile_90d
  FROM univ_regime_windows
),

relative_ranks AS (
  SELECT
    token_address,
    price_timestamp,
    CAST(PERCENT_RANK() OVER (PARTITION BY price_timestamp ORDER BY cumret_24h) AS FLOAT64) AS rel_rank_cumret_24h,
    CAST(PERCENT_RANK() OVER (PARTITION BY price_timestamp ORDER BY cumret_7d) AS FLOAT64) AS rel_rank_cumret_7d,
    CAST(PERCENT_RANK() OVER (PARTITION BY price_timestamp ORDER BY dollar_vol_24h) AS FLOAT64) AS rel_rank_dollar_vol_24h
  FROM active_universe_rows
),

-- ============================================================================
-- OHLC candle features (build spec 2026-07-03)
-- Source: token_ohlcv (the pipeline's only OHLC source; the existing price /
-- volume columns come from public_historical_prices, a different source).
-- dvol_1h uses the spec §1.1 dollar proxy volume * typ_price (token_ohlcv
-- volume unit unconfirmed). ad_price_diverge_168h uses a locally computed
-- OHLC log-close trend slope so both z-scores share one row series (noted
-- deviation from the spec, which named the existing trend_slope_7d).
-- Every rolling feature is causal (ROWS ... PRECEDING AND CURRENT ROW) and
-- full-window gated on row_idx (row_idx >= N; lag-dependent features >= N+1).
-- Joined LEFT below so the existing columns and row count are unchanged.
-- ============================================================================
ohlc_dedup AS (
  SELECT
    o.token_address,
    o.price_timestamp,
    SAFE_CAST(o.open   AS FLOAT64) AS open,
    SAFE_CAST(o.high   AS FLOAT64) AS high,
    SAFE_CAST(o.low    AS FLOAT64) AS low,
    SAFE_CAST(o.close  AS FLOAT64) AS close,
    SAFE_CAST(o.volume AS FLOAT64) AS volume
  FROM {{ ref('token_ohlcv') }} o
  WHERE o.token_address IN (SELECT DISTINCT token_address FROM base)
     OR o.token_address IN ('btcusdt', 'So11111111111111111111111111111111111111112')
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY o.token_address, o.price_timestamp
    ORDER BY SAFE_CAST(o.volume AS FLOAT64) DESC
  ) = 1
),

-- Per-bar scalars (§3.1). No cross-bar terms yet.
ohlc_scalars AS (
  SELECT
    d.token_address,
    d.price_timestamp,
    d.open, d.high, d.low, d.close, d.volume,
    (d.high + d.low + d.close) / 3.0                              AS typ_price_1h,
    d.volume * ((d.high + d.low + d.close) / 3.0)                 AS dvol_1h,
    d.high - d.low                                                AS range_raw_1h,
    SAFE_DIVIDE(d.high - d.low, d.close)                          AS range_1h,
    COALESCE(SAFE.LN(SAFE_DIVIDE(d.high, d.low)), 0.0)            AS log_hl_1h,
    COALESCE(SAFE_DIVIDE(2*d.close - d.high - d.low, d.high - d.low), 0.0) AS clv_1h,
    COALESCE(SAFE_DIVIDE(ABS(d.close - d.open), d.high - d.low), 0.0)      AS body_frac_1h,
    COALESCE(SAFE_DIVIDE(d.high - GREATEST(d.open, d.close), d.high - d.low), 0.0) AS uw_frac_1h,
    COALESCE(SAFE_DIVIDE(LEAST(d.open, d.close) - d.low, d.high - d.low), 0.0)     AS lw_frac_1h,
    COALESCE(SAFE.LN(SAFE_DIVIDE(d.close, d.open)),  0.0)         AS ln_co,
    SAFE.LN(d.high)                                               AS ln_high,
    SAFE.LN(d.low)                                                AS ln_low,
    SAFE.LN(d.close)                                              AS ln_close,
    COALESCE(SAFE.LN(SAFE_DIVIDE(d.high, d.close)), 0.0)         AS ln_hc,
    COALESCE(SAFE.LN(SAFE_DIVIDE(d.high, d.open)),  0.0)         AS ln_ho,
    COALESCE(SAFE.LN(SAFE_DIVIDE(d.low,  d.close)), 0.0)         AS ln_lc,
    COALESCE(SAFE.LN(SAFE_DIVIDE(d.low,  d.open)),  0.0)         AS ln_lo,
    ROW_NUMBER() OVER w                                           AS row_idx,
    LAG(d.close) OVER w                                           AS prev_close,
    LAG(d.high)  OVER w                                           AS prev_high,
    LAG(d.low)   OVER w                                           AS prev_low
  FROM ohlc_dedup d
  WINDOW w AS (PARTITION BY d.token_address ORDER BY d.price_timestamp)
),

-- Cross-bar per-bar terms (LAG-based helpers).
ohlc_bar AS (
  SELECT
    s.*,
    s.clv_1h * s.dvol_1h                                          AS signed_dvol_1h,
    s.typ_price_1h * s.dvol_1h                                    AS typ_dvol_1h,
    s.ln_close - LAG(s.ln_close) OVER w                           AS logret_close_1h,
    GREATEST(s.range_raw_1h, ABS(s.high - s.prev_close), ABS(s.low - s.prev_close)) AS tr_raw_1h,
    SAFE_DIVIDE(
      GREATEST(s.range_raw_1h, ABS(s.high - s.prev_close), ABS(s.low - s.prev_close)),
      s.prev_close
    )                                                             AS tr_1h,
    SAFE_DIVIDE(s.open, s.prev_close) - 1                         AS gap_1h,
    IF(s.high < s.prev_high AND s.low > s.prev_low, 1, 0)         AS inside_1h,
    ABS(s.close - s.prev_close)                                   AS abs_dclose_1h,
    LAG(s.close, 24)  OVER w                                      AS close_lag24_1h,
    LAG(s.close, 168) OVER w                                      AS close_lag168_1h,
    IF(s.typ_price_1h > LAG(s.typ_price_1h) OVER w, s.typ_price_1h * s.dvol_1h, 0.0) AS mf_pos_1h,
    IF(s.typ_price_1h < LAG(s.typ_price_1h) OVER w, s.typ_price_1h * s.dvol_1h, 0.0) AS mf_neg_1h,
    IF((s.high - s.prev_high) > (s.prev_low - s.low) AND (s.high - s.prev_high) > 0,
       s.high - s.prev_high, 0.0)                                 AS plus_dm_1h,
    IF((s.prev_low - s.low) > (s.high - s.prev_high) AND (s.prev_low - s.low) > 0,
       s.prev_low - s.low, 0.0)                                   AS minus_dm_1h,
    ABS(s.high - s.prev_low)                                      AS vm_plus_1h,
    ABS(s.low - s.prev_high)                                      AS vm_minus_1h,
    POW(s.log_hl_1h, 2) + POW(LAG(s.log_hl_1h) OVER w, 2)         AS cs_beta_1h,
    POW(COALESCE(SAFE.LN(SAFE_DIVIDE(GREATEST(s.high, s.prev_high), LEAST(s.low, s.prev_low))), 0.0), 2) AS cs_gamma_1h,
    (s.ln_high + s.ln_low) / 2.0                                  AS eta_t_1h,
    (LAG(s.ln_high) OVER w + LAG(s.ln_low) OVER w) / 2.0          AS eta_prev_1h,
    LAG(s.ln_close) OVER w                                        AS c_prev_1h,
    0.5 * POW(s.log_hl_1h, 2) - (2.0*LN(2) - 1.0) * POW(s.ln_co, 2) AS gk_term_1h,
    s.ln_hc * s.ln_ho + s.ln_lc * s.ln_lo                        AS rs_term_1h
  FROM ohlc_scalars s
  WINDOW w AS (PARTITION BY s.token_address ORDER BY s.price_timestamp)
),

-- Per-bar derived estimators (Corwin-Schultz s_t, Abdi-Ranaldo x_t, impact).
ohlc_derived AS (
  SELECT
    b.*,
    SAFE_DIVIDE(b.range_1h, NULLIF(b.dvol_1h, 0.0))              AS range_over_dvol_1h,
    (b.c_prev_1h - b.eta_prev_1h) * (b.c_prev_1h - b.eta_t_1h)   AS ar_x_1h,
    CASE
      WHEN b.cs_beta_1h IS NULL OR b.cs_gamma_1h IS NULL THEN NULL
      WHEN ((SQRT(2.0*b.cs_beta_1h) - SQRT(b.cs_beta_1h)) / (3.0 - 2.0*SQRT(2.0)))
            - SQRT(b.cs_gamma_1h / (3.0 - 2.0*SQRT(2.0))) <= 0 THEN 0.0
      ELSE
        2.0 * (EXP(
          ((SQRT(2.0*b.cs_beta_1h) - SQRT(b.cs_beta_1h)) / (3.0 - 2.0*SQRT(2.0)))
           - SQRT(b.cs_gamma_1h / (3.0 - 2.0*SQRT(2.0)))
        ) - 1.0)
        / (1.0 + EXP(
          ((SQRT(2.0*b.cs_beta_1h) - SQRT(b.cs_beta_1h)) / (3.0 - 2.0*SQRT(2.0)))
           - SQRT(b.cs_gamma_1h / (3.0 - 2.0*SQRT(2.0)))
        ))
    END                                                          AS cs_s_1h
  FROM ohlc_bar b
),

-- First window pass (w24 / w168 / w720 / w24_excl).
ohlc_win1 AS (
  SELECT
    o.token_address, o.price_timestamp, o.row_idx,
    o.close, o.high, o.low, o.range_1h, o.inside_1h, o.clv_1h,
    o.signed_dvol_1h, o.dvol_1h, o.logret_close_1h,
    -- §4A order flow / accumulation
    IF(o.row_idx >= 24,  AVG(o.clv_1h) OVER w24,  NULL)          AS clv_mean_24h,
    IF(o.row_idx >= 168, AVG(o.clv_1h) OVER w168, NULL)          AS clv_mean_168h,
    IF(o.row_idx >= 24,  SAFE_DIVIDE(SUM(o.signed_dvol_1h) OVER w24,  NULLIF(SUM(o.dvol_1h) OVER w24, 0.0)),  NULL) AS flow_imbalance_24h,
    IF(o.row_idx >= 168, SAFE_DIVIDE(SUM(o.signed_dvol_1h) OVER w168, NULLIF(SUM(o.dvol_1h) OVER w168, 0.0)), NULL) AS flow_imbalance_168h,
    IF(o.row_idx >= 25,  100.0 * SAFE_DIVIDE(SUM(o.mf_pos_1h) OVER w24, NULLIF(SUM(o.mf_pos_1h) OVER w24 + SUM(o.mf_neg_1h) OVER w24, 0.0)), NULL) AS mfi_24h,
    IF(o.row_idx >= 24,  SAFE.LN(SAFE_DIVIDE(o.close, SAFE_DIVIDE(SUM(o.typ_dvol_1h) OVER w24,  NULLIF(SUM(o.dvol_1h) OVER w24, 0.0)))),  NULL) AS vwap_dist_24h,
    IF(o.row_idx >= 168, SAFE.LN(SAFE_DIVIDE(o.close, SAFE_DIVIDE(SUM(o.typ_dvol_1h) OVER w168, NULLIF(SUM(o.dvol_1h) OVER w168, 0.0)))), NULL) AS vwap_dist_168h,
    IF(o.row_idx >= 24,  AVG(o.lw_frac_1h - o.uw_frac_1h) OVER w24, NULL) AS wick_asym_24h,
    -- §4B range volatility & compression
    IF(o.row_idx >= 25,  AVG(o.tr_1h) OVER w24,  NULL)          AS atr_24h,
    IF(o.row_idx >= 169, AVG(o.tr_1h) OVER w168, NULL)          AS atr_168h,
    IF(o.row_idx >= 24,  SQRT(SAFE_DIVIDE(AVG(POW(o.log_hl_1h, 2)) OVER w24,  4.0*LN(2))), NULL) AS parkinson_rv_24h,
    IF(o.row_idx >= 168, SQRT(SAFE_DIVIDE(AVG(POW(o.log_hl_1h, 2)) OVER w168, 4.0*LN(2))), NULL) AS parkinson_rv_7d,
    AVG(o.range_1h) OVER w24                                     AS _range_mean_24h,
    STDDEV_SAMP(o.range_1h) OVER w24                             AS _range_std_24h,
    IF(o.row_idx >= 24,  AVG(IF(o.high = o.low, 1.0, 0.0)) OVER w24, NULL) AS zero_range_frac_24h,
    -- §4C extremes / breakouts / path
    IF(o.row_idx >= 24,  COALESCE(SAFE.LN(SAFE_DIVIDE(o.close, MAX(o.high) OVER w24)),  0.0), NULL) AS dist_to_true_high_24h,
    IF(o.row_idx >= 168, COALESCE(SAFE.LN(SAFE_DIVIDE(o.close, MAX(o.high) OVER w168)), 0.0), NULL) AS dist_to_true_high_168h,
    IF(o.row_idx >= 720, COALESCE(SAFE.LN(SAFE_DIVIDE(o.close, MAX(o.high) OVER w720)), 0.0), NULL) AS dist_to_true_high_720h,
    IF(o.row_idx >= 24,  COALESCE(SAFE.LN(SAFE_DIVIDE(o.close, MIN(o.low) OVER w24)),   0.0), NULL) AS dist_to_true_low_24h,
    IF(o.row_idx >= 168, COALESCE(SAFE.LN(SAFE_DIVIDE(o.close, MIN(o.low) OVER w168)),  0.0), NULL) AS dist_to_true_low_168h,
    IF(o.row_idx >= 25,  IF(o.close > MAX(o.high) OVER w24x, 1.0, 0.0), NULL) AS true_breakout_high_24h,
    IF(o.row_idx >= 25,  IF(o.close < MIN(o.low)  OVER w24x, 1.0, 0.0), NULL) AS true_breakout_low_24h,
    IF(o.row_idx >= 168, SAFE_DIVIDE(o.close - MIN(o.low) OVER w168, NULLIF(MAX(o.high) OVER w168 - MIN(o.low) OVER w168, 0.0)), NULL) AS true_range_pos_168h,
    -- §4D microstructure
    IF(o.row_idx >= 25,  AVG(o.cs_s_1h) OVER w24 * 10000.0, NULL) AS cs_spread_24h_bps,
    IF(o.row_idx >= 24,  AVG(o.range_over_dvol_1h) OVER w24, NULL) AS range_impact_24h,
    -- adl normalizer & cumulative log price (used downstream)
    SUM(o.dvol_1h) OVER w24                                      AS dvol_24h,
    SUM(o.logret_close_1h) OVER wcum                             AS _logclose_rel,
    -- §7 tier-2 directional / efficiency intermediates
    100.0 * SAFE_DIVIDE(SUM(o.plus_dm_1h)  OVER w24,  NULLIF(SUM(o.tr_raw_1h) OVER w24, 0.0))  AS di_plus_24h,
    100.0 * SAFE_DIVIDE(SUM(o.minus_dm_1h) OVER w24,  NULLIF(SUM(o.tr_raw_1h) OVER w24, 0.0))  AS di_minus_24h,
    100.0 * SAFE_DIVIDE(SUM(o.plus_dm_1h)  OVER w168, NULLIF(SUM(o.tr_raw_1h) OVER w168, 0.0)) AS di_plus_168h,
    100.0 * SAFE_DIVIDE(SUM(o.minus_dm_1h) OVER w168, NULLIF(SUM(o.tr_raw_1h) OVER w168, 0.0)) AS di_minus_168h,
    IF(o.row_idx >= 169, 100.0 * SAFE_DIVIDE(
        SAFE.LOG(SAFE_DIVIDE(SUM(o.tr_raw_1h) OVER w168, NULLIF(MAX(o.high) OVER w168 - MIN(o.low) OVER w168, 0.0)), 10.0),
        LOG(168.0, 10.0)), NULL)                                 AS choppiness_168h,
    IF(o.row_idx >= 25,
       SAFE_DIVIDE(SUM(o.vm_plus_1h)  OVER w24, NULLIF(SUM(o.tr_raw_1h) OVER w24, 0.0))
     - SAFE_DIVIDE(SUM(o.vm_minus_1h) OVER w24, NULLIF(SUM(o.tr_raw_1h) OVER w24, 0.0)), NULL) AS vortex_24h,
    IF(o.row_idx >= 25,  SAFE_DIVIDE(ABS(o.close - o.close_lag24_1h),  NULLIF(SUM(o.abs_dclose_1h) OVER w24,  0.0)), NULL) AS er_24h,
    IF(o.row_idx >= 169, SAFE_DIVIDE(ABS(o.close - o.close_lag168_1h), NULLIF(SUM(o.abs_dclose_1h) OVER w168, 0.0)), NULL) AS er_168h,
    IF(o.row_idx >= 24,  SQRT(GREATEST(AVG(o.gk_term_1h) OVER w24, 0.0)), NULL) AS gk_rv_24h,
    IF(o.row_idx >= 24,  SQRT(GREATEST(AVG(o.rs_term_1h) OVER w24, 0.0)), NULL) AS rs_rv_24h,
    IF(o.row_idx >= 25,  AVG(ABS(o.gap_1h)) OVER w24, NULL)      AS gap_abs_mean_24h,
    IF(o.row_idx >= 169, MAX(ABS(o.gap_1h)) OVER w168, NULL)     AS max_gap_168h,
    IF(o.row_idx >= 24,  AVG(o.uw_frac_1h) OVER w24, NULL)       AS uw_frac_mean_24h,
    IF(o.row_idx >= 24,  AVG(o.lw_frac_1h) OVER w24, NULL)       AS lw_frac_mean_24h,
    IF(o.row_idx >= 24,  MAX(o.uw_frac_1h) OVER w24, NULL)       AS max_uw_frac_24h,
    IF(o.row_idx >= 24,  AVG(o.body_frac_1h) OVER w24, NULL)     AS body_frac_mean_24h,
    IF(o.row_idx >= 25,  2.0 * SQRT(GREATEST(AVG(o.ar_x_1h) OVER w24, 0.0)) * 10000.0, NULL) AS ar_spread_24h_bps
  FROM ohlc_derived o
  WINDOW
    w24  AS (PARTITION BY o.token_address ORDER BY o.price_timestamp ROWS BETWEEN 23  PRECEDING AND CURRENT ROW),
    w168 AS (PARTITION BY o.token_address ORDER BY o.price_timestamp ROWS BETWEEN 167 PRECEDING AND CURRENT ROW),
    w720 AS (PARTITION BY o.token_address ORDER BY o.price_timestamp ROWS BETWEEN 719 PRECEDING AND CURRENT ROW),
    w24x AS (PARTITION BY o.token_address ORDER BY o.price_timestamp ROWS BETWEEN 24  PRECEDING AND 1 PRECEDING),
    wcum AS (PARTITION BY o.token_address ORDER BY o.price_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),

-- Second window pass: z-scores, slopes, per-window arrays, adl component.
ohlc_win2 AS (
  SELECT
    w.*,
    IF(w.row_idx >= 24, SAFE_DIVIDE(w.range_1h - w._range_mean_24h, NULLIF(w._range_std_24h, 0.0)), NULL) AS range_z_24h,
    IF(w.row_idx >= 192, SAFE_DIVIDE(
        w.cs_spread_24h_bps - AVG(w.cs_spread_24h_bps) OVER w168,
        NULLIF(STDDEV_SAMP(w.cs_spread_24h_bps) OVER w168, 0.0)), NULL) AS cs_spread_z_168h,
    w.di_plus_24h  - w.di_minus_24h                             AS di_diff_24h,
    100.0 * SAFE_DIVIDE(ABS(w.di_plus_24h  - w.di_minus_24h),  NULLIF(w.di_plus_24h  + w.di_minus_24h,  0.0)) AS dx_24h,
    100.0 * SAFE_DIVIDE(ABS(w.di_plus_168h - w.di_minus_168h), NULLIF(w.di_plus_168h + w.di_minus_168h, 0.0)) AS dx_168h,
    SAFE_DIVIDE(w.signed_dvol_1h, NULLIF(w.dvol_24h, 0.0))      AS adl_component,
    IF(w.row_idx >= 168, SAFE_DIVIDE(
        COVAR_POP(w._logclose_rel, CAST(w.row_idx AS FLOAT64)) OVER w168,
        NULLIF(VAR_POP(CAST(w.row_idx AS FLOAT64)) OVER w168, 0.0)), NULL) AS _trend_slope_7d_ohlc,
    -- analytic ARRAY_AGG cannot use IGNORE NULLS in BigQuery; COALESCE warmup
    -- NULLs to a -1.0 sentinel (atr/range/high are all >= 0) and filter it out
    -- in the consuming subqueries below.
    ARRAY_AGG(COALESCE(w.atr_24h, -1.0)) OVER w720             AS _arr_atr_720,
    ARRAY_AGG(COALESCE(w.range_1h, -1.0)) OVER w24             AS _arr_range_24,
    ARRAY_AGG(COALESCE(w.high, -1.0)) OVER w168                AS _arr_high_168,
    MAX(IF(w.inside_1h = 0, w.row_idx, NULL)) OVER wcum         AS _last_non_inside
  FROM ohlc_win1 w
  WINDOW
    w24  AS (PARTITION BY w.token_address ORDER BY w.price_timestamp ROWS BETWEEN 23  PRECEDING AND CURRENT ROW),
    w168 AS (PARTITION BY w.token_address ORDER BY w.price_timestamp ROWS BETWEEN 167 PRECEDING AND CURRENT ROW),
    w720 AS (PARTITION BY w.token_address ORDER BY w.price_timestamp ROWS BETWEEN 719 PRECEDING AND CURRENT ROW),
    wcum AS (PARTITION BY w.token_address ORDER BY w.price_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),

-- Third window pass: cumulative adl, adx smoothing, array percent-ranks.
ohlc_win3 AS (
  SELECT
    w.*,
    SUM(w.adl_component) OVER wcum                              AS adl,
    IF(w.row_idx >= 48,  AVG(w.dx_24h)  OVER w24,  NULL)       AS adx_24h,
    IF(w.row_idx >= 336, AVG(w.dx_168h) OVER w168, NULL)       AS adx_168h,
    CAST(LEAST(w.row_idx - COALESCE(w._last_non_inside, 0), 6) AS FLOAT64) AS consec_inside_6h,
    CASE WHEN (SELECT COUNT(1) FROM UNNEST(w._arr_atr_720) AS v WHERE v >= 0) >= 720 THEN
      (SELECT SAFE_DIVIDE(COUNTIF(v <= w.atr_24h), COUNT(1)) FROM UNNEST(w._arr_atr_720) AS v WHERE v >= 0)
    END                                                        AS squeeze_pctile_720h,
    CASE WHEN (SELECT COUNT(1) FROM UNNEST(w._arr_range_24) AS v WHERE v >= 0) >= 24 THEN
      (SELECT SAFE_DIVIDE(COUNTIF(v <= w.range_1h), COUNT(1)) FROM UNNEST(w._arr_range_24) AS v WHERE v >= 0)
    END                                                        AS nr_pctrank_24h,
    CASE WHEN ARRAY_LENGTH(w._arr_high_168) >= 168 THEN
      (ARRAY_LENGTH(w._arr_high_168) - 1
        - (SELECT MAX(off) FROM UNNEST(w._arr_high_168) AS v WITH OFFSET off
             WHERE v = (SELECT MAX(v2) FROM UNNEST(w._arr_high_168) AS v2 WHERE v2 >= 0))
      ) / 168.0
    END                                                        AS bars_since_true_high_168h
  FROM ohlc_win2 w
  WINDOW
    w24  AS (PARTITION BY w.token_address ORDER BY w.price_timestamp ROWS BETWEEN 23  PRECEDING AND CURRENT ROW),
    w168 AS (PARTITION BY w.token_address ORDER BY w.price_timestamp ROWS BETWEEN 167 PRECEDING AND CURRENT ROW),
    wcum AS (PARTITION BY w.token_address ORDER BY w.price_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),

-- Fourth window pass: adl slope over the cumulative flow line.
ohlc_win4 AS (
  SELECT
    w.*,
    IF(w.row_idx >= 168, SAFE_DIVIDE(
        COVAR_POP(w.adl, CAST(w.row_idx AS FLOAT64)) OVER w168,
        NULLIF(VAR_POP(CAST(w.row_idx AS FLOAT64)) OVER w168, 0.0)), NULL) AS ad_slope_168h
  FROM ohlc_win3 w
  WINDOW
    w168 AS (PARTITION BY w.token_address ORDER BY w.price_timestamp ROWS BETWEEN 167 PRECEDING AND CURRENT ROW)
),

-- Fifth window pass: ad/price divergence (z720 of ad_slope minus z720 of slope).
ohlc_win5 AS (
  SELECT
    w.*,
    IF(w.row_idx >= 720,
        SAFE_DIVIDE(w.ad_slope_168h - AVG(w.ad_slope_168h) OVER w720, NULLIF(STDDEV_SAMP(w.ad_slope_168h) OVER w720, 0.0))
      - SAFE_DIVIDE(w._trend_slope_7d_ohlc - AVG(w._trend_slope_7d_ohlc) OVER w720, NULLIF(STDDEV_SAMP(w._trend_slope_7d_ohlc) OVER w720, 0.0)),
      NULL)                                                     AS ad_price_diverge_168h
  FROM ohlc_win4 w
  WINDOW
    w720 AS (PARTITION BY w.token_address ORDER BY w.price_timestamp ROWS BETWEEN 719 PRECEDING AND CURRENT ROW)
),

-- Shipped per-token OHLC feature columns.
ohlc_token_features AS (
  SELECT
    token_address,
    price_timestamp,
    CAST(clv_1h AS FLOAT64) AS clv_1h,
    CAST(clv_mean_24h AS FLOAT64) AS clv_mean_24h,
    CAST(clv_mean_168h AS FLOAT64) AS clv_mean_168h,
    CAST(flow_imbalance_24h AS FLOAT64) AS flow_imbalance_24h,
    CAST(flow_imbalance_168h AS FLOAT64) AS flow_imbalance_168h,
    CAST(ad_slope_168h AS FLOAT64) AS ad_slope_168h,
    CAST(ad_price_diverge_168h AS FLOAT64) AS ad_price_diverge_168h,
    CAST(mfi_24h AS FLOAT64) AS mfi_24h,
    CAST(vwap_dist_24h AS FLOAT64) AS vwap_dist_24h,
    CAST(vwap_dist_168h AS FLOAT64) AS vwap_dist_168h,
    CAST(wick_asym_24h AS FLOAT64) AS wick_asym_24h,
    CAST(atr_24h AS FLOAT64) AS atr_24h,
    CAST(atr_168h AS FLOAT64) AS atr_168h,
    CAST(SAFE_DIVIDE(atr_24h, NULLIF(atr_168h, 0.0)) AS FLOAT64) AS atr_ratio_24_168,
    CAST(parkinson_rv_24h AS FLOAT64) AS parkinson_rv_24h,
    CAST(parkinson_rv_7d AS FLOAT64) AS parkinson_rv_7d,
    CAST(range_z_24h AS FLOAT64) AS range_z_24h,
    CAST(squeeze_pctile_720h AS FLOAT64) AS squeeze_pctile_720h,
    CAST(nr_pctrank_24h AS FLOAT64) AS nr_pctrank_24h,
    CAST(consec_inside_6h AS FLOAT64) AS consec_inside_6h,
    CAST(dist_to_true_high_24h AS FLOAT64) AS dist_to_true_high_24h,
    CAST(dist_to_true_high_168h AS FLOAT64) AS dist_to_true_high_168h,
    CAST(dist_to_true_high_720h AS FLOAT64) AS dist_to_true_high_720h,
    CAST(dist_to_true_low_24h AS FLOAT64) AS dist_to_true_low_24h,
    CAST(dist_to_true_low_168h AS FLOAT64) AS dist_to_true_low_168h,
    CAST(true_breakout_high_24h AS FLOAT64) AS true_breakout_high_24h,
    CAST(true_breakout_low_24h AS FLOAT64) AS true_breakout_low_24h,
    CAST(true_range_pos_168h AS FLOAT64) AS true_range_pos_168h,
    CAST(bars_since_true_high_168h AS FLOAT64) AS bars_since_true_high_168h,
    CAST(zero_range_frac_24h AS FLOAT64) AS zero_range_frac_24h,
    CAST(cs_spread_24h_bps AS FLOAT64) AS cs_spread_24h_bps,
    CAST(cs_spread_z_168h AS FLOAT64) AS cs_spread_z_168h,
    CAST(range_impact_24h AS FLOAT64) AS range_impact_24h,
    CAST(adx_24h AS FLOAT64) AS adx_24h,
    CAST(adx_168h AS FLOAT64) AS adx_168h,
    CAST(di_diff_24h AS FLOAT64) AS di_diff_24h,
    CAST(choppiness_168h AS FLOAT64) AS choppiness_168h,
    CAST(vortex_24h AS FLOAT64) AS vortex_24h,
    CAST(er_24h AS FLOAT64) AS er_24h,
    CAST(er_168h AS FLOAT64) AS er_168h,
    CAST(gk_rv_24h AS FLOAT64) AS gk_rv_24h,
    CAST(rs_rv_24h AS FLOAT64) AS rs_rv_24h,
    CAST(gap_abs_mean_24h AS FLOAT64) AS gap_abs_mean_24h,
    CAST(max_gap_168h AS FLOAT64) AS max_gap_168h,
    CAST(uw_frac_mean_24h AS FLOAT64) AS uw_frac_mean_24h,
    CAST(lw_frac_mean_24h AS FLOAT64) AS lw_frac_mean_24h,
    CAST(max_uw_frac_24h AS FLOAT64) AS max_uw_frac_24h,
    CAST(body_frac_mean_24h AS FLOAT64) AS body_frac_mean_24h,
    CAST(ar_spread_24h_bps AS FLOAT64) AS ar_spread_24h_bps
  FROM ohlc_win5
),

-- §5A/§5B cross-sectional inputs: OHLC features on the active universe only.
ohlc_active AS (
  SELECT
    otf.token_address,
    otf.price_timestamp,
    otf.clv_mean_24h,
    otf.flow_imbalance_24h,
    otf.flow_imbalance_168h,
    otf.atr_ratio_24_168,
    otf.true_breakout_high_24h,
    otf.true_breakout_low_24h,
    otf.squeeze_pctile_720h,
    otf.cs_spread_24h_bps
  FROM ohlc_token_features otf
  JOIN feature_input fi
    ON otf.token_address = fi.token_address
   AND otf.price_timestamp = fi.price_timestamp
  WHERE fi.active_universe
),

-- §5A per-timestamp universe aggregates.
ohlc_univ AS (
  SELECT
    price_timestamp,
    CAST(APPROX_QUANTILES(clv_mean_24h, 100)[OFFSET(50)] AS FLOAT64)         AS univ_med_clv_24h,
    CAST(APPROX_QUANTILES(flow_imbalance_24h, 100)[OFFSET(50)] AS FLOAT64)   AS univ_med_flow_imbalance_24h,
    CAST(AVG(true_breakout_high_24h) AS FLOAT64)                             AS univ_frac_true_breakout_high_24h,
    CAST(AVG(true_breakout_low_24h) AS FLOAT64)                              AS univ_frac_true_breakout_low_24h,
    CAST(APPROX_QUANTILES(atr_ratio_24_168, 100)[OFFSET(50)] AS FLOAT64)     AS univ_med_atr_ratio_24_168,
    CAST(AVG(IF(squeeze_pctile_720h < 0.25, 1.0, 0.0)) AS FLOAT64)           AS univ_frac_squeeze,
    CAST(APPROX_QUANTILES(cs_spread_24h_bps, 100)[OFFSET(50)] AS FLOAT64)    AS univ_med_cs_spread_24h
  FROM ohlc_active
  GROUP BY price_timestamp
),

-- §5B per-token ranks within the same-timestamp active universe.
ohlc_rel AS (
  SELECT
    token_address,
    price_timestamp,
    CAST(PERCENT_RANK() OVER (PARTITION BY price_timestamp ORDER BY flow_imbalance_168h) AS FLOAT64) AS rel_rank_flow_imbalance_168h,
    CAST(PERCENT_RANK() OVER (PARTITION BY price_timestamp ORDER BY atr_ratio_24_168)    AS FLOAT64) AS rel_rank_atr_ratio_24_168
  FROM ohlc_active
),

-- §5C BTC / SOL market-reference series (same features, single series each).
ohlc_btc_ref AS (
  SELECT
    price_timestamp,
    atr_ratio_24_168     AS btc_atr_ratio_24_168,
    parkinson_rv_24h     AS btc_parkinson_rv_24h,
    clv_mean_24h         AS btc_clv_mean_24h,
    squeeze_pctile_720h  AS btc_squeeze_pctile_720h
  FROM ohlc_token_features
  WHERE token_address = 'btcusdt'
),
ohlc_sol_ref AS (
  SELECT
    price_timestamp,
    atr_ratio_24_168     AS sol_atr_ratio_24_168,
    parkinson_rv_24h     AS sol_parkinson_rv_24h,
    clv_mean_24h         AS sol_clv_mean_24h,
    squeeze_pctile_720h  AS sol_squeeze_pctile_720h
  FROM ohlc_token_features
  WHERE token_address = 'So11111111111111111111111111111111111111112'
),

final_features AS (
  SELECT
    tf.*,
    ur.univ_n_active,
    ur.univ_med_logret_24h,
    ur.univ_mean_logret_24h,
    ur.univ_disp_logret_24h,
    ur.univ_pct_pos_24h,
    ur.univ_pct_above_sma_24h,
    ur.univ_pct_above_sma_168h,
    ur.univ_pct_in_dd_gt_10pct,
    ur.univ_med_logret_168h,
    ur.univ_med_rv_24h,
    ur.univ_med_rv_24h_z_30d,
    ur.btc_rv_24h_pctile_90d,
    rr.rel_rank_cumret_24h,
    rr.rel_rank_cumret_7d,
    CAST(tf.cumret_24h - ur.univ_med_logret_24h AS FLOAT64) AS rel_excess_cumret_24h,
    CAST(tf.cumret_7d - ur.univ_med_logret_168h AS FLOAT64) AS rel_excess_cumret_7d,
    CAST(tf.cumret_24h - tf.btc_cumret_24h AS FLOAT64) AS rel_excess_vs_btc_24h,
    CAST(tf.cumret_7d - tf.btc_cumret_7d AS FLOAT64) AS rel_excess_vs_btc_7d,
    rr.rel_rank_dollar_vol_24h
  FROM trend_features tf
  LEFT JOIN univ_regime ur
    ON tf.price_timestamp = ur.price_timestamp
  LEFT JOIN relative_ranks rr
    ON tf.token_address = rr.token_address
   AND tf.price_timestamp = rr.price_timestamp
)

SELECT
  ff.*,
  otf.* EXCEPT (token_address, price_timestamp),
  -- §4B rv_eff_ratio uses the existing per-token rv_24h as denominator
  CAST(SAFE_DIVIDE(otf.parkinson_rv_24h, NULLIF(ff.rv_24h, 0.0)) AS FLOAT64) AS rv_eff_ratio_24h,
  -- §5A universe aggregates
  ou.univ_med_clv_24h,
  ou.univ_med_flow_imbalance_24h,
  ou.univ_frac_true_breakout_high_24h,
  ou.univ_frac_true_breakout_low_24h,
  ou.univ_med_atr_ratio_24_168,
  ou.univ_frac_squeeze,
  ou.univ_med_cs_spread_24h,
  -- §5B relative ranks
  orl.rel_rank_flow_imbalance_168h,
  orl.rel_rank_atr_ratio_24_168,
  -- §5C BTC / SOL market references (rv_eff uses existing btc_rv_24h / sol_rv_24h)
  ob.btc_atr_ratio_24_168,
  ob.btc_parkinson_rv_24h,
  ob.btc_clv_mean_24h,
  ob.btc_squeeze_pctile_720h,
  CAST(SAFE_DIVIDE(ob.btc_parkinson_rv_24h, NULLIF(ff.btc_rv_24h, 0.0)) AS FLOAT64) AS btc_rv_eff_ratio_24h,
  os.sol_atr_ratio_24_168,
  os.sol_parkinson_rv_24h,
  os.sol_clv_mean_24h,
  os.sol_squeeze_pctile_720h,
  CAST(SAFE_DIVIDE(os.sol_parkinson_rv_24h, NULLIF(ff.sol_rv_24h, 0.0)) AS FLOAT64) AS sol_rv_eff_ratio_24h
FROM final_features ff
LEFT JOIN ohlc_token_features otf
  ON ff.token_address = otf.token_address
 AND ff.price_timestamp = otf.price_timestamp
LEFT JOIN ohlc_univ ou
  ON ff.price_timestamp = ou.price_timestamp
LEFT JOIN ohlc_rel orl
  ON ff.token_address = orl.token_address
 AND ff.price_timestamp = orl.price_timestamp
LEFT JOIN ohlc_btc_ref ob
  ON ff.price_timestamp = ob.price_timestamp
LEFT JOIN ohlc_sol_ref os
  ON ff.price_timestamp = os.price_timestamp
