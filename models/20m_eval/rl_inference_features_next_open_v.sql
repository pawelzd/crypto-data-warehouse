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

-- Per-token OHLC candle features (spec 2026-07-03), materialized separately
-- in rl_ohlc_candle_features so this view stays within BigQuery's
-- query-planning complexity limit. See that model for the full derivation.
ohlc_token_features AS (
  SELECT * FROM {{ ref('rl_ohlc_candle_features') }}
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

-- §5C / BTC-OHLCV spec (2026-07-05): BTC and SOL market-regime features, one
-- row per price_timestamp, broadcast to every altcoin row by the join below.
-- These are the single-series OHLCV features from rl_ohlc_candle_features
-- (a single-token PARTITION BY token_address window IS the single series).
-- Degenerate-for-a-liquid-series columns are intentionally not surfaced:
-- zero_range_frac_24h (always 0), range_impact_24h (no liquidity signal on a
-- reference series), and the noisy per-bar clv_1h (ship smoothed clv_mean_*).
ohlc_btc_ref AS (
  SELECT
    price_timestamp,
    -- already shipped (do not rebuild)
    atr_ratio_24_168           AS btc_atr_ratio_24_168,
    parkinson_rv_24h           AS btc_parkinson_rv_24h,
    clv_mean_24h               AS btc_clv_mean_24h,
    squeeze_pctile_720h        AS btc_squeeze_pctile_720h,
    -- flow / accumulation
    flow_imbalance_24h         AS btc_flow_imbalance_24h,
    flow_imbalance_168h        AS btc_flow_imbalance_168h,
    ad_slope_168h              AS btc_ad_slope_168h,
    ad_price_diverge_168h      AS btc_ad_price_diverge_168h,
    mfi_24h                    AS btc_mfi_24h,
    vwap_dist_24h              AS btc_vwap_dist_24h,
    vwap_dist_168h             AS btc_vwap_dist_168h,
    wick_asym_24h              AS btc_wick_asym_24h,
    -- volatility / compression regime
    atr_24h                    AS btc_atr_24h,
    atr_168h                   AS btc_atr_168h,
    range_z_24h                AS btc_range_z_24h,
    nr_pctrank_24h             AS btc_nr_pctrank_24h,
    -- spread / stress regime
    cs_spread_24h_bps          AS btc_cs_spread_24h_bps,
    cs_spread_z_168h           AS btc_cs_spread_z_168h,
    -- trend quality
    adx_24h                    AS btc_adx_24h,
    adx_168h                   AS btc_adx_168h,
    di_diff_24h                AS btc_di_diff_24h,
    choppiness_168h            AS btc_choppiness_168h,
    vortex_24h                 AS btc_vortex_24h,
    er_24h                     AS btc_er_24h,
    er_168h                    AS btc_er_168h,
    -- true extremes / path
    dist_to_true_high_24h      AS btc_dist_to_true_high_24h,
    dist_to_true_high_168h     AS btc_dist_to_true_high_168h,
    dist_to_true_high_720h     AS btc_dist_to_true_high_720h,
    dist_to_true_low_24h       AS btc_dist_to_true_low_24h,
    dist_to_true_low_168h      AS btc_dist_to_true_low_168h,
    true_breakout_high_24h     AS btc_true_breakout_high_24h,
    true_breakout_low_24h      AS btc_true_breakout_low_24h,
    true_range_pos_168h        AS btc_true_range_pos_168h,
    bars_since_true_high_168h  AS btc_bars_since_true_high_168h,
    -- extra drift-robust vol estimators
    gk_rv_24h                  AS btc_gk_rv_24h,
    rs_rv_24h                  AS btc_rs_rv_24h
  FROM ohlc_token_features
  WHERE token_address = 'btcusdt'
),
ohlc_sol_ref AS (
  SELECT
    price_timestamp,
    atr_ratio_24_168           AS sol_atr_ratio_24_168,
    parkinson_rv_24h           AS sol_parkinson_rv_24h,
    clv_mean_24h               AS sol_clv_mean_24h,
    squeeze_pctile_720h        AS sol_squeeze_pctile_720h,
    flow_imbalance_24h         AS sol_flow_imbalance_24h,
    flow_imbalance_168h        AS sol_flow_imbalance_168h,
    ad_slope_168h              AS sol_ad_slope_168h,
    ad_price_diverge_168h      AS sol_ad_price_diverge_168h,
    mfi_24h                    AS sol_mfi_24h,
    vwap_dist_24h              AS sol_vwap_dist_24h,
    vwap_dist_168h             AS sol_vwap_dist_168h,
    wick_asym_24h              AS sol_wick_asym_24h,
    atr_24h                    AS sol_atr_24h,
    atr_168h                   AS sol_atr_168h,
    range_z_24h                AS sol_range_z_24h,
    nr_pctrank_24h             AS sol_nr_pctrank_24h,
    cs_spread_24h_bps          AS sol_cs_spread_24h_bps,
    cs_spread_z_168h           AS sol_cs_spread_z_168h,
    adx_24h                    AS sol_adx_24h,
    adx_168h                   AS sol_adx_168h,
    di_diff_24h                AS sol_di_diff_24h,
    choppiness_168h            AS sol_choppiness_168h,
    vortex_24h                 AS sol_vortex_24h,
    er_24h                     AS sol_er_24h,
    er_168h                    AS sol_er_168h,
    dist_to_true_high_24h      AS sol_dist_to_true_high_24h,
    dist_to_true_high_168h     AS sol_dist_to_true_high_168h,
    dist_to_true_high_720h     AS sol_dist_to_true_high_720h,
    dist_to_true_low_24h       AS sol_dist_to_true_low_24h,
    dist_to_true_low_168h      AS sol_dist_to_true_low_168h,
    true_breakout_high_24h     AS sol_true_breakout_high_24h,
    true_breakout_low_24h      AS sol_true_breakout_low_24h,
    true_range_pos_168h        AS sol_true_range_pos_168h,
    bars_since_true_high_168h  AS sol_bars_since_true_high_168h,
    gk_rv_24h                  AS sol_gk_rv_24h,
    rs_rv_24h                  AS sol_rs_rv_24h
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
  -- §5C / BTC-OHLCV (2026-07-05): BTC & SOL market-regime features (broadcast
  -- by price_timestamp); rv_eff uses the existing close-derived btc_rv_24h /
  -- sol_rv_24h as denominator.
  ob.* EXCEPT (price_timestamp),
  CAST(SAFE_DIVIDE(ob.btc_parkinson_rv_24h, NULLIF(ff.btc_rv_24h, 0.0)) AS FLOAT64) AS btc_rv_eff_ratio_24h,
  os.* EXCEPT (price_timestamp),
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
