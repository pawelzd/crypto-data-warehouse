{{ config(
    materialized = 'view'
) }}

WITH past AS (
  SELECT
    token_address,
    ts_hour AS decision_ts,
    has_168h AS has_full_lookback,
    price, ret_1h, logret_1h,
    mean_ret_24h, std_ret_24h, mean_ret_72h, std_ret_72h, mean_ret_168h, std_ret_168h,
    rv_24h, rv_4h, rv_12h, rv_7d,
    sharpe_24h, sharpe_7d, ret_z_24h, cumret_24h, cumret_7d,
    sma_6h, sma_12h, sma_24h, sma_48h, sma_72h, sma_168h,
     sma6h_slope_24h, sma12h_slope_24h,
    macd_sma_12_26h, price_z_24h, pct_in_range_24h,
    dist_to_sma_6h, dist_to_sma_12h, dist_to_sma_24h, dist_to_sma_72h, dist_to_sma_168h,
    dist_to_high_24h, dist_to_low_24h, dist_to_high_4h, dist_to_low_4h, dist_to_high_12h, dist_to_low_12h,
    breakout_high_24h, breakout_low_24h,
    drawdown_7d, drawdown_48h, drawdown_24h, has_168h,
    rsi_14, rsi_vol_interaction,
    dow_1_sun_7_sat, hour_of_day, sin_hour, cos_hour, sin_dow, cos_dow,
    vol_ratio_24_7d, vol_ratio_24_72, vol_ratio_72_168, vol_ratio_4_24, vol_ratio_12_24, ret_over_rv_12h,
    sma_diff_12_48, sma_diff_fast_slow,
    sma6h_slope_12h, sma12h_slope_12h, sma12h_slope_72h, sma24h_slope_24h, sma48h_slope_24h,
    volume_ret_1h, volume_ret_24h,
    log_volume, log_volume_per_supply,
    log_volume_mean_24h, log_volume_mean_168h,
    log_volume_mean_24h_per_supply, log_volume_mean_168h_per_supply,
    volume_cv_24h, volume_cv_168h, volume_spike_ratio_24h_excl, volume_z_24h,
    volume_accel_6v24, volume_accel_24v168,
    volume_sum_6h, volume_sum_24h, volume_sum_168h,
    volume_mean_24h, volume_mean_168h,
    volume_std_24h, volume_std_168h, volume_n_24h,
    volume_ema_fast, volume_ema_slow,
    sharpe_delta
  FROM {{ ref('20m_cv_prod_72_7d_before') }}
  
),

final AS (
  SELECT
    token_address,
    decision_ts,
    has_full_lookback,

    -- keep all the explicit features from joined (everything after flags above)
    {{- "\n    " -}}
    price, ret_1h, logret_1h,
     sma6h_slope_24h, sma12h_slope_24h,
    mean_ret_24h, std_ret_24h, mean_ret_72h, std_ret_72h, mean_ret_168h, std_ret_168h,
    rv_24h, rv_4h, rv_12h, rv_7d,
    sharpe_24h, sharpe_7d, ret_z_24h, cumret_24h, cumret_7d,
    sma_6h, sma_12h, sma_24h, sma_48h, sma_72h, sma_168h,
    macd_sma_12_26h, price_z_24h, pct_in_range_24h,
    dist_to_sma_6h, dist_to_sma_12h, dist_to_sma_24h, dist_to_sma_72h, dist_to_sma_168h,
    dist_to_high_24h, dist_to_low_24h, dist_to_high_4h, dist_to_low_4h, dist_to_high_12h, dist_to_low_12h,
    breakout_high_24h, breakout_low_24h,
    drawdown_7d, drawdown_48h, drawdown_24h, rsi_14, rsi_vol_interaction,
    dow_1_sun_7_sat, hour_of_day, sin_hour, cos_hour, sin_dow, cos_dow,
    vol_ratio_24_7d, vol_ratio_24_72, vol_ratio_72_168, vol_ratio_4_24, vol_ratio_12_24, ret_over_rv_12h,
    sma_diff_12_48, sma_diff_fast_slow,
    sma6h_slope_12h, sma12h_slope_12h, sma12h_slope_72h, sma24h_slope_24h, sma48h_slope_24h,
    volume_ret_1h, volume_ret_24h,
    log_volume, log_volume_per_supply,
    log_volume_mean_24h, log_volume_mean_168h,
    log_volume_mean_24h_per_supply, log_volume_mean_168h_per_supply,
    volume_cv_24h, volume_cv_168h, volume_spike_ratio_24h_excl, volume_z_24h,
    volume_accel_6v24, volume_accel_24v168,
    volume_sum_6h, volume_sum_24h, volume_sum_168h,
    volume_mean_24h, volume_mean_168h,
    volume_std_24h, volume_std_168h, volume_n_24h,
    volume_ema_fast, volume_ema_slow,
    sharpe_delta
  FROM past
  WHERE has_full_lookback = 1
  ),

-- Pre-filter BTC and SOL once, and dedupe to one row per ts_hour
btc_mt AS (
  SELECT *
  FROM (
    SELECT m.*,
           ROW_NUMBER() OVER (PARTITION BY ts_hour ORDER BY ts_hour) AS rn
    FROM {{ ref('cv_btc_sol_1h') }} m
    WHERE m.token_address = '3NZ9JMVBmGAqocybic2c7LQCJScmgsAZ6vQqTDzcqmJh'
  )
  WHERE rn = 1
),
sol_mt AS (
  SELECT *
  FROM (
    SELECT m.*,
           ROW_NUMBER() OVER (PARTITION BY ts_hour ORDER BY ts_hour) AS rn
    FROM {{ ref('cv_btc_sol_1h') }} m
    WHERE m.token_address = 'So11111111111111111111111111111111111111112'
  )
  WHERE rn = 1
),

btc_join AS (
  SELECT
    dt.*,
    COALESCE(b.ret_1h, 0) AS btc_ret_1h,
    COALESCE(b.logret_1h, 0) AS btc_logret_1h,
    COALESCE(b.mean_ret_24h, 0) AS btc_mean_ret_24h,
    COALESCE(b.std_ret_24h, 0)  AS btc_std_ret_24h,
    COALESCE(b.mean_ret_72h, 0) AS btc_mean_ret_72h,
    COALESCE(b.std_ret_72h, 0)  AS btc_std_ret_72h,
    COALESCE(b.mean_ret_168h, 0) AS btc_mean_ret_168h,
    COALESCE(b.std_ret_168h, 0)  AS btc_std_ret_168h,
    COALESCE(b.rv_24h, 0) AS btc_rv_24h,
    COALESCE(b.rv_7d, 0) AS btc_rv_7d,
    COALESCE(b.sharpe_24h, 0) AS btc_sharpe_24h,
    COALESCE(b.sharpe_7d, 0) AS btc_sharpe_7d,
    COALESCE(b.ret_z_24h, 0) AS btc_ret_z_24h,
    COALESCE(b.cumret_24h, 0) AS btc_cumret_24h,
    COALESCE(b.cumret_7d, 0) AS btc_cumret_7d,
    COALESCE(b.macd_sma_12_26h, 0) AS btc_macd_sma_12_26h,
    COALESCE(b.dist_to_sma_6h, 0) AS btc_dist_to_sma_6h,
    COALESCE(b.dist_to_sma_12h, 0) AS btc_dist_to_sma_12h,
    COALESCE(b.dist_to_sma_24h, 0) AS btc_dist_to_sma_24h,
    COALESCE(b.dist_to_sma_72h, 0) AS btc_dist_to_sma_72h,
    COALESCE(b.dist_to_sma_168h, 0) AS btc_dist_to_sma_168h,
    COALESCE(b.pct_in_range_24h, 0) AS btc_pct_in_range_24h,
    COALESCE(b.dist_to_high_24h, 0) AS btc_dist_to_high_24h,
    COALESCE(b.dist_to_low_24h, 0) AS btc_dist_to_low_24h,
    COALESCE(b.breakout_high_24h, 0) AS btc_breakout_high_24h,
    COALESCE(b.breakout_low_24h, 0) AS btc_breakout_low_24h,
    COALESCE(b.drawdown_7d, 0) AS btc_drawdown_7d,
    COALESCE(b.rsi_14, 0) AS btc_rsi_14,
    COALESCE(b.acf1_72h, 0) AS btc_acf1_72h,
    COALESCE(b.sin_hour, 0) AS btc_sin_hour,
    COALESCE(b.cos_hour, 0) AS btc_cos_hour,
    COALESCE(b.sin_dow, 0) AS btc_sin_dow,
    COALESCE(b.cos_dow, 0) AS btc_cos_dow,
    COALESCE(b.vol_ratio_24_7d, 0) AS btc_vol_ratio_24_7d,
    COALESCE(b.sharpe_delta, 0) AS btc_sharpe_delta,
    COALESCE(dt.ret_1h - b.ret_1h, 0) AS spread_ret_1h,
    COALESCE(dt.logret_1h - b.logret_1h, 0) AS spread_logret_1h
  FROM final dt
  LEFT JOIN btc_mt b
    ON dt.decision_ts = b.ts_hour      -- NOTE: token filter is already pushed into btc_mt
),

final_with_sol AS (
  SELECT
    bt.* EXCEPT (rv_4h,
    rv_12h,
    dist_to_high_4h,
    dist_to_low_4h,
    dist_to_high_12h,
    dist_to_low_12h,
    drawdown_48h,
    drawdown_24h,
    vol_ratio_4_24,
    vol_ratio_12_24,
    sma_diff_12_48,
    sma6h_slope_12h,
    sma12h_slope_12h,
    sma12h_slope_72h,
    sma24h_slope_24h,
    sma48h_slope_24h,
    btc_cumret_24h,
    btc_cumret_7d,
    btc_dist_to_sma_72h),
    COALESCE(s.ret_1h, 0) AS sol_ret_1h,
    COALESCE(s.logret_1h, 0) AS sol_logret_1h,
    COALESCE(s.mean_ret_24h, 0) AS sol_mean_ret_24h,
    COALESCE(s.std_ret_24h, 0)  AS sol_std_ret_24h,
    COALESCE(s.mean_ret_72h, 0) AS sol_mean_ret_72h,
    COALESCE(s.std_ret_72h, 0)  AS sol_std_ret_72h,
    COALESCE(s.mean_ret_168h, 0) AS sol_mean_ret_168h,
    COALESCE(s.std_ret_168h, 0)  AS sol_std_ret_168h,
    COALESCE(s.rv_24h, 0) AS sol_rv_24h,
    COALESCE(s.rv_7d, 0) AS sol_rv_7d,
    COALESCE(s.sharpe_24h, 0) AS sol_sharpe_24h,
    COALESCE(s.sharpe_7d, 0) AS sol_sharpe_7d,
    COALESCE(s.ret_z_24h, 0) AS sol_ret_z_24h,
    COALESCE(s.cumret_24h, 0) AS sol_cumret_24h,
    COALESCE(s.cumret_7d, 0) AS sol_cumret_7d,
    COALESCE(s.macd_sma_12_26h, 0) AS sol_macd_sma_12_26h,
    COALESCE(s.dist_to_sma_6h, 0) AS sol_dist_to_sma_6h,
    COALESCE(s.dist_to_sma_12h, 0) AS sol_dist_to_sma_12h,
    COALESCE(s.dist_to_sma_24h, 0) AS sol_dist_to_sma_24h,
    COALESCE(s.dist_to_sma_72h, 0) AS sol_dist_to_sma_72h,
    COALESCE(s.dist_to_sma_168h, 0) AS sol_dist_to_sma_168h,
    COALESCE(s.pct_in_range_24h, 0) AS sol_pct_in_range_24h,
    COALESCE(s.dist_to_high_24h, 0) AS sol_dist_to_high_24h,
    COALESCE(s.dist_to_low_24h, 0) AS sol_dist_to_low_24h,
    COALESCE(s.breakout_high_24h, 0) AS sol_breakout_high_24h,
    COALESCE(s.breakout_low_24h, 0) AS sol_breakout_low_24h,
    COALESCE(s.drawdown_7d, 0) AS sol_drawdown_7d,
    COALESCE(s.rsi_14, 0) AS sol_rsi_14,
    COALESCE(s.acf1_72h, 0) AS sol_acf1_72h,
    COALESCE(s.sin_hour, 0) AS sol_sin_hour,
    COALESCE(s.cos_hour, 0) AS sol_cos_hour,
    COALESCE(s.sin_dow, 0) AS sol_sin_dow,
    COALESCE(s.cos_dow, 0) AS sol_cos_dow,
    COALESCE(s.vol_ratio_24_7d, 0) AS sol_vol_ratio_24_7d,
    COALESCE(s.sharpe_delta, 0) AS sol_sharpe_delta
  FROM btc_join bt
  LEFT JOIN sol_mt s
    ON bt.decision_ts = s.ts_hour
)

SELECT *
FROM final_with_sol
ORDER BY token_address, decision_ts
