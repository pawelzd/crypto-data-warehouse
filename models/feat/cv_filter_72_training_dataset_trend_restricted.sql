WITH past AS (
  SELECT
    token_address,
    monitoring_session_id,
    ts_hour AS decision_ts,
    in_core_monitoring,
    has_168h AS has_full_lookback,

    -- core returns/vol
    price, ret_1h, logret_1h,
    rv_4h, rv_12h, rv_24h, rv_7d,
    sharpe_24h, sharpe_7d,
    ret_z_24h, cumret_24h, cumret_7d,

    -- price structure & distances
    sma_6h, sma_12h, sma_24h, sma_48h, sma_72h, sma_168h,
    dist_to_sma_6h, dist_to_sma_12h, dist_to_sma_24h, dist_to_sma_72h, dist_to_sma_168h,
    dist_to_high_4h, dist_to_low_4h,
    dist_to_high_12h, dist_to_low_12h,
    dist_to_high_24h, dist_to_low_24h,
    breakout_high_24h, breakout_low_24h,
    price_z_24h,
    pct_in_range_12h, pct_in_range_24h, pct_in_range_72h,
    donchian_bw_12h, donchian_bw_24h, donchian_bw_72h,
    bb_bw_24h,

    -- OLS trend quality
    slope_ols_8h,  r2_ols_8h,
    slope_ols_12h, r2_ols_12h,
    slope_ols_24h, r2_ols_24h,

    -- volatility ratios
    vol_ratio_4_12, vol_ratio_12_24, vol_ratio_12_72,
    vol_ratio_24_72, vol_ratio_24_7d, vol_ratio_72_168,

    -- momentum slopes / interactions
    sma_diff_fast_slow, sma_diff_12_48,
    sma6h_slope_12h, sma6h_slope_24h,
    sma12h_slope_12h, sma12h_slope_24h, sma12h_slope_72h,
    sma24h_slope_24h,
    rsi_14, rsi_vol_interaction,
    sharpe_delta,

    -- volume features (keep derived, not raw totals)
    volume_cv_24h, volume_cv_168h,
    volume_spike_ratio_24h_excl,
    volume_z_24h,
    volume_accel_6v24, volume_accel_24v168,
    volume_ema_fast, volume_ema_slow,
    price_vol_interaction_24h,

    -- regime / persistence
    drawdown_7d,
    acf1_72h,
    up_count_12h, down_count_12h, up_count_24h, down_count_24h,
    chop_24h,

    -- cyclic features (keep only sin/cos)
    sin_hour, cos_hour, sin_dow, cos_dow,

    -- return stats
    mean_ret_24h, std_ret_24h, mean_ret_72h, std_ret_72h, mean_ret_168h, std_ret_168h,

    -- housekeeping (only the derived ones)
    volume_ret_1h_norm, volume_ret_24h_norm,
    log_volume_per_supply_z
  FROM {{ ref('cv_filter_72_7d_before_trend') }} c
  LEFT JOIN {{ source('core', 'token_metadata_jup_tmp') }} t
    ON c.token_address = t.id
  WHERE t.mcap >= 100000000
),


-- Deduplicate the future table on the join keys
future_min AS (
  SELECT *
  FROM (
    SELECT
      token_address,
      monitoring_session_id,
      first_acquired_timestamp,
      has_full_entry,
      has_full_manage,
      label_entry_k8,
      label_manage_k2,
      ROW_NUMBER() OVER (
        PARTITION BY token_address, monitoring_session_id, first_acquired_timestamp
        ORDER BY first_acquired_timestamp
      ) AS rn
    FROM {{ ref('cv_filter_72_3d_after_trend') }}
  )
  WHERE rn = 1
),

joined AS (
  SELECT
    p.token_address,
    p.monitoring_session_id,
    p.decision_ts,
    p.in_core_monitoring,
    p.has_full_lookback,
    f.has_full_entry,
    f.has_full_manage,
    label_entry_k8,
    label_manage_k2,

    -- everything final needs:
    -- core returns/vol
    p.price, p.ret_1h, p.logret_1h,
    p.rv_4h, p.rv_12h, p.rv_24h, p.rv_7d,
    p.sharpe_24h, p.sharpe_7d,
    p.ret_z_24h, p.cumret_24h, p.cumret_7d,

    -- structure & distances
    p.sma_6h, p.sma_12h, p.sma_24h, p.sma_48h, p.sma_72h, p.sma_168h,
    p.dist_to_sma_6h, p.dist_to_sma_12h, p.dist_to_sma_24h, p.dist_to_sma_72h, p.dist_to_sma_168h,
    p.dist_to_high_4h, p.dist_to_low_4h,
    p.dist_to_high_12h, p.dist_to_low_12h,
    p.dist_to_high_24h, p.dist_to_low_24h,
    p.breakout_high_24h, p.breakout_low_24h,
    p.price_z_24h,
    p.pct_in_range_12h, p.pct_in_range_24h, p.pct_in_range_72h,
    p.donchian_bw_12h, p.donchian_bw_24h, p.donchian_bw_72h,
    p.bb_bw_24h,

    -- OLS trend quality
    p.slope_ols_8h,  p.r2_ols_8h,
    p.slope_ols_12h, p.r2_ols_12h,
    p.slope_ols_24h, p.r2_ols_24h,

    -- volatility ratios
    p.vol_ratio_4_12, p.vol_ratio_12_24, p.vol_ratio_12_72,
    p.vol_ratio_24_72, p.vol_ratio_24_7d, p.vol_ratio_72_168,

    -- momentum / interactions
    p.sma_diff_fast_slow, p.sma_diff_12_48,
    p.sma6h_slope_12h, p.sma6h_slope_24h,
    p.sma12h_slope_12h, p.sma12h_slope_24h, p.sma12h_slope_72h,
    p.sma24h_slope_24h,
    p.rsi_14, p.rsi_vol_interaction,
    p.sharpe_delta,

    -- volume (derived)
    p.volume_cv_24h, p.volume_cv_168h,
    p.volume_spike_ratio_24h_excl,
    p.volume_z_24h,
    p.volume_accel_6v24, p.volume_accel_24v168,
    p.volume_ema_fast, p.volume_ema_slow,
    p.price_vol_interaction_24h,
    p.volume_ret_1h_norm, p.volume_ret_24h_norm,

    -- regime / persistence
    p.drawdown_7d,
    p.acf1_72h,
    p.up_count_12h, p.down_count_12h, p.up_count_24h, p.down_count_24h,
    p.chop_24h,

    -- cyclic
    p.sin_hour, p.cos_hour, p.sin_dow, p.cos_dow,

    -- return stats
    p.mean_ret_24h, p.std_ret_24h, p.mean_ret_72h, p.std_ret_72h, p.mean_ret_168h, p.std_ret_168h,

    -- housekeeping
    p.log_volume_per_supply_z

  FROM past p
  JOIN future_min f
    ON p.token_address = f.token_address
   AND p.monitoring_session_id = f.monitoring_session_id
   AND p.decision_ts = f.first_acquired_timestamp
),


final AS (
  SELECT
    token_address,
    monitoring_session_id,
    decision_ts,

    CASE WHEN label_entry_k8 = 1 THEN 1 ELSE 0 END AS label_uptrend_8h,
    CASE WHEN label_manage_k2 = 1 OR label_manage_k2 = 0 THEN 0 ELSE 1 END AS label_downtrend_2h,

    in_core_monitoring,
    has_full_lookback,
    has_full_entry,
    has_full_manage,

    -- core returns/vol
    price, ret_1h, logret_1h,
    rv_4h, rv_12h, rv_24h, rv_7d,
    sharpe_24h, sharpe_7d,
    ret_z_24h, cumret_24h, cumret_7d,

    -- price structure & distances
    sma_6h, sma_12h, sma_24h, sma_48h, sma_72h, sma_168h,
    dist_to_sma_6h, dist_to_sma_12h, dist_to_sma_24h, dist_to_sma_72h, dist_to_sma_168h,
    dist_to_high_4h, dist_to_low_4h,
    dist_to_high_12h, dist_to_low_12h,
    dist_to_high_24h, dist_to_low_24h,
    breakout_high_24h, breakout_low_24h,
    price_z_24h,
    pct_in_range_12h, pct_in_range_24h, pct_in_range_72h,
    donchian_bw_12h, donchian_bw_24h, donchian_bw_72h,
    bb_bw_24h,

    -- OLS trend quality
    slope_ols_8h,  r2_ols_8h,
    slope_ols_12h, r2_ols_12h,
    slope_ols_24h, r2_ols_24h,

    -- volatility ratios
    vol_ratio_4_12, vol_ratio_12_24, vol_ratio_12_72, vol_ratio_24_72, vol_ratio_24_7d, vol_ratio_72_168,

    -- momentum slopes / interactions
    sma_diff_fast_slow, sma_diff_12_48,
    sma6h_slope_12h, sma6h_slope_24h,
    sma12h_slope_12h, sma12h_slope_24h, sma12h_slope_72h,
    sma24h_slope_24h,
    rsi_14, rsi_vol_interaction,
    sharpe_delta,

    -- volume features (derived only)
    volume_cv_24h, volume_cv_168h,
    volume_spike_ratio_24h_excl,
    volume_z_24h,
    volume_accel_6v24, volume_accel_24v168,
    volume_ema_fast, volume_ema_slow,
    price_vol_interaction_24h,

    -- regime / persistence
    drawdown_7d,
    acf1_72h,
    up_count_12h, down_count_12h, up_count_24h, down_count_24h,
    chop_24h,

    -- cyclic (sin/cos only)
    sin_hour, cos_hour, sin_dow, cos_dow,

    -- return stats
    mean_ret_24h, std_ret_24h, mean_ret_72h, std_ret_72h, mean_ret_168h, std_ret_168h,

    -- housekeeping
    volume_ret_1h_norm, volume_ret_24h_norm,
    log_volume_per_supply_z
  FROM joined
  WHERE has_full_lookback = 1
    AND has_full_entry = 1
    AND has_full_manage = 1
    AND in_core_monitoring = TRUE
),
-- Pre-filter BTC and SOL once, and dedupe to one row per ts_hour
btc_mt AS (
  SELECT *
  FROM (
    SELECT m.*,
           ROW_NUMBER() OVER (PARTITION BY ts_hour ORDER BY ts_hour) AS rn
    FROM {{ ref('cv_btc_sol_1h') }} m
    WHERE m.token_address = 'btcusdt'
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
    bt.* EXCEPT (  rv_4h, rv_12h,
  dist_to_high_4h, dist_to_low_4h,
  dist_to_high_12h, dist_to_low_12h,
  sma_diff_12_48,
  sma6h_slope_12h, sma12h_slope_12h, sma12h_slope_72h, sma24h_slope_24h,
  price,
  mean_ret_24h, std_ret_24h, mean_ret_72h, std_ret_72h, mean_ret_168h, std_ret_168h,

  -- BTC fields to drop (leave SOL as-is)
  btc_cumret_24h, btc_cumret_7d,
  btc_macd_sma_12_26h,
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
