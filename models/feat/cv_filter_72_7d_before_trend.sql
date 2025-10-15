WITH indexed AS (
  -- index + logp to compute OLS slope/R2 on moving windows
  SELECT
    f.*,
    ROW_NUMBER() OVER (PARTITION BY f.token_address ORDER BY f.ts_hour) AS idx,
    SAFE.LOG(GREATEST(f.price, 1e-12)) AS logp
  FROM {{ref('cv_filter_72_7d_before')}} f
)
, ols_feats AS (
  SELECT
    *,
    -- helper for generic OLS on window length L:
    -- slope = cov(x,y)/var(x); R2 = cov^2/(varx*vary)
    -- L = 8h
    (
      (SUM(idx*logp) OVER w8 - (SUM(idx) OVER w8)*(SUM(logp) OVER w8)/8.0) /
      NULLIF(SUM(idx*idx) OVER w8 - (SUM(idx) OVER w8)*(SUM(idx) OVER w8)/8.0, 0)
    ) AS slope_ols_8h,
    POW(
      (SUM(idx*logp) OVER w8 - (SUM(idx) OVER w8)*(SUM(logp) OVER w8)/8.0), 2
    ) / NULLIF(
      (SUM(idx*idx) OVER w8 - (SUM(idx) OVER w8)*(SUM(idx) OVER w8)/8.0) *
      (SUM(logp*logp) OVER w8 - (SUM(logp) OVER w8)*(SUM(logp) OVER w8)/8.0), 0
    ) AS r2_ols_8h,

    -- L = 12h
    (
      (SUM(idx*logp) OVER w12 - (SUM(idx) OVER w12)*(SUM(logp) OVER w12)/12.0) /
      NULLIF(SUM(idx*idx) OVER w12 - (SUM(idx) OVER w12)*(SUM(idx) OVER w12)/12.0, 0)
    ) AS slope_ols_12h,
    POW(
      (SUM(idx*logp) OVER w12 - (SUM(idx) OVER w12)*(SUM(logp) OVER w12)/12.0), 2
    ) / NULLIF(
      (SUM(idx*idx) OVER w12 - (SUM(idx) OVER w12)*(SUM(idx) OVER w12)/12.0) *
      (SUM(logp*logp) OVER w12 - (SUM(logp) OVER w12)*(SUM(logp) OVER w12)/12.0), 0
    ) AS r2_ols_12h,

    -- L = 24h
    (
      (SUM(idx*logp) OVER w24 - (SUM(idx) OVER w24)*(SUM(logp) OVER w24)/24.0) /
      NULLIF(SUM(idx*idx) OVER w24 - (SUM(idx) OVER w24)*(SUM(idx) OVER w24)/24.0, 0)
    ) AS slope_ols_24h,
    POW(
      (SUM(idx*logp) OVER w24 - (SUM(idx) OVER w24)*(SUM(logp) OVER w24)/24.0), 2
    ) / NULLIF(
      (SUM(idx*idx) OVER w24 - (SUM(idx) OVER w24)*(SUM(idx) OVER w24)/24.0) *
      (SUM(logp*logp) OVER w24 - (SUM(logp) OVER w24)*(SUM(logp) OVER w24)/24.0), 0
    ) AS r2_ols_24h
  FROM indexed
  WINDOW
    w8  AS (PARTITION BY token_address ORDER BY ts_hour ROWS BETWEEN 7  PRECEDING AND CURRENT ROW),
    w12 AS (PARTITION BY token_address ORDER BY ts_hour ROWS BETWEEN 11 PRECEDING AND CURRENT ROW),
    w24 AS (PARTITION BY token_address ORDER BY ts_hour ROWS BETWEEN 23 PRECEDING AND CURRENT ROW)
)
, structure_feats AS (
  SELECT
    s.*,

    -- Donchian bandwidths (range / SMA) on multiple windows
    (MAX(price) OVER w12 - MIN(price) OVER w12) / NULLIF(sma_12h, 0) AS donchian_bw_12h,
    (MAX(price) OVER w24 - MIN(price) OVER w24) / NULLIF(sma_24h, 0) AS donchian_bw_24h,
    (MAX(price) OVER w72 - MIN(price) OVER w72) / NULLIF(sma_72h, 0) AS donchian_bw_72h,

    -- Extra percent-in-range windows
    SAFE_DIVIDE(price - MIN(price) OVER w12, NULLIF(MAX(price) OVER w12 - MIN(price) OVER w12, 0)) AS pct_in_range_12h,
    SAFE_DIVIDE(price - MIN(price) OVER w72, NULLIF(MAX(price) OVER w72 - MIN(price) OVER w72, 0)) AS pct_in_range_72h,

    -- Bollinger bandwidth (std / sma) on 24h
    (STDDEV_SAMP(price) OVER w24) / NULLIF(sma_24h, 0) AS bb_bw_24h,

    -- Extra vol ratios
    SAFE_DIVIDE(rv_12h, NULLIF(rv_72h, 0)) AS vol_ratio_12_72,
    SAFE_DIVIDE(rv_4h,  NULLIF(rv_12h, 0)) AS vol_ratio_4_12,

    -- Trend persistence
    SUM(CASE WHEN ret_1h >  0 THEN 1 ELSE 0 END) OVER w12 AS up_count_12h,
    SUM(CASE WHEN ret_1h <  0 THEN 1 ELSE 0 END) OVER w12 AS down_count_12h,
    SUM(CASE WHEN ret_1h >  0 THEN 1 ELSE 0 END) OVER w24 AS up_count_24h,
    SUM(CASE WHEN ret_1h <  0 THEN 1 ELSE 0 END) OVER w24 AS down_count_24h,

    -- Choppiness: sum(|ret|)/|sum(ret)| (clip to avoid div0)
    LEAST(
      50.0,
      GREATEST(
        SAFE_DIVIDE(SUM(ABS(COALESCE(ret_1h,0))) OVER w24, ABS(SUM(COALESCE(ret_1h,0)) OVER w24)),
        0.0
      )
    ) AS chop_24h,

    -- Price × Volume interaction
    COALESCE(ret_z_24h * volume_z_24h, 0) AS price_vol_interaction_24h

  FROM ols_feats s
  WINDOW
    w12 AS (PARTITION BY token_address ORDER BY ts_hour ROWS BETWEEN 11 PRECEDING AND CURRENT ROW),
    w24 AS (PARTITION BY token_address ORDER BY ts_hour ROWS BETWEEN 23 PRECEDING AND CURRENT ROW),
    w72 AS (PARTITION BY token_address ORDER BY ts_hour ROWS BETWEEN 71 PRECEDING AND CURRENT ROW)
)

-- ===== FINAL OUTPUT (cleaned & enriched) =====
SELECT
  token_address, monitoring_session_id, session_start, session_end,
  extended_start, extended_end, in_pre_extension, in_core_monitoring, in_post_extension,
  ts_hour, price,

  -- core returns/vol
  ret_1h, logret_1h,
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
  mean_ret_24h, std_ret_24h, mean_ret_72h, std_ret_72h, mean_ret_168h, std_ret_168h,
  -- housekeeping
  LEAST(GREATEST(volume_ret_1h, -10), 10) AS volume_ret_1h_norm,
    LEAST(GREATEST(volume_ret_24h, -10), 10) AS volume_ret_24h_norm,

    -- Standardize log_volume_per_supply per token (if included upstream)
    SAFE_DIVIDE(
      log_volume_per_supply - AVG(log_volume_per_supply) OVER w168,
      NULLIF(STDDEV_SAMP(log_volume_per_supply) OVER w168, 0)
    ) AS log_volume_per_supply_z,
  has_168h
  
  FROM structure_feats s
  WINDOW
    w168 AS (
      PARTITION BY token_address
      ORDER BY ts_hour
      ROWS BETWEEN 167 PRECEDING AND CURRENT ROW
    )
