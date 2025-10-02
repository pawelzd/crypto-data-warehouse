{{ config(
    schema='gold_ml_coins_mon_72',
    materialized='table'
) }}

-- Past features only + label (derived from minimal future fields).
-- We do NOT re-coalesce past fields (already validated upstream).

WITH past AS (
  SELECT
    token_address,
    monitoring_session_id,
    ts_hour AS decision_ts,   -- decision timestamp
    in_core_monitoring,
    has_168h AS has_full_lookback,

    -- engineered features (already null-safe upstream)
    price, ret_1h,
    logret_1h,
    mean_ret_24h,
    std_ret_24h,
    mean_ret_72h,
    std_ret_72h,
    mean_ret_168h,
    std_ret_168h,
    rv_24h,
    rv_4h,
    rv_12h,
    rv_7d,
    sharpe_24h,
    sharpe_7d,
    ret_z_24h,
    cumret_24h,
    cumret_7d,
    sma_6h,
    sma_12h,
    sma_24h,
    sma_48h,
    sma_72h,
    sma_168h,
    macd_sma_12_26h,
    price_z_24h,
    pct_in_range_24h,
    dist_to_sma_6h,
    dist_to_sma_12h,
    dist_to_sma_24h,
    dist_to_sma_72h,
    dist_to_sma_168h,
    dist_to_high_24h,
    dist_to_low_24h,
    dist_to_high_4h,
    dist_to_low_4h,
    dist_to_high_12h,
    dist_to_low_12h,
    breakout_high_24h,
    breakout_low_24h,
    drawdown_7d,
    drawdown_48h,
    drawdown_24h,
    has_168h,
    rsi_14,
    acf1_72h,
    dow_1_sun_7_sat,
    hour_of_day,
    sin_hour,
    cos_hour,
    sin_dow,
    cos_dow,
    vol_ratio_24_7d,
    vol_ratio_24_72,
    vol_ratio_72_168,
    vol_ratio_4_24,
    vol_ratio_12_24,
    ret_over_rv_12h
    sma_diff_12_48,
    sma_diff_fast_slow,
    sma6h_slope_12h,
    sma12h_slope_12h,
    sma12h_slope_72h,
    sma24h_slope_24h,
    sma48h_slope_24h
  FROM {{ ref('price_filter_72_features_7daysbefore') }}
),

-- Minimal future fields needed ONLY to compute the label and filter on full lookahead.
future_min AS (
  SELECT
    token_address,
    monitoring_session_id,
    first_acquired_timestamp,   -- aligns to past.decision_ts
    has_full_3d,                -- forward window completeness (filter)
    t_hit_dn_25,
    t_hit_dn_20,
    t_hit_dn_15,
    t_hit_up_35,                -- for label (+35%)
    t_hit_up_25,                -- for label (−25%)
    t_hit_up_20,
    t_hit_up_15,
    t_hit_up_10
  FROM {{ ref('price_filter_72_features_3daysafter') }}
),

joined AS (
  SELECT
    p.token_address,
    p.monitoring_session_id,
    p.decision_ts,

    -- filters/flags
    p.in_core_monitoring,
    p.has_full_lookback,
    f.has_full_3d AS has_full_lookahead,

    -- Non-null label components (use 75 sentinel = "never hit")
    LEAST(COALESCE(f.t_hit_up_35, 75), 75) AS t_hit_up_35_nn,
    LEAST(COALESCE(f.t_hit_up_25, 75), 75) AS t_hit_up_25_nn,
    LEAST(COALESCE(f.t_hit_up_20, 75), 75) AS t_hit_up_20_nn,
    LEAST(COALESCE(f.t_hit_up_15, 75), 75) AS t_hit_up_15_nn,
    LEAST(COALESCE(f.t_hit_up_10, 75), 75) AS t_hit_up_10_nn,
    LEAST(COALESCE(f.t_hit_dn_25, 75), 75) AS t_hit_dn_25_nn,
    LEAST(COALESCE(f.t_hit_dn_20, 75), 75) AS t_hit_dn_20_nn,
    LEAST(COALESCE(f.t_hit_dn_15, 75), 75) AS t_hit_dn_15_nn,

    -- All past features (unchanged)
    p.* EXCEPT(token_address, monitoring_session_id, decision_ts, in_core_monitoring, has_full_lookback)
  FROM past p
  JOIN future_min f
    ON p.token_address = f.token_address
   AND p.monitoring_session_id = f.monitoring_session_id
   AND p.decision_ts = f.first_acquired_timestamp
)

SELECT
  token_address,
  monitoring_session_id,
  decision_ts,

  -- Final label: +35% before −25% (always 0/1; no NULLs)
  CASE
    WHEN t_hit_up_35_nn < 75 AND t_hit_up_35_nn < t_hit_dn_25_nn THEN 1
    ELSE 0
  END AS label_profit35_before_loss25,

  CASE
    WHEN t_hit_up_20_nn < 75 AND t_hit_up_20_nn < t_hit_dn_25_nn THEN 1
    ELSE 0
  END AS label_profit20_before_loss25,

  CASE
    WHEN t_hit_up_15_nn < 75 AND t_hit_up_15_nn < t_hit_dn_20_nn THEN 1
    ELSE 0
  END AS label_profit15_before_loss20,

  CASE
    WHEN t_hit_up_25_nn < 75 AND t_hit_up_25_nn < t_hit_dn_25_nn THEN 1
    ELSE 0
  END AS label_profit25_before_loss25,

  CASE 
    WHEN t_hit_up_10_nn < 75 AND t_hit_up_10_nn < t_hit_dn_15_nn THEN 1
    ELSE 0
  END AS label_profit10_before_loss15,

  -- Flags for cleanliness (already validated upstream)
  in_core_monitoring,
  has_full_lookback,
  has_full_lookahead,

  -- All past features
  * EXCEPT(
    token_address,
    monitoring_session_id,
    decision_ts,
    -- exclude helper *_nn fields & intermediate flags to avoid leaking into model matrix
    t_hit_up_35_nn,
    t_hit_up_25_nn,
    t_hit_up_20_nn,
    t_hit_up_15_nn,
    t_hit_up_10_nn,
    t_hit_dn_25_nn,
    t_hit_dn_20_nn,
    t_hit_dn_15_nn,
    in_core_monitoring,
    has_full_lookback,
    has_full_lookahead,
    sma_6h,
    sma_12h,
    sma_24h,
    sma_48h,
    sma_72h,
    sma_168h,
    dist_to_sma_6h,
    dist_to_sma_12h,
    dist_to_sma_24h,
    dist_to_sma_72h,
    dist_to_sma_168h,
    cumret_24h, cumret_7d,
    mean_ret_24h, std_ret_24h, mean_ret_72h, std_ret_72h, mean_ret_168h, std_ret_168h,
    ret_1h, acf1_72h
  )
FROM joined
WHERE has_full_lookback = 1
  AND has_full_lookahead = 1
  AND in_core_monitoring = TRUE
ORDER BY token_address, decision_ts
