{{ config(
    materialized='table'
) }}

{#----------------------------------------------------------
  Combine past features and future path metrics into a
  single training dataset at the decision hour grain.
  - Join keys: token_address, monitoring_session_id, ts_hour == first_acquired_timestamp
  - Filter to core monitoring hours with full history & full lookahead.
  - Build label: +35% before −25% within 7 days.
-----------------------------------------------------------#}

WITH past AS (
  SELECT
    token_address,
    monitoring_session_id,
    ts_hour,                 -- decision timestamp
    in_core_monitoring,
    has_168h,                -- full 7d lookback
    -- keep whatever features you engineered:
    price, ret_1h, logret_1h,
    mean_ret_24h, std_ret_24h, mean_ret_72h, std_ret_72h, mean_ret_168h, std_ret_168h,
    rv_24h, rv_7d, sharpe_24h, sharpe_7d, ret_z_24h, cumret_24h, cumret_7d,
    sma_6h, sma_12h, sma_24h, sma_72h, sma_168h, macd_sma_12_26h,
    price_z_24h, pct_in_range_24h,
    dist_to_sma_6h, dist_to_sma_12h, dist_to_sma_24h, dist_to_sma_72h, dist_to_sma_168h,
    dist_to_high_24h, dist_to_low_24h, breakout_high_24h, breakout_low_24h, drawdown_7d,
    rsi_14, acf1_72h,
    dow_1_sun_7_sat, hour_of_day, sin_hour, cos_hour, sin_dow, cos_dow,
    vol_ratio_24_7d, sharpe_delta,
    miss_24h, miss_72h, miss_168h
  FROM {{ ref('price_filter_features_7daysbefore') }}
),

future AS (
  SELECT
    token_address,
    monitoring_session_id,
    first_acquired_timestamp,  -- aligns to past.ts_hour
    has_full_7d,               -- full 0..168h forward window
    -- checkpoints (optional to keep for diagnostics)
    ret_24h, ret_48h, ret_72h, ret_96h, ret_120h, ret_144h, ret_168h,
    -- time-to-thresholds used for label
    t_hit_up_35,
    t_hit_dn_25,
    -- extras you might want to keep (optional)
    max_gain_from_entry_7d, max_drawdown_7d,
    t_peak_h, t_trough_h,
    rv_7d AS rv_7d_forward, std_logret_1h_7d, mean_logret_1h_7d, p95_abs_logret_1h,
    pct_pos_hours, sharpe_like_7d,
    slope_logprice_per_h, r2_logprice_trend,
    auc_cumret_avg, avg_cumret_0_24h, avg_cumret_25_72h, avg_cumret_73_168h
  FROM {{ ref('price_filter_features_7daysafter') }}
),

joined AS (
  SELECT
    p.token_address,
    p.monitoring_session_id,
    p.ts_hour AS decision_ts,

    -- filters/flags to keep
    p.in_core_monitoring,
    p.has_168h AS has_full_lookback,
    f.has_full_7d AS has_full_lookahead,

    -- label components
    CASE WHEN f.t_hit_up_35 IS NULL THEN NULL
         WHEN f.t_hit_up_35 >= 200 THEN NULL        -- never hit
         ELSE f.t_hit_up_35 END AS up_h,
    CASE WHEN f.t_hit_dn_25 IS NULL THEN NULL
         WHEN f.t_hit_dn_25 >= 200 THEN NULL
         ELSE f.t_hit_dn_25 END AS dn_h,

    -- past features (inputs)
    p.* EXCEPT(token_address, monitoring_session_id, ts_hour, in_core_monitoring, has_168h),

    -- future diagnostics (optional but useful for analysis/backtests)
    f.* EXCEPT(token_address, monitoring_session_id, first_acquired_timestamp, has_full_7d, t_hit_up_35, t_hit_dn_25)
  FROM past p
  JOIN future f
    ON p.token_address = f.token_address
   AND p.monitoring_session_id = f.monitoring_session_id
   AND p.ts_hour = f.first_acquired_timestamp
)

SELECT
  token_address,
  monitoring_session_id,
  decision_ts,

  -- Final label: +35% before −25%
  CASE
    WHEN up_h IS NOT NULL AND (dn_h IS NULL OR up_h < dn_h) THEN 1
    ELSE 0
  END AS label_profit35_before_loss25,

  -- Helpful label metadata
  up_h   AS hours_to_hit_more_35,
  dn_h   AS hours_to_hit_less_25,
  ret_168h AS forward_ret_7d,          -- sanity check
  max_gain_from_entry_7d,
  max_drawdown_7d,

  -- Input features
  in_core_monitoring,
  has_full_lookback,
  has_full_lookahead,

  -- (keep only decisions with full lookback & lookahead to avoid censoring)
  -- You can comment this WHERE out if you want soft filtering upstream.
  -- NOTE: leave it in to ensure clean supervised labels:
  -- WHERE has_full_lookback = 1 AND has_full_lookahead = 1 AND in_core_monitoring = TRUE

  -- All past features (already excludes key/flags duplicated above)
  *
FROM joined
WHERE has_full_lookback = 1
  AND has_full_lookahead = 1
  AND in_core_monitoring = TRUE
ORDER BY token_address, decision_ts
