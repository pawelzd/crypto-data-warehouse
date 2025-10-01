{{ config(
  schema='gold_ml_coins_mon_72_prod',
  materialized='view'
) }}

WITH past AS (
  SELECT
  token_address,
  ts_hour AS decision_ts,   -- decision timestamp
  --has_168h AS has_full_lookback,

  price, ret_1h, logret_1h,
  sma_diff_fast_slow, sma6h_slope_24h, sma12h_slope_24h,
  mean_ret_24h, std_ret_24h, mean_ret_72h, std_ret_72h, mean_ret_168h, std_ret_168h,
  rv_24h, rv_7d, sharpe_24h, sharpe_7d, ret_z_24h, cumret_24h, cumret_7d,
  sma_6h, sma_12h, sma_24h, sma_72h, sma_168h, macd_sma_12_26h,
  price_z_24h, pct_in_range_24h,
  dist_to_sma_6h, dist_to_sma_12h, dist_to_sma_24h, dist_to_sma_72h, dist_to_sma_168h,
  dist_to_high_24h, dist_to_low_24h, breakout_high_24h, breakout_low_24h, drawdown_7d,
  rsi_14, acf1_72h,
  dow_1_sun_7_sat, hour_of_day, sin_hour, cos_hour, sin_dow, cos_dow,
  vol_ratio_24_7d, vol_ratio_24_72, vol_ratio_72_168, rsi_vol_interaction, sharpe_delta,
  miss_24h, miss_72h
  FROM {{ ref('price_filter_72_features_7daysbefore_prod') }}
)

SELECT
  token_address,
  decision_ts,
  --in_core_monitoring,
  --has_full_lookback,
  --has_full_lookahead,

  * EXCEPT(
  token_address,
  decision_ts
  --has_full_lookback,
  --has_full_lookahead
  )
FROM past
-- WHERE has_full_lookback = 1
--   AND has_full_lookahead = 1
ORDER BY token_address, decision_ts
