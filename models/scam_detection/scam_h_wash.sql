{{ config(materialized='view') }}

WITH f AS (
  SELECT *
  FROM {{ ref('scam_h_features') }}
)

SELECT
  chain,
  token_address,
  n,
  last_ts,
  close_vol_ratio,
  max_to_med_vol_ratio,
  zero_vol_share,
  CASE WHEN
    close_vol_ratio <= 0.03                 -- flat price
    AND max_to_med_vol_ratio >= 10          -- huge volume spikes vs median
    AND zero_vol_share <= 0.10              -- few zero-volume candles
  THEN 1 ELSE 0 END AS pattern_wash_trading
FROM f
WHERE
  close_vol_ratio <= 0.03
  AND max_to_med_vol_ratio >= 10
  AND zero_vol_share <= 0.10
