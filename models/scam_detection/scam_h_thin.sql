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
  range_ratio,
  zero_vol_share,
  CASE WHEN
    range_ratio >= 0.30                      -- large swings
    AND zero_vol_share >= 0.40               -- lots of zero-volume candles
  THEN 1 ELSE 0 END AS pattern_low_vol_high_volatility
FROM f
WHERE
  range_ratio >= 0.30
  AND zero_vol_share >= 0.40