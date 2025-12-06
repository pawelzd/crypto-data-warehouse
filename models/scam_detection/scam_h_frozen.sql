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
  tiny_move_share,
  CASE WHEN
    close_vol_ratio <= 0.02     -- very low volatility
    AND tiny_move_share >= 0.70 -- 70%+ candles barely move
  THEN 1 ELSE 0 END AS pattern_frozen_price
FROM f
WHERE
  close_vol_ratio <= 0.02
  AND tiny_move_share >= 0.70
