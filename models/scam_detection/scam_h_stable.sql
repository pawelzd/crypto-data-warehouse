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
  median_close,
  close_vol_ratio,
  max_to_min_ratio,

  CASE WHEN
    close_vol_ratio <= 0.01         -- extremely low volatility
    AND max_to_min_ratio <= 1.05    -- total variation <= 5%, independent of peg level
  THEN 1 ELSE 0 END AS pattern_stablecoin

FROM f
WHERE
  close_vol_ratio <= 0.01
  AND max_to_min_ratio <= 1.05
